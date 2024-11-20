mod error;
mod function;
mod io;
#[cfg(feature = "json")]
mod json;
mod pseudo;
mod schema;
mod storage;
mod translate;
mod types;
mod util;
mod vdbe;

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use fallible_iterator::FallibleIterator;
use log::trace;
use schema::Schema;
use sqlite3_parser::ast;
use sqlite3_parser::{ast::Cmd, lexer::sql::Parser};
use std::rc::Weak;
use std::sync::{Arc, OnceLock};
use std::{cell::RefCell, rc::Rc};
use storage::btree::btree_init_page;
#[cfg(feature = "fs")]
use storage::database::FileStorage;
use storage::pager::allocate_page;
use storage::sqlite3_ondisk::{DatabaseHeader, DATABASE_HEADER_SIZE};
pub use storage::wal::WalFile;
use util::parse_schema_rows;

use translate::optimizer::optimize_plan;
use translate::planner::prepare_select_plan;

pub use error::LimboError;
pub type Result<T> = std::result::Result<T, error::LimboError>;

pub use io::OpenFlags;
#[cfg(feature = "fs")]
pub use io::PlatformIO;
pub use io::{Buffer, Completion, File, WriteCompletion, IO};
pub use storage::buffer_pool::BufferPool;
pub use storage::database::DatabaseStorage;
pub use storage::pager::Page;
pub use storage::pager::Pager;
pub use storage::wal::CheckpointStatus;
pub use storage::wal::Wal;
pub use types::Value;

pub static DATABASE_VERSION: OnceLock<String> = OnceLock::new();

#[derive(Clone)]
enum TransactionState {
    Write,
    Read,
    None,
}

pub struct Database {
    pager: Rc<Pager>,
    schema: Rc<RefCell<Schema>>,
    header: Rc<RefCell<DatabaseHeader>>,
    transaction_state: RefCell<TransactionState>,
}

impl Database {
    #[cfg(feature = "fs")]
    pub fn open_file(io: Arc<dyn IO>, path: &str) -> Result<Rc<Database>> {
        let file = io.open_file(path, io::OpenFlags::Create, true)?;
        maybe_init_database_file(&file, &io)?;
        let page_io = Rc::new(FileStorage::new(file));
        let wal_path = format!("{}-wal", path);
        let db_header = Pager::begin_open(page_io.clone())?;
        io.run_once()?;
        let wal = Rc::new(RefCell::new(WalFile::new(
            io.clone(),
            wal_path,
            db_header.borrow().page_size as usize,
        )));
        Self::open(io, page_io, wal)
    }

    pub fn open(
        io: Arc<dyn IO>,
        page_io: Rc<dyn DatabaseStorage>,
        wal: Rc<RefCell<dyn Wal>>,
    ) -> Result<Rc<Database>> {
        let db_header = Pager::begin_open(page_io.clone())?;
        io.run_once()?;
        DATABASE_VERSION.get_or_init(|| {
            let version = db_header.borrow().version_number;
            version.to_string()
        });
        let pager = Rc::new(Pager::finish_open(
            db_header.clone(),
            page_io,
            wal,
            io.clone(),
        )?);
        let bootstrap_schema = Rc::new(RefCell::new(Schema::new()));
        let conn = Rc::new(Connection {
            pager: pager.clone(),
            schema: bootstrap_schema.clone(),
            header: db_header.clone(),
            db: Weak::new(),
        });
        let mut schema = Schema::new();
        let rows = conn.query("SELECT * FROM sqlite_schema")?;
        parse_schema_rows(rows, &mut schema, io)?;
        let schema = Rc::new(RefCell::new(schema));
        let header = db_header;
        Ok(Rc::new(Database {
            pager,
            schema,
            header,
            transaction_state: RefCell::new(TransactionState::None),
        }))
    }

    pub fn connect(self: &Rc<Database>) -> Rc<Connection> {
        Rc::new(Connection {
            pager: self.pager.clone(),
            schema: self.schema.clone(),
            header: self.header.clone(),
            db: Rc::downgrade(self),
        })
    }
}

pub fn maybe_init_database_file(file: &Rc<dyn File>, io: &Arc<dyn IO>) -> Result<()> {
    if file.size().unwrap() == 0 {
        // init db
        let db_header = DatabaseHeader::default();
        let page1 = allocate_page(
            1,
            &Rc::new(BufferPool::new(db_header.page_size as usize)),
            DATABASE_HEADER_SIZE,
        );
        {
            // Create the sqlite_schema table, for this we just need to create the btree page
            // for the first page of the database which is basically like any other btree page
            // but with a 100 byte offset, so we just init the page so that sqlite understands
            // this is a correct page.
            btree_init_page(
                &page1,
                storage::sqlite3_ondisk::PageType::TableLeaf,
                &db_header,
                DATABASE_HEADER_SIZE,
            );

            let mut page = page1.borrow_mut();
            let contents = page.contents.as_mut().unwrap();
            contents.write_database_header(&db_header);
            // write the first page to disk synchronously
            let flag_complete = Rc::new(RefCell::new(false));
            {
                let flag_complete = flag_complete.clone();
                let completion = Completion::Write(WriteCompletion::new(Box::new(move |_| {
                    *flag_complete.borrow_mut() = true;
                })));
                file.pwrite(0, contents.buffer.clone(), Rc::new(completion))
                    .unwrap();
            }
            io.run_once()?;
            assert!(*flag_complete.borrow());
        }
    };
    Ok(())
}

pub struct Connection {
    pager: Rc<Pager>,
    schema: Rc<RefCell<Schema>>,
    header: Rc<RefCell<DatabaseHeader>>,
    db: Weak<Database>, // backpointer to the database holding this connection
}

impl Connection {
    pub fn prepare(self: &Rc<Connection>, sql: impl Into<String>) -> Result<Statement> {
        let sql = sql.into();
        trace!("Preparing: {}", sql);
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        if let Some(cmd) = cmd {
            match cmd {
                Cmd::Stmt(stmt) => {
                    let program = Rc::new(translate::translate(
                        &*self.schema.borrow(),
                        stmt,
                        self.header.clone(),
                        self.pager.clone(),
                        Rc::downgrade(self),
                    )?);
                    Ok(Statement::new(program, self.pager.clone()))
                }
                Cmd::Explain(_stmt) => todo!(),
                Cmd::ExplainQueryPlan(_stmt) => todo!(),
            }
        } else {
            todo!()
        }
    }

    pub fn query(self: &Rc<Connection>, sql: impl Into<String>) -> Result<Option<Rows>> {
        let sql = sql.into();
        trace!("Querying: {}", sql);
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        if let Some(cmd) = cmd {
            match cmd {
                Cmd::Stmt(stmt) => {
                    let program = Rc::new(translate::translate(
                        &*self.schema.borrow(),
                        stmt,
                        self.header.clone(),
                        self.pager.clone(),
                        Rc::downgrade(&self),
                    )?);
                    let stmt = Statement::new(program, self.pager.clone());
                    Ok(Some(Rows { stmt }))
                }
                Cmd::Explain(stmt) => {
                    let program = translate::translate(
                        &*self.schema.borrow(),
                        stmt,
                        self.header.clone(),
                        self.pager.clone(),
                        Rc::downgrade(self),
                    )?;
                    program.explain();
                    Ok(None)
                }
                Cmd::ExplainQueryPlan(stmt) => {
                    match stmt {
                        ast::Stmt::Select(select) => {
                            let plan = prepare_select_plan(&*self.schema.borrow(), select)?;
                            let (plan, _) = optimize_plan(plan)?;
                            println!("{}", plan);
                        }
                        _ => todo!(),
                    }
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    pub fn execute(self: &Rc<Connection>, sql: impl Into<String>) -> Result<()> {
        let sql = sql.into();
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        if let Some(cmd) = cmd {
            match cmd {
                Cmd::Explain(stmt) => {
                    let program = translate::translate(
                        &*self.schema.borrow(),
                        stmt,
                        self.header.clone(),
                        self.pager.clone(),
                        Rc::downgrade(self),
                    )?;
                    program.explain();
                }
                Cmd::ExplainQueryPlan(_stmt) => todo!(),
                Cmd::Stmt(stmt) => {
                    let program = translate::translate(
                        &*self.schema.borrow(),
                        stmt,
                        self.header.clone(),
                        self.pager.clone(),
                        Rc::downgrade(self),
                    )?;
                    let mut state = vdbe::ProgramState::new(program.max_registers);
                    program.step(&mut state, self.pager.clone())?;
                }
            }
        }
        Ok(())
    }

    pub fn cacheflush(&self) -> Result<CheckpointStatus> {
        self.pager.cacheflush()
    }

    pub fn clear_page_cache(&self) -> Result<()> {
        self.pager.clear_page_cache();
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.pager.clear_page_cache();
        Ok(())
    }

    /// Close a connection and checkpoint.
    pub fn close(&self) -> Result<()> {
        loop {
            // TODO: make this async?
            match self.pager.checkpoint()? {
                CheckpointStatus::Done => {
                    return Ok(());
                }
                CheckpointStatus::IO => {
                    self.pager.io.run_once()?;
                }
            };
        }
    }
}

pub struct Statement {
    program: Rc<vdbe::Program>,
    state: vdbe::ProgramState,
    pager: Rc<Pager>,
}

impl Statement {
    pub fn new(program: Rc<vdbe::Program>, pager: Rc<Pager>) -> Self {
        let state = vdbe::ProgramState::new(program.max_registers);
        Self {
            program,
            state,
            pager,
        }
    }

    pub fn step(&mut self) -> Result<RowResult<'_>> {
        let result = self.program.step(&mut self.state, self.pager.clone())?;
        match result {
            vdbe::StepResult::Row(row) => Ok(RowResult::Row(Row { values: row.values })),
            vdbe::StepResult::IO => Ok(RowResult::IO),
            vdbe::StepResult::Done => Ok(RowResult::Done),
        }
    }

    pub fn query(&mut self) -> Result<Rows> {
        let stmt = Statement::new(self.program.clone(), self.pager.clone());
        Ok(Rows::new(stmt))
    }

    pub fn reset(&self) {}
}

pub enum RowResult<'a> {
    Row(Row<'a>),
    IO,
    Done,
}

pub struct Row<'a> {
    pub values: Vec<Value<'a>>,
}

impl<'a> Row<'a> {
    pub fn get<T: crate::types::FromValue<'a> + 'a>(&self, idx: usize) -> Result<T> {
        let value = &self.values[idx];
        T::from_value(value)
    }
}

pub struct Rows {
    stmt: Statement,
}

impl Rows {
    pub fn new(stmt: Statement) -> Self {
        Self { stmt }
    }

    pub fn next_row(&mut self) -> Result<RowResult<'_>> {
        self.stmt.step()
    }
}
