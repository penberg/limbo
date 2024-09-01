mod error;
mod function;
mod io;
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
use std::sync::Arc;
use std::{cell::RefCell, rc::Rc};
#[cfg(feature = "fs")]
use storage::database::FileStorage;
use storage::pager::Pager;
use storage::sqlite3_ondisk::DatabaseHeader;
#[cfg(feature = "fs")]
use storage::wal::WalFile;

use translate::optimizer::optimize_plan;
use translate::planner::prepare_select_plan;

pub use error::LimboError;
pub type Result<T> = std::result::Result<T, error::LimboError>;

#[cfg(feature = "fs")]
pub use io::PlatformIO;
pub use io::{Buffer, Completion, File, WriteCompletion, IO};
pub use storage::database::DatabaseStorage;
pub use storage::pager::Page;
pub use storage::wal::Wal;
pub use types::Value;

pub struct Database {
    pager: Rc<Pager>,
    schema: Rc<Schema>,
    header: Rc<RefCell<DatabaseHeader>>,
}

impl Database {
    #[cfg(feature = "fs")]
    pub fn open_file(io: Arc<dyn crate::io::IO>, path: &str) -> Result<Database> {
        let file = io.open_file(path)?;
        let page_io = Rc::new(FileStorage::new(file));
        let wal_path = format!("{}-wal", path);
        let wal = Rc::new(WalFile::new(io.clone(), wal_path));
        Self::open(io, page_io, wal)
    }

    pub fn open(
        io: Arc<dyn crate::io::IO>,
        page_io: Rc<dyn DatabaseStorage>,
        wal: Rc<dyn Wal>,
    ) -> Result<Database> {
        let db_header = Pager::begin_open(page_io.clone())?;
        io.run_once()?;
        let pager = Rc::new(Pager::finish_open(
            db_header.clone(),
            page_io,
            wal,
            io.clone(),
        )?);
        let bootstrap_schema = Rc::new(Schema::new());
        let conn = Connection {
            pager: pager.clone(),
            schema: bootstrap_schema.clone(),
            header: db_header.clone(),
        };
        let mut schema = Schema::new();
        let rows = conn.query("SELECT * FROM sqlite_schema")?;
        if let Some(mut rows) = rows {
            loop {
                match rows.next_row()? {
                    RowResult::Row(row) => {
                        let ty = row.get::<&str>(0)?;
                        if ty != "table" && ty != "index"{
                            continue;
                        }
                        match ty {
                            "table" => {
                                let root_page: i64 = row.get::<i64>(3)?;
                                let sql: &str = row.get::<&str>(4)?;
                                let table = schema::BTreeTable::from_sql(sql, root_page as usize)?;
                                schema.add_table(Rc::new(table));
                            }
                            "index" => {
                                let root_page: i64 = row.get::<i64>(3)?;
                                let sql: &str = row.get::<&str>(4)?;
                                let index = schema::Index::from_sql(sql, root_page as usize)?;
                                schema.add_index(Rc::new(index));
                            }
                            _ => continue
                        }
                    }
                    RowResult::IO => {
                        // TODO: How do we ensure that the I/O we submitted to
                        // read the schema is actually complete?
                        io.run_once()?;
                    }
                    RowResult::Done => break,
                }
            }
        }
        let schema = Rc::new(schema);
        let header = db_header;
        Ok(Database {
            pager,
            schema,
            header,
        })
    }

    pub fn connect(&self) -> Connection {
        Connection {
            pager: self.pager.clone(),
            schema: self.schema.clone(),
            header: self.header.clone(),
        }
    }
}

pub struct Connection {
    pager: Rc<Pager>,
    schema: Rc<Schema>,
    header: Rc<RefCell<DatabaseHeader>>,
}

impl Connection {
    pub fn prepare(&self, sql: impl Into<String>) -> Result<Statement> {
        let sql = sql.into();
        trace!("Preparing: {}", sql);
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        if let Some(cmd) = cmd {
            match cmd {
                Cmd::Stmt(stmt) => {
                    let program = Rc::new(translate::translate(
                        &self.schema,
                        stmt,
                        self.header.clone(),
                        self.pager.clone(),
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

    pub fn query(&self, sql: impl Into<String>) -> Result<Option<Rows>> {
        let sql = sql.into();
        trace!("Querying: {}", sql);
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        if let Some(cmd) = cmd {
            match cmd {
                Cmd::Stmt(stmt) => {
                    let program = Rc::new(translate::translate(
                        &self.schema,
                        stmt,
                        self.header.clone(),
                        self.pager.clone(),
                    )?);
                    let stmt = Statement::new(program, self.pager.clone());
                    Ok(Some(Rows { stmt }))
                }
                Cmd::Explain(stmt) => {
                    let program = translate::translate(
                        &self.schema,
                        stmt,
                        self.header.clone(),
                        self.pager.clone(),
                    )?;
                    program.explain();
                    Ok(None)
                }
                Cmd::ExplainQueryPlan(stmt) => {
                    match stmt {
                        ast::Stmt::Select(select) => {
                            let plan = prepare_select_plan(&self.schema, select)?;
                            let plan = optimize_plan(plan)?;
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

    pub fn execute(&self, sql: impl Into<String>) -> Result<()> {
        let sql = sql.into();
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        if let Some(cmd) = cmd {
            match cmd {
                Cmd::Explain(stmt) => {
                    let program = translate::translate(
                        &self.schema,
                        stmt,
                        self.header.clone(),
                        self.pager.clone(),
                    )?;
                    program.explain();
                }
                Cmd::ExplainQueryPlan(_stmt) => todo!(),
                Cmd::Stmt(stmt) => {
                    let program = translate::translate(
                        &self.schema,
                        stmt,
                        self.header.clone(),
                        self.pager.clone(),
                    )?;
                    let mut state = vdbe::ProgramState::new(program.max_registers);
                    program.step(&mut state, self.pager.clone())?;
                }
            }
        }
        Ok(())
    }

    pub fn cacheflush(&self) -> Result<()> {
        self.pager.cacheflush()?;
        Ok(())
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
