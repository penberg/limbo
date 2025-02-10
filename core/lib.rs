mod error;
mod ext;
mod function;
mod functions;
mod info;
mod io;
#[cfg(feature = "json")]
mod json;
pub mod mvcc;
mod parameters;
mod pseudo;
pub mod result;
mod schema;
mod storage;
mod translate;
mod types;
mod util;
mod vdbe;
mod vector;

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use fallible_iterator::FallibleIterator;
#[cfg(not(target_family = "wasm"))]
use libloading::{Library, Symbol};
#[cfg(not(target_family = "wasm"))]
use limbo_ext::{ExtensionApi, ExtensionEntryPoint};
use limbo_ext::{ResultCode, VTabModuleImpl, Value as ExtValue};
use log::trace;
use parking_lot::RwLock;
use schema::{Column, Schema};
use sqlite3_parser::{ast, ast::Cmd, lexer::sql::Parser};
use std::cell::Cell;
use std::collections::HashMap;
use std::num::NonZero;
use std::sync::{Arc, OnceLock};
use std::{cell::RefCell, rc::Rc};
use storage::btree::btree_init_page;
#[cfg(feature = "fs")]
use storage::database::FileStorage;
use storage::page_cache::DumbLruPageCache;
use storage::pager::allocate_page;
pub use storage::pager::PageRef;
use storage::sqlite3_ondisk::{DatabaseHeader, DATABASE_HEADER_SIZE};
pub use storage::wal::CheckpointMode;
pub use storage::wal::WalFile;
pub use storage::wal::WalFileShared;
use types::OwnedValue;
pub use types::Value;
use util::parse_schema_rows;
use vdbe::builder::QueryMode;
use vdbe::VTabOpaqueCursor;

pub use error::LimboError;
use translate::select::prepare_select_plan;
pub type Result<T, E = LimboError> = std::result::Result<T, E>;

use crate::storage::wal::CheckpointResult;
use crate::translate::optimizer::optimize_plan;
pub use io::OpenFlags;
pub use io::PlatformIO;
#[cfg(all(feature = "fs", target_family = "unix"))]
pub use io::UnixIO;
#[cfg(all(feature = "fs", target_os = "linux", feature = "io_uring"))]
pub use io::UringIO;
pub use io::{Buffer, Completion, File, MemoryIO, WriteCompletion, IO};
pub use storage::buffer_pool::BufferPool;
pub use storage::database::DatabaseStorage;
pub use storage::pager::Page;
pub use storage::pager::Pager;
pub use storage::wal::CheckpointStatus;
pub use storage::wal::Wal;

pub static DATABASE_VERSION: OnceLock<String> = OnceLock::new();

#[derive(Clone, PartialEq, Eq)]
enum TransactionState {
    Write,
    Read,
    None,
}

pub struct Database {
    pager: Rc<Pager>,
    schema: Rc<RefCell<Schema>>,
    header: Rc<RefCell<DatabaseHeader>>,
    syms: Rc<RefCell<SymbolTable>>,
    vtab_modules: HashMap<String, Rc<VTabModuleImpl>>,
    // Shared structures of a Database are the parts that are common to multiple threads that might
    // create DB connections.
    _shared_page_cache: Arc<RwLock<DumbLruPageCache>>,
    _shared_wal: Arc<RwLock<WalFileShared>>,
}

impl Database {
    #[cfg(feature = "fs")]
    pub fn open_file(io: Arc<dyn IO>, path: &str) -> Result<Arc<Database>> {
        use storage::wal::WalFileShared;

        let file = io.open_file(path, OpenFlags::Create, true)?;
        maybe_init_database_file(&file, &io)?;
        let page_io = Rc::new(FileStorage::new(file));
        let wal_path = format!("{}-wal", path);
        let db_header = Pager::begin_open(page_io.clone())?;
        io.run_once()?;
        let page_size = db_header.borrow().page_size;
        let wal_shared = WalFileShared::open_shared(&io, wal_path.as_str(), page_size)?;
        let buffer_pool = Rc::new(BufferPool::new(page_size as usize));
        let wal = Rc::new(RefCell::new(WalFile::new(
            io.clone(),
            db_header.borrow().page_size as usize,
            wal_shared.clone(),
            buffer_pool.clone(),
        )));
        Self::open(io, page_io, wal, wal_shared, buffer_pool)
    }

    #[allow(clippy::arc_with_non_send_sync)]
    pub fn open(
        io: Arc<dyn IO>,
        page_io: Rc<dyn DatabaseStorage>,
        wal: Rc<RefCell<dyn Wal>>,
        shared_wal: Arc<RwLock<WalFileShared>>,
        buffer_pool: Rc<BufferPool>,
    ) -> Result<Arc<Database>> {
        let db_header = Pager::begin_open(page_io.clone())?;
        io.run_once()?;
        DATABASE_VERSION.get_or_init(|| {
            let version = db_header.borrow().version_number;
            version.to_string()
        });
        let _shared_page_cache = Arc::new(RwLock::new(DumbLruPageCache::new(10)));
        let pager = Rc::new(Pager::finish_open(
            db_header.clone(),
            page_io,
            wal,
            io.clone(),
            _shared_page_cache.clone(),
            buffer_pool,
        )?);
        let header = db_header;
        let schema = Rc::new(RefCell::new(Schema::new()));
        let syms = Rc::new(RefCell::new(SymbolTable::new()));
        let db = Database {
            pager: pager.clone(),
            schema: schema.clone(),
            header: header.clone(),
            _shared_page_cache: _shared_page_cache.clone(),
            _shared_wal: shared_wal.clone(),
            syms,
            vtab_modules: HashMap::new(),
        };
        if let Err(e) = db.register_builtins() {
            return Err(LimboError::ExtensionError(e));
        }
        let db = Arc::new(db);
        let conn = Rc::new(Connection {
            db: db.clone(),
            pager,
            schema: schema.clone(),
            header,
            transaction_state: RefCell::new(TransactionState::None),
            last_insert_rowid: Cell::new(0),
            last_change: Cell::new(0),
            total_changes: Cell::new(0),
        });
        let rows = conn.query("SELECT * FROM sqlite_schema")?;
        let mut schema = schema.borrow_mut();
        parse_schema_rows(rows, &mut schema, io)?;
        Ok(db)
    }

    pub fn connect(self: &Arc<Database>) -> Rc<Connection> {
        Rc::new(Connection {
            db: self.clone(),
            pager: self.pager.clone(),
            schema: self.schema.clone(),
            header: self.header.clone(),
            last_insert_rowid: Cell::new(0),
            transaction_state: RefCell::new(TransactionState::None),
            last_change: Cell::new(0),
            total_changes: Cell::new(0),
        })
    }

    #[cfg(not(target_family = "wasm"))]
    pub fn load_extension<P: AsRef<std::ffi::OsStr>>(&self, path: P) -> Result<()> {
        let api = Box::new(self.build_limbo_ext());
        let lib =
            unsafe { Library::new(path).map_err(|e| LimboError::ExtensionError(e.to_string()))? };
        let entry: Symbol<ExtensionEntryPoint> = unsafe {
            lib.get(b"register_extension")
                .map_err(|e| LimboError::ExtensionError(e.to_string()))?
        };
        let api_ptr: *const ExtensionApi = Box::into_raw(api);
        let result_code = unsafe { entry(api_ptr) };
        if result_code.is_ok() {
            self.syms.borrow_mut().extensions.push((lib, api_ptr));
            Ok(())
        } else {
            if !api_ptr.is_null() {
                let _ = unsafe { Box::from_raw(api_ptr.cast_mut()) };
            }
            Err(LimboError::ExtensionError(
                "Extension registration failed".to_string(),
            ))
        }
    }
}

pub fn maybe_init_database_file(file: &Rc<dyn File>, io: &Arc<dyn IO>) -> Result<()> {
    if file.size()? == 0 {
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

            let contents = page1.get().contents.as_mut().unwrap();
            contents.write_database_header(&db_header);
            // write the first page to disk synchronously
            let flag_complete = Rc::new(RefCell::new(false));
            {
                let flag_complete = flag_complete.clone();
                let completion = Completion::Write(WriteCompletion::new(Box::new(move |_| {
                    *flag_complete.borrow_mut() = true;
                })));
                file.pwrite(0, contents.buffer.clone(), completion)?;
            }
            let mut limit = 100;
            loop {
                io.run_once()?;
                if *flag_complete.borrow() {
                    break;
                }
                limit -= 1;
                if limit == 0 {
                    panic!("Database file couldn't be initialized, io loop run for {} iterations and write didn't finish", limit);
                }
            }
        }
    };
    Ok(())
}

pub struct Connection {
    db: Arc<Database>,
    pager: Rc<Pager>,
    schema: Rc<RefCell<Schema>>,
    header: Rc<RefCell<DatabaseHeader>>,
    transaction_state: RefCell<TransactionState>,
    last_insert_rowid: Cell<u64>,
    last_change: Cell<i64>,
    total_changes: Cell<i64>,
}

impl Connection {
    pub fn prepare(self: &Rc<Connection>, sql: impl AsRef<str>) -> Result<Statement> {
        let sql = sql.as_ref();
        trace!("Preparing: {}", sql);
        let db = &self.db;
        let mut parser = Parser::new(sql.as_bytes());
        let syms = &db.syms.borrow();
        let cmd = parser.next()?;
        if let Some(cmd) = cmd {
            match cmd {
                Cmd::Stmt(stmt) => {
                    let program = Rc::new(translate::translate(
                        &self.schema.borrow(),
                        stmt,
                        self.header.clone(),
                        self.pager.clone(),
                        Rc::downgrade(self),
                        syms,
                        QueryMode::Normal,
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

    pub fn query(self: &Rc<Connection>, sql: impl AsRef<str>) -> Result<Option<Statement>> {
        let sql = sql.as_ref();
        trace!("Querying: {}", sql);
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        match cmd {
            Some(cmd) => self.run_cmd(cmd),
            None => Ok(None),
        }
    }

    pub(crate) fn run_cmd(self: &Rc<Connection>, cmd: Cmd) -> Result<Option<Statement>> {
        let db = self.db.clone();
        let syms: &SymbolTable = &db.syms.borrow();
        match cmd {
            Cmd::Stmt(stmt) => {
                let program = Rc::new(translate::translate(
                    &self.schema.borrow(),
                    stmt,
                    self.header.clone(),
                    self.pager.clone(),
                    Rc::downgrade(self),
                    syms,
                    QueryMode::Normal,
                )?);
                let stmt = Statement::new(program, self.pager.clone());
                Ok(Some(stmt))
            }
            Cmd::Explain(stmt) => {
                let program = translate::translate(
                    &self.schema.borrow(),
                    stmt,
                    self.header.clone(),
                    self.pager.clone(),
                    Rc::downgrade(self),
                    syms,
                    QueryMode::Explain,
                )?;
                program.explain();
                Ok(None)
            }
            Cmd::ExplainQueryPlan(stmt) => {
                match stmt {
                    ast::Stmt::Select(select) => {
                        let mut plan = prepare_select_plan(
                            &self.schema.borrow(),
                            *select,
                            &self.db.syms.borrow(),
                            None,
                        )?;
                        optimize_plan(&mut plan, &self.schema.borrow())?;
                        println!("{}", plan);
                    }
                    _ => todo!(),
                }
                Ok(None)
            }
        }
    }

    pub fn query_runner<'a>(self: &'a Rc<Connection>, sql: &'a [u8]) -> QueryRunner<'a> {
        QueryRunner::new(self, sql)
    }

    pub fn execute(self: &Rc<Connection>, sql: impl AsRef<str>) -> Result<()> {
        let sql = sql.as_ref();
        let db = &self.db;
        let syms: &SymbolTable = &db.syms.borrow();
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        if let Some(cmd) = cmd {
            match cmd {
                Cmd::Explain(stmt) => {
                    let program = translate::translate(
                        &self.schema.borrow(),
                        stmt,
                        self.header.clone(),
                        self.pager.clone(),
                        Rc::downgrade(self),
                        syms,
                        QueryMode::Explain,
                    )?;
                    program.explain();
                }
                Cmd::ExplainQueryPlan(_stmt) => todo!(),
                Cmd::Stmt(stmt) => {
                    let program = translate::translate(
                        &self.schema.borrow(),
                        stmt,
                        self.header.clone(),
                        self.pager.clone(),
                        Rc::downgrade(self),
                        syms,
                        QueryMode::Normal,
                    )?;

                    let mut state =
                        vdbe::ProgramState::new(program.max_registers, program.cursor_ref.len());
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

    pub fn checkpoint(&self) -> Result<CheckpointResult> {
        let checkpoint_result = self.pager.clear_page_cache();
        Ok(checkpoint_result)
    }

    #[cfg(not(target_family = "wasm"))]
    pub fn load_extension<P: AsRef<std::ffi::OsStr>>(&self, path: P) -> Result<()> {
        Database::load_extension(&self.db, path)
    }

    /// Close a connection and checkpoint.
    pub fn close(&self) -> Result<()> {
        loop {
            // TODO: make this async?
            match self.pager.checkpoint()? {
                CheckpointStatus::Done(_) => {
                    return Ok(());
                }
                CheckpointStatus::IO => {
                    self.pager.io.run_once()?;
                }
            };
        }
    }

    pub fn last_insert_rowid(&self) -> u64 {
        self.last_insert_rowid.get()
    }

    fn update_last_rowid(&self, rowid: u64) {
        self.last_insert_rowid.set(rowid);
    }

    pub fn set_changes(&self, nchange: i64) {
        self.last_change.set(nchange);
        let prev_total_changes = self.total_changes.get();
        self.total_changes.set(prev_total_changes + nchange);
    }

    pub fn total_changes(&self) -> i64 {
        self.total_changes.get()
    }
}

pub struct Statement {
    program: Rc<vdbe::Program>,
    state: vdbe::ProgramState,
    pager: Rc<Pager>,
}

impl Statement {
    pub fn new(program: Rc<vdbe::Program>, pager: Rc<Pager>) -> Self {
        let state = vdbe::ProgramState::new(program.max_registers, program.cursor_ref.len());
        Self {
            program,
            state,
            pager,
        }
    }

    pub fn interrupt(&mut self) {
        self.state.interrupt();
    }

    pub fn step(&mut self) -> Result<StepResult> {
        self.program.step(&mut self.state, self.pager.clone())
    }

    pub fn num_columns(&self) -> usize {
        self.program.result_columns.len()
    }

    pub fn get_column_name(&self, idx: usize) -> Option<&String> {
        self.program.result_columns[idx].name(&self.program.table_references)
    }

    pub fn parameters(&self) -> &parameters::Parameters {
        &self.program.parameters
    }

    pub fn parameters_count(&self) -> usize {
        self.program.parameters.count()
    }

    pub fn bind_at(&mut self, index: NonZero<usize>, value: Value) {
        self.state.bind_at(index, value.into());
    }

    pub fn reset(&mut self) {
        self.state.reset();
    }

    pub fn row(&self) -> Option<&Row> {
        self.state.result_row.as_ref()
    }
}

pub type Row = types::Record;

pub type StepResult = vdbe::StepResult;

#[derive(Clone, Debug)]
pub struct VirtualTable {
    name: String,
    args: Option<Vec<ast::Expr>>,
    pub implementation: Rc<VTabModuleImpl>,
    columns: Vec<Column>,
}

impl VirtualTable {
    pub fn open(&self) -> VTabOpaqueCursor {
        let cursor = unsafe { (self.implementation.open)() };
        VTabOpaqueCursor::new(cursor)
    }

    pub fn filter(
        &self,
        cursor: &VTabOpaqueCursor,
        arg_count: usize,
        args: Vec<OwnedValue>,
    ) -> Result<bool> {
        let mut filter_args = Vec::with_capacity(arg_count);
        for i in 0..arg_count {
            let ownedvalue_arg = args.get(i).unwrap();
            let extvalue_arg: ExtValue = match ownedvalue_arg {
                OwnedValue::Null => Ok(ExtValue::null()),
                OwnedValue::Integer(i) => Ok(ExtValue::from_integer(*i)),
                OwnedValue::Float(f) => Ok(ExtValue::from_float(*f)),
                OwnedValue::Text(t) => Ok(ExtValue::from_text(t.as_str().to_string())),
                OwnedValue::Blob(b) => Ok(ExtValue::from_blob((**b).clone())),
                other => Err(LimboError::ExtensionError(format!(
                    "Unsupported value type: {:?}",
                    other
                ))),
            }?;
            filter_args.push(extvalue_arg);
        }
        let rc = unsafe {
            (self.implementation.filter)(cursor.as_ptr(), arg_count as i32, filter_args.as_ptr())
        };
        match rc {
            ResultCode::OK => Ok(true),
            ResultCode::EOF => Ok(false),
            _ => Err(LimboError::ExtensionError(rc.to_string())),
        }
    }

    pub fn column(&self, cursor: &VTabOpaqueCursor, column: usize) -> Result<OwnedValue> {
        let val = unsafe { (self.implementation.column)(cursor.as_ptr(), column as u32) };
        OwnedValue::from_ffi(&val)
    }

    pub fn next(&self, cursor: &VTabOpaqueCursor) -> Result<bool> {
        let rc = unsafe { (self.implementation.next)(cursor.as_ptr()) };
        match rc {
            ResultCode::OK => Ok(true),
            ResultCode::EOF => Ok(false),
            _ => Err(LimboError::ExtensionError("Next failed".to_string())),
        }
    }
}

pub(crate) struct SymbolTable {
    pub functions: HashMap<String, Rc<function::ExternalFunc>>,
    #[cfg(not(target_family = "wasm"))]
    extensions: Vec<(Library, *const ExtensionApi)>,
    pub vtabs: HashMap<String, VirtualTable>,
}

impl std::fmt::Debug for SymbolTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SymbolTable")
            .field("functions", &self.functions)
            .finish()
    }
}

fn is_shared_library(path: &std::path::Path) -> bool {
    path.extension()
        .map_or(false, |ext| ext == "so" || ext == "dylib" || ext == "dll")
}

pub fn resolve_ext_path(extpath: &str) -> Result<std::path::PathBuf> {
    let path = std::path::Path::new(extpath);
    if !path.exists() {
        if is_shared_library(path) {
            return Err(LimboError::ExtensionError(format!(
                "Extension file not found: {}",
                extpath
            )));
        };
        let maybe = path.with_extension(std::env::consts::DLL_EXTENSION);
        maybe
            .exists()
            .then_some(maybe)
            .ok_or(LimboError::ExtensionError(format!(
                "Extension file not found: {}",
                extpath
            )))
    } else {
        Ok(path.to_path_buf())
    }
}

impl SymbolTable {
    pub fn new() -> Self {
        Self {
            functions: HashMap::new(),
            vtabs: HashMap::new(),
            #[cfg(not(target_family = "wasm"))]
            extensions: Vec::new(),
        }
    }

    pub fn resolve_function(
        &self,
        name: &str,
        _arg_count: usize,
    ) -> Option<Rc<function::ExternalFunc>> {
        self.functions.get(name).cloned()
    }
}

pub struct QueryRunner<'a> {
    parser: Parser<'a>,
    conn: &'a Rc<Connection>,
}

impl<'a> QueryRunner<'a> {
    pub(crate) fn new(conn: &'a Rc<Connection>, statements: &'a [u8]) -> Self {
        Self {
            parser: Parser::new(statements),
            conn,
        }
    }
}

impl Iterator for QueryRunner<'_> {
    type Item = Result<Option<Statement>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.parser.next() {
            Ok(Some(cmd)) => Some(self.conn.run_cmd(cmd)),
            Ok(None) => None,
            Err(err) => {
                self.parser.finalize();
                Some(Result::Err(LimboError::from(err)))
            }
        }
    }
}
