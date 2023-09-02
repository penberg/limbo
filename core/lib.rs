mod btree;
mod buffer_pool;
mod pager;
mod schema;
mod sqlite3_ondisk;
mod types;
mod vdbe;

use anyhow::Result;
use fallible_iterator::FallibleIterator;
use pager::Pager;
use schema::Schema;
use sqlite3_parser::{ast::Cmd, lexer::sql::Parser};
use std::sync::Arc;

pub use types::Value;

pub struct Database {
    pager: Arc<Pager>,
    schema: Arc<Schema>,
}

impl Database {
    pub fn open(io: Arc<dyn IO>, path: &str) -> Result<Database> {
        let pager = Arc::new(Pager::open(io.clone(), path)?);
        let schema = Arc::new(Schema::new());
        let conn = Connection {
            pager: pager.clone(),
            schema: schema.clone(),
        };
        conn.query("SELECT * FROM sqlite_schema")?;
        Ok(Database { pager, schema })
    }

    pub fn connect(&self) -> Connection {
        Connection {
            pager: self.pager.clone(),
            schema: self.schema.clone(),
        }
    }
}

pub struct Connection {
    pager: Arc<Pager>,
    schema: Arc<Schema>,
}

impl Connection {
    pub fn query(&self, sql: impl Into<String>) -> Result<Option<Rows>> {
        let sql = sql.into();
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        if let Some(cmd) = cmd {
            match cmd {
                Cmd::Stmt(stmt) => {
                    let program = vdbe::translate(&self.schema, stmt)?;
                    let mut state =
                        vdbe::ProgramState::new(self.pager.clone(), program.max_registers);
                    Ok(Some(Rows::new(state, program)))
                }
                Cmd::Explain(stmt) => {
                    let program = vdbe::translate(&self.schema, stmt)?;
                    program.explain();
                    Ok(None)
                }
                Cmd::ExplainQueryPlan(_stmt) => Ok(None),
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
                    let program = vdbe::translate(&self.schema, stmt)?;
                    program.explain();
                }
                Cmd::ExplainQueryPlan(_stmt) => todo!(),
                Cmd::Stmt(stmt) => {
                    let program = vdbe::translate(&self.schema, stmt)?;
                    let mut state =
                        vdbe::ProgramState::new(self.pager.clone(), program.max_registers);
                    program.step(&mut state)?;
                }
            }
        }
        Ok(())
    }
}

pub struct Rows {
    state: vdbe::ProgramState,
    program: vdbe::Program,
}

impl Rows {
    pub fn new(state: vdbe::ProgramState, program: vdbe::Program) -> Self {
        Self { state, program }
    }

    pub fn next(&mut self) -> Result<Option<crate::types::Record>> {
        loop {
            let result = self.program.step(&mut self.state)?;
            match result {
                vdbe::StepResult::Row(row) => {
                    return Ok(Some(row));
                }
                vdbe::StepResult::IO => todo!(),
                vdbe::StepResult::Done => {
                    return Ok(None);
                }
            }
        }
    }
}

pub type DatabaseRef = usize;

pub trait IO {
    /// Open a database file.
    fn open(&self, path: &str) -> Result<DatabaseRef>;

    /// Get a page from the database file.
    fn get(&self, database_ref: DatabaseRef, page_idx: usize, buf: &mut [u8]) -> Result<()>;
}
