mod buffer_pool;
mod pager;
mod schema;
mod sqlite3_ondisk;
mod vdbe;

use anyhow::Result;
use fallible_iterator::FallibleIterator;
use pager::Pager;
use schema::Schema;
use sqlite3_parser::{ast::Cmd, lexer::sql::Parser};
use std::sync::Arc;

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
    pub fn query(&self, sql: impl Into<String>) -> Result<()> {
        let sql = sql.into();
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        if let Some(cmd) = cmd {
            match cmd {
                Cmd::Stmt(stmt) => {
                    let mut program = vdbe::translate(self.pager.clone(), &self.schema, stmt)?;
                    loop {
                        let result = program.step()?;
                        match result {
                            vdbe::StepResult::Row => {
                                let mut row = Vec::new();
                                for i in 0..program.column_count() {
                                    row.push(program.column(i).unwrap().to_string());
                                }
                                println!("{:?}", row);
                            }
                            vdbe::StepResult::IO => todo!(),
                            vdbe::StepResult::Done => break,
                        }
                    }
                }
                Cmd::Explain(stmt) => {
                    todo!();
                }
                Cmd::ExplainQueryPlan(_stmt) => todo!(),
            }
        }
        Ok(())
    }

    pub fn execute(&self, sql: impl Into<String>) -> Result<()> {
        let sql = sql.into();
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        if let Some(cmd) = cmd {
            match cmd {
                Cmd::Explain(stmt) => {
                    let program = vdbe::translate(self.pager.clone(), &self.schema, stmt)?;
                    program.explain();
                }
                Cmd::ExplainQueryPlan(_stmt) => todo!(),
                Cmd::Stmt(stmt) => {
                    let mut program = vdbe::translate(self.pager.clone(), &self.schema, stmt)?;
                    program.step()?;
                }
            }
        }
        Ok(())
    }
}

pub type DatabaseRef = usize;

pub trait IO {
    /// Open a database file.
    fn open(&self, path: &str) -> Result<DatabaseRef>;

    /// Get a page from the database file.
    fn get(&self, database_ref: DatabaseRef, page_idx: usize, buf: &mut [u8]) -> Result<()>;
}
