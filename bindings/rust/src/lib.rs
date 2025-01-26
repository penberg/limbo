pub mod params;
mod value;

pub use params::params_from_iter;

use crate::params::*;
use crate::value::*;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQL conversion failure: `{0}`")]
    ToSqlConversionFailure(BoxError),
}

impl From<limbo_core::LimboError> for Error {
    fn from(_err: limbo_core::LimboError) -> Self {
        todo!();
    }
}

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;
pub struct Builder {
    path: String,
}

impl Builder {
    pub fn new_local(path: &str) -> Self {
        Self {
            path: path.to_string(),
        }
    }

    #[allow(unused_variables, clippy::arc_with_non_send_sync)]
    pub async fn build(self) -> Result<Database> {
        match self.path.as_str() {
            ":memory:" => {
                let io: Arc<dyn limbo_core::IO> = Arc::new(limbo_core::MemoryIO::new()?);
                let db = limbo_core::Database::open_file(io, self.path.as_str())?;
                Ok(Database { inner: db })
            }
            _ => todo!(),
        }
    }
}

pub struct Database {
    inner: Arc<limbo_core::Database>,
}

impl Database {
    pub fn connect(self) -> Result<Connection> {
        let conn = self.inner.connect();
        Ok(Connection { inner: conn })
    }
}

pub struct Connection {
    inner: Rc<limbo_core::Connection>,
}

impl Connection {
    pub async fn query(&self, sql: &str, params: impl IntoParams) -> Result<Rows> {
        let mut stmt = self.prepare(sql).await?;
        stmt.query(params).await
    }

    pub async fn execute(&self, sql: &str, params: impl IntoParams) -> Result<u64> {
        let mut stmt = self.prepare(sql).await?;
        stmt.execute(params).await
    }

    pub async fn prepare(&self, sql: &str) -> Result<Statement> {
        let stmt = self.inner.prepare(sql)?;
        Ok(Statement {
            _inner: Rc::new(stmt),
        })
    }
}

pub struct Statement {
    _inner: Rc<limbo_core::Statement>,
}

impl Statement {
    pub async fn query(&mut self, params: impl IntoParams) -> Result<Rows> {
        let _params = params.into_params()?;
        todo!();
    }

    pub async fn execute(&mut self, params: impl IntoParams) -> Result<u64> {
        let _params = params.into_params()?;
        todo!();
    }
}

pub trait IntoValue {
    fn into_value(self) -> Result<Value>;
}

#[derive(Debug, Clone)]
pub enum Params {
    None,
    Positional(Vec<Value>),
    Named(Vec<(String, Value)>),
}
pub struct Transaction {}

pub struct Rows {
    _inner: Rc<limbo_core::Statement>,
}

impl Rows {
    pub async fn next(&mut self) -> Result<Option<Row>> {
        todo!();
    }
}

pub struct Row {}

impl Row {
    pub fn get_value(&self, _index: usize) -> Result<Value> {
        todo!();
    }
}
