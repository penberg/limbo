use std::any::Any;
use std::rc::Rc;
use std::sync::Arc;

pub type Result<T> = std::result::Result<T, LimboApiError>;

pub trait Extension {
    fn load(&self) -> Result<()>;
}

#[derive(Debug)]
pub enum LimboApiError {
    ConnectionError(String),
    RegisterFunctionError(String),
    ValueError(String),
    VTableError(String),
}

impl From<std::io::Error> for LimboApiError {
    fn from(e: std::io::Error) -> Self {
        Self::ConnectionError(e.to_string())
    }
}

impl std::fmt::Display for LimboApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::ConnectionError(e) => write!(f, "Connection error: {e}"),
            Self::RegisterFunctionError(e) => write!(f, "Register function error: {e}"),
            Self::ValueError(e) => write!(f, "Value error: {e}"),
            Self::VTableError(e) => write!(f, "VTable error: {e}"),
        }
    }
}

pub trait ExtensionApi {
    fn register_scalar_function(&self, name: &str, func: Arc<dyn ScalarFunction>) -> Result<()>;
    fn register_aggregate_function(
        &self,
        name: &str,
        func: Arc<dyn AggregateFunction>,
    ) -> Result<()>;
    fn register_virtual_table(&self, name: &str, table: Arc<dyn VirtualTable>) -> Result<()>;
}

pub trait ScalarFunction {
    fn execute(&self, args: &[Value]) -> Result<Value>;
}

pub trait AggregateFunction {
    fn init(&self) -> Box<dyn Any>;
    fn step(&self, state: &mut dyn Any, args: &[Value]) -> Result<()>;
    fn finalize(&self, state: Box<dyn Any>) -> Result<Value>;
}

pub trait VirtualTable {
    fn schema(&self) -> &'static str;
    fn create_cursor(&self) -> Box<dyn Cursor>;
}

pub trait Cursor {
    fn next(&mut self) -> Result<Option<Row>>;
}

pub struct Row {
    pub values: Vec<Value>,
}

pub enum Value {
    Text(String),
    Blob(Vec<u8>),
    Integer(i64),
    Float(f64),
    Null,
}
