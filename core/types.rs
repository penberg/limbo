use std::rc::Rc;

use anyhow::Result;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Text(Rc<String>),
    Blob(Vec<u8>),
}

pub trait FromValue {
    fn from_value(value: &Value) -> Result<Self>
    where
        Self: Sized;
}

impl FromValue for i64 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Integer(i) => Ok(*i),
            _ => anyhow::bail!("Expected integer value"),
        }
    }
}

impl FromValue for String {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Text(s) => Ok(s.to_string()),
            _ => anyhow::bail!("Expected text value"),
        }
    }
}

#[derive(Debug)]
pub struct Record {
    pub values: Vec<Value>,
}

impl Record {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }
}
