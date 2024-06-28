use std::{borrow::Borrow, cell::Ref, ops::Add, rc::Rc};

use anyhow::Result;

#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    Null,
    Integer(i64),
    Float(f64),
    Text(&'a String),
    Blob(&'a Vec<u8>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggContext {
    Avg(f64, usize), // acc and count
}

#[derive(Debug, Clone, PartialEq)]
pub enum OwnedValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(Rc<String>),
    Blob(Rc<Vec<u8>>),
    Agg(Box<AggContext>),
}

impl std::ops::AddAssign for OwnedValue {
    fn add_assign(&mut self, rhs: Self) {
        let l = self.clone();
        *self = l.add(rhs);
    }
}

impl std::ops::Add for OwnedValue {
    type Output = OwnedValue;

    fn add(self, rhs: Self) -> Self::Output {
        assert!(matches!(&self, rhs));
        assert!(matches!(&self, OwnedValue::Integer(_)) || matches!(&self, OwnedValue::Float(_)));
        match &self {
            OwnedValue::Integer(l) => {
                if let OwnedValue::Integer(r) = rhs {
                    OwnedValue::Integer(l + r)
                } else {
                    panic!();
                }
            }
            OwnedValue::Float(l) => {
                if let OwnedValue::Float(r) = rhs {
                    OwnedValue::Float(l + r)
                } else {
                    panic!();
                }
            }
            _ => todo!(),
        }
    }
}

pub fn to_value(value: &OwnedValue) -> Value<'_> {
    match value {
        OwnedValue::Null => Value::Null,
        OwnedValue::Integer(i) => Value::Integer(*i),
        OwnedValue::Float(f) => Value::Float(*f),
        OwnedValue::Text(s) => Value::Text(s),
        OwnedValue::Blob(b) => Value::Blob(b),
        OwnedValue::Agg(a) => match a.as_ref() {
            AggContext::Avg(acc, count) => Value::Float(*acc), // we assume aggfinal was called
            _ => todo!(),
        },
    }
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
pub struct Record<'a> {
    pub values: Vec<Value<'a>>,
}

impl<'a> Record<'a> {
    pub fn new(values: Vec<Value<'a>>) -> Self {
        Self { values }
    }
}

pub struct OwnedRecord {
    pub values: Vec<OwnedValue>,
}

impl OwnedRecord {
    pub fn new(values: Vec<OwnedValue>) -> Self {
        Self { values }
    }
}

pub enum CursorResult<T> {
    Ok(T),
    IO,
}

pub trait Cursor {
    fn is_empty(&self) -> bool;
    fn rewind(&mut self) -> Result<CursorResult<()>>;
    fn next(&mut self) -> Result<CursorResult<()>>;
    fn wait_for_completion(&mut self) -> Result<()>;
    fn rowid(&self) -> Result<Ref<Option<u64>>>;
    fn record(&self) -> Result<Ref<Option<OwnedRecord>>>;
}
