use std::fmt::Display;
use std::{cell::Ref, rc::Rc};

use anyhow::Result;

#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    Null,
    Integer(i64),
    Float(f64),
    Text(&'a String),
    Blob(&'a Vec<u8>),
}

impl<'a> Display for Value<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Text(s) => write!(f, "{}", s),
            Value::Blob(b) => write!(f, "{:?}", b),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OwnedValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(Rc<String>),
    Blob(Rc<Vec<u8>>),
    Agg(Box<AggContext>), // TODO(pere): make this without Box. Currently this might cause cache miss but let's leave it for future analysis
    Record(OwnedRecord),
}

impl Display for OwnedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OwnedValue::Null => write!(f, "NULL"),
            OwnedValue::Integer(i) => write!(f, "{}", i),
            OwnedValue::Float(fl) => write!(f, "{}", fl),
            OwnedValue::Text(s) => write!(f, "{}", s),
            OwnedValue::Blob(b) => write!(f, "{:?}", b),
            OwnedValue::Agg(a) => match a.as_ref() {
                AggContext::Avg(acc, _count) => write!(f, "{}", acc),
                AggContext::Sum(acc) => write!(f, "{}", acc),
                AggContext::Count(count) => write!(f, "{}", count),
            },
            OwnedValue::Record(r) => write!(f, "{:?}", r),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggContext {
    Avg(OwnedValue, OwnedValue), // acc and count
    Sum(OwnedValue),
    Count(OwnedValue),
}

impl std::ops::Add<OwnedValue> for OwnedValue {
    type Output = OwnedValue;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (OwnedValue::Integer(int_left), OwnedValue::Integer(int_right)) => {
                OwnedValue::Integer(int_left + int_right)
            }
            (OwnedValue::Integer(int_left), OwnedValue::Float(float_right)) => {
                OwnedValue::Float(int_left as f64 + float_right)
            }
            (OwnedValue::Float(float_left), OwnedValue::Integer(int_right)) => {
                OwnedValue::Float(float_left + int_right as f64)
            }
            (OwnedValue::Float(float_left), OwnedValue::Float(float_right)) => {
                OwnedValue::Float(float_left + float_right)
            }
            _ => unreachable!(),
        }
    }
}

impl std::ops::Add<f64> for OwnedValue {
    type Output = OwnedValue;

    fn add(self, rhs: f64) -> Self::Output {
        match self {
            OwnedValue::Integer(int_left) => OwnedValue::Float(int_left as f64 + rhs),
            OwnedValue::Float(float_left) => OwnedValue::Float(float_left + rhs),
            _ => unreachable!(),
        }
    }
}

impl std::ops::Add<i64> for OwnedValue {
    type Output = OwnedValue;

    fn add(self, rhs: i64) -> Self::Output {
        match self {
            OwnedValue::Integer(int_left) => OwnedValue::Integer(int_left + rhs),
            OwnedValue::Float(float_left) => OwnedValue::Float(float_left + rhs as f64),
            _ => unreachable!(),
        }
    }
}

impl std::ops::AddAssign for OwnedValue {
    fn add_assign(&mut self, rhs: Self) {
        *self = self.clone() + rhs;
    }
}

impl std::ops::AddAssign<i64> for OwnedValue {
    fn add_assign(&mut self, rhs: i64) {
        *self = self.clone() + rhs;
    }
}

impl std::ops::AddAssign<f64> for OwnedValue {
    fn add_assign(&mut self, rhs: f64) {
        *self = self.clone() + rhs;
    }
}

impl std::ops::Div<OwnedValue> for OwnedValue {
    type Output = OwnedValue;

    fn div(self, rhs: OwnedValue) -> Self::Output {
        match (self, rhs) {
            (OwnedValue::Integer(int_left), OwnedValue::Integer(int_right)) => {
                OwnedValue::Integer(int_left / int_right)
            }
            (OwnedValue::Integer(int_left), OwnedValue::Float(float_right)) => {
                OwnedValue::Float(int_left as f64 / float_right)
            }
            (OwnedValue::Float(float_left), OwnedValue::Integer(int_right)) => {
                OwnedValue::Float(float_left / int_right as f64)
            }
            (OwnedValue::Float(float_left), OwnedValue::Float(float_right)) => {
                OwnedValue::Float(float_left / float_right)
            }
            _ => unreachable!(),
        }
    }
}

impl std::ops::DivAssign<OwnedValue> for OwnedValue {
    fn div_assign(&mut self, rhs: OwnedValue) {
        *self = self.clone() / rhs;
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
            AggContext::Avg(acc, _count) => to_value(acc), // we assume aggfinal was called
            AggContext::Sum(acc) => to_value(acc),
            AggContext::Count(count) => to_value(count),
        },
        OwnedValue::Record(_) => todo!(),
    }
}

pub trait FromValue<'a> {
    fn from_value(value: &Value<'a>) -> Result<Self>
    where
        Self: Sized + 'a;
}

impl<'a> FromValue<'a> for i64 {
    fn from_value(value: &Value<'a>) -> Result<Self> {
        match value {
            Value::Integer(i) => Ok(*i),
            _ => anyhow::bail!("Expected integer value"),
        }
    }
}

impl<'a> FromValue<'a> for String {
    fn from_value(value: &Value<'a>) -> Result<Self> {
        match value {
            Value::Text(s) => Ok(s.to_string()),
            _ => anyhow::bail!("Expected text value"),
        }
    }
}

impl<'a> FromValue<'a> for &'a str {
    fn from_value(value: &Value<'a>) -> Result<&'a str> {
        match value {
            Value::Text(s) => Ok(&s),
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

#[derive(Debug, Clone, PartialEq)]
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
    fn insert(&mut self, record: &OwnedRecord) -> Result<()>;
}
