use std::{cell::Ref, fmt::Display, rc::Rc};

use crate::{error::LimboError, storage::sqlite3_ondisk::write_varint, Result};

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
            Self::Null => write!(f, "NULL"),
            Self::Integer(i) => write!(f, "{}", i),
            Self::Float(fl) => write!(f, "{}", fl),
            Self::Text(s) => write!(f, "{}", s),
            Self::Blob(b) => write!(f, "{:?}", b),
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
            Self::Null => write!(f, "NULL"),
            Self::Integer(i) => write!(f, "{}", i),
            Self::Float(fl) => write!(f, "{:?}", fl),
            Self::Text(s) => write!(f, "{}", s),
            Self::Blob(b) => write!(f, "{}", String::from_utf8_lossy(b)),
            Self::Agg(a) => match a.as_ref() {
                AggContext::Avg(acc, _count) => write!(f, "{}", acc),
                AggContext::Sum(acc) => write!(f, "{}", acc),
                AggContext::Count(count) => write!(f, "{}", count),
                AggContext::Max(max) => write!(f, "{}", max.as_ref().unwrap_or(&Self::Null)),
                AggContext::Min(min) => write!(f, "{}", min.as_ref().unwrap_or(&Self::Null)),
                AggContext::GroupConcat(s) => write!(f, "{}", s),
            },
            Self::Record(r) => write!(f, "{:?}", r),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggContext {
    Avg(OwnedValue, OwnedValue), // acc and count
    Sum(OwnedValue),
    Count(OwnedValue),
    Max(Option<OwnedValue>),
    Min(Option<OwnedValue>),
    GroupConcat(OwnedValue),
}

const NULL: OwnedValue = OwnedValue::Null;

impl AggContext {
    pub fn final_value(&self) -> &OwnedValue {
        match self {
            Self::Avg(acc, _count) => acc,
            Self::Sum(acc) => acc,
            Self::Count(count) => count,
            Self::Max(max) => max.as_ref().unwrap_or(&NULL),
            Self::Min(min) => min.as_ref().unwrap_or(&NULL),
            Self::GroupConcat(s) => s,
        }
    }
}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd<OwnedValue> for OwnedValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Integer(int_left), Self::Integer(int_right)) => int_left.partial_cmp(int_right),
            (Self::Integer(int_left), Self::Float(float_right)) => {
                (*int_left as f64).partial_cmp(float_right)
            }
            (Self::Float(float_left), Self::Integer(int_right)) => {
                float_left.partial_cmp(&(*int_right as f64))
            }
            (Self::Float(float_left), Self::Float(float_right)) => {
                float_left.partial_cmp(float_right)
            }
            // Numeric vs Text/Blob
            (Self::Integer(_) | Self::Float(_), Self::Text(_) | Self::Blob(_)) => {
                Some(std::cmp::Ordering::Less)
            }
            (Self::Text(_) | Self::Blob(_), Self::Integer(_) | Self::Float(_)) => {
                Some(std::cmp::Ordering::Greater)
            }

            (Self::Text(text_left), Self::Text(text_right)) => text_left.partial_cmp(text_right),
            // Text vs Blob
            (Self::Text(_), Self::Blob(_)) => Some(std::cmp::Ordering::Less),
            (Self::Blob(_), Self::Text(_)) => Some(std::cmp::Ordering::Greater),

            (Self::Blob(blob_left), Self::Blob(blob_right)) => blob_left.partial_cmp(blob_right),
            (Self::Null, Self::Null) => Some(std::cmp::Ordering::Equal),
            (Self::Null, _) => Some(std::cmp::Ordering::Less),
            (_, Self::Null) => Some(std::cmp::Ordering::Greater),
            (Self::Agg(a), Self::Agg(b)) => a.partial_cmp(b),
            (Self::Agg(a), other) => a.final_value().partial_cmp(other),
            (other, Self::Agg(b)) => other.partial_cmp(b.final_value()),
            other => todo!("{:?}", other),
        }
    }
}

impl std::cmp::PartialOrd<AggContext> for AggContext {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Avg(a, _), Self::Avg(b, _))
            | (Self::Sum(a), Self::Sum(b))
            | (Self::Count(a), Self::Count(b))
            | (Self::GroupConcat(a), Self::GroupConcat(b)) => a.partial_cmp(b),
            (Self::Max(a), Self::Max(b)) | (Self::Min(a), Self::Min(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl std::cmp::Eq for OwnedValue {}

impl std::cmp::Ord for OwnedValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl std::ops::Add for OwnedValue {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::Integer(int_left), Self::Integer(int_right)) => {
                Self::Integer(int_left + int_right)
            }
            (Self::Integer(int_left), Self::Float(float_right)) => {
                Self::Float(int_left as f64 + float_right)
            }
            (Self::Float(float_left), Self::Integer(int_right)) => {
                Self::Float(float_left + int_right as f64)
            }
            (Self::Float(float_left), Self::Float(float_right)) => {
                Self::Float(float_left + float_right)
            }
            (Self::Text(string_left), Self::Text(string_right)) => {
                Self::Text(Rc::new(string_left.to_string() + &string_right.to_string()))
            }
            (Self::Text(string_left), Self::Integer(int_right)) => {
                Self::Text(Rc::new(string_left.to_string() + &int_right.to_string()))
            }
            (Self::Integer(int_left), Self::Text(string_right)) => {
                Self::Text(Rc::new(int_left.to_string() + &string_right.to_string()))
            }
            (Self::Text(string_left), Self::Float(float_right)) => {
                let string_right = Self::Float(float_right).to_string();
                Self::Text(Rc::new(string_left.to_string() + &string_right))
            }
            (Self::Float(float_left), Self::Text(string_right)) => {
                let string_left = Self::Float(float_left).to_string();
                Self::Text(Rc::new(string_left + &string_right.to_string()))
            }
            (lhs, Self::Null) => lhs,
            (Self::Null, rhs) => rhs,
            _ => Self::Float(0.0),
        }
    }
}

impl std::ops::Add<f64> for OwnedValue {
    type Output = Self;

    fn add(self, rhs: f64) -> Self {
        match self {
            Self::Integer(int_left) => Self::Float(int_left as f64 + rhs),
            Self::Float(float_left) => Self::Float(float_left + rhs),
            _ => unreachable!(),
        }
    }
}

impl std::ops::Add<i64> for OwnedValue {
    type Output = Self;

    fn add(self, rhs: i64) -> Self {
        match self {
            Self::Integer(int_left) => Self::Integer(int_left + rhs),
            Self::Float(float_left) => Self::Float(float_left + rhs as f64),
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

impl std::ops::Div for OwnedValue {
    type Output = Self;

    fn div(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::Integer(int_left), Self::Integer(int_right)) => {
                Self::Integer(int_left / int_right)
            }
            (Self::Integer(int_left), Self::Float(float_right)) => {
                Self::Float(int_left as f64 / float_right)
            }
            (Self::Float(float_left), Self::Integer(int_right)) => {
                Self::Float(float_left / int_right as f64)
            }
            (Self::Float(float_left), Self::Float(float_right)) => {
                Self::Float(float_left / float_right)
            }
            _ => Self::Float(0.0),
        }
    }
}

impl std::ops::DivAssign<OwnedValue> for OwnedValue {
    fn div_assign(&mut self, rhs: Self) {
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
            AggContext::Avg(acc, _count) => match acc {
                OwnedValue::Integer(i) => Value::Integer(*i),
                OwnedValue::Float(f) => Value::Float(*f),
                _ => Value::Float(0.0),
            },
            AggContext::Sum(acc) => match acc {
                OwnedValue::Integer(i) => Value::Integer(*i),
                OwnedValue::Float(f) => Value::Float(*f),
                _ => Value::Float(0.0),
            },
            AggContext::Count(count) => to_value(count),
            AggContext::Max(max) => match max {
                Some(max) => to_value(max),
                None => Value::Null,
            },
            AggContext::Min(min) => match min {
                Some(min) => to_value(min),
                None => Value::Null,
            },
            AggContext::GroupConcat(s) => to_value(s),
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
            _ => Err(LimboError::ConversionError("Expected integer value".into())),
        }
    }
}

impl<'a> FromValue<'a> for String {
    fn from_value(value: &Value<'a>) -> Result<Self> {
        match value {
            Value::Text(s) => Ok(s.to_string()),
            _ => Err(LimboError::ConversionError("Expected text value".into())),
        }
    }
}

impl<'a> FromValue<'a> for &'a str {
    fn from_value(value: &Value<'a>) -> Result<&'a str> {
        match value {
            Value::Text(s) => Ok(s),
            _ => Err(LimboError::ConversionError("Expected text value".into())),
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct OwnedRecord {
    pub values: Vec<OwnedValue>,
}

impl OwnedRecord {
    pub fn new(values: Vec<OwnedValue>) -> Self {
        Self { values }
    }

    pub fn serialize(&self, buf: &mut Vec<u8>) {
        let initial_i = buf.len();

        for value in &self.values {
            let serial_type = match value {
                OwnedValue::Null => 0,
                OwnedValue::Integer(_) => 6, // for now let's only do i64
                OwnedValue::Float(_) => 7,
                OwnedValue::Text(t) => (t.len() * 2 + 13) as u64,
                OwnedValue::Blob(b) => (b.len() * 2 + 12) as u64,
                // not serializable values
                OwnedValue::Agg(_) => unreachable!(),
                OwnedValue::Record(_) => unreachable!(),
            };

            buf.resize(buf.len() + 9, 0); // Ensure space for varint
            let len = buf.len();
            let n = write_varint(&mut buf[len - 9..], serial_type);
            buf.truncate(buf.len() - 9 + n); // Remove unused bytes
        }

        let mut header_size = buf.len() - initial_i;
        // write content
        for value in &self.values {
            // TODO: make integers and floats with smaller serial types
            match value {
                OwnedValue::Null => {}
                OwnedValue::Integer(i) => buf.extend_from_slice(&i.to_be_bytes()),
                OwnedValue::Float(f) => buf.extend_from_slice(&f.to_be_bytes()),
                OwnedValue::Text(t) => buf.extend_from_slice(t.as_bytes()),
                OwnedValue::Blob(b) => buf.extend_from_slice(b),
                // non serializable
                OwnedValue::Agg(_) => unreachable!(),
                OwnedValue::Record(_) => unreachable!(),
            };
        }

        let mut header_bytes_buf: Vec<u8> = vec![];
        if header_size <= 126 {
            // common case
            header_size += 1;
        } else {
            todo!("calculate big header size extra bytes");
            // get header varint len
            // header_size += n;
            // if( nVarint<sqlite3VarintLen(nHdr) ) nHdr++;
        }
        assert!(header_size <= 126);
        header_bytes_buf.extend(std::iter::repeat(0).take(9));
        let n = write_varint(header_bytes_buf.as_mut_slice(), header_size as u64);
        header_bytes_buf.truncate(n);
        buf.splice(initial_i..initial_i, header_bytes_buf.iter().cloned());
    }
}

#[derive(PartialEq, Debug)]
pub enum CursorResult<T> {
    Ok(T),
    IO,
}

#[derive(Clone, PartialEq, Debug)]
pub enum SeekOp {
    EQ,
    GE,
    GT,
}

#[derive(Clone, PartialEq, Debug)]
pub enum SeekKey<'a> {
    TableRowId(u64),
    IndexKey(&'a OwnedRecord),
}

pub trait Cursor {
    fn is_empty(&self) -> bool;
    fn rewind(&mut self) -> Result<CursorResult<()>>;
    fn last(&mut self) -> Result<CursorResult<()>>;
    fn next(&mut self) -> Result<CursorResult<()>>;
    fn prev(&mut self) -> Result<CursorResult<()>>;
    fn wait_for_completion(&mut self) -> Result<()>;
    fn rowid(&self) -> Result<Option<u64>>;
    fn seek(&mut self, key: SeekKey, op: SeekOp) -> Result<CursorResult<bool>>;
    fn seek_to_last(&mut self) -> Result<CursorResult<()>>;
    fn record(&self) -> Result<Ref<Option<OwnedRecord>>>;
    fn insert(
        &mut self,
        key: &OwnedValue,
        record: &OwnedRecord,
        moved_before: bool, /* Tells inserter that it doesn't need to traverse in order to find leaf page */
    ) -> Result<CursorResult<()>>; //
    fn exists(&mut self, key: &OwnedValue) -> Result<CursorResult<bool>>;
    fn set_null_flag(&mut self, flag: bool);
    fn get_null_flag(&self) -> bool;
    fn btree_create(&mut self, flags: usize) -> u32;
}
