use std::fmt::Display;
use std::rc::Rc;

use crate::error::LimboError;
use crate::Result;

use crate::storage::sqlite3_ondisk::write_varint;

#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    Null,
    Integer(i64),
    Float(f64),
    Text(&'a String),
    Blob(&'a Vec<u8>),
}

impl Display for Value<'_> {
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
pub enum TextSubtype {
    Text,
    Json,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LimboText {
    pub value: Rc<String>,
    pub subtype: TextSubtype,
}

impl LimboText {
    pub fn new(value: Rc<String>) -> Self {
        Self {
            value,
            subtype: TextSubtype::Text,
        }
    }

    pub fn json(value: Rc<String>) -> Self {
        Self {
            value,
            subtype: TextSubtype::Json,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OwnedValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(LimboText),
    Blob(Rc<Vec<u8>>),
    Agg(Box<AggContext>), // TODO(pere): make this without Box. Currently this might cause cache miss but let's leave it for future analysis
    Record(OwnedRecord),
}

impl OwnedValue {
    // A helper function that makes building a text OwnedValue easier.
    pub fn build_text(text: Rc<String>) -> Self {
        Self::Text(LimboText::new(text))
    }
}

impl Display for OwnedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Integer(i) => write!(f, "{}", i),
            Self::Float(fl) => write!(f, "{:?}", fl),
            Self::Text(s) => write!(f, "{}", s.value),
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

            (Self::Text(text_left), Self::Text(text_right)) => {
                text_left.value.partial_cmp(&text_right.value)
            }
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
    fn partial_cmp(&self, other: &AggContext) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Avg(a, _), Self::Avg(b, _)) => a.partial_cmp(b),
            (Self::Sum(a), Self::Sum(b)) => a.partial_cmp(b),
            (Self::Count(a), Self::Count(b)) => a.partial_cmp(b),
            (Self::Max(a), Self::Max(b)) => a.partial_cmp(b),
            (Self::Min(a), Self::Min(b)) => a.partial_cmp(b),
            (Self::GroupConcat(a), Self::GroupConcat(b)) => a.partial_cmp(b),
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

impl std::ops::Add<OwnedValue> for OwnedValue {
    type Output = OwnedValue;

    fn add(self, rhs: Self) -> Self::Output {
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
            (Self::Text(string_left), Self::Text(string_right)) => Self::build_text(Rc::new(
                string_left.value.to_string() + &string_right.value.to_string(),
            )),
            (Self::Text(string_left), Self::Integer(int_right)) => Self::build_text(Rc::new(
                string_left.value.to_string() + &int_right.to_string(),
            )),
            (Self::Integer(int_left), Self::Text(string_right)) => Self::build_text(Rc::new(
                int_left.to_string() + &string_right.value.to_string(),
            )),
            (Self::Text(string_left), Self::Float(float_right)) => {
                let string_right = Self::Float(float_right).to_string();
                Self::build_text(Rc::new(string_left.value.to_string() + &string_right))
            }
            (Self::Float(float_left), Self::Text(string_right)) => {
                let string_left = Self::Float(float_left).to_string();
                Self::build_text(Rc::new(string_left + &string_right.value.to_string()))
            }
            (lhs, Self::Null) => lhs,
            (Self::Null, rhs) => rhs,
            _ => Self::Float(0.0),
        }
    }
}

impl std::ops::Add<f64> for OwnedValue {
    type Output = OwnedValue;

    fn add(self, rhs: f64) -> Self::Output {
        match self {
            Self::Integer(int_left) => Self::Float(int_left as f64 + rhs),
            Self::Float(float_left) => Self::Float(float_left + rhs),
            _ => unreachable!(),
        }
    }
}

impl std::ops::Add<i64> for OwnedValue {
    type Output = OwnedValue;

    fn add(self, rhs: i64) -> Self::Output {
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

impl std::ops::Div<OwnedValue> for OwnedValue {
    type Output = OwnedValue;

    fn div(self, rhs: OwnedValue) -> Self::Output {
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
    fn div_assign(&mut self, rhs: OwnedValue) {
        *self = self.clone() / rhs;
    }
}

pub fn to_value(value: &OwnedValue) -> Value<'_> {
    match value {
        OwnedValue::Null => Value::Null,
        OwnedValue::Integer(i) => Value::Integer(*i),
        OwnedValue::Float(f) => Value::Float(*f),
        OwnedValue::Text(s) => Value::Text(&s.value),
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

const I8_LOW: i64 = -128;
const I8_HIGH: i64 = 127;
const I16_LOW: i64 = -32768;
const I16_HIGH: i64 = 32767;
const I24_LOW: i64 = -8388608;
const I24_HIGH: i64 = 8388607;
const I32_LOW: i64 = -2147483648;
const I32_HIGH: i64 = 2147483647;
const I48_LOW: i64 = -140737488355328;
const I48_HIGH: i64 = 140737488355327;

/// Sqlite Serial Types
/// https://www.sqlite.org/fileformat.html#record_format
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SerialType {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    F64,
    Text { content_size: usize },
    Blob { content_size: usize },
}

impl From<&OwnedValue> for SerialType {
    fn from(value: &OwnedValue) -> Self {
        match value {
            OwnedValue::Null => SerialType::Null,
            OwnedValue::Integer(i) => match i {
                i if *i >= I8_LOW && *i <= I8_HIGH => SerialType::I8,
                i if *i >= I16_LOW && *i <= I16_HIGH => SerialType::I16,
                i if *i >= I24_LOW && *i <= I24_HIGH => SerialType::I24,
                i if *i >= I32_LOW && *i <= I32_HIGH => SerialType::I32,
                i if *i >= I48_LOW && *i <= I48_HIGH => SerialType::I48,
                _ => SerialType::I64,
            },
            OwnedValue::Float(_) => SerialType::F64,
            OwnedValue::Text(t) => SerialType::Text {
                content_size: t.value.len(),
            },
            OwnedValue::Blob(b) => SerialType::Blob {
                content_size: b.len(),
            },
            OwnedValue::Agg(_) => unreachable!(),
            OwnedValue::Record(_) => unreachable!(),
        }
    }
}

impl From<SerialType> for u64 {
    fn from(serial_type: SerialType) -> Self {
        match serial_type {
            SerialType::Null => 0,
            SerialType::I8 => 1,
            SerialType::I16 => 2,
            SerialType::I24 => 3,
            SerialType::I32 => 4,
            SerialType::I48 => 5,
            SerialType::I64 => 6,
            SerialType::F64 => 7,
            SerialType::Text { content_size } => (content_size * 2 + 13) as u64,
            SerialType::Blob { content_size } => (content_size * 2 + 12) as u64,
        }
    }
}

impl OwnedRecord {
    pub fn new(values: Vec<OwnedValue>) -> Self {
        Self { values }
    }

    pub fn serialize(&self, buf: &mut Vec<u8>) {
        let initial_i = buf.len();

        // write serial types
        for value in &self.values {
            let serial_type = SerialType::from(value);
            buf.resize(buf.len() + 9, 0); // Ensure space for varint (1-9 bytes in length)
            let len = buf.len();
            let n = write_varint(&mut buf[len - 9..], serial_type.into());
            buf.truncate(buf.len() - 9 + n); // Remove unused bytes
        }

        let mut header_size = buf.len() - initial_i;
        // write content
        for value in &self.values {
            match value {
                OwnedValue::Null => {}
                OwnedValue::Integer(i) => {
                    let serial_type = SerialType::from(value);
                    match serial_type {
                        SerialType::I8 => buf.extend_from_slice(&(*i as i8).to_be_bytes()),
                        SerialType::I16 => buf.extend_from_slice(&(*i as i16).to_be_bytes()),
                        SerialType::I24 => buf.extend_from_slice(&(*i as i32).to_be_bytes()[1..]), // remove most significant byte
                        SerialType::I32 => buf.extend_from_slice(&(*i as i32).to_be_bytes()),
                        SerialType::I48 => buf.extend_from_slice(&i.to_be_bytes()[2..]), // remove 2 most significant bytes
                        SerialType::I64 => buf.extend_from_slice(&i.to_be_bytes()),
                        _ => unreachable!(),
                    }
                }
                OwnedValue::Float(f) => buf.extend_from_slice(&f.to_be_bytes()),
                OwnedValue::Text(t) => buf.extend_from_slice(t.value.as_bytes()),
                OwnedValue::Blob(b) => buf.extend_from_slice(b),
                // non serializable
                OwnedValue::Agg(_) => unreachable!(),
                OwnedValue::Record(_) => unreachable!(),
            };
        }

        let mut header_bytes_buf: Vec<u8> = Vec::new();
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::rc::Rc;

    #[test]
    fn test_serialize_null() {
        let record = OwnedRecord::new(vec![OwnedValue::Null]);
        let mut buf = Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8);
        // Second byte should be serial type for NULL
        assert_eq!(header[1] as u64, u64::from(SerialType::Null));
        // Check that the buffer is empty after the header
        assert_eq!(buf.len(), header_length);
    }

    #[test]
    fn test_serialize_integers() {
        let record = OwnedRecord::new(vec![
            OwnedValue::Integer(42),                // Should use SERIAL_TYPE_I8
            OwnedValue::Integer(1000),              // Should use SERIAL_TYPE_I16
            OwnedValue::Integer(1_000_000),         // Should use SERIAL_TYPE_I24
            OwnedValue::Integer(1_000_000_000),     // Should use SERIAL_TYPE_I32
            OwnedValue::Integer(1_000_000_000_000), // Should use SERIAL_TYPE_I48
            OwnedValue::Integer(i64::MAX),          // Should use SERIAL_TYPE_I64
        ]);
        let mut buf = Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert!(header[0] == header_length as u8); // Header should be larger than number of values

        // Check that correct serial types were chosen
        assert_eq!(header[1] as u64, u64::from(SerialType::I8));
        assert_eq!(header[2] as u64, u64::from(SerialType::I16));
        assert_eq!(header[3] as u64, u64::from(SerialType::I24));
        assert_eq!(header[4] as u64, u64::from(SerialType::I32));
        assert_eq!(header[5] as u64, u64::from(SerialType::I48));
        assert_eq!(header[6] as u64, u64::from(SerialType::I64));

        // test that the bytes after the header can be interpreted as the correct values
        let mut cur_offset = header_length;
        let i8_bytes = &buf[cur_offset..cur_offset + size_of::<i8>()];
        cur_offset += size_of::<i8>();
        let i16_bytes = &buf[cur_offset..cur_offset + size_of::<i16>()];
        cur_offset += size_of::<i16>();
        let i24_bytes = &buf[cur_offset..cur_offset + size_of::<i32>() - 1];
        cur_offset += size_of::<i32>() - 1; // i24
        let i32_bytes = &buf[cur_offset..cur_offset + size_of::<i32>()];
        cur_offset += size_of::<i32>();
        let i48_bytes = &buf[cur_offset..cur_offset + size_of::<i64>() - 2];
        cur_offset += size_of::<i64>() - 2; // i48
        let i64_bytes = &buf[cur_offset..cur_offset + size_of::<i64>()];

        let val_int8 = i8::from_be_bytes(i8_bytes.try_into().unwrap());
        let val_int16 = i16::from_be_bytes(i16_bytes.try_into().unwrap());

        let mut leading_0 = vec![0];
        leading_0.extend(i24_bytes);
        let val_int24 = i32::from_be_bytes(leading_0.try_into().unwrap());

        let val_int32 = i32::from_be_bytes(i32_bytes.try_into().unwrap());

        let mut leading_00 = vec![0, 0];
        leading_00.extend(i48_bytes);
        let val_int48 = i64::from_be_bytes(leading_00.try_into().unwrap());

        let val_int64 = i64::from_be_bytes(i64_bytes.try_into().unwrap());

        assert_eq!(val_int8, 42);
        assert_eq!(val_int16, 1000);
        assert_eq!(val_int24, 1_000_000);
        assert_eq!(val_int32, 1_000_000_000);
        assert_eq!(val_int48, 1_000_000_000_000);
        assert_eq!(val_int64, i64::MAX);

        // assert correct size of buffer: header + values (bytes per value depends on serial type)
        assert_eq!(
            buf.len(),
            header_length
                + size_of::<i8>()
                + size_of::<i16>()
                + (size_of::<i32>() - 1) // i24
                + size_of::<i32>()
                + (size_of::<i64>() - 2) // i48
                + size_of::<f64>()
        );
    }

    #[test]
    fn test_serialize_float() {
        #[warn(clippy::approx_constant)]
        let record = OwnedRecord::new(vec![OwnedValue::Float(3.15555)]);
        let mut buf = Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8);
        // Second byte should be serial type for FLOAT
        assert_eq!(header[1] as u64, u64::from(SerialType::F64));
        // Check that the bytes after the header can be interpreted as the float
        let float_bytes = &buf[header_length..header_length + size_of::<f64>()];
        let float = f64::from_be_bytes(float_bytes.try_into().unwrap());
        assert_eq!(float, 3.15555);
        // Check that buffer length is correct
        assert_eq!(buf.len(), header_length + size_of::<f64>());
    }

    #[test]
    fn test_serialize_text() {
        let text = Rc::new("hello".to_string());
        let record = OwnedRecord::new(vec![OwnedValue::Text(LimboText::new(text.clone()))]);
        let mut buf = Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8);
        // Second byte should be serial type for TEXT, which is (len * 2 + 13)
        assert_eq!(header[1], (5 * 2 + 13) as u8);
        // Check the actual text bytes
        assert_eq!(&buf[2..7], b"hello");
        // Check that buffer length is correct
        assert_eq!(buf.len(), header_length + text.len());
    }

    #[test]
    fn test_serialize_blob() {
        let blob = Rc::new(vec![1, 2, 3, 4, 5]);
        let record = OwnedRecord::new(vec![OwnedValue::Blob(blob.clone())]);
        let mut buf = Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8);
        // Second byte should be serial type for BLOB, which is (len * 2 + 12)
        assert_eq!(header[1], (5 * 2 + 12) as u8);
        // Check the actual blob bytes
        assert_eq!(&buf[2..7], &[1, 2, 3, 4, 5]);
        // Check that buffer length is correct
        assert_eq!(buf.len(), header_length + blob.len());
    }

    #[test]
    fn test_serialize_mixed_types() {
        let text = Rc::new("test".to_string());
        let record = OwnedRecord::new(vec![
            OwnedValue::Null,
            OwnedValue::Integer(42),
            OwnedValue::Float(3.15),
            OwnedValue::Text(LimboText::new(text.clone())),
        ]);
        let mut buf = Vec::new();
        record.serialize(&mut buf);

        let header_length = record.values.len() + 1;
        let header = &buf[0..header_length];
        // First byte should be header size
        assert_eq!(header[0], header_length as u8);
        // Second byte should be serial type for NULL
        assert_eq!(header[1] as u64, u64::from(SerialType::Null));
        // Third byte should be serial type for I8
        assert_eq!(header[2] as u64, u64::from(SerialType::I8));
        // Fourth byte should be serial type for F64
        assert_eq!(header[3] as u64, u64::from(SerialType::F64));
        // Fifth byte should be serial type for TEXT, which is (len * 2 + 13)
        assert_eq!(header[4] as u64, (4 * 2 + 13) as u64);

        // Check that the bytes after the header can be interpreted as the correct values
        let mut cur_offset = header_length;
        let i8_bytes = &buf[cur_offset..cur_offset + size_of::<i8>()];
        cur_offset += size_of::<i8>();
        let f64_bytes = &buf[cur_offset..cur_offset + size_of::<f64>()];
        cur_offset += size_of::<f64>();
        let text_bytes = &buf[cur_offset..cur_offset + text.len()];

        let val_int8 = i8::from_be_bytes(i8_bytes.try_into().unwrap());
        let val_float = f64::from_be_bytes(f64_bytes.try_into().unwrap());
        let val_text = String::from_utf8(text_bytes.to_vec()).unwrap();

        assert_eq!(val_int8, 42);
        assert_eq!(val_float, 3.15);
        assert_eq!(val_text, "test");

        // Check that buffer length is correct
        assert_eq!(
            buf.len(),
            header_length + size_of::<i8>() + size_of::<f64>() + text.len()
        );
    }
}
