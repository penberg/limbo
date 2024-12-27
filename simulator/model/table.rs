use std::{fmt::Display, ops::Deref};

pub(crate) struct Name(pub(crate) String);

impl Deref for Name {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Table {
    pub(crate) rows: Vec<Vec<Value>>,
    pub(crate) name: String,
    pub(crate) columns: Vec<Column>,
}

#[derive(Debug, Clone)]
pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) column_type: ColumnType,
    pub(crate) primary: bool,
    pub(crate) unique: bool,
}

#[derive(Debug, Clone)]
pub(crate) enum ColumnType {
    Integer,
    Float,
    Text,
    Blob,
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer => write!(f, "INTEGER"),
            Self::Float => write!(f, "REAL"),
            Self::Text => write!(f, "TEXT"),
            Self::Blob => write!(f, "BLOB"),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

fn to_sqlite_blob(bytes: &[u8]) -> String {
    let hex: String = bytes.iter().map(|b| format!("{:02X}", b)).collect();
    format!("X'{}'", hex)
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Integer(i) => write!(f, "{}", i),
            Self::Float(fl) => write!(f, "{}", fl),
            Self::Text(t) => write!(f, "'{}'", t),
            Self::Blob(b) => write!(f, "{}", to_sqlite_blob(b)),
        }
    }
}
