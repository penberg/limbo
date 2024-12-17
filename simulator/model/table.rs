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

impl Table {
    pub fn to_create_str(&self) -> String {
        let mut out = String::new();

        out.push_str(format!("CREATE TABLE {} (", self.name).as_str());

        assert!(!self.columns.is_empty());
        for column in &self.columns {
            out.push_str(format!("{} {},", column.name, column.column_type.as_str()).as_str());
        }
        // remove last comma
        out.pop();

        out.push_str(");");
        out
    }
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

impl ColumnType {
    pub fn as_str(&self) -> &str {
        match self {
            ColumnType::Integer => "INTEGER",
            ColumnType::Float => "FLOAT",
            ColumnType::Text => "TEXT",
            ColumnType::Blob => "BLOB",
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
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Text(t) => write!(f, "'{}'", t),
            Value::Blob(b) => write!(f, "{}", to_sqlite_blob(b)),
        }
    }
}
