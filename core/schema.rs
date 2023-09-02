use anyhow::Result;
use core::fmt;
use fallible_iterator::FallibleIterator;
use sqlite3_parser::{
    ast::{Cmd, CreateTableBody, Stmt},
    lexer::sql::Parser,
};
use std::collections::HashMap;

pub struct Schema {
    pub tables: HashMap<String, Table>,
}

impl Schema {
    pub fn new() -> Self {
        let mut tables: HashMap<String, Table> = HashMap::new();
        tables.insert("sqlite_schema".to_string(), sqlite_schema_table());
        Self { tables }
    }

    pub fn add_table(&mut self, name: String, table: Table) {
        self.tables.insert(name, table);
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }
}

pub struct Table {
    pub root_page: usize,
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn from_sql(sql: &str, root_page: usize) -> Result<Table> {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        match cmd {
            Some(cmd) => match cmd {
                Cmd::Stmt(stmt) => match stmt {
                    Stmt::CreateTable { tbl_name, body, .. } => {
                        let mut cols = vec![];
                        match body {
                            CreateTableBody::ColumnsAndConstraints { columns, .. } => {
                                for column in columns {
                                    let name = column.col_name.0.to_string();
                                    let ty = match column.col_type {
                                        Some(data_type) => match data_type.name.as_str() {
                                            "INT" => Type::Integer,
                                            "REAL" => Type::Real,
                                            "TEXT" => Type::Text,
                                            "BLOB" => Type::Blob,
                                            _ => unreachable!("Unknown type: {:?}", data_type.name),
                                        },
                                        None => Type::Null,
                                    };
                                    cols.push(Column { name, ty });
                                }
                            }
                            CreateTableBody::AsSelect(_) => todo!(),
                        };
                        Ok(Table {
                            root_page,
                            name: tbl_name.name.to_string(),
                            columns: cols,
                        })
                    }
                    _ => {
                        anyhow::bail!("Expected CREATE TABLE statement");
                    }
                },
                _ => {
                    anyhow::bail!("Expected CREATE TABLE statement");
                }
            },
            None => {
                anyhow::bail!("Expected CREATE TABLE statement");
            }
        }
    }

    pub fn to_sql(&self) -> String {
        let mut sql = format!("CREATE TABLE {} (\n", self.name);
        for (i, column) in self.columns.iter().enumerate() {
            if i > 0 {
                sql.push_str(",\n");
            }
            sql.push_str("  ");
            sql.push_str(&column.name);
            sql.push_str(" ");
            sql.push_str(&column.ty.to_string());
        }
        sql.push_str(");\n");
        sql
    }
}

pub struct Column {
    pub name: String,
    pub ty: Type,
}

pub enum Type {
    Null,
    Integer,
    Real,
    Text,
    Blob,
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Type::Null => "NULL",
            Type::Integer => "INTEGER",
            Type::Real => "REAL",
            Type::Text => "TEXT",
            Type::Blob => "BLOB",
        };
        write!(f, "{}", s)
    }
}

pub fn sqlite_schema_table() -> Table {
    Table {
        root_page: 1,
        name: "sqlite_schema".to_string(),
        columns: vec![
            Column {
                name: "type".to_string(),
                ty: Type::Text,
            },
            Column {
                name: "name".to_string(),
                ty: Type::Text,
            },
            Column {
                name: "tbl_name".to_string(),
                ty: Type::Text,
            },
            Column {
                name: "rootpage".to_string(),
                ty: Type::Integer,
            },
            Column {
                name: "sql".to_string(),
                ty: Type::Text,
            },
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_sqlite_schema() {
        let expected = r#"CREATE TABLE sqlite_schema (
  type TEXT,
  name TEXT,
  tbl_name TEXT,
  rootpage INTEGER,
  sql TEXT);
"#;
        let actual = sqlite_schema_table().to_sql();
        assert_eq!(expected, actual);
    }
}
