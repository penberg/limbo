use crate::{util::normalize_ident, Result};
use core::fmt;
use fallible_iterator::FallibleIterator;
use log::trace;
use sqlite3_parser::ast::{Expr, Literal, TableOptions};
use sqlite3_parser::{
    ast::{Cmd, CreateTableBody, QualifiedName, ResultColumn, Stmt},
    lexer::sql::Parser,
};
use std::collections::HashMap;
use std::rc::Rc;

pub struct Schema {
    pub tables: HashMap<String, Rc<BTreeTable>>,
    // table_name to list of indexes for the table
    pub indexes: HashMap<String, Vec<Rc<Index>>>,
}

impl Schema {
    pub fn new() -> Self {
        let mut tables: HashMap<String, Rc<BTreeTable>> = HashMap::new();
        let indexes: HashMap<String, Vec<Rc<Index>>> = HashMap::new();
        tables.insert("sqlite_schema".to_string(), Rc::new(sqlite_schema_table()));
        Self { tables, indexes }
    }

    pub fn add_table(&mut self, table: Rc<BTreeTable>) {
        let name = normalize_ident(&table.name);
        self.tables.insert(name, table);
    }

    pub fn get_table(&self, name: &str) -> Option<Rc<BTreeTable>> {
        let name = normalize_ident(name);
        self.tables.get(&name).cloned()
    }

    pub fn add_index(&mut self, index: Rc<Index>) {
        let table_name = normalize_ident(&index.table_name);
        self.indexes
            .entry(table_name)
            .or_default()
            .push(index.clone())
    }
}

#[derive(Clone, Debug)]
pub enum Table {
    BTree(Rc<BTreeTable>),
    Index(Rc<Index>),
    Pseudo(Rc<PseudoTable>),
}

impl Table {
    pub fn is_pseudo(&self) -> bool {
        matches!(self, Table::Pseudo(_))
    }

    pub fn get_rowid_alias_column(&self) -> Option<(usize, &Column)> {
        match self {
            Table::BTree(table) => table.get_rowid_alias_column(),
            Table::Index(_) => None,
            Table::Pseudo(_) => None,
        }
    }

    pub fn column_is_rowid_alias(&self, col: &Column) -> bool {
        match self {
            Table::BTree(table) => table.column_is_rowid_alias(col),
            Table::Index(_) => false,
            Table::Pseudo(_) => false,
        }
    }

    pub fn get_name(&self) -> &str {
        match self {
            Table::BTree(table) => &table.name,
            Table::Index(index) => &index.name,
            Table::Pseudo(_) => "",
        }
    }

    pub fn column_index_to_name(&self, index: usize) -> Option<&str> {
        match self {
            Table::BTree(table) => match table.columns.get(index) {
                Some(column) => Some(&column.name),
                None => None,
            },
            Table::Index(i) => match i.columns.get(index) {
                Some(column) => Some(&column.name),
                None => None,
            },
            Table::Pseudo(table) => match table.columns.get(index) {
                Some(column) => Some(&column.name),
                None => None,
            },
        }
    }

    pub fn get_column(&self, name: &str) -> Option<(usize, &Column)> {
        match self {
            Table::BTree(table) => table.get_column(name),
            Table::Index(_) => unimplemented!(),
            Table::Pseudo(table) => table.get_column(name),
        }
    }

    pub fn get_column_at(&self, index: usize) -> &Column {
        match self {
            Table::BTree(table) => table.columns.get(index).unwrap(),
            Table::Index(_) => unimplemented!(),
            Table::Pseudo(table) => table.columns.get(index).unwrap(),
        }
    }

    pub fn columns(&self) -> &Vec<Column> {
        match self {
            Table::BTree(table) => &table.columns,
            Table::Index(_) => unimplemented!(),
            Table::Pseudo(table) => &table.columns,
        }
    }

    pub fn has_rowid(&self) -> bool {
        match self {
            Table::BTree(table) => table.has_rowid,
            Table::Index(_) => unimplemented!(),
            Table::Pseudo(_) => unimplemented!(),
        }
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Table::BTree(a), Table::BTree(b)) => Rc::ptr_eq(a, b),
            (Table::Pseudo(a), Table::Pseudo(b)) => Rc::ptr_eq(a, b),
            _ => false,
        }
    }
}

#[derive(Debug)]
pub struct BTreeTable {
    pub root_page: usize,
    pub name: String,
    pub primary_key_column_names: Vec<String>,
    pub columns: Vec<Column>,
    pub has_rowid: bool,
}

impl BTreeTable {
    pub fn get_rowid_alias_column(&self) -> Option<(usize, &Column)> {
        if self.primary_key_column_names.len() == 1 {
            let (idx, col) = self.get_column(&self.primary_key_column_names[0]).unwrap();
            if self.column_is_rowid_alias(col) {
                return Some((idx, col));
            }
        }
        None
    }

    pub fn column_is_rowid_alias(&self, col: &Column) -> bool {
        col.primary_key
            && col.ty == Type::Integer
            && self.primary_key_column_names.len() == 1
            && self.has_rowid
    }

    pub fn get_column(&self, name: &str) -> Option<(usize, &Column)> {
        let name = normalize_ident(name);
        for (i, column) in self.columns.iter().enumerate() {
            if column.name == name {
                return Some((i, column));
            }
        }
        None
    }

    pub fn from_sql(sql: &str, root_page: usize) -> Result<BTreeTable> {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        match cmd {
            Some(Cmd::Stmt(Stmt::CreateTable { tbl_name, body, .. })) => {
                create_table(tbl_name, body, root_page)
            }
            _ => todo!("Expected CREATE TABLE statement"),
        }
    }

    #[cfg(test)]
    pub fn to_sql(&self) -> String {
        let mut sql = format!("CREATE TABLE {} (\n", self.name);
        for (i, column) in self.columns.iter().enumerate() {
            if i > 0 {
                sql.push_str(",\n");
            }
            sql.push_str("  ");
            sql.push_str(&column.name);
            sql.push(' ');
            sql.push_str(&column.ty.to_string());
        }
        sql.push_str(");\n");
        sql
    }
}

#[derive(Debug)]
pub struct PseudoTable {
    pub columns: Vec<Column>,
}

impl PseudoTable {
    pub fn new() -> Self {
        Self { columns: vec![] }
    }

    pub fn add_column(&mut self, name: &str, ty: Type, primary_key: bool) {
        self.columns.push(Column {
            name: normalize_ident(name),
            ty,
            primary_key,
        });
    }
    pub fn get_column(&self, name: &str) -> Option<(usize, &Column)> {
        let name = normalize_ident(name);
        for (i, column) in self.columns.iter().enumerate() {
            if column.name == name {
                return Some((i, column));
            }
        }
        None
    }
}

impl Default for PseudoTable {
    fn default() -> Self {
        Self::new()
    }
}

fn create_table(
    tbl_name: QualifiedName,
    body: CreateTableBody,
    root_page: usize,
) -> Result<BTreeTable> {
    let table_name = normalize_ident(&tbl_name.name.0);
    trace!("Creating table {}", table_name);
    let mut has_rowid = true;
    let mut primary_key_column_names = vec![];
    let mut cols = vec![];
    match body {
        CreateTableBody::ColumnsAndConstraints {
            columns,
            constraints,
            options,
        } => {
            if let Some(constraints) = constraints {
                for c in constraints {
                    if let sqlite3_parser::ast::TableConstraint::PrimaryKey { columns, .. } =
                        c.constraint
                    {
                        for column in columns {
                            primary_key_column_names.push(match column.expr {
                                Expr::Id(id) => normalize_ident(&id.0),
                                Expr::Literal(Literal::String(value)) => {
                                    value.trim_matches('\'').to_owned()
                                }
                                _ => {
                                    todo!("Unsupported primary key expression");
                                }
                            });
                        }
                    }
                }
            }
            for (col_name, col_def) in columns {
                let name = col_name.0.to_string();
                let ty = match col_def.col_type {
                    Some(data_type) => {
                        let type_name = data_type.name.as_str().to_uppercase();
                        if type_name.contains("INTEGER") {
                            Type::Integer
                        } else if type_name.contains("CHAR")
                            || type_name.contains("CLOB")
                            || type_name.contains("TEXT")
                        {
                            Type::Text
                        } else if type_name.contains("BLOB") || type_name.is_empty() {
                            Type::Blob
                        } else if type_name.contains("REAL")
                            || type_name.contains("FLOA")
                            || type_name.contains("DOUB")
                        {
                            Type::Real
                        } else {
                            Type::Numeric
                        }
                    }
                    None => Type::Null,
                };
                let mut primary_key = col_def.constraints.iter().any(|c| {
                    matches!(
                        c.constraint,
                        sqlite3_parser::ast::ColumnConstraint::PrimaryKey { .. }
                    )
                });
                if primary_key {
                    primary_key_column_names.push(name.clone());
                } else if primary_key_column_names.contains(&name) {
                    primary_key = true;
                }
                cols.push(Column {
                    name: normalize_ident(&name),
                    ty,
                    primary_key,
                });
            }
            if options.contains(TableOptions::WITHOUT_ROWID) {
                has_rowid = false;
            }
        }
        CreateTableBody::AsSelect(_) => todo!(),
    };
    Ok(BTreeTable {
        root_page,
        name: table_name,
        has_rowid,
        primary_key_column_names,
        columns: cols,
    })
}

pub fn _build_pseudo_table(columns: &[ResultColumn]) -> PseudoTable {
    let table = PseudoTable::new();
    for column in columns {
        match column {
            ResultColumn::Expr(expr, _as_name) => {
                todo!("unsupported expression {:?}", expr);
            }
            ResultColumn::Star => {
                todo!();
            }
            ResultColumn::TableStar(_) => {
                todo!();
            }
        }
    }
    table
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub ty: Type,
    pub primary_key: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Type {
    Null,
    Text,
    Numeric,
    Integer,
    Real,
    Blob,
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Type::Null => "NULL",
            Type::Text => "TEXT",
            Type::Numeric => "NUMERIC",
            Type::Integer => "INTEGER",
            Type::Real => "REAL",
            Type::Blob => "BLOB",
        };
        write!(f, "{}", s)
    }
}

pub fn sqlite_schema_table() -> BTreeTable {
    BTreeTable {
        root_page: 1,
        name: "sqlite_schema".to_string(),
        has_rowid: true,
        primary_key_column_names: vec![],
        columns: vec![
            Column {
                name: "type".to_string(),
                ty: Type::Text,
                primary_key: false,
            },
            Column {
                name: "name".to_string(),
                ty: Type::Text,
                primary_key: false,
            },
            Column {
                name: "tbl_name".to_string(),
                ty: Type::Text,
                primary_key: false,
            },
            Column {
                name: "rootpage".to_string(),
                ty: Type::Integer,
                primary_key: false,
            },
            Column {
                name: "sql".to_string(),
                ty: Type::Text,
                primary_key: false,
            },
        ],
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Index {
    pub name: String,
    pub table_name: String,
    pub root_page: usize,
    pub columns: Vec<IndexColumn>,
    pub unique: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct IndexColumn {
    pub name: String,
    pub order: Order,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Order {
    Ascending,
    Descending,
}

impl Index {
    pub fn from_sql(sql: &str, root_page: usize) -> Result<Index> {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        match cmd {
            Some(Cmd::Stmt(Stmt::CreateIndex {
                idx_name,
                tbl_name,
                columns,
                unique,
                ..
            })) => {
                let index_name = normalize_ident(&idx_name.name.0);
                let index_columns = columns
                    .into_iter()
                    .map(|col| IndexColumn {
                        name: normalize_ident(&col.expr.to_string()),
                        order: match col.order {
                            Some(sqlite3_parser::ast::SortOrder::Asc) => Order::Ascending,
                            Some(sqlite3_parser::ast::SortOrder::Desc) => Order::Descending,
                            None => Order::Ascending,
                        },
                    })
                    .collect();
                Ok(Index {
                    name: index_name,
                    table_name: normalize_ident(&tbl_name.0),
                    root_page,
                    columns: index_columns,
                    unique,
                })
            }
            _ => todo!("Expected create index statement"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_has_rowid_true() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        assert!(table.has_rowid, "has_rowid should be set to true");
        Ok(())
    }

    #[test]
    pub fn test_has_rowid_false() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT) WITHOUT ROWID;"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        assert!(!table.has_rowid, "has_rowid should be set to false");
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_single_text() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a TEXT PRIMARY KEY, b TEXT);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            !table.column_is_rowid_alias(column),
            "column 'a´ has type different than INTEGER so can't be a rowid alias"
        );
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_single_integer() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            table.column_is_rowid_alias(column),
            "column 'a´ should be a rowid alias"
        );
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_single_integer_separate_primary_key_definition() -> Result<()>
    {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            table.column_is_rowid_alias(column),
            "column 'a´ should be a rowid alias"
        );
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_single_integer_separate_primary_key_definition_without_rowid(
    ) -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a)) WITHOUT ROWID;"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            !table.column_is_rowid_alias(column),
            "column 'a´ shouldn't be a rowid alias because table has no rowid"
        );
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_single_integer_without_rowid() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT) WITHOUT ROWID;"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            !table.column_is_rowid_alias(column),
            "column 'a´ shouldn't be a rowid alias because table has no rowid"
        );
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_inline_composite_primary_key() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT PRIMARY KEY);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            !table.column_is_rowid_alias(column),
            "column 'a´ shouldn't be a rowid alias because table has composite primary key"
        );
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_separate_composite_primary_key_definition() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a, b));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            !table.column_is_rowid_alias(column),
            "column 'a´ shouldn't be a rowid alias because table has composite primary key"
        );
        Ok(())
    }

    #[test]
    pub fn test_primary_key_inline_single() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT, c REAL);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.primary_key, "column 'a' should be a primary key");
        let column = table.get_column("b").unwrap().1;
        assert!(!column.primary_key, "column 'b' shouldn't be a primary key");
        let column = table.get_column("c").unwrap().1;
        assert!(!column.primary_key, "column 'c' shouldn't be a primary key");
        assert_eq!(
            vec!["a"],
            table.primary_key_column_names,
            "primary key column names should be ['a']"
        );
        Ok(())
    }

    #[test]
    pub fn test_primary_key_inline_multiple() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT PRIMARY KEY, c REAL);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.primary_key, "column 'a' should be a primary key");
        let column = table.get_column("b").unwrap().1;
        assert!(column.primary_key, "column 'b' shouldn be a primary key");
        let column = table.get_column("c").unwrap().1;
        assert!(!column.primary_key, "column 'c' shouldn't be a primary key");
        assert_eq!(
            vec!["a", "b"],
            table.primary_key_column_names,
            "primary key column names should be ['a', 'b']"
        );
        Ok(())
    }

    #[test]
    pub fn test_primary_key_separate_single() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, PRIMARY KEY(a));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.primary_key, "column 'a' should be a primary key");
        let column = table.get_column("b").unwrap().1;
        assert!(!column.primary_key, "column 'b' shouldn't be a primary key");
        let column = table.get_column("c").unwrap().1;
        assert!(!column.primary_key, "column 'c' shouldn't be a primary key");
        assert_eq!(
            vec!["a"],
            table.primary_key_column_names,
            "primary key column names should be ['a']"
        );
        Ok(())
    }

    #[test]
    pub fn test_primary_key_separate_multiple() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, PRIMARY KEY(a, b));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.primary_key, "column 'a' should be a primary key");
        let column = table.get_column("b").unwrap().1;
        assert!(column.primary_key, "column 'b' shouldn be a primary key");
        let column = table.get_column("c").unwrap().1;
        assert!(!column.primary_key, "column 'c' shouldn't be a primary key");
        assert_eq!(
            vec!["a", "b"],
            table.primary_key_column_names,
            "primary key column names should be ['a', 'b']"
        );
        Ok(())
    }

    #[test]
    pub fn test_primary_key_separate_single_quoted() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, PRIMARY KEY('a'));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.primary_key, "column 'a' should be a primary key");
        let column = table.get_column("b").unwrap().1;
        assert!(!column.primary_key, "column 'b' shouldn't be a primary key");
        let column = table.get_column("c").unwrap().1;
        assert!(!column.primary_key, "column 'c' shouldn't be a primary key");
        assert_eq!(
            vec!["a"],
            table.primary_key_column_names,
            "primary key column names should be ['a']"
        );
        Ok(())
    }
    #[test]
    pub fn test_primary_key_separate_single_doubly_quoted() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, PRIMARY KEY("a"));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.primary_key, "column 'a' should be a primary key");
        let column = table.get_column("b").unwrap().1;
        assert!(!column.primary_key, "column 'b' shouldn't be a primary key");
        let column = table.get_column("c").unwrap().1;
        assert!(!column.primary_key, "column 'c' shouldn't be a primary key");
        assert_eq!(
            vec!["a"],
            table.primary_key_column_names,
            "primary key column names should be ['a']"
        );
        Ok(())
    }

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
