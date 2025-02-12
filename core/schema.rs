use crate::VirtualTable;
use crate::{util::normalize_ident, Result};
use core::fmt;
use fallible_iterator::FallibleIterator;
use limbo_sqlite3_parser::ast::{Expr, Literal, TableOptions};
use limbo_sqlite3_parser::{
    ast::{Cmd, CreateTableBody, QualifiedName, ResultColumn, Stmt},
    lexer::sql::Parser,
};
use std::collections::HashMap;
use std::rc::Rc;
use tracing::trace;

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
    Pseudo(Rc<PseudoTable>),
    Virtual(Rc<VirtualTable>),
}

impl Table {
    pub fn get_root_page(&self) -> usize {
        match self {
            Table::BTree(table) => table.root_page,
            Table::Pseudo(_) => unimplemented!(),
            Table::Virtual(_) => unimplemented!(),
        }
    }

    pub fn get_name(&self) -> &str {
        match self {
            Self::BTree(table) => &table.name,
            Self::Pseudo(_) => "",
            Self::Virtual(table) => &table.name,
        }
    }

    pub fn get_column_at(&self, index: usize) -> Option<&Column> {
        match self {
            Self::BTree(table) => table.columns.get(index),
            Self::Pseudo(table) => table.columns.get(index),
            Self::Virtual(table) => table.columns.get(index),
        }
    }

    pub fn columns(&self) -> &Vec<Column> {
        match self {
            Self::BTree(table) => &table.columns,
            Self::Pseudo(table) => &table.columns,
            Self::Virtual(table) => &table.columns,
        }
    }

    pub fn btree(&self) -> Option<Rc<BTreeTable>> {
        match self {
            Self::BTree(table) => Some(table.clone()),
            Self::Pseudo(_) => None,
            Self::Virtual(_) => None,
        }
    }

    pub fn virtual_table(&self) -> Option<Rc<VirtualTable>> {
        match self {
            Self::Virtual(table) => Some(table.clone()),
            _ => None,
        }
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::BTree(a), Self::BTree(b)) => Rc::ptr_eq(a, b),
            (Self::Pseudo(a), Self::Pseudo(b)) => Rc::ptr_eq(a, b),
            (Self::Virtual(a), Self::Virtual(b)) => Rc::ptr_eq(a, b),
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
        col.is_rowid_alias
    }

    pub fn get_column(&self, name: &str) -> Option<(usize, &Column)> {
        let name = normalize_ident(name);
        for (i, column) in self.columns.iter().enumerate() {
            if column.name.as_ref().map_or(false, |n| *n == name) {
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
                create_table(tbl_name, *body, root_page)
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
            sql.push_str(column.name.as_ref().expect("column name is None"));
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

    pub fn new_with_columns(columns: Vec<Column>) -> Self {
        Self { columns }
    }

    pub fn add_column(&mut self, name: &str, ty: Type, primary_key: bool) {
        self.columns.push(Column {
            name: Some(normalize_ident(name)),
            ty,
            ty_str: ty.to_string(),
            primary_key,
            is_rowid_alias: false,
            notnull: false,
            default: None,
        });
    }
    pub fn get_column(&self, name: &str) -> Option<(usize, &Column)> {
        let name = normalize_ident(name);
        for (i, column) in self.columns.iter().enumerate() {
            if column.name.as_ref().map_or(false, |n| *n == name) {
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
                    if let limbo_sqlite3_parser::ast::TableConstraint::PrimaryKey {
                        columns, ..
                    } = c.constraint
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
                // Regular sqlite tables have an integer rowid that uniquely identifies a row.
                // Even if you create a table with a column e.g. 'id INT PRIMARY KEY', there will still
                // be a separate hidden rowid, and the 'id' column will have a separate index built for it.
                //
                // However:
                // A column defined as exactly INTEGER PRIMARY KEY is a rowid alias, meaning that the rowid
                // and the value of this column are the same.
                // https://www.sqlite.org/lang_createtable.html#rowids_and_the_integer_primary_key
                let mut typename_exactly_integer = false;
                let (ty, ty_str) = match col_def.col_type {
                    Some(data_type) => {
                        let s = data_type.name.as_str();
                        let ty_str = if matches!(
                            s.to_uppercase().as_str(),
                            "TEXT" | "INT" | "INTEGER" | "BLOB" | "REAL"
                        ) {
                            s.to_uppercase().to_string()
                        } else {
                            s.to_string()
                        };

                        // https://www.sqlite.org/datatype3.html
                        let type_name = ty_str.to_uppercase();
                        if type_name.contains("INT") {
                            typename_exactly_integer = type_name == "INTEGER";
                            (Type::Integer, ty_str)
                        } else if type_name.contains("CHAR")
                            || type_name.contains("CLOB")
                            || type_name.contains("TEXT")
                        {
                            (Type::Text, ty_str)
                        } else if type_name.contains("BLOB") {
                            (Type::Blob, ty_str)
                        } else if type_name.is_empty() {
                            (Type::Blob, "".to_string())
                        } else if type_name.contains("REAL")
                            || type_name.contains("FLOA")
                            || type_name.contains("DOUB")
                        {
                            (Type::Real, ty_str)
                        } else {
                            (Type::Numeric, ty_str)
                        }
                    }
                    None => (Type::Null, "".to_string()),
                };

                let mut default = None;
                let mut primary_key = false;
                let mut notnull = false;
                for c_def in &col_def.constraints {
                    match &c_def.constraint {
                        limbo_sqlite3_parser::ast::ColumnConstraint::PrimaryKey { .. } => {
                            primary_key = true;
                        }
                        limbo_sqlite3_parser::ast::ColumnConstraint::NotNull { .. } => {
                            notnull = true;
                        }
                        limbo_sqlite3_parser::ast::ColumnConstraint::Default(expr) => {
                            default = Some(expr.clone())
                        }
                        _ => {}
                    }
                }

                if primary_key {
                    primary_key_column_names.push(name.clone());
                } else if primary_key_column_names.contains(&name) {
                    primary_key = true;
                }

                cols.push(Column {
                    name: Some(normalize_ident(&name)),
                    ty,
                    ty_str,
                    primary_key,
                    is_rowid_alias: typename_exactly_integer && primary_key,
                    notnull,
                    default,
                });
            }
            if options.contains(TableOptions::WITHOUT_ROWID) {
                has_rowid = false;
            }
        }
        CreateTableBody::AsSelect(_) => todo!(),
    };
    // flip is_rowid_alias back to false if the table has multiple primary keys
    // or if the table has no rowid
    if !has_rowid || primary_key_column_names.len() > 1 {
        for col in cols.iter_mut() {
            col.is_rowid_alias = false;
        }
    }
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
    pub name: Option<String>,
    pub ty: Type,
    // many sqlite operations like table_info retain the original string
    pub ty_str: String,
    pub primary_key: bool,
    pub is_rowid_alias: bool,
    pub notnull: bool,
    pub default: Option<Expr>,
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
            Self::Null => "",
            Self::Text => "TEXT",
            Self::Numeric => "NUMERIC",
            Self::Integer => "INTEGER",
            Self::Real => "REAL",
            Self::Blob => "BLOB",
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
                name: Some("type".to_string()),
                ty: Type::Text,
                ty_str: "TEXT".to_string(),
                primary_key: false,
                is_rowid_alias: false,
                notnull: false,
                default: None,
            },
            Column {
                name: Some("name".to_string()),
                ty: Type::Text,
                ty_str: "TEXT".to_string(),
                primary_key: false,
                is_rowid_alias: false,
                notnull: false,
                default: None,
            },
            Column {
                name: Some("tbl_name".to_string()),
                ty: Type::Text,
                ty_str: "TEXT".to_string(),
                primary_key: false,
                is_rowid_alias: false,
                notnull: false,
                default: None,
            },
            Column {
                name: Some("rootpage".to_string()),
                ty: Type::Integer,
                ty_str: "INT".to_string(),
                primary_key: false,
                is_rowid_alias: false,
                notnull: false,
                default: None,
            },
            Column {
                name: Some("sql".to_string()),
                ty: Type::Text,
                ty_str: "TEXT".to_string(),
                primary_key: false,
                is_rowid_alias: false,
                notnull: false,
                default: None,
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
                            Some(limbo_sqlite3_parser::ast::SortOrder::Asc) => Order::Ascending,
                            Some(limbo_sqlite3_parser::ast::SortOrder::Desc) => Order::Descending,
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

    pub fn automatic_from_primary_key(
        table: &BTreeTable,
        index_name: &str,
        root_page: usize,
    ) -> Result<Index> {
        if table.primary_key_column_names.is_empty() {
            return Err(crate::LimboError::InternalError(
                "Cannot create automatic index for table without primary key".to_string(),
            ));
        }

        let index_columns = table
            .primary_key_column_names
            .iter()
            .map(|col_name| {
                // Verify that each primary key column exists in the table
                if table.get_column(col_name).is_none() {
                    return Err(crate::LimboError::InternalError(format!(
                        "Primary key column {} not found in table {}",
                        col_name, table.name
                    )));
                }
                Ok(IndexColumn {
                    name: normalize_ident(col_name),
                    order: Order::Ascending, // Primary key indexes are always ascending
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Index {
            name: normalize_ident(index_name),
            table_name: table.name.clone(),
            root_page,
            columns: index_columns,
            unique: true, // Primary key indexes are always unique
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LimboError;

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
    pub fn test_default_value() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER DEFAULT 23);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        let default = column.default.clone().unwrap();
        assert_eq!(default.to_string(), "23");
        Ok(())
    }

    #[test]
    pub fn test_col_notnull() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER NOT NULL);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert_eq!(column.notnull, true);
        Ok(())
    }

    #[test]
    pub fn test_col_notnull_negative() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert_eq!(column.notnull, false);
        Ok(())
    }

    #[test]
    pub fn test_col_type_string_integer() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a InTeGeR);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert_eq!(column.ty_str, "INTEGER");
        Ok(())
    }

    #[test]
    pub fn test_col_type_string_int() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a InT);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert_eq!(column.ty_str, "INT");
        Ok(())
    }

    #[test]
    pub fn test_col_type_string_blob() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a bLoB);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert_eq!(column.ty_str, "BLOB");
        Ok(())
    }

    #[test]
    pub fn test_col_type_string_empty() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert_eq!(column.ty_str, "");
        Ok(())
    }

    #[test]
    pub fn test_col_type_string_some_nonsense() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a someNonsenseName);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert_eq!(column.ty_str, "someNonsenseName");
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

    #[test]
    fn test_automatic_index_single_column() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let index = Index::automatic_from_primary_key(&table, "sqlite_autoindex_t1_1", 2)?;

        assert_eq!(index.name, "sqlite_autoindex_t1_1");
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.root_page, 2);
        assert!(index.unique);
        assert_eq!(index.columns.len(), 1);
        assert_eq!(index.columns[0].name, "a");
        assert!(matches!(index.columns[0].order, Order::Ascending));
        Ok(())
    }

    #[test]
    fn test_automatic_index_composite_key() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a, b));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let index = Index::automatic_from_primary_key(&table, "sqlite_autoindex_t1_1", 2)?;

        assert_eq!(index.name, "sqlite_autoindex_t1_1");
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.root_page, 2);
        assert!(index.unique);
        assert_eq!(index.columns.len(), 2);
        assert_eq!(index.columns[0].name, "a");
        assert_eq!(index.columns[1].name, "b");
        assert!(matches!(index.columns[0].order, Order::Ascending));
        assert!(matches!(index.columns[1].order, Order::Ascending));
        Ok(())
    }

    #[test]
    fn test_automatic_index_no_primary_key() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let result = Index::automatic_from_primary_key(&table, "sqlite_autoindex_t1_1", 2);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            LimboError::InternalError(msg) if msg.contains("without primary key")
        ));
        Ok(())
    }

    #[test]
    fn test_automatic_index_nonexistent_column() -> Result<()> {
        // Create a table with a primary key column that doesn't exist in the table
        let table = BTreeTable {
            root_page: 0,
            name: "t1".to_string(),
            has_rowid: true,
            primary_key_column_names: vec!["nonexistent".to_string()],
            columns: vec![Column {
                name: Some("a".to_string()),
                ty: Type::Integer,
                ty_str: "INT".to_string(),
                primary_key: false,
                is_rowid_alias: false,
                notnull: false,
                default: None,
            }],
        };

        let result = Index::automatic_from_primary_key(&table, "sqlite_autoindex_t1_1", 2);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            LimboError::InternalError(msg) if msg.contains("not found in table")
        ));
        Ok(())
    }
}
