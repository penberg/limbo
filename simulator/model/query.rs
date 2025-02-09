use std::fmt::Display;

use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};

use crate::{
    model::table::{Table, Value},
    runner::env::SimulatorEnv,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum Predicate {
    And(Vec<Predicate>),  // p1 AND p2 AND p3... AND pn
    Or(Vec<Predicate>),   // p1 OR p2 OR p3... OR pn
    Eq(String, Value),    // column = Value
    Neq(String, Value),   // column != Value
    Gt(String, Value),    // column > Value
    Lt(String, Value),    // column < Value
    Like(String, String), // column LIKE Value
}

/// This function is a duplication of the exec_like function in core/vdbe/mod.rs at commit 9b9d5f9b4c9920e066ef1237c80878f4c3968524
/// Any updates to the original function should be reflected here, otherwise the test will be incorrect.
fn construct_like_regex(pattern: &str) -> Regex {
    let mut regex_pattern = String::with_capacity(pattern.len() * 2);

    regex_pattern.push('^');

    for c in pattern.chars() {
        match c {
            '\\' => regex_pattern.push_str("\\\\"),
            '%' => regex_pattern.push_str(".*"),
            '_' => regex_pattern.push('.'),
            ch => {
                if regex_syntax::is_meta_character(c) {
                    regex_pattern.push('\\');
                }
                regex_pattern.push(ch);
            }
        }
    }

    regex_pattern.push('$');

    RegexBuilder::new(&regex_pattern)
        .case_insensitive(true)
        .dot_matches_new_line(true)
        .build()
        .unwrap()
}

fn exec_like(pattern: &str, text: &str) -> bool {
    let re = construct_like_regex(pattern);
    re.is_match(text)
}

impl Predicate {
    pub(crate) fn true_() -> Self {
        Self::And(vec![])
    }

    pub(crate) fn false_() -> Self {
        Self::Or(vec![])
    }

    pub(crate) fn test(&self, row: &[Value], table: &Table) -> bool {
        let get_value = |name: &str| {
            table
                .columns
                .iter()
                .zip(row.iter())
                .find(|(column, _)| column.name == name)
                .map(|(_, value)| value)
        };

        match self {
            Predicate::And(vec) => vec.iter().all(|p| p.test(row, table)),
            Predicate::Or(vec) => vec.iter().any(|p| p.test(row, table)),
            Predicate::Eq(column, value) => get_value(column) == Some(value),
            Predicate::Neq(column, value) => get_value(column) != Some(value),
            Predicate::Gt(column, value) => get_value(column).map(|v| v > value).unwrap_or(false),
            Predicate::Lt(column, value) => get_value(column).map(|v| v < value).unwrap_or(false),
            Predicate::Like(column, value) => get_value(column)
                .map(|v| exec_like(v.to_string().as_str(), value.as_str()))
                .unwrap_or(false),
        }
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::And(predicates) => {
                if predicates.is_empty() {
                    // todo: Make this TRUE when the bug is fixed
                    write!(f, "TRUE")
                } else {
                    write!(f, "(")?;
                    for (i, p) in predicates.iter().enumerate() {
                        if i != 0 {
                            write!(f, " AND ")?;
                        }
                        write!(f, "{}", p)?;
                    }
                    write!(f, ")")
                }
            }
            Self::Or(predicates) => {
                if predicates.is_empty() {
                    write!(f, "FALSE")
                } else {
                    write!(f, "(")?;
                    for (i, p) in predicates.iter().enumerate() {
                        if i != 0 {
                            write!(f, " OR ")?;
                        }
                        write!(f, "{}", p)?;
                    }
                    write!(f, ")")
                }
            }
            Self::Eq(name, value) => write!(f, "{} = {}", name, value),
            Self::Neq(name, value) => write!(f, "{} != {}", name, value),
            Self::Gt(name, value) => write!(f, "{} > {}", name, value),
            Self::Lt(name, value) => write!(f, "{} < {}", name, value),
            Self::Like(name, value) => write!(f, "{} LIKE '{}'", name, value),
        }
    }
}

// This type represents the potential queries on the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Query {
    Create(Create),
    Select(Select),
    Insert(Insert),
    Delete(Delete),
}

impl Query {
    pub(crate) fn dependencies(&self) -> Vec<String> {
        match self {
            Query::Create(_) => vec![],
            Query::Select(Select { table, .. })
            | Query::Insert(Insert::Select { table, .. })
            | Query::Insert(Insert::Values { table, .. })
            | Query::Delete(Delete { table, .. }) => vec![table.clone()],
        }
    }
    pub(crate) fn uses(&self) -> Vec<String> {
        match self {
            Query::Create(Create { table }) => vec![table.name.clone()],
            Query::Select(Select { table, .. })
            | Query::Insert(Insert::Select { table, .. })
            | Query::Insert(Insert::Values { table, .. })
            | Query::Delete(Delete { table, .. }) => vec![table.clone()],
        }
    }

    pub(crate) fn shadow(&self, env: &mut SimulatorEnv) -> Vec<Vec<Value>> {
        match self {
            Query::Create(create) => create.shadow(env),
            Query::Insert(insert) => insert.shadow(env),
            Query::Delete(delete) => delete.shadow(env),
            Query::Select(select) => select.shadow(env),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Create {
    pub(crate) table: Table,
}

impl Create {
    pub(crate) fn shadow(&self, env: &mut SimulatorEnv) -> Vec<Vec<Value>> {
        if !env.tables.iter().any(|t| t.name == self.table.name) {
            env.tables.push(self.table.clone());
        }

        vec![]
    }
}

impl Display for Create {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE TABLE {} (", self.table.name)?;

        for (i, column) in self.table.columns.iter().enumerate() {
            if i != 0 {
                write!(f, ",")?;
            }
            write!(f, "{} {}", column.name, column.column_type)?;
        }

        write!(f, ")")
    }
}

/// `SELECT` distinctness
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Distinctness {
    /// `DISTINCT`
    Distinct,
    /// `ALL`
    All,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Select {
    pub(crate) table: String,
    pub(crate) predicate: Predicate,
    pub(crate) distinct: Distinctness,
    pub(crate) limit: Option<usize>,
}

impl Select {
    pub(crate) fn shadow(&self, env: &mut SimulatorEnv) -> Vec<Vec<Value>> {
        let table = env.tables.iter().find(|t| t.name == self.table.as_str());
        if let Some(table) = table {
            table
                .rows
                .iter()
                .filter(|row| self.predicate.test(row, table))
                .cloned()
                .collect()
        } else {
            vec![]
        }
    }
}

impl Display for Select {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SELECT * FROM {} WHERE {}{}",
            self.table,
            self.predicate,
            self.limit
                .map_or("".to_string(), |l| format!(" LIMIT {}", l))
        )
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum Insert {
    Values {
        table: String,
        values: Vec<Vec<Value>>,
    },
    Select {
        table: String,
        select: Box<Select>,
    },
}

impl Insert {
    pub(crate) fn shadow(&self, env: &mut SimulatorEnv) -> Vec<Vec<Value>> {
        match self {
            Insert::Values { table, values } => {
                if let Some(t) = env.tables.iter_mut().find(|t| &t.name == table) {
                    t.rows.extend(values.clone());
                }
            }
            Insert::Select { table, select } => {
                let rows = select.shadow(env);
                if let Some(t) = env.tables.iter_mut().find(|t| &t.name == table) {
                    t.rows.extend(rows);
                }
            }
        }

        vec![]
    }

    pub(crate) fn table(&self) -> &str {
        match self {
            Insert::Values { table, .. } | Insert::Select { table, .. } => table,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Delete {
    pub(crate) table: String,
    pub(crate) predicate: Predicate,
}

impl Delete {
    pub(crate) fn shadow(&self, _env: &mut SimulatorEnv) -> Vec<Vec<Value>> {
        vec![]
    }
}

impl Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create(create) => write!(f, "{}", create),
            Self::Select(select) => write!(f, "{}", select),
            Self::Insert(Insert::Values { table, values }) => {
                write!(f, "INSERT INTO {} VALUES ", table)?;
                for (i, row) in values.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "(")?;
                    for (j, value) in row.iter().enumerate() {
                        if j != 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{}", value)?;
                    }
                    write!(f, ")")?;
                }
                Ok(())
            }
            Self::Insert(Insert::Select { table, select }) => {
                write!(f, "INSERT INTO {} ", table)?;
                write!(f, "{}", select)
            }
            Self::Delete(Delete {
                table,
                predicate: guard,
            }) => write!(f, "DELETE FROM {} WHERE {}", table, guard),
        }
    }
}
