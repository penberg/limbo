use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::{
    model::table::{Table, Value},
    runner::env::SimulatorEnv,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum Predicate {
    And(Vec<Predicate>), // p1 AND p2 AND p3... AND pn
    Or(Vec<Predicate>),  // p1 OR p2 OR p3... OR pn
    Eq(String, Value),   // column = Value
    Neq(String, Value),  // column != Value
    Gt(String, Value),   // column > Value
    Lt(String, Value),   // column < Value
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
            | Query::Insert(Insert { table, .. })
            | Query::Delete(Delete { table, .. }) => vec![table.clone()],
        }
    }
    pub(crate) fn uses(&self) -> Vec<String> {
        match self {
            Query::Create(Create { table }) => vec![table.name.clone()],
            Query::Select(Select { table, .. })
            | Query::Insert(Insert { table, .. })
            | Query::Delete(Delete { table, .. }) => vec![table.clone()],
        }
    }

    pub(crate) fn shadow(&self, env: &mut SimulatorEnv) {
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
    pub(crate) fn shadow(&self, env: &mut SimulatorEnv) {
        if !env.tables.iter().any(|t| t.name == self.table.name) {
            env.tables.push(self.table.clone());
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Select {
    pub(crate) table: String,
    pub(crate) predicate: Predicate,
}

impl Select {
    pub(crate) fn shadow(&self, _env: &mut SimulatorEnv) {}
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Insert {
    pub(crate) table: String,
    pub(crate) values: Vec<Vec<Value>>,
}

impl Insert {
    pub(crate) fn shadow(&self, env: &mut SimulatorEnv) {
        if let Some(t) = env.tables.iter_mut().find(|t| t.name == self.table) {
            t.rows.extend(self.values.clone());
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Delete {
    pub(crate) table: String,
    pub(crate) predicate: Predicate,
}

impl Delete {
    pub(crate) fn shadow(&self, _env: &mut SimulatorEnv) {
        todo!()
    }
}

impl Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create(Create { table }) => {
                write!(f, "CREATE TABLE {} (", table.name)?;

                for (i, column) in table.columns.iter().enumerate() {
                    if i != 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{} {}", column.name, column.column_type)?;
                }

                write!(f, ")")
            }
            Self::Select(Select {
                table,
                predicate: guard,
            }) => write!(f, "SELECT * FROM {} WHERE {}", table, guard),
            Self::Insert(Insert { table, values }) => {
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
            Self::Delete(Delete {
                table,
                predicate: guard,
            }) => write!(f, "DELETE FROM {} WHERE {}", table, guard),
        }
    }
}
