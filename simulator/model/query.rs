use std::fmt::Display;

use crate::model::table::{Table, Value};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Predicate {
    And(Vec<Predicate>), // p1 AND p2 AND p3... AND pn
    Or(Vec<Predicate>),  // p1 OR p2 OR p3... OR pn
    Eq(String, Value),   // column = Value
    Neq(String, Value),  // column != Value
    Gt(String, Value),   // column > Value
    Lt(String, Value),   // column < Value
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
#[derive(Debug)]
pub(crate) enum Query {
    Create(Create),
    Select(Select),
    Insert(Insert),
    Delete(Delete),
}

#[derive(Debug)]
pub(crate) struct Create {
    pub(crate) table: Table,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Select {
    pub(crate) table: String,
    pub(crate) predicate: Predicate,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Insert {
    pub(crate) table: String,
    pub(crate) values: Vec<Vec<Value>>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Delete {
    pub(crate) table: String,
    pub(crate) predicate: Predicate,
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
