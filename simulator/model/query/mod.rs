use std::fmt::Display;

pub(crate) use create::Create;
pub(crate) use delete::Delete;
pub(crate) use drop::Drop;
pub(crate) use insert::Insert;
pub(crate) use select::Select;
use serde::{Deserialize, Serialize};

use crate::{model::table::Value, runner::env::SimulatorEnv};

pub mod create;
pub mod delete;
pub mod drop;
pub mod insert;
pub mod select;

// This type represents the potential queries on the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Query {
    Create(Create),
    Select(Select),
    Insert(Insert),
    Delete(Delete),
    Drop(Drop),
}

impl Query {
    pub(crate) fn dependencies(&self) -> Vec<String> {
        match self {
            Query::Create(_) => vec![],
            Query::Select(Select { table, .. })
            | Query::Insert(Insert::Select { table, .. })
            | Query::Insert(Insert::Values { table, .. })
            | Query::Delete(Delete { table, .. })
            | Query::Drop(Drop { table, .. }) => vec![table.clone()],
        }
    }
    pub(crate) fn uses(&self) -> Vec<String> {
        match self {
            Query::Create(Create { table }) => vec![table.name.clone()],
            Query::Select(Select { table, .. })
            | Query::Insert(Insert::Select { table, .. })
            | Query::Insert(Insert::Values { table, .. })
            | Query::Delete(Delete { table, .. })
            | Query::Drop(Drop { table, .. }) => vec![table.clone()],
        }
    }

    pub(crate) fn shadow(&self, env: &mut SimulatorEnv) -> Vec<Vec<Value>> {
        match self {
            Query::Create(create) => create.shadow(env),
            Query::Insert(insert) => insert.shadow(env),
            Query::Delete(delete) => delete.shadow(env),
            Query::Select(select) => select.shadow(env),
            Query::Drop(drop) => drop.shadow(env),
        }
    }
}

impl Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create(create) => write!(f, "{}", create),
            Self::Select(select) => write!(f, "{}", select),
            Self::Insert(insert) => write!(f, "{}", insert),
            Self::Delete(delete) => write!(f, "{}", delete),
            Self::Drop(drop) => write!(f, "{}", drop),
        }
    }
}
