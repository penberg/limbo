use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::{model::table::Value, SimulatorEnv};

use super::select::Predicate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Delete {
    pub(crate) table: String,
    pub(crate) predicate: Predicate,
}

impl Delete {
    pub(crate) fn shadow(&self, env: &mut SimulatorEnv) -> Vec<Vec<Value>> {
        let table = env
            .tables
            .iter_mut()
            .find(|t| t.name == self.table)
            .unwrap();

        let t2 = table.clone();

        table.rows.retain_mut(|r| self.predicate.test(r, &t2));

        vec![]
    }
}

impl Display for Delete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DELETE FROM {} WHERE {}", self.table, self.predicate)
    }
}
