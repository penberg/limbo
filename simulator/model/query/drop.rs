use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::{model::table::Value, SimulatorEnv};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Drop {
    pub(crate) table: String,
}

impl Drop {
    pub(crate) fn shadow(&self, env: &mut SimulatorEnv) -> Vec<Vec<Value>> {
        env.tables.retain(|t| t.name != self.table);
        vec![]
    }
}

impl Display for Drop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP TABLE {}", self.table)
    }
}
