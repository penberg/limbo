use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::{
    model::table::{Table, Value},
    SimulatorEnv,
};

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
