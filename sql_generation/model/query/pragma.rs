use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Pragma {
    AutoVacuumMode(VacuumMode),
    ForeignKeyList(String),
    WalCheckpoint {
        database: Option<String>,
        mode: CheckpointMode,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CheckpointMode {
    Passive,
    Full,
    Restart,
    Truncate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VacuumMode {
    None,
    Incremental,
    Full,
}

impl Display for Pragma {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Pragma::AutoVacuumMode(vacuum_mode) => {
                let mode = match vacuum_mode {
                    VacuumMode::None => "none",
                    VacuumMode::Incremental => "incremental",
                    VacuumMode::Full => "full",
                };

                write!(f, "PRAGMA auto_vacuum={mode}")?;
                Ok(())
            }
            Pragma::ForeignKeyList(table_name) => {
                let table_name = table_name.replace('\'', "''");
                write!(f, "PRAGMA foreign_key_list('{table_name}')")
            }
            Pragma::WalCheckpoint { database, mode } => {
                write!(f, "PRAGMA ")?;
                if let Some(database) = database {
                    write!(f, "{database}.")?;
                }
                write!(
                    f,
                    "wal_checkpoint({})",
                    match mode {
                        CheckpointMode::Passive => "PASSIVE",
                        CheckpointMode::Full => "FULL",
                        CheckpointMode::Restart => "RESTART",
                        CheckpointMode::Truncate => "TRUNCATE",
                    }
                )
            }
        }
    }
}
