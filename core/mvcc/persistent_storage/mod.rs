use std::fmt::Debug;

use crate::mvcc::database::{LogRecord, Result};
use crate::mvcc::errors::DatabaseError;

#[derive(Debug)]
pub enum Storage {
    Noop,
}

impl Storage {
    pub fn new_noop() -> Self {
        Self::Noop
    }
}

impl Storage {
    pub fn log_tx<T>(&self, _m: LogRecord<T>) -> Result<()> {
        match self {
            Self::Noop => (),
        }
        Ok(())
    }

    pub fn read_tx_log<T>(&self) -> Result<Vec<LogRecord<T>>> {
        match self {
            Self::Noop => Err(DatabaseError::Io(
                "cannot read from Noop storage".to_string(),
            )),
        }
    }
}
