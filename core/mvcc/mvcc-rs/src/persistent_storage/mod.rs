use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt::Debug;

use crate::database::{LogRecord, Result};
use crate::errors::DatabaseError;

pub mod s3;

#[derive(Debug)]
pub enum Storage {
    Noop,
    JsonOnDisk(std::path::PathBuf),
    S3(s3::Replicator),
}

impl Storage {
    pub fn new_noop() -> Self {
        Self::Noop
    }

    pub fn new_json_on_disk(path: impl Into<std::path::PathBuf>) -> Self {
        let path = path.into();
        Self::JsonOnDisk(path)
    }

    pub fn new_s3(options: s3::Options) -> Result<Self> {
        let replicator = futures::executor::block_on(s3::Replicator::new(options))?;
        Ok(Self::S3(replicator))
    }
}

impl Storage {
    pub fn log_tx<T: Serialize>(&self, m: LogRecord<T>) -> Result<()> {
        match self {
            Self::JsonOnDisk(path) => {
                use std::io::Write;
                let t = serde_json::to_vec(&m).map_err(|e| DatabaseError::Io(e.to_string()))?;
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .map_err(|e| DatabaseError::Io(e.to_string()))?;
                file.write_all(&t)
                    .map_err(|e| DatabaseError::Io(e.to_string()))?;
                file.write_all(b"\n")
                    .map_err(|e| DatabaseError::Io(e.to_string()))?;
            }
            Self::S3(replicator) => {
                futures::executor::block_on(replicator.replicate_tx(m))?;
            }
            Self::Noop => (),
        }
        Ok(())
    }

    pub fn read_tx_log<T: DeserializeOwned + Debug>(&self) -> Result<Vec<LogRecord<T>>> {
        match self {
            Self::JsonOnDisk(path) => {
                use std::io::BufRead;
                let file = std::fs::OpenOptions::new()
                    .read(true)
                    .open(path)
                    .map_err(|e| DatabaseError::Io(e.to_string()))?;

                let mut records: Vec<LogRecord<T>> = Vec::new();
                let mut lines = std::io::BufReader::new(file).lines();
                while let Some(Ok(line)) = lines.next() {
                    records.push(
                        serde_json::from_str(&line)
                            .map_err(|e| DatabaseError::Io(e.to_string()))?,
                    )
                }
                Ok(records)
            }
            Self::S3(replicator) => futures::executor::block_on(replicator.read_tx_log()),
            Self::Noop => Err(crate::errors::DatabaseError::Io(
                "cannot read from Noop storage".to_string(),
            )),
        }
    }
}
