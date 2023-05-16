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

    pub async fn new_s3(options: s3::Options) -> Result<Self> {
        Ok(Self::S3(s3::Replicator::new(options).await?))
    }
}

impl Storage {
    pub async fn log_tx(&mut self, m: LogRecord) -> Result<()> {
        match self {
            Self::JsonOnDisk(path) => {
                use tokio::io::AsyncWriteExt;
                let t = serde_json::to_vec(&m).map_err(|e| DatabaseError::Io(e.to_string()))?;
                let mut file = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .await
                    .map_err(|e| DatabaseError::Io(e.to_string()))?;
                file.write_all(&t)
                    .await
                    .map_err(|e| DatabaseError::Io(e.to_string()))?;
                file.write_all(b"\n")
                    .await
                    .map_err(|e| DatabaseError::Io(e.to_string()))?;
            }
            Self::S3(replicator) => {
                replicator.replicate_tx(m).await?;
            }
            Self::Noop => (),
        }
        Ok(())
    }

    pub async fn read_tx_log(&self) -> Result<Vec<LogRecord>> {
        match self {
            Self::JsonOnDisk(path) => {
                use tokio::io::AsyncBufReadExt;
                let file = tokio::fs::OpenOptions::new()
                    .read(true)
                    .open(&path)
                    .await
                    .map_err(|e| DatabaseError::Io(e.to_string()))?;

                let mut records: Vec<LogRecord> = Vec::new();
                let mut lines = tokio::io::BufReader::new(file).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    records.push(
                        serde_json::from_str(&line)
                            .map_err(|e| DatabaseError::Io(e.to_string()))?,
                    )
                }
                Ok(records)
            }
            Self::S3(replicator) => replicator.read_tx_log().await,
            Self::Noop => Err(crate::errors::DatabaseError::Io(
                "cannot read from Noop storage".to_string(),
            )),
        }
    }
}
