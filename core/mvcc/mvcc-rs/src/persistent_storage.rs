use crate::database::{LogRecord, Result};
use crate::errors::DatabaseError;

#[derive(Debug)]
pub enum Storage {
    Noop,
    JsonOnDisk(std::path::PathBuf),
}

impl Storage {
    pub fn new_noop() -> Self {
        Self::Noop
    }

    pub fn new_json_on_disk(path: impl Into<std::path::PathBuf>) -> Self {
        let path = path.into();
        Self::JsonOnDisk(path)
    }
}

#[pin_project::pin_project]
pub struct JsonOnDiskStream {
    #[pin]
    inner: tokio_stream::wrappers::LinesStream<tokio::io::BufReader<tokio::fs::File>>,
}

impl futures::stream::Stream for JsonOnDiskStream {
    type Item = LogRecord;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner
            .poll_next(cx)
            .map(|x| x.and_then(|x| x.ok().and_then(|x| serde_json::from_str(x.as_str()).ok())))
    }
}

impl Storage {
    pub async fn log_tx(&mut self, m: LogRecord) -> Result<()> {
        if let Self::JsonOnDisk(path) = self {
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
        Ok(())
    }

    pub async fn read_tx_log(&self) -> Result<JsonOnDiskStream> {
        if let Self::JsonOnDisk(path) = self {
            use tokio::io::AsyncBufReadExt;
            let file = tokio::fs::OpenOptions::new()
                .read(true)
                .open(&path)
                .await
                .map_err(|e| DatabaseError::Io(e.to_string()))?;
            Ok(JsonOnDiskStream {
                inner: tokio_stream::wrappers::LinesStream::new(
                    tokio::io::BufReader::new(file).lines(),
                ),
            })
        } else {
            Err(crate::errors::DatabaseError::Io(
                "cannot read from Noop storage".to_string(),
            ))
        }
    }
}
