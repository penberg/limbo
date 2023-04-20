use crate::database::{Result, Mutation};
use crate::errors::DatabaseError;

/// Persistent storage API for storing and retrieving transactions.
/// TODO: final design in heavy progress!
#[async_trait::async_trait]
pub trait Storage {
    type Stream: futures::stream::Stream<Item = Mutation>;

    async fn store(&mut self, m: Mutation) -> Result<()>;
    async fn scan(&self) -> Result<Self::Stream>;
}

pub struct Noop {}

#[async_trait::async_trait]
impl Storage for Noop {
    type Stream = futures::stream::Empty<Mutation>;

    async fn store(&mut self, _m: Mutation) -> Result<()> {
        Ok(())
    }

    async fn scan(&self) -> Result<Self::Stream> {
        Ok(futures::stream::empty())
    }
}

pub struct JsonOnDisk {
    pub path: std::path::PathBuf,
}

impl JsonOnDisk {
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        let path = path.into();
        Self { path }
    }
}

#[cfg(feature = "tokio")]
#[pin_project::pin_project]
pub struct JsonOnDiskStream {
    #[pin]
    inner: tokio_stream::wrappers::LinesStream<tokio::io::BufReader<tokio::fs::File>>,
}

#[cfg(feature = "tokio")]
impl futures::stream::Stream for JsonOnDiskStream {
    type Item = Mutation;

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

#[cfg(feature = "tokio")]
#[async_trait::async_trait]
impl Storage for JsonOnDisk {
    type Stream = JsonOnDiskStream;

    async fn store(&mut self, m: Mutation) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        let t = serde_json::to_vec(&m).map_err(|e| DatabaseError::Io(e.to_string()))?;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .map_err(|e| DatabaseError::Io(e.to_string()))?;
        file.write_all(&t)
            .await
            .map_err(|e| DatabaseError::Io(e.to_string()))?;
        file.write_all(b"\n")
            .await
            .map_err(|e| DatabaseError::Io(e.to_string()))?;
        Ok(())
    }

    async fn scan(&self) -> Result<Self::Stream> {
        use tokio::io::AsyncBufReadExt;
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .open(&self.path)
            .await
            .unwrap();
        Ok(JsonOnDiskStream {
            inner: tokio_stream::wrappers::LinesStream::new(
                tokio::io::BufReader::new(file).lines(),
            ),
        })
    }
}
