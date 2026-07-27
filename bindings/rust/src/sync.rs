use std::{
    future::Future,
    io::ErrorKind,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
    time::Duration,
};

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{header::AUTHORIZATION, Request};
use hyper_rustls::HttpsConnector;
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use tokio::sync::mpsc;
use turso_sdk_kit::IoBackend;

use crate::{connection::Connection, Error, Result};

// Public re-exports of sync types for users of this crate.
pub use turso_sync_sdk_kit::rsapi::DatabaseSyncStats;
pub use turso_sync_sdk_kit::rsapi::PartialBootstrapStrategy;
pub use turso_sync_sdk_kit::rsapi::PartialSyncOpts;

// Constants used across the sync module
const DEFAULT_CLIENT_NAME: &str = "turso-sync-rust";
const CHECKPOINT_BUSY_RETRY_DELAY: Duration = Duration::from_millis(10);
const CHECKPOINT_BUSY_MAX_ATTEMPTS: usize = 100;

/// Future returned by an auth token provider. Resolves to a bearer token string
/// (without the `Bearer ` prefix — that prefix is added when building the header).
pub type AuthTokenFut = Pin<Box<dyn Future<Output = Result<String>> + Send + 'static>>;

/// Async callback that produces an auth token on demand. Invoked before every
/// HTTP request issued by the sync engine, so it can return a freshly-rotated
/// token (e.g. fetched from a secrets manager or refreshed via OAuth).
pub type AuthTokenFn = Arc<dyn Fn() -> AuthTokenFut + Send + Sync + 'static>;

/// Encryption cipher for Turso Cloud remote encryption.
/// These match the server-side encryption settings.
#[derive(Debug, Clone, Copy)]
pub enum RemoteEncryptionCipher {
    Aes256Gcm,
    Aes128Gcm,
    ChaCha20Poly1305,
    Aegis128L,
    Aegis128X2,
    Aegis128X4,
    Aegis256,
    Aegis256X2,
    Aegis256X4,
}

impl RemoteEncryptionCipher {
    /// Returns the total reserved bytes as required by the server
    pub fn reserved_bytes(&self) -> usize {
        match self {
            Self::Aes256Gcm | Self::Aes128Gcm | Self::ChaCha20Poly1305 => 28,
            Self::Aegis128L | Self::Aegis128X2 | Self::Aegis128X4 => 32,
            Self::Aegis256 | Self::Aegis256X2 | Self::Aegis256X4 => 48,
        }
    }
}

impl std::str::FromStr for RemoteEncryptionCipher {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "aes256gcm" | "aes-256-gcm" => Ok(Self::Aes256Gcm),
            "aes128gcm" | "aes-128-gcm" => Ok(Self::Aes128Gcm),
            "chacha20poly1305" | "chacha20-poly1305" => Ok(Self::ChaCha20Poly1305),
            "aegis128l" | "aegis-128l" => Ok(Self::Aegis128L),
            "aegis128x2" | "aegis-128x2" => Ok(Self::Aegis128X2),
            "aegis128x4" | "aegis-128x4" => Ok(Self::Aegis128X4),
            "aegis256" | "aegis-256" => Ok(Self::Aegis256),
            "aegis256x2" | "aegis-256x2" => Ok(Self::Aegis256X2),
            "aegis256x4" | "aegis-256x4" => Ok(Self::Aegis256X4),
            _ => Err(format!(
                "unknown cipher: '{s}'. Supported: aes256gcm, aes128gcm, chacha20poly1305, \
                 aegis128l, aegis128x2, aegis128x4, aegis256, aegis256x2, aegis256x4"
            )),
        }
    }
}

// Builder for a synced database.
pub struct Builder {
    // Absolute or relative path to local database file (":memory:" is supported).
    path: String,
    // Remote URL base. Supports https://, http://, libsql:// and turso:// (the latter two are
    // translated to https://).
    remote_url: Option<String>,
    // Optional authorization token provider (static string or async callback).
    auth_token: Option<AuthTokenFn>,
    // Optional custom client identifier used by the sync engine for telemetry/tracing.
    client_name: Option<String>,
    // Optional long-poll timeout when waiting for server changes.
    long_poll_timeout: Option<Duration>,
    // Whether to bootstrap a database if it's empty (download schema and initial data).
    bootstrap_if_empty: bool,
    // Partial sync configuration (EXPERIMENTAL).
    partial_sync_config_experimental: Option<PartialSyncOpts>,
    // Encryption key (base64-encoded) for the Turso Cloud database
    remote_encryption_key: Option<String>,
    // Encryption cipher for the Turso Cloud database
    remote_encryption_cipher: Option<RemoteEncryptionCipher>,
    // Sync-protocol override: None (default) auto-detects the remote protocol
    // from the first pull-updates response; Some(true) forces MVCC logical-log
    // pulls; Some(false) forces page-stream pulls.
    logical_mvcc_pull: Option<bool>,
    // Experimental engine features to enable on the local synced database.
    // These mirror the local [`crate::Builder`] flags so synced databases
    // expose the same SQL surface as their local-only counterparts. Local
    // at-rest `encryption` is intentionally omitted because the sync engine
    // does not support local encryption (cloud encryption is configured
    // separately via `with_remote_encryption`).
    enable_attach: bool,
    enable_custom_types: bool,
    enable_index_method: bool,
    enable_materialized_views: bool,
    enable_vacuum: bool,
    enable_generated_columns: bool,
    enable_multiprocess_wal: bool,
    enable_without_rowid: bool,
}

impl Builder {
    // Create a new Builder for a synced database.
    pub fn new_remote(path: &str) -> Self {
        Self {
            path: path.to_string(),
            remote_url: None,
            auth_token: None,
            client_name: None,
            long_poll_timeout: None,
            bootstrap_if_empty: true,
            partial_sync_config_experimental: None,
            remote_encryption_key: None,
            remote_encryption_cipher: None,
            logical_mvcc_pull: None,
            enable_attach: false,
            enable_custom_types: false,
            enable_index_method: false,
            enable_materialized_views: false,
            enable_vacuum: false,
            enable_generated_columns: false,
            enable_multiprocess_wal: false,
            enable_without_rowid: false,
        }
    }

    /// Enable the experimental `attach` engine feature for the synced database.
    /// Mirrors the local [`crate::Builder::experimental_attach`] method.
    pub fn experimental_attach(mut self, enable: bool) -> Self {
        self.enable_attach = enable;
        self
    }

    /// Enable the experimental `custom_types` engine feature for the synced
    /// database. Mirrors the local [`crate::Builder::experimental_custom_types`].
    pub fn experimental_custom_types(mut self, enable: bool) -> Self {
        self.enable_custom_types = enable;
        self
    }

    /// Enable the experimental `index_method` engine feature for the synced
    /// database. When enabled, SQL statements like
    /// `CREATE INDEX idx ON t USING fts (...)` are accepted by the local
    /// engine. Mirrors the local [`crate::Builder::experimental_index_method`]
    /// method so callers can use the same SQL surface in synced mode.
    pub fn experimental_index_method(mut self, enable: bool) -> Self {
        self.enable_index_method = enable;
        self
    }

    /// Enable the experimental materialized `views` engine feature for the
    /// synced database. Mirrors the local
    /// [`crate::Builder::experimental_materialized_views`].
    pub fn experimental_materialized_views(mut self, enable: bool) -> Self {
        self.enable_materialized_views = enable;
        self
    }

    /// Enable the experimental `vacuum` engine feature for the synced database.
    /// Mirrors the local [`crate::Builder::experimental_vacuum`].
    pub fn experimental_vacuum(mut self, enable: bool) -> Self {
        self.enable_vacuum = enable;
        self
    }

    /// Enable the experimental `generated_columns` engine feature for the
    /// synced database. Mirrors the local
    /// [`crate::Builder::experimental_generated_columns`].
    pub fn experimental_generated_columns(mut self, enable: bool) -> Self {
        self.enable_generated_columns = enable;
        self
    }

    /// Enable the experimental `multiprocess_wal` engine feature for the synced
    /// database. Mirrors the local
    /// [`crate::Builder::experimental_multiprocess_wal`].
    pub fn experimental_multiprocess_wal(mut self, enable: bool) -> Self {
        self.enable_multiprocess_wal = enable;
        self
    }

    /// Enable the experimental `without_rowid` engine feature for the synced
    /// database. Mirrors the local
    /// [`crate::Builder::experimental_without_rowid`].
    pub fn experimental_without_rowid(mut self, enable: bool) -> Self {
        self.enable_without_rowid = enable;
        self
    }

    // Set remote_url for HTTP requests.
    // If remote_url omitted in configuration - tursodb will try to load it from the metadata file
    pub fn with_remote_url(mut self, remote_url: impl Into<String>) -> Self {
        self.remote_url = Some(remote_url.into());
        self
    }

    // Set optional authorization token for HTTP requests.
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        let token = token.into();
        self.auth_token = Some(Arc::new(move || {
            let token = token.clone();
            Box::pin(async move { Ok(token) })
        }));
        self
    }

    /// Set an async callback that produces an auth token on demand.
    ///
    /// The callback is invoked before every HTTP request, so it can return a
    /// freshly rotated token (e.g. fetched from a secrets manager or refreshed
    /// via OAuth). If the callback returns an error, the in-flight sync
    /// operation fails with that error.
    ///
    /// Calling this overrides any previously configured static token.
    pub fn with_auth_token_fn<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String>> + Send + 'static,
    {
        self.auth_token = Some(Arc::new(move || Box::pin(f())));
        self
    }

    // Set custom client name (defaults to 'turso-sync-rust').
    pub fn with_client_name(mut self, name: impl Into<String>) -> Self {
        self.client_name = Some(name.into());
        self
    }

    // Set long poll timeout for waiting remote changes.
    pub fn with_long_poll_timeout(mut self, timeout: Duration) -> Self {
        self.long_poll_timeout = Some(timeout);
        self
    }

    // Configure bootstrap behavior for empty databases.
    pub fn bootstrap_if_empty(mut self, enable: bool) -> Self {
        self.bootstrap_if_empty = enable;
        self
    }

    // Set experimental partial sync configuration.
    pub fn with_partial_sync_opts_experimental(mut self, opts: PartialSyncOpts) -> Self {
        self.partial_sync_config_experimental = Some(opts);
        self
    }

    /// Set encryption key (base64-encoded) and cipher for the Turso Cloud database.
    /// The cipher is used to calculate the correct reserved_bytes for the database.
    pub fn with_remote_encryption(
        mut self,
        base64_key: impl Into<String>,
        cipher: RemoteEncryptionCipher,
    ) -> Self {
        self.remote_encryption_key = Some(base64_key.into());
        self.remote_encryption_cipher = Some(cipher);
        self
    }

    /// Set encryption key (base64-encoded) for the Turso Cloud database.
    /// The key will be sent as x-turso-encryption-key header with sync HTTP requests.
    /// Note: For deferred sync (no initial bootstrap), use with_remote_encryption() instead
    /// to also specify the cipher for correct reserved_bytes calculation.
    pub fn with_remote_encryption_key(mut self, base64_key: impl Into<String>) -> Self {
        self.remote_encryption_key = Some(base64_key.into());
        self
    }

    /// Override the sync protocol used for incremental pulls.
    ///
    /// By default the protocol is auto-detected from the first pull-updates
    /// response and persisted in the sync metadata, so calling this is only
    /// needed for tests or as an escape hatch: `true` forces MVCC logical-log
    /// pulls, `false` forces page-stream pulls.
    pub fn with_logical_mvcc_pull(mut self, enable: bool) -> Self {
        self.logical_mvcc_pull = Some(enable);
        self
    }

    /// Compose the `experimental_features` comma-separated string consumed by
    /// [`turso_sdk_kit::rsapi::TursoDatabaseConfig`] (and ultimately
    /// `turso_core::DatabaseOpts::with_experimental_feature`) from the boolean
    /// flags on this Builder. Returns `None` when no feature is enabled. The
    /// feature tokens must match the names parsed by the core.
    fn experimental_features_string(&self) -> Option<String> {
        let mut features: Vec<&str> = Vec::new();
        if self.enable_attach {
            features.push("attach");
        }
        if self.enable_custom_types {
            features.push("custom_types");
        }
        if self.enable_index_method {
            features.push("index_method");
        }
        if self.enable_materialized_views {
            features.push("views");
        }
        if self.enable_vacuum {
            features.push("vacuum");
        }
        if self.enable_generated_columns {
            features.push("generated_columns");
        }
        if self.enable_multiprocess_wal {
            features.push("multiprocess_wal");
        }
        if self.enable_without_rowid {
            features.push("without_rowid");
        }
        if features.is_empty() {
            None
        } else {
            Some(features.join(","))
        }
    }

    // Build the synced database object, initialize and open it.
    pub async fn build(self) -> Result<Database> {
        // Compose the experimental_features string from the boolean flags
        // exposed on this Builder.
        let experimental_features = self.experimental_features_string();

        // Build core database config for the embedded engine.
        let db_config = turso_sdk_kit::rsapi::TursoDatabaseConfig {
            path: self.path.clone(),
            experimental_features,
            // IMPORTANT: async IO must be turned on to delegate IO to this layer.
            async_io: true,
            encryption: None,
            vfs: IoBackend::Default,
            io: None,
            db_file: None,
            page_codec: None,
            open_flags: Default::default(),
        };

        let url = if let Some(remote_url) = &self.remote_url {
            Some(normalize_base_url(remote_url).map_err(Error::Error)?)
        } else {
            None
        };

        // Calculate reserved_bytes from cipher if provided.
        let reserved_bytes = self
            .remote_encryption_cipher
            .map(|cipher| cipher.reserved_bytes());

        // Build sync engine config.
        let sync_config = turso_sync_sdk_kit::rsapi::TursoDatabaseSyncConfig {
            path: self.path.clone(),
            remote_url: url.clone(),
            client_name: self
                .client_name
                .clone()
                .unwrap_or_else(|| DEFAULT_CLIENT_NAME.to_string()),
            long_poll_timeout_ms: self
                .long_poll_timeout
                .map(|d| d.as_millis().min(u32::MAX as u128) as u32),
            bootstrap_if_empty: self.bootstrap_if_empty,
            reserved_bytes,
            partial_sync_opts: self.partial_sync_config_experimental.clone(),
            remote_encryption_key: self.remote_encryption_key.clone(),
            push_operations_threshold: None,
            pull_bytes_threshold: None,
            logical_mvcc_pull: self.logical_mvcc_pull,
        };

        // Create sync wrapper.
        let sync =
            turso_sync_sdk_kit::rsapi::TursoDatabaseSync::<Bytes>::new(db_config, sync_config)
                .map_err(Error::from)?;

        // IO worker will process SyncEngine IO queue on a dedicated tokio thread.
        let io_worker = IoWorker::spawn(sync.clone(), url, self.auth_token.clone());

        // Create (bootstrap + open) database in one go.
        let op = sync.create();
        drive_operation(op, io_worker.clone()).await?;

        Ok(Database {
            sync,
            io: io_worker,
        })
    }
}

// Synced Database handle.
#[derive(Clone)]
pub struct Database {
    sync: Arc<turso_sync_sdk_kit::rsapi::TursoDatabaseSync<Bytes>>,
    io: Arc<IoWorker>,
}

impl Database {
    // Push local changes to the remote.
    pub async fn push(&self) -> Result<()> {
        let op = self.sync.push_changes();
        drive_operation(op, self.io.clone()).await?;
        Ok(())
    }

    // Pull remote changes; returns true if any changes were applied.
    pub async fn pull(&self) -> Result<bool> {
        // First, wait for changes...
        let op = self.sync.wait_changes();
        let result = drive_operation_result(op, self.io.clone()).await?;
        let mut has_changes = false;

        if let Some(
            turso_sync_sdk_kit::turso_async_operation::TursoAsyncOperationResult::Changes {
                changes,
            },
        ) = result
        {
            if !changes.empty() {
                has_changes = true;
                // Then, apply them.
                let op_apply = self.sync.apply_changes(changes);
                drive_operation(op_apply, self.io.clone()).await?;
            }
        }

        Ok(has_changes)
    }

    // Force WAL checkpoint for the main database.
    pub async fn checkpoint(&self) -> Result<()> {
        for attempt in 0..CHECKPOINT_BUSY_MAX_ATTEMPTS {
            let op = self.sync.checkpoint();
            let result = drive_operation(op, self.io.clone()).await;
            match result {
                Ok(()) => return Ok(()),
                Err(error)
                    if is_sync_busy_error(&error) && attempt + 1 < CHECKPOINT_BUSY_MAX_ATTEMPTS =>
                {
                    tokio::time::sleep(CHECKPOINT_BUSY_RETRY_DELAY).await;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    // Retrieve sync statistics for the database.
    pub async fn stats(&self) -> Result<DatabaseSyncStats> {
        let op = self.sync.stats();
        let result = drive_operation_result(op, self.io.clone()).await?;
        match result {
            Some(turso_sync_sdk_kit::turso_async_operation::TursoAsyncOperationResult::Stats {
                stats,
            }) => Ok(stats),
            _ => Err(Error::Misuse(
                "unexpected result type from stats operation".to_string(),
            )),
        }
    }

    // Create a SQL connection to the synced database.
    pub async fn connect(&self) -> Result<Connection> {
        let op = self.sync.connect();
        let result = drive_operation_result(op, self.io.clone()).await?;
        match result {
            Some(
                turso_sync_sdk_kit::turso_async_operation::TursoAsyncOperationResult::Connection {
                    connection,
                },
            ) => {
                // Provide extra_io callback to kick IO worker when driver needs to make progress.
                let io = self.io.clone();
                let extra_io = Arc::new(move |waker| {
                    io.register(waker);
                    io.kick();
                    Ok(())
                });
                Ok(Connection::create(connection, Some(extra_io)))
            }
            _ => Err(Error::Misuse(
                "unexpected result type from connect operation".to_string(),
            )),
        }
    }
}

// Drive an operation that has no result (returns None when done).
async fn drive_operation(
    op: Box<turso_sync_sdk_kit::turso_async_operation::TursoDatabaseAsyncOperation>,
    io: Arc<IoWorker>,
) -> Result<()> {
    let fut = AsyncOpFuture::new(op, io);
    fut.await.map(|_| ())
}

// Drive an operation and retrieve its result (if any).
async fn drive_operation_result(
    op: Box<turso_sync_sdk_kit::turso_async_operation::TursoDatabaseAsyncOperation>,
    io: Arc<IoWorker>,
) -> Result<Option<turso_sync_sdk_kit::turso_async_operation::TursoAsyncOperationResult>> {
    let fut = AsyncOpFuture::new(op, io);
    fut.await
}

fn is_sync_busy_error(error: &Error) -> bool {
    match error {
        Error::Busy(_) => true,
        Error::Error(message) => message.contains("Database is busy"),
        _ => false,
    }
}

// Custom Future that integrates with TursoDatabaseAsyncOperation and our IO worker.
struct AsyncOpFuture {
    op: Option<Box<turso_sync_sdk_kit::turso_async_operation::TursoDatabaseAsyncOperation>>,
    io: Arc<IoWorker>,
}

impl AsyncOpFuture {
    fn new(
        op: Box<turso_sync_sdk_kit::turso_async_operation::TursoDatabaseAsyncOperation>,
        io: Arc<IoWorker>,
    ) -> Self {
        Self { op: Some(op), io }
    }
}

impl Future for AsyncOpFuture {
    type Output =
        Result<Option<turso_sync_sdk_kit::turso_async_operation::TursoAsyncOperationResult>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let Some(op) = &this.op else {
            return Poll::Ready(Err(Error::Misuse(
                "operation future has been already completed".to_string(),
            )));
        };

        this.io.register(cx.waker().clone());

        // Try to resume the operation.
        match op.resume() {
            Ok(turso_sdk_kit::rsapi::TursoStatusCode::Done) => {
                // Try to take the result (may be None).
                let result = op.take_result().map(Some).or_else(|err| match err {
                    turso_sdk_kit::rsapi::TursoError::Misuse(msg)
                        if msg.contains("operation has no result") =>
                    {
                        Ok(None)
                    }
                    other => Err(Error::from(other)),
                })?;
                // Drop the op and complete.
                this.op.take();
                Poll::Ready(Ok(result))
            }
            Ok(turso_sdk_kit::rsapi::TursoStatusCode::Io) => {
                // Kick IO worker to process queued IO.
                this.io.kick();
                // Wait until IO worker makes progress and wakes us.
                Poll::Pending
            }
            Ok(turso_sdk_kit::rsapi::TursoStatusCode::Row) => {
                // Not expected from top-level sync operations.
                Poll::Ready(Err(Error::Misuse(
                    "unexpected row status in sync operation".to_string(),
                )))
            }
            Err(e) => Poll::Ready(Err(Error::from(e))),
        }
    }
}

// Normalize remote base URL, mapping libsql:// and turso:// to https:// and validating allowed
// schemes.
fn normalize_base_url(input: &str) -> std::result::Result<String, String> {
    let s = input.trim();
    let s = if let Some(rest) = s
        .strip_prefix("libsql://")
        .or_else(|| s.strip_prefix("turso://"))
    {
        format!("https://{rest}")
    } else {
        s.to_string()
    };
    // Accept http or https only
    if !(s.starts_with("https://") || s.starts_with("http://")) {
        return Err(format!("unsupported remote URL scheme: {input}"));
    }
    // Ensure no trailing slash to make join predictable.
    let base = s.trim_end_matches('/').to_string();
    Ok(base)
}

// Largest body frame we hand to hyper in one piece. Hyper turns each frame
// into a single IoSlice for vectored socket writes, and on Windows
// IoSlice::new panics for buffers larger than u32::MAX because WSABUF stores
// the length as a 32-bit integer. Keep frames far below that limit.
const MAX_BODY_FRAME_SIZE: usize = 4 * 1024 * 1024;

// Request body that yields its payload in frames of at most
// MAX_BODY_FRAME_SIZE bytes. Frames are zero-copy slices of the original
// buffer, so this adds no extra memory over sending the body whole.
struct ChunkedBody {
    rest: Bytes,
}

impl ChunkedBody {
    fn new(data: Bytes) -> Self {
        Self { rest: data }
    }
}

impl hyper::body::Body for ChunkedBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<hyper::body::Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        if this.rest.is_empty() {
            return Poll::Ready(None);
        }
        let len = this.rest.len().min(MAX_BODY_FRAME_SIZE);
        let chunk = this.rest.split_to(len);
        Poll::Ready(Some(Ok(hyper::body::Frame::data(chunk))))
    }

    fn is_end_stream(&self) -> bool {
        self.rest.is_empty()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        hyper::body::SizeHint::with_exact(self.rest.len() as u64)
    }
}

// The IO worker owns a dedicated Tokio runtime on a separate thread, and processes
// the SyncEngine IO queue (HTTP and atomic file operations).
struct IoWorker {
    // Channel to wake the worker to process IO.
    tx: mpsc::UnboundedSender<()>,
    // Wakers to notify pending futures when IO makes progress.
    wakers: Arc<Mutex<Vec<Waker>>>,
}

impl IoWorker {
    fn spawn(
        sync: Arc<turso_sync_sdk_kit::rsapi::TursoDatabaseSync<Bytes>>,
        base_url: Option<String>,
        auth_token: Option<AuthTokenFn>,
    ) -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel::<()>();
        let wakers = Arc::new(Mutex::new(Vec::new()));
        let weak_sync = Arc::downgrade(&sync);

        let worker = Arc::new(Self {
            tx,
            wakers: wakers.clone(),
        });

        // Keep the worker thread independent from the handle so dropping the
        // last Database releases the sync engine immediately on Windows.
        std::thread::Builder::new()
            .name("turso-sync-io".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build IO runtime");

                rt.block_on(async move {
                    IoWorker::run_loop(weak_sync, base_url, auth_token, rx, wakers).await
                });
            })
            .expect("failed to spawn IO worker thread");

        worker
    }

    // Register a waker to be awakened upon IO progress.
    fn register(&self, waker: Waker) {
        let mut wakers = self.wakers.lock().unwrap();
        wakers.push(waker);
    }

    // Kick the IO worker to process IO queue.
    fn kick(&self) {
        let _ = self.tx.send(());
    }

    // Called from the IO thread once progress has been made to notify all pending futures.
    fn notify_progress(wakers: &Mutex<Vec<Waker>>) {
        let wakers = {
            let mut guard = wakers.lock().unwrap();
            std::mem::take(&mut *guard)
        };
        for w in wakers {
            w.wake();
        }
    }

    async fn run_loop(
        sync: Weak<turso_sync_sdk_kit::rsapi::TursoDatabaseSync<Bytes>>,
        base_url: Option<String>,
        auth_token: Option<AuthTokenFn>,
        mut rx: mpsc::UnboundedReceiver<()>,
        wakers: Arc<Mutex<Vec<Waker>>>,
    ) {
        // Create HTTPS-capable Hyper client.
        let mut http_connector = HttpConnector::new();
        http_connector.enforce_http(false);
        let https: HttpsConnector<HttpConnector> = HttpsConnector::<HttpConnector>::builder()
            .with_native_roots()
            .expect("failed to load native root CA certificates")
            .https_or_http()
            .enable_http1()
            .build();
        let client: Client<HttpsConnector<HttpConnector>, ChunkedBody> =
            Client::builder(TokioExecutor::new()).build::<_, ChunkedBody>(https);

        while rx.recv().await.is_some() {
            let Some(sync) = sync.upgrade() else {
                break;
            };
            // Process all pending items in the sync IO queue.
            let mut made_progress = false;
            loop {
                let item = sync.take_io_item();
                let Some(item) = item else {
                    sync.step_io_callbacks();
                    IoWorker::notify_progress(&wakers);
                    break;
                };

                made_progress = true;

                // Take the request by value so large HTTP bodies move into
                // the outgoing request instead of being copied.
                let (request, completion) = item.into_parts();
                match request {
                    turso_sync_sdk_kit::sync_engine_io::SyncEngineIoRequest::Http {
                        url,
                        method,
                        path,
                        body,
                        headers,
                    } => {
                        IoWorker::process_http(
                            &sync,
                            base_url.as_deref(),
                            auth_token.as_ref(),
                            &wakers,
                            &client,
                            url.as_deref(),
                            &method,
                            &path,
                            body.map(Bytes::from),
                            &headers,
                            completion,
                        )
                        .await;
                    }
                    turso_sync_sdk_kit::sync_engine_io::SyncEngineIoRequest::FullRead { path } => {
                        IoWorker::process_full_read(&path, completion, &sync).await;
                    }
                    turso_sync_sdk_kit::sync_engine_io::SyncEngineIoRequest::FullWrite {
                        path,
                        content,
                    } => {
                        IoWorker::process_full_write(&path, &content, completion, &sync).await;
                    }
                }
            }

            // Run queued IO callbacks and wake all pending ops, yielding control
            // to allow them to make progress before we loop again.
            if made_progress {
                sync.step_io_callbacks();
                IoWorker::notify_progress(&wakers);
                // Let waiting tasks run on their executors.
                tokio::task::yield_now().await;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_http(
        sync: &turso_sync_sdk_kit::rsapi::TursoDatabaseSync<Bytes>,
        base_url: Option<&str>,
        auth_token: Option<&AuthTokenFn>,
        wakers: &Mutex<Vec<Waker>>,
        client: &Client<HttpsConnector<HttpConnector>, ChunkedBody>,
        url: Option<&str>,
        method: &str,
        path: &str,
        body: Option<Bytes>,
        headers: &[(String, String)],
        completion: turso_sync_sdk_kit::sync_engine_io::SyncEngineIoCompletion<Bytes>,
    ) {
        // Build full URL.
        let full_url = if path.starts_with("http://") || path.starts_with("https://") {
            path.to_string()
        } else {
            // Ensure the path begins with '/'
            let p = if path.starts_with('/') {
                path.to_string()
            } else {
                format!("/{path}")
            };
            let Some(url) = base_url.or(url) else {
                completion.poison("remote_url is not available".to_string());
                return;
            };
            format!("{url}{p}")
        };

        // Resolve auth token (may fail if a dynamic provider returns an error).
        // Resolved here rather than once at spawn so dynamic providers can rotate
        // the token between requests.
        let auth_token = match auth_token {
            Some(provider) => match provider().await {
                Ok(token) => Some(token),
                Err(err) => {
                    completion.poison(format!("failed to resolve auth token: {err}"));
                    sync.step_io_callbacks();
                    return;
                }
            },
            None => None,
        };

        let mut builder = Request::builder().method(method).uri(&full_url);

        // Set headers from request
        if let Some(headers_map) = builder.headers_mut() {
            for (k, v) in headers {
                if let Ok(name) = hyper::header::HeaderName::try_from(k.as_str()) {
                    if let Ok(value) = hyper::header::HeaderValue::try_from(v.as_str()) {
                        headers_map.insert(name, value);
                    }
                }
            }
            // Add Authorization header if not already set
            if let Some(token) = &auth_token {
                if !headers_map.contains_key(AUTHORIZATION) {
                    let value = format!("Bearer {token}");
                    if let Ok(hv) = hyper::header::HeaderValue::try_from(value.as_str()) {
                        headers_map.insert(AUTHORIZATION, hv);
                    }
                }
            }
        }

        let req_body = ChunkedBody::new(body.unwrap_or_default());

        let request = match builder.body(req_body) {
            Ok(r) => r,
            Err(err) => {
                completion.poison(format!("failed to build request: {err}"));
                sync.step_io_callbacks();
                return;
            }
        };

        let mut response = match client.request(request).await {
            Ok(r) => r,
            Err(err) => {
                completion.poison(format!("http request failed: {err}"));
                sync.step_io_callbacks();
                return;
            }
        };

        // Propagate status
        let status = response.status().as_u16();
        completion.status(status as u32);
        sync.step_io_callbacks();
        IoWorker::notify_progress(wakers);

        // Stream response body in chunks
        while let Some(frame_res) = response.body_mut().frame().await {
            match frame_res {
                Ok(frame) => {
                    if let Some(chunk) = frame.data_ref() {
                        completion.push_buffer(chunk.clone());
                        sync.step_io_callbacks();
                        IoWorker::notify_progress(wakers);
                    }
                }
                Err(err) => {
                    completion.poison(format!("error reading response body: {err}"));
                    sync.step_io_callbacks();
                    IoWorker::notify_progress(wakers);
                    return;
                }
            }
        }

        // Done streaming
        completion.done();
        sync.step_io_callbacks();
        IoWorker::notify_progress(wakers);
    }

    async fn process_full_read(
        path: &str,
        completion: turso_sync_sdk_kit::sync_engine_io::SyncEngineIoCompletion<Bytes>,
        sync: &turso_sync_sdk_kit::rsapi::TursoDatabaseSync<Bytes>,
    ) {
        match tokio::fs::read(path).await {
            Ok(content) => {
                completion.push_buffer(Bytes::from(content));
                completion.done();
            }
            Err(err) if err.kind() == ErrorKind::NotFound => completion.done(),
            Err(err) => {
                completion.poison(format!("full read failed for {path}: {err}"));
            }
        }
        // Step callbacks after progress.
        sync.step_io_callbacks();
    }

    async fn process_full_write(
        path: &str,
        content: &Vec<u8>,
        completion: turso_sync_sdk_kit::sync_engine_io::SyncEngineIoCompletion<Bytes>,
        sync: &turso_sync_sdk_kit::rsapi::TursoDatabaseSync<Bytes>,
    ) {
        // Write the whole content in one go (non-chunked)
        match tokio::fs::write(path, content).await {
            Ok(_) => {
                // For full write there is no data to stream back; just finish.
                completion.done();
            }
            Err(err) => {
                completion.poison(format!("full write failed for {path}: {err}"));
            }
        }
        // Step callbacks after progress.
        sync.step_io_callbacks();
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{anyhow, Context, Result};
    use rand::{distr::Alphanumeric, Rng};
    use reqwest::Client;
    use serde_json::json;
    use std::{
        env,
        process::{Child, Command, Stdio},
        thread::sleep,
        time::{Duration, Instant},
    };
    use tempfile::TempDir;
    use turso_sync_sdk_kit::rsapi::PartialBootstrapStrategy;

    use crate::sync::PartialSyncOpts;
    use crate::{Rows, Value};

    const ADMIN_URL: &str = "http://localhost:8081";
    const USER_URL: &str = "http://localhost:8080";

    fn random_str() -> String {
        rand::rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect()
    }

    // Regression test for a Windows panic: hyper turns each body frame into
    // one IoSlice, and IoSlice::new panics on Windows for buffers larger than
    // u32::MAX. The request body must therefore never yield a frame that big.
    #[test]
    fn http_body_never_yields_a_frame_larger_than_the_frame_limit() {
        use std::pin::Pin;
        use std::task::{Context, Poll, Waker};

        use hyper::body::Body;

        use crate::sync::{ChunkedBody, MAX_BODY_FRAME_SIZE};

        let payload = bytes::Bytes::from(vec![7u8; MAX_BODY_FRAME_SIZE * 2 + 123]);
        let mut body = ChunkedBody::new(payload.clone());
        let mut cx = Context::from_waker(Waker::noop());
        let mut collected = Vec::with_capacity(payload.len());
        loop {
            match Pin::new(&mut body).poll_frame(&mut cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    let data = frame.into_data().expect("body yields only data frames");
                    assert!(data.len() <= MAX_BODY_FRAME_SIZE);
                    collected.extend_from_slice(&data);
                }
                Poll::Ready(None) => break,
                Poll::Ready(Some(Err(err))) => match err {},
                Poll::Pending => panic!("in-memory body must never be pending"),
            }
        }
        assert_eq!(collected, payload);
        assert!(body.is_end_stream());
    }

    #[test]
    fn normalize_base_url_schemes() {
        use crate::sync::normalize_base_url;

        assert_eq!(
            normalize_base_url("libsql://db.turso.io").unwrap(),
            "https://db.turso.io"
        );
        assert_eq!(
            normalize_base_url("turso://db.turso.io").unwrap(),
            "https://db.turso.io"
        );
        assert_eq!(
            normalize_base_url("https://db.turso.io/").unwrap(),
            "https://db.turso.io"
        );
        assert_eq!(
            normalize_base_url("http://localhost:8080").unwrap(),
            "http://localhost:8080"
        );
        assert!(normalize_base_url("ftp://db.turso.io").is_err());
    }

    #[test]
    fn experimental_features_string_composition() {
        use crate::sync::Builder;

        // No features enabled -> no experimental_features string at all.
        assert_eq!(
            Builder::new_remote(":memory:").experimental_features_string(),
            None
        );

        // A single feature.
        assert_eq!(
            Builder::new_remote(":memory:")
                .experimental_index_method(true)
                .experimental_features_string()
                .as_deref(),
            Some("index_method")
        );

        // Multiple features are emitted in a stable, comma-separated order
        // using the exact tokens the core parser understands.
        assert_eq!(
            Builder::new_remote(":memory:")
                .experimental_attach(true)
                .experimental_custom_types(true)
                .experimental_index_method(true)
                .experimental_materialized_views(true)
                .experimental_vacuum(true)
                .experimental_generated_columns(true)
                .experimental_multiprocess_wal(true)
                .experimental_without_rowid(true)
                .experimental_features_string()
                .as_deref(),
            Some(
                "attach,custom_types,index_method,views,vacuum,generated_columns,multiprocess_wal,without_rowid"
            )
        );
    }

    #[test]
    fn logical_mvcc_pull_defaults_to_auto_detection() {
        use crate::sync::Builder;

        assert_eq!(Builder::new_remote(":memory:").logical_mvcc_pull, None);
        assert_eq!(
            Builder::new_remote(":memory:")
                .with_logical_mvcc_pull(true)
                .logical_mvcc_pull,
            Some(true)
        );
        assert_eq!(
            Builder::new_remote(":memory:")
                .with_logical_mvcc_pull(false)
                .logical_mvcc_pull,
            Some(false)
        );
    }

    async fn handle_response(resp: reqwest::Response) -> Result<()> {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();

        if status == 400 && text.contains("already exists") {
            return Ok(());
        }

        if !status.is_success() {
            return Err(anyhow!("request failed: {status} {text}"));
        }

        Ok(())
    }

    pub struct TursoServer {
        user_url: String,
        db_url: String,
        host: String,
        db_prefix: String,
        server: Option<Child>,
        // Keeps the directory alive while the spawned server may still write to it.
        _sync_dir: Option<TempDir>,
        client: Client,
    }

    impl TursoServer {
        pub async fn new() -> Result<Self> {
            let client = Client::new();

            if env::var("LOCAL_SYNC_SERVER").is_err() {
                let name = random_str();
                let tokens: Vec<&str> = USER_URL.split("://").collect();

                handle_response(
                    client
                        .post(format!("{ADMIN_URL}/v1/tenants/{name}"))
                        .send()
                        .await?,
                )
                .await?;
                handle_response(
                    client
                        .post(format!("{ADMIN_URL}/v1/tenants/{name}/groups/{name}"))
                        .send()
                        .await?,
                )
                .await?;
                handle_response(
                    client
                        .post(format!(
                            "{ADMIN_URL}/v1/tenants/{name}/groups/{name}/databases/{name}"
                        ))
                        .send()
                        .await?,
                )
                .await?;

                Ok(Self {
                    user_url: USER_URL.to_string(),
                    db_url: format!("{}://{}--{}--{}.{}", tokens[0], name, name, name, tokens[1]),
                    host: format!("{name}--{name}--{name}.localhost"),
                    db_prefix: String::new(),
                    server: None,
                    _sync_dir: None,
                    client,
                })
            } else {
                let server_bin = env::var("LOCAL_SYNC_SERVER").unwrap();

                let sync_dir = env::var("LOCAL_SYNC_SERVER_DIR")
                    .is_ok()
                    .then(|| TempDir::new().context("failed to create --sync-dir tempdir"))
                    .transpose()?;

                // The random port can be unusable: Windows runners reserve
                // large chunks of 10_000..=65_535 for Hyper-V (bind fails with
                // WSAEACCES), and concurrently running tests can collide on
                // the same port. In both cases the server child exits right
                // away, so waiting for readiness without watching the child
                // hangs the test forever. Detect child exit and retry with a
                // fresh port instead.
                const SPAWN_ATTEMPTS: u32 = 10;
                const READY_TIMEOUT: Duration = Duration::from_secs(60);
                for attempt in 1..=SPAWN_ATTEMPTS {
                    let port: u16 = rand::rng().random_range(10_000..=65_535);

                    let mut args = vec!["--sync-server".to_string(), format!("0.0.0.0:{port}")];
                    if let Some(dir) = &sync_dir {
                        args.push("--sync-dir".to_string());
                        args.push(dir.path().to_string_lossy().into_owned());
                    }

                    // IMPORTANT: do not use Stdio::piped() here. Nothing reads from
                    // those pipes, so once the kernel pipe buffer (~64 KiB on Linux)
                    // fills, the child blocks forever inside write() and stops
                    // servicing HTTP requests, deadlocking sync operations in
                    // long-running tests like test_sync_parallel_writes_with_sync_ops.
                    let mut child = Command::new(&server_bin)
                        .args(&args)
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .spawn()
                        .context("failed to spawn local sync server")?;

                    let user_url = format!("http://localhost:{port}");

                    // wait for server readiness
                    let started = Instant::now();
                    loop {
                        if client.get(&user_url).send().await.is_ok() {
                            let db_prefix = if sync_dir.is_some() {
                                format!("/db/{}", random_str())
                            } else {
                                String::new()
                            };
                            let db_url = format!("{user_url}{db_prefix}");
                            return Ok(Self {
                                user_url,
                                db_url,
                                host: String::new(),
                                db_prefix,
                                server: Some(child),
                                _sync_dir: sync_dir,
                                client,
                            });
                        }
                        match child
                            .try_wait()
                            .context("failed to poll local sync server")?
                        {
                            Some(status) => {
                                // Child exited (most likely the port was
                                // reserved or already taken): retry.
                                eprintln!(
                                    "local sync server on port {port} exited with {status} \
                                     before becoming ready (attempt {attempt}/{SPAWN_ATTEMPTS})"
                                );
                                break;
                            }
                            None => {
                                if started.elapsed() > READY_TIMEOUT {
                                    let _ = child.kill();
                                    let _ = child.wait();
                                    return Err(anyhow!(
                                        "local sync server on port {port} did not become ready \
                                         within {READY_TIMEOUT:?}"
                                    ));
                                }
                            }
                        }
                        sleep(Duration::from_millis(100));
                    }
                }
                Err(anyhow!(
                    "local sync server failed to start after {SPAWN_ATTEMPTS} attempts"
                ))
            }
        }

        pub fn db_url(&self) -> &str {
            &self.db_url
        }

        pub async fn db_sql(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
            let resp = self
                .client
                .post(format!("{}{}/v2/pipeline", self.user_url, self.db_prefix))
                .header("Host", &self.host)
                .json(&json!({
                    "requests": [{
                        "type": "execute",
                        "stmt": { "sql": sql }
                    }]
                }))
                .send()
                .await?
                .error_for_status()?;

            let value: serde_json::Value = resp.json().await?;

            let result = &value["results"][0];
            if result["type"] != "ok" {
                return Err(anyhow!("remote sql execution failed: {value}"));
            }

            let rows = result["response"]["result"]["rows"]
                .as_array()
                .ok_or_else(|| anyhow!("invalid response shape"))?;

            Ok(rows
                .iter()
                .map(|row| {
                    row.as_array()
                        .unwrap()
                        .iter()
                        .map(|cell| match cell["value"].clone() {
                            serde_json::Value::Null => Value::Null,
                            serde_json::Value::Number(number) => {
                                if number.is_i64() {
                                    Value::Integer(number.as_i64().unwrap())
                                } else {
                                    Value::Real(number.as_f64().unwrap())
                                }
                            }
                            serde_json::Value::String(s) => Value::Text(s),
                            _ => panic!("unexpected json output"),
                        })
                        .collect()
                })
                .collect())
        }
    }

    impl Drop for TursoServer {
        fn drop(&mut self) {
            if let Some(child) = &mut self.server {
                let _ = child.kill();
            }
        }
    }

    async fn all_rows(mut rows: Rows) -> Result<Vec<Vec<Value>>> {
        let mut result = Vec::new();
        while let Some(row) = rows.next().await? {
            result.push(row.values.into_iter().map(|x| x.into()).collect());
        }
        Ok(result)
    }

    #[tokio::test]
    pub async fn test_sync_bootstrap() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();
        server.db_sql("CREATE TABLE t(x)").await.unwrap();
        server
            .db_sql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync')")
            .await
            .unwrap();
        server.db_sql("SELECT * FROM t").await.unwrap();
        let db = crate::sync::Builder::new_remote(":memory:")
            .with_remote_url(server.db_url())
            .build()
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();
        let rows = conn.query("SELECT * FROM t", ()).await.unwrap();
        let all = all_rows(rows).await.unwrap();
        assert_eq!(
            all,
            vec![
                vec![Value::Text("hello".to_string())],
                vec![Value::Text("turso".to_string())],
                vec![Value::Text("sync".to_string())],
            ]
        );
    }

    #[tokio::test]
    pub async fn test_sync_bootstrap_persistence() {
        let _ = tracing_subscriber::fmt::try_init();
        let dir = TempDir::new().unwrap();
        let server = TursoServer::new().await.unwrap();
        server.db_sql("CREATE TABLE t(x)").await.unwrap();
        server
            .db_sql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync')")
            .await
            .unwrap();
        server.db_sql("SELECT * FROM t").await.unwrap();
        let db = crate::sync::Builder::new_remote(dir.path().join("local.db").to_str().unwrap())
            .with_remote_url(server.db_url())
            .build()
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();
        let rows = conn.query("SELECT * FROM t", ()).await.unwrap();
        let all = all_rows(rows).await.unwrap();
        assert_eq!(
            all,
            vec![
                vec![Value::Text("hello".to_string())],
                vec![Value::Text("turso".to_string())],
                vec![Value::Text("sync".to_string())],
            ]
        );
    }

    #[tokio::test]
    pub async fn test_sync_config_persistence() {
        let _ = tracing_subscriber::fmt::try_init();
        let dir = TempDir::new().unwrap();
        let server = TursoServer::new().await.unwrap();
        server.db_sql("CREATE TABLE t(x)").await.unwrap();
        server.db_sql("INSERT INTO t VALUES (42)").await.unwrap();
        {
            let db1 =
                crate::sync::Builder::new_remote(dir.path().join("local.db").to_str().unwrap())
                    .with_remote_url(server.db_url())
                    .build()
                    .await
                    .unwrap();
            let conn = db1.connect().await.unwrap();
            let rows = conn.query("SELECT * FROM t", ()).await.unwrap();
            let all = all_rows(rows).await.unwrap();
            assert_eq!(all, vec![vec![Value::Integer(42)],]);
        }
        server.db_sql("INSERT INTO t VALUES (41)").await.unwrap();
        {
            let db2 =
                crate::sync::Builder::new_remote(dir.path().join("local.db").to_str().unwrap())
                    .build()
                    .await
                    .unwrap();
            db2.pull().await.unwrap();
            let conn = db2.connect().await.unwrap();
            let rows = conn.query("SELECT * FROM t", ()).await.unwrap();
            let all = all_rows(rows).await.unwrap();
            assert_eq!(
                all,
                vec![vec![Value::Integer(42)], vec![Value::Integer(41)],]
            );
        }
    }

    #[tokio::test]
    pub async fn test_sync_pull() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();
        server.db_sql("CREATE TABLE t(x)").await.unwrap();
        server
            .db_sql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync')")
            .await
            .unwrap();
        server.db_sql("SELECT * FROM t").await.unwrap();
        let db = crate::sync::Builder::new_remote(":memory:")
            .with_remote_url(server.db_url())
            .build()
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();
        let rows = conn.query("SELECT * FROM t", ()).await.unwrap();
        let all = all_rows(rows).await.unwrap();
        assert_eq!(
            all,
            vec![
                vec![Value::Text("hello".to_string())],
                vec![Value::Text("turso".to_string())],
                vec![Value::Text("sync".to_string())],
            ]
        );

        server
            .db_sql("INSERT INTO t VALUES ('pull works')")
            .await
            .unwrap();

        let rows = conn.query("SELECT * FROM t", ()).await.unwrap();
        let all = all_rows(rows).await.unwrap();
        assert_eq!(
            all,
            vec![
                vec![Value::Text("hello".to_string())],
                vec![Value::Text("turso".to_string())],
                vec![Value::Text("sync".to_string())],
            ]
        );

        db.pull().await.unwrap();

        let rows = conn.query("SELECT * FROM t", ()).await.unwrap();
        let all = all_rows(rows).await.unwrap();
        assert_eq!(
            all,
            vec![
                vec![Value::Text("hello".to_string())],
                vec![Value::Text("turso".to_string())],
                vec![Value::Text("sync".to_string())],
                vec![Value::Text("pull works".to_string())],
            ]
        );
    }

    #[tokio::test]
    pub async fn test_sync_pull_no_changes_updates_last_pull_unix_time() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();
        server.db_sql("CREATE TABLE t(x)").await.unwrap();
        server.db_sql("INSERT INTO t VALUES (1)").await.unwrap();

        let db = crate::sync::Builder::new_remote(":memory:")
            .with_remote_url(server.db_url())
            .build()
            .await
            .unwrap();

        let before = db.stats().await.unwrap().last_pull_unix_time.unwrap();

        // unix time has 1s resolution - wait long enough for the timestamp to advance
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // remote has no new changes since bootstrap - pull is a no-op
        assert!(!db.pull().await.unwrap());

        let after = db.stats().await.unwrap().last_pull_unix_time.unwrap();
        assert!(
            after > before,
            "last_pull_unix_time must advance after a no-op pull: before={before}, after={after}"
        );
    }

    #[tokio::test]
    pub async fn test_sync_push() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();
        server.db_sql("CREATE TABLE t(x)").await.unwrap();
        server
            .db_sql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync')")
            .await
            .unwrap();
        server.db_sql("SELECT * FROM t").await.unwrap();
        let db = crate::sync::Builder::new_remote(":memory:")
            .with_remote_url(server.db_url())
            .build()
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();
        let rows = conn.query("SELECT * FROM t", ()).await.unwrap();
        let all = all_rows(rows).await.unwrap();
        assert_eq!(
            all,
            vec![
                vec![Value::Text("hello".to_string())],
                vec![Value::Text("turso".to_string())],
                vec![Value::Text("sync".to_string())],
            ]
        );

        conn.execute("INSERT INTO t VALUES ('push works')", ())
            .await
            .unwrap();

        let all = server.db_sql("SELECT * FROM t").await.unwrap();
        assert_eq!(
            all,
            vec![
                vec![Value::Text("hello".to_string())],
                vec![Value::Text("turso".to_string())],
                vec![Value::Text("sync".to_string())],
            ]
        );

        db.push().await.unwrap();

        let rows = conn.query("SELECT * FROM t", ()).await.unwrap();
        let all = all_rows(rows).await.unwrap();
        assert_eq!(
            all,
            vec![
                vec![Value::Text("hello".to_string())],
                vec![Value::Text("turso".to_string())],
                vec![Value::Text("sync".to_string())],
                vec![Value::Text("push works".to_string())],
            ]
        );
    }

    #[tokio::test]
    pub async fn test_sync_checkpoint() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();
        let db = crate::sync::Builder::new_remote(":memory:")
            .with_remote_url(server.db_url())
            .build()
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();
        conn.execute("CREATE TABLE t(x)", ()).await.unwrap();
        for i in 0..1024 {
            conn.execute("INSERT INTO t VALUES (?)", (i,))
                .await
                .unwrap();
        }

        let stats1 = db.stats().await.unwrap();
        assert!(stats1.main_wal_size > 1024 * 1024);
        db.checkpoint().await.unwrap();
        let stats2 = db.stats().await.unwrap();
        assert!(stats2.main_wal_size < 8 * 1024);
    }

    #[tokio::test]
    pub async fn test_sync_partial() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();
        server.db_sql("CREATE TABLE t(x)").await.unwrap();
        server
            .db_sql("INSERT INTO t SELECT randomblob(1024) FROM generate_series(1, 2000)")
            .await
            .unwrap();
        {
            let full_db = crate::sync::Builder::new_remote(":memory:")
                .with_remote_url(server.db_url())
                .build()
                .await
                .unwrap();
            let conn = full_db.connect().await.unwrap();
            let _ = all_rows(
                conn.query("SELECT LENGTH(x) FROM t LIMIT 1", ())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            assert!(full_db.stats().await.unwrap().network_received_bytes > 2000 * 1024);
        }
        {
            let partial_db = crate::sync::Builder::new_remote(":memory:")
                .with_remote_url(server.db_url())
                .with_partial_sync_opts_experimental(PartialSyncOpts {
                    bootstrap_strategy: Some(PartialBootstrapStrategy::Prefix {
                        length: 128 * 1024,
                    }),
                    segment_size: 128 * 1024,
                    prefetch: false,
                })
                .build()
                .await
                .unwrap();
            let conn = partial_db.connect().await.unwrap();
            let _ = all_rows(
                conn.query("SELECT LENGTH(x) FROM t LIMIT 1", ())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            assert!(partial_db.stats().await.unwrap().network_received_bytes < 256 * (1024 + 10));
            let before = tokio::time::Instant::now();
            let all = all_rows(
                conn.query("SELECT SUM(LENGTH(x)) FROM t", ())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            println!(
                "duration: {:?}",
                tokio::time::Instant::now().duration_since(before)
            );
            assert_eq!(all, vec![vec![Value::Integer(2000 * 1024)]]);
            assert!(partial_db.stats().await.unwrap().network_received_bytes > 2000 * 1024);
        }
    }

    #[tokio::test]
    pub async fn test_sync_partial_segment_size() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();
        server.db_sql("CREATE TABLE t(x)").await.unwrap();
        server
            .db_sql("INSERT INTO t SELECT randomblob(1024) FROM generate_series(1, 256)")
            .await
            .unwrap();
        {
            let full_db = crate::sync::Builder::new_remote(":memory:")
                .with_remote_url(server.db_url())
                .build()
                .await
                .unwrap();
            let conn = full_db.connect().await.unwrap();
            let _ = all_rows(
                conn.query("SELECT LENGTH(x) FROM t LIMIT 1", ())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            assert!(full_db.stats().await.unwrap().network_received_bytes > 256 * 1024);
        }
        {
            let partial_db = crate::sync::Builder::new_remote(":memory:")
                .with_remote_url(server.db_url())
                .with_partial_sync_opts_experimental(PartialSyncOpts {
                    bootstrap_strategy: Some(PartialBootstrapStrategy::Prefix {
                        length: 128 * 1024,
                    }),
                    segment_size: 4 * 1024,
                    prefetch: false,
                })
                .build()
                .await
                .unwrap();
            let conn = partial_db.connect().await.unwrap();
            let _ = all_rows(
                conn.query("SELECT LENGTH(x) FROM t LIMIT 1", ())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            assert!(partial_db.stats().await.unwrap().network_received_bytes < 128 * 1024 * 3 / 2);
            let before = tokio::time::Instant::now();
            let all = all_rows(
                conn.query("SELECT SUM(LENGTH(x)) FROM t", ())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            println!(
                "duration segment size: {:?}",
                tokio::time::Instant::now().duration_since(before)
            );
            assert_eq!(all, vec![vec![Value::Integer(256 * 1024)]]);
            assert!(partial_db.stats().await.unwrap().network_received_bytes > 256 * 1024);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_sync_partial_prefetch() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();
        server.db_sql("CREATE TABLE t(x)").await.unwrap();
        server
            .db_sql("INSERT INTO t SELECT randomblob(1024) FROM generate_series(1, 2000)")
            .await
            .unwrap();
        {
            let full_db = crate::sync::Builder::new_remote(":memory:")
                .with_remote_url(server.db_url())
                .build()
                .await
                .unwrap();
            let conn = full_db.connect().await.unwrap();
            let _ = all_rows(
                conn.query("SELECT LENGTH(x) FROM t LIMIT 1", ())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            assert!(full_db.stats().await.unwrap().network_received_bytes > 2000 * 1024);
        }
        {
            let partial_db = crate::sync::Builder::new_remote(":memory:")
                .with_remote_url(server.db_url())
                .with_partial_sync_opts_experimental(PartialSyncOpts {
                    bootstrap_strategy: Some(PartialBootstrapStrategy::Prefix {
                        length: 128 * 1024,
                    }),
                    segment_size: 128 * 1024,
                    prefetch: true,
                })
                .build()
                .await
                .unwrap();
            let conn = partial_db.connect().await.unwrap();
            let _ = all_rows(
                conn.query("SELECT LENGTH(x) FROM t LIMIT 1", ())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            assert!(partial_db.stats().await.unwrap().network_received_bytes < 1300 * (1024 + 10));
            let before = tokio::time::Instant::now();
            let all = all_rows(
                conn.query("SELECT SUM(LENGTH(x)) FROM t", ())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            println!(
                "duration prefetch: {:?}",
                tokio::time::Instant::now().duration_since(before)
            );
            assert_eq!(all, vec![vec![Value::Integer(2000 * 1024)]]);
            assert!(partial_db.stats().await.unwrap().network_received_bytes > 2000 * 1024);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "flaky, see https://github.com/tursodatabase/turso/issues/7087"]
    pub async fn test_sync_parallel_writes_with_sync_ops() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use tokio::sync::Mutex as TokioMutex;

        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();

        let db = crate::sync::Builder::new_remote(":memory:")
            .with_remote_url(server.db_url())
            .build()
            .await
            .unwrap();

        let conn = db.connect().await.unwrap();
        conn.execute(
            "CREATE TABLE test_data (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL)",
            (),
        )
        .await
        .unwrap();

        // ~200KB payload per row
        let payload = "X".repeat(200 * 1024);

        let done = Arc::new(AtomicBool::new(false));
        let sync_lock = Arc::new(TokioMutex::new(()));

        // Spawn periodic push/pull/checkpoint task (sequential, guarded by sync_lock)
        let sync_db = db.clone();
        let sync_done = done.clone();
        let sync_lock_clone = sync_lock.clone();
        let sync_task = tokio::spawn(async move {
            let mut cycle = 0u32;
            while !sync_done.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _guard = sync_lock_clone.lock().await;
                eprintln!("sync cycle {cycle}: push");
                if let Err(e) = sync_db.push().await {
                    eprintln!("push error (cycle {cycle}): {e}");
                }
                eprintln!("sync cycle {cycle}: pull");
                if let Err(e) = sync_db.pull().await {
                    eprintln!("pull error (cycle {cycle}): {e}");
                }
                eprintln!("sync cycle {cycle}: checkpoint");
                if let Err(e) = sync_db.checkpoint().await {
                    eprintln!("checkpoint error (cycle {cycle}): {e}");
                }
                cycle += 1;
            }
            cycle
        });

        // Parallel writes: 4 connections, each inserting 5 rows (~200KB each)
        let mut write_handles = Vec::new();
        let mut connections = Vec::new();
        let (conn_cnt, iterations_cnt, after_cnt) = (8u32, 100u32, 100u32);
        for _ in 0..conn_cnt {
            let db = db.clone();
            let conn = db.connect().await.unwrap();
            conn.execute("PRAGMA busy_timeout=5000", ()).await.unwrap();
            connections.push(Some((db, conn)));
        }
        for conn_id in 0..conn_cnt {
            let (_, conn) = connections[conn_id as usize].take().unwrap();
            let payload = payload.clone();
            write_handles.push(tokio::spawn(async move {
                for row_id in 0..iterations_cnt {
                    let tag = format!("conn{conn_id}_row{row_id}");
                    let data = format!("{tag}_{payload}");
                    loop {
                        match conn
                            .execute(
                                "INSERT INTO test_data (payload) VALUES (?)",
                                crate::params::Params::Positional(vec![Value::Text(data.clone())]),
                            )
                            .await
                        {
                            Ok(_) => break,
                            Err(crate::Error::Busy(_)) => {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                                continue;
                            }
                            Err(e) => panic!("insert failed (conn{conn_id}, row{row_id}): {e:?}"),
                        }
                    }
                }
            }));
        }
        for h in write_handles {
            h.await.unwrap();
        }

        // Sequential writes: 3 more large inserts
        for i in 0..after_cnt {
            let data = format!("sequential_{i}_{payload}");
            conn.execute(
                "INSERT INTO test_data (payload) VALUES (?)",
                crate::params::Params::Positional(vec![Value::Text(data)]),
            )
            .await
            .unwrap();
        }

        // Signal sync task to stop and wait for it
        done.store(true, Ordering::Relaxed);
        let sync_cycles = sync_task.await.unwrap();
        eprintln!("completed {sync_cycles} sync cycles during writes");

        let rows = conn
            .query("SELECT count(*) FROM test_data", ())
            .await
            .unwrap();
        let all = all_rows(rows).await.unwrap();
        assert_eq!(
            all,
            vec![vec![Value::Integer(
                (after_cnt + conn_cnt * iterations_cnt) as i64
            )]]
        );

        // Report WAL size via stats
        let stats = db.stats().await.unwrap();
        eprintln!(
            "WAL size after all writes: {} bytes ({:.2} KB)",
            stats.main_wal_size,
            stats.main_wal_size as f64 / 1024.0
        );
    }

    /// Reproducer for schema-divergence during sync.
    ///
    /// 1. Bootstrap a local client from the remote (table `t` with some rows).
    /// 2. Push + pull so both sides are even.
    /// 3. Locally: insert more rows, CREATE two new tables, insert into all three tables.
    /// 4. Add data on the remote side (simulating another client).
    /// 5. Push the local changes so the remote has the new schema too.
    /// 6. Pull into the local client – this must succeed despite the schema having changed.
    #[tokio::test]
    pub async fn test_sync_pull_after_local_ddl_and_remote_writes() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();

        server
            .db_sql("CREATE TABLE t(x TEXT PRIMARY KEY, y)")
            .await
            .unwrap();
        server
            .db_sql("INSERT INTO t VALUES ('a', '1'), ('b', '2'), ('c', '3')")
            .await
            .unwrap();

        let db = crate::sync::Builder::new_remote(":memory:")
            .with_remote_url(server.db_url())
            .build()
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();

        let rows = all_rows(conn.query("SELECT * FROM t", ()).await.unwrap())
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);

        db.push().await.unwrap();
        db.pull().await.unwrap();

        conn.execute(
            "INSERT INTO t VALUES ('d', '4-local'), ('e', '5-local')",
            (),
        )
        .await
        .unwrap();

        conn.execute("CREATE TABLE t2(y INTEGER, z TEXT)", ())
            .await
            .unwrap();
        conn.execute("CREATE TABLE t3(id INTEGER PRIMARY KEY, payload TEXT)", ())
            .await
            .unwrap();
        conn.execute("INSERT INTO t2 VALUES (1, 'hello'), (2, 'world')", ())
            .await
            .unwrap();
        conn.execute(
            "INSERT INTO t3 VALUES (100, 'payload1'), (200, 'payload2')",
            (),
        )
        .await
        .unwrap();

        server
            .db_sql("INSERT INTO t VALUES ('e', '5-remote'), ('f', '6-remote')")
            .await
            .unwrap();

        db.pull().await.unwrap();

        let rows_t = all_rows(
            conn.query("SELECT x, y FROM t ORDER BY x", ())
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            rows_t,
            vec![
                vec![Value::Text("a".to_string()), Value::Text("1".to_string())],
                vec![Value::Text("b".to_string()), Value::Text("2".to_string())],
                vec![Value::Text("c".to_string()), Value::Text("3".to_string())],
                vec![
                    Value::Text("d".to_string()),
                    Value::Text("4-local".to_string())
                ],
                vec![
                    Value::Text("e".to_string()),
                    Value::Text("5-local".to_string())
                ],
                vec![
                    Value::Text("f".to_string()),
                    Value::Text("6-remote".to_string())
                ],
            ]
        );

        let rows_t2 = all_rows(
            conn.query("SELECT y, z FROM t2 ORDER BY y", ())
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            rows_t2,
            vec![
                vec![Value::Integer(1), Value::Text("hello".to_string())],
                vec![Value::Integer(2), Value::Text("world".to_string())],
            ]
        );

        let rows_t3 = all_rows(
            conn.query("SELECT id, payload FROM t3 ORDER BY id", ())
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            rows_t3,
            vec![
                vec![Value::Integer(100), Value::Text("payload1".to_string())],
                vec![Value::Integer(200), Value::Text("payload2".to_string())],
            ]
        );
    }

    /// Pull test: remote adds a column that local already has.
    /// The pull must succeed (idempotent ALTER TABLE ADD COLUMN).
    #[tokio::test]
    pub async fn test_sync_pull_alter_table_add_column_idempotent() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();

        // Remote: create table with 2 columns and insert data
        server
            .db_sql("CREATE TABLE t(x TEXT PRIMARY KEY, y TEXT)")
            .await
            .unwrap();
        server
            .db_sql("INSERT INTO t VALUES ('a', 'alpha')")
            .await
            .unwrap();

        // Local: bootstrap from remote
        let db = crate::sync::Builder::new_remote(":memory:")
            .with_remote_url(server.db_url())
            .build()
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();

        let rows = all_rows(conn.query("SELECT x, y FROM t", ()).await.unwrap())
            .await
            .unwrap();
        assert_eq!(
            rows,
            vec![vec![
                Value::Text("a".to_string()),
                Value::Text("alpha".to_string())
            ]]
        );

        // Both sides independently add the same column z
        server
            .db_sql("ALTER TABLE t ADD COLUMN z TEXT")
            .await
            .unwrap();
        server
            .db_sql("INSERT INTO t VALUES ('b', 'beta', 'from-remote')")
            .await
            .unwrap();

        conn.execute("ALTER TABLE t ADD COLUMN z TEXT", ())
            .await
            .unwrap();

        // Pull should succeed despite both sides having column z
        db.pull().await.unwrap();

        // Verify local data is accessible after pull
        let rows = all_rows(
            conn.query("SELECT x, y, z FROM t ORDER BY x", ())
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Text("a".to_string()));
        assert_eq!(rows[1][0], Value::Text("b".to_string()));
        assert_eq!(rows[1][2], Value::Text("from-remote".to_string()));
    }

    /// Push test: local adds a column that remote already has.
    /// The push must succeed (ALTER TABLE ADD COLUMN error is ignored in the batch).
    #[tokio::test]
    pub async fn test_sync_push_alter_table_add_column_idempotent() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();

        // Remote: create table with 2 columns and insert data
        server
            .db_sql("CREATE TABLE t(x TEXT PRIMARY KEY, y TEXT)")
            .await
            .unwrap();
        server
            .db_sql("INSERT INTO t VALUES ('a', 'alpha')")
            .await
            .unwrap();

        // Local: bootstrap from remote
        let db = crate::sync::Builder::new_remote(":memory:")
            .with_remote_url(server.db_url())
            .build()
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();

        let rows = all_rows(conn.query("SELECT x, y FROM t", ()).await.unwrap())
            .await
            .unwrap();
        assert_eq!(
            rows,
            vec![vec![
                Value::Text("a".to_string()),
                Value::Text("alpha".to_string())
            ]]
        );

        // Remote adds column z first
        server
            .db_sql("ALTER TABLE t ADD COLUMN z TEXT")
            .await
            .unwrap();

        // Local also adds column z and inserts data
        conn.execute("ALTER TABLE t ADD COLUMN z TEXT", ())
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES ('b', 'beta', 'from-local')", ())
            .await
            .unwrap();

        // Push should succeed despite remote already having column z
        db.push().await.unwrap();

        // Verify the data row made it to remote
        let remote_rows = server
            .db_sql("SELECT x, y, z FROM t ORDER BY x")
            .await
            .unwrap();
        assert_eq!(remote_rows.len(), 2);
        assert_eq!(remote_rows[1][0], Value::Text("b".to_string()));
        assert_eq!(remote_rows[1][1], Value::Text("beta".to_string()));
        assert_eq!(remote_rows[1][2], Value::Text("from-local".to_string()));
    }

    /// Read-only consumer: pull + checkpoint loop with concurrent readers must
    /// not panic with `frame_count must be not less than frame_watermark` when
    /// a checkpoint backfills every frame and the next write tx restarts the
    /// WAL header behind a stale sync-engine watermark.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    pub async fn test_sync_pull_panics_after_full_backfill() {
        use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
        use std::sync::Arc;

        let _ = tracing_subscriber::fmt::try_init();
        let dir = TempDir::new().unwrap();
        let server = TursoServer::new().await.unwrap();

        let db = crate::sync::Builder::new_remote(dir.path().join("local.db").to_str().unwrap())
            .with_remote_url(server.db_url())
            .build()
            .await
            .unwrap();

        server.db_sql("CREATE TABLE t(y BLOB)").await.unwrap();
        db.pull().await.unwrap();

        // Read-only consumer: only opens a connection and queries.
        let conn = db.connect().await.unwrap();

        // Spawn random readers on independent connections that race against
        // the pull+checkpoint loop. Each reader picks a random query out of a
        // small set, sleeps a random short interval, and verifies that the
        // observed row count never goes backward and never exceeds what the
        // pull loop has already applied.
        let done = Arc::new(AtomicBool::new(false));
        let applied_total = Arc::new(AtomicI64::new(0));
        let mut readers = Vec::new();
        for _ in 0..4 {
            let reader_conn = db.connect().await.unwrap();
            let done = done.clone();
            let applied_total = applied_total.clone();
            readers.push(tokio::spawn(async move {
                let mut last_seen: i64 = 0;
                while !done.load(Ordering::Relaxed) {
                    let sleep_ms = rand::rng().random_range(0..=4);
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;

                    let sql = match rand::rng().random_range(0..3) {
                        0 => "SELECT count(*) FROM t",
                        1 => "SELECT count(length(y)) FROM t",
                        _ => "SELECT count(*) FROM t WHERE length(y) > 0",
                    };
                    let rows = match reader_conn.query(sql, ()).await {
                        Ok(rows) => rows,
                        // Acceptable transient errors during pull/checkpoint;
                        // anything else (including the WAL panic) propagates.
                        Err(crate::Error::Busy(_)) => continue,
                        Err(e) => panic!("reader query failed: {e:?}"),
                    };
                    let all = match all_rows(rows).await {
                        Ok(all) => all,
                        Err(e)
                            if e.downcast_ref::<crate::Error>()
                                .is_some_and(|error| matches!(error, crate::Error::Busy(_))) =>
                        {
                            continue;
                        }
                        Err(e) => panic!("reader query failed: {e:?}"),
                    };
                    let Value::Integer(n) = all[0][0] else {
                        panic!("unexpected reader value: {:?}", all[0][0]);
                    };
                    let upper = applied_total.load(Ordering::Acquire);
                    assert!(
                        n >= last_seen && n <= upper,
                        "reader saw inconsistent count {n}, last_seen={last_seen}, upper={upper}"
                    );
                    last_seen = n;
                }
            }));
        }

        let mut total: i64 = 0;
        for _ in 0..25 {
            let cnt: u16 = rand::rng().random_range(1..=16);
            let size: u32 = rand::rng().random_range(1..=4 * 1024);

            server
                .db_sql(&format!(
                    "INSERT INTO t SELECT randomblob({size}) FROM generate_series(1, {cnt})"
                ))
                .await
                .unwrap();
            total += cnt as i64;

            applied_total.store(total, Ordering::Release);
            loop {
                match db.pull().await {
                    Ok(_) => break,
                    Err(e) if super::is_sync_busy_error(&e) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(e) => panic!("pull failed: {e:?}"),
                }
            }

            let _ = db.checkpoint().await;

            let rows = all_rows(conn.query("SELECT count(*) FROM t", ()).await.unwrap())
                .await
                .unwrap();
            assert_eq!(rows, vec![vec![Value::Integer(total)]]);
        }

        done.store(true, Ordering::Relaxed);
        for h in readers {
            h.await.unwrap();
        }
    }

    /// Spin up a minimal mock HTTP server that captures the headers of every
    /// request, returns 500, and closes the connection. Returns the bound URL
    /// and a snapshot handle.
    async fn spawn_mock_http_server() -> (
        String,
        std::sync::Arc<std::sync::Mutex<Vec<Vec<String>>>>,
        std::sync::Arc<tokio::sync::Notify>,
    ) {
        use std::sync::Arc as StdArc;
        use std::sync::Mutex as StdMutex;
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;
        use tokio::sync::Notify;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}");

        let captured: StdArc<StdMutex<Vec<Vec<String>>>> = StdArc::new(StdMutex::new(Vec::new()));
        let notify = StdArc::new(Notify::new());

        let server_captured = captured.clone();
        let server_notify = notify.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let captured = server_captured.clone();
                let notify = server_notify.clone();
                tokio::spawn(async move {
                    let (read_half, mut write_half) = socket.split();
                    let mut reader = BufReader::new(read_half);
                    let mut headers = Vec::new();
                    loop {
                        let mut line = String::new();
                        match reader.read_line(&mut line).await {
                            Ok(0) => return,
                            Ok(_) => {
                                if line == "\r\n" || line == "\n" {
                                    break;
                                }
                                headers.push(line.trim_end().to_string());
                            }
                            Err(_) => return,
                        }
                    }
                    captured.lock().unwrap().push(headers);
                    notify.notify_waiters();
                    let _ = write_half
                        .write_all(
                            b"HTTP/1.1 500 Internal Server Error\r\n\
                              Content-Length: 0\r\n\
                              Connection: close\r\n\r\n",
                        )
                        .await;
                });
            }
        });

        (url, captured, notify)
    }

    fn extract_bearer_token(headers: &[String]) -> Option<String> {
        let auth = headers
            .iter()
            .find(|h| h.to_ascii_lowercase().starts_with("authorization:"))?;
        let prefix = "Bearer ";
        let pos = auth.find(prefix)?;
        Some(auth[pos + prefix.len()..].trim().to_string())
    }

    /// Mock-server test: every HTTP request the sync engine issues must carry
    /// `Authorization: Bearer <token>` when the builder was configured with an
    /// auth token. Uses a raw `tokio::net::TcpListener` instead of a full
    /// sync server so the test runs without any external infrastructure.
    #[tokio::test]
    pub async fn test_sync_sends_bearer_auth_header() {
        let _ = tracing_subscriber::fmt::try_init();

        let (url, captured, notify) = spawn_mock_http_server().await;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("local.db");
        let build_task = tokio::spawn({
            let url = url.clone();
            let path = path.to_str().unwrap().to_string();
            async move {
                // Build is expected to fail (mock server returns 500) — we
                // only care that it issued a request with the right header.
                let _ = crate::sync::Builder::new_remote(&path)
                    .with_remote_url(&url)
                    .with_auth_token("my-secret-token-XYZ")
                    .build()
                    .await;
            }
        });

        tokio::time::timeout(Duration::from_secs(10), notify.notified())
            .await
            .expect("mock server did not receive any HTTP request from build");
        build_task.abort();

        let requests = captured.lock().unwrap().clone();
        let first = requests.first().expect("expected at least one request");
        let token = extract_bearer_token(first)
            .unwrap_or_else(|| panic!("no Bearer Authorization in request: {first:?}"));
        assert_eq!(token, "my-secret-token-XYZ");
    }

    /// `with_auth_token_fn` is documented to invoke its callback before every
    /// HTTP request so callers can rotate the token between requests. Verify
    /// that: (1) the callback fires once per request the engine issues, and
    /// (2) the value it produced is the value sent in `Authorization`.
    #[tokio::test]
    pub async fn test_sync_auth_token_fn_called_per_request() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
        use std::sync::Arc as StdArc;

        let _ = tracing_subscriber::fmt::try_init();

        let (url, captured, notify) = spawn_mock_http_server().await;

        let counter = StdArc::new(AtomicUsize::new(0));
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("local.db");

        // bootstrap_if_empty(false) keeps `build()` from issuing any HTTP
        // requests so we control invocations purely through `pull()` calls.
        let db = crate::sync::Builder::new_remote(path.to_str().unwrap())
            .with_remote_url(&url)
            .bootstrap_if_empty(false)
            .with_auth_token_fn({
                let counter = counter.clone();
                move || {
                    let n = counter.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                    async move { Ok(format!("rotating-token-{n}")) }
                }
            })
            .build()
            .await
            .expect("build with bootstrap_if_empty(false) must not issue HTTP");

        // Each pull issues at least one HTTP request; mock returns 500 so
        // pull errors, but the auth callback was invoked before the send.
        const PULLS: usize = 3;
        for _ in 0..PULLS {
            let _ = db.pull().await;
        }

        // Drain in case responses raced ahead of the captured-vector push.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if captured.lock().unwrap().len() >= PULLS {
                    break;
                }
                notify.notified().await;
            }
        })
        .await
        .expect("did not see expected requests");

        let requests = captured.lock().unwrap().clone();
        assert!(
            requests.len() >= PULLS,
            "expected >= {PULLS} captured requests, got {}",
            requests.len()
        );

        let invocations = counter.load(AtomicOrdering::SeqCst);
        assert!(
            invocations >= requests.len(),
            "auth callback called {invocations} times for {} requests",
            requests.len()
        );

        // Every captured request carries a unique `Bearer rotating-token-N`
        // value drawn from `1..=invocations`.
        let mut tokens: Vec<String> = Vec::new();
        for (i, req) in requests.iter().enumerate() {
            let tok = extract_bearer_token(req)
                .unwrap_or_else(|| panic!("no Bearer in request {i}: {req:?}"));
            assert!(
                tok.starts_with("rotating-token-"),
                "unexpected token shape in request {i}: {tok:?}"
            );
            println!("token: {tok}");
            let n: usize = tok["rotating-token-".len()..]
                .parse()
                .unwrap_or_else(|_| panic!("bad token suffix: {tok:?}"));
            assert!(
                (1..=invocations).contains(&n),
                "token {n} outside expected range 1..={invocations}"
            );
            tokens.push(tok);
        }
        let unique: std::collections::HashSet<_> = tokens.iter().cloned().collect();
        assert_eq!(
            unique.len(),
            tokens.len(),
            "tokens must be unique per request: {tokens:?}"
        );
    }
}
