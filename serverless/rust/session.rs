//! HTTP session management: one session per connection, tracking the
//! stream baton, base URL, and transaction state.

use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc,
    },
};

use futures::StreamExt;

use crate::{
    protocol::{
        decode_value, Batch, BatchCond, BatchStep, CursorEntry, CursorRequest, CursorResponse,
        PipelineRequest, PipelineResponse, Stmt, StreamRequest, StreamResponse, StreamResult,
        ENCRYPTION_KEY_HEADER,
    },
    rows::Row,
    AuthTokenFn, Column, Error, Result,
};

/// Rewrite `libsql://` and `turso://` URLs to `https://` and strip any
/// trailing slashes, since endpoint paths are appended with a leading slash.
pub fn normalize_url(url: &str) -> String {
    let url = if let Some(rest) = url.strip_prefix("libsql://") {
        format!("https://{rest}")
    } else if let Some(rest) = url.strip_prefix("turso://") {
        format!("https://{rest}")
    } else {
        url.to_string()
    };
    url.trim_end_matches('/').to_string()
}

/// Connection state observable without a server round trip. Shared between
/// the session (which updates it) and the connection handle (which reads it
/// from synchronous accessors like `is_autocommit`).
pub struct SharedState {
    /// Whether the connection is in autocommit mode, from the server's most
    /// recent authoritative answer. A non-null baton does NOT imply a
    /// transaction (section 4.3), so this is tracked explicitly: every
    /// pipeline carries a `get_autocommit` request and every cursor batch a
    /// trailing step gated on `is_autocommit`.
    pub autocommit: AtomicBool,
    pub last_insert_rowid: AtomicI64,
}

/// The decoded output of one executed statement.
pub struct StmtOutput {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    pub rows_affected: u64,
}

pub struct Session {
    client: reqwest::Client,
    auth_token: Option<AuthTokenFn>,
    remote_encryption_key: Option<String>,
    base_url: String,
    baton: Option<String>,
    pub shared: Arc<SharedState>,
}

#[cfg(not(target_arch = "wasm32"))]
type ResponseStream = Pin<Box<dyn futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Send>>;

/// On wasm32 the response body is a JavaScript `ReadableStream`, which is not `Send`.
#[cfg(target_arch = "wasm32")]
type ResponseStream = Pin<Box<dyn futures::Stream<Item = reqwest::Result<bytes::Bytes>>>>;

/// Reads newline-separated JSON values from a cursor response body
/// (section 7.2).
struct LineReader {
    stream: ResponseStream,
    buf: Vec<u8>,
    eof: bool,
}

impl LineReader {
    fn new(response: reqwest::Response) -> Self {
        Self {
            stream: Box::pin(response.bytes_stream()),
            buf: Vec::new(),
            eof: false,
        }
    }

    async fn next_line(&mut self) -> Result<Option<String>> {
        loop {
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = self.buf.drain(..=pos).collect();
                let line = String::from_utf8(line)
                    .map_err(|e| Error::Http(format!("invalid UTF-8 in cursor response: {e}")))?;
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                return Ok(Some(line.to_string()));
            }
            if self.eof {
                if self.buf.is_empty() {
                    return Ok(None);
                }
                let line = String::from_utf8(std::mem::take(&mut self.buf))
                    .map_err(|e| Error::Http(format!("invalid UTF-8 in cursor response: {e}")))?;
                let line = line.trim();
                if line.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(line.to_string()));
            }
            match self.stream.next().await {
                Some(Ok(chunk)) => self.buf.extend_from_slice(&chunk),
                Some(Err(e)) => return Err(Error::Http(format!("cursor stream failed: {e}"))),
                None => self.eof = true,
            }
        }
    }

    async fn next_entry(&mut self) -> Result<Option<CursorEntry>> {
        match self.next_line().await? {
            None => Ok(None),
            Some(line) => {
                let entry: CursorEntry = serde_json::from_str(&line)
                    .map_err(|e| Error::Http(format!("invalid cursor entry: {e}")))?;
                Ok(Some(entry))
            }
        }
    }
}

impl Session {
    pub fn new(
        url: &str,
        auth_token: Option<AuthTokenFn>,
        remote_encryption_key: Option<String>,
    ) -> (Self, Arc<SharedState>) {
        let shared = Arc::new(SharedState {
            autocommit: AtomicBool::new(true),
            last_insert_rowid: AtomicI64::new(0),
        });
        let session = Self {
            client: reqwest::Client::new(),
            auth_token,
            remote_encryption_key,
            base_url: normalize_url(url),
            baton: None,
            shared: shared.clone(),
        };
        (session, shared)
    }

    /// Forget the stream. Any transaction on it was (or will be) rolled
    /// back by the server, so the connection is back in autocommit mode.
    fn reset_stream(&mut self) {
        self.baton = None;
        self.shared.autocommit.store(true, Ordering::Relaxed);
    }

    async fn send(&self, path: &str, body: String) -> Result<reqwest::Response> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self
            .client
            .post(&url)
            .header("Content-Type", "application/json");
        // Resolved here rather than once at construction so dynamic providers
        // can rotate the token between requests.
        if let Some(provider) = &self.auth_token {
            let token = provider().await?;
            request = request.header("Authorization", format!("Bearer {token}"));
        }
        if let Some(key) = &self.remote_encryption_key {
            request = request.header(ENCRYPTION_KEY_HEADER, key);
        }
        request
            .body(body)
            .send()
            .await
            .map_err(|e| Error::Http(format!("request to {url} failed: {e}")))
    }

    async fn http_error(response: reqwest::Response) -> Error {
        let status = response.status();
        let mut message = None;
        if let Ok(body) = response.text().await {
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&body) {
                message = parsed
                    .get("error")
                    .and_then(|v| v.as_str())
                    .or_else(|| parsed.get("message").and_then(|v| v.as_str()))
                    .map(|s| s.to_string());
            }
        }
        match message {
            Some(msg) => Error::Http(format!("HTTP status {status}: {msg}")),
            None => Error::Http(format!("HTTP status {status}")),
        }
    }

    /// POST a request that carries the current baton. Any failure, including
    /// a non-200 response, is fatal for the stream (section 9.3): the error
    /// surfaces to the caller and the next statement starts a fresh stream.
    /// The driver never re-sends a request on its own; whether re-running a
    /// failed statement is safe is the application's call.
    async fn post(
        &mut self,
        path: &str,
        make_body: impl FnOnce(Option<String>) -> Result<String>,
    ) -> Result<reqwest::Response> {
        let response = match self.send(path, make_body(self.baton.clone())?).await {
            Ok(response) => response,
            Err(e) => {
                self.reset_stream();
                return Err(e);
            }
        };
        if !response.status().is_success() {
            self.reset_stream();
            return Err(Self::http_error(response).await);
        }
        Ok(response)
    }

    fn update_stream(&mut self, baton: Option<String>, base_url: Option<String>) {
        self.baton = baton;
        if let Some(base_url) = base_url {
            self.base_url = normalize_url(&base_url);
        }
    }

    /// Execute a pipeline (section 5). When `track_autocommit` is set, a
    /// `get_autocommit` request is appended and its answer refreshes the
    /// cached transaction state; the returned results cover only the
    /// caller's requests.
    pub async fn pipeline(
        &mut self,
        mut requests: Vec<StreamRequest>,
        track_autocommit: bool,
    ) -> Result<Vec<StreamResult>> {
        if track_autocommit {
            requests.push(StreamRequest::GetAutocommit);
        }
        let request_count = requests.len();
        let response = self
            .post("/v3/pipeline", |baton| {
                serde_json::to_string(&PipelineRequest { baton, requests })
                    .map_err(|e| Error::Error(format!("failed to encode request: {e}")))
            })
            .await?;
        let mut response: PipelineResponse = response
            .json()
            .await
            .map_err(|e| Error::Http(format!("invalid pipeline response: {e}")))?;
        self.update_stream(response.baton.take(), response.base_url.take());
        let mut results = response.results;
        if results.len() != request_count {
            return Err(Error::Http(format!(
                "pipeline response has {} results for {request_count} requests",
                results.len()
            )));
        }
        if track_autocommit {
            let result = results
                .pop()
                .expect("track_autocommit appended a request, so results is non-empty");
            match result {
                StreamResult::Ok {
                    response: StreamResponse::GetAutocommit { is_autocommit },
                } => {
                    self.shared
                        .autocommit
                        .store(is_autocommit, Ordering::Relaxed);
                }
                StreamResult::Ok { response } => {
                    return Err(Error::Http(format!(
                        "expected get_autocommit result in pipeline response, got {response:?}"
                    )));
                }
                StreamResult::Error { error } => return Err(error.into()),
            }
        }
        Ok(results)
    }

    /// Refresh the cached transaction state with a standalone
    /// `get_autocommit` request. Failures are swallowed: this runs on error
    /// paths where the original failure must not be masked, and a dead
    /// stream already reset the state to autocommit.
    async fn refresh_autocommit(&mut self) {
        let _ = self.pipeline(Vec::new(), true).await;
    }

    /// The trailing batch step appended to every cursor batch: a no-op
    /// statement gated on `is_autocommit`. The cursor endpoint cannot carry
    /// a `get_autocommit` request, so whether this step executed tells us
    /// the connection's transaction state without an extra round trip.
    fn autocommit_probe_step() -> BatchStep {
        BatchStep {
            condition: Some(BatchCond::IsAutocommit),
            stmt: Stmt::new("SELECT 1", false),
        }
    }

    /// Execute a single statement on the cursor endpoint (section 7),
    /// decoding the streamed entries into a buffered result.
    pub async fn execute_cursor_stmt(&mut self, stmt: Stmt) -> Result<StmtOutput> {
        let steps = vec![
            BatchStep {
                condition: None,
                stmt,
            },
            Self::autocommit_probe_step(),
        ];
        let probe_step = 1u32;

        let response = self
            .post("/v3/cursor", |baton| {
                serde_json::to_string(&CursorRequest {
                    baton,
                    batch: Batch { steps },
                })
                .map_err(|e| Error::Error(format!("failed to encode request: {e}")))
            })
            .await?;

        let mut reader = LineReader::new(response);
        let first_line = reader.next_line().await?.ok_or_else(|| {
            Error::Http("cursor response body ended before the cursor response line".to_string())
        })?;
        let cursor_response: CursorResponse = serde_json::from_str(&first_line)
            .map_err(|e| Error::Http(format!("invalid cursor response: {e}")))?;
        self.update_stream(cursor_response.baton, cursor_response.base_url);

        let mut output = StmtOutput {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: 0,
        };
        let mut last_insert_rowid = None;
        let mut step_error: Option<Error> = None;
        let mut in_probe = false;
        let mut probe_executed = false;
        let mut probe_unreliable = false;
        let mut fatal: Option<Error> = None;

        loop {
            let entry = match reader.next_entry().await {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(e) => {
                    // The body failed mid-stream; the stream is unusable.
                    self.reset_stream();
                    return Err(step_error.unwrap_or(e));
                }
            };
            match entry {
                CursorEntry::StepBegin { step, cols } => {
                    if step == probe_step {
                        in_probe = true;
                        probe_executed = true;
                    } else {
                        in_probe = false;
                        output.columns = cols
                            .into_iter()
                            .map(|c| Column {
                                name: c.name.unwrap_or_default(),
                                decl_type: c.decltype,
                            })
                            .collect();
                    }
                }
                CursorEntry::Row { row } => {
                    if !in_probe && step_error.is_none() {
                        let values = match row.iter().map(decode_value).collect::<Result<Vec<_>>>()
                        {
                            Ok(values) => values,
                            Err(e) => {
                                // An undecodable value from the server;
                                // abandon the stream rather than resume it
                                // in an unknown position.
                                self.reset_stream();
                                return Err(e);
                            }
                        };
                        output.rows.push(Row::new(values));
                    }
                }
                CursorEntry::StepEnd {
                    affected_row_count,
                    last_insert_rowid: rowid,
                } => {
                    if !in_probe {
                        output.rows_affected = affected_row_count;
                        if let Some(rowid) = rowid {
                            match rowid.to_i64() {
                                Ok(rowid) => last_insert_rowid = Some(rowid),
                                Err(e) => {
                                    self.reset_stream();
                                    return Err(e);
                                }
                            }
                        }
                    }
                }
                CursorEntry::StepError { step, error } => {
                    if step == probe_step {
                        probe_unreliable = true;
                    } else if step_error.is_none() {
                        step_error = Some(error.into());
                    }
                    in_probe = false;
                }
                CursorEntry::Error { error } => {
                    fatal = Some(error.into());
                    break;
                }
                // Reserved terminating entry. Turso Cloud ends the body
                // without it, so a cleanly ended body counts as complete
                // (matching the JavaScript driver); truncated bodies
                // surface as transport errors through the HTTP framing.
                CursorEntry::ReplicationIndex {} => {}
                CursorEntry::Unknown => {}
            }
        }

        if let Some(fatal) = fatal {
            // The cursor died but the stream may survive; ask the server
            // for the authoritative transaction state.
            self.refresh_autocommit().await;
            return Err(step_error.unwrap_or(fatal));
        }
        if probe_unreliable {
            self.refresh_autocommit().await;
        } else {
            self.shared
                .autocommit
                .store(probe_executed, Ordering::Relaxed);
        }
        if let Some(error) = step_error {
            return Err(error);
        }
        if let Some(rowid) = last_insert_rowid {
            self.shared
                .last_insert_rowid
                .store(rowid, Ordering::Relaxed);
        }
        Ok(output)
    }

    /// Close the stream (section 6.8). Errors are ignored: the stream may
    /// already have expired.
    pub async fn close(&mut self) {
        if self.baton.is_some() {
            let _ = self.pipeline(vec![StreamRequest::Close], false).await;
        }
        self.reset_stream();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_url_rewrites_schemes() {
        assert_eq!(normalize_url("libsql://db.turso.io"), "https://db.turso.io");
        assert_eq!(normalize_url("turso://db.turso.io"), "https://db.turso.io");
        assert_eq!(normalize_url("https://db.turso.io"), "https://db.turso.io");
        assert_eq!(
            normalize_url("http://localhost:8080"),
            "http://localhost:8080"
        );
    }

    #[test]
    fn normalize_url_strips_trailing_slash() {
        assert_eq!(normalize_url("https://db.turso.io/"), "https://db.turso.io");
        assert_eq!(
            normalize_url("libsql://db.turso.io/"),
            "https://db.turso.io"
        );
        assert_eq!(normalize_url("turso://db.turso.io/"), "https://db.turso.io");
    }
}
