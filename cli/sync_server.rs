use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use prost::Message;
use roaring::RoaringBitmap;
use tracing::{debug, error, info};

use turso_core::{
    Connection, Database, DatabaseOpts, OpenFlags, SqliteDialect, Value as CoreValue,
};
use turso_sync_engine::server_proto::{
    BatchCond, BatchResult, BatchStep, BatchStreamReq, BatchStreamResp, Col, Error,
    ExecuteStreamReq, ExecuteStreamResp, MvccLogicalLogMetadataProto, MvccLogicalLogRangeProto,
    PageData, PageSetRawEncodingProto, PageUpdatesEncodingReq, PipelineReqBody, PipelineRespBody,
    PullUpdatesApplyMode, PullUpdatesProtocol, PullUpdatesReqProtoBody, PullUpdatesRespProtoBody,
    PullUpdatesStreamKind, Row, StmtResult, StreamRequest, StreamResponse, StreamResult, Value,
};

const WAL_FRAME_HEADER_SIZE: usize = 24;
const PAGE_SIZE: usize = 4096;
const MVCC_LOG_MAGIC: u32 = 0x4C4D4C32;
const MVCC_LOG_VERSION: u8 = 3;
const MVCC_LOG_HEADER_SIZE: usize = 56;
const MVCC_LOG_HEADER_SALT_START: usize = 8;
const MVCC_LOG_HEADER_SALT_END: usize = 16;
const MVCC_LOG_HEADER_RESERVED_START: usize = 16;
const MVCC_LOG_HEADER_CRC_START: usize = 52;
const MVCC_TX_FRAME_MAGIC: u32 = 0x5854564D;
const MVCC_TX_EXT_FRAME_MAGIC: u32 = 0x5845564D;
const MVCC_TX_END_MAGIC: u32 = 0x4554564D;
const MVCC_TX_HEADER_SIZE: usize = 24;
const MVCC_TX_EXT_HEADER_SIZE: usize = 40;
const MVCC_TX_TRAILER_SIZE: usize = 8;
const MVCC_TX_FRAME_FLAG_HAS_EXTENSION_BLOCK: u32 = 1 << 0;
const MAX_HEADER_BYTES: usize = 32 * 1024;

pub struct OpenConfig {
    pub vfs: Option<String>,
    pub flags: OpenFlags,
    pub db_opts: DatabaseOpts,
}

struct DbHandle {
    conn: Mutex<Arc<Connection>>,
    path: String,
}

enum DbSource {
    Single(Arc<DbHandle>),
    Dir {
        base: PathBuf,
        config: OpenConfig,
        open: Mutex<HashMap<String, Arc<DbHandle>>>,
    },
}

pub struct TursoSyncServer {
    address: String,
    source: DbSource,
    interrupt_count: Arc<AtomicUsize>,
}

impl TursoSyncServer {
    pub fn new(
        address: String,
        db_path: String,
        conn: Arc<Connection>,
        interrupt_count: Arc<AtomicUsize>,
    ) -> Result<Self> {
        conn.wal_auto_actions_disable();

        Ok(Self {
            address,
            source: DbSource::Single(Arc::new(DbHandle {
                conn: Mutex::new(conn),
                path: db_path,
            })),
            interrupt_count,
        })
    }

    pub fn new_dir(
        address: String,
        base: PathBuf,
        interrupt_count: Arc<AtomicUsize>,
        config: OpenConfig,
    ) -> Result<Self> {
        if !base.is_dir() {
            return Err(anyhow!(
                "--sync-dir path does not exist or is not a directory: {}",
                base.display()
            ));
        }
        Ok(Self {
            address,
            source: DbSource::Dir {
                base: base.canonicalize()?,
                config,
                open: Mutex::new(HashMap::new()),
            },
            interrupt_count,
        })
    }

    fn resolve_db(
        &self,
        requested: Option<&str>,
    ) -> std::result::Result<Arc<DbHandle>, HttpResponse> {
        match (&self.source, requested) {
            (DbSource::Single(h), None) => Ok(h.clone()),
            (DbSource::Single(_), Some(_)) => Err(text_response(404, "Not Found")),
            (DbSource::Dir { .. }, None) => Err(text_response(404, "Not Found")),
            (DbSource::Dir { base, config, open }, Some(name)) => {
                if !validate_db_name(name) {
                    return Err(text_response(400, "Invalid database name"));
                }
                let mut open = open.lock().unwrap();
                let entry = match open.entry(name.to_string()) {
                    Entry::Occupied(entry) => return Ok(entry.get().clone()),
                    Entry::Vacant(entry) => entry,
                };
                let path = db_path_for(base, name);
                let dir = path.parent().expect("database path has a parent directory");
                if !config.flags.contains(OpenFlags::ReadOnly) {
                    if let Err(err) = std::fs::create_dir_all(dir) {
                        error!("failed to create directory for database {name}: {err}");
                        return Err(text_response(500, &format!("Internal Server Error: {err}")));
                    }
                }
                if !dir.canonicalize().is_ok_and(|dir| dir.starts_with(base)) {
                    return Err(text_response(404, "Not Found"));
                }
                let handle = match open_db_handle(&path, config) {
                    Ok(handle) => handle,
                    // Sync clients retry a 500 forever but can act on a 404.
                    Err(err) if path.exists() => {
                        error!("failed to open database {name}: {err}");
                        return Err(text_response(500, &format!("Internal Server Error: {err}")));
                    }
                    Err(err) => {
                        debug!("no database named {name}: {err}");
                        return Err(text_response(404, "Not Found"));
                    }
                };
                Ok(entry.insert(handle).clone())
            }
        }
    }

    pub fn run(&self) -> Result<()> {
        info!("Starting TursoSyncServer on {}", self.address);

        let listener = TcpListener::bind(&self.address)?;
        listener.set_nonblocking(true)?;

        let interrupt_count = self.interrupt_count.clone();
        let shutdown_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let shutdown_flag_clone = shutdown_flag.clone();

        let monitor_handle = thread::spawn(move || loop {
            if interrupt_count.load(Ordering::SeqCst) > 0 {
                debug!("Interrupt detected, signaling shutdown");
                shutdown_flag_clone.store(true, Ordering::SeqCst);
                break;
            }
            thread::sleep(std::time::Duration::from_millis(100));
        });

        loop {
            if shutdown_flag.load(Ordering::SeqCst) {
                info!("Shutdown signal received, stopping server");
                break;
            }

            match listener.accept() {
                Ok((stream, addr)) => {
                    info!("Accepted connection from {}", addr);
                    if let Err(e) = self.handle_connection(stream) {
                        error!("Error handling connection: {}", e);
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }
                Err(e) => {
                    error!("Error accepting connection: {}", e);
                }
            }
        }

        let _ = monitor_handle.join();
        info!("TursoSyncServer stopped");
        Ok(())
    }

    fn handle_connection(&self, mut stream: TcpStream) -> Result<()> {
        stream.set_nonblocking(false)?;
        stream.set_read_timeout(Some(std::time::Duration::from_secs(30)))?;

        let mut buffer = [0u8; 8192];
        let mut request_data = Vec::new();

        loop {
            let n = stream.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            // Bytes before this offset hold no terminator, and one can still
            // straddle the last three of them.
            let unscanned = request_data.len().saturating_sub(3);
            request_data.extend_from_slice(&buffer[..n]);

            let Some(header_end) = find_header_end(&request_data, unscanned) else {
                if request_data.len() > MAX_HEADER_BYTES {
                    return Err(anyhow!(
                        "HTTP request headers exceed {MAX_HEADER_BYTES} bytes"
                    ));
                }
                continue;
            };
            let headers = String::from_utf8_lossy(&request_data[..header_end]);
            if let Some(content_length) = parse_content_length(&headers) {
                let total_expected = request_end(header_end, content_length)?;
                while request_data.len() < total_expected {
                    let n = stream.read(&mut buffer)?;
                    if n == 0 {
                        break;
                    }
                    request_data.extend_from_slice(&buffer[..n]);
                }
            }
            break;
        }

        let (method, path, body) = parse_http_request(&request_data)?;
        info!("Request: {} {}", method, path);

        let response = match parse_route(&method, &path) {
            Route::Options => Ok(text_response(204, "")),
            Route::Pipeline { db } => match self.resolve_db(db) {
                Ok(handle) => {
                    debug!("Handling /v2/pipeline request");
                    self.handle_pipeline(&handle, &body)
                }
                Err(resp) => Ok(resp),
            },
            Route::PullUpdates { db } => match self.resolve_db(db) {
                Ok(handle) => {
                    debug!("Handling /pull-updates request");
                    self.handle_pull_updates(&handle, &body)
                }
                Err(resp) => Ok(resp),
            },
            Route::NotFound => {
                info!("Unknown endpoint: {} {}", method, path);
                Ok(text_response(404, "Not Found"))
            }
        };

        let http_response = match response {
            Ok(resp) => resp,
            Err(e) => {
                error!("Request error: {}", e);
                HttpResponse {
                    status: 500,
                    content_type: "text/plain".to_string(),
                    body: format!("Internal Server Error: {e}").into_bytes(),
                }
            }
        };

        let response_bytes = format_http_response(&http_response);
        stream.write_all(&response_bytes)?;
        stream.flush()?;

        Ok(())
    }

    fn handle_pipeline(&self, db: &DbHandle, body: &[u8]) -> Result<HttpResponse> {
        let req: PipelineReqBody = serde_json::from_slice(body)
            .map_err(|e| anyhow!("Failed to parse pipeline request: {}", e))?;

        debug!("Pipeline request: {:?}", req);

        let conn = db.conn.lock().unwrap();

        let mut results = Vec::new();

        for request in req.requests {
            let result = match request {
                StreamRequest::Execute(exec_req) => self.execute_statement(&conn, &exec_req),
                StreamRequest::Batch(batch_req) => self.execute_batch(&conn, &batch_req),
                StreamRequest::None => StreamResult::Error {
                    error: Error {
                        message: "Unknown request type".to_string(),
                        code: "UNKNOWN".to_string(),
                    },
                },
            };
            results.push(result);
        }

        let resp = PipelineRespBody {
            baton: req.baton,
            base_url: None,
            results,
        };

        let body = serde_json::to_vec(&resp)?;

        Ok(HttpResponse {
            status: 200,
            content_type: "application/json".to_string(),
            body,
        })
    }

    fn execute_statement(&self, conn: &Arc<Connection>, req: &ExecuteStreamReq) -> StreamResult {
        let sql = match &req.stmt.sql {
            Some(s) => s.clone(),
            None => {
                return StreamResult::Error {
                    error: Error {
                        message: "No SQL provided".to_string(),
                        code: "NO_SQL".to_string(),
                    },
                }
            }
        };

        debug!("Executing SQL: {}", sql);

        let mut stmt = match conn.prepare(&sql) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to prepare statement: {}", e);
                return StreamResult::Error {
                    error: Error {
                        message: e.to_string(),
                        code: "PREPARE_ERROR".to_string(),
                    },
                };
            }
        };

        for (i, arg) in req.stmt.args.iter().enumerate() {
            let core_value = convert_value_to_core(arg);
            if let Err(err) = stmt.bind_at(std::num::NonZero::new(i + 1).unwrap(), core_value) {
                error!("Failed to bind statement argument: {}", err);
                return StreamResult::Error {
                    error: Error {
                        message: err.to_string(),
                        code: "BIND_ERROR".to_string(),
                    },
                };
            }
        }

        let want_rows = req.stmt.want_rows.unwrap_or(true);

        if want_rows {
            match stmt.run_collect_rows() {
                Ok(rows) => {
                    let cols: Vec<Col> = (0..stmt.num_columns())
                        .map(|i| Col {
                            name: Some(stmt.get_column_name(i).to_string()),
                            decltype: stmt.get_column_decltype(i),
                        })
                        .collect();

                    let result_rows: Vec<Row> = rows
                        .into_iter()
                        .map(|row| Row {
                            values: row.into_iter().map(convert_core_to_value).collect(),
                        })
                        .collect();

                    StreamResult::Ok {
                        response: StreamResponse::Execute(ExecuteStreamResp {
                            result: StmtResult {
                                cols,
                                rows: result_rows,
                                affected_row_count: 0,
                                last_insert_rowid: None,
                                replication_index: None,
                                rows_read: 0,
                                rows_written: 0,
                                query_duration_ms: 0.0,
                            },
                        }),
                    }
                }
                Err(e) => {
                    error!("Failed to execute statement: {}", e);
                    StreamResult::Error {
                        error: Error {
                            message: e.to_string(),
                            code: "EXECUTE_ERROR".to_string(),
                        },
                    }
                }
            }
        } else {
            match stmt.run_ignore_rows() {
                Ok(()) => StreamResult::Ok {
                    response: StreamResponse::Execute(ExecuteStreamResp {
                        result: StmtResult {
                            cols: vec![],
                            rows: vec![],
                            affected_row_count: 0,
                            last_insert_rowid: None,
                            replication_index: None,
                            rows_read: 0,
                            rows_written: 0,
                            query_duration_ms: 0.0,
                        },
                    }),
                },
                Err(e) => {
                    error!("Failed to execute statement: {}", e);
                    StreamResult::Error {
                        error: Error {
                            message: e.to_string(),
                            code: "EXECUTE_ERROR".to_string(),
                        },
                    }
                }
            }
        }
    }

    fn execute_batch(&self, conn: &Arc<Connection>, req: &BatchStreamReq) -> StreamResult {
        let batch = &req.batch;
        let mut step_results: Vec<Option<StmtResult>> = Vec::with_capacity(batch.steps.len());
        let mut step_errors: Vec<Option<Error>> = Vec::with_capacity(batch.steps.len());

        for (step_idx, step) in batch.steps.iter().enumerate() {
            let should_execute = match &step.condition {
                None => true,
                Some(cond) => Self::evaluate_condition(cond, &step_results, &step_errors, conn),
            };

            if should_execute {
                let result = self.execute_batch_step(conn, step);
                match result {
                    Ok(stmt_result) => {
                        step_results.push(Some(stmt_result));
                        step_errors.push(None);
                    }
                    Err(e) => {
                        error!("Batch step {} failed: {}", step_idx, e);
                        step_results.push(None);
                        step_errors.push(Some(Error {
                            message: e.to_string(),
                            code: "BATCH_STEP_ERROR".to_string(),
                        }));
                    }
                }
            } else {
                step_results.push(None);
                step_errors.push(None);
            }
        }

        StreamResult::Ok {
            response: StreamResponse::Batch(BatchStreamResp {
                result: BatchResult {
                    step_results,
                    step_errors,
                    replication_index: None,
                },
            }),
        }
    }

    fn evaluate_condition(
        cond: &BatchCond,
        step_results: &[Option<StmtResult>],
        step_errors: &[Option<Error>],
        conn: &Arc<Connection>,
    ) -> bool {
        match cond {
            BatchCond::None => true,
            BatchCond::Ok { step } => {
                let idx = *step as usize;
                idx < step_results.len() && step_results[idx].is_some()
            }
            BatchCond::Error { step } => {
                let idx = *step as usize;
                idx < step_errors.len() && step_errors[idx].is_some()
            }
            BatchCond::Not { cond } => {
                !Self::evaluate_condition(cond, step_results, step_errors, conn)
            }
            BatchCond::And(list) => list
                .conds
                .iter()
                .all(|c| Self::evaluate_condition(c, step_results, step_errors, conn)),
            BatchCond::Or(list) => list
                .conds
                .iter()
                .any(|c| Self::evaluate_condition(c, step_results, step_errors, conn)),
            BatchCond::IsAutocommit {} => conn.get_auto_commit(),
        }
    }

    fn execute_batch_step(&self, conn: &Arc<Connection>, step: &BatchStep) -> Result<StmtResult> {
        let sql = step
            .stmt
            .sql
            .as_ref()
            .ok_or_else(|| anyhow!("No SQL in batch step"))?;

        debug!("Executing batch step SQL: {}", sql);

        let mut stmt = conn.prepare(sql)?;

        for (i, arg) in step.stmt.args.iter().enumerate() {
            let core_value = convert_value_to_core(arg);
            stmt.bind_at(std::num::NonZero::new(i + 1).unwrap(), core_value)?;
        }

        let want_rows = step.stmt.want_rows.unwrap_or(true);

        if want_rows {
            let rows = stmt.run_collect_rows()?;

            let cols: Vec<Col> = (0..stmt.num_columns())
                .map(|i| Col {
                    name: Some(stmt.get_column_name(i).to_string()),
                    decltype: stmt.get_column_decltype(i),
                })
                .collect();

            let result_rows: Vec<Row> = rows
                .into_iter()
                .map(|row| Row {
                    values: row.into_iter().map(convert_core_to_value).collect(),
                })
                .collect();

            Ok(StmtResult {
                cols,
                rows: result_rows,
                affected_row_count: 0,
                last_insert_rowid: None,
                replication_index: None,
                rows_read: 0,
                rows_written: 0,
                query_duration_ms: 0.0,
            })
        } else {
            stmt.run_ignore_rows()?;
            Ok(StmtResult {
                cols: vec![],
                rows: vec![],
                affected_row_count: 0,
                last_insert_rowid: None,
                replication_index: None,
                rows_read: 0,
                rows_written: 0,
                query_duration_ms: 0.0,
            })
        }
    }

    fn handle_pull_updates(&self, db: &DbHandle, body: &[u8]) -> Result<HttpResponse> {
        let req = <PullUpdatesReqProtoBody as Message>::decode(body)
            .map_err(|e| anyhow!("Failed to decode PullUpdatesRequest: {}", e))?;

        debug!(
            "Pull updates request: server_revision={}, client_revision={}",
            req.server_revision, req.client_revision
        );

        let encoding =
            PageUpdatesEncodingReq::try_from(req.encoding).unwrap_or(PageUpdatesEncodingReq::Raw);

        if encoding == PageUpdatesEncodingReq::Zstd {
            return Err(anyhow!("Zstd encoding is not supported"));
        }

        if PullUpdatesStreamKind::try_from(req.stream_kind).unwrap_or(PullUpdatesStreamKind::Pages)
            == PullUpdatesStreamKind::MvccLogicalLog
        {
            return self.handle_logical_pull_updates(db, &req);
        }

        self.handle_page_pull_updates(db, &req, PullUpdatesApplyMode::Incremental)
    }

    fn handle_page_pull_updates(
        &self,
        db: &DbHandle,
        req: &PullUpdatesReqProtoBody,
        apply_mode: PullUpdatesApplyMode,
    ) -> Result<HttpResponse> {
        let conn = db.conn.lock().unwrap();

        let wal_state = conn.wal_state()?;
        debug!("WAL state: max_frame={}", wal_state.max_frame);

        let server_revision: u64 = if req.server_revision.is_empty() {
            wal_state.max_frame
        } else {
            req.server_revision.parse().unwrap_or(wal_state.max_frame)
        };

        let client_revision: u64 = if req.client_revision.is_empty() {
            0
        } else {
            req.client_revision.parse().unwrap_or(0)
        };

        debug!(
            "Using server_revision={}, client_revision={}",
            server_revision, client_revision
        );

        let pages_selector: Option<RoaringBitmap> = if !req.server_pages_selector.is_empty() {
            Some(
                RoaringBitmap::deserialize_from(&req.server_pages_selector[..])
                    .map_err(|e| anyhow!("Failed to parse server_pages_selector: {}", e))?,
            )
        } else {
            None
        };

        let mut seen_pages: HashSet<u32> = HashSet::new();
        let mut pages_to_send: Vec<(u32, Vec<u8>)> = Vec::new();

        let frame_size = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;
        let mut frame_buffer = vec![0u8; frame_size];

        debug!(
            "pull-updates: scanning WAL frames {}..={} (client_revision={}, server_revision={})",
            client_revision + 1,
            server_revision,
            client_revision,
            server_revision
        );

        if server_revision > client_revision {
            for frame_no in (client_revision + 1..=server_revision).rev() {
                let frame_info = conn.wal_get_frame(frame_no, &mut frame_buffer)?;

                let page_no = frame_info.page_no;
                // WAL uses 1-based page numbers, sync protocol uses 0-based
                let page_id = page_no - 1;

                if seen_pages.contains(&page_no) {
                    continue;
                }

                if let Some(ref selector) = pages_selector {
                    if !selector.contains(page_id) {
                        continue;
                    }
                }

                seen_pages.insert(page_no);

                let type_byte = frame_buffer[WAL_FRAME_HEADER_SIZE];
                debug!(
                    "pull-updates: including page_no={}, frame_no={}, type_byte={}, db_size={}",
                    page_no, frame_no, type_byte, frame_info.db_size
                );

                let page_data = frame_buffer[WAL_FRAME_HEADER_SIZE..].to_vec();
                pages_to_send.push((page_id, page_data));
            }
        }

        debug!(
            "pull-updates: sending {} pages, seen_pages={:?}",
            pages_to_send.len(),
            seen_pages
        );
        pages_to_send.reverse();

        let db_size = current_db_size_pages(&conn, wal_state.max_frame)?;

        let header = PullUpdatesRespProtoBody {
            server_revision: server_revision.to_string(),
            db_size,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Pages as i32,
            apply_mode: apply_mode as i32,
            mvcc_log: None,
            // The protocol hint reflects the database, not the response shape:
            // page bootstraps of an MVCC database advertise MvccLogical so
            // auto-detecting clients switch to logical pulls, mirroring the
            // production server.
            protocol: if conn.mvcc_enabled() {
                PullUpdatesProtocol::MvccLogical as i32
            } else {
                PullUpdatesProtocol::Pages as i32
            },
        };

        let mut response_body = Vec::new();

        let header_bytes = header.encode_to_vec();
        encode_length_delimited(&mut response_body, &header_bytes);

        for (page_id, page_data) in pages_to_send {
            let page_msg = PageData {
                page_id: page_id as u64,
                encoded_page: Bytes::from(page_data),
            };
            let page_bytes = page_msg.encode_to_vec();
            encode_length_delimited(&mut response_body, &page_bytes);
        }

        debug!(
            "Sending {} bytes in pull-updates response",
            response_body.len()
        );

        Ok(HttpResponse {
            status: 200,
            content_type: "application/protobuf".to_string(),
            body: response_body,
        })
    }

    fn handle_logical_pull_updates(
        &self,
        db: &DbHandle,
        req: &PullUpdatesReqProtoBody,
    ) -> Result<HttpResponse> {
        let (db_size, fallback_revision, legacy_current_revision) = {
            let conn = db.conn.lock().unwrap();
            let wal_state = conn.wal_state()?;
            (
                current_db_size_pages(&conn, wal_state.max_frame)?,
                format!("page:{}", wal_state.max_frame),
                wal_state.max_frame.to_string(),
            )
        };
        let log_path = match logical_log_path(&db.path) {
            Ok(path) => path,
            Err(_) if is_in_memory_db_path(&db.path) => {
                info!(
                    "logical pull requested for in-memory sync server database; returning incremental pages"
                );
                return self.handle_page_pull_updates(db, req, PullUpdatesApplyMode::Incremental);
            }
            Err(err) => return Err(err),
        };
        let log = match std::fs::read(&log_path) {
            Ok(log) => log,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                info!(
                    "logical pull requested but no MVCC log exists at {}; returning replace-base fallback",
                    log_path.display()
                );
                return self.handle_logical_fallback(
                    db,
                    req,
                    fallback_revision,
                    &legacy_current_revision,
                    db_size,
                );
            }
            Err(err) => return Err(err.into()),
        };
        let snapshot = match scan_mvcc_log(&log) {
            Ok(snapshot) => snapshot,
            Err(err) if is_nonportable_mvcc_log_error(&err) => {
                info!(
                    "logical pull requested but MVCC log is not portable; returning replace-base pages: {err}"
                );
                return self.handle_logical_fallback(
                    db,
                    req,
                    fallback_revision,
                    &legacy_current_revision,
                    db_size,
                );
            }
            Err(err) => return Err(err),
        };
        let start_offset = parse_mvcc_revision_offset(&req.client_revision, snapshot.end_offset)?;
        if start_offset > snapshot.end_offset {
            return Err(anyhow!(
                "MVCC logical pull revision is from the future: client_offset={} server_offset={}",
                start_offset,
                snapshot.end_offset
            ));
        }
        let start = usize::try_from(start_offset)
            .map_err(|_| anyhow!("MVCC logical pull start offset overflows usize"))?;
        let end = usize::try_from(snapshot.end_offset)
            .map_err(|_| anyhow!("MVCC logical pull end offset overflows usize"))?;

        let mut response_body = Vec::new();
        let (mvcc_log, body) = if start == end {
            (None, Vec::new())
        } else {
            let crc_seed = if start_offset == 0 {
                None
            } else {
                let seed = snapshot.crc_seed_at(start_offset)?;
                Some(seed.to_le_bytes().to_vec())
            };
            (
                Some(MvccLogicalLogMetadataProto {
                    format: "lml3".to_string(),
                    checkpoint_transition: false,
                    ranges: vec![MvccLogicalLogRangeProto {
                        generation: 1,
                        start_offset,
                        end_offset: snapshot.end_offset,
                        starts_with_header: start_offset == 0,
                        crc_seed,
                    }],
                }),
                log[start..end].to_vec(),
            )
        };

        let header = PullUpdatesRespProtoBody {
            server_revision: format!("g1:o{}", snapshot.end_offset),
            db_size,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
            mvcc_log,
            protocol: PullUpdatesProtocol::MvccLogical as i32,
        };

        let header_bytes = header.encode_to_vec();
        encode_length_delimited(&mut response_body, &header_bytes);
        response_body.extend_from_slice(&body);

        debug!(
            "pull-updates logical: path={} client_revision={} end_offset={} body_bytes={}",
            log_path.display(),
            req.client_revision,
            snapshot.end_offset,
            body.len()
        );

        Ok(HttpResponse {
            status: 200,
            content_type: "application/protobuf".to_string(),
            body: response_body,
        })
    }

    fn handle_logical_fallback(
        &self,
        db: &DbHandle,
        req: &PullUpdatesReqProtoBody,
        server_revision: String,
        legacy_current_revision: &str,
        db_size: u64,
    ) -> Result<HttpResponse> {
        if req.client_revision == server_revision {
            return self.handle_empty_logical_pull(server_revision, db_size);
        }
        if req.client_revision == legacy_current_revision {
            return self.handle_empty_logical_pull(req.client_revision.clone(), db_size);
        }

        self.handle_replace_base_pages(db, server_revision)
    }

    fn handle_empty_logical_pull(
        &self,
        server_revision: String,
        db_size: u64,
    ) -> Result<HttpResponse> {
        let header = PullUpdatesRespProtoBody {
            server_revision,
            db_size,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
            mvcc_log: None,
            protocol: PullUpdatesProtocol::MvccLogical as i32,
        };

        let mut response_body = Vec::new();
        let header_bytes = header.encode_to_vec();
        encode_length_delimited(&mut response_body, &header_bytes);

        Ok(HttpResponse {
            status: 200,
            content_type: "application/protobuf".to_string(),
            body: response_body,
        })
    }

    fn handle_replace_base_pages(
        &self,
        db: &DbHandle,
        server_revision: String,
    ) -> Result<HttpResponse> {
        let (db_size, pages) = self.read_replace_base_pages(db)?;

        let header = PullUpdatesRespProtoBody {
            server_revision,
            db_size,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Pages as i32,
            apply_mode: PullUpdatesApplyMode::ReplaceBase as i32,
            mvcc_log: None,
            // Replace-base is only served from the MVCC logical flow here.
            protocol: PullUpdatesProtocol::MvccLogical as i32,
        };

        let mut response_body = Vec::new();
        let header_bytes = header.encode_to_vec();
        encode_length_delimited(&mut response_body, &header_bytes);

        for (page_id, page) in pages {
            let page_msg = PageData {
                page_id,
                encoded_page: Bytes::from(page),
            };
            let page_bytes = page_msg.encode_to_vec();
            encode_length_delimited(&mut response_body, &page_bytes);
        }

        Ok(HttpResponse {
            status: 200,
            content_type: "application/protobuf".to_string(),
            body: response_body,
        })
    }

    #[allow(clippy::type_complexity)]
    fn read_replace_base_pages(&self, db: &DbHandle) -> Result<(u64, Vec<(u64, Vec<u8>)>)> {
        let conn = db.conn.lock().unwrap();
        let wal_state = conn.wal_state()?;
        let frame_watermark = Some(wal_state.max_frame);
        let db_size = current_snapshot_db_size_pages(&conn, wal_state.max_frame)?;
        let pages_capacity = usize::try_from(db_size)
            .map_err(|_| anyhow!("database page count does not fit usize: {db_size}"))?;
        let mut pages = Vec::with_capacity(pages_capacity);

        for page_no in 1..=db_size {
            let page_no_u32 = u32::try_from(page_no)
                .map_err(|_| anyhow!("database page number does not fit u32: {page_no}"))?;
            let mut page = vec![0; PAGE_SIZE];
            let found =
                conn.try_wal_watermark_read_page(page_no_u32, &mut page, frame_watermark)?;
            if !found {
                return Err(anyhow!(
                    "database page {} is missing from replace-base snapshot",
                    page_no
                ));
            }
            pages.push((page_no - 1, page));
        }

        Ok((db_size, pages))
    }
}

struct HttpResponse {
    status: u16,
    content_type: String,
    body: Vec<u8>,
}

struct MvccLogSnapshot {
    end_offset: u64,
    crc_by_offset: Vec<(u64, u32)>,
}

impl MvccLogSnapshot {
    fn crc_seed_at(&self, offset: u64) -> Result<u32> {
        self.crc_by_offset
            .iter()
            .find_map(|(boundary, crc)| (*boundary == offset).then_some(*crc))
            .ok_or_else(|| {
                anyhow!("MVCC logical pull offset is not a transaction boundary: {offset}")
            })
    }
}

fn logical_log_path(db_path: &str) -> Result<PathBuf> {
    Ok(db_file_path(db_path)?.with_extension("db-log"))
}

fn is_in_memory_db_path(db_path: &str) -> bool {
    db_path == ":memory:"
}

fn db_file_path(db_path: &str) -> Result<PathBuf> {
    if is_in_memory_db_path(db_path) {
        return Err(anyhow!(
            "MVCC logical pull is not supported for in-memory sync server databases"
        ));
    }
    let path = if let Some(rest) = db_path.strip_prefix("file:") {
        rest.split_once('?').map_or(rest, |(path, _)| path)
    } else {
        db_path
    };
    Ok(PathBuf::from(path))
}

fn parse_mvcc_revision_offset(revision: &str, legacy_default: u64) -> Result<u64> {
    if revision.is_empty() {
        return Ok(0);
    }
    if let Some((generation, offset)) = revision.split_once(":o") {
        let generation = generation
            .strip_prefix('g')
            .ok_or_else(|| anyhow!("invalid MVCC pull revision generation: {revision}"))?
            .parse::<u64>()
            .map_err(|err| anyhow!("invalid MVCC pull revision generation: {revision}: {err}"))?;
        if generation != 1 {
            return Err(anyhow!(
                "sync_server supports only single-generation MVCC logical pulls: {revision}"
            ));
        }
        return offset
            .parse::<u64>()
            .map_err(|err| anyhow!("invalid MVCC pull revision offset: {revision}: {err}"));
    }
    // Older page bootstrap responses from this test server used WAL frame
    // numbers. Treat them as "the page snapshot already includes the current
    // logical log" so the required follow-up logical pull becomes a no-op.
    Ok(legacy_default)
}

fn scan_mvcc_log(log: &[u8]) -> Result<MvccLogSnapshot> {
    if log.is_empty() {
        return Ok(MvccLogSnapshot {
            end_offset: 0,
            crc_by_offset: vec![(0, 0)],
        });
    }
    if log.len() < MVCC_LOG_HEADER_SIZE {
        return Err(anyhow!(
            "truncated MVCC logical log header: len={} header_size={}",
            log.len(),
            MVCC_LOG_HEADER_SIZE
        ));
    }
    validate_mvcc_log_header(log)?;
    let mut running_crc = initial_mvcc_log_crc(log)?;
    let mut offset = MVCC_LOG_HEADER_SIZE;
    let mut crc_by_offset = vec![(MVCC_LOG_HEADER_SIZE as u64, running_crc)];

    while offset < log.len() {
        let Some((frame_end, frame_crc)) = read_mvcc_frame_boundary(log, offset, running_crc)?
        else {
            break;
        };
        running_crc = frame_crc;
        offset = frame_end;
        crc_by_offset.push((offset as u64, running_crc));
    }

    Ok(MvccLogSnapshot {
        end_offset: offset as u64,
        crc_by_offset,
    })
}

fn is_nonportable_mvcc_log_error(err: &anyhow::Error) -> bool {
    let message = err.to_string();
    message.starts_with("unsupported MVCC logical log version ")
}

fn validate_mvcc_log_header(log: &[u8]) -> Result<()> {
    if read_u32_le(log, 0)? != MVCC_LOG_MAGIC {
        return Err(anyhow!("invalid MVCC logical log magic"));
    }
    if log[4] != MVCC_LOG_VERSION {
        return Err(anyhow!("unsupported MVCC logical log version {}", log[4]));
    }
    if log[5] & 0b1111_1110 != 0 {
        return Err(anyhow!("invalid MVCC logical log header flags"));
    }
    let header_len = u16::from_le_bytes([log[6], log[7]]) as usize;
    if header_len != MVCC_LOG_HEADER_SIZE {
        return Err(anyhow!(
            "invalid MVCC logical log header length: {header_len}"
        ));
    }
    if log[MVCC_LOG_HEADER_RESERVED_START..MVCC_LOG_HEADER_CRC_START]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(anyhow!(
            "MVCC logical log header reserved bytes must be zero"
        ));
    }
    let stored_crc = read_u32_le(log, MVCC_LOG_HEADER_CRC_START)?;
    let mut crc_buf = [0u8; MVCC_LOG_HEADER_SIZE];
    crc_buf.copy_from_slice(&log[..MVCC_LOG_HEADER_SIZE]);
    crc_buf[MVCC_LOG_HEADER_CRC_START..MVCC_LOG_HEADER_SIZE].fill(0);
    let expected_crc = crc32c::crc32c(&crc_buf);
    if stored_crc != expected_crc {
        return Err(anyhow!("MVCC logical log header checksum mismatch"));
    }
    Ok(())
}

fn initial_mvcc_log_crc(log: &[u8]) -> Result<u32> {
    let salt = u64::from_le_bytes(
        log[MVCC_LOG_HEADER_SALT_START..MVCC_LOG_HEADER_SALT_END]
            .try_into()
            .expect("fixed-size salt slice"),
    );
    Ok(crc32c::crc32c(&salt.to_le_bytes()))
}

fn read_mvcc_frame_boundary(
    log: &[u8],
    offset: usize,
    running_crc: u32,
) -> Result<Option<(usize, u32)>> {
    if log.len() - offset < MVCC_TX_HEADER_SIZE + MVCC_TX_TRAILER_SIZE {
        return Ok(None);
    }
    let frame_magic = read_u32_le(log, offset)?;
    let has_extension_header = frame_magic == MVCC_TX_EXT_FRAME_MAGIC;
    if frame_magic != MVCC_TX_FRAME_MAGIC && !has_extension_header {
        return Err(anyhow!(
            "invalid MVCC logical log frame magic at offset {offset}: {frame_magic:#x}"
        ));
    }
    let header_size = if has_extension_header {
        MVCC_TX_EXT_HEADER_SIZE
    } else {
        MVCC_TX_HEADER_SIZE
    };
    if log.len() - offset < header_size + MVCC_TX_TRAILER_SIZE {
        return Ok(None);
    }
    let payload_size = usize::try_from(read_u64_le(log, offset + 4)?)
        .map_err(|_| anyhow!("MVCC logical log payload size overflows usize"))?;
    let extension_size = if has_extension_header {
        let extension_size = usize::try_from(read_u64_le(log, offset + 24)?)
            .map_err(|_| anyhow!("MVCC logical log extension size overflows usize"))?;
        let extension_record_count = read_u32_le(log, offset + 32)?;
        let frame_flags = read_u32_le(log, offset + 36)?;
        if frame_flags & !MVCC_TX_FRAME_FLAG_HAS_EXTENSION_BLOCK != 0 {
            return Err(anyhow!(
                "unsupported MVCC logical log frame flags at offset {offset}: {frame_flags:#x}"
            ));
        }
        if extension_size == 0 && extension_record_count != 0 {
            return Err(anyhow!(
                "MVCC logical log extension record count without extension block at offset {offset}"
            ));
        }
        if extension_size > 0 && frame_flags & MVCC_TX_FRAME_FLAG_HAS_EXTENSION_BLOCK == 0 {
            return Err(anyhow!(
                "MVCC logical log extension block missing flag at offset {offset}"
            ));
        }
        extension_size
    } else {
        0
    };
    let trailer_start = offset
        .checked_add(header_size)
        .and_then(|value| value.checked_add(payload_size))
        .and_then(|value| value.checked_add(extension_size))
        .ok_or_else(|| anyhow!("MVCC logical log frame offset overflow"))?;
    let frame_end = trailer_start
        .checked_add(MVCC_TX_TRAILER_SIZE)
        .ok_or_else(|| anyhow!("MVCC logical log frame end overflow"))?;
    if frame_end > log.len() {
        return Ok(None);
    }
    let expected_crc = crc32c::crc32c_append(running_crc, &log[offset..trailer_start]);
    let stored_crc = read_u32_le(log, trailer_start)?;
    if stored_crc != expected_crc {
        return Err(anyhow!(
            "MVCC logical log frame checksum mismatch at offset {offset}"
        ));
    }
    let end_magic = read_u32_le(log, trailer_start + 4)?;
    if end_magic != MVCC_TX_END_MAGIC {
        return Err(anyhow!(
            "invalid MVCC logical log frame end magic at offset {offset}"
        ));
    }
    Ok(Some((frame_end, stored_crc)))
}

fn read_u32_le(buf: &[u8], offset: usize) -> Result<u32> {
    let bytes = buf
        .get(offset..offset + 4)
        .ok_or_else(|| anyhow!("buffer too short for u32 at offset {offset}"))?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u64_le(buf: &[u8], offset: usize) -> Result<u64> {
    let bytes = buf
        .get(offset..offset + 8)
        .ok_or_else(|| anyhow!("buffer too short for u64 at offset {offset}"))?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn current_db_size_pages(conn: &Connection, max_frame: u64) -> Result<u64> {
    if max_frame > 0 {
        let frame_size = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;
        let mut last_frame = vec![0u8; frame_size];
        let last_info = conn.wal_get_frame(max_frame, &mut last_frame)?;
        Ok(last_info.db_size as u64)
    } else {
        Ok(0)
    }
}

fn current_snapshot_db_size_pages(conn: &Connection, max_frame: u64) -> Result<u64> {
    if max_frame > 0 {
        return current_db_size_pages(conn, max_frame);
    }

    let mut page = vec![0u8; PAGE_SIZE];
    if conn.try_wal_watermark_read_page(1, &mut page, Some(max_frame))? {
        Ok(db_size_from_page(&page) as u64)
    } else {
        Ok(0)
    }
}

fn db_size_from_page(page: &[u8]) -> u32 {
    u32::from_be_bytes(page[28..32].try_into().unwrap())
}

/// A client controls Content-Length, so the end of the body has to be
/// computed without trusting it to fit.
fn request_end(header_end: usize, content_length: usize) -> Result<usize> {
    (header_end + 4)
        .checked_add(content_length)
        .ok_or_else(|| anyhow!("HTTP request length overflows: {content_length}"))
}

fn find_header_end(data: &[u8], start: usize) -> Option<usize> {
    (start..data.len().saturating_sub(3)).find(|&i| &data[i..i + 4] == b"\r\n\r\n")
}

fn parse_content_length(headers: &str) -> Option<usize> {
    for line in headers.lines() {
        let lower = line.to_lowercase();
        if lower.starts_with("content-length:") {
            let value = line.split(':').nth(1)?.trim();
            return value.parse().ok();
        }
    }
    None
}

fn parse_http_request(data: &[u8]) -> Result<(String, String, Vec<u8>)> {
    let header_end = find_header_end(data, 0).ok_or_else(|| anyhow!("Invalid HTTP request"))?;
    let headers = String::from_utf8_lossy(&data[..header_end]);

    let first_line = headers
        .lines()
        .next()
        .ok_or_else(|| anyhow!("Empty request"))?;
    let parts: Vec<&str> = first_line.split_whitespace().collect();

    if parts.len() < 2 {
        return Err(anyhow!("Invalid request line"));
    }

    let method = parts[0].to_string();
    let path = parts[1].to_string();
    let body = data[header_end + 4..].to_vec();

    Ok((method, path, body))
}

fn text_response(status: u16, body: &str) -> HttpResponse {
    HttpResponse {
        status,
        content_type: "text/plain".to_string(),
        body: body.as_bytes().to_vec(),
    }
}

fn format_http_response(resp: &HttpResponse) -> Vec<u8> {
    let status_text = match resp.status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Unknown",
    };

    let header = format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: {}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n\
         Access-Control-Allow-Headers: *\r\n\
         Access-Control-Expose-Headers: *\r\n\
         \r\n",
        resp.status,
        status_text,
        resp.content_type,
        resp.body.len()
    );

    let mut result = header.into_bytes();
    result.extend_from_slice(&resp.body);
    result
}

#[derive(Debug, PartialEq, Eq)]
enum Route<'a> {
    Pipeline { db: Option<&'a str> },
    PullUpdates { db: Option<&'a str> },
    Options,
    NotFound,
}

fn parse_route<'a>(method: &str, path: &'a str) -> Route<'a> {
    if method == "OPTIONS" {
        return Route::Options;
    }
    if method != "POST" {
        return Route::NotFound;
    }
    match path {
        "/v2/pipeline" => return Route::Pipeline { db: None },
        "/pull-updates" => return Route::PullUpdates { db: None },
        _ => {}
    }
    let Some(rest) = path.strip_prefix("/db/") else {
        return Route::NotFound;
    };
    let Some((name, tail)) = rest.split_once('/') else {
        return Route::NotFound;
    };
    match tail {
        "v2/pipeline" => Route::Pipeline { db: Some(name) },
        "pull-updates" => Route::PullUpdates { db: Some(name) },
        _ => Route::NotFound,
    }
}

const WINDOWS_RESERVED_DEVICE_NAMES: [&str; 22] = [
    "con", "prn", "aux", "nul", "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8",
    "com9", "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9",
];

fn validate_db_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        && !is_windows_reserved_device_name(name)
}

fn is_windows_reserved_device_name(name: &str) -> bool {
    WINDOWS_RESERVED_DEVICE_NAMES
        .iter()
        .any(|reserved| name.eq_ignore_ascii_case(reserved))
}

fn db_path_for(base: &Path, name: &str) -> PathBuf {
    // Extensionless like sqld's layout: the files appear as data, data-wal,
    // data-shm (and data.db-log under MVCC) inside the database's directory.
    base.join(name).join("data")
}

fn open_db_handle(path: &Path, config: &OpenConfig) -> Result<Arc<DbHandle>> {
    let path_str = path.to_string_lossy().to_string();
    let (_io, db) = Database::open_new(
        &path_str,
        config.vfs.as_deref(),
        config.flags,
        config.db_opts.turso_cli(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect()?;
    conn.wal_auto_actions_disable();
    Ok(Arc::new(DbHandle {
        conn: Mutex::new(conn),
        path: path_str,
    }))
}

fn encode_length_delimited(output: &mut Vec<u8>, data: &[u8]) {
    let mut len = data.len();
    while len >= 0x80 {
        output.push((len as u8) | 0x80);
        len >>= 7;
    }
    output.push(len as u8);
    output.extend_from_slice(data);
}

fn convert_value_to_core(value: &Value) -> CoreValue {
    match value {
        Value::None | Value::Null => CoreValue::Null,
        Value::Integer { value } => CoreValue::from_i64(*value),
        Value::Float { value } => CoreValue::from_f64(*value),
        Value::Text { value } => CoreValue::Text(turso_core::types::Text {
            value: std::borrow::Cow::Owned(value.clone()),
            subtype: turso_core::types::TextSubtype::Text,
        }),
        Value::Blob { value } => CoreValue::Blob(value.to_vec()),
    }
}

fn convert_core_to_value(value: CoreValue) -> Value {
    match value {
        CoreValue::Null => Value::Null,
        CoreValue::Numeric(turso_core::Numeric::Integer(v)) => Value::Integer { value: v },
        CoreValue::Numeric(turso_core::Numeric::Float(v)) => Value::Float {
            value: f64::from(v),
        },
        CoreValue::Text(t) => Value::Text {
            value: t.value.to_string(),
        },
        CoreValue::Blob(b) => Value::Blob {
            value: Bytes::from(b),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn validates_database_names() {
        for ok in [
            "db1",
            "a-b_c",
            "A1",
            "x",
            "console",
            "common",
            "com",
            "com10",
            "lpt",
            "nullable",
            &"n".repeat(128),
        ] {
            assert!(validate_db_name(ok), "expected {ok:?} to be valid");
        }
        for bad in [
            "",
            "..",
            "../x",
            "a/b",
            "a\\b",
            ".hidden",
            "a.b",
            "a%2fb",
            "a b",
            "nul",
            "NUL",
            "Con",
            "aux",
            "prn",
            "com1",
            "lpt9",
            &"n".repeat(129),
        ] {
            assert!(!validate_db_name(bad), "expected {bad:?} to be rejected");
        }
    }

    /// Mirrors the read loop: the terminator must be found whatever the chunk
    /// boundaries, including when it straddles two reads.
    #[test]
    fn finds_header_end_across_read_boundaries() {
        let request = b"POST / HTTP/1.1\r\nHost: x\r\n\r\nbody".to_vec();
        let expected = find_header_end(&request, 0).expect("terminator is present");

        for chunk in 1..=request.len() {
            let mut data = Vec::new();
            let mut found = None;
            for piece in request.chunks(chunk) {
                let unscanned = data.len().saturating_sub(3);
                data.extend_from_slice(piece);
                if let Some(end) = find_header_end(&data, unscanned) {
                    found = Some(end);
                    break;
                }
            }
            assert_eq!(found, Some(expected), "missed terminator at chunk {chunk}");
        }
    }

    #[test]
    fn rejects_content_length_that_overflows() {
        assert!(request_end(0, usize::MAX).is_err());
        assert_eq!(request_end(10, 5).unwrap(), 19);
    }

    #[test]
    fn parses_single_and_multi_routes() {
        assert_eq!(
            parse_route("POST", "/v2/pipeline"),
            Route::Pipeline { db: None }
        );
        assert_eq!(
            parse_route("POST", "/pull-updates"),
            Route::PullUpdates { db: None }
        );
        assert_eq!(
            parse_route("POST", "/db/db1/v2/pipeline"),
            Route::Pipeline { db: Some("db1") }
        );
        assert_eq!(
            parse_route("POST", "/db/db1/pull-updates"),
            Route::PullUpdates { db: Some("db1") }
        );
        assert_eq!(parse_route("OPTIONS", "/anything"), Route::Options);
        assert_eq!(parse_route("GET", "/v2/pipeline"), Route::NotFound);
        assert_eq!(parse_route("POST", "/nope"), Route::NotFound);
        assert_eq!(parse_route("POST", "/db/a/b/v2/pipeline"), Route::NotFound);
        assert_eq!(
            parse_route("POST", "/db//v2/pipeline"),
            Route::Pipeline { db: Some("") }
        );
    }

    #[test]
    fn each_database_resolves_its_own_files() {
        let base = Path::new("/tmp/dbs");
        let db1 = db_path_for(base, "db1");
        let db2 = db_path_for(base, "db2");

        assert_eq!(db1, Path::new("/tmp/dbs/db1/data"));
        assert_ne!(db1, db2);

        let log1 = logical_log_path(&db1.to_string_lossy()).unwrap();
        let log2 = logical_log_path(&db2.to_string_lossy()).unwrap();
        assert_ne!(log1, log2, "databases must not share a logical log");
        assert_eq!(log1, Path::new("/tmp/dbs/db1/data.db-log"));
    }

    fn dir_server(base: &Path) -> TursoSyncServer {
        TursoSyncServer::new_dir(
            "127.0.0.1:0".to_string(),
            base.to_path_buf(),
            Arc::new(AtomicUsize::new(0)),
            OpenConfig {
                vfs: None,
                flags: OpenFlags::default(),
                db_opts: DatabaseOpts::new(),
            },
        )
        .unwrap()
    }

    #[test]
    fn refuses_a_database_directory_that_escapes_the_served_tree() {
        let base = std::env::temp_dir().join(format!("turso-sync-escape-{}", std::process::id()));
        let outside =
            std::env::temp_dir().join(format!("turso-sync-outside-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let _ = std::fs::remove_dir_all(&outside);
        std::fs::create_dir_all(&base).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, base.join("escaped")).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_dir(&outside, base.join("escaped")).unwrap();

        let server = dir_server(&base);
        let Err(refused) = server.resolve_db(Some("escaped")) else {
            panic!("a symlinked database directory must be refused");
        };
        assert_eq!(refused.status, 404);
        assert!(
            !outside.join("data").exists(),
            "a refused name must not create files outside the served tree"
        );

        std::fs::remove_dir_all(&base).unwrap();
        std::fs::remove_dir_all(&outside).unwrap();
    }
}
