use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use turso_core::SqliteDialect;

use turso_core::{Buffer, Completion, DatabaseStorage, LimboError, OpenDbAsyncState, OpenFlags};

use crate::{
    database_replay_generator::DatabaseReplayGenerator,
    database_sync_engine_io::SyncEngineIo,
    database_sync_lazy_storage::LazyDatabaseStorage,
    database_sync_operations::{
        acquire_slot,
        apply_logical_transactions_file_without_commit_excluding_client_txns_with_table_map_and_stats,
        apply_transformation, bootstrap_db_file, connect_untracked, count_local_changes, has_table,
        is_logically_replayable_table, list_user_tables, max_local_change_id, pull_updates_v1,
        push_logical_changes, read_last_change_id, read_logical_replay_table_map, read_wal_salt,
        reset_wal_file, should_replay_local_change, sync_file, update_last_change_id,
        wait_all_results, wal_apply_from_file, wal_pull_to_file, PullUpdatesV1Result,
        SyncEngineIoStats, SyncOperationCtx, PAGE_SIZE, WAL_FRAME_HEADER, WAL_FRAME_SIZE,
    },
    database_tape::{
        run_stmt_ignore_rows, try_wal_watermark_read_page, DatabaseChangesIteratorMode,
        DatabaseChangesIteratorOpts, DatabaseReplaySession, DatabaseReplaySessionOpts,
        DatabaseTape, DatabaseTapeOpts, DatabaseWalSession, CDC_PRAGMA_NAME,
    },
    errors::Error,
    io_operations::IoOperations,
    types::{
        Coro, DatabaseMetadata, DatabasePullRevision, DatabaseRowTransformResult,
        DatabaseSavedConfiguration, DatabaseSyncEngineProtocolVersion, DatabaseTapeOperation,
        DatabaseTapeRowChange, DatabaseTapeRowChangeType, DbChangesStatus, DbChangesStreamKind,
        PartialSyncOpts, RemotePullProtocol, SyncEngineIoResult, SyncEngineStats,
        DATABASE_METADATA_VERSION,
    },
    wal_session::WalSession,
    Result,
};

#[derive(Clone, Debug)]
pub struct DatabaseSyncEngineOpts {
    pub remote_url: Option<String>,
    pub client_name: String,
    pub tables_ignore: Vec<String>,
    pub use_transform: bool,
    pub wal_pull_batch_size: u64,
    pub long_poll_timeout: Option<std::time::Duration>,
    pub protocol_version_hint: DatabaseSyncEngineProtocolVersion,
    pub bootstrap_if_empty: bool,
    pub reserved_bytes: usize,
    /// Experimental [`turso_core::DatabaseOpts`] applied whenever the sync
    /// engine opens the local database itself (the main connection in
    /// [`DatabaseSyncEngine::create_db`] and the revert connection in
    /// [`DatabaseSyncEngine::open_revert_db_conn`]). Bindings translate their
    /// user-facing experimental feature list into these options. Note that
    /// callers which open the main database on their own (e.g. the sdk-kit
    /// path) must still apply the same options there — this field only governs
    /// databases opened internally by the engine.
    pub db_opts: turso_core::DatabaseOpts,
    pub partial_sync_opts: Option<PartialSyncOpts>,
    /// Base64-encoded encryption key for the Turso Cloud database
    pub remote_encryption_key: Option<String>,
    /// When set, [`push_changes_to_remote`] sends the local change set to the
    /// remote in multiple HTTP batches, sealing the current batch as soon as it
    /// has accumulated >= `push_operations_threshold` operations *and* the
    /// next batch boundary lines up with a transaction boundary in the local
    /// CDC log. Splits never happen mid-transaction. `None` preserves the
    /// previous behaviour of pushing the whole change set in one batch.
    pub push_operations_threshold: Option<usize>,
    /// Optional hint, in bytes, to chunk the initial bootstrap download into
    /// multiple `/pull-updates` HTTP requests using the `server_pages_selector`
    /// bitmap. Each chunk covers the smallest contiguous range of pages whose
    /// total size is >= `pull_bytes_threshold`. `None` (default) bootstraps in
    /// a single HTTP round-trip. Currently applied only to the bootstrap phase
    /// — incremental pulls are unaffected. **No-op when partial-sync uses the
    /// `Query` bootstrap strategy** — the server picks the page set, so the
    /// client can't chunk it locally.
    pub pull_bytes_threshold: Option<usize>,
    /// Sync-protocol override for incremental pulls.
    ///
    /// `None` (default) auto-detects the remote protocol from the first
    /// pull-updates response and persists it in the metadata. `Some(true)`
    /// forces raw MVCC logical-log streams; `Some(false)` forces page streams.
    /// Explicit values exist for tests and as an escape hatch — users should
    /// not need to set this.
    pub logical_mvcc_pull: Option<bool>,
}

pub struct DataStats {
    pub written_bytes: AtomicUsize,
    pub read_bytes: AtomicUsize,
}

impl Default for DataStats {
    fn default() -> Self {
        Self::new()
    }
}

impl DataStats {
    pub fn new() -> Self {
        Self {
            written_bytes: AtomicUsize::new(0),
            read_bytes: AtomicUsize::new(0),
        }
    }
    pub fn write(&self, size: usize) {
        self.written_bytes.fetch_add(size, Ordering::SeqCst);
    }
    pub fn read(&self, size: usize) {
        self.read_bytes.fetch_add(size, Ordering::SeqCst);
    }
}

pub struct DatabaseSyncEngine<IO: SyncEngineIo> {
    io: Arc<dyn turso_core::IO>,
    sync_engine_io: SyncEngineIoStats<IO>,
    db_file: Arc<dyn turso_core::storage::database::DatabaseStorage>,
    main_tape: DatabaseTape,
    main_db_wal_path: String,
    revert_db_wal_path: String,
    main_db_path: String,
    meta_path: String,
    changes_file: Arc<Mutex<Option<Arc<dyn turso_core::File>>>>,
    opts: DatabaseSyncEngineOpts,
    meta: Mutex<DatabaseMetadata>,
    client_unique_id: String,
}

fn db_size_from_page(page: &[u8]) -> u32 {
    u32::from_be_bytes(page[28..28 + 4].try_into().unwrap())
}
fn is_memory(main_db_path: &str) -> bool {
    main_db_path == ":memory:"
}
pub fn sync_database_file_paths(main_db_path: &str) -> Vec<String> {
    vec![
        // Core opens the database and its WAL.
        main_db_path.to_string(),
        create_main_db_wal_path(main_db_path),
        // Sync keeps rollback data, metadata, and downloaded changes separately.
        create_revert_db_wal_path(main_db_path),
        create_meta_path(main_db_path),
        create_changes_path(main_db_path),
        // MVCC may be selected only after the remote protocol is known.
        create_main_db_log_path(main_db_path),
    ]
}
fn create_main_db_wal_path(main_db_path: &str) -> String {
    format!("{main_db_path}-wal")
}
fn create_main_db_log_path(main_db_path: &str) -> String {
    std::path::Path::new(main_db_path)
        .with_extension("db-log")
        .to_string_lossy()
        .to_string()
}
fn create_revert_db_wal_path(main_db_path: &str) -> String {
    format!("{main_db_path}-wal-revert")
}
fn create_meta_path(main_db_path: &str) -> String {
    format!("{main_db_path}-info")
}
fn create_changes_path(main_db_path: &str) -> String {
    format!("{main_db_path}-changes")
}
fn create_replace_base_marker_path(main_db_path: &str) -> String {
    format!("{main_db_path}-replace-base-apply")
}
fn replace_base_backup_path(main_db_path: &str, name: &str) -> String {
    format!("{main_db_path}-replace-base-apply-{name}.backup")
}

#[cfg(test)]
thread_local! {
    static REPLACE_BASE_LOCAL_REPLAY_FAILURE_AFTER: std::cell::Cell<usize> =
        const { std::cell::Cell::new(usize::MAX) };
}

#[cfg(test)]
fn replace_base_local_replay_failure_after() -> usize {
    REPLACE_BASE_LOCAL_REPLAY_FAILURE_AFTER.with(|value| value.get())
}

fn maybe_inject_replace_base_local_replay_failure(
    replace_base_pages: bool,
    replay_index: usize,
) -> Result<()> {
    #[cfg(test)]
    {
        if replace_base_pages && replay_index == replace_base_local_replay_failure_after() {
            return Err(Error::DatabaseSyncEngineError(format!(
                "injected replace-base local replay failure at change {replay_index}"
            )));
        }
    }
    let _ = (replace_base_pages, replay_index);
    Ok(())
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct ReplaceBaseBackupFile {
    path: String,
    backup_path: String,
    present: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct ReplaceBaseBackupManifest {
    version: u32,
    operation: String,
    main_db_path: String,
    previous_synced_revision: Option<DatabasePullRevision>,
    files: Vec<ReplaceBaseBackupFile>,
}

struct ReplaceBaseApplyGuard<IO: SyncEngineIo> {
    io: Arc<dyn turso_core::IO>,
    sync_engine_io: SyncEngineIoStats<IO>,
    main_db_path: String,
    manifest: ReplaceBaseBackupManifest,
}

/// caller has no access to the memory io - so we handle it here implicitly
/// ideally, we should add necessary methods to the turso_core::IO trait - but so far I am struggling with nice interface to do that
/// so, I decided to keep a little bit of mess in sync-engine for a little bit longer
async fn full_read<Ctx, IO: SyncEngineIo>(
    coro: &Coro<Ctx>,
    io: Option<Arc<dyn turso_core::IO>>,
    sync_engine_io: Arc<IO>,
    path: &str,
    is_memory: bool,
) -> Result<Option<Vec<u8>>> {
    if !is_memory {
        let completion = sync_engine_io.full_read(path)?;
        let data = wait_all_results(coro, &completion, None).await?;
        if data.is_empty() {
            return Ok(None);
        } else {
            return Ok(Some(data));
        }
    }
    let Some(io) = io else {
        return Err(Error::DatabaseSyncEngineError(
            "MemoryIO must be set".to_string(),
        ));
    };
    let Ok(file) = io.open_file(path, OpenFlags::None, false) else {
        return Ok(None);
    };
    let mut content = Vec::new();
    let mut offset = 0;
    let buffer = Arc::new(Buffer::new_temporary(4096));
    let read_len = Arc::new(Mutex::new(0));
    loop {
        let c = Completion::new_read(buffer.clone(), {
            let read_len = read_len.clone();
            move |r| {
                *read_len.lock().unwrap() = r.expect("memory io must not fail").1;
                None
            }
        });
        let read = file.pread(offset, c).expect("memory io must not fail");
        assert!(read.finished(), "memory io must complete immediately");
        let read_len = *read_len.lock().unwrap();
        if read_len == 0 {
            break;
        }
        content.extend_from_slice(&buffer.as_slice()[0..read_len as usize]);
        offset += read_len as u64;
    }
    Ok(Some(content))
}

/// caller has no access to the memory io - so we handle it here implicitly
/// ideally, we should add necessary methods to the turso_core::IO trait - but so far I am struggling with nice interface to do that
/// so, I decided to keep a little bit of mess in sync-engine for a little bit longer
async fn full_write<Ctx, IO: SyncEngineIo>(
    coro: &Coro<Ctx>,
    io: Arc<dyn turso_core::IO>,
    sync_engine_io: Arc<IO>,
    path: &str,
    is_memory: bool,
    content: Vec<u8>,
) -> Result<()> {
    if !is_memory {
        let completion = sync_engine_io.full_write(path, content)?;
        wait_all_results(coro, &completion, None).await?;
        return Ok(());
    }
    let file = io.open_file(path, OpenFlags::Create, false)?;
    let trunc = file
        .truncate(0, Completion::new_trunc(|_| {}))
        .expect("memory io must not fail");
    assert!(trunc.finished(), "memory io must complete immediately");
    let write = file
        .pwrite(
            0,
            Arc::new(Buffer::new(content)),
            Completion::new_write(|_| {}),
        )
        .expect("memory io must nof fail");
    assert!(write.finished(), "memory io must complete immediately");
    Ok(())
}

async fn sync_path_if_present<Ctx>(
    coro: &Coro<Ctx>,
    io: &Arc<dyn turso_core::IO>,
    path: &str,
) -> Result<()> {
    let Some(file) = io.try_open(path)? else {
        return Ok(());
    };
    let sync = file.sync(
        Completion::new_sync(|_| {}),
        turso_core::io::FileSyncType::Fsync,
    )?;
    while !sync.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    Ok(())
}

fn remove_file_if_present(io: &Arc<dyn turso_core::IO>, path: &str) -> Result<()> {
    if io.try_open(path)?.is_some() {
        io.remove_file(path)?;
    }
    Ok(())
}

/// Fsync the directory that contains `path`, making newly created or removed
/// directory entries (the replace-base marker and backups) durable across a
/// crash. Fsyncing a file's contents does not guarantee its *directory entry*
/// survives a crash, so the guard's crash-safety ordering (marker/backups
/// created before the apply mutates real files; marker removed before backups
/// during cleanup) is only meaningful once the containing directory is synced.
///
/// The replace-base guard requires real durable storage (memory paths are
/// rejected in [`ReplaceBaseApplyGuard::create`]), so a direct `std::fs`
/// directory fsync is appropriate here even though normal file IO flows through
/// [`turso_core::IO`], which has no directory concept. On platforms without
/// POSIX directory-fsync semantics this is a no-op.
fn sync_parent_dir(path: &str) -> Result<()> {
    #[cfg(unix)]
    {
        let parent = std::path::Path::new(path)
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(std::path::Path::to_path_buf)
            .unwrap_or_else(|| std::path::PathBuf::from("."));
        let dir = std::fs::File::open(&parent).map_err(|err| {
            Error::DatabaseSyncEngineError(format!(
                "failed to open directory {parent:?} for fsync: {err}"
            ))
        })?;
        dir.sync_all().map_err(|err| {
            Error::DatabaseSyncEngineError(format!("failed to fsync directory {parent:?}: {err}"))
        })?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

async fn copy_sync_engine_file<Ctx, IO: SyncEngineIo>(
    coro: &Coro<Ctx>,
    io: Arc<dyn turso_core::IO>,
    sync_engine_io: Arc<IO>,
    source_path: &str,
    target_path: &str,
    is_memory: bool,
) -> Result<bool> {
    let Some(content) = full_read(
        coro,
        Some(io.clone()),
        sync_engine_io.clone(),
        source_path,
        is_memory,
    )
    .await?
    else {
        remove_file_if_present(&io, target_path)?;
        return Ok(false);
    };
    full_write(
        coro,
        io.clone(),
        sync_engine_io,
        target_path,
        is_memory,
        content,
    )
    .await?;
    sync_path_if_present(coro, &io, target_path).await?;
    Ok(true)
}

impl<IO: SyncEngineIo> ReplaceBaseApplyGuard<IO> {
    #[allow(dead_code)]
    async fn create<Ctx>(
        coro: &Coro<Ctx>,
        io: Arc<dyn turso_core::IO>,
        sync_engine_io: SyncEngineIoStats<IO>,
        main_db_path: &str,
        previous_synced_revision: Option<DatabasePullRevision>,
    ) -> Result<Self> {
        if is_memory(main_db_path) {
            return Err(Error::DatabaseSyncEngineError(
                "replace-base local replay preservation requires durable local storage".to_string(),
            ));
        }
        let is_memory = is_memory(main_db_path);
        let file_specs = [
            (main_db_path.to_string(), "main-db"),
            (create_main_db_wal_path(main_db_path), "main-wal"),
            (create_main_db_log_path(main_db_path), "main-log"),
            (create_revert_db_wal_path(main_db_path), "revert-wal"),
            (create_meta_path(main_db_path), "metadata"),
        ];
        let mut files = Vec::with_capacity(file_specs.len());
        for (path, name) in file_specs {
            let backup_path = replace_base_backup_path(main_db_path, name);
            let present = copy_sync_engine_file(
                coro,
                io.clone(),
                sync_engine_io.io.clone(),
                &path,
                &backup_path,
                is_memory,
            )
            .await?;
            files.push(ReplaceBaseBackupFile {
                path,
                backup_path,
                present,
            });
        }
        let manifest = ReplaceBaseBackupManifest {
            version: 1,
            operation: "replace_base_apply".to_string(),
            main_db_path: main_db_path.to_string(),
            previous_synced_revision,
            files,
        };
        let marker_path = create_replace_base_marker_path(main_db_path);
        full_write(
            coro,
            io.clone(),
            sync_engine_io.io.clone(),
            &marker_path,
            is_memory,
            serde_json::to_vec(&manifest)?,
        )
        .await?;
        sync_path_if_present(coro, &io, &marker_path).await?;
        // Barrier: the marker and all backups (contents already fsynced above and
        // in `copy_sync_engine_file`) must have durable *directory entries* before
        // the caller begins mutating the real db/wal/log/meta files. Otherwise a
        // crash mid-apply could leave the real files partially replaced with no
        // recoverable marker to roll them back.
        sync_parent_dir(main_db_path)?;
        Ok(Self {
            io,
            sync_engine_io,
            main_db_path: main_db_path.to_string(),
            manifest,
        })
    }

    async fn recover_pending<Ctx>(
        coro: &Coro<Ctx>,
        io: Arc<dyn turso_core::IO>,
        sync_engine_io: SyncEngineIoStats<IO>,
        main_db_path: &str,
    ) -> Result<bool> {
        let marker_path = create_replace_base_marker_path(main_db_path);
        let is_memory = is_memory(main_db_path);
        let Some(marker) = full_read(
            coro,
            Some(io.clone()),
            sync_engine_io.io.clone(),
            &marker_path,
            is_memory,
        )
        .await?
        else {
            return Ok(false);
        };
        let manifest: ReplaceBaseBackupManifest =
            serde_json::from_slice(&marker).map_err(|err| {
                Error::DatabaseSyncEngineError(format!(
                    "failed to parse pending replace-base recovery marker {marker_path}: {err}"
                ))
            })?;
        tracing::warn!(
            "recover_pending_replace_base(path={}): restoring local files from pending marker",
            main_db_path
        );
        Self {
            io,
            sync_engine_io,
            main_db_path: main_db_path.to_string(),
            manifest,
        }
        .restore(coro)
        .await?;
        Ok(true)
    }

    async fn restore<Ctx>(&self, coro: &Coro<Ctx>) -> Result<()> {
        let is_memory = is_memory(&self.main_db_path);
        for file in &self.manifest.files {
            if file.present {
                let restored = copy_sync_engine_file(
                    coro,
                    self.io.clone(),
                    self.sync_engine_io.io.clone(),
                    &file.backup_path,
                    &file.path,
                    is_memory,
                )
                .await?;
                if !restored {
                    return Err(Error::DatabaseSyncEngineError(format!(
                        "replace-base recovery backup is missing: {}",
                        file.backup_path
                    )));
                }
            } else {
                remove_file_if_present(&self.io, &file.path)?;
            }
        }
        self.cleanup(coro).await
    }

    async fn cleanup<Ctx>(&self, coro: &Coro<Ctx>) -> Result<()> {
        // Remove the marker BEFORE the backups. The marker is the sole signal
        // that "backups are authoritative and must be restored on recovery", so
        // its removal must be made durable before any backup disappears.
        // Removing backups first would let a crash in this window leave a marker
        // pointing at missing backups, which fails `restore` on the next open and
        // bricks the database. Once the marker is durably gone, leftover backups
        // are merely orphaned files (harmless, overwritten by the next apply).
        remove_file_if_present(
            &self.io,
            &create_replace_base_marker_path(&self.main_db_path),
        )?;
        sync_parent_dir(&self.main_db_path)?;
        for file in &self.manifest.files {
            remove_file_if_present(&self.io, &file.backup_path)?;
        }
        sync_path_if_present(coro, &self.io, &self.main_db_path).await?;
        Ok(())
    }

    #[allow(dead_code)]
    async fn mark_complete<Ctx>(&mut self, coro: &Coro<Ctx>) -> Result<()> {
        self.cleanup(coro).await
    }

    #[allow(dead_code)]
    async fn restore_after_error<Ctx>(&self, coro: &Coro<Ctx>, apply_error: Error) -> Error {
        match self.restore(coro).await {
            Ok(()) => apply_error,
            Err(restore_error) => Error::DatabaseSyncEngineError(format!(
                "{apply_error}; additionally failed to restore replace-base backup: {restore_error}"
            )),
        }
    }
}

/// Applies the explicit `logical_mvcc_pull` override on top of the persisted
/// (detected) remote protocol. `None` (auto) defers entirely to detection.
fn resolve_remote_pull_protocol(
    logical_mvcc_pull_override: Option<bool>,
    persisted: RemotePullProtocol,
) -> RemotePullProtocol {
    match logical_mvcc_pull_override {
        Some(true) => RemotePullProtocol::MvccLogical,
        Some(false) => RemotePullProtocol::Pages,
        None => persisted,
    }
}

/// MVCC logical sync is incompatible with partial sync and encrypted remotes.
/// This used to silently downgrade to page pulls, but the server rejects page
/// incremental pulls for MVCC databases, so a silent downgrade just defers the
/// failure to an opaque protocol error on every pull. Fail fast and loudly
/// instead. Never called for page-mode (legacy) replicas.
fn ensure_logical_mvcc_pull_supported(
    partial_sync_active: bool,
    remote_encryption_key: Option<&str>,
) -> Result<()> {
    if partial_sync_active {
        return Err(Error::DatabaseSyncEngineError(
            "MVCC logical sync does not support partial sync; disable partialSyncExperimental for this database".to_string(),
        ));
    }
    if remote_encryption_key.is_some() {
        return Err(Error::DatabaseSyncEngineError(
            "MVCC logical sync does not support encrypted remote databases".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn should_request_logical_pull(
    logical_mvcc_pull_active: bool,
    revision: &Option<DatabasePullRevision>,
) -> bool {
    logical_mvcc_pull_active && matches!(revision, Some(DatabasePullRevision::V1 { .. }))
}

#[cfg(test)]
fn ensure_stream_kind_can_use_legacy_page_apply(stream_kind: DbChangesStreamKind) -> Result<()> {
    match stream_kind {
        DbChangesStreamKind::LegacyPages | DbChangesStreamKind::Pages => Ok(()),
        DbChangesStreamKind::Logical => Err(Error::DatabaseSyncEngineError(
            "logical MVCC apply is not wired into the sync loop yet".to_string(),
        )),
        DbChangesStreamKind::ReplaceBasePages => Err(Error::DatabaseSyncEngineError(
            "replace-base page apply is not wired into the sync loop yet".to_string(),
        )),
    }
}

fn stream_kind_for_pull_updates_v1_result(result: &PullUpdatesV1Result) -> DbChangesStreamKind {
    match result {
        PullUpdatesV1Result::Logical { .. } => DbChangesStreamKind::Logical,
        PullUpdatesV1Result::Pages {
            replace_base: false,
        } => DbChangesStreamKind::Pages,
        PullUpdatesV1Result::Pages { replace_base: true } => DbChangesStreamKind::ReplaceBasePages,
    }
}

/// Executes schema-sensitive SQL and retries transient schema-cookie races.
///
/// Replace-base and raw-page apply can swap database files underneath an open
/// connection. A `SchemaUpdated` error in that window usually means the SQL
/// applied or observed the new schema boundary; reparsing and retrying lets the
/// connection converge without masking persistent SQL errors.
#[allow(dead_code)]
fn execute_with_schema_retry(conn: &Arc<turso_core::Connection>, sql: &str) -> Result<()> {
    for attempt in 0..=4 {
        match conn.execute(sql) {
            Ok(()) => return Ok(()),
            Err(LimboError::SchemaUpdated) if attempt < 4 => {
                force_reparse_schema_with_retry(conn)?;
                publish_schema_after_external_restore(conn, "schema retry")?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// Forces a schema reparse with a small bounded retry for restore races.
#[allow(dead_code)]
fn force_reparse_schema_with_retry(conn: &Arc<turso_core::Connection>) -> Result<()> {
    for attempt in 0..=2 {
        match conn.force_reparse_schema() {
            Ok(()) => return Ok(()),
            Err(LimboError::SchemaUpdated) if attempt < 2 => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// Reparse only this connection's own schema after a raw page/WAL replay,
/// without publishing it to the shared database cache. The caller must publish
/// once the schema is complete (see [`Connection::force_reparse_schema_without_publish`]).
fn force_reparse_schema_without_publish_with_retry(
    conn: &Arc<turso_core::Connection>,
) -> Result<()> {
    for attempt in 0..=2 {
        match conn.force_reparse_schema_without_publish() {
            Ok(()) => return Ok(()),
            Err(LimboError::SchemaUpdated) if attempt < 2 => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// Refresh this connection's own schema from freshly replayed pages without
/// publishing to the shared cache. Publishing is deferred to the caller so the
/// shared schema is only ever updated once the CDC table has been recreated,
/// preventing concurrent statements from observing a `turso_cdc`-less schema.
fn refresh_schema_after_raw_wal_replay_local(
    conn: &Arc<turso_core::Connection>,
    context: &str,
) -> Result<()> {
    conn.get_pager().clear_page_cache(true);
    force_reparse_schema_without_publish_with_retry(conn).map_err(|error| {
        Error::DatabaseSyncEngineError(
            format!("failed to refresh schema after {context}: {error}",),
        )
    })
}

/// Refreshes an open connection after sync has restored files outside SQLite.
///
/// This rebuilds WAL/MVCC state, clears cached pages, reparses schema, and
/// publishes that schema so subsequent replay uses the restored database view.
#[allow(dead_code)]
fn reload_connection_after_external_restore(
    conn: &Arc<turso_core::Connection>,
    context: &str,
) -> Result<()> {
    conn.discard_main_mvcc_tx_after_external_restore();
    match conn.reload_wal_after_external_restore() {
        Ok(()) => {}
        Err(LimboError::SchemaUpdated) => {
            force_reparse_schema_with_retry(conn).map_err(|error| {
                Error::DatabaseSyncEngineError(format!(
                    "failed to refresh schema before retrying WAL reload after {context}: {error}",
                ))
            })?;
            conn.reload_wal_after_external_restore().map_err(|error| {
                Error::DatabaseSyncEngineError(format!(
                    "failed to reload WAL state after {context}: {error}",
                ))
            })?;
        }
        Err(error) => {
            return Err(Error::DatabaseSyncEngineError(format!(
                "failed to reload WAL state after {context}: {error}",
            )));
        }
    }
    conn.get_pager().clear_page_cache(true);
    force_reparse_schema_with_retry(conn).map_err(|error| {
        Error::DatabaseSyncEngineError(
            format!("failed to refresh schema after {context}: {error}",),
        )
    })?;
    publish_schema_after_external_restore(conn, context)?;
    Ok(())
}

fn publish_schema_after_sync_checkpoint(conn: &Arc<turso_core::Connection>) -> Result<()> {
    conn.get_pager().clear_page_cache(true);
    match force_reparse_schema_with_retry(conn) {
        Ok(()) => match conn.publish_schema_after_external_restore() {
            Ok(()) => Ok(()),
            Err(LimboError::Busy) => {
                tracing::debug!(
                        "checkpoint: schema publish after sync checkpoint skipped due to local busy connection"
                    );
                Ok(())
            }
            Err(error) => Err(Error::DatabaseSyncEngineError(format!(
                "failed to publish schema after sync checkpoint: {error}",
            ))),
        },
        Err(Error::TursoError(LimboError::Busy)) => {
            tracing::debug!(
                "checkpoint: schema refresh after sync checkpoint skipped due to local busy connection"
            );
            Ok(())
        }
        Err(error) => Err(Error::DatabaseSyncEngineError(format!(
            "failed to refresh schema after sync checkpoint: {error}",
        ))),
    }
}

fn publish_schema_after_external_restore(
    conn: &Arc<turso_core::Connection>,
    context: &str,
) -> Result<()> {
    conn.publish_schema_after_external_restore()
        .map_err(|error| {
            Error::DatabaseSyncEngineError(format!(
                "failed to publish schema after {context}: {error}",
            ))
        })
}

/// Marks all current CDC rows as already observed by `client_id`.
#[allow(dead_code)]
async fn acknowledge_existing_cdc_for_client<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    client_id: &str,
    context: &str,
) -> Result<()> {
    let Some(max_change_id) = max_local_change_id(coro, conn).await? else {
        tracing::info!("{context}: no existing local CDC rows to acknowledge");
        return Ok(());
    };
    let (pull_gen, current_change_id) = read_last_change_id(coro, conn, client_id).await?;
    if current_change_id.is_some_and(|change_id| change_id >= max_change_id) {
        tracing::info!(
            "{context}: local CDC already acknowledged through change_id={max_change_id}"
        );
        return Ok(());
    }
    tracing::info!(
        "{context}: acknowledging imported local CDC through change_id={max_change_id} for client_id={client_id}"
    );
    update_last_change_id(coro, conn, client_id, pull_gen, max_change_id).await
}

/// Recreates the local sync metadata table after page-level restore.
///
/// Raw page replay can replace the table contents along with ordinary pages.
/// This helper writes the high-water mark that should survive the restore.
#[allow(dead_code)]
async fn rebuild_local_sync_metadata_table<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    client_id: &str,
    pull_gen: i64,
    change_id: i64,
) -> Result<()> {
    publish_schema_after_external_restore(conn, "local sync metadata rebuild")?;
    update_last_change_id(coro, conn, client_id, pull_gen, change_id).await
}

/// Drives a core I/O completion from the sync coroutine executor.
#[allow(dead_code)]
async fn drive_core_completion<Ctx>(
    coro: &Coro<Ctx>,
    io: &Arc<dyn turso_core::IO>,
    completion: Completion,
) -> Result<()> {
    while !completion.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    io.wait_for_completion(completion)?;
    Ok(())
}

/// Returns true for pull-update streams that physically install remote pages.
#[allow(dead_code)]
fn stream_kind_applies_remote_pages(stream_kind: DbChangesStreamKind) -> bool {
    matches!(
        stream_kind,
        DbChangesStreamKind::LegacyPages
            | DbChangesStreamKind::Pages
            | DbChangesStreamKind::ReplaceBasePages
    )
}

/// Decides whether page updates should be replayed through a SQL connection.
///
/// Legacy protocol keeps the original WAL-session path. V1 page streams use the
/// SQL connection only when there is no active logical replay connection.
#[allow(dead_code)]
fn should_replay_raw_pages_on_sql_conn(
    protocol_version_hint: DatabaseSyncEngineProtocolVersion,
    logical_mvcc_pull_active: bool,
    logical_replay_conn_active: bool,
    stream_kind: DbChangesStreamKind,
    mvcc_active: bool,
) -> bool {
    protocol_version_hint != DatabaseSyncEngineProtocolVersion::Legacy
        && logical_mvcc_pull_active
        && mvcc_active
        && !logical_replay_conn_active
        && matches!(
            stream_kind,
            DbChangesStreamKind::Pages | DbChangesStreamKind::ReplaceBasePages
        )
}

fn resolve_local_replay_floor_change_id(
    use_pushed_change_hint: bool,
    local_pull_gen: i64,
    remote_pull_gen: i64,
    remote_last_change_id: Option<i64>,
    last_pushed_pull_gen_hint: i64,
    last_pushed_change_id_hint: i64,
) -> Option<i64> {
    let mut last_change_id = if remote_pull_gen == local_pull_gen {
        remote_last_change_id
    } else {
        Some(0)
    };

    if use_pushed_change_hint
        && last_pushed_pull_gen_hint == local_pull_gen
        && last_pushed_change_id_hint > 0
        && last_change_id.unwrap_or(0) < last_pushed_change_id_hint
    {
        last_change_id = Some(last_pushed_change_id_hint);
    }

    last_change_id
}

fn synced_change_id_after_remote_apply(
    replayed_local_changes: bool,
    local_replay_floor_change_id: Option<i64>,
    post_apply_change_id: i64,
) -> i64 {
    let change_id = if replayed_local_changes {
        local_replay_floor_change_id.unwrap_or(0)
    } else {
        post_apply_change_id
    };
    change_id.min(post_apply_change_id)
}

fn use_pushed_change_hint_for_local_replay(
    stream_kind: DbChangesStreamKind,
    raw_page_replay_on_sql_conn: bool,
) -> bool {
    !raw_page_replay_on_sql_conn && matches!(stream_kind, DbChangesStreamKind::LegacyPages)
}

impl<IO: SyncEngineIo> DatabaseSyncEngine<IO> {
    pub async fn read_db_meta<Ctx>(
        coro: &Coro<Ctx>,
        io: Option<Arc<dyn turso_core::IO>>,
        sync_engine_io: SyncEngineIoStats<IO>,
        main_db_path: &str,
    ) -> Result<Option<DatabaseMetadata>> {
        let path = create_meta_path(main_db_path);
        let is_memory = is_memory(main_db_path);
        let meta = full_read(coro, io, sync_engine_io.io.clone(), &path, is_memory).await?;
        match meta {
            Some(meta) => Ok(Some(DatabaseMetadata::load(&meta)?)),
            None => Ok(None),
        }
    }

    pub async fn bootstrap_db<Ctx>(
        coro: &Coro<Ctx>,
        io: Arc<dyn turso_core::IO>,
        sync_engine_io: SyncEngineIoStats<IO>,
        main_db_path: &str,
        opts: &DatabaseSyncEngineOpts,
        meta: Option<DatabaseMetadata>,
    ) -> Result<DatabaseMetadata> {
        tracing::info!("bootstrap_db(path={}): opts={:?}", main_db_path, opts);
        let meta_path = create_meta_path(main_db_path);
        let partial_sync_opts = opts.partial_sync_opts.clone();
        let partial = partial_sync_opts.is_some();
        // Explicit override for tests / escape hatch. None (the default)
        // auto-detects the remote protocol from the first pull-updates
        // response and persists it in the metadata.
        let forced_protocol = match opts.logical_mvcc_pull {
            Some(true) => Some(RemotePullProtocol::MvccLogical),
            Some(false) => Some(RemotePullProtocol::Pages),
            None => None,
        };
        if forced_protocol == Some(RemotePullProtocol::MvccLogical) {
            // The legacy wire protocol has no MVCC variant; reject the
            // override at open time instead of failing on the first pull.
            if opts.protocol_version_hint == DatabaseSyncEngineProtocolVersion::Legacy {
                return Err(Error::DatabaseSyncEngineError(
                    "the logical_mvcc_pull override is not supported with the legacy sync protocol"
                        .to_string(),
                ));
            }
            ensure_logical_mvcc_pull_supported(partial, opts.remote_encryption_key.as_deref())?;
        }

        let configuration = DatabaseSavedConfiguration {
            remote_url: opts.remote_url.clone(),
            partial_sync_prefetch: opts.partial_sync_opts.as_ref().map(|p| p.prefetch),
            partial_sync_segment_size: opts.partial_sync_opts.as_ref().map(|p| p.segment_size),
        };
        let meta = match meta {
            Some(mut meta) => {
                let mut metadata_changed = meta.update_configuration(configuration);
                if let Some(forced) = forced_protocol {
                    if meta.remote_pull_protocol != forced {
                        meta.remote_pull_protocol = forced;
                        metadata_changed = true;
                    }
                }
                // A replica already syncing logically must fail fast if the
                // configuration drifted into an unsupported combination.
                if meta.remote_pull_protocol == RemotePullProtocol::MvccLogical {
                    ensure_logical_mvcc_pull_supported(
                        partial,
                        opts.remote_encryption_key.as_deref(),
                    )?;
                }
                if metadata_changed {
                    full_write(
                        coro,
                        io.clone(),
                        sync_engine_io.io.clone(),
                        &meta_path,
                        is_memory(main_db_path),
                        meta.dump()?,
                    )
                    .await?;
                }
                meta
            }
            None if opts.bootstrap_if_empty => {
                let client_unique_id = format!("{}-{}", opts.client_name, uuid::Uuid::new_v4());
                let (revision, detected_protocol) = bootstrap_db_file(
                    &SyncOperationCtx::new(
                        coro,
                        &sync_engine_io,
                        opts.remote_url.clone(),
                        opts.remote_encryption_key.as_deref(),
                    ),
                    &io,
                    main_db_path,
                    opts.protocol_version_hint,
                    partial_sync_opts,
                    opts.pull_bytes_threshold,
                )
                .await?;
                let remote_pull_protocol = forced_protocol.unwrap_or(detected_protocol);
                if remote_pull_protocol == RemotePullProtocol::MvccLogical {
                    ensure_logical_mvcc_pull_supported(
                        partial,
                        opts.remote_encryption_key.as_deref(),
                    )?;
                }
                let meta = DatabaseMetadata {
                    version: DATABASE_METADATA_VERSION.to_string(),
                    client_unique_id,
                    synced_revision: Some(revision.clone()),
                    revert_since_wal_salt: None,
                    revert_since_wal_watermark: 0,
                    last_pushed_change_id_hint: 0,
                    last_pushed_pull_gen_hint: 0,
                    last_pushed_replay_floor_change_id_hint: 0,
                    last_pull_unix_time: Some(io.current_time_wall_clock().secs),
                    last_push_unix_time: None,
                    partial_bootstrap_server_revision: if partial {
                        Some(revision.clone())
                    } else {
                        None
                    },
                    fresh_bootstrap_pending_cdc_ack: false,
                    remote_pull_protocol,
                    logical_table_names_by_stable_id: Default::default(),
                    saved_configuration: Some(configuration),
                };
                tracing::info!("write meta after successful bootstrap: meta={meta:?}");

                full_write(
                    coro,
                    io.clone(),
                    sync_engine_io.io.clone(),
                    &meta_path,
                    is_memory(main_db_path),
                    meta.dump()?,
                )
                .await?;
                // todo: what happen if we will actually update the metadata on disk but fail and so in memory state will not be updated
                meta
            }
            None => {
                if opts.protocol_version_hint == DatabaseSyncEngineProtocolVersion::Legacy {
                    return Err(Error::DatabaseSyncEngineError(
                        "deferred bootstrap is not supported for legacy protocol".to_string(),
                    ));
                }
                if partial {
                    return Err(Error::DatabaseSyncEngineError(
                        "deferred bootstrap is not supported for partial sync".to_string(),
                    ));
                }
                let client_unique_id = format!("{}-{}", opts.client_name, uuid::Uuid::new_v4());
                // Deferred bootstrap: the remote's protocol is unknowable
                // until the first server contact; the first pull resolves it.
                let remote_pull_protocol = forced_protocol.unwrap_or(RemotePullProtocol::Unknown);
                let meta = DatabaseMetadata {
                    version: DATABASE_METADATA_VERSION.to_string(),
                    client_unique_id,
                    synced_revision: None,
                    revert_since_wal_salt: None,
                    revert_since_wal_watermark: 0,
                    last_pushed_change_id_hint: 0,
                    last_pushed_pull_gen_hint: 0,
                    last_pushed_replay_floor_change_id_hint: 0,
                    last_pull_unix_time: None,
                    last_push_unix_time: None,
                    partial_bootstrap_server_revision: None,
                    fresh_bootstrap_pending_cdc_ack: false,
                    remote_pull_protocol,
                    logical_table_names_by_stable_id: Default::default(),
                    saved_configuration: Some(configuration),
                };
                tracing::info!("write meta after successful bootstrap: meta={meta:?}");
                full_write(
                    coro,
                    io.clone(),
                    sync_engine_io.io.clone(),
                    &meta_path,
                    is_memory(main_db_path),
                    meta.dump()?,
                )
                .await?;
                // todo: what happen if we will actually update the metadata on disk but fail and so in memory state will not be updated
                meta
            }
        };

        if meta.version != DATABASE_METADATA_VERSION {
            return Err(Error::DatabaseSyncEngineError(format!(
                "unsupported metadata version: {}",
                meta.version
            )));
        }

        tracing::info!("check if main db file exists");

        let main_exists = io.try_open(main_db_path)?.is_some();
        if !main_exists && meta.synced_revision.is_some() {
            let error = "main DB file doesn't exists, but metadata is".to_string();
            return Err(Error::DatabaseSyncEngineError(error));
        }

        Ok(meta)
    }

    pub fn init_db_storage(
        io: Arc<dyn turso_core::IO>,
        sync_engine_io: SyncEngineIoStats<IO>,
        meta: &DatabaseMetadata,
        main_db_path: &str,
        remote_encryption_key: Option<&str>,
    ) -> Result<Arc<dyn DatabaseStorage>> {
        let db_file = io.open_file(main_db_path, turso_core::OpenFlags::Create, false)?;
        let db_file: Arc<dyn DatabaseStorage> = if let Some(partial_sync_opts) =
            meta.partial_sync_opts()
        {
            let Some(partial_bootstrap_server_revision) = &meta.partial_bootstrap_server_revision
            else {
                return Err(Error::DatabaseSyncEngineError(
                    "partial_bootstrap_server_revision must be set in the metadata".to_string(),
                ));
            };
            let DatabasePullRevision::V1 { revision } = &partial_bootstrap_server_revision else {
                return Err(Error::DatabaseSyncEngineError(
                    "partial sync is supported only for V1 protocol".to_string(),
                ));
            };
            tracing::info!("create LazyDatabaseStorage database storage");
            let encoded_key = remote_encryption_key.map(|k| k.to_string());
            Arc::new(LazyDatabaseStorage::new(
                db_file,
                None, // todo(sivukhin): allocate dirty file for FS IO
                sync_engine_io.clone(),
                revision.to_string(),
                partial_sync_opts,
                meta.saved_configuration
                    .as_ref()
                    .and_then(|x| x.remote_url.as_ref())
                    .cloned(),
                encoded_key,
            )?)
        } else {
            Arc::new(turso_core::storage::database::DatabaseFile::new(db_file))
        };

        Ok(db_file)
    }

    pub async fn open_db<Ctx>(
        coro: &Coro<Ctx>,
        io: Arc<dyn turso_core::IO>,
        sync_engine_io: SyncEngineIoStats<IO>,
        main_db: Arc<turso_core::Database>,
        opts: DatabaseSyncEngineOpts,
    ) -> Result<Self> {
        let main_db_path = main_db.path.to_string();
        tracing::info!("open_db(path={}): opts={:?}", main_db_path, opts);
        // A previous process may have crashed after writing the replace-base
        // marker but before completing local replay. Restore the backed-up
        // files before reading metadata or creating tape connections.
        let recovered = ReplaceBaseApplyGuard::recover_pending(
            coro,
            io.clone(),
            sync_engine_io.clone(),
            &main_db_path,
        )
        .await?;
        if recovered {
            // Recovery restored files behind this already-open Database handle,
            // so rebuild its shared WAL/MVCC view before continuing.
            main_db.reload_wal_after_external_restore()?;
        }

        let meta_path = create_meta_path(&main_db_path);

        let meta = full_read(
            coro,
            Some(io.clone()),
            sync_engine_io.io.clone(),
            &meta_path,
            is_memory(&main_db_path),
        )
        .await?;
        let Some(meta) = meta else {
            return Err(Error::DatabaseSyncEngineError(
                "meta must be initialized before open".to_string(),
            ));
        };
        let meta = DatabaseMetadata::load(&meta)?;

        // DB wasn't synced with remote but will be encrypted on remote - so we must properly set reserved bytes field in advance
        if meta.synced_revision.is_none() && opts.reserved_bytes != 0 {
            let conn = main_db.connect()?;
            conn.wal_auto_actions_disable();
            conn.set_reserved_bytes(opts.reserved_bytes as u8)?;

            // write transaction forces allocation of root DB page
            conn.execute("BEGIN IMMEDIATE")?;
            conn.execute("COMMIT")?;
        }

        let tape_opts = DatabaseTapeOpts {
            cdc_table: None,
            cdc_mode: Some("full".to_string()),
            disable_auto_checkpoint: true,
        };
        tracing::info!("initialize database tape connection: path={}", main_db_path);
        let main_db_io = main_db.io.clone();
        let main_db_file = main_db.db_file.clone();
        let main_tape = DatabaseTape::new_with_opts(main_db, tape_opts);
        // Initialize CDC pragma and cache CDC version so iterate_changes() can work
        main_tape.connect(coro).await?;

        let changes_path = create_changes_path(&main_db_path);
        let changes_file = main_db_io.open_file(&changes_path, OpenFlags::Create, false)?;

        let db = Self {
            io: main_db_io,
            sync_engine_io,
            db_file: main_db_file,
            main_tape,
            main_db_path: main_db_path.to_string(),
            main_db_wal_path: create_main_db_wal_path(&main_db_path),
            revert_db_wal_path: create_revert_db_wal_path(&main_db_path),
            meta_path: create_meta_path(&main_db_path),
            changes_file: Arc::new(Mutex::new(Some(changes_file))),
            opts,
            meta: Mutex::new(meta.clone()),
            client_unique_id: meta.client_unique_id.clone(),
        };

        let synced_revision = meta.synced_revision.as_ref();
        if let Some(DatabasePullRevision::Legacy {
            synced_frame_no: None,
            ..
        }) = synced_revision
        {
            // sync WAL from the remote in case of bootstrap - all subsequent initializations will be fast
            db.pull_changes_from_remote(coro).await?;
        }

        tracing::info!("sync engine was initialized");
        Ok(db)
    }

    /// Creates new instance of SyncEngine and initialize it immediately if no consistent local data exists
    pub async fn create_db<Ctx>(
        coro: &Coro<Ctx>,
        io: Arc<dyn turso_core::IO>,
        sync_engine_io: SyncEngineIoStats<IO>,
        main_db_path: &str,
        opts: DatabaseSyncEngineOpts,
    ) -> Result<Self> {
        ReplaceBaseApplyGuard::recover_pending(
            coro,
            io.clone(),
            sync_engine_io.clone(),
            main_db_path,
        )
        .await?;
        let meta = Self::read_db_meta(coro, Some(io.clone()), sync_engine_io.clone(), main_db_path)
            .await?;
        let fresh_bootstrap = meta.is_none() && opts.bootstrap_if_empty;
        let meta = Self::bootstrap_db(
            coro,
            io.clone(),
            sync_engine_io.clone(),
            main_db_path,
            &opts,
            meta,
        )
        .await?;
        let main_db_storage = Self::init_db_storage(
            io.clone(),
            sync_engine_io.clone(),
            &meta,
            main_db_path,
            opts.remote_encryption_key.as_deref(),
        )?;

        // Use async database opening that yields on IO for large schemas
        let main_db_options = turso_core::OpenOptions::new(Arc::new(SqliteDialect))
            .storage(main_db_storage)
            .flags(OpenFlags::Create)
            .db_opts(opts.db_opts);
        let mut open_state = turso_core::OpenDbAsyncState::new();
        let main_db = loop {
            match turso_core::Database::open_async(
                &mut open_state,
                io.clone(),
                main_db_path,
                &main_db_options,
            )? {
                turso_core::IOResult::Done(db) => break db,
                turso_core::IOResult::IO(io_completion) => {
                    while !io_completion.finished() {
                        coro.yield_(SyncEngineIoResult::IO).await?;
                    }
                }
            }
        };

        let engine = Self::open_db(coro, io, sync_engine_io, main_db, opts).await?;
        // An MVCC bootstrap serves the last durable generation base so commits
        // since the last natural checkpoint exist only in the retained
        // logical log. Catch up with one incremental pull so a freshly
        // bootstrapped database is current at connect time, matching what
        // page-protocol bootstraps always provided.
        if fresh_bootstrap {
            engine.catch_up_after_fresh_bootstrap(coro).await?;
        }
        Ok(engine)
    }

    async fn open_revert_db_conn<Ctx>(
        &self,
        coro: &Coro<Ctx>,
    ) -> Result<Arc<turso_core::Connection>> {
        let db = {
            let options = turso_core::OpenOptions::new(Arc::new(SqliteDialect))
                .storage(self.db_file.clone())
                .wal_path(self.revert_db_wal_path.clone())
                .flags(OpenFlags::Create)
                .db_opts(self.opts.db_opts);
            let mut state = OpenDbAsyncState::new();
            loop {
                match turso_core::Database::do_open_async(
                    &mut state,
                    self.io.clone(),
                    &self.main_db_path,
                    &options,
                )? {
                    turso_core::IOResult::Done(db) => break db,
                    turso_core::IOResult::IO(io_completion) => {
                        while !io_completion.finished() {
                            coro.yield_(SyncEngineIoResult::IO).await?;
                        }
                        continue;
                    }
                }
            }
        };
        let conn = db.connect()?;
        conn.wal_auto_actions_disable();
        Ok(conn)
    }

    async fn checkpoint_passive<Ctx>(&self, coro: &Coro<Ctx>) -> Result<(Option<Vec<u32>>, u64)> {
        let watermark = self.meta().revert_since_wal_watermark;
        tracing::info!(
            "checkpoint(path={:?}): revert_since_wal_watermark={}",
            self.main_db_path,
            watermark
        );
        let main_conn = connect_untracked(&self.main_tape)?;
        let main_wal = self.io.try_open(&self.main_db_wal_path)?;
        let main_wal_salt = if let Some(main_wal) = main_wal {
            read_wal_salt(coro, &main_wal).await?
        } else {
            None
        };

        tracing::info!(
            "checkpoint(path={:?}): main_wal_salt={:?}",
            self.main_db_path,
            main_wal_salt
        );

        let revert_since_wal_salt = self.meta().revert_since_wal_salt.clone();
        if revert_since_wal_salt.is_some() && main_wal_salt != revert_since_wal_salt {
            self.update_meta(coro, |meta| {
                meta.revert_since_wal_watermark = 0;
                meta.revert_since_wal_salt = main_wal_salt.clone();
            })
            .await?;
            return Ok((main_wal_salt, 0));
        }
        // we do this Passive checkpoint in order to transfer all synced frames to the DB file and make history of revert DB valid
        // if we will not do that we will be in situation where WAL in the revert DB is not valid relative to the DB file
        let result = main_conn.checkpoint(turso_core::CheckpointMode::Passive {
            upper_bound_inclusive: Some(watermark),
        })?;
        tracing::info!(
            "checkpoint(path={:?}): checkpointed portion of WAL: {:?}",
            self.main_db_path,
            result
        );
        if result.wal_max_frame < watermark {
            return Err(Error::DatabaseSyncEngineError(
                format!("unable to checkpoint synced portion of WAL: result={result:?}, watermark={watermark}"),
            ));
        }
        Ok((main_wal_salt, watermark))
    }

    pub async fn stats<Ctx>(&self, coro: &Coro<Ctx>) -> Result<SyncEngineStats> {
        let main_conn = connect_untracked(&self.main_tape)?;
        let change_id = self.meta().last_pushed_change_id_hint;
        let last_pull_unix_time = self.meta().last_pull_unix_time;
        let revision = self.meta().synced_revision.clone().map(|x| match x {
            DatabasePullRevision::Legacy {
                generation,
                synced_frame_no,
            } => format!("generation={generation},synced_frame_no={synced_frame_no:?}"),
            DatabasePullRevision::V1 { revision } => revision,
        });
        let last_push_unix_time = self.meta().last_push_unix_time;
        let revert_wal_path = &self.revert_db_wal_path;
        let revert_wal_file = self.io.try_open(revert_wal_path)?;
        let revert_wal_size = revert_wal_file.map(|f| f.size()).transpose()?.unwrap_or(0);
        let main_wal_frames = main_conn.wal_state()?.max_frame;
        let main_wal_size = if main_wal_frames == 0 {
            0
        } else {
            WAL_FRAME_HEADER as u64 + WAL_FRAME_SIZE as u64 * main_wal_frames
        };
        Ok(SyncEngineStats {
            cdc_operations: count_local_changes(coro, &main_conn, &self.opts, change_id).await?,
            main_wal_size,
            revert_wal_size,
            last_pull_unix_time,
            last_push_unix_time,
            revision,
            network_sent_bytes: self
                .sync_engine_io
                .network_stats
                .written_bytes
                .load(Ordering::SeqCst),
            network_received_bytes: self
                .sync_engine_io
                .network_stats
                .read_bytes
                .load(Ordering::SeqCst),
        })
    }

    pub async fn checkpoint<Ctx>(&self, coro: &Coro<Ctx>) -> Result<()> {
        let main_conn = connect_untracked(&self.main_tape)?;
        if self.meta().logical_mvcc_pull_active() && main_conn.mvcc_enabled() {
            let main_wal_state = main_conn.wal_state()?;
            let result = main_conn.checkpoint(turso_core::CheckpointMode::Truncate {
                upper_bound_inclusive: Some(main_wal_state.max_frame),
            })?;
            tracing::info!(
                "checkpoint(path={:?}): logical MVCC TRUNCATE checkpoint result: {:?}",
                self.main_db_path,
                result
            );
            if !result.everything_backfilled() {
                return Err(LimboError::Busy.into());
            }
            self.update_meta(coro, |meta| {
                meta.revert_since_wal_salt = None;
                meta.revert_since_wal_watermark = 0;
            })
            .await?;
            publish_schema_after_sync_checkpoint(&main_conn)?;
            return Ok(());
        }

        let (main_wal_salt, watermark) = self.checkpoint_passive(coro).await?;

        tracing::info!(
            "checkpoint(path={:?}): passive checkpoint is done",
            self.main_db_path
        );
        let revert_conn = self.open_revert_db_conn(coro).await?;

        let mut page = [0u8; PAGE_SIZE];
        let db_size = if try_wal_watermark_read_page(coro, &revert_conn, 1, &mut page, None).await?
        {
            db_size_from_page(&page)
        } else {
            0
        };

        tracing::info!(
            "checkpoint(path={:?}): revert DB initial size: {}",
            self.main_db_path,
            db_size
        );

        let main_wal_state;
        {
            let mut revert_session = WalSession::new(revert_conn.clone());
            revert_session.begin()?;

            let mut main_session = WalSession::new(main_conn.clone());
            main_session.begin()?;

            main_wal_state = main_conn.wal_state()?;
            tracing::info!(
                "checkpoint(path={:?}): main DB WAL state: {:?}",
                self.main_db_path,
                main_wal_state
            );

            let mut revert_session = DatabaseWalSession::new(coro, revert_session).await?;

            let main_changed_pages = main_conn.wal_changed_pages_after(watermark)?;
            tracing::info!(
                "checkpoint(path={:?}): collected {} changed pages",
                self.main_db_path,
                main_changed_pages.len()
            );
            let revert_changed_pages: HashSet<u32> = revert_conn
                .wal_changed_pages_after(0)?
                .into_iter()
                .collect();
            for page_no in main_changed_pages {
                if revert_changed_pages.contains(&page_no) {
                    tracing::debug!(
                        "checkpoint(path={:?}): skip page {} as it present in revert WAL",
                        self.main_db_path,
                        page_no
                    );
                    continue;
                }
                if page_no > db_size {
                    tracing::debug!(
                        "checkpoint(path={:?}): skip page {} as it ahead of revert-DB size",
                        self.main_db_path,
                        page_no
                    );
                    continue;
                }

                let end_read_result = try_wal_watermark_read_page(
                    coro,
                    &main_conn,
                    page_no,
                    &mut page,
                    Some(watermark),
                )
                .await?;
                if !end_read_result {
                    tracing::debug!(
                        "checkpoint(path={:?}): skip page {} as it was allocated in the WAL portion for revert",
                        self.main_db_path,
                        page_no
                    );
                    continue;
                }
                tracing::debug!(
                    "checkpoint(path={:?}): append page {} (current db_size={})",
                    self.main_db_path,
                    page_no,
                    db_size
                );
                revert_session.append_page(page_no, &page)?;
            }
            revert_session.commit(db_size)?;
            revert_session.wal_session.end(false)?;
        }
        // The revert frames above are written through raw WAL inserts (pwrite
        // without fsync) and the session is closed with force_commit=false,
        // which doesn't sync either. Persist them durably before update_meta
        // records the new revert watermark: otherwise a crash could lose revert
        // frames the metadata claims exist, corrupting the next rollback.
        if let Some(revert_wal_file) = self.io.try_open(&self.revert_db_wal_path)? {
            sync_file(coro, &revert_wal_file).await?;
        }
        self.update_meta(coro, |meta| {
            meta.revert_since_wal_salt = main_wal_salt;
            meta.revert_since_wal_watermark = main_wal_state.max_frame;
        })
        .await?;

        let result = main_conn.checkpoint(turso_core::CheckpointMode::Truncate {
            upper_bound_inclusive: Some(main_wal_state.max_frame),
        })?;
        tracing::info!(
            "checkpoint(path={:?}): main DB TRUNCATE checkpoint result: {:?}",
            self.main_db_path,
            result
        );
        publish_schema_after_sync_checkpoint(&main_conn)?;

        Ok(())
    }

    /// Converts the local database to MVCC journal mode when the remote
    /// speaks the MVCC logical-log protocol but the local database is still
    /// in WAL mode.
    ///
    /// This happens for deferred-bootstrap replicas ("converted to cloud
    /// sync later"): a fresh bootstrap inherits MVCC-ness from the base
    /// image's page-1 header, but a deferred replica opened its WAL database
    /// before ever contacting the server. Applying an MVCC page base into a
    /// WAL-mode process would leave the file header and the in-process
    /// journal mode disagreeing — this process would keep WAL semantics (no
    /// logical log, WAL checkpoint path) while the next open reads the MVCC
    /// header and starts an empty MvStore, an uncoordinated mode flip.
    /// Converting up front via `PRAGMA journal_mode = 'mvcc'` (WAL
    /// checkpoint, header rewrite, MvStore bootstrap over the existing
    /// btree) puts the replica on the same footing as a fresh MVCC bootstrap
    /// before the replace-base apply runs.
    async fn ensure_local_mvcc_journal_mode<Ctx>(&self, coro: &Coro<Ctx>) -> Result<()> {
        // The pragma rejects connections with CDC capture enabled, so use an
        // untracked connection (the tape connection has capture on).
        let main_conn = connect_untracked(&self.main_tape)?;
        if main_conn.mvcc_enabled() {
            return Ok(());
        }
        // Replace-base preserves local data by replaying the CDC log on top
        // of the new base. Changes without CDC provenance (a database file
        // that lived outside the sync engine) would be silently dropped —
        // reject loudly instead. This runs only before the first sync, when
        // nothing has been pruned from the CDC log yet, so "user tables exist
        // but the CDC log is empty" reliably means foreign data. (The
        // turso_cdc table itself is created eagerly by the capture pragma,
        // so its existence proves nothing.)
        let cdc_high_water = max_local_change_id(coro, &main_conn).await?.unwrap_or(0);
        if cdc_high_water == 0 {
            let user_tables = list_user_tables(coro, &main_conn).await?;
            if !user_tables.is_empty() {
                return Err(Error::DatabaseSyncEngineError(format!(
                    "local database contains tables without CDC history ({}); the sync engine cannot preserve their data across the initial sync with an MVCC-mode remote (create the local database through the sync engine, or start from an empty local file)",
                    user_tables.join(", "),
                )));
            }
        }
        tracing::info!(
            "ensure_local_mvcc_journal_mode(path={}): converting local database to MVCC journal mode for MVCC-protocol remote",
            self.main_db_path
        );
        let mut stmt = main_conn.prepare("PRAGMA journal_mode = 'mvcc'")?;
        run_stmt_ignore_rows(coro, &mut stmt)
            .await
            .map_err(|error| {
                Error::DatabaseSyncEngineError(format!(
                    "failed to convert local database to MVCC journal mode: {error}"
                ))
            })?;
        assert!(
            main_conn.mvcc_enabled(),
            "journal_mode=mvcc pragma succeeded but MvStore is not open"
        );
        Ok(())
    }

    /// Catches a freshly bootstrapped MVCC replica up to the retained
    /// logical log. The bootstrap page image is the last durable generation
    /// base (the server deliberately never checkpoints for a bootstrap), so
    /// without this pull `connect()` would hand out a database missing every
    /// commit since the last natural checkpoint. Long-polling is disabled:
    /// when the base is already current this must return immediately rather
    /// than hold `connect()` open waiting for future changes.
    ///
    /// No-op for page-protocol replicas (their bootstrap is already
    /// current). Every initialization path that can perform a fresh
    /// bootstrap must call this after opening the engine — both
    /// [`DatabaseSyncEngine::create_db`] and the sdk-kit `rsapi` create flow,
    /// which assembles the same sequence manually.
    pub async fn catch_up_after_fresh_bootstrap<Ctx>(&self, coro: &Coro<Ctx>) -> Result<()> {
        if self.meta().remote_pull_protocol != RemotePullProtocol::MvccLogical {
            return Ok(());
        }
        let changes = self.wait_changes_from_remote_inner(coro, None).await?;
        if changes.file_slot.is_some() {
            self.apply_changes_from_remote(coro, changes).await?;
        }
        Ok(())
    }

    pub async fn wait_changes_from_remote<Ctx>(&self, coro: &Coro<Ctx>) -> Result<DbChangesStatus> {
        self.wait_changes_from_remote_inner(coro, self.opts.long_poll_timeout)
            .await
    }

    async fn wait_changes_from_remote_inner<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        long_poll_timeout: Option<std::time::Duration>,
    ) -> Result<DbChangesStatus> {
        tracing::info!("wait_changes(path={})", self.main_db_path);

        let file = acquire_slot(&self.changes_file)?;

        let now = self.io.current_time_wall_clock();
        let revision = self.meta().synced_revision.clone();
        let ctx = &SyncOperationCtx::new(
            coro,
            &self.sync_engine_io,
            self.meta().remote_url(),
            self.opts.remote_encryption_key.as_deref(),
        );
        let mut stream_kind =
            if self.opts.protocol_version_hint == DatabaseSyncEngineProtocolVersion::Legacy {
                DbChangesStreamKind::LegacyPages
            } else {
                DbChangesStreamKind::Pages
            };
        let remote_pull_protocol = resolve_remote_pull_protocol(
            self.opts.logical_mvcc_pull,
            self.meta().remote_pull_protocol,
        );
        let next_revision = match (remote_pull_protocol, &revision) {
            (RemotePullProtocol::MvccLogical, Some(DatabasePullRevision::V1 { revision })) => {
                match pull_updates_v1(ctx, &file.value, revision, long_poll_timeout, true).await? {
                    (next_revision, result @ PullUpdatesV1Result::Logical { txns, ops }, _) => {
                        tracing::info!(
                            "wait_changes(path={}): logical pull returned {} transactions / {} ops",
                            self.main_db_path,
                            txns,
                            ops
                        );
                        stream_kind = stream_kind_for_pull_updates_v1_result(&result);
                        next_revision
                    }
                    (next_revision, result @ PullUpdatesV1Result::Pages { replace_base }, _) => {
                        tracing::info!(
                            "wait_changes(path={}): logical pull returned a page stream fallback; replace_base={replace_base}",
                            self.main_db_path,
                        );
                        stream_kind = stream_kind_for_pull_updates_v1_result(&result);
                        next_revision
                    }
                }
            }
            (RemotePullProtocol::MvccLogical, None) => {
                let (next_revision, result, _) =
                    pull_updates_v1(ctx, &file.value, "", long_poll_timeout, false).await?;
                tracing::info!(
                    "wait_changes(path={}): initial logical MVCC sync returned page base",
                    self.main_db_path,
                );
                // Deferred replicas may still be in WAL mode locally; the
                // MVCC page base must be applied to an MVCC-mode database.
                self.ensure_local_mvcc_journal_mode(coro).await?;
                stream_kind = match result {
                    PullUpdatesV1Result::Pages { .. } => DbChangesStreamKind::ReplaceBasePages,
                    PullUpdatesV1Result::Logical { txns, ops } => {
                        tracing::info!(
                            "wait_changes(path={}): initial logical MVCC sync returned logical stream with {} transactions / {} ops",
                            self.main_db_path,
                            txns,
                            ops
                        );
                        DbChangesStreamKind::Logical
                    }
                };
                next_revision
            }
            // A Legacy revision only exists on replicas that synced with the
            // legacy wire protocol, which has no MVCC variant — detection
            // never selects MvccLogical for them, so this combination can
            // only be a caller forcing `logical_mvcc_pull: Some(true)` on a
            // legacy replica. The server cannot resume an MVCC logical
            // stream from a legacy revision; fail loudly rather than
            // silently ignoring the override.
            (RemotePullProtocol::MvccLogical, Some(DatabasePullRevision::Legacy { .. })) => {
                return Err(Error::DatabaseSyncEngineError(
                    "the logical_mvcc_pull override is not supported for replicas synced with the legacy protocol; remove the override to keep using legacy page sync".to_string(),
                ));
            }
            // First server contact of an auto-mode replica whose remote
            // protocol is not yet known (deferred bootstrap). The request is
            // identical on the wire to the page path's no-revision request;
            // the response header tells us which protocol the remote speaks.
            (RemotePullProtocol::Unknown, None) => {
                let (next_revision, result, detected) =
                    pull_updates_v1(ctx, &file.value, "", long_poll_timeout, false).await?;
                if detected == RemotePullProtocol::MvccLogical {
                    ensure_logical_mvcc_pull_supported(
                        self.opts.partial_sync_opts.is_some(),
                        self.opts.remote_encryption_key.as_deref(),
                    )?;
                    // Deferred replicas may still be in WAL mode locally; the
                    // MVCC page base must be applied to an MVCC-mode database.
                    self.ensure_local_mvcc_journal_mode(coro).await?;
                }
                tracing::info!(
                    "wait_changes(path={}): detected remote pull protocol {:?} on first contact",
                    self.main_db_path,
                    detected
                );
                self.update_meta(coro, |m| {
                    m.remote_pull_protocol = detected;
                })
                .await?;
                stream_kind = match (detected, &result) {
                    // Page-protocol remote: mirror the page path exactly,
                    // including its rejection of replace-base streams.
                    (
                        RemotePullProtocol::Pages | RemotePullProtocol::Unknown,
                        PullUpdatesV1Result::Pages { replace_base },
                    ) => {
                        if *replace_base {
                            return Err(Error::DatabaseSyncEngineError(
                                "wait_changes_from_remote does not support replace-base page streams yet".to_string(),
                            ));
                        }
                        DbChangesStreamKind::Pages
                    }
                    // MVCC remote: the page base applies as replace-base so
                    // local (deferred) changes are preserved and replayed.
                    (RemotePullProtocol::MvccLogical, PullUpdatesV1Result::Pages { .. }) => {
                        DbChangesStreamKind::ReplaceBasePages
                    }
                    (_, PullUpdatesV1Result::Logical { .. }) => {
                        return Err(Error::DatabaseSyncEngineError(
                            "server returned a logical stream for a page-stream first-contact request".to_string(),
                        ));
                    }
                };
                next_revision
            }
            (RemotePullProtocol::Pages | RemotePullProtocol::Unknown, _) => {
                wal_pull_to_file(
                    ctx,
                    &file.value,
                    &revision,
                    self.opts.wal_pull_batch_size,
                    long_poll_timeout,
                )
                .await?
            }
        };

        if file.value.size()? == 0 {
            tracing::info!(
                "wait_changes(path={}): no changes detected",
                self.main_db_path
            );
            self.update_meta(coro, |m| {
                m.synced_revision = Some(next_revision.clone());
                m.last_pull_unix_time = Some(now.secs);
            })
            .await?;
            return Ok(DbChangesStatus {
                time: now,
                revision: next_revision,
                file_slot: None,
                stream_kind,
            });
        }

        tracing::info!(
            "wait_changes_from_remote(path={}): revision: {:?} -> {:?}",
            self.main_db_path,
            revision,
            next_revision
        );

        Ok(DbChangesStatus {
            time: now,
            revision: next_revision,
            file_slot: Some(file),
            stream_kind,
        })
    }

    /// Sync all new changes from remote DB and apply them locally
    /// This method will **not** send local changed to the remote
    /// This method will block writes for the period of pull
    pub async fn apply_changes_from_remote<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        remote_changes: DbChangesStatus,
    ) -> Result<()> {
        let mut new_revision = remote_changes.revision.clone();
        if remote_changes.is_empty() {
            self.update_meta(coro, |m| {
                m.synced_revision = Some(new_revision.clone());
                m.last_pull_unix_time = Some(remote_changes.time.secs);
            })
            .await?;
            return Ok(());
        }
        let changes_file = remote_changes
            .file_slot
            .as_ref()
            .expect("non-empty remote changes must keep a file slot")
            .value
            .clone();
        if matches!(remote_changes.stream_kind, DbChangesStreamKind::Logical) {
            let logical_table_names_by_stable_id = self
                .apply_logical_mvcc_changes_internal(coro, &changes_file)
                .await?;
            self.update_meta(coro, |m| {
                m.revert_since_wal_salt = None;
                m.revert_since_wal_watermark = 0;
                m.synced_revision = Some(new_revision.clone());
                m.last_pushed_pull_gen_hint = 0;
                m.last_pushed_change_id_hint = 0;
                m.last_pushed_replay_floor_change_id_hint = 0;
                m.last_pull_unix_time = Some(remote_changes.time.secs);
                m.logical_table_names_by_stable_id = logical_table_names_by_stable_id;
            })
            .await?;
            return Ok(());
        }
        let pull_result = self
            .apply_changes_internal(
                coro,
                &changes_file,
                remote_changes.stream_kind,
                &remote_changes.revision,
            )
            .await;
        let Ok((revert_since_wal_watermark, logical_table_names_by_stable_id, followup_revision)) =
            pull_result
        else {
            return Err(pull_result.err().unwrap());
        };
        if let Some(revision) = followup_revision {
            new_revision = revision;
        }

        let revert_wal_file = self.io.open_file(
            &self.revert_db_wal_path,
            turso_core::OpenFlags::Create,
            false,
        )?;
        reset_wal_file(coro, revert_wal_file, 0).await?;

        self.update_meta(coro, |m| {
            m.revert_since_wal_watermark = revert_since_wal_watermark;
            m.synced_revision = Some(new_revision);
            m.last_pushed_pull_gen_hint = 0;
            m.last_pushed_change_id_hint = 0;
            m.last_pushed_replay_floor_change_id_hint = 0;
            m.last_pull_unix_time = Some(remote_changes.time.secs);
            m.logical_table_names_by_stable_id = logical_table_names_by_stable_id;
        })
        .await?;
        Ok(())
    }

    async fn apply_logical_mvcc_changes_internal<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        changes_file: &Arc<dyn turso_core::File>,
    ) -> Result<BTreeMap<u64, String>> {
        tracing::info!("apply_logical_mvcc_changes(path={})", self.main_db_path);

        let conn = connect_untracked(&self.main_tape)?;
        let had_cdc_table = has_table(coro, &conn, "turso_cdc").await?;
        let (local_pull_gen, local_last_change_id) =
            read_last_change_id(coro, &conn, &self.client_unique_id).await?;
        let mut precollected_local_changes = Vec::new();
        if had_cdc_table {
            let iterate_opts = DatabaseChangesIteratorOpts {
                first_change_id: None,
                mode: DatabaseChangesIteratorMode::Apply,
                ignore_schema_changes: false,
                ..Default::default()
            };
            let mut iterator = self.main_tape.iterate_changes(iterate_opts)?;
            while let Some(operation) = iterator.next(coro).await? {
                match operation {
                    DatabaseTapeOperation::RowChange(change) => {
                        precollected_local_changes.push(change)
                    }
                    DatabaseTapeOperation::Commit => continue,
                    DatabaseTapeOperation::StmtReplay(_)
                    | DatabaseTapeOperation::SchemaReplay(_) => {
                        panic!("changes iterator must not use replay-only operations")
                    }
                }
            }
        }

        conn.execute("BEGIN IMMEDIATE")?;
        let mut logical_table_names_by_stable_id =
            self.meta().logical_table_names_by_stable_id.clone();
        let mut replay = DatabaseReplaySession {
            conn: conn.clone(),
            cached_delete_stmt: HashMap::new(),
            cached_insert_stmt: HashMap::new(),
            cached_update_stmt: HashMap::new(),
            in_txn: true,
            generator: DatabaseReplayGenerator {
                conn: conn.clone(),
                opts: DatabaseReplaySessionOpts {
                    use_implicit_rowid: true,
                },
            },
        };
        let apply_result = async {
            let remote_apply_stats =
                apply_logical_transactions_file_without_commit_excluding_client_txns_with_table_map_and_stats(
                    coro,
                    &mut replay,
                    changes_file,
                    &self.client_unique_id,
                    &mut logical_table_names_by_stable_id,
                )
                .await?;
	            tracing::info!(
	                "apply_logical_mvcc_changes(path={}): applied remote logical transactions; touched_rows={}",
	                self.main_db_path,
	                remote_apply_stats.touched_rows.len()
	            );

            let (last_pushed_pull_gen_hint, last_pushed_change_id_hint) = {
                let meta = self.meta();
                (
                    meta.last_pushed_pull_gen_hint,
                    meta.last_pushed_change_id_hint,
                )
            };
            // Raise the replay floor to the pushed change-id hint so already-pushed
            // local changes are excluded from replay. A subsequent remote update to
            // a row this replica pushed (e.g. sync-fuzzer phase 3 push then phase 4
            // remote update of the same rows) then wins, instead of being clobbered
            // by re-replaying the stale pushed value.
            let last_change_id = resolve_local_replay_floor_change_id(
                true,
                local_pull_gen,
                local_pull_gen,
                local_last_change_id,
                last_pushed_pull_gen_hint,
                last_pushed_change_id_hint,
            );
            let replay_floor = last_change_id.unwrap_or(0);
            let local_changes = precollected_local_changes
                .into_iter()
                .filter(|change| change.change_id > replay_floor)
                .collect::<Vec<_>>();
            let mut skipped_internal_local_changes = 0usize;
            let local_changes = local_changes
                .into_iter()
                .filter_map(|change| match should_replay_local_change(&change) {
                    Ok(true) => Some(Ok(change)),
                    Ok(false) => {
                        skipped_internal_local_changes += 1;
                        None
                    }
                    Err(error) => Some(Err(error)),
                })
                .collect::<Result<Vec<_>>>()?;
            let replayed_local_changes = !local_changes.is_empty();
            tracing::info!(
                "apply_logical_mvcc_changes(path={}): logical local replay floor={replay_floor}; local_changes={} skipped_internal_local_changes={}",
                self.main_db_path,
                local_changes.len(),
                skipped_internal_local_changes,
            );

            if replayed_local_changes {
                let mut local_replay = DatabaseReplaySession {
                    conn: conn.clone(),
                    cached_delete_stmt: HashMap::new(),
                    cached_insert_stmt: HashMap::new(),
                    cached_update_stmt: HashMap::new(),
                    in_txn: true,
                    generator: DatabaseReplayGenerator {
                        conn: conn.clone(),
                        opts: DatabaseReplaySessionOpts {
                            use_implicit_rowid: false,
                        },
                    },
                };

                let mut transformed = if self.opts.use_transform {
                    let ctx = &SyncOperationCtx::new(
                        coro,
                        &self.sync_engine_io,
                        self.meta().remote_url(),
                        self.opts.remote_encryption_key.as_deref(),
                    );
                    Some(apply_transformation(ctx, &local_changes, &local_replay.generator).await?)
                } else {
                    None
                };

                assert!(!local_replay.conn().get_auto_commit());
                for (i, change) in local_changes.into_iter().enumerate() {
                    let operation = if let Some(transformed) = &mut transformed {
                        match std::mem::replace(
                            &mut transformed[i],
                            DatabaseRowTransformResult::Skip,
                        ) {
                            DatabaseRowTransformResult::Keep => {
                                DatabaseTapeOperation::RowChange(change)
                            }
                            DatabaseRowTransformResult::Skip => continue,
                            DatabaseRowTransformResult::Rewrite(replay) => {
                                DatabaseTapeOperation::StmtReplay(replay)
                            }
                        }
                    } else {
                        DatabaseTapeOperation::RowChange(change)
                    };
                    local_replay.replay(coro, operation).await?;
                }
                assert!(!local_replay.conn().get_auto_commit());
            }

            tracing::info!(
                "apply_logical_mvcc_changes(path={}): logical local replay complete; reading CDC high-water",
                self.main_db_path
            );
            let post_apply_change_id = max_local_change_id(coro, &conn).await?.unwrap_or(0);
            let synced_change_id = synced_change_id_after_remote_apply(
                replayed_local_changes,
                last_change_id,
                post_apply_change_id,
            );
            tracing::info!(
                "apply_logical_mvcc_changes(path={}): updating sync high-water to {synced_change_id}",
                self.main_db_path
            );
            update_last_change_id(
                coro,
                &conn,
                &self.client_unique_id,
                local_pull_gen,
                synced_change_id,
            )
            .await?;
            replay.replay(coro, DatabaseTapeOperation::Commit).await?;
            Result::Ok(logical_table_names_by_stable_id)
        }
        .await;

        if let Err(error) = &apply_result {
            tracing::error!(
                "apply_logical_mvcc_changes(path={}): rolling back failed logical apply: {error}",
                self.main_db_path
            );
            if !conn.get_auto_commit() {
                if let Err(rollback_error) = conn.execute("ROLLBACK") {
                    tracing::error!(
                        "apply_logical_mvcc_changes(path={}): rollback failed after logical apply error: {rollback_error}",
                        self.main_db_path
                    );
                }
            }
        }
        apply_result
    }

    #[allow(dead_code)]
    async fn reject_replace_base_pages_apply_until_wired<Ctx>(
        &self,
        coro: &Coro<Ctx>,
    ) -> Result<()> {
        let conn = connect_untracked(&self.main_tape)?;
        if has_table(coro, &conn, "turso_cdc").await? {
            let (_, local_last_change_id) =
                read_last_change_id(coro, &conn, &self.client_unique_id).await?;
            let pending_local_changes =
                count_local_changes(coro, &conn, &self.opts, local_last_change_id.unwrap_or(0))
                    .await?;
            if pending_local_changes != 0 {
                return Err(Error::DatabaseSyncEngineError(format!(
                    "replace-base page apply with pending local CDC changes is not wired yet: {pending_local_changes} local changes need replay"
                )));
            }
        }
        Err(Error::DatabaseSyncEngineError(
            "replace-base page apply is not wired yet".to_string(),
        ))
    }

    async fn apply_changes_internal<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        changes_file: &Arc<dyn turso_core::File>,
        stream_kind: DbChangesStreamKind,
        remote_revision: &DatabasePullRevision,
    ) -> Result<(u64, BTreeMap<u64, String>, Option<DatabasePullRevision>)> {
        tracing::info!("apply_changes(path={})", self.main_db_path);

        let (_, watermark) = self.checkpoint_passive(coro).await?;

        let revert_conn = self.open_revert_db_conn(coro).await?;
        let main_conn = connect_untracked(&self.main_tape)?;
        let replace_base_pages = matches!(stream_kind, DbChangesStreamKind::ReplaceBasePages);

        let mut revert_session = WalSession::new(revert_conn.clone());
        revert_session.begin()?;

        // start of the pull updates apply process
        // during this process we need to be very careful with the state of the WAL as at some points it can be not safe to read data from it
        // the reasons why this can be not safe:
        // 1. we are in the middle of rollback or apply from remote WAL - so DB now is in some weird state and no operations can be made safely
        // 2. after rollback or apply from remote WAL it's unsafe to prepare statements because schema cookie can go "back in time" and we first need to adjust it before executing any statement over DB
        let mut main_session = WalSession::new(main_conn.clone());
        main_session.begin()?;

        // we need to make sure that updates from the session will not be commited accidentally in the middle of the pull process
        // in order to achieve that we mark current session as "nested program" which eliminates possibility that data will be actually commited without our explicit command
        //
        // the reason to not use auto-commit is because it has its own rules which resets the flag in case of statement reset - which we do under the hood sometimes
        // that's why nested executed was chosen instead of auto-commit=false mode
        main_conn.start_nested();

        let had_cdc_table = has_table(coro, &main_conn, "turso_cdc").await?;
        let pre_apply_local_change_id = if had_cdc_table {
            max_local_change_id(coro, &main_conn).await?
        } else {
            None
        };
        tracing::info!(
            "apply_changes(path={}): pre-apply local CDC high-water mark: {:?}",
            self.main_db_path,
            pre_apply_local_change_id
        );

        // read current pull generation from local table for the given client
        let (local_pull_gen, local_last_change_id) =
            read_last_change_id(coro, &main_conn, &self.client_unique_id).await?;
        let no_checkpoint_replace_base =
            replace_base_pages && self.meta().logical_mvcc_pull_active();
        tracing::info!(
            "apply_changes(path={}): local sync high-water before remote apply: client_id={} pull_gen={} change_id={:?} replace_base={}",
            self.main_db_path,
            self.client_unique_id,
            local_pull_gen,
            local_last_change_id,
            replace_base_pages,
        );
        let (last_pushed_pull_gen_hint, last_pushed_change_id_hint) = {
            let meta = self.meta();
            (
                meta.last_pushed_pull_gen_hint,
                meta.last_pushed_change_id_hint,
            )
        };

        // read schema version after initiating WAL session (in order to read it with consistent max_frame_no)
        // note, that as we initiated WAL session earlier - no changes can be made in between and we will have consistent race-free view of schema version
        let main_conn_schema_version = main_conn.read_schema_version()?;

        let precollect_local_changes_before_remote_apply = replace_base_pages
            || matches!(stream_kind, DbChangesStreamKind::Logical)
            || should_replay_raw_pages_on_sql_conn(
                self.opts.protocol_version_hint,
                self.meta().logical_mvcc_pull_active(),
                false,
                stream_kind,
                main_conn.mv_store().as_ref().is_some(),
            );
        let replace_base_precollection_floor = if replace_base_pages {
            if no_checkpoint_replace_base {
                None
            } else {
                resolve_local_replay_floor_change_id(
                    true,
                    local_pull_gen,
                    local_pull_gen,
                    local_last_change_id,
                    last_pushed_pull_gen_hint,
                    last_pushed_change_id_hint,
                )
            }
        } else {
            None
        };
        let mut precollected_local_changes = Vec::with_capacity(64);
        if precollect_local_changes_before_remote_apply {
            let iterate_opts = DatabaseChangesIteratorOpts {
                first_change_id: if replace_base_pages {
                    replace_base_precollection_floor.map(|change_id| change_id + 1)
                } else {
                    None
                },
                mode: DatabaseChangesIteratorMode::Apply,
                ignore_schema_changes: false,
                ..Default::default()
            };
            let mut iterator = self.main_tape.iterate_changes(iterate_opts)?;
            while let Some(operation) = iterator.next(coro).await? {
                match operation {
                    DatabaseTapeOperation::RowChange(change) => {
                        precollected_local_changes.push(change)
                    }
                    DatabaseTapeOperation::Commit => continue,
                    DatabaseTapeOperation::StmtReplay(_)
                    | DatabaseTapeOperation::SchemaReplay(_) => {
                        panic!("changes iterator must not use replay-only operations")
                    }
                }
            }
            tracing::info!(
                "apply_changes(path={}): precollected {} local changes for replay",
                self.main_db_path,
                precollected_local_changes.len(),
            );
        } else {
            tracing::info!(
                "apply_changes(path={}): using post-apply local change collection",
                self.main_db_path,
            );
        }
        let previous_synced_revision = self.meta().synced_revision.clone();
        let mut logical_table_names_by_stable_id =
            self.meta().logical_table_names_by_stable_id.clone();
        let mut replace_base_guard = if replace_base_pages {
            Some(
                ReplaceBaseApplyGuard::create(
                    coro,
                    self.io.clone(),
                    self.sync_engine_io.clone(),
                    &self.main_db_path,
                    previous_synced_revision,
                )
                .await?,
            )
        } else {
            None
        };

        let apply_result: Result<(
            u64,
            BTreeMap<u64, String>,
            Option<DatabasePullRevision>,
        )> = async {
            let mut main_session = Some(DatabaseWalSession::new(coro, main_session).await?);

            // Phase 1 (start): rollback local changes from the WAL

            // Phase 1.a: rollback local changes not checkpointed to the revert-db
            tracing::info!(
                "apply_changes(path={}): rolling back frames after {} watermark, max_frame={}",
                self.main_db_path,
                watermark,
                main_conn.wal_state()?.max_frame
            );
            let local_rollback = main_session
                .as_mut()
                .expect("main WAL session must be active")
                .rollback_changes_after(coro, watermark)
                .await?;
            let mut frame = [0u8; WAL_FRAME_SIZE];

            let remote_rollback = revert_conn.wal_state()?.max_frame;
            tracing::info!(
                "apply_changes(path={}): rolling back {} frames from revert DB",
                self.main_db_path,
                remote_rollback
            );
            // Phase 1.b: rollback local changes by using frames from revert-db
            // it's important to append pages from revert-db after local revert - because pages from revert-db must overwrite rollback from main DB
            for frame_no in 1..=remote_rollback {
                let info = revert_session.read_at(frame_no, &mut frame)?;
                main_session
                    .as_mut()
                    .expect("main WAL session must be active")
                    .append_page(info.page_no, &frame[WAL_FRAME_HEADER..])?;
            }
            revert_session.end(false)?;

            // Phase 2: after revert DB has no local changes in its latest state - so its safe to apply changes from remote
            let mut logical_replay_conn = None;
            let applied_raw_db_size: u32;
            let mut revert_since_wal_watermark = None;
            let mut followup_revision = None;
            match stream_kind {
                stream_kind if stream_kind_applies_remote_pages(stream_kind) => {
                    if matches!(stream_kind, DbChangesStreamKind::ReplaceBasePages) {
                        tracing::warn!(
                            "apply_changes(path={}): applying replace-base page snapshot from remote",
                            self.main_db_path,
                        );
                    }
                    let db_size = wal_apply_from_file(
                        coro,
                        changes_file,
                        main_session
                            .as_mut()
                            .expect("main WAL session must be active"),
                    )
                    .await?;
                    tracing::info!(
                        "apply_changes(path={}): applied changes from remote: db_size={}",
                        self.main_db_path,
                        db_size,
                    );
                    applied_raw_db_size = db_size;
                    if replace_base_pages {
                        let mut finished_main_session = main_session
                            .take()
                            .expect("main WAL session must be active");
                        finished_main_session.commit(applied_raw_db_size)?;
                        main_conn.end_nested();
                        finished_main_session.wal_session.end(true).map_err(|error| {
                            Error::DatabaseSyncEngineError(format!(
                                "failed to finalize replace-base raw WAL session: {error}",
                            ))
                        })?;
                        drop(finished_main_session);
                        if let Some(mv_store) = main_conn.mv_store().as_ref() {
                            let pager = main_conn.get_pager();
                            let reset = mv_store.reset_logical_log_after_external_restore().map_err(
                                |error| {
                                    Error::DatabaseSyncEngineError(format!(
                                        "failed to reset MVCC logical log after replace-base snapshot: {error}",
                                    ))
                                },
                            )?;
                            drive_core_completion(coro, &pager.io, reset).await.map_err(
                                |error| {
                                    Error::DatabaseSyncEngineError(format!(
                                        "failed to complete MVCC logical log reset after replace-base snapshot: {error}",
                                    ))
                                },
                            )?;
                            if let Some(sync) = mv_store
                                .sync_logical_log_after_external_restore(&main_conn)
                                .map_err(|error| {
                                    Error::DatabaseSyncEngineError(format!(
                                        "failed to sync MVCC logical log after replace-base snapshot: {error}",
                                    ))
                                })?
                            {
                                drive_core_completion(coro, &pager.io, sync).await.map_err(
                                    |error| {
                                        Error::DatabaseSyncEngineError(format!(
                                            "failed to complete MVCC logical log sync after replace-base snapshot: {error}",
                                        ))
                                    },
                                )?;
                            }
                        }
                        reload_connection_after_external_restore(
                            &main_conn,
                            "replace-base snapshot",
                        )?;

                        let mut conn = match connect_untracked(&self.main_tape) {
                            Ok(conn) => conn,
                            Err(Error::TursoError(LimboError::SchemaUpdated)) => {
                                force_reparse_schema_with_retry(&main_conn).map_err(|error| {
                                    Error::DatabaseSyncEngineError(format!(
                                        "failed to refresh schema after replace-base reconnect schema update: {error}",
                                    ))
                                })?;
                                connect_untracked(&self.main_tape).map_err(|error| {
                                    Error::DatabaseSyncEngineError(format!(
                                        "failed to reconnect after replace-base schema refresh: {error}",
                                    ))
                                })?
                            }
                            Err(error) => return Err(error),
                        };
                        force_reparse_schema_with_retry(&conn).map_err(|error| {
                            Error::DatabaseSyncEngineError(format!(
                                "failed to refresh replay connection schema after replace-base snapshot: {error}",
                            ))
                        })?;
                        crate::database_sync_operations::ensure_sync_last_change_id_table(
                            coro,
                            &conn,
                            &self.client_unique_id,
                        )
                        .await
                        .map_err(|error| {
                            Error::DatabaseSyncEngineError(format!(
                                "failed to initialize sync high-water mark table after replace-base snapshot: {error}"
                            ))
                        })?;
                        publish_schema_after_external_restore(
                            &conn,
                            "replace-base sync table initialization",
                        )?;
                        conn = match connect_untracked(&self.main_tape) {
                            Ok(conn) => conn,
                            Err(Error::TursoError(LimboError::SchemaUpdated)) => {
                                force_reparse_schema_with_retry(&main_conn).map_err(|error| {
                                    Error::DatabaseSyncEngineError(format!(
                                        "failed to refresh schema after replace-base sync table initialization: {error}",
                                    ))
                                })?;
                                connect_untracked(&self.main_tape).map_err(|error| {
                                    Error::DatabaseSyncEngineError(format!(
                                        "failed to reconnect after replace-base sync table initialization: {error}",
                                    ))
                                })?
                            }
                            Err(error) => return Err(error),
                        };
                        force_reparse_schema_with_retry(&conn).map_err(|error| {
                            Error::DatabaseSyncEngineError(format!(
                                "failed to refresh replay connection schema after replace-base sync table initialization: {error}",
                            ))
                        })?;
                        publish_schema_after_external_restore(
                            &conn,
                            "replace-base sync table reconnect",
                        )?;
                        execute_with_schema_retry(&conn, "BEGIN IMMEDIATE").map_err(|error| {
                            Error::DatabaseSyncEngineError(format!(
                                "failed to begin replace-base replay transaction: {error}",
                            ))
                        })?;
                        logical_replay_conn = Some(conn);

                        if self.meta().logical_mvcc_pull_active() {
                            let DatabasePullRevision::V1 { revision } = remote_revision else {
                                return Err(Error::DatabaseSyncEngineError(format!(
                                    "replace-base follow-up logical pull requires a V1 revision, got {remote_revision:?}"
                                )));
                            };
                            tracing::info!(
                                "apply_changes(path={}): replace-base snapshot installed; issuing one follow-up logical pull from revision {} before local replay",
                                self.main_db_path,
                                revision,
                            );
                            let ctx = &SyncOperationCtx::new(
                                coro,
                                &self.sync_engine_io,
                                self.meta().remote_url(),
                                self.opts.remote_encryption_key.as_deref(),
                            );
                            match pull_updates_v1(ctx, changes_file, revision, None, true).await? {
                                (next_revision, PullUpdatesV1Result::Logical { txns, ops }, _) => {
                                    tracing::info!(
                                        "apply_changes(path={}): replace-base follow-up logical pull returned {} transactions / {} ops",
                                        self.main_db_path,
                                        txns,
                                        ops,
                                    );
                                    followup_revision = Some(next_revision);
                                    if txns != 0 || ops != 0 || changes_file.size()? != 0 {
                                        let replay_conn = logical_replay_conn
                                            .as_ref()
                                            .expect("replace-base replay connection should exist");
                                        let mut replay = DatabaseReplaySession {
                                            conn: replay_conn.clone(),
                                            cached_delete_stmt: HashMap::new(),
                                            cached_insert_stmt: HashMap::new(),
                                            cached_update_stmt: HashMap::new(),
                                            in_txn: true,
                                            generator: DatabaseReplayGenerator {
                                                conn: replay_conn.clone(),
                                                opts: DatabaseReplaySessionOpts {
                                                    use_implicit_rowid: true,
                                                },
                                            },
                                        };
                                        apply_logical_transactions_file_without_commit_excluding_client_txns_with_table_map_and_stats(
                                            coro,
                                            &mut replay,
                                            changes_file,
                                            &self.client_unique_id,
                                            &mut logical_table_names_by_stable_id,
                                        )
                                        .await
                                        .map_err(|error| {
                                            Error::DatabaseSyncEngineError(format!(
                                                "failed to apply replace-base follow-up logical transactions: {error}",
                                            ))
                                        })?;
                                    }
                                }
                                (
                                    advertised_revision,
                                    PullUpdatesV1Result::Pages { replace_base },
                                    _,
                                ) => {
                                    // A second page snapshot means the remote still cannot serve a
                                    // direct logical range from this revision: its logical log for
                                    // that generation has no replayable prefix. Either the
                                    // generation predates portable logical changes, or its first
                                    // commits carry only internal recovery ops (`turso_sync_*`
                                    // bookkeeping from a push whose user statements matched no
                                    // row) and were written by a remote that predates marking such
                                    // commits explicitly.
                                    //
                                    // Installing this snapshot here is not possible: the WAL
                                    // sessions for the one we just applied are still open, and the
                                    // apply path is a single linear pass. Retrying in place would
                                    // also be pointless - the remote's answer only changes when its
                                    // log state does. So fail cleanly and let the caller's next
                                    // pull() re-enter apply from scratch: that retry succeeds once
                                    // the remote rolls over to a new generation, without recreating
                                    // the database.
                                    return Err(Error::DatabaseSyncEngineError(format!(
                                        "remote cannot serve a logical MVCC range for {main_db_path} from revision {revision}: \
                                         the replace-base follow-up pull returned another page snapshot \
                                         (replace_base={replace_base}, advertised revision {advertised_revision:?}). \
                                         The remote's logical log for that generation has no prefix this client can replay. \
                                         This is safe to retry: pull again after the remote checkpoints to a new generation, \
                                         or upgrade the remote to a build that marks internal-only commits explicitly.",
                                        main_db_path = self.main_db_path,
                                    )));
                                }
                            }
                        }
                    }
                }
                DbChangesStreamKind::Logical => {
                    unreachable!("logical streams are handled by apply_logical_mvcc_changes_internal")
                }
                DbChangesStreamKind::LegacyPages
                | DbChangesStreamKind::Pages
                | DbChangesStreamKind::ReplaceBasePages => {
                    unreachable!("page stream kinds are handled by stream_kind_applies_remote_pages")
                }
            }

            let raw_page_replay_on_sql_conn = should_replay_raw_pages_on_sql_conn(
                self.opts.protocol_version_hint,
                self.meta().logical_mvcc_pull_active(),
                logical_replay_conn.is_some(),
                stream_kind,
                main_conn.mv_store().as_ref().is_some(),
            );
            if raw_page_replay_on_sql_conn {
                let mut finished_main_session = main_session
                    .take()
                    .expect("main WAL session must be active");
                finished_main_session.commit(applied_raw_db_size)?;
                main_conn.end_nested();
                finished_main_session.wal_session.end(true).map_err(|error| {
                    Error::DatabaseSyncEngineError(format!(
                        "failed to finalize raw page WAL session before local replay: {error}",
                    ))
                })?;
                drop(finished_main_session);
                // Refresh main_conn's own schema from the replayed pages, but do NOT publish
                // it to the shared cache. Publishing the intermediate schema here would expose
                // a turso_cdc-less view (the raw pages replace the local-only CDC table) to
                // concurrent CDC-enabled statements on other connections, which would then fail
                // to compile with "no such table: turso_cdc". The shared cache is published
                // once, at the end of apply, after the replay connection recreates turso_cdc.
                refresh_schema_after_raw_wal_replay_local(&main_conn, "raw WAL replay")?;

                let conn = connect_untracked(&self.main_tape).map_err(|error| {
                    Error::DatabaseSyncEngineError(format!(
                        "failed to open post-raw-apply replay connection: {error}",
                    ))
                })?;
                // The replay connection is seeded from the (deliberately not-yet-updated)
                // shared schema, so reparse it locally from the freshly replayed pages before
                // it replays local changes.
                refresh_schema_after_raw_wal_replay_local(&conn, "raw WAL replay replay-conn")?;
                execute_with_schema_retry(&conn, "BEGIN IMMEDIATE").map_err(|error| {
                    Error::DatabaseSyncEngineError(format!(
                        "failed to begin post-raw-apply replay transaction: {error}",
                    ))
                })?;
                logical_replay_conn = Some(conn);
            }
            let phase_conn = logical_replay_conn.as_ref().unwrap_or(&main_conn);
            if logical_replay_conn.is_none() {
                main_session
                    .as_mut()
                    .expect("main WAL session must be active")
                    .commit(0)?;
                revert_since_wal_watermark = Some(
                    main_session
                        .as_ref()
                        .expect("main WAL session must be active")
                        .frames_count()?,
                );
                if stream_kind_applies_remote_pages(stream_kind) && !replace_base_pages {
                    let current_schema_version = main_conn.read_schema_version()?;
                    let final_schema_version =
                        current_schema_version.max(main_conn_schema_version) + 1;
                    main_conn.write_schema_version(final_schema_version)?;
                    tracing::info!(
                        "apply_changes(path={}): updated schema version to {}",
                        self.main_db_path,
                        final_schema_version
                    );
                }
            }
            let use_pushed_change_hint =
                use_pushed_change_hint_for_local_replay(stream_kind, raw_page_replay_on_sql_conn);

            // Phase 4: as now DB has all data from remote - let's read pull generation and last change id for current client
            let (remote_pull_gen, remote_last_change_id) =
                read_last_change_id(coro, phase_conn, &self.client_unique_id)
                    .await
                    .map_err(|error| {
                        Error::DatabaseSyncEngineError(format!(
                            "failed to read last_change_id after remote apply: {error}",
                        ))
                    })?;

            if remote_pull_gen > local_pull_gen {
                return Err(Error::DatabaseSyncEngineError(format!(
                    "protocol error: remote_pull_gen > local_pull_gen: {remote_pull_gen} > {local_pull_gen}"
                )));
            }
            let last_change_id = resolve_local_replay_floor_change_id(
                use_pushed_change_hint,
                local_pull_gen,
                remote_pull_gen,
                remote_last_change_id,
                last_pushed_pull_gen_hint,
                last_pushed_change_id_hint,
            );
            tracing::info!(
                "apply_changes(path={}): local replay floor: use_pushed_change_hint={} local_pull_gen={} remote_pull_gen={} remote_last_change_id={:?} last_pushed_pull_gen_hint={} last_pushed_change_id_hint={} resolved_last_change_id={:?}",
                self.main_db_path,
                use_pushed_change_hint,
                local_pull_gen,
                remote_pull_gen,
                remote_last_change_id,
                last_pushed_pull_gen_hint,
                last_pushed_change_id_hint,
                last_change_id,
            );

            // Phase 5: collect/filter local changes for replay.
            let replay_floor = last_change_id.unwrap_or(0);
            let local_changes = if precollect_local_changes_before_remote_apply {
                precollected_local_changes
                    .into_iter()
                    .filter(|change| change.change_id > replay_floor)
                    .collect::<Vec<_>>()
            } else {
                let iterate_opts = DatabaseChangesIteratorOpts {
                    first_change_id: Some(replay_floor + 1),
                    mode: DatabaseChangesIteratorMode::Apply,
                    ignore_schema_changes: false,
                    ..Default::default()
                };
                let mut local_changes = Vec::new();
                let mut iterator = self.main_tape.iterate_changes(iterate_opts)?;
                while let Some(operation) = iterator.next(coro).await? {
                    match operation {
                        DatabaseTapeOperation::RowChange(change) => local_changes.push(change),
                        DatabaseTapeOperation::Commit => continue,
                        DatabaseTapeOperation::StmtReplay(_)
                        | DatabaseTapeOperation::SchemaReplay(_) => {
                            panic!("changes iterator must not use replay-only operations")
                        }
                    }
                }
                local_changes
            };
            let mut skipped_internal_local_changes = 0usize;
            let local_changes = local_changes
                .into_iter()
                .filter_map(|change| match should_replay_local_change(&change) {
                    Ok(true) => Some(Ok(change)),
                    Ok(false) => {
                        skipped_internal_local_changes += 1;
                        None
                    }
                    Err(error) => Some(Err(error)),
                })
                .collect::<Result<Vec<_>>>()?;
            let replayed_local_changes = !local_changes.is_empty();
            let recaptured_local_changes = replayed_local_changes;
            let mut preserve_local_replay_floor = None;
            tracing::info!(
                "apply_changes(path={}): collected {} changes, skipped_internal_local_changes={}",
                self.main_db_path,
                local_changes.len(),
                skipped_internal_local_changes,
            );
            let post_remote_apply_change_id = if replace_base_pages || raw_page_replay_on_sql_conn {
                max_local_change_id(coro, phase_conn).await?.unwrap_or(0)
            } else {
                0
            };

            // Phase 6: replay local changes.
            if !local_changes.is_empty()
                || local_rollback != 0
                || remote_rollback != 0
                || had_cdc_table
            {
                if logical_replay_conn.is_none() {
                    phase_conn.reset_main_mvcc_tx_for_wal_session();
                }
                if replace_base_pages || raw_page_replay_on_sql_conn {
                    tracing::info!(
                        "apply_changes(path={}): skipping pre-replay sync high-water reset; final high-water will be persisted after replay commit",
                        self.main_db_path
                    );
                } else {
                    update_last_change_id(
                        coro,
                        phase_conn,
                        &self.client_unique_id,
                        local_pull_gen + 1,
                        0,
                    )
                    .await
                    .map_err(|error| {
                        Error::DatabaseSyncEngineError(format!(
                            "failed to reset sync high-water mark before local replay: {error}",
                        ))
                    })
                    .inspect_err(|e| tracing::error!("update_last_change_id failed: {e}"))?;
                }

                let mut cdc_enabled_for_local_replay = false;
                if replayed_local_changes && had_cdc_table {
                    tracing::info!(
                        "apply_changes(path={}): reinitialize CDC pragma before local replay",
                        self.main_db_path,
                    );
                    phase_conn
                        .pragma_update(CDC_PRAGMA_NAME, "'full'")
                        .map_err(|error| {
                            Error::DatabaseSyncEngineError(format!(
                                "failed to reinitialize CDC pragma before local replay: {error}",
                            ))
                        })?;
                    cdc_enabled_for_local_replay = true;
                }

                let mut replay = DatabaseReplaySession {
                    conn: phase_conn.clone(),
                    cached_delete_stmt: HashMap::new(),
                    cached_insert_stmt: HashMap::new(),
                    cached_update_stmt: HashMap::new(),
                    in_txn: true,
                    generator: DatabaseReplayGenerator {
                        conn: phase_conn.clone(),
                        opts: DatabaseReplaySessionOpts {
                            use_implicit_rowid: raw_page_replay_on_sql_conn,
                        },
                    },
                };

                let should_transform_local_change = |change: &DatabaseTapeRowChange| {
                    is_logically_replayable_table(&change.table_name)
                        && !self
                            .opts
                            .tables_ignore
                            .iter()
                            .any(|table| table == &change.table_name)
                };
                let transform_changes = if self.opts.use_transform {
                    local_changes
                        .iter()
                        .filter(|change| should_transform_local_change(change))
                        .cloned()
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                let mut transformed = if self.opts.use_transform {
                    let ctx = &SyncOperationCtx::new(
                        coro,
                        &self.sync_engine_io,
                        self.meta().remote_url(),
                        self.opts.remote_encryption_key.as_deref(),
                    );
                    Some(apply_transformation(ctx, &transform_changes, &replay.generator).await?)
                } else {
                    None
                };
                let mut transform_index = 0usize;

                assert!(!replay.conn().get_auto_commit());
                for (i, change) in local_changes.into_iter().enumerate() {
                    let preserve_original_change_floor =
                        matches!(&change.change, DatabaseTapeRowChangeType::Delete { .. });
                    let original_change_floor = change.change_id.saturating_sub(1);
                    let operation = if should_transform_local_change(&change) {
                        if let Some(transformed) = &mut transformed {
                            let transform_result = std::mem::replace(
                                &mut transformed[transform_index],
                                DatabaseRowTransformResult::Skip,
                            );
                            transform_index += 1;
                            match transform_result {
                                DatabaseRowTransformResult::Keep => {
                                    DatabaseTapeOperation::RowChange(change)
                                }
                                DatabaseRowTransformResult::Skip => continue,
                                DatabaseRowTransformResult::Rewrite(replay) => {
                                    DatabaseTapeOperation::StmtReplay(replay)
                                }
                            }
                        } else {
                            DatabaseTapeOperation::RowChange(change)
                        }
                    } else {
                        DatabaseTapeOperation::RowChange(change)
                    };
                    if had_cdc_table && !cdc_enabled_for_local_replay {
                        tracing::info!(
                            "apply_changes(path={}): reinitialize CDC pragma before unpushed local replay",
                            self.main_db_path,
                        );
                        phase_conn
                            .pragma_update(CDC_PRAGMA_NAME, "'full'")
                            .map_err(|error| {
                                Error::DatabaseSyncEngineError(format!(
                                    "failed to reinitialize CDC pragma before unpushed local replay: {error}",
                                ))
                            })?;
                        cdc_enabled_for_local_replay = true;
                    }
                    if preserve_original_change_floor {
                        preserve_local_replay_floor = Some(
                            preserve_local_replay_floor.map_or(
                                original_change_floor,
                                |floor: i64| floor.min(original_change_floor),
                            ),
                        );
                    }
                    maybe_inject_replace_base_local_replay_failure(replace_base_pages, i)?;
                    replay.replay(coro, operation).await.map_err(|error| {
                        Error::DatabaseSyncEngineError(format!(
                            "failed to replay local change after remote apply: {error}",
                        ))
                    })?;
                }
                assert!(!replay.conn().get_auto_commit());
            }

            if let Some(logical_conn) = logical_replay_conn {
                logical_conn.execute("COMMIT").map_err(|error| {
                    Error::DatabaseSyncEngineError(format!(
                        "failed to commit remote apply replay transaction: {error}",
                    ))
                })?;
                if had_cdc_table || replace_base_pages {
                    tracing::info!(
                        "apply_changes(path={}): reinitialize CDC pragma after logical replay commit",
                        self.main_db_path,
                    );
                    logical_conn
                        .pragma_update(CDC_PRAGMA_NAME, "'full'")
                        .map_err(|error| {
                            Error::DatabaseSyncEngineError(format!(
                                "failed to reinitialize CDC pragma after remote apply: {error}",
                            ))
                        })?;
                    let change_id = max_local_change_id(coro, &logical_conn).await?.unwrap_or(0);
                    tracing::info!(
                        "apply_changes(path={}): post-logical-replay CDC high-water mark: {}",
                        self.main_db_path,
                        change_id
                    );
                    let synced_change_id = synced_change_id_after_remote_apply(
                        recaptured_local_changes,
                        pre_apply_local_change_id,
                        change_id,
                    );
                    let synced_change_id = if recaptured_local_changes && raw_page_replay_on_sql_conn
                    {
                        post_remote_apply_change_id
                    } else {
                        synced_change_id
                    };
                    let synced_change_id = preserve_local_replay_floor
                        .map_or(synced_change_id, |floor| synced_change_id.min(floor));
                    let synced_change_id = synced_change_id.min(change_id);
                    if raw_page_replay_on_sql_conn {
                        rebuild_local_sync_metadata_table(
                            coro,
                            &logical_conn,
                            &self.client_unique_id,
                            local_pull_gen + 1,
                            synced_change_id,
                        )
                        .await?;
                    } else {
                        update_last_change_id(
                            coro,
                            &logical_conn,
                            &self.client_unique_id,
                            local_pull_gen + 1,
                            synced_change_id,
                        )
                        .await?;
                    }
                }
                // Publish the post-apply schema to the shared cache exactly once, and only
                // after any turso_cdc recreation above. Publishing between the replay commit
                // and the CDC-table recreation would briefly expose a version-bumped schema
                // that omits turso_cdc.
                logical_conn.publish_schema_if_newer();
                let logical_table_names_by_stable_id =
                    read_logical_replay_table_map(coro, &logical_conn).await?;
                return Ok((
                    logical_conn.wal_state()?.max_frame,
                    logical_table_names_by_stable_id,
                    followup_revision,
                ));
            }
            let raw_replay_refresh = replace_base_pages || raw_page_replay_on_sql_conn;
            let recreate_cdc =
                replace_base_pages || had_cdc_table && stream_kind_applies_remote_pages(stream_kind);

            // Common path (no external page replacement): recreate turso_cdc *inside* the apply
            // transaction, before the commit below, so the committed on-disk state is never
            // observed without turso_cdc. A concurrent reader that sees the new schema cookie
            // reparses its schema from disk (see Connection::maybe_reparse_schema); if turso_cdc
            // were missing from the committed image at that instant, a CDC-enabled statement
            // would fail to compile with "no such table: turso_cdc". Recreating within the same
            // commit keeps the transition atomic (this matches the pre-MVCC apply ordering).
            //
            // The raw-replay path cannot do this: it must commit first so the schema can be
            // reparsed from the externally replaced pages, so it recreates turso_cdc after the
            // commit instead (see below).
            if recreate_cdc && !raw_replay_refresh {
                tracing::info!(
                    "apply_changes(path={}): reinitialize CDC pragma before remote apply commit",
                    self.main_db_path,
                );
                main_conn
                    .pragma_update(CDC_PRAGMA_NAME, "'full'")
                    .map_err(|error| {
                        Error::DatabaseSyncEngineError(format!(
                            "failed to reinitialize CDC pragma before remote apply commit: {error}",
                        ))
                    })?;
            }

            if main_conn.has_main_mvcc_tx_for_wal_session() {
                main_conn
                    .commit_main_mvcc_tx_for_wal_session()
                    .map_err(|error| {
                        Error::DatabaseSyncEngineError(format!(
                            "failed to commit main MVCC transaction after remote apply: {error}",
                        ))
                    })?;
            }
            main_conn.end_nested();
            // wal_session.end(force_commit=true) fsyncs the WAL before returning, so the
            // caller can durably record this revision as synced in the metadata.
            main_session
                .take()
                .expect("main WAL session must be active")
                .wal_session
                .end(true)
                .map_err(|error| {
                    Error::DatabaseSyncEngineError(format!(
                        "failed to finalize raw WAL session after remote apply: {error}",
                    ))
                })?;
            if raw_replay_refresh {
                // Refresh this connection's own schema from the replayed pages, but do NOT
                // publish it to the shared cache yet. Publishing happens once below, after
                // any turso_cdc recreation, so a concurrent CDC-enabled statement on another
                // connection never reparses to a schema that lacks turso_cdc.
                refresh_schema_after_raw_wal_replay_local(&main_conn, "raw WAL replay")?;
                if recreate_cdc {
                    tracing::info!(
                        "apply_changes(path={}): reinitialize CDC pragma after WAL replay commit",
                        self.main_db_path,
                    );
                    execute_with_schema_retry(
                        &main_conn,
                        &format!("PRAGMA {CDC_PRAGMA_NAME} = 'full'"),
                    )
                    .map_err(|error| {
                        Error::DatabaseSyncEngineError(format!(
                            "failed to reinitialize CDC pragma after remote apply: {error}",
                        ))
                    })?;
                }
            }
            // Publish the post-replay schema to the shared cache once turso_cdc has been
            // recreated (either pre-commit for the common path, or just above for the
            // raw-replay path) and before the high-water-mark bookkeeping below. The published
            // schema always contains turso_cdc, so the transition is atomic — a concurrent
            // CDC-enabled statement never observes a schema lacking turso_cdc ("no such table:
            // turso_cdc"). Publishing before the bookkeeping also lets concurrent readers adopt
            // the shared schema cheaply instead of reparsing from disk during that window.
            if raw_replay_refresh || recreate_cdc {
                publish_schema_after_external_restore(
                    &main_conn,
                    "post-raw-WAL-replay schema publish",
                )?;
            }
            if recreate_cdc {
                let change_id = max_local_change_id(coro, &main_conn).await?.unwrap_or(0);
                tracing::info!(
                    "apply_changes(path={}): post-WAL-replay CDC high-water mark: {}",
                    self.main_db_path,
                    change_id
                );
                let synced_change_id = synced_change_id_after_remote_apply(
                    recaptured_local_changes,
                    pre_apply_local_change_id,
                    change_id,
                );
                let synced_change_id =
                    if recaptured_local_changes && (replace_base_pages || raw_page_replay_on_sql_conn)
                    {
                        post_remote_apply_change_id
                    } else {
                        synced_change_id
                };
                let synced_change_id = preserve_local_replay_floor
                    .map_or(synced_change_id, |floor| synced_change_id.min(floor));
                let synced_change_id = synced_change_id.min(change_id);
                update_last_change_id(
                    coro,
                    &main_conn,
                    &self.client_unique_id,
                    local_pull_gen + 1,
                    synced_change_id,
                )
                .await
                .map_err(|error| {
                    Error::DatabaseSyncEngineError(format!(
                        "failed to persist sync high-water mark after raw remote apply: {error}",
                    ))
                })?;
            }

            let logical_table_names_by_stable_id =
                read_logical_replay_table_map(coro, &main_conn).await?;
            let revert_since_wal_watermark =
                revert_since_wal_watermark.unwrap_or(main_conn.wal_state()?.max_frame);
            Ok((
                revert_since_wal_watermark,
                logical_table_names_by_stable_id,
                followup_revision,
            ))
        }
        .await;

        match apply_result {
            Ok(frame) => {
                if let Some(guard) = &mut replace_base_guard {
                    guard.mark_complete(coro).await?;
                }
                Ok(frame)
            }
            Err(error) => {
                if let Some(guard) = &replace_base_guard {
                    let error = guard.restore_after_error(coro, error).await;
                    match reload_connection_after_external_restore(
                        &main_conn,
                        "replace-base error recovery",
                    ) {
                        Ok(()) => Err(error),
                        Err(reload_error) => Err(Error::DatabaseSyncEngineError(format!(
                            "{error}; additionally failed to reload restored database state: {reload_error}",
                        ))),
                    }
                } else {
                    Err(error)
                }
            }
        }
    }

    /// Sync local changes to remote DB
    /// This method will **not** pull remote changes to the local DB
    /// This method will **not** block writes for the period of sync
    pub async fn push_changes_to_remote<Ctx>(&self, coro: &Coro<Ctx>) -> Result<()> {
        tracing::info!("push_changes(path={})", self.main_db_path);

        let ctx = &SyncOperationCtx::new(
            coro,
            &self.sync_engine_io,
            self.meta().remote_url(),
            self.opts.remote_encryption_key.as_deref(),
        );
        let (pull_gen, replay_floor_change_id, change_id) =
            push_logical_changes(ctx, &self.main_tape, &self.client_unique_id, &self.opts).await?;

        self.update_meta(coro, |m| {
            m.last_pushed_pull_gen_hint = pull_gen;
            m.last_pushed_replay_floor_change_id_hint = replay_floor_change_id;
            m.last_pushed_change_id_hint = change_id;
            m.last_push_unix_time = Some(self.io.current_time_wall_clock().secs);
        })
        .await?;

        Ok(())
    }

    /// Create read/write database connection and appropriately configure it before use
    pub async fn connect_rw<Ctx>(&self, coro: &Coro<Ctx>) -> Result<Arc<turso_core::Connection>> {
        let conn = self.main_tape.connect(coro).await?;
        assert_eq!(
            conn.wal_auto_actions(),
            turso_core::WalAutoActions::empty(),
            "tape must be configured to have all auto-WAL actions disabled"
        );
        Ok(conn)
    }

    /// Sync local changes to remote DB and bring new changes from remote to local
    /// This method will block writes for the period of sync
    pub async fn sync<Ctx>(&self, coro: &Coro<Ctx>) -> Result<()> {
        // todo(sivukhin): this is bit suboptimal as both 'push' and 'pull' will call pull_synced_from_remote
        // but for now - keep it simple
        self.push_changes_to_remote(coro).await?;
        self.pull_changes_from_remote(coro).await?;
        Ok(())
    }

    pub async fn pull_changes_from_remote<Ctx>(&self, coro: &Coro<Ctx>) -> Result<()> {
        let changes = self.wait_changes_from_remote(coro).await?;
        self.apply_changes_from_remote(coro, changes).await?;
        Ok(())
    }

    fn meta(&self) -> std::sync::MutexGuard<'_, DatabaseMetadata> {
        self.meta.lock().unwrap()
    }

    async fn update_meta<Ctx>(
        &self,
        coro: &Coro<Ctx>,
        update: impl FnOnce(&mut DatabaseMetadata),
    ) -> Result<()> {
        let mut meta = self.meta().clone();
        update(&mut meta);
        tracing::info!("update_meta: {meta:?}");
        full_write(
            coro,
            self.io.clone(),
            self.sync_engine_io.io.clone(),
            &self.meta_path,
            is_memory(&self.main_db_path),
            meta.dump()?,
        )
        .await?;
        // todo: what happen if we will actually update the metadata on disk but fail and so in memory state will not be updated
        *self.meta.lock().unwrap() = meta;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        create_main_db_log_path, create_main_db_wal_path, create_meta_path,
        create_replace_base_marker_path, create_revert_db_wal_path,
        ensure_logical_mvcc_pull_supported, ensure_stream_kind_can_use_legacy_page_apply,
        replace_base_backup_path, resolve_local_replay_floor_change_id,
        resolve_remote_pull_protocol, should_replay_raw_pages_on_sql_conn,
        should_request_logical_pull, stream_kind_applies_remote_pages,
        stream_kind_for_pull_updates_v1_result, sync_database_file_paths,
        synced_change_id_after_remote_apply, use_pushed_change_hint_for_local_replay,
        DatabaseSyncEngine, DatabaseSyncEngineOpts, ReplaceBaseApplyGuard,
        REPLACE_BASE_LOCAL_REPLAY_FAILURE_AFTER,
    };
    use crate::{
        client_proto::{
            LogicalOp, LogicalOpType, LogicalSchemaAction, LogicalSchemaKind, LogicalTxnData,
        },
        database_sync_engine_io::{DataCompletion, DataPollResult, SyncEngineIo},
        database_sync_operations::{
            count_local_changes, max_local_change_id, read_last_change_id, update_last_change_id,
            MutexSlot, PullUpdatesV1Result, SyncEngineIoStats,
        },
        database_tape::{run_stmt_once, DatabaseTape, DatabaseTapeOpts},
        errors::Error,
        io_operations::IoOperations,
        server_proto::{
            PageData, PageSetRawEncodingProto, PullUpdatesApplyMode, PullUpdatesProtocol,
            PullUpdatesReqProtoBody, PullUpdatesRespProtoBody, PullUpdatesStreamKind,
        },
        types::{
            Coro, DatabaseMetadata, DatabasePullRevision, DatabaseSavedConfiguration,
            DatabaseSyncEngineProtocolVersion, DbChangesStatus, DbChangesStreamKind,
            PartialSyncOpts, RemotePullProtocol, SyncEngineIoResult, DATABASE_METADATA_VERSION,
        },
        Result,
    };
    use bytes::Bytes;
    use prost::Message;
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };
    use tempfile::NamedTempFile;
    use turso_core::SqliteDialect;

    #[test]
    fn sync_database_paths_include_core_sync_and_mvcc_files() {
        assert_eq!(
            sync_database_file_paths("replica.sqlite"),
            vec![
                "replica.sqlite",
                "replica.sqlite-wal",
                "replica.sqlite-wal-revert",
                "replica.sqlite-info",
                "replica.sqlite-changes",
                "replica.db-log",
            ]
        );
    }

    #[test]
    fn explicit_override_wins_over_persisted_protocol() {
        for persisted in [
            RemotePullProtocol::Unknown,
            RemotePullProtocol::Pages,
            RemotePullProtocol::MvccLogical,
        ] {
            assert_eq!(
                resolve_remote_pull_protocol(Some(true), persisted),
                RemotePullProtocol::MvccLogical
            );
            assert_eq!(
                resolve_remote_pull_protocol(Some(false), persisted),
                RemotePullProtocol::Pages
            );
            assert_eq!(resolve_remote_pull_protocol(None, persisted), persisted);
        }
    }

    #[test]
    fn logical_mvcc_pull_with_partial_sync_is_a_hard_error() {
        // Silent downgrade to page pull would just defer the failure to an
        // opaque server-side protocol error on every incremental pull.
        assert!(ensure_logical_mvcc_pull_supported(true, None).is_err());
    }

    #[test]
    fn logical_mvcc_pull_with_remote_encryption_is_a_hard_error() {
        assert!(ensure_logical_mvcc_pull_supported(false, Some("key")).is_err());
    }

    #[test]
    fn logical_mvcc_pull_remains_enabled_for_plain_full_sync() {
        assert!(ensure_logical_mvcc_pull_supported(false, None).is_ok());
    }

    #[test]
    fn logical_pull_is_requested_only_for_active_v1_revisions() {
        assert!(!should_request_logical_pull(false, &None));
        assert!(!should_request_logical_pull(true, &None));
        assert!(!should_request_logical_pull(
            true,
            &Some(DatabasePullRevision::Legacy {
                generation: 1,
                synced_frame_no: Some(10),
            })
        ));
        assert!(should_request_logical_pull(
            true,
            &Some(DatabasePullRevision::V1 {
                revision: "g1:o42".to_string(),
            })
        ));
    }

    #[test]
    fn legacy_page_apply_rejects_non_page_streams() {
        assert!(
            ensure_stream_kind_can_use_legacy_page_apply(DbChangesStreamKind::LegacyPages).is_ok()
        );
        assert!(ensure_stream_kind_can_use_legacy_page_apply(DbChangesStreamKind::Pages).is_ok());
        let logical_err =
            ensure_stream_kind_can_use_legacy_page_apply(DbChangesStreamKind::Logical).unwrap_err();
        assert!(
            logical_err.to_string().contains("logical MVCC apply"),
            "unexpected error: {logical_err:?}"
        );
        let replace_base_err =
            ensure_stream_kind_can_use_legacy_page_apply(DbChangesStreamKind::ReplaceBasePages)
                .unwrap_err();
        assert!(
            replace_base_err
                .to_string()
                .contains("replace-base page apply"),
            "unexpected error: {replace_base_err:?}"
        );
    }

    #[test]
    fn logical_pull_page_fallback_preserves_replace_base_kind() {
        assert_eq!(
            stream_kind_for_pull_updates_v1_result(&PullUpdatesV1Result::Pages {
                replace_base: false
            }),
            DbChangesStreamKind::Pages
        );
        assert_eq!(
            stream_kind_for_pull_updates_v1_result(&PullUpdatesV1Result::Pages {
                replace_base: true
            }),
            DbChangesStreamKind::ReplaceBasePages
        );
        assert_eq!(
            stream_kind_for_pull_updates_v1_result(&PullUpdatesV1Result::Logical {
                txns: 1,
                ops: 2
            }),
            DbChangesStreamKind::Logical
        );
    }

    #[test]
    fn replace_base_pages_use_remote_page_transport() {
        assert!(stream_kind_applies_remote_pages(
            DbChangesStreamKind::LegacyPages
        ));
        assert!(stream_kind_applies_remote_pages(DbChangesStreamKind::Pages));
        assert!(stream_kind_applies_remote_pages(
            DbChangesStreamKind::ReplaceBasePages
        ));
        assert!(!stream_kind_applies_remote_pages(
            DbChangesStreamKind::Logical
        ));
    }

    #[test]
    fn sql_replay_page_routing_keeps_legacy_on_wal_session() {
        assert!(!should_replay_raw_pages_on_sql_conn(
            DatabaseSyncEngineProtocolVersion::Legacy,
            true,
            false,
            DbChangesStreamKind::LegacyPages,
            true,
        ));
        assert!(!should_replay_raw_pages_on_sql_conn(
            DatabaseSyncEngineProtocolVersion::Legacy,
            true,
            false,
            DbChangesStreamKind::ReplaceBasePages,
            true,
        ));
        assert!(!should_replay_raw_pages_on_sql_conn(
            DatabaseSyncEngineProtocolVersion::V1,
            false,
            false,
            DbChangesStreamKind::Pages,
            true,
        ));
        assert!(!should_replay_raw_pages_on_sql_conn(
            DatabaseSyncEngineProtocolVersion::V1,
            true,
            false,
            DbChangesStreamKind::Pages,
            false,
        ));
        assert!(should_replay_raw_pages_on_sql_conn(
            DatabaseSyncEngineProtocolVersion::V1,
            true,
            false,
            DbChangesStreamKind::Pages,
            true,
        ));
        assert!(should_replay_raw_pages_on_sql_conn(
            DatabaseSyncEngineProtocolVersion::V1,
            true,
            false,
            DbChangesStreamKind::ReplaceBasePages,
            true,
        ));
        assert!(!should_replay_raw_pages_on_sql_conn(
            DatabaseSyncEngineProtocolVersion::V1,
            true,
            true,
            DbChangesStreamKind::ReplaceBasePages,
            true,
        ));
        assert!(!should_replay_raw_pages_on_sql_conn(
            DatabaseSyncEngineProtocolVersion::V1,
            true,
            false,
            DbChangesStreamKind::Logical,
            true,
        ));
    }

    #[test]
    fn v1_page_replay_uses_remote_snapshot_sync_row_not_later_push_hint() {
        assert!(!use_pushed_change_hint_for_local_replay(
            DbChangesStreamKind::Pages,
            false
        ));
        let floor = resolve_local_replay_floor_change_id(false, 7, 7, Some(12), 7, 34);
        assert_eq!(floor, Some(12));
    }

    #[test]
    fn legacy_page_replay_can_use_last_pushed_hint_when_sync_row_is_stale() {
        assert!(use_pushed_change_hint_for_local_replay(
            DbChangesStreamKind::LegacyPages,
            false
        ));
        let floor = resolve_local_replay_floor_change_id(true, 7, 7, Some(12), 7, 34);
        assert_eq!(floor, Some(34));
    }

    #[test]
    fn local_replay_ignores_last_pushed_hint_from_stale_pull_generation() {
        let floor = resolve_local_replay_floor_change_id(true, 7, 7, Some(12), 6, 34);
        assert_eq!(floor, Some(12));
    }

    #[test]
    fn raw_wal_replay_preserves_existing_floor_when_hints_are_disabled() {
        let floor = resolve_local_replay_floor_change_id(false, 7, 7, Some(12), 7, 34);
        assert_eq!(floor, Some(12));
    }

    #[test]
    fn remote_apply_acknowledges_pre_apply_cdc_when_local_changes_are_recaptured() {
        assert_eq!(
            synced_change_id_after_remote_apply(false, Some(12), 40),
            40,
            "without local replay, all CDC generated by remote apply is acknowledged"
        );
        assert_eq!(
            synced_change_id_after_remote_apply(true, Some(12), 40),
            12,
            "with local replay, preserve the replay floor so local rows remain pushable"
        );
        assert_eq!(
            synced_change_id_after_remote_apply(true, Some(11), 8),
            8,
            "the persisted sync floor must never advance beyond the local CDC high-water"
        );
        assert_eq!(
            synced_change_id_after_remote_apply(true, None, 40),
            0,
            "a database with no pre-existing CDC still starts from zero"
        );
    }

    struct EmptyPollResult<T>(Vec<T>);

    impl<T: Send + Sync + 'static> DataPollResult<T> for EmptyPollResult<T> {
        fn data(&self) -> &[T] {
            &self.0
        }
    }

    struct EmptyCompletion<T> {
        data: Mutex<Option<Vec<T>>>,
    }

    impl<T> EmptyCompletion<T> {
        fn empty() -> Self {
            Self {
                data: Mutex::new(Some(Vec::new())),
            }
        }

        fn with_data(data: Vec<T>) -> Self {
            Self {
                data: Mutex::new(Some(data)),
            }
        }
    }

    impl<T: Send + Sync + 'static> DataCompletion<T> for EmptyCompletion<T> {
        type DataPollResult = EmptyPollResult<T>;

        fn status(&self) -> Result<Option<u16>> {
            Ok(Some(200))
        }

        fn poll_data(&self) -> Result<Option<Self::DataPollResult>> {
            let data = self.data.lock().unwrap().take().unwrap_or_default();
            if data.is_empty() {
                Ok(None)
            } else {
                Ok(Some(EmptyPollResult(data)))
            }
        }

        fn is_done(&self) -> Result<bool> {
            Ok(self.data.lock().unwrap().as_ref().is_none_or(Vec::is_empty))
        }
    }

    #[derive(Default)]
    struct NoopSyncEngineIo;

    impl SyncEngineIo for NoopSyncEngineIo {
        type DataCompletionBytes = EmptyCompletion<u8>;
        type DataCompletionTransform = EmptyCompletion<crate::types::DatabaseRowTransformResult>;

        fn full_read(&self, path: &str) -> Result<Self::DataCompletionBytes> {
            let data = match std::fs::read(path) {
                Ok(data) => data,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(error) => {
                    return Err(crate::errors::Error::DatabaseSyncEngineError(format!(
                        "test full_read failed for {path}: {error}"
                    )));
                }
            };
            Ok(EmptyCompletion::with_data(data))
        }

        fn full_write(&self, path: &str, content: Vec<u8>) -> Result<Self::DataCompletionBytes> {
            std::fs::write(path, content).map_err(|error| {
                crate::errors::Error::DatabaseSyncEngineError(format!(
                    "test full_write failed for {path}: {error}"
                ))
            })?;
            Ok(EmptyCompletion::empty())
        }

        fn transform(
            &self,
            _mutations: Vec<crate::types::DatabaseRowMutation>,
        ) -> Result<Self::DataCompletionTransform> {
            Ok(EmptyCompletion::empty())
        }

        fn http(
            &self,
            _url: Option<&str>,
            _method: &str,
            _path: &str,
            _body: Option<Vec<u8>>,
            _headers: &[(&str, &str)],
        ) -> Result<Self::DataCompletionBytes> {
            Ok(EmptyCompletion::empty())
        }

        fn add_io_callback(&self, _callback: Box<dyn FnMut() -> bool + Send>) {}

        fn step_io_callbacks(&self) {}
    }

    struct CapturingSyncEngineIo {
        response: Mutex<Option<Vec<u8>>>,
        #[allow(clippy::type_complexity)]
        request: Mutex<Option<(String, String, Option<Vec<u8>>)>>,
    }

    impl SyncEngineIo for CapturingSyncEngineIo {
        type DataCompletionBytes = EmptyCompletion<u8>;
        type DataCompletionTransform = EmptyCompletion<crate::types::DatabaseRowTransformResult>;

        fn full_read(&self, path: &str) -> Result<Self::DataCompletionBytes> {
            let data = std::fs::read(path).unwrap_or_default();
            Ok(EmptyCompletion::with_data(data))
        }

        fn full_write(&self, path: &str, content: Vec<u8>) -> Result<Self::DataCompletionBytes> {
            std::fs::write(path, content).map_err(|error| {
                crate::errors::Error::DatabaseSyncEngineError(format!(
                    "test full_write failed for {path}: {error}"
                ))
            })?;
            Ok(EmptyCompletion::empty())
        }

        fn transform(
            &self,
            _mutations: Vec<crate::types::DatabaseRowMutation>,
        ) -> Result<Self::DataCompletionTransform> {
            Ok(EmptyCompletion::empty())
        }

        fn http(
            &self,
            _url: Option<&str>,
            method: &str,
            path: &str,
            body: Option<Vec<u8>>,
            _headers: &[(&str, &str)],
        ) -> Result<Self::DataCompletionBytes> {
            self.request
                .lock()
                .unwrap()
                .replace((method.to_string(), path.to_string(), body));
            let response = self.response.lock().unwrap().take().unwrap_or_default();
            Ok(EmptyCompletion::with_data(response))
        }

        fn add_io_callback(&self, _callback: Box<dyn FnMut() -> bool + Send>) {}

        fn step_io_callbacks(&self) {}
    }

    /// Serves canned HTTP responses in order; panics if the engine makes more
    /// requests than responses were queued. Records every request for
    /// assertions on the wire traffic.
    struct QueuedSyncEngineIo {
        responses: Mutex<std::collections::VecDeque<Vec<u8>>>,
        #[allow(clippy::type_complexity)]
        requests: Mutex<Vec<(String, String, Option<Vec<u8>>)>>,
    }

    impl SyncEngineIo for QueuedSyncEngineIo {
        type DataCompletionBytes = EmptyCompletion<u8>;
        type DataCompletionTransform = EmptyCompletion<crate::types::DatabaseRowTransformResult>;

        fn full_read(&self, path: &str) -> Result<Self::DataCompletionBytes> {
            let data = std::fs::read(path).unwrap_or_default();
            Ok(EmptyCompletion::with_data(data))
        }

        fn full_write(&self, path: &str, content: Vec<u8>) -> Result<Self::DataCompletionBytes> {
            std::fs::write(path, content).map_err(|error| {
                crate::errors::Error::DatabaseSyncEngineError(format!(
                    "test full_write failed for {path}: {error}"
                ))
            })?;
            Ok(EmptyCompletion::empty())
        }

        fn transform(
            &self,
            _mutations: Vec<crate::types::DatabaseRowMutation>,
        ) -> Result<Self::DataCompletionTransform> {
            Ok(EmptyCompletion::empty())
        }

        fn http(
            &self,
            _url: Option<&str>,
            method: &str,
            path: &str,
            body: Option<Vec<u8>>,
            _headers: &[(&str, &str)],
        ) -> Result<Self::DataCompletionBytes> {
            self.requests
                .lock()
                .unwrap()
                .push((method.to_string(), path.to_string(), body));
            let response = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("engine made more HTTP requests than queued responses");
            Ok(EmptyCompletion::with_data(response))
        }

        fn add_io_callback(&self, _callback: Box<dyn FnMut() -> bool + Send>) {}

        fn step_io_callbacks(&self) {}
    }

    fn record(values: &[turso_core::Value]) -> Bytes {
        Bytes::from(
            turso_core::types::ImmutableRecord::from_values(values, values.len())
                .unwrap()
                .into_payload(),
        )
    }

    fn encoded_logical_txns(txns: &[LogicalTxnData]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for txn in txns {
            bytes.extend_from_slice(&txn.encode_length_delimited_to_vec());
        }
        bytes
    }

    fn default_test_opts() -> DatabaseSyncEngineOpts {
        DatabaseSyncEngineOpts {
            remote_url: None,
            client_name: "test-client".to_string(),
            tables_ignore: vec![],
            use_transform: false,
            wal_pull_batch_size: 0,
            long_poll_timeout: Some(Duration::from_millis(1)),
            protocol_version_hint: DatabaseSyncEngineProtocolVersion::V1,
            bootstrap_if_empty: false,
            reserved_bytes: 0,
            db_opts: turso_core::DatabaseOpts::default(),
            partial_sync_opts: None::<PartialSyncOpts>,
            remote_encryption_key: None,
            push_operations_threshold: None,
            pull_bytes_threshold: None,
            logical_mvcc_pull: Some(true),
        }
    }

    fn replace_base_guard_test_paths(
        main_path: &str,
    ) -> Vec<(&'static str, String, Option<&'static [u8]>)> {
        vec![
            ("main-db", main_path.to_string(), Some(b"main-old")),
            (
                "main-wal",
                create_main_db_wal_path(main_path),
                Some(b"wal-old"),
            ),
            (
                "main-log",
                create_main_db_log_path(main_path),
                Some(b"log-old"),
            ),
            ("revert-wal", create_revert_db_wal_path(main_path), None),
            ("metadata", create_meta_path(main_path), Some(b"meta-old")),
        ]
    }

    fn write_replace_base_guard_test_files(main_path: &str) {
        for (_, path, content) in replace_base_guard_test_paths(main_path) {
            if let Some(content) = content {
                std::fs::write(path, content).unwrap();
            }
        }
    }

    fn assert_replace_base_backups_removed(main_path: &str) {
        assert!(std::fs::read(create_replace_base_marker_path(main_path)).is_err());
        for (name, _, _) in replace_base_guard_test_paths(main_path) {
            assert!(std::fs::read(replace_base_backup_path(main_path, name)).is_err());
        }
    }

    async fn write_replace_base_pages_file<Ctx>(
        coro: &Coro<Ctx>,
        io: &Arc<dyn turso_core::IO>,
        source_db_path: &str,
        changes_path: &str,
    ) -> Result<Arc<dyn turso_core::File>> {
        let pages = std::fs::read(source_db_path).unwrap();
        assert_eq!(pages.len() % super::PAGE_SIZE, 0);
        let db_size = (pages.len() / super::PAGE_SIZE) as u32;
        let changes_file = io.open_file(changes_path, turso_core::OpenFlags::Create, false)?;

        let truncate = changes_file.truncate(0, turso_core::Completion::new_trunc(|_| {}))?;
        while !truncate.succeeded() {
            coro.yield_(SyncEngineIoResult::IO).await?;
        }

        for (page_idx, page) in pages.chunks_exact(super::PAGE_SIZE).enumerate() {
            let mut frame = vec![0; super::WAL_FRAME_SIZE];
            frame[super::WAL_FRAME_HEADER..].copy_from_slice(page);
            let frame_info = turso_core::types::WalFrameInfo {
                page_no: page_idx as u32 + 1,
                db_size: if page_idx + 1 == db_size as usize {
                    db_size
                } else {
                    0
                },
            };
            frame_info.put_to_frame_header(&mut frame);
            let offset = (page_idx * super::WAL_FRAME_SIZE) as u64;
            let len = frame.len();
            let write = changes_file.pwrite(
                offset,
                Arc::new(turso_core::Buffer::new(frame)),
                turso_core::Completion::new_write(move |result| {
                    let Ok(size) = result else {
                        return;
                    };
                    assert_eq!(size as usize, len);
                }),
            )?;
            while !write.succeeded() {
                coro.yield_(SyncEngineIoResult::IO).await?;
            }
        }

        let sync = changes_file.sync(
            turso_core::Completion::new_sync(|_| {}),
            turso_core::io::FileSyncType::Fsync,
        )?;
        while !sync.succeeded() {
            coro.yield_(SyncEngineIoResult::IO).await?;
        }
        Ok(changes_file)
    }

    #[test]
    fn replace_base_guard_restores_original_files_and_removes_created_files() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let main_path = temp_dir
            .path()
            .join("guard-restore.db")
            .to_string_lossy()
            .to_string();
        write_replace_base_guard_test_files(&main_path);

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let sync_io = Arc::new(CapturingSyncEngineIo {
            response: Mutex::new(None),
            request: Mutex::new(None),
        });
        let sync_stats = SyncEngineIoStats::new(sync_io);
        let old_revision = DatabasePullRevision::V1 {
            revision: "old-revision".to_string(),
        };

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let main_path = main_path.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let guard = ReplaceBaseApplyGuard::create(
                    &coro,
                    io.clone(),
                    sync_stats,
                    &main_path,
                    Some(old_revision),
                )
                .await?;

                std::fs::write(&main_path, b"main-new").unwrap();
                std::fs::write(create_main_db_wal_path(&main_path), b"wal-new").unwrap();
                std::fs::write(create_main_db_log_path(&main_path), b"log-new").unwrap();
                std::fs::write(create_revert_db_wal_path(&main_path), b"revert-created").unwrap();
                std::fs::write(create_meta_path(&main_path), b"meta-new").unwrap();

                guard.restore(&coro).await?;
                Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }

        assert_eq!(std::fs::read(&main_path).unwrap(), b"main-old");
        assert_eq!(
            std::fs::read(create_main_db_wal_path(&main_path)).unwrap(),
            b"wal-old"
        );
        assert_eq!(
            std::fs::read(create_main_db_log_path(&main_path)).unwrap(),
            b"log-old"
        );
        assert!(std::fs::read(create_revert_db_wal_path(&main_path)).is_err());
        assert_eq!(
            std::fs::read(create_meta_path(&main_path)).unwrap(),
            b"meta-old"
        );
        assert_replace_base_backups_removed(&main_path);
    }

    #[test]
    fn replace_base_guard_recovers_pending_marker() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let main_path = temp_dir
            .path()
            .join("guard-recover.db")
            .to_string_lossy()
            .to_string();
        write_replace_base_guard_test_files(&main_path);

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let sync_io = Arc::new(CapturingSyncEngineIo {
            response: Mutex::new(None),
            request: Mutex::new(None),
        });
        let sync_stats = SyncEngineIoStats::new(sync_io);

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let main_path = main_path.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let _guard = ReplaceBaseApplyGuard::create(
                    &coro,
                    io.clone(),
                    sync_stats.clone(),
                    &main_path,
                    None,
                )
                .await?;

                std::fs::write(&main_path, b"main-new").unwrap();
                std::fs::write(create_meta_path(&main_path), b"meta-new").unwrap();

                let recovered = ReplaceBaseApplyGuard::recover_pending(
                    &coro,
                    io.clone(),
                    sync_stats,
                    &main_path,
                )
                .await?;
                assert!(recovered);
                Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }

        assert_eq!(std::fs::read(&main_path).unwrap(), b"main-old");
        assert_eq!(
            std::fs::read(create_meta_path(&main_path)).unwrap(),
            b"meta-old"
        );
        assert_replace_base_backups_removed(&main_path);
    }

    #[test]
    fn replace_base_guard_mark_complete_removes_marker_without_restoring() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let main_path = temp_dir
            .path()
            .join("guard-complete.db")
            .to_string_lossy()
            .to_string();
        write_replace_base_guard_test_files(&main_path);

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let sync_io = Arc::new(CapturingSyncEngineIo {
            response: Mutex::new(None),
            request: Mutex::new(None),
        });
        let sync_stats = SyncEngineIoStats::new(sync_io);

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let main_path = main_path.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let mut guard =
                    ReplaceBaseApplyGuard::create(&coro, io.clone(), sync_stats, &main_path, None)
                        .await?;
                std::fs::write(&main_path, b"main-new").unwrap();
                guard.mark_complete(&coro).await?;
                Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }

        assert_eq!(std::fs::read(&main_path).unwrap(), b"main-new");
        assert_replace_base_backups_removed(&main_path);
    }

    #[test]
    fn initial_logical_mvcc_pull_page_bootstrap_uses_replace_base_apply() {
        let temp_file = NamedTempFile::new().unwrap();
        let main_path = temp_file.path().to_str().unwrap().to_string();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let main_db =
            turso_core::Database::open_file(io.clone(), &main_path, Arc::new(SqliteDialect))
                .unwrap();

        let meta = DatabaseMetadata {
            version: DATABASE_METADATA_VERSION.to_string(),
            client_unique_id: "initial-client".to_string(),
            synced_revision: None,
            revert_since_wal_salt: None,
            revert_since_wal_watermark: 0,
            last_pull_unix_time: None,
            last_push_unix_time: None,
            last_pushed_pull_gen_hint: 0,
            last_pushed_change_id_hint: 0,
            last_pushed_replay_floor_change_id_hint: 0,
            partial_bootstrap_server_revision: None,
            fresh_bootstrap_pending_cdc_ack: false,
            remote_pull_protocol: RemotePullProtocol::MvccLogical,
            logical_table_names_by_stable_id: Default::default(),
            saved_configuration: Some(DatabaseSavedConfiguration {
                remote_url: Some("https://example.com".to_string()),
                partial_sync_prefetch: None,
                partial_sync_segment_size: None,
            }),
        };
        std::fs::write(create_meta_path(&main_path), meta.dump().unwrap()).unwrap();

        let header = PullUpdatesRespProtoBody {
            protocol: 0,
            server_revision: "g1:o10".to_string(),
            db_size: 0,
            raw_encoding: None,
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Pages as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
            mvcc_log: None,
        };
        let sync_io = Arc::new(CapturingSyncEngineIo {
            response: Mutex::new(Some(header.encode_length_delimited_to_vec())),
            request: Mutex::new(None),
        });
        let sync_stats = SyncEngineIoStats::new(sync_io.clone());
        let mut opts = default_test_opts();
        opts.remote_url = Some("https://example.com".to_string());
        opts.logical_mvcc_pull = Some(true);

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let main_db = main_db.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let engine =
                    DatabaseSyncEngine::open_db(&coro, io, sync_stats, main_db, opts).await?;
                let status = engine.wait_changes_from_remote(&coro).await?;
                assert!(status.file_slot.is_none());
                assert!(matches!(
                    status.stream_kind,
                    DbChangesStreamKind::ReplaceBasePages
                ));
                Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }

        let (_method, path, body) = sync_io.request.lock().unwrap().clone().unwrap();
        assert_eq!(path, "/pull-updates");
        let request = PullUpdatesReqProtoBody::decode(body.unwrap().as_slice()).unwrap();
        assert_eq!(request.stream_kind, PullUpdatesStreamKind::Pages as i32);
        assert_eq!(request.client_revision, "");
        assert_eq!(request.server_revision, "");
    }

    #[test]
    fn apply_changes_from_remote_applies_logical_stream_without_local_replay() {
        let db_temp = NamedTempFile::new().unwrap();
        let meta_temp = NamedTempFile::new().unwrap();
        let changes_temp = NamedTempFile::new().unwrap();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

        let txns = vec![
            LogicalTxnData {
                end_offset: 1,
                commit_ts: 1,
                origin_client_id: "remote".to_string(),
                ops: vec![LogicalOp {
                    op_type: LogicalOpType::Schema as i32,
                    table_name: String::new(),
                    rowid: 0,
                    record: Bytes::new(),
                    sql: "CREATE TABLE items(x TEXT)".to_string(),
                    user_version: None,
                    application_id: None,
                    schema_action: Some(LogicalSchemaAction::Create as i32),
                    schema_kind: Some(LogicalSchemaKind::Table as i32),
                    schema_name: "items".to_string(),
                    stable_table_id: 7,
                }],
            },
            LogicalTxnData {
                end_offset: 2,
                commit_ts: 2,
                origin_client_id: "remote".to_string(),
                ops: vec![LogicalOp {
                    op_type: LogicalOpType::UpsertRow as i32,
                    table_name: String::new(),
                    rowid: 2,
                    record: record(&[turso_core::Value::Text(turso_core::types::Text::new(
                        "remote".to_string(),
                    ))]),
                    sql: String::new(),
                    user_version: None,
                    application_id: None,
                    schema_action: None,
                    schema_kind: None,
                    schema_name: String::new(),
                    stable_table_id: 7,
                }],
            },
        ];
        std::fs::write(changes_temp.path(), encoded_logical_txns(&txns)).unwrap();

        let db = turso_core::Database::open_file(
            io.clone(),
            db_temp.path().to_str().unwrap(),
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let db_file = db.db_file.clone();
        let db_io = db.io.clone();
        let main_tape = DatabaseTape::new_with_opts(
            db,
            DatabaseTapeOpts {
                cdc_table: None,
                cdc_mode: Some("full".to_string()),
                disable_auto_checkpoint: true,
            },
        );
        let sync_io = Arc::new(NoopSyncEngineIo);
        let sync_engine_io = SyncEngineIoStats::new(sync_io);
        let changes_file = io
            .open_file(
                changes_temp.path().to_str().unwrap(),
                turso_core::OpenFlags::None,
                false,
            )
            .unwrap();
        let slot = Arc::new(Mutex::new(None));
        let meta = DatabaseMetadata {
            version: DATABASE_METADATA_VERSION.to_string(),
            client_unique_id: "client-a".to_string(),
            synced_revision: Some(DatabasePullRevision::V1 {
                revision: "g1:o1".to_string(),
            }),
            revert_since_wal_salt: None,
            revert_since_wal_watermark: 0,
            last_pull_unix_time: None,
            last_push_unix_time: None,
            last_pushed_pull_gen_hint: 0,
            last_pushed_change_id_hint: 0,
            last_pushed_replay_floor_change_id_hint: 0,
            partial_bootstrap_server_revision: None,
            fresh_bootstrap_pending_cdc_ack: false,
            remote_pull_protocol: RemotePullProtocol::MvccLogical,
            logical_table_names_by_stable_id: Default::default(),
            saved_configuration: Some(DatabaseSavedConfiguration {
                remote_url: None,
                partial_sync_prefetch: None,
                partial_sync_segment_size: None,
            }),
        };
        let engine = DatabaseSyncEngine {
            io: db_io,
            sync_engine_io,
            db_file,
            main_tape,
            main_db_path: db_temp.path().to_str().unwrap().to_string(),
            main_db_wal_path: super::create_main_db_wal_path(db_temp.path().to_str().unwrap()),
            revert_db_wal_path: super::create_revert_db_wal_path(db_temp.path().to_str().unwrap()),
            meta_path: meta_temp.path().to_str().unwrap().to_string(),
            changes_file: Arc::new(Mutex::new(None)),
            opts: default_test_opts(),
            meta: Mutex::new(meta),
            client_unique_id: "client-a".to_string(),
        };
        let remote_changes = DbChangesStatus {
            time: io.current_time_wall_clock(),
            revision: DatabasePullRevision::V1 {
                revision: "g1:o2".to_string(),
            },
            file_slot: Some(crate::database_sync_operations::MutexSlot {
                value: changes_file,
                slot,
            }),
            stream_kind: DbChangesStreamKind::Logical,
        };

        let mut gen = genawaiter::sync::Gen::new({
            let engine = engine;
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                engine
                    .apply_changes_from_remote(&coro, remote_changes)
                    .await
                    .unwrap();
                let conn = engine.main_tape.connect(&coro).await.unwrap();
                let mut stmt = conn.prepare("SELECT rowid, x FROM items").unwrap();
                let mut rows = Vec::new();
                while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                    rows.push(row.get_values().cloned().collect::<Vec<_>>());
                }
                let (pull_gen, change_id) =
                    read_last_change_id(&coro, &conn, &engine.client_unique_id)
                        .await
                        .unwrap();
                let pending_local_changes =
                    count_local_changes(&coro, &conn, &engine.opts, change_id.unwrap())
                        .await
                        .unwrap();
                let meta = engine.meta.lock().unwrap().clone();
                (rows, meta, pull_gen, change_id, pending_local_changes)
            }
        });
        let (rows, meta, pull_gen, change_id, pending_local_changes) = loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => break result,
            }
        };

        assert_eq!(
            rows,
            vec![vec![
                turso_core::Value::from_i64(2),
                turso_core::Value::Text(turso_core::types::Text::new("remote".to_string())),
            ]]
        );
        assert_eq!(
            meta.synced_revision,
            Some(DatabasePullRevision::V1 {
                revision: "g1:o2".to_string(),
            })
        );
        assert_eq!(
            meta.logical_table_names_by_stable_id.get(&7).unwrap(),
            "items"
        );
        assert_eq!(meta.revert_since_wal_watermark, 0);
        assert_eq!(pull_gen, 0);
        assert!(change_id.is_some());
        assert_eq!(pending_local_changes, 0);
    }

    #[test]
    fn apply_changes_from_remote_replays_pending_local_changes() {
        let db_temp = NamedTempFile::new().unwrap();
        let meta_temp = NamedTempFile::new().unwrap();
        let changes_temp = NamedTempFile::new().unwrap();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

        let txns = vec![LogicalTxnData {
            end_offset: 1,
            commit_ts: 1,
            origin_client_id: "remote".to_string(),
            ops: vec![
                LogicalOp {
                    op_type: LogicalOpType::Schema as i32,
                    table_name: String::new(),
                    rowid: 0,
                    record: Bytes::new(),
                    sql: "CREATE TABLE remote_items(id INTEGER PRIMARY KEY, x TEXT)".to_string(),
                    user_version: None,
                    application_id: None,
                    schema_action: Some(LogicalSchemaAction::Create as i32),
                    schema_kind: Some(LogicalSchemaKind::Table as i32),
                    schema_name: "remote_items".to_string(),
                    stable_table_id: 9,
                },
                LogicalOp {
                    op_type: LogicalOpType::UpsertRow as i32,
                    table_name: String::new(),
                    rowid: 2,
                    record: record(&[
                        turso_core::Value::from_i64(2),
                        turso_core::Value::Text(turso_core::types::Text::new("remote".to_string())),
                    ]),
                    sql: String::new(),
                    user_version: None,
                    application_id: None,
                    schema_action: None,
                    schema_kind: None,
                    schema_name: String::new(),
                    stable_table_id: 9,
                },
            ],
        }];
        std::fs::write(changes_temp.path(), encoded_logical_txns(&txns)).unwrap();

        let db = turso_core::Database::open_file(
            io.clone(),
            db_temp.path().to_str().unwrap(),
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let db_file = db.db_file.clone();
        let db_io = db.io.clone();
        let main_tape = DatabaseTape::new_with_opts(
            db,
            DatabaseTapeOpts {
                cdc_table: None,
                cdc_mode: Some("full".to_string()),
                disable_auto_checkpoint: true,
            },
        );
        let sync_engine_io = SyncEngineIoStats::new(Arc::new(NoopSyncEngineIo));
        let changes_file = io
            .open_file(
                changes_temp.path().to_str().unwrap(),
                turso_core::OpenFlags::None,
                false,
            )
            .unwrap();
        let old_revision = DatabasePullRevision::V1 {
            revision: "g1:o1".to_string(),
        };
        let meta = DatabaseMetadata {
            version: DATABASE_METADATA_VERSION.to_string(),
            client_unique_id: "client-a".to_string(),
            synced_revision: Some(old_revision.clone()),
            revert_since_wal_salt: None,
            revert_since_wal_watermark: 0,
            last_pull_unix_time: None,
            last_push_unix_time: None,
            last_pushed_pull_gen_hint: 0,
            last_pushed_change_id_hint: 0,
            last_pushed_replay_floor_change_id_hint: 0,
            partial_bootstrap_server_revision: None,
            fresh_bootstrap_pending_cdc_ack: false,
            remote_pull_protocol: RemotePullProtocol::MvccLogical,
            logical_table_names_by_stable_id: Default::default(),
            saved_configuration: Some(DatabaseSavedConfiguration {
                remote_url: None,
                partial_sync_prefetch: None,
                partial_sync_segment_size: None,
            }),
        };
        let engine = DatabaseSyncEngine {
            io: db_io,
            sync_engine_io,
            db_file,
            main_tape,
            main_db_path: db_temp.path().to_str().unwrap().to_string(),
            main_db_wal_path: super::create_main_db_wal_path(db_temp.path().to_str().unwrap()),
            revert_db_wal_path: super::create_revert_db_wal_path(db_temp.path().to_str().unwrap()),
            meta_path: meta_temp.path().to_str().unwrap().to_string(),
            changes_file: Arc::new(Mutex::new(None)),
            opts: default_test_opts(),
            meta: Mutex::new(meta),
            client_unique_id: "client-a".to_string(),
        };
        let remote_changes = DbChangesStatus {
            time: io.current_time_wall_clock(),
            revision: DatabasePullRevision::V1 {
                revision: "g1:o2".to_string(),
            },
            file_slot: Some(crate::database_sync_operations::MutexSlot {
                value: changes_file,
                slot: Arc::new(Mutex::new(None)),
            }),
            stream_kind: DbChangesStreamKind::Logical,
        };

        let mut gen = genawaiter::sync::Gen::new({
            let engine = engine;
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let conn = engine.main_tape.connect(&coro).await.unwrap();
                conn.execute("CREATE TABLE local_items(id INTEGER PRIMARY KEY, x TEXT)")
                    .unwrap();
                conn.execute("INSERT INTO local_items(id, x) VALUES (1, 'local')")
                    .unwrap();

                engine
                    .apply_changes_from_remote(&coro, remote_changes)
                    .await
                    .unwrap();

                let mut stmt = conn
                    .prepare("SELECT id, x FROM remote_items ORDER BY id")
                    .unwrap();
                let remote_row = run_stmt_once(&coro, &mut stmt)
                    .await
                    .unwrap()
                    .unwrap()
                    .get_values()
                    .cloned()
                    .collect::<Vec<_>>();
                assert!(run_stmt_once(&coro, &mut stmt).await.unwrap().is_none());

                let mut stmt = conn
                    .prepare("SELECT id, x FROM local_items ORDER BY id")
                    .unwrap();
                let local_row = run_stmt_once(&coro, &mut stmt)
                    .await
                    .unwrap()
                    .unwrap()
                    .get_values()
                    .cloned()
                    .collect::<Vec<_>>();
                assert!(run_stmt_once(&coro, &mut stmt).await.unwrap().is_none());

                let (_, synced_change_id) =
                    read_last_change_id(&coro, &conn, &engine.client_unique_id)
                        .await
                        .unwrap();
                let pending_local_changes =
                    count_local_changes(&coro, &conn, &engine.opts, synced_change_id.unwrap())
                        .await
                        .unwrap();
                let meta = engine.meta.lock().unwrap().clone();
                (remote_row, local_row, pending_local_changes, meta)
            }
        });
        let (remote_row, local_row, pending_local_changes, meta) = loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => break result,
            }
        };
        assert_eq!(
            remote_row,
            vec![
                turso_core::Value::from_i64(2),
                turso_core::Value::Text(turso_core::types::Text::new("remote".to_string())),
            ]
        );
        assert_eq!(
            local_row,
            vec![
                turso_core::Value::from_i64(1),
                turso_core::Value::Text(turso_core::types::Text::new("local".to_string())),
            ]
        );
        assert!(
            pending_local_changes > 0,
            "recaptured local CDC should remain pending for push"
        );
        assert_eq!(
            meta.synced_revision,
            Some(DatabasePullRevision::V1 {
                revision: "g1:o2".to_string(),
            })
        );
    }

    #[test]
    fn failed_replace_base_local_replay_does_not_advance_synced_revision() {
        let main_file = NamedTempFile::new().unwrap();
        let remote_file = NamedTempFile::new().unwrap();
        let changes_file = NamedTempFile::new().unwrap();
        let main_path = main_file.path().to_str().unwrap().to_string();
        let remote_path = remote_file.path().to_str().unwrap().to_string();
        let changes_path = changes_file.path().to_str().unwrap().to_string();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let remote_db =
            turso_core::Database::open_file(io.clone(), &remote_path, Arc::new(SqliteDialect))
                .unwrap();
        let remote_conn = remote_db.connect().unwrap();
        remote_conn
            .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        remote_conn
            .execute("INSERT INTO items VALUES (1, 'duplicate'), (2, 'duplicate')")
            .unwrap();
        remote_conn
            .checkpoint(turso_core::CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            })
            .unwrap();

        let sync_engine_io = SyncEngineIoStats::new(Arc::new(NoopSyncEngineIo));
        let old_revision = DatabasePullRevision::V1 {
            revision: "old-revision".to_string(),
        };
        let new_revision = DatabasePullRevision::V1 {
            revision: "new-revision".to_string(),
        };

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let sync_engine_io = sync_engine_io.clone();
            let main_path = main_path.clone();
            let remote_path = remote_path.clone();
            let changes_path = changes_path.clone();
            let old_revision = old_revision.clone();
            let new_revision = new_revision.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let engine = DatabaseSyncEngine::create_db(
                    &coro,
                    io.clone(),
                    sync_engine_io.clone(),
                    &main_path,
                    default_test_opts(),
                )
                .await
                .map_err(|error| {
                    Error::DatabaseSyncEngineError(format!("test create_db failed: {error}"))
                })?;
                engine
                    .update_meta(&coro, |meta| {
                        meta.synced_revision = Some(old_revision.clone());
                        meta.remote_pull_protocol = RemotePullProtocol::Pages;
                    })
                    .await
                    .map_err(|error| {
                        Error::DatabaseSyncEngineError(format!("test update_meta failed: {error}"))
                    })?;

                let conn = engine.connect_rw(&coro).await.map_err(|error| {
                    Error::DatabaseSyncEngineError(format!("test connect_rw failed: {error}"))
                })?;
                conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")?;
                conn.execute("INSERT INTO items VALUES (1, 'local-only')")?;
                let base_change_id = max_local_change_id(&coro, &conn).await?.unwrap_or(0);
                update_last_change_id(&coro, &conn, &engine.client_unique_id, 1, base_change_id)
                    .await?;
                conn.execute("CREATE UNIQUE INDEX items_value_unique ON items(value)")?;

                let changes_file =
                    write_replace_base_pages_file(&coro, &io, &remote_path, &changes_path).await?;
                let slot = Arc::new(Mutex::new(Some(changes_file)));
                let file_slot = MutexSlot {
                    value: slot.lock().unwrap().take().unwrap(),
                    slot: slot.clone(),
                };

                REPLACE_BASE_LOCAL_REPLAY_FAILURE_AFTER.with(|value| value.set(0));
                let result = engine
                    .apply_changes_from_remote(
                        &coro,
                        DbChangesStatus {
                            time: turso_core::WallClockInstant {
                                secs: 10,
                                micros: 0,
                            },
                            revision: new_revision.clone(),
                            file_slot: Some(file_slot),
                            stream_kind: DbChangesStreamKind::ReplaceBasePages,
                        },
                    )
                    .await;
                REPLACE_BASE_LOCAL_REPLAY_FAILURE_AFTER.with(|value| value.set(usize::MAX));

                let err = result.unwrap_err();
                assert!(
                    format!("{err:#}").contains("injected replace-base local replay failure"),
                    "{err:#}"
                );
                assert_eq!(engine.meta().synced_revision, Some(old_revision.clone()));

                let on_disk_meta = DatabaseSyncEngine::<NoopSyncEngineIo>::read_db_meta(
                    &coro,
                    Some(io.clone()),
                    sync_engine_io.clone(),
                    &main_path,
                )
                .await
                .map_err(|error| {
                    Error::DatabaseSyncEngineError(format!("test read_db_meta failed: {error}"))
                })?
                .unwrap();
                assert_eq!(on_disk_meta.synced_revision, Some(old_revision));
                assert!(io
                    .try_open(&create_replace_base_marker_path(&main_path))?
                    .is_none());

                drop(conn);
                drop(engine);
                let verify_db = turso_core::Database::open_file(
                    io.clone(),
                    &main_path,
                    Arc::new(SqliteDialect),
                )
                .map_err(|error| {
                    Error::DatabaseSyncEngineError(format!("test verify open failed: {error}"))
                })?;
                let verify_conn = verify_db.connect().map_err(|error| {
                    Error::DatabaseSyncEngineError(format!("test verify connect failed: {error}"))
                })?;
                let mut value_stmt = verify_conn
                    .prepare("SELECT value FROM items WHERE id = 1")
                    .unwrap();
                let row = run_stmt_once(&coro, &mut value_stmt).await?.unwrap();
                assert_eq!(
                    row.get_values().cloned().collect::<Vec<_>>(),
                    vec![turso_core::Value::Text(turso_core::types::Text::new(
                        "local-only"
                    ))]
                );
                assert!(run_stmt_once(&coro, &mut value_stmt).await?.is_none());

                let mut index_stmt = verify_conn
                    .prepare("SELECT sql FROM sqlite_schema WHERE name = 'items_value_unique'")
                    .unwrap();
                let row = run_stmt_once(&coro, &mut index_stmt).await?.unwrap();
                assert!(row
                    .get_value(0)
                    .to_text()
                    .unwrap()
                    .contains("CREATE UNIQUE INDEX items_value_unique"));
                assert!(run_stmt_once(&coro, &mut index_stmt).await?.is_none());
                Result::Ok(())
            }
        });

        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    REPLACE_BASE_LOCAL_REPLAY_FAILURE_AFTER.with(|value| value.set(usize::MAX));
                    result.unwrap();
                    break;
                }
            }
        }
    }

    /// Full-fidelity page-stream response: header (with the given protocol
    /// hint) followed by one PageData message per page of `db_bytes`.
    fn encoded_page_stream_response(
        db_bytes: &[u8],
        server_revision: &str,
        protocol: PullUpdatesProtocol,
    ) -> Vec<u8> {
        assert_eq!(db_bytes.len() % super::PAGE_SIZE, 0);
        let header = PullUpdatesRespProtoBody {
            server_revision: server_revision.to_string(),
            db_size: (db_bytes.len() / super::PAGE_SIZE) as u64,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Pages as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
            mvcc_log: None,
            protocol: protocol as i32,
        };
        let mut bytes = header.encode_length_delimited_to_vec();
        for (page_idx, page) in db_bytes.chunks_exact(super::PAGE_SIZE).enumerate() {
            let page_data = PageData {
                page_id: page_idx as u64,
                encoded_page: Bytes::copy_from_slice(page),
            };
            bytes.extend_from_slice(&page_data.encode_length_delimited_to_vec());
        }
        bytes
    }

    /// Deferred-bootstrap replica ("converted to cloud sync later"): a local
    /// WAL-mode database with local writes meets an MVCC-protocol remote on
    /// first contact. The engine must detect the protocol, convert the local
    /// database to MVCC journal mode, apply the remote base as replace-base,
    /// replay the local changes on top, and stay MVCC across a reopen.
    #[test]
    fn deferred_first_contact_converts_wal_replica_to_mvcc_and_applies_base() {
        let main_file = NamedTempFile::new().unwrap();
        let remote_file = NamedTempFile::new().unwrap();
        let main_path = main_file.path().to_str().unwrap().to_string();
        let remote_path = remote_file.path().to_str().unwrap().to_string();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let remote_db =
            turso_core::Database::open_file(io.clone(), &remote_path, Arc::new(SqliteDialect))
                .unwrap();
        let remote_conn = remote_db.connect().unwrap();
        remote_conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        assert!(remote_conn.mvcc_enabled());
        remote_conn
            .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        remote_conn
            .execute("INSERT INTO items VALUES (1, 'remote-a'), (2, 'remote-b')")
            .unwrap();
        let remote_wal_state = remote_conn.wal_state().unwrap();
        remote_conn
            .checkpoint(turso_core::CheckpointMode::Truncate {
                upper_bound_inclusive: Some(remote_wal_state.max_frame),
            })
            .unwrap();
        drop(remote_conn);
        drop(remote_db);
        let remote_bytes = std::fs::read(&remote_path).unwrap();
        assert!(!remote_bytes.is_empty());

        let server_revision = "g1:o0";
        let first_contact_response = encoded_page_stream_response(
            &remote_bytes,
            server_revision,
            PullUpdatesProtocol::MvccLogical,
        );
        // The replace-base apply issues one follow-up logical pull from the
        // new revision; serve it an empty logical stream.
        let followup_logical_response = PullUpdatesRespProtoBody {
            server_revision: server_revision.to_string(),
            db_size: (remote_bytes.len() / super::PAGE_SIZE) as u64,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
            mvcc_log: None,
            protocol: PullUpdatesProtocol::MvccLogical as i32,
        }
        .encode_length_delimited_to_vec();
        let sync_io = Arc::new(QueuedSyncEngineIo {
            responses: Mutex::new(
                vec![first_contact_response, followup_logical_response]
                    .into_iter()
                    .collect(),
            ),
            requests: Mutex::new(Vec::new()),
        });
        let sync_engine_io = SyncEngineIoStats::new(sync_io);

        let mut opts = default_test_opts();
        opts.remote_url = Some("https://example.com".to_string());
        opts.bootstrap_if_empty = false;
        opts.logical_mvcc_pull = None; // auto-detect

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let main_path = main_path.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let engine = DatabaseSyncEngine::create_db(
                    &coro,
                    io.clone(),
                    sync_engine_io.clone(),
                    &main_path,
                    opts,
                )
                .await
                .map_err(|error| {
                    Error::DatabaseSyncEngineError(format!("test create_db failed: {error}"))
                })?;
                assert_eq!(
                    engine.meta().remote_pull_protocol,
                    RemotePullProtocol::Unknown
                );

                // Local writes before ever contacting the server.
                let conn = engine.connect_rw(&coro).await.map_err(|error| {
                    Error::DatabaseSyncEngineError(format!("test connect_rw failed: {error}"))
                })?;
                assert!(!conn.mvcc_enabled());
                conn.execute("CREATE TABLE local_notes(id INTEGER PRIMARY KEY, note TEXT)")?;
                conn.execute("INSERT INTO local_notes VALUES (1, 'kept-across-conversion')")?;
                let base_change_id = max_local_change_id(&coro, &conn).await?.unwrap_or(0);
                update_last_change_id(&coro, &conn, &engine.client_unique_id, 1, base_change_id)
                    .await?;
                drop(conn);

                // First contact: detection, conversion, replace-base pull.
                let status = engine.wait_changes_from_remote(&coro).await?;
                assert!(matches!(
                    status.stream_kind,
                    DbChangesStreamKind::ReplaceBasePages
                ));
                assert!(status.file_slot.is_some());
                assert_eq!(
                    engine.meta().remote_pull_protocol,
                    RemotePullProtocol::MvccLogical
                );
                assert!(engine.meta().logical_mvcc_pull_active());

                engine.apply_changes_from_remote(&coro, status).await?;
                assert_eq!(
                    engine.meta().synced_revision,
                    Some(DatabasePullRevision::V1 {
                        revision: server_revision.to_string(),
                    })
                );

                // Remote base and replayed local data both present, on an
                // MVCC-mode connection.
                let conn = engine.connect_rw(&coro).await.map_err(|error| {
                    Error::DatabaseSyncEngineError(format!("test reconnect failed: {error}"))
                })?;
                assert!(conn.mvcc_enabled());
                let mut stmt = conn.prepare("SELECT value FROM items ORDER BY id")?;
                let mut values = Vec::new();
                while let Some(row) = run_stmt_once(&coro, &mut stmt).await? {
                    values.push(row.get_value(0).to_text().unwrap().to_string());
                }
                assert_eq!(values, vec!["remote-a".to_string(), "remote-b".to_string()]);
                let mut stmt = conn.prepare("SELECT note FROM local_notes")?;
                let row = run_stmt_once(&coro, &mut stmt).await?.unwrap();
                assert_eq!(
                    row.get_value(0).to_text().unwrap(),
                    "kept-across-conversion"
                );
                assert!(run_stmt_once(&coro, &mut stmt).await?.is_none());
                drop(stmt);
                drop(conn);
                drop(engine);

                // The conversion must survive a reopen: header version and
                // logical log agree, so a fresh open comes up in MVCC mode
                // with the same data.
                let verify_db = turso_core::Database::open_file(
                    io.clone(),
                    &main_path,
                    Arc::new(SqliteDialect),
                )
                .map_err(|error| {
                    Error::DatabaseSyncEngineError(format!("test verify open failed: {error}"))
                })?;
                let verify_conn = verify_db.connect().map_err(|error| {
                    Error::DatabaseSyncEngineError(format!("test verify connect failed: {error}"))
                })?;
                assert!(verify_conn.mvcc_enabled());
                let mut stmt = verify_conn.prepare("SELECT COUNT(*) FROM items")?;
                let row = run_stmt_once(&coro, &mut stmt).await?.unwrap();
                assert_eq!(row.get_value(0).as_int(), Some(2));
                Result::Ok(())
            }
        });

        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }
    }

    /// A fresh bootstrap of an MVCC database serves the last durable
    /// generation base only. `create_db` must follow it with exactly one
    /// non-long-polling incremental logical pull so `connect()` hands out a
    /// current database instead of one missing every commit since the last
    /// natural checkpoint.
    #[test]
    fn fresh_mvcc_bootstrap_catches_up_with_one_logical_pull() {
        let main_file = NamedTempFile::new().unwrap();
        let remote_file = NamedTempFile::new().unwrap();
        let main_path = main_file.path().to_str().unwrap().to_string();
        let remote_path = remote_file.path().to_str().unwrap().to_string();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let remote_db =
            turso_core::Database::open_file(io.clone(), &remote_path, Arc::new(SqliteDialect))
                .unwrap();
        let remote_conn = remote_db.connect().unwrap();
        remote_conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        remote_conn
            .execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")
            .unwrap();
        let remote_wal_state = remote_conn.wal_state().unwrap();
        remote_conn
            .checkpoint(turso_core::CheckpointMode::Truncate {
                upper_bound_inclusive: Some(remote_wal_state.max_frame),
            })
            .unwrap();
        drop(remote_conn);
        drop(remote_db);
        let remote_bytes = std::fs::read(&remote_path).unwrap();

        let server_revision = "g1:o64";
        let bootstrap_response = encoded_page_stream_response(
            &remote_bytes,
            server_revision,
            PullUpdatesProtocol::MvccLogical,
        );
        // The catch-up pull gets an empty logical stream: already current.
        let catch_up_response = PullUpdatesRespProtoBody {
            server_revision: server_revision.to_string(),
            db_size: (remote_bytes.len() / super::PAGE_SIZE) as u64,
            raw_encoding: Some(PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
            mvcc_log: None,
            protocol: PullUpdatesProtocol::MvccLogical as i32,
        }
        .encode_length_delimited_to_vec();
        let sync_io = Arc::new(QueuedSyncEngineIo {
            responses: Mutex::new(
                vec![bootstrap_response, catch_up_response]
                    .into_iter()
                    .collect(),
            ),
            requests: Mutex::new(Vec::new()),
        });
        let sync_engine_io = SyncEngineIoStats::new(sync_io.clone());

        let mut opts = default_test_opts();
        opts.remote_url = Some("https://example.com".to_string());
        opts.bootstrap_if_empty = true;
        opts.logical_mvcc_pull = None; // auto-detect

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let main_path = main_path.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let engine = DatabaseSyncEngine::create_db(
                    &coro,
                    io.clone(),
                    sync_engine_io.clone(),
                    &main_path,
                    opts,
                )
                .await
                .map_err(|error| {
                    Error::DatabaseSyncEngineError(format!("test create_db failed: {error}"))
                })?;
                assert_eq!(
                    engine.meta().remote_pull_protocol,
                    RemotePullProtocol::MvccLogical
                );
                assert_eq!(
                    engine.meta().synced_revision,
                    Some(DatabasePullRevision::V1 {
                        revision: server_revision.to_string(),
                    })
                );
                Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }

        // Both queued responses were consumed: the bootstrap plus exactly one
        // catch-up pull, and the catch-up was a non-long-polling logical pull
        // from the bootstrap revision.
        assert!(sync_io.responses.lock().unwrap().is_empty());
        let requests = sync_io.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let catch_up =
            PullUpdatesReqProtoBody::decode(requests[1].2.as_ref().unwrap().as_slice()).unwrap();
        assert_eq!(
            catch_up.stream_kind,
            PullUpdatesStreamKind::MvccLogicalLog as i32
        );
        assert_eq!(catch_up.client_revision, server_revision);
        assert_eq!(catch_up.long_poll_timeout_ms, 0);
    }

    /// Forcing `logical_mvcc_pull: Some(true)` on a replica whose revision
    /// came from the legacy wire protocol is a misconfiguration: the server
    /// cannot resume an MVCC logical stream from a legacy revision.
    #[test]
    fn forced_logical_pull_on_legacy_revision_replica_is_rejected() {
        let temp_file = NamedTempFile::new().unwrap();
        let main_path = temp_file.path().to_str().unwrap().to_string();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let main_db =
            turso_core::Database::open_file(io.clone(), &main_path, Arc::new(SqliteDialect))
                .unwrap();

        let meta = DatabaseMetadata {
            version: DATABASE_METADATA_VERSION.to_string(),
            client_unique_id: "legacy-client".to_string(),
            synced_revision: Some(DatabasePullRevision::Legacy {
                generation: 3,
                synced_frame_no: Some(17),
            }),
            revert_since_wal_salt: None,
            revert_since_wal_watermark: 0,
            last_pull_unix_time: None,
            last_push_unix_time: None,
            last_pushed_pull_gen_hint: 0,
            last_pushed_change_id_hint: 0,
            last_pushed_replay_floor_change_id_hint: 0,
            partial_bootstrap_server_revision: None,
            fresh_bootstrap_pending_cdc_ack: false,
            remote_pull_protocol: RemotePullProtocol::Pages,
            logical_table_names_by_stable_id: Default::default(),
            saved_configuration: Some(DatabaseSavedConfiguration {
                remote_url: Some("https://example.com".to_string()),
                partial_sync_prefetch: None,
                partial_sync_segment_size: None,
            }),
        };
        std::fs::write(create_meta_path(&main_path), meta.dump().unwrap()).unwrap();

        let sync_stats = SyncEngineIoStats::new(Arc::new(NoopSyncEngineIo));
        let mut opts = default_test_opts();
        opts.remote_url = Some("https://example.com".to_string());
        opts.logical_mvcc_pull = Some(true);

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let main_db = main_db.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let engine =
                    DatabaseSyncEngine::open_db(&coro, io, sync_stats, main_db, opts).await?;
                let err = engine.wait_changes_from_remote(&coro).await.unwrap_err();
                assert!(format!("{err:#}").contains("legacy protocol"), "{err:#}");
                Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }
    }

    /// A database file that lived outside the sync engine has rows without
    /// CDC provenance; replace-base replay would silently drop them, so the
    /// MVCC conversion must refuse instead.
    #[test]
    fn first_contact_mvcc_conversion_rejects_local_data_without_cdc_history() {
        let main_file = NamedTempFile::new().unwrap();
        let main_path = main_file.path().to_str().unwrap().to_string();

        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let foreign_db =
            turso_core::Database::open_file(io.clone(), &main_path, Arc::new(SqliteDialect))
                .unwrap();
        let foreign_conn = foreign_db.connect().unwrap();
        foreign_conn
            .execute("CREATE TABLE orphaned(id INTEGER PRIMARY KEY)")
            .unwrap();
        foreign_conn
            .execute("INSERT INTO orphaned VALUES (1)")
            .unwrap();
        drop(foreign_conn);
        drop(foreign_db);

        let sync_engine_io = SyncEngineIoStats::new(Arc::new(NoopSyncEngineIo));
        let mut opts = default_test_opts();
        opts.remote_url = Some("https://example.com".to_string());
        opts.bootstrap_if_empty = false;
        opts.logical_mvcc_pull = None;

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let main_path = main_path.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let engine = DatabaseSyncEngine::create_db(
                    &coro,
                    io.clone(),
                    sync_engine_io.clone(),
                    &main_path,
                    opts,
                )
                .await
                .map_err(|error| {
                    Error::DatabaseSyncEngineError(format!("test create_db failed: {error}"))
                })?;
                let err = engine
                    .ensure_local_mvcc_journal_mode(&coro)
                    .await
                    .unwrap_err();
                assert!(
                    format!("{err:#}").contains("without CDC history"),
                    "{err:#}"
                );
                Result::Ok(())
            }
        });

        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }
    }
}
