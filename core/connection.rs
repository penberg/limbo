use crate::alloc::TryClone;
use crate::error::io_error;
#[cfg(any(test, injected_yields))]
use crate::mvcc::yield_points::{FailureInjector, YieldInjector};
use crate::statement::StatementOrigin;
use crate::storage::{journal_mode, pager::SavepointResult};
use crate::sync::{
    atomic::{
        AtomicBool, AtomicI32, AtomicI64, AtomicIsize, AtomicU16, AtomicU64, AtomicU8, Ordering,
    },
    Arc, Mutex, RwLock,
};
use crate::types::IOResultOr;
#[cfg(all(feature = "fs", feature = "conn_raw_api"))]
use crate::types::{WalFrameInfo, WalState};
#[cfg(feature = "fs")]
use crate::util::{OpenMode, OpenOptions};
#[cfg(all(feature = "fs", feature = "conn_raw_api"))]
use crate::Page;
use crate::{
    ast, function,
    io::{MemoryIO, IO},
    progress::{ProgressHandler, ProgressHandlerCallback},
    translate,
    translate::collate::CollationSeq,
    util::IOExt,
    vdbe, AllViewsTxState, AtomicCipherMode, AtomicSyncMode, AtomicTempStore, BusyHandler,
    BusyHandlerCallback, CaptureDataChangesInfo, CheckpointMode, CheckpointResult, CipherMode, Cmd,
    Completion, ConnectionMetrics, Database, DatabaseCatalog, DatabaseOpts, Duration,
    EncryptionKey, EncryptionOpts, IOResult, IndexMethod, LimboError, MvStore, OpenFlags, PageSize,
    Pager, Program, QueryMode, QueryRunner, Result, Schema, Statement, SyncMode, TransactionMode,
    Trigger, Value, VirtualTable, WalAutoActions,
};
use crate::{is_memory_like, turso_assert};
use crate::{MAIN_DB_ID, TEMP_DB_ID};
use arc_swap::ArcSwap;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use smallvec::SmallVec;
use std::cmp::Ordering as CmpOrdering;
use std::fmt::Display;
use std::ops::Deref;
#[cfg(feature = "simulator")]
use std::path::Path;
#[cfg(not(target_family = "wasm"))]
use tempfile::TempDir;
use tracing::{instrument, Level};
use turso_macros::{turso_assert_ne, AtomicEnum};

#[cfg(feature = "simulator")]
fn db_identity_for_testing(db_path: &Path) -> Result<(u32, u32)> {
    let bytes =
        std::fs::read(db_path).map_err(|e| io_error(e, "read db header for simulator testing"))?;
    let db_header_size = crate::storage::sqlite3_ondisk::DatabaseHeader::SIZE;
    if bytes.len() < db_header_size {
        return Err(LimboError::InternalError(format!(
            "database file is smaller than the header: got {}, need at least {}",
            bytes.len(),
            db_header_size
        )));
    }
    let db_size_pages = u32::from_be_bytes(bytes[28..32].try_into().unwrap());
    let crc = crc32c::crc32c(&bytes[..db_header_size]);
    Ok((db_size_pages, crc))
}

#[derive(Clone, AtomicEnum, Copy, PartialEq, Eq, Debug)]
pub(crate) enum TransactionState {
    Write {
        schema_did_change: bool,
    },
    Read,
    /// PendingUpgrade remembers what transaction state was before upgrade to write (has_read_txn is true if before transaction were in Read state)
    /// This is important, because if we failed to initialize write transaction immediatley - we need to end implicitly started read txn (e.g. for simiple INSERT INTO operation)
    /// But for late upgrade of transaction we should keep read transaction active (e.g. BEGIN; SELECT ...; INSERT INTO ...)
    PendingUpgrade {
        has_read_txn: bool,
    },
    None,
}

pub(crate) struct TempDatabase {
    pub(crate) db: Arc<Database>,
    pub(crate) pager: Arc<Pager>,
    #[cfg(not(target_family = "wasm"))]
    _temp_dir: Option<TempDir>,
}

/// All of the connection-local state needed to manage the `TEMP` schema.
///
/// Grouped so that anything that touches the temp database (the pager,
/// the last committed schema snapshot, the dirty-schema flag) lives in
/// one place. The individual fields keep their own locks because their
/// access patterns differ: `database` is read on every temp-qualified
/// lookup while `committed_schema` is only touched at commit/rollback
/// boundaries, and `schema_did_change` is flipped from inside `SetCookie`.
pub(crate) struct TempDbContext {
    /// Per-connection temp database (`TEMP_DB_ID`/schema `temp`).
    /// Lazily initialized on first temp DDL.
    pub(crate) database: RwLock<Option<TempDatabase>>,
    /// Last committed snapshot of `database.read().as_ref().unwrap().db.schema`.
    /// Updated on successful commit and consulted on full-txn rollback
    /// to restore the in-memory temp schema (there is no shared
    /// `Database::schema` for temp the way main has). `None` until the
    /// first temp DDL is committed; a rollback with `None` resets the
    /// temp schema to empty.
    pub(crate) committed_schema: RwLock<Option<Arc<Schema>>>,
    /// Set by `SetCookie` when a temp DDL runs; read by commit/rollback
    /// to decide whether to snapshot/restore. Cleared on transaction
    /// end. Mirrors the `schema_did_change` field inside
    /// `TransactionState::Write` for the main DB.
    pub(crate) schema_did_change: AtomicBool,
}

impl TempDbContext {
    pub(crate) fn new() -> Self {
        Self {
            database: RwLock::new(None),
            committed_schema: RwLock::new(None),
            schema_did_change: AtomicBool::new(false),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NamedSavepointFrame {
    pub(crate) name: String,
    pub(crate) starts_transaction: bool,
    pub(crate) deferred_fk_violations: isize,
    /// Snapshot of `conn.schema` taken at SAVEPOINT begin. Used by
    /// ROLLBACK TO to restore the in-memory main schema without re-
    /// reading sqlite_schema from disk — disk reparse from inside a
    /// vdbe opcode would block on cursor I/O (and additionally, for
    /// sequences, on `prepare_internal + run_with_row_callback`),
    /// violating the vdbe async contract. Cheap to capture: bumps
    /// the `Arc<Schema>` refcount. DDL after SAVEPOINT goes through
    /// `Connection::with_schema_mut` which uses `Arc::make_mut`, so
    /// the snapshot keeps pointing to the pre-DDL schema even when
    /// the current schema diverges.
    pub(crate) main_schema_snapshot: Arc<Schema>,
    /// Snapshot of `temp_db.db.schema` taken at SAVEPOINT begin. `None`
    /// when the temp database had not been initialized yet. Used by
    /// ROLLBACK TO to restore the in-memory temp schema after the
    /// on-disk pages have been rolled back via the mirror call.
    pub(crate) temp_schema_snapshot: Option<Arc<Schema>>,
    /// Snapshot of the connection-local `database_schemas` map at
    /// SAVEPOINT begin. Cheap — values are `Arc`. Used by ROLLBACK TO
    /// to restore staged DDL on attached databases.
    pub(crate) staged_schema_snapshot: HashMap<usize, Arc<Schema>>,
}

/// Info returned by `rollback_named_savepoint_frame` so callers can
/// restore in-memory schema state after the pager has rolled back.
pub(crate) struct RollbackFrameInfo {
    pub(crate) main_schema_snapshot: Arc<Schema>,
    pub(crate) temp_schema_snapshot: Option<Arc<Schema>>,
    pub(crate) staged_schema_snapshot: HashMap<usize, Arc<Schema>>,
}

struct SchemaReparseGuard {
    connection: Arc<Connection>,
}

impl Drop for SchemaReparseGuard {
    fn drop(&mut self) {
        self.connection
            .schema_reparse_in_progress
            .store(false, Ordering::SeqCst);
    }
}

#[derive(Debug, Default)]
pub struct PrepareOptions {
    pub unqualified_database_search_path: Option<Vec<String>>,
}

/// Re-entrant state for [`Connection::reparse_schema_nonblock`] and the
/// VACUUM-only [`Connection::reparse_schema_with_cookie_keeping_sequences`].
/// `Start` is the fresh state (cookie not yet read / schema build not yet
/// begun); `Building` carries the half-built schema, captured table-valued
/// functions, the held reparse guard, and the current sub-phase across IO
/// yields.
#[derive(Default)]
pub enum ReparseSchemaState {
    #[default]
    Start,
    Building(Box<ReparseSchemaInner>),
}

pub struct ReparseSchemaInner {
    /// Held for the whole reparse so a concurrent reparse on this connection
    /// trips the recursion assert. Dropped when the schema is finalized.
    _guard: SchemaReparseGuard,
    fresh: Schema,
    /// Built-in table-valued functions captured from the old schema; rehydrated
    /// after the sqlite_schema scan since they don't survive re-parsing.
    tvfs: Vec<Arc<crate::vtab::VirtualTable>>,
    /// VACUUM-supplied sequence descriptors to graft onto the rebuilt schema
    /// instead of re-reading each backing table. `None` for a normal reparse,
    /// which recovers descriptors from disk in the `PopulateSequences` phase.
    preserved_sequences: Option<rustc_hash::FxHashMap<String, Arc<crate::schema::Sequence>>>,
    phase: ReparsePhase,
}

/// Test-only control over correlated-subquery rewrites.
#[cfg(feature = "simulator")]
#[derive(Debug, AtomicEnum, Clone, Copy, PartialEq, Eq)]
pub enum SubqueryUnnestingMode {
    /// Let the cost model choose between the correlated and rewritten plans.
    Auto = 0,
    /// Keep the original correlated plan.
    Disabled = 1,
    /// Use the rewritten plan whenever the rewrite applies.
    Forced = 2,
}

enum ReparsePhase {
    /// Scanning `SELECT * FROM sqlite_schema` into `fresh`.
    ParseSchema {
        parse: Box<crate::util::ParseSchemaRowsState>,
    },
    /// Recovering sequence descriptors from each `__turso_internal_seq_*`
    /// backing table via SQL (or grafting the VACUUM-preserved map). The
    /// per-backing-table descriptor read yields IO, so the worklist and the
    /// in-flight statement are carried across re-entry here.
    PopulateSequences {
        /// `(backing_table_name, seq_name)` worklist; `None` until lazily
        /// computed. Left empty when preserved sequences are grafted.
        pending: Option<crate::alloc::Vec<(String, String)>>,
        /// Index of the backing table currently being read.
        idx: usize,
        /// In-flight descriptor `SELECT`, created lazily per backing table.
        stmt: Option<Box<Statement>>,
        /// Descriptor row `(start, inc, min, max, cycle)` captured from `stmt`.
        meta: Option<(i64, i64, i64, i64, bool)>,
        /// Sequence reconstructed from `meta`, retained while the watermark
        /// query yields IO.
        seq: Option<crate::schema::Sequence>,
        /// In-flight watermark `SELECT`, created after `seq` is known.
        watermark_stmt: Option<Box<Statement>>,
        /// Watermark row `(value, is_called)` captured from `watermark_stmt`.
        watermark_row: Option<(i64, bool)>,
    },
    /// Loading custom type definitions from the internal types table.
    LoadTypes {
        stmt: Box<Statement>,
        type_rows: Vec<String>,
    },
    /// Best-effort ANALYZE-stats refresh before finalizing.
    RefreshStats {
        stats: crate::stats::RefreshAnalyzeStatsState,
    },
}

#[cfg(not(feature = "fs"))]
#[derive(Default)]
pub(crate) enum AttachDatabaseState {
    #[default]
    Start,
}

#[cfg(feature = "fs")]
#[derive(Default)]
pub(crate) enum AttachDatabaseState {
    #[default]
    Start,
    Init(Box<AttachDatabaseInitState>),
    Bootstrap(Box<AttachDatabaseBootstrapState>),
    Publish {
        alias: String,
        db: Arc<Database>,
        pager: Arc<Pager>,
    },
    Done,
}

#[cfg(feature = "fs")]
pub(crate) struct AttachDatabaseInitState {
    alias: String,
    reserved_space: Option<u8>,
    db: Arc<Database>,
    attached_is_fresh: bool,
    encryption_key: Option<EncryptionKey>,
    init_st: crate::InitState,
}

#[cfg(feature = "fs")]
pub(crate) struct AttachDatabaseBootstrapState {
    alias: String,
    db: Arc<Database>,
    pager: Arc<Pager>,
    encryption_key: Option<EncryptionKey>,
    bootstrap_conn: Option<Arc<Connection>>,
    bootstrap_st: crate::mvcc::database::BootstrapState,
}

/// Re-entrant driver state for
/// [`Connection::load_sequence_descriptors_via_sql_nonblock`]. Walks every
/// `__turso_internal_seq_*` backing table and registers its descriptor,
/// carrying the worklist and the in-flight descriptor read across IO yields.
#[derive(Default)]
pub enum LoadSequenceDescriptorsState {
    #[default]
    Start,
    Reading {
        /// `(backing_table_name, seq_name)` worklist captured from the schema.
        pending: crate::alloc::Vec<(String, String)>,
        /// Index of the backing table currently being read.
        idx: usize,
        /// In-flight descriptor `SELECT`, created lazily per backing table.
        stmt: Option<Box<Statement>>,
        /// Descriptor row `(start, inc, min, max, cycle)` captured from `stmt`.
        meta: Option<(i64, i64, i64, i64, bool)>,
        /// Sequence reconstructed from `meta`, retained while the watermark
        /// query yields IO.
        seq: Option<crate::schema::Sequence>,
        /// In-flight watermark `SELECT`, created after `seq` is known.
        watermark_stmt: Option<Box<Statement>>,
        /// Watermark row `(value, is_called)` captured from `watermark_stmt`.
        watermark_row: Option<(i64, bool)>,
    },
}

/// Re-entrant driver state for
/// [`Connection::sync_autoincrement_backing_tables_from_sqlite_sequence_nonblock`].
#[derive(Default)]
pub enum SyncAutoincrementState {
    #[default]
    Start,
    /// Reading all `(name, seq)` rows from `sqlite_sequence`.
    ReadSeqRows {
        stmt: Box<Statement>,
        rows: Vec<(String, i64)>,
    },
    /// Per-table: read the backing `MAX(value)` then maybe upsert a watermark.
    Process {
        rows: Vec<(String, i64)>,
        idx: usize,
        sub: SyncRowStep,
    },
}

/// Per-row sub-state of [`SyncAutoincrementState::Process`].
#[derive(Default)]
pub enum SyncRowStep {
    /// Resolve `rows[idx]`'s backing table and start reading `MAX(value)`.
    #[default]
    Start,
    /// Reading `MAX(value)` from the backing table.
    ReadMax {
        backing_table_name: String,
        stmt: Box<Statement>,
        current_max: Option<i64>,
    },
    /// Running the `INSERT OR REPLACE` watermark upsert.
    Upsert { stmt: Box<Statement> },
}

#[derive(Default)]
pub(crate) struct StatementActivity {
    explicit_checkpoint_active: bool,
}

pub(crate) struct ExplicitCheckpointGuard {
    activity: Arc<Mutex<StatementActivity>>,
    pager: Arc<Pager>,
}

impl Drop for ExplicitCheckpointGuard {
    fn drop(&mut self) {
        if self.pager.is_checkpointing() {
            self.pager.cleanup_after_checkpoint_failure();
        }
        let mut activity = self.activity.lock();
        turso_assert!(
            activity.explicit_checkpoint_active,
            "dropping an inactive explicit checkpoint guard"
        );
        activity.explicit_checkpoint_active = false;
    }
}

/// Database connection handle.
///
/// If you add a setting that affects SQL compilation or execution, call
/// `bump_prepare_context_generation()` in its setter so cached prepared
/// statements know they need to be reprepared.
pub struct Connection {
    pub(crate) db: Arc<Database>,
    pub(crate) pager: ArcSwap<Pager>,
    pub(crate) schema: RwLock<Arc<Schema>>,
    /// Per-database schema cache (database_index -> schema)
    /// Loaded lazily to avoid copying all schemas on connection open
    pub(super) database_schemas: RwLock<HashMap<usize, Arc<Schema>>>,
    /// Whether to automatically commit transaction
    pub(crate) auto_commit: AtomicBool,
    pub(super) transaction_state: AtomicTransactionState,
    /// True when an unfinished write statement inside an explicit transaction
    /// was reset or dropped and there was no statement savepoint to undo only
    /// that statement. COMMIT must roll back the whole transaction.
    pub(crate) poisoned_tx: AtomicBool,
    pub(super) last_insert_rowid: AtomicI64,
    pub(crate) changes: AtomicI64,
    pub(crate) total_changes: AtomicI64,
    pub(crate) syms: parking_lot::RwLock<SymbolTable>,
    pub(super) _shared_cache: bool,
    pub(super) cache_size: AtomicI32,
    /// page size used for an uninitialized database or the next vacuum command.
    /// it's not always equal to the current page size of the database
    pub(super) page_size: AtomicU16,
    /// Allowed automatic WAL maintenance actions for this connection.
    /// Stored as the `bits()` of a `WalAutoActions`. Default is
    /// `WalAutoActions::all_enabled()`. `wal_auto_actions_disable` clears
    /// every bit, opting out of both auto-checkpoint and WAL header
    /// restart — sync-engine consumers rely on the latter staying disabled
    /// because rotating the WAL header invalidates their published
    /// watermarks.
    pub(super) wal_auto_actions: AtomicU8,
    /// Whether MVCC commits should include portable logical-change metadata in
    /// the logical log.
    ///
    /// This is off by default because the metadata is only useful for raw-log
    /// consumers such as sync clients.
    #[cfg(feature = "conn_raw_api")]
    pub(super) portable_logical_changes_enabled: AtomicBool,
    #[cfg(feature = "conn_raw_api")]
    pub(super) mvcc_log_metadata: RwLock<HashMap<String, String>>,
    pub(super) capture_data_changes: RwLock<Option<CaptureDataChangesInfo>>,
    /// CDC v2: transaction ID for grouping CDC records by transaction.
    /// -1 means unset (will be assigned on first CDC write in the transaction).
    pub(crate) cdc_transaction_id: AtomicI64,
    pub(super) closed: AtomicBool,
    /// Per-connection state for the `TEMP` schema (pager, last-committed
    /// snapshot, dirty-schema flag). See `TempDbContext`.
    pub(crate) temp: TempDbContext,
    /// Attached databases
    pub(super) attached_databases: RwLock<DatabaseCatalog>,
    pub(super) query_only: AtomicBool,
    pub(super) vdbe_trace: AtomicBool,
    /// If enabled, the UPDATE/DELETE statements must have a WHERE clause
    pub(super) dml_require_where: AtomicBool,
    /// PRAGMA count_changes: when ON, each INSERT, UPDATE and DELETE returns
    /// one row with the number of rows it changed.
    pub(super) count_changes: AtomicBool,
    /// SQLite DQS misfeature: when ON (default), unresolved double-quoted identifiers
    /// in DML statements fall back to string literals instead of raising an error.
    pub(super) dqs_dml: AtomicBool,
    /// Deprecated pragma: when ON, column names include table prefix (TABLE.COLUMN)
    pub(super) full_column_names: AtomicBool,
    /// Deprecated pragma: when ON (default), column refs use just the column name
    pub(super) short_column_names: AtomicBool,
    /// Simulator-only planner control used by the differential fuzzer.
    #[cfg(feature = "simulator")]
    pub(super) subquery_unnesting_mode: AtomicSubqueryUnnestingMode,
    /// Per-connection runtime extension loading flag.
    pub(super) enable_load_extension: AtomicBool,
    /// Cumulative count of autonomous sequence inner-tx retries (each
    /// `WriteWriteConflict` / `BusySnapshot` / `Conflict` that
    /// `op_sequence_commit_inner_tx` absorbs via its retry budget bumps
    /// this). Lives on the connection rather than `ProgramState` because
    /// autocommit nextval/setval allocates a fresh `ProgramState` per
    /// statement — the per-state counter resets on every Step and can't
    /// witness across-statement contention. Tests use this counter to
    /// assert the hot path is conflict-free: any non-zero increment on
    /// a non-CYCLE nextval means inline backing-table compaction
    /// (or another contended write) was reintroduced.
    pub(crate) sequence_inner_retries: AtomicU64,
    pub(crate) mv_tx: RwLock<Option<(crate::mvcc::database::TxID, TransactionMode)>>,
    /// Per-attached-database MVCC transactions.
    /// Main DB uses `mv_tx` above for zero-cost hot path access.
    pub(crate) attached_mv_txs:
        RwLock<HashMap<usize, (crate::mvcc::database::TxID, TransactionMode)>>,
    #[cfg(any(test, injected_yields))]
    pub(super) yield_injector: RwLock<Option<Arc<dyn YieldInjector>>>,
    #[cfg(any(test, injected_yields))]
    pub(super) failure_injector: RwLock<Option<Arc<dyn FailureInjector>>>,
    #[cfg(any(test, injected_yields))]
    pub(super) yield_instance_id_counter: AtomicU64,

    /// Per-connection view transaction states for uncommitted changes. This represents
    /// one entry per view that was touched in the transaction.
    pub(crate) view_transaction_states: AllViewsTxState,
    /// Connection-level metrics aggregation
    pub metrics: RwLock<ConnectionMetrics>,
    /// Greater than zero if connection executes a program within a program
    /// This is necessary in order for connection to not "finalize" transaction (commit/abort) when program ends
    /// (because parent program is still pending and it will handle "finalization" instead)
    ///
    /// The state is integer as we may want to spawn deep nested programs (e.g. Root -[run]-> S1 -[run]-> S2 -[run]-> ...)
    /// and we need to track current nestedness depth in order to properly understand when we will reach the root back again
    pub(super) nestedness: AtomicI32,
    /// Stack of currently compiling triggers to prevent recursive trigger subprogram compilation
    pub(super) compiling_triggers: RwLock<Vec<Arc<Trigger>>>,
    /// Stack of currently executing triggers to prevent recursive trigger execution
    /// Only prevents the same trigger from firing again, allowing different triggers on the same table to fire
    pub(super) executing_triggers: RwLock<Vec<Arc<Trigger>>>,
    pub(crate) encryption_key: RwLock<Option<EncryptionKey>>,
    pub(super) encryption_cipher_mode: AtomicCipherMode,
    pub(super) sync_mode: AtomicSyncMode,
    pub(super) temp_store: AtomicTempStore,
    pub(super) data_sync_retry: AtomicBool,
    /// Busy handler for lock contention
    /// Default is BusyHandler::None (return SQLITE_BUSY immediately)
    pub(super) busy_handler: RwLock<BusyHandler>,
    /// Step-based progress callback for SQLite-compatible cancellation hooks.
    pub(super) progress_handler: ProgressHandler,
    /// Maximum execution time for a single statement on this connection.
    /// `Duration::ZERO` means disabled.
    pub(super) query_timeout_ms: AtomicU64,
    /// True when sqlite3_interrupt()-style cancellation is pending for active root statements.
    pub(super) interrupt_requested: AtomicBool,
    /// Whether this is an internal connection used for MVCC bootstrap
    pub(super) is_mvcc_bootstrap_connection: AtomicBool,
    /// Whether pragma foreign_keys=ON for this connection
    pub(super) fk_pragma: AtomicBool,
    pub(crate) fk_deferred_violations: AtomicIsize,
    /// Number of active top-level write statements on this connection.
    ///
    /// This is currently only 0 or 1. We return Busy instead of allowing a
    /// second same-connection writer to start.
    pub(crate) n_active_writes: AtomicI32,
    /// Number of active root statements currently executing on this connection.
    /// This is Turso's equivalent of SQLite's top-level active-VDBE count
    /// (`db->nVdbeActive`) for user statements, excluding internal helpers and
    /// subprogram execution.
    pub(crate) n_active_root_statements: AtomicI32,
    /// How many of `n_active_root_statements` are parked incremental blob
    /// handles. A blob handle keeps a Root statement open from `blob_open`
    /// until close, but between blob operations it just sits on its row, so
    /// explicit checkpoints subtract these instead of treating them as
    /// statements in progress (SQLite checkpoints fine with open blob
    /// handles; a handle re-positions on its next blob operation after the
    /// checkpoint's cache clear).
    pub(crate) n_active_blob_statements: AtomicI32,
    /// Prevents root statements and explicit checkpoints from overlapping on this connection.
    pub(crate) statement_activity: Arc<Mutex<StatementActivity>>,
    /// Whether pragma ignore_check_constraints=ON for this connection
    pub(super) check_constraints_pragma: AtomicBool,
    /// Track when each virtual table instance is currently in transaction.
    pub(crate) vtab_txn_states: RwLock<HashSet<u64>>,
    /// One prepared cursor per index-method attachment touched by the active
    /// database transaction. Statement reset transfers cursors here so the
    /// eventual transaction outcome is delivered exactly once per attachment.
    pub(crate) index_method_tx_cursors:
        Mutex<Vec<crate::index_method::TransactionIndexMethodCursor>>,
    /// True while `index_method_tx_cursors` may be non-empty. Lets every
    /// commit/rollback skip the mutex and sort when no index method was ever
    /// touched — the overwhelmingly common case.
    pub(crate) has_index_method_tx_cursors: crate::sync::atomic::AtomicBool,
    /// Connection-level named savepoint stack used to mirror savepoint state
    /// onto temp/attached databases that start participating after SAVEPOINT.
    pub(crate) named_savepoints: RwLock<Vec<NamedSavepointFrame>>,
    /// True while this connection is rebuilding its schema from sqlite_schema.
    /// Internal helper statements used during reload must not recursively
    /// trigger another schema reparse on the same connection.
    pub(crate) schema_reparse_in_progress: AtomicBool,
    /// Generation counter bumped whenever any setting that affects PrepareContext
    /// changes. Allows prepared statements to cheaply detect when they need to be
    /// reprepared (single u64 comparison instead of rebuilding the full context).
    /// IMPORTANT: this is a bit of a regression landmine because the generation
    /// MUST be incremented whenever any setting that affects PrepareContext changes,
    /// and this is not currently centralized; each setter bumps the generation individually.
    pub(crate) prepare_context_generation: AtomicU64,
    /// Per-connection last-returned value for each sequence (for currval()).
    pub(crate) sequence_currvals: RwLock<HashMap<String, i64>>,
}

// SAFETY: This needs to be audited for thread safety.
// See: https://github.com/tursodatabase/turso/issues/1552
crate::assert::assert_send_sync!(Connection);

impl Drop for Connection {
    fn drop(&mut self) {
        if !self.is_closed() {
            // A handle dropped mid-transaction rolls that transaction back
            // below, so parked index-method cursors must receive the same
            // rollback outcome they would get from an explicit close().
            self.index_methods_on_transaction_rolled_back();

            // Roll back any active MVCC transactions so that MvStore entries
            // don't leak and block future checkpoints.  The tx may have
            // already been committed/aborted externally (e.g. by tests that
            // manipulate MvStore directly), so only rollback if still active.
            if let Some(mv_store) = self.db.get_mv_store().as_ref() {
                if let Some(tx_id) = self.get_mv_tx_id() {
                    let pager = self.pager.load();
                    if mv_store.is_tx_rollbackable(tx_id) {
                        mv_store.rollback_tx(tx_id, pager.clone(), self, MAIN_DB_ID);
                    } else {
                        self.set_mv_tx(None);
                    }
                    pager.end_read_tx();
                }
            }
            self.rollback_attached_mvcc_txs(false);

            // Release any WAL locks the connection might be holding.
            // This prevents deadlocks if a connection is dropped (e.g., due to a panic)
            // while holding a read or write lock.
            let pager = self.pager.load();
            if let Some(wal) = &pager.wal {
                if wal.holds_write_lock() {
                    wal.end_write_tx();
                }
                if wal.holds_read_lock() {
                    wal.end_read_tx();
                }
            }

            // Also release WAL locks on all attached database pagers
            self.with_all_attached_pagers_with_index(|attached_pagers| {
                for (_, attached_pager) in attached_pagers {
                    if let Some(wal) = &attached_pager.wal {
                        if wal.holds_write_lock() {
                            wal.end_write_tx();
                        }
                        if wal.holds_read_lock() {
                            wal.end_read_tx();
                        }
                    }
                }
            });

            // if connection wasn't properly closed, decrement the connection counter
            self.db
                .n_connections
                .fetch_sub(1, crate::sync::atomic::Ordering::SeqCst);
        }
    }
}

impl Connection {
    fn schema_reparse_guard(self: &Arc<Connection>) -> SchemaReparseGuard {
        let was_reparsing = self.schema_reparse_in_progress.swap(true, Ordering::SeqCst);
        turso_assert!(
            !was_reparsing,
            "schema reparse must not recurse on the same connection"
        );
        SchemaReparseGuard {
            connection: self.clone(),
        }
    }

    pub(crate) fn schema_reparse_in_progress(&self) -> bool {
        self.schema_reparse_in_progress.load(Ordering::Acquire)
    }

    pub(crate) fn empty_temp_schema(&self) -> Arc<Schema> {
        // with_options only fails if built-in type SQL is malformed (programmer bug).
        let mut schema = Schema::with_options(
            self.db.experimental_custom_types_enabled(),
            self.db.dialect().as_ref(),
        )
        .expect("built-in type definitions are malformed");
        schema.generated_columns_enabled = self.db.experimental_generated_columns_enabled();
        Arc::new(schema)
    }

    fn make_temp_database_opts(&self) -> DatabaseOpts {
        DatabaseOpts::new()
            .with_views(self.db.experimental_views_enabled())
            .with_custom_types(self.db.experimental_custom_types_enabled())
            .with_index_method(self.db.experimental_index_method_enabled())
            .with_vacuum(self.db.experimental_vacuum_enabled())
            .with_generated_columns(self.db.experimental_generated_columns_enabled())
            .with_without_rowid(self.db.experimental_without_rowid_enabled())
    }

    fn effective_temp_store(&self) -> crate::TempStore {
        let temp_store = self.get_temp_store();
        #[cfg(feature = "fs")]
        {
            temp_store
        }
        #[cfg(not(feature = "fs"))]
        {
            let _ = temp_store;
            crate::TempStore::Memory
        }
    }

    #[cfg(feature = "fs")]
    fn create_temp_database(&self) -> Result<TempDatabase> {
        let temp_store = self.effective_temp_store();
        let db_opts = self.make_temp_database_opts();
        let page_size = self.get_page_size();

        if matches!(temp_store, crate::TempStore::Memory) {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = Database::open_file_with_flags(
                io,
                crate::util::MEMORY_PATH,
                OpenFlags::Create,
                db_opts,
                None,
                self.db.dialect(),
            )?;
            let pager = Arc::new(db._init(None, None)?);
            pager.set_initial_page_size(page_size)?;
            return Ok(TempDatabase {
                db,
                pager,
                #[cfg(not(target_family = "wasm"))]
                _temp_dir: None,
            });
        }

        #[cfg(not(target_family = "wasm"))]
        {
            let temp_dir = self.create_tempdir()?;
            let temp_path = temp_dir.path().join("tursodb-temp.db");
            let temp_path_str = temp_path.to_str().ok_or_else(|| {
                LimboError::InternalError("temp db path is not valid UTF-8".into())
            })?;
            // Always create a fresh IO for the temp file. Cloning the
            // main db's IO is wrong when the main db uses a mock /
            // simulated backend (e.g. the deterministic simulator
            // with `--io-backend=memory`) that can't access real
            // filesystem paths produced by `tempfile::tempdir()`.
            let io = Database::io_for_path(temp_path_str)?;
            let db = Database::open_file_with_flags(
                io,
                temp_path_str,
                OpenFlags::Create,
                db_opts,
                None,
                self.db.dialect(),
            )?;
            let pager = Arc::new(db._init(None, None)?);
            pager.set_initial_page_size(page_size)?;
            Ok(TempDatabase {
                db,
                pager,
                _temp_dir: Some(temp_dir),
            })
        }

        #[cfg(target_family = "wasm")]
        {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = Database::open_file_with_flags(
                io,
                crate::util::MEMORY_PATH,
                OpenFlags::Create,
                db_opts,
                None,
                self.db.dialect(),
            )?;
            let pager = Arc::new(db._init(None, None)?);
            pager.set_initial_page_size(page_size)?;
            Ok(TempDatabase { db, pager })
        }
    }

    #[cfg(not(feature = "fs"))]
    fn create_temp_database(&self) -> Result<TempDatabase> {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            crate::util::MEMORY_PATH,
            OpenFlags::Create,
            self.make_temp_database_opts(),
            None,
            self.db.dialect(),
        )?;
        let pager = Arc::new(db._init(None, None)?);
        pager.set_initial_page_size(self.get_page_size())?;
        Ok(TempDatabase {
            db,
            pager,
            #[cfg(not(target_family = "wasm"))]
            _temp_dir: None,
        })
    }

    pub(crate) fn ensure_temp_database(&self) -> Result<()> {
        if self.temp.database.read().is_some() {
            return Ok(());
        }

        let temp_db = self.create_temp_database()?;
        let mut guard = self.temp.database.write();
        if guard.is_none() {
            *guard = Some(temp_db);
        }
        Ok(())
    }

    /// Tear down the per-connection temp database.
    ///
    /// Drops the temp pager, clears the committed schema snapshot and
    /// the dirty-schema flag. Called by `set_temp_store` when the user
    /// changes `PRAGMA temp_store` outside of an explicit transaction.
    fn reset_temp_database(&self) {
        if let Some(temp_db) = self.temp.database.write().take() {
            temp_db.pager.rollback_attached();
        }
        *self.temp.committed_schema.write() = None;
        self.temp.schema_did_change.store(false, Ordering::Release);
    }

    /// Flag a temp-schema mutation within the current transaction so the
    /// commit/rollback path knows to snapshot or restore the in-memory
    /// temp schema. Called from `SetCookie` for `TEMP_DB_ID`.
    pub(crate) fn mark_temp_schema_did_change(&self) {
        // If we're marking the temp schema dirty, temp DDL must have
        // just run against the temp pager — which means the temp
        // database was initialized. The opposite state is unreachable.
        turso_assert!(
            self.temp.database.read().is_some(),
            "mark_temp_schema_did_change called without an initialized temp database"
        );
        self.temp.schema_did_change.store(true, Ordering::Release);
    }

    /// On successful commit, snapshot the current `temp_db.db.schema`
    /// into `committed_temp_schema` so a future full-txn rollback can
    /// restore it. No-op if no temp DDL ran in this transaction.
    pub(crate) fn commit_temp_schema(&self) {
        if !self.temp.schema_did_change.load(Ordering::Acquire) {
            return;
        }
        // `schema_did_change` is only ever set by
        // `mark_temp_schema_did_change`, which asserts temp is
        // initialized. If it's somehow clear here we have a logic
        // bug — no safe recovery, so fail loud.
        let guard = self.temp.database.read();
        turso_assert!(
            guard.is_some(),
            "commit_temp_schema: schema_did_change set but temp is uninitialized"
        );
        let snap = guard
            .as_ref()
            .expect("asserted above")
            .db
            .schema
            .lock()
            .clone();
        drop(guard);
        // save snapshot for potential future rollback.
        *self.temp.committed_schema.write() = Some(snap);
        self.temp.schema_did_change.store(false, Ordering::Release);
    }

    /// On full-txn rollback, restore `temp_db.db.schema` from the last
    /// committed snapshot. If nothing was ever committed, reset to an
    /// empty schema (matches the disk state the pager rolled back to).
    pub(crate) fn rollback_temp_schema(&self) {
        if !self.temp.schema_did_change.load(Ordering::Acquire) {
            return;
        }
        // Same invariant as `commit_temp_schema` — the flag can only
        // be set while temp is initialized.
        let committed = self.temp.committed_schema.read().clone();
        {
            let guard = self.temp.database.read();
            turso_assert!(
                guard.is_some(),
                "rollback_temp_schema: schema_did_change set but temp is uninitialized"
            );
            let temp_db = guard.as_ref().expect("asserted above");
            match committed {
                Some(snap) => *temp_db.db.schema.lock() = snap,
                None => *temp_db.db.schema.lock() = self.empty_temp_schema(),
            }
        }
        self.temp.schema_did_change.store(false, Ordering::Release);
        self.bump_prepare_context_generation();
    }

    /// Bump the prepare context generation counter. Must be called whenever any
    /// connection setting that is tracked in `PrepareContext` changes, so that
    /// prepared statements know they need to be reprepared.
    #[inline]
    pub(crate) fn bump_prepare_context_generation(&self) {
        self.prepare_context_generation
            .fetch_add(1, Ordering::Release);
    }

    #[inline]
    pub(crate) fn prepare_context_generation(&self) -> u64 {
        self.prepare_context_generation.load(Ordering::Acquire)
    }

    /// Choose how correlated subqueries are planned in newly prepared statements.
    #[cfg(feature = "simulator")]
    pub fn set_subquery_unnesting_mode(&self, mode: SubqueryUnnestingMode) {
        self.subquery_unnesting_mode.set(mode);
        self.bump_prepare_context_generation();
    }

    #[cfg(feature = "simulator")]
    pub(crate) fn subquery_unnesting_mode(&self) -> SubqueryUnnestingMode {
        self.subquery_unnesting_mode.get()
    }

    /// check if connection executes nested program (so it must not do any "finalization" work as parent program will handle it)
    pub fn is_nested_stmt(&self) -> bool {
        self.nestedness.load(Ordering::SeqCst) > 0
    }
    /// starts nested program execution
    pub fn start_nested(&self) {
        self.nestedness.fetch_add(1, Ordering::SeqCst);
    }
    /// ends nested program execution
    pub fn end_nested(&self) {
        self.nestedness.fetch_add(-1, Ordering::SeqCst);
    }

    /// Check if a specific trigger is currently compiling (for recursive trigger prevention)
    pub fn trigger_is_compiling(&self, trigger: &Arc<Trigger>) -> bool {
        let compiling = self.compiling_triggers.read();
        if let Some(trigger) = compiling.iter().find(|t| Arc::ptr_eq(t, trigger)) {
            tracing::debug!("Trigger is already compiling: {}", trigger.name);
            return true;
        }
        false
    }

    pub fn start_trigger_compilation(&self, trigger: Arc<Trigger>) {
        tracing::debug!("Starting trigger compilation: {}", trigger.name);
        self.compiling_triggers.write().push(trigger);
    }

    pub fn end_trigger_compilation(&self) {
        tracing::debug!(
            "Ending trigger compilation: {:?}",
            self.compiling_triggers.read().last().map(|t| &t.name)
        );
        self.compiling_triggers.write().pop();
    }

    /// Check if a specific trigger is currently executing (for recursive trigger prevention)
    pub fn is_trigger_executing(&self, trigger: &Arc<Trigger>) -> bool {
        let executing = self.executing_triggers.read();
        if let Some(active_trigger) = executing.iter().find(|t| Arc::ptr_eq(t, trigger)) {
            tracing::debug!("Trigger is already executing: {}", trigger.name);
            debug_assert!(Arc::ptr_eq(active_trigger, trigger));
            return true;
        }
        false
    }

    pub fn start_trigger_execution(&self, trigger: Arc<Trigger>) {
        tracing::debug!("Starting trigger execution: {}", trigger.name);
        self.executing_triggers.write().push(trigger);
    }

    pub fn end_trigger_execution(&self) {
        tracing::debug!(
            "Ending trigger execution: {:?}",
            self.executing_triggers.read().last().map(|t| &t.name)
        );
        self.executing_triggers.write().pop();
    }

    fn should_retry_cross_process_schema_lookup(
        self: &Arc<Connection>,
        err: &LimboError,
    ) -> Result<bool> {
        let LimboError::ParseError(msg) = err else {
            return Ok(false);
        };
        if !msg.contains("no such table") && !msg.contains("table not found") {
            return Ok(false);
        }
        if self.get_tx_state() != TransactionState::None {
            return Ok(false);
        }
        if self.db.shared_wal_coordination()?.is_none() {
            return Ok(false);
        }
        self.maybe_reparse_schema()?;
        Ok(true)
    }

    #[turso_macros::trace_stack]
    fn compile_cmd(
        self: &Arc<Connection>,
        cmd: Cmd,
        input: &str,
        origin: StatementOrigin,
        prepare_options: &PrepareOptions,
    ) -> Result<(Program, Arc<Pager>, QueryMode)> {
        self.maybe_update_schema();

        let syms = self.syms.read();
        let pager = self.pager.load().clone();
        let mode = QueryMode::new(&cmd);
        let (Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan { stmt, .. }) = cmd;
        let schema = self.schema.read().clone();
        match translate::translate(
            &schema,
            stmt,
            pager.clone(),
            self.clone(),
            &syms,
            mode,
            input,
            origin,
            prepare_options,
        ) {
            Ok(program) => Ok((program, pager, mode)),
            Err(err) if self.should_retry_cross_process_schema_lookup(&err)? => {
                // Cold path: re-parse the SQL from scratch after schema refresh rather
                // than cloning the original AST, which can overflow the stack
                // on deeply nested expression trees.
                drop(syms);
                let cmd = {
                    crate::stack::trace_stack!("schema_retry_parse");
                    let (cmd, _) = self.parse_sql(input)?;
                    let Some(cmd) = cmd else {
                        return Err(err);
                    };
                    cmd
                };
                self.maybe_update_schema();
                let syms = self.syms.read();
                let pager = self.pager.load().clone();
                let mode = QueryMode::new(&cmd);
                let (Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan { stmt, .. }) =
                    cmd;
                let schema = self.schema.read().clone();
                translate::translate(
                    &schema,
                    stmt,
                    pager.clone(),
                    self.clone(),
                    &syms,
                    mode,
                    input,
                    origin,
                    prepare_options,
                )
                .map(|program| (program, pager, mode))
            }
            Err(err) => Err(err),
        }
    }

    pub fn prepare(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Statement> {
        self._prepare(sql)
    }

    pub fn prepare_sqlite(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Statement> {
        self.prepare_with_origin(sql, StatementOrigin::Root)
    }

    #[doc(hidden)]
    pub fn prepare_internal(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Statement> {
        self.prepare_with_origin(sql, StatementOrigin::InternalHelper)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn _prepare(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Statement> {
        self.prepare_with_origin(sql, StatementOrigin::Root)
    }

    #[turso_macros::trace_stack]
    pub(crate) fn prepare_with_origin(
        self: &Arc<Connection>,
        sql: impl AsRef<str>,
        origin: StatementOrigin,
    ) -> Result<Statement> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        if sql.as_ref().is_empty() {
            return Err(LimboError::InvalidArgument(
                "The supplied SQL string contains no statements".to_string(),
            ));
        }

        let needs_nested_guard = origin.needs_nested_guard();
        if needs_nested_guard {
            self.start_nested();
        }
        let result = (|| {
            let sql = sql.as_ref();
            tracing::debug!("Preparing: {}", sql);

            let (cmd, byte_offset_end) = {
                crate::stack::trace_stack!("parse");
                self.parse_sql(sql)?
            };
            let cmd = match cmd {
                Some(cmd) => cmd,
                None => {
                    return Err(LimboError::InvalidArgument(
                        "The supplied SQL string contains no statements".to_string(),
                    ));
                }
            };
            let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
                .unwrap()
                .trim();
            let prepare_options = PrepareOptions::default();
            let (program, pager, mode) = self.compile_cmd(cmd, input, origin, &prepare_options)?;

            Ok(Statement::new_with_origin(
                program,
                pager,
                mode,
                byte_offset_end,
                origin,
                needs_nested_guard,
            ))
        })();
        if result.is_err() && needs_nested_guard {
            self.end_nested();
        }
        result
    }

    /// Prepare an already-translated statement while keeping the original
    /// SQL text.
    ///
    /// A frontend that already has an engine AST can call this instead of
    /// parsing through [`Dialect::parse`](crate::Dialect::parse), while the
    /// original text remains available for schema storage, diagnostics, and
    /// later re-preparation through the dialect.
    pub fn prepare_translated_stmt(
        self: &Arc<Connection>,
        stmt: ast::Stmt,
        input: &str,
    ) -> Result<Statement> {
        self.prepare_translated_cmd(ast::Cmd::Stmt(stmt), input)
    }

    pub fn prepare_translated_stmt_with_options(
        self: &Arc<Connection>,
        stmt: ast::Stmt,
        input: &str,
        prepare_options: &PrepareOptions,
    ) -> Result<Statement> {
        self.prepare_translated_cmd_with_options(ast::Cmd::Stmt(stmt), input, prepare_options)
    }

    /// Prepare an already-translated command while keeping the original SQL
    /// text.
    pub fn prepare_translated_cmd(
        self: &Arc<Connection>,
        cmd: ast::Cmd,
        input: &str,
    ) -> Result<Statement> {
        self.prepare_translated_cmd_with_options(cmd, input, &PrepareOptions::default())
    }

    pub fn prepare_translated_cmd_with_options(
        self: &Arc<Connection>,
        cmd: ast::Cmd,
        input: &str,
        prepare_options: &PrepareOptions,
    ) -> Result<Statement> {
        self.prepare_cmd_with_input_and_origin(cmd, input, StatementOrigin::Root, prepare_options)
    }

    #[turso_macros::trace_stack]
    fn prepare_cmd_with_input_and_origin(
        self: &Arc<Connection>,
        cmd: ast::Cmd,
        input: &str,
        origin: StatementOrigin,
        prepare_options: &PrepareOptions,
    ) -> Result<Statement> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let needs_nested_guard = origin.needs_nested_guard();
        if needs_nested_guard {
            self.start_nested();
        }
        let result = (|| {
            let (program, pager, mode) = self.compile_cmd(cmd, input, origin, prepare_options)?;
            Ok(Statement::new_with_origin(
                program,
                pager,
                mode,
                0,
                origin,
                needs_nested_guard,
            ))
        })();
        if result.is_err() && needs_nested_guard {
            self.end_nested();
        }
        result
    }

    /// Whether this is an internal connection used for MVCC bootstrap
    pub fn is_mvcc_bootstrap_connection(&self) -> bool {
        self.is_mvcc_bootstrap_connection.load(Ordering::SeqCst)
    }

    /// Promote MVCC bootstrap connection to a regular connection so it reads from the MV store again.
    pub fn promote_to_regular_connection(&self) {
        assert!(self.is_mvcc_bootstrap_connection.load(Ordering::SeqCst));
        self.is_mvcc_bootstrap_connection
            .store(false, Ordering::SeqCst);
    }

    /// Demote regular connection to MVCC bootstrap connection so it does not read from the MV store.
    pub fn demote_to_mvcc_connection(&self) {
        assert!(!self.is_mvcc_bootstrap_connection.load(Ordering::SeqCst));
        self.is_mvcc_bootstrap_connection
            .store(true, Ordering::SeqCst);
    }

    /// Parse schema from scratch if version of schema for the connection differs from the schema cookie in the root page.
    /// This function must be called outside of any transaction because internally it will start transaction session by itself.
    /// In multi-process mode, this is the only way to discover schema changes made by other processes.
    pub fn maybe_reparse_schema(self: &Arc<Connection>) -> Result<()> {
        let pager = self.pager.load().clone();
        let mv_store = self.mv_store();

        // maybe_reparse_schema must be called outside any explicit transaction
        // because it starts its own read transaction to load a fresh view of
        // sqlite_schema from disk.
        if self.get_tx_state() != TransactionState::None {
            return Ok(());
        }
        let had_main_mv_tx = self.get_mv_tx().is_some();

        if self.db.shared_wal_coordination()?.is_some() {
            // Cross-process schema changes can leave page 1 and sqlite_schema
            // pages cached from an earlier WAL snapshot. Drop the cache before
            // probing the cookie so reparsing observes the current committed view.
            pager.clear_page_cache(false);
            pager.set_schema_cookie(None);
        }

        let on_disk_schema_version = if mv_store.as_ref().is_some() {
            self.read_current_schema_cookie().or_else(|err| match err {
                LimboError::Page1NotAlloc => Ok(0),
                other => Err(other),
            })?
        } else {
            // first, quickly read schema_version from the root page in order to check if schema changed
            pager.begin_read_tx()?;
            let on_disk_schema_version = pager
                .io
                .block(|| pager.with_header(|header| header.schema_cookie));

            let on_disk_schema_version = match on_disk_schema_version {
                Ok(db_schema_version) => db_schema_version.get(),
                Err(LimboError::Page1NotAlloc) => {
                    // this means this is a fresh db, so return a schema version of 0
                    0
                }
                Err(err) => {
                    pager.end_read_tx();
                    return Err(err);
                }
            };
            pager.end_read_tx();
            on_disk_schema_version
        };

        let db_schema_version = self.db.schema.lock().schema_version;
        tracing::debug!(
            "path: {}, db_schema_version={} vs on_disk_schema_version={}",
            self.db.path,
            db_schema_version,
            on_disk_schema_version
        );
        // if schema_versions matches - exit early
        if db_schema_version == on_disk_schema_version {
            return Ok(());
        }

        // start read transaction manually, because we will read schema cookie once again and
        // we must be sure that it will consistent with schema content
        //
        // from now on we must be very careful with errors propagation
        // in order to not accidentally keep read transaction opened
        pager.begin_read_tx()?;
        self.set_tx_state(TransactionState::Read);

        let reparse_result = self.reparse_schema();

        let previous = self.transaction_state.swap(TransactionState::None);
        turso_assert!(
            matches!(previous, TransactionState::None | TransactionState::Read),
            "unexpected end transaction state"
        );
        // close opened transaction if it was kept open
        // (in most cases, it will be automatically closed if stmt was executed properly)
        if previous == TransactionState::Read {
            pager.end_read_tx();
        }
        if !had_main_mv_tx {
            self.clear_internal_main_mvcc_tx(&pager);
        }

        reparse_result?;

        let schema = self.schema.read().clone();
        self.db.update_schema_if_newer(schema);
        Ok(())
    }

    /// Parse schema from scratch even if the schema cookie did not change.
    ///
    /// Sync replace-base can install a page snapshot outside ordinary SQL DDL.
    /// The replacement may reuse the same schema cookie while changing root
    /// pages, so cookie-based refresh would keep stale btree metadata.
    #[cfg(feature = "conn_raw_api")]
    pub fn force_reparse_schema(self: &Arc<Connection>) -> Result<()> {
        self.force_reparse_schema_inner(true)
    }

    /// Like [`Self::force_reparse_schema`], but refreshes only this connection's
    /// own schema snapshot without publishing it to the shared database cache.
    ///
    /// Use this when the caller must further mutate the schema before it becomes
    /// visible to other connections.
    pub fn force_reparse_schema_without_publish(self: &Arc<Connection>) -> Result<()> {
        self.force_reparse_schema_inner(false)
    }

    fn force_reparse_schema_inner(self: &Arc<Connection>, publish: bool) -> Result<()> {
        if self.get_tx_state() != TransactionState::None {
            return Err(LimboError::Busy);
        }
        if self.get_mv_tx().is_some() || self.next_attached_mv_tx().is_some() {
            return Err(LimboError::Busy);
        }

        let pager = self.pager.load().clone();
        pager.clear_page_cache(false);
        pager.set_schema_cookie(None);
        pager.begin_read_tx()?;
        self.set_tx_state(TransactionState::Read);

        let reparse_result = self.reparse_schema();

        let previous = self.transaction_state.swap(TransactionState::None);
        turso_assert!(
            matches!(previous, TransactionState::None | TransactionState::Read),
            "unexpected end transaction state"
        );
        if previous == TransactionState::Read {
            pager.end_read_tx();
        }
        self.clear_internal_main_mvcc_tx(&pager);

        reparse_result?;

        if publish {
            let schema = self.schema.read().clone();
            self.db.update_schema_if_newer(schema);
        }
        Ok(())
    }

    fn clear_internal_main_mvcc_tx(&self, pager: &Arc<Pager>) {
        let Some(tx_id) = self.get_mv_tx_id() else {
            return;
        };
        if let Some(mv_store) = self.mv_store().as_ref() {
            if mv_store.is_tx_rollbackable(tx_id) {
                mv_store.rollback_tx(tx_id, pager.clone(), self, MAIN_DB_ID);
            } else {
                self.set_mv_tx(None);
            }
        } else {
            self.set_mv_tx(None);
        }
        pager.cleanup_read_tx();
    }

    /// Blocking shim: drives [`Self::reparse_schema_nonblock`] to completion.
    /// Retained for the many synchronous callers (statement reprepare, attach,
    /// extension load, vacuum). The genuinely non-blocking callers (MVCC
    /// bootstrap, the open-db state machine) drive `*_nonblock` directly.
    pub(crate) fn reparse_schema(self: &Arc<Connection>) -> Result<()> {
        let io = self.pager.load().io.clone();
        let mut state = ReparseSchemaState::default();
        io.block(|| self.reparse_schema_nonblock(&mut state))
    }

    /// VACUUM-only reparse that grafts a caller-supplied sequence-
    /// descriptor map onto the freshly parsed schema rather than re-
    /// reading the backing tables from disk. VACUUM preserves every
    /// sequence's definition (start/inc/min/max/cycle) — only physical
    /// page locations change — so the source connection's pre-VACUUM
    /// sequences map is still valid for the post-VACUUM image.
    ///
    /// Blocking shim: VACUUM runs through the VDBE stepping layer (which
    /// does not thread IO out), so it drives the non-blocking reparse to
    /// completion here, seeding the `PopulateSequences` phase with the
    /// preserved map so it grafts rather than re-reading each backing table.
    pub(crate) fn reparse_schema_with_cookie_keeping_sequences(
        self: &Arc<Connection>,
        cookie: u32,
        sequences: rustc_hash::FxHashMap<String, Arc<crate::schema::Sequence>>,
    ) -> Result<()> {
        let io = self.pager.load().io.clone();
        let mut state = ReparseSchemaState::default();
        // `init_reparse_building` consumes the preserved map exactly once, on
        // the first (Start) invocation; `io.block` re-invokes the closure on
        // every IO completion, so `take()` yields `Some` only that first time.
        let mut preserved = Some(sequences);
        io.block(|| {
            if matches!(state, ReparseSchemaState::Start) {
                state = ReparseSchemaState::Building(Box::new(
                    self.init_reparse_building(cookie, preserved.take())?,
                ));
            }
            self.drive_reparse_building(&mut state)
        })
    }

    /// Non-blocking schema reparse. Reads the current schema cookie, then drives
    /// the schema rebuild via the shared [`ReparseSchemaState`].
    pub(crate) fn reparse_schema_nonblock(
        self: &Arc<Connection>,
        state: &mut ReparseSchemaState,
    ) -> crate::types::IOResultOr<()> {
        use crate::types::IOResult;
        if matches!(state, ReparseSchemaState::Start) {
            // read cookie before consuming statement program - otherwise we can
            // end up reading cookie with closed transaction state
            let cookie = crate::return_if_io!(self.read_current_schema_cookie_nonblock());
            *state =
                ReparseSchemaState::Building(Box::new(self.init_reparse_building(cookie, None)?));
        }
        self.drive_reparse_building(state)
    }

    /// Synchronous setup shared by the reparse entry points: install the cookie,
    /// build a fresh schema, capture table-valued functions, install the empty
    /// schema (the reprepare hack), and prepare the sqlite_schema scan.
    /// `preserved_sequences` is `Some` only for VACUUM, which grafts the map in
    /// the `PopulateSequences` phase instead of re-reading the backing tables.
    fn init_reparse_building(
        self: &Arc<Connection>,
        cookie: u32,
        preserved_sequences: Option<rustc_hash::FxHashMap<String, Arc<crate::schema::Sequence>>>,
    ) -> Result<ReparseSchemaInner> {
        let guard = self.schema_reparse_guard();
        self.pager.load().set_schema_cookie(Some(cookie));
        // create fresh schema as some objects can be deleted
        let mut fresh = Schema::with_options(
            self.experimental_custom_types_enabled(),
            self.db.dialect().as_ref(),
        )?;
        fresh.generated_columns_enabled = self.db.experimental_generated_columns_enabled();
        fresh.schema_version = cookie;

        // Capture built-in table-valued functions (e.g. generate_series, json_each)
        // before dropping the old schema. These are registered programmatically and
        // don't survive re-parsing from sqlite_schema alone.
        let tvfs: Vec<Arc<crate::vtab::VirtualTable>> = self
            .schema
            .read()
            .tables
            .values()
            .filter_map(|table| match table.as_ref() {
                crate::schema::Table::Virtual(vtab)
                    if matches!(vtab.kind, turso_ext::VTabKind::TableValuedFunction) =>
                {
                    Some(vtab.clone())
                }
                _ => None,
            })
            .collect();

        // TODO: this is hack to avoid a cyclical problem with schema reprepare
        // The problem here is that we prepare a statement here, but when the statement tries
        // to execute it, it first checks the schema cookie to see if it needs to reprepare the statement.
        // But in this occasion it will always reprepare, and we get an error. So we trick the statement by swapping our schema
        // with a new clean schema that has the same header cookie.
        self.with_schema_mut(|schema| {
            *schema = fresh.try_clone()?;
            Ok::<_, crate::alloc::TryReserveError>(())
        })??;

        let stmt = self.prepare("SELECT * FROM sqlite_schema")?;

        // MVCC bootstrap connection gets the "baseline" from the DB file and ignores anything in MV store
        let mv_tx = if self.is_mvcc_bootstrap_connection() {
            None
        } else {
            self.get_mv_tx()
        };
        Ok(ReparseSchemaInner {
            _guard: guard,
            fresh,
            tvfs,
            preserved_sequences,
            phase: ReparsePhase::ParseSchema {
                parse: Box::new(crate::util::ParseSchemaRowsState::new(stmt, mv_tx)),
            },
        })
    }

    /// Drive the schema-rebuild phases held in `state` (must be `Building`).
    /// Yields IO; on completion installs the finished schema and resets `state`.
    fn drive_reparse_building(
        self: &Arc<Connection>,
        state: &mut ReparseSchemaState,
    ) -> crate::types::IOResultOr<()> {
        use crate::types::IOResult;
        loop {
            let ReparseSchemaState::Building(inner) = state else {
                unreachable!("drive_reparse_building requires Building state");
            };
            match &mut inner.phase {
                ReparsePhase::ParseSchema { parse } => {
                    // Resolver so attached-db qualifiers in temp triggers can be
                    // mapped to their actual index on this connection.
                    let attached_resolver = |name: &str| -> Option<usize> {
                        self.attached_databases
                            .read()
                            .get_database_by_name(&crate::util::normalize_ident(name))
                            .map(|(idx, _)| idx)
                    };
                    crate::return_if_io!(crate::util::parse_schema_rows(
                        parse,
                        &mut inner.fresh,
                        &self.syms.read(),
                        &attached_resolver,
                        self.db.dialect().as_ref(),
                    ));

                    // Rehydrate built-in table-valued functions captured at init.
                    for vtab in &inner.tvfs {
                        let normalized = crate::util::normalize_ident(&vtab.name);
                        inner.fresh.tables.entry(normalized).or_insert_with(|| {
                            Arc::new(crate::schema::Table::Virtual(vtab.clone()))
                        });
                    }

                    // Next: recover sequence descriptors (or graft the VACUUM map).
                    inner.phase = ReparsePhase::PopulateSequences {
                        pending: None,
                        idx: 0,
                        stmt: None,
                        meta: None,
                        seq: None,
                        watermark_stmt: None,
                        watermark_row: None,
                    };
                }
                ReparsePhase::PopulateSequences {
                    pending,
                    idx,
                    stmt,
                    meta,
                    seq,
                    watermark_stmt,
                    watermark_row,
                } => {
                    // Lazy init: graft the VACUUM-preserved descriptor map, or
                    // compute the worklist of backing tables to read from disk.
                    // When there is real work, install `fresh` first so the
                    // descriptor SELECTs can resolve the backing tables.
                    if pending.is_none() {
                        if let Some(sequences) = inner.preserved_sequences.take() {
                            inner.fresh.sequences = sequences;
                            *pending = Some(crate::alloc::vec![]);
                        } else {
                            let work = inner.fresh.sequence_backing_table_names();
                            if !work.is_empty() {
                                self.with_schema_mut(|schema| {
                                    *schema = inner.fresh.try_clone()?;
                                    Ok::<_, crate::alloc::TryReserveError>(())
                                })??;
                            }
                            *pending = Some(work);
                        }
                    }

                    // Read each remaining backing table's descriptor row. The
                    // backing table is internal; a missing/unreadable descriptor
                    // row is on-disk corruption (not "sequence missing"), so any
                    // failure surfaces as `Corrupt` and fails the open rather than
                    // silently dropping the sequence.
                    loop {
                        let entry = {
                            let work = pending.as_ref().expect("pending initialized above");
                            if *idx >= work.len() {
                                break;
                            }
                            work[*idx].clone()
                        };
                        let (backing_table_name, seq_name) = entry;
                        let normalized = crate::util::normalize_ident(&seq_name);
                        if inner.fresh.sequences.contains_key(&normalized) {
                            *idx += 1;
                            *stmt = None;
                            *meta = None;
                            *seq = None;
                            *watermark_stmt = None;
                            *watermark_row = None;
                            continue;
                        }
                        if seq.is_none() {
                            crate::return_if_io!(self.read_seq_descriptor_row_nonblock(
                                &backing_table_name,
                                &seq_name,
                                stmt,
                                meta,
                            ));
                            *seq = Some(Self::sequence_from_descriptor_meta(
                                &seq_name,
                                &backing_table_name,
                                *meta,
                            )?);
                            *stmt = None;
                            *meta = None;
                        }
                        let sequence = seq.as_ref().expect("sequence set above");
                        crate::return_if_io!(self.read_sequence_watermark_row_nonblock(
                            &backing_table_name,
                            sequence,
                            watermark_stmt,
                            watermark_row,
                        ));
                        let watermark = Self::sequence_watermark_from_row(
                            &backing_table_name,
                            sequence,
                            *watermark_row,
                        )?;
                        if let Some(mv_store) = self.db.get_mv_store().as_ref() {
                            mv_store.set_sequence_watermark(&normalized, watermark);
                        }
                        let sequence = seq.take().expect("sequence set above");
                        inner.fresh.sequences.insert(normalized, Arc::new(sequence));
                        *idx += 1;
                        *stmt = None;
                        *meta = None;
                        *watermark_stmt = None;
                        *watermark_row = None;
                    }

                    // Decide whether to load custom types next.
                    if self.experimental_custom_types_enabled()
                        && inner
                            .fresh
                            .tables
                            .contains_key(crate::schema::TURSO_TYPES_TABLE_NAME)
                    {
                        // Temporarily install the schema so we can query against it.
                        self.with_schema_mut(|schema| {
                            *schema = inner.fresh.try_clone()?;
                            Ok::<_, crate::alloc::TryReserveError>(())
                        })??;
                        let stmt = self.prepare_internal(format!(
                            "SELECT name, sql FROM {}",
                            crate::schema::TURSO_TYPES_TABLE_NAME
                        ))?;
                        inner.phase = ReparsePhase::LoadTypes {
                            stmt: Box::new(stmt),
                            type_rows: Vec::new(),
                        };
                    } else {
                        inner.phase = ReparsePhase::RefreshStats {
                            stats: Default::default(),
                        };
                    }
                }
                ReparsePhase::LoadTypes { stmt, type_rows } => {
                    // Type loading is best-effort: log and continue on error.
                    let scan = (|| -> IOResultOr<()> {
                        crate::return_if_io!(stmt.run_with_row_callback_nonblock(|row| {
                            type_rows.push(row.get::<&str>(1)?.to_string());
                            Ok(())
                        }));
                        Ok(IOResult::Done(()))
                    })();
                    match scan {
                        Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
                        Ok(IOResult::Done(())) => {
                            let type_rows = std::mem::take(type_rows);
                            if let Err(e) = inner.fresh.load_type_definitions(&type_rows) {
                                tracing::warn!("Failed to load custom types: {}", e);
                            }
                            inner.phase = ReparsePhase::RefreshStats {
                                stats: Default::default(),
                            };
                        }
                        Err(e) => {
                            tracing::warn!("Failed to load custom types: {}", e);
                            inner.phase = ReparsePhase::RefreshStats {
                                stats: Default::default(),
                            };
                        }
                    }
                }
                ReparsePhase::RefreshStats { stats } => {
                    // Best-effort load stats if sqlite_stat1 is present.
                    crate::return_if_io!(crate::stats::refresh_analyze_stats_nonblock(self, stats));

                    // Finalize: install the rebuilt schema. Take ownership so the
                    // guard drops and `state` is reusable.
                    let ReparseSchemaState::Building(inner) = std::mem::take(state) else {
                        unreachable!("state is Building");
                    };
                    let fresh = inner.fresh;
                    tracing::debug!(
                        "reparse_schema: schema_version={}, tables={:?}",
                        fresh.schema_version,
                        fresh.tables.keys()
                    );
                    self.with_schema_mut(|schema| {
                        *schema = fresh;
                    })?;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    pub(crate) fn read_current_schema_cookie(&self) -> Result<u32> {
        if let Some(mv_store) = self.mv_store().as_ref() {
            let tx_id = self.get_mv_tx_id();
            mv_store.with_header(|header| header.schema_cookie.get(), tx_id.as_ref())
        } else {
            let pager = self.pager.load();
            pager
                .io
                .block(|| pager.with_header(|header| header.schema_cookie))
                .map(|cookie| cookie.get())
        }
    }

    /// Non-blocking variant of [`Self::read_current_schema_cookie`]. The MVCC
    /// path reads an in-memory header (never yields); the pager path may yield
    /// while reading page 1. Idempotent across re-entry.
    pub(crate) fn read_current_schema_cookie_nonblock(&self) -> crate::types::IOResultOr<u32> {
        use crate::types::IOResult;
        if let Some(mv_store) = self.mv_store().as_ref() {
            let tx_id = self.get_mv_tx_id();
            let cookie =
                mv_store.with_header(|header| header.schema_cookie.get(), tx_id.as_ref())?;
            Ok(crate::types::IOResult::Done(cookie))
        } else {
            let pager = self.pager.load();
            let cookie = crate::return_if_io!(pager.with_header(|header| header.schema_cookie));
            Ok(crate::types::IOResult::Done(cookie.get()))
        }
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn prepare_execute_batch(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<()> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        if sql.as_ref().is_empty() {
            return Err(LimboError::InvalidArgument(
                "The supplied SQL string contains no statements".to_string(),
            ));
        }
        let sql = sql.as_ref();
        tracing::trace!("Preparing and executing batch: {}", sql);

        let mut remaining = sql;
        let prepare_options = PrepareOptions::default();
        while let (Some(cmd), byte_offset_end) = self.parse_sql(remaining)? {
            let input = str::from_utf8(&remaining.as_bytes()[..byte_offset_end])
                .unwrap()
                .trim();
            let (program, pager, mode) =
                self.compile_cmd(cmd, input, StatementOrigin::Root, &prepare_options)?;
            Statement::new(program, pager, mode, 0).run_ignore_rows()?;
            remaining = &remaining[byte_offset_end..];
        }
        Ok(())
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn query(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Option<Statement>> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let sql = sql.as_ref();
        tracing::trace!("Querying: {}", sql);

        let (cmd, byte_offset_end) = self.parse_sql(sql)?;
        let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
            .unwrap()
            .trim();
        match cmd {
            Some(cmd) => self.run_cmd(cmd, input),
            None => Ok(None),
        }
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub(crate) fn run_cmd(
        self: &Arc<Connection>,
        cmd: Cmd,
        input: &str,
    ) -> Result<Option<Statement>> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let prepare_options = PrepareOptions::default();
        let (program, pager, mode) =
            self.compile_cmd(cmd, input, StatementOrigin::Root, &prepare_options)?;
        let stmt = Statement::new(program, pager, mode, 0);
        Ok(Some(stmt))
    }

    pub fn query_runner<'a>(self: &'a Arc<Connection>, sql: &'a [u8]) -> QueryRunner<'a> {
        QueryRunner::new(self, sql)
    }

    /// Execute will run a query from start to finish taking ownership of I/O because it will run pending I/Os if it didn't finish.
    /// TODO: make this api async
    #[instrument(skip_all, level = Level::DEBUG)]
    #[turso_macros::trace_stack]
    pub fn execute(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<()> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let sql = sql.as_ref();
        let mut remaining = sql;
        let prepare_options = PrepareOptions::default();
        while let (Some(cmd), byte_offset_end) = self.parse_sql(remaining)? {
            let input = str::from_utf8(&remaining.as_bytes()[..byte_offset_end])
                .unwrap()
                .trim();
            let (program, pager, mode) =
                self.compile_cmd(cmd, input, StatementOrigin::Root, &prepare_options)?;
            {
                crate::stack::trace_stack!("run");
                Statement::new(program, pager.clone(), mode, 0).run_ignore_rows()?;
            }
            remaining = &remaining[byte_offset_end..];
        }
        Ok(())
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn consume_stmt(
        self: &Arc<Connection>,
        sql: impl AsRef<str>,
    ) -> Result<Option<(Statement, usize)>> {
        let (cmd, byte_offset_end) = self.parse_sql(sql.as_ref())?;
        let Some(cmd) = cmd else {
            return Ok(None);
        };
        let input = str::from_utf8(&sql.as_ref().as_bytes()[..byte_offset_end])
            .unwrap()
            .trim();
        let prepare_options = PrepareOptions::default();
        let (program, pager, mode) =
            self.compile_cmd(cmd, input, StatementOrigin::Root, &prepare_options)?;
        let stmt = Statement::new(program, pager, mode, 0);
        Ok(Some((stmt, byte_offset_end)))
    }

    pub(crate) fn parse_sql(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
        self.db.dialect().parse(sql)
    }

    #[cfg(feature = "fs")]
    pub fn from_uri(
        uri: &str,
        db_opts: DatabaseOpts,
        dialect: Arc<dyn crate::Dialect>,
    ) -> Result<(Arc<dyn IO>, Arc<Connection>)> {
        use crate::util::MEMORY_PATH;
        let opts = OpenOptions::parse(uri)?;
        let flags = opts.get_flags()?;
        if opts.path == MEMORY_PATH || matches!(opts.mode, OpenMode::Memory) {
            let io = Arc::new(MemoryIO::new());
            let db = Database::open_file_with_flags(
                io.clone(),
                MEMORY_PATH,
                flags,
                db_opts,
                None,
                dialect,
            )?;
            let conn = db.connect()?;
            return Ok((io, conn));
        }
        let encryption_opts = match (opts.cipher.clone(), opts.hexkey.clone()) {
            (Some(cipher), Some(hexkey)) => Some(EncryptionOpts { cipher, hexkey }),
            (Some(_), None) => {
                return Err(LimboError::InvalidArgument(
                    "hexkey is required when cipher is provided".to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(LimboError::InvalidArgument(
                    "cipher is required when hexkey is provided".to_string(),
                ));
            }
            (None, None) => None,
        };
        let (io, db) = Database::open_new(
            &opts.path,
            opts.vfs.as_ref(),
            flags,
            db_opts,
            encryption_opts,
            dialect,
        )?;
        if let Some(modeof) = opts.modeof {
            let perms = std::fs::metadata(modeof).map_err(|e| io_error(e, "metadata"))?;
            std::fs::set_permissions(&opts.path, perms.permissions())
                .map_err(|e| io_error(e, "set_permissions"))?;
        }
        let conn = db.connect()?;
        if let Some(cipher) = opts.cipher {
            let _ = conn.pragma_update("cipher", format!("'{cipher}'"));
        }
        if let Some(hexkey) = opts.hexkey {
            let _ = conn.pragma_update("hexkey", format!("'{hexkey}'"));
        }
        Ok((io, conn))
    }

    #[cfg(feature = "fs")]
    fn from_uri_attached(
        uri: &str,
        mut db_opts: DatabaseOpts,
        main_db_flags: OpenFlags,
        io: Arc<dyn IO>,
        dialect: Arc<dyn crate::Dialect>,
    ) -> Result<(Arc<Database>, Option<EncryptionOpts>)> {
        let opts = OpenOptions::parse(uri)?;
        let mut flags = opts.get_flags()?;
        if main_db_flags.contains(OpenFlags::ReadOnly) {
            flags |= OpenFlags::ReadOnly;
        }
        let encryption_opts = match (opts.cipher.clone(), opts.hexkey.clone()) {
            (Some(cipher), Some(hexkey)) => Some(EncryptionOpts { cipher, hexkey }),
            (Some(_), None) => {
                return Err(LimboError::InvalidArgument(
                    "hexkey is required when cipher is provided".to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(LimboError::InvalidArgument(
                    "cipher is required when hexkey is provided".to_string(),
                ));
            }
            (None, None) => None,
        };
        if encryption_opts.is_some() {
            db_opts = db_opts.with_encryption(true);
        }
        let io = opts.vfs.map(Database::io_for_vfs).unwrap_or(Ok(io))?;
        let db = Database::open_file_with_flags(
            io.clone(),
            &opts.path,
            flags,
            db_opts,
            encryption_opts.clone(),
            dialect,
        )?;
        if let Some(modeof) = opts.modeof {
            let perms = std::fs::metadata(modeof).map_err(|e| io_error(e, "metadata"))?;
            std::fs::set_permissions(&opts.path, perms.permissions())
                .map_err(|e| io_error(e, "set_permissions"))?;
        }
        Ok((db, encryption_opts))
    }

    pub fn set_foreign_keys_enabled(&self, enable: bool) {
        self.fk_pragma.store(enable, Ordering::Release);
        self.bump_prepare_context_generation();
    }

    pub fn foreign_keys_enabled(&self) -> bool {
        self.fk_pragma.load(Ordering::Acquire)
    }

    pub fn set_check_constraints_ignored(&self, ignore: bool) {
        self.check_constraints_pragma
            .store(ignore, Ordering::Release);
    }

    pub fn check_constraints_ignored(&self) -> bool {
        self.check_constraints_pragma.load(Ordering::Acquire)
    }

    pub(crate) fn clear_deferred_foreign_key_violations(&self) -> isize {
        self.fk_deferred_violations.swap(0, Ordering::Release)
    }

    pub(crate) fn get_deferred_foreign_key_violations(&self) -> isize {
        self.fk_deferred_violations.load(Ordering::Acquire)
    }

    pub(crate) fn increment_deferred_foreign_key_violations(&self, v: isize) {
        self.fk_deferred_violations.fetch_add(v, Ordering::AcqRel);
    }

    /// Query the CREATE TYPE SQL definitions stored in __turso_internal_types.
    /// The connection's schema must already contain the table definitions so
    /// that `prepare` can resolve the table name. Returns an empty Vec if the
    /// types table does not exist.
    pub(crate) fn query_stored_type_definitions(self: &Arc<Connection>) -> Result<Vec<String>> {
        let has_types_table = {
            let s = self.schema.read();
            s.tables.contains_key(crate::schema::TURSO_TYPES_TABLE_NAME)
        };
        if !has_types_table {
            return Ok(Vec::new());
        }
        let mut type_stmt = self.prepare_internal(format!(
            "SELECT name, sql FROM {}",
            crate::schema::TURSO_TYPES_TABLE_NAME
        ))?;
        let mut type_rows = Vec::new();
        type_stmt.run_with_row_callback(|row| {
            type_rows.push(row.get::<&str>(1)?.to_string());
            Ok(())
        })?;
        Ok(type_rows)
    }

    pub fn maybe_update_schema(&self) {
        if self.schema_reparse_in_progress() {
            return;
        }
        // Inside a transaction the schema cannot change under the
        // connection, so there is nothing to adopt. This runs on every step
        // of every statement in MVCC mode, and the check below takes the
        // database's shared schema lock, which every connection contends
        // for: decide without it whenever possible.
        if !self.has_no_open_transaction_state() {
            return;
        }
        let current_schema = self.schema.read().clone();
        let schema = self.db.schema.lock();
        // MVCC checkpoint can publish physical btree roots into the shared
        // schema without changing SQLite's schema cookie. If this connection
        // still has the older schema snapshot, prepared statements must be
        // invalidated and recompiled with the published roots.
        if current_schema.schema_version != schema.schema_version
            || self.has_mvcc_schema_snapshot_changed_with_same_version(&current_schema, &schema)
        {
            let mut adopted = schema.clone();
            // Resolve placeholder (negative) roots to the real pages a checkpoint has
            // materialized, so consumers that skip negative roots (integrity_check) see them.
            let mv_store_guard = self.db.get_mv_store();
            if let Some(mv_store) = mv_store_guard.as_ref() {
                if let Ok(schema) = Schema::try_make_mut(&mut adopted) {
                    mv_store.resolve_schema_negative_roots(schema);
                }
            }
            *self.schema.write() = adopted;
            self.bump_prepare_context_generation();
        }
    }

    fn has_no_open_transaction_state(&self) -> bool {
        matches!(self.get_tx_state(), TransactionState::None)
            && self.get_mv_tx().is_none()
            && self.next_attached_mv_tx().is_none()
    }

    fn has_mvcc_schema_snapshot_changed_with_same_version(
        &self,
        current_schema: &Arc<Schema>,
        schema: &Arc<Schema>,
    ) -> bool {
        self.mvcc_enabled()
            && current_schema.schema_version == schema.schema_version
            && !Arc::ptr_eq(current_schema, schema)
    }

    pub(crate) fn mvcc_schema_requires_reprepare_before_tx(&self) -> bool {
        if !self.has_no_open_transaction_state() {
            return false;
        }
        let current_schema = self.schema.read().clone();
        let schema = self.db.schema.lock();
        self.has_mvcc_schema_snapshot_changed_with_same_version(&current_schema, &schema)
    }

    /// Begin-tx schema gate for MVCC. Returns the `MvStore::schema_generation` this connection's
    /// prepared schema is valid as of, or `SchemaUpdated` if it is already stale (a passive
    /// checkpoint republished physical roots without a cookie change). The returned generation is
    /// re-checked inside `begin_tx`'s clock callback: a publish bumps `schema_generation` under the
    /// same clock, so if one lands between here and the begin clock the generations differ and the
    /// statement is forced to reprepare against the published roots.
    pub(crate) fn mvcc_begin_schema_generation(&self) -> Result<Option<u64>> {
        let mv_guard = self.db.get_mv_store();
        let Some(mv) = mv_guard.as_ref() else {
            return Ok(None);
        };
        // Mid-transaction (e.g. a multi-statement BEGIN): the snapshot and schema are fixed at the
        // first begin, so a later checkpoint republication must not gate or reprepare here. Mirror
        // the guard of `mvcc_schema_requires_reprepare_before_tx`.
        if !self.has_no_open_transaction_state() {
            return Ok(None);
        }
        // Read the generation before the snapshot comparison: any publish that mutates the shared
        // schema after this read is caught by the comparison below (it changes the Arc), and any
        // publish that lands during begin is caught by the clock re-check (it bumps the generation).
        let generation = mv.schema_generation();
        let current_schema = self.schema.read().clone();
        let schema = self.db.schema.lock();
        if self.has_mvcc_schema_snapshot_changed_with_same_version(&current_schema, &schema) {
            return Err(LimboError::SchemaUpdated);
        }
        Ok(Some(generation))
    }

    pub(crate) fn refresh_schema_from_shared_for_reprepare(&self) {
        let current_schema = self.schema.read().clone();
        let schema = self.db.schema.lock().clone();
        if current_schema.schema_version < schema.schema_version
            || (self.has_no_open_transaction_state()
                && self
                    .has_mvcc_schema_snapshot_changed_with_same_version(&current_schema, &schema))
        {
            *self.schema.write() = schema;
            self.bump_prepare_context_generation();
        }
    }

    /// Read schema version at current transaction
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn read_schema_version(&self) -> Result<u32> {
        let pager = self.pager.load();
        pager
            .io
            .block(|| pager.with_header(|header| header.schema_cookie))
            .map(|version| version.get())
    }

    /// Update schema version to the new value within opened write transaction
    ///
    /// New version of the schema must be strictly greater than previous one - otherwise method will panic
    /// Write transaction must be opened in advance - otherwise method will panic
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn write_schema_version(self: &Arc<Connection>, version: u32) -> Result<()> {
        let TransactionState::Write { .. } = self.get_tx_state() else {
            return Err(LimboError::InternalError(
                "write_schema_version must be called from within Write transaction".to_string(),
            ));
        };
        let pager = self.pager.load();
        pager.io.block(|| {
            pager.with_header_mut(|header| {
                turso_assert!(
                    header.schema_cookie.get() < version,
                    "cookie can't go back in time"
                );
                self.with_schema_mut(|schema| schema.schema_version = version)
                    .map(|()| {
                        self.set_tx_state(TransactionState::Write {
                            schema_did_change: true,
                        });
                        header.schema_cookie = version.into();
                    })
            })
        })??;
        self.reparse_schema()?;
        Ok(())
    }

    /// Try to read page with given ID with fixed WAL watermark position
    /// This method return false if page is not found (so, this is probably new page created after watermark position which wasn't checkpointed to the DB file yet)
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn try_wal_watermark_read_page(
        &self,
        page_idx: u32,
        page: &mut [u8],
        frame_watermark: Option<u64>,
    ) -> Result<bool> {
        let Some((page_ref, c)) =
            self.try_wal_watermark_read_page_begin(page_idx, frame_watermark)?
        else {
            return Ok(false);
        };
        match self.get_pager().io.wait_for_completion(c) {
            Err(LimboError::CompletionError(err))
                if Self::wal_watermark_read_error_is_absent_page(&err) =>
            {
                return Ok(false);
            }
            Err(e) => return Err(e),
            _ => {}
        }

        self.try_wal_watermark_read_page_end(page, page_ref)
    }

    /// Classify a completion error raised while reading a page at a fixed WAL
    /// watermark. On Windows under `experimental_win_iocp`, an absent /
    /// zero-length page read surfaces as `UnexpectedEof` (see
    /// `core/io/win_iocp.rs`); every watermark-read site must treat that as
    /// "page absent" (size 0) rather than a hard error. Centralized here so the
    /// platform handling cannot drift across the (now four) call sites.
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_watermark_read_error_is_absent_page(err: &crate::error::CompletionError) -> bool {
        #[cfg(all(target_os = "windows", feature = "experimental_win_iocp"))]
        {
            matches!(
                err,
                crate::error::CompletionError::IOError(std::io::ErrorKind::UnexpectedEof, _)
            )
        }
        #[cfg(not(all(target_os = "windows", feature = "experimental_win_iocp")))]
        {
            let _ = err;
            false
        }
    }

    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn try_wal_watermark_read_page_begin(
        &self,
        page_idx: u32,
        frame_watermark: Option<u64>,
    ) -> Result<Option<(Arc<Page>, Completion)>> {
        let pager = self.pager.load();
        let (page_ref, c) =
            match pager.read_page_no_cache(page_idx as i64, frame_watermark, true, None) {
                Ok(result) => result,
                // on windows, zero read will trigger UnexpectedEof
                #[cfg(target_os = "windows")]
                Err(LimboError::CompletionError(crate::error::CompletionError::IOError(
                    std::io::ErrorKind::UnexpectedEof,
                    _,
                ))) => return Ok(None),
                Err(err) => return Err(err),
            };

        Ok(Some((page_ref, c)))
    }

    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn try_wal_watermark_read_page_end(
        &self,
        page: &mut [u8],
        page_ref: Arc<Page>,
    ) -> Result<bool> {
        let content = page_ref.get_contents();
        // empty read - attempt to read absent page
        if content.buffer.as_ref().is_none_or(|b| b.is_empty()) {
            return Ok(false);
        }
        page.copy_from_slice(content.as_ptr());
        Ok(true)
    }

    /// Return unique set of page numbers changes after WAL watermark position in the current WAL session
    /// (so, if concurrent connection wrote something to the WAL - this method will not see this change)
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_changed_pages_after(&self, frame_watermark: u64) -> Result<Vec<u32>> {
        self.pager.load().wal_changed_pages_after(frame_watermark)
    }

    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_state(&self) -> Result<WalState> {
        self.pager.load().wal_state()
    }

    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_get_frame(&self, frame_no: u64, frame: &mut [u8]) -> Result<WalFrameInfo> {
        use crate::storage::sqlite3_ondisk::parse_wal_frame_header;

        let c = self.pager.load().wal_get_frame(frame_no, frame)?;
        self.db.io.wait_for_completion(c)?;
        let (header, _) = parse_wal_frame_header(frame);
        Ok(WalFrameInfo {
            page_no: header.page_number,
            db_size: header.db_size,
        })
    }

    /// Insert `frame` (header included) at the position `frame_no` in the WAL
    /// If WAL already has frame at that position - turso-db will compare content of the page and either report conflict or return OK
    /// If attempt to write frame at the position `frame_no` will create gap in the WAL - method will return error
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_insert_frame(&self, frame_no: u64, frame: &[u8]) -> Result<WalFrameInfo> {
        self.pager.load().wal_insert_frame(frame_no, frame)
    }

    /// Start WAL session by initiating read+write transaction for this connection
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_insert_begin(&self) -> Result<()> {
        let pager = self.pager.load();
        pager.begin_read_tx()?;
        // Sync-engine drives WAL maintenance explicitly: any auto-restart of
        // the WAL header here would invalidate the watermarks the caller has
        // already published (see `wal_changed_pages_after`), so opt out of
        // every auto action for this write transaction.
        pager
            .io
            .block(|| pager.begin_write_tx(WalAutoActions::empty()))
            .inspect_err(|_| {
                pager.end_read_tx();
            })?;

        // start write transaction and disable auto-commit mode as SQL can be executed within WAL session (at caller own risk)
        self.set_tx_state(TransactionState::Write {
            schema_did_change: false,
        });
        self.auto_commit.store(false, Ordering::SeqCst);

        Ok(())
    }

    /// Finish WAL session by ending read+write transaction taken in the [Self::wal_insert_begin] method
    /// All frames written after last commit frame (db_size > 0) within the session will be rolled back
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_insert_end(self: &Arc<Connection>, force_commit: bool) -> Result<()> {
        use crate::{return_if_io, types::IOResult};

        {
            let pager = self.pager.load();

            let Some(wal) = pager.wal.as_ref() else {
                return Err(LimboError::InternalError(
                    "wal_insert_end called without a wal".to_string(),
                ));
            };

            let commit_err = if force_commit {
                pager
                    .io
                    .block(|| {
                        return_if_io!(pager.commit_wal(
                            WalAutoActions::empty(),
                            self.get_sync_mode(),
                            self.get_data_sync_retry(),
                        ));
                        pager.commit_wal_end();
                        Ok(IOResult::Done(()))
                    })
                    .err()
            } else {
                None
            };

            self.auto_commit.store(true, Ordering::SeqCst);
            self.set_tx_state(TransactionState::None);
            wal.end_write_tx();
            wal.end_read_tx();

            if !force_commit {
                // remove all non-commited changes in case if WAL session left some suffix without commit frame
                if let Some(mv_store) = self.mv_store().as_ref() {
                    if let Some(tx_id) = self.get_mv_tx_id() {
                        mv_store.rollback_tx(tx_id, pager.clone(), self, MAIN_DB_ID);
                    }
                }
                pager.rollback(false, self, true);
            }
            if let Some(err) = commit_err {
                return Err(err);
            }
        }

        // let's re-parse schema from scratch if schema cookie changed compared to the our in-memory view of schema
        self.maybe_reparse_schema()?;
        Ok(())
    }

    /// Flush dirty pages to disk.
    pub fn cacheflush(&self) -> Result<Vec<Completion>> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let pager = self.pager.load();
        pager.io.block(|| pager.cacheflush())
    }

    pub fn checkpoint(self: &Arc<Self>, mode: CheckpointMode) -> Result<CheckpointResult> {
        use crate::mvcc::database::CheckpointStateMachine;
        use crate::state_machine::{StateTransition, TransitionResult};
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        // Checkpointing invalidates this connection's cursors and page cache,
        // which breaks any statement suspended mid-execution, so refuse to run
        // while statements are active — same rule as PRAGMA wal_checkpoint.
        // The guard also rejects new statements until the checkpoint finishes
        // and cleans up pager checkpoint state if we bail out mid-flight.
        let _checkpoint_guard = self.begin_explicit_checkpoint(self.pager.load().clone(), false)?;
        if let Some(mv_store) = self.mv_store().as_ref() {
            let mode = if self.experimental_mvcc_passive_checkpoint_enabled() {
                assert!(
                    matches!(
                        mode,
                        CheckpointMode::Passive { .. } | CheckpointMode::Truncate { .. }
                    ),
                    "MVCC checkpoint supports only Truncate or Passive when experimental_mvcc_passive_checkpoint is enabled"
                );
                mode
            } else {
                CheckpointMode::Truncate {
                    upper_bound_inclusive: None,
                }
            };
            let pager = self.pager.load().clone();
            let io = pager.io.clone();
            let mut ckpt_sm = CheckpointStateMachine::new(
                pager,
                mv_store.clone(),
                self.clone(),
                true,
                self.get_sync_mode(),
                MAIN_DB_ID,
                mode,
            );
            loop {
                match ckpt_sm.step(&()) {
                    Ok(TransitionResult::Continue) => {}
                    Ok(TransitionResult::Done(result)) => return Ok(result),
                    Ok(TransitionResult::Io(iocompletions)) => {
                        if let Err(err) = iocompletions.wait(io.as_ref()) {
                            ckpt_sm.cleanup_after_external_io_error(err.clone())?;
                            return Err(err);
                        }
                    }
                    Err(err) => return Err(err),
                }
            }
        } else {
            self.pager
                .load()
                .blocking_checkpoint(mode, self.get_sync_mode())
        }
    }

    /// Close a connection and checkpoint.
    pub fn close(&self) -> Result<()> {
        if self.is_closed() {
            return Ok(());
        }
        self.closed.store(true, Ordering::SeqCst);
        let pager = self.pager.load();

        match self.get_tx_state() {
            TransactionState::None => {
                // No active transaction
            }
            _ => {
                if self.mvcc_enabled() {
                    if let Some(mv_store) = self.mv_store().as_ref() {
                        if let Some(tx_id) = self.get_mv_tx_id() {
                            mv_store.rollback_tx(tx_id, pager.clone(), self, MAIN_DB_ID);
                        }
                    }
                    pager.end_read_tx();
                } else {
                    pager.rollback_tx(self);
                }
                // Roll back all attached DB transactions regardless of main
                // DB mode — a :memory: attached DB may use WAL even when the
                // main DB uses MVCC.
                self.rollback_attached_mvcc_txs(false);
                self.rollback_attached_wal_txns();
                self.set_tx_state(TransactionState::None);
            }
        }
        self.index_methods_on_transaction_rolled_back();
        self.clear_mvcc_log_meta();

        let is_memory_db = is_memory_like(&self.db.path);
        let should_checkpoint_on_close = pager
            .wal
            .as_ref()
            .is_none_or(|wal| wal.should_checkpoint_on_close());
        if self.db.n_connections.fetch_sub(1, Ordering::SeqCst).eq(&1)
            && !self.db.is_readonly()
            && !is_memory_db
            && should_checkpoint_on_close
        {
            self.pager
                .load()
                .checkpoint_shutdown(self.wal_auto_actions(), self.get_sync_mode())?;
        };
        Ok(())
    }

    /// Disable every automatic WAL maintenance action for this connection
    /// (auto-checkpoint AND WAL header restart). Sync-engine consumers call
    /// this so they own all WAL bookkeeping themselves.
    pub fn wal_auto_actions_disable(&self) {
        self.wal_auto_actions
            .store(WalAutoActions::empty().bits(), Ordering::SeqCst);
    }

    /// Returns the set of automatic WAL maintenance actions this connection
    /// permits. MVCC connections always return an empty set because the
    /// MVCC checkpoint state machine drives WAL maintenance explicitly.
    pub fn wal_auto_actions(&self) -> WalAutoActions {
        if self.db.get_mv_store().is_some() {
            return WalAutoActions::empty();
        }
        WalAutoActions::from_bits_truncate(self.wal_auto_actions.load(Ordering::SeqCst))
    }

    /// Publish the connection's current schema snapshot to the shared database
    /// cache after a successful commit so other live connections can refresh.
    pub fn publish_schema_if_newer(&self) {
        let schema = self.schema.read().clone();
        self.db.update_schema_if_newer(schema);
    }

    /// Publish the connection's current schema snapshot after pages were
    /// replaced outside normal SQL commit ordering.
    ///
    /// External restore paths can move the schema cookie backwards. In that
    /// case the shared schema cache must be replaced rather than updated
    /// monotonically, otherwise new connections can re-adopt stale metadata.
    #[cfg(feature = "conn_raw_api")]
    pub fn publish_schema_after_external_restore(&self) -> Result<()> {
        if self.get_tx_state() != TransactionState::None {
            return Err(LimboError::Busy);
        }
        if self.get_mv_tx().is_some() || self.next_attached_mv_tx().is_some() {
            return Err(LimboError::Busy);
        }

        let schema = self.schema.read().clone();
        self.db.with_schema_mut(|current| {
            *current = schema.as_ref().try_clone()?;
            Ok(())
        })?;
        Ok(())
    }

    /// Roll back the main-database MVCC transaction while keeping the
    /// surrounding raw WAL-insert session open.
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn reset_main_mvcc_tx_for_wal_session(&self) {
        let mv_store = self.mv_store();
        let Some(mv_store) = mv_store.as_ref() else {
            return;
        };
        let Some(tx_id) = self.get_mv_tx_id() else {
            return;
        };
        let pager = self.pager.load();
        mv_store.rollback_tx(tx_id, pager.clone(), self, MAIN_DB_ID);
    }

    /// Discard the main-db MVCC transaction left by a sync raw-WAL session
    /// before reparsing state after external file replacement.
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn discard_main_mvcc_tx_after_external_restore(&self) {
        let pager = self.pager.load();
        self.clear_internal_main_mvcc_tx(&pager);
    }

    /// Returns whether the main database currently has a live MVCC transaction.
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn has_main_mvcc_tx_for_wal_session(&self) -> bool {
        self.get_mv_tx_id().is_some()
    }

    /// Commit the main-database MVCC transaction while keeping the surrounding
    /// raw WAL-insert session open.
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn commit_main_mvcc_tx_for_wal_session(self: &Arc<Self>) -> Result<()> {
        let mv_store_handle = self.mv_store();
        let Some(mv_store) = mv_store_handle.as_ref() else {
            return Ok(());
        };
        let Some(tx_id) = self.get_mv_tx_id() else {
            return Ok(());
        };

        let mut state_machine = mv_store.commit_tx(tx_id, self, MAIN_DB_ID)?;
        while let IOResult::IO(io) = state_machine.step(mv_store)? {
            io.wait(self.db.io.as_ref())?;
        }
        assert!(state_machine.is_finalized());
        self.set_mv_tx(None);
        self.publish_schema_if_newer();
        Ok(())
    }

    #[cfg(feature = "conn_raw_api")]
    pub fn reload_wal_after_external_restore(&self) -> Result<()> {
        self.db.reload_wal_after_external_restore()
    }

    /// Enable or disable writing portable logical-change metadata into MVCC
    /// logical-log frames.
    pub fn set_portable_logical_changes_enabled(&self, enabled: bool) {
        #[cfg(feature = "conn_raw_api")]
        {
            self.portable_logical_changes_enabled
                .store(enabled, Ordering::Release);
        }
        let _ = enabled;
    }

    pub fn portable_logical_changes_enabled(&self) -> bool {
        #[cfg(feature = "conn_raw_api")]
        {
            self.portable_logical_changes_enabled
                .load(Ordering::Acquire)
        }
        #[cfg(not(feature = "conn_raw_api"))]
        {
            false
        }
    }

    pub fn set_mvcc_log_meta(&self, key: String, value: Option<String>) {
        #[cfg(feature = "conn_raw_api")]
        {
            let mut metadata = self.mvcc_log_metadata.write();
            match value {
                Some(value) => {
                    metadata.insert(key, value);
                }
                None => {
                    metadata.remove(&key);
                }
            }
        }
        #[cfg(not(feature = "conn_raw_api"))]
        {
            let _ = (key, value);
        }
    }

    #[cfg(feature = "conn_raw_api")]
    pub(crate) fn mvcc_log_meta_snapshot(&self) -> HashMap<String, String> {
        self.mvcc_log_metadata.read().clone()
    }

    pub(crate) fn clear_mvcc_log_meta(&self) {
        #[cfg(feature = "conn_raw_api")]
        {
            self.mvcc_log_metadata.write().clear();
        }
    }

    pub fn mvcc_log_meta(&self, key: &str) -> Option<String> {
        #[cfg(feature = "conn_raw_api")]
        {
            return self.mvcc_log_metadata.read().get(key).cloned();
        }
        #[cfg(not(feature = "conn_raw_api"))]
        {
            let _ = key;
            None
        }
    }

    #[cfg(feature = "simulator")]
    pub fn checkpoint_for_testing(&self, mode: CheckpointMode) -> Result<CheckpointResult> {
        let pager = self.pager.load();
        pager
            .io
            .block(|| pager.checkpoint(mode, SyncMode::Full, true))
    }

    #[cfg(all(feature = "simulator", target_pointer_width = "64", host_shared_wal))]
    pub fn install_unpublished_backfill_proof_for_testing(
        &self,
        upper_bound_inclusive: u64,
    ) -> Result<()> {
        let pager = self.pager.load();
        let proof_nbackfills =
            pager.run_checkpoint_until_post_sync_gap_for_testing(CheckpointMode::Passive {
                upper_bound_inclusive: Some(upper_bound_inclusive),
            })?;
        let authority = self.db.shared_wal_coordination()?.ok_or_else(|| {
            LimboError::InternalError("shared WAL authority is unavailable".into())
        })?;
        let snapshot_before_publish = authority.snapshot();
        if snapshot_before_publish.nbackfills != 0 {
            return Err(LimboError::InternalError(
                "unpublished-proof setup requires nbackfills to remain unpublished".into(),
            ));
        }

        let (db_size_pages, db_header_crc32c) = db_identity_for_testing(Path::new(&self.db.path))?;
        authority.install_backfill_proof(
            crate::storage::shared_wal_coordination::SharedWalCoordinationHeader {
                nbackfills: proof_nbackfills,
                ..snapshot_before_publish
            },
            db_size_pages,
            db_header_crc32c,
        );
        Ok(())
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid.load(Ordering::SeqCst)
    }

    pub(crate) fn update_last_rowid(&self, rowid: i64) {
        self.last_insert_rowid.store(rowid, Ordering::SeqCst);
    }

    pub(crate) fn add_total_changes(&self, num_changes: i64) {
        self.total_changes.fetch_add(num_changes, Ordering::SeqCst);
    }

    pub fn set_changes(&self, num_changes: i64) {
        self.changes.store(num_changes, Ordering::SeqCst);
    }

    pub fn changes(&self) -> i64 {
        self.changes.load(Ordering::SeqCst)
    }

    pub fn total_changes(&self) -> i64 {
        self.total_changes.load(Ordering::SeqCst)
    }

    pub fn get_cache_size(&self) -> i32 {
        self.cache_size.load(Ordering::SeqCst)
    }
    pub fn set_cache_size(&self, size: i32) {
        self.cache_size.store(size, Ordering::SeqCst);
        self.bump_prepare_context_generation();
    }

    pub fn get_capture_data_changes_info(
        &self,
    ) -> crate::sync::RwLockReadGuard<'_, Option<CaptureDataChangesInfo>> {
        self.capture_data_changes.read()
    }
    pub fn set_capture_data_changes_info(&self, opts: Option<CaptureDataChangesInfo>) {
        *self.capture_data_changes.write() = opts;
        self.bump_prepare_context_generation();
    }
    pub fn get_cdc_transaction_id(&self) -> i64 {
        self.cdc_transaction_id.load(Ordering::SeqCst)
    }
    pub fn set_cdc_transaction_id(&self, id: i64) {
        self.cdc_transaction_id.store(id, Ordering::SeqCst);
    }
    pub fn get_page_size(&self) -> PageSize {
        let value = self.page_size.load(Ordering::SeqCst);
        PageSize::new_from_header_u16(value).unwrap_or_default()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub fn is_query_only(&self) -> bool {
        self.query_only.load(Ordering::SeqCst)
    }

    pub fn get_database_canonical_path(&self) -> String {
        self.db.get_database_canonical_path()
    }

    /// Check if a specific attached database is read only or not, by its index
    pub fn is_readonly(&self, index: usize) -> bool {
        match index {
            crate::MAIN_DB_ID => self.db.is_readonly(),
            crate::TEMP_DB_ID => self
                .temp
                .database
                .read()
                .as_ref()
                .is_some_and(|temp_db| temp_db.db.is_readonly()),
            _ => {
                let db = self.attached_databases.read().get_database_by_index(index);
                db.expect("Should never have called this without being sure the database exists")
                    .is_readonly()
            }
        }
    }

    /// Reset the page size for the current connection.
    ///
    /// Specifying a new page size does not change the page size immediately.
    /// Instead, the new page size is remembered and is used to set the page size when the database
    /// is first created, if it does not already exist when the page_size pragma is issued,
    /// or at the next VACUUM command that is run on the same database connection while not in WAL mode.
    pub fn reset_page_size(&self, size: u32) -> Result<()> {
        if self.db.initialized() {
            return Ok(());
        }
        let Some(size) = PageSize::new(size) else {
            return Ok(());
        };

        self.pager.load().set_initial_page_size(size)?;
        self.page_size.store(size.get_raw(), Ordering::SeqCst);
        // MvStore caches a copy of the database header in `global_header`, captured from the
        // pager during bootstrap (before any PRAGMA page_size can run). Propagate the new
        // page size so subsequent transactions and any header lookups see the same value the
        // pager will write to disk; otherwise paths like op_open_ephemeral allocate buffers
        // sized to the connection's page_size but compute usable_space from the stale 4 KiB
        // global header, tripping the btree_init_page assertion.
        if let Some(mv_store) = self.db.get_mv_store().as_ref() {
            mv_store.set_global_page_size(size);
        }
        self.bump_prepare_context_generation();

        Ok(())
    }

    #[cfg(feature = "fs")]
    pub fn open_new(
        &self,
        path: &str,
        vfs: &str,
        dialect: Arc<dyn crate::Dialect>,
    ) -> Result<(Arc<dyn IO>, Arc<Database>)> {
        Database::open_with_vfs(&self.db, path, vfs, dialect)
    }

    pub fn list_vfs(&self) -> Vec<String> {
        #[allow(unused_mut)]
        let mut all_vfs = vec![String::from("memory")];
        #[cfg(feature = "fs")]
        {
            #[cfg(target_family = "unix")]
            {
                all_vfs.push("syscall".to_string());
            }
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            {
                all_vfs.push("io_uring".to_string());
            }
            #[cfg(all(target_os = "windows", feature = "experimental_win_iocp"))]
            {
                all_vfs.push("experimental_win_iocp".to_string());
            }
            all_vfs.extend(crate::ext::list_vfs_modules());
        }
        all_vfs
    }

    pub fn get_auto_commit(&self) -> bool {
        self.auto_commit.load(Ordering::SeqCst)
    }

    /// Mark the active explicit transaction poisoned so COMMIT rolls it back.
    ///
    /// This is used when a write statement under BEGIN is abandoned before it
    /// reaches Halt/Done and that statement did not open a statement savepoint.
    pub(crate) fn mark_tx_poisoned(&self) {
        self.poisoned_tx.store(true, Ordering::SeqCst);
    }

    /// Return whether the active explicit transaction must roll back at COMMIT.
    pub(crate) fn tx_is_poisoned(&self) -> bool {
        self.poisoned_tx.load(Ordering::SeqCst)
    }

    /// Clear the poison marker after BEGIN, COMMIT, or ROLLBACK.
    pub(crate) fn clear_tx_poison(&self) {
        self.poisoned_tx.store(false, Ordering::SeqCst);
    }

    pub fn set_load_extension_enabled(&self, enabled: bool) {
        self.enable_load_extension.store(enabled, Ordering::Release);
    }

    pub(crate) fn can_load_extensions(&self) -> bool {
        self.enable_load_extension.load(Ordering::Acquire)
    }

    pub fn reparse_schema_after_extension_load(self: &Arc<Connection>) -> Result<()> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        // Collect row data from the Statement first, then drop the Statement
        // before taking the schema write lock. This prevents a deadlock in MVCC
        // mode where Statement::drop -> abort -> rollback_tx -> schema.read()
        // would deadlock against the schema write lock.
        let mut rows_data: Vec<(String, String, String, i64, Option<String>)> = Vec::new();
        {
            let mut rows = self
                .query("SELECT * FROM sqlite_schema")?
                .expect("query must be parsed to statement");
            rows.run_with_row_callback(|row| {
                let ty = row.get::<&str>(0)?.to_string();
                let name = row.get::<&str>(1)?.to_string();
                let table_name = row.get::<&str>(2)?.to_string();
                let root_page = row.get::<i64>(3)?;
                let sql = row.get::<&str>(4).ok().map(|s| s.to_string());
                rows_data.push((ty, name, table_name, root_page, sql));
                Ok(())
            })?;
        } // Statement dropped here, before schema write lock

        let syms = self.syms.read();
        self.with_schema_mut(|schema| -> Result<()> {
            // Incremental re-parse after extension loading. The schema already has
            // tables/indices/views from initial parse. We only need to pick up
            // entries that previously failed (e.g. virtual tables whose module
            // wasn't loaded yet). "Already exists" errors are expected and skipped.
            let mut from_sql_indexes = crate::alloc::vec![];
            let mut automatic_indices = HashMap::default();
            let mut dbsp_state_roots = HashMap::default();
            let mut dbsp_state_index_roots = HashMap::default();
            let mut materialized_view_info = HashMap::default();

            let attached_resolver = |name: &str| -> Option<usize> {
                self.attached_databases
                    .read()
                    .get_database_by_name(&crate::util::normalize_ident(name))
                    .map(|(idx, _)| idx)
            };
            for (ty, name, table_name, root_page, sql) in &rows_data {
                match schema.handle_schema_row(
                    ty,
                    name,
                    table_name,
                    *root_page,
                    sql.as_deref(),
                    &syms,
                    &mut from_sql_indexes,
                    &mut automatic_indices,
                    &mut dbsp_state_roots,
                    &mut dbsp_state_index_roots,
                    &mut materialized_view_info,
                    &attached_resolver,
                    self.db.dialect().as_ref(),
                ) {
                    Ok(()) => {}
                    Err(LimboError::ParseError(msg)) if msg.contains("already exists") => {}
                    Err(LimboError::ExtensionError(msg)) => {
                        eprintln!("Warning: {msg}");
                    }
                    Err(e) => return Err(e),
                }
            }

            match schema.populate_indices(&syms, from_sql_indexes, automatic_indices, false) {
                Ok(()) => {}
                Err(LimboError::ParseError(msg)) if msg.contains("already exists") => {}
                Err(LimboError::ExtensionError(msg)) => eprintln!("Warning: {msg}"),
                Err(e) => return Err(e),
            }
            match schema.populate_materialized_views(
                materialized_view_info,
                dbsp_state_roots,
                dbsp_state_index_roots,
            ) {
                Ok(()) => {}
                Err(LimboError::ExtensionError(msg)) => eprintln!("Warning: {msg}"),
                Err(e) => return Err(e),
            }
            Ok(())
        })?
    }

    // Clearly there is something to improve here, Vec<Vec<Value>> isn't a couple of tea
    /// Query the current rows/values of `pragma_name`.
    pub fn pragma_query(self: &Arc<Connection>, pragma_name: &str) -> Result<Vec<Vec<Value>>> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let pragma = format!("PRAGMA {pragma_name}");
        let mut stmt = self.prepare(pragma)?;
        stmt.run_collect_rows()
    }

    /// Set a new value to `pragma_name`.
    ///
    /// Some pragmas will return the updated value which cannot be retrieved
    /// with this method.
    pub fn pragma_update<V: Display>(
        self: &Arc<Connection>,
        pragma_name: &str,
        pragma_value: V,
    ) -> Result<Vec<Vec<Value>>> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let pragma = format!("PRAGMA {pragma_name} = {pragma_value}");
        let mut stmt = self.prepare(pragma)?;
        stmt.run_collect_rows()
    }

    /// The SQL dialect of the database this connection belongs to.
    pub fn dialect(&self) -> Arc<dyn crate::Dialect> {
        self.db.dialect()
    }

    pub fn register_internal_vtab<T>(&self, table: T) -> Result<String>
    where
        T: crate::vtab::InternalVirtualTable + 'static,
    {
        let name = self.db.register_internal_vtab(table)?;
        *self.schema.write() = self.db.clone_schema();
        self.bump_prepare_context_generation();
        Ok(name)
    }

    pub fn register_virtual_table(&self, table: Arc<crate::VirtualTable>) -> Result<String> {
        let name = self.db.register_virtual_table(table)?;
        *self.schema.write() = self.db.clone_schema();
        self.bump_prepare_context_generation();
        Ok(name)
    }

    pub fn current_schema(&self) -> Arc<Schema> {
        self.schema.read().clone()
    }

    pub fn attached_database_names(&self) -> Vec<String> {
        self.attached_databases
            .read()
            .name_to_index
            .keys()
            .cloned()
            .collect()
    }

    pub fn experimental_views_enabled(&self) -> bool {
        self.db.experimental_views_enabled()
    }

    pub fn experimental_index_method_enabled(&self) -> bool {
        self.db.experimental_index_method_enabled()
    }

    pub fn experimental_custom_types_enabled(&self) -> bool {
        self.db.experimental_custom_types_enabled()
    }

    pub fn experimental_attach_enabled(&self) -> bool {
        self.db.experimental_attach_enabled()
    }

    pub fn experimental_vacuum_enabled(&self) -> bool {
        self.db.experimental_vacuum_enabled()
    }

    pub fn experimental_mvcc_passive_checkpoint_enabled(&self) -> bool {
        self.db.experimental_mvcc_passive_checkpoint_enabled()
    }

    pub fn experimental_multiprocess_wal_enabled(&self) -> bool {
        self.db.experimental_multiprocess_wal_enabled()
    }

    pub fn experimental_generated_columns_enabled(&self) -> bool {
        self.db.experimental_generated_columns_enabled()
    }

    pub fn experimental_without_rowid_enabled(&self) -> bool {
        self.db.experimental_without_rowid_enabled()
    }

    pub fn mvcc_enabled(&self) -> bool {
        self.db.mvcc_enabled()
    }

    pub fn mv_store(&self) -> impl Deref<Target = Option<Arc<MvStore>>> {
        struct TransparentWrapper<T>(T);

        impl<T> Deref for TransparentWrapper<T> {
            type Target = T;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        // Never use MV store for bootstrapping - we read state directly from sqlite_schema in the DB file.
        if !self.is_mvcc_bootstrap_connection() {
            either::Left(self.db.get_mv_store())
        } else {
            either::Right(TransparentWrapper(None))
        }
    }

    #[cfg(any(test, injected_yields))]
    pub fn set_yield_injector(&self, injector: Option<Arc<dyn YieldInjector>>) {
        let mut slot = self.yield_injector.write();
        match injector {
            Some(injector) => {
                turso_assert!(
                    slot.is_none(),
                    "yield injector should be empty before installing a new one"
                );
                *slot = Some(injector);
            }
            None => {
                turso_assert!(
                    slot.is_some(),
                    "yield injector should be installed before it is cleared"
                );
                *slot = None;
            }
        }
    }

    #[cfg(any(test, injected_yields))]
    pub(crate) fn yield_injector(&self) -> Option<Arc<dyn YieldInjector>> {
        self.yield_injector.read().clone()
    }

    #[cfg(any(test, injected_yields))]
    pub fn set_failure_injector(&self, injector: Option<Arc<dyn FailureInjector>>) {
        let mut slot = self.failure_injector.write();
        match injector {
            Some(injector) => {
                turso_assert!(
                    slot.is_none(),
                    "failure injector should be empty before installing a new one"
                );
                *slot = Some(injector);
            }
            None => {
                turso_assert!(
                    slot.is_some(),
                    "failure injector should be installed before it is cleared"
                );
                *slot = None;
            }
        }
    }

    #[cfg(any(test, injected_yields))]
    pub(crate) fn failure_injector(&self) -> Option<Arc<dyn FailureInjector>> {
        self.failure_injector.read().clone()
    }

    #[cfg(any(test, injected_yields))]
    #[inline(always)]
    pub(crate) fn next_yield_instance_id(&self) -> u64 {
        self.yield_instance_id_counter
            .fetch_add(1, Ordering::Relaxed)
    }

    /// Query the current value(s) of `pragma_name` associated to
    /// `pragma_value`.
    ///
    /// This method can be used with query-only pragmas which need an argument
    /// (e.g. `table_info('one_tbl')`) or pragmas which returns value(s)
    /// (e.g. `integrity_check`).
    pub fn pragma<V: Display>(
        self: &Arc<Connection>,
        pragma_name: &str,
        pragma_value: V,
    ) -> Result<Vec<Vec<Value>>> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let pragma = format!("PRAGMA {pragma_name}({pragma_value})");
        let mut stmt = self.prepare(pragma)?;
        let mut results = Vec::new();
        loop {
            match stmt.step()? {
                vdbe::StepResult::Row => {
                    let row: Vec<Value> = stmt.row().unwrap().get_values().cloned().collect();
                    results.push(row);
                }
                vdbe::StepResult::Interrupt | vdbe::StepResult::Busy => {
                    return Err(LimboError::Busy);
                }
                _ => break,
            }
        }

        Ok(results)
    }

    #[inline]
    pub fn with_schema_mut<T>(&self, f: impl FnOnce(&mut Schema) -> T) -> Result<T> {
        let mut schema_ref = self.schema.write();
        let schema = Schema::try_make_mut(&mut schema_ref)?;
        Ok(f(schema))
    }

    /// Mutate the schema for a specific database (main or attached).
    pub(crate) fn with_database_schema_mut<T>(
        &self,
        database_id: usize,
        f: impl FnOnce(&mut Schema) -> T,
    ) -> Result<T> {
        match database_id {
            crate::MAIN_DB_ID => self.with_schema_mut(f),
            crate::TEMP_DB_ID => {
                // The temp database is connection-local, no other connection can
                // reference its schema, so we can mutate it directly without cloning
                // into `database_schemas`.
                let temp_db_guard = self.temp.database.read();
                let temp_db = temp_db_guard
                    .as_ref()
                    .expect("temp database should be initialized before schema mutation");
                let mut schema_guard = temp_db.db.schema.lock();
                let schema = Schema::try_make_mut(&mut schema_guard)?;
                let result = f(schema);
                self.bump_prepare_context_generation();
                Ok(result)
            }
            _ => {
                // For attached databases, update a connection-local copy of the schema.
                // We don't update the shared db.schema until after the WAL commit, so
                // other connections won't see uncommitted schema changes (which would
                // cause SchemaUpdated mismatches).
                let mut schemas = self.database_schemas.write();
                let schema_arc = schemas.entry(database_id).or_insert_with(|| {
                    let attached_dbs = self.attached_databases.read();
                    let entry = attached_dbs
                        .index_to_data
                        .get(&database_id)
                        .expect("Database ID should be valid");
                    let schema = entry.db.schema.lock().clone();
                    schema
                });
                let schema = Schema::try_make_mut(schema_arc)?;
                let result = f(schema);
                self.bump_prepare_context_generation();
                Ok(result)
            }
        }
    }

    pub fn is_db_initialized(&self) -> bool {
        self.db.initialized()
    }

    pub(crate) fn get_pager_from_database_index(&self, index: &usize) -> Result<Arc<Pager>> {
        match *index {
            crate::MAIN_DB_ID => Ok(self.pager.load().clone()),
            crate::TEMP_DB_ID => {
                // Lazily initialize the temp database if it hasn't been created yet.
                if self.temp.database.read().is_none() {
                    self.ensure_temp_database()?;
                }
                Ok(self
                    .temp
                    .database
                    .read()
                    .as_ref()
                    .map(|temp_db| temp_db.pager.clone())
                    .expect("temp database should be initialized after ensure_temp_database"))
            }
            _ => self
                .attached_databases
                .read()
                .get_pager_by_index(index)
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "database index {index} is missing from the attached catalog"
                    ))
                }),
        }
    }

    /// Get the database name for a given database index.
    /// Returns "main" for index 0, "temp" for index 1, and the alias for attached databases.
    pub(crate) fn get_database_name_by_index(&self, index: usize) -> Option<String> {
        match index {
            MAIN_DB_ID => Some("main".to_string()),
            TEMP_DB_ID => Some("temp".to_string()),
            _ => self.attached_databases.read().get_name_by_index(index),
        }
    }

    /// Get the database id for a schema name ("main", "temp", or an attached db alias).
    pub(crate) fn get_database_id_by_name(&self, name: &str) -> Result<usize> {
        let normalized: String = crate::util::normalize_ident(name);
        match normalized.as_str() {
            "main" => Ok(MAIN_DB_ID),
            "temp" => Ok(TEMP_DB_ID),
            _ => self
                .attached_databases
                .read()
                .get_database_by_name(&normalized)
                .map(|(idx, _)| idx)
                .ok_or_else(|| LimboError::InvalidArgument(format!("no such database: {name}"))),
        }
    }

    /// Get the Database object for a given database id.
    pub(crate) fn get_source_database(&self, database_id: usize) -> Arc<Database> {
        match database_id {
            MAIN_DB_ID => self.db.clone(),
            TEMP_DB_ID => self
                .temp
                .database
                .read()
                .as_ref()
                .map(|temp_db| temp_db.db.clone())
                .unwrap_or_else(|| self.db.clone()),
            _ => self
                .attached_databases
                .read()
                .get_database_by_index(database_id)
                .expect("database index should be valid"),
        }
    }

    pub(crate) fn is_attached(&self, alias: &str) -> bool {
        self.attached_databases
            .read()
            .name_to_index
            .contains_key(alias)
    }

    /// Returns the reserved-space value inherited from the main connection's pager.
    /// (This reads the main database pager, not the pager of db to be attached)
    fn inherited_reserved_space_for_fresh_attach(&self) -> u8 {
        let pager = self.pager.load();
        pager
            .get_reserved_space()
            .unwrap_or_else(|| pager.io_ctx.read().get_reserved_space_bytes())
    }

    /// Returns the minimum reserved space required by the attached pager's own IO context.
    /// This is used as a floor so inherited or explicit values cannot undercut the attached DB.
    fn minimum_reserved_space_for_fresh_attach(pager: &Pager) -> u8 {
        pager
            .get_reserved_space()
            .unwrap_or(0)
            .max(pager.io_ctx.read().get_reserved_space_bytes())
    }

    fn database_has_existing_wal_state(db: &Database) -> bool {
        let shared_wal = db.shared_wal.read();
        shared_wal.page_size() != 0 || shared_wal.last_checksum_and_max_frame().1 != 0
    }

    fn install_database_wal_on_pager(db: &Arc<Database>, pager: &mut Arc<Pager>) {
        let shared_wal = db.shared_wal.clone();
        let last_checksum_and_max_frame = shared_wal.read().last_checksum_and_max_frame();
        let wal = Arc::new(crate::storage::wal::WalFile::new(
            db.io.clone(),
            shared_wal,
            last_checksum_and_max_frame,
            db.buffer_pool.clone(),
        ));

        let pager = Arc::get_mut(pager)
            .expect("fresh attached pager must not be shared before bootstrap or publication");
        pager.set_wal(wal);
    }

    fn set_mvcc_journal_mode_fresh_db(pager: &Pager) -> Result<()> {
        turso_assert!(!pager.db_initialized());
        pager.set_initial_journal_version(crate::storage::sqlite3_ondisk::Version::Mvcc)
    }

    fn validate_attach_target(db: &Database, is_fresh: bool, alias: &str) -> Result<()> {
        if is_fresh && Self::database_has_existing_wal_state(db) {
            return Err(LimboError::InvalidArgument(format!(
                "cannot attach database '{alias}': main database file is uninitialized but WAL state exists"
            )));
        }

        if is_fresh && db.is_readonly() {
            return Err(LimboError::InvalidArgument(format!(
                "cannot attach database '{alias}': fresh read-only databases cannot be initialized during attach"
            )));
        }
        Ok(())
    }

    fn apply_page_layout_to_fresh_attach_db(
        &self,
        alias: &str,
        attached_db_pager: &Pager,
        reserved_space: Option<u8>,
    ) -> Result<()> {
        let target_page_size = self.get_page_size();
        let attached_min_reserved_space =
            Self::minimum_reserved_space_for_fresh_attach(attached_db_pager);
        let target_reserved_space = match reserved_space {
            Some(space) => {
                // this happens reserved_space is explicitly passed along with encryption or checksum
                if space < attached_min_reserved_space {
                    return Err(LimboError::InvalidArgument(format!(
                        "cannot attach database '{alias}': reserved space {space} is smaller than attached database minimum {attached_min_reserved_space}"
                    )));
                }
                Some(space)
            }
            None => Some(
                self.inherited_reserved_space_for_fresh_attach()
                    .max(attached_min_reserved_space),
            ),
        };

        attached_db_pager.set_initial_page_size(target_page_size)?;
        if let Some(reserved_space) = target_reserved_space {
            attached_db_pager.set_reserved_space_bytes(reserved_space);
        }
        Ok(())
    }

    fn reject_initialized_attach_mismatches(
        &self,
        alias: &str,
        db: &Database,
        pager: &Pager,
    ) -> Result<()> {
        // Reject incompatible journal modes for initialized attached databases:
        // we cannot silently convert the header (the user may have attached read-only).
        if self.mvcc_enabled() != db.mvcc_enabled() {
            let main_mode = if self.mvcc_enabled() { "MVCC" } else { "WAL" };
            let attached_mode = if db.mvcc_enabled() { "MVCC" } else { "WAL" };
            return Err(LimboError::InvalidArgument(format!(
                "cannot attach database '{alias}': main database uses {main_mode} journal mode \
                 but attached database uses {attached_mode}. Both must use the same journal mode."
            )));
        }

        // Reject mismatched page sizes: ephemeral tables and cross-database
        // operations assume a uniform page size across all attached databases.
        let main_pager = self.pager.load();
        if let (Some(main_ps), Some(attached_ps)) =
            (main_pager.get_page_size(), pager.get_page_size())
        {
            if main_ps != attached_ps {
                return Err(LimboError::InvalidArgument(format!(
                    "cannot attach database '{alias}': page size mismatch \
                     (main={main_ps:?}, attached={attached_ps:?})"
                )));
            }
        }

        Ok(())
    }

    fn reject_unsupported_fresh_mvcc_attach_durable_storage(
        &self,
        alias: &str,
        db: &Database,
        attached_is_fresh: bool,
    ) -> Result<()> {
        if attached_is_fresh
            && self.mvcc_enabled()
            && self.db.durable_storage.is_some()
            && db.durable_storage.is_none()
        {
            return Err(LimboError::InvalidArgument(format!(
                "cannot attach database '{alias}': fresh MVCC attach does not support inheriting custom durable storage"
            )));
        }

        Ok(())
    }

    /// Attach a database file with the given alias name
    #[cfg(not(feature = "fs"))]
    pub(crate) fn attach_database(
        &self,
        _path: &str,
        _alias: &str,
        _state: &mut AttachDatabaseState,
    ) -> IOResultOr<()> {
        Err(LimboError::InvalidArgument(
            "attach not available in this build (no-fs)".to_string(),
        ))
    }

    #[cfg(not(feature = "fs"))]
    pub(crate) fn attach_database_with_config(
        &self,
        _path: &str,
        _alias: &str,
        _reserved_space: Option<u8>,
        _state: &mut AttachDatabaseState,
    ) -> IOResultOr<()> {
        // File-backed ATTACH is unavailable without `fs`, so pre-initialization
        // page-layout overrides are also unsupported in this build.
        self.attach_database(_path, _alias, _state)
    }

    /// Attach a database file with the given alias name
    #[cfg(feature = "fs")]
    pub(crate) fn attach_database(
        &self,
        path: &str,
        alias: &str,
        state: &mut AttachDatabaseState,
    ) -> IOResultOr<()> {
        self.attach_database_with_config(path, alias, None, state)
    }

    /// Attach a database file with an optional pre-initialization reserved-space override.
    #[cfg(feature = "fs")]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn attach_database_with_config(
        &self,
        path: &str,
        alias: &str,
        reserved_space: Option<u8>,
        state: &mut AttachDatabaseState,
    ) -> IOResultOr<()> {
        loop {
            match state {
                AttachDatabaseState::Start => {
                    if self.is_closed() {
                        return Err(
                            LimboError::InternalError("Connection closed".to_string()).into()
                        );
                    }

                    if self.is_attached(alias) {
                        return Err(LimboError::InvalidArgument(format!(
                            "database {alias} is already in use"
                        ))
                        .into());
                    }

                    if alias.eq_ignore_ascii_case("main") || alias.eq_ignore_ascii_case("temp") {
                        return Err(LimboError::InvalidArgument(format!(
                            "reserved name {alias} is already in use"
                        ))
                        .into());
                    }
                    if self.pager.load().has_external_page_codec() {
                        return Err(LimboError::InvalidArgument(
                            "ATTACH is unsupported for connections using an external page codec"
                                .to_string(),
                        )
                        .into());
                    }

                    let db_opts = DatabaseOpts::new()
                        .with_views(self.db.experimental_views_enabled())
                        .with_custom_types(self.db.experimental_custom_types_enabled())
                        .with_index_method(self.db.experimental_index_method_enabled())
                        .with_vacuum(self.db.experimental_vacuum_enabled())
                        .with_generated_columns(self.db.experimental_generated_columns_enabled())
                        .with_without_rowid(self.db.experimental_without_rowid_enabled());
                    let is_memory_db = is_memory_like(path);
                    let io: Arc<dyn IO> = if is_memory_db {
                        Arc::new(MemoryIO::new())
                    } else if self.db.is_in_memory_db() {
                        Database::io_for_path(path)?
                    } else {
                        self.db.io.clone()
                    };
                    let main_db_flags = self.db.open_flags;
                    let (db, encryption_opts) = Self::from_uri_attached(
                        path,
                        db_opts,
                        main_db_flags,
                        io,
                        self.db.dialect(),
                    )?;
                    let attached_is_fresh = !db.initialized();
                    if !is_memory_db {
                        Self::validate_attach_target(&db, attached_is_fresh, alias)?;
                    }
                    self.reject_unsupported_fresh_mvcc_attach_durable_storage(
                        alias,
                        &db,
                        attached_is_fresh,
                    )?;

                    let encryption_key = if let Some(ref enc) = encryption_opts {
                        Some(EncryptionKey::from_hex_string(&enc.hexkey)?)
                    } else {
                        None
                    };

                    *state = AttachDatabaseState::Init(Box::new(AttachDatabaseInitState {
                        alias: alias.to_string(),
                        reserved_space,
                        db,
                        attached_is_fresh,
                        encryption_key,
                        init_st: crate::InitState::default(),
                    }));
                }
                AttachDatabaseState::Init(init) => {
                    let mut pager = Arc::new(crate::return_if_io!(init.db._init_nonblock(
                        &mut init.init_st,
                        init.encryption_key.as_ref(),
                        None,
                    )));

                    if !init.attached_is_fresh {
                        self.reject_initialized_attach_mismatches(&init.alias, &init.db, &pager)?;
                        *state = AttachDatabaseState::Publish {
                            alias: init.alias.clone(),
                            db: init.db.clone(),
                            pager,
                        };
                        continue;
                    }

                    self.apply_page_layout_to_fresh_attach_db(
                        &init.alias,
                        &pager,
                        init.reserved_space,
                    )?;

                    if self.mvcc_enabled() && !init.db.mvcc_enabled() {
                        Self::set_mvcc_journal_mode_fresh_db(&pager)?;
                        Self::install_database_wal_on_pager(&init.db, &mut pager);
                        let enc_ctx = pager.io_ctx.read().encryption_context().cloned();
                        let mv_store = journal_mode::open_mv_store(
                            init.db.io.clone(),
                            &init.db.path,
                            init.db.open_flags,
                            init.db.durable_storage.clone(),
                            enc_ctx,
                            init.db.mv_store_allocator.clone(),
                            init.db.experimental_mvcc_passive_checkpoint_enabled(),
                        )?;
                        init.db.mv_store.store(Some(mv_store));
                        *state = AttachDatabaseState::Bootstrap(Box::new(
                            AttachDatabaseBootstrapState {
                                alias: init.alias.clone(),
                                db: init.db.clone(),
                                pager,
                                encryption_key: init.encryption_key.take(),
                                bootstrap_conn: None,
                                bootstrap_st: crate::mvcc::database::BootstrapState::default(),
                            },
                        ));
                    } else {
                        *state = AttachDatabaseState::Publish {
                            alias: init.alias.clone(),
                            db: init.db.clone(),
                            pager,
                        };
                    }
                }
                AttachDatabaseState::Bootstrap(bootstrap) => {
                    if bootstrap.bootstrap_conn.is_none() {
                        let default_cache_size = match bootstrap
                            .pager
                            .with_header(|header| header.default_page_cache_size)
                        {
                            Ok(IOResult::Done(default_cache_size)) => default_cache_size.get(),
                            Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
                            Err(_) => 0,
                        };
                        bootstrap.bootstrap_conn =
                            Some(bootstrap.db._connect_with_pager_and_default_cache_size(
                                true,
                                bootstrap.pager.clone(),
                                bootstrap.encryption_key.take(),
                                default_cache_size,
                            )?);
                    }

                    let mv_store_guard = bootstrap.db.get_mv_store();
                    let Some(mv_store) = mv_store_guard.as_ref() else {
                        return Err(LimboError::InternalError(
                            "fresh MVCC attach missing MV store".to_string(),
                        )
                        .into());
                    };
                    crate::return_if_io!(mv_store.bootstrap_nonblock(
                        bootstrap
                            .bootstrap_conn
                            .as_ref()
                            .expect("bootstrap connection initialized above"),
                        &mut bootstrap.bootstrap_st,
                    ));

                    *state = AttachDatabaseState::Publish {
                        alias: bootstrap.alias.clone(),
                        db: bootstrap.db.clone(),
                        pager: bootstrap.pager.clone(),
                    };
                }
                AttachDatabaseState::Publish { alias, db, pager } => {
                    self.attached_databases.write().insert(
                        alias.as_str(),
                        db.clone(),
                        pager.clone(),
                    );
                    self.bump_prepare_context_generation();
                    *state = AttachDatabaseState::Done;
                    return Ok(IOResult::Done(()));
                }
                AttachDatabaseState::Done => {
                    return Err(LimboError::InternalError(
                        "attach_database called after completion".to_string(),
                    )
                    .into());
                }
            }
        }
    }

    // Detach a database by alias name
    pub(crate) fn detach_database(&self, alias: &str) -> Result<()> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }

        if alias == "main" || alias == "temp" {
            return Err(LimboError::InvalidArgument(format!(
                "cannot detach database: {alias}"
            )));
        }

        // Look up the database index first, then rollback any MVCC transaction
        // *before* removing the database from the catalog.  mv_store_for_db
        // and get_pager_from_database_index read `attached_databases`, so we
        // must not hold the write lock during the rollback.
        let database_id = {
            let attached_dbs = self.attached_databases.read();
            match attached_dbs.name_to_index.get(alias).copied() {
                Some(id) => id,
                None => {
                    return Err(LimboError::InvalidArgument(format!(
                        "no such database: {alias}"
                    )));
                }
            }
        };

        // Rollback any active transaction on this database before detaching.
        // After the Database is removed from the catalog, the MvStore / Pager
        // become unreachable and the transaction would leak forever.
        let pager = self
            .get_pager_from_database_index(&database_id)
            .expect("attached database should always have a pager");

        if pager.holds_read_lock() || pager.holds_write_lock() {
            return Err(LimboError::InvalidArgument(format!(
                "database {alias} is locked"
            )));
        }

        if let Some((tx_id, _mode)) = self.get_mv_tx_for_db(database_id) {
            if let Some(mv_store) = self.mv_store_for_db(database_id) {
                mv_store.rollback_tx(tx_id, pager.clone(), self, database_id);
                pager.end_read_tx();
            }
            self.set_mv_tx_for_db(database_id, None);
        } else {
            // Non-MVCC attached DB (e.g. :memory:) — rollback WAL state.
            pager.rollback_attached();
        }

        // Remove from catalog. The write lock must be released before
        // acquiring database_schemas.write() to maintain consistent lock
        // ordering (attached_databases before database_schemas).
        {
            let mut attached_dbs = self.attached_databases.write();
            attached_dbs.remove(alias);
        }

        // Invalidate the cached schema for this database index so that a future
        // ATTACH reusing the same index won't see stale schema entries.
        self.database_schemas.write().remove(&database_id);
        self.bump_prepare_context_generation();

        Ok(())
    }

    /// List all attached database aliases
    pub fn list_attached_databases(&self) -> Vec<String> {
        self.attached_databases
            .read()
            .name_to_index
            .keys()
            .cloned()
            .collect()
    }

    /// Invoke `f` with a slice of all non-main database (index, pager) pairs
    /// (temp + attached).The internal locks are released before `f` runs, which also
    /// makes it safe for `f` to call back into the connection (e.g. `mv_store_for_db`,
    /// which re-reads the attached-database catalog).
    pub(crate) fn with_all_attached_pagers_with_index<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&[(usize, Arc<Pager>)]) -> R,
    {
        let mut pagers: SmallVec<[(usize, Arc<Pager>); 8]> = SmallVec::new();
        if let Some(temp_db) = self.temp.database.read().as_ref() {
            pagers.push((crate::TEMP_DB_ID, temp_db.pager.clone()));
        }
        {
            let catalog = self.attached_databases.read();
            for (&idx, entry) in catalog.index_to_data.iter() {
                pagers.push((idx, entry.pager.clone()));
            }
        }
        f(&pagers)
    }

    pub(crate) fn database_schemas(&self) -> &RwLock<HashMap<usize, Arc<Schema>>> {
        &self.database_schemas
    }

    fn cached_non_main_schema(&self, database_id: usize) -> Arc<Schema> {
        turso_assert_ne!(database_id, crate::MAIN_DB_ID);
        // TEMP is the sole source-of-truth path: writes go directly to
        // `temp_db.db.schema` (see `with_database_schema_mut`), so skip
        // `database_schemas` entirely to avoid stale reads.
        if database_id == crate::TEMP_DB_ID {
            return self
                .temp
                .database
                .read()
                .as_ref()
                .map(|temp_db| temp_db.db.schema.lock().clone())
                .unwrap_or_else(|| self.empty_temp_schema());
        }
        if let Some(schema) = self.database_schemas.read().get(&database_id).cloned() {
            return schema;
        }

        let attached_dbs = self.attached_databases.read();
        let entry = attached_dbs
            .index_to_data
            .get(&database_id)
            .expect("Database ID should be valid after resolve_database_id");
        let schema = entry.db.schema.lock().clone();
        schema
    }

    /// Publish a connection-local non-main schema after commit.
    ///
    /// TEMP is not staged in `database_schemas` — writes go directly to
    /// `temp_db.db.schema` via `with_database_schema_mut`, so there is
    /// nothing to publish here. Attached databases still stage mutations
    /// in `database_schemas` so other connections don't see uncommitted
    /// DDL; those get published to the shared `db.schema` on commit.
    pub(crate) fn publish_database_schema(&self, database_id: usize) {
        if database_id == crate::TEMP_DB_ID {
            return;
        }
        let mut schemas = self.database_schemas.write();
        if let Some(local_schema) = schemas.remove(&database_id) {
            let attached_dbs = self.attached_databases.read();
            if let Some(entry) = attached_dbs.index_to_data.get(&database_id) {
                *entry.db.schema.lock() = local_schema;
            }
            self.bump_prepare_context_generation();
        }
    }

    pub(crate) fn attached_databases(&self) -> &RwLock<DatabaseCatalog> {
        &self.attached_databases
    }

    /// Access schema for a database using a closure pattern to avoid cloning
    pub(crate) fn with_schema<T>(&self, database_id: usize, f: impl FnOnce(&Schema) -> T) -> T {
        match database_id {
            crate::MAIN_DB_ID => {
                let schema = self.schema.read();
                f(&schema)
            }
            _ => {
                let schema = self.cached_non_main_schema(database_id);
                f(&schema)
            }
        }
    }

    /// Clone the shared schema for `database_id`, bypassing the connection-local cache.
    ///
    /// Panics if `database_id` is neither main nor an attached database.
    pub(crate) fn clone_shared_schema(&self, database_id: usize) -> Arc<Schema> {
        if database_id == crate::MAIN_DB_ID {
            self.db.clone_schema()
        } else {
            let attached_databases = self.attached_databases.read();
            let entry = attached_databases
                .index_to_data
                .get(&database_id)
                .expect("shared schema requested for unknown attached database");
            entry.db.clone_schema()
        }
    }

    // Get the canonical path for a database given its Database object
    fn get_canonical_path_for_database(db: &Database) -> String {
        if db.is_in_memory_db() {
            // For in-memory databases, SQLite shows empty string
            String::new()
        } else {
            // For file databases, try to show the full absolute path if that doesn't fail
            match std::fs::canonicalize(&db.path) {
                Ok(abs_path) => abs_path.to_string_lossy().to_string(),
                Err(_) => db.path.to_string(),
            }
        }
    }

    /// List all databases (main + attached) with their sequence numbers, names, and file paths
    /// Returns a vector of tuples: (seq_number, name, file_path)
    pub fn list_all_databases(&self) -> Vec<(usize, String, String)> {
        let mut databases = Vec::new();

        // Add main database (always seq=0, name="main")
        let main_path = Self::get_canonical_path_for_database(&self.db);
        databases.push((MAIN_DB_ID, "main".to_string(), main_path));

        // SQLite only exposes the temp schema in database_list after it has
        // been initialized, and reports an empty path rather than the backing
        // temp filename.
        if self.temp.database.read().is_some() {
            databases.push((crate::TEMP_DB_ID, "temp".to_string(), String::new()));
        }

        // Add attached databases
        let attached_dbs = self.attached_databases.read();
        for (alias, &seq_number) in attached_dbs.name_to_index.iter() {
            let file_path = if let Some(entry) = attached_dbs.index_to_data.get(&seq_number) {
                Self::get_canonical_path_for_database(&entry.db)
            } else {
                String::new()
            };
            databases.push((seq_number, alias.clone(), file_path));
        }

        // Sort by sequence number to ensure consistent ordering
        databases.sort_by_key(|&(seq, _, _)| seq);
        databases
    }

    pub fn get_pager(&self) -> Arc<Pager> {
        self.pager.load().clone()
    }

    pub fn get_query_only(&self) -> bool {
        self.is_query_only()
    }

    pub fn set_query_only(&self, value: bool) {
        self.query_only.store(value, Ordering::SeqCst);
        self.bump_prepare_context_generation();
    }

    pub fn set_vdbe_trace(&self, value: bool) {
        self.vdbe_trace.store(value, Ordering::SeqCst);
    }

    pub fn get_vdbe_trace(&self) -> bool {
        self.vdbe_trace.load(Ordering::SeqCst)
    }

    pub fn get_dml_require_where(&self) -> bool {
        self.dml_require_where.load(Ordering::SeqCst)
    }

    pub fn set_dml_require_where(&self, value: bool) {
        self.dml_require_where.store(value, Ordering::SeqCst);
    }

    pub fn get_count_changes(&self) -> bool {
        self.count_changes.load(Ordering::SeqCst)
    }

    pub fn set_count_changes(&self, value: bool) {
        self.count_changes.store(value, Ordering::SeqCst);
    }

    pub fn get_dqs_dml(&self) -> bool {
        self.dqs_dml.load(Ordering::SeqCst)
    }

    pub fn set_dqs_dml(&self, value: bool) {
        self.dqs_dml.store(value, Ordering::SeqCst);
        self.bump_prepare_context_generation();
    }

    pub fn get_full_column_names(&self) -> bool {
        self.full_column_names.load(Ordering::SeqCst)
    }

    pub fn set_full_column_names(&self, value: bool) {
        self.full_column_names.store(value, Ordering::SeqCst);
        self.bump_prepare_context_generation();
    }

    pub fn get_short_column_names(&self) -> bool {
        self.short_column_names.load(Ordering::SeqCst)
    }

    pub fn set_short_column_names(&self, value: bool) {
        self.short_column_names.store(value, Ordering::SeqCst);
        self.bump_prepare_context_generation();
    }

    pub fn get_sync_mode(&self) -> SyncMode {
        self.sync_mode.get()
    }

    pub fn set_sync_mode(&self, mode: SyncMode) {
        self.sync_mode.set(mode);
        self.bump_prepare_context_generation();
    }

    pub(crate) fn get_sync_mode_for_database(&self, database_id: usize) -> Result<SyncMode> {
        match database_id {
            MAIN_DB_ID => Ok(self.get_sync_mode()),
            TEMP_DB_ID => Ok(SyncMode::Off),
            _ => self
                .attached_databases
                .read()
                .index_to_data
                .get(&database_id)
                .map(|entry| entry.sync_mode)
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "database index {database_id} is missing from the attached catalog"
                    ))
                }),
        }
    }

    pub(crate) fn set_sync_mode_for_database(
        &self,
        database_id: usize,
        mode: SyncMode,
    ) -> Result<()> {
        match database_id {
            MAIN_DB_ID => self.sync_mode.set(mode),
            // SQLite fixes temp databases at synchronous=OFF.
            TEMP_DB_ID => return Ok(()),
            _ => {
                let mut attached_databases = self.attached_databases.write();
                let entry = attached_databases
                    .index_to_data
                    .get_mut(&database_id)
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "database index {database_id} is missing from the attached catalog"
                        ))
                    })?;
                entry.sync_mode = mode;
            }
        }
        self.bump_prepare_context_generation();
        Ok(())
    }

    pub fn get_temp_store(&self) -> crate::TempStore {
        self.temp_store.get()
    }

    pub fn set_temp_store(&self, value: crate::TempStore) {
        if self.temp_store.get() == value {
            return;
        }
        self.reset_temp_database();
        self.temp_store.set(value);
        self.bump_prepare_context_generation();
    }

    /// Find a sequence by name, supporting optional schema qualification.
    ///
    /// - `"my_seq"` → searches main database only
    /// - `"aux.my_seq"` → searches the attached database named `aux`
    pub fn find_sequence(&self, name: &str) -> Result<Arc<crate::schema::Sequence>> {
        let (db_id, seq_name) = if let Some((schema, seq)) = name.split_once('.') {
            let db_id = self.get_database_id_by_name(schema)?;
            (db_id, crate::util::normalize_ident(seq))
        } else {
            (MAIN_DB_ID, crate::util::normalize_ident(name))
        };

        self.with_schema(db_id, |schema| {
            schema.get_sequence(&seq_name).map(Arc::clone)
        })
        .ok_or_else(|| LimboError::ParseError(format!("sequence \"{name}\" does not exist")))
    }

    /// Record that this connection has seen a value from the named sequence (for currval).
    pub fn set_sequence_currval(&self, name: &str, value: i64) {
        let normalized = crate::util::normalize_ident(name);
        self.sequence_currvals.write().insert(normalized, value);
    }

    /// Get the last value returned by nextval/setval for the named sequence on this connection.
    pub fn get_sequence_currval(&self, name: &str) -> Option<i64> {
        let normalized = crate::util::normalize_ident(name);
        self.sequence_currvals.read().get(&normalized).copied()
    }

    /// Drop this connection's currval entry for a sequence. Called on DROP
    /// SEQUENCE (and implicit drops via DROP TABLE on AUTOINCREMENT) so that
    /// a subsequent `CREATE SEQUENCE <same-name>` does not silently inherit
    /// the stale per-session currval from the prior sequence — `currval()`
    /// on the fresh sequence must error with "not yet defined in this
    /// session" until a nextval/setval establishes it.
    pub fn clear_sequence_currval(&self, name: &str) {
        let normalized = crate::util::normalize_ident(name);
        self.sequence_currvals.write().remove(&normalized);
    }

    /// Total times this connection's autonomous sequence inner-tx ran into
    /// a transient conflict (`WriteWriteConflict` / `BusySnapshot` /
    /// `Conflict(_)`) and was retried by `op_sequence_commit_inner_tx`.
    /// A non-CYCLE nextval on a non-contended seq must keep this at zero —
    /// the regression test for "no inline backing-table compaction"
    /// asserts the delta is 0 across the concurrent-nextval scenario.
    pub fn sequence_inner_retries(&self) -> u64 {
        self.sequence_inner_retries
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Reset the inner-tx retry counter. Test-only helper so a setup
    /// phase (priming the backing table, etc.) doesn't pollute the
    /// counter the assertion phase observes.
    #[doc(hidden)]
    pub fn reset_sequence_inner_retries(&self) {
        self.sequence_inner_retries
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// Bootstrap-time sequence descriptor loader. Used by MVCC bootstrap
    /// after log recovery: walks `__turso_internal_seq_*` tables and registers
    /// a pure descriptor for each into the active schema. No atomic state is
    /// seeded — the runtime watermark is always read from disk by
    /// nextval/setval.
    ///
    /// Non-blocking: driven by the bootstrap state machine via `return_if_io!`,
    /// so the per-backing-table descriptor read yields IO rather than pumping
    /// `io.step()`. Re-entrant — the worklist and in-flight read live in
    /// `state`.
    pub(crate) fn load_sequence_descriptors_via_sql_nonblock(
        self: &Arc<Connection>,
        state: &mut LoadSequenceDescriptorsState,
    ) -> crate::types::IOResultOr<()> {
        use crate::types::IOResult;
        loop {
            match state {
                LoadSequenceDescriptorsState::Start => {
                    // Walk schema.tables in-memory rather than issuing a SELECT
                    // against sqlite_master — avoids the side-effects of running
                    // a fresh statement here, which can leave the connection's
                    // mv_tx in a non-exclusive state and cause the next DDL to
                    // trip the exclusive-tx guard in op_open_write.
                    let pending =
                        self.with_schema(MAIN_DB_ID, |s| s.sequence_backing_table_names());
                    *state = LoadSequenceDescriptorsState::Reading {
                        pending,
                        idx: 0,
                        stmt: None,
                        meta: None,
                        seq: None,
                        watermark_stmt: None,
                        watermark_row: None,
                    };
                }
                LoadSequenceDescriptorsState::Reading {
                    pending,
                    idx,
                    stmt,
                    meta,
                    seq,
                    watermark_stmt,
                    watermark_row,
                } => loop {
                    let entry = {
                        if *idx >= pending.len() {
                            return Ok(IOResult::Done(()));
                        }
                        pending[*idx].clone()
                    };
                    let (backing_table_name, seq_name) = entry;
                    let normalized = crate::util::normalize_ident(&seq_name);
                    let already_present =
                        self.with_schema(MAIN_DB_ID, |s| s.get_sequence(&normalized).is_some());
                    if already_present {
                        *idx += 1;
                        *stmt = None;
                        *meta = None;
                        *seq = None;
                        *watermark_stmt = None;
                        *watermark_row = None;
                        continue;
                    }
                    if seq.is_none() {
                        crate::return_if_io!(self.read_seq_descriptor_row_nonblock(
                            &backing_table_name,
                            &seq_name,
                            stmt,
                            meta,
                        ));
                        *seq = Some(Self::sequence_from_descriptor_meta(
                            &seq_name,
                            &backing_table_name,
                            *meta,
                        )?);
                        *stmt = None;
                        *meta = None;
                    }
                    let sequence = seq.as_ref().expect("sequence set above");
                    crate::return_if_io!(self.read_sequence_watermark_row_nonblock(
                        &backing_table_name,
                        sequence,
                        watermark_stmt,
                        watermark_row,
                    ));
                    let watermark = Self::sequence_watermark_from_row(
                        &backing_table_name,
                        sequence,
                        *watermark_row,
                    )?;
                    if let Some(mv_store) = self.db.get_mv_store().as_ref() {
                        mv_store.set_sequence_watermark(&normalized, watermark);
                    }
                    let sequence = seq.take().expect("sequence set above");
                    self.with_database_schema_mut(MAIN_DB_ID, |schema| {
                        schema
                            .sequences
                            .insert(normalized.clone(), Arc::new(sequence));
                    })?;
                    *idx += 1;
                    *stmt = None;
                    *meta = None;
                    *watermark_stmt = None;
                    *watermark_row = None;
                },
            }
        }
    }

    /// Drive one backing-table descriptor read to completion (re-entrant).
    /// Lazily prepares the `SELECT` into `*stmt`, then runs it non-blocking,
    /// stashing the captured row in `*meta`. The backing table is internal
    /// (`__turso_internal_seq_*`); a prepare/read failure is on-disk
    /// corruption, not "the sequence doesn't exist", so it surfaces
    /// `LimboError::Corrupt` — silently dropping the sequence would manifest
    /// as a misleading "sequence does not exist" error on the next nextval
    /// that masks the real problem.
    fn read_seq_descriptor_row_nonblock(
        self: &Arc<Connection>,
        backing_table_name: &str,
        seq_name: &str,
        stmt: &mut Option<Box<Statement>>,
        meta: &mut Option<(i64, i64, i64, i64, bool)>,
    ) -> crate::types::IOResultOr<()> {
        use crate::types::IOResult;
        if stmt.is_none() {
            let escaped = backing_table_name.replace('"', "\"\"");
            let sql = format!("SELECT start, inc, min, max, cycle FROM \"{escaped}\" LIMIT 1");
            let prepared = self.prepare_internal(sql).map_err(|err| {
                LimboError::Corrupt(format!(
                    "internal sequence backing table \"{backing_table_name}\" for sequence \
                     \"{seq_name}\": cannot prepare descriptor SELECT: {err}"
                ))
            })?;
            *stmt = Some(Box::new(prepared));
            // Fresh statement → clear any descriptor captured for a prior backing
            // table, so an empty backing table is detected as missing-row
            // corruption rather than silently reusing the previous descriptor.
            *meta = None;
        }
        let s = stmt.as_mut().expect("stmt set above");
        match s.run_with_row_callback_nonblock(|row| {
            *meta = Some((
                row.get::<i64>(0)?,
                row.get::<i64>(1)?,
                row.get::<i64>(2)?,
                row.get::<i64>(3)?,
                row.get::<i64>(4)? != 0,
            ));
            Ok(())
        }) {
            Ok(IOResult::IO(io)) => Ok(IOResult::IO(io)),
            Ok(IOResult::Done(())) => Ok(IOResult::Done(())),
            Err(err) => Err(LimboError::Corrupt(format!(
                "internal sequence backing table \"{backing_table_name}\" for sequence \
                 \"{seq_name}\": descriptor row read failed: {err}"
            ))
            .into()),
        }
    }

    /// Build a `Sequence` from a descriptor row captured by
    /// [`Self::read_seq_descriptor_row_nonblock`]. An absent/invalid descriptor
    /// is on-disk corruption (see that method's doc).
    fn sequence_from_descriptor_meta(
        seq_name: &str,
        backing_table_name: &str,
        meta: Option<(i64, i64, i64, i64, bool)>,
    ) -> Result<crate::schema::Sequence> {
        let (start, inc, min, max, cycle) = meta.ok_or_else(|| {
            LimboError::Corrupt(format!(
                "internal sequence backing table \"{backing_table_name}\" for sequence \
                 \"{seq_name}\" is empty; the descriptor metadata row must always be present"
            ))
        })?;
        crate::schema::Sequence::new(
            seq_name.to_string(),
            Some(start),
            Some(inc),
            Some(min),
            Some(max),
            cycle,
        )
        .map_err(|err| {
            LimboError::Corrupt(format!(
                "internal sequence backing table \"{backing_table_name}\" for sequence \
                 \"{seq_name}\" descriptor is invalid: {err}"
            ))
        })
    }

    /// Drive one backing-table watermark read to completion (re-entrant).
    ///
    /// The returned row is converted by [`Self::sequence_watermark_from_row`]
    /// into the exclusive upper bound used by `sequence_watermark_experimental()`.
    fn read_sequence_watermark_row_nonblock(
        self: &Arc<Connection>,
        backing_table_name: &str,
        seq: &crate::schema::Sequence,
        stmt: &mut Option<Box<Statement>>,
        row: &mut Option<(i64, bool)>,
    ) -> crate::types::IOResultOr<()> {
        use crate::types::IOResult;
        if stmt.is_none() {
            let escaped = backing_table_name.replace('"', "\"\"");
            let direction = if seq.increment_by >= 0 { "DESC" } else { "ASC" };
            let sql = format!(
                "SELECT value, is_called FROM \"{escaped}\" ORDER BY value {direction} LIMIT 1"
            );
            let prepared = self.prepare_internal(sql).map_err(|err| {
                LimboError::Corrupt(format!(
                    "internal sequence backing table \"{backing_table_name}\" for sequence \
                     \"{}\": cannot prepare watermark SELECT: {err}",
                    seq.name
                ))
            })?;
            *stmt = Some(Box::new(prepared));
            *row = None;
        }
        let s = stmt.as_mut().expect("stmt set above");
        match s.run_with_row_callback_nonblock(|r| {
            let value = r.get::<i64>(0)?;
            let is_called = r.get::<i64>(1)? != 0;
            *row = Some((value, is_called));
            Ok(())
        }) {
            Ok(IOResult::IO(io)) => Ok(IOResult::IO(io)),
            Ok(IOResult::Done(())) => Ok(IOResult::Done(())),
            Err(err) => Err(LimboError::Corrupt(format!(
                "internal sequence backing table \"{backing_table_name}\" for sequence \
                 \"{}\": watermark row read failed: {err}",
                seq.name
            ))
            .into()),
        }
    }

    fn sequence_watermark_from_row(
        backing_table_name: &str,
        seq: &crate::schema::Sequence,
        row: Option<(i64, bool)>,
    ) -> Result<i64> {
        let (value, is_called) = row.ok_or_else(|| {
            LimboError::Corrupt(format!(
                "internal sequence backing table \"{backing_table_name}\" for sequence \
                 \"{}\" is empty; cannot derive sequence watermark",
                seq.name
            ))
        })?;
        Ok(crate::mvcc::database::first_unsafe_sequence_watermark(
            seq, value, is_called,
        ))
    }

    /// Sync AUTOINCREMENT backing-table watermarks from `sqlite_sequence`.
    ///
    /// Covers the WAL→MVCC mode-switch compatibility path: a WAL-mode
    /// database with AUTOINCREMENT tables tracks the high-water mark in
    /// `sqlite_sequence` (legacy SQLite contract) and never writes to
    /// the backing table created by CREATE TABLE bytecode. The MVCC
    /// AUTOINCREMENT path reads the backing table to compute the next
    /// rowid, so without a sync step the next INSERT would regress to
    /// start_value and collide with the existing rowid.
    ///
    /// For each `name` in `sqlite_sequence`, locate the backing table
    /// `__turso_internal_seq___turso_internal_autoincrement_<name>` and,
    /// if its current MAX(value) is below the sqlite_sequence value,
    /// INSERT a new watermark row to advance it. This is the same
    /// pattern the translator emits for `emit_disk_advance_past`,
    /// expressed as statement-level SQL so it can run at bootstrap.
    ///
    /// Tables whose backing table is missing are skipped — that
    /// indicates the table was never an AUTOINCREMENT under Turso's
    /// CREATE TABLE bytecode (i.e. it predates this engine touching
    /// the DB), and synthesising a backing table here would forge data
    /// the user did not author. Importing a foreign SQLite database is
    /// out of scope for this helper.
    ///
    /// Non-blocking: driven by the bootstrap state machine via `return_if_io!`.
    /// Each step is on the correctness path documented above — a silent failure
    /// leaves MVCC AUTOINCREMENT able to re-emit a rowid already in use after a
    /// WAL→MVCC mode switch, so errors propagate to fail the open rather than
    /// continue into a state where the next INSERT NULL collides on disk.
    pub(crate) fn sync_autoincrement_backing_tables_from_sqlite_sequence_nonblock(
        self: &Arc<Connection>,
        state: &mut SyncAutoincrementState,
    ) -> crate::types::IOResultOr<()> {
        use crate::schema::{autoincrement_sequence_name, SQLITE_SEQUENCE_TABLE_NAME};
        use crate::translate::sequence::sequence_backing_table_name;
        use crate::types::IOResult;

        loop {
            match state {
                SyncAutoincrementState::Start => {
                    let has_seq_table = self.with_schema(MAIN_DB_ID, |s| {
                        s.get_btree_table(SQLITE_SEQUENCE_TABLE_NAME).is_some()
                    });
                    if !has_seq_table {
                        return Ok(IOResult::Done(()));
                    }
                    let stmt = self.prepare_internal(format!(
                        "SELECT name, seq FROM {SQLITE_SEQUENCE_TABLE_NAME}"
                    ))?;
                    *state = SyncAutoincrementState::ReadSeqRows {
                        stmt: Box::new(stmt),
                        rows: Vec::new(),
                    };
                }
                SyncAutoincrementState::ReadSeqRows { stmt, rows } => {
                    crate::return_if_io!(stmt.run_with_row_callback_nonblock(|row| {
                        let name = row.get::<&str>(0)?.to_string();
                        let seq = row.get::<i64>(1)?;
                        rows.push((name, seq));
                        Ok(())
                    }));
                    let rows = std::mem::take(rows);
                    *state = SyncAutoincrementState::Process {
                        rows,
                        idx: 0,
                        sub: SyncRowStep::Start,
                    };
                }
                SyncAutoincrementState::Process { rows, idx, sub } => {
                    if *idx >= rows.len() {
                        return Ok(IOResult::Done(()));
                    }
                    match sub {
                        SyncRowStep::Start => {
                            let backing_table_name = sequence_backing_table_name(
                                &autoincrement_sequence_name(&rows[*idx].0),
                            );
                            let has_backing = self.with_schema(MAIN_DB_ID, |s| {
                                s.get_btree_table(&backing_table_name).is_some()
                            });
                            if !has_backing {
                                *idx += 1;
                                continue;
                            }
                            // Read current backing watermark; only upsert if we'd
                            // actually advance it (avoids needless writes on boot).
                            let escaped = backing_table_name.replace('"', "\"\"");
                            let stmt = self.prepare_internal(format!(
                                "SELECT MAX(value) FROM \"{escaped}\""
                            ))?;
                            *sub = SyncRowStep::ReadMax {
                                backing_table_name,
                                stmt: Box::new(stmt),
                                current_max: None,
                            };
                        }
                        SyncRowStep::ReadMax {
                            backing_table_name,
                            stmt,
                            current_max,
                        } => {
                            crate::return_if_io!(stmt.run_with_row_callback_nonblock(|row| {
                                if let crate::Value::Numeric(crate::Numeric::Integer(v)) =
                                    row.get_value(0)
                                {
                                    *current_max = Some(*v);
                                }
                                Ok(())
                            }));
                            let watermark = rows[*idx].1;
                            // Skip only when the backing table is already strictly
                            // ahead; an equal value is NOT enough because the
                            // initial row written by CREATE TABLE bytecode is
                            // (value=1, is_called=false), which would cause the
                            // next nextval to re-emit value=1 and collide with the
                            // rowid already inserted in WAL mode. We always upsert
                            // with is_called=1 so the next nextval computes
                            // watermark+1 like sqlite_sequence semantics demand.
                            if matches!(*current_max, Some(c) if c > watermark) {
                                *idx += 1;
                                *sub = SyncRowStep::Start;
                                continue;
                            }
                            // Standard AUTOINCREMENT descriptor columns (start=1,
                            // inc=1, min=1, max=i64::MAX, cycle=0) — mirror what
                            // the translator emits when CREATE TABLE bytecode
                            // creates the backing table for an AUTOINCREMENT column.
                            let escaped = backing_table_name.replace('"', "\"\"");
                            let insert_sql = format!(
                                "INSERT OR REPLACE INTO \"{escaped}\"\
                                 (value, is_called, start, inc, min, max, cycle) \
                                 VALUES ({watermark}, 1, 1, 1, 1, {}, 0)",
                                i64::MAX
                            );
                            let stmt = self.prepare_internal(insert_sql)?;
                            *sub = SyncRowStep::Upsert {
                                stmt: Box::new(stmt),
                            };
                        }
                        SyncRowStep::Upsert { stmt } => {
                            crate::return_if_io!(stmt.run_with_row_callback_nonblock(|_| Ok(())));
                            if let Some(mv_store) = self.db.get_mv_store().as_ref() {
                                let watermark = rows[*idx].1;
                                let first_unsafe = watermark.checked_add(1).unwrap_or(watermark);
                                mv_store.set_sequence_watermark(
                                    &autoincrement_sequence_name(&rows[*idx].0),
                                    first_unsafe,
                                );
                            }
                            *idx += 1;
                            *sub = SyncRowStep::Start;
                        }
                    }
                }
            }
        }
    }

    #[doc(hidden)]
    pub fn db_file_path(&self) -> &str {
        &self.db.path
    }

    /// Create a `TempDir` honoring `TURSO_TMPDIR` and `SQLITE_TMPDIR`,
    /// falling back to the OS default (`env::temp_dir()`).
    ///
    /// `&self` is reserved for a future per-connection
    /// `temp_store_directory` setting (e.g. `PRAGMA temp_store_directory`)
    /// so call sites don't need to change when that lands.
    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn create_tempdir(&self) -> Result<TempDir> {
        let res = if let Some(d) = std::env::var_os("TURSO_TMPDIR") {
            tempfile::tempdir_in(d)
        } else if let Some(d) = std::env::var_os("SQLITE_TMPDIR") {
            tempfile::tempdir_in(d)
        } else {
            tempfile::tempdir()
        };
        res.map_err(|e| io_error(e, "tempdir"))
    }

    pub fn get_data_sync_retry(&self) -> bool {
        self.data_sync_retry
            .load(crate::sync::atomic::Ordering::SeqCst)
    }

    pub fn set_data_sync_retry(&self, value: bool) {
        self.data_sync_retry
            .store(value, crate::sync::atomic::Ordering::SeqCst);
        self.bump_prepare_context_generation();
    }

    /// Get the sync type setting.
    pub fn get_sync_type(&self) -> crate::io::FileSyncType {
        self.pager.load().get_sync_type()
    }

    /// Set the sync type (for PRAGMA fullfsync).
    pub fn set_sync_type(&self, value: crate::io::FileSyncType) {
        self.pager.load().set_sync_type(value);
    }

    /// Creates a HashSet of modules that have been loaded
    pub fn get_syms_vtab_mods(&self) -> HashSet<String> {
        self.syms.read().vtab_modules.keys().cloned().collect()
    }

    /// Returns external (extension) functions: (name, is_aggregate, argc, deterministic)
    pub fn get_syms_functions(&self) -> Vec<(String, bool, i32, bool)> {
        self.syms
            .read()
            .functions
            .values()
            .map(|f| {
                let is_agg = f.func.is_aggregate();
                let argc = match &f.func {
                    function::ExtFunc::Aggregate { argc, .. } => *argc,
                    function::ExtFunc::Scalar { argc, .. } => *argc,
                };
                (
                    f.name.clone(),
                    is_agg,
                    argc,
                    function::Deterministic::is_deterministic(f.as_ref()),
                )
            })
            .collect()
    }

    pub fn register_external_collation(
        &self,
        name: String,
        context: usize,
        callback: crate::ContextCollationFunction,
        context_destructor: Option<crate::ContextDestructor>,
    ) {
        let collation = CollationSeq::custom(&name);
        let normalized_name = crate::util::normalize_ident(&name);
        self.syms.write().collations.insert(
            collation.id(),
            Arc::new(function::ExternalCollation::new(
                normalized_name,
                context,
                callback,
                context_destructor,
            )),
        );
        self.bump_prepare_context_generation();
    }

    pub fn unregister_external_collation(&self, name: &str) {
        if let Some(collation) = CollationSeq::known_custom(name) {
            if self
                .syms
                .write()
                .collations
                .remove(&collation.id())
                .is_some()
            {
                self.bump_prepare_context_generation();
            }
        }
    }

    pub(crate) fn get_external_collation(
        &self,
        collation: CollationSeq,
    ) -> Result<Arc<function::ExternalCollation>> {
        self.syms
            .read()
            .collations
            .get(&collation.id())
            .cloned()
            .ok_or_else(|| {
                LimboError::ParseError(format!("no such collation sequence: {}", collation.name()))
            })
    }

    pub(crate) fn custom_collation_compare(
        external: &function::ExternalCollation,
        left: &str,
        right: &str,
    ) -> CmpOrdering {
        let result = unsafe {
            (external.callback)(
                external.context,
                left.as_ptr(),
                left.len(),
                right.as_ptr(),
                right.len(),
            )
        };
        result.cmp(&0)
    }

    pub(crate) fn external_collation_comparator(
        external: Arc<function::ExternalCollation>,
    ) -> crate::vdbe::sorter::SortComparator {
        Arc::new(move |left, right| {
            Ok(match (left, right) {
                (crate::ValueRef::Text(left), crate::ValueRef::Text(right)) => {
                    Self::custom_collation_compare(&external, left.as_str(), right.as_str())
                }
                _ => left.partial_cmp(right).unwrap_or(CmpOrdering::Equal),
            })
        })
    }

    pub(crate) fn make_collation_comparator(
        &self,
        collation: CollationSeq,
    ) -> Result<crate::vdbe::sorter::SortComparator> {
        let external = self.get_external_collation(collation)?;
        Ok(Self::external_collation_comparator(external))
    }

    pub(crate) fn compare_external_collation(
        &self,
        collation: CollationSeq,
        left: &str,
        right: &str,
    ) -> Result<CmpOrdering> {
        let external = self.get_external_collation(collation)?;
        Ok(Self::custom_collation_compare(&external, left, right))
    }

    pub(crate) fn database_ptr(&self) -> usize {
        Arc::as_ptr(&self.db) as usize
    }

    pub fn set_encryption_key(&self, key: EncryptionKey) -> Result<()> {
        tracing::trace!("setting encryption key for connection");
        self.ensure_can_change_encryption_settings()?;
        *self.encryption_key.write() = Some(key);
        self.bump_prepare_context_generation();
        self.set_encryption_context()
    }

    pub fn set_encryption_cipher(&self, cipher_mode: CipherMode) -> Result<()> {
        tracing::trace!("setting encryption cipher for connection");
        self.ensure_can_change_encryption_settings()?;
        self.encryption_cipher_mode.set(cipher_mode);
        self.bump_prepare_context_generation();
        self.set_encryption_context()
    }

    pub fn set_reserved_bytes(&self, reserved_bytes: u8) -> Result<()> {
        let pager = self.pager.load();
        if let Some(codec) = pager.page_codec_external() {
            let required_reserved_bytes = codec.required_reserved_bytes();
            if reserved_bytes != required_reserved_bytes {
                return Err(LimboError::InvalidArgument(format!(
                    "page codec requires exactly {required_reserved_bytes} reserved bytes"
                )));
            }
        }
        pager.set_reserved_space_bytes(reserved_bytes);
        Ok(())
    }

    /// Get the reserved bytes value from the pager cache.
    /// Returns None if not yet set (database not initialized).
    pub fn get_reserved_bytes(&self) -> Option<u8> {
        let pager = self.pager.load();
        pager.get_reserved_space()
    }

    pub fn get_encryption_cipher_mode(&self) -> Option<CipherMode> {
        match self.encryption_cipher_mode.get() {
            CipherMode::None => None,
            mode => Some(mode),
        }
    }

    fn ensure_can_change_encryption_settings(&self) -> Result<()> {
        let pager = self.pager.load();
        if pager.is_encryption_ctx_set() {
            return Err(LimboError::InvalidArgument(
                "cannot reset encryption attributes if already set in the session".to_string(),
            ));
        }
        if pager.has_external_page_codec() {
            return Err(LimboError::InvalidArgument(
                "cannot configure built-in encryption while an external page codec is installed"
                    .to_string(),
            ));
        }
        if self.db.get_mv_store().is_some() {
            return Err(LimboError::InvalidArgument(
                "cannot enable encryption after MVCC is active; configure encryption before PRAGMA journal_mode='mvcc'"
                    .to_string(),
            ));
        }
        Ok(())
    }

    // if both key and cipher are set, set encryption context on pager
    fn set_encryption_context(&self) -> Result<()> {
        let key_guard = self.encryption_key.read();
        let Some(key) = key_guard.as_ref() else {
            return Ok(());
        };
        let cipher_mode = self.get_encryption_cipher_mode();
        let Some(cipher_mode) = cipher_mode else {
            return Ok(());
        };
        tracing::trace!("setting encryption ctx for connection");
        let pager = self.pager.load();
        pager.set_encryption_context(cipher_mode, key)
    }

    /// Sets a custom busy handler callback.
    pub fn set_busy_handler(&self, handler: Option<BusyHandlerCallback>) {
        *self.busy_handler.write() = match handler {
            Some(callback) => BusyHandler::Custom { callback },
            None => BusyHandler::None,
        };
        self.bump_prepare_context_generation();
    }

    /// Sets maximum total accumulated timeout. If the duration is Zero, we unset the busy handler.
    pub fn set_busy_timeout(&self, duration: Duration) {
        *self.busy_handler.write() = if duration.is_zero() {
            BusyHandler::None
        } else {
            BusyHandler::Timeout(duration)
        };
        self.bump_prepare_context_generation();
    }

    /// Get the busy timeout duration.
    pub fn get_busy_timeout(&self) -> Duration {
        match &*self.busy_handler.read() {
            BusyHandler::Timeout(d) => *d,
            _ => Duration::ZERO,
        }
    }

    /// Sets the maximum duration a statement is allowed to run.
    /// `Duration::ZERO` disables query timeout.
    pub fn set_query_timeout(&self, duration: Duration) {
        let millis = duration.as_millis().min(u128::from(u64::MAX)) as u64;
        self.query_timeout_ms.store(millis, Ordering::SeqCst);
    }

    /// Get the query timeout duration.
    pub fn get_query_timeout(&self) -> Duration {
        Duration::from_millis(self.query_timeout_ms.load(Ordering::SeqCst))
    }

    /// Get a reference to the busy handler.
    pub fn get_busy_handler(&self) -> crate::sync::RwLockReadGuard<'_, BusyHandler> {
        self.busy_handler.read()
    }

    /// Sets a progress handler invoked approximately every `ops` VM steps.
    /// Passing `ops == 0` or `None` disables the progress handler.
    pub fn set_progress_handler(&self, ops: u64, handler: Option<ProgressHandlerCallback>) {
        self.progress_handler.set(ops, handler);
    }

    /// Configured progress-handler interval in VM steps; 0 when disabled.
    pub fn progress_ops(&self) -> u64 {
        self.progress_handler.ops()
    }

    /// Returns true when the step-based progress handler requests interruption
    /// for the VM steps executed in `(prev_steps, vm_steps]`.
    pub fn should_interrupt_for_progress(&self, prev_steps: u64, vm_steps: u64) -> bool {
        self.progress_handler.should_interrupt(prev_steps, vm_steps)
    }

    /// Request interruption of currently running root statements on this connection.
    /// If no root statement is active, the request is ignored to match SQLite semantics.
    pub fn interrupt(&self) {
        if self.n_active_root_statements.load(Ordering::SeqCst) > 0 {
            self.interrupt_requested.store(true, Ordering::SeqCst);
        }
    }

    /// Returns true if an interrupt is currently pending for this connection.
    pub fn is_interrupted(&self) -> bool {
        self.interrupt_requested.load(Ordering::SeqCst)
    }

    /// Clear the connection interrupt once no root statements remain active.
    pub(crate) fn clear_interrupt_if_idle(&self) {
        if self.n_active_root_statements.load(Ordering::SeqCst) == 0 {
            self.interrupt_requested.store(false, Ordering::SeqCst);
        }
    }

    pub(crate) fn start_root_statement(&self) -> Result<()> {
        let activity = self.statement_activity.lock();
        if activity.explicit_checkpoint_active {
            return Err(LimboError::StatementsInProgress(
                "cannot start a statement while a checkpoint is active",
            ));
        }
        self.n_active_root_statements.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// `from_statement` is true when the checkpoint runs inside a root
    /// statement (PRAGMA wal_checkpoint), which itself counts as one active
    /// root statement; the direct `Connection::checkpoint` API runs outside
    /// any statement, so it requires zero.
    pub(crate) fn begin_explicit_checkpoint(
        &self,
        pager: Arc<Pager>,
        from_statement: bool,
    ) -> Result<ExplicitCheckpointGuard> {
        let statements_expected = if from_statement { 1 } else { 0 };
        let mut activity = self.statement_activity.lock();
        // Parked incremental blob handles don't count: they hold a Root
        // statement open for their whole lifetime, but between blob
        // operations they just sit on their row and re-position on their
        // next blob operation after the checkpoint's cache clear. Counting
        // them would make one open blob handle block every explicit
        // checkpoint until it is closed.
        let active_non_blob = self.n_active_root_statements.load(Ordering::SeqCst)
            - self.n_active_blob_statements.load(Ordering::SeqCst);
        if activity.explicit_checkpoint_active || active_non_blob != statements_expected {
            return Err(LimboError::StatementsInProgress(
                "cannot checkpoint while another statement is active",
            ));
        }
        activity.explicit_checkpoint_active = true;
        Ok(ExplicitCheckpointGuard {
            activity: self.statement_activity.clone(),
            pager,
        })
    }

    pub(crate) fn set_tx_state(&self, state: TransactionState) {
        self.transaction_state.set(state);
    }

    pub(crate) fn get_tx_state(&self) -> TransactionState {
        self.transaction_state.get()
    }

    /// Returns true if the connection is currently in a write transaction.
    /// Used by index methods to determine if it's safe to flush writes.
    pub fn is_in_write_tx(&self) -> bool {
        matches!(self.get_tx_state(), TransactionState::Write { .. })
    }

    pub(crate) fn get_mv_tx_id(&self) -> Option<u64> {
        self.mv_tx.read().map(|(tx_id, _)| tx_id)
    }

    pub(crate) fn get_mv_tx(&self) -> Option<(u64, TransactionMode)> {
        *self.mv_tx.read()
    }

    #[inline(always)]
    pub(crate) fn set_mv_tx(&self, tx_id_and_mode: Option<(u64, TransactionMode)>) {
        tracing::debug!("set_mv_tx: {:?}", tx_id_and_mode);
        *self.mv_tx.write() = tx_id_and_mode;
    }

    /// Get MVCC transaction ID for a specific database.
    /// Uses fast path for main DB, O(1) HashMap lookup for attached DBs.
    pub(crate) fn get_mv_tx_id_for_db(&self, db: usize) -> Option<u64> {
        if db == crate::MAIN_DB_ID {
            self.get_mv_tx_id()
        } else {
            self.attached_mv_txs
                .read()
                .get(&db)
                .map(|(tx_id, _)| *tx_id)
        }
    }

    /// Get MVCC transaction ID and mode for a specific database.
    pub(crate) fn get_mv_tx_for_db(&self, db: usize) -> Option<(u64, TransactionMode)> {
        if db == crate::MAIN_DB_ID {
            self.get_mv_tx()
        } else {
            self.attached_mv_txs.read().get(&db).copied()
        }
    }

    /// Set MVCC transaction for a specific database.
    pub(crate) fn set_mv_tx_for_db(&self, db: usize, val: Option<(u64, TransactionMode)>) {
        if db == crate::MAIN_DB_ID {
            self.set_mv_tx(val);
        } else {
            let mut txs = self.attached_mv_txs.write();
            match val {
                Some(v) => {
                    txs.insert(db, v);
                }
                None => {
                    txs.remove(&db);
                }
            }
        }
    }

    /// Rollback MVCC transactions on all attached databases and clear the
    /// attached transaction list.  When `clear_schemas` is true the
    /// connection-local schema cache for each attached DB is also removed so
    /// that post-rollback queries see the committed schema.
    ///
    /// This is the single source of truth for attached-MVCC rollback logic —
    /// callers in `close()`, `rollback_current_txn()`, and `op_auto_commit`
    /// should all delegate here.
    pub(crate) fn rollback_attached_mvcc_txs(&self, clear_schemas: bool) {
        let txs: HashMap<usize, _> = self.attached_mv_txs.read().clone();
        let mut cleared_any_schema = false;
        for (&db_id, &(tx_id, _mode)) in &txs {
            if let Some(attached_mv_store) = self.mv_store_for_db(db_id) {
                let attached_pager = self
                    .get_pager_from_database_index(&db_id)
                    .expect("attached MVCC transaction should always have a pager");
                if attached_mv_store.is_tx_rollbackable(tx_id) {
                    attached_mv_store.rollback_tx(tx_id, attached_pager.clone(), self, db_id);
                } else {
                    self.set_mv_tx_for_db(db_id, None);
                }
                if clear_schemas {
                    self.database_schemas().write().remove(&db_id);
                    cleared_any_schema = true;
                }
                attached_pager.end_read_tx();
            }
        }
        self.attached_mv_txs.write().clear();
        if cleared_any_schema {
            self.bump_prepare_context_generation();
        }
    }

    /// Rollback WAL-mode transactions on all attached databases and discard
    /// their connection-local schema caches.  MVCC-enabled attached databases
    /// are skipped — those are handled by `rollback_attached_mvcc_txs`.
    pub(crate) fn rollback_attached_wal_txns(&self) {
        self.with_all_attached_pagers_with_index(|pagers| {
            // Record indices of WAL-mode entries so we can batch the schema
            // removal under a single write lock and avoid calling
            // `mv_store_for_db` more than once per entry.
            let mut wal_indices: SmallVec<[usize; 4]> = SmallVec::new();
            for (i, (db_id, _)) in pagers.iter().enumerate() {
                if self.mv_store_for_db(*db_id).is_none() {
                    wal_indices.push(i);
                }
            }
            if wal_indices.is_empty() {
                return;
            }
            {
                let mut schemas = self.database_schemas().write();
                for &i in &wal_indices {
                    schemas.remove(&pagers[i].0);
                }
            }
            self.bump_prepare_context_generation();
            for &i in &wal_indices {
                pagers[i].1.rollback_attached();
            }
        });
    }

    pub(crate) fn with_named_savepoints<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&[NamedSavepointFrame]) -> T,
    {
        let savepoints = self.named_savepoints.read();
        f(&savepoints)
    }

    pub(crate) fn push_named_savepoint(&self, frame: NamedSavepointFrame) {
        self.named_savepoints.write().push(frame);
    }

    /// Snapshot the in-memory schemas (main, temp, attached) for a
    /// savepoint frame so ROLLBACK TO can restore them without re-
    /// reading sqlite_schema from disk. Disk reparse from inside the
    /// vdbe ROLLBACK TO opcode would block on cursor I/O and violate
    /// the vdbe async contract.
    pub(crate) fn with_savepoint_schema_snapshot<F, T>(&self, f: F) -> T
    where
        F: FnOnce(Arc<Schema>, Option<Arc<Schema>>, HashMap<usize, Arc<Schema>>) -> T,
    {
        let main_schema_snapshot = self.schema.read().clone();
        let temp_schema_snapshot = self
            .temp
            .database
            .read()
            .as_ref()
            .map(|temp_db| temp_db.db.schema.lock().clone());
        let staged_schema_snapshot = self.database_schemas.read().clone();
        f(
            main_schema_snapshot,
            temp_schema_snapshot,
            staged_schema_snapshot,
        )
    }

    pub(crate) fn release_named_savepoint_frame(&self, name: &str) -> SavepointResult {
        let mut savepoints = self.named_savepoints.write();
        let Some(target_idx) = savepoints
            .iter()
            .rposition(|savepoint| savepoint.name == name)
        else {
            return SavepointResult::NotFound;
        };
        if savepoints[target_idx].starts_transaction && target_idx == 0 {
            return SavepointResult::Commit;
        }
        savepoints.truncate(target_idx);
        SavepointResult::Release
    }

    pub(crate) fn rollback_named_savepoint_frame(&self, name: &str) -> Option<RollbackFrameInfo> {
        let mut savepoints = self.named_savepoints.write();
        let target_idx = savepoints
            .iter()
            .rposition(|savepoint| savepoint.name == name)?;
        let frame = &savepoints[target_idx];
        let info = RollbackFrameInfo {
            main_schema_snapshot: frame.main_schema_snapshot.clone(),
            temp_schema_snapshot: frame.temp_schema_snapshot.clone(),
            staged_schema_snapshot: frame.staged_schema_snapshot.clone(),
        };
        // ROLLBACK TO keeps the target savepoint itself on the stack;
        // only nested savepoints above it are discarded.
        savepoints.truncate(target_idx + 1);
        Some(info)
    }

    pub(crate) fn clear_named_savepoints(&self) {
        self.named_savepoints.write().clear();
    }

    /// Park a statement's index-method cursor on the connection until the
    /// transaction's outcome is known. A cursor replaced by a later
    /// statement's cursor for the same attachment is closed immediately and
    /// receives neither `on_transaction_committed` nor `on_transaction_rolled_back`
    /// — that replacement is the third legitimate end of a cursor's life
    /// (see the `IndexMethodCursor` trait docs). Only the newest cursor per
    /// attachment gets the final outcome.
    pub(crate) fn register_index_method_transaction_cursor(
        &self,
        mut entry: crate::index_method::TransactionIndexMethodCursor,
    ) {
        entry.cursor.on_statement_committed(&entry.context);
        self.has_index_method_tx_cursors
            .store(true, Ordering::Release);
        let mut entries = self.index_method_tx_cursors.lock();
        let previous = if let Some(position) = entries
            .iter()
            .position(|existing| existing.same_attachment(&entry.context))
        {
            Some(std::mem::replace(&mut entries[position], entry))
        } else {
            entries.push(entry);
            None
        };
        drop(entries);

        if let Some(mut previous) = previous {
            previous.cursor.close(&previous.context);
        }
    }

    fn take_index_method_transaction_cursors(
        &self,
    ) -> Vec<crate::index_method::TransactionIndexMethodCursor> {
        // Fast path for the overwhelmingly common no-index-method case:
        // skip the mutex and the sort below entirely.
        if !self
            .has_index_method_tx_cursors
            .swap(false, Ordering::AcqRel)
        {
            return Vec::new();
        }
        let mut entries = std::mem::take(&mut *self.index_method_tx_cursors.lock());
        entries.sort_by(|left, right| {
            (
                left.context.database().id,
                left.context.index().method_name.as_str(),
                left.context.index().table_name.as_str(),
                left.context.index().index_name.as_str(),
                left.context.index().runtime_id,
            )
                .cmp(&(
                    right.context.database().id,
                    right.context.index().method_name.as_str(),
                    right.context.index().table_name.as_str(),
                    right.context.index().index_name.as_str(),
                    right.context.index().runtime_id,
                ))
        });
        entries
    }

    pub(crate) fn index_methods_on_transaction_committed(&self) {
        let entries = self.take_index_method_transaction_cursors();
        tracing::trace!(
            attachments = entries.len(),
            "publishing transaction-scoped index-method state"
        );
        for mut entry in entries {
            entry.cursor.on_transaction_committed(&entry.context);
            entry.cursor.close(&entry.context);
        }
    }

    pub(crate) fn index_methods_on_transaction_rolled_back(&self) {
        let entries = self.take_index_method_transaction_cursors();
        tracing::trace!(
            attachments = entries.len(),
            "invalidating transaction-scoped index-method state"
        );
        for mut entry in entries {
            entry.cursor.on_transaction_rolled_back(&entry.context);
            entry.cursor.close(&entry.context);
        }
    }

    pub(crate) fn index_methods_on_savepoint_rolled_back(&self) {
        if !self.has_index_method_tx_cursors.load(Ordering::Acquire) {
            return;
        }
        let mut entries = self.index_method_tx_cursors.lock();
        tracing::trace!(
            attachments = entries.len(),
            "invalidating index-method state after savepoint rollback"
        );
        for entry in entries.iter_mut() {
            entry.cursor.on_savepoint_rolled_back(&entry.context);
        }
    }

    /// Roll back the current main-db transaction state and any attached-db
    /// transaction state on this connection.
    pub(crate) fn rollback_current_txn_state(
        &self,
        pager: &Arc<Pager>,
        clear_attached_schemas: bool,
    ) {
        if let Some(mv_store) = self.mv_store().as_ref() {
            if let Some(tx_id) = self.get_mv_tx_id() {
                self.auto_commit.store(true, Ordering::SeqCst);
                if mv_store.is_tx_rollbackable(tx_id) {
                    mv_store.rollback_tx(tx_id, pager.clone(), self, crate::MAIN_DB_ID);
                } else {
                    self.set_mv_tx(None);
                }
            }
            pager.end_read_tx();
            self.rollback_attached_mvcc_txs(clear_attached_schemas);
        } else {
            pager.rollback_tx(self);
            self.auto_commit.store(true, Ordering::SeqCst);
        }
        self.rollback_attached_wal_txns();
        self.index_methods_on_transaction_rolled_back();
        self.set_tx_state(TransactionState::None);
        self.clear_tx_poison();
    }

    /// Roll back transaction state for helpers that start a manual `BEGIN`
    /// outside the normal Transaction opcode path.
    ///
    /// Unlike `rollback_current_txn_state`, this tolerates the attached-only
    /// case where the connection flipped `auto_commit` off but never opened a
    /// main-db read transaction.
    pub(crate) fn rollback_manual_txn_cleanup(
        &self,
        pager: &Arc<Pager>,
        clear_attached_schemas: bool,
    ) {
        let main_has_implicit_state = self.get_tx_state() != TransactionState::None
            || self.get_mv_tx().is_some()
            || pager.holds_read_lock()
            || pager.holds_write_lock();

        if main_has_implicit_state {
            self.rollback_current_txn_state(pager, clear_attached_schemas);
        } else {
            if self.next_attached_mv_tx().is_some() {
                self.rollback_attached_mvcc_txs(clear_attached_schemas);
            }
            self.rollback_attached_wal_txns();
            self.index_methods_on_transaction_rolled_back();
            self.set_tx_state(TransactionState::None);
            self.auto_commit.store(true, Ordering::SeqCst);
        }

        self.rollback_temp_schema();
        self.clear_tx_poison();
        self.set_cdc_transaction_id(-1);
        self.clear_named_savepoints();
        self.clear_deferred_foreign_key_violations();
    }

    /// Iterate over all attached MVCC transactions, calling `f(db_id, tx_id)` for each.
    pub(crate) fn for_each_attached_mv_tx(&self, mut f: impl FnMut(usize, u64)) {
        let txs = self.attached_mv_txs.read();
        for (&db_id, &(tx_id, _)) in txs.iter() {
            f(db_id, tx_id);
        }
    }

    /// Get the next attached MVCC transaction.
    /// Returns an arbitrary entry from `attached_mv_txs`, or `None` if empty.
    pub(crate) fn next_attached_mv_tx(&self) -> Option<(usize, u64, TransactionMode)> {
        self.attached_mv_txs
            .read()
            .iter()
            .next()
            .map(|(&db_id, &(tx_id, mode))| (db_id, tx_id, mode))
    }

    /// Get the MvStore for a specific database.
    /// Returns None for databases without MVCC or for bootstrap connections.
    pub(crate) fn mv_store_for_db(&self, db: usize) -> Option<Arc<MvStore>> {
        if self.is_mvcc_bootstrap_connection() {
            return None;
        }
        match db {
            crate::MAIN_DB_ID => self.db.get_mv_store().as_ref().cloned(),
            crate::TEMP_DB_ID => None,
            _ => {
                let catalog = self.attached_databases.read();
                catalog
                    .index_to_data
                    .get(&db)
                    .and_then(|entry| entry.db.get_mv_store().as_ref().cloned())
            }
        }
    }

    pub(crate) fn set_mvcc_checkpoint_threshold(&self, threshold: i64) -> Result<()> {
        match self.db.get_mv_store().as_ref() {
            Some(mv_store) => {
                mv_store.set_checkpoint_threshold(threshold);
                self.bump_prepare_context_generation();
                Ok(())
            }
            None => Err(LimboError::InternalError("MVCC not enabled".into())),
        }
    }

    pub(crate) fn mvcc_checkpoint_threshold(&self) -> Result<i64> {
        match self.db.get_mv_store().as_ref() {
            Some(mv_store) => Ok(mv_store.checkpoint_threshold()),
            None => Err(LimboError::InternalError("MVCC not enabled".into())),
        }
    }

    pub(crate) fn set_mvcc_gc_threshold(&self, threshold: i64) -> Result<()> {
        match self.db.get_mv_store().as_ref() {
            Some(mv_store) => {
                mv_store.set_gc_threshold(threshold);
                self.bump_prepare_context_generation();
                Ok(())
            }
            None => Err(LimboError::InternalError("MVCC not enabled".into())),
        }
    }

    pub(crate) fn mvcc_gc_threshold(&self) -> Result<i64> {
        match self.db.get_mv_store().as_ref() {
            Some(mv_store) => Ok(mv_store.gc_threshold()),
            None => Err(LimboError::InternalError("MVCC not enabled".into())),
        }
    }

    pub(crate) fn set_mvcc_group_commit(&self, enabled: bool) -> Result<()> {
        match self.db.get_mv_store().as_ref() {
            Some(mv_store) => {
                mv_store.set_group_commit_enabled(enabled);
                self.bump_prepare_context_generation();
                Ok(())
            }
            None => Err(LimboError::InternalError("MVCC not enabled".into())),
        }
    }

    pub(crate) fn mvcc_group_commit(&self) -> Result<bool> {
        match self.db.get_mv_store().as_ref() {
            Some(mv_store) => Ok(mv_store.group_commit_enabled()),
            None => Err(LimboError::InternalError("MVCC not enabled".into())),
        }
    }

    pub(crate) fn mvcc_tx_should_abort(&self) -> bool {
        match (self.db.get_mv_store().clone(), self.get_mv_tx_id()) {
            (Some(mv_store), Some(tx_id)) => mv_store.tx_should_abort(tx_id),
            _ => false,
        }
    }
}

pub type Row = vdbe::Row;

pub type StepResult = vdbe::StepResult;

#[derive(Default)]
pub struct SymbolTable {
    pub functions: HashMap<String, Arc<function::ExternalFunc>>,
    pub collations: HashMap<u32, Arc<function::ExternalCollation>>,
    pub vtabs: HashMap<String, Arc<VirtualTable>>,
    pub vtab_modules: HashMap<String, Arc<crate::ext::VTabImpl>>,
    pub index_methods: HashMap<String, Arc<dyn IndexMethod>>,
}

impl std::fmt::Debug for SymbolTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SymbolTable")
            .field("functions", &self.functions)
            .field("collations", &self.collations)
            .finish()
    }
}

fn is_shared_library(path: &std::path::Path) -> bool {
    path.extension()
        .is_some_and(|ext| ext == "so" || ext == "dylib" || ext == "dll")
}

pub fn resolve_ext_path(extpath: &str) -> Result<std::path::PathBuf> {
    let path = std::path::Path::new(extpath);
    if !path.exists() {
        if is_shared_library(path) {
            return Err(LimboError::ExtensionError(format!(
                "Extension file not found: {extpath}"
            )));
        };
        let maybe = path.with_extension(std::env::consts::DLL_EXTENSION);
        maybe.exists().then_some(maybe).ok_or_else(|| {
            LimboError::ExtensionError(format!("Extension file not found: {extpath}"))
        })
    } else {
        Ok(path.to_path_buf())
    }
}

impl SymbolTable {
    pub fn new() -> Self {
        Self {
            functions: HashMap::default(),
            collations: HashMap::default(),
            vtabs: HashMap::default(),
            vtab_modules: HashMap::default(),
            index_methods: HashMap::default(),
        }
    }
    pub fn resolve_function(
        &self,
        name: &str,
        arg_count: usize,
    ) -> Option<Arc<function::ExternalFunc>> {
        self.functions
            .get(name)
            .cloned()
            .or_else(|| {
                self.functions
                    .get(&crate::util::normalize_ident(name))
                    .cloned()
            })
            .filter(|func| func.func.matches_arg_count(arg_count))
    }

    pub fn resolve_collation(&self, name: &str) -> Option<CollationSeq> {
        let collation = CollationSeq::known_custom(name)?;
        self.collations
            .contains_key(&collation.id())
            .then_some(collation)
    }

    pub fn extend(&mut self, other: &SymbolTable) {
        for (name, func) in &other.functions {
            self.functions.insert(name.clone(), func.clone());
        }
        for (id, collation) in &other.collations {
            self.collations.insert(*id, collation.clone());
        }
        for (name, vtab) in &other.vtabs {
            self.vtabs.insert(name.clone(), vtab.clone());
        }
        for (name, module) in &other.vtab_modules {
            self.vtab_modules.insert(name.clone(), module.clone());
        }
        for (name, module) in &other.index_methods {
            self.index_methods.insert(name.clone(), module.clone());
        }
    }
}

#[cfg(all(test, feature = "fs"))]
mod tests {
    use super::*;
    use crate::SqliteDialect;
    use tempfile::TempDir;

    fn open_connection_with_opts(path: &std::path::Path, opts: DatabaseOpts) -> Arc<Connection> {
        let io: Arc<dyn IO> = Arc::new(crate::PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            path.to_str().unwrap(),
            OpenFlags::default(),
            opts,
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        db.connect().unwrap()
    }

    fn open_connection(path: &std::path::Path) -> Arc<Connection> {
        open_connection_with_opts(path, DatabaseOpts::new())
    }

    fn drive_attach(conn: &Arc<Connection>, path: &str, alias: &str) -> Result<()> {
        let mut state = AttachDatabaseState::default();
        loop {
            match conn.attach_database(path, alias, &mut state)? {
                IOResult::Done(()) => return Ok(()),
                IOResult::IO(io) => io.wait(conn.db.io.as_ref())?,
            }
        }
    }

    fn drive_attach_with_config(
        conn: &Arc<Connection>,
        path: &str,
        alias: &str,
        reserved_space: Option<u8>,
    ) -> Result<()> {
        let mut state = AttachDatabaseState::default();
        loop {
            match conn.attach_database_with_config(path, alias, reserved_space, &mut state)? {
                IOResult::Done(()) => return Ok(()),
                IOResult::IO(io) => io.wait(conn.db.io.as_ref())?,
            }
        }
    }

    fn query_single_i64(conn: &Arc<Connection>, sql: &str) -> i64 {
        let mut stmt = conn.prepare(sql).unwrap();
        match stmt.step().unwrap() {
            StepResult::Row => stmt.row().unwrap().get::<i64>(0).unwrap(),
            other => panic!("expected a row, got {other:?}"),
        }
    }

    fn text_value(value: &Value) -> &str {
        match value {
            Value::Text(text) => text.as_str(),
            other => panic!("expected text value, got {other:?}"),
        }
    }

    // given a attached 'alias', return the Database and Pager for that attached database
    fn attached_entry(conn: &Connection, alias: &str) -> (Arc<Database>, Arc<Pager>) {
        let catalog = conn.attached_databases.read();
        let index = *catalog.name_to_index.get(alias).unwrap();
        let entry = catalog.index_to_data.get(&index).unwrap();
        (entry.db.clone(), entry.pager.clone())
    }

    #[test]
    fn test_named_memory_databases_on_same_io_are_distinct() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let draft_db =
            Database::open_file(io.clone(), ":memory:sync-draft", Arc::new(SqliteDialect)).unwrap();
        let synced_db =
            Database::open_file(io, ":memory:sync-synced", Arc::new(SqliteDialect)).unwrap();
        assert!(!Arc::ptr_eq(&draft_db, &synced_db));

        let draft = draft_db.connect().unwrap();
        let synced = synced_db.connect().unwrap();

        for conn in [&draft, &synced] {
            assert_eq!(conn.get_database_canonical_path(), "");
            assert_eq!(
                conn.list_all_databases(),
                vec![(MAIN_DB_ID, "main".to_string(), String::new())]
            );
        }

        draft
            .execute("CREATE TABLE t(x INTEGER); INSERT INTO t VALUES(11)")
            .unwrap();
        synced
            .execute("CREATE TABLE t(x INTEGER); INSERT INTO t VALUES(22)")
            .unwrap();

        assert_eq!(query_single_i64(&draft, "SELECT x FROM t"), 11);
        assert_eq!(query_single_i64(&synced, "SELECT x FROM t"), 22);
    }

    #[test]
    fn test_named_memory_database_reopened_on_same_io_sees_same_rows() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());

        let first_db =
            Database::open_file(io.clone(), ":memory:reopen", Arc::new(SqliteDialect)).unwrap();
        let first = first_db.connect().unwrap();
        first
            .execute("CREATE TABLE t(x INTEGER); INSERT INTO t VALUES(99)")
            .unwrap();

        let second_db = Database::open_file(io, ":memory:reopen", Arc::new(SqliteDialect)).unwrap();
        let second = second_db.connect().unwrap();
        assert_eq!(query_single_i64(&second, "SELECT x FROM t"), 99);
    }

    #[test]
    fn test_attach_named_memory_database_reports_empty_path() {
        let temp_dir = TempDir::new().unwrap();
        let main_path = temp_dir.path().join("main.db");
        let conn = open_connection_with_opts(&main_path, DatabaseOpts::new().with_attach(true));

        conn.execute("ATTACH ':memory:aux' AS aux").unwrap();
        conn.execute("CREATE TABLE aux.t(x INTEGER); INSERT INTO aux.t VALUES(5)")
            .unwrap();

        assert_eq!(query_single_i64(&conn, "SELECT x FROM aux.t"), 5);
        let database_list = conn.pragma_query("database_list").unwrap();
        let aux = database_list
            .iter()
            .find(|row| text_value(&row[1]) == "aux")
            .expect("attached aux database must be listed");
        assert_eq!(text_value(&aux[2]), "");
    }

    #[test]
    fn test_named_memory_parent_can_attach_real_file_database() {
        let temp_dir = TempDir::new().unwrap();
        let aux_path = temp_dir.path().join("aux.db");
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            ":memory:named-main",
            OpenFlags::default(),
            DatabaseOpts::new().with_attach(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();

        conn.execute(format!("ATTACH '{}' AS aux", aux_path.to_str().unwrap()))
            .unwrap();
        conn.execute("CREATE TABLE aux.t(x INTEGER); INSERT INTO aux.t VALUES(7)")
            .unwrap();
        conn.execute("DETACH aux").unwrap();

        let reopened = open_connection(&aux_path);
        assert_eq!(query_single_i64(&reopened, "SELECT x FROM t"), 7);
    }

    #[test]
    fn test_attach_database_with_config_overrides_reserved_space_before_initialization() {
        let temp_dir = TempDir::new().unwrap();
        let main_path = temp_dir.path().join("main.db");
        let aux_path = temp_dir.path().join("aux.db");
        let conn = open_connection(&main_path);

        drive_attach_with_config(&conn, aux_path.to_str().unwrap(), "aux", Some(48)).unwrap();

        let (attached_db, pager) = attached_entry(&conn, "aux");
        assert!(!attached_db.initialized());
        assert!(!pager.db_initialized());
        assert_eq!(pager.get_reserved_space(), Some(48));
    }

    #[cfg(feature = "checksum")]
    #[test]
    fn test_attach_database_with_config_rejects_reserved_space_below_minimum() {
        let temp_dir = TempDir::new().unwrap();
        let main_path = temp_dir.path().join("main.db");
        let aux_path = temp_dir.path().join("aux.db");
        let conn = open_connection(&main_path);

        let err = drive_attach_with_config(&conn, aux_path.to_str().unwrap(), "aux", Some(0))
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "cannot attach database 'aux': reserved space 0 is smaller than attached database minimum 8"
        );
    }

    #[test]
    fn test_fresh_mvcc_attach_installs_wal_before_bootstrap() {
        // this is a test to check that mvcc db on attach with a fresh db, makes the
        // attached db also mvcc
        let temp_dir = TempDir::new().unwrap();
        let main_path = temp_dir.path().join("main.db");
        let aux_path = temp_dir.path().join("aux.db");
        let conn = open_connection(&main_path);

        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        drive_attach(&conn, aux_path.to_str().unwrap(), "aux").unwrap();

        let (attached_db, pager) = attached_entry(&conn, "aux");
        assert!(attached_db.get_mv_store().as_ref().is_some());
        assert!(pager.has_wal());

        conn.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
        conn.execute("INSERT INTO aux.t VALUES(1)").unwrap();
        conn.execute("PRAGMA aux.wal_checkpoint(TRUNCATE)").unwrap();
    }

    #[test]
    fn test_fresh_mvcc_attach_reuses_database_shared_wal() {
        let temp_dir = TempDir::new().unwrap();
        let main_path = temp_dir.path().join("main.db");
        let aux_path = temp_dir.path().join("aux.db");
        let conn = open_connection(&main_path);

        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        drive_attach(&conn, aux_path.to_str().unwrap(), "aux").unwrap();
        conn.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
        conn.execute("INSERT INTO aux.t VALUES(1)").unwrap();

        let (attached_db, pager) = attached_entry(&conn, "aux");
        let pager_shared_ptr = pager
            .wal_shared_ptr()
            .expect("fresh MVCC attach must expose WAL shared state in tests");
        let db_shared_ptr = Arc::as_ptr(&attached_db.shared_wal) as usize;

        assert_eq!(pager_shared_ptr, db_shared_ptr);
    }

    #[test]
    fn test_temp_tables_are_connection_local_and_shadow_main() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("main.db");
        let conn1 = open_connection(&db_path);

        conn1.execute("CREATE TABLE t(x INTEGER)").unwrap();
        conn1.execute("INSERT INTO main.t VALUES(1)").unwrap();
        let conn2 = open_connection(&db_path);
        conn1.execute("CREATE TEMP TABLE t(x INTEGER)").unwrap();
        conn1.execute("INSERT INTO temp.t VALUES(2)").unwrap();

        assert_eq!(query_single_i64(&conn1, "SELECT x FROM t"), 2);
        assert_eq!(query_single_i64(&conn1, "SELECT x FROM main.t"), 1);
        assert_eq!(query_single_i64(&conn2, "SELECT x FROM t"), 1);

        let err = conn2
            .prepare("SELECT x FROM temp.t")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no such table"),
            "expected no such table error, got: {err}"
        );
    }

    #[test]
    fn test_reprepare_after_temp_store_reset_does_not_panic() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("main.db");
        let conn = open_connection(&db_path);

        conn.execute("CREATE TEMP TABLE t(x INTEGER)").unwrap();
        let mut stmt = conn.prepare("SELECT x FROM t").unwrap();

        conn.execute("PRAGMA temp_store = MEMORY").unwrap();

        let err = stmt.step().unwrap_err().to_string();
        assert!(
            err.contains("no such table"),
            "expected no such table after temp reset, got: {err}"
        );
    }

    #[test]
    fn test_temp_trigger_abort_rolls_back_temp_writes_without_panicking() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("main.db");
        let conn = open_connection(&db_path);

        conn.execute("CREATE TEMP TABLE t(x INTEGER)").unwrap();
        conn.execute("CREATE TEMP TABLE u(y INTEGER)").unwrap();
        conn.execute(
            "CREATE TRIGGER tr BEFORE INSERT ON temp.t BEGIN \
             INSERT INTO u VALUES (NEW.x); \
             SELECT RAISE(ABORT, 'boom'); \
             END;",
        )
        .unwrap();

        let err = conn.execute("INSERT INTO temp.t VALUES(1)").unwrap_err();
        assert!(
            err.to_string().contains("boom"),
            "expected trigger abort error, got: {err}"
        );
        assert_eq!(query_single_i64(&conn, "SELECT COUNT(*) FROM temp.u"), 0);
        assert_eq!(query_single_i64(&conn, "SELECT COUNT(*) FROM temp.t"), 0);
    }

    #[test]
    fn test_temp_trigger_abort_rolls_back_main_and_temp_writes() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("main.db");
        let conn = open_connection(&db_path);

        conn.execute("CREATE TABLE m(x INTEGER)").unwrap();
        conn.execute("CREATE TEMP TABLE t(x INTEGER)").unwrap();
        conn.execute("CREATE TEMP TABLE u(y INTEGER)").unwrap();
        conn.execute(
            "CREATE TRIGGER tr BEFORE INSERT ON temp.t BEGIN \
             INSERT INTO m VALUES (NEW.x); \
             INSERT INTO u VALUES (NEW.x); \
             SELECT RAISE(ABORT, 'boom'); \
             END;",
        )
        .unwrap();

        let err = conn.execute("INSERT INTO temp.t VALUES(1)").unwrap_err();
        assert!(
            err.to_string().contains("boom"),
            "expected trigger abort error, got: {err}"
        );
        assert_eq!(query_single_i64(&conn, "SELECT COUNT(*) FROM main.m"), 0);
        assert_eq!(query_single_i64(&conn, "SELECT COUNT(*) FROM temp.u"), 0);
        assert_eq!(query_single_i64(&conn, "SELECT COUNT(*) FROM temp.t"), 0);
    }

    #[test]
    fn test_distinct_triggers_with_same_name_in_different_schemas_can_fire_nested() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("main.db");
        let conn = open_connection(&db_path);

        conn.execute("CREATE TABLE src(x INTEGER)").unwrap();
        conn.execute("CREATE TABLE dst(y INTEGER)").unwrap();
        conn.execute("CREATE TABLE audit(z INTEGER)").unwrap();
        conn.execute(
            "CREATE TRIGGER shared_name AFTER INSERT ON dst BEGIN \
             INSERT INTO audit VALUES (NEW.y); \
             END;",
        )
        .unwrap();
        conn.execute(
            "CREATE TEMP TRIGGER shared_name AFTER INSERT ON main.src BEGIN \
             INSERT INTO dst VALUES (NEW.x); \
             END;",
        )
        .unwrap();

        conn.execute("INSERT INTO src VALUES(7)").unwrap();

        assert_eq!(query_single_i64(&conn, "SELECT COUNT(*) FROM main.dst"), 1);
        assert_eq!(query_single_i64(&conn, "SELECT SUM(z) FROM main.audit"), 7);
    }

    /// A committed `setval(X, false)` stores an unconsumed sequence value.
    /// After sequence initialization reloads persisted state, the in-memory
    /// sequence must still represent that value as unconsumed, so the next
    /// `nextval()` returns `X` rather than advancing past it.
    /// Disk-only sequence design: setval(value, is_called=false) must be
    /// observable as the next nextval() result. Previously this exercised
    /// the in-memory-atomic reseeding path; that path no longer exists,
    /// but the user-visible contract still holds because every nextval
    /// reads the backing-table watermark and applies is_called semantics
    /// in op_sequence_compute_next.
    #[test]
    fn test_setval_uncalled_emits_stored_value_as_next() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("seq_init.db");
        let conn = open_connection_with_opts(&path, DatabaseOpts::new());

        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.execute("CREATE SEQUENCE s START 1 INCREMENT 3")?;
        conn.execute("SELECT setval('s', 13, 0)")?;

        let next_val = query_single_i64(&conn, "SELECT nextval('s')");
        assert_eq!(
            next_val, 13,
            "setval(13, false) committed: next nextval must return 13"
        );
        Ok(())
    }
}
