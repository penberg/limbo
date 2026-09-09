use std::sync::{Arc, Weak};

use crate::types::IOResultOr;
use rustc_hash::FxHashMap as HashMap;
#[cfg(any(test, injected_yields))]
use strum::EnumCount;
use turso_parser::ast;

use crate::{
    mvcc::cursor::MvccCursorType,
    schema::IndexColumn,
    storage::{
        btree::{BTreeCursor, CursorTrait},
        journal_mode::JournalMode,
    },
    translate::emitter::TransactionMode,
    types::{IOResult, IndexInfo, KeyInfo},
    vdbe::Register,
    Connection, LimboError, MvCursor, Result, Value,
};

pub mod backing_btree;
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
pub mod fts;
pub mod toy_vector_sparse_ivf;

pub const BACKING_BTREE_INDEX_METHOD_NAME: &str = "backing_btree";
pub const TOY_VECTOR_SPARSE_IVF_INDEX_METHOD_NAME: &str = "toy_vector_sparse_ivf";

/// Default for `PRAGMA fts_merge_threshold`: the number of visible FTS
/// segments a statement flush may leave behind before the write path merges
/// them. Lives here (not in the feature-gated `fts` module) because the
/// connection setting exists regardless of the feature.
pub const DEFAULT_FTS_MERGE_THRESHOLD: i64 = 32;

/// index method "entry point" which can create attachment of the method to the table with given configuration
/// (this trait acts like a "factory")
pub trait IndexMethod: std::fmt::Debug + Send + Sync {
    /// create attachment of the index method to the specific table with specific method configuration
    fn attach(
        &self,
        configuration: &IndexMethodConfiguration,
    ) -> Result<Arc<dyn IndexMethodAttachment>>;
}

#[derive(Debug, Clone)]
pub struct IndexMethodConfiguration {
    /// table name for which index_method is defined
    pub table_name: String,
    /// index name
    pub index_name: String,
    /// columns c1, c2, c3, ... provided to the index method (e.g. create index t_idx on t using method (c1, c2, c3, ...))
    pub columns: crate::alloc::Vec<IndexColumn>,
    /// optional parameters provided to the index method through WITH clause
    pub parameters: HashMap<String, Value>,
}

/// index method attached to the table with specific configuration
/// the attachment is capable of generating SELECT patterns where index can be used and also can create cursor for query execution
pub trait IndexMethodAttachment: std::fmt::Debug + Send + Sync {
    fn definition<'a>(&'a self) -> IndexMethodDefinition<'a>;
    fn init(&self) -> Result<Box<dyn IndexMethodCursor>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexMethodMvccSupport {
    /// The method cannot be opened while MVCC is active.
    Unsupported,
    /// The method may query MVCC snapshots but has no transactional write path.
    ReadOnly,
    /// Persistent state is stored exclusively through core-provided,
    /// MVCC-aware backing storage.
    ///
    /// Under MVCC, concurrent `BEGIN CONCURRENT` transactions may write one
    /// index of this kind at the same time when the method's writes commute
    /// (FTS appends immutable segments under fresh ids). Deletes and
    /// updates that target existing entries are mutually excluded with
    /// index maintenance: merge/OPTIMIZE holds the per-index lease (the
    /// merge mutex, `Busy` on contention, `WriteWriteConflict` when its
    /// snapshot is stale), and tombstone writers overlapping a merge are
    /// refused the same way so their deletes cannot be lost.
    TransactionalBackingStore,
    /// Persistent state is external and implements transaction outcome hooks.
    ExternalTransactional,
}

#[derive(Debug)]
pub struct IndexMethodDefinition<'a> {
    /// index method name
    pub method_name: &'a str,
    /// table to which the index is attached
    pub table_name: &'a str,
    /// index name
    pub index_name: &'a str,
    /// SELECT patterns where index method can be used
    /// the patterns can contain positional placeholder which will make planner to capture parameters from the original query and provide them to the index method
    /// (for example, pattern 'SELECT * FROM {table} LIMIT ?' will capture LIMIT parameter and provide its value from the query to the index method query_start(...) call)
    pub patterns: &'a [ast::Select],
    /// special marker which forces tursodb core to treat index method as backing btree - so it will allocate real btree on disk for that index method
    pub backing_btree: bool,
    /// Whether `query_start()` materializes all matching rowids up front (e.g. into a Vec/VecDeque).
    /// When `true`, the cursor is safe to use during DML because it does not lazily stream from
    /// a live data structure that writes could invalidate.
    /// When `false`, the emitter will collect rowids into a RowSet/ephemeral table before writing.
    pub results_materialized: bool,
    /// Declares how this method participates in MVCC transactions.
    pub mvcc_support: IndexMethodMvccSupport,
}

/// Transaction mode exposed to index methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexMethodTransactionMode {
    Read,
    Write,
    Concurrent,
}

/// Stable coordinates for the snapshot visible to an index method operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexMethodSnapshotIdentity {
    /// WAL readers are pinned to a checkpoint sequence and maximum frame.
    Wal {
        checkpoint_sequence: u32,
        max_frame: u64,
    },
    /// MVCC readers are pinned to the transaction's begin timestamp.
    Mvcc {
        /// The MVCC transaction the operation runs inside.
        transaction_id: u64,
        begin_timestamp: u64,
    },
}

/// Append-only synthetic-yield markers for index-method lifecycle boundaries.
///
/// Ordinals are consumed by deterministic simulator plans; never reorder or
/// reuse an existing value.
#[cfg(any(test, injected_yields))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum_macros::EnumCount)]
#[repr(u8)]
pub(crate) enum IndexMethodYieldPoint {
    BeforePrepareStatement = 0,
    AfterPrepareStatement = 1,
}

#[cfg(any(test, injected_yields))]
impl crate::mvcc::yield_hooks::YieldPointMarker for IndexMethodYieldPoint {
    const POINT_COUNT: u8 = Self::COUNT as u8;

    fn ordinal(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexMethodDatabaseIdentity {
    /// Connection-local slot used to address the database.
    pub id: usize,
    pub name: String,
    /// Runtime identity of the shared `Database` object. Unlike `id` and
    /// `name`, this distinguishes detach/reattach and close/reopen lifetimes.
    pub incarnation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexMethodIdentity {
    pub method_name: String,
    pub table_name: String,
    pub index_name: String,
    /// Runtime identity derived from the database incarnation, the schema
    /// generation, and the index's names. Attach/detach and close/reopen
    /// cannot compare equal. Drop/recreate inside one MVCC transaction can
    /// (MVCC DDL does not advance the schema generation); the consumers
    /// tolerate that, because a colliding cursor is merely replaced-and-closed
    /// and index content is validated separately by the persisted
    /// (incarnation, generation) pair.
    pub runtime_id: u64,
    /// Schema root assigned to the logical definition. Custom index methods
    /// must keep this at zero; physical ownership belongs to backing objects.
    pub schema_root: i64,
}

fn index_method_runtime_id(
    database_incarnation: u64,
    schema_generation: u64,
    definition: &IndexMethodDefinition<'_>,
) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64 ^ database_incarnation;
    hash = (hash ^ schema_generation).wrapping_mul(0x100_0000_01b3);
    for byte in definition
        .method_name
        .bytes()
        .chain(definition.table_name.bytes())
        .chain(definition.index_name.bytes())
    {
        hash = (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3);
    }
    hash
}

/// Read-only execution and storage context for an index method operation.
///
/// Persistent cursors can only be created through this context, ensuring they
/// are promoted to snapshot-aware MVCC cursors whenever MVCC is active.
#[derive(Clone)]
pub struct IndexMethodContext {
    /// Weak so a cursor parked on its connection (with its context) does not
    /// make the connection reference itself — a strong edge here kept leaked
    /// connections alive forever, holding their WAL locks.
    connection: Weak<Connection>,
    database: IndexMethodDatabaseIdentity,
    journal_mode: JournalMode,
    transaction_mode: IndexMethodTransactionMode,
    snapshot: IndexMethodSnapshotIdentity,
    schema_generation: u64,
    index: IndexMethodIdentity,
    #[cfg(any(test, injected_yields))]
    yield_instance_id: u64,
}

impl std::fmt::Debug for IndexMethodContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IndexMethodContext")
            .field("database", &self.database)
            .field("journal_mode", &self.journal_mode)
            .field("transaction_mode", &self.transaction_mode)
            .field("snapshot", &self.snapshot)
            .field("schema_generation", &self.schema_generation)
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

impl IndexMethodContext {
    pub(crate) fn new(
        connection: &Arc<Connection>,
        database_id: usize,
        definition: &IndexMethodDefinition<'_>,
    ) -> Result<Self> {
        let pager = connection.get_pager_from_database_index(&database_id)?;
        let schema_root = connection.with_schema(database_id, |schema| {
            schema
                .get_index(definition.table_name, definition.index_name)
                .map_or(0, |index| index.root_page)
        });
        if !definition.backing_btree && schema_root != 0 {
            return Err(LimboError::Corrupt(format!(
                "logical index-method definition '{}.{}' owns physical root {schema_root}",
                definition.table_name, definition.index_name
            )));
        }

        let (journal_mode, transaction_mode, snapshot, schema_generation) = if let Some(mv_store) =
            connection.mv_store_for_db(database_id)
        {
            let (tx_id, mode) = connection.get_mv_tx_for_db(database_id).ok_or_else(|| {
                LimboError::InternalError(format!(
                    "index method '{}' opened MVCC storage without an active transaction",
                    definition.method_name
                ))
            })?;
            let transaction_mode = match mode {
                TransactionMode::None | TransactionMode::Read => IndexMethodTransactionMode::Read,
                TransactionMode::Write => IndexMethodTransactionMode::Write,
                TransactionMode::Concurrent => IndexMethodTransactionMode::Concurrent,
            };
            (
                JournalMode::Mvcc,
                transaction_mode,
                IndexMethodSnapshotIdentity::Mvcc {
                    transaction_id: tx_id,
                    begin_timestamp: mv_store.read_snapshot_ts(tx_id),
                },
                mv_store.schema_generation(),
            )
        } else {
            let (checkpoint_sequence, max_frame) = pager.wal_pos();
            (
                JournalMode::Wal,
                if connection.is_in_write_tx() {
                    IndexMethodTransactionMode::Write
                } else {
                    IndexMethodTransactionMode::Read
                },
                IndexMethodSnapshotIdentity::Wal {
                    checkpoint_sequence,
                    max_frame,
                },
                pager.get_schema_cookie_cached().unwrap_or_default() as u64,
            )
        };

        let source_database = connection.get_source_database(database_id);
        let database_incarnation = source_database.incarnation;
        let runtime_id =
            index_method_runtime_id(database_incarnation, schema_generation, definition);

        Ok(Self {
            connection: Arc::downgrade(connection),
            database: IndexMethodDatabaseIdentity {
                id: database_id,
                name: connection
                    .get_database_name_by_index(database_id)
                    .unwrap_or_else(|| format!("database-{database_id}")),
                incarnation: database_incarnation,
            },
            journal_mode,
            transaction_mode,
            snapshot,
            schema_generation,
            index: IndexMethodIdentity {
                method_name: definition.method_name.to_string(),
                table_name: definition.table_name.to_string(),
                index_name: definition.index_name.to_string(),
                runtime_id,
                schema_root,
            },
            #[cfg(any(test, injected_yields))]
            yield_instance_id: connection.next_yield_instance_id(),
        })
    }

    /// Build the same core-owned context for raw index-method integration
    /// tests. Production callers receive contexts only from the VDBE.
    #[cfg(any(feature = "test_helper", feature = "conn_raw_api"))]
    #[doc(hidden)]
    pub fn for_test(
        connection: &Arc<Connection>,
        database_id: usize,
        attachment: &dyn IndexMethodAttachment,
    ) -> Result<Self> {
        Self::new(connection, database_id, &attachment.definition())
    }

    /// The connection this context was built for. Errors once the connection
    /// is being torn down; outcome hooks running at that point have nothing
    /// left to clean up and should just return.
    pub fn connection(&self) -> Result<Arc<Connection>> {
        self.connection.upgrade().ok_or_else(|| {
            LimboError::InternalError("index method context outlived its connection".to_string())
        })
    }

    pub fn database(&self) -> &IndexMethodDatabaseIdentity {
        &self.database
    }

    pub fn journal_mode(&self) -> JournalMode {
        self.journal_mode
    }

    pub fn transaction_mode(&self) -> IndexMethodTransactionMode {
        self.transaction_mode
    }

    /// The MVCC transaction this operation runs inside; `None` under WAL.
    pub fn transaction_id(&self) -> Option<u64> {
        match self.snapshot {
            IndexMethodSnapshotIdentity::Mvcc { transaction_id, .. } => Some(transaction_id),
            IndexMethodSnapshotIdentity::Wal { .. } => None,
        }
    }

    pub fn snapshot(&self) -> IndexMethodSnapshotIdentity {
        self.snapshot
    }

    pub fn schema_generation(&self) -> u64 {
        self.schema_generation
    }

    pub fn index(&self) -> &IndexMethodIdentity {
        &self.index
    }

    pub fn open_table_cursor(&self, table: &str) -> Result<Box<dyn CursorTrait>> {
        open_table_cursor(&self.connection()?, self.database.id, table)
    }

    pub fn open_index_cursor<I, E>(
        &self,
        table: &str,
        index: &str,
        keys: I,
    ) -> Result<Box<dyn CursorTrait>>
    where
        I: IntoIterator<Item = KeyInfo, IntoIter = E>,
        E: ExactSizeIterator<Item = KeyInfo>,
    {
        open_index_cursor(&self.connection()?, self.database.id, table, index, keys)
    }
}

#[cfg(any(test, injected_yields))]
impl crate::mvcc::yield_hooks::ProvidesYieldContext for IndexMethodContext {
    fn yield_context(&self) -> crate::mvcc::yield_hooks::YieldContext {
        let mut selection_key = 0x494e_4458_4d45_5448u64;
        selection_key ^= self.database.id as u64;
        selection_key = selection_key.rotate_left(17) ^ self.schema_generation;
        selection_key = selection_key.rotate_left(17) ^ self.index.schema_root as u64;
        for byte in self
            .index
            .method_name
            .bytes()
            .chain(self.index.index_name.bytes())
        {
            selection_key = selection_key.wrapping_mul(0x100_0000_01b3) ^ u64::from(byte);
        }
        let connection = self
            .connection
            .upgrade()
            .expect("yield context requires a live connection");
        crate::mvcc::yield_hooks::YieldContext::new(
            connection.yield_injector(),
            connection.failure_injector(),
            self.yield_instance_id,
            selection_key,
        )
    }
}

pub(crate) fn ensure_mvcc_support(
    definition: &IndexMethodDefinition<'_>,
    write: bool,
) -> Result<()> {
    match (definition.mvcc_support, write) {
        (IndexMethodMvccSupport::Unsupported, _) => Err(LimboError::ParseError(format!(
            "index method '{}' does not support MVCC",
            definition.method_name
        ))),
        (IndexMethodMvccSupport::ReadOnly, true) => Err(LimboError::ParseError(format!(
            "index method '{}' is read-only in MVCC",
            definition.method_name
        ))),
        (
            IndexMethodMvccSupport::ReadOnly
            | IndexMethodMvccSupport::TransactionalBackingStore
            | IndexMethodMvccSupport::ExternalTransactional,
            _,
        ) => Ok(()),
    }
}

/// Cost estimate returned by custom index methods for optimizer integration.
/// This enables the optimizer to make cost-based decisions when choosing between
/// custom index methods and traditional BTree indexes.
#[derive(Debug, Clone, Copy)]
pub struct IndexMethodCostEstimate {
    /// Estimated CPU/IO cost (lower is better, comparable to optimizer Cost values)
    pub estimated_cost: f64,
    /// Estimated number of rows returned by the query
    pub estimated_rows: u64,
}

/// Planning-time inputs available to an index method's cost model.
///
/// `arguments` are the query expressions captured from the selected pattern,
/// ordered by parameter number. They may be literals or runtime expressions;
/// implementations must treat unknown values conservatively.
#[derive(Debug, Clone, Copy)]
pub struct IndexMethodCostContext<'a> {
    pub pattern_idx: usize,
    pub base_table_rows: f64,
    pub arguments: &'a [ast::Expr],
}

/// Internal index state exposed only to test-helper builds.
#[cfg(feature = "test_helper")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexMethodTestStats {
    /// Persistent index-method storage format version.
    pub storage_format_version: Option<u32>,
    /// Persistent logical incarnation from the control record.
    pub index_incarnation: Option<u64>,
    /// Transactional manifest generation visible to this cursor.
    pub manifest_generation: Option<u64>,
    /// Files referenced by the visible manifest.
    pub manifest_file_count: Option<usize>,
    /// Number of physical files visible in the index method's storage.
    pub storage_file_count: usize,
    /// Number of searchable engine segments, when the method is segmented.
    pub segment_count: Option<usize>,
    /// Number of connection-local read snapshots retained by the method.
    pub cached_connection_count: Option<usize>,
    /// Resident bytes held by retained read-snapshot file caches.
    pub cached_bytes: Option<usize>,
    /// Complete snapshots rejected from retention because they exceed the
    /// aggregate cache budget by themselves.
    pub cache_admission_rejections: Option<usize>,
    /// Whether the method retained a committed writer for statement reuse.
    pub cached_writer: Option<bool>,
    /// Number of Tantivy writers constructed by this attachment.
    pub tantivy_writer_constructions: Option<usize>,
    /// Retained-writer cache lookups.
    pub writer_cache_lookups: Option<usize>,
    /// Retained-writer cache hits.
    pub writer_cache_hits: Option<usize>,
    /// Retained writers rejected because their owner or snapshot diverged.
    pub writer_cache_validation_failures: Option<usize>,
    /// Transaction-private retained writers discarded by rollback.
    pub writer_cache_rollback_discards: Option<usize>,
    /// Retained-writer cache lookups with no reusable entry.
    pub writer_cache_misses: Option<usize>,
    /// Read-snapshot cache lookups.
    pub read_cache_lookups: Option<usize>,
    /// Snapshot cache checkouts that avoided a backing-directory scan.
    pub read_cache_hits: Option<usize>,
    /// Read-snapshot cache lookups that required a full snapshot load.
    pub read_cache_misses: Option<usize>,
    /// Complete backing-directory snapshots loaded by the async scan.
    pub full_snapshot_loads: Option<usize>,
    /// Cross-snapshot control-record validations that reused a cached manifest.
    pub manifest_validation_hits: Option<usize>,
    /// Cross-snapshot control-record validations that rejected a stale cache.
    pub manifest_validation_misses: Option<usize>,
    /// Number of successful MVCC writer-lease acquisitions, including
    /// reentrant acquisition by the owning transaction.
    pub write_lease_acquisitions: Option<usize>,
    /// Number of writer-lease acquisitions rejected due to contention.
    pub write_lease_rejections: Option<usize>,
}

/// cursor opened for index method and capable of executing DML/DDL/DQL queries for the index method over fixed table
///
/// # Statement and transaction lifecycle
///
/// A cursor that wrote anything goes through these hooks, in this order:
///
/// 1. `stage_statement_commit` — stage every pending change durably (the
///    only fallible, I/O-capable phase), at the statement's halt.
/// 2. `on_statement_committed` — the statement's savepoint was released.
/// 3. Exactly one of three ends:
///    * `on_transaction_committed` — the transaction is durable;
///    * `on_transaction_rolled_back` — everything the transaction staged was
///      undone;
///    * replacement — a later statement in the same transaction opened a
///      newer cursor for the same attachment, so this one is closed without
///      either transaction outcome (the newer cursor receives it).
/// 4. `close`.
///
/// A failed statement gets `abort_statement` instead of steps 1–2.
///
/// The empty default bodies below are correct **only for a method that keeps
/// no transaction-private in-memory state** (everything lives in core-owned
/// backing storage, which the engine rolls back on its own). A method that
/// mirrors state in memory must implement every outcome hook: skipping
/// `on_transaction_rolled_back` silently publishes rolled-back work, and
/// skipping `stage_statement_commit` silently loses writes.
pub trait IndexMethodCursor: Send {
    /// create necessary components for index method (usually, this is a bunch of btree-s)
    fn create(&mut self, context: &IndexMethodContext) -> IOResultOr<()>;
    /// destroy components created in the create(...) call for index method
    fn destroy(&mut self, context: &IndexMethodContext) -> IOResultOr<()>;

    /// open necessary components for reading the index
    fn open_read(&mut self, context: &IndexMethodContext) -> IOResultOr<()>;
    /// open necessary components for writing the index
    fn open_write(&mut self, context: &IndexMethodContext) -> IOResultOr<()>;

    /// handle insert action
    /// "values" argument contains registers with values for index columns followed by rowid Integer register
    /// (e.g. for "CREATE INDEX i ON t USING method (x, z)" insert(...) call will have 3 registers in values: [x, z, rowid])
    fn insert(&mut self, values: &[Register]) -> IOResultOr<()>;
    /// handle delete action
    /// "values" argument contains registers with values for index columns followed by rowid Integer register
    /// (e.g. for "CREATE INDEX i ON t USING method (x, z)" insert(...) call will have 3 registers in values: [x, z, rowid])
    fn delete(&mut self, values: &[Register]) -> IOResultOr<()>;

    /// initialize query to the index method
    /// first element of "values" slice is the Integer register which holds index of the chosen [IndexMethodDefinition::patterns] by query planner
    /// next arguments of the "values" slice are values from the original query expression captured by pattern
    ///
    /// For example, for 2 patterns ["SELECT * FROM {table} LIMIT ?", "SELECT * FROM {table} WHERE x = ?"], query_start(...) call can have following arguments:
    /// - [Integer(0), Integer(10)] - pattern "SELECT * FROM {table} LIMIT ?" was chosen with LIMIT parameter equals to 10
    /// - [Integer(1), Text("turso")] - pattern "SELECT * FROM {table} WHERE x = ?" was chosen with equality comparison equals to "turso"
    ///
    /// Returns false if query will produce no rows (similar to VFilter/Rewind op codes)
    fn query_start(&mut self, values: &[Register]) -> IOResultOr<bool>;

    /// Moves cursor to the next response row
    /// Returns false if query exhausted all rows
    fn query_next(&mut self) -> IOResultOr<bool>;

    /// Return column with given idx (zero-based) from current row
    fn query_column(&mut self, idx: usize) -> IOResultOr<Value>;

    /// Return rowid of the original table row which corresponds to the current cursor row
    ///
    /// This method is used by tursodb core in order to "enrich" response from query pattern with additional fields from original table
    /// For example, consider pattern like this:
    ///
    /// > SELECT vector_distance_jaccard(embedding, ?) as d FROM table ORDER BY d LIMIT 10
    ///
    /// It can be used in more complex query:
    ///
    /// > SELECT name, comment, rating, vector_distance_jaccard(embedding, ?) as d FROM table ORDER BY d LIMIT 10
    ///
    /// In this case query planner will execute index method query first, and then
    /// enrich its result with name, comment, rating columns from original table accessing original row by its rowid
    /// returned from query_rowid(...) method
    fn query_rowid(&mut self) -> IOResultOr<Option<i64>>;

    /// Stage all pending index changes before the statement savepoint is
    /// released. Any fallible work or I/O belongs in this phase.
    fn stage_statement_commit(&mut self, _context: &IndexMethodContext) -> IOResultOr<()> {
        Ok(IOResult::Done(()))
    }

    /// Discard statement-owned in-memory work. This hook must not perform I/O.
    fn abort_statement(&mut self, _context: &IndexMethodContext) {}

    /// Publish transaction-private in-memory state after the statement
    /// savepoint has been released successfully. This hook is infallible and
    /// must not make uncommitted state visible to another transaction.
    fn on_statement_committed(&mut self, _context: &IndexMethodContext) {}

    /// Publish in-memory state after the database transaction commits.
    /// This hook is infallible and must not perform I/O.
    fn on_transaction_committed(&mut self, _context: &IndexMethodContext) {}

    /// Invalidate transaction-owned in-memory state after rollback.
    /// This hook is infallible and must not perform I/O.
    fn on_transaction_rolled_back(&mut self, _context: &IndexMethodContext) {}

    /// Invalidate state newer than a rolled-back savepoint.
    /// This hook is infallible and must not perform I/O.
    fn on_savepoint_rolled_back(&mut self, _context: &IndexMethodContext) {}

    /// Release resources without performing I/O or persistent writes.
    fn close(&mut self, _context: &IndexMethodContext) {}

    /// Optimize the index by merging segments or performing other maintenance.
    fn optimize(&mut self, _context: &IndexMethodContext) -> IOResultOr<()> {
        Ok(IOResult::Done(()))
    }

    /// Estimate the cost of executing a query with the given pattern.
    ///
    /// This method enables the optimizer to make cost-based decisions when choosing
    /// between custom index methods and traditional BTree indexes.
    fn estimate_cost(
        &self,
        context: &IndexMethodCostContext<'_>,
    ) -> Option<IndexMethodCostEstimate> {
        let _ = context;
        None
    }

    /// Return internal storage statistics for invariant tests.
    #[cfg(feature = "test_helper")]
    fn test_stats(&self) -> Result<Option<IndexMethodTestStats>> {
        Ok(None)
    }
}

pub(crate) struct TransactionIndexMethodCursor {
    pub(crate) cursor: Box<dyn IndexMethodCursor>,
    pub(crate) context: Arc<IndexMethodContext>,
}

impl TransactionIndexMethodCursor {
    pub(crate) fn same_attachment(&self, context: &IndexMethodContext) -> bool {
        self.context.database == context.database && self.context.index == context.index
    }
}

fn promote_to_mvcc_cursor(
    connection: &Arc<Connection>,
    database_id: usize,
    root_page: i64,
    cursor: Box<dyn CursorTrait>,
    cursor_type: MvccCursorType,
) -> Result<Box<dyn CursorTrait>> {
    let Some(mv_store) = connection.mv_store_for_db(database_id) else {
        return Ok(cursor);
    };
    let tx_id = connection.get_mv_tx_id_for_db(database_id).ok_or_else(|| {
        LimboError::InternalError(
            "index method opened an MVCC cursor without an active transaction".to_string(),
        )
    })?;
    Ok(Box::new(MvCursor::new(
        mv_store,
        connection,
        tx_id,
        root_page,
        cursor_type,
        cursor,
    )?))
}

fn btree_root_page(connection: &Connection, database_id: usize, root_page: i64) -> i64 {
    if root_page >= 0 {
        return root_page;
    }
    connection
        .mv_store_for_db(database_id)
        .map_or(root_page, |mv_store| mv_store.get_real_table_id(root_page))
}

/// Helper method to open a table cursor in an index method implementation.
pub(crate) fn open_table_cursor(
    connection: &Arc<Connection>,
    database_id: usize,
    table: &str,
) -> Result<Box<dyn CursorTrait>> {
    let pager = connection.get_pager_from_database_index(&database_id)?;
    let Some(table) = connection.with_schema(database_id, |schema| schema.get_table(table)) else {
        return Err(LimboError::InternalError(format!(
            "table {table} not found",
        )));
    };
    let root_page = table.get_root_page()?;
    let cursor = Box::new(BTreeCursor::new_table(
        pager,
        btree_root_page(connection, database_id, root_page),
        table.columns().len(),
    ));
    promote_to_mvcc_cursor(
        connection,
        database_id,
        root_page,
        cursor,
        MvccCursorType::Table,
    )
}

/// Helper method to open an index cursor in an index method implementation.
pub(crate) fn open_index_cursor<I, E>(
    connection: &Arc<Connection>,
    database_id: usize,
    table: &str,
    index: &str,
    keys: I,
) -> Result<Box<dyn CursorTrait>>
where
    I: IntoIterator<Item = KeyInfo, IntoIter = E>,
    E: ExactSizeIterator<Item = KeyInfo>,
{
    let pager = connection.get_pager_from_database_index(&database_id)?;
    let Some(scratch) = connection.with_schema(database_id, |schema| {
        schema.get_index(table, index).cloned()
    }) else {
        return Err(LimboError::InternalError(format!(
            "index {index} for table {table} not found",
        )));
    };
    let keys = keys.into_iter();
    let num_cols = keys.len();
    let index_info = Arc::new(IndexInfo::new(keys, false, num_cols, scratch.unique)?);
    let mut cursor = BTreeCursor::new(
        pager,
        btree_root_page(connection, database_id, scratch.root_page),
        num_cols,
    );
    cursor.index_info = Some(index_info.clone());
    promote_to_mvcc_cursor(
        connection,
        database_id,
        scratch.root_page,
        Box::new(cursor),
        MvccCursorType::Index(index_info),
    )
}

/// helper method to parse select patterns for [IndexMethodAttachment::definition] call
pub(crate) fn parse_patterns(patterns: &[&str]) -> Result<Vec<ast::Select>> {
    let mut parsed = Vec::new();
    for pattern in patterns {
        let mut parser = turso_parser::parser::Parser::new(pattern.as_bytes());
        let Some(ast) = parser.next() else {
            return Err(LimboError::ParseError(format!(
                "unable to parse pattern statement: {pattern}",
            )));
        };
        let ast = ast?;
        let ast::Cmd::Stmt(ast::Stmt::Select(select)) = ast else {
            return Err(LimboError::ParseError(format!(
                "only select patterns are allowed: {pattern}",
            )));
        };
        parsed.push(select);
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::{ensure_mvcc_support, IndexMethodDefinition, IndexMethodMvccSupport};

    fn definition(support: IndexMethodMvccSupport) -> IndexMethodDefinition<'static> {
        IndexMethodDefinition {
            method_name: "test_method",
            table_name: "test_table",
            index_name: "test_index",
            patterns: &[],
            backing_btree: false,
            results_materialized: true,
            mvcc_support: support,
        }
    }

    #[test]
    fn mvcc_support_declaration_rejects_unsupported_access() {
        let error = ensure_mvcc_support(&definition(IndexMethodMvccSupport::Unsupported), false)
            .unwrap_err();
        assert!(matches!(error, crate::LimboError::ParseError(_)));

        ensure_mvcc_support(&definition(IndexMethodMvccSupport::ReadOnly), false).unwrap();
        assert!(ensure_mvcc_support(&definition(IndexMethodMvccSupport::ReadOnly), true).is_err());
        ensure_mvcc_support(
            &definition(IndexMethodMvccSupport::TransactionalBackingStore),
            true,
        )
        .unwrap();
    }
}
