//! The [`Database`] object and everything needed to open one: open-time
//! options, the async open state machine, the process-wide registry that
//! keeps a single `Database` per file, and the per-connection catalog of
//! attached databases.

use crate::types::IOResultOr;
use crate::util::IOExt;
#[cfg(feature = "io_memory_yield")]
use crate::MemoryYieldIO;
#[cfg(all(feature = "fs", target_os = "linux", feature = "io_uring", not(miri)))]
use crate::UringIO;
#[cfg(all(
    feature = "fs",
    target_os = "windows",
    feature = "experimental_win_iocp",
    not(miri)
))]
use crate::WindowsIOCP;
use crate::{
    alloc, bail_corrupt_error,
    busy::BusyHandler,
    ext,
    incremental::view::AllViewsTxState,
    io, io_error, io_yield_one, mvcc,
    progress::ProgressHandler,
    return_if_io,
    schema::{self, Schema},
    stats::refresh_analyze_stats,
    storage::{
        self,
        checksum::CHECKSUM_REQUIRED_RESERVED_BYTES,
        encryption::{AtomicCipherMode, SQLITE_HEADER, TURSO_HEADER_PREFIX},
        journal_mode,
        page_cache::PageCache,
        page_transform::PageTransform,
        pager::{self, AutoVacuumMode, HeaderRef, HeaderRefMut},
        sqlite3_ondisk::{DatabaseHeader, PageSize, RawVersion, TextEncoding, Version},
    },
    sync::{
        self,
        atomic::{
            AtomicBool, AtomicI32, AtomicI64, AtomicIsize, AtomicU16, AtomicU64, AtomicU8,
            AtomicUsize, Ordering,
        },
        Arc, LazyLock, Mutex, RwLock, Weak,
    },
    turso_assert, turso_assert_greater_than_or_equal,
    types::{self, IOCompletions},
    vdbe::metrics::ConnectionMetrics,
    AtomicSyncMode, AtomicTempStore, AtomicTransactionState, Buffer, BufferPool, CipherMode,
    Completion, CompletionError, Connection, DatabaseStorage, Dialect, EncryptionKey, IOResult,
    InternalVirtualTable, LimboError, MemoryIO, MvStore, OpenFlags, Page, PageCodec, PageCodecId,
    PageRef, Pager, PlatformIO, Result, SymbolTable, SyncMode, SyscallIO, TempStore,
    TransactionState, VirtualTable, Wal, WalAutoActions, WalFile, WalFileShared, IO,
};
use arc_swap::{ArcSwap, ArcSwapOption};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
#[cfg(host_shared_wal)]
use std::path::Path;
#[cfg(host_shared_wal)]
use std::sync::OnceLock;
use std::{fmt, ops::Deref};
#[cfg(feature = "fs")]
use storage::database::DatabaseFile;
#[cfg(host_shared_wal)]
use storage::shared_wal_coordination::MappedSharedWalCoordination;
use tracing::{instrument, Level};

/// Configuration for database features
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DatabaseOpts {
    pub enable_views: bool,
    pub enable_custom_types: bool,
    pub enable_encryption: bool,
    pub enable_index_method: bool,
    pub enable_autovacuum: bool,
    pub enable_vacuum: bool,
    pub enable_attach: bool,
    pub enable_generated_columns: bool,
    pub enable_multiprocess_wal: bool,
    pub enable_without_rowid: bool,
    pub enable_experimental_mvcc_passive_checkpoint: bool,
    pub unsafe_testing: bool,
    pub(crate) enable_load_extension: bool,
}

impl DatabaseOpts {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(feature = "cli_only")]
    pub fn turso_cli(mut self) -> Self {
        self.enable_load_extension = true;
        self
    }

    pub fn with_views(mut self, enable: bool) -> Self {
        self.enable_views = enable;
        self
    }

    pub fn with_custom_types(mut self, enable: bool) -> Self {
        self.enable_custom_types = enable;
        self
    }

    pub fn with_encryption(mut self, enable: bool) -> Self {
        self.enable_encryption = enable;
        self
    }

    pub fn with_index_method(mut self, enable: bool) -> Self {
        self.enable_index_method = enable;
        self
    }

    pub fn with_autovacuum(mut self, enable: bool) -> Self {
        self.enable_autovacuum = enable;
        self
    }

    pub fn with_vacuum(mut self, enable: bool) -> Self {
        self.enable_vacuum = enable;
        self
    }

    pub fn with_experimental_mvcc_passive_checkpoint(mut self, enable: bool) -> Self {
        self.enable_experimental_mvcc_passive_checkpoint = enable;
        self
    }

    pub fn with_attach(mut self, enable: bool) -> Self {
        self.enable_attach = enable;
        self
    }

    pub fn with_generated_columns(mut self, enable: bool) -> Self {
        self.enable_generated_columns = enable;
        self
    }

    pub fn with_multiprocess_wal(mut self, enable: bool) -> Self {
        self.enable_multiprocess_wal = enable;
        self
    }

    pub fn with_without_rowid(mut self, enable: bool) -> Self {
        self.enable_without_rowid = enable;
        self
    }

    pub fn with_unsafe_testing(mut self, enable: bool) -> Self {
        self.unsafe_testing = enable;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedWalCoordinationOpenTelemetryMode {
    Exclusive,
    MultiProcess,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedWalOpenTelemetry {
    pub loaded_from_disk_scan: bool,
    pub reopened_max_frame: u64,
    pub reopened_nbackfills: u64,
    pub reopened_checkpoint_seq: u32,
    pub coordination_open_mode: Option<SharedWalCoordinationOpenTelemetryMode>,
    pub sanitized_backfill_proof_on_open: bool,
}

#[cfg(feature = "simulator")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedWalTestingSnapshot {
    pub max_frame: u64,
    pub nbackfills: u64,
    pub checkpoint_seq: u32,
    pub frame_index_overflowed: bool,
}

#[derive(Clone, Debug, Default)]
pub struct EncryptionOpts {
    pub cipher: String,
    pub hexkey: String,
}

impl EncryptionOpts {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Options for opening a [`Database`].
///
/// Mirrors the `std::fs::OpenOptions` idiom: configure, then open.
///
/// ```ignore
/// let db = Database::open(
///     io,
///     "app.db",
///     OpenOptions::new(Arc::new(SqliteDialect)).flags(OpenFlags::ReadOnly),
/// )?;
/// ```
#[derive(Clone)]
pub struct OpenOptions {
    /// Pre-opened database storage for the file at the database path.
    storage: Option<Arc<dyn DatabaseStorage>>,
    /// WAL file path override. Defaults to `"{path}-wal"`. Only honored by
    /// [`Database::do_open`]/[`Database::do_open_async`]; the registry-aware
    /// [`Database::open`]/[`Database::open_async`] reject it, because the
    /// process-wide registry keys on the default WAL for a path.
    wal_path: Option<String>,
    flags: OpenFlags,
    db_opts: DatabaseOpts,
    encryption: Option<EncryptionOpts>,
    page_codec: Option<Arc<dyn PageCodec>>,
    durable_storage: Option<Arc<dyn crate::mvcc::persistent_storage::DurableStorage>>,
    allocator: alloc::DynAllocator,
    /// SQL dialect the database is opened with. The dialect is fixed at open
    /// time and shared by every user of the registered instance; a registry
    /// hit with a different dialect is an error.
    dialect: Arc<dyn Dialect>,
}

impl OpenOptions {
    /// The dialect has no default: it is fixed at open time and shared by
    /// every user of the instance, so the caller must choose it explicitly.
    pub fn new(dialect: Arc<dyn Dialect>) -> Self {
        Self {
            storage: None,
            wal_path: None,
            flags: OpenFlags::default(),
            db_opts: DatabaseOpts::default(),
            encryption: None,
            page_codec: None,
            durable_storage: None,
            allocator: alloc::DynAllocator::default(),
            dialect,
        }
    }

    pub fn storage(mut self, storage: Arc<dyn DatabaseStorage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Override the WAL file path (defaults to `"{path}-wal"`). Only honored
    /// by [`Database::do_open`]/[`Database::do_open_async`]; passing it to the
    /// registry-aware entry points is an error.
    pub fn wal_path(mut self, wal_path: impl Into<String>) -> Self {
        self.wal_path = Some(wal_path.into());
        self
    }

    pub fn flags(mut self, flags: OpenFlags) -> Self {
        self.flags = flags;
        self
    }

    pub fn db_opts(mut self, db_opts: DatabaseOpts) -> Self {
        self.db_opts = db_opts;
        self
    }

    pub fn encryption(mut self, encryption: impl Into<Option<EncryptionOpts>>) -> Self {
        self.encryption = encryption.into();
        self
    }

    pub fn page_codec(mut self, page_codec: impl Into<Option<Arc<dyn PageCodec>>>) -> Self {
        self.page_codec = page_codec.into();
        self
    }

    pub fn durable_storage(
        mut self,
        durable_storage: impl Into<Option<Arc<dyn crate::mvcc::persistent_storage::DurableStorage>>>,
    ) -> Self {
        self.durable_storage = durable_storage.into();
        self
    }

    pub fn allocator(mut self, allocator: alloc::DynAllocator) -> Self {
        self.allocator = allocator;
        self
    }
}

/// Returns true for in memory databases (i.e. databases backed by MemoryIO)
///
/// Turso treats every path with the `:memory:` prefix as a named
/// in-memory database.
pub(crate) fn is_memory_like(path: &str) -> bool {
    path.starts_with(":memory:") || path.starts_with("file::memory:") || path.is_empty()
}

/// Creates a read completion for database header reads that checks for short reads.
/// The header is always on page 1, so this function hardcodes that page index.
fn new_header_read_completion(buf: Arc<Buffer>) -> Completion {
    let expected = buf.len();
    Completion::new_read(buf, move |res| {
        let Ok((_buf, bytes_read)) = res else {
            return None; // IO error already captured in completion
        };
        if (bytes_read as usize) < expected {
            tracing::error!(
                "short read on database header: expected {expected} bytes, got {bytes_read}"
            );
            return Some(CompletionError::ShortRead {
                page_idx: 1, // header is on page 1
                expected,
                actual: bytes_read as usize,
            });
        }
        None
    })
}

/// Phase tracking for async database opening
#[derive(Default, Debug)]
pub enum OpenDbAsyncPhase {
    #[default]
    Init,
    /// Drives `Database::header_validation` (header validation + WAL recovery)
    /// as a sub state machine so WAL recovery on open does not block.
    ValidatingHeader,
    ReadingHeader,
    LoadingSchema,
    BootstrapMvStore,
    Done,
}

/// Result of [`Database::read_db_header_buf`].
pub(crate) enum DbHeaderRead {
    /// The full 512-byte header was read.
    Full(Arc<Buffer>),
    /// The file is shorter than the header. Only the first `len` bytes of
    /// `buf` came from the file, and `err` is the short-read error.
    Short {
        buf: Arc<Buffer>,
        len: usize,
        err: CompletionError,
    },
}

impl DbHeaderRead {
    /// The bytes that actually came from the file.
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Full(buf) => buf.as_slice(),
            Self::Short { buf, len, .. } => &buf.as_slice()[..*len],
        }
    }

    /// The short-read error, if the file is shorter than the header.
    fn short_read(&self) -> Option<CompletionError> {
        match self {
            Self::Full(_) => None,
            Self::Short { err, .. } => Some(*err),
        }
    }
}

/// Sub state machine for [`Database::read_db_header_buf`], the non-blocking
/// read of the 512-byte database header. Not an open phase: it is driven
/// from connect-time pager init ([`Database::_init`] via `init_pager`), which
/// runs outside the open state machine.
#[derive(Default)]
pub(crate) enum DbHeaderReadState {
    #[default]
    Start,
    Reading {
        buf: Arc<Buffer>,
        completion: Completion,
    },
}

/// Sub state machine for [`Database::_init`], driven from
/// [`HeaderValidationState::Start`]. Builds the `Pager` (reading page-size /
/// reserved bytes from the DB header), begins a read transaction, then reads
/// page 1 to determine the autovacuum mode — all without blocking.
#[derive(Default)]
pub(crate) enum InitState {
    #[default]
    Start,
    /// Driving `init_pager` (its only IO is the DB-header read).
    InitPager(DbHeaderReadState),
    /// Pager built and read-tx open; reading page 1 for the autovacuum mode.
    ReadPage1 { pager: Box<Pager> },
}

/// Sub state machine for [`Database::header_validation`], driven from
/// [`OpenDbAsyncPhase::ValidatingHeader`]. Keeps WAL recovery on open
/// non-blocking by yielding through its IO instead of `io.block`.
enum HeaderValidationState {
    Start {
        init: InitState,
    },
    /// Pager created; (re-entrant) header reads + validation. Holds the owned
    /// `Pager` because `set_wal` needs `&mut Pager`; it is `Arc`-wrapped only
    /// once validation completes. `is_readonly`/`log_exists` are captured in
    /// `Start` (before the autovacuum check may force ReadOnly) so re-entry
    /// observes the original values.
    Validate {
        pager: Box<Pager>,
        is_readonly: bool,
        log_exists: bool,
    },
    /// A modified header (e.g. Legacy→WAL conversion) must be written to disk
    /// before the WAL is attached. `completion` is the in-flight write.
    WriteHeader {
        pager: Box<Pager>,
        page: PageRef,
        open_mv_store: bool,
        completion: Option<Completion>,
    },
    /// Open/recover the shared WAL. On non-host builds `driver` drives the
    /// `OpenSharedWal` recovery scan; on host builds the WAL is produced
    /// synchronously (native, where `io.step` pumps).
    OpenWal {
        pager: Box<Pager>,
        open_mv_store: bool,
        driver: Option<storage::wal::OpenSharedWal>,
        /// Set once the WAL of an empty database file has been thrown away
        /// (see the orphan-WAL handling in [`Database::header_validation`]).
        /// The WAL is reopened after that, and the reopened WAL must come
        /// back empty, so this can only ever happen once per open.
        discarded_orphan_wal: bool,
    },
}

impl Default for HeaderValidationState {
    fn default() -> Self {
        Self::Start {
            init: InitState::default(),
        }
    }
}

/// State machine for async database opening
pub struct OpenDbAsyncState {
    phase: OpenDbAsyncPhase,
    db: Option<Arc<Database>>,
    pager: Option<Arc<Pager>>,
    conn: Option<Arc<Connection>>,
    encryption_key: Option<EncryptionKey>,
    make_from_btree_state: schema::MakeFromBtreeState,
    /// Schema lock held during LoadingSchema phase to ensure atomicity across IO yields
    schema_guard: Option<sync::ArcMutexGuard<Arc<Schema>>>,
    /// Registry key for insertion (computed once at start)
    pub(crate) registry_key: Option<DatabaseKey>,
    /// The database being built, held across the ValidatingHeader phase yields
    /// before it is wrapped in an `Arc`.
    building_db: Option<Database>,
    /// Sub state machine for `header_validation`, driven in ValidatingHeader.
    header_validation_state: HeaderValidationState,
    /// The dedicated bootstrap connection used by `BootstrapMvStore`, held
    /// across yields from `MvStore::bootstrap_nonblock`.
    mvcc_bootstrap_conn: Option<Arc<Connection>>,
    /// Sub state machine for `MvStore::bootstrap_nonblock`, driven in
    /// `BootstrapMvStore`.
    mvcc_bootstrap_state: mvcc::database::BootstrapState,
}

impl Default for OpenDbAsyncState {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenDbAsyncState {
    pub fn new() -> Self {
        Self {
            phase: OpenDbAsyncPhase::Init,
            db: None,
            pager: None,
            conn: None,
            encryption_key: None,
            make_from_btree_state: schema::MakeFromBtreeState::new(),
            schema_guard: None,
            registry_key: None,
            building_db: None,
            header_validation_state: HeaderValidationState::default(),
            mvcc_bootstrap_conn: None,
            mvcc_bootstrap_state: mvcc::database::BootstrapState::default(),
        }
    }
}

impl Drop for OpenDbAsyncState {
    fn drop(&mut self) {
        if let Some(registry_key) = self.registry_key.take() {
            let mut registry = DATABASE_MANAGER.lock();
            registry.remove(&registry_key);
        }
    }
}

/// Per-path entry in the database registry.
pub(crate) enum RegistryEntry {
    /// Another caller is currently opening this database. Callers that see
    /// this should yield and retry later.
    Opening,
    /// The database has been opened and is (or was) live.
    Ready(Weak<Database>),
}

/// The database manager ensures that there is a single, shared
/// `Database` object per a database file. We need because it is not safe
/// to have multiple independent WAL files open because coordination
/// happens at process-level POSIX file advisory locks.
///
/// Uses parking_lot::Mutex instead of crate::sync::Mutex because this static
/// must persist across shuttle test iterations. Shuttle resets its execution
/// state between iterations, but static variables persist - using shuttle's
/// Mutex here would cause panics when the second iteration tries to lock a
/// mutex that belongs to a stale execution context.
/// Registry key for the process-wide database manager.
/// File-backed databases are keyed by their OS-level identity (dev, ino),
/// matching SQLite's inodeList approach. Shared in-memory databases use
/// their name as the key.
///
/// IMPORTANT: The mutex must only be held for brief HashMap operations, never
/// across I/O yields. Holding it across yields deadlocks single-threaded
/// event loops because the blocked thread
/// can never resume the coroutine that owns the lock.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum DatabaseKey {
    File(io::FileId),
    SharedMemory(String),
}

#[allow(clippy::type_complexity)]
pub(crate) static DATABASE_MANAGER: LazyLock<
    Arc<parking_lot::Mutex<HashMap<DatabaseKey, RegistryEntry>>>,
> = LazyLock::new(|| Arc::new(parking_lot::Mutex::new(HashMap::default())));

#[cfg(feature = "simulator")]
pub fn clear_database_registry() {
    DATABASE_MANAGER.lock().clear();
}

/// The `Database` object contains per database file state that is shared
/// between multiple connections.
///
/// Do that `Database` object is cached and can be long lived. DO NOT store anything sensitive like
/// encryption key here.
pub struct Database<A: alloc::ConcurrentAllocator = alloc::DynAllocator> {
    pub(crate) mv_store: ArcSwapOption<mvcc::MvStore<mvcc::MvccClock, A>>,
    pub(crate) mv_store_allocator: A,
    pub(crate) schema: Arc<Mutex<Arc<Schema>>>,
    pub db_file: Arc<dyn DatabaseStorage>,
    pub path: String,
    wal_path: String,
    pub io: Arc<dyn IO>,
    pub(crate) buffer_pool: Arc<BufferPool>,
    // Shared structures of a Database are the parts that are common to multiple threads that might
    // create DB connections.
    _shared_page_cache: Arc<RwLock<PageCache>>,

    /// Optional per-database MVCC durable storage override.
    ///
    /// When set, MVCC will use this implementation for logical-log durability
    /// (commit, sync, checkpoint thresholds, etc.) instead of the built-in storage.
    pub(crate) durable_storage: Option<Arc<dyn crate::mvcc::persistent_storage::DurableStorage>>,
    pub(crate) shared_wal: Arc<RwLock<WalFileShared>>,
    #[cfg(host_shared_wal)]
    shared_wal_coordination: OnceLock<Arc<MappedSharedWalCoordination>>,
    init_lock: Arc<Mutex<()>>,
    pub(crate) open_flags: OpenFlags,
    // Use parking lot RwLock here and not `crate::sync::RwLock` because it relies on `data_ptr` and that is experimental
    // in std.
    pub(crate) builtin_syms: parking_lot::RwLock<SymbolTable>,
    /// SQL dialect this database runs under, interpreting `sqlite_schema`
    /// SQL rows. Passed explicitly by every open path, fixed at open time,
    /// and shared by all connections because the parsed [`Schema`] is
    /// shared per database.
    dialect: Arc<dyn Dialect>,
    pub(crate) opts: DatabaseOpts,
    pub(crate) n_connections: AtomicUsize,
    /// Process-unique id minted at construction. Unlike the `Arc`'s heap
    /// address, this can never repeat within a process, so detach/reattach
    /// and close/reopen produce distinguishable values.
    pub(crate) incarnation: u64,

    /// In Memory Page 1 for Empty Dbs
    init_page_1: Arc<ArcSwapOption<Page>>,

    // Encryption
    encryption_cipher_mode: AtomicCipherMode,
    page_codec_id: Option<PageCodecId>,
}

// SAFETY: This needs to be audited for thread safety.
// See: https://github.com/tursodatabase/turso/issues/1552
crate::assert::assert_send_sync!(Database);

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("Database");
        debug_struct
            .field("path", &self.path)
            .field("open_flags", &self.open_flags);

        // Database state information
        let db_state_value = match &*self.init_page_1.load() {
            // If init_page1 exists, this means the DB is empty
            Some(_) => "uninitialized",
            None => "initialized",
        };
        debug_struct.field("db_state", &db_state_value);

        let mv_store_status = if self.get_mv_store().is_some() {
            "present"
        } else {
            "none"
        };
        debug_struct.field("mv_store", &mv_store_status);

        let init_lock_status = if self.init_lock.try_lock().is_some() {
            "unlocked"
        } else {
            "locked"
        };
        debug_struct.field("init_lock", &init_lock_status);

        let wal_status = match self.shared_wal.try_read() {
            Some(wal) if wal.metadata.enabled.load(Ordering::SeqCst) => "enabled",
            Some(_) => "disabled",
            None => "locked_for_write",
        };
        debug_struct.field("wal_state", &wal_status);

        // Page cache info (just basic stats, not full contents)
        let cache_info = match self._shared_page_cache.try_read() {
            Some(cache) => format!("( capacity {}, used: {} )", cache.capacity(), cache.len()),
            None => "locked".to_string(),
        };
        debug_struct.field("page_cache", &cache_info);

        debug_struct.field(
            "n_connections",
            &self
                .n_connections
                .load(crate::sync::atomic::Ordering::SeqCst),
        );
        debug_struct.finish()
    }
}

impl Database {
    /// Returns true if this database is backed by MemoryIO.
    pub fn is_in_memory_db(&self) -> bool {
        is_memory_like(&self.path)
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        opts: DatabaseOpts,
        flags: OpenFlags,
        path: impl Into<String>,
        wal_path: impl Into<String>,
        io: &Arc<dyn IO>,
        db_file: Arc<dyn DatabaseStorage>,
        encryption_opts: Option<EncryptionOpts>,
        mv_store_allocator: alloc::DynAllocator,
        page_codec_id: Option<PageCodecId>,
        dialect: Arc<dyn Dialect>,
    ) -> Result<Self> {
        let path = path.into();
        let wal_path = wal_path.into();
        let shared_wal = WalFileShared::new_noop();
        let mv_store = ArcSwapOption::empty();

        let db_size = db_file.size()?;

        let shared_page_cache = Arc::new(RwLock::new(PageCache::default()));
        let syms = SymbolTable::new();
        let arena_size = if std::env::var("TESTING").is_ok_and(|v| v.eq_ignore_ascii_case("true")) {
            BufferPool::TEST_ARENA_SIZE
        } else {
            BufferPool::DEFAULT_ARENA_SIZE
        };

        let encryption_cipher_mode = if let Some(encryption_opts) = encryption_opts {
            Some(CipherMode::try_from(encryption_opts.cipher.as_str())?)
        } else {
            None
        };

        let init_page_1 = if db_size == 0 {
            let default_page_1 = pager::default_page1(encryption_cipher_mode.as_ref());

            Some(default_page_1)
        } else {
            None
        };

        let enable_custom_types = opts.enable_custom_types || dialect.requires_custom_types();

        let db = Database {
            mv_store,
            mv_store_allocator,
            path,
            wal_path,
            schema: Arc::new(Mutex::new(Arc::new({
                let mut s = Schema::with_options(enable_custom_types, dialect.as_ref())?;
                s.generated_columns_enabled = opts.enable_generated_columns;
                s
            }))),
            _shared_page_cache: shared_page_cache,
            shared_wal,
            #[cfg(host_shared_wal)]
            shared_wal_coordination: OnceLock::new(),
            db_file,
            builtin_syms: parking_lot::RwLock::new(syms),
            dialect,
            io: io.clone(),
            open_flags: flags,
            init_lock: Arc::new(Mutex::new(())),
            opts,
            buffer_pool: BufferPool::begin_init(io, arena_size),
            n_connections: AtomicUsize::new(0),
            incarnation: {
                // Deliberately std, not crate::sync: this static outlives a
                // shuttle test execution, and a shuttle-tracked atomic that
                // survives into the next execution corrupts shuttle's vector
                // clocks (task ids restart, the stale clock is longer than
                // the new task table, and clock bookkeeping underflows).
                // A plain std atomic is fine here: the counter only mints
                // process-unique ids and needs no ordering guarantees.
                static NEXT_DATABASE_INCARNATION: std::sync::atomic::AtomicU64 =
                    std::sync::atomic::AtomicU64::new(1);
                NEXT_DATABASE_INCARNATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            },

            init_page_1: Arc::new(ArcSwapOption::new(init_page_1)),

            encryption_cipher_mode: AtomicCipherMode::new(
                encryption_cipher_mode.unwrap_or(CipherMode::None),
            ),
            page_codec_id,

            durable_storage: None,
        };

        db.register_global_builtin_extensions()
            .expect("unable to register global extensions");
        Ok(db)
    }

    /// Deprecated convenience shim: prefer [`Database::open`] with
    /// [`OpenOptions`]. Equivalent to
    /// `Database::open(io, path, OpenOptions::new(dialect))`. Kept for existing
    /// callers; new code should not use it.
    #[cfg(feature = "fs")]
    pub fn open_file(
        io: Arc<dyn IO>,
        path: &str,
        dialect: Arc<dyn Dialect>,
    ) -> Result<Arc<Database>> {
        Self::open(io, path, OpenOptions::new(dialect))
    }

    /// Open or retrieve a shared named in-memory database.
    /// Multiple connections to the same `name` share a single `Database`,
    /// matching SQLite's `file:name?mode=memory&cache=shared` semantics.
    #[cfg(feature = "fs")]
    pub fn open_shared_memory(name: &str, dialect: Arc<dyn Dialect>) -> Result<Arc<Database>> {
        let key = DatabaseKey::SharedMemory(name.to_string());

        {
            let registry = DATABASE_MANAGER.lock();
            if let Some(RegistryEntry::Ready(weak)) = registry.get(&key) {
                if let Some(db) = weak.upgrade() {
                    Self::check_registry_dialect(&db, dialect.as_ref())?;
                    return Ok(db);
                }
            }
        }
        // `:memory:` paths bypass DATABASE_MANAGER internally, so no deadlock.
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Self::open_file(io, ":memory:", dialect.clone())?;

        let mut registry = DATABASE_MANAGER.lock();
        if let Some(RegistryEntry::Ready(weak)) = registry.get(&key) {
            if let Some(existing) = weak.upgrade() {
                Self::check_registry_dialect(&existing, dialect.as_ref())?;
                return Ok(existing);
            }
        }
        registry.insert(key, RegistryEntry::Ready(Arc::downgrade(&db)));
        Ok(db)
    }

    #[cfg(feature = "fs")]
    #[cfg(host_shared_wal)]
    fn effective_open_flags_for_path(
        io: &Arc<dyn IO>,
        path: &str,
        flags: OpenFlags,
        opts: DatabaseOpts,
    ) -> Result<OpenFlags> {
        if !opts.enable_multiprocess_wal {
            return Ok(flags);
        }

        if is_memory_like(path) {
            return Err(LimboError::InvalidArgument(format!(
                "experimental multiprocess WAL is not supported for in-memory database path '{path}'"
            )));
        }
        if !io.supports_shared_wal_coordination() {
            return Err(LimboError::InvalidArgument(format!(
                "experimental multiprocess WAL is not supported by the active IO backend for '{path}'"
            )));
        }
        if !Self::path_allows_shared_wal_coordination(Path::new(path))? {
            return Err(LimboError::InvalidArgument(format!(
                "experimental multiprocess WAL is not supported on the filesystem backing '{path}'"
            )));
        }

        if !flags.contains(OpenFlags::ReadOnly) {
            return Ok(flags | OpenFlags::NoLock);
        }

        Ok(flags)
    }

    #[cfg(feature = "fs")]
    #[cfg(not(host_shared_wal))]
    fn effective_open_flags_for_path(
        _io: &Arc<dyn IO>,
        _path: &str,
        flags: OpenFlags,
        _opts: DatabaseOpts,
    ) -> Result<OpenFlags> {
        // On unsupported platforms, keep the flag as a no-op so generic
        // cross-platform helpers/tests can request multiprocess WAL without
        // breaking legacy single-process behavior.
        Ok(flags)
    }

    fn validate_external_page_codec_options(
        opts: DatabaseOpts,
        has_external_page_codec: bool,
    ) -> Result<()> {
        if has_external_page_codec && opts.enable_multiprocess_wal {
            return Err(LimboError::InvalidArgument(
                "external page codecs are not supported with experimental multiprocess WAL"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn validate_open_options(options: &OpenOptions) -> Result<()> {
        Self::validate_external_page_codec_options(options.db_opts, options.page_codec.is_some())?;
        if options.encryption.is_some() && options.page_codec.is_some() {
            return Err(LimboError::InvalidArgument(
                "built-in encryption cannot be combined with an external page codec".to_string(),
            ));
        }
        Ok(())
    }

    #[cfg(feature = "fs")]
    #[cfg(host_shared_wal)]
    pub(crate) fn reject_live_multiprocess_wal_for_legacy_open(
        io: &Arc<dyn IO>,
        path: &str,
        wal_path: Option<&str>,
        opts: DatabaseOpts,
    ) -> Result<()> {
        if opts.enable_multiprocess_wal
            || is_memory_like(path)
            || !io.supports_shared_wal_coordination()
            || !Self::path_allows_shared_wal_coordination(Path::new(path))?
        {
            return Ok(());
        }

        // The coordination file is derived from the WAL path, so probe the
        // configured WAL (not a hard-coded `{path}-wal`) or a custom-WAL open
        // would check the wrong coordination file and miss a live authority.
        let wal_path = wal_path
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{path}-wal"));
        let coordination_path = storage::wal::coordination_path_for_wal_path(&wal_path);
        let Some(authority) =
            MappedSharedWalCoordination::open_existing(io, Path::new(&coordination_path), 64)?
        else {
            return Ok(());
        };

        if matches!(
            authority.open_mode(),
            storage::shared_wal_coordination::SharedWalCoordinationOpenMode::MultiProcess
        ) {
            return Err(LimboError::LockingError(format!(
                "Failed opening database '{path}'. Database is already open with experimental multiprocess WAL in another process"
            )));
        }

        Ok(())
    }

    #[cfg(feature = "fs")]
    #[cfg(not(host_shared_wal))]
    pub(crate) fn reject_live_multiprocess_wal_for_legacy_open(
        _io: &Arc<dyn IO>,
        _path: &str,
        _wal_path: Option<&str>,
        _opts: DatabaseOpts,
    ) -> Result<()> {
        Ok(())
    }

    #[cfg(feature = "fs")]
    #[cfg(host_shared_wal)]
    fn reject_live_legacy_wal_for_multiprocess_open(
        io: &Arc<dyn IO>,
        path: &str,
        flags: OpenFlags,
        opts: DatabaseOpts,
    ) -> Result<()> {
        if !opts.enable_multiprocess_wal || flags.contains(OpenFlags::ReadOnly) {
            return Ok(());
        }

        let probe_flags = (flags | OpenFlags::Create) & !OpenFlags::NoLock & !OpenFlags::ReadOnly;
        match io.open_file(path, probe_flags, true) {
            Ok(_probe_file) => Ok(()),
            Err(LimboError::LockingError(_)) => Err(LimboError::LockingError(format!(
                "Failed opening database '{path}'. Database is already open without experimental multiprocess WAL in another process"
            ))),
            Err(err) => Err(err),
        }
    }

    #[cfg(feature = "fs")]
    #[cfg(not(host_shared_wal))]
    fn reject_live_legacy_wal_for_multiprocess_open(
        _io: &Arc<dyn IO>,
        _path: &str,
        _flags: OpenFlags,
        _opts: DatabaseOpts,
    ) -> Result<()> {
        Ok(())
    }

    /// Check that a registry hit was opened with the dialect the caller
    /// requested. The dialect is fixed at open time and shared by every
    /// user of the registered instance, so a mismatch is an error rather
    /// than a silent share.
    fn check_registry_dialect(db: &Database, requested: &dyn Dialect) -> Result<()> {
        let requested_name = requested.name();
        if db.dialect.name() != requested_name {
            return Err(LimboError::InvalidArgument(format!(
                "database is already open with dialect '{}'; requested '{}'",
                db.dialect.name(),
                requested_name
            )));
        }
        Ok(())
    }

    /// Look up a database in the process-wide registry by file identity.
    /// Returns the cached Database if found, with encryption validation.
    /// This avoids opening a file (and acquiring a file lock) when the
    /// database is already open in this process.
    fn lookup_in_registry(
        path: &str,
        encryption_opts: &Option<EncryptionOpts>,
        dialect: &dyn Dialect,
        page_codec: Option<&dyn PageCodec>,
    ) -> Result<Option<Arc<Database>>> {
        if is_memory_like(path) {
            return Ok(None);
        }
        let file_id = match io::get_file_id(path) {
            Ok(id) => id,
            Err(_) => return Ok(None), // file doesn't exist yet
        };
        let key = DatabaseKey::File(file_id);
        let registry = DATABASE_MANAGER.lock();
        let db = match registry.get(&key) {
            Some(RegistryEntry::Ready(weak)) => match weak.upgrade() {
                Some(db) => db,
                None => return Ok(None),
            },
            _ => return Ok(None),
        };

        // Validate encryption compatibility (key is not stored for security,
        // so we can only check cipher mode)
        let db_is_encrypted = !matches!(db.encryption_cipher_mode.get(), CipherMode::None);
        if db_is_encrypted && encryption_opts.is_none() {
            return Err(LimboError::InvalidArgument(
                "Database is encrypted but no encryption options provided".to_string(),
            ));
        }
        db.validate_page_codec(page_codec)?;

        Self::check_registry_dialect(&db, dialect)?;

        Ok(Some(db))
    }

    fn validate_page_codec(&self, page_codec: Option<&dyn PageCodec>) -> Result<()> {
        match (self.page_codec_id, page_codec) {
            (Some(_), None) => Err(LimboError::InvalidArgument(
                "Database was opened with an external page codec; reopen with a page codec"
                    .to_string(),
            )),
            (None, Some(_)) => Err(LimboError::InvalidArgument(
                "Database is already open without an external page codec".to_string(),
            )),
            (Some(expected), Some(codec)) if expected != codec.codec_id() => {
                Err(LimboError::InvalidArgument(
                    "page codec identity does not match the existing database".to_string(),
                ))
            }
            _ => Ok(()),
        }
    }

    /// Deprecated convenience shim: prefer [`Database::open`] with
    /// [`OpenOptions`]. Equivalent to `Database::open(io, path,
    /// OpenOptions::new(dialect).flags(flags).db_opts(opts).encryption(enc))`.
    /// Kept for existing callers; new code should not use it.
    #[cfg(feature = "fs")]
    pub fn open_file_with_flags(
        io: Arc<dyn IO>,
        path: &str,
        flags: OpenFlags,
        opts: DatabaseOpts,
        encryption_opts: Option<EncryptionOpts>,
        dialect: Arc<dyn Dialect>,
    ) -> Result<Arc<Database>> {
        Self::open(
            io,
            path,
            OpenOptions::new(dialect)
                .flags(flags)
                .db_opts(opts)
                .encryption(encryption_opts),
        )
    }

    /// Resolve `OpenOptions::storage` for a file-backed open when the caller
    /// did not supply pre-opened storage: optionally consult the registry, run
    /// the legacy/multiprocess WAL probes, open the file, and fill in
    /// `options.storage` and the effective flags in place. Returns `Some(db)`
    /// when a registry hit short-circuits the open (only possible when
    /// `use_registry` is set).
    #[cfg(feature = "fs")]
    fn resolve_default_storage(
        io: &Arc<dyn IO>,
        path: &str,
        options: &mut OpenOptions,
        use_registry: bool,
    ) -> Result<Option<Arc<Database>>> {
        // Check the registry before opening the file to avoid acquiring a file
        // lock that would conflict with an already-open Database in this process.
        if use_registry {
            if let Some(db) = Self::lookup_in_registry(
                path,
                &options.encryption,
                options.dialect.as_ref(),
                options.page_codec.as_deref(),
            )? {
                if options.durable_storage.is_some() && db.durable_storage.is_none() {
                    return Err(LimboError::InvalidArgument(
                        "database already open without custom durable storage; \
                         close the existing instance before reopening with a custom DurableStorage"
                            .to_string(),
                    ));
                }
                return Ok(Some(db));
            }
        }
        // Mixed legacy/multiprocess opens are incompatible, but the two modes
        // advertise themselves through different lock domains (`.tshm` vs DB
        // file lock). We therefore probe both directions around the actual file
        // open to narrow the TOCTOU window:
        //
        // 1. legacy open rejects an already-live multiprocess authority
        Self::reject_live_multiprocess_wal_for_legacy_open(
            io,
            path,
            options.wal_path.as_deref(),
            options.db_opts,
        )?;
        let effective_flags =
            Self::effective_open_flags_for_path(io, path, options.flags, options.db_opts)?;

        // 2. multiprocess open rejects an already-live legacy DB-file lock
        Self::reject_live_legacy_wal_for_multiprocess_open(
            io,
            path,
            options.flags,
            options.db_opts,
        )?;
        let file = io.open_file(path, effective_flags, true)?;

        // 3. legacy open re-checks after `open_file()` in case a multiprocess
        //    authority appeared between the initial probe and the actual open
        Self::reject_live_multiprocess_wal_for_legacy_open(
            io,
            path,
            options.wal_path.as_deref(),
            options.db_opts,
        )?;
        options.flags = effective_flags;
        options.storage = Some(Arc::new(DatabaseFile::new(file)));
        Ok(None)
    }

    #[cfg(not(feature = "fs"))]
    fn resolve_default_storage(
        _io: &Arc<dyn IO>,
        _path: &str,
        _options: &mut OpenOptions,
        _use_registry: bool,
    ) -> Result<Option<Arc<Database>>> {
        Err(LimboError::InvalidArgument(
            "OpenOptions::storage is required to open a database without the `fs` feature"
                .to_string(),
        ))
    }

    /// The registry-aware entry points reject a custom WAL path: the
    /// process-wide registry keys on the default WAL, so an instance reading a
    /// nonstandard WAL must go through [`Database::do_open`]/
    /// [`Database::do_open_async`] instead.
    fn reject_wal_path_for_registry_open(options: &OpenOptions) -> Result<()> {
        if options.wal_path.is_some() {
            return Err(LimboError::InvalidArgument(
                "OpenOptions::wal_path is only supported by Database::do_open/do_open_async, \
                 which skip the process-wide registry; the registry keys on the default WAL path"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Open a database with the given [`OpenOptions`].
    ///
    /// Drives the IO loop internally. When `OpenOptions::storage` is unset,
    /// opens the file at `path` (consulting the process-wide registry first).
    pub fn open(io: Arc<dyn IO>, path: &str, mut options: OpenOptions) -> Result<Arc<Database>> {
        // Reject before resolving default storage: a registry hit there would
        // otherwise return the cached default-WAL instance and silently ignore
        // the custom wal_path before open_async runs its own check.
        Self::reject_wal_path_for_registry_open(&options)?;
        Self::validate_open_options(&options)?;
        if options.storage.is_none() {
            if let Some(db) = Self::resolve_default_storage(&io, path, &mut options, true)? {
                return Ok(db);
            }
        }
        let mut state = OpenDbAsyncState::new();
        loop {
            match Self::open_async(&mut state, io.clone(), path, &options)? {
                IOResult::Done(db) => return Ok(db),
                IOResult::IO(io_completion) => {
                    io_completion.wait(&*io)?;
                }
            }
        }
    }

    /// IOResult-driven twin of [`Database::open`]: the caller drives the IO
    /// loop and passes `state` between calls. `OpenOptions::storage` must be
    /// set.
    ///
    /// This matters for the sync engine, which must yield on IO when the
    /// schema table spans multiple pages (potentially needing network IO to
    /// load them).
    ///
    /// Uses the database registry to ensure a single Database instance per
    /// file within a process; an `Opening` sentinel prevents concurrent opens
    /// of the same path without holding the mutex across I/O yields. Callers
    /// that need a second Database instance for one file (e.g. a copied or
    /// revert WAL) use [`Database::do_open_async`] with `OpenOptions::wal_path`;
    /// passing `wal_path` here is an error, because the registry keys on the
    /// default WAL path.
    pub fn open_async(
        state: &mut OpenDbAsyncState,
        io: Arc<dyn IO>,
        path: &str,
        options: &OpenOptions,
    ) -> IOResultOr<Arc<Database>> {
        Self::reject_wal_path_for_registry_open(options)?;
        Self::validate_open_options(options)?;
        let Some(storage) = options.storage.clone() else {
            return Err(LimboError::InvalidArgument(
                "OpenOptions::storage is required for Database::open_async".to_string(),
            )
            .into());
        };
        // Re-derive lock-mode flags from opts: multiprocess WAL must open the
        // WAL file with NoLock or the second process fails to lock `-wal`.
        // Callers may hand us default flags on every poll, so this runs each
        // time; the rewrite is idempotent. The raw do_open_async path does not
        // do this — it is reserved for the registry-aware entry point.
        #[cfg(feature = "fs")]
        let flags = Self::effective_open_flags_for_path(&io, path, options.flags, options.db_opts)?;
        #[cfg(not(feature = "fs"))]
        let flags = options.flags;

        // turso-sync-engine creates 2 databases with different names in the same IO if MemoryIO is used
        // in this case we need to bypass registry (as this is MemoryIO DB) but also preserve original distinction in names (e.g. :memory:-draft and :memory:-synced)
        // so, we bypass registry for all in memory dbs (i.e. db paths which starts with ":memory:")
        if matches!(state.phase, OpenDbAsyncPhase::Init) && !is_memory_like(path) {
            // Briefly lock the registry to check/reserve — never hold across I/O yields.
            let mut registry = DATABASE_MANAGER.lock();

            // Look up by file identity (dev, ino). If file doesn't exist
            // yet (CREATE mode), skip lookup — no cached entry is possible.
            if let Ok(file_id) = io.file_id(path) {
                let key = DatabaseKey::File(file_id);
                match registry.get(&key) {
                    Some(RegistryEntry::Ready(weak)) => {
                        if let Some(db) = weak.upgrade() {
                            tracing::debug!("took database {path:?} from the registry");

                            let db_is_encrypted =
                                !matches!(db.encryption_cipher_mode.get(), CipherMode::None);
                            if db_is_encrypted && options.encryption.is_none() {
                                return Err(LimboError::InvalidArgument(
                                    "Database is encrypted but no encryption options provided"
                                        .to_string(),
                                )
                                .into());
                            }
                            db.validate_page_codec(options.page_codec.as_deref())?;
                            Self::check_registry_dialect(&db, options.dialect.as_ref())?;
                            return Ok(IOResult::Done(db));
                        }
                        // Weak ref expired — treat as absent, fall through to insert Opening.
                        registry.insert(key.clone(), RegistryEntry::Opening);
                    }
                    Some(RegistryEntry::Opening) => {
                        // Another caller is already opening this path. Yield so the
                        // event loop can make progress and we retry later.
                        return Ok(IOResult::IO(types::IOCompletions(
                            io::Completion::new_yield(),
                        )));
                    }
                    None => {
                        // Not in registry — mark as Opening and proceed.
                        registry.insert(key.clone(), RegistryEntry::Opening);
                    }
                }
                state.registry_key = Some(key);
            }
            // Lock is dropped here — the Opening sentinel prevents concurrent opens
            // of the same path without holding the mutex across yields.
        }

        // Open the database (no registry lock held; never re-consults it).
        let result = Self::do_open_async_guarded(
            state,
            io.clone(),
            path,
            None,
            storage,
            flags,
            options.db_opts,
            options.encryption.clone(),
            options.durable_storage.clone(),
            options.page_codec.clone(),
            options.allocator.clone(),
            options.dialect.clone(),
        );

        match &result {
            Ok(IOResult::Done(db)) => {
                // Register the opened database and remove the Opening sentinel.
                if let Some(registry_key) = state.registry_key.take() {
                    let mut registry = DATABASE_MANAGER.lock();
                    registry.insert(registry_key, RegistryEntry::Ready(Arc::downgrade(db)));
                }
            }
            Err(_) => {
                // On error, remove the Opening sentinel so other callers can proceed.
                if let Some(registry_key) = state.registry_key.take() {
                    let mut registry = DATABASE_MANAGER.lock();
                    registry.remove(&registry_key);
                }
            }
            Ok(IOResult::IO(_)) => {}
        }
        result
    }

    /// Synchronous [`Database::do_open_async`] that drives the IO loop.
    ///
    /// Test-only helper for scenarios that intentionally open a second
    /// Database instance for one file (e.g. reading through a copied WAL);
    /// production code uses the registry-aware [`Database::open`].
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn do_open(io: Arc<dyn IO>, path: &str, mut options: OpenOptions) -> Result<Arc<Database>> {
        Self::validate_open_options(&options)?;
        if options.storage.is_none() {
            // `use_registry = false`: the raw path never consults the registry,
            // so this only opens the file and never returns a cached Database.
            Self::resolve_default_storage(&io, path, &mut options, false)?;
        }
        let mut state = OpenDbAsyncState::new();
        loop {
            match Self::do_open_async(&mut state, io.clone(), path, &options)? {
                IOResult::Done(db) => return Ok(db),
                IOResult::IO(io_completion) => {
                    io_completion.wait(&*io)?;
                }
            }
        }
    }

    /// Raw open that never consults the process-wide registry, driven by the
    /// caller's IO loop. This is the only entry point that honors
    /// `OpenOptions::wal_path`. Prefer [`Database::open_async`] unless you
    /// deliberately need a second Database instance for a file (e.g. the sync
    /// engine's revert WAL).
    pub fn do_open_async(
        state: &mut OpenDbAsyncState,
        io: Arc<dyn IO>,
        path: &str,
        options: &OpenOptions,
    ) -> IOResultOr<Arc<Database>> {
        Self::validate_open_options(options)?;
        let Some(storage) = options.storage.clone() else {
            return Err(LimboError::InvalidArgument(
                "OpenOptions::storage is required for Database::do_open_async".to_string(),
            )
            .into());
        };
        Self::do_open_async_guarded(
            state,
            io,
            path,
            options.wal_path.as_deref(),
            storage,
            options.flags,
            options.db_opts,
            options.encryption.clone(),
            options.durable_storage.clone(),
            options.page_codec.clone(),
            options.allocator.clone(),
            options.dialect.clone(),
        )
    }

    /// Run the open state machine and release the schema guard if it fails.
    /// Never touches the registry; both the registry-aware and raw entry
    /// points funnel through here.
    #[allow(clippy::too_many_arguments)]
    fn do_open_async_guarded(
        state: &mut OpenDbAsyncState,
        io: Arc<dyn IO>,
        path: &str,
        wal_path: Option<&str>,
        db_file: Arc<dyn DatabaseStorage>,
        flags: OpenFlags,
        opts: DatabaseOpts,
        encryption_opts: Option<EncryptionOpts>,
        durable_storage: Option<Arc<dyn crate::mvcc::persistent_storage::DurableStorage>>,
        page_codec: Option<Arc<dyn PageCodec>>,
        allocator: alloc::DynAllocator,
        dialect: Arc<dyn Dialect>,
    ) -> IOResultOr<Arc<Database>> {
        Self::validate_external_page_codec_options(opts, page_codec.is_some())?;
        if encryption_opts.is_some() && page_codec.is_some() {
            return Err(LimboError::InvalidArgument(
                "built-in encryption cannot be combined with an external page codec".to_string(),
            )
            .into());
        }
        let result = Self::do_open_async_internal(
            state,
            io,
            path,
            wal_path,
            db_file,
            flags,
            opts,
            encryption_opts,
            durable_storage,
            page_codec,
            allocator,
            dialect,
        );
        if result.is_err() {
            let _ = state.schema_guard.take();
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn do_open_async_internal(
        state: &mut OpenDbAsyncState,
        io: Arc<dyn IO>,
        path: &str,
        wal_path: Option<&str>,
        db_file: Arc<dyn DatabaseStorage>,
        flags: OpenFlags,
        opts: DatabaseOpts,
        encryption_opts: Option<EncryptionOpts>,
        durable_storage: Option<Arc<dyn crate::mvcc::persistent_storage::DurableStorage>>,
        page_codec: Option<Arc<dyn PageCodec>>,
        allocator: alloc::DynAllocator,
        dialect: Arc<dyn Dialect>,
    ) -> IOResultOr<Arc<Database>> {
        loop {
            tracing::debug!("do_open_async_internal: state.phase={:?}", state.phase);
            match &state.phase {
                OpenDbAsyncPhase::Init => {
                    // Parse encryption key from encryption_opts if provided
                    let encryption_key = if let Some(ref enc_opts) = encryption_opts {
                        Some(EncryptionKey::from_hex_string(&enc_opts.hexkey)?)
                    } else {
                        None
                    };

                    let wal_path = if let Some(wal_path) = wal_path {
                        wal_path
                    } else {
                        &format!("{path}-wal")
                    };
                    let mut db = Self::new(
                        opts,
                        flags,
                        path,
                        wal_path,
                        &io,
                        db_file.clone(),
                        encryption_opts.clone(),
                        allocator.clone(),
                        page_codec.as_deref().map(PageCodec::codec_id),
                        dialect.clone(),
                    )?;
                    db.durable_storage.clone_from(&durable_storage);

                    // Header validation + WAL recovery runs as a sub state
                    // machine in the ValidatingHeader phase so it can yield
                    // through IO instead of blocking. Stash the owned db and
                    // the parsed key for that phase.
                    state.building_db = Some(db);
                    state.encryption_key = encryption_key;
                    state.header_validation_state = HeaderValidationState::default();
                    state.phase = OpenDbAsyncPhase::ValidatingHeader;
                }

                OpenDbAsyncPhase::ValidatingHeader => {
                    let db = state
                        .building_db
                        .as_mut()
                        .expect("building_db must be set in Init phase");
                    let mut hv_state = std::mem::take(&mut state.header_validation_state);
                    let result = db.header_validation(
                        &mut hv_state,
                        state.encryption_key.as_ref(),
                        page_codec.as_ref(),
                    );
                    state.header_validation_state = hv_state;
                    let pager = return_if_io!(result);

                    let mut db = state
                        .building_db
                        .take()
                        .expect("building_db must be set in Init phase");

                    #[cfg(debug_assertions)]
                    {
                        let wal_enabled =
                            db.shared_wal.read().metadata.enabled.load(Ordering::SeqCst);
                        let mv_store_enabled = db.get_mv_store().is_some();
                        assert!(
                            db.is_readonly() || wal_enabled || mv_store_enabled,
                            "Either WAL or MVStore must be enabled"
                        );
                    }
                    let _ = &mut db;

                    // Wrap db in Arc before connecting
                    let db = Arc::new(db);

                    // Check: https://github.com/tursodatabase/turso/pull/1761#discussion_r2154013123
                    let conn = db._connect(
                        false,
                        Some(pager.clone()),
                        state.encryption_key.clone(),
                        page_codec.clone(),
                    )?;

                    // Acquire schema lock and hold it through ReadingHeader and LoadingSchema phases
                    // to ensure schema_version and make_from_btree are atomic
                    let guard = db.schema.lock_arc();

                    state.db = Some(db);
                    state.pager = Some(pager);
                    state.conn = Some(conn);
                    state.schema_guard = Some(guard);

                    state.phase = OpenDbAsyncPhase::ReadingHeader;
                }

                OpenDbAsyncPhase::ReadingHeader => {
                    let pager = state
                        .pager
                        .as_ref()
                        .expect("pager must be initialized in Init phase");
                    let header_schema_cookie =
                        return_if_io!(pager.with_header(|header| header.schema_cookie.get()));
                    let guard = state
                        .schema_guard
                        .as_mut()
                        .expect("schema_guard must be acquired in Init phase");
                    // We logically exclusively own schema via the Opening sentinel in the
                    // registry which prevents concurrent opens of the same path.
                    // At this point we already created a connection which cloned the schema
                    // internally, so we can't use get_mut here.
                    //
                    // it's not ideal but correctness is OK - before prepare connection call maybe_update_schema and in case of divergence update schema ref from the db + we always check connection cookie in the VDBE program itself
                    let schema = Schema::try_make_mut(guard)?;
                    schema.schema_version = header_schema_cookie;

                    state.phase = OpenDbAsyncPhase::LoadingSchema;
                }

                OpenDbAsyncPhase::LoadingSchema => {
                    let pager = state
                        .pager
                        .as_ref()
                        .expect("pager must be initialized in Init phase");
                    let conn = state
                        .conn
                        .as_ref()
                        .expect("conn must be initialized in Init phase");
                    let syms = conn.syms.read();

                    let guard = state
                        .schema_guard
                        .as_mut()
                        .expect("schema_guard must be acquired in Init phase");
                    // while we logically exclusively own schema as we hold DATABASE_MANAGER lock in the top level `open_async` function
                    // at the moment we already created connection which cloned the schema internally
                    // so, we can't use get_mut here for now
                    //
                    // it's not ideal but correctness is OK - before prepare connection call maybe_update_schema and in case of divergence update schema ref from the db + we always check connection cookie in the VDBE program itself
                    let schema = Schema::try_make_mut(guard)?;

                    let dialect = conn.dialect();
                    let result = schema.make_from_btree(
                        &mut state.make_from_btree_state,
                        None,
                        pager,
                        &syms,
                        dialect.as_ref(),
                    );

                    match result {
                        Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
                        Ok(IOResult::Done(())) => {
                            // Release the schema lock
                            state.schema_guard = None;
                        }
                        Err(err) if matches!(*err, LimboError::ExtensionError(_)) => {
                            let LimboError::ExtensionError(e) = *err else {
                                unreachable!()
                            };
                            // this means that a vtab exists and we no longer have the module loaded.
                            // we print a warning to the user to load the module
                            state.schema_guard = None;
                            tracing::warn!("open warning, failed to load extension: {e}");
                        }
                        Err(e) => return Err(e),
                    }

                    // Load custom types from __turso_internal_types if the table
                    // exists and custom types are enabled. The schema loaded by
                    // make_from_btree includes the table definition but not its
                    // contents. We need to read the stored type definitions so
                    // that DECODE/ENCODE and affinity metadata are available to
                    // all subsequent connections.
                    let conn = state
                        .conn
                        .as_ref()
                        .expect("conn must be initialized in Init phase");
                    if conn.experimental_custom_types_enabled() {
                        // Sync the connection's schema from the database so it
                        // can query __turso_internal_types.
                        conn.maybe_update_schema();
                        let load_result: Result<()> = (|| {
                            let type_sqls = conn.query_stored_type_definitions()?;
                            if !type_sqls.is_empty() {
                                let db = state
                                    .db
                                    .as_ref()
                                    .expect("db must be initialized in Init phase");
                                db.with_schema_mut(|schema| {
                                    schema.load_type_definitions(&type_sqls)
                                })?;
                            }
                            Ok(())
                        })();
                        if let Err(e) = load_result {
                            tracing::warn!("Failed to load custom types during open: {}", e);
                        }
                    }

                    state.phase = OpenDbAsyncPhase::BootstrapMvStore;
                }

                OpenDbAsyncPhase::BootstrapMvStore => {
                    let db = state
                        .db
                        .as_ref()
                        .expect("db must be initialized in Init phase");
                    let pager = state
                        .pager
                        .as_ref()
                        .expect("pager must be initialized in Init phase");

                    if let Some(mv_store) = db.get_mv_store().as_ref() {
                        // Create the dedicated bootstrap connection once and
                        // hold it across yields. Re-entry reuses the existing
                        // connection and the persisted `BootstrapState`.
                        if state.mvcc_bootstrap_conn.is_none() {
                            state.mvcc_bootstrap_conn = Some(db._connect(
                                true,
                                Some(pager.clone()),
                                state.encryption_key.clone(),
                                page_codec.clone(),
                            )?);
                        }
                        let conn = state.mvcc_bootstrap_conn.as_ref().expect("created above");
                        return_if_io!(
                            mv_store.bootstrap_nonblock(conn, &mut state.mvcc_bootstrap_state)
                        );
                        // Done — drop the bootstrap connection.
                        state.mvcc_bootstrap_conn = None;
                    }

                    state.phase = OpenDbAsyncPhase::Done;
                    return Ok(IOResult::Done(
                        state
                            .db
                            .take()
                            .expect("db must be initialized in Init phase"),
                    ));
                }

                OpenDbAsyncPhase::Done => {
                    panic!("do_open_async_internal called after completion");
                }
            }
        }
    }

    /// Necessary Pager initialization, so that we are prepared to read from Page 1.
    /// For encrypted databases, the encryption key must be provided to properly decrypt page 1.
    /// Blocking shim over [`Database::_init_nonblock`], retained for the
    /// synchronous callers (connection setup paths). The open state machine
    /// uses `_init_nonblock` directly so a fresh open never blocks here.
    pub(crate) fn _init(
        &self,
        encryption_key: Option<&EncryptionKey>,
        page_codec: Option<Arc<dyn PageCodec>>,
    ) -> Result<Pager> {
        let mut st = InitState::default();
        self.io
            .block(|| self._init_nonblock(&mut st, encryption_key, page_codec.as_ref()))
    }

    /// Necessary Pager initialization, so that we are prepared to read from
    /// Page 1. For encrypted databases, the encryption key must be provided to
    /// properly decrypt page 1. Non-blocking: drives `init_pager` (DB-header
    /// read) and the page-1 autovacuum read through their IO.
    pub(crate) fn _init_nonblock(
        &self,
        st: &mut InitState,
        encryption_key: Option<&EncryptionKey>,
        page_codec: Option<&Arc<dyn PageCodec>>,
    ) -> IOResultOr<Pager> {
        if encryption_key.is_some() && page_codec.is_some() {
            return Err(LimboError::InvalidArgument(
                "built-in encryption cannot be combined with an external page codec".to_string(),
            )
            .into());
        }
        loop {
            match st {
                InitState::Start => {
                    *st = InitState::InitPager(DbHeaderReadState::default());
                }
                InitState::InitPager(hdr_st) => {
                    let pager =
                        return_if_io!(self.init_pager(None, hdr_st, encryption_key, page_codec));
                    pager.enable_encryption(self.opts.enable_encryption);

                    // Set up encryption context BEFORE reading the header page.
                    // For encrypted databases, page 1 has:
                    // - Bytes 0-15: Turso magic header (replaces SQLite magic)
                    // - Bytes 16-100: Unencrypted header metadata
                    // - Bytes 100+: Encrypted content
                    // The encryption context is needed to properly decrypt page 1 when reopening.
                    if let Some(key) = encryption_key {
                        let cipher_mode = self.encryption_cipher_mode.get();
                        pager.set_encryption_context(cipher_mode, key)?;
                    } else if let Some(codec) = page_codec {
                        pager.set_page_codec(codec.clone())?;
                    }

                    // Start a read transaction before reading page 1 to prevent a concurrent
                    // checkpoint from truncating the WAL underneath bootstrap. Under heavy
                    // same-process connection churn, the shared WAL bootstrap path can
                    // briefly contend on short-lived in-process locks, so treat Busy here as
                    // a transient and retry rather than failing `connect()`.
                    let mut read_tx_attempts = 0u32;
                    loop {
                        match pager.begin_read_tx() {
                            Ok(()) => break,
                            Err(LimboError::Busy) => {
                                read_tx_attempts += 1;
                                if read_tx_attempts > 1 {
                                    return Err(LimboError::Busy.into());
                                }
                                pager.io.yield_now();
                            }
                            Err(err) => return Err(err.into()),
                        }
                    }

                    *st = InitState::ReadPage1 {
                        pager: Box::new(pager),
                    };
                }
                InitState::ReadPage1 { pager } => {
                    // Read page 1 within the read transaction to determine the
                    // autovacuum mode. The read tx stays open across an IO
                    // yield here (re-entry resumes the read); we only end it
                    // once the read completes or errors.
                    let mode = match HeaderRef::from_pager(pager) {
                        Ok(IOResult::Done(header_ref)) => {
                            let header = header_ref.borrow();
                            let validate_codec_header = || -> Result<()> {
                                if self.initialized() {
                                    let page_transform =
                                        pager.io_ctx.read().page_transform().clone();
                                    if let PageTransform::Codec(codec) = page_transform {
                                        let bootstrap_page_size =
                                            pager.get_page_size_unchecked().get() as usize;
                                        let bootstrap_reserved_space =
                                            pager.get_reserved_space().ok_or_else(|| {
                                                LimboError::InternalError(
                                                    "page codec reserved space was not initialized"
                                                        .to_string(),
                                                )
                                            })?;
                                        let decoded_page_size = header.page_size.get() as usize;
                                        if decoded_page_size != bootstrap_page_size {
                                            return Err(LimboError::InvalidArgument(format!(
                                            "page codec bootstrap page size {bootstrap_page_size} does not match decoded page-1 size {decoded_page_size}"
                                        )));
                                        }
                                        if header.reserved_space != bootstrap_reserved_space {
                                            return Err(LimboError::InvalidArgument(format!(
                                            "page codec bootstrap reserved space {bootstrap_reserved_space} does not match decoded page-1 reserved space {}",
                                            header.reserved_space
                                        )));
                                        }
                                        let required_reserved_space =
                                            codec.required_reserved_bytes();
                                        if header.reserved_space != required_reserved_space {
                                            return Err(LimboError::InvalidArgument(format!(
                                            "page codec requires exactly {required_reserved_space} reserved bytes, but decoded page 1 provides {}",
                                            header.reserved_space
                                        )));
                                        }
                                    }
                                }
                                Ok(())
                            };
                            if let Err(err) = validate_codec_header() {
                                pager.end_read_tx();
                                return Err(err.into());
                            }
                            if header.vacuum_mode_largest_root_page.get() > 0 {
                                if header.incremental_vacuum_enabled.get() > 0 {
                                    AutoVacuumMode::Incremental
                                } else {
                                    AutoVacuumMode::Full
                                }
                            } else {
                                AutoVacuumMode::None
                            }
                        }
                        Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
                        Err(err) => {
                            pager.end_read_tx();
                            return Err(err);
                        }
                    };

                    pager.end_read_tx();
                    pager.set_auto_vacuum_mode(mode);

                    let InitState::ReadPage1 { pager } = std::mem::take(st) else {
                        unreachable!("state is ReadPage1");
                    };
                    return Ok(IOResult::Done(*pager));
                }
            }
        }
    }

    /// Checks the Version numbers in the DatabaseHeader, and changes it according to the required options
    ///
    /// Will also open MVStore and WAL if needed.
    ///
    /// Driven as a sub state machine (see [`HeaderValidationState`]) from the
    /// `ValidatingHeader` open phase so that WAL recovery on open yields
    /// through its IO instead of blocking — this is what lets a fresh open
    /// make progress on runtimes (e.g. WASM) that cannot pump `io.step`
    /// synchronously.
    fn header_validation(
        &mut self,
        st: &mut HeaderValidationState,
        encryption_key: Option<&EncryptionKey>,
        page_codec: Option<&Arc<dyn PageCodec>>,
    ) -> IOResultOr<Arc<Pager>> {
        loop {
            match st {
                HeaderValidationState::Start { init } => {
                    // `_init` does not modify `open_flags` (the autovacuum
                    // override happens later in `Validate`), so capturing
                    // `is_readonly` across the `_init` yields is stable.
                    let pager =
                        return_if_io!(self._init_nonblock(init, encryption_key, page_codec));
                    let log_exists =
                        journal_mode::logical_log_exists(std::path::Path::new(&self.path));
                    let is_readonly = self.open_flags.contains(OpenFlags::ReadOnly);
                    turso_assert!(pager.wal.is_none(), "Pager should have no WAL yet");
                    *st = HeaderValidationState::Validate {
                        pager: Box::new(pager),
                        is_readonly,
                        log_exists,
                    };
                }
                HeaderValidationState::Validate {
                    pager,
                    is_readonly,
                    log_exists,
                } => {
                    let is_readonly = *is_readonly;
                    let log_exists = *log_exists;

                    // Re-entrant reads: both `with_header` and `from_pager`
                    // resume via their own state machines, and the autovacuum
                    // flag update is idempotent.
                    let is_autovacuumed_db = return_if_io!(pager.with_header(|header| {
                        header.vacuum_mode_largest_root_page.get() > 0
                            || header.incremental_vacuum_enabled.get() > 0
                    }));
                    if is_autovacuumed_db && !self.opts.enable_autovacuum {
                        tracing::warn!(
                            "Database has autovacuum enabled but --experimental-autovacuum flag is not set. Opening in readonly mode."
                        );
                        self.open_flags |= OpenFlags::ReadOnly;
                    }

                    let header: HeaderRefMut = return_if_io!(HeaderRefMut::from_pager(pager));
                    let header_mut = header.borrow_mut();

                    if !header_mut.text_encoding.is_utf8() {
                        return Err(LimboError::UnsupportedEncoding(
                            header_mut.text_encoding.to_string(),
                        )
                        .into());
                    }

                    let (read_version, write_version) =
                        { (header_mut.read_version, header_mut.write_version) };

                    if encryption_key.is_none() && header_mut.magic != SQLITE_HEADER {
                        tracing::error!(
                            "invalid value of database header magic bytes: {:?}",
                            header_mut.magic
                        );
                        return Err(LimboError::NotADB.into());
                    }
                    // when we open fresh db with encryption params - header will be SQLite at this point
                    if encryption_key.is_some()
                        && (header_mut.magic != SQLITE_HEADER
                            && !header_mut.magic.starts_with(TURSO_HEADER_PREFIX))
                    {
                        tracing::error!(
                            "invalid value of database header magic bytes: {:?}",
                            header_mut.magic
                        );
                        return Err(LimboError::NotADB.into());
                    }

                    // TODO: right now we don't support READ ONLY and no READ or WRITE in the Version header
                    // https://www.sqlite.org/fileformat.html#file_format_version_numbers
                    if read_version != write_version {
                        return Err(LimboError::Corrupt(format!(
                            "Read version `{read_version:?}` is not equal to Write version `{write_version:?} in database header`"
                        )).into());
                    }

                    let (read_version, _write_version) = (
                        read_version.to_version().map_err(|val| {
                            LimboError::Corrupt(format!("Invalid read_version: {val}"))
                        })?,
                        write_version.to_version().map_err(|val| {
                            LimboError::Corrupt(format!("Invalid write_version: {val}"))
                        })?,
                    );

                    // Validate fixed header fields per SQLite spec
                    if header_mut.max_embed_frac != 64 {
                        return Err(LimboError::Corrupt(format!(
                            "Invalid max_embed_frac: expected 64, got {}",
                            header_mut.max_embed_frac
                        ))
                        .into());
                    }
                    if header_mut.min_embed_frac != 32 {
                        return Err(LimboError::Corrupt(format!(
                            "Invalid min_embed_frac: expected 32, got {}",
                            header_mut.min_embed_frac
                        ))
                        .into());
                    }
                    if header_mut.leaf_frac != 32 {
                        return Err(LimboError::Corrupt(format!(
                            "Invalid leaf_frac: expected 32, got {}",
                            header_mut.leaf_frac
                        ))
                        .into());
                    }
                    let schema_format = header_mut.schema_format.get();
                    // If the database is completely empty, if it has no schema, then the schema format number can be zero.
                    if !(0..=4).contains(&schema_format) {
                        return Err(LimboError::Corrupt(format!(
                            "Invalid schema_format: expected 1-4, got {schema_format}"
                        ))
                        .into());
                    }
                    if !matches!(
                        header_mut.text_encoding,
                        TextEncoding::Unset
                            | TextEncoding::Utf8
                            | TextEncoding::Utf16Le
                            | TextEncoding::Utf16Be
                    ) {
                        return Err(LimboError::Corrupt(format!(
                            "Invalid text_encoding: {}",
                            header_mut.text_encoding
                        ))
                        .into());
                    }
                    if !matches!(
                        header_mut.text_encoding,
                        TextEncoding::Unset | TextEncoding::Utf8
                    ) {
                        return Err(LimboError::Corrupt(format!(
                            "Only utf8 text_encoding is supported by tursodb: got={}",
                            header_mut.text_encoding
                        ))
                        .into());
                    }

                    // Determine if we should open in MVCC mode based on the database header version
                    // MVCC is controlled only by the database header (set via PRAGMA journal_mode)
                    let open_mv_store = matches!(read_version, Version::Mvcc);
                    if open_mv_store && page_codec.is_some() {
                        return Err(LimboError::InvalidArgument(
                            "external page codecs are not supported with MVCC databases"
                                .to_string(),
                        )
                        .into());
                    }

                    // MVCC has no cross-process coordination: commit
                    // serialization, the logical-log append offset, and
                    // checkpoint exclusion are all process-local, so
                    // concurrent multiprocess access silently loses committed
                    // transactions and corrupts live views.
                    if open_mv_store && self.opts.enable_multiprocess_wal {
                        return Err(LimboError::InvalidArgument(format!(
                            "cannot open MVCC database '{}' with experimental multiprocess WAL: MVCC does not support multiprocess access",
                            self.path
                        )).into());
                    }

                    // Now check the Header Version to see which mode the DB file really is on
                    // Track if header was modified so we can write it to disk
                    let header_modified = match read_version {
                        Version::Legacy => {
                            if is_readonly {
                                tracing::warn!(
                                    "Database {} is opened in readonly mode, cannot convert Legacy mode to WAL. Running in Legacy mode.",
                                    self.path
                                );
                                false
                            } else {
                                // Convert Legacy to WAL mode
                                header_mut.read_version = RawVersion::from(Version::Wal);
                                header_mut.write_version = RawVersion::from(Version::Wal);
                                true
                            }
                        }
                        Version::Wal => false,
                        Version::Mvcc => false,
                    };

                    // In WAL mode, a logical log is always unexpected.
                    // In MVCC mode, WAL and logical-log coexistence can happen across interrupted checkpoint
                    // recovery and is reconciled in MvStore::bootstrap().
                    if !open_mv_store && log_exists {
                        return Err(LimboError::Corrupt(format!(
                            "MVCC logical log file exists for database {}, but database header indicates WAL mode. The database may be corrupted.",
                            self.path
                        )).into());
                    }

                    let page = header.page().clone();
                    // `header` (a cheap Arc<Page> wrapper, no lock) is dropped
                    // here; the page ref carries the (possibly modified) header
                    // buffer forward.
                    drop(header);

                    // Move the owned pager out of the state to build the next.
                    let HeaderValidationState::Validate { pager, .. } = std::mem::take(st) else {
                        unreachable!("state is Validate");
                    };
                    *st = if header_modified {
                        HeaderValidationState::WriteHeader {
                            pager,
                            page,
                            open_mv_store,
                            completion: None,
                        }
                    } else {
                        HeaderValidationState::OpenWal {
                            pager,
                            open_mv_store,
                            driver: None,
                            discarded_orphan_wal: false,
                        }
                    };
                }
                HeaderValidationState::WriteHeader {
                    pager,
                    page,
                    open_mv_store,
                    completion,
                } => {
                    // If header was modified, write it directly to disk before we attach the
                    // WAL / clear the cache (must hit the DB file, not the WAL).
                    let c = match completion.take() {
                        Some(c) => c,
                        None => storage::sqlite3_ondisk::begin_write_btree_page(pager, page, None)?,
                    };
                    if !c.succeeded() {
                        *completion = Some(c.clone());
                        io_yield_one!(c);
                    }
                    let open_mv_store = *open_mv_store;
                    let HeaderValidationState::WriteHeader { pager, .. } = std::mem::take(st)
                    else {
                        unreachable!("state is WriteHeader");
                    };
                    *st = HeaderValidationState::OpenWal {
                        pager,
                        open_mv_store,
                        driver: None,
                        discarded_orphan_wal: false,
                    };
                }
                HeaderValidationState::OpenWal {
                    open_mv_store,
                    driver,
                    discarded_orphan_wal,
                    ..
                } => {
                    // Always open shared WAL and set it in the Database and Pager.
                    // MVCC currently requires a WAL open to function.
                    let mut shared_wal = {
                        #[cfg(not(host_shared_wal))]
                        {
                            if driver.is_none() {
                                *driver = Some(WalFileShared::open_shared_if_exists_begin(
                                    &self.io,
                                    &self.wal_path,
                                    self.open_flags,
                                )?);
                            }
                            return_if_io!(driver.as_mut().expect("driver initialized above").poll())
                        }
                        #[cfg(host_shared_wal)]
                        {
                            // Native-only coordination path: `io.step` pumps
                            // synchronously here, so the blocking shims are
                            // fine. (Driver field is unused on host.)
                            let _ = &driver;
                            let flags = self.open_flags;
                            let shared_authority = self.open_shared_wal_coordination_for_open()?;
                            if let Some(authority) = shared_authority.as_ref() {
                                if !authority.frame_index_overflowed() {
                                    WalFileShared::open_shared_from_authority_if_exists(
                                        &self.io,
                                        &self.wal_path,
                                        flags,
                                        authority,
                                        &self.db_file,
                                    )?
                                } else {
                                    WalFileShared::open_shared_if_exists(
                                        &self.io,
                                        &self.wal_path,
                                        flags,
                                    )?
                                }
                            } else {
                                WalFileShared::open_shared_if_exists(
                                    &self.io,
                                    &self.wal_path,
                                    flags,
                                )?
                            }
                        }
                    };

                    // A WAL never belongs to a database file with zero pages:
                    // `Pager::allocate_page1` fsyncs page 1 to the main file
                    // before any commit can fsync WAL frames, so no crash of
                    // ours leaves frames next to an empty database file. When
                    // that pair shows up anyway, the database file was deleted
                    // (a common way to reset a database is to remove the `.db`
                    // and forget the `-wal`) or the image predates the page-1
                    // fsync. Either way the frames describe a database that no
                    // longer exists: replaying them would splice a dead
                    // database's pages into this one as soon as it grows past
                    // zero pages.
                    //
                    // Throw the orphan WAL away and open the database empty,
                    // which is what SQLite does — `pagerOpenWalIfPresent()`
                    // deletes a WAL it finds next to a zero-page database —
                    // and what upstream tests expect.
                    if shared_wal.read().last_checksum_and_max_frame().1 > 0
                        && self.db_file.size()? == 0
                    {
                        turso_assert!(
                            !*discarded_orphan_wal,
                            "orphan WAL reopened with frames after being discarded"
                        );
                        *discarded_orphan_wal = true;
                        tracing::warn!(
                            "discarding WAL '{}': it holds frames but database file '{}' has no pages, so the WAL does not belong to it",
                            self.wal_path,
                            self.path
                        );
                        if self.open_flags.contains(OpenFlags::ReadOnly) {
                            // A read-only open must not delete files, so just
                            // detach the WAL. This is the same state a
                            // read-only open reaches when there is no WAL file
                            // at all.
                            shared_wal = WalFileShared::new_noop();
                        } else {
                            drop(shared_wal);
                            *driver = None;
                            self.io.remove_file(&self.wal_path)?;
                            // Reopen the (now absent) WAL: `open_file` recreates
                            // it empty and recovery of a WAL shorter than its
                            // header finishes without any IO.
                            continue;
                        }
                    }

                    let open_mv_store = *open_mv_store;
                    let HeaderValidationState::OpenWal { mut pager, .. } = std::mem::take(st)
                    else {
                        unreachable!("state is OpenWal");
                    };

                    self.shared_wal = shared_wal;
                    let last_checksum_and_max_frame =
                        self.shared_wal.read().last_checksum_and_max_frame();
                    let wal =
                        self.build_wal(last_checksum_and_max_frame, pager.buffer_pool.clone())?;
                    pager.set_wal(wal);

                    // Clear page cache after attaching WAL since pages may have been cached
                    // from disk reads before WAL was attached. The WAL may contain newer
                    // versions of these pages (e.g., page 1 with updated schema_cookie).
                    pager.clear_page_cache(true);
                    pager.set_schema_cookie(None);

                    if open_mv_store {
                        let canonical_path = self.get_database_canonical_path();
                        let enc_ctx = pager.io_ctx.read().encryption_context().cloned();
                        let mv_store = journal_mode::open_mv_store(
                            self.io.clone(),
                            &canonical_path,
                            self.open_flags,
                            self.durable_storage.clone(),
                            enc_ctx,
                            self.mv_store_allocator.clone(),
                            self.experimental_mvcc_passive_checkpoint_enabled(),
                        )?;
                        self.mv_store.store(Some(mv_store));
                    }

                    return Ok(IOResult::Done(Arc::new(*pager)));
                }
            }
        }
    }

    pub fn get_database_canonical_path(&self) -> String {
        if self.is_in_memory_db() {
            // For in-memory databases, SQLite shows empty string
            String::new()
        } else {
            // For file databases, try show the full absolute path if that doesn't fail
            match std::fs::canonicalize(&self.path) {
                Ok(abs_path) => abs_path.to_string_lossy().to_string(),
                Err(_) => self.path.to_string(),
            }
        }
    }

    #[cfg(feature = "conn_raw_api")]
    /// Rebuild the process-local shared WAL view after a caller restores the
    /// database and WAL files outside the pager.
    pub fn reload_wal_after_external_restore(self: &Arc<Self>) -> Result<()> {
        if self.page_codec_id.is_some() {
            return Err(LimboError::InvalidArgument(
                "reloading a WAL after external restore is not supported with an external page codec"
                    .to_string(),
            ));
        }
        let flags = self.open_flags;
        #[cfg(host_shared_wal)]
        let shared_authority = self.open_shared_wal_coordination_for_open()?;
        #[cfg(not(host_shared_wal))]
        let shared_authority: Option<()> = None;

        let new_shared_wal = {
            #[cfg(host_shared_wal)]
            {
                if let Some(authority) = shared_authority.as_ref() {
                    if !authority.frame_index_overflowed() {
                        WalFileShared::open_shared_from_authority_if_exists(
                            &self.io,
                            &self.wal_path,
                            flags,
                            authority,
                            &self.db_file,
                        )?
                    } else {
                        WalFileShared::open_shared_if_exists(&self.io, &self.wal_path, flags)?
                    }
                } else {
                    WalFileShared::open_shared_if_exists(&self.io, &self.wal_path, flags)?
                }
            }
            #[cfg(not(host_shared_wal))]
            {
                WalFileShared::open_shared_if_exists(&self.io, &self.wal_path, flags)?
            }
        };
        let new_shared_wal = Arc::try_unwrap(new_shared_wal).map_err(|_| {
            LimboError::InternalError(
                "new WAL state unexpectedly shared during external restore reload".to_string(),
            )
        })?;
        self.shared_wal
            .write()
            .replace_after_external_restore(new_shared_wal.into_inner());
        if self.mvcc_enabled() || journal_mode::logical_log_exists(std::path::Path::new(&self.path))
        {
            let mv_store = journal_mode::open_mv_store(
                self.io.clone(),
                &self.path,
                self.open_flags,
                self.durable_storage.clone(),
                None,
                self.mv_store_allocator.clone(),
                self.experimental_mvcc_passive_checkpoint_enabled(),
            )?;
            self.mv_store.store(Some(mv_store.clone()));
            let mvcc_bootstrap_conn = self._connect(true, None, None, None)?;
            match mv_store.bootstrap(mvcc_bootstrap_conn.clone()) {
                Ok(()) => {}
                Err(LimboError::SchemaUpdated) => {
                    mvcc_bootstrap_conn.force_reparse_schema()?;
                    mv_store.bootstrap(mvcc_bootstrap_conn)?;
                }
                Err(error) => return Err(error),
            }
        } else {
            self.mv_store.store(None);
        }
        Ok(())
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn connect(self: &Arc<Database>) -> Result<Arc<Connection>> {
        self._connect(false, None, None, None)
    }

    /// Connect with an encryption key.
    /// Use this when opening an encrypted database where the key is known at connect time.
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn connect_with_encryption(
        self: &Arc<Database>,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Arc<Connection>> {
        self._connect(false, None, encryption_key, None)
    }

    /// Connect with an external page codec.
    ///
    /// The codec may contain sensitive key material, so it is installed only on
    /// the pager for this connection and is not cached on the shared `Database`.
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn connect_with_page_codec(
        self: &Arc<Database>,
        page_codec: Arc<dyn PageCodec>,
    ) -> Result<Arc<Connection>> {
        self._connect(false, None, None, Some(page_codec))
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn _connect(
        self: &Arc<Database>,
        is_mvcc_bootstrap_connection: bool,
        pager: Option<Arc<Pager>>,
        encryption_key: Option<EncryptionKey>,
        page_codec: Option<Arc<dyn PageCodec>>,
    ) -> Result<Arc<Connection>> {
        if self.page_codec_id.is_some() && page_codec.is_none() {
            return Err(LimboError::InvalidArgument(
                "database requires an external page codec".to_string(),
            ));
        }
        if self.page_codec_id.is_none() && page_codec.is_some() {
            return Err(LimboError::InvalidArgument(
                "database was opened without an external page codec".to_string(),
            ));
        }
        if let Some(page_codec) = page_codec.as_deref() {
            self.validate_page_codec(Some(page_codec))?;
        }
        let pager = if let Some(pager) = pager {
            pager
        } else {
            // Pass encryption key to _init so it can set up encryption context
            // before reading page 1. This is required for reopening encrypted databases.
            Arc::new(self._init(encryption_key.as_ref(), page_codec.clone())?)
        };
        let default_cache_size = pager
            .io
            .block(|| pager.with_header(|header| header.default_page_cache_size))
            .unwrap_or_default()
            .get();

        self._connect_with_pager_and_default_cache_size(
            is_mvcc_bootstrap_connection,
            pager,
            encryption_key,
            default_cache_size,
        )
    }

    pub(crate) fn _connect_with_pager_and_default_cache_size(
        self: &Arc<Database>,
        is_mvcc_bootstrap_connection: bool,
        pager: Arc<Pager>,
        encryption_key: Option<EncryptionKey>,
        default_cache_size: i32,
    ) -> Result<Arc<Connection>> {
        let page_size = pager.get_page_size_unchecked();
        let encryption_cipher = self.encryption_cipher_mode.get();
        let conn = Arc::new(Connection {
            db: self.clone(),
            pager: ArcSwap::new(pager),
            schema: RwLock::new(self.schema.lock().clone()),
            database_schemas: RwLock::new(HashMap::default()),
            auto_commit: AtomicBool::new(true),
            transaction_state: AtomicTransactionState::new(TransactionState::None),
            poisoned_tx: AtomicBool::new(false),
            last_insert_rowid: AtomicI64::new(0),
            changes: AtomicI64::new(0),
            total_changes: AtomicI64::new(0),
            syms: parking_lot::RwLock::new(SymbolTable::new()),
            _shared_cache: false,
            cache_size: AtomicI32::new(default_cache_size),
            page_size: AtomicU16::new(page_size.get_raw()),
            wal_auto_actions: AtomicU8::new(WalAutoActions::all_enabled().bits()),
            #[cfg(feature = "conn_raw_api")]
            portable_logical_changes_enabled: AtomicBool::new(false),
            #[cfg(feature = "conn_raw_api")]
            mvcc_log_metadata: RwLock::new(HashMap::default()),
            capture_data_changes: RwLock::new(None),
            cdc_transaction_id: AtomicI64::new(-1),
            closed: AtomicBool::new(false),
            temp: crate::connection::TempDbContext::new(),
            attached_databases: RwLock::new(DatabaseCatalog::new()),
            query_only: AtomicBool::new(false),
            vdbe_trace: AtomicBool::new(false),
            dml_require_where: AtomicBool::new(false),
            count_changes: AtomicBool::new(false),
            dqs_dml: AtomicBool::new(true),
            sequence_inner_retries: AtomicU64::new(0),
            mv_tx: RwLock::new(None),
            attached_mv_txs: RwLock::new(HashMap::default()),
            #[cfg(any(test, injected_yields))]
            yield_injector: RwLock::new(None),
            #[cfg(any(test, injected_yields))]
            failure_injector: RwLock::new(None),
            #[cfg(any(test, injected_yields))]
            yield_instance_id_counter: AtomicU64::new(1),
            view_transaction_states: AllViewsTxState::new(),
            metrics: RwLock::new(ConnectionMetrics::new()),
            nestedness: AtomicI32::new(0),
            compiling_triggers: RwLock::new(Vec::new()),
            executing_triggers: RwLock::new(Vec::new()),
            encryption_key: RwLock::new(encryption_key),
            encryption_cipher_mode: AtomicCipherMode::new(encryption_cipher),
            sync_mode: AtomicSyncMode::new(SyncMode::Full),
            temp_store: AtomicTempStore::new(TempStore::Default),
            data_sync_retry: AtomicBool::new(false),
            busy_handler: RwLock::new(BusyHandler::None),
            progress_handler: ProgressHandler::new(),
            query_timeout_ms: AtomicU64::new(0),
            interrupt_requested: AtomicBool::new(false),
            is_mvcc_bootstrap_connection: AtomicBool::new(is_mvcc_bootstrap_connection),
            full_column_names: AtomicBool::new(false),
            short_column_names: AtomicBool::new(true),
            #[cfg(feature = "simulator")]
            subquery_unnesting_mode: crate::connection::AtomicSubqueryUnnestingMode::new(
                crate::connection::SubqueryUnnestingMode::Auto,
            ),
            enable_load_extension: AtomicBool::new(self.can_load_extensions()),
            fk_pragma: AtomicBool::new(false),
            fk_deferred_violations: AtomicIsize::new(0),
            n_active_writes: AtomicI32::new(0),
            n_active_root_statements: AtomicI32::new(0),
            n_active_blob_statements: AtomicI32::new(0),
            statement_activity: Arc::new(Mutex::new(
                crate::connection::StatementActivity::default(),
            )),
            check_constraints_pragma: AtomicBool::new(false),
            vtab_txn_states: RwLock::new(HashSet::default()),
            index_method_tx_cursors: crate::sync::Mutex::new(Vec::new()),
            has_index_method_tx_cursors: crate::sync::atomic::AtomicBool::new(false),
            named_savepoints: RwLock::new(Vec::new()),
            schema_reparse_in_progress: AtomicBool::new(false),
            prepare_context_generation: AtomicU64::new(0),
            sequence_currvals: RwLock::new(HashMap::default()),
        });
        self.n_connections
            .fetch_add(1, crate::sync::atomic::Ordering::SeqCst);
        let builtin_syms = self.builtin_syms.read();
        // add built-in extensions symbols to the connection to prevent having to load each time
        conn.syms.write().extend(&builtin_syms);
        refresh_analyze_stats(&conn);
        Ok(conn)
    }

    pub fn is_readonly(&self) -> bool {
        self.open_flags.contains(OpenFlags::ReadOnly)
    }

    /// Non-blocking read of the 512-byte database file header (page 1's
    /// header region). Yields the read completion via the supplied state until
    /// it finishes. The header may not be complete, in which case a
    /// [`DbHeaderRead::Short`] is returned.
    fn read_db_header_buf(&self, st: &mut DbHeaderReadState) -> IOResultOr<DbHeaderRead> {
        loop {
            match st {
                DbHeaderReadState::Start => {
                    turso_assert!(
                        PageSize::MIN % 512 == 0,
                        "header read must be a multiple of 512 for O_DIRECT"
                    );
                    let buf = Arc::new(Buffer::new_temporary(PageSize::MIN as usize));
                    let c = new_header_read_completion(buf.clone());
                    let c = self.db_file.read_header(c)?;
                    *st = DbHeaderReadState::Reading { buf, completion: c };
                }
                DbHeaderReadState::Reading { completion, .. } => {
                    let err = completion.get_error();
                    if err.is_none() && !completion.succeeded() {
                        let c = completion.clone();
                        io_yield_one!(c);
                    }
                    let DbHeaderReadState::Reading { buf, .. } = std::mem::take(st) else {
                        unreachable!("header read state must be Reading here");
                    };
                    return match err {
                        None => Ok(IOResult::Done(DbHeaderRead::Full(buf))),
                        Some(err @ CompletionError::ShortRead { actual, .. }) => {
                            Ok(IOResult::Done(DbHeaderRead::Short {
                                buf,
                                len: actual,
                                err,
                            }))
                        }
                        Some(err) => Err(err.into()),
                    };
                }
            }
        }
    }

    /// Determine the actual page size, in order of preference:
    /// 1. From the WAL header if it exists and is initialized
    /// 2. From `header_page_size` (read from the DB header by the caller) if
    ///    the database is initialized
    ///
    /// Otherwise, fall back to, in order of preference:
    /// 1. From the requested page size if it is provided
    /// 2. PageSize::default(), i.e. 4096
    fn determine_actual_page_size(
        &self,
        shared_wal: &WalFileShared,
        requested_page_size: Option<usize>,
        header_page_size: Option<PageSize>,
    ) -> Result<PageSize> {
        if shared_wal.metadata.enabled.load(Ordering::SeqCst) {
            let size_in_wal = shared_wal.page_size();
            if size_in_wal != 0 {
                let Some(page_size) = PageSize::new(size_in_wal) else {
                    bail_corrupt_error!("invalid page size in WAL: {size_in_wal}");
                };
                return Ok(page_size);
            }
        }
        if let Some(page_size) = header_page_size {
            Ok(page_size)
        } else {
            let Some(size) = requested_page_size else {
                return Ok(PageSize::default());
            };
            let Some(page_size) = PageSize::new(size as u32) else {
                bail_corrupt_error!("invalid requested page size: {size}");
            };
            Ok(page_size)
        }
    }

    #[cfg(all(unix, target_pointer_width = "64", target_os = "macos"))]
    fn filesystem_type_allows_shared_wal(fs_type: &str) -> bool {
        // Network and distributed filesystems where mmap'd shared memory
        // cannot guarantee cross-process coherency.
        !matches!(
            fs_type,
            "nfs" | "smbfs" | "afpfs" | "webdav" | "cifs" | "acfs"
        )
    }

    #[cfg(all(
        unix,
        target_pointer_width = "64",
        not(any(target_os = "linux", target_os = "android")),
        not(target_os = "macos")
    ))]
    fn filesystem_type_allows_shared_wal(_fs_type: &str) -> bool {
        true
    }

    #[cfg(all(
        unix,
        target_pointer_width = "64",
        any(target_os = "linux", target_os = "android")
    ))]
    pub(crate) fn filesystem_magic_allows_shared_wal(filesystem_magic: libc::c_long) -> bool {
        const AFS_SUPER_MAGIC: libc::c_long = 0x5346_414f;
        const CIFS_SUPER_MAGIC: libc::c_long = 0xFF53_4D42u32 as libc::c_long;
        const CODA_SUPER_MAGIC: libc::c_long = 0x7375_7245;
        const CEPH_SUPER_MAGIC: libc::c_long = 0x00C3_6400;
        const GFS2_SUPER_MAGIC: libc::c_long = 0x0116_1970;
        const LUSTRE_SUPER_MAGIC: libc::c_long = 0x0BD0_0BD0;
        const NCP_SUPER_MAGIC: libc::c_long = 0x564c;
        const NFS_SUPER_MAGIC: libc::c_long = 0x6969;
        const OCFS2_SUPER_MAGIC: libc::c_long = 0x7461_636f;
        const SMB2_SUPER_MAGIC: libc::c_long = 0xFE53_4D42u32 as libc::c_long;
        const V9FS_SUPER_MAGIC: libc::c_long = 0x0102_1997;

        !matches!(
            filesystem_magic,
            AFS_SUPER_MAGIC
                | CIFS_SUPER_MAGIC
                | CODA_SUPER_MAGIC
                | CEPH_SUPER_MAGIC
                | GFS2_SUPER_MAGIC
                | LUSTRE_SUPER_MAGIC
                | NCP_SUPER_MAGIC
                | NFS_SUPER_MAGIC
                | OCFS2_SUPER_MAGIC
                | SMB2_SUPER_MAGIC
                | V9FS_SUPER_MAGIC
        )
    }

    #[cfg(all(
        unix,
        target_pointer_width = "64",
        any(target_os = "linux", target_os = "android")
    ))]
    pub(crate) fn path_allows_shared_wal_coordination(path: &Path) -> Result<bool> {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let probe_path = if path.exists() {
            path
        } else {
            path.parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."))
        };
        let c_path = CString::new(probe_path.as_os_str().as_bytes()).map_err(|_| {
            LimboError::InvalidArgument(format!(
                "path contains interior NUL bytes: {}",
                probe_path.display()
            ))
        })?;
        let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
        let rc = unsafe { libc::statfs(c_path.as_ptr(), stat.as_mut_ptr()) };
        if rc != 0 {
            return Err(io_error(
                std::io::Error::last_os_error(),
                "statfs shared WAL coordination path",
            ));
        }
        let stat = unsafe { stat.assume_init() };
        Ok(Self::filesystem_magic_allows_shared_wal(
            stat.f_type as libc::c_long,
        ))
    }

    #[cfg(all(
        unix,
        target_pointer_width = "64",
        not(any(target_os = "linux", target_os = "android"))
    ))]
    pub(crate) fn path_allows_shared_wal_coordination(path: &Path) -> Result<bool> {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let probe_path = if path.exists() {
            path
        } else {
            path.parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."))
        };
        let c_path = CString::new(probe_path.as_os_str().as_bytes()).map_err(|_| {
            LimboError::InvalidArgument(format!(
                "path contains interior NUL bytes: {}",
                probe_path.display()
            ))
        })?;
        let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
        let rc = unsafe { libc::statfs(c_path.as_ptr(), stat.as_mut_ptr()) };
        if rc != 0 {
            return Err(io_error(
                std::io::Error::last_os_error(),
                "statfs shared WAL coordination path",
            ));
        }
        let stat = unsafe { stat.assume_init() };
        // macOS and other BSDs expose the filesystem type as a
        // null-terminated string in f_fstypename rather than an
        // integer magic number.
        let fs_type = unsafe {
            std::ffi::CStr::from_ptr(stat.f_fstypename.as_ptr())
                .to_str()
                .unwrap_or("")
        };
        Ok(Self::filesystem_type_allows_shared_wal(fs_type))
    }

    #[cfg(all(target_os = "windows", target_pointer_width = "64"))]
    pub(crate) fn path_allows_shared_wal_coordination(path: &Path) -> Result<bool> {
        use std::iter::once;
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{GetDriveTypeW, GetVolumePathNameW};

        const DRIVE_REMOVABLE: u32 = 2;
        const DRIVE_FIXED: u32 = 3;
        const DRIVE_REMOTE: u32 = 4;
        const DRIVE_RAMDISK: u32 = 6;

        let probe_path = if path.exists() {
            path.to_path_buf()
        } else {
            path.parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf()
        };
        let probe_path = if probe_path.is_absolute() {
            probe_path
        } else {
            std::env::current_dir()
                .map_err(|err| io_error(err, "resolve shared WAL coordination path"))?
                .join(probe_path)
        };
        let probe_path_wide: Vec<u16> = probe_path
            .as_os_str()
            .encode_wide()
            .chain(once(0))
            .collect();
        let mut volume_path = vec![0u16; 261];
        let result = unsafe {
            GetVolumePathNameW(
                probe_path_wide.as_ptr(),
                volume_path.as_mut_ptr(),
                volume_path.len() as u32,
            )
        };
        if result == 0 {
            return Err(io_error(
                std::io::Error::last_os_error(),
                "GetVolumePathNameW shared WAL coordination path",
            ));
        }

        let drive_type = unsafe { GetDriveTypeW(volume_path.as_ptr()) };
        Ok(
            matches!(drive_type, DRIVE_FIXED | DRIVE_RAMDISK | DRIVE_REMOVABLE)
                && drive_type != DRIVE_REMOTE,
        )
    }

    #[cfg(host_shared_wal)]
    pub(crate) fn shared_wal_coordination(
        &self,
    ) -> Result<Option<Arc<MappedSharedWalCoordination>>> {
        let shared_wal = self.shared_wal.read();
        if !shared_wal.metadata.enabled.load(Ordering::Acquire) {
            return Ok(None);
        }
        drop(shared_wal);
        self.open_shared_wal_coordination_inner()
    }

    #[cfg(not(host_shared_wal))]
    pub(crate) fn shared_wal_coordination(&self) -> Result<Option<()>> {
        Ok(None)
    }

    #[cfg(host_shared_wal)]
    pub(crate) fn open_shared_wal_coordination_for_open(
        &self,
    ) -> Result<Option<Arc<MappedSharedWalCoordination>>> {
        self.open_shared_wal_coordination_inner()
    }

    #[cfg(host_shared_wal)]
    fn open_shared_wal_coordination_inner(
        &self,
    ) -> Result<Option<Arc<MappedSharedWalCoordination>>> {
        if !self.opts.enable_multiprocess_wal {
            return Ok(None);
        }
        if !self.io.supports_shared_wal_coordination() {
            return Err(LimboError::InvalidArgument(format!(
                "experimental multiprocess WAL is not supported by the active IO backend for '{}'",
                self.path
            )));
        }
        if is_memory_like(&self.path) || is_memory_like(&self.wal_path) {
            return Err(LimboError::InvalidArgument(format!(
                "experimental multiprocess WAL is not supported for in-memory database path '{}'",
                self.path
            )));
        }
        if !Self::path_allows_shared_wal_coordination(Path::new(&self.path))? {
            return Err(LimboError::InvalidArgument(format!(
                "experimental multiprocess WAL is not supported on the filesystem backing '{}'",
                self.path
            )));
        }
        if let Some(authority) = self.shared_wal_coordination.get() {
            return Ok(Some(authority.clone()));
        }

        let path = storage::wal::coordination_path_for_wal_path(&self.wal_path);
        let authority = if self.open_flags.contains(OpenFlags::ReadOnly) {
            let Some(authority) = MappedSharedWalCoordination::open_existing(
                &self.io,
                std::path::Path::new(&path),
                64,
            )?
            else {
                // Read-only opens cannot create `.tshm`. If no shared
                // coordination file exists, degrade to the legacy read-only WAL
                // path rather than failing the open. This keeps binding-level
                // option plumbing advisory for readers while writable opens
                // still enforce the stricter multiprocess contract.
                return Ok(None);
            };
            Arc::new(authority)
        } else {
            Arc::new(MappedSharedWalCoordination::create_or_open(
                &self.io,
                std::path::Path::new(&path),
                64,
            )?)
        };
        let _ = self.shared_wal_coordination.set(authority.clone());
        Ok(Some(
            self.shared_wal_coordination
                .get()
                .cloned()
                .unwrap_or(authority),
        ))
    }

    pub fn shared_wal_open_telemetry(&self) -> Result<SharedWalOpenTelemetry> {
        let shared_wal = self.shared_wal.read();
        let loaded_from_disk_scan = shared_wal
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire);
        let reopened_max_frame = shared_wal.metadata.max_frame.load(Ordering::Acquire);
        let reopened_nbackfills = shared_wal.metadata.nbackfills.load(Ordering::Acquire);
        let reopened_checkpoint_seq = shared_wal.metadata.wal_header.lock().checkpoint_seq;
        drop(shared_wal);

        #[cfg(host_shared_wal)]
        let (coordination_open_mode, sanitized_backfill_proof_on_open) =
            if let Some(authority) = self.shared_wal_coordination()? {
                let mode = match authority.open_mode() {
                storage::shared_wal_coordination::SharedWalCoordinationOpenMode::Exclusive => {
                    SharedWalCoordinationOpenTelemetryMode::Exclusive
                }
                storage::shared_wal_coordination::SharedWalCoordinationOpenMode::MultiProcess => {
                    SharedWalCoordinationOpenTelemetryMode::MultiProcess
                }
            };
                (Some(mode), authority.sanitized_backfill_proof_on_open())
            } else {
                (None, false)
            };
        #[cfg(not(host_shared_wal))]
        let (coordination_open_mode, sanitized_backfill_proof_on_open) = (None, false);

        Ok(SharedWalOpenTelemetry {
            loaded_from_disk_scan,
            reopened_max_frame,
            reopened_nbackfills,
            reopened_checkpoint_seq,
            coordination_open_mode,
            sanitized_backfill_proof_on_open,
        })
    }

    #[cfg(feature = "simulator")]
    pub fn shared_wal_snapshot_for_testing(&self) -> Result<Option<SharedWalTestingSnapshot>> {
        #[cfg(host_shared_wal)]
        if let Some(authority) = self.shared_wal_coordination()? {
            let snapshot = authority.snapshot();
            return Ok(Some(SharedWalTestingSnapshot {
                max_frame: snapshot.max_frame,
                nbackfills: snapshot.nbackfills,
                checkpoint_seq: snapshot.checkpoint_seq,
                frame_index_overflowed: authority.frame_index_overflowed(),
            }));
        }

        Ok(None)
    }

    #[cfg(feature = "simulator")]
    pub fn shared_wal_find_frame_for_testing(&self, page_id: u64) -> Result<Option<u64>> {
        #[cfg(host_shared_wal)]
        if let Some(authority) = self.shared_wal_coordination()? {
            let snapshot = authority.snapshot();
            return Ok(authority.find_frame(page_id, 0, snapshot.max_frame, None));
        }

        Ok(None)
    }

    #[cfg(feature = "simulator")]
    pub fn local_wal_find_frame_for_testing(&self, page_id: u64) -> Result<Option<u64>> {
        let shared = self.shared_wal.read();
        let max_frame = shared.metadata.max_frame.load(Ordering::Acquire);
        let frame_cache = shared.runtime.frame_cache.lock();
        Ok(frame_cache.get(&page_id).and_then(|frames| {
            frames
                .iter()
                .rfind(|&&frame_id| frame_id <= max_frame)
                .copied()
        }))
    }

    #[cfg(feature = "simulator")]
    pub fn local_wal_max_frame_for_testing(&self) -> Result<u64> {
        Ok(self
            .shared_wal
            .read()
            .metadata
            .max_frame
            .load(Ordering::Acquire))
    }

    #[cfg(feature = "simulator")]
    pub fn clear_backfill_proof_for_testing(&self) -> Result<()> {
        #[cfg(host_shared_wal)]
        {
            let authority = self.shared_wal_coordination()?.ok_or_else(|| {
                LimboError::InternalError("shared WAL authority is unavailable".into())
            })?;
            authority.clear_backfill_proof();
            Ok(())
        }

        #[cfg(not(host_shared_wal))]
        {
            Err(LimboError::InternalError(
                "shared WAL authority is unavailable on this platform".into(),
            ))
        }
    }

    pub(crate) fn build_wal(
        &self,
        last_checksum_and_max_frame: ((u32, u32), u64),
        buffer_pool: Arc<BufferPool>,
    ) -> Result<Arc<dyn Wal>> {
        #[cfg(host_shared_wal)]
        if let Some(authority) = self.shared_wal_coordination()? {
            return Ok(Arc::new(WalFile::new_with_shared_coordination(
                self.io.clone(),
                self.shared_wal.clone(),
                authority,
                last_checksum_and_max_frame,
                buffer_pool,
            )));
        }

        Ok(Arc::new(WalFile::new(
            self.io.clone(),
            self.shared_wal.clone(),
            last_checksum_and_max_frame,
            buffer_pool,
        )))
    }

    fn init_pager(
        &self,
        requested_page_size: Option<usize>,
        hdr_st: &mut DbHeaderReadState,
        encryption_key: Option<&EncryptionKey>,
        page_codec: Option<&Arc<dyn PageCodec>>,
    ) -> IOResultOr<Pager> {
        let cipher = self.encryption_cipher_mode.get();
        let encrypted = encryption_key.is_some() || !matches!(cipher, CipherMode::None);

        // For an existing (initialized) database, read the 512-byte header
        // once (non-blocking) and recover both the reserved-space byte and the
        // on-disk page size from it.
        let (header_reserved_bytes, header_page_size) = if self.initialized() {
            let header = return_if_io!(self.read_db_header_buf(hdr_st));
            if let Some(codec) = page_codec {
                // A codec may transform the magic bytes, so a short codec
                // file cannot be judged here.
                if let Some(err) = header.short_read() {
                    return Err(err.into());
                }
                let header_info = codec.bootstrap_page_info(header.bytes())?;
                let page_size_u32 = u32::try_from(header_info.page_size).map_err(|_| {
                    LimboError::InvalidArgument(format!(
                        "page codec reported invalid page size {}",
                        header_info.page_size
                    ))
                })?;
                let Some(page_size) = PageSize::new(page_size_u32) else {
                    return Err(LimboError::InvalidArgument(format!(
                        "page codec reported invalid page size {}",
                        header_info.page_size
                    ))
                    .into());
                };
                if !page_size.has_valid_reserved_space(header_info.reserved_space) {
                    return Err(LimboError::InvalidArgument(format!(
                        "page codec reported invalid reserved space {} for page size {}",
                        header_info.reserved_space,
                        page_size.get()
                    ))
                    .into());
                }
                (Some(header_info.reserved_space), Some(page_size))
            } else {
                let (reserved, page_size) = Self::plain_header_page_info(&header, encrypted)?;
                (Some(reserved), Some(page_size))
            }
        } else {
            (None, None)
        };
        if let (Some(codec), Some(reserved_bytes)) = (page_codec, header_reserved_bytes) {
            let required_reserved_bytes = codec.required_reserved_bytes();
            if reserved_bytes != required_reserved_bytes {
                return Err(LimboError::InvalidArgument(format!(
                    "page codec requires exactly {required_reserved_bytes} reserved bytes, but database provides {reserved_bytes}"
                )).into());
            }
        }

        let reserved_bytes = header_reserved_bytes.or_else(|| {
            if !matches!(cipher, CipherMode::None) {
                // For encryption, use the cipher's metadata size
                Some(cipher.metadata_size() as u8)
            } else {
                None
            }
        });
        let disable_checksums = if let Some(reserved_bytes) = reserved_bytes {
            // if the required reserved bytes for checksums is not present, disable checksums
            reserved_bytes != CHECKSUM_REQUIRED_RESERVED_BYTES
        } else {
            false
        };
        // Check if WAL is enabled
        let shared_wal = self.shared_wal.read();

        let page_size =
            self.determine_actual_page_size(&shared_wal, requested_page_size, header_page_size)?;

        let buffer_pool = self.buffer_pool.clone();
        if self.initialized() {
            buffer_pool.finalize_with_page_size(page_size.get() as usize)?;
        }

        let wal_enabled = shared_wal.metadata.enabled.load(Ordering::SeqCst);
        let last_checksum_and_max_frame = shared_wal.last_checksum_and_max_frame();
        drop(shared_wal);
        let pager_wal: Option<Arc<dyn Wal>> = if wal_enabled {
            Some(self.build_wal(last_checksum_and_max_frame, buffer_pool.clone())?)
        } else {
            None
        };

        let pager = Pager::new(
            self.db_file.clone(),
            pager_wal,
            self.io.clone(),
            PageCache::default(),
            buffer_pool,
            self.init_lock.clone(),
            self.init_page_1.clone(),
        )?;
        pager.set_page_size(page_size);
        if let Some(reserved_bytes) = reserved_bytes {
            pager.set_reserved_space_bytes(reserved_bytes);
        }
        if disable_checksums {
            pager.reset_checksum_context();
        }

        Ok(IOResult::Done(pager))
    }

    /// Recovers the page size and reserved-space byte from a database header
    /// that is not handled by a page codec.
    ///
    /// The magic bytes are not judged here: an encrypted database carries the
    /// Turso prefix instead, and its key may arrive only after connect (PRAGMA
    /// key). The magic is checked at open validation, where the key is known.
    fn plain_header_page_info(header: &DbHeaderRead, encrypted: bool) -> Result<(u8, PageSize)> {
        // SQLite zero-fills a file shorter than a page before judging its
        // header (btree.c lockBtree), so a short file is judged the same way.
        let mut bytes = [0u8; DatabaseHeader::SIZE];
        let read = header.bytes();
        let len = read.len().min(bytes.len());
        bytes[..len].copy_from_slice(&read[..len]);

        let reserved = bytes[20];
        let page_size = PageSize::new_from_header_u16(u16::from_be_bytes([bytes[16], bytes[17]]));

        let judge_as_sqlite = !encrypted && !bytes.starts_with(TURSO_HEADER_PREFIX);
        if judge_as_sqlite
            && !page_size
                .as_ref()
                .is_ok_and(|page_size| page_size.has_valid_reserved_space(reserved))
        {
            return Err(LimboError::NotADB);
        }
        if let Some(err) = header.short_read() {
            return Err(err.into());
        }
        Ok((reserved, page_size?))
    }

    #[cfg(feature = "fs")]
    pub fn io_for_path(path: &str) -> Result<Arc<dyn IO>> {
        let io: Arc<dyn IO> = if is_memory_like(path.trim()) {
            Arc::new(MemoryIO::new())
        } else {
            Arc::new(PlatformIO::new()?)
        };
        Ok(io)
    }

    #[cfg(feature = "fs")]
    pub fn io_for_vfs<S: AsRef<str> + std::fmt::Display>(vfs: S) -> Result<Arc<dyn IO>> {
        if let Some(io) = crate::io::get_registered_io(vfs.as_ref()) {
            return Ok(io);
        }
        let vfsmods = ext::add_builtin_vfs_extensions(None)?;
        let io: Arc<dyn IO> = match vfsmods
            .iter()
            .find(|v| v.0 == vfs.as_ref())
            .map(|v| v.1.clone())
        {
            Some(vfs) => vfs,
            None => match vfs.as_ref() {
                "memory" => Arc::new(MemoryIO::new()),
                #[cfg(feature = "io_memory_yield")]
                "memory_yield" => Arc::new(MemoryYieldIO::new()),
                "syscall" => Arc::new(SyscallIO::new()?),
                #[cfg(all(target_os = "linux", feature = "io_uring", not(miri)))]
                "io_uring" => Arc::new(UringIO::new()?),
                #[cfg(all(target_os = "windows", feature = "experimental_win_iocp", not(miri)))]
                "experimental_win_iocp" => Arc::new(WindowsIOCP::new()?),

                other => {
                    return Err(LimboError::InvalidArgument(format!("no such VFS: {other}")));
                }
            },
        };
        Ok(io)
    }

    /// Open a new database file with optionally specifying a VFS without an existing database
    /// connection and symbol table to register extensions.
    #[cfg(feature = "fs")]
    pub fn open_new<S>(
        path: &str,
        vfs: Option<S>,
        flags: OpenFlags,
        opts: DatabaseOpts,
        encryption_opts: Option<EncryptionOpts>,
        dialect: Arc<dyn Dialect>,
    ) -> Result<(Arc<dyn IO>, Arc<Database>)>
    where
        S: AsRef<str> + std::fmt::Display,
    {
        let io = vfs
            .map(|vfs| Self::io_for_vfs(vfs))
            .or_else(|| Some(Self::io_for_path(path)))
            .transpose()?
            .unwrap();
        let db =
            Self::open_file_with_flags(io.clone(), path, flags, opts, encryption_opts, dialect)?;
        Ok((io, db))
    }

    #[inline]
    pub(crate) fn initialized(&self) -> bool {
        self.init_page_1.load().is_none()
    }

    pub(crate) fn can_load_extensions(&self) -> bool {
        self.opts.enable_load_extension
    }

    #[inline]
    pub(crate) fn with_schema_mut<T>(&self, f: impl FnOnce(&mut Schema) -> Result<T>) -> Result<T> {
        let mut schema_ref = self.schema.lock();
        let schema = Schema::try_make_mut(&mut schema_ref)?;
        f(schema)
    }

    pub(crate) fn replace_schema(&self, schema: Arc<Schema>) {
        *self.schema.lock() = schema;
    }

    /// Register an `InternalVirtualTable` into this database's catalog. The
    /// table is visible to connections opened after this call and is queryable
    /// like any other table.
    ///
    /// Intended for callers that want to surface state as a queryable table
    /// without going through `CREATE VIRTUAL TABLE` — for example, extensions
    /// contributing metadata tables or alternative-dialect catalogs.
    ///
    /// Call before opening connections. Connections that already exist will
    /// not pick up the new table unless they re-read the shared schema (e.g.
    /// via the usual schema-change path).
    pub fn register_internal_vtab<T>(&self, table: T) -> Result<String>
    where
        T: InternalVirtualTable + 'static,
    {
        self.with_schema_mut(|schema| schema.register_internal_vtab(table))
    }

    /// The SQL dialect this database was opened with.
    pub fn dialect(&self) -> Arc<dyn Dialect> {
        self.dialect.clone()
    }

    pub fn register_virtual_table(&self, table: Arc<VirtualTable>) -> Result<String> {
        let name = table.name.clone();
        self.with_schema_mut(|schema| schema.add_virtual_table(table))?;
        Ok(name)
    }

    pub(crate) fn clone_schema(&self) -> Arc<Schema> {
        let schema = self.schema.lock();
        schema.clone()
    }

    pub(crate) fn update_schema_if_newer(&self, another: Arc<Schema>) {
        let mut schema = self.schema.lock();
        if schema.schema_version < another.schema_version {
            tracing::debug!(
                "DB schema is outdated: {} < {}",
                schema.schema_version,
                another.schema_version
            );
            *schema = another;
        } else {
            tracing::debug!(
                "DB schema is up to date: {} >= {}",
                schema.schema_version,
                another.schema_version
            );
        }
    }

    pub fn get_mv_store(&self) -> impl Deref<Target = Option<Arc<MvStore>>> {
        self.mv_store.load()
    }

    pub fn experimental_views_enabled(&self) -> bool {
        self.opts.enable_views
    }

    pub fn experimental_index_method_enabled(&self) -> bool {
        self.opts.enable_index_method
    }

    pub fn experimental_custom_types_enabled(&self) -> bool {
        self.opts.enable_custom_types || self.dialect.requires_custom_types()
    }

    pub fn experimental_encryption_enabled(&self) -> bool {
        self.opts.enable_encryption
    }

    pub fn experimental_autovacuum_enabled(&self) -> bool {
        self.opts.enable_autovacuum
    }

    pub fn experimental_vacuum_enabled(&self) -> bool {
        self.opts.enable_vacuum
    }

    pub fn experimental_mvcc_passive_checkpoint_enabled(&self) -> bool {
        self.opts.enable_experimental_mvcc_passive_checkpoint
    }

    pub fn experimental_attach_enabled(&self) -> bool {
        self.opts.enable_attach
    }

    pub fn experimental_generated_columns_enabled(&self) -> bool {
        self.opts.enable_generated_columns
    }

    pub fn experimental_multiprocess_wal_enabled(&self) -> bool {
        self.opts.enable_multiprocess_wal
    }

    pub fn experimental_without_rowid_enabled(&self) -> bool {
        self.opts.enable_without_rowid
    }

    /// check if database is currently in MVCC mode
    pub fn mvcc_enabled(&self) -> bool {
        self.mv_store.load().is_some()
    }

    #[cfg(feature = "test_helper")]
    pub fn set_pending_byte(val: u32) {
        Pager::set_pending_byte(val);
    }

    #[cfg(feature = "test_helper")]
    pub fn get_pending_byte() -> u32 {
        Pager::get_pending_byte()
    }
}

pub(crate) struct AttachedDatabase {
    pub(crate) db: Arc<Database>,
    pub(crate) pager: Arc<Pager>,
    pub(crate) sync_mode: SyncMode,
}

// Optimized for fast get() operations and supports unlimited attached databases.
pub(crate) struct DatabaseCatalog {
    pub(crate) name_to_index: HashMap<String, usize>,
    allocated: Vec<u64>,
    pub(crate) index_to_data: HashMap<usize, AttachedDatabase>,
}

#[allow(unused)]
impl DatabaseCatalog {
    pub(crate) fn new() -> Self {
        Self {
            name_to_index: HashMap::default(),
            index_to_data: HashMap::default(),
            allocated: vec![3], // 0 | 1, as those are reserved for main and temp
        }
    }

    pub(crate) fn get_database_by_index(&self, index: usize) -> Option<Arc<Database>> {
        self.index_to_data.get(&index).map(|entry| entry.db.clone())
    }

    pub(crate) fn get_name_by_index(&self, index: usize) -> Option<String> {
        self.name_to_index
            .iter()
            .find(|(_, &idx)| idx == index)
            .map(|(name, _)| name.clone())
    }

    pub(crate) fn get_database_by_name(&self, s: &str) -> Option<(usize, Arc<Database>)> {
        match self.name_to_index.get(s) {
            None => None,
            Some(idx) => self
                .index_to_data
                .get(idx)
                .map(|entry| (*idx, entry.db.clone())),
        }
    }

    pub(crate) fn get_pager_by_index(&self, idx: &usize) -> Option<Arc<Pager>> {
        self.index_to_data.get(idx).map(|entry| entry.pager.clone())
    }

    fn add(&mut self, s: &str) -> usize {
        turso_assert!(
            !self.name_to_index.contains_key(s),
            "lib: database name already exists in catalog",
            { "name": s }
        );

        let index = self.allocate_index();
        self.name_to_index.insert(s.to_string(), index);
        index
    }

    pub(crate) fn insert(&mut self, s: &str, db: Arc<Database>, pager: Arc<Pager>) -> usize {
        let idx = self.add(s);
        self.index_to_data.insert(
            idx,
            AttachedDatabase {
                db,
                pager,
                sync_mode: SyncMode::Full,
            },
        );
        idx
    }

    pub(crate) fn remove(&mut self, s: &str) -> Option<usize> {
        if let Some(index) = self.name_to_index.remove(s) {
            // Should be impossible to remove main or temp.
            turso_assert_greater_than_or_equal!(index, 2);
            self.deallocate_index(index);
            self.index_to_data.remove(&index);
            Some(index)
        } else {
            None
        }
    }

    #[inline(always)]
    fn deallocate_index(&mut self, index: usize) {
        let word_idx = index / 64;
        let bit_idx = index % 64;

        if word_idx < self.allocated.len() {
            self.allocated[word_idx] &= !(1u64 << bit_idx);
        }
    }

    fn allocate_index(&mut self) -> usize {
        for word_idx in 0..self.allocated.len() {
            let word = self.allocated[word_idx];

            if word != u64::MAX {
                let free_bit = Self::find_first_zero_bit(word);
                let index = word_idx * 64 + free_bit;

                self.allocated[word_idx] |= 1u64 << free_bit;

                return index;
            }
        }

        // Need to expand bitmap
        let word_idx = self.allocated.len();
        self.allocated.push(1u64); // Mark first bit as allocated
        word_idx * 64
    }

    #[inline(always)]
    fn find_first_zero_bit(word: u64) -> usize {
        // Invert to find first zero as first one
        let inverted = !word;

        // Use trailing zeros count (compiles to single instruction on most CPUs)
        inverted.trailing_zeros() as usize
    }
}

#[cfg(test)]
mod database_tests {
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };

    use super::{is_memory_like, Database, InitState};
    use crate::storage::encryption::EncryptionKey;
    use crate::storage::page_transform::{
        PageCodec, PageCodecContext, PageCodecHeaderInfo, PageCodecId, PageLocation,
    };
    use crate::storage::sqlite3_ondisk::DatabaseHeader;
    use crate::{
        storage::database::DatabaseFile, CompletionError, Connection, DatabaseOpts,
        DatabaseStorage, EncryptionOpts, IOResult, LimboError, OpenDbAsyncState, OpenFlags,
        OpenOptions, PlatformIO, SqliteDialect, IO,
    };

    #[test]
    fn memory_path_classifies_named_memory_databases() {
        assert!(is_memory_like(":memory:"));
        assert!(is_memory_like(":memory:sync-draft"));
        assert!(is_memory_like("file::memory:?cache=shared"));
        assert!(is_memory_like(""));
        assert!(!is_memory_like("memory.db"));
        assert!(!is_memory_like("file:memory.db"));
    }

    #[cfg(feature = "fs")]
    #[test]
    fn io_for_path_uses_memory_io_for_named_memory_database() {
        let path = format!(":memory:named-io-selection-{}", std::process::id());
        assert!(std::fs::metadata(&path).is_err());

        let io = Database::io_for_path(&path).unwrap();

        assert!(io.file_id(&path).is_ok());
        assert!(std::fs::metadata(&path).is_err());
    }

    #[derive(Debug)]
    struct IdentityPageCodec;

    impl PageCodec for IdentityPageCodec {
        fn codec_id(&self) -> PageCodecId {
            PageCodecId::new(*b"identity-codec--")
        }

        fn required_reserved_bytes(&self) -> u8 {
            0
        }

        fn encode_page(
            &self,
            _context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> crate::Result<()> {
            output.copy_from_slice(input);
            Ok(())
        }

        fn decode_page(
            &self,
            _context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> crate::Result<()> {
            output.copy_from_slice(input);
            Ok(())
        }
    }

    #[derive(Debug)]
    struct InvalidHeaderPageCodec {
        page_size: usize,
        reserved_space: u8,
    }

    impl PageCodec for InvalidHeaderPageCodec {
        fn codec_id(&self) -> PageCodecId {
            PageCodecId::new(*b"invalid-header-c")
        }

        fn bootstrap_page_info(
            &self,
            _raw_page1_prefix: &[u8],
        ) -> crate::Result<PageCodecHeaderInfo> {
            Ok(PageCodecHeaderInfo {
                page_size: self.page_size,
                reserved_space: self.reserved_space,
            })
        }

        fn required_reserved_bytes(&self) -> u8 {
            0
        }

        fn encode_page(
            &self,
            _context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> crate::Result<()> {
            output.copy_from_slice(input);
            Ok(())
        }

        fn decode_page(
            &self,
            _context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> crate::Result<()> {
            output.copy_from_slice(input);
            Ok(())
        }
    }

    #[derive(Debug)]
    struct XorPageCodec {
        mask: u8,
        reserved_bytes: u8,
    }

    impl XorPageCodec {
        fn transform(&self, page: &[u8], output: &mut [u8]) {
            for (input, output) in page.iter().zip(output) {
                *output = input ^ self.mask;
            }
        }
    }

    impl PageCodec for XorPageCodec {
        fn codec_id(&self) -> PageCodecId {
            let mut id = *b"xor-page-codec--";
            id[15] = self.mask;
            id[14] = self.reserved_bytes;
            PageCodecId::new(id)
        }

        fn bootstrap_page_info(
            &self,
            raw_page1_prefix: &[u8],
        ) -> crate::Result<PageCodecHeaderInfo> {
            if raw_page1_prefix.len() < 21 {
                return Err(LimboError::NotADB);
            }

            let decoded_magic = raw_page1_prefix[..16]
                .iter()
                .map(|byte| byte ^ self.mask)
                .collect::<Vec<_>>();
            if decoded_magic.as_slice() != b"SQLite format 3\0" {
                return Err(LimboError::NotADB);
            }

            let ps_raw = u16::from_be_bytes([
                raw_page1_prefix[16] ^ self.mask,
                raw_page1_prefix[17] ^ self.mask,
            ]);
            let page_size = if ps_raw == 1 { 65536 } else { ps_raw as usize };
            Ok(PageCodecHeaderInfo {
                page_size,
                reserved_space: raw_page1_prefix[20] ^ self.mask,
            })
        }

        fn required_reserved_bytes(&self) -> u8 {
            self.reserved_bytes
        }

        fn encode_page(
            &self,
            _context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> crate::Result<()> {
            self.transform(input, output);
            Ok(())
        }

        fn decode_page(
            &self,
            _context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> crate::Result<()> {
            self.transform(input, output);
            Ok(())
        }
    }

    #[derive(Debug)]
    struct CountingPageCodec {
        inner: XorPageCodec,
        database_page1_decodes: Arc<AtomicUsize>,
    }

    impl PageCodec for CountingPageCodec {
        fn codec_id(&self) -> PageCodecId {
            self.inner.codec_id()
        }

        fn bootstrap_page_info(
            &self,
            raw_page1_prefix: &[u8],
        ) -> crate::Result<PageCodecHeaderInfo> {
            self.inner.bootstrap_page_info(raw_page1_prefix)
        }

        fn required_reserved_bytes(&self) -> u8 {
            self.inner.required_reserved_bytes()
        }

        fn encode_page(
            &self,
            context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> crate::Result<()> {
            self.inner.encode_page(context, input, output)
        }

        fn decode_page(
            &self,
            context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> crate::Result<()> {
            if context.page_no == DatabaseHeader::PAGE_ID as u32
                && context.location == PageLocation::Database
            {
                self.database_page1_decodes.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.decode_page(context, input, output)
        }
    }

    #[derive(Debug)]
    struct FailOncePageCodec {
        inner: XorPageCodec,
        fail_decode: Arc<AtomicBool>,
        fail_context: PageCodecContext,
    }

    impl PageCodec for FailOncePageCodec {
        fn codec_id(&self) -> PageCodecId {
            self.inner.codec_id()
        }

        fn bootstrap_page_info(
            &self,
            raw_page1_prefix: &[u8],
        ) -> crate::Result<PageCodecHeaderInfo> {
            self.inner.bootstrap_page_info(raw_page1_prefix)
        }

        fn required_reserved_bytes(&self) -> u8 {
            self.inner.required_reserved_bytes()
        }

        fn encode_page(
            &self,
            context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> crate::Result<()> {
            self.inner.encode_page(context, input, output)
        }

        fn decode_page(
            &self,
            context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> crate::Result<()> {
            if context == self.fail_context && self.fail_decode.swap(false, Ordering::Relaxed) {
                return Err(LimboError::InternalError(
                    "injected page decode failure".into(),
                ));
            }
            self.inner.decode_page(context, input, output)
        }
    }

    #[derive(Debug, Default)]
    struct PageCodecFailureSwitches {
        wal_encode: AtomicBool,
        wal_decode: AtomicBool,
        database_encode: AtomicBool,
        database_decode_page: AtomicUsize,
    }

    #[derive(Debug)]
    struct FailOnceTransformPageCodec {
        inner: XorPageCodec,
        failures: Arc<PageCodecFailureSwitches>,
    }

    impl PageCodec for FailOnceTransformPageCodec {
        fn codec_id(&self) -> PageCodecId {
            self.inner.codec_id()
        }

        fn bootstrap_page_info(
            &self,
            raw_page1_prefix: &[u8],
        ) -> crate::Result<PageCodecHeaderInfo> {
            self.inner.bootstrap_page_info(raw_page1_prefix)
        }

        fn required_reserved_bytes(&self) -> u8 {
            self.inner.required_reserved_bytes()
        }

        fn encode_page(
            &self,
            context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> crate::Result<()> {
            let failure = match context.location {
                PageLocation::Database => &self.failures.database_encode,
                PageLocation::Wal => &self.failures.wal_encode,
            };
            if failure.swap(false, Ordering::Relaxed) {
                return Err(LimboError::InternalError(format!(
                    "injected {:?} encode failure",
                    context.location
                )));
            }
            self.inner.encode_page(context, input, output)
        }

        fn decode_page(
            &self,
            context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> crate::Result<()> {
            let fail = match context.location {
                PageLocation::Database => self
                    .failures
                    .database_decode_page
                    .compare_exchange(
                        context.page_no as usize,
                        0,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok(),
                PageLocation::Wal => self.failures.wal_decode.swap(false, Ordering::Relaxed),
            };
            if fail {
                if let (Some(input), Some(output)) = (input.first(), output.first_mut()) {
                    *output = *input;
                }
                return Err(LimboError::InternalError(format!(
                    "injected {:?} decode failure",
                    context.location
                )));
            }
            self.inner.decode_page(context, input, output)
        }
    }

    #[derive(Debug)]
    struct TaggedPageCodec;

    impl TaggedPageCodec {
        const TAG_BYTES: usize = std::mem::size_of::<u64>();

        // This is deliberately a small test-only integrity tag, not a
        // cryptographic authenticator.
        fn tag(context: PageCodecContext, data: &[u8]) -> u64 {
            let location = match context.location {
                PageLocation::Database => 0x4442_5041_4745_0000,
                PageLocation::Wal => 0x5741_4c50_4147_4500,
            };
            data.iter()
                .fold(location ^ u64::from(context.page_no), |tag, byte| {
                    tag.rotate_left(5) ^ u64::from(*byte)
                })
        }
    }

    impl PageCodec for TaggedPageCodec {
        fn codec_id(&self) -> PageCodecId {
            PageCodecId::new(*b"tagged-page-----")
        }

        fn required_reserved_bytes(&self) -> u8 {
            Self::TAG_BYTES as u8
        }

        fn encode_page(
            &self,
            context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> crate::Result<()> {
            if input.len() != output.len() || input.len() < Self::TAG_BYTES {
                return Err(LimboError::InvalidArgument(
                    "tagged codec requires equal page-sized buffers".into(),
                ));
            }
            let data_len = input.len() - Self::TAG_BYTES;
            output[..data_len].copy_from_slice(&input[..data_len]);
            output[data_len..]
                .copy_from_slice(&Self::tag(context, &input[..data_len]).to_le_bytes());
            Ok(())
        }

        fn decode_page(
            &self,
            context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> crate::Result<()> {
            if input.len() != output.len() || input.len() < Self::TAG_BYTES {
                return Err(LimboError::InvalidArgument(
                    "tagged codec requires equal page-sized buffers".into(),
                ));
            }
            let data_len = input.len() - Self::TAG_BYTES;
            let stored = u64::from_le_bytes(
                input[data_len..]
                    .try_into()
                    .expect("tag length was checked above"),
            );
            let expected = Self::tag(context, &input[..data_len]);
            if stored != expected {
                return Err(LimboError::Corrupt(format!(
                    "invalid page tag for page {} at {:?}",
                    context.page_no, context.location
                )));
            }
            output[..data_len].copy_from_slice(&input[..data_len]);
            output[data_len..].fill(0);
            Ok(())
        }
    }

    #[derive(Debug)]
    struct LocationPageCodec;

    impl LocationPageCodec {
        const MASK: u8 = 0x6d;

        fn byte_key(page_no: u32, location: PageLocation, offset: usize) -> u8 {
            // Persisted transforms must not depend on the host pointer width.
            let page_id_byte = (page_no as u64).to_le_bytes()[offset % std::mem::size_of::<u64>()];
            let page_id_rotation =
                ((offset / std::mem::size_of::<u64>()) % (u8::BITS as usize)) as u32;
            Self::MASK
                ^ page_id_byte.rotate_left(page_id_rotation)
                ^ match location {
                    PageLocation::Database => 0,
                    PageLocation::Wal => 0xa5,
                }
        }

        fn transform(input: &[u8], output: &mut [u8], context: PageCodecContext) {
            for (offset, (input, output)) in input.iter().zip(output).enumerate() {
                *output = input ^ Self::byte_key(context.page_no, context.location, offset);
            }
        }
    }

    impl PageCodec for LocationPageCodec {
        fn codec_id(&self) -> PageCodecId {
            PageCodecId::new(*b"location-page-co")
        }

        fn bootstrap_page_info(
            &self,
            raw_page1_prefix: &[u8],
        ) -> crate::Result<PageCodecHeaderInfo> {
            if raw_page1_prefix.len() < 21 {
                return Err(LimboError::NotADB);
            }
            if !raw_page1_prefix[..16]
                .iter()
                .enumerate()
                .zip(b"SQLite format 3\0")
                .all(|((offset, encoded), expected)| {
                    *encoded
                        ^ Self::byte_key(
                            DatabaseHeader::PAGE_ID as u32,
                            PageLocation::Database,
                            offset,
                        )
                        == *expected
                })
            {
                return Err(LimboError::NotADB);
            }

            let page_size = u16::from_be_bytes([
                raw_page1_prefix[16]
                    ^ Self::byte_key(DatabaseHeader::PAGE_ID as u32, PageLocation::Database, 16),
                raw_page1_prefix[17]
                    ^ Self::byte_key(DatabaseHeader::PAGE_ID as u32, PageLocation::Database, 17),
            ]);
            Ok(PageCodecHeaderInfo {
                page_size: if page_size == 1 {
                    65_536
                } else {
                    page_size as usize
                },
                reserved_space: raw_page1_prefix[20]
                    ^ Self::byte_key(DatabaseHeader::PAGE_ID as u32, PageLocation::Database, 20),
            })
        }

        fn required_reserved_bytes(&self) -> u8 {
            1
        }

        fn encode_page(
            &self,
            context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> crate::Result<()> {
            Self::transform(input, output, context);
            Ok(())
        }

        fn decode_page(
            &self,
            context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> crate::Result<()> {
            Self::transform(input, output, context);
            Ok(())
        }
    }

    #[test]
    fn location_page_codec_uses_full_page_id() {
        let input = vec![0; 512];
        let mut page_one = vec![0; input.len()];
        let mut page_two_fifty_seven = vec![0; input.len()];
        LocationPageCodec::transform(
            &input,
            &mut page_one,
            PageCodecContext::new(1, PageLocation::Database),
        );
        LocationPageCodec::transform(
            &input,
            &mut page_two_fifty_seven,
            PageCodecContext::new(257, PageLocation::Database),
        );

        assert_ne!(page_one, page_two_fifty_seven);

        let mut decoded = vec![0; input.len()];
        LocationPageCodec::transform(
            &page_two_fifty_seven,
            &mut decoded,
            PageCodecContext::new(257, PageLocation::Database),
        );
        assert_eq!(decoded, input);
    }

    #[derive(Debug)]
    struct XorDefaultBootstrapPageCodec {
        mask: u8,
        reserved_bytes: u8,
    }

    impl XorDefaultBootstrapPageCodec {
        fn transform(&self, page: &[u8], output: &mut [u8]) {
            for (input, output) in page.iter().zip(output) {
                *output = input ^ self.mask;
            }
        }
    }

    impl PageCodec for XorDefaultBootstrapPageCodec {
        fn codec_id(&self) -> PageCodecId {
            let mut id = *b"xor-no-probe----";
            id[15] = self.mask;
            id[14] = self.reserved_bytes;
            PageCodecId::new(id)
        }

        fn required_reserved_bytes(&self) -> u8 {
            self.reserved_bytes
        }

        fn encode_page(
            &self,
            _context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> crate::Result<()> {
            self.transform(input, output);
            Ok(())
        }

        fn decode_page(
            &self,
            _context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> crate::Result<()> {
            self.transform(input, output);
            Ok(())
        }
    }

    #[cfg(feature = "fs")]
    fn open_with_page_codec_result(
        io: Arc<dyn IO>,
        path: &str,
        codec: Arc<dyn PageCodec>,
    ) -> crate::Result<Arc<Database>> {
        open_with_page_codec_with_opts_result(io, path, codec, DatabaseOpts::new())
    }

    #[cfg(feature = "fs")]
    fn open_with_page_codec_with_opts_result(
        io: Arc<dyn IO>,
        path: &str,
        codec: Arc<dyn PageCodec>,
        opts: DatabaseOpts,
    ) -> crate::Result<Arc<Database>> {
        let file = io.open_file(path, OpenFlags::Create, true).unwrap();
        let db_file: Arc<dyn DatabaseStorage> = Arc::new(DatabaseFile::new(file));
        let mut state = OpenDbAsyncState::new();
        let options = OpenOptions::new(Arc::new(SqliteDialect))
            .storage(db_file)
            .flags(OpenFlags::Create)
            .db_opts(opts)
            .page_codec(codec);

        loop {
            match Database::open_async(&mut state, io.clone(), path, &options)? {
                IOResult::Done(db) => return Ok(db),
                IOResult::IO(completion) => completion.wait(&*io)?,
            }
        }
    }

    #[cfg(feature = "fs")]
    fn open_with_page_codec(
        io: Arc<dyn IO>,
        path: &str,
        codec: Arc<dyn PageCodec>,
    ) -> Arc<Database> {
        open_with_page_codec_result(io, path, codec).unwrap()
    }

    #[cfg(feature = "fs")]
    fn count_test_rows(conn: &Arc<Connection>) -> i64 {
        let mut stmt = conn.prepare("select count(*) from test").unwrap();
        let mut count = 0;
        stmt.run_with_row_callback(|row| {
            count = row.get(0).unwrap();
            Ok(())
        })
        .unwrap();
        count
    }

    fn try_count_test_rows(conn: &Arc<Connection>) -> crate::Result<i64> {
        let mut stmt = conn.prepare("select count(*) from test")?;
        let mut count = 0;
        stmt.run_with_row_callback(|row| {
            count = row.get(0)?;
            Ok(())
        })?;
        Ok(count)
    }

    #[cfg(feature = "fs")]
    fn passive_checkpoint_busy(conn: &Arc<Connection>) -> crate::Result<i64> {
        let mut stmt = conn.prepare("PRAGMA wal_checkpoint(PASSIVE)")?;
        let mut busy = None;
        stmt.run_with_row_callback(|row| {
            busy = Some(row.get(0)?);
            Ok(())
        })?;
        busy.ok_or_else(|| {
            LimboError::InternalError("wal_checkpoint did not return a result row".to_owned())
        })
    }

    #[cfg(feature = "fs")]
    #[test]
    fn registry_reuses_cached_database_without_retaining_page_codec() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-registry.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let first_codec: Arc<dyn PageCodec> = Arc::new(IdentityPageCodec);
        let first_codec_weak = Arc::downgrade(&first_codec);
        let second_codec: Arc<dyn PageCodec> = Arc::new(IdentityPageCodec);

        let first = open_with_page_codec(io.clone(), path, first_codec);
        assert!(
            first_codec_weak.upgrade().is_none(),
            "cached Database must not retain the page codec"
        );
        let second = open_with_page_codec(io, path, second_codec);

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_identity_must_match_cached_database() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-identity.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let first_codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
            mask: 0xa5,
            reserved_bytes: 1,
        });
        let different_codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
            mask: 0x5a,
            reserved_bytes: 1,
        });

        let db = open_with_page_codec(io.clone(), path, first_codec);

        let err = match db.connect_with_page_codec(different_codec.clone()) {
            Ok(_) => panic!("a cached codec-backed database must reject a different codec"),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("page codec identity does not match the existing database"));

        let err = open_with_page_codec_result(io, path, different_codec).unwrap_err();
        assert!(err
            .to_string()
            .contains("page codec identity does not match the existing database"));
    }

    #[cfg(feature = "fs")]
    #[test]
    fn cached_database_rejects_encryption_and_page_codec() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-encryption-cached.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec: Arc<dyn PageCodec> = Arc::new(IdentityPageCodec);
        let _db = open_with_page_codec(io.clone(), path, codec.clone());

        let err = Database::open(
            io,
            path,
            OpenOptions::new(Arc::new(SqliteDialect))
                .flags(OpenFlags::Create)
                .encryption(EncryptionOpts {
                    cipher: "aes256gcm".to_string(),
                    hexkey: "00".repeat(32),
                })
                .page_codec(codec),
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("built-in encryption cannot be combined with an external page codec"));
    }

    #[cfg(feature = "fs")]
    #[test]
    fn external_page_codec_rejects_multiprocess_wal_before_opening_file() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-multiprocess-wal.db");
        let path_str = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());

        let err = Database::open(
            io,
            path_str,
            OpenOptions::new(Arc::new(SqliteDialect))
                .flags(OpenFlags::Create)
                .db_opts(DatabaseOpts::new().with_multiprocess_wal(true))
                .page_codec(Arc::new(IdentityPageCodec) as Arc<dyn PageCodec>),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            LimboError::InvalidArgument(ref message)
                if message
                    == "external page codecs are not supported with experimental multiprocess WAL"
        ));
        assert!(!path.exists());
    }

    #[cfg(feature = "fs")]
    #[test]
    fn bypass_registry_page_codec_open_rejects_multiprocess_wal() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-bypass-multiprocess-wal.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let file = io.open_file(path, OpenFlags::Create, true).unwrap();
        let db_file: Arc<dyn DatabaseStorage> = Arc::new(DatabaseFile::new(file));
        let mut state = OpenDbAsyncState::new();

        let options = OpenOptions::new(Arc::new(SqliteDialect))
            .storage(db_file)
            .flags(OpenFlags::Create)
            .db_opts(DatabaseOpts::new().with_multiprocess_wal(true))
            .page_codec(Arc::new(IdentityPageCodec) as Arc<dyn PageCodec>);
        let err = match Database::do_open_async(&mut state, io, path, &options) {
            Err(err) => err,
            Ok(_) => panic!("multiprocess WAL must reject an external page codec"),
        };

        assert!(matches!(
            *err,
            LimboError::InvalidArgument(ref message)
                if message
                    == "external page codecs are not supported with experimental multiprocess WAL"
        ));
    }

    #[cfg(feature = "fs")]
    #[test]
    fn cached_page_codec_database_requires_codec_at_connection_time() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-registry-no-codec.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
            mask: 0xa5,
            reserved_bytes: 1,
        });
        let _db = open_with_page_codec(io.clone(), path, codec.clone());
        let db = open_with_page_codec(io.clone(), path, codec.clone());
        let conn = db.connect_with_page_codec(codec).unwrap();
        conn.execute("create table test(id integer primary key)")
            .unwrap();
        drop(conn);

        assert!(
            Database::open_file_with_flags(
                io,
                path,
                OpenFlags::Create,
                DatabaseOpts::new(),
                None,
                Arc::new(SqliteDialect),
            )
            .is_err(),
            "opening without the codec must not reuse a codec-required database"
        );
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_checkpoint_decodes_database_page1_for_identity() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-checkpoint-identity.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let database_page1_decodes = Arc::new(AtomicUsize::new(0));
        let codec: Arc<dyn PageCodec> = Arc::new(CountingPageCodec {
            inner: XorPageCodec {
                mask: 0x5a,
                reserved_bytes: 1,
            },
            database_page1_decodes: database_page1_decodes.clone(),
        });
        let db = open_with_page_codec(io, path, codec.clone());
        let conn = db.connect_with_page_codec(codec).unwrap();
        conn.execute("PRAGMA journal_mode = 'wal'").unwrap();
        conn.execute(
            "create table test(id integer primary key, value text);
             insert into test(value) values ('alpha');",
        )
        .unwrap();
        conn.set_sync_mode(crate::SyncMode::Full);

        database_page1_decodes.store(0, Ordering::Relaxed);
        let checkpoint = conn
            .checkpoint(crate::storage::wal::CheckpointMode::Passive {
                upper_bound_inclusive: None,
            })
            .unwrap();

        assert!(checkpoint.wal_checkpoint_backfilled > 0);
        assert_eq!(
            database_page1_decodes.load(Ordering::Relaxed),
            1,
            "checkpoint identity must decode database page 1 exactly once"
        );
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_checkpoint_recovers_after_page1_decode_failure() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-checkpoint-retry.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let fail_database_page1_decode = Arc::new(AtomicBool::new(false));
        let codec: Arc<dyn PageCodec> = Arc::new(FailOncePageCodec {
            inner: XorPageCodec {
                mask: 0x5a,
                reserved_bytes: 1,
            },
            fail_decode: fail_database_page1_decode.clone(),
            fail_context: PageCodecContext::new(
                DatabaseHeader::PAGE_ID as u32,
                PageLocation::Database,
            ),
        });
        let db = open_with_page_codec(io, path, codec.clone());
        let conn = db.connect_with_page_codec(codec).unwrap();
        conn.execute("PRAGMA journal_mode = 'wal'").unwrap();
        conn.execute(
            "create table test(id integer primary key, value text);
             insert into test(value) values ('alpha');",
        )
        .unwrap();
        conn.set_sync_mode(crate::SyncMode::Full);

        fail_database_page1_decode.store(true, Ordering::Relaxed);
        let err = conn
            .checkpoint(crate::storage::wal::CheckpointMode::Passive {
                upper_bound_inclusive: None,
            })
            .unwrap_err();
        assert!(matches!(
            err,
            LimboError::CompletionError(CompletionError::PageCodecError { page_idx: 1 })
        ));

        let checkpoint = conn
            .checkpoint(crate::storage::wal::CheckpointMode::Passive {
                upper_bound_inclusive: None,
            })
            .unwrap();
        assert!(checkpoint.wal_checkpoint_backfilled > 0);
        assert_eq!(count_test_rows(&conn), 1);
    }

    #[cfg(feature = "fs")]
    #[test]
    fn pragma_checkpoint_codec_error_releases_checkpoint_lock() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-pragma-checkpoint-retry.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let fail_wal_page2_decode = Arc::new(AtomicBool::new(false));
        let codec: Arc<dyn PageCodec> = Arc::new(FailOncePageCodec {
            inner: XorPageCodec {
                mask: 0x5a,
                reserved_bytes: 1,
            },
            fail_decode: fail_wal_page2_decode.clone(),
            fail_context: PageCodecContext::new(2, PageLocation::Wal),
        });
        let db = open_with_page_codec(io, path, codec.clone());
        let writer = db.connect_with_page_codec(codec.clone()).unwrap();
        writer.wal_auto_actions_disable();
        writer.execute("PRAGMA journal_mode = 'wal'").unwrap();
        writer
            .execute(
                "create table test(id integer primary key, value text);
                 insert into test(value) values ('alpha');",
            )
            .unwrap();

        let pager = writer.get_pager();
        assert!(
            pager.wal_pos().1 > 0,
            "test setup must leave committed WAL frames"
        );
        assert_eq!(
            pager.wal_backfill_frame(),
            Some(0),
            "test setup must not checkpoint the WAL before injecting the codec error"
        );
        assert!(
            pager.wal_changed_pages_after(0).unwrap().contains(&2),
            "test setup must leave page 2 in the WAL"
        );

        let first = db.connect_with_page_codec(codec.clone()).unwrap();
        assert!(first.get_pager().has_wal());
        fail_wal_page2_decode.store(true, Ordering::Relaxed);
        let first_error = passive_checkpoint_busy(&first).unwrap_err();
        assert!(matches!(first_error, LimboError::CheckpointFailed(_)));
        assert!(!fail_wal_page2_decode.load(Ordering::Relaxed));

        let second = db.connect_with_page_codec(codec).unwrap();
        assert!(second.get_pager().has_wal());
        let other_connection_retry = passive_checkpoint_busy(&second);
        let same_connection_retry = passive_checkpoint_busy(&first);
        assert!(
            matches!(same_connection_retry, Ok(0)) && matches!(other_connection_retry, Ok(0)),
            "checkpoint failure must not leave stale state or retain its guard: same connection \
             returned {same_connection_retry:?}, other connection returned {other_connection_retry:?}"
        );
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_transaction_recovers_after_wal_encode_failure() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-commit-retry.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let failures = Arc::new(PageCodecFailureSwitches::default());
        let codec: Arc<dyn PageCodec> = Arc::new(FailOnceTransformPageCodec {
            inner: XorPageCodec {
                mask: 0x5a,
                reserved_bytes: 1,
            },
            failures: failures.clone(),
        });
        let db = open_with_page_codec(io.clone(), path, codec.clone());
        let conn = db.connect_with_page_codec(codec.clone()).unwrap();
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();

        failures.wal_encode.store(true, Ordering::Relaxed);
        let err = conn
            .execute("insert into test(value) values ('not-committed')")
            .unwrap_err();
        assert!(err.to_string().contains("injected Wal encode failure"));
        assert_eq!(count_test_rows(&conn), 0);

        conn.execute("insert into test(value) values ('committed')")
            .unwrap();
        assert_eq!(count_test_rows(&conn), 1);
        conn.checkpoint(crate::CheckpointMode::Full).unwrap();
        drop(conn);
        drop(db);

        let db = open_with_page_codec(io, path, codec.clone());
        let conn = db.connect_with_page_codec(codec).unwrap();
        assert_eq!(count_test_rows(&conn), 1);
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_pager_read_recovers_after_decode_failure() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-read-retry.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let failures = Arc::new(PageCodecFailureSwitches::default());
        let codec: Arc<dyn PageCodec> = Arc::new(FailOnceTransformPageCodec {
            inner: XorPageCodec {
                mask: 0x5a,
                reserved_bytes: 1,
            },
            failures: failures.clone(),
        });

        {
            let db = open_with_page_codec(io.clone(), path, codec.clone());
            let conn = db.connect_with_page_codec(codec.clone()).unwrap();
            conn.execute(
                "create table test(id integer primary key, value text);
                 insert into test(value) values ('persisted');",
            )
            .unwrap();
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }

        let db = open_with_page_codec(io, path, codec.clone());
        let conn = db.connect_with_page_codec(codec.clone()).unwrap();
        failures.database_decode_page.store(2, Ordering::Relaxed);
        let err = try_count_test_rows(&conn).unwrap_err();
        assert!(matches!(
            err,
            LimboError::CompletionError(CompletionError::PageCodecError { page_idx: 2 })
        ));

        assert_eq!(try_count_test_rows(&conn).unwrap(), 1);
        let second = db.connect_with_page_codec(codec).unwrap();
        assert_eq!(try_count_test_rows(&second).unwrap(), 1);
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_checkpoint_recovers_after_backfill_transform_failures() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-backfill-retry.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let failures = Arc::new(PageCodecFailureSwitches::default());
        let codec: Arc<dyn PageCodec> = Arc::new(FailOnceTransformPageCodec {
            inner: XorPageCodec {
                mask: 0x5a,
                reserved_bytes: 1,
            },
            failures: failures.clone(),
        });
        let db = open_with_page_codec(io.clone(), path, codec.clone());
        let conn = db.connect_with_page_codec(codec.clone()).unwrap();
        conn.execute(
            "create table test(id integer primary key, value text);
             insert into test(value) values ('first');",
        )
        .unwrap();

        conn.pager.load().clear_page_cache(false);
        failures.wal_decode.store(true, Ordering::Relaxed);
        let err = conn.checkpoint(crate::CheckpointMode::Full).unwrap_err();
        assert!(matches!(
            err,
            LimboError::CompletionError(CompletionError::PageCodecError { .. })
        ));
        assert_eq!(count_test_rows(&conn), 1);
        conn.checkpoint(crate::CheckpointMode::Full).unwrap();

        conn.execute("insert into test(value) values ('second')")
            .unwrap();
        failures.database_encode.store(true, Ordering::Relaxed);
        let err = conn.checkpoint(crate::CheckpointMode::Full).unwrap_err();
        assert!(err.to_string().contains("injected Database encode failure"));
        assert_eq!(count_test_rows(&conn), 2);
        conn.checkpoint(crate::CheckpointMode::Full).unwrap();
        drop(conn);
        drop(db);

        let db = open_with_page_codec(io, path, codec.clone());
        let conn = db.connect_with_page_codec(codec).unwrap();
        assert_eq!(count_test_rows(&conn), 2);
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_round_trips_wal_and_checkpointed_database_with_bootstrap_header() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-roundtrip.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
            mask: 0x5a,
            reserved_bytes: 1,
        });

        {
            let db = open_with_page_codec(io.clone(), path, codec.clone());
            let conn = db.connect_with_page_codec(codec.clone()).unwrap();
            conn.execute("PRAGMA journal_mode = 'wal'").unwrap();
            conn.execute(
                "create table test(id integer primary key, value text);
                 insert into test(value) values ('alpha'), ('bravo');",
            )
            .unwrap();
            assert_eq!(count_test_rows(&conn), 2);
            conn.set_sync_mode(crate::SyncMode::Full);
            let passive = conn
                .checkpoint(crate::storage::wal::CheckpointMode::Passive {
                    upper_bound_inclusive: None,
                })
                .unwrap();
            assert!(
                passive.wal_checkpoint_backfilled > 0,
                "PASSIVE checkpoint must backfill codec-transformed pages"
            );
            conn.execute("insert into test(value) values ('charlie')")
                .unwrap();
            let full = conn
                .checkpoint(crate::storage::wal::CheckpointMode::Full)
                .unwrap();
            assert!(
                full.wal_checkpoint_backfilled > 0,
                "FULL checkpoint must backfill codec-transformed pages"
            );
            assert_eq!(count_test_rows(&conn), 3);
        }

        let raw_database = std::fs::read(path).unwrap();
        assert_ne!(&raw_database[..16], b"SQLite format 3\0");

        let reopened = open_with_page_codec(io, path, codec.clone());
        let conn = reopened.connect_with_page_codec(codec).unwrap();
        assert_eq!(count_test_rows(&conn), 3);
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_reopens_live_wal_then_checkpointed_database() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-live-wal-recovery.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec: Arc<dyn PageCodec> = Arc::new(LocationPageCodec);

        {
            let db = open_with_page_codec(io.clone(), path, codec.clone());
            let conn = db.connect_with_page_codec(codec.clone()).unwrap();
            conn.execute("PRAGMA journal_mode = 'wal'").unwrap();
            conn.execute(
                "create table test(id integer primary key, value text);
                 insert into test(value) values ('alpha'), ('bravo'), ('charlie');",
            )
            .unwrap();
            assert_eq!(count_test_rows(&conn), 3);
        }
        let wal_path = format!("{path}-wal");
        assert!(
            std::fs::metadata(&wal_path).unwrap().len() > 0,
            "the first reopen must recover committed codec-transformed WAL frames"
        );

        {
            let db = open_with_page_codec(io.clone(), path, codec.clone());
            let conn = db.connect_with_page_codec(codec.clone()).unwrap();
            assert_eq!(count_test_rows(&conn), 3);
            let checkpoint = conn
                .checkpoint(crate::storage::wal::CheckpointMode::Full)
                .unwrap();
            assert!(
                checkpoint.wal_checkpoint_backfilled > 0,
                "the recovery checkpoint must backfill codec-transformed WAL frames"
            );
        }
        let db = open_with_page_codec(io, path, codec.clone());
        let conn = db.connect_with_page_codec(codec).unwrap();
        assert_eq!(count_test_rows(&conn), 3);
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_reopen_requires_recoverable_bootstrap_header() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-no-probe-roundtrip.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec: Arc<dyn PageCodec> = Arc::new(XorDefaultBootstrapPageCodec {
            mask: 0x3c,
            reserved_bytes: 1,
        });

        {
            let db = open_with_page_codec(io.clone(), path, codec.clone());
            let conn = db.connect_with_page_codec(codec.clone()).unwrap();
            conn.execute(
                "create table test(id integer primary key, value text);
                 insert into test(value) values ('alpha'), ('bravo');",
            )
            .unwrap();
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }

        let err = open_with_page_codec_result(io, path, codec).unwrap_err();
        assert!(err
            .to_string()
            .contains("page codec reported invalid page size"));
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_rejects_mvcc_mode() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-mvcc.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec: Arc<dyn PageCodec> = Arc::new(IdentityPageCodec);
        let db = open_with_page_codec(io, path, codec.clone());
        let conn = db.connect_with_page_codec(codec).unwrap();

        let err = conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap_err();
        assert!(err
            .to_string()
            .contains("external page codecs are not supported with MVCC"));
    }

    #[cfg(feature = "fs")]
    #[test]
    fn plaintext_database_rejects_page_codec_connection() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("plaintext-codec-connection.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            path,
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();

        let err = match db.connect_with_page_codec(Arc::new(IdentityPageCodec)) {
            Ok(_) => panic!("plaintext databases must reject external page codecs"),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("database was opened without an external page codec"));
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_database_rejects_codecless_connection() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-requires-connection-codec.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = open_with_page_codec(io, path, Arc::new(IdentityPageCodec));

        let err = match db.connect() {
            Ok(_) => panic!("codec-backed databases must reject codec-less connections"),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("database requires an external page codec"));
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_reopen_rejects_mismatched_reserved_space() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-reserved-space.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let initial_codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
            mask: 0x5a,
            reserved_bytes: 1,
        });

        {
            let db = open_with_page_codec(io.clone(), path, initial_codec.clone());
            let conn = db.connect_with_page_codec(initial_codec).unwrap();
            conn.execute("create table test(id integer primary key)")
                .unwrap();
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }

        for required_reserved_bytes in [0, 2] {
            let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
                mask: 0x5a,
                reserved_bytes: required_reserved_bytes,
            });
            let err = open_with_page_codec_result(io.clone(), path, codec).unwrap_err();
            assert!(err.to_string().contains(&format!(
                "page codec requires exactly {required_reserved_bytes} reserved bytes, but database provides 1"
            )));
        }
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_reopen_rejects_invalid_header_layout() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-invalid-header-layout.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());

        {
            let db = open_with_page_codec(io.clone(), path, Arc::new(IdentityPageCodec));
            let conn = db
                .connect_with_page_codec(Arc::new(IdentityPageCodec))
                .unwrap();
            conn.execute("create table test(id integer primary key)")
                .unwrap();
            conn.execute("pragma wal_checkpoint(truncate)").unwrap();
        }

        for (codec, expected_error) in [
            (
                InvalidHeaderPageCodec {
                    page_size: 513,
                    reserved_space: 0,
                },
                "page codec reported invalid page size 513",
            ),
            (
                InvalidHeaderPageCodec {
                    page_size: 512,
                    reserved_space: 33,
                },
                "page codec reported invalid reserved space 33 for page size 512",
            ),
            (
                InvalidHeaderPageCodec {
                    page_size: 8192,
                    reserved_space: 0,
                },
                "page codec bootstrap page size 8192 does not match decoded page-1 size 4096",
            ),
        ] {
            let err = open_with_page_codec_result(io.clone(), path, Arc::new(codec)).unwrap_err();
            assert!(err.to_string().contains(expected_error));
        }
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_reopen_reports_short_header_read() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-short-header.db");
        std::fs::write(&path, [0u8]).unwrap();
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());

        let err = open_with_page_codec_result(io, path, Arc::new(IdentityPageCodec)).unwrap_err();
        assert!(err.to_string().contains("short read on page 1"));
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_and_encryption_cannot_share_pager() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-encryption-conflict.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec: Arc<dyn PageCodec> = Arc::new(IdentityPageCodec);
        let db = open_with_page_codec(io, path, codec.clone());
        let encryption_key = EncryptionKey::from_hex_string(&"00".repeat(32)).unwrap();
        let mut state = InitState::default();

        let err = match db._init_nonblock(&mut state, Some(&encryption_key), Some(&codec)) {
            Ok(_) => panic!("encryption and an external page codec must not share a pager"),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("built-in encryption cannot be combined with an external page codec"));
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_connection_rejects_builtin_encryption_settings() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-encryption-settings.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec: Arc<dyn PageCodec> = Arc::new(IdentityPageCodec);
        let db = open_with_page_codec(io, path, codec.clone());
        let conn = db.connect_with_page_codec(codec).unwrap();

        let err = conn
            .set_encryption_key(EncryptionKey::from_hex_string(&"00".repeat(32)).unwrap())
            .unwrap_err();
        assert!(matches!(
            err,
            LimboError::InvalidArgument(message)
                if message
                    == "cannot configure built-in encryption while an external page codec is installed"
        ));
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_vacuum_preserves_encoded_database() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-vacuum.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
            mask: 0x4d,
            reserved_bytes: 1,
        });
        let db = open_with_page_codec_with_opts_result(
            io.clone(),
            path,
            codec.clone(),
            DatabaseOpts::new().with_vacuum(true),
        )
        .unwrap();
        let conn = db.connect_with_page_codec(codec.clone()).unwrap();
        conn.execute(
            "create table test(id integer primary key, value text);
             insert into test(value) values ('alpha'), ('bravo');",
        )
        .unwrap();
        conn.execute("VACUUM").unwrap();
        assert_eq!(count_test_rows(&conn), 2);
        drop(conn);
        drop(db);

        assert_ne!(&std::fs::read(path).unwrap()[..16], b"SQLite format 3\0");
        let reopened = open_with_page_codec(io, path, codec.clone());
        let conn = reopened.connect_with_page_codec(codec).unwrap();
        assert_eq!(count_test_rows(&conn), 2);
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_vacuum_into_preserves_encoded_database() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-vacuum-into.db");
        let output_path = temp_dir.path().join("codec-vacuum-into-copy.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
            mask: 0x4d,
            reserved_bytes: 1,
        });
        let db = open_with_page_codec_with_opts_result(
            io.clone(),
            path,
            codec.clone(),
            DatabaseOpts::new().with_vacuum(true),
        )
        .unwrap();
        let conn = db.connect_with_page_codec(codec.clone()).unwrap();
        conn.execute(
            "create table test(id integer primary key, value text);
             insert into test(value) values ('secret-value');",
        )
        .unwrap();
        conn.execute(format!("VACUUM INTO '{}'", output_path.display()))
            .unwrap();

        let raw_output = std::fs::read(&output_path).unwrap();
        assert_ne!(&raw_output[..16], b"SQLite format 3\0");
        let output = open_with_page_codec(io, output_path.to_str().unwrap(), codec.clone());
        let output_conn = output.connect_with_page_codec(codec).unwrap();
        assert_eq!(count_test_rows(&output_conn), 1);
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_connection_uses_plain_internal_temp_database() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-temp.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
            mask: 0x4d,
            reserved_bytes: 1,
        });
        let db = open_with_page_codec(io.clone(), path, codec.clone());
        let conn = db.connect_with_page_codec(codec.clone()).unwrap();
        conn.execute(
            "create table test(id integer primary key, value text);
             insert into test(value) values ('main');
             create temp table temp_values(value integer);
             insert into temp_values values (3), (1), (2);",
        )
        .unwrap();

        let mut values = Vec::new();
        conn.prepare("select value from temp_values order by value")
            .unwrap()
            .run_with_row_callback(|row| {
                values.push(row.get::<i64>(0).unwrap());
                Ok(())
            })
            .unwrap();
        assert_eq!(values, vec![1, 2, 3]);
        assert_eq!(count_test_rows(&conn), 1);
        drop(conn);
        drop(db);

        let db = open_with_page_codec(io, path, codec.clone());
        let conn = db.connect_with_page_codec(codec).unwrap();
        assert_eq!(count_test_rows(&conn), 1);
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_connections_preserve_old_reader_snapshot_during_checkpoint() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-connections.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec = || -> Arc<dyn PageCodec> {
            Arc::new(XorPageCodec {
                mask: 0x5a,
                reserved_bytes: 1,
            })
        };
        let db = open_with_page_codec(io.clone(), path, codec());
        let writer = db.connect_with_page_codec(codec()).unwrap();
        let old_reader = db.connect_with_page_codec(codec()).unwrap();
        writer
            .execute(
                "create table test(id integer primary key, value text);
                 insert into test(value) values ('first');",
            )
            .unwrap();

        old_reader.execute("BEGIN").unwrap();
        assert_eq!(count_test_rows(&old_reader), 1);
        writer
            .execute("insert into test(value) values ('second')")
            .unwrap();
        assert_eq!(count_test_rows(&old_reader), 1);

        let new_reader = db.connect_with_page_codec(codec()).unwrap();
        assert_eq!(count_test_rows(&new_reader), 2);
        let passive = writer
            .checkpoint(crate::CheckpointMode::Passive {
                upper_bound_inclusive: None,
            })
            .unwrap();
        assert!(passive.wal_total_backfilled < passive.wal_max_frame);
        assert_eq!(count_test_rows(&old_reader), 1);
        assert_eq!(count_test_rows(&new_reader), 2);

        old_reader.execute("COMMIT").unwrap();
        writer.checkpoint(crate::CheckpointMode::Full).unwrap();
        drop(new_reader);
        drop(old_reader);
        drop(writer);
        drop(db);

        let db = open_with_page_codec(io, path, codec());
        let conn = db.connect_with_page_codec(codec()).unwrap();
        assert_eq!(count_test_rows(&conn), 2);
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_accepts_minimum_usable_space_boundary() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-minimum-usable-space.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
            mask: 0x5a,
            reserved_bytes: 32,
        });

        {
            let db = open_with_page_codec(io.clone(), path, codec.clone());
            let conn = db.connect_with_page_codec(codec.clone()).unwrap();
            conn.execute("PRAGMA page_size = 512").unwrap();
            assert_eq!(conn.get_page_size().get(), 512);
            conn.execute(
                "create table test(id integer primary key, value blob);
                 insert into test(value) values (zeroblob(2000));",
            )
            .unwrap();
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }

        let db = open_with_page_codec(io, path, codec.clone());
        let conn = db.connect_with_page_codec(codec).unwrap();
        assert_eq!(conn.get_page_size().get(), 512);
        assert_eq!(count_test_rows(&conn), 1);
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_reserved_tail_detects_persistent_page_corruption() {
        use std::fs::OpenOptions as StdOpenOptions;
        use std::io::{Read, Seek, SeekFrom, Write};

        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-tagged-page.db");
        let path_str = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec: Arc<dyn PageCodec> = Arc::new(TaggedPageCodec);

        {
            let db = open_with_page_codec(io.clone(), path_str, codec.clone());
            let conn = db.connect_with_page_codec(codec.clone()).unwrap();
            conn.execute("PRAGMA page_size = 512").unwrap();
            conn.execute(
                "create table test(id integer primary key, value blob);
                 insert into test(value) values (zeroblob(2000));",
            )
            .unwrap();
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            assert_eq!(count_test_rows(&conn), 1);
        }

        let tag_offset = 2 * 512 - 1;
        let mut file = StdOpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(tag_offset)).unwrap();
        let mut original = [0];
        file.read_exact(&mut original).unwrap();
        file.seek(SeekFrom::Start(tag_offset)).unwrap();
        file.write_all(&[original[0] ^ 1]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        {
            let db = open_with_page_codec(io.clone(), path_str, codec.clone());
            let conn = db.connect_with_page_codec(codec.clone()).unwrap();
            let err = try_count_test_rows(&conn).unwrap_err();
            assert!(matches!(
                err,
                LimboError::CompletionError(CompletionError::PageCodecError { page_idx: 2 })
            ));
        }

        let mut file = StdOpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(tag_offset)).unwrap();
        file.write_all(&original).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let db = open_with_page_codec(io, path_str, codec.clone());
        let conn = db.connect_with_page_codec(codec).unwrap();
        assert_eq!(count_test_rows(&conn), 1);
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_reopens_checkpointed_database_read_only() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-read-only.db");
        let path_str = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
            mask: 0x5a,
            reserved_bytes: 1,
        });

        {
            let db = open_with_page_codec(io.clone(), path_str, codec.clone());
            let conn = db.connect_with_page_codec(codec.clone()).unwrap();
            conn.execute(
                "create table test(id integer primary key, value text);
                 insert into test(value) values ('read-only');",
            )
            .unwrap();
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }
        let before = std::fs::metadata(&path).unwrap().modified().unwrap();

        let db = Database::open(
            io,
            path_str,
            OpenOptions::new(Arc::new(SqliteDialect))
                .flags(OpenFlags::ReadOnly)
                .page_codec(codec.clone()),
        )
        .unwrap();
        let conn = db.connect_with_page_codec(codec).unwrap();
        assert_eq!(count_test_rows(&conn), 1);
        drop(conn);
        drop(db);
        assert_eq!(std::fs::metadata(path).unwrap().modified().unwrap(), before);
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_rejects_database_already_in_mvcc_mode() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-existing-mvcc.db");
        let path_str = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());

        {
            let db = Database::open_file_with_flags(
                io.clone(),
                path_str,
                OpenFlags::Create,
                DatabaseOpts::new(),
                None,
                Arc::new(SqliteDialect),
            )
            .unwrap();
            let conn = db.connect().unwrap();
            conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        }

        let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
            mask: 0,
            reserved_bytes: if cfg!(feature = "checksum") {
                crate::storage::checksum::CHECKSUM_REQUIRED_RESERVED_BYTES
            } else {
                0
            },
        });
        let err = open_with_page_codec_result(io.clone(), path_str, codec).unwrap_err();
        assert!(
            err.to_string()
                .contains("external page codecs are not supported with MVCC databases"),
            "unexpected error: {err}"
        );

        let db = Database::open_file_with_flags(
            io,
            path_str,
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        db.connect().unwrap();
    }

    #[cfg(feature = "fs")]
    #[test]
    fn location_page_codec_round_trips_wal_checkpoint_and_overflow_pages() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("location-codec.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec: Arc<dyn PageCodec> = Arc::new(LocationPageCodec);

        {
            let db = open_with_page_codec(io.clone(), path, codec.clone());
            let conn = db.connect_with_page_codec(codec.clone()).unwrap();
            conn.execute(
                "create table test(id integer primary key, value blob);
                 insert into test(value) values (zeroblob(12000));
                 delete from test where id = 1;
                 insert into test(value)
                 values (cast(replace(printf('%0*d', 16000, 0), '0', 'x') as blob));",
            )
            .unwrap();
            let checkpoint = conn
                .checkpoint(crate::storage::wal::CheckpointMode::Full)
                .unwrap();
            assert!(checkpoint.wal_checkpoint_backfilled > 0);
        }

        let raw_database = std::fs::read(path).unwrap();
        assert_ne!(&raw_database[..16], b"SQLite format 3\0");
        let reopened = open_with_page_codec(io, path, codec.clone());
        let conn = reopened.connect_with_page_codec(codec).unwrap();
        let mut values: Vec<(i64, String)> = Vec::new();
        conn.prepare("select length(value), hex(substr(value, 1, 8)) from test order by id")
            .unwrap()
            .run_with_row_callback(|row| {
                values.push((row.get(0).unwrap(), row.get(1).unwrap()));
                Ok(())
            })
            .unwrap();
        assert_eq!(values, vec![(16_000, "7878787878787878".to_string())]);
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_attach_is_rejected() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let main_path = temp_dir.path().join("codec-main.db");
        let aux_path = temp_dir.path().join("codec-aux.db");
        let main_path = main_path.to_str().unwrap();
        let aux_path = aux_path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
            mask: 0x91,
            reserved_bytes: 1,
        });
        let db = open_with_page_codec_with_opts_result(
            io.clone(),
            main_path,
            codec.clone(),
            DatabaseOpts::new().with_attach(true),
        )
        .unwrap();
        let conn = db.connect_with_page_codec(codec.clone()).unwrap();
        let err = conn
            .execute(format!("ATTACH '{aux_path}' AS aux"))
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("ATTACH is unsupported for connections using an external page codec"));
        assert!(!std::path::Path::new(aux_path).exists());
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_rejects_reserved_space_mutation() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-reserved-space.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
            mask: 0x91,
            reserved_bytes: 1,
        });
        let db = open_with_page_codec(io, path, codec.clone());
        let conn = db.connect_with_page_codec(codec).unwrap();

        let err = conn.set_reserved_bytes(0).unwrap_err();
        assert!(err
            .to_string()
            .contains("page codec requires exactly 1 reserved bytes"));
        assert_eq!(conn.get_reserved_bytes(), Some(1));
    }

    #[cfg(feature = "fs")]
    #[test]
    fn page_codec_rejects_page_size_incompatible_with_reserved_space() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("codec-page-size.db");
        let path = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let codec: Arc<dyn PageCodec> = Arc::new(XorPageCodec {
            mask: 0x5a,
            reserved_bytes: 33,
        });

        {
            let db = open_with_page_codec(io.clone(), path, codec.clone());
            let conn = db.connect_with_page_codec(codec.clone()).unwrap();
            // 512 - 33 = 479 usable bytes < 480: must be rejected up front,
            // otherwise the engine creates a database it refuses to reopen.
            let err = conn.execute("PRAGMA page_size = 512").unwrap_err();
            assert!(err.to_string().contains("usable bytes"));
            assert_eq!(conn.get_page_size().get(), 4096);
            // The database must remain usable at a compatible page size.
            conn.execute("create table test(id integer primary key, value text)")
                .unwrap();
            conn.execute("insert into test(value) values ('alpha')")
                .unwrap();
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }

        let reopened = open_with_page_codec(io, path, codec.clone());
        let conn = reopened.connect_with_page_codec(codec).unwrap();
        assert_eq!(count_test_rows(&conn), 1);
    }
}
