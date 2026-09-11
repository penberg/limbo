use crate::SqliteDialect;
use rustc_hash::FxHashSet as HashSet;

use super::*;
use crate::alloc::{TursoAllocator, TursoIteratorExt, TursoTryWithCapacityExt};
use crate::io::{PlatformIO, IO};
use crate::mvcc::clock::MvccClock;
use crate::mvcc::cursor::{CursorYieldPoint, MvccCursorType};
use crate::mvcc::database::checkpoint_state_machine::CheckpointYieldPoint;
use crate::mvcc::database::{CommitYieldPoint, ExclusiveTxYieldPoint};
use crate::mvcc::persistent_storage::logical_log::{
    LogicalLog, ENCRYPTED_PAYLOAD_CHUNK_SIZE, EXT_FRAME_MAGIC, FRAME_MAGIC, LOG_HDR_SIZE,
};
#[cfg(feature = "conn_raw_api")]
use crate::mvcc::persistent_storage::logical_log::{ParsedOp, StreamingLogicalLogReader};
#[cfg(feature = "conn_raw_api")]
use crate::mvcc::portable_logical::{PortableLogicalBuilder, PortableObjectMapEntry};
use crate::mvcc::yield_hooks::YieldPointMarker;
use crate::mvcc::yield_points::{FailureInjector, YieldInjector, YieldPoint};
use crate::state_machine::{StateTransition, TransitionResult};
use crate::storage::sqlite3_ondisk::{
    checksum_wal, read_varint, write_varint, DatabaseHeader, WalHeader, WAL_FRAME_HEADER_SIZE,
    WAL_HEADER_SIZE,
};
use crate::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use crate::sync::Mutex;
use crate::sync::RwLock;
use crate::types::ImmutableRecordRef;
use crate::vdbe::execute::TransactionYieldPoint;
use crate::{
    Buffer, Completion, DatabaseOpts, EncryptionKey, LimboError, OpenFlags, StatementStatusCounter,
};
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::sync::Arc;

const TX_BASE_HEADER_SIZE: usize = 24;
const TX_EXT_HEADER_SIZE: usize = 40;
const TX_TRAILER_SIZE: usize = 8;

pub(crate) struct MvccTestDbNoConn {
    pub(crate) db: Option<Arc<Database>>,
    path: Option<String>,
    opts: DatabaseOpts,
    enc_opts: Option<crate::EncryptionOpts>,
    // Stored mainly to not drop the temp dir before the test is done.
    _temp_dir: Option<tempfile::TempDir>,
}
pub(crate) struct MvccTestDb {
    pub(crate) mvcc_store: Arc<crate::MvStore>,
    pub(crate) db: Arc<Database>,
    pub(crate) conn: Arc<Connection>,
}

#[derive(Debug)]
struct FixedYieldInjector {
    remaining: Mutex<HashSet<YieldPoint>>,
}

impl FixedYieldInjector {
    fn new(points: impl IntoIterator<Item = YieldPoint>) -> Arc<Self> {
        Arc::new(Self {
            remaining: Mutex::new(points.into_iter().collect()),
        })
    }

    fn is_empty(&self) -> bool {
        self.remaining.lock().is_empty()
    }

    fn remaining_len(&self) -> usize {
        self.remaining.lock().len()
    }
}

impl YieldInjector for FixedYieldInjector {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        self.remaining.lock().remove(&point)
    }
}

fn drive_attach(conn: &Arc<Connection>, path: &str, alias: &str) {
    let mut state = crate::connection::AttachDatabaseState::default();
    loop {
        match conn.attach_database(path, alias, &mut state).unwrap() {
            crate::IOResult::Done(()) => return,
            crate::IOResult::IO(io) => io.wait(conn.db.io.as_ref()).unwrap(),
        }
    }
}

struct CommitWriterOnExclusiveAcquireInjector {
    point: YieldPoint,
    selection_key: u64,
    writer: Arc<Connection>,
    fired: AtomicBool,
}

impl CommitWriterOnExclusiveAcquireInjector {
    fn new(point: YieldPoint, selection_key: u64, writer: Arc<Connection>) -> Arc<Self> {
        Arc::new(Self {
            point,
            selection_key,
            writer,
            fired: AtomicBool::new(false),
        })
    }

    fn fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }
}

impl std::fmt::Debug for CommitWriterOnExclusiveAcquireInjector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitWriterOnExclusiveAcquireInjector")
            .field("point", &self.point)
            .field("selection_key", &self.selection_key)
            .field("fired", &self.fired())
            .finish_non_exhaustive()
    }
}

impl YieldInjector for CommitWriterOnExclusiveAcquireInjector {
    fn should_yield(&self, _instance_id: u64, selection_key: u64, point: YieldPoint) -> bool {
        if point != self.point
            || selection_key != self.selection_key
            || self.fired.swap(true, Ordering::AcqRel)
        {
            return false;
        }
        self.writer.execute("COMMIT").unwrap();
        true
    }
}

struct CheckpointingDeleteAtMvccBeginInjector {
    writer: Arc<Connection>,
    fired: AtomicBool,
}

impl CheckpointingDeleteAtMvccBeginInjector {
    fn new(writer: Arc<Connection>) -> Arc<Self> {
        Arc::new(Self {
            writer,
            fired: AtomicBool::new(false),
        })
    }

    fn fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }
}

impl std::fmt::Debug for CheckpointingDeleteAtMvccBeginInjector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointingDeleteAtMvccBeginInjector")
            .field("fired", &self.fired())
            .finish_non_exhaustive()
    }
}

impl YieldInjector for CheckpointingDeleteAtMvccBeginInjector {
    fn should_yield(&self, _instance_id: u64, selection_key: u64, point: YieldPoint) -> bool {
        if point != TransactionYieldPoint::BeforeMvccBegin.point()
            || selection_key != crate::MAIN_DB_ID as u64
            || self.fired.swap(true, Ordering::AcqRel)
        {
            return false;
        }

        self.writer
            .execute("PRAGMA mvcc_checkpoint_threshold = 0")
            .unwrap();
        self.writer
            .execute("DELETE FROM keep WHERE id = 1")
            .unwrap();
        false
    }
}

struct CheckpointingInsertAtMvccBeginInjector {
    writer: Arc<Connection>,
    fired: AtomicBool,
}

impl CheckpointingInsertAtMvccBeginInjector {
    fn new(writer: Arc<Connection>) -> Arc<Self> {
        Arc::new(Self {
            writer,
            fired: AtomicBool::new(false),
        })
    }

    fn fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }
}

impl std::fmt::Debug for CheckpointingInsertAtMvccBeginInjector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointingInsertAtMvccBeginInjector")
            .field("fired", &self.fired())
            .finish_non_exhaustive()
    }
}

impl YieldInjector for CheckpointingInsertAtMvccBeginInjector {
    fn should_yield(&self, _instance_id: u64, selection_key: u64, point: YieldPoint) -> bool {
        if point != TransactionYieldPoint::BeforeMvccBegin.point()
            || selection_key != crate::MAIN_DB_ID as u64
            || self.fired.swap(true, Ordering::AcqRel)
        {
            return false;
        }

        self.writer
            .execute("PRAGMA mvcc_checkpoint_threshold = 0")
            .unwrap();
        self.writer
            .execute("INSERT INTO keep VALUES (2, zeroblob(50000))")
            .unwrap();
        false
    }
}

#[derive(Debug)]
struct FixedFailureInjector {
    remaining: Mutex<rustc_hash::FxHashMap<YieldPoint, LimboError>>,
}

impl FixedFailureInjector {
    fn new(points: impl IntoIterator<Item = (YieldPoint, LimboError)>) -> Arc<Self> {
        Arc::new(Self {
            remaining: Mutex::new(points.into_iter().collect()),
        })
    }

    fn is_empty(&self) -> bool {
        self.remaining.lock().is_empty()
    }
}

impl FailureInjector for FixedFailureInjector {
    fn should_fail(
        &self,
        _instance_id: u64,
        _selection_key: u64,
        point: YieldPoint,
    ) -> Option<LimboError> {
        self.remaining.lock().remove(&point)
    }
}

#[derive(Clone, Default)]
struct FailOnDemandAlloc {
    fail: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl FailOnDemandAlloc {
    fn fail_allocations(&self, fail: bool) {
        self.fail.store(fail, std::sync::atomic::Ordering::Relaxed);
    }
}

#[test]
fn write_set_take_transfers_entries_without_cloning() {
    let mut write_set = WriteSet::<TursoAllocator>::new();
    let row_versions: RowVersions<TursoAllocator> = Arc::new(RwLock::new(crate::alloc::vec![]));
    let row_id = RowID::new(MVTableId::from(-2), RowKey::Int(1));

    assert!(write_set.insert(row_id, Arc::clone(&row_versions)));
    let entries_ptr = write_set.entries.as_ptr();
    let entries_capacity = write_set.entries.capacity();
    let strong_count = Arc::strong_count(&row_versions);

    let transferred = write_set.take();

    assert!(write_set.entries.is_empty());
    assert!(write_set.seen.is_empty());
    assert_eq!(transferred.entries.as_ptr(), entries_ptr);
    assert_eq!(transferred.entries.capacity(), entries_capacity);
    assert_eq!(transferred.seen.len(), 1);
    assert_eq!(Arc::strong_count(&row_versions), strong_count);
    assert!(Arc::ptr_eq(&transferred.entries[0].1, &row_versions));
}

#[test]
#[should_panic(expected = "write set cannot be modified unless transaction is active")]
fn aborted_transaction_rejects_write_set_insert() {
    let tx = new_tx(1, 1, TransactionState::Aborted);
    let row_versions: RowVersions<TursoAllocator> = Arc::new(RwLock::new(crate::alloc::vec![]));
    let row_id = RowID::new(MVTableId::from(-2), RowKey::Int(1));

    tx.insert_to_write_set(row_id, row_versions);
}

unsafe impl crate::alloc::ApiAllocator for FailOnDemandAlloc {
    fn allocate(
        &self,
        layout: crate::alloc::Layout,
    ) -> std::result::Result<std::ptr::NonNull<[u8]>, crate::alloc::AllocError> {
        if self.fail.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(crate::alloc::AllocError);
        }
        <crate::alloc::TursoAllocator as crate::alloc::ApiAllocator>::allocate(
            &crate::alloc::TursoAllocator,
            layout,
        )
    }

    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: crate::alloc::Layout) {
        unsafe {
            <crate::alloc::TursoAllocator as crate::alloc::ApiAllocator>::deallocate(
                &crate::alloc::TursoAllocator,
                ptr,
                layout,
            )
        }
    }
}

fn test_mvcc_storage(name: &str) -> Arc<dyn crate::mvcc::persistent_storage::DurableStorage> {
    let io = Arc::new(MemoryIO::new());
    let file = io.open_file(name, OpenFlags::Create, false).unwrap();
    Arc::new(crate::mvcc::persistent_storage::Storage::new(
        file, io, None,
    ))
}

#[test]
fn mv_store_skiplist_allocations_are_fallible() {
    let alloc = FailOnDemandAlloc::default();
    alloc.fail_allocations(true);
    let store = MvStore::new_in(
        MvccClock::new(),
        test_mvcc_storage("mv-store-oom-new.db-log"),
        alloc.clone(),
        false,
    );
    assert!(matches!(store, Err(LimboError::OutOfMemory)));

    alloc.fail_allocations(false);
    let store = MvStore::new_in(
        MvccClock::new(),
        test_mvcc_storage("mv-store-oom-insert.db-log"),
        alloc.clone(),
        false,
    )
    .unwrap();
    alloc.fail_allocations(true);

    let row_id = RowID::new(MVTableId::from(-2), RowKey::Int(1));
    let row_version = RowVersion {
        id: 1,
        begin: PackedTs::pack(Some(TxTimestampOrID::TxID(1))),
        end: PackedTs::pack(None),
        row: Row::new_table_row(row_id.clone(), &[], 0).unwrap(),
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };
    let result = store.insert_version(row_id, row_version);
    assert!(matches!(result, Err(crate::alloc::TryReserveError)));
    assert!(store.rows.is_empty());
}

#[cfg(nightly)]
#[test]
fn row_payload_allocation_uses_passed_allocator() {
    let alloc = FailOnDemandAlloc::default();
    let row_id = RowID::new(MVTableId::from(-2), RowKey::Int(1));

    alloc.fail_allocations(true);
    let result = Row::new_table_row_in(row_id.clone(), &[1, 2, 3], 1, alloc.clone());
    assert!(matches!(result, Err(crate::alloc::TryReserveError)));

    alloc.fail_allocations(false);
    let row = Row::new_table_row_in(row_id, &[1, 2, 3], 1, alloc).unwrap();
    assert_eq!(row.payload(), &[1, 2, 3]);
}

#[cfg(nightly)]
#[test]
fn index_key_payload_allocation_uses_passed_allocator() {
    let alloc = FailOnDemandAlloc::default();
    let record = ImmutableRecord::from_values(&[Value::from_i64(42)], 1).unwrap();
    let record_ref = ImmutableRecordRef::from_bin_record(record.get_payload());
    let index_info = Arc::new(
        IndexInfo::new(
            [crate::types::KeyInfo {
                sort_order: turso_parser::ast::SortOrder::Asc,
                collation: crate::translate::collate::CollationSeq::Binary,
                nulls_order: None,
            }],
            false,
            1,
            false,
        )
        .unwrap(),
    );

    alloc.fail_allocations(true);
    let result = SortableIndexKey::new_from_payload_in(&record, index_info.clone(), alloc.clone());
    assert!(matches!(result, Err(crate::alloc::TryReserveError)));

    alloc.fail_allocations(false);
    let key = SortableIndexKey::new_from_payload_in(record_ref, index_info, alloc).unwrap();
    assert_eq!(key.key.get_payload(), record.get_payload());
}

#[cfg(nightly)]
#[test]
#[should_panic(expected = "empty tombstone row")]
fn seqcompact_tombstone_payload_uses_store_allocator() {
    let alloc = FailOnDemandAlloc::default();
    let store = MvStore::new_in(
        MvccClock::new(),
        test_mvcc_storage("mv-store-seqcompact-allocator.db-log"),
        alloc.clone(),
        false,
    )
    .unwrap();
    let row_id = RowID::new(MVTableId::from(-2), RowKey::Int(1));
    let versions = store
        .get_or_create_table_row_versions(row_id.clone())
        .unwrap();
    versions.write().try_reserve(1).unwrap();

    alloc.fail_allocations(true);
    store.seqcompact_commit_delete(row_id, 1, 10);
}

#[test]
fn mv_store_insert_allocation_failure_leaves_tx_state_untouched() {
    let alloc = FailOnDemandAlloc::default();
    let store = MvStore::new_in(
        MvccClock::new(),
        test_mvcc_storage("mv-store-oom-insert-ordering.db-log"),
        alloc.clone(),
        false,
    )
    .unwrap();

    let tx_id = 7;
    let tx = new_tx_in::<FailOnDemandAlloc>(tx_id, 1, TransactionState::Active);
    tx.begin_savepoint();
    store.txs.try_insert(tx_id, tx).unwrap();

    let table_id = MVTableId::from(-2);
    let row_id = RowID::new(table_id, RowKey::Int(42));
    let row = Row::new_table_row(row_id.clone(), &[], 0).unwrap();
    let allocator = store.get_rowid_allocator(&table_id);

    alloc.fail_allocations(true);
    let result = store.insert(tx_id, row);
    assert!(matches!(result, Err(LimboError::OutOfMemory)));

    assert!(store.rows.get(&row_id).is_none());
    assert_eq!(allocator.max_rowid.load(Ordering::SeqCst), 0);

    let tx = store.txs.get(&tx_id).unwrap();
    let tx = tx.value();
    assert!(tx.write_set.lock().is_empty());

    let savepoints = tx.savepoint_stack.read();
    let savepoint = savepoints.last().unwrap();
    assert!(savepoint.created_table_versions.is_empty());
    assert!(savepoint.newly_added_to_write_set.is_empty());
}

impl MvccTestDb {
    pub fn new() -> Self {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        // Enable MVCC via PRAGMA
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        let mvcc_store = db.get_mv_store().clone().unwrap();
        Self {
            mvcc_store,
            db,
            conn,
        }
    }

    #[cfg(feature = "conn_raw_api")]
    fn new_with_portable_logical_changes() -> Self {
        let db = Self::new();
        db.conn.set_portable_logical_changes_enabled(true);
        db
    }
}

#[test]
fn mvcc_active_read_tx_blocks_vacuum_gate() {
    let db = MvccTestDb::new();
    let pager = db.conn.pager.load().clone();
    let tx_id = db.mvcc_store.begin_tx(pager).unwrap();

    assert!(matches!(
        db.mvcc_store.try_begin_vacuum_gate(),
        Err(LimboError::Busy)
    ));

    db.mvcc_store.remove_tx(tx_id).unwrap();
    db.mvcc_store.try_begin_vacuum_gate().unwrap();
    db.mvcc_store.release_vacuum_gate();
}

#[test]
fn mvcc_active_write_tx_blocks_vacuum_gate() {
    let db = MvccTestDb::new();
    let pager = db.conn.pager.load().clone();
    let tx_id = db
        .mvcc_store
        .begin_exclusive_tx(pager.clone(), None, &db.conn, None)
        .unwrap();

    assert!(matches!(
        db.mvcc_store.try_begin_vacuum_gate(),
        Err(LimboError::Busy)
    ));

    db.mvcc_store
        .rollback_tx(tx_id, pager, &db.conn, crate::MAIN_DB_ID);
    db.mvcc_store.try_begin_vacuum_gate().unwrap();
    db.mvcc_store.release_vacuum_gate();
}

#[test]
fn mvcc_vacuum_gate_blocks_new_read_and_write_tx() {
    let db = MvccTestDb::new();
    let pager = db.conn.pager.load().clone();

    db.mvcc_store.try_begin_vacuum_gate().unwrap();

    assert!(matches!(
        db.mvcc_store.begin_tx(pager.clone()),
        Err(LimboError::Busy)
    ));
    assert!(matches!(
        db.mvcc_store
            .begin_exclusive_tx(pager, None, &db.conn, None),
        Err(LimboError::Busy)
    ));

    db.mvcc_store.release_vacuum_gate();
}

#[test]
fn mvcc_pragma_page_size_propagates_to_global_header() {
    // MvStore captures global_header from the pager during bootstrap (before any user PRAGMA
    // can run), so without explicit propagation a later `PRAGMA page_size = N` updates the
    // pager but leaves global_header at the default 4 KiB. Ephemeral paths that derive the
    // working page size from MvStore would then disagree with the pager's actual buffer size.
    let db = MvccTestDb::new();

    let initial = db
        .mvcc_store
        .with_header(|h| h.page_size.get(), None)
        .unwrap();
    assert_eq!(
        initial,
        crate::storage::buffer_pool::BufferPool::DEFAULT_PAGE_SIZE as u32,
        "global_header should start at the default page size"
    );

    db.conn.execute("PRAGMA page_size = 512").unwrap();

    let after = db
        .mvcc_store
        .with_header(|h| h.page_size.get(), None)
        .unwrap();
    assert_eq!(
        after, 512,
        "PRAGMA page_size must propagate to MvStore.global_header"
    );
}

#[test]
fn mvcc_reset_after_vacuum_installs_header_and_rootpages() {
    let db = MvccTestDb::new();
    db.conn
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    db.conn.execute("CREATE INDEX idx_t_v ON t(v)").unwrap();
    db.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    db.conn.demote_to_mvcc_connection();
    db.conn.reparse_schema().unwrap();
    let schema = db.conn.schema.read().clone();
    db.conn.promote_to_regular_connection();
    let table_root = match schema.tables.get("t").expect("table t").as_ref() {
        Table::BTree(btree) => btree.root_page,
        _ => panic!("expected btree table"),
    };
    let index_root = schema
        .indexes
        .get("t")
        .and_then(|indexes| indexes.front())
        .map(|index| index.root_page)
        .expect("index idx_t_v");

    let mut header = DatabaseHeader::default();
    header.schema_cookie = 77.into();

    db.mvcc_store
        .global_header
        .write()
        .replace(DatabaseHeader::default());
    db.mvcc_store
        .insert_table_id_to_rootpage(MVTableId::from(-999_i64), Some(999));

    db.mvcc_store.try_begin_vacuum_gate().unwrap();
    db.mvcc_store.reset_after_vacuum(header, schema.as_ref());
    db.mvcc_store.release_vacuum_gate();

    assert_eq!(
        db.mvcc_store
            .with_header(|header| header.schema_cookie.get(), None)
            .unwrap(),
        77
    );
    assert_eq!(
        db.mvcc_store
            .current_root_page(&SQLITE_SCHEMA_MVCC_TABLE_ID),
        Some(1)
    );
    assert_eq!(
        db.mvcc_store
            .current_root_page(&MVTableId::from(-(table_root))),
        Some(table_root as u64)
    );
    assert_eq!(
        db.mvcc_store
            .current_root_page(&MVTableId::from(-(index_root))),
        Some(index_root as u64)
    );
    assert!(
        db.mvcc_store
            .table_id_to_rootpage
            .get(&MVTableId::from(-999_i64))
            .is_none(),
        "stale root-page entries must be cleared"
    );
}

#[test]
fn mvcc_passive_gc_retains_until_reader_mark_reaches_materialization() {
    use crate::mvcc::database::WalPos;
    let frame = |f: u64| WalPos {
        checkpoint_seq: 1,
        frame: f,
    };

    // Sole current insert (begin=Ts(5), end=None), materialized at WAL frame 100.
    let stamped_insert = || {
        let mut rv = make_rv(ts(5), None);
        rv.set_materialized_at(frame(100));
        crate::alloc::vec![rv]
    };
    // Superseded delete (begin=Ts(3), end=Ts(5)<=lwm), materialized at frame 100.
    let stamped_delete = || {
        let mut rv = make_rv(ts(3), ts(5));
        rv.set_materialized_at(frame(100));
        crate::alloc::vec![rv]
    };

    // Reader pinned below the materialization frame: keep both current and delete.
    for mut v in [stamped_insert(), stamped_delete()] {
        let dropped =
            MvStore::<MvccClock>::gc_version_chain(&mut v, 10, 10, true, frame(50), false);
        assert_eq!(
            dropped, 0,
            "version needed by a reader pinned below frame 100 must be kept"
        );
        assert_eq!(v.len(), 1);
    }

    // Readers at the materialization frame: Rule 2 drops deletes; Passive keeps
    // currents unless drop_current_if_in_btree (Truncate).
    let mut deleted = stamped_delete();
    let dropped =
        MvStore::<MvccClock>::gc_version_chain(&mut deleted, 10, 10, true, frame(100), false);
    assert_eq!(
        dropped, 1,
        "materialized + reader-reachable superseded delete must be reclaimed"
    );
    assert!(deleted.is_empty());

    let mut current = stamped_insert();
    let dropped =
        MvStore::<MvccClock>::gc_version_chain(&mut current, 10, 10, true, frame(100), false);
    assert_eq!(
        dropped, 0,
        "Passive keeps the current SkipMap version when drop_current_if_in_btree is false"
    );
    assert_eq!(current.len(), 1);

    let mut current = stamped_insert();
    let dropped =
        MvStore::<MvccClock>::gc_version_chain(&mut current, 10, 10, true, frame(100), true);
    assert_eq!(
        dropped, 0,
        "Passive keeps a live copy while a snapshot is open, even when materialized"
    );
    assert_eq!(current.len(), 1);

    let mut current = stamped_insert();
    let dropped =
        MvStore::<MvccClock>::gc_version_chain(&mut current, u64::MAX, 10, true, frame(100), true);
    assert_eq!(
        dropped, 1,
        "Passive Rule 3 drops a materialized current once no snapshot is open"
    );
    assert!(current.is_empty());

    // Unmaterialized versions are never reclaimed on the Passive path.
    for drop_current_if_in_btree in [false, true] {
        let mut v = crate::alloc::vec![make_rv(ts(5), None)];
        let dropped = MvStore::<MvccClock>::gc_version_chain(
            &mut v,
            10,
            10,
            true,
            WalPos::STAGED,
            drop_current_if_in_btree,
        );
        assert_eq!(
            dropped, 0,
            "passive GC must not reclaim a version not yet in the B-tree"
        );
        assert_eq!(v.len(), 1);
    }
}

/// Ignored Truncate-vs-Passive GC metrics harness.
///
/// ```console
/// cargo test -p turso_core --lib debug_gc_metrics_truncate_vs_passive -- --ignored --nocapture
/// ```
#[test]
#[ignore = "manual GC metrics debug harness"]
fn debug_gc_metrics_truncate_vs_passive() {
    fn sample_line(
        mode: &str,
        step: usize,
        updates: usize,
        snap: &crate::mvcc::database::GcDebugSnapshot,
    ) {
        println!(
            "{mode},step={step},updates={updates},\
             rows_slots={},rows_empty={},rows_versions={},\
             index_slots={},index_empty={},index_versions={},\
             live_approx={},live_at_last_gc={},\
             lwm={},durable_max={},\
             log_offset={},log_size={},txs={},\
             min_reader=({},{}),backfill=({},{})",
            snap.rows_slots,
            snap.rows_empty_slots,
            snap.rows_versions,
            snap.index_slots,
            snap.index_empty_slots,
            snap.index_versions,
            snap.live_version_count_approx,
            snap.live_versions_at_last_gc,
            snap.lwm,
            snap.durable_txid_max,
            snap.logical_log_offset,
            snap.logical_log_size,
            snap.active_txs,
            snap.min_reader_mark.checkpoint_seq,
            snap.min_reader_mark.frame,
            snap.backfill_floor.checkpoint_seq,
            snap.backfill_floor.frame,
        );
    }

    /// Pinned-reader UPDATE load; explicit checkpoints.
    fn run_mode(passive: bool, keys: usize, rounds: usize, updates_per_round: usize) {
        let mode = if passive { "passive" } else { "truncate" };
        let db = if passive {
            MvccTestDbNoConn::new_with_random_db_passive()
        } else {
            MvccTestDbNoConn::new_with_random_db()
        };
        let mv = db.get_mvcc_store();
        let bootstrap = db.connect();
        bootstrap
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, data TEXT NOT NULL)")
            .unwrap();
        bootstrap
            .execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        bootstrap.execute("PRAGMA mvcc_gc_threshold = 64").unwrap();

        bootstrap.execute("BEGIN CONCURRENT").unwrap();
        for i in 0..keys {
            bootstrap
                .execute(format!("INSERT INTO t VALUES ({i}, 'seed')"))
                .unwrap();
        }
        bootstrap.execute("COMMIT").unwrap();
        let ckpt = if passive { "PASSIVE" } else { "TRUNCATE" };
        let _ = bootstrap.execute(format!("PRAGMA wal_checkpoint({ckpt})"));

        let reader = db.connect();
        reader.execute("BEGIN CONCURRENT").unwrap();
        reader.execute("SELECT count(*) FROM t").unwrap();

        let writer = db.connect();
        let checkpointer = db.connect();

        println!("# mode={mode} keys={keys} rounds={rounds} updates_per_round={updates_per_round}");
        sample_line(mode, 0, 0, &mv.debug_gc_snapshot());

        let mut updates = 0usize;
        for step in 1..=rounds {
            writer.execute("BEGIN CONCURRENT").unwrap();
            for u in 0..updates_per_round {
                let id = (updates + u) % keys;
                writer
                    .execute(format!("UPDATE t SET data = 'r{step}_{u}' WHERE id = {id}"))
                    .unwrap();
            }
            writer.execute("COMMIT").unwrap();
            updates += updates_per_round;

            let ckpt_res = checkpointer.execute(format!("PRAGMA wal_checkpoint({ckpt})"));
            let ckpt_status = match &ckpt_res {
                Ok(_) => "ok".to_string(),
                Err(e) => format!("err:{e}"),
            };
            let snap = mv.debug_gc_snapshot();
            println!(
                "{mode},step={step},updates={updates},ckpt={ckpt_status},\
                 rows_slots={},rows_empty={},rows_versions={},\
                 live_approx={},lwm={},durable_max={},\
                 log_offset={},log_size={},txs={},\
                 min_reader=({},{}),backfill=({},{})",
                snap.rows_slots,
                snap.rows_empty_slots,
                snap.rows_versions,
                snap.live_version_count_approx,
                snap.lwm,
                snap.durable_txid_max,
                snap.logical_log_offset,
                snap.logical_log_size,
                snap.active_txs,
                snap.min_reader_mark.checkpoint_seq,
                snap.min_reader_mark.frame,
                snap.backfill_floor.checkpoint_seq,
                snap.backfill_floor.frame,
            );
        }

        reader.execute("COMMIT").unwrap();
        let _ = checkpointer.execute(format!("PRAGMA wal_checkpoint({ckpt})"));
        sample_line(mode, rounds + 1, updates, &mv.debug_gc_snapshot());
    }

    /// Same load without a pinned reader.
    fn run_mode_unpinned(passive: bool, keys: usize, rounds: usize, updates_per_round: usize) {
        let mode = if passive {
            "passive_unpinned"
        } else {
            "truncate_unpinned"
        };
        let db = if passive {
            MvccTestDbNoConn::new_with_random_db_passive()
        } else {
            MvccTestDbNoConn::new_with_random_db()
        };
        let mv = db.get_mvcc_store();
        let bootstrap = db.connect();
        bootstrap
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, data TEXT NOT NULL)")
            .unwrap();
        bootstrap
            .execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        bootstrap.execute("PRAGMA mvcc_gc_threshold = 64").unwrap();

        bootstrap.execute("BEGIN CONCURRENT").unwrap();
        for i in 0..keys {
            bootstrap
                .execute(format!("INSERT INTO t VALUES ({i}, 'seed')"))
                .unwrap();
        }
        bootstrap.execute("COMMIT").unwrap();
        let ckpt = if passive { "PASSIVE" } else { "TRUNCATE" };
        let _ = bootstrap.execute(format!("PRAGMA wal_checkpoint({ckpt})"));

        let writer = db.connect();
        let checkpointer = db.connect();
        println!("# mode={mode} keys={keys} rounds={rounds} updates_per_round={updates_per_round}");
        sample_line(mode, 0, 0, &mv.debug_gc_snapshot());

        let mut updates = 0usize;
        for step in 1..=rounds {
            writer.execute("BEGIN CONCURRENT").unwrap();
            for u in 0..updates_per_round {
                let id = (updates + u) % keys;
                writer
                    .execute(format!("UPDATE t SET data = 'r{step}_{u}' WHERE id = {id}"))
                    .unwrap();
            }
            writer.execute("COMMIT").unwrap();
            updates += updates_per_round;

            let ckpt_res = checkpointer.execute(format!("PRAGMA wal_checkpoint({ckpt})"));
            let ckpt_status = match &ckpt_res {
                Ok(_) => "ok".to_string(),
                Err(e) => format!("err:{e}"),
            };
            let snap = mv.debug_gc_snapshot();
            let (wal_seq, wal_max) = checkpointer.pager.load().wal_pos();
            let wal_bf = checkpointer.pager.load().wal_backfill_frame().unwrap_or(0);
            println!(
                "{mode},step={step},updates={updates},ckpt={ckpt_status},\
                 rows_slots={},rows_empty={},rows_versions={},\
                 live_approx={},lwm={},durable_max={},\
                 log_offset={},log_size={},txs={},\
                 min_reader=({},{}),backfill=({},{}),\
                 wal_pos=({wal_seq},{wal_max}),wal_nbackfill={wal_bf}",
                snap.rows_slots,
                snap.rows_empty_slots,
                snap.rows_versions,
                snap.live_version_count_approx,
                snap.lwm,
                snap.durable_txid_max,
                snap.logical_log_offset,
                snap.logical_log_size,
                snap.active_txs,
                snap.min_reader_mark.checkpoint_seq,
                snap.min_reader_mark.frame,
                snap.backfill_floor.checkpoint_seq,
                snap.backfill_floor.frame,
            );
        }
    }

    const KEYS: usize = 200;
    const ROUNDS: usize = 5;
    const UPDATES_PER_ROUND: usize = 50;
    run_mode_unpinned(false, KEYS, ROUNDS, UPDATES_PER_ROUND);
    run_mode_unpinned(true, KEYS, ROUNDS, UPDATES_PER_ROUND);
    run_mode(false, KEYS, ROUNDS, UPDATES_PER_ROUND);
    run_mode(true, KEYS, ROUNDS, UPDATES_PER_ROUND);
}

/// Passive checkpoint publishes nbackfills and reclaims superseded versions.
#[test]
fn mvcc_passive_checkpoint_publishes_backfill_and_reclaims_versions() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let mv = db.get_mvcc_store();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, data TEXT NOT NULL)")
        .unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    for i in 0..50 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, 'seed')"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

    let after_seed = mv.debug_gc_snapshot();
    assert!(
        after_seed.backfill_floor.frame > 0 || after_seed.rows_versions == 0,
        "passive checkpoint must advance backfill_floor or reclaim seed versions: {after_seed:?}"
    );

    for round in 0..5 {
        conn.execute("BEGIN CONCURRENT").unwrap();
        for i in 0..50 {
            conn.execute(format!("UPDATE t SET data = 'r{round}' WHERE id = {i}"))
                .unwrap();
        }
        conn.execute("COMMIT").unwrap();
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    }

    let snap = mv.debug_gc_snapshot();
    let wal_bf = conn.pager.load().wal_backfill_frame().unwrap_or(0);
    assert!(
        wal_bf > 0,
        "passive checkpoint must publish nbackfills, got wal_nbackfill={wal_bf}, snap={snap:?}"
    );
    assert_eq!(
        snap.rows_empty_slots, 0,
        "passive Finalize must drain empty SkipMap slots: {snap:?}"
    );
    assert_eq!(
        snap.backfill_floor.frame, wal_bf,
        "backfill_floor must track published nbackfills: {snap:?}"
    );

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let after_truncate = mv.debug_gc_snapshot();
    assert_eq!(
        after_truncate.rows_versions, 0,
        "truncate Finalize must reclaim SkipMap versions: {after_truncate:?}"
    );
    assert_eq!(
        after_truncate.rows_slots, 0,
        "truncate Finalize must unlink SkipMap slots: {after_truncate:?}"
    );
}

/// Snapshot isolation after Passive Finalize reclaims a materialized current
/// version: a reader whose snapshot predates a later write must still see the
/// pre-write value. Idle-only Rule 3 (`lwm == MAX`) is what keeps a positioned
/// cursor valid while a snapshot is open; this test covers the re-read after
/// GC has already run.
#[test]
fn passive_reader_snapshot_survives_later_write_after_row_versions_gc() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let mv = db.get_mvcc_store();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, bal INTEGER NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1000)").unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    // Seed via an actual PASSIVE checkpoint (not TRUNCATE, which always runs the
    // blocking protocol regardless of `experimental_mvcc_passive_checkpoint`): this
    // materializes row 1 into the B-tree and runs Passive Finalize GC on it.
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    // With no transaction open, Rule 3 clears the sole stamped current in the
    // same Finalize sweep. The reader below must not notice either way.
    assert_eq!(
        mv.debug_gc_snapshot().rows_versions,
        0,
        "with no reader open, Passive Finalize reclaims the materialized version"
    );

    // Reader opens a snapshot BEFORE any further write.
    let reader = db.connect();
    reader.execute("BEGIN CONCURRENT").unwrap();
    let before = get_rows(&reader, "SELECT bal FROM t WHERE id = 1");
    assert_eq!(before, vec![vec![Value::from_i64(1000)]]);

    // Writer updates + immediately passive-checkpoints (threshold=0 => on commit).
    let writer = db.connect();
    writer
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    writer
        .execute("UPDATE t SET bal = 2000 WHERE id = 1")
        .unwrap();

    // Reader re-reads the SAME row within its ALREADY-OPEN snapshot: must still see 1000.
    let after = get_rows(&reader, "SELECT bal FROM t WHERE id = 1");
    reader.execute("COMMIT").unwrap();

    assert_eq!(
        after, before,
        "reader's snapshot must not change mid-transaction"
    );
}

/// Passive checkpoint may run btree writes while a pinned reader is active.
#[test]
fn mvcc_passive_checkpoint_busy_under_pinned_reader_no_corruption() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let writer = db.connect();
    let reader = db.connect();

    writer
        .execute("CREATE TABLE t(k TEXT PRIMARY KEY, v TEXT)")
        .unwrap();
    writer.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

    for i in 0..5 {
        writer
            .execute(format!("INSERT INTO t VALUES ('k{i}', 'v{i}')"))
            .unwrap();
    }

    // Reader opens a snapshot and sees all 5 committed rows.
    reader.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(
        get_rows(&reader, "SELECT count(*) FROM t"),
        vec![vec![Value::from_i64(5)]],
        "reader must see all 5 committed rows",
    );

    // Passive checkpoint may complete while the reader is pinned; it must not
    // corrupt the reader's snapshot.
    writer.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

    assert_eq!(
        get_rows(&reader, "SELECT count(*) FROM t"),
        vec![vec![Value::from_i64(5)]],
        "reader snapshot must be unchanged after checkpoint under pinned reader",
    );
    reader.execute("COMMIT").unwrap();

    assert_eq!(
        get_rows(&db.connect(), "SELECT count(*) FROM t"),
        vec![vec![Value::from_i64(5)]],
    );
}

/// Auto passive checkpoint retries publish when a reader holds the checkpoint lock.
#[test]
fn mvcc_passive_auto_checkpoint_retries_publish_while_reader_pinned() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let mv_store = db.get_mvcc_store();
    mv_store.set_checkpoint_threshold(0);

    let writer = db.connect();
    let reader = db.connect();

    writer
        .execute("CREATE TABLE t(k TEXT PRIMARY KEY, v TEXT)")
        .unwrap();
    writer.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

    reader.execute("BEGIN CONCURRENT").unwrap();

    let durable_before = mv_store.durable_txid_max.load(Ordering::SeqCst);
    let writer_thread = writer;
    let commit_handle = std::thread::spawn(move || {
        writer_thread.execute("BEGIN CONCURRENT").unwrap();
        writer_thread
            .execute("INSERT INTO t VALUES ('hello', 'hello')")
            .unwrap();
        writer_thread.execute("COMMIT").unwrap();
    });

    for _ in 0..10_000 {
        if mv_store.durable_txid_max.load(Ordering::SeqCst) > durable_before {
            break;
        }
        std::thread::yield_now();
    }
    if mv_store.durable_txid_max.load(Ordering::SeqCst) == durable_before {
        reader.execute("COMMIT").unwrap();
    }
    commit_handle.join().unwrap();

    let durable_after = mv_store.durable_txid_max.load(Ordering::SeqCst);
    assert!(
        durable_after > durable_before,
        "auto passive checkpoint should publish durable boundary despite pinned reader (before={durable_before}, after={durable_after})",
    );

    if reader.get_tx_state() != crate::connection::TransactionState::None {
        reader.execute("COMMIT").unwrap();
    }
    assert_eq!(
        get_rows(&db.connect(), "SELECT v FROM t WHERE k = 'hello'"),
        vec![vec![Value::from_text("hello".to_string())]],
    );
}

/// A passive checkpoint that publishes an UNRELATED object's physical roots must NOT invalidate
/// an open transaction reading a different table. Invalidation is per-root and snapshot-scoped
/// (see `MvccLazyCursor::new`): a reader is only re-prepared when the specific root it opens was
/// dropped/reused at its snapshot — not whenever any concurrent checkpoint publishes some root.
#[test]
fn mvcc_passive_unrelated_root_publication_does_not_invalidate_open_txn() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let writer = db.connect();
    let reader = db.connect();

    writer.execute("CREATE TABLE t(x)").unwrap();
    writer.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

    reader.execute("BEGIN CONCURRENT").unwrap();
    // `u` is created and checkpointed AFTER the reader's snapshot; it is irrelevant to `t`.
    writer
        .execute("CREATE TABLE u(y INTEGER PRIMARY KEY)")
        .unwrap();
    writer.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

    assert!(
        reader.execute("SELECT * FROM t").is_ok(),
        "reader of table t must not be invalidated by an unrelated table's passive checkpoint",
    );
    reader.execute("ROLLBACK").unwrap();
}

#[test]
fn mvcc_passive_drop_index_then_reuse_page_integrity() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let conn = &db.connect();
    let assert_ok = |conn: &Arc<Connection>, label: &str| {
        let rows = get_rows(conn, "PRAGMA integrity_check");
        assert_eq!(
            rows,
            vec![vec![Value::from_text("ok".to_string())]],
            "integrity_check not ok ({label}): {rows:?}"
        );
    };

    for i in 0..6 {
        conn.execute(format!(
            "CREATE TABLE t{i}(id INTEGER PRIMARY KEY, a TEXT, b TEXT)"
        ))
        .unwrap();
        conn.execute(format!("CREATE INDEX idx{i}_a ON t{i}(a)"))
            .unwrap();
        conn.execute(format!("INSERT INTO t{i} VALUES ({i}, 'a{i}', 'b{i}')"))
            .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    assert_ok(conn, "after initial build");

    for round in 0..6 {
        for i in 0..6 {
            conn.execute(format!("DROP INDEX idx{i}_a")).unwrap();
        }
        for i in 0..6 {
            conn.execute(format!("CREATE INDEX idx{i}_a ON t{i}(a, b)"))
                .unwrap();
            conn.execute(format!(
                "CREATE TABLE r{round}_{i}(id INTEGER PRIMARY KEY, v TEXT)"
            ))
            .unwrap();
        }
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
        assert_ok(conn, &format!("round {round}"));
    }
}

#[test]
fn mvcc_btree_read_dual_gate() {
    use crate::mvcc::database::WalPos;
    let db = MvccTestDb::new();
    let store = &db.mvcc_store;

    let old_id = MVTableId::from(-50_i64);
    let new_id = MVTableId::from(-900_i64);
    let root: u64 = 286;
    let c_ts = 100u64; // checkpoint snapshot (logical begin)
    let drop = 300u64;
    // The materialization lands at WAL (epoch 2, frame 40). A reader's mark is also a WalPos.
    let mat = WalPos {
        checkpoint_seq: 2,
        frame: 40,
    };
    let mark = |seq, frame| WalPos {
        checkpoint_seq: seq,
        frame,
    };

    // Uncheckpointed: not readable at any snapshot/mark.
    store.insert_table_id_to_rootpage(old_id, None);
    assert!(!store.is_btree_readable_at(&old_id, 200, WalPos::STAGED));

    // STAGED at btree_create: the checkpoint can resolve the root, but NO transaction may read it,
    // not even one whose mark is maximal (STAGED is the not-committed sentinel).
    store.record_rootpage_alloc(old_id, root, c_ts, WalPos::STAGED);
    assert_eq!(store.current_root_page(&old_id), Some(root));
    assert!(!store.is_btree_readable_at(&old_id, c_ts + 50, WalPos::STAGED));

    // PUBLISH (post-CommitPagerTxn): materialized_at set to the WAL position of the frames.
    store.publish_rootpage_visible(old_id, mat);

    // Hazard tx: begin_ts > c_ts (logical OK) but its mark is in the same epoch at a LOWER frame
    // (frame 39 < 40) -> can't reach the frames -> NOT readable; stays version-store-only.
    assert!(!store.is_btree_readable_at(&old_id, c_ts + 50, mark(2, 39)));
    // Mark in the same epoch covering the frames -> readable.
    assert!(store.is_btree_readable_at(&old_id, c_ts + 50, mark(2, 40)));
    // Mark in a LATER epoch -> the frames were backfilled into the DB file before the epoch bumped
    // -> reachable via the base regardless of frame.
    assert!(store.is_btree_readable_at(&old_id, c_ts + 50, mark(3, 0)));
    // Logical gate still required: a snapshot predating c_ts never reads it.
    assert!(!store.is_btree_readable_at(&old_id, c_ts - 1, WalPos::STAGED));

    // Drop + reuse: reverse lookup is end-gated; a pre-drop snapshot still resolves the old owner.
    store.retire_rootpage(old_id, drop);
    store.record_rootpage_alloc(new_id, root, drop + 5, mark(3, 10));
    store.publish_rootpage_visible(new_id, mark(3, 10));
    assert_eq!(
        store.get_table_id_from_root_page_at(root as i64, drop - 1),
        old_id
    );
    assert!(!store.is_btree_readable_at(&old_id, drop + 1, WalPos::STAGED)); // past its end
    assert!(store.is_btree_readable_at(&new_id, drop + 20, mark(3, 10))); // new owner reachable

    // Once lwm passes the drop, the retired old binding is reclaimed; the live owner remains.
    assert_eq!(store.gc_rootpage_entries(drop), 1);
    assert_eq!(store.get_table_id_from_root_page(root as i64), new_id);
}

#[test]
fn mvcc_try_get_table_id_stale_schema_read_returns_none() {
    // Regression: under PASSIVE checkpointing a reader can capture a schema cookie older than a
    // DROP that committed within its own snapshot (the drop publishes its cookie after the reader
    // read the header, even though the drop's commit ts precedes the reader's begin ts). The
    // compiled cursor then references a positive root page the reader's snapshot already sees
    // dropped. The fallible lookup must report this (None -> SchemaUpdated reprepare) instead of
    // the panicking variant firing. See `try_get_table_id_from_root_page_at`.
    use crate::mvcc::database::WalPos;
    let db = MvccTestDb::new();
    let store = &db.mvcc_store;

    let table_id = MVTableId::from(-26_i64);
    let root: u64 = 26;
    let begin = 34u64;
    let drop = 4007u64;

    store.record_rootpage_alloc(table_id, root, begin, WalPos::STAGED);
    store.publish_rootpage_visible(
        table_id,
        WalPos {
            checkpoint_seq: 2,
            frame: 952,
        },
    );

    // Live (not yet dropped): both variants resolve the owner.
    assert_eq!(
        store.try_get_table_id_from_root_page_at(root as i64, 4000),
        Some(table_id)
    );

    store.retire_rootpage(table_id, drop);

    // A snapshot predating the drop still resolves the owner.
    assert_eq!(
        store.try_get_table_id_from_root_page_at(root as i64, drop - 1),
        Some(table_id)
    );
    // A snapshot at/after the drop sees no covering binding -> None (would have panicked before).
    assert_eq!(
        store.try_get_table_id_from_root_page_at(root as i64, drop + 1),
        None
    );

    // Negative (uncheckpointed) roots always resolve to themselves regardless of snapshot.
    assert_eq!(
        store.try_get_table_id_from_root_page_at(-12, 0),
        Some(MVTableId::from(-12_i64))
    );
}

#[test]
fn mvcc_reset_after_vacuum_clears_checkpointed_empty_version_buckets() {
    let db = MvccTestDb::new();
    db.conn
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    db.conn.execute("CREATE INDEX idx_t_v ON t(v)").unwrap();

    db.conn
        .execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    db.conn
        .execute("UPDATE t SET v = 'z' WHERE id = 1")
        .unwrap();
    db.conn.execute("DELETE FROM t WHERE id = 2").unwrap();
    db.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Checkpoint-time GC removes the buckets it empties (it runs under the
    // blocking lock), so recreate the condition VACUUM reset must handle:
    // lazy background GC empties chains but leaves the buckets in the maps.
    db.conn.execute("BEGIN").unwrap();
    db.conn.execute("INSERT INTO t VALUES (4, 'd')").unwrap();
    db.conn.execute("ROLLBACK").unwrap();
    db.mvcc_store.drop_unused_row_versions();

    let empty_row_ids = db
        .mvcc_store
        .rows
        .iter()
        .filter(|entry| entry.value().read().is_empty())
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    let empty_index_ids = db
        .mvcc_store
        .index_rows
        .iter()
        .filter(|entry| {
            entry
                .value()
                .iter()
                .all(|row_entry| row_entry.value().read().is_empty())
        })
        .map(|entry| *entry.key())
        .collect::<Vec<_>>();
    assert!(
        !empty_row_ids.is_empty(),
        "lazy GC should leave empty table row buckets before VACUUM reset"
    );
    assert!(
        !empty_index_ids.is_empty(),
        "lazy GC should leave empty index buckets before VACUUM reset"
    );

    db.conn.demote_to_mvcc_connection();
    db.conn.reparse_schema().unwrap();
    let schema = db.conn.schema.read().clone();
    db.conn.promote_to_regular_connection();

    db.mvcc_store.try_begin_vacuum_gate().unwrap();
    db.mvcc_store
        .reset_after_vacuum(DatabaseHeader::default(), schema.as_ref());
    db.mvcc_store.release_vacuum_gate();

    for row_id in empty_row_ids {
        assert!(
            db.mvcc_store.rows.get(&row_id).is_none(),
            "empty table row buckets must be cleared across VACUUM reset"
        );
    }
    for index_id in empty_index_ids {
        assert!(
            db.mvcc_store.index_rows.get(&index_id).is_none(),
            "empty index buckets must be cleared across VACUUM reset"
        );
    }
}

impl MvccTestDbNoConn {
    pub fn new() -> Self {
        let io = Arc::new(MemoryIO::new());
        let opts = DatabaseOpts::new();
        let db = Database::open_file_with_flags(
            io,
            ":memory:",
            OpenFlags::default(),
            opts,
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        // Enable MVCC via PRAGMA
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
        Self {
            db: Some(db),
            path: None,
            opts,
            enc_opts: None,
            _temp_dir: None,
        }
    }

    /// Opens a database with a file
    pub fn new_with_random_db() -> Self {
        Self::new_with_random_db_with_opts(DatabaseOpts::new())
    }

    /// Opens a database with the experimental passive (non-blocking)
    /// auto-checkpoint enabled. Used by the passive-checkpoint-specific tests.
    pub fn new_with_random_db_passive() -> Self {
        Self::new_with_random_db_with_opts(
            DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(true),
        )
    }

    /// Opens a database with a file and the requested options.
    pub fn new_with_random_db_with_opts(opts: DatabaseOpts) -> Self {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join(format!("test_{}", rand::random::<u64>()));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let io = Arc::new(PlatformIO::new().unwrap());
        println!("path: {}", path.as_os_str().to_str().unwrap());
        let db = Database::open_file_with_flags(
            io,
            path.as_os_str().to_str().unwrap(),
            OpenFlags::default(),
            opts,
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        // Enable MVCC via PRAGMA
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
        Self {
            db: Some(db),
            path: Some(path.to_str().unwrap().to_string()),
            opts,
            enc_opts: None,
            _temp_dir: Some(temp_dir),
        }
    }

    /// Opens a file-backed encrypted database with the given hex key.
    pub fn new_encrypted(hex_key: &str) -> Self {
        let opts = DatabaseOpts::new().with_encryption(true);
        let enc_opts = crate::EncryptionOpts {
            cipher: "aes256gcm".to_string(),
            hexkey: hex_key.to_string(),
        };
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            path.as_os_str().to_str().unwrap(),
            OpenFlags::default(),
            opts,
            Some(enc_opts.clone()),
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let encryption_key = EncryptionKey::from_hex_string(hex_key).unwrap();
        let conn = db.connect_with_encryption(Some(encryption_key)).unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
        Self {
            db: Some(db),
            path: Some(path.to_str().unwrap().to_string()),
            opts,
            enc_opts: Some(enc_opts),
            _temp_dir: Some(temp_dir),
        }
    }

    /// Restarts the database, make sure there is no connection to the database open before calling this!
    pub fn restart(&mut self) {
        self.restart_result().unwrap();
    }

    /// Creates a file-backed MVCC test database, randomly picking a cipher
    /// when `encrypted` is true.
    pub fn new_maybe_encrypted(encrypted: bool) -> Self {
        if !encrypted {
            return Self::new_with_random_db();
        }
        const KEY128: &str = "b1bbfda4f589dc9daaf004fe21111e00";
        const KEY256: &str = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";
        let ciphers: &[(&str, &str)] = &[
            ("aes128gcm", KEY128),
            ("aes256gcm", KEY256),
            ("aegis128l", KEY128),
            ("aegis128x2", KEY128),
            ("aegis128x4", KEY128),
            ("aegis256", KEY256),
            ("aegis256x2", KEY256),
            ("aegis256x4", KEY256),
        ];
        let (cipher, hexkey) = ciphers[rand::random_range(0..ciphers.len())];
        Self::new_encrypted_with_cipher(hexkey, cipher)
    }

    fn new_encrypted_with_cipher(hex_key: &str, cipher: &str) -> Self {
        let opts = DatabaseOpts::new().with_encryption(true);
        let enc_opts = crate::EncryptionOpts {
            cipher: cipher.to_string(),
            hexkey: hex_key.to_string(),
        };
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            path.as_os_str().to_str().unwrap(),
            OpenFlags::default(),
            opts,
            Some(enc_opts.clone()),
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let encryption_key = EncryptionKey::from_hex_string(hex_key).unwrap();
        let conn = db.connect_with_encryption(Some(encryption_key)).unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
        Self {
            db: Some(db),
            path: Some(path.to_str().unwrap().to_string()),
            opts,
            enc_opts: Some(enc_opts),
            _temp_dir: Some(temp_dir),
        }
    }

    /// Like `restart`, but returns the error instead of panicking.
    /// Useful for testing wrong-key scenarios.
    pub fn restart_result(&mut self) -> crate::Result<()> {
        // First let's clear any entries in database manager in order to force restart.
        // If not, we will load the same database instance again.
        {
            let mut manager = DATABASE_MANAGER.lock();
            manager.clear();
        }
        // Now open again.
        let io = Arc::new(PlatformIO::new().unwrap());
        let path = self.path.as_ref().unwrap();
        let db = Database::open_file_with_flags(
            io,
            path,
            OpenFlags::default(),
            self.opts,
            self.enc_opts.clone(),
            Arc::new(SqliteDialect),
        )?;
        self.db.replace(db);
        Ok(())
    }

    /// Asumes there is a database open
    pub fn get_db(&self) -> Arc<Database> {
        self.db.as_ref().unwrap().clone()
    }

    pub fn connect(&self) -> Arc<Connection> {
        let enc_key = self
            .enc_opts
            .as_ref()
            .map(|e| EncryptionKey::from_hex_string(&e.hexkey).unwrap());
        self.get_db().connect_with_encryption(enc_key).unwrap()
    }

    pub fn get_mvcc_store(&self) -> Arc<crate::MvStore> {
        self.get_db().get_mv_store().clone().unwrap()
    }
}

pub(crate) fn generate_simple_string_row(table_id: MVTableId, id: i64, data: &str) -> Row {
    let record =
        ImmutableRecord::from_values(&[Value::Text(Text::new(data.to_string()))], 1).unwrap();
    Row::new_table_row(RowID::new(table_id, RowKey::Int(id)), record.as_blob(), 1).unwrap()
}

pub(crate) fn generate_simple_string_record(data: &str) -> ImmutableRecord {
    ImmutableRecord::from_values(&[Value::Text(Text::new(data.to_string()))], 1).unwrap()
}

fn advance_checkpoint_until_wal_has_commit_frame(
    mvcc_store: Arc<crate::MvStore>,
    conn: &Arc<Connection>,
) {
    let pager = conn.pager.load().clone();
    let initial_wal_max_frame = pager
        .wal
        .as_ref()
        .expect("mvcc mode requires wal")
        .get_max_frame_in_wal();
    let mut checkpoint_sm = CheckpointStateMachine::new(
        pager.clone(),
        mvcc_store,
        conn.clone(),
        true,
        conn.get_sync_mode(),
        crate::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    for _ in 0..10_000 {
        if pager
            .wal
            .as_ref()
            .expect("mvcc mode requires wal")
            .get_max_frame_in_wal()
            > initial_wal_max_frame
        {
            return;
        }

        match checkpoint_sm.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => {
                panic!("checkpoint finalized before WAL had committed frames")
            }
        }
    }

    panic!("checkpoint did not produce committed WAL frame in bounded steps");
}

fn overwrite_log_header_byte(path: &str, offset: u64, value: u8) {
    let log_path = std::path::Path::new(path).with_extension("db-log");
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(log_path)
        .unwrap();
    use std::io::{Seek, SeekFrom, Write};
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&[value]).unwrap();
    file.sync_all().unwrap();
}

fn overwrite_file_with_junk(path: &std::path::Path, size: usize, byte: u8) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap();
    let portable_changes = vec![byte; size];
    use std::io::Write;
    file.write_all(&portable_changes).unwrap();
    file.sync_all().unwrap();
}

fn wal_path_for_db(path: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{path}-wal"))
}

fn force_close_for_artifact_tamper(db: &mut MvccTestDbNoConn) {
    db.db.take();
    let mut manager = DATABASE_MANAGER.lock();
    manager.clear();
}

fn read_db_page_size(path: &str) -> usize {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::OpenOptions::new().read(true).open(path).unwrap();
    let mut header = [0u8; 100];
    file.seek(SeekFrom::Start(0)).unwrap();
    file.read_exact(&mut header).unwrap();
    let raw = u16::from_be_bytes([header[16], header[17]]);
    if raw == 1 {
        65536
    } else {
        raw as usize
    }
}

fn page_file_offset(page_no: u32, page_size: usize) -> u64 {
    (page_no as u64 - 1) * page_size as u64
}

fn page_header_offset(page_no: u32) -> usize {
    if page_no == 1 {
        100
    } else {
        0
    }
}

fn read_db_page(path: &str, page_no: u32, page_size: usize) -> Vec<u8> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::OpenOptions::new().read(true).open(path).unwrap();
    let mut page = vec![0u8; page_size];
    file.seek(SeekFrom::Start(page_file_offset(page_no, page_size)))
        .unwrap();
    file.read_exact(&mut page).unwrap();
    page
}

fn write_db_page(path: &str, page_no: u32, page_size: usize, page: &[u8]) {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(page_file_offset(page_no, page_size)))
        .unwrap();
    file.write_all(page).unwrap();
    file.sync_all().unwrap();
}

#[derive(Debug, Clone, Copy)]
struct TableLeafCellLoc {
    cell_offset: usize,
    payload_varint_len: usize,
    payload_len: usize,
    payload_offset: usize,
}

fn table_leaf_cell_locs(page: &[u8], page_no: u32) -> Vec<TableLeafCellLoc> {
    let hdr_off = page_header_offset(page_no);
    assert_eq!(page[hdr_off], 0x0D, "expected table-leaf page type");
    let cell_count = u16::from_be_bytes([page[hdr_off + 3], page[hdr_off + 4]]) as usize;
    let ptr_base = hdr_off + 8;
    let mut locs = Vec::with_capacity(cell_count);
    for i in 0..cell_count {
        let ptr_off = ptr_base + i * 2;
        let cell_ptr = u16::from_be_bytes([page[ptr_off], page[ptr_off + 1]]) as usize;
        let (payload_len_u64, payload_varint_len) = read_varint(&page[cell_ptr..]).unwrap();
        let payload_len = payload_len_u64 as usize;
        let (_, rowid_varint_len) = read_varint(&page[cell_ptr + payload_varint_len..]).unwrap();
        let payload_offset = cell_ptr + payload_varint_len + rowid_varint_len;
        locs.push(TableLeafCellLoc {
            cell_offset: cell_ptr,
            payload_varint_len,
            payload_len,
            payload_offset,
        });
    }
    locs
}

fn table_leaf_first_cell_loc(page: &[u8], page_no: u32) -> TableLeafCellLoc {
    let locs = table_leaf_cell_locs(page, page_no);
    let cell_count = locs.len();
    assert!(
        cell_count > 0,
        "expected at least one cell in metadata page"
    );
    locs[0]
}

fn rewrite_table_leaf_cell_payload(page: &mut [u8], loc: TableLeafCellLoc, new_payload: &[u8]) {
    assert!(
        new_payload.len() <= loc.payload_len,
        "new payload {} exceeds existing payload {}",
        new_payload.len(),
        loc.payload_len
    );
    let mut varint_buf = [0u8; 9];
    let n = write_varint(&mut varint_buf, new_payload.len() as u64);
    assert_eq!(
        n, loc.payload_varint_len,
        "payload varint length changed; in-place rewrite is unsafe"
    );
    page[loc.cell_offset..loc.cell_offset + n].copy_from_slice(&varint_buf[..n]);
    page[loc.payload_offset..loc.payload_offset + new_payload.len()].copy_from_slice(new_payload);
    if new_payload.len() < loc.payload_len {
        page[loc.payload_offset + new_payload.len()..loc.payload_offset + loc.payload_len].fill(0);
    }
}

fn tamper_table_leaf_value_serial_type(page: &mut [u8], page_no: u32, new_serial_type: u8) -> bool {
    let loc = table_leaf_first_cell_loc(page, page_no);
    let portable_changes = &mut page[loc.payload_offset..loc.payload_offset + loc.payload_len];

    let (header_size, hs_len) = read_varint(portable_changes).unwrap();
    let header_size = header_size as usize;
    if header_size < hs_len + 2 || header_size > portable_changes.len() {
        return false;
    }

    let mut idx = hs_len;
    let (_serial_type0, n0) = read_varint(&portable_changes[idx..header_size]).unwrap();
    idx += n0;
    if idx >= header_size {
        return false;
    }
    portable_changes[idx] = new_serial_type;
    true
}

fn wipe_table_leaf_cells(page: &mut [u8], page_no: u32) -> bool {
    let hdr_off = page_header_offset(page_no);
    if page.len() <= hdr_off + 8 || page[hdr_off] != 0x0D {
        return false;
    }
    let page_size = page.len();
    page[hdr_off + 3..hdr_off + 5].copy_from_slice(&0u16.to_be_bytes()); // number of cells
    page[hdr_off + 5..hdr_off + 7].copy_from_slice(&(page_size as u16).to_be_bytes()); // cell content area start
    page[hdr_off + 7] = 0; // fragmented free bytes
    true
}

fn metadata_root_page(conn: &Arc<Connection>) -> u32 {
    let rows = get_rows(
        conn,
        "SELECT rootpage FROM sqlite_schema
         WHERE type = 'table' AND name = '__turso_internal_mvcc_meta'",
    );
    assert_eq!(rows.len(), 1, "expected exactly one metadata table row");
    rows[0][0].as_int().unwrap() as u32
}

fn tamper_db_metadata_row_value(db_path: &str, metadata_root_page: u32, new_value: i64) {
    let page_size = read_db_page_size(db_path);
    let mut page = read_db_page(db_path, metadata_root_page, page_size);
    let loc = table_leaf_first_cell_loc(&page, metadata_root_page);
    let payload = &page[loc.payload_offset..loc.payload_offset + loc.payload_len];
    let record = ImmutableRecordRef::from_bin_record(payload);
    let key = record
        .get_value_opt(0)
        .expect("metadata key column missing");
    let ValueRef::Text(key) = key else {
        panic!("metadata key must be text");
    };
    let new_record = ImmutableRecord::from_values(
        &[
            Value::Text(Text::new(key.as_str().to_string())),
            Value::from_i64(new_value),
        ],
        2,
    )
    .unwrap();
    rewrite_table_leaf_cell_payload(&mut page, loc, new_record.as_blob());
    write_db_page(db_path, metadata_root_page, page_size, &page);
}

fn tamper_db_metadata_row_value_by_key(
    db_path: &str,
    metadata_root_page: u32,
    target_key: &str,
    new_value: i64,
) {
    let page_size = read_db_page_size(db_path);
    let mut page = read_db_page(db_path, metadata_root_page, page_size);
    let mut updated = false;
    for loc in table_leaf_cell_locs(&page, metadata_root_page) {
        let payload = &page[loc.payload_offset..loc.payload_offset + loc.payload_len];
        let record = ImmutableRecordRef::from_bin_record(payload);
        let key = record
            .get_value_opt(0)
            .expect("metadata key column missing");
        let ValueRef::Text(key) = key else {
            panic!("metadata key must be text");
        };
        if key.as_str() != target_key {
            continue;
        }
        let new_record = ImmutableRecord::from_values(
            &[
                Value::Text(Text::new(target_key.to_string())),
                Value::from_i64(new_value),
            ],
            2,
        )
        .unwrap();
        rewrite_table_leaf_cell_payload(&mut page, loc, new_record.as_blob());
        updated = true;
    }
    assert!(updated, "expected metadata key {target_key} to exist");
    write_db_page(db_path, metadata_root_page, page_size, &page);
}

fn tamper_db_metadata_value_serial_type(
    db_path: &str,
    metadata_root_page: u32,
    new_serial_type: u8,
) {
    let page_size = read_db_page_size(db_path);
    let mut page = read_db_page(db_path, metadata_root_page, page_size);
    assert!(
        tamper_table_leaf_value_serial_type(&mut page, metadata_root_page, new_serial_type),
        "expected metadata serial-type tamper to succeed"
    );
    write_db_page(db_path, metadata_root_page, page_size, &page);
}

fn tamper_db_metadata_row_key(db_path: &str, metadata_root_page: u32, new_key: &str) {
    let page_size = read_db_page_size(db_path);
    let mut page = read_db_page(db_path, metadata_root_page, page_size);
    let loc = table_leaf_first_cell_loc(&page, metadata_root_page);
    let payload = &page[loc.payload_offset..loc.payload_offset + loc.payload_len];
    let record = ImmutableRecordRef::from_bin_record(payload);
    let value = record
        .get_value_opt(1)
        .expect("metadata value column missing");
    let ValueRef::Numeric(Numeric::Integer(value)) = value else {
        panic!("metadata value must be integer");
    };
    let new_record = ImmutableRecord::from_values(
        &[
            Value::Text(Text::new(new_key.to_string())),
            Value::from_i64(value),
        ],
        2,
    )
    .unwrap();
    rewrite_table_leaf_cell_payload(&mut page, loc, new_record.as_blob());
    write_db_page(db_path, metadata_root_page, page_size, &page);
}

fn tamper_wal_metadata_value_serial_type(
    wal_path: &std::path::Path,
    metadata_root_page: u32,
    new_serial_type: u8,
) -> bool {
    use std::io::{Read, Seek, SeekFrom, Write};

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(wal_path)
        .unwrap();

    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    if bytes.len() < WAL_HEADER_SIZE {
        return false;
    }

    let header = WalHeader {
        magic: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
        file_format: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
        page_size: u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
        checkpoint_seq: u32::from_be_bytes(bytes[12..16].try_into().unwrap()),
        salt_1: u32::from_be_bytes(bytes[16..20].try_into().unwrap()),
        salt_2: u32::from_be_bytes(bytes[20..24].try_into().unwrap()),
        checksum_1: u32::from_be_bytes(bytes[24..28].try_into().unwrap()),
        checksum_2: u32::from_be_bytes(bytes[28..32].try_into().unwrap()),
    };
    let use_native_endian = cfg!(target_endian = "big") == ((header.magic & 1) != 0);
    let frame_size = WAL_FRAME_HEADER_SIZE + header.page_size as usize;
    let mut frame_offset = WAL_HEADER_SIZE;
    let mut prev_checksums = (header.checksum_1, header.checksum_2);
    let mut mutated = false;

    while frame_offset + frame_size <= bytes.len() {
        let frame = &mut bytes[frame_offset..frame_offset + frame_size];
        let page_no = u32::from_be_bytes(frame[0..4].try_into().unwrap());
        if page_no == metadata_root_page {
            let page_image = &mut frame
                [WAL_FRAME_HEADER_SIZE..WAL_FRAME_HEADER_SIZE + header.page_size as usize];
            let hdr_off = page_header_offset(metadata_root_page);
            if page_image.len() > hdr_off + 5 && page_image[hdr_off] == 0x0D {
                let cell_count =
                    u16::from_be_bytes([page_image[hdr_off + 3], page_image[hdr_off + 4]]);
                if cell_count > 0
                    && tamper_table_leaf_value_serial_type(
                        page_image,
                        metadata_root_page,
                        new_serial_type,
                    )
                {
                    mutated = true;
                }
            }
        }

        let header_checksum =
            checksum_wal(&frame[0..8], &header, prev_checksums, use_native_endian);
        let final_checksum = checksum_wal(
            &frame[WAL_FRAME_HEADER_SIZE..WAL_FRAME_HEADER_SIZE + header.page_size as usize],
            &header,
            header_checksum,
            use_native_endian,
        );
        frame[16..20].copy_from_slice(&final_checksum.0.to_be_bytes());
        frame[20..24].copy_from_slice(&final_checksum.1.to_be_bytes());
        prev_checksums = final_checksum;
        frame_offset += frame_size;
    }

    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
    mutated
}

fn tamper_wal_metadata_page_empty(wal_path: &std::path::Path, metadata_root_page: u32) -> bool {
    use std::io::{Read, Seek, SeekFrom, Write};

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(wal_path)
        .unwrap();

    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    if bytes.len() < WAL_HEADER_SIZE {
        return false;
    }

    let header = WalHeader {
        magic: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
        file_format: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
        page_size: u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
        checkpoint_seq: u32::from_be_bytes(bytes[12..16].try_into().unwrap()),
        salt_1: u32::from_be_bytes(bytes[16..20].try_into().unwrap()),
        salt_2: u32::from_be_bytes(bytes[20..24].try_into().unwrap()),
        checksum_1: u32::from_be_bytes(bytes[24..28].try_into().unwrap()),
        checksum_2: u32::from_be_bytes(bytes[28..32].try_into().unwrap()),
    };
    let use_native_endian = cfg!(target_endian = "big") == ((header.magic & 1) != 0);
    let frame_size = WAL_FRAME_HEADER_SIZE + header.page_size as usize;
    let mut frame_offset = WAL_HEADER_SIZE;
    let mut prev_checksums = (header.checksum_1, header.checksum_2);
    let mut mutated = false;

    while frame_offset + frame_size <= bytes.len() {
        let frame = &mut bytes[frame_offset..frame_offset + frame_size];
        let page_no = u32::from_be_bytes(frame[0..4].try_into().unwrap());
        if page_no == metadata_root_page {
            let page_image = &mut frame
                [WAL_FRAME_HEADER_SIZE..WAL_FRAME_HEADER_SIZE + header.page_size as usize];
            if wipe_table_leaf_cells(page_image, metadata_root_page) {
                mutated = true;
            }
        }

        let header_checksum =
            checksum_wal(&frame[0..8], &header, prev_checksums, use_native_endian);
        let final_checksum = checksum_wal(
            &frame[WAL_FRAME_HEADER_SIZE..WAL_FRAME_HEADER_SIZE + header.page_size as usize],
            &header,
            header_checksum,
            use_native_endian,
        );
        frame[16..20].copy_from_slice(&final_checksum.0.to_be_bytes());
        frame[20..24].copy_from_slice(&final_checksum.1.to_be_bytes());
        prev_checksums = final_checksum;
        frame_offset += frame_size;
    }

    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
    mutated
}

fn rewrite_wal_frames_as_non_commit(path: &std::path::Path) {
    use std::io::{Read, Seek, SeekFrom, Write};

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();

    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    assert!(bytes.len() >= WAL_HEADER_SIZE);

    let header = WalHeader {
        magic: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
        file_format: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
        page_size: u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
        checkpoint_seq: u32::from_be_bytes(bytes[12..16].try_into().unwrap()),
        salt_1: u32::from_be_bytes(bytes[16..20].try_into().unwrap()),
        salt_2: u32::from_be_bytes(bytes[20..24].try_into().unwrap()),
        checksum_1: u32::from_be_bytes(bytes[24..28].try_into().unwrap()),
        checksum_2: u32::from_be_bytes(bytes[28..32].try_into().unwrap()),
    };
    let use_native_endian = cfg!(target_endian = "big") == ((header.magic & 1) != 0);
    let frame_size = WAL_FRAME_HEADER_SIZE + header.page_size as usize;
    let mut frame_offset = WAL_HEADER_SIZE;
    let mut prev_checksums = (header.checksum_1, header.checksum_2);

    while frame_offset + frame_size <= bytes.len() {
        let frame = &mut bytes[frame_offset..frame_offset + frame_size];
        frame[4..8].copy_from_slice(&0u32.to_be_bytes());
        let header_checksum =
            checksum_wal(&frame[0..8], &header, prev_checksums, use_native_endian);
        let final_checksum = checksum_wal(
            &frame[WAL_FRAME_HEADER_SIZE..WAL_FRAME_HEADER_SIZE + header.page_size as usize],
            &header,
            header_checksum,
            use_native_endian,
        );
        frame[16..20].copy_from_slice(&final_checksum.0.to_be_bytes());
        frame[20..24].copy_from_slice(&final_checksum.1.to_be_bytes());
        prev_checksums = final_checksum;
        frame_offset += frame_size;
    }

    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_recovery_clock_monotonicity() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let max_commit_ts = {
        let conn = db.connect();
        conn.execute("CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)")
            .unwrap();
        conn.execute("INSERT INTO test(id, data) VALUES (1, 'foo')")
            .unwrap();
        let mvcc_store = db.get_mvcc_store();
        mvcc_store.last_committed_tx_ts.load(Ordering::SeqCst)
    };

    db.restart();
    let conn = db.connect();
    let pager = conn.pager.load().clone();
    let mvcc_store = db.get_mvcc_store();
    let tx_id = mvcc_store.begin_tx(pager).unwrap();
    let tx_entry = mvcc_store
        .txs
        .get(&tx_id)
        .expect("transaction should exist");
    let tx = tx_entry.value();
    assert!(
        tx.begin_ts > max_commit_ts,
        "expected begin_ts {} to be > max_commit_ts {}",
        tx.begin_ts,
        max_commit_ts
    );
}

/// What this test checks: Recovery stops cleanly at a torn/incomplete tail and keeps all previously validated frames.
/// Why this matters: Crashes can leave partial writes at EOF; we need durable-prefix recovery, not all-or-nothing failure.
#[test]
fn test_recover_logical_log_short_file_ignored() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    let file = mvcc_store.get_logical_log_file();

    let c = file.truncate(1, Completion::new_write(|_| {})).unwrap();
    conn.db.io.wait_for_completion(c).unwrap();

    let c = file
        .pwrite(
            0,
            Arc::new(Buffer::new(vec![0xAB])),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    conn.db.io.wait_for_completion(c).unwrap();
    assert_eq!(file.size().unwrap(), 1);

    use crate::util::IOExt as _;
    let io = conn.db.io.clone();
    let mut st = RecoverLogicalLogState::default();
    let recovered = io
        .block(|| mvcc_store.maybe_recover_logical_log(&conn, &mut st))
        .unwrap();
    assert!(!recovered);
}

/// What this test checks: Recovery replays a transaction frame in which data ops precede the
/// sqlite_schema op that registers their table, and the recovered table is queryable afterwards.
/// Why this matters: Logs written before the schema-first frame serialization fix (#7218) sorted
/// the whole write set, so a same-transaction CREATE TABLE + INSERT could serialize the data row
/// before the schema row. Recovery must stay tolerant to logs written by those older versions.
#[test]
fn test_recovery_replays_schema_op_after_data_op_in_frame() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();
    let io = db.get_db().io.clone();
    let file = mvcc_store.get_logical_log_file();

    let c = file.truncate(0, Completion::new_trunc(|_| {})).unwrap();
    io.wait_for_completion(c).unwrap();

    // A CREATE TABLE t2 + INSERT INTO t2 transaction as serialized by a pre-#7218 writer:
    // the data row comes before the sqlite_schema row that registers its table_id.
    let table_id = MVTableId::from(-999);
    let commit_ts = 1u64 << 40;
    let data_version = RowVersion {
        id: 1,
        begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::Timestamp(commit_ts))),
        end: crate::mvcc::database::PackedTs::pack(None),
        row: generate_simple_string_row(table_id, 1, "data"),
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };
    let schema_record = ImmutableRecord::from_values(
        &[
            Value::Text(Text::new("table".to_string())),
            Value::Text(Text::new("t2".to_string())),
            Value::Text(Text::new("t2".to_string())),
            Value::from_i64(i64::from(table_id)),
            Value::Text(Text::new("CREATE TABLE t2 (v TEXT)".to_string())),
        ],
        5,
    )
    .unwrap();
    let schema_version = RowVersion {
        id: 2,
        begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::Timestamp(commit_ts))),
        end: crate::mvcc::database::PackedTs::pack(None),
        row: Row::new_table_row(
            RowID::new(SQLITE_SCHEMA_MVCC_TABLE_ID, RowKey::Int(2)),
            schema_record.as_blob(),
            5,
        )
        .unwrap(),
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };
    let tx = LogRecord::for_test(commit_ts, &[data_version, schema_version], None);

    let mut log = LogicalLog::new(file.clone(), io.clone(), None);
    let c = log.log_tx(tx).unwrap();
    io.wait_for_completion(c).unwrap();
    drop(log);
    drop(file);
    drop(mvcc_store);

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT v FROM t2");
    assert_eq!(rows, vec![vec![Value::Text(Text::new("data".to_string()))]]);
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_journal_mode_switch_from_mvcc_to_wal_without_log_frames() {
    let db = MvccTestDb::new();
    let rows = get_rows(&db.conn, "PRAGMA journal_mode = 'wal'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_string().to_lowercase(), "wal");
}

#[test]
fn abandoned_journal_mode_checkpoint_releases_pager_transaction_and_lock() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'one')").unwrap();

    let mvcc_store = db.get_mvcc_store();
    let injector = FixedYieldInjector::new([CheckpointYieldPoint::BeforePagerCommit.point()]);
    conn.set_yield_injector(Some(injector.clone()));
    let mut stmt = conn.prepare("PRAGMA journal_mode = 'wal'").unwrap();
    for _ in 0..10_000 {
        match stmt.step().unwrap() {
            crate::StepResult::Yield if injector.is_empty() => break,
            crate::StepResult::IO => stmt.get_pager().io.step().unwrap(),
            crate::StepResult::Yield => {}
            other => {
                panic!("journal-mode checkpoint completed before pager-commit yield: {other:?}")
            }
        }
    }
    assert!(injector.is_empty(), "checkpoint did not reach pager commit");

    stmt.reset().unwrap();
    conn.set_yield_injector(None);

    let tx_state = conn.get_tx_state();
    let checkpoint_lock_released = mvcc_store.blocking_checkpoint_lock.write();
    if checkpoint_lock_released {
        mvcc_store.blocking_checkpoint_lock.unlock();
    }
    assert!(
        tx_state == crate::connection::TransactionState::None && checkpoint_lock_released,
        "reset left journal-mode checkpoint resources owned: \
         transaction_state={tx_state:?}, checkpoint_lock_released={checkpoint_lock_released}"
    );
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
}

#[turso_macros::test(encryption)]
fn test_recovery_checkpoint_then_more_writes() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
    }

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "b");
    assert_eq!(rows[2][0].as_int().unwrap(), 3);
    assert_eq!(rows[2][1].to_string(), "c");
}

/// This test checks that after MVCC restart, the auto-indexes for PRIMARY KEY and UNIQUE
/// constraints stay associated with the columns they were created for.
#[test]
fn test_restart_preserves_autoindex_to_column_mapping() {
    let mut db = MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new());
    {
        let conn = db.connect();
        // The dummy table exposes the bug because of the implementation of the HashMap used to
        // store schema rows. This test is not perfect, because it may not catch a regression if
        // the implementation changes. But until we patch the simulator to reproduce the bug,
        // this'll do.
        conn.execute("CREATE TABLE dummy(x)").unwrap();
        conn.execute("CREATE TABLE t(a TEXT PRIMARY KEY, b TEXT UNIQUE)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES('aa', 'bb')").unwrap();
        conn.close().unwrap();
    }

    db.restart();

    let conn = db.connect();
    let a_rows = get_rows(&conn, "SELECT a FROM t");
    assert_eq!(a_rows.len(), 1);
    assert_eq!(a_rows[0][0].to_string(), "aa");
    let b_rows = get_rows(&conn, "SELECT b FROM t");
    assert_eq!(b_rows.len(), 1);
    assert_eq!(b_rows[0][0].to_string(), "bb");
}

/// What this test checks: when transaction A updates a row and a concurrent
/// transaction B (later begin_ts) speculatively tombstones that row while A
/// is in `Preparing`, A's commit must serialize its OWN writes — not the
/// DELETEs that B's tombstone TxID, still pinned to the versions' `end`
/// fields, would imply.
///
/// References: Hekaton paper (https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf)
/// §2.5 Table 1 (speculative read of preparing writer), §2.7 (commit deps).
#[test]
fn test_concurrent_update_then_delete_serializes_correctly_across_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new());

    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
            .unwrap();
        conn.execute("INSERT INTO t(id, v) VALUES (1, 'initial')")
            .unwrap();
        conn.close().unwrap();
    }

    {
        let conn_a = db.connect();
        let conn_b = db.connect();

        conn_a.execute("BEGIN CONCURRENT").unwrap();
        conn_a
            .execute("UPDATE t SET v = 'a_value' WHERE id = 1")
            .unwrap();

        conn_a.set_yield_injector(Some(FixedYieldInjector::new([
            CommitYieldPoint::CommitValidation.point(),
        ])));

        let mut commit_stmt = conn_a.prepare("COMMIT").unwrap();
        let mut yielded = false;
        for _ in 0..100 {
            match commit_stmt.step().unwrap() {
                StepResult::Yield => {
                    yielded = true;
                    break;
                }
                StepResult::Done => break,
                _ => {}
            }
        }
        assert!(
            yielded,
            "tx_a's COMMIT should yield at CommitYieldPoint::CommitValidation"
        );

        // tx_b begins *after* tx_a's prepare so tx_b.begin_ts > tx_a's
        // prepared end_ts; its DELETE plants a speculative tombstone whose
        // TxID(tx_b) lands in the `end` field of tx_a's new versions.
        conn_b.execute("BEGIN CONCURRENT").unwrap();
        conn_b.execute("DELETE FROM t WHERE id = 1").unwrap();

        commit_stmt.run_collect_rows().unwrap();
        drop(commit_stmt);

        let rows = get_rows(&conn_a, "SELECT id, v FROM t");
        assert_eq!(rows.len(), 1);

        conn_b.execute("ROLLBACK").unwrap();

        conn_a.close().unwrap();
        conn_b.close().unwrap();
    }

    db.restart();

    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t");
    assert_eq!(
        rows.len(),
        1,
        "tx_a's committed row must survive recovery, got {rows:?}"
    );
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a_value");
}

/// What this test checks: MVCC restart handles sqlite_schema rows with rootpage=0 (triggers).
/// Why this matters: Trigger definitions are stored without btrees and should not break recovery.
#[test]
fn test_restart_with_trigger_rootpage_zero() {
    let mut db = MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new());
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, a TEXT)")
            .unwrap();
        conn.execute("CREATE TABLE audit(id INTEGER PRIMARY KEY, action TEXT)")
            .unwrap();
        conn.execute(
            "CREATE TRIGGER trg_del AFTER DELETE ON t1 \
             BEGIN INSERT INTO audit VALUES (NULL, 'deleted'); END;",
        )
        .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'x')").unwrap();
        conn.close().unwrap();
    }

    db.restart();

    {
        let conn = db.connect();
        conn.execute("DELETE FROM t1 WHERE id = 1").unwrap();
        let rows = get_rows(&conn, "SELECT action FROM audit ORDER BY id");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].to_string(), "deleted");
    }
}

#[turso_macros::test(encryption)]
fn test_btree_resident_recovery_then_checkpoint_delete_stays_deleted() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'keep')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'gone')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    // Delete a B-tree resident row and crash/restart before checkpoint.
    {
        let conn = db.connect();
        conn.execute("DELETE FROM t WHERE id = 2").unwrap();
    }

    db.restart();
    {
        let conn = db.connect();
        // Recovery tombstone must hide stale B-tree row before checkpoint.
        let rows = get_rows(&conn, "SELECT id FROM t ORDER BY id");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_int().unwrap(), 1);

        // After checkpoint + GC, row must stay deleted (B-tree delete persisted).
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        let rows = get_rows(&conn, "SELECT id FROM t ORDER BY id");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_int().unwrap(), 1);

        let rows = get_rows(&conn, "PRAGMA integrity_check");
        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][0].to_string(), "ok");
    }
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_recovery_overwrites_torn_tail_on_next_append() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    }

    // Corrupt only the tail of the latest frame.
    {
        let conn = db.connect();
        let mvcc_store = db.get_mvcc_store();
        let file = mvcc_store.get_logical_log_file();
        let size = file.size().unwrap();
        assert!(size > 1);
        let c = file
            .truncate(size - 1, Completion::new_trunc(|_| {}))
            .unwrap();
        conn.db.io.wait_for_completion(c).unwrap();
    }

    // First restart: recovery should stop at torn tail and reset log write offset.
    db.restart();
    {
        let conn = db.connect();
        let rows = get_rows(&conn, "SELECT id FROM t ORDER BY id");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_int().unwrap(), 1);
        conn.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
    }

    // Second restart: row 3 must be recoverable, proving it was appended at last_valid_offset.
    db.restart();
    {
        let conn = db.connect();
        let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].as_int().unwrap(), 1);
        assert_eq!(rows[0][1].to_string(), "a");
        assert_eq!(rows[1][0].as_int().unwrap(), 3);
        assert_eq!(rows[1][1].to_string(), "c");
    }
}

/// First-time MVCC bootstrap repairs a torn short `.db-log` header before metadata writes commit.
#[test]
#[ignore = "Needs a dedicated bootstrap harness that can create header=MVCC + missing metadata + torn short log atomically"]
fn test_bootstrap_repairs_torn_short_log_before_metadata_init() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let db_path = temp_dir
        .path()
        .join(format!("bootstrap_torn_{}", rand::random::<u64>()));
    let db_path_str = db_path.to_str().unwrap().to_string();

    {
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io, &db_path_str, Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.close().unwrap();
    }

    let log_path = std::path::Path::new(&db_path_str).with_extension("db-log");
    overwrite_file_with_junk(&log_path, LOG_HDR_SIZE / 2, 0xAB);

    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }
    {
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io, &db_path_str, Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
    }

    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, &db_path_str, Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    let meta = get_rows(
        &conn,
        "SELECT v FROM __turso_internal_mvcc_meta WHERE k = 'persistent_tx_ts_max'",
    );
    assert_eq!(meta.len(), 1);
    assert_eq!(meta[0][0].as_int().unwrap(), 0);

    let log_len = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);
    assert!(
        log_len >= LOG_HDR_SIZE as u64,
        "expected bootstrap to rewrite durable logical-log header"
    );
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_bootstrap_completes_interrupted_checkpoint_with_committed_wal() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store, &conn);

        let pager = conn.pager.load().clone();
        assert!(
            pager
                .wal
                .as_ref()
                .expect("wal must exist")
                .get_max_frame_in_wal()
                > 0
        );
        let log_file = db.get_mvcc_store().get_logical_log_file();
        assert!(log_file.size().unwrap() > LOG_HDR_SIZE as u64);
    }

    db.restart();

    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "b");

    let log_size = db.get_mvcc_store().get_logical_log_file().size().unwrap();
    assert!(
        log_size >= LOG_HDR_SIZE as u64,
        "logical log must be at least {LOG_HDR_SIZE} bytes after interrupted-checkpoint reconciliation"
    );
    let wal_path = wal_path_for_db(&db_path);
    let wal_len = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
    assert_eq!(wal_len, 0);
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_checkpoint_truncates_wal_last() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let wal_path = wal_path_for_db(&db_path);
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();

    let mvcc_store = db.get_mvcc_store();
    let pager = conn.pager.load().clone();
    let mut checkpoint_sm = CheckpointStateMachine::new(
        pager.clone(),
        mvcc_store.clone(),
        conn.clone(),
        true,
        conn.get_sync_mode(),
        crate::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    let mut saw_truncate_log_state_with_wal = false;
    let mut finished = false;
    for _ in 0..50_000 {
        let state = checkpoint_sm.state_for_test();

        if state == CheckpointState::TruncateLogicalLog {
            let wal_len = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
            assert!(wal_len > 0, "WAL must still exist before log truncation");
            saw_truncate_log_state_with_wal = true;
        }

        if state == CheckpointState::TruncateWal {
            assert!(
                saw_truncate_log_state_with_wal,
                "must truncate logical log before truncating WAL"
            );
            assert_eq!(
                mvcc_store.get_logical_log_file().size().unwrap(),
                0,
                "logical log should be truncated to 0"
            );
        }

        match checkpoint_sm.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => {
                finished = true;
                break;
            }
        }
    }

    assert!(finished, "checkpoint state machine did not finish");
    assert!(saw_truncate_log_state_with_wal);

    let final_wal_len = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
    assert_eq!(final_wal_len, 0);
    assert_eq!(
        mvcc_store.get_logical_log_file().size().unwrap(),
        0,
        "logical log should be truncated to 0 after checkpoint"
    );
}

/// Truncate checkpoint must collect commits that land while waiting for `AcquireLock`, and zero the logical log.
#[test]
fn test_blocking_truncate_zeros_log_when_commit_races_acquire_lock() {
    use crate::StepResult;

    let _ = tracing_subscriber::fmt::try_init();
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t1 VALUES (0, 'seed')").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
        .unwrap();
    conn.execute("INSERT INTO t1 VALUES (1, 'pending')")
        .unwrap();

    let mvcc_store = db.get_mvcc_store();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();

    let injector = FixedYieldInjector::new([CheckpointYieldPoint::BeforeAcquireLock.point()]);
    conn.set_yield_injector(Some(injector.clone()));
    let mut insert_stmt = conn
        .prepare("INSERT INTO t1 VALUES (2, 'trigger')")
        .unwrap();
    let mut parked = false;
    for _ in 0..10_000 {
        match insert_stmt.step().unwrap() {
            StepResult::IO | StepResult::Yield if injector.is_empty() => {
                parked = true;
                break;
            }
            StepResult::IO | StepResult::Yield => {}
            StepResult::Done => {
                panic!("INSERT completed before the checkpoint acquire-lock yield fired")
            }
            other => panic!("unexpected INSERT step result before yield: {other:?}"),
        }
    }
    assert!(
        parked,
        "blocking TRUNCATE auto-checkpoint should yield before acquiring the lock"
    );

    let sibling = db.connect();
    sibling
        .execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
        .unwrap();
    sibling
        .execute("CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    sibling.execute("INSERT INTO t2 VALUES (1, 'x')").unwrap();

    let mut finished = false;
    for _ in 0..100_000 {
        match insert_stmt.step().unwrap() {
            StepResult::Done => {
                finished = true;
                break;
            }
            StepResult::IO | StepResult::Yield => {}
            other => panic!("unexpected resume step result: {other:?}"),
        }
    }
    conn.set_yield_injector(None);
    assert!(
        finished,
        "TRUNCATE checkpoint must complete after sibling commit"
    );
    drop(insert_stmt);

    assert_eq!(
        mvcc_store.get_logical_log_file().size().unwrap(),
        0,
        "blocking TRUNCATE must zero the logical log even when a sibling commit raced acquire-lock"
    );
    let wal_path = wal_path_for_db(db.path.as_ref().unwrap());
    assert_eq!(
        wal_path.metadata().map(|m| m.len()).unwrap_or(0),
        0,
        "blocking TRUNCATE must zero the WAL"
    );

    let tables = get_rows(
        &conn,
        "SELECT name FROM sqlite_schema WHERE type='table' AND name IN ('t1','t2') ORDER BY name",
    );
    assert_eq!(tables.len(), 2, "both tables must exist: {tables:?}");
    let t2_rows = get_rows(&conn, "SELECT id, v FROM t2");
    assert_eq!(t2_rows.len(), 1);
    assert_eq!(t2_rows[0][0].as_int().unwrap(), 1);
    assert_eq!(t2_rows[0][1].to_string(), "x");

    assert_integrity_ok(&conn);

    db.restart();
    let conn = db.connect();
    assert_integrity_ok(&conn);
    let t2_after_reopen = get_rows(&conn, "SELECT id, v FROM t2");
    assert_eq!(t2_after_reopen.len(), 1);
    assert_eq!(t2_after_reopen[0][1].to_string(), "x");
    assert_eq!(
        db.get_mvcc_store().get_logical_log_file().size().unwrap(),
        0,
        "logical log must stay empty after reopen following TRUNCATE checkpoint"
    );
}

/// What this test checks: Checkpoint accepts sqlite_schema index-row updates for already-checkpointed indexes
/// (e.g. column rename), without requiring create/destroy special writes.
/// Why this matters: RENAME COLUMN on indexed tables rewrites sqlite_schema index SQL text while preserving rootpage.
/// Treating that as an impossible state crashes checkpoint.
#[test]
fn test_checkpoint_allows_index_schema_update_after_rename_column() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(a INTEGER, b INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX idx_t_a ON t(a)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 2)").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Rewrites sqlite_schema entry for the existing index while keeping positive rootpage.
    conn.execute("ALTER TABLE t RENAME COLUMN a TO c").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "SELECT c, b FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 2);
}

#[test]
fn test_bootstrap_recovers_committed_wal_without_log_file() {
    // A Passive checkpoint truncates the logical log to 0 but leaves the WAL non-empty,
    // so reopen sees NoLog + committed WAL — the normal steady state, not corruption.
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let db_path = db.path.as_ref().unwrap().clone();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store, &conn);
    }

    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }

    let log_path = std::path::Path::new(&db_path).with_extension("db-log");
    std::fs::remove_file(&log_path).unwrap();

    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        &db_path,
        OpenFlags::default(),
        DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(true),
        None,
        Arc::new(SqliteDialect),
    )
    .expect("open should recover, not fail closed");
    let conn = db
        .connect()
        .expect("connect should recover the committed WAL");
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1, "committed row must survive recovery");
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "x");
}

/// MVCC supports only Passive and Truncate, so a requested FULL checkpoint maps to
/// Truncate (resets the WAL). The reopen then recovers cleanly. Regression for whopper
/// `--enable-mvcc` (no passive flag) hitting "WAL has committed frames but logical log
/// header is missing" back when FULL kept the WAL while truncating the logical log.
#[test]
fn test_full_checkpoint_reopen_recovers_truncate_mode() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(FULL)").unwrap();
    }

    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }

    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, &db_path, Arc::new(SqliteDialect))
        .expect("FULL checkpoint reopen should recover");
    let conn = db
        .connect()
        .expect("connect should recover after FULL checkpoint");
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(
        rows.len(),
        1,
        "committed row must survive FULL-checkpoint reopen"
    );
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "x");
}

/// In default (flag-off) mode an explicit TRUNCATE that loses the blocking-checkpoint lock to a
/// concurrent reader/writer must report `Busy` (the pre-feature contract), never a false-success
/// no-op. An open transaction holds the checkpoint lock for its lifetime.
#[test]
fn test_flag_off_truncate_busy_when_lock_contended() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let c1 = db.connect();
    c1.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        .unwrap();
    // Open transaction on c1 pins the blocking checkpoint lock.
    c1.execute("BEGIN").unwrap();
    c1.execute("INSERT INTO t VALUES (1)").unwrap();

    let c2 = db.connect();
    let res = c2.execute("PRAGMA wal_checkpoint(TRUNCATE)");
    assert!(
        matches!(res, Err(LimboError::Busy)),
        "contended flag-off TRUNCATE must return Busy, got {res:?}"
    );

    c1.execute("COMMIT").unwrap();
}

/// `checkpoint_snapshot_ts` must clamp the published checkpoint boundary below any
/// in-flight (Preparing) commit. `last_committed_tx_ts` is a fetch_max high-water mark,
/// so a commit that assigned a LOWER end_ts and is still Preparing (commits finalize out
/// of timestamp order) sits below it; a checkpoint that published a boundary above that
/// end_ts and skipped the commit (not yet Committed) would lose it on reopen. Regression
/// for the boundary-straddle data-loss path (review finding #1).
#[test]
fn test_checkpoint_snapshot_ts_clamps_below_inflight_preparing() {
    let db = MvccTestDb::new();
    let store = &db.mvcc_store;
    let pager = db.conn.pager.load().clone();
    let inflight = store.begin_tx(pager).unwrap();

    // Out-of-order finalize: a higher-ts commit advanced the watermark to 1000 while
    // `inflight` is still Preparing at the lower end_ts 500.
    store.last_committed_tx_ts.store(1000, Ordering::SeqCst);
    store
        .txs
        .get(&inflight)
        .unwrap()
        .value()
        .state
        .store(TransactionState::Preparing(500));
    assert_eq!(
        store.checkpoint_snapshot_ts(),
        499,
        "boundary must clamp below in-flight Preparing(500), not reach last_committed=1000"
    );

    // Once it commits, the clamp lifts back to the watermark.
    store
        .txs
        .get(&inflight)
        .unwrap()
        .value()
        .state
        .store(TransactionState::Committed(500));
    assert_eq!(store.checkpoint_snapshot_ts(), 1000);
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_bootstrap_rejects_torn_log_header_with_committed_wal() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let wal_path = wal_path_for_db(&db_path);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'y')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store, &conn);
    }

    overwrite_log_header_byte(&db_path, 0, 0x00);

    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }

    let io = Arc::new(PlatformIO::new().unwrap());
    match Database::open_file(io, &db_path, Arc::new(SqliteDialect)) {
        Ok(db) => match db.connect() {
            Ok(_) => panic!("expected connect to fail with Corrupt"),
            Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
        },
        Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
    }
    let wal_len = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
    assert!(
        wal_len > 0,
        "failed bootstrap must not truncate WAL before header validation"
    );
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_bootstrap_rejects_corrupt_log_header_without_wal() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
    }

    overwrite_log_header_byte(&db_path, 0, 0x00);

    {
        let wal_path = wal_path_for_db(&db_path);
        let _ = std::fs::remove_file(&wal_path);
        overwrite_file_with_junk(&wal_path, 0, 0x00);
    }

    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }

    let io = Arc::new(PlatformIO::new().unwrap());
    match Database::open_file(io, &db_path, Arc::new(SqliteDialect)) {
        Ok(db) => match db.connect() {
            Ok(_) => panic!("expected connect to fail with Corrupt"),
            Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
        },
        Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
    }
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_bootstrap_handles_committed_wal_when_log_truncated() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store.clone(), &conn);

        let log_file = mvcc_store.get_logical_log_file();
        let c = log_file
            .truncate(LOG_HDR_SIZE as u64, Completion::new_trunc(|_| {}))
            .unwrap();
        conn.db.io.wait_for_completion(c).unwrap();
    }

    db.restart();

    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "b");

    let log_size = db.get_mvcc_store().get_logical_log_file().size().unwrap();
    assert_eq!(log_size, LOG_HDR_SIZE as u64);
    let wal_path = wal_path_for_db(&db_path);
    let wal_len = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
    assert_eq!(wal_len, 0);
}

/// What this test checks: WAL frames without a commit marker are treated as non-committed tail and ignored.
/// Why this matters: Recovery must preserve availability by discarding invalid WAL tail bytes instead of failing startup.
#[test]
fn test_bootstrap_ignores_wal_frames_without_commit_marker() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let wal_path = wal_path_for_db(&db_path);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store, &conn);
    }

    rewrite_wal_frames_as_non_commit(&wal_path);
    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }
    let io = Arc::new(PlatformIO::new().unwrap());
    let db2 =
        Database::open_file(io, &db_path, Arc::new(SqliteDialect)).expect("open should succeed");
    let conn2 = db2.connect().expect("connect should succeed");
    let rows = get_rows(&conn2, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "x");
}

/// What this test checks: Recovery after checkpoint (empty log) seeds new tx timestamps above durable metadata boundary.
/// Why this matters: Timestamp rewind below checkpointed boundary would break MVCC ordering.
#[test]
fn test_empty_log_recovery_loads_checkpoint_watermark() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let persistent_tx_ts_max = {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        assert_eq!(
            mvcc_store.get_logical_log_file().size().unwrap(),
            0,
            "logical log should be truncated to 0 after checkpoint"
        );
        let meta = get_rows(
            &conn,
            "SELECT v FROM __turso_internal_mvcc_meta WHERE k = 'persistent_tx_ts_max'",
        );
        assert_eq!(meta.len(), 1);
        meta[0][0].as_int().unwrap() as u64
    };

    db.restart();
    let conn = db.connect();
    let pager = conn.pager.load().clone();
    let mvcc_store = db.get_mvcc_store();
    let tx_id = mvcc_store.begin_tx(pager).unwrap();
    let tx_entry = mvcc_store
        .txs
        .get(&tx_id)
        .expect("transaction should exist");
    assert!(
        tx_entry.value().begin_ts > persistent_tx_ts_max,
        "expected begin_ts {} > persistent_tx_ts_max {}",
        tx_entry.value().begin_ts,
        persistent_tx_ts_max
    );
}

/// TDD recovery/checkpoint matrix for metadata-table source of truth.
///
/// Proposed semantics under test:
/// - Source of truth for replay boundary is internal SQLite table
///   `turso_internal_mvcc_meta` with `persistent_tx_ts_max`.
/// - Logical-log header carries no replay timestamps.
/// - On startup with committed WAL frames, recovery must reconcile WAL first, then read metadata.
///
/// Enumerated cases:
/// 1. No committed WAL + no logical-log frames + metadata row present.
/// 2. No committed WAL + logical-log frames + metadata row present -> replay `ts > persistent_tx_ts_max`.
/// 3. No committed WAL + logical-log frames + metadata row missing/corrupt -> fail closed.
/// 4. Committed WAL + metadata row present -> reconcile WAL first, then replay above metadata boundary.
/// 5. Committed WAL + metadata row missing -> fail closed.
/// 6. Committed WAL + metadata row malformed/corrupt -> fail closed.
/// 7. Metadata table exists but has duplicate rows/invalid key shape -> fail closed.
/// 8. User tampered metadata row downward -> detect and fail closed.
/// 9. User deleted metadata row -> detect and fail closed.
/// 10. Checkpoint pager commit atomically upserts metadata row in same WAL txn.
/// 11. Auto-checkpoint failure after pager commit keeps COMMIT result stable and recoverable.
/// 12. Replay gate correctness: never apply `commit_ts <= persistent_tx_ts_max`.
#[test]
fn test_meta_recovery_case_1_no_wal_no_log_metadata_present_clean_boot() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let wal_path = wal_path_for_db(&db_path);
    let log_path = std::path::Path::new(&db_path).with_extension("db-log");

    {
        let conn = db.connect();
        let rows = get_rows(
            &conn,
            "SELECT k, v FROM __turso_internal_mvcc_meta ORDER BY rowid",
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].to_string(), "persistent_tx_ts_max");
        assert_eq!(rows[0][1].as_int().unwrap(), 0);
    }

    db.restart();
    let conn = db.connect();
    let rows = get_rows(
        &conn,
        "SELECT k, v FROM __turso_internal_mvcc_meta ORDER BY rowid",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_string(), "persistent_tx_ts_max");
    assert_eq!(rows[0][1].as_int().unwrap(), 0);

    let wal_len = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
    assert_eq!(
        wal_len, 0,
        "expected no committed WAL tail after clean boot"
    );
    let log_len = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);
    assert_eq!(
        log_len, LOG_HDR_SIZE as u64,
        "expected logical log to be {LOG_HDR_SIZE} bytes (bootstrap header) on clean boot"
    );
}

/// With no committed WAL and metadata present, replay includes only frames above `persistent_tx_ts_max`.
#[turso_macros::test(encryption)]
fn test_meta_recovery_case_2_no_wal_replay_above_metadata_boundary() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        let meta = get_rows(
            &conn,
            "SELECT v FROM __turso_internal_mvcc_meta WHERE k = 'persistent_tx_ts_max'",
        );
        assert_eq!(meta.len(), 1);
        let boundary = meta[0][0].as_int().unwrap();
        assert!(
            boundary >= 2,
            "expected metadata boundary >= 2 after checkpoint, got {boundary}"
        );

        conn.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
    }

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "b");
    assert_eq!(rows[2][0].as_int().unwrap(), 3);
    assert_eq!(rows[2][1].to_string(), "c");
}

/// What this test checks: Header-only commits are durably replayed from the logical log and
/// then persisted into the database header by checkpoint.
/// Why this matters: PRAGMA header mutations (for example user_version) must survive restart
/// both before and after log truncation, including implicit autocommit statement transactions.
#[turso_macros::test(encryption)]
fn test_header_only_mutation_is_replayed_and_checkpointed() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);

    {
        let conn = db.connect();
        conn.execute("PRAGMA user_version = 42").unwrap();
        let rows = get_rows(&conn, "PRAGMA user_version");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_int().unwrap(), 42);
    }

    db.restart();
    {
        let conn = db.connect();
        let rows = get_rows(&conn, "PRAGMA user_version");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0].as_int().unwrap(),
            42,
            "header mutation should recover from logical log before checkpoint",
        );
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    db.restart();
    {
        let conn = db.connect();
        let rows = get_rows(&conn, "PRAGMA user_version");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0].as_int().unwrap(),
            42,
            "header mutation should persist in DB header after checkpoint truncates logical log",
        );
    }
}

/// What this test checks: Header PRAGMAs in MVCC require an exclusive transaction and reject
/// BEGIN CONCURRENT writes.
/// Why this matters: Header updates have no row-level conflict keys, so they must not run under
/// optimistic concurrent write mode.
#[test]
fn test_mvcc_header_updates_require_exclusive_transaction() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("BEGIN CONCURRENT").unwrap();
    let err = conn.execute("PRAGMA user_version = 42").unwrap_err();
    assert!(
        err.to_string().contains("exclusive transaction"),
        "expected exclusive-transaction error, got: {err:?}"
    );
    conn.execute("ROLLBACK").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("PRAGMA user_version = 7").unwrap();
    conn.execute("COMMIT").unwrap();

    let rows = get_rows(&conn, "PRAGMA user_version");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 7);
}

/// What this test checks: Header PRAGMAs in MVCC succeed in autocommit mode, where the VM
/// opens an implicit single-statement write transaction.
/// Why this matters: The exclusive-transaction gate must block BEGIN CONCURRENT, but not reject
/// valid autocommit writes that are internally upgraded to exclusive write mode.
#[test]
fn test_mvcc_header_updates_allow_autocommit_statement_tx() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("PRAGMA user_version = 19").unwrap();

    let rows = get_rows(&conn, "PRAGMA user_version");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 19);
}

/// Missing/corrupt metadata with logical-log frames and no WAL causes fail-closed startup.
#[test]
#[cfg_attr(
    feature = "checksum",
    ignore = "byte-level tamper caught by checksum layer"
)]
fn test_meta_recovery_case_3_no_wal_log_frames_without_valid_metadata_fails_closed() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let metadata_root_page = {
        let conn = db.connect();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        metadata_root_page(&conn)
    };
    force_close_for_artifact_tamper(&mut db);
    tamper_db_metadata_row_value(&db_path, metadata_root_page, -1);
    let wal_path = wal_path_for_db(&db_path);
    let _ = std::fs::remove_file(&wal_path);
    overwrite_file_with_junk(&wal_path, 0, 0);

    {
        // Ensure cold open after artifact tamper.
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }
    let io = Arc::new(PlatformIO::new().unwrap());
    match Database::open_file(io, &db_path, Arc::new(SqliteDialect)) {
        Ok(db2) => match db2.connect() {
            Ok(_) => panic!("expected connect to fail with Corrupt"),
            Err(err) => assert!(
                matches!(err, LimboError::Corrupt(_)),
                "unexpected connect error: {err:?}"
            ),
        },
        Err(err) => assert!(
            matches!(err, LimboError::Corrupt(_)),
            "unexpected open error: {err:?}"
        ),
    }
}

/// What this test checks: Recovery reconciles committed WAL first, then applies logical-log replay boundary from metadata.
/// Why this matters: Ordering prevents double-apply and loss when WAL and logical log both exist.
#[test]
fn test_meta_recovery_case_4_committed_wal_reconcile_before_metadata_boundary_replay() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let wal_path = wal_path_for_db(&db_path);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store, &conn);
    }

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[1][0].as_int().unwrap(), 2);

    let meta = get_rows(
        &conn,
        "SELECT v FROM __turso_internal_mvcc_meta WHERE k = 'persistent_tx_ts_max'",
    );
    assert_eq!(meta.len(), 1);
    assert!(
        meta[0][0].as_int().unwrap() >= 2,
        "expected replay boundary to advance after committed-WAL reconciliation",
    );

    let wal_len = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
    assert_eq!(wal_len, 0, "reconciliation must truncate WAL at the end");
}

/// Committed WAL with missing metadata row fails closed.
#[test]
#[cfg_attr(
    feature = "checksum",
    ignore = "byte-level tamper caught by checksum layer"
)]
fn test_meta_recovery_case_5_committed_wal_missing_metadata_fails_closed() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let wal_path = wal_path_for_db(&db_path);
    let metadata_root_page = {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        let root_page = metadata_root_page(&conn);
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store, &conn);
        root_page
    };
    force_close_for_artifact_tamper(&mut db);
    let mutated = tamper_wal_metadata_page_empty(&wal_path, metadata_root_page);
    assert!(
        mutated,
        "expected metadata WAL frame to be mutated into missing-row shape"
    );

    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }
    let io = Arc::new(PlatformIO::new().unwrap());
    match Database::open_file(io, &db_path, Arc::new(SqliteDialect)) {
        Ok(db2) => match db2.connect() {
            Ok(_) => panic!("expected connect to fail closed"),
            Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
        },
        Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
    }
}

/// What this test checks: Committed WAL with malformed metadata row fails closed.
/// Why this matters: Corrupt internal metadata must never be interpreted best-effort.
#[test]
fn test_meta_recovery_case_6_committed_wal_corrupt_metadata_fails_closed() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let wal_path = wal_path_for_db(&db_path);
    let metadata_root_page = {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        let root_page = metadata_root_page(&conn);
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store, &conn);
        root_page
    };
    force_close_for_artifact_tamper(&mut db);
    let mutated = tamper_wal_metadata_value_serial_type(&wal_path, metadata_root_page, 0);
    assert!(
        mutated,
        "expected at least one metadata WAL frame to be mutated"
    );

    {
        // Ensure cold open after artifact tamper.
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }
    let io = Arc::new(PlatformIO::new().unwrap());
    if Database::open_file(io, &db_path, Arc::new(SqliteDialect))
        .is_ok_and(|db2| db2.connect().is_ok())
    {
        panic!("expected connect to fail closed")
    }
}

/// What this test checks: Invalid metadata-table shape (duplicates or bad key domain) fails closed.
/// Why this matters: Schema/shape corruption in internal state must not be silently tolerated.
#[test]
fn test_meta_recovery_case_7_metadata_table_shape_violation_fails_closed() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let metadata_root_page = {
        let conn = db.connect();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        metadata_root_page(&conn)
    };
    force_close_for_artifact_tamper(&mut db);
    tamper_db_metadata_value_serial_type(&db_path, metadata_root_page, 0);
    let wal_path = wal_path_for_db(&db_path);
    let _ = std::fs::remove_file(&wal_path);
    overwrite_file_with_junk(&wal_path, 0, 0);

    {
        // Ensure cold open after artifact tamper.
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }
    let io = Arc::new(PlatformIO::new().unwrap());
    if Database::open_file(io, &db_path, Arc::new(SqliteDialect))
        .is_ok_and(|db2| db2.connect().is_ok())
    {
        panic!("expected connect to fail closed")
    }
}

/// Deletion of metadata row is detected and rejected.
#[test]
#[cfg_attr(
    feature = "checksum",
    ignore = "byte-level tamper caught by checksum layer"
)]
fn test_meta_recovery_case_9_metadata_row_deleted_fails_closed() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let metadata_root_page = {
        let conn = db.connect();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        metadata_root_page(&conn)
    };
    force_close_for_artifact_tamper(&mut db);
    tamper_db_metadata_row_key(&db_path, metadata_root_page, "persistent_tx_ts_may");
    let wal_path = wal_path_for_db(&db_path);
    let _ = std::fs::remove_file(&wal_path);
    overwrite_file_with_junk(&wal_path, 0, 0);

    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }
    let io = Arc::new(PlatformIO::new().unwrap());
    match Database::open_file(io, &db_path, Arc::new(SqliteDialect)) {
        Ok(db2) => match db2.connect() {
            Ok(_) => panic!("expected connect to fail closed"),
            Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
        },
        Err(err) => assert!(matches!(err, LimboError::Corrupt(_))),
    }
}

/// What this test checks: Checkpoint pager commit writes data pages and metadata row in the same WAL transaction.
/// Why this matters: This is the atomicity mechanism replacing logical-log header checkpoint timestamps.
#[test]
fn test_meta_checkpoint_case_10_metadata_upsert_is_atomic_with_pager_commit() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        let committed_ts = mvcc_store.last_committed_tx_ts.load(Ordering::SeqCst);
        assert!(committed_ts > 0);

        let pager = conn.pager.load().clone();
        let mut checkpoint_sm = CheckpointStateMachine::new(
            pager.clone(),
            mvcc_store,
            conn.clone(),
            true,
            conn.get_sync_mode(),
            crate::MAIN_DB_ID,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );

        for _ in 0..50_000 {
            if checkpoint_sm.state_for_test() == CheckpointState::CheckpointWal {
                break;
            }
            match checkpoint_sm.step(&()).unwrap() {
                TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
                TransitionResult::Continue => {}
                TransitionResult::Done(_) => {
                    panic!("checkpoint finished before expected stop window")
                }
            }
        }
    }

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a");

    let meta = get_rows(
        &conn,
        "SELECT v FROM __turso_internal_mvcc_meta WHERE k = 'persistent_tx_ts_max'",
    );
    assert_eq!(meta.len(), 1);
    assert!(
        meta[0][0].as_int().unwrap() >= 1,
        "expected metadata boundary to persist with pager commit"
    );
}

/// What this test checks: ordinary prepared reads recompile when checkpoint publishes a table root page.
/// Why this matters: root publication invalidates compiled bytecode generally, not only PRAGMA integrity_check.
#[test]
fn test_prepared_select_reprepares_after_checkpoint_root_publish() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Reprepare), 0);

    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = stmt.run_collect_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(&rows[0][1].to_string(), "a");
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Reprepare), 1);
}

/// What this test checks: data-only MVCC checkpoints do not invalidate already-prepared statements.
/// Why this matters: only root/drop publication should force reprepare; ordinary row checkpointing should leave prepared bytecode reusable.
#[test]
fn test_prepared_select_does_not_reprepare_after_data_only_checkpoint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Reprepare), 0);

    conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = stmt.run_collect_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(&rows[0][1].to_string(), "a");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(&rows[1][1].to_string(), "b");
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Reprepare), 0);
}

/// What this test checks: prepared index lookups recompile when checkpoint publishes an index root page.
/// Why this matters: table and index roots are published independently, and stale index bytecode must not survive checkpoint.
#[test]
fn test_prepared_index_lookup_reprepares_after_checkpoint_root_publish() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, payload TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_t_v ON t(v)").unwrap();

    let mut stmt = conn
        .prepare("SELECT id, payload FROM t INDEXED BY idx_t_v WHERE v = 'b'")
        .unwrap();
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Reprepare), 0);

    conn.execute("INSERT INTO t VALUES (1, 'a', 'one')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'b', 'two')")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = stmt.run_collect_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 2);
    assert_eq!(&rows[0][1].to_string(), "two");
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Reprepare), 1);
}

/// Runs an auto-checkpoint, forces it to yield, then injects an error after the
/// pager commit has made the new root pages visible. Even though later checkpoint
/// cleanup fails, prepared integrity_check statements must use the committed root
/// pages and report a clean database.
#[test]
fn test_integrity_check_after_checkpoint_io_yield_then_post_durable_failure_uses_user_apis() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
        .unwrap();
    let stale_schema_conn = db.connect();
    let mut stale_integrity_check = stale_schema_conn.prepare("PRAGMA integrity_check").unwrap();
    let schema_version = get_rows(&conn, "PRAGMA schema_version");
    assert_eq!(schema_version.len(), 1);
    let schema_version_before_checkpoint = schema_version[0][0].as_int().unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();

    let injector = FixedYieldInjector::new([CheckpointYieldPoint::BeforeAcquireLock.point()]);
    conn.set_yield_injector(Some(injector.clone()));
    let failure_injector = FixedFailureInjector::new([(
        CheckpointYieldPoint::AfterDurableBoundaryAdvanced.point(),
        LimboError::TxError("synthetic checkpoint failure after pager commit".to_string()),
    )]);
    conn.set_failure_injector(Some(failure_injector.clone()));

    let mut same_conn_stale_integrity_check = conn.prepare("PRAGMA integrity_check").unwrap();
    let mut insert_stmt = conn.prepare("INSERT INTO t VALUES (1, 'a')").unwrap();
    let mut yielded_before_checkpoint_lock = false;
    for _ in 0..10_000 {
        match insert_stmt.step().unwrap() {
            crate::StepResult::Yield if injector.is_empty() => {
                yielded_before_checkpoint_lock = true;
                break;
            }
            crate::StepResult::IO | crate::StepResult::Yield => {}
            crate::StepResult::Done => {
                panic!("INSERT completed before checkpoint acquire-lock yield fired")
            }
            other => panic!("unexpected INSERT step result before yield: {other:?}"),
        }
    }
    assert!(
        yielded_before_checkpoint_lock,
        "expected INSERT auto-checkpoint to yield before acquiring the checkpoint lock"
    );

    let mut completed_after_durable_boundary_failure = false;
    for _ in 0..10_000 {
        match insert_stmt.step() {
            Ok(crate::StepResult::Done) if failure_injector.is_empty() => {
                completed_after_durable_boundary_failure = true;
                break;
            }
            Ok(crate::StepResult::Done) => {
                panic!("INSERT completed before checkpoint durable-boundary failure fired")
            }
            Err(err) => panic!("unexpected INSERT error after yield: {err:?}"),
            Ok(crate::StepResult::IO | crate::StepResult::Yield) => {}
            Ok(other) => panic!("unexpected INSERT step result after yield: {other:?}"),
        }
    }
    assert!(
        completed_after_durable_boundary_failure,
        "expected INSERT auto-checkpoint to observe durable-boundary failure and finish"
    );

    conn.set_yield_injector(None);
    conn.set_failure_injector(None);

    let schema_version = get_rows(&conn, "PRAGMA schema_version");
    assert_eq!(schema_version.len(), 1);
    assert_eq!(
        schema_version[0][0].as_int().unwrap(),
        schema_version_before_checkpoint
    );

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");

    let rows = same_conn_stale_integrity_check.run_collect_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");

    let rows = stale_integrity_check.run_collect_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");

    let rows = get_rows(&stale_schema_conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");

    let rows = get_rows(&conn, "SELECT id, v FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(&rows[0][1].to_string(), "a");
}

/// Steps:
/// 1. Create an MVCC database with a table and unique index whose roots are still logical MVCC roots.
/// 2. Prepare `PRAGMA integrity_check` on a second connection.
/// 3. Start stepping it, then yield immediately before `OP_Transaction` opens the read transaction.
/// 4. On the writer connection, insert a row and run `wal_checkpoint(TRUNCATE)` so checkpoint publishes physical roots.
/// 5. Resume the stale statement; it must force one reprepare and then report `ok`.
#[test]
fn test_running_integrity_check_reprepares_after_checkpoint_root_publish() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let writer = db.connect();
    writer
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
        .unwrap();

    let stale_conn = db.connect();
    let injector = FixedYieldInjector::new([TransactionYieldPoint::BeforeStart.point()]);
    stale_conn.set_yield_injector(Some(injector.clone()));
    let mut stale_integrity_check = stale_conn.prepare("PRAGMA integrity_check").unwrap();
    assert!(
        matches!(
            stale_integrity_check.step().unwrap(),
            crate::StepResult::Yield
        ) && injector.is_empty(),
        "integrity_check should yield before opening its read transaction"
    );

    writer.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    stale_conn.set_yield_injector(None);

    // The statement has already passed its public step-time schema refresh, but
    // its transaction has not opened yet. Checkpoint root publication does not
    // change SQLite's schema cookie, so OP_Transaction must still force a
    // reprepare before stale bytecode can use the new header with old roots.
    let rows = stale_integrity_check.run_collect_rows().unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
    assert_eq!(
        stale_integrity_check.stmt_status(StatementStatusCounter::Reprepare),
        1
    );
}

/// Steps:
/// 1. Create an MVCC database with a table and unique index whose roots are still logical MVCC roots.
/// 2. Open a deferred transaction on a second connection without starting its MVCC read transaction yet.
/// 3. Prepare `PRAGMA integrity_check` inside that deferred transaction.
/// 4. Start stepping it, then yield immediately before `OP_Transaction` opens the read transaction.
/// 5. On the writer connection, insert a row and run `wal_checkpoint(TRUNCATE)` so checkpoint publishes physical roots.
/// 6. Resume the deferred statement; it must force one reprepare, report `ok`, and leave the transaction committable.
#[test]
fn test_deferred_begin_integrity_check_reprepares_after_checkpoint_root_publish() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let writer = db.connect();
    writer
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
        .unwrap();

    let stale_conn = db.connect();
    stale_conn.execute("BEGIN").unwrap();

    let injector = FixedYieldInjector::new([TransactionYieldPoint::BeforeStart.point()]);
    stale_conn.set_yield_injector(Some(injector.clone()));
    let mut stale_integrity_check = stale_conn.prepare("PRAGMA integrity_check").unwrap();
    assert!(
        matches!(
            stale_integrity_check.step().unwrap(),
            crate::StepResult::Yield
        ) && injector.is_empty(),
        "integrity_check should yield before opening its read transaction"
    );

    writer.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    stale_conn.set_yield_injector(None);

    let rows = stale_integrity_check.run_collect_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
    assert_eq!(
        stale_integrity_check.stmt_status(StatementStatusCounter::Reprepare),
        1
    );

    stale_conn.execute("COMMIT").unwrap();
}

/// Steps:
/// 1. Create an MVCC database with a table and unique index whose roots are still logical MVCC roots.
/// 2. Prepare `PRAGMA integrity_check` on a second connection and record that it has not reprepared yet.
/// 3. Record `PRAGMA schema_version` before the checkpoint.
/// 4. Start stepping the stale statement, then yield immediately before `OP_Transaction` opens the read transaction.
/// 5. On the writer connection, insert a row and run `wal_checkpoint(TRUNCATE)` so checkpoint publishes physical roots.
/// 6. Assert the checkpoint did not bump SQLite's schema cookie.
/// 7. Resume the stale statement; it must force one reprepare and then report `ok`.
#[test]
fn test_running_integrity_check_reprepares_without_schema_cookie_bump() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let writer = db.connect();
    writer
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
        .unwrap();

    let stale_conn = db.connect();
    let injector = FixedYieldInjector::new([TransactionYieldPoint::BeforeStart.point()]);
    stale_conn.set_yield_injector(Some(injector.clone()));
    let mut stale_integrity_check = stale_conn.prepare("PRAGMA integrity_check").unwrap();
    assert_eq!(
        stale_integrity_check.stmt_status(StatementStatusCounter::Reprepare),
        0
    );

    let schema_version_before = get_rows(&writer, "PRAGMA schema_version")[0][0]
        .as_int()
        .unwrap();
    assert!(
        matches!(
            stale_integrity_check.step().unwrap(),
            crate::StepResult::Yield
        ) && injector.is_empty(),
        "integrity_check should yield before opening its read transaction"
    );

    writer.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let schema_version_after = get_rows(&writer, "PRAGMA schema_version")[0][0]
        .as_int()
        .unwrap();
    assert_eq!(
        schema_version_after, schema_version_before,
        "checkpoint root publication must not change SQLite's schema cookie"
    );

    stale_conn.set_yield_injector(None);
    let rows = stale_integrity_check.run_collect_rows().unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
    assert_eq!(
        stale_integrity_check.stmt_status(StatementStatusCounter::Reprepare),
        1
    );
}

#[test]
fn reader_does_not_pin_read_mark_until_checkpoint_gate_is_available() {
    use crate::StepResult;

    let db = MvccTestDbNoConn::new_with_random_db();
    let writer = db.connect();
    writer
        .execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    writer
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
        .unwrap();
    writer.execute("INSERT INTO t VALUES (1, 'seed')").unwrap();
    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    writer
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();

    let checkpoint_injector =
        FixedYieldInjector::new([CheckpointYieldPoint::BeforePagerCommit.point()]);
    writer.set_yield_injector(Some(checkpoint_injector.clone()));
    let mut checkpointing_insert = writer
        .prepare("INSERT INTO t VALUES (2, 'checkpointed')")
        .unwrap();
    let checkpoint_io = checkpointing_insert.get_pager().io.clone();
    step_until_checkpoint_before_pager_commit_yield(
        &mut checkpointing_insert,
        &checkpoint_injector,
        &checkpoint_io,
        "insert",
    );

    let reader = db.connect();
    let read_mark_probe = FixedYieldInjector::new([TransactionYieldPoint::BeforeMvccBegin.point()]);
    reader.set_yield_injector(Some(read_mark_probe.clone()));
    let mut read = reader.prepare("SELECT v FROM t WHERE id = 1").unwrap();
    assert!(
        matches!(read.step().unwrap(), StepResult::Yield) && read_mark_probe.is_empty(),
        "reader should yield before opening its MVCC transaction"
    );
    assert!(
        !reader.get_pager().holds_read_lock(),
        "reader must not pin a pager read mark before MVCC begin is allowed"
    );

    match read.step() {
        Ok(StepResult::Busy) | Err(LimboError::Busy) => {}
        other => panic!("reader should be Busy before pinning a read mark, got {other:?}"),
    }
    assert!(
        !reader.get_pager().holds_read_lock(),
        "Busy reader must not leave a pager read mark pinned"
    );
    drop(read);

    writer.set_yield_injector(None);
    step_until_done(&mut checkpointing_insert, &checkpoint_io, "insert");
}

#[test]
fn integrity_check_does_not_pin_read_mark_until_checkpoint_gate_is_available() {
    use crate::StepResult;

    let db = MvccTestDbNoConn::new_with_random_db();
    let writer = db.connect();
    writer
        .execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    writer
        .execute("CREATE TABLE keep(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    writer
        .execute("CREATE TABLE free_me(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    writer
        .execute("INSERT INTO keep VALUES (1, 'seed')")
        .unwrap();
    writer
        .execute("INSERT INTO free_me VALUES (1, 'soon freed')")
        .unwrap();
    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    writer
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();

    let checkpoint_injector =
        FixedYieldInjector::new([CheckpointYieldPoint::BeforePagerCommit.point()]);
    writer.set_yield_injector(Some(checkpoint_injector.clone()));
    let mut checkpointing_drop = writer.prepare("DROP TABLE free_me").unwrap();
    let checkpoint_io = checkpointing_drop.get_pager().io.clone();
    step_until_checkpoint_before_pager_commit_yield(
        &mut checkpointing_drop,
        &checkpoint_injector,
        &checkpoint_io,
        "drop-table checkpoint",
    );

    let reader = db.connect();
    let read_mark_probe = FixedYieldInjector::new([TransactionYieldPoint::BeforeMvccBegin.point()]);
    reader.set_yield_injector(Some(read_mark_probe.clone()));
    let mut integrity_check = reader.prepare("PRAGMA integrity_check").unwrap();
    assert!(
        matches!(integrity_check.step().unwrap(), StepResult::Yield) && read_mark_probe.is_empty(),
        "integrity_check should yield before opening its MVCC transaction"
    );
    assert!(
        !reader.get_pager().holds_read_lock(),
        "integrity_check must not pin a pager read mark before MVCC begin is allowed"
    );

    match integrity_check.step() {
        Ok(StepResult::Busy) | Err(LimboError::Busy) => {}
        other => panic!("integrity_check should be Busy before pinning a read mark, got {other:?}"),
    }
    assert!(
        !reader.get_pager().holds_read_lock(),
        "Busy integrity_check must not leave a pager read mark pinned"
    );
    drop(integrity_check);
    reader.set_yield_injector(None);

    writer.set_yield_injector(None);
    step_until_done(
        &mut checkpointing_drop,
        &checkpoint_io,
        "drop-table checkpoint",
    );

    let rows = get_rows(&reader, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn integrity_check_does_not_report_freelist_count_mismatch_after_checkpoint_begin_race() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let writer = db.connect();
    writer
        .execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    writer
        .execute("CREATE TABLE keep(id INTEGER PRIMARY KEY, payload BLOB)")
        .unwrap();
    writer
        .execute("CREATE TABLE trash(id INTEGER PRIMARY KEY, payload BLOB)")
        .unwrap();
    writer
        .execute("INSERT INTO keep VALUES (1, zeroblob(20000))")
        .unwrap();
    writer
        .execute("INSERT INTO trash VALUES (1, zeroblob(20000))")
        .unwrap();
    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    writer.execute("DELETE FROM trash WHERE id = 1").unwrap();
    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let old_freelist_count = get_rows(&writer, "PRAGMA freelist_count")[0][0]
        .as_int()
        .unwrap();
    assert!(
        old_freelist_count > 0,
        "setup should leave a materialized freelist"
    );

    let reader = db.connect();
    let checkpointing_delete = CheckpointingDeleteAtMvccBeginInjector::new(writer.clone());
    reader.set_yield_injector(Some(checkpointing_delete.clone()));
    let mut integrity_check = reader.prepare("PRAGMA integrity_check").unwrap();
    let rows = integrity_check.run_collect_rows().unwrap();
    reader.set_yield_injector(None);
    assert!(
        checkpointing_delete.fired(),
        "checkpointing delete should run at the MVCC begin boundary"
    );
    let new_freelist_count = get_rows(&writer, "PRAGMA freelist_count")[0][0]
        .as_int()
        .unwrap();
    assert!(
        new_freelist_count > old_freelist_count,
        "delete checkpoint should increase freelist count: old={old_freelist_count}, new={new_freelist_count}"
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn integrity_check_does_not_report_page_never_used_after_checkpoint_begin_race() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let writer = db.connect();
    writer
        .execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    writer
        .execute("CREATE TABLE keep(id INTEGER PRIMARY KEY, payload BLOB)")
        .unwrap();
    writer
        .execute("INSERT INTO keep VALUES (1, zeroblob(1000))")
        .unwrap();
    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let old_page_count = get_rows(&writer, "PRAGMA page_count")[0][0]
        .as_int()
        .unwrap();

    let reader = db.connect();
    let checkpointing_insert = CheckpointingInsertAtMvccBeginInjector::new(writer.clone());
    reader.set_yield_injector(Some(checkpointing_insert.clone()));
    let mut integrity_check = reader.prepare("PRAGMA integrity_check").unwrap();
    let rows = integrity_check.run_collect_rows().unwrap();
    reader.set_yield_injector(None);
    assert!(
        checkpointing_insert.fired(),
        "checkpointing insert should run at the MVCC begin boundary"
    );
    let new_page_count = get_rows(&writer, "PRAGMA page_count")[0][0]
        .as_int()
        .unwrap();
    assert!(
        new_page_count > old_page_count,
        "checkpointed insert should grow the database: old={old_page_count}, new={new_page_count}"
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

fn step_until_checkpoint_before_pager_commit_yield(
    stmt: &mut crate::Statement,
    injector: &FixedYieldInjector,
    io: &Arc<dyn IO>,
    context: &str,
) {
    for _ in 0..100_000 {
        match stmt.step().unwrap() {
            crate::StepResult::Yield if injector.is_empty() => return,
            crate::StepResult::IO | crate::StepResult::Yield => io.step().unwrap(),
            crate::StepResult::Row => {}
            crate::StepResult::Done => {
                panic!("{context} completed before BeforePagerCommit yield")
            }
            other => panic!("unexpected {context} step before yield: {other:?}"),
        }
    }
    panic!("checkpoint did not reach its guarded write phase");
}

fn step_until_done(stmt: &mut crate::Statement, io: &Arc<dyn IO>, context: &str) {
    for _ in 0..100_000 {
        match stmt.step().unwrap() {
            crate::StepResult::Done => return,
            crate::StepResult::Row => {}
            crate::StepResult::IO | crate::StepResult::Yield => io.step().unwrap(),
            other => panic!("unexpected {context} step after yield: {other:?}"),
        }
    }
    panic!("{context} did not finish");
}

#[test]
fn attached_reader_does_not_pin_read_mark_until_checkpoint_gate_is_available() {
    use crate::StepResult;

    let db = MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new().with_attach(true));
    let aux_dir = tempfile::TempDir::new().unwrap();
    let aux_path = aux_dir
        .path()
        .join(format!("aux_{}.db", rand::random::<u64>()));
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    let writer = db.connect();
    writer
        .execute(format!("ATTACH '{aux_path_str}' AS aux"))
        .unwrap();
    writer
        .execute("PRAGMA aux.journal_mode = 'experimental_mvcc'")
        .unwrap();
    let aux_db_id = writer.get_database_id_by_name("aux").unwrap();
    let aux_mv_store = writer
        .mv_store_for_db(aux_db_id)
        .expect("attached aux database must be MVCC");
    aux_mv_store.set_checkpoint_threshold(-1);
    writer
        .execute("CREATE TABLE aux.t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
        .unwrap();
    writer
        .execute("INSERT INTO aux.t VALUES (1, 'seed')")
        .unwrap();
    aux_mv_store.set_checkpoint_threshold(0);

    let checkpoint_io = writer
        .get_pager_from_database_index(&aux_db_id)
        .unwrap()
        .io
        .clone();
    let checkpoint_injector =
        FixedYieldInjector::new([CheckpointYieldPoint::BeforePagerCommit.point()]);
    writer.set_yield_injector(Some(checkpoint_injector.clone()));
    let mut checkpointing_insert = writer
        .prepare("INSERT INTO aux.t VALUES (2, 'checkpointed')")
        .unwrap();
    step_until_checkpoint_before_pager_commit_yield(
        &mut checkpointing_insert,
        &checkpoint_injector,
        &checkpoint_io,
        "attached insert",
    );

    let reader = db.connect();
    reader
        .execute(format!("ATTACH '{aux_path_str}' AS aux"))
        .unwrap();
    let reader_aux_db_id = reader.get_database_id_by_name("aux").unwrap();
    let reader_aux_pager = reader
        .get_pager_from_database_index(&reader_aux_db_id)
        .unwrap();
    let read_mark_probe = FixedYieldInjector::new([TransactionYieldPoint::BeforeMvccBegin.point()]);
    reader.set_yield_injector(Some(read_mark_probe.clone()));
    let mut read = reader.prepare("SELECT v FROM aux.t WHERE id = 1").unwrap();
    assert!(
        matches!(read.step().unwrap(), StepResult::Yield) && read_mark_probe.is_empty(),
        "attached reader should yield before opening its MVCC transaction"
    );
    assert!(
        !reader_aux_pager.holds_read_lock(),
        "attached reader must not pin a pager read mark before MVCC begin is allowed"
    );

    match read.step() {
        Ok(StepResult::Busy) | Err(LimboError::Busy) => {}
        other => panic!("attached reader should be Busy before pinning a read mark, got {other:?}"),
    }
    assert!(
        !reader_aux_pager.holds_read_lock(),
        "Busy attached reader must not leave a pager read mark pinned"
    );
    drop(read);
    reader.set_yield_injector(None);

    writer.set_yield_injector(None);
    step_until_done(&mut checkpointing_insert, &checkpoint_io, "attached insert");
}

#[test]
fn savepoint_does_not_pin_read_mark_until_checkpoint_gate_is_available() {
    use crate::StepResult;

    let db = MvccTestDbNoConn::new_with_random_db();
    let writer = db.connect();
    writer
        .execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    writer
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
        .unwrap();
    writer.execute("INSERT INTO t VALUES (1, 'seed')").unwrap();
    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    writer
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();

    let checkpoint_injector =
        FixedYieldInjector::new([CheckpointYieldPoint::BeforePagerCommit.point()]);
    writer.set_yield_injector(Some(checkpoint_injector.clone()));
    let mut checkpointing_insert = writer
        .prepare("INSERT INTO t VALUES (2, 'checkpointed')")
        .unwrap();
    let checkpoint_io = checkpointing_insert.get_pager().io.clone();
    step_until_checkpoint_before_pager_commit_yield(
        &mut checkpointing_insert,
        &checkpoint_injector,
        &checkpoint_io,
        "insert",
    );

    let reader = db.connect();
    let read_mark_probe = FixedYieldInjector::new([TransactionYieldPoint::BeforeMvccBegin.point()]);
    reader.set_yield_injector(Some(read_mark_probe.clone()));
    let mut savepoint = reader.prepare("SAVEPOINT s").unwrap();
    assert!(
        matches!(savepoint.step().unwrap(), StepResult::Yield) && read_mark_probe.is_empty(),
        "SAVEPOINT should yield before opening its MVCC transaction"
    );
    assert!(
        !reader.get_pager().holds_read_lock(),
        "SAVEPOINT must not pin a pager read mark before MVCC begin is allowed"
    );

    match savepoint.step() {
        Ok(StepResult::Busy) | Err(LimboError::Busy) => {}
        other => panic!("SAVEPOINT should be Busy before pinning a read mark, got {other:?}"),
    }
    assert!(
        !reader.get_pager().holds_read_lock(),
        "Busy SAVEPOINT must not leave a pager read mark pinned"
    );
    drop(savepoint);
    reader.set_yield_injector(None);

    writer.set_yield_injector(None);
    step_until_done(&mut checkpointing_insert, &checkpoint_io, "insert");

    reader.execute("SAVEPOINT s").unwrap();
    assert!(
        reader.get_pager().holds_read_lock(),
        "successful MVCC SAVEPOINT must pin a pager read mark"
    );
    reader.execute("ROLLBACK").unwrap();
}

/// What this test checks: Auto-checkpoint post-commit failure does not invalidate committed transaction visibility on restart.
/// Why this matters: Commit contract must remain stable even when checkpoint cleanup fails mid-flight.
#[test]
fn test_meta_checkpoint_case_11_auto_checkpoint_failure_after_commit_remains_recoverable() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    let mvcc_store = db.get_mvcc_store();
    let ts1 = mvcc_store.last_committed_tx_ts.load(Ordering::SeqCst);
    assert!(ts1 > 0, "expected committed timestamp for first insert");

    let pager = conn.pager.load().clone();
    let mut checkpoint_sm = CheckpointStateMachine::new(
        pager.clone(),
        mvcc_store.clone(),
        conn.clone(),
        true,
        conn.get_sync_mode(),
        crate::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    let mut reached_truncate = false;
    for _ in 0..50_000 {
        if checkpoint_sm.state_for_test() == CheckpointState::TruncateLogicalLog {
            reached_truncate = true;
            break; // Simulate checkpoint aborting before log truncation
        }
        match checkpoint_sm.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => {
                panic!("checkpoint finished before reaching truncate state")
            }
        }
    }
    assert!(
        reached_truncate,
        "expected to reach TruncateLogicalLog state"
    );

    // Pager commit already succeeded before log truncation.
    // Same-process retries must advance from this durable boundary.
    let durable_boundary = mvcc_store.durable_txid_max.load(Ordering::SeqCst);
    assert!(
        durable_boundary >= ts1,
        "expected in-memory durable checkpoint boundary to advance after pager commit: boundary={durable_boundary} ts1={ts1}"
    );

    let sync_mode = conn.get_sync_mode();
    let checkpoint_sm2 = CheckpointStateMachine::new(
        pager,
        mvcc_store,
        conn,
        true,
        sync_mode,
        crate::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    let (old_boundary, _) = checkpoint_sm2.checkpoint_bounds_for_test();
    assert!(
        old_boundary.unwrap_or_default() >= ts1,
        "expected retry checkpoint to start from durable boundary: old={old_boundary:?} ts1={ts1}"
    );
}

/// What this test checks: a checkpoint state machine created before another checkpoint
/// advances the durable boundary must resample that boundary after taking the checkpoint lock.
/// Why this matters: otherwise a delayed checkpoint can replay an already-durable unique-index
/// delete and fail. This test uses raw APIs, check test_checkpoint_resamples_boundary_before_starting_with_yield_injection
/// which uses only user facing APIs to simulate the same error.
#[test]
fn test_checkpoint_resamples_boundary_before_starting() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute(
        "CREATE TABLE dry_floor_846 (
            sour_sand_972 BLOB UNIQUE,
            sour_river_140 REAL,
            sweet_wall_518 BLOB,
            fast_grass_379 TEXT,
            dark_wave_139 REAL UNIQUE,
            sad_wind_216 INTEGER UNIQUE PRIMARY KEY
        )",
    )
    .unwrap();

    conn.execute(
        "INSERT INTO dry_floor_846 (
            sour_sand_972, sour_river_140, sweet_wall_518,
            fast_grass_379, dark_wave_139, sad_wind_216
        ) VALUES (
            zeroblob(16), 6.85, x'736d6172745f6c6561665f353637',
            'wild_hill_714', 8.43, 788
        )",
    )
    .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mvcc_store = db.get_mvcc_store();
    let first_boundary = mvcc_store.durable_txid_max.load(Ordering::SeqCst);
    assert!(first_boundary > 0);

    conn.execute(
        "UPDATE dry_floor_846
            SET sour_sand_972 = x'66756c6c5f737461725f333732',
                sour_river_140 = 5.75,
                sweet_wall_518 = zeroblob(32),
                fast_grass_379 = 'old_moon_16',
                dark_wave_139 = 2.90
          WHERE sad_wind_216 = 788",
    )
    .unwrap();
    let update_ts = mvcc_store.last_committed_tx_ts.load(Ordering::SeqCst);
    assert!(update_ts > first_boundary);

    let delayed_conn = db.connect();
    let delayed_pager = delayed_conn.pager.load().clone();
    let mut delayed_checkpoint = CheckpointStateMachine::new(
        delayed_pager.clone(),
        mvcc_store.clone(),
        delayed_conn.clone(),
        true,
        delayed_conn.get_sync_mode(),
        crate::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    let (old_boundary, _) = delayed_checkpoint.checkpoint_bounds_for_test();
    assert_eq!(old_boundary, Some(first_boundary));

    let interrupted_conn = db.connect();
    let interrupted_pager = interrupted_conn.pager.load().clone();
    let mut interrupted_checkpoint = CheckpointStateMachine::new(
        interrupted_pager.clone(),
        mvcc_store.clone(),
        interrupted_conn.clone(),
        true,
        interrupted_conn.get_sync_mode(),
        crate::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    let mut reached_wal_checkpoint = false;
    for _ in 0..50_000 {
        if interrupted_checkpoint.state_for_test() == CheckpointState::CheckpointWal {
            reached_wal_checkpoint = true;
            break;
        }
        match interrupted_checkpoint.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(interrupted_pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => {
                panic!("checkpoint finished before reaching WAL checkpoint")
            }
        }
    }
    assert!(
        reached_wal_checkpoint,
        "expected checkpoint to reach WAL checkpoint"
    );
    assert_eq!(
        mvcc_store.durable_txid_max.load(Ordering::SeqCst),
        update_ts
    );
    interrupted_checkpoint
        .cleanup_after_external_io_error(LimboError::Interrupt)
        .unwrap();

    let mut finished = false;
    for _ in 0..50_000 {
        match delayed_checkpoint.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(delayed_pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => {
                finished = true;
                break;
            }
        }
    }
    assert!(finished, "delayed checkpoint did not finish");

    let rows = get_rows(
        &conn,
        "SELECT sad_wind_216, dark_wave_139, hex(sour_sand_972)
           FROM dry_floor_846
          WHERE sad_wind_216 = 788",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 788);
    assert_eq!(rows[0][1].to_string(), "2.9");
    assert_eq!(&rows[0][2].to_string(), "66756C6C5F737461725F333732");

    let integrity = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(integrity.len(), 1);
    assert_eq!(&integrity[0][0].to_string(), "ok");
}

#[test]
fn test_reader_consistent_during_large_indexed_commit_rewrite() {
    use crate::StepResult;
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let c1 = db.connect();
    c1.execute("CREATE TABLE t(pk INTEGER PRIMARY KEY, v INTEGER UNIQUE)")
        .unwrap();
    // > 1024 rows so the commit's RewriteLiveVersions spans multiple batches.
    c1.execute("BEGIN").unwrap();
    for i in 0..1500i64 {
        c1.execute(format!("INSERT INTO t VALUES ({i}, {})", i + 1_000_000))
            .unwrap();
    }
    c1.execute("COMMIT").unwrap();
    c1.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let c2 = db.connect();

    // Large UPDATE of the indexed column in one tx; drive its COMMIT step-by-step.
    c1.execute("BEGIN CONCURRENT").unwrap();
    c1.execute("UPDATE t SET v = v + 5_000_000").unwrap();
    let mut commit = c1.prepare("COMMIT").unwrap();
    // Co-drive c1's COMMIT and a c2 integrity_check non-blocking against the shared IO.
    // A blocking read on c2 would deadlock: it can't finish while c1's commit is parked
    // mid-RewriteLiveVersions, and c1 only advances when stepped. Stepping both keeps
    // progress flowing while still exercising c2 reads across the rewrite window.
    let io = c1.pager.load().io.clone();
    let mut check = c2.prepare("PRAGMA integrity_check").unwrap();
    let mut last_row: Option<Vec<Value>> = None;
    let mut commit_done = false;
    let mut checks = 0u32;
    loop {
        if !commit_done {
            match commit.step().unwrap() {
                StepResult::Done => commit_done = true,
                StepResult::IO | StepResult::Yield => {}
                other => panic!("unexpected commit step: {other:?}"),
            }
        }
        match check.step().unwrap() {
            StepResult::Row => {
                last_row = Some(check.row().unwrap().get_values().cloned().collect());
            }
            StepResult::Done => {
                let row = last_row.take().expect("integrity_check returns a row");
                assert_eq!(
                    &row[0].to_string(),
                    "ok",
                    "integrity failed mid-rewrite: {row:?}"
                );
                checks += 1;
                if commit_done {
                    break;
                }
                check = c2.prepare("PRAGMA integrity_check").unwrap();
            }
            StepResult::IO | StepResult::Yield => {}
            other => panic!("unexpected check step: {other:?}"),
        }
        io.step().unwrap();
    }
    assert!(
        checks >= 1,
        "expected at least one concurrent integrity_check"
    );
    let integ = get_rows(&c1, "PRAGMA integrity_check");
    assert_eq!(&integ[0][0].to_string(), "ok", "final integrity: {integ:?}");
}

#[test]
fn test_checkpoint_two_scan_toctou_orphans_first_checkpoint_unique_index() {
    use crate::StepResult;
    let _ = tracing_subscriber::fmt::try_init();
    let db = MvccTestDbNoConn::new_with_random_db_passive();

    // connV (victim writer): create + populate, but DO NOT checkpoint, so the
    // table btree and both UNIQUE autoindexes are created fresh in the pass below.
    let conn_v = db.connect();
    conn_v
        .execute("CREATE TABLE t(pk NUMERIC PRIMARY KEY, v NUMERIC UNIQUE)")
        .unwrap();
    conn_v.execute("INSERT INTO t VALUES (615, 329)").unwrap();
    // A second surviving row so the table btree is non-empty regardless of the
    // victim row's fate (keeps integrity_check scanning the table).
    conn_v.execute("INSERT INTO t VALUES (616, 330)").unwrap();

    let conn_c = db.connect();
    conn_c
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    let injector = FixedYieldInjector::new([
        CheckpointYieldPoint::AfterCollectTableRows.point(),
        CheckpointYieldPoint::BeforeAcquireLock.point(),
    ]);
    conn_c.set_yield_injector(Some(injector.clone()));
    // Force a checkpoint and stop before getting rows
    let mut checkpoint = conn_c.prepare("INSERT INTO t VALUES (617, 331)").unwrap();
    let pager_io = conn_c.pager.load().io.clone();

    // Helper: step the auto-checkpoint until the NEXT injected yield fires (the
    // injector's remaining-set shrinks). Returns when a fresh yield is observed.
    let step_to_next_yield = |checkpoint: &mut crate::Statement, expect_remaining: usize| {
        for _ in 0..200_000 {
            match checkpoint.step().unwrap() {
                StepResult::IO | StepResult::Yield => {
                    if injector.remaining_len() == expect_remaining {
                        return true;
                    }
                    pager_io.step().unwrap();
                }
                StepResult::Done => return false,
                other => panic!("unexpected checkpoint step: {other:?}"),
            }
        }
        false
    };

    assert!(
        step_to_next_yield(&mut checkpoint, 1),
        "auto-checkpoint must yield after the table scan, before the index scan"
    );

    // start deleting rows so that we mark end with TxID, but not commit so that there
    // isn't any Timestamps to use, meaning we shouldn't checkpoint that one.
    let conn_d = db.connect();
    conn_d.execute("BEGIN").unwrap();
    conn_d.execute("DELETE FROM t WHERE pk = 615").unwrap();

    assert!(
        step_to_next_yield(&mut checkpoint, 0),
        "auto-checkpoint must yield before acquiring the blocking lock"
    );

    // rollback, this signifies we should see any change from this tx
    conn_d.execute("ROLLBACK").unwrap();

    // Complete checkpoint
    let mut checkpoint_done = false;
    for _ in 0..200_000 {
        match checkpoint.step().unwrap() {
            StepResult::Done => {
                checkpoint_done = true;
                break;
            }
            StepResult::IO | StepResult::Yield => pager_io.step().unwrap(),
            other => panic!("unexpected checkpoint step after resume: {other:?}"),
        }
    }
    assert!(checkpoint_done, "checkpoint did not complete");

    conn_c.set_yield_injector(None);

    let verifier = db.connect();
    let integ = get_rows(&verifier, "PRAGMA integrity_check");
    assert_eq!(
        integ.len(),
        1,
        "integrity_check must be a single 'ok' row, got: {integ:?}"
    );
    assert_eq!(
        &integ[0][0].to_string(),
        "ok",
        "checkpoint orphaned an index entry: {integ:?}"
    );
}

#[test]
fn test_checkpoint_gc_anchor_loss_update_then_delete_strands_stale_row() {
    use crate::StepResult;
    let _ = tracing_subscriber::fmt::try_init();
    let db = MvccTestDbNoConn::new_with_random_db_passive();

    let conn_v = db.connect();
    conn_v
        .execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, u NUMERIC UNIQUE)")
        .unwrap();
    conn_v.execute("INSERT INTO t VALUES (1, 724)").unwrap();

    let conn_c = db.connect();
    conn_c
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    let injector = FixedYieldInjector::new([CheckpointYieldPoint::BeforeAcquireLock.point()]);
    conn_c.set_yield_injector(Some(injector.clone()));
    let mut checkpoint = conn_c.prepare("INSERT INTO t VALUES (2, 999)").unwrap();
    let pager_io = conn_c.pager.load().io.clone();

    let step_to_next_yield = |checkpoint: &mut crate::Statement, expect_remaining: usize| {
        for _ in 0..200_000 {
            match checkpoint.step().unwrap() {
                StepResult::IO | StepResult::Yield => {
                    if injector.remaining_len() == expect_remaining {
                        return true;
                    }
                    pager_io.step().unwrap();
                }
                StepResult::Done => return false,
                other => panic!("unexpected checkpoint step: {other:?}"),
            }
        }
        false
    };

    // Pause AFTER both concurrent collection scans (row 1 collected live at T_snap) and
    // BEFORE the blocking lock.
    assert!(
        step_to_next_yield(&mut checkpoint, 0),
        "auto-checkpoint must yield before acquiring the blocking lock"
    );

    // UPDATER: autocommits at T_upd > T_snap while the checkpoint is paused. Same rowid =>
    // the table chain gains a current version; the index moves to a NEW {943} chain.
    let conn_u = db.connect();
    conn_u.execute("UPDATE t SET u = 943 WHERE pk = 1").unwrap();

    // Resume: the checkpoint writes its stale snapshot (u=724 + index {724}), publishes
    // boundary T_snap, and its GC drops OLD from the table chain (the anchor loss).
    let mut checkpoint_done = false;
    for _ in 0..200_000 {
        match checkpoint.step().unwrap() {
            StepResult::Done => {
                checkpoint_done = true;
                break;
            }
            StepResult::IO | StepResult::Yield => pager_io.step().unwrap(),
            other => panic!("unexpected checkpoint step after resume: {other:?}"),
        }
    }
    assert!(checkpoint_done, "first checkpoint did not complete");
    conn_c.set_yield_injector(None);

    // DELETER: autocommits at T_del. Table chain is now [NEW: T_upd -> T_del], whose
    // begin exceeds the published boundary.
    let conn_d = db.connect();
    conn_d.execute("DELETE FROM t WHERE pk = 1").unwrap();

    // Second checkpoint (threshold=0 commit on conn_c; no injected yields remain). The
    // table tombstone is unclassifiable (exists_in_db_file=false => skipped), while the
    // index {724} tombstone IS applied — leaving the durable table/index desynced.
    conn_c.execute("INSERT INTO t VALUES (3, 555)").unwrap();

    let verifier = db.connect();
    let integ = get_rows(&verifier, "PRAGMA integrity_check");
    assert_eq!(
        integ.len(),
        1,
        "integrity_check must be a single 'ok' row, got: {integ:?}"
    );
    assert_eq!(
        &integ[0][0].to_string(),
        "ok",
        "GC anchor loss stranded a stale table row: {integ:?}"
    );
}

/// Concurrent checkpoint + WWC abort stress; oracle is `integrity_check` only.
#[test]
fn test_conflict_abort_ckpt_indexed_update_savepoint_integrity_check() {
    conflict_abort_ckpt_indexed_update_body(MvccTestDbNoConn::new_with_random_db());
}

/// Same workload with passive checkpoint enabled.
#[test]
fn test_conflict_abort_ckpt_indexed_update_savepoint_integrity_check_passive() {
    conflict_abort_ckpt_indexed_update_body(MvccTestDbNoConn::new_with_random_db_passive());
}

fn conflict_abort_ckpt_indexed_update_body(db: MvccTestDbNoConn) {
    let conn = db.connect();
    // NUMERIC UNIQUE column => autoindex, mirroring empty_leaf_594 in the trace.
    conn.execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, u NUMERIC UNIQUE)")
        .unwrap();
    for i in 0..120 {
        conn.execute(format!("INSERT INTO t VALUES ({}, {})", i, 700 + i))
            .unwrap();
    }
    // Checkpoint so the seeded index/table values are btree-resident.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    // Passive auto-checkpoint on every commit.
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();

    // Retry transient MVCC concurrency errors (Busy / BusySnapshot / a snapshot whose
    // dependency aborted) so they aren't mistaken for a repro — only a non-"ok" integrity
    // result or a genuine error should fail the test.
    fn is_transient(e: &LimboError) -> bool {
        matches!(
            e,
            LimboError::Busy | LimboError::BusySnapshot | LimboError::CommitDependencyAborted
        )
    }
    fn read_retry(conn: &Arc<Connection>, query: &str) -> Option<Vec<Vec<Value>>> {
        for _ in 0..100_000 {
            let mut stmt = match conn.prepare(query) {
                Ok(s) => s,
                Err(e) if is_transient(&e) => {
                    std::thread::yield_now();
                    continue;
                }
                Err(e) => panic!("prepare failed: {e:?}"),
            };
            let mut rows = Vec::new();
            let res = stmt.run_with_row_callback(|row| {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
                Ok(())
            });
            match res {
                Ok(()) => return Some(rows),
                Err(e) if is_transient(&e) => {
                    std::thread::yield_now();
                    continue;
                }
                Err(e) => panic!("read query {query:?} failed: {e:?}"),
            }
        }
        None
    }

    let db_arc = db.get_db();
    let stop = Arc::new(AtomicBool::new(false));

    let reader_stop = stop.clone();
    let reader_db = db_arc.clone();
    let reader_handle = std::thread::spawn(move || {
        let reader = reader_db.connect().unwrap();
        reader
            .execute("PRAGMA mvcc_checkpoint_threshold = 0")
            .unwrap();
        let mut iters = 0u64;
        while !reader_stop.load(Ordering::Acquire) {
            // Single-snapshot integrity_check: the only sound concurrent oracle — it checks
            // table rows against index entries in one consistent read, no cross-snapshot
            // assumptions.
            if let Some(ic) = read_retry(&reader, "PRAGMA integrity_check") {
                assert_eq!(
                    ic.len(),
                    1,
                    "reader iter {iters}: integrity_check rows: {ic:?}"
                );
                assert_eq!(
                    &ic[0][0].to_string(),
                    "ok",
                    "reader iter {iters}: integrity_check failed: {:?}",
                    ic[0][0].to_string()
                );
            }
            iters += 1;
        }
    });

    let writer_db = db_arc;
    let writer_handle = std::thread::spawn(move || {
        let conn1 = writer_db.connect().unwrap();
        conn1
            .execute("PRAGMA mvcc_checkpoint_threshold = 0")
            .unwrap();
        let conn2 = writer_db.connect().unwrap();
        conn2
            .execute("PRAGMA mvcc_checkpoint_threshold = 0")
            .unwrap();
        let exec_retry = |c: &Arc<Connection>, sql: &str| -> Result<(), LimboError> {
            for _ in 0..1000 {
                match c.execute(sql) {
                    Ok(_) => return Ok(()),
                    Err(LimboError::Busy) => std::thread::yield_now(),
                    Err(e) => return Err(e),
                }
            }
            Err(LimboError::Busy)
        };
        for round in 0..400i64 {
            let survivor_pk = round % 120;
            let survivor_u = 700 + survivor_pk;

            conn1.execute("BEGIN CONCURRENT").unwrap();
            if exec_retry(
                &conn1,
                &format!(
                    "UPDATE t SET u = {} WHERE pk = {survivor_pk}",
                    90000 + round
                ),
            )
            .is_err()
                || exec_retry(&conn1, "SAVEPOINT sp").is_err()
                || exec_retry(
                    &conn1,
                    &format!(
                        "UPDATE t SET u = {} WHERE pk = {survivor_pk}",
                        91000 + round
                    ),
                )
                .is_err()
                || exec_retry(&conn1, "ROLLBACK TO sp").is_err()
            {
                let _ = conn1.execute("ROLLBACK");
                continue;
            }

            conn2.execute("BEGIN CONCURRENT").unwrap();
            let mut update_ok = false;
            for _ in 0..1000 {
                match conn2.execute(format!(
                    "UPDATE t SET u = {survivor_u} WHERE pk = {survivor_pk}"
                )) {
                    Ok(_) => {
                        update_ok = true;
                        break;
                    }
                    Err(LimboError::Busy) => std::thread::yield_now(),
                    Err(LimboError::WriteWriteConflict) | Err(LimboError::TxError(_)) => break,
                    Err(e) => panic!("conn2 update failed: {e:?}"),
                }
            }
            if !update_ok {
                let _ = conn2.execute("ROLLBACK");
                let _ = conn1.execute("COMMIT");
                let _ = conn1.execute("ROLLBACK");
                continue;
            }
            for _ in 0..1000 {
                match conn2.execute("COMMIT") {
                    Ok(_) => break,
                    Err(LimboError::Busy) => std::thread::yield_now(),
                    Err(_) => {
                        let _ = conn2.execute("ROLLBACK");
                        break;
                    }
                }
            }
            let _ = conn1.execute("COMMIT");
            let _ = conn1.execute("ROLLBACK");
        }
    });

    writer_handle.join().unwrap();
    stop.store(true, Ordering::Release);
    reader_handle.join().unwrap();

    let mut swept = false;
    for _ in 0..1000 {
        match conn.execute("PRAGMA wal_checkpoint(TRUNCATE)") {
            Ok(_) => {
                swept = true;
                break;
            }
            Err(LimboError::Busy) => std::thread::yield_now(),
            Err(e) => panic!("final checkpoint failed: {e:?}"),
        }
    }
    assert!(swept, "final checkpoint never succeeded");
    let ic2 = read_retry(&conn, "PRAGMA integrity_check").expect("final integrity_check");
    assert_eq!(ic2.len(), 1);
    assert_eq!(
        &ic2[0][0].to_string(),
        "ok",
        "post-checkpoint integrity_check: {ic2:?}"
    );
}

/// Content correctness under the passive checkpoint: `integrity_check` proves rows agree with
/// their indexes but not that values are right. A concurrent writer shuffles a fixed total
/// between accounts (each transfer sum-preserving and atomic) while passive checkpoints run on
/// every commit, so every reader snapshot must see the exact unchanged SUM and row COUNT.
#[test]
fn test_passive_concurrent_transfer_preserves_sum_and_count() {
    const N: i64 = 50;
    const INIT: i64 = 1000;
    const TOTAL: i64 = N * INIT;

    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE accounts(id INTEGER PRIMARY KEY, bal INTEGER NOT NULL)")
        .unwrap();
    for i in 0..N {
        setup
            .execute(format!("INSERT INTO accounts VALUES ({i}, {INIT})"))
            .unwrap();
    }
    // Materialize the seed rows, then passive auto-checkpoint on every commit.
    setup.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    setup
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();

    fn is_transient(e: &LimboError) -> bool {
        matches!(
            e,
            LimboError::Busy
                | LimboError::BusySnapshot
                | LimboError::CommitDependencyAborted
                | LimboError::WriteWriteConflict
        )
    }
    fn as_i64(v: &Value) -> i64 {
        v.to_string().parse().unwrap()
    }

    let db_arc = db.get_db();
    let stop = Arc::new(AtomicBool::new(false));

    // Reader: COUNT and SUM in ONE statement => one consistent snapshot. Both must be exact.
    let reader_stop = stop.clone();
    let reader_db = db_arc.clone();
    let reader = std::thread::spawn(move || {
        let conn = reader_db.connect().unwrap();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
            .unwrap();
        let mut iters = 0u64;
        while !reader_stop.load(Ordering::Acquire) {
            let mut stmt = match conn.prepare("SELECT COUNT(*), SUM(bal) FROM accounts") {
                Ok(s) => s,
                Err(ref e) if is_transient(e) => {
                    std::thread::yield_now();
                    continue;
                }
                Err(e) => panic!("reader prepare: {e:?}"),
            };
            let mut got: Option<(i64, i64)> = None;
            let res = stmt.run_with_row_callback(|row| {
                let vals: Vec<Value> = row.get_values().cloned().collect();
                got = Some((as_i64(&vals[0]), as_i64(&vals[1])));
                Ok(())
            });
            match res {
                Ok(()) => {
                    let (count, sum) = got.expect("aggregate yields one row");
                    assert_eq!(
                        count, N,
                        "reader iter {iters}: row count changed ({count} != {N})"
                    );
                    assert_eq!(
                        sum, TOTAL,
                        "reader iter {iters}: total balance changed ({sum} != {TOTAL}) — content corruption"
                    );
                }
                Err(ref e) if is_transient(e) => {
                    std::thread::yield_now();
                    continue;
                }
                Err(e) => panic!("reader run: {e:?}"),
            }
            iters += 1;
        }
    });

    // Writer: sum-preserving transfers between accounts, atomic per txn.
    let writer_db = db_arc;
    let writer = std::thread::spawn(move || {
        let conn = writer_db.connect().unwrap();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
            .unwrap();
        let mut rng = 0x9e3779b97f4a7c15u64;
        let mut next = move || {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (rng >> 33) as i64
        };
        for _ in 0..1500i64 {
            let a = next().rem_euclid(N);
            let mut b = next().rem_euclid(N);
            if b == a {
                b = (b + 1).rem_euclid(N);
            }
            let amt = next().rem_euclid(50) + 1;
            if conn.execute("BEGIN CONCURRENT").is_err() {
                continue;
            }
            let moved = conn
                .execute(format!(
                    "UPDATE accounts SET bal = bal - {amt} WHERE id = {a}"
                ))
                .is_ok()
                && conn
                    .execute(format!(
                        "UPDATE accounts SET bal = bal + {amt} WHERE id = {b}"
                    ))
                    .is_ok();
            if !moved {
                let _ = conn.execute("ROLLBACK");
                continue;
            }
            // Atomic commit: both updates apply or neither does, so the total is preserved.
            if conn.execute("COMMIT").is_err() {
                let _ = conn.execute("ROLLBACK");
            }
        }
    });

    writer.join().unwrap();
    stop.store(true, Ordering::Release);
    reader.join().unwrap();

    // Final exact content check on a fresh snapshot: every id present once, total preserved.
    let check = db.connect();
    let rows = get_rows(&check, "SELECT id, bal FROM accounts ORDER BY id");
    assert_eq!(rows.len() as i64, N, "final row count");
    let mut total = 0i64;
    for (i, r) in rows.iter().enumerate() {
        assert_eq!(
            as_i64(&r[0]),
            i as i64,
            "id {i} must be present exactly once, in order"
        );
        total += as_i64(&r[1]);
    }
    assert_eq!(total, TOTAL, "final total balance must be unchanged");
}

#[test]
fn test_reader_does_not_see_inflight_index_tombstone() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let c1 = db.connect();
    c1.execute("CREATE TABLE t(pk NUMERIC PRIMARY KEY, v NUMERIC UNIQUE)")
        .unwrap();
    c1.execute("INSERT INTO t VALUES (1, 719)").unwrap();
    c1.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap(); // 719 btree-resident

    let c2 = db.connect();
    // c2 updates the indexed column but does NOT commit (in-flight tombstone of 719).
    c2.execute("BEGIN CONCURRENT").unwrap();
    c2.execute("UPDATE t SET v = 743 WHERE pk = 1").unwrap();

    // c1 reads in its own snapshot (auto-commit) — must still see v=719 via the index.
    let via_idx_719 = get_rows(&c1, "SELECT pk FROM t WHERE v = 719");
    assert_eq!(
        via_idx_719.len(),
        1,
        "concurrent reader must still see v=719 via the index while c2's UPDATE is in flight: {via_idx_719:?}"
    );
    let integ = get_rows(&c1, "PRAGMA integrity_check");
    assert_eq!(&integ[0][0].to_string(), "ok", "integrity: {integ:?}");

    // c2 aborts; 719 must remain.
    c2.execute("ROLLBACK").unwrap();
    let after = get_rows(&c1, "SELECT pk FROM t WHERE v = 719");
    assert_eq!(after.len(), 1, "v=719 must survive c2 rollback: {after:?}");
}

/// An UPDATE of an indexed UNIQUE column inside a tx that cleanly ROLLs BACK must not
/// leave the pre-update index entry tombstoned (regression guard; this path is correct).
#[test]
fn test_rollback_of_indexed_update_keeps_btree_resident_index_entry() {
    // Repro for the turso_stress "row missing from index" bug: an UPDATE of an indexed
    // UNIQUE column inside a tx that ROLLS BACK must not leave the pre-update index entry
    // tombstoned — especially when it's already btree-resident (the UPDATE then creates a
    // synthetic tombstone over the btree entry).
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(pk NUMERIC PRIMARY KEY, v NUMERIC UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 719)").unwrap();
    // Make value 719's index entry btree-resident (and drop MVCC-store versions).
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Update the indexed column, then abort the transaction.
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("UPDATE t SET v = 743 WHERE pk = 1").unwrap();
    conn.execute("ROLLBACK").unwrap();

    // The row's original indexed value (719) must still be reachable via the index,
    // and 743 (never committed) must not be.
    let via_idx_719 = get_rows(&conn, "SELECT pk FROM t WHERE v = 719");
    assert_eq!(
        via_idx_719.len(),
        1,
        "row must remain in autoindex under v=719 after rollback: {via_idx_719:?}"
    );
    let via_idx_743 = get_rows(&conn, "SELECT pk FROM t WHERE v = 743");
    assert!(
        via_idx_743.is_empty(),
        "aborted UPDATE's v=743 must not be in the index: {via_idx_743:?}"
    );
    let integ = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(&integ[0][0].to_string(), "ok", "integrity: {integ:?}");
}

#[test]
fn test_conflict_abort_of_indexed_update_keeps_btree_resident_index_entry() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let c1 = db.connect();
    c1.execute("CREATE TABLE t(pk NUMERIC PRIMARY KEY, v NUMERIC UNIQUE)")
        .unwrap();
    c1.execute("INSERT INTO t VALUES (1, 719)").unwrap();
    c1.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap(); // 719 btree-resident

    let c2 = db.connect();

    // c1 updates the indexed column of pk=1 to 743 (tombstones the btree-resident 719
    // entry, stages a 743 entry) — but does not commit yet.
    c1.execute("BEGIN CONCURRENT").unwrap();
    c1.execute("UPDATE t SET v = 743 WHERE pk = 1").unwrap();

    // c2 commits the SAME index key (743) on a different row, so c1's commit must
    // write-write-conflict on the unique index and abort.
    c2.execute("BEGIN CONCURRENT").unwrap();
    c2.execute("INSERT INTO t VALUES (2, 743)").unwrap();
    c2.execute("COMMIT").unwrap();

    let c1_commit = c1.execute("COMMIT");
    assert!(
        c1_commit.is_err(),
        "c1 commit should write-write conflict on index key 743, got {c1_commit:?}"
    );

    // pk=1 is unchanged (719) and must still be reachable via the index; pk=2 has 743.
    let via_idx_719 = get_rows(&c1, "SELECT pk FROM t WHERE v = 719");
    assert_eq!(
        via_idx_719.len(),
        1,
        "pk=1 must remain in autoindex under v=719 after c1's conflict-abort: {via_idx_719:?}"
    );
    let integ = get_rows(&c1, "PRAGMA integrity_check");
    assert_eq!(&integ[0][0].to_string(), "ok", "integrity: {integ:?}");
}

#[test]
fn test_passive_checkpoint_tolerates_concurrent_create_after_snapshot() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let conn = db.connect();
    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t1 VALUES (0, 'seed')").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    // Force an auto-checkpoint on the next commit.
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();

    // Drive the auto-checkpoint via this INSERT's commit and park it at
    // BeforeAcquireLock (snapshot_ts captured in PrepareCheckpoint; blocking lock
    // not yet held, so a concurrent writer can still commit).
    let injector = FixedYieldInjector::new([CheckpointYieldPoint::BeforeAcquireLock.point()]);
    conn.set_yield_injector(Some(injector.clone()));
    let mut insert_stmt = conn.prepare("INSERT INTO t1 VALUES (1, 'a')").unwrap();
    let mut parked = false;
    for _ in 0..10_000 {
        match insert_stmt.step().unwrap() {
            StepResult::IO | StepResult::Yield if injector.is_empty() => {
                parked = true;
                break;
            }
            StepResult::IO | StepResult::Yield => {}
            StepResult::Done => {
                panic!("INSERT completed before the checkpoint acquire-lock yield fired")
            }
            other => panic!("unexpected INSERT step result before yield: {other:?}"),
        }
    }
    assert!(
        parked,
        "auto-checkpoint should yield before acquiring the checkpoint lock"
    );
    conn.set_yield_injector(None);

    // Concurrent connection creates a table that commits AFTER the checkpoint's
    // snapshot. It lands in the shared schema with a negative root page but is NOT
    // part of this checkpoint's write set; the in-progress gate stops its own commit
    // from checkpointing it.
    let other = db.connect();
    other
        .execute("CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    // Resume the parked checkpoint to completion. Before the fix this panics at the
    // has_pending_root_publication assert in TruncateWal.
    let mut done = false;
    for _ in 0..100_000 {
        match insert_stmt.step().unwrap() {
            StepResult::Done => {
                done = true;
                break;
            }
            StepResult::IO | StepResult::Yield => {}
            other => panic!("unexpected resume step result: {other:?}"),
        }
    }
    assert!(
        done,
        "checkpoint must complete despite a CREATE that committed after its snapshot"
    );
    drop(insert_stmt);

    // Both tables survive; t2 is usable; integrity holds.
    let tables = get_rows(
        &conn,
        "SELECT name FROM sqlite_schema WHERE type='table' AND name IN ('t1','t2') ORDER BY name",
    );
    assert_eq!(tables.len(), 2, "t1 and t2 must both exist: {tables:?}");
    other.execute("INSERT INTO t2 VALUES (1, 'x')").unwrap();
    let rows = get_rows(&other, "SELECT id, v FROM t2");
    assert_eq!(rows.len(), 1);
    let integrity = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(&integrity[0][0].to_string(), "ok");
}

/// What this test checks: if one checkpoint makes a unique-index delete durable in the B-tree
/// but fails before MVCC cleanup finishes, a later checkpoint retry must not try to delete that
/// same unique key again.
///
/// Steps:
/// 1. Disable automatic checkpoints.
/// 2. Insert `(75, 'blue_river_906')`.
/// 3. Checkpoint it so the blue key is durable in the B-tree.
/// 4. Start and roll back a concurrent delete to leave stale MVCC state behind.
/// 5. Update row `75` to `old_path_352`.
/// 6. Run a checkpoint that commits pager changes and then fails after advancing the durable boundary.
/// 7. Update row `75` again to `empty_path_27`, then to `shy_cloud_434`.
/// 8. Retry checkpoint. Before the fix, this retried the old blue-key delete and hit
///    `Corrupt("MVCC delete ... not found")`.
#[test]
fn test_checkpoint_retry_does_not_replay_checkpointed_btree_resident_unique_delete() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX idx_t_v ON t(v)").unwrap();
    conn.execute("INSERT INTO t VALUES (75, 'blue_river_906')")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("DELETE FROM t WHERE id = 75").unwrap();
    conn.execute("ROLLBACK").unwrap();
    conn.execute("UPDATE t SET v = 'old_path_352' WHERE id = 75")
        .unwrap();

    let rows = get_rows(&conn, "SELECT id FROM t WHERE v = 'blue_river_906'");
    assert!(
        rows.is_empty(),
        "old unique key should no longer be visible"
    );
    let rows = get_rows(&conn, "SELECT id FROM t WHERE v = 'old_path_352'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 75);

    let ckpt = db.connect();
    ckpt.set_failure_injector(Some(FixedFailureInjector::new([(
        CheckpointYieldPoint::AfterDurableBoundaryAdvanced.point(),
        LimboError::TxError("synthetic checkpoint failure after pager commit".to_string()),
    )])));
    let err = ckpt
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .expect_err("checkpoint should fail");
    assert!(
        matches!(err, LimboError::TxError(_)),
        "expected injected checkpoint failure, got: {err:?}"
    );

    conn.execute("UPDATE t SET v = 'empty_path_27' WHERE id = 75")
        .unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("UPDATE t SET v = 'shy_cloud_434' WHERE id = 75")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    let rows = get_rows(&conn, "SELECT id FROM t WHERE v = 'empty_path_27'");
    assert!(
        rows.is_empty(),
        "intermediate unique key should not remain visible"
    );
    let rows = get_rows(&conn, "SELECT id FROM t WHERE v = 'shy_cloud_434'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 75);

    let retry_conn = db.connect();
    retry_conn
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .expect("retry checkpoint should not replay the already-durable blue delete");

    let rows = get_rows(&conn, "SELECT id, v FROM t WHERE id = 75");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 75);
    assert_eq!(rows[0][1].cast_text().unwrap(), "shy_cloud_434");

    let rows = get_rows(&conn, "SELECT id FROM t WHERE v = 'blue_river_906'");
    assert!(
        rows.is_empty(),
        "blue key should stay absent after checkpoint retry"
    );
    let rows = get_rows(&conn, "SELECT id FROM t WHERE v = 'old_path_352'");
    assert!(
        rows.is_empty(),
        "old_path key should stay absent after checkpoint retry"
    );
    let rows = get_rows(&conn, "SELECT id FROM t WHERE v = 'empty_path_27'");
    assert!(
        rows.is_empty(),
        "empty_path key should stay absent after checkpoint retry"
    );
    let rows = get_rows(&conn, "SELECT id FROM t WHERE v = 'shy_cloud_434'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 75);

    let integrity = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(integrity.len(), 1);
    assert_eq!(&integrity[0][0].to_string(), "ok");
}

/// What this test checks: user-facing SQL plus a commit yield can produce out-of-order commit completion without lowering checkpoint metadata.
#[test]
fn test_checkpoint_stale_unique_index_delete_with_out_of_order_commit_yield() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT UNIQUE)")
        .unwrap();
    conn.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO s VALUES (1, 'first')").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'first')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'second')").unwrap();
    conn.execute("INSERT INTO t VALUES (75, 'blue_river_906')")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let older = db.connect();
    older.execute("BEGIN CONCURRENT").unwrap();
    older
        .execute("UPDATE s SET v = 'older_commit' WHERE id = 1")
        .unwrap();
    older.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));
    let mut older_commit = older.prepare("COMMIT").unwrap();
    assert!(
        matches!(older_commit.step().unwrap(), StepResult::Yield),
        "older commit should yield after taking its commit timestamp"
    );

    let updater = db.connect();
    updater.execute("BEGIN CONCURRENT").unwrap();
    updater
        .execute("UPDATE t SET v = 'old_path_352' WHERE id = 75")
        .unwrap();
    updater.execute("COMMIT").unwrap();

    older_commit.run_ignore_rows().unwrap();
    drop(older_commit);

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "SELECT id, v FROM t WHERE id = 75");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 75);
    assert_eq!(&rows[0][1].to_string(), "old_path_352");
}

/// What this test checks: SQL-only recovery must not replay a CREATE TABLE frame already made durable by checkpoint.
/// Why this matters: a regressed checkpoint boundary can make recovery replay the pre-checkpoint schema row with its negative root page after WAL recovery has already installed the positive root page.
///
/// Steps:
/// 1. Disable automatic checkpoints and create a baseline table.
/// 2. Checkpoint the baseline state so the durable boundary is non-zero.
/// 3. Start an older concurrent transaction and yield it after it commits, releases the
///    commit lock, and is just about to update the committed timestamp watermark.
/// 4. Commit a newer `CREATE TABLE` plus row insert through ordinary SQL.
/// 5. Resume the older transaction; without a monotonic committed watermark this regresses
///    the checkpoint boundary source.
/// 6. Run a checkpoint that fails after pager commit, leaving WAL recovery to install the
///    checkpointed schema row with its positive root page.
/// 7. Restart and query the created table; recovery must not also replay the stale logical-log
///    `CREATE TABLE` frame whose schema row still has the negative MVCC root page.
#[test]
fn test_checkpoint_stale_boundary_does_not_replay_checkpointed_create_table_after_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        conn.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO s VALUES (1, 'first')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        let older = db.connect();
        older.execute("BEGIN CONCURRENT").unwrap();
        older
            .execute("UPDATE s SET v = 'older_commit' WHERE id = 1")
            .unwrap();
        older.set_yield_injector(Some(FixedYieldInjector::new([
            CommitYieldPoint::BeforeGlobalHeaderUpdate.point(),
        ])));
        let mut older_commit = older.prepare("COMMIT").unwrap();
        assert!(
            matches!(older_commit.step().unwrap(), StepResult::Yield),
            "older commit should yield before updating the committed timestamp watermark"
        );

        let creator = db.connect();
        creator
            .execute("CREATE TABLE created_after_yield (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        creator
            .execute("INSERT INTO created_after_yield VALUES (1, 'persisted')")
            .unwrap();

        older_commit.run_ignore_rows().unwrap();
        drop(older_commit);

        conn.set_failure_injector(Some(FixedFailureInjector::new([(
            CheckpointYieldPoint::AfterDurableBoundaryAdvanced.point(),
            LimboError::TxError("synthetic checkpoint failure after pager commit".to_string()),
        )])));
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .expect_err("checkpoint should fail after pager commit");
        conn.set_failure_injector(None);
    };

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM created_after_yield ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(&rows[0][1].to_string(), "persisted");

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let rows = get_rows(&conn, "SELECT id, v FROM created_after_yield ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(&rows[0][1].to_string(), "persisted");
}

#[test]
fn test_checkpoint_post_durable_failure_then_unique_update_removes_stale_autoindex_entry() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        [
            "PRAGMA mvcc_checkpoint_threshold = -1",
            "CREATE TABLE t(a UNIQUE, b, c PRIMARY KEY)",
            "INSERT INTO t VALUES ('old', 1.0, 1)",
            "PRAGMA wal_checkpoint(TRUNCATE)",
            "UPDATE t SET a = 'mid', b = 2.0 WHERE c = 1",
        ]
        .iter()
        .for_each(|sql| conn.execute(sql).unwrap());

        let checkpoint_conn = db.connect();
        let failure_injector = FixedFailureInjector::new([(
            CheckpointYieldPoint::AfterDurableBoundaryAdvanced.point(),
            LimboError::TxError("synthetic checkpoint failure after pager commit".to_string()),
        )]);
        checkpoint_conn.set_failure_injector(Some(failure_injector));
        checkpoint_conn
            .execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .expect_err("checkpoint should fail after pager commit");

        conn.execute("UPDATE t SET a = 'new', b = 3.0 WHERE c = 1")
            .unwrap();
        assert_integrity_ok(&conn);
    }

    db.restart();
    let conn = db.connect();
    assert_integrity_ok(&conn);

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(
        &conn,
        "SELECT rowid, a, b, c FROM t INDEXED BY sqlite_autoindex_t_1 WHERE c = 1",
    );
    assert_eq!(
        rows.len(),
        1,
        "stale autoindex entries after checkpoint: {rows:?}"
    );
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "new");
    assert_eq!(rows[0][3].as_int().unwrap(), 1);
    assert_integrity_ok(&conn);
}

#[test]
fn test_checkpoint_post_durable_failure_then_delete_removes_stale_table_row() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'keep')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        conn.execute("INSERT INTO t VALUES (2, 'doomed')").unwrap();

        let checkpoint_conn = db.connect();
        let failure_injector = FixedFailureInjector::new([(
            CheckpointYieldPoint::AfterDurableBoundaryAdvanced.point(),
            LimboError::TxError("synthetic checkpoint failure after pager commit".to_string()),
        )]);
        checkpoint_conn.set_failure_injector(Some(failure_injector));
        checkpoint_conn
            .execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .expect_err("checkpoint should fail after pager commit");
        checkpoint_conn.set_failure_injector(None);

        conn.execute("DELETE FROM t WHERE id = 2").unwrap();
        assert_integrity_ok(&conn);
    }

    db.restart();
    let conn = db.connect();
    assert_integrity_ok(&conn);

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1, "stale table rows after checkpoint: {rows:?}");
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "keep");
    assert_integrity_ok(&conn);
}

/// Replay gate uses metadata boundary and never applies frames at or below it.
#[test]
#[cfg_attr(
    feature = "checksum",
    ignore = "byte-level tamper caught by checksum layer"
)]
fn test_meta_recovery_case_12_replay_gate_skips_at_or_below_metadata_boundary() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let boundary = {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        let root_page = metadata_root_page(&conn);
        conn.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
        let ts3 = db
            .get_mvcc_store()
            .last_committed_tx_ts
            .load(Ordering::SeqCst);
        drop(conn);
        force_close_for_artifact_tamper(&mut db);
        tamper_db_metadata_row_value_by_key(
            &db_path,
            root_page,
            MVCC_META_KEY_PERSISTENT_TX_TS_MAX,
            ts3 as i64,
        );
        ts3
    };

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[1][0].as_int().unwrap(), 2);

    let meta = get_rows(
        &conn,
        "SELECT v FROM __turso_internal_mvcc_meta WHERE k = 'persistent_tx_ts_max'",
    );
    assert_eq!(meta.len(), 1);
    assert_eq!(meta[0][0].as_int().unwrap() as u64, boundary);
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_mvcc_memory_keeps_builtin_table_valued_functions() {
    let db = MvccTestDb::new();
    let rows = get_rows(&db.conn, "SELECT value FROM generate_series(1,3)");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[2][0].as_int().unwrap(), 3);
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_insert_read() {
    let db = MvccTestDb::new();

    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    let tx2 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_read_nonexistent() {
    let db = MvccTestDb::new();
    let tx = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = db.mvcc_store.read(
        tx,
        &RowID {
            table_id: (-2).into(),
            row_id: RowKey::Int(1),
        },
    );
    assert!(row.unwrap().is_none());
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_delete() {
    let db = MvccTestDb::new();

    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    db.mvcc_store
        .delete(
            tx1,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert!(row.is_none());
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    let tx2 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert!(row.is_none());
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_delete_nonexistent() {
    let db = MvccTestDb::new();
    let tx = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    assert!(!db
        .mvcc_store
        .delete(
            tx,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1)
            },
        )
        .unwrap());
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_commit() {
    let db = MvccTestDb::new();
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    let tx1_updated_row = generate_simple_string_row((-2).into(), 1, "World");
    db.mvcc_store.update(tx1, tx1_updated_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_updated_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    let tx2 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx2).unwrap();
    assert_eq!(tx1_updated_row, row);
    db.mvcc_store.drop_unused_row_versions();
}

/// What this test checks: Rollback/savepoint behavior restores exactly the intended state when statements or transactions fail.
/// Why this matters: Partial rollback mistakes leave data in impossible intermediate states.
#[test]
fn test_rollback() {
    let db = MvccTestDb::new();
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row1 = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, row1.clone()).unwrap();
    let row2 = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(row1, row2);
    let row3 = generate_simple_string_row((-2).into(), 1, "World");
    db.mvcc_store.update(tx1, row3.clone()).unwrap();
    let row4 = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(row3, row4);
    db.mvcc_store.rollback_tx(
        tx1,
        db.conn.pager.load().clone(),
        &db.conn,
        crate::MAIN_DB_ID,
    );
    let tx2 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row5 = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert_eq!(row5, None);
}

/// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
/// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
#[test]
fn test_dirty_write() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1, but does not commit.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);

    let conn2 = db.db.connect().unwrap();
    // T2 attempts to delete row with ID 1, but fails because T1 has not committed.
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let tx2_row = generate_simple_string_row((-2).into(), 1, "World");
    assert!(!db.mvcc_store.update(tx2, tx2_row).unwrap());

    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

/// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
/// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
#[test]
fn test_dirty_read() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1, but does not commit.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row1 = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, row1).unwrap();

    // T2 attempts to read row with ID 1, but doesn't see one because T1 has not committed.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let row2 = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert_eq!(row2, None);
}

/// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
/// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
#[test]
fn test_dirty_read_deleted() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 deletes row with ID 1, but does not commit.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    assert!(db
        .mvcc_store
        .delete(
            tx2,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1)
            },
        )
        .unwrap());

    // T3 reads row with ID 1, but doesn't see the delete because T2 hasn't committed.
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.load().clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx3,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_fuzzy_read() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "First");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 reads the row with ID 1 within an active transaction.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);

    // T3 updates the row and commits.
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.load().clone()).unwrap();
    let tx3_row = generate_simple_string_row((-2).into(), 1, "Second");
    db.mvcc_store.update(tx3, tx3_row).unwrap();
    commit_tx(db.mvcc_store.clone(), &conn3, tx3).unwrap();

    // T2 still reads the same version of the row as before.
    let row = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);

    // T2 tries to update the row, but fails because T3 has already committed an update to the row,
    // so T2 trying to write would violate snapshot isolation if it succeeded.
    let tx2_newrow = generate_simple_string_row((-2).into(), 1, "Third");
    let update_result = db.mvcc_store.update(tx2, tx2_newrow);
    assert!(matches!(update_result, Err(LimboError::WriteWriteConflict)));
}

/// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
/// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
#[test]
fn test_lost_update() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 attempts to update row ID 1 within an active transaction.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let tx2_row = generate_simple_string_row((-2).into(), 1, "World");
    assert!(db.mvcc_store.update(tx2, tx2_row.clone()).unwrap());

    // T3 also attempts to update row ID 1 within an active transaction.
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.load().clone()).unwrap();
    let tx3_row = generate_simple_string_row((-2).into(), 1, "Hello, world!");
    assert!(matches!(
        db.mvcc_store.update(tx3, tx3_row),
        Err(LimboError::WriteWriteConflict)
    ));
    // hack: in the actual tursodb database we rollback the mvcc tx ourselves, so manually roll it back here
    db.mvcc_store
        .rollback_tx(tx3, conn3.pager.load().clone(), &conn3, crate::MAIN_DB_ID);

    commit_tx(db.mvcc_store.clone(), &conn2, tx2).unwrap();
    assert!(matches!(
        commit_tx(db.mvcc_store.clone(), &conn3, tx3),
        Err(LimboError::TxTerminated)
    ));

    let conn4 = db.db.connect().unwrap();
    let tx4 = db.mvcc_store.begin_tx(conn4.pager.load().clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx4,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx2_row, row);
}

// Test for the visibility to check if a new transaction can see old committed values.
// This test checks for the typo present in the paper, explained in https://github.com/penberg/mvcc-rs/issues/15
#[test]
fn test_committed_visibility() {
    let db = MvccTestDb::new();

    // let's add $10 to my account since I like money
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "10");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // but I like more money, so let me try adding $10 more
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let tx2_row = generate_simple_string_row((-2).into(), 1, "20");
    assert!(db.mvcc_store.update(tx2, tx2_row.clone()).unwrap());
    let row = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(row, tx2_row);

    // can I check how much money I have?
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.load().clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx3,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

// Test to check if a older transaction can see (un)committed future rows
#[test]
fn test_future_row() {
    let db = MvccTestDb::new();

    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();

    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let tx2_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx2, tx2_row).unwrap();

    // transaction in progress, so tx1 shouldn't be able to see the value
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert_eq!(row, None);

    // lets commit the transaction and check if tx1 can see it
    commit_tx(db.mvcc_store.clone(), &conn2, tx2).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert_eq!(row, None);
}

use crate::mvcc::cursor::MvccLazyCursor;
use crate::mvcc::database::CommitYieldPoint::LogRecordPrepared;
use crate::mvcc::database::{MvStore, Row, RowID};
use crate::schema::IndexColumn;
use crate::types::Text;
use crate::Value;
use crate::{Database, StepResult};
use crate::{MemoryIO, Statement};
use crate::{ValueRef, DATABASE_MANAGER};
// Simple atomic clock implementation for testing

fn setup_test_db() -> (MvccTestDb, u64, MVTableId, i64) {
    let db = MvccTestDb::new();
    db.conn
        .execute("CREATE TABLE mvcc_lazy_gap_test(x INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    let root_page = get_rows(
        &db.conn,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 'mvcc_lazy_gap_test'",
    )[0][0]
        .as_int()
        .unwrap();
    let table_id = db.mvcc_store.get_table_id_from_root_page(root_page);
    let btree_root_page = root_page.abs();

    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();

    let test_rows = [
        (5, "row5"),
        (10, "row10"),
        (15, "row15"),
        (20, "row20"),
        (30, "row30"),
    ];

    for (row_id, data) in test_rows.iter() {
        let id = RowID::new(table_id, RowKey::Int(*row_id));
        let record =
            ImmutableRecord::from_values(&[Value::Text(Text::new(data.to_string()))], 1).unwrap();
        let row = Row::new_table_row(id, record.as_blob(), 1).unwrap();
        db.mvcc_store.insert(tx_id, row).unwrap();
    }

    commit_tx(db.mvcc_store.clone(), &db.conn, tx_id).unwrap();

    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    (db, tx_id, table_id, btree_root_page)
}

fn setup_lazy_db(initial_keys: &[i64]) -> (MvccTestDb, u64, MVTableId, i64) {
    let db = MvccTestDb::new();
    db.conn
        .execute("CREATE TABLE mvcc_lazy_basic_test(x INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    let root_page = get_rows(
        &db.conn,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 'mvcc_lazy_basic_test'",
    )[0][0]
        .as_int()
        .unwrap();
    let table_id = db.mvcc_store.get_table_id_from_root_page(root_page);
    let btree_root_page = root_page.abs();

    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();

    for i in initial_keys {
        let id = RowID::new(table_id, RowKey::Int(*i));
        let data = format!("row{i}");
        let record = ImmutableRecord::from_values(&[Value::Text(Text::new(data))], 1).unwrap();
        let row = Row::new_table_row(id, record.as_blob(), 1).unwrap();
        db.mvcc_store.insert(tx_id, row).unwrap();
    }

    commit_tx(db.mvcc_store.clone(), &db.conn, tx_id).unwrap();

    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    (db, tx_id, table_id, btree_root_page)
}

#[test]
fn test_mvcc_cursor_next_yields_with_injected_yield() {
    let db = MvccTestDb::new();
    db.conn
        .execute("CREATE TABLE cursor_yield_test(x INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    let root_page = get_rows(
        &db.conn,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 'cursor_yield_test'",
    )[0][0]
        .as_int()
        .unwrap();
    let table_id = db.mvcc_store.get_table_id_from_root_page(root_page);
    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    db.conn.set_yield_injector(Some(FixedYieldInjector::new([
        CursorYieldPoint::NextStart.point()
    ])));

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        &db.conn,
        tx_id,
        i64::from(table_id),
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(
            db.conn.pager.load().clone(),
            root_page.abs(),
            1,
        )),
    )
    .unwrap();

    let saw_yield = matches!(
        cursor.next().unwrap(),
        IOResult::IO(io) if io.is_explicit_yield()
    );
    db.mvcc_store
        .rollback_tx(tx_id, db.conn.pager.load().clone(), db.conn.as_ref(), 0);

    assert!(
        saw_yield,
        "MVCC cursor should inject an explicit yield on the first next() transition",
    );
}

pub(crate) fn commit_tx(
    mv_store: Arc<crate::MvStore>,
    conn: &Arc<Connection>,
    tx_id: u64,
) -> Result<()> {
    let mut sm = mv_store.commit_tx(tx_id, conn, crate::MAIN_DB_ID).unwrap();
    // TODO: sync IO hack
    loop {
        let res = sm.step(&mv_store)?;
        match res {
            IOResult::IO(io) => {
                io.wait(conn.db.io.as_ref())?;
            }
            IOResult::Done(_) => break,
        }
    }
    assert!(sm.is_finalized());
    Ok(())
}

pub(crate) fn commit_tx_no_conn(
    db: &MvccTestDbNoConn,
    tx_id: u64,
    conn: &Arc<Connection>,
) -> Result<(), LimboError> {
    let mv_store = db.get_mvcc_store();
    let mut sm = mv_store.commit_tx(tx_id, conn, crate::MAIN_DB_ID).unwrap();
    // TODO: sync IO hack
    loop {
        let res = sm.step(&mv_store)?;
        match res {
            IOResult::IO(io) => {
                io.wait(conn.db.io.as_ref())?;
            }
            IOResult::Done(_) => break,
        }
    }
    assert!(sm.is_finalized());
    Ok(())
}

#[test]
fn test_sequence_watermark_tracks_lowest_active_allocation() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    let mv_store = db.get_mvcc_store();
    let pager = conn.pager.load().clone();

    mv_store.set_sequence_watermark("turso_cdc_pk_autoincrement", 13);
    let tx1 = mv_store.begin_tx(pager.clone()).unwrap();
    let tx2 = mv_store.begin_tx(pager.clone()).unwrap();
    mv_store
        .register_sequence_allocation(tx1, "turso_cdc_pk_autoincrement", 10)
        .unwrap();
    mv_store
        .register_sequence_allocation(tx2, "turso_cdc_pk_autoincrement", 12)
        .unwrap();
    mv_store
        .register_sequence_allocation(tx1, "turso_cdc_pk_autoincrement", 11)
        .unwrap();

    assert_eq!(
        mv_store.sequence_watermark("turso_cdc_pk_autoincrement"),
        Some(10)
    );

    commit_tx(mv_store.clone(), &conn, tx1).unwrap();
    assert_eq!(
        mv_store.sequence_watermark("turso_cdc_pk_autoincrement"),
        Some(12)
    );

    mv_store.rollback_tx(tx2, pager, conn.as_ref(), crate::MAIN_DB_ID);
    assert_eq!(
        mv_store.sequence_watermark("turso_cdc_pk_autoincrement"),
        Some(13)
    );
}

#[test]
fn test_sequence_watermark_function_returns_current_watermark_without_active_allocations() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    let mv_store = db.get_mvcc_store();
    let pager = conn.pager.load().clone();

    conn.execute("CREATE SEQUENCE s").unwrap();
    mv_store.set_sequence_watermark("s", 42);
    let rows = get_rows(&conn, "SELECT sequence_watermark_experimental('s')");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 42);

    let tx_id = mv_store.begin_tx(pager.clone()).unwrap();
    mv_store
        .register_sequence_allocation(tx_id, "s", 10)
        .unwrap();
    let rows = get_rows(&conn, "SELECT sequence_watermark_experimental('s')");

    assert_eq!(rows[0][0].as_int().unwrap(), 10);

    mv_store.rollback_tx(tx_id, pager, conn.as_ref(), crate::MAIN_DB_ID);
    let rows = get_rows(&conn, "SELECT sequence_watermark_experimental('s')");

    assert_eq!(rows[0][0].as_int().unwrap(), 42);
}

#[test]
fn test_sequence_watermark_tracks_nextval_allocations() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup.execute("CREATE SEQUENCE s START WITH 1").unwrap();

    let rows = get_rows(&setup, "SELECT sequence_watermark_experimental('s')");
    assert!(matches!(rows[0][0], Value::Null));

    let rows = get_rows(&setup, "SELECT nextval('s')");
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    let rows = get_rows(&setup, "SELECT sequence_watermark_experimental('s')");
    assert_eq!(rows[0][0].as_int().unwrap(), 2);

    let writer = db.connect();
    writer.execute("BEGIN CONCURRENT").unwrap();
    let rows = get_rows(&writer, "SELECT nextval('s')");
    assert_eq!(rows[0][0].as_int().unwrap(), 2);

    let observer = db.connect();
    let rows = get_rows(&observer, "SELECT sequence_watermark_experimental('s')");
    assert_eq!(rows[0][0].as_int().unwrap(), 2);

    writer.execute("COMMIT").unwrap();
    let rows = get_rows(&observer, "SELECT sequence_watermark_experimental('s')");
    assert_eq!(rows[0][0].as_int().unwrap(), 3);
}

/// What this test checks: a sync-style cursor that bounds its scan by
/// `sequence_watermark_experimental()` (read *outside* a transaction, as the
/// contract requires) never skips a committed row, even while many writers
/// concurrently allocate sequence ids in overlapping `BEGIN CONCURRENT`
/// transactions and commit/abort them at random.
///
/// Why this matters: this is the exact hazard the watermark exists to prevent.
/// A writer can allocate a low sequence id, stay open, and commit *after*
/// another transaction publishes a higher id. A cursor that only advances on
/// `id > last_seen` would step over the lower id and lose it forever. The
/// watermark is the first id that is *not* safe to pass, so a reader that
/// claims "everything below the watermark is final" must be correct: every
/// committed row whose id is below the highest watermark the reader ever
/// passed must have been observed by the reader.
///
/// The reader collects ids it sees and tracks `last` = the highest
/// `watermark - 1` it has advanced to. After the workload drains, we read the
/// authoritative committed set and assert no committed id `<= last` is missing
/// from what the reader collected — i.e. the watermark never let the reader
/// advance past a row it had not yet seen.
#[test]
fn test_sequence_watermark_reader_never_skips_committed_rows_fuzz() {
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let setup = db.connect();
        setup
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, who INTEGER)")
            .unwrap();
        setup.execute("CREATE SEQUENCE s START WITH 1").unwrap();
        setup.close().unwrap();
    }

    const N_WRITERS: u64 = 4;
    const INSERTS_PER_WRITER: u64 = 25;
    // Per-row commit retry budget: the only contention is the autonomous inner
    // tx that advances the sequence backing table, which is transient, so a
    // generous budget lets every row eventually commit without the test
    // wedging.
    const COMMIT_ATTEMPTS: u64 = 200;

    let done = Arc::new(AtomicU64::new(0));

    let mut writers = Vec::new();
    for who in 0..N_WRITERS {
        let db_arc = db.get_db();
        let done = done.clone();
        writers.push(std::thread::spawn(move || {
            let conn = db_arc.connect().unwrap();
            // Seed each writer differently so jitter (and thus the
            // commit/abort interleaving) varies between threads but stays
            // reproducible across runs.
            let mut rng = ChaCha8Rng::seed_from_u64(0x5EED_0000 + who);
            for _ in 0..INSERTS_PER_WRITER {
                for _ in 0..COMMIT_ATTEMPTS {
                    let txn = (|| -> crate::Result<bool> {
                        conn.execute("BEGIN CONCURRENT")?;
                        conn.execute(format!(
                            "INSERT INTO t(id, who) VALUES (nextval('s'), {who})"
                        ))?;
                        // Widen the window in which the row is allocated but
                        // uncommitted, so other writers publish higher ids
                        // while this low id is still in flight — the precise
                        // ordering the watermark must defend against. Roughly
                        // 1-in-6 transactions abort instead of commit, leaving
                        // sequence "holes" the reader must also tolerate.
                        std::thread::sleep(Duration::from_micros(rng.random_range(0..200)));
                        if rng.random_range(0..6) == 0 {
                            conn.execute("ROLLBACK")?;
                            Ok(false)
                        } else {
                            conn.execute("COMMIT")?;
                            Ok(true)
                        }
                    })();
                    match txn {
                        Ok(_) => break,
                        Err(_) => {
                            // Transient conflict (e.g. Busy on the sequence
                            // inner tx). Abandon this attempt's tx and retry.
                            let _ = conn.execute("ROLLBACK");
                            std::thread::sleep(Duration::from_micros(50));
                        }
                    }
                }
            }
            conn.close().unwrap();
            done.fetch_add(1, Ordering::SeqCst);
        }));
    }

    let reader = db.connect();

    // Read ids strictly below the watermark, advancing `last` to `watermark-1`.
    // Returns false only when the watermark is not yet published (NULL), so the
    // caller can tell a poll apart from a no-op.
    let poll = |last: &mut i64, collected: &mut BTreeSet<i64>| -> bool {
        let mut wm_stmt = match reader.prepare("SELECT sequence_watermark_experimental('s')") {
            Ok(s) => s,
            Err(_) => return false,
        };
        let mut watermark = None;
        if wm_stmt
            .run_with_row_callback(|row| {
                watermark = row.get_values().next().and_then(|v| v.as_int());
                Ok(())
            })
            .is_err()
        {
            return false;
        }
        let Some(watermark) = watermark else {
            return false;
        };
        // The watermark is the first *unsafe* id, so it is an exclusive upper
        // bound on what the reader may claim as final.
        let query = format!("SELECT id FROM t WHERE id > {last} AND id < {watermark} ORDER BY id");
        if let Ok(mut stmt) = reader.prepare(&query) {
            let _ = stmt.run_with_row_callback(|row| {
                if let Some(id) = row.get_values().next().and_then(|v| v.as_int()) {
                    collected.insert(id);
                }
                Ok(())
            });
        }
        let new_last = watermark - 1;
        if new_last > *last {
            *last = new_last;
        }
        true
    };

    let mut last: i64 = 0;
    let mut collected: BTreeSet<i64> = BTreeSet::new();

    // Poll concurrently with the writers.
    while done.load(Ordering::SeqCst) < N_WRITERS {
        poll(&mut last, &mut collected);
        std::thread::sleep(Duration::from_micros(100));
    }
    for writer in writers {
        writer.join().unwrap();
    }

    // Drain: with no active allocations left, the watermark is the sequence
    // boundary, so `last` advances to cover every committed row. Loop until it
    // stops moving (bounded, so a bug cannot hang the test).
    for _ in 0..1000 {
        let prev = last;
        poll(&mut last, &mut collected);
        if last == prev {
            break;
        }
    }

    // Authoritative committed set, read outside any transaction.
    let mut committed: BTreeSet<i64> = BTreeSet::new();
    {
        let mut stmt = reader.prepare("SELECT id FROM t ORDER BY id").unwrap();
        stmt.run_with_row_callback(|row| {
            if let Some(id) = row.get_values().next().and_then(|v| v.as_int()) {
                committed.insert(id);
            }
            Ok(())
        })
        .unwrap();
    }

    // Sanity: the workload actually committed rows and the reader actually
    // advanced — otherwise the invariant below is vacuous.
    assert!(
        !committed.is_empty(),
        "no rows committed; workload did not exercise the watermark"
    );
    assert!(last > 0, "reader never advanced past any watermark");

    // The invariant: every committed id the reader claimed as safe (id <= last)
    // must have actually been observed. A skipped row is a watermark failure.
    for id in &committed {
        if *id <= last {
            assert!(
                collected.contains(id),
                "watermark reader skipped committed id {id} (advanced last to {last}); \
                 collected={collected:?}"
            );
        }
    }
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[test]
fn test_lazy_scan_cursor_basic() {
    let (db, tx_id, table_id, btree_root_page) = setup_lazy_db(&[1, 2, 3, 4, 5]);

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        &db.conn,
        tx_id,
        i64::from(table_id),
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(
            db.conn.pager.load().clone(),
            btree_root_page,
            1,
        )),
    )
    .unwrap();

    // Check first row
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(cursor.has_record());
    assert!(!cursor.is_empty());
    let row = cursor.read_mvcc_current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id.to_int_or_panic(), 1);

    // Iterate through all rows
    let mut count = 1;
    loop {
        let res = cursor.next().unwrap();
        let IOResult::Done(()) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !cursor.has_record() {
            break;
        }
        count += 1;
        let row = cursor.read_mvcc_current_row().unwrap().unwrap();
        assert_eq!(row.id.row_id.to_int_or_panic(), count);
    }

    // Should have found 5 rows
    assert_eq!(count, 5);

    // After the last row, is_empty should return true
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(!cursor.has_record());
    assert!(cursor.is_empty());
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[test]
fn test_lazy_scan_cursor_with_gaps() {
    let (db, tx_id, table_id, btree_root_page) = setup_test_db();

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        &db.conn,
        tx_id,
        i64::from(table_id),
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(
            db.conn.pager.load().clone(),
            btree_root_page,
            1,
        )),
    )
    .unwrap();

    // Check first row
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(cursor.has_record());
    assert!(!cursor.is_empty());
    let row = cursor.read_mvcc_current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id.to_int_or_panic(), 5);

    // Test moving forward and checking IDs
    let expected_ids = [5, 10, 15, 20, 30];
    let mut index = 0;

    let IOResult::Done(rowid) = cursor.rowid().unwrap() else {
        unreachable!();
    };
    let rowid = rowid.unwrap();
    assert_eq!(rowid, expected_ids[index]);

    loop {
        let res = cursor.next().unwrap();
        let IOResult::Done(()) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !cursor.has_record() {
            break;
        }
        index += 1;
        if index < expected_ids.len() {
            let IOResult::Done(rowid) = cursor.rowid().unwrap() else {
                unreachable!();
            };
            let rowid = rowid.unwrap();
            assert_eq!(rowid, expected_ids[index]);
        }
    }

    // Should have found all 5 rows
    assert_eq!(index, expected_ids.len() - 1);
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[test]
fn test_cursor_basic() {
    let (db, tx_id, table_id, btree_root_page) = setup_lazy_db(&[1, 2, 3, 4, 5]);

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        &db.conn,
        tx_id,
        i64::from(table_id),
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(
            db.conn.pager.load().clone(),
            btree_root_page,
            1,
        )),
    )
    .unwrap();

    let _ = cursor.next().unwrap();

    // Check first row
    assert!(!cursor.is_empty());
    let row = cursor.read_mvcc_current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id.to_int_or_panic(), 1);

    // Iterate through all rows
    let mut count = 1;
    loop {
        let res = cursor.next().unwrap();
        let IOResult::Done(()) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !cursor.has_record() {
            break;
        }
        count += 1;
        let row = cursor.read_mvcc_current_row().unwrap().unwrap();
        assert_eq!(row.id.row_id.to_int_or_panic(), count);
    }

    // Should have found 5 rows
    assert_eq!(count, 5);

    // After the last row, is_empty should return true
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(!cursor.has_record());
    assert!(cursor.is_empty());
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[test]
fn test_cursor_with_empty_table() {
    let db = MvccTestDb::new();
    {
        // FIXME: force page 1 initialization
        let pager = db.conn.pager.load().clone();
        let tx_id = db.mvcc_store.begin_tx(pager).unwrap();
        commit_tx(db.mvcc_store.clone(), &db.conn, tx_id).unwrap();
    }
    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let table_id = -1; // Empty table

    // Test LazyScanCursor with empty table
    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        &db.conn,
        tx_id,
        table_id,
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(db.conn.pager.load().clone(), -table_id, 1)),
    )
    .unwrap();
    assert!(cursor.is_empty());
    let rowid = cursor.rowid().unwrap();
    assert!(matches!(rowid, IOResult::Done(None)));
}

/// What this test checks: Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
/// Why this matters: Read-path correctness is critical: wrong cursor semantics directly surface as wrong query answers.
#[test]
fn test_cursor_modification_during_scan() {
    let _ = tracing_subscriber::fmt::try_init();
    let (db, tx_id, table_id, btree_root_page) = setup_lazy_db(&[1, 2, 4, 5]);

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        &db.conn,
        tx_id,
        i64::from(table_id),
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(
            db.conn.pager.load().clone(),
            btree_root_page,
            1,
        )),
    )
    .unwrap();

    // Read first row
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(cursor.has_record());
    let first_row = cursor.read_mvcc_current_row().unwrap().unwrap();
    assert_eq!(first_row.id.row_id.to_int_or_panic(), 1);

    // Insert a new row with ID between existing rows
    let new_row_id = RowID::new(table_id, RowKey::Int(3));
    let new_row = generate_simple_string_record("new_row");

    let _ = cursor
        .insert(&BTreeKey::TableRowId((
            new_row_id.row_id.to_int_or_panic(),
            Some(&new_row),
        )))
        .unwrap();

    let mut read_rowids = vec![];
    loop {
        let res = cursor.next().unwrap();
        let IOResult::Done(()) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !cursor.has_record() {
            break;
        }
        read_rowids.push(
            cursor
                .read_mvcc_current_row()
                .unwrap()
                .unwrap()
                .id
                .row_id
                .to_int_or_panic(),
        );
    }
    assert_eq!(read_rowids, vec![2, 3, 4, 5]);
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(!cursor.has_record());
    assert!(cursor.is_empty());
}

/* States described in the Hekaton paper *for serializability*:

Table 1: Case analysis of action to take when version V’s
Begin field contains the ID of transaction TB
------------------------------------------------------------------------------------------------------
TB’s state   | TB’s end timestamp | Action to take when transaction T checks visibility of version V.
------------------------------------------------------------------------------------------------------
Active       | Not set            | V is visible only if TB=T and V’s end timestamp equals infinity.
------------------------------------------------------------------------------------------------------
Preparing    | TS                 | V’s begin timestamp will be TS ut V is not yet committed. Use TS
                                  | as V’s begin time when testing visibility. If the test is true,
                                  | allow T to speculatively read V. Committed TS V’s begin timestamp
                                  | will be TS and V is committed. Use TS as V’s begin time to test
                                  | visibility.
------------------------------------------------------------------------------------------------------
Committed    | TS                 | V’s begin timestamp will be TS and V is committed. Use TS as V’s
                                  | begin time to test visibility.
------------------------------------------------------------------------------------------------------
Aborted      | Irrelevant         | Ignore V; it’s a garbage version.
------------------------------------------------------------------------------------------------------
Terminated   | Irrelevant         | Reread V’s Begin field. TB has terminated so it must have finalized
or not found |                    | the timestamp.
------------------------------------------------------------------------------------------------------

Table 2: Case analysis of action to take when V's End field
contains a transaction ID TE.
------------------------------------------------------------------------------------------------------
TE’s state   | TE’s end timestamp | Action to take when transaction T checks visibility of a version V
             |                    | as of read time RT.
------------------------------------------------------------------------------------------------------
Active       | Not set            | V is visible only if TE is not T.
------------------------------------------------------------------------------------------------------
Preparing    | TS                 | V’s end timestamp will be TS provided that TE commits. If TS > RT,
                                  | V is visible to T. If TS < RT, T speculatively ignores V.
------------------------------------------------------------------------------------------------------
Committed    | TS                 | V’s end timestamp will be TS and V is committed. Use TS as V’s end
                                  | timestamp when testing visibility.
------------------------------------------------------------------------------------------------------
Aborted      | Irrelevant         | V is visible.
------------------------------------------------------------------------------------------------------
Terminated   | Irrelevant         | Reread V’s End field. TE has terminated so it must have finalized
or not found |                    | the timestamp.
*/

fn new_tx(tx_id: TxID, begin_ts: u64, state: TransactionState) -> Transaction {
    new_tx_in(tx_id, begin_ts, state)
}

fn new_tx_in<A: super::RowVersionAllocator>(
    tx_id: TxID,
    begin_ts: u64,
    state: TransactionState,
) -> Transaction<A> {
    let state = state.into();
    Transaction {
        state,
        tx_id,
        begin_ts,
        write_set: Mutex::new(WriteSet::new()),
        header: RwLock::new(DatabaseHeader::default()),
        header_dirty: AtomicBool::new(false),
        savepoint_stack: RwLock::new(Vec::new()),
        pager_commit_lock_held: AtomicBool::new(false),
        log_appended: AtomicBool::new(false),
        commit_dep_counter: AtomicU64::new(0),
        abort_now: AtomicBool::new(false),
        commit_dep_set: Mutex::new(HashSet::default()),
        holds_blocking_checkpoint_read: AtomicBool::new(false),
        schema_generation_at_begin: 0,
        read_mark: crate::mvcc::database::WalPos::ORIGIN,
    }
}

/// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
/// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
#[test]
fn test_snapshot_isolation_tx_visible1() {
    let txs: SkipMap<TxID, Transaction> = SkipMap::from_iter([
        (1, new_tx(1, 1, TransactionState::Committed(2))),
        (2, new_tx(2, 2, TransactionState::Committed(5))),
        (3, new_tx(3, 3, TransactionState::Aborted)),
        (5, new_tx(5, 5, TransactionState::Preparing(8))),
        (6, new_tx(6, 6, TransactionState::Committed(10))),
        (7, new_tx(7, 7, TransactionState::Active)),
        // tx 8 with Preparing(3): current_tx (begin_ts=4) can speculatively read
        (8, new_tx(8, 1, TransactionState::Preparing(3))),
    ]);
    let finalized_tx_states: SkipMap<TxID, TransactionState> = SkipMap::new();

    let current_tx = new_tx(4, 4, TransactionState::Preparing(7));

    let rv_visible = |begin: Option<TxTimestampOrID>, end: Option<TxTimestampOrID>| {
        let row_version = RowVersion {
            id: 0, // Dummy ID for visibility tests
            begin: crate::mvcc::database::PackedTs::pack(begin),
            end: crate::mvcc::database::PackedTs::pack(end),
            row: generate_simple_string_row((-2).into(), 1, "testme"),
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        };
        tracing::debug!("Testing visibility of {row_version:?}");
        row_version.is_visible_to(&current_tx, &txs, &finalized_tx_states)
    };

    // begin visible:   transaction committed with ts < current_tx.begin_ts
    // end visible:     inf
    assert!(rv_visible(Some(TxTimestampOrID::TxID(1)), None));

    // begin invisible: transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(2)), None));

    // begin invisible: transaction aborted
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(3)), None));

    // begin visible:   timestamp < current_tx.begin_ts
    // end invisible:   transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(1))
    ));

    // begin visible:   timestamp < current_tx.begin_ts
    // end visible:     transaction committed with ts < current_tx.begin_ts
    assert!(rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(2))
    ));

    // begin visible:   timestamp < current_tx.begin_ts
    // end visible:     transaction aborted, delete never happened (Table 2)
    assert!(rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(3))
    ));

    // begin invisible: transaction preparing with end_ts(8) > begin_ts(4)
    // Speculative read condition (begin_ts >= end_ts) is false: 4 >= 8 is false
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(5)), None));

    // begin VISIBLE via speculative read: tx 8 is Preparing(3), begin_ts(4) >= end_ts(3)
    // Hekaton Table 1: speculatively read and register commit dependency
    assert!(rv_visible(Some(TxTimestampOrID::TxID(8)), None));
    // Verify dependency was registered via register-and-report protocol
    assert_eq!(
        current_tx.commit_dep_counter.load(Ordering::Acquire),
        1,
        "speculative read should register a commit dependency"
    );

    // begin invisible: transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(6)), None));

    // begin invisible: transaction active
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(7)), None));

    // begin invisible: transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(6)), None));

    // begin invisible:   transaction active
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(7)), None));

    // begin visible:   timestamp < current_tx.begin_ts
    // end visible:     transaction preparing with TS(8) > RT(4) (Table 2)
    assert!(rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(5))
    ));

    // begin invisible: timestamp > current_tx.begin_ts
    assert!(!rv_visible(
        Some(TxTimestampOrID::Timestamp(6)),
        Some(TxTimestampOrID::TxID(6))
    ));

    // begin visible:   timestamp < current_tx.begin_ts
    // end visible:     some active transaction will eventually overwrite this version,
    //                  but that hasn't happened
    //                  (this is the https://avi.im/blag/2023/hekaton-paper-typo/ case, I believe!)
    assert!(rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(7))
    ));

    assert!(!rv_visible(None, None));
}

#[test]
fn test_visibility_uses_finalized_state_for_removed_committed_tx() {
    let txs: SkipMap<TxID, Transaction> = SkipMap::new();
    let finalized_tx_states: SkipMap<TxID, TransactionState> =
        SkipMap::from_iter([(42, TransactionState::Committed(5))]);
    let reader = new_tx(7, 10, TransactionState::Active);

    let inserted_row = RowVersion {
        id: 1,
        begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(42))),
        end: crate::mvcc::database::PackedTs::pack(None),
        row: generate_simple_string_row((-2).into(), 1, "x"),
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };
    assert!(
        inserted_row.is_visible_to(&reader, &txs, &finalized_tx_states),
        "stale begin=TxID should resolve via finalized committed state"
    );

    let deleted_row = RowVersion {
        id: 2,
        begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::Timestamp(1))),
        end: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(42))),
        row: generate_simple_string_row((-2).into(), 2, "y"),
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };
    assert!(
        !deleted_row.is_visible_to(&reader, &txs, &finalized_tx_states),
        "stale end=TxID should resolve via finalized committed state"
    );
}

#[test]
fn test_read_only_commit_does_not_cache_finalized_state() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    // Establish a clean baseline after schema/setup writes.
    mvcc_store.drop_unused_row_versions();
    let baseline = mvcc_store.finalized_tx_states.len();

    conn.execute("BEGIN CONCURRENT").unwrap();
    let _ = get_rows(&conn, "SELECT 1");
    conn.execute("COMMIT").unwrap();

    assert_eq!(
        mvcc_store.finalized_tx_states.len(),
        baseline,
        "read-only commit should not add finalized tx cache entries"
    );
}

#[test]
fn test_drop_unused_row_versions_prunes_unreferenced_finalized_tx_states() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    // Establish a clean baseline after schema/setup writes.
    mvcc_store.drop_unused_row_versions();
    let baseline = mvcc_store.finalized_tx_states.len();

    conn.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    let after_write = mvcc_store.finalized_tx_states.len();
    assert!(
        after_write > baseline,
        "write commit should add at least one finalized tx cache entry"
    );

    mvcc_store.drop_unused_row_versions();

    assert_eq!(
        mvcc_store.finalized_tx_states.len(),
        baseline,
        "GC scan should prune finalized tx cache entries with no remaining TxID references"
    );
}

/// Test Hekaton register-and-report: speculative read increments CommitDepCounter
/// and adds to CommitDepSet.
#[test]
fn test_commit_dependency_speculative_read() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Preparing(5)))]);
    let finalized_tx_states: SkipMap<TxID, TransactionState> = SkipMap::new();

    // Reader with begin_ts=10 > end_ts=5 → speculative read → dependency
    let reader = new_tx(2, 10, TransactionState::Active);

    let rv = RowVersion {
        id: 0,
        begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(1))),
        end: crate::mvcc::database::PackedTs::pack(None),
        row: generate_simple_string_row((-2).into(), 1, "test"),
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };

    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 0);

    // Speculative read: begin_ts(10) >= end_ts(5) → visible, dependency registered
    assert!(rv.is_visible_to(&reader, &txs, &finalized_tx_states));
    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 1);

    // Verify tx 1's CommitDepSet contains reader's tx_id
    let dep_set = txs.get(&1).unwrap();
    assert_eq!(
        *dep_set.value().commit_dep_set.lock(),
        HashSet::from_iter([2])
    );
}

/// Test cascade abort: when depended-on tx aborts, it sets AbortNow on dependents
/// and decrements their CommitDepCounter.
#[test]
fn test_commit_dependency_cascade_abort() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Preparing(5)))]);
    let finalized_tx_states: SkipMap<TxID, TransactionState> = SkipMap::new();

    let reader = new_tx(2, 10, TransactionState::Active);

    let rv = RowVersion {
        id: 0,
        begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(1))),
        end: crate::mvcc::database::PackedTs::pack(None),
        row: generate_simple_string_row((-2).into(), 1, "test"),
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };

    // Speculative read registers dependency
    assert!(rv.is_visible_to(&reader, &txs, &finalized_tx_states));
    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 1);
    assert!(!reader.abort_now.load(Ordering::Acquire));

    // Simulate tx 1 aborting and cascading to dependents
    let tx1 = txs.get(&1).unwrap();
    let tx1 = tx1.value();
    tx1.state.store(TransactionState::Aborted);

    // Add reader to txs so cascade can find it
    txs.insert(2, reader);

    for dep_tx_id in tx1.commit_dep_set.lock().drain() {
        if let Some(dep_tx_entry) = txs.get(&dep_tx_id) {
            let dep_tx = dep_tx_entry.value();
            dep_tx.abort_now.store(true, Ordering::Release);
            dep_tx.commit_dep_counter.fetch_sub(1, Ordering::AcqRel);
        }
    }

    let reader = txs.get(&2).unwrap();
    let reader = reader.value();
    assert!(reader.abort_now.load(Ordering::Acquire));
    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 0);
}

/// Test that registering a dependency on an already-committed tx is a no-op.
#[test]
fn test_commit_dependency_already_committed() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Committed(5)))]);

    let reader = new_tx(2, 10, TransactionState::Active);

    register_commit_dependency(&txs, &reader, 1);

    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 0);
    assert!(!reader.abort_now.load(Ordering::Acquire));
}

/// Test that registering a dependency on an already-aborted tx sets AbortNow.
#[test]
fn test_commit_dependency_already_aborted() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Aborted))]);

    let reader = new_tx(2, 10, TransactionState::Active);

    register_commit_dependency(&txs, &reader, 1);

    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 0);
    assert!(reader.abort_now.load(Ordering::Acquire));
}

/// Test speculative ignore in is_end_visible registers dependency.
#[test]
fn test_commit_dependency_speculative_ignore() {
    let txs: SkipMap<TxID, Transaction> = SkipMap::from_iter([
        (1, new_tx(1, 1, TransactionState::Committed(2))),
        (3, new_tx(3, 3, TransactionState::Preparing(5))),
    ]);
    let finalized_tx_states: SkipMap<TxID, TransactionState> = SkipMap::new();

    // Reader with begin_ts=10 > end_ts=5: will speculatively ignore (treat as deleted)
    let reader = new_tx(4, 10, TransactionState::Active);

    let rv = RowVersion {
        id: 0,
        begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::Timestamp(2))),
        end: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(3))),
        row: generate_simple_string_row((-2).into(), 1, "test"),
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };

    // is_end_visible: Preparing(5), begin_ts(10) < 5 = false → deletion visible
    // is_begin_visible: Timestamp(2), 10 >= 2 = true
    // Combined: true && false = false (row not visible because it was deleted)
    assert!(!rv.is_visible_to(&reader, &txs, &finalized_tx_states));
    assert_eq!(
        reader.commit_dep_counter.load(Ordering::Acquire),
        1,
        "speculative ignore should register a commit dependency"
    );
}

/// Regression: the forward-scan [`IndexShadowFinger`] must NOT evaluate the
/// shadow predicate (and thus must not fire its `register_commit_dependency`
/// side effect) for index versions whose keys it merely *steps over* — i.e.
/// MVCC-only keys with no matching B-tree row. The authoritative
/// `query_btree_version_is_valid` path only ever evaluates the version chain for
/// keys that exactly match a B-tree row (`index_rows.get(btree_key)`), so an
/// eager finger that resolved the shadow bit on every advance would register a
/// commit dependency on a `Preparing` writer for a row the scan never observes —
/// a spurious dependency that cascade-aborts the reader if that writer aborts.
///
/// Setup: B-tree keys 10 and 30 (no MVCC versions), plus a single MVCC-only
/// tombstone at key 20 deleted by a `Preparing` writer that the reader would
/// speculatively invalidate. A forward scan checks 10 (finger ahead → visible)
/// then 30 (finger behind → steps over key 20). Key 20 is never an exact match,
/// so no dependency may be registered.
#[test]
fn test_index_finger_no_spurious_dep_on_stepped_over_key() {
    use crate::mvcc::cursor::IndexShadowFinger;

    let db = MvccTestDb::new();
    let store = &db.mvcc_store;
    let table_id = MVTableId::from(-999_i64);

    // Single-column ascending integer index key.
    let info = std::sync::Arc::new(
        crate::types::IndexInfo::new(
            crate::alloc::vec![crate::types::KeyInfo {
                sort_order: turso_parser::ast::SortOrder::Asc,
                collation: crate::translate::collate::CollationSeq::Binary,
                nulls_order: None,
            }],
            false,
            1,
            false,
        )
        .unwrap(),
    );
    let idx_key = |v: i64| {
        let rec = crate::types::ImmutableRecord::from_values(&[Value::from_i64(v)], 1).unwrap();
        std::sync::Arc::new(
            SortableIndexKey::new_from_payload_in(&rec, info.clone(), crate::alloc::TursoAllocator)
                .unwrap(),
        )
    };

    // Reader started after the writer's prepared end_ts → speculatively
    // invalidates the writer's tombstone (the dependency-registering path).
    let reader_id: TxID = 9_000_100;
    let writer_id: TxID = 9_000_050;
    store.txs.insert(
        writer_id,
        new_tx_in::<crate::alloc::DynAllocator>(writer_id, 1, TransactionState::Preparing(40)),
    );
    store.txs.insert(
        reader_id,
        new_tx_in::<crate::alloc::DynAllocator>(reader_id, 100, TransactionState::Active),
    );

    // MVCC-only tombstone at key 20: committed insert (begin Timestamp) deleted
    // by the Preparing writer (end TxID). Not present in the B-tree.
    let key20 = idx_key(20);
    let row_id = RowID::new(table_id, RowKey::Record(key20.clone()));
    let tombstone = RowVersion {
        id: 20,
        begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::Timestamp(5))),
        end: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(writer_id))),
        row: Row::new_index_row(row_id, 1),
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };
    let mut tombstone_versions =
        <RowVersionChain<crate::alloc::DynAllocator> as crate::alloc::TursoVecInExt<
            RowVersion,
            crate::alloc::DynAllocator,
        >>::new_in(crate::alloc::DynAllocator::default());
    tombstone_versions.push(tombstone);
    store
        .get_or_create_index_rows(table_id)
        .unwrap()
        .value()
        .insert(key20, Arc::new(RwLock::new(tombstone_versions)));

    let mut finger = IndexShadowFinger::default();
    // B-tree key 10: finger seeds at the first index key >= 10 (key 20), which is
    // ahead → row visible, predicate not evaluated.
    assert!(finger.btree_row_is_valid(store, table_id, reader_id, &idx_key(10)));
    // B-tree key 30: finger (at key 20) is behind → steps over the tombstone.
    // It must advance past it WITHOUT evaluating the shadow predicate.
    assert!(finger.btree_row_is_valid(store, table_id, reader_id, &idx_key(30)));

    let reader = store.txs.get(&reader_id).unwrap();
    assert_eq!(
        reader.value().commit_dep_counter.load(Ordering::Acquire),
        0,
        "finger registered a spurious commit dependency for a key it only stepped over"
    );
    let writer = store.txs.get(&writer_id).unwrap();
    assert!(
        writer.value().commit_dep_set.lock().is_empty(),
        "writer's commit-dep set must stay empty: reader never observed the tombstoned row"
    );
}

/// Test that multiple speculative reads from the same preparing tx only
/// register one commit dependency (dedup).
#[test]
fn test_commit_dependency_multiple_reads_dedup() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Preparing(5)))]);
    let finalized_tx_states: SkipMap<TxID, TransactionState> = SkipMap::new();

    let reader = new_tx(2, 10, TransactionState::Active);

    let make_rv = |row_id: i64| RowVersion {
        id: row_id as u64,
        begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(1))),
        end: crate::mvcc::database::PackedTs::pack(None),
        row: generate_simple_string_row((-2).into(), row_id, "test"),
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };

    // Read 3 rows from the same preparing tx — dependency is deduplicated
    assert!(make_rv(1).is_visible_to(&reader, &txs, &finalized_tx_states));
    assert!(make_rv(2).is_visible_to(&reader, &txs, &finalized_tx_states));
    assert!(make_rv(3).is_visible_to(&reader, &txs, &finalized_tx_states));

    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 1);

    // tx 1's CommitDepSet has 1 entry for reader (deduplicated)
    let dep_set = txs.get(&1).unwrap();
    assert_eq!(dep_set.value().commit_dep_set.lock().len(), 1);
}

/// Hekaton §2.7 cascade abort with real connections and threads.
///
/// A Preparing writer is speculatively read by a reader on another thread.
/// When the writer aborts, the reader's COMMIT must fail with
/// CommitDependencyAborted (cascade abort via AbortNow).
///
/// Sequence:
///   1. Writer: BEGIN CONCURRENT → UPDATE (real SQL)
///   2. Writer state manually set to Preparing(end_ts)
///   3. Reader thread: BEGIN → SELECT (speculative read → dependency) → INSERT
///   4. Main thread: rollback writer → cascade abort via AbortNow
///   5. Reader COMMIT → CommitDependencyAborted
#[test]
fn test_commit_dep_threaded_abort_cascades() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();

    // Writer: real SQL operations
    let writer_conn = db.connect();
    writer_conn.execute("BEGIN CONCURRENT").unwrap();
    writer_conn
        .execute("UPDATE t SET value = 'modified' WHERE id = 1")
        .unwrap();
    let writer_tx_id = writer_conn.get_mv_tx_id().unwrap();

    // Simulate mid-commit: transition writer to Preparing.
    // end_ts comes from the global clock so the reader's begin_ts will be >=
    // end_ts, satisfying the speculative read condition (Hekaton Table 1).
    let _end_ts = mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&writer_tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });

    // Reader signals after speculative read so main thread can abort writer.
    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc.connect().unwrap();
        reader_conn.execute("BEGIN CONCURRENT").unwrap();

        // SELECT triggers speculative read: reader.begin_ts >= writer.end_ts
        // → Hekaton Table 1: visible, register commit dependency
        let mut stmt = reader_conn
            .prepare("SELECT value FROM t WHERE id = 1")
            .unwrap();
        let rows = stmt.run_collect_rows().unwrap();

        // Write so COMMIT exercises the full commit state machine path.
        reader_conn
            .execute("INSERT INTO t VALUES (2, 'reader_data')")
            .unwrap();

        // Signal: speculative read done, dependency registered
        signal_tx.send(()).unwrap();

        // COMMIT blocks in WaitForDependencies until the writer resolves.
        // Writer will abort → AbortNow set → CommitDependencyAborted.
        let commit_result = reader_conn.execute("COMMIT");
        let _ = reader_conn.close(); // cleanup (rolls back if still active)
        (rows, commit_result)
    });

    // Wait for reader to complete speculative read
    signal_rx.recv().unwrap();

    // Abort writer → cascade: sets AbortNow on reader, decrements counter
    mvcc_store.rollback_tx(
        writer_tx_id,
        writer_conn.pager.load().clone(),
        &writer_conn,
        crate::MAIN_DB_ID,
    );

    let (rows, commit_result) = reader_handle.join().unwrap();

    // Reader saw the writer's modified value via speculative read
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0].to_text().unwrap(),
        "modified",
        "reader should have speculatively read the Preparing writer's value"
    );

    // Reader's COMMIT must fail: depended-on writer aborted
    assert!(
        matches!(commit_result, Err(LimboError::CommitDependencyAborted)),
        "expected CommitDependencyAborted, got: {commit_result:?}",
    );

    // Verify database consistency
    {
        let conn = db.connect();

        // Only the initial value should remain
        let mut stmt = conn.prepare("SELECT value FROM t WHERE id = 1").unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].to_text().unwrap(), "initial");

        // Reader's INSERT must not be visible (cascade-aborted)
        let mut stmt = conn.prepare("SELECT * FROM t WHERE id = 2").unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert!(
            rows.is_empty(),
            "reader's write should not be visible after cascade abort"
        );
    }
}

/// Hekaton §2.7: multiple readers depending on the same Preparing writer
/// all cascade-abort when the writer aborts.
#[test]
fn test_commit_dep_threaded_multiple_dependents_abort() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();

    // Writer
    let writer_conn = db.connect();
    writer_conn.execute("BEGIN CONCURRENT").unwrap();
    writer_conn
        .execute("UPDATE t SET value = 'modified' WHERE id = 1")
        .unwrap();
    let writer_tx_id = writer_conn.get_mv_tx_id().unwrap();

    let _end_ts = mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&writer_tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });

    let num_readers = 4;
    // Barrier: all readers + main thread synchronize after speculative reads
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(num_readers + 1));

    let mut handles = Vec::new();
    for i in 0..num_readers {
        let db_arc = db.get_db();
        let barrier_clone = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let conn = db_arc.connect().unwrap();
            conn.execute("BEGIN CONCURRENT").unwrap();

            // Speculative read from Preparing writer
            let mut stmt = conn.prepare("SELECT value FROM t WHERE id = 1").unwrap();
            let rows = stmt.run_collect_rows().unwrap();

            // Each reader writes to a unique row (no conflicts)
            conn.execute(format!("INSERT INTO t VALUES ({}, 'reader_{i}')", i + 10,))
                .unwrap();

            // Signal: all readers done with speculative reads
            barrier_clone.wait();

            let commit_result = conn.execute("COMMIT");
            let _ = conn.close();
            (rows, commit_result)
        }));
    }

    // Wait for all readers to complete speculative reads
    barrier.wait();

    // Abort writer → cascade to ALL readers
    mvcc_store.rollback_tx(
        writer_tx_id,
        writer_conn.pager.load().clone(),
        &writer_conn,
        crate::MAIN_DB_ID,
    );

    for handle in handles {
        let (rows, commit_result) = handle.join().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].to_text().unwrap(), "modified");
        assert!(
            matches!(commit_result, Err(LimboError::CommitDependencyAborted)),
            "expected CommitDependencyAborted, got: {commit_result:?}",
        );
    }

    // All reader writes should be invisible — only the initial row remains
    {
        let conn = db.connect();
        let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert_eq!(rows[0][0].as_int().unwrap(), 1);
    }
}

/// Hekaton §2.7 happy path: when a Preparing writer commits, the dependent
/// reader's CommitDepCounter is decremented and the reader can proceed.
#[test]
fn test_commit_dep_threaded_commit_resolves() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();

    // Writer: UPDATE via real connection, then set to Preparing
    let writer_conn = db.connect();
    writer_conn.execute("BEGIN CONCURRENT").unwrap();
    writer_conn
        .execute("UPDATE t SET value = 'committed' WHERE id = 1")
        .unwrap();
    let writer_tx_id = writer_conn.get_mv_tx_id().unwrap();

    let end_ts = mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&writer_tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });

    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc.connect().unwrap();
        reader_conn.execute("BEGIN CONCURRENT").unwrap();

        let mut stmt = reader_conn
            .prepare("SELECT value FROM t WHERE id = 1")
            .unwrap();
        let rows = stmt.run_collect_rows().unwrap();

        reader_conn
            .execute("INSERT INTO t VALUES (2, 'reader_data')")
            .unwrap();

        signal_tx.send(()).unwrap();

        // COMMIT blocks in WaitForDependencies. Writer will commit →
        // counter decremented → reader proceeds.
        let commit_result = reader_conn.execute("COMMIT");
        let _ = reader_conn.close();
        (rows, commit_result)
    });

    signal_rx.recv().unwrap();

    // Complete the writer's commit manually (postprocessing):
    // 1. Convert TxID → Timestamp in row versions
    // 2. Set state to Committed
    // 3. Drain CommitDepSet, decrement dependents' counters
    {
        let writer_tx = mvcc_store.txs.get(&writer_tx_id).unwrap();
        let writer_tx = writer_tx.value();

        // Convert TxID→Timestamp in row versions (Hekaton §3.3 postprocessing)
        for entry in mvcc_store.rows.iter() {
            let mut rvs = entry.value().write();
            for rv in rvs.iter_mut() {
                if rv.begin() == Some(TxTimestampOrID::TxID(writer_tx_id)) {
                    rv.set_begin(Some(TxTimestampOrID::Timestamp(end_ts)));
                }
                if rv.end() == Some(TxTimestampOrID::TxID(writer_tx_id)) {
                    rv.set_end(Some(TxTimestampOrID::Timestamp(end_ts)));
                }
            }
        }

        // Committed state + notify dependents
        writer_tx.state.store(TransactionState::Committed(end_ts));
        for dep_tx_id in writer_tx.commit_dep_set.lock().drain() {
            if let Some(dep_tx_entry) = mvcc_store.txs.get(&dep_tx_id) {
                dep_tx_entry
                    .value()
                    .commit_dep_counter
                    .fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    let (rows, commit_result) = reader_handle.join().unwrap();

    // Reader speculatively read the writer's value
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_text().unwrap(), "committed");

    // Reader's COMMIT succeeds: dependency resolved by writer's commit
    assert!(
        commit_result.is_ok(),
        "expected reader COMMIT to succeed, got: {commit_result:?}",
    );

    // Both writes are visible
    {
        let conn = db.connect();
        let mut stmt = conn.prepare("SELECT value FROM t ORDER BY id").unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].to_text().unwrap(), "committed");
        assert_eq!(rows[1][0].to_text().unwrap(), "reader_data");
    }
}

/// Regression: the write_set.is_empty() fast path used to commit read-only
/// transactions without checking commit dependencies. A read-only tx that
/// speculatively read from a Preparing writer must still honour AbortNow.
#[test]
fn test_commit_dep_threaded_readonly_abort_cascades() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();

    // Writer
    let writer_conn = db.connect();
    writer_conn.execute("BEGIN CONCURRENT").unwrap();
    writer_conn
        .execute("UPDATE t SET value = 'modified' WHERE id = 1")
        .unwrap();
    let writer_tx_id = writer_conn.get_mv_tx_id().unwrap();

    let _end_ts = mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&writer_tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });

    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc.connect().unwrap();
        reader_conn.execute("BEGIN CONCURRENT").unwrap();

        // Read-only: no writes, only SELECT → triggers speculative read
        let mut stmt = reader_conn
            .prepare("SELECT value FROM t WHERE id = 1")
            .unwrap();
        let rows = stmt.run_collect_rows().unwrap();

        signal_tx.send(()).unwrap();

        // COMMIT on a read-only tx hits the write_set.is_empty() fast path.
        // It must still check commit dependencies.
        let commit_result = reader_conn.execute("COMMIT");
        let _ = reader_conn.close();
        (rows, commit_result)
    });

    signal_rx.recv().unwrap();

    // Abort writer → cascade to read-only reader
    mvcc_store.rollback_tx(
        writer_tx_id,
        writer_conn.pager.load().clone(),
        &writer_conn,
        crate::MAIN_DB_ID,
    );

    let (rows, commit_result) = reader_handle.join().unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_text().unwrap(), "modified");

    // Read-only tx must still fail when its dependency aborts
    assert!(
        matches!(commit_result, Err(LimboError::CommitDependencyAborted)),
        "read-only tx should fail with CommitDependencyAborted, got: {commit_result:?}",
    );
}

/// Test that register_commit_dependency increments counter before pushing to
/// dep_set, preventing underflow. If counter is incremented after push+unlock,
/// a concurrent drain could fetch_sub(1) on a zero counter, wrapping to MAX.
#[test]
fn test_commit_dependency_counter_no_underflow() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Preparing(5)))]);
    let reader = new_tx(2, 10, TransactionState::Active);

    // Register dependency: counter should go 0 → 1
    register_commit_dependency(&txs, &reader, 1);
    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 1);

    // Simulate drain (as in CommitEnd): fetch_sub should go 1 → 0, not wrap
    reader.commit_dep_counter.fetch_sub(1, Ordering::AcqRel);
    assert_eq!(
        reader.commit_dep_counter.load(Ordering::Acquire),
        0,
        "counter should be exactly 0, not u64::MAX (underflow)"
    );
}

/// Test that registering a dependency on a Terminated (aborted+removed from map)
/// transaction correctly sets AbortNow. Before the fix, rollback_tx removed the
/// tx from txs, so register_commit_dependency saw None and assumed "committed."
#[test]
fn test_commit_dependency_terminated_tx_sets_abort() {
    let txs: SkipMap<TxID, Transaction> =
        SkipMap::from_iter([(1, new_tx(1, 1, TransactionState::Terminated))]);

    let reader = new_tx(2, 10, TransactionState::Active);
    register_commit_dependency(&txs, &reader, 1);

    // Terminated means the tx aborted — must set abort_now
    assert!(
        reader.abort_now.load(Ordering::Acquire),
        "dependency on Terminated tx should set abort_now"
    );
    assert_eq!(
        reader.commit_dep_counter.load(Ordering::Acquire),
        0,
        "no counter increment for aborted/terminated dependency"
    );
}

/// Test that when tx is NOT in the map (removed), register_commit_dependency
/// treats it as committed (no abort_now, no counter increment). This is correct
/// only for committed transactions. Aborted transactions should NOT be removed
/// from the map (Issue #3 fix ensures this).
#[test]
fn test_commit_dependency_missing_tx_assumes_committed() {
    let txs: SkipMap<TxID, Transaction> = SkipMap::new();

    let reader = new_tx(2, 10, TransactionState::Active);
    register_commit_dependency(&txs, &reader, 99);

    assert!(
        !reader.abort_now.load(Ordering::Acquire),
        "missing tx (committed+removed) should not set abort_now"
    );
    assert_eq!(reader.commit_dep_counter.load(Ordering::Acquire), 0);
}

/// Test that read-only transactions with resolved dependencies do NOT advance
/// last_committed_tx_ts. A read-only tx going through WaitForDependencies →
/// CommitEnd would update last_committed_tx_ts, causing spurious Busy errors
/// from acquire_exclusive_tx.
#[test]
fn test_commit_dep_readonly_does_not_advance_timestamp() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();
    let ts_before = mvcc_store.last_committed_tx_ts.load(Ordering::Acquire);

    // Writer: UPDATE then set to Preparing
    let writer_conn = db.connect();
    writer_conn.execute("BEGIN CONCURRENT").unwrap();
    writer_conn
        .execute("UPDATE t SET value = 'modified' WHERE id = 1")
        .unwrap();
    let writer_tx_id = writer_conn.get_mv_tx_id().unwrap();

    let end_ts = mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&writer_tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });

    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let mvcc_clone = mvcc_store.clone();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc.connect().unwrap();
        reader_conn.execute("BEGIN CONCURRENT").unwrap();

        // Read-only: SELECT only → speculative read registers dependency
        let mut stmt = reader_conn
            .prepare("SELECT value FROM t WHERE id = 1")
            .unwrap();
        let _rows = stmt.run_collect_rows().unwrap();

        signal_tx.send(()).unwrap();

        // COMMIT: read-only with dependency → WaitForDependencies
        let commit_result = reader_conn.execute("COMMIT");
        let _ = reader_conn.close();
        commit_result
    });

    signal_rx.recv().unwrap();

    // Complete writer's commit manually (resolve dependency)
    {
        let writer_tx = mvcc_store.txs.get(&writer_tx_id).unwrap();
        let writer_tx = writer_tx.value();
        for entry in mvcc_store.rows.iter() {
            let mut rvs = entry.value().write();
            for rv in rvs.iter_mut() {
                if rv.begin() == Some(TxTimestampOrID::TxID(writer_tx_id)) {
                    rv.set_begin(Some(TxTimestampOrID::Timestamp(end_ts)));
                }
                if rv.end() == Some(TxTimestampOrID::TxID(writer_tx_id)) {
                    rv.set_end(Some(TxTimestampOrID::Timestamp(end_ts)));
                }
            }
        }
        writer_tx.state.store(TransactionState::Committed(end_ts));
        for dep_tx_id in writer_tx.commit_dep_set.lock().drain() {
            if let Some(dep_tx_entry) = mvcc_store.txs.get(&dep_tx_id) {
                dep_tx_entry
                    .value()
                    .commit_dep_counter
                    .fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    let commit_result = reader_handle.join().unwrap();
    assert!(
        commit_result.is_ok(),
        "read-only tx with resolved dependency should commit: {commit_result:?}",
    );

    let ts_after = mvcc_clone.last_committed_tx_ts.load(Ordering::Acquire);
    assert_eq!(
        ts_before, ts_after,
        "read-only tx should NOT advance last_committed_tx_ts (was {ts_before}, now {ts_after})"
    );
}

/// What this test checks: the committed timestamp cache is a monotonic watermark even when independent commits finish out of timestamp order.
/// Why this matters: checkpoints use this cache as a durable replay boundary; lowering it can make DB pages advance past MVCC metadata.
#[test]
fn test_last_committed_timestamp_is_monotonic_for_out_of_order_commits() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    setup.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    setup.execute("INSERT INTO t VALUES (2, 'b')").unwrap();

    let mvcc_store = db.get_mvcc_store();
    let conn_a = db.connect();
    let conn_b = db.connect();

    conn_a.execute("BEGIN CONCURRENT").unwrap();
    conn_a
        .execute("UPDATE t SET v = 'a1' WHERE id = 1")
        .unwrap();
    let tx_a_id = conn_a.get_mv_tx_id().expect("tx_a should be active");
    conn_a.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));

    let mut commit_a = conn_a.prepare("COMMIT").unwrap();
    assert!(
        matches!(commit_a.step().unwrap(), StepResult::Yield),
        "tx_a should yield after getting its commit timestamp"
    );
    let tx_a_end_ts = match mvcc_store
        .txs
        .get(&tx_a_id)
        .expect("tx_a should still be tracked")
        .value()
        .state
        .load()
    {
        TransactionState::Preparing(ts) => ts,
        state => panic!("expected tx_a to be Preparing, got {state:?}"),
    };

    conn_b.execute("BEGIN CONCURRENT").unwrap();
    conn_b
        .execute("UPDATE t SET v = 'b1' WHERE id = 2")
        .unwrap();
    conn_b.execute("COMMIT").unwrap();
    let tx_b_committed = mvcc_store.last_committed_tx_ts.load(Ordering::Acquire);
    assert!(
        tx_b_committed > tx_a_end_ts,
        "tx_b should commit at a newer timestamp than the yielded tx_a"
    );

    commit_a.run_ignore_rows().unwrap();
    let final_watermark = mvcc_store.last_committed_tx_ts.load(Ordering::Acquire);
    assert_eq!(
        final_watermark, tx_b_committed,
        "finishing an older commit must not lower the committed timestamp watermark"
    );
}

/// Test that a new transaction can still acquire the exclusive lock after a
/// read-only dependent tx commits. Before the fix, the read-only tx would
/// advance last_committed_tx_ts via CommitEnd, making acquire_exclusive_tx
/// return Busy for transactions that started before the read.
#[test]
fn test_commit_dep_readonly_does_not_cause_spurious_busy() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();
        conn.close().unwrap();
    }

    let mvcc_store = db.get_mvcc_store();

    // Writer: UPDATE then set to Preparing
    let writer_conn = db.connect();
    writer_conn.execute("BEGIN CONCURRENT").unwrap();
    writer_conn
        .execute("UPDATE t SET value = 'modified' WHERE id = 1")
        .unwrap();
    let writer_tx_id = writer_conn.get_mv_tx_id().unwrap();

    let end_ts = mvcc_store.get_commit_timestamp(|ts| {
        mvcc_store
            .txs
            .get(&writer_tx_id)
            .unwrap()
            .value()
            .state
            .store(TransactionState::Preparing(ts));
    });

    // Start a non-CONCURRENT tx that will try to get exclusive lock later.
    // Its begin_ts is assigned now, before the read-only tx commits.
    let exclusive_conn = db.connect();
    exclusive_conn.execute("BEGIN CONCURRENT").unwrap();
    let exclusive_tx_id = exclusive_conn.get_mv_tx_id().unwrap();

    let (signal_tx, signal_rx) = std::sync::mpsc::channel();

    let db_arc = db.get_db();
    let reader_handle = std::thread::spawn(move || {
        let reader_conn = db_arc.connect().unwrap();
        reader_conn.execute("BEGIN CONCURRENT").unwrap();

        let mut stmt = reader_conn
            .prepare("SELECT value FROM t WHERE id = 1")
            .unwrap();
        let _rows = stmt.run_collect_rows().unwrap();

        signal_tx.send(()).unwrap();

        let commit_result = reader_conn.execute("COMMIT");
        let _ = reader_conn.close();
        commit_result
    });

    signal_rx.recv().unwrap();

    // Resolve the writer's commit (unblocks reader's WaitForDependencies)
    {
        let writer_tx = mvcc_store.txs.get(&writer_tx_id).unwrap();
        let writer_tx = writer_tx.value();
        for entry in mvcc_store.rows.iter() {
            let mut rvs = entry.value().write();
            for rv in rvs.iter_mut() {
                if rv.begin() == Some(TxTimestampOrID::TxID(writer_tx_id)) {
                    rv.set_begin(Some(TxTimestampOrID::Timestamp(end_ts)));
                }
                if rv.end() == Some(TxTimestampOrID::TxID(writer_tx_id)) {
                    rv.set_end(Some(TxTimestampOrID::Timestamp(end_ts)));
                }
            }
        }
        writer_tx.state.store(TransactionState::Committed(end_ts));
        for dep_tx_id in writer_tx.commit_dep_set.lock().drain() {
            if let Some(dep_tx_entry) = mvcc_store.txs.get(&dep_tx_id) {
                dep_tx_entry
                    .value()
                    .commit_dep_counter
                    .fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    let commit_result = reader_handle.join().unwrap();
    assert!(commit_result.is_ok());

    // Now try to acquire exclusive lock for the tx that started before the
    // read-only dependent committed. Should succeed because the read-only tx
    // did not advance last_committed_tx_ts.
    let acquire_result = mvcc_store.acquire_exclusive_tx(&exclusive_tx_id, None);
    assert!(
        acquire_result.is_ok(),
        "acquire_exclusive_tx should not return Busy after a read-only dependent committed: {acquire_result:?}",
    );
    mvcc_store.release_exclusive_tx(&exclusive_tx_id);
}

#[test]
fn test_exclusive_tx_does_not_deadlock_behind_preparing_concurrent_commit() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn_a = db.connect();
    conn_a
        .execute("CREATE TABLE t (key TEXT PRIMARY KEY, value BLOB)")
        .unwrap();

    conn_a.execute("BEGIN CONCURRENT").unwrap();
    conn_a
        .execute("INSERT INTO t VALUES ('a', zeroblob(16))")
        .unwrap();
    conn_a.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));

    let mut commit_a = conn_a.prepare("COMMIT").unwrap();
    assert!(
        matches!(commit_a.step().unwrap(), StepResult::Yield),
        "first commit must pause after publishing Preparing and before taking the log lock",
    );

    let conn_b = db.connect();
    let mut insert_b = conn_b
        .prepare("INSERT INTO t VALUES ('b', zeroblob(16))")
        .unwrap();
    let mut saw_busy = false;
    for _ in 0..64 {
        match insert_b.step() {
            Ok(StepResult::IO | StepResult::Yield) => continue,
            Ok(StepResult::Busy) | Err(LimboError::Busy) => {
                saw_busy = true;
                break;
            }
            Ok(StepResult::Done) => {
                panic!("exclusive insert started while another tx was Preparing")
            }
            Ok(other) => panic!("unexpected insert step result: {other:?}"),
            Err(err) => panic!("unexpected insert error: {err:?}"),
        }
    }
    assert!(
        saw_busy,
        "exclusive insert should return Busy instead of waiting while holding the log lock",
    );
    insert_b.reset().unwrap();

    let mut committed = false;
    for _ in 0..1024 {
        match commit_a.step().unwrap() {
            StepResult::Done => {
                committed = true;
                break;
            }
            StepResult::IO | StepResult::Yield => {}
            other => panic!("unexpected commit step result: {other:?}"),
        }
    }
    assert!(
        committed,
        "paused concurrent commit should finish after Busy"
    );

    conn_a.set_yield_injector(None);

    let rows = get_rows(&conn_a, "SELECT key FROM t ORDER BY key");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_text().unwrap(), "a");

    conn_a.close().unwrap();
    conn_b.close().unwrap();
}

/// Insert a synthetic table and a single row via the MVCC store, then commit.
/// Used by restart / recovery tests to seed durable state before a restart cycle.
fn write_synthetic_row(db: &MvccTestDbNoConn, value: &str) {
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    let max_root_page = get_rows(
        &conn,
        "SELECT COALESCE(MAX(rootpage), 0) FROM sqlite_schema WHERE rootpage > 0",
    )[0][0]
        .as_int()
        .unwrap();
    let next_schema_rowid = get_rows(
        &conn,
        "SELECT COALESCE(MAX(rowid), 0) + 1 FROM sqlite_schema",
    )[0][0]
        .as_int()
        .unwrap();
    let synthetic_root = -(max_root_page + 100);
    let synthetic_table_id = MVTableId::new(synthetic_root);
    let tx_id = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
    let data = ImmutableRecord::from_values(
        &[
            Value::Text(Text::new("table")),
            Value::Text(Text::new("test")),
            Value::Text(Text::new("test")),
            Value::from_i64(synthetic_root),
            Value::Text(Text::new(
                "CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)",
            )),
        ],
        5,
    )
    .unwrap();
    mvcc_store
        .insert(
            tx_id,
            Row::new_table_row(
                RowID::new((-1).into(), RowKey::Int(next_schema_rowid)),
                data.as_blob(),
                5,
            )
            .unwrap(),
        )
        .unwrap();
    let row = generate_simple_string_row(synthetic_table_id, 1, value);
    mvcc_store.insert(tx_id, row).unwrap();
    commit_tx(mvcc_store, &conn, tx_id).unwrap();
    conn.close().unwrap();
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    write_synthetic_row(&db, "foo");
    db.restart();

    {
        let conn = db.connect();
        let mvcc_store = db.get_mvcc_store();
        let max_root_page = get_rows(
            &conn,
            "SELECT COALESCE(MAX(rootpage), 0) FROM sqlite_schema WHERE rootpage > 0",
        )[0][0]
            .as_int()
            .unwrap();
        let synthetic_table_id = MVTableId::new(-(max_root_page + 100));
        let tx_id = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
        let row = generate_simple_string_row(synthetic_table_id, 2, "bar");

        mvcc_store.insert(tx_id, row).unwrap();
        commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();

        let tx_id = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
        let row = mvcc_store
            .read(tx_id, &RowID::new(synthetic_table_id, RowKey::Int(2)))
            .unwrap()
            .unwrap();
        let record = get_record_value(&row);
        match record.get_value(0).unwrap() {
            ValueRef::Text(text) => {
                assert_eq!(text.as_str(), "bar");
            }
            _ => panic!("Expected Text value"),
        }
        conn.close().unwrap();
    }
}

/// What this test checks: The implementation maintains the intended invariant for this scenario.
/// Why this matters: The invariant protects correctness across commit, replay, and query execution paths.
#[test]
fn test_connection_sees_other_connection_changes() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn0 = db.connect();
    conn0
        .execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, text TEXT)")
        .unwrap();
    let conn1 = db.connect();
    conn1
        .execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, text TEXT)")
        .unwrap();
    conn0
        .execute("INSERT INTO test_table (id, text) VALUES (965, 'text_877')")
        .unwrap();
    let mut stmt = conn1.query("SELECT * FROM test_table").unwrap().unwrap();
    stmt.run_with_row_callback(|row| {
        let text = row.get_value(1).to_text().unwrap();
        assert_eq!(text, "text_877");
        Ok(())
    })
    .unwrap();
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_delete_with_conn() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn0 = db.connect();
    conn0.execute("CREATE TABLE test(t)").unwrap();

    let mut inserts = vec![1, 2, 3, 4, 5, 6, 7];

    for t in &inserts {
        conn0
            .execute(format!("INSERT INTO test(t) VALUES ({t})"))
            .unwrap();
    }

    conn0.execute("DELETE FROM test WHERE t = 5").unwrap();
    inserts.remove(4);

    let mut stmt = conn0.prepare("SELECT * FROM test").unwrap();
    let mut pos = 0;
    stmt.run_with_row_callback(|row| {
        let t = row.get_value(0).as_int().unwrap();
        assert_eq!(t, inserts[pos]);
        pos += 1;
        Ok(())
    })
    .unwrap();
}

fn get_record_value(row: &Row) -> ImmutableRecord {
    let mut record = ImmutableRecord::new(1024).unwrap();
    record.start_serialization(row.payload()).unwrap();
    record
}

/// What this test checks: The implementation maintains the intended invariant for this scenario.
/// Why this matters: The invariant protects correctness across commit, replay, and query execution paths.
#[test]
fn test_interactive_transaction() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    // do some transaction
    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE test (x)").unwrap();
    conn.execute("INSERT INTO test (x) VALUES (1)").unwrap();
    conn.execute("INSERT INTO test (x) VALUES (2)").unwrap();
    conn.execute("COMMIT").unwrap();

    // expect other transaction to see the changes
    let rows = get_rows(&conn, "SELECT * FROM test");
    assert_eq!(
        rows,
        vec![vec![Value::from_i64(1)], vec![Value::from_i64(2)]]
    );
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_commit_without_tx() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    // do not start interactive transaction
    conn.execute("CREATE TABLE test (x)").unwrap();
    conn.execute("INSERT INTO test (x) VALUES (1)").unwrap();

    // expect error on trying to commit a non-existent interactive transaction
    let err = conn.execute("COMMIT").unwrap_err();
    if let LimboError::TxError(e) = err {
        assert_eq!(e, "cannot commit - no transaction is active");
    } else {
        panic!("Expected TxError");
    }
}

fn get_rows(conn: &Arc<Connection>, query: &str) -> Vec<Vec<Value>> {
    let mut stmt = conn.prepare(query).unwrap();
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        let values = row.get_values().cloned().collect::<Vec<_>>();
        rows.push(values);
        Ok(())
    })
    .unwrap();
    rows
}

/// Any ddl specially CREATE INDEX must cause SchemaUpdated errors on ongoing INSERTS because
/// we shouldn't commit an insert without inserting rows to this new index that is being created.
/// Here we test that case by injecting in the middle of CREATE INDEX's commit and then doing a
/// regular concurrent insert that will not take into account new index.
#[test]
fn test_insert_in_middle_commit_of_create_index_returns_err() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let setup = db.connect();
        setup
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
            .unwrap();
        setup.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        setup.close().unwrap();
    }

    let conn_a = db.connect();
    let conn_b = db.connect();

    // T1 (conn_a): CREATE INDEX, yielding at `LogRecordPrepared` — the
    // point in the commit pipeline where `end_ts` has been assigned and the
    // log record is built, but the global header and
    // `last_committed_schema_change_ts` haven't been published yet.
    conn_a.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));
    let mut create_idx = conn_a.prepare("CREATE INDEX i ON t(c)").unwrap();
    let mut yielded = false;
    for _ in 0..200 {
        match create_idx.step().unwrap() {
            StepResult::Yield => {
                yielded = true;
                break;
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(
        yielded,
        "CREATE INDEX should yield at CommitYieldPoint::LogRecordPrepared"
    );

    // T2 (conn_b): start a new tx now — its begin_ts will be > T1's end_ts,
    // but the global schema view still doesn't know about the new index `i`.
    // The INSERT compiles its bytecode against the stale schema and emits
    // IdxInsert ops only for the indexes the stale schema knows about.
    conn_b.execute("BEGIN CONCURRENT").unwrap();
    conn_b.execute("INSERT INTO t VALUES (2, 20)").unwrap();

    // T1 finishes finalizing the CREATE INDEX. After this point, the global
    // header carries the new schema_cookie and `last_committed_schema_change_ts`
    // is bumped to T1's `end_ts`.
    create_idx.run_ignore_rows().unwrap();
    drop(create_idx);

    // T2 commits. The schema-conflict check at `CommitState::Initial` compares
    // `last_committed_schema_change_ts (= T1.end_ts) > tx_b.begin_ts (> T1.end_ts)`
    // which is FALSE — so no conflict is raised and T2 commits cleanly, even
    // though its writes never touched the new index.

    let commit_result = conn_b.execute("COMMIT");

    assert!(
        matches!(
            commit_result,
            Err(LimboError::SchemaConflict | LimboError::SchemaUpdated)
        ),
        "BUG: tx_b's COMMIT returned {commit_result:?} but should have been \
         aborted with SchemaConflict/SchemaUpdated. tx_b began with a stale \
         schema (missing index `i`), so its INSERT silently skipped writing \
         to that index. Allowing the commit leaves `i` permanently short the \
         row tx_b wrote."
    );
}

#[test]
#[ignore]
fn test_concurrent_writes() {
    struct ConnectionState {
        conn: Arc<Connection>,
        inserts: Vec<i64>,
        current_statement: Option<Statement>,
    }
    let db = MvccTestDbNoConn::new_with_random_db();
    let mut connections = Vec::new();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE test (x)").unwrap();
        conn.close().unwrap();
    }
    let num_connections = 20;
    let num_inserts_per_connection = 10000;
    for i in 0..num_connections {
        let conn = db.connect();
        let mut inserts = ((num_inserts_per_connection * i)
            ..(num_inserts_per_connection * (i + 1)))
            .collect::<Vec<i64>>();
        inserts.reverse();
        connections.push(ConnectionState {
            conn,
            inserts,
            current_statement: None,
        });
    }

    loop {
        let mut all_finished = true;
        for conn in &mut connections {
            if !conn.inserts.is_empty() || conn.current_statement.is_some() {
                all_finished = false;
                break;
            }
        }
        for (conn_id, conn) in connections.iter_mut().enumerate() {
            // println!("connection {conn_id} inserts: {:?}", conn.inserts);
            if conn.current_statement.is_none() && !conn.inserts.is_empty() {
                let write = conn.inserts.pop().unwrap();
                println!("inserting row {write} from connection {conn_id}");
                conn.current_statement = Some(
                    conn.conn
                        .prepare(format!("INSERT INTO test (x) VALUES ({write})"))
                        .unwrap(),
                );
            }
            if conn.current_statement.is_none() {
                continue;
            }
            println!("connection step {conn_id}");
            let stmt = conn.current_statement.as_mut().unwrap();
            match stmt.step().unwrap() {
                // These you be only possible cases in write concurrency.
                // No rows because insert doesn't return
                // No interrupt because insert doesn't interrupt
                // No busy because insert in mvcc should be multi concurrent write
                StepResult::Done => {
                    println!("connection {conn_id} done");
                    conn.current_statement = None;
                }
                StepResult::IO | StepResult::Yield => {
                    // let's skip doing I/O here, we want to perform io only after all the statements are stepped
                }
                StepResult::Busy => {
                    println!("connection {conn_id} busy");
                    // stmt.reprepare().unwrap();
                    unreachable!();
                }
                _ => {
                    unreachable!()
                }
            }
        }
        db.get_db().io.step().unwrap();

        if all_finished {
            println!("all finished");
            break;
        }
    }

    // Now let's find out if we wrote everything we intended to write.
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT * FROM test ORDER BY x ASC");
    assert_eq!(
        rows.len() as i64,
        num_connections * num_inserts_per_connection
    );
    for (row_id, row) in rows.iter().enumerate() {
        assert_eq!(row[0].as_int().unwrap(), row_id as i64);
    }
    conn.close().unwrap();
}

/// The implementation maintains the intended invariant for this scenario.
#[test]
fn transaction_display() {
    let state = AtomicTransactionState::from(TransactionState::Preparing(20250915));
    let tx_id = 42;
    let begin_ts = 20250914;

    let empty_versions = || Arc::new(RwLock::new(crate::alloc::vec![]));
    let write_set = Mutex::new({
        let mut write_set: WriteSet = WriteSet::new();
        write_set.insert(RowID::new((-2).into(), RowKey::Int(11)), empty_versions());
        write_set.insert(RowID::new((-2).into(), RowKey::Int(13)), empty_versions());
        write_set
    });

    let tx = Transaction {
        state,
        tx_id,
        begin_ts,
        write_set,
        header: RwLock::new(DatabaseHeader::default()),
        header_dirty: AtomicBool::new(false),
        savepoint_stack: RwLock::new(Vec::new()),
        pager_commit_lock_held: AtomicBool::new(false),
        log_appended: AtomicBool::new(false),
        commit_dep_counter: AtomicU64::new(0),
        abort_now: AtomicBool::new(false),
        commit_dep_set: Mutex::new(HashSet::default()),
        holds_blocking_checkpoint_read: AtomicBool::new(false),
        schema_generation_at_begin: 0,
        read_mark: crate::mvcc::database::WalPos::ORIGIN,
    };

    let expected = "{ state: Preparing(20250915), id: 42, begin_ts: 20250914, write_set: [RowID { table_id: MVTableId(-2), row_id: Int(11) }, RowID { table_id: MVTableId(-2), row_id: Int(13) }] }";
    let output = format!("{tx}");
    assert_eq!(output, expected);
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_should_checkpoint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let mv_store = db.get_mvcc_store();
    assert!(!mv_store.storage.should_checkpoint());
    mv_store.set_checkpoint_threshold(0);
    assert!(mv_store.storage.should_checkpoint());
}

/// What this test checks: After restart recovery, checkpoint-threshold checks use the recovered log offset.
/// Why this matters: Shadow-offset drift can suppress auto-checkpoint despite a large recovered log tail.
#[test]
fn test_should_checkpoint_after_recovery_uses_recovered_offset() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x)").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
    }

    db.restart();
    let _conn = db.connect();
    let mv_store = db.get_mvcc_store();

    // We used to assert on the concrete logical-log offset here, but MVCC durable storage
    // is now abstracted behind a trait object (to allow injecting custom implementations).
    // Validate behavior instead: after recovery, the recovered offset should be reflected
    // in should_checkpoint() when the threshold is set very low.
    mv_store.set_checkpoint_threshold(1);
    assert!(
        mv_store.storage.should_checkpoint(),
        "expected should_checkpoint() to reflect the recovered logical-log offset"
    );
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_insert_with_checkpoint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let mv_store = db.get_mvcc_store();
    // force checkpoint on every transaction
    mv_store.set_checkpoint_threshold(0);
    let conn = db.connect();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let rows = get_rows(&conn, "SELECT * FROM t");
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    assert_eq!(row.len(), 1);
    let value = row.first().unwrap();
    match value {
        Value::Numeric(crate::numeric::Numeric::Integer(i)) => assert_eq!(*i, 1),
        _ => unreachable!(),
    }
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_auto_checkpoint_busy_is_ignored() {
    let db = MvccTestDb::new();
    db.mvcc_store.set_checkpoint_threshold(0);

    // Keep a second transaction open to hold the checkpoint read lock.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx2 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();

    let row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, row).unwrap();

    // Regression: auto-checkpoint returning Busy used to bubble up and cause
    // statement abort/rollback after the tx was removed.
    // Commit should succeed even if the auto-checkpoint is busy.
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // Cleanup: release the read lock held by tx2.
    db.mvcc_store.rollback_tx(
        tx2,
        db.conn.pager.load().clone(),
        &db.conn,
        crate::MAIN_DB_ID,
    );
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_mvcc_read_tx_lifecycle() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("SELECT * FROM t").unwrap();

    let pager = conn.pager.load();
    let wal = pager.wal.as_ref().expect("wal should be enabled");
    assert!(wal.holds_read_lock());

    conn.execute("COMMIT").unwrap();
    assert!(!wal.holds_read_lock());
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_mvcc_conn_drop_releases_read_tx() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(x)").unwrap();

    let pager = conn.pager.load();
    pager.begin_read_tx().unwrap();
    let wal = pager.wal.as_ref().expect("wal should be enabled").clone();
    assert!(wal.holds_read_lock());

    drop(conn);
    assert!(!wal.holds_read_lock());
}

/// What this test checks: The implementation maintains the intended invariant for this scenario.
/// Why this matters: The invariant protects correctness across commit, replay, and query execution paths.
#[test]
fn test_select_empty_table() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let mv_store = db.get_mvcc_store();
    // force checkpoint on every transaction
    mv_store.set_checkpoint_threshold(0);
    let conn = db.connect();
    conn.execute("CREATE TABLE t(x integer primary key)")
        .unwrap();
    let rows = get_rows(&conn, "SELECT * FROM t where x > 100");
    assert!(rows.is_empty());
}

/// Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
#[turso_macros::test(encryption)]
fn test_cursor_with_btree_and_mvcc() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO t VALUES (2)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    println!("getting rows");
    let rows = get_rows(&conn, "SELECT * FROM t");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![Value::from_i64(1)]);
    assert_eq!(rows[1], vec![Value::from_i64(2)]);
}

/// Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
#[turso_macros::test(encryption)]
fn test_cursor_with_btree_and_mvcc_2() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO t VALUES (3)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    // Insert a new row so that we have a gap in the BTree.
    conn.execute("INSERT INTO t VALUES (2)").unwrap();
    println!("getting rows");
    let rows = get_rows(&conn, "SELECT * FROM t");
    dbg!(&rows);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec![Value::from_i64(1)]);
    assert_eq!(rows[1], vec![Value::from_i64(2)]);
    assert_eq!(rows[2], vec![Value::from_i64(3)]);
}

/// Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
#[turso_macros::test(encryption)]
fn test_cursor_with_btree_and_mvcc_with_backward_cursor() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO t VALUES (3)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    // Insert a new row so that we have a gap in the BTree.
    conn.execute("INSERT INTO t VALUES (2)").unwrap();
    let rows = get_rows(&conn, "SELECT * FROM t order by x desc");
    dbg!(&rows);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec![Value::from_i64(3)]);
    assert_eq!(rows[1], vec![Value::from_i64(2)]);
    assert_eq!(rows[2], vec![Value::from_i64(1)]);
}

/// Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
#[turso_macros::test(encryption)]
fn test_cursor_with_btree_and_mvcc_with_backward_cursor_with_delete() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO t VALUES (2)").unwrap();
        conn.execute("INSERT INTO t VALUES (4)").unwrap();
        conn.execute("INSERT INTO t VALUES (5)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    // Insert a new row so that we have a gap in the BTree.
    conn.execute("INSERT INTO t VALUES (3)").unwrap();
    conn.execute("DELETE FROM t WHERE x = 2").unwrap();
    println!("getting rows");
    let rows = get_rows(&conn, "SELECT * FROM t order by x desc");
    dbg!(&rows);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], vec![Value::from_i64(5)]);
    assert_eq!(rows[1], vec![Value::from_i64(4)]);
    assert_eq!(rows[2], vec![Value::from_i64(3)]);
    assert_eq!(rows[3], vec![Value::from_i64(1)]);
}

/// Cursor traversal and seek operations honor MVCC visibility and key ordering under updates/deletes.
#[turso_macros::test(encryption)]
#[ignore] // FIXME: This fails constantly on main and is really annoying, disabling for now :]
fn test_cursor_with_btree_and_mvcc_fuzz() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    let mut rows_in_db = sorted_vec::SortedVec::new();
    let mut seen = HashSet::default();
    let (mut rng, _seed) = rng_from_time_or_env();
    println!("seed: {_seed}");

    let mut maybe_conn = Some(db.connect());
    {
        maybe_conn
            .as_mut()
            .unwrap()
            .execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
    }

    #[repr(u8)]
    #[derive(Debug)]
    enum Op {
        Insert = 0,
        Delete = 1,
        SelectForward = 2,
        SelectBackward = 3,
        SeekForward = 4,
        SeekBackward = 5,
        Checkpoint = 6,
    }

    impl From<u8> for Op {
        fn from(value: u8) -> Self {
            match value {
                0 => Op::Insert,
                1 => Op::Delete,
                2 => Op::SelectForward,
                3 => Op::SelectBackward,
                4 => Op::SeekForward,
                5 => Op::SeekBackward,
                6 => Op::Checkpoint,
                _ => unreachable!(),
            }
        }
    }

    for i in 0..10000 {
        let conn = maybe_conn.as_mut().unwrap();
        let op = rng.random_range(0..=Op::Checkpoint as usize);
        let op = Op::from(op as u8);
        println!("tick: {i} op: {op:?} ");
        match op {
            Op::Insert => {
                let value = loop {
                    let value = rng.random_range(0..10000);
                    if !seen.contains(&value) {
                        seen.insert(value);
                        break value;
                    }
                };
                let query = format!("INSERT INTO t VALUES ({value})");
                println!("inserting: {query}");
                conn.execute(query.as_str()).unwrap();
                rows_in_db.push(value);
            }
            Op::Delete => {
                if rows_in_db.is_empty() {
                    continue;
                }
                let index = rng.random_range(0..rows_in_db.len());
                let value = rows_in_db[index];
                let query = format!("DELETE FROM t WHERE x = {value}");
                println!("deleting: {query}");
                conn.execute(query.as_str()).unwrap();
                rows_in_db.remove_index(index);
                seen.remove(&value);
            }
            Op::SelectForward => {
                let rows = get_rows(conn, "SELECT * FROM t order by x asc");
                assert_eq!(
                    rows.len(),
                    rows_in_db.len(),
                    "expected {} rows, got {}",
                    rows_in_db.len(),
                    rows.len()
                );
                for (row, expected_rowid) in rows.iter().zip(rows_in_db.iter()) {
                    assert_eq!(
                        row[0].as_int().unwrap(),
                        *expected_rowid,
                        "expected row id {}  got {}",
                        *expected_rowid,
                        row[0].as_int().unwrap()
                    );
                }
            }
            Op::SelectBackward => {
                let rows = get_rows(conn, "SELECT * FROM t order by x desc");
                assert_eq!(
                    rows.len(),
                    rows_in_db.len(),
                    "expected {} rows, got {}",
                    rows_in_db.len(),
                    rows.len()
                );
                for (row, expected_rowid) in rows.iter().zip(rows_in_db.iter().rev()) {
                    assert_eq!(
                        row[0].as_int().unwrap(),
                        *expected_rowid,
                        "expected row id {}  got {}",
                        *expected_rowid,
                        row[0].as_int().unwrap()
                    );
                }
            }
            Op::SeekForward => {
                let value = rng.random_range(0..10000);
                let rows = get_rows(
                    conn,
                    format!("SELECT * FROM t where x > {value} order by x asc").as_str(),
                );
                let filtered_rows_in_db = rows_in_db
                    .iter()
                    .filter(|&id| *id > value)
                    .cloned()
                    .collect::<Vec<i64>>();

                assert_eq!(
                    rows.len(),
                    filtered_rows_in_db.len(),
                    "expected {} rows, got {}",
                    filtered_rows_in_db.len(),
                    rows.len()
                );
                for (row, expected_rowid) in rows.iter().zip(filtered_rows_in_db.iter()) {
                    assert_eq!(
                        row[0].as_int().unwrap(),
                        *expected_rowid,
                        "expected row id {}  got {}",
                        *expected_rowid,
                        row[0].as_int().unwrap()
                    );
                }
            }
            Op::SeekBackward => {
                let value = rng.random_range(0..10000);
                let rows = get_rows(
                    conn,
                    format!("SELECT * FROM t where x > {value} order by x desc").as_str(),
                );
                let filtered_rows_in_db = rows_in_db
                    .iter()
                    .filter(|&id| *id > value)
                    .cloned()
                    .collect::<Vec<i64>>();

                assert_eq!(
                    rows.len(),
                    filtered_rows_in_db.len(),
                    "expected {} rows, got {}",
                    filtered_rows_in_db.len(),
                    rows.len()
                );
                for (row, expected_rowid) in rows.iter().zip(filtered_rows_in_db.iter().rev()) {
                    assert_eq!(
                        row[0].as_int().unwrap(),
                        *expected_rowid,
                        "expected row id {}  got {}",
                        *expected_rowid,
                        row[0].as_int().unwrap()
                    );
                }
            }
            Op::Checkpoint => {
                // This forces things to move to the BTree file (.db)
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
                // This forces MVCC to be cleared
                db.restart();
                maybe_conn = Some(db.connect());
            }
        }
    }
}

pub fn rng_from_time_or_env() -> (ChaCha8Rng, u64) {
    let seed = std::env::var("SEED").map_or(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        |v| {
            v.parse()
                .expect("Failed to parse SEED environment variable as u64")
        },
    );
    let rng = ChaCha8Rng::seed_from_u64(seed as u64);
    (rng, seed as u64)
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_cursor_with_btree_and_mvcc_insert_after_checkpoint_repeated_key() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO t VALUES (2)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    // Insert a new row so that we have a gap in the BTree.
    let res = conn.execute("INSERT INTO t VALUES (2)");
    assert!(res.is_err(), "Expected error because key 2 already exists");
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_cursor_with_btree_and_mvcc_seek_after_checkpoint() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO t VALUES (2)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    // Seek to the second row.
    let res = get_rows(&conn, "SELECT * FROM t WHERE x = 2");
    assert_eq!(res.len(), 1);
    assert_eq!(res[0][0].as_int().unwrap(), 2);
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_cursor_with_btree_and_mvcc_delete_after_checkpoint() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    conn.execute("DELETE FROM t WHERE x = 1").unwrap();
    let rows = get_rows(&conn, "SELECT * FROM t order by x desc");
    assert_eq!(rows.len(), 0);
}

/// Core MVCC read/write semantics for AUTOINCREMENT with rowid update.
/// After INSERT (rowid 1), UPDATE rowid 1→2, and a second INSERT,
/// the second insert must get rowid 3 (never reuse 1 or 2).
#[test]
#[ignore = "MVCC RowidAllocator does not yet track rowid changes from UPDATE"]
fn test_skips_updated_rowid() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT)")
        .unwrap();

    // First insert gets rowid 1
    conn.execute("INSERT INTO t DEFAULT VALUES").unwrap();
    let rows = get_rows(&conn, "SELECT a FROM t ORDER BY a");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);

    // Update rowid 1 → 2
    conn.execute("UPDATE t SET a = a + 1").unwrap();
    let rows = get_rows(&conn, "SELECT a FROM t ORDER BY a");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 2);

    // Second insert must get rowid > 2 (sequence tracks the high-water mark)
    conn.execute("INSERT INTO t DEFAULT VALUES").unwrap();
    let rows = get_rows(&conn, "SELECT a FROM t ORDER BY a");
    assert_eq!(rows.len(), 2);
    assert!(
        rows[1][0].as_int().unwrap() > 2,
        "second insert rowid should be > 2, got {}",
        rows[1][0].as_int().unwrap()
    );
}

/// What this test checks: The implementation maintains the intended invariant for this scenario.
/// Why this matters: The invariant protects correctness across commit, replay, and query execution paths.
#[test]
fn test_mvcc_integrity_check() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY)")
        .unwrap();

    // we insert with default values
    conn.execute("INSERT INTO t values(1)").unwrap();

    let ensure_integrity = || {
        let rows = get_rows(&conn, "PRAGMA integrity_check");
        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][0].cast_text().unwrap(), "ok");
    };

    ensure_integrity();

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    ensure_integrity();
}

#[test]
fn test_checkpoint_index_writer_overwrites_existing_interior_key() {
    fn run_pager_until_done<T>(
        mut action: impl FnMut() -> IOResultOr<T>,
        pager: &Pager,
    ) -> Result<T> {
        loop {
            match action()? {
                IOResult::Done(value) => return Ok(value),
                IOResult::IO(io) => io.wait(pager.io.as_ref())?,
            }
        }
    }

    let db = MvccTestDb::new();
    let pager = db.conn.pager.load().clone();
    let index = crate::schema::Index {
        name: "testindex".to_string(),
        table_name: "test".to_string(),
        root_page: 0,
        columns: IndexColumn::new_many(vec!["id"]),
        unique: true,
        ephemeral: false,
        has_rowid: true,
        where_clause: None,
        index_method: None,
        on_conflict: None,
    };

    pager.begin_read_tx().unwrap();
    run_pager_until_done(
        || pager.begin_write_tx(crate::storage::wal::WalAutoActions::all_enabled()),
        pager.as_ref(),
    )
    .unwrap();
    let root_page = pager
        .io
        .block(|| pager.btree_create(&crate::storage::pager::CreateBTreeFlags::new_index()))
        .unwrap() as i64;
    let cursor = Arc::new(RwLock::new(
        BTreeCursor::new_index(pager.clone(), root_page, &index, index.columns.len()).unwrap(),
    ));

    for key in 1..=600 {
        let record =
            ImmutableRecord::from_values(&[Value::from_i64(key), Value::from_i64(key)], 2).unwrap();
        let seek_result = run_pager_until_done(
            || {
                cursor.write().seek(
                    crate::types::SeekKey::IndexKey(record.as_record_ref()),
                    crate::types::SeekOp::GE { eq_only: true },
                )
            },
            pager.as_ref(),
        )
        .unwrap();
        if matches!(seek_result, SeekResult::TryAdvance) {
            run_pager_until_done(|| cursor.write().next(), pager.as_ref()).unwrap();
        }
        run_pager_until_done(
            || {
                cursor
                    .write()
                    .insert(&BTreeKey::new_index_key(record.as_record_ref()))
            },
            pager.as_ref(),
        )
        .unwrap();
    }
    run_pager_until_done(
        || pager.commit_tx(&db.conn, db.conn.get_sync_mode(), true),
        pager.as_ref(),
    )
    .unwrap();

    pager.begin_read_tx().unwrap();
    let mut interior_key = None;
    for key in 1..=600 {
        let record =
            ImmutableRecord::from_values(&[Value::from_i64(key), Value::from_i64(key)], 2).unwrap();
        let seek_result = run_pager_until_done(
            || {
                cursor.write().seek(
                    crate::types::SeekKey::IndexKey(record.as_record_ref()),
                    crate::types::SeekOp::GE { eq_only: true },
                )
            },
            pager.as_ref(),
        )
        .unwrap();
        if matches!(seek_result, SeekResult::TryAdvance) {
            interior_key = Some(key);
            break;
        }
    }
    let interior_key = interior_key.expect("test setup should create an index interior key");
    let count_before = run_pager_until_done(|| cursor.write().count(), pager.as_ref()).unwrap();

    run_pager_until_done(
        || pager.begin_write_tx(crate::storage::wal::WalAutoActions::all_enabled()),
        pager.as_ref(),
    )
    .unwrap();
    let index_info = Arc::new(IndexInfo::new_from_index(&index).unwrap());
    let record = ImmutableRecord::from_values(
        &[Value::from_i64(interior_key), Value::from_i64(interior_key)],
        2,
    )
    .unwrap();
    let row_key =
        SortableIndexKey::new_from_payload_in(&record, index_info, crate::alloc::TursoAllocator)
            .unwrap();
    let row = Row::new_index_row(
        RowID::new(MVTableId::new(-42), RowKey::Record(Arc::new(row_key))),
        index.columns.len(),
    );
    let mut write_row_sm = db
        .mvcc_store
        .write_row_to_pager(&row, cursor.clone(), true)
        .unwrap();
    loop {
        match write_row_sm.step(&()).unwrap() {
            IOResult::Done(()) => break,
            IOResult::IO(io) => io.wait(pager.io.as_ref()).unwrap(),
        }
    }
    run_pager_until_done(
        || pager.commit_tx(&db.conn, db.conn.get_sync_mode(), true),
        pager.as_ref(),
    )
    .unwrap();

    pager.begin_read_tx().unwrap();
    let count_after = run_pager_until_done(|| cursor.write().count(), pager.as_ref()).unwrap();
    assert_eq!(
        count_after, count_before,
        "checkpoint index writer should overwrite an existing interior key, not insert a duplicate"
    );
}

#[test]
fn test_sql_checkpoint_reinsert_existing_interior_index_key_keeps_sqlite_integrity() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path.as_ref().unwrap().clone();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("CREATE TABLE t(payload BLOB, id INTEGER UNIQUE)")
        .unwrap();

    for id in 1..=600 {
        conn.execute(format!(
            "INSERT INTO t(rowid, payload, id) VALUES ({id}, x'70796c6f6164', {id})"
        ))
        .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    for id in 1..=600 {
        conn.execute(format!("DELETE FROM t WHERE id = {id}"))
            .unwrap();
        conn.execute(format!(
            "INSERT INTO t(rowid, payload, id) VALUES ({id}, x'7265696e73657274', {id})"
        ))
        .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.execute("PRAGMA journal_mode = 'wal'").unwrap();

    conn.close().unwrap();
    force_close_for_artifact_tamper(&mut db);

    let sqlite = rusqlite::Connection::open(db_path).unwrap();
    let integrity: String = sqlite
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
}

fn assert_integrity_ok(conn: &Arc<Connection>) {
    let rows = get_rows(conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1, "integrity_check rows: {rows:?}");
    assert_eq!(rows[0][0].to_string(), "ok");
}

fn setup_mvcc_checkpointed_indexed_table(conn: &Arc<Connection>, with_b_index: bool) {
    conn.execute("PRAGMA mvcc_gc_threshold = 1").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a, b)")
        .unwrap();
    conn.execute("CREATE INDEX t_a ON t(a)").unwrap();
    if with_b_index {
        conn.execute("CREATE INDEX t_b ON t(b)").unwrap();
    }
    conn.execute(
        "INSERT INTO t VALUES
            (1,10,100),(2,20,200),(3,30,300),(4,40,400),(5,50,500)",
    )
    .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
}

fn assert_checkpointed_replace_delete_result(conn: &Arc<Connection>) {
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    assert_integrity_ok(conn);

    let rows = get_rows(conn, "SELECT id,a,b FROM t ORDER BY id");
    assert_eq!(rows.len(), 4);
    for (idx, row) in rows.iter().enumerate() {
        let id = (idx as i64) + 1;
        assert_eq!(
            row,
            &vec![
                Value::from_i64(id),
                Value::from_i64(id * 10),
                Value::from_i64(id * 100)
            ]
        );
    }
}

#[test]
fn test_mvcc_checkpoint_insert_or_replace_then_delete_removes_checkpointed_index_entries() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    setup_mvcc_checkpointed_indexed_table(&conn, false);

    conn.execute("INSERT OR REPLACE INTO t(id,a,b) VALUES(5,25,325)")
        .unwrap();
    conn.execute("DELETE FROM t WHERE id=5").unwrap();

    assert_checkpointed_replace_delete_result(&conn);
}

#[test]
fn test_mvcc_checkpoint_update_or_replace_then_delete_removes_checkpointed_index_entries() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    setup_mvcc_checkpointed_indexed_table(&conn, true);

    conn.execute("UPDATE OR REPLACE t SET a=25,b=325 WHERE id=5")
        .unwrap();
    conn.execute("DELETE FROM t WHERE id=5").unwrap();

    assert_checkpointed_replace_delete_result(&conn);
}

#[test]
fn test_mvcc_repeated_delete_after_replace_delete_checkpoint_is_noop() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    setup_mvcc_checkpointed_indexed_table(&conn, false);

    conn.execute("INSERT OR REPLACE INTO t(id,a,b) VALUES(5,25,325)")
        .unwrap();
    conn.execute("DELETE FROM t WHERE id=5").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    conn.execute("DELETE FROM t WHERE id=5").unwrap();

    assert_integrity_ok(&conn);
    let rows = get_rows(&conn, "SELECT id,a,b FROM t ORDER BY id");
    assert_eq!(rows.len(), 4);
}

#[test]
fn test_mvcc_checkpoint_reopen_text_pk_upsert_delete_removes_autoindex_entry() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let mut conn = db.connect();
    conn.execute("PRAGMA journal_mode=mvcc").unwrap();
    conn.execute("CREATE TABLE t(k TEXT PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES('k1','orig')").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES('k1','u1') ON CONFLICT(k) DO UPDATE SET v=excluded.v")
        .unwrap();
    conn.execute("INSERT INTO t VALUES('k1','u2') ON CONFLICT(k) DO UPDATE SET v=excluded.v")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    conn.close().unwrap();
    drop(conn);
    db.restart();
    conn = db.connect();
    conn.execute("PRAGMA journal_mode=mvcc").unwrap();
    conn.execute("DELETE FROM t WHERE k='k1'").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    assert_eq!(
        get_rows(&conn, "SELECT count(*) FROM t WHERE k='k1'"),
        vec![vec![Value::from_i64(0)]]
    );
    assert!(get_rows(&conn, "SELECT quote(k), quote(v) FROM t").is_empty());
    assert!(get_rows(
        &conn,
        "SELECT quote(k), quote(v) FROM t NOT INDEXED WHERE k='k1'"
    )
    .is_empty());
    assert_integrity_ok(&conn);
}

#[test]
fn test_mvcc_checkpoint_integrity_after_upsert_with_secondary_indexes() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute(
        "CREATE TABLE t(\
            id INTEGER PRIMARY KEY,\
            owner TEXT NOT NULL,\
            portable_changes TEXT NOT NULL,\
            rev INTEGER NOT NULL DEFAULT 0,\
            note TEXT,\
            bucket INTEGER NOT NULL DEFAULT 0,\
            tag TEXT,\
            status INTEGER NOT NULL DEFAULT 0\
        )",
    )
    .unwrap();
    conn.execute("CREATE INDEX t_owner_rev_idx ON t(owner, rev)")
        .unwrap();
    conn.execute("CREATE INDEX t_note_idx ON t(note)").unwrap();
    conn.execute("CREATE INDEX t_bucket_idx ON t(bucket)")
        .unwrap();
    conn.execute("CREATE INDEX t_tag_idx ON t(tag)").unwrap();
    conn.execute("CREATE INDEX t_status_idx ON t(status)")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    for id in 1..=200 {
        conn.execute(format!(
            "INSERT INTO t(id, owner, portable_changes, rev, note, bucket, tag, status) \
             VALUES ({id}, 'owner-{id}', 'portable_changes-{id}', 1, 'note-{id}', {bucket}, 'tag-{tag}', {status}) \
             ON CONFLICT(id) DO UPDATE SET \
                id = excluded.id, \
                owner = excluded.owner, \
                portable_changes = excluded.portable_changes, \
                rev = excluded.rev, \
                note = excluded.note, \
                bucket = excluded.bucket, \
                tag = excluded.tag, \
                status = excluded.status",
            bucket = id % 17,
            tag = id % 7,
            status = id % 9,
        ))
        .unwrap();
    }

    for id in 1..=100 {
        conn.execute(format!(
            "INSERT INTO t(id, owner, portable_changes, rev, note, bucket, tag, status) \
             VALUES ({id}, 'owner-{id}', 'portable_changes-{id}-updated', 2, 'note-{id}-updated', {bucket}, 'tag-{tag}-updated', {status}) \
             ON CONFLICT(id) DO UPDATE SET \
                id = excluded.id, \
                owner = excluded.owner, \
                portable_changes = excluded.portable_changes, \
                rev = excluded.rev, \
                note = excluded.note, \
                bucket = excluded.bucket, \
                tag = excluded.tag, \
                status = excluded.status",
            bucket = (id + 3) % 17,
            tag = (id + 2) % 7,
            status = (id + 1) % 9,
        ))
        .unwrap();
    }

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_mvcc_cached_insert_reprepared_after_index_create() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, owner TEXT, rev INTEGER, portable_changes TEXT)",
    )
    .unwrap();
    let mut insert = conn
        .prepare(
            "INSERT INTO t(id, owner, rev, portable_changes) VALUES (1, 'a', 1, 'before') \
             ON CONFLICT(id) DO UPDATE SET owner = excluded.owner, rev = excluded.rev, portable_changes = excluded.portable_changes",
        )
        .unwrap();
    insert.run_ignore_rows().unwrap();

    conn.execute("CREATE INDEX t_owner_rev_idx ON t(owner, rev)")
        .unwrap();

    insert.reset().unwrap();
    insert.run_ignore_rows().unwrap();
    conn.execute("INSERT INTO t(id, owner, rev, portable_changes) VALUES (2, 'b', 1, 'after')")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_mvcc_integrity_after_mixed_dml_create_index_transaction() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute(
        "CREATE TABLE t(\
            id INTEGER PRIMARY KEY,\
            owner TEXT NOT NULL,\
            portable_changes TEXT NOT NULL,\
            rev INTEGER NOT NULL DEFAULT 0,\
            note TEXT,\
            bucket INTEGER NOT NULL DEFAULT 0,\
            tag TEXT,\
            status INTEGER NOT NULL DEFAULT 0\
        )",
    )
    .unwrap();
    conn.execute("CREATE INDEX t_owner_rev_idx ON t(owner, rev)")
        .unwrap();
    conn.execute("CREATE INDEX t_note_idx ON t(note)").unwrap();
    conn.execute("CREATE INDEX t_bucket_idx ON t(bucket)")
        .unwrap();
    conn.execute("CREATE INDEX t_tag_idx ON t(tag)").unwrap();
    conn.execute("CREATE INDEX t_status_idx ON t(status)")
        .unwrap();
    for id in 1..=13 {
        conn.execute(format!(
            "INSERT INTO t(id, owner, portable_changes, rev, note, bucket, tag, status) \
             VALUES ({id}, 'owner-{id}', 'portable_changes-{id}', 1, 'note-{id}', {bucket}, 'tag-{tag}', {status})",
            bucket = id % 17,
            tag = id % 7,
            status = id % 9,
        ))
        .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    conn.execute("BEGIN IMMEDIATE").unwrap();
    conn.execute("UPDATE t SET portable_changes = 'local-mixed-portable_changes', rev = rev + 1 WHERE id = 1")
        .unwrap();
    conn.execute("CREATE INDEX \"t local mixed idx\" ON t(portable_changes)")
        .unwrap();
    conn.execute(
        "INSERT INTO t(id, owner, portable_changes, rev, note, bucket, tag, status) \
         VALUES (20006, 'replica-1', 'replica-1-mixed-insert', 1, 'replica-1-note-20006', 1, 'tag-0', 1)",
    )
    .unwrap();
    conn.execute("COMMIT").unwrap();

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test that integrity_check passes after DROP TABLE but before checkpoint.
/// Issue #4975: After checkpointing a table and then dropping it, integrity_check
/// would fail because the dropped table's btree pages still exist but aren't
/// tracked by the schema. The fix is to track dropped root pages until checkpoint.
#[test]
fn test_integrity_check_after_drop_table_before_checkpoint() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_t_data ON t(data)").unwrap();

    // Insert data to force page allocation
    for i in 0..10 {
        let data = format!("data_{i}");
        conn.execute(format!("INSERT INTO t VALUES ({i}, '{data}')"))
            .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(conn);

    db.restart();

    let conn = db.connect();

    // Now drop table. Before the fix, this would make integrity_check fail because
    // we dropped the table before checkpointing, meaning integrity_check would find
    // pages not being used since we didn't provide root page of table t for checks.
    conn.execute("DROP TABLE t").unwrap();
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test that integrity_check passes after DROP INDEX but before checkpoint.
/// Issue #4975: After checkpointing an index and then dropping it, integrity_check
/// would fail because the dropped index's btree pages still exist but aren't
/// tracked by the schema. The fix is to track dropped root pages until checkpoint.
#[test]
fn test_integrity_check_after_drop_index_before_checkpoint() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_t_data ON t(data)").unwrap();

    // Insert data to force page allocation
    for i in 0..10 {
        let data = format!("data_{i}");
        conn.execute(format!("INSERT INTO t VALUES ({i}, '{data}')"))
            .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(conn);

    db.restart();

    let conn = db.connect();

    // Now drop index. Before the fix, this would make integrity_check fail because
    // we dropped the index before checkpointing, meaning integrity_check would find
    // pages not being used since we didn't provide root page of index idx_t_data for checks.
    conn.execute("DROP INDEX idx_t_data").unwrap();
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_interrupted_drop_table_rolls_back_schema_table_and_indexes() {
    let io = Arc::new(MemoryIO::new());
    let path = ":memory:interrupted-drop-table-schema-rollback";
    let db = Database::open_file(io.clone(), path, Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();

    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();

    conn.execute("CREATE TABLE repro_target(c0 INTEGER, c1 REAL)")
        .unwrap();
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_repro_target_c0 \
         ON repro_target (c0) WHERE c1 IS NULL",
    )
    .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let target_schema_rows = get_rows(
        &conn,
        "SELECT type, name FROM sqlite_schema \
         WHERE tbl_name = 'repro_target' ORDER BY rowid",
    );
    assert_eq!(target_schema_rows.len(), 2);
    assert_eq!(target_schema_rows[0][0].to_string(), "table");
    assert_eq!(target_schema_rows[0][1].to_string(), "repro_target");
    assert_eq!(target_schema_rows[1][0].to_string(), "index");
    assert_eq!(target_schema_rows[1][1].to_string(), "idx_repro_target_c0");

    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CursorYieldPoint::NextStart.point()
    ])));

    let mut drop_stmt = conn.prepare("DROP TABLE repro_target").unwrap();
    match drop_stmt.step().unwrap() {
        crate::StepResult::Yield => {}
        other => panic!("expected injected yield while dropping repro_target; got {other:?}"),
    }
    conn.set_yield_injector(None);

    let rows = get_rows(&conn, "SELECT 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_string(), "1");

    drop(drop_stmt);
    drop(conn);
    drop(db);

    // Reopening used to fail here because the same-connection SELECT could
    // commit the interrupted DROP TABLE's partial sqlite_schema delete.
    let db = Database::open_file(io, path, Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    let target_schema_rows = get_rows(
        &conn,
        "SELECT type, name FROM sqlite_schema \
         WHERE tbl_name = 'repro_target' ORDER BY rowid",
    );
    assert_eq!(target_schema_rows.len(), 2);
    assert_eq!(target_schema_rows[0][0].to_string(), "table");
    assert_eq!(target_schema_rows[0][1].to_string(), "repro_target");
    assert_eq!(target_schema_rows[1][0].to_string(), "index");
    assert_eq!(target_schema_rows[1][1].to_string(), "idx_repro_target_c0");
}

/// What this test checks: Rollback/savepoint behavior restores exactly the intended state when statements or transactions fail.
/// Why this matters: Partial rollback mistakes leave data in impossible intermediate states.
#[test]
fn test_rollback_with_index() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER UNIQUE)")
        .unwrap();

    // we insert with default values
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t values (1, 1)").unwrap();
    conn.execute("ROLLBACK").unwrap();

    // This query will try to use index to find the row, if we rollback correctly it shouldn't panic
    let rows = get_rows(&conn, "SELECT * FROM t where b = 1");
    assert_eq!(rows.len(), 0);

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

fn try_idxdelete_during_preparing_corruption() -> Option<String> {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup.execute("PRAGMA page_size = 512").unwrap();
    setup
        .execute(
            "CREATE TABLE t(
                a NUMERIC NOT NULL,
                b REAL UNIQUE,
                blob BLOB NOT NULL,
                id INTEGER PRIMARY KEY,
                c NUMERIC,
                u TEXT UNIQUE,
                d REAL
            )",
        )
        .unwrap();
    setup
        .execute(
            "INSERT INTO t VALUES(784, 9.99, zeroblob(8192), 322, 627, 'small_leaf_292', 2.24)",
        )
        .unwrap();
    setup
        .execute("INSERT INTO t VALUES(440, 8.25, zeroblob(8192), 502, 962, 'fast_sun_915', 3.31)")
        .unwrap();
    for filler in 1..=64i64 {
        let rowid = 10_000 + filler;
        setup
            .execute(format!(
                "INSERT INTO t VALUES({}, {}, zeroblob(512), {}, {}, 'seed_{filler}', {})",
                10_000 + filler,
                100_000.0 + filler as f64,
                rowid,
                20_000 + filler,
                (filler % 97) as f64 + 0.01
            ))
            .unwrap();
    }
    setup.execute("PRAGMA data_sync_retry = 1").unwrap();
    setup
        .execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    setup.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    setup.close().unwrap();

    let old = db.connect();
    let deleter = db.connect();
    let victim = db.connect();

    old.execute("BEGIN CONCURRENT").unwrap();
    let _ = get_rows(&old, "SELECT COUNT(*) FROM t WHERE id = 322");

    deleter.execute("DELETE FROM t WHERE id = 322").unwrap();

    for filler in 1..=64i64 {
        let rowid = 10_000 + filler;
        old.execute(format!(
            "UPDATE t SET a = {}, b = {}, blob = zeroblob(512), c = {}, u = 'old_{rowid}', d = {} WHERE id = {rowid}",
            30_000 + filler,
            300_000.0 + filler as f64,
            40_000 + filler,
            (filler % 101) as f64 + 0.02,
        ))
        .unwrap();
    }
    old.execute(
        "UPDATE t SET a = 179, b = 7.75, blob = zeroblob(4194304), c = 453, u = 'hot_hill_935', d = 5.05 WHERE id = 322",
    )
    .unwrap();

    let mv_store = db.get_mvcc_store();
    let old_tx_id = old.get_mv_tx_id().expect("old txn should be active");
    old.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::CommitValidation.point(),
    ])));

    let (at_preparing_tx, at_preparing_rx) = std::sync::mpsc::channel();
    let (proceed_tx, proceed_rx) = std::sync::mpsc::channel();

    let commit_handle = std::thread::spawn(move || {
        let mut commit = old.prepare("COMMIT").unwrap();
        match commit.step().unwrap() {
            crate::StepResult::Yield => {}
            other => panic!("old COMMIT should yield at CommitValidation, got {other:?}"),
        }
        at_preparing_tx.send(()).unwrap();
        proceed_rx.recv().unwrap();
        commit.run_ignore_rows()
    });

    at_preparing_rx.recv().unwrap();

    let saw_preparing = mv_store
        .txs
        .get(&old_tx_id)
        .is_some_and(|entry| matches!(entry.value().state.load(), TransactionState::Preparing(_)));

    victim.execute("BEGIN CONCURRENT").unwrap();
    let victim_delete = victim.execute("DELETE FROM t WHERE id = 322");
    proceed_tx.send(()).unwrap();
    let old_commit = commit_handle.join().unwrap();
    let _ = victim.execute("ROLLBACK");

    if let Err(LimboError::Corrupt(msg)) = victim_delete {
        return Some(msg);
    }

    match victim_delete {
        Ok(_)
        | Err(LimboError::WriteWriteConflict)
        | Err(LimboError::Busy)
        | Err(LimboError::BusySnapshot)
        | Err(LimboError::CommitDependencyAborted) => {}
        other => panic!("unexpected victim DELETE result: {other:?}"),
    }

    assert!(
        saw_preparing,
        "old txn should reach Preparing during COMMIT"
    );
    assert!(
        matches!(old_commit, Err(LimboError::WriteWriteConflict)),
        "stale updater should lose to concurrent delete, got {old_commit:?}"
    );

    assert_integrity_ok(&db.connect());
    None
}

/// Concurrent DELETE while another txn is committing an UPDATE on a row that a
/// third txn already deleted must not corrupt unique indexes.
///
/// Sequence (from idxdelete_speculative_abort_repro):
/// 1. `old` begins and pins a snapshot containing row 322.
/// 2. `deleter` autocommits DELETE of row 322 (MVCC delete; btree unchanged with
///    checkpoint disabled).
/// 3. `old` updates row 322 anyway (stale snapshot), rewriting unique columns.
/// 4. `old` enters `Preparing` during COMMIT.
/// 5. `victim` DELETEs row 322: table cursor reads `old`'s new unique values, but
///    IdxDelete cannot find those keys in the btree/MVCC index → corruption.
#[test]
fn test_delete_during_preparing_update_of_stale_deleted_row_no_idxdelete_corruption() {
    const ATTEMPTS: usize = 20;
    for attempt in 0..ATTEMPTS {
        if let Some(msg) = try_idxdelete_during_preparing_corruption() {
            panic!("DELETE corrupted indexes on attempt {attempt}: {msg}");
        }
    }
}

/// 1. BEGIN CONCURRENT (start interactive transaction)
/// 2. UPDATE modifies col_a's index, then fails constraint check on col_b
/// 3. The partial index changes are NOT rolled back (this is the bug!)
/// 4. COMMIT succeeds, persisting the inconsistent state
/// 5. Later UPDATE on same row fails: "IdxDelete: no matching index entry found"
///    because table row has old value but index has new value
#[test]
fn test_update_multiple_unique_columns_partial_rollback() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    // Create table with multiple unique columns (like blue_sun_77 in the bug)
    conn.execute(
        "CREATE TABLE t(
            id INTEGER PRIMARY KEY,
            col_a TEXT UNIQUE,
            col_b REAL UNIQUE
        )",
    )
    .unwrap();

    // Insert two rows - one to update, one to cause conflict
    conn.execute("INSERT INTO t VALUES (1, 'original_a', 1.0)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'other_a', 2.0)")
        .unwrap();

    // Start an INTERACTIVE transaction - this is KEY to reproducing the bug!
    // In auto-commit mode, the entire transaction is rolled back on error.
    // In interactive mode, only the statement should be rolled back.
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Try to UPDATE row 1 with:
    // - col_a = 'new_a' (index modification happens first)
    // - col_b = 2.0 (should FAIL - conflicts with row 2)
    //
    // The UPDATE bytecode does:
    // 1. Delete old index entry for col_a ('original_a', 1)
    // 2. Insert new index entry for col_a ('new_a', 1)
    // 3. Delete old index entry for col_b (1.0, 1)
    // 4. Check constraint for col_b (2.0) - FAIL with Halt err_code=1555!
    //
    // BUG: Without proper statement rollback, steps 1-3 are committed!
    let result = conn.execute("UPDATE t SET col_a = 'new_a', col_b = 2.0 WHERE id = 1");
    assert!(
        result.is_err(),
        "Expected unique constraint violation on col_b"
    );

    // COMMIT the transaction - this is what the stress test does after the error!
    // In the buggy case, this commits the partial index changes from the failed UPDATE.
    conn.execute("COMMIT").unwrap();

    // Now in a NEW transaction, try to UPDATE the same row.
    // If the previous statement's partial changes were committed:
    // - Table row still has col_a = 'original_a' (UPDATE didn't complete)
    // - But index for col_a now has 'new_a' instead of 'original_a'!
    // - This UPDATE reads 'original_a' from table, tries to delete that index entry
    // - CRASH: "IdxDelete: no matching index entry found for key ['original_a', 1]"
    conn.execute("UPDATE t SET col_a = 'updated_a', col_b = 3.0 WHERE id = 1")
        .unwrap();

    // Verify the update worked
    let rows = get_rows(&conn, "SELECT * FROM t WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1].cast_text().unwrap(), "updated_a");

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

// ─── GC helpers ───────────────────────────────────────────────────────────

fn unique_index_btree_is_allocated(mv: &crate::MvStore) -> bool {
    mv.index_rows
        .iter()
        .any(|idx| mv.is_btree_allocated(idx.key()))
}

/// An index current that no checkpoint has written to the B-tree yet. GC must
/// keep it: the SkipMap is the only copy of that key.
fn skipmap_has_unmaterialized_index_current(mv: &crate::MvStore) -> bool {
    mv.index_rows.iter().any(|idx| {
        idx.value().iter().any(|e| {
            e.value().read().iter().any(|rv| {
                rv.end().is_none() && rv.materialized_at() == crate::mvcc::database::WalPos::ORIGIN
            })
        })
    })
}

fn skipmap_version_count(mv: &crate::MvStore) -> usize {
    mv.rows.iter().map(|e| e.value().read().len()).sum()
}

fn make_rv(begin: Option<TxTimestampOrID>, end: Option<TxTimestampOrID>) -> RowVersion {
    RowVersion {
        id: 0,
        begin: crate::mvcc::database::PackedTs::pack(begin),
        end: crate::mvcc::database::PackedTs::pack(end),
        row: generate_simple_string_row((-2).into(), 1, "gc_test"),
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    }
}

/// [`make_rv`] for a version a checkpoint already wrote to the B-tree, which is
/// what Rule 3 requires before it may drop the last SkipMap copy.
fn make_materialized_rv(
    begin: Option<TxTimestampOrID>,
    end: Option<TxTimestampOrID>,
) -> RowVersion {
    let mut rv = make_rv(begin, end);
    rv.set_materialized_at(crate::mvcc::database::WalPos {
        checkpoint_seq: 1,
        frame: 1,
    });
    rv
}

fn ts(v: u64) -> Option<TxTimestampOrID> {
    Some(TxTimestampOrID::Timestamp(v))
}

fn txid(v: u64) -> Option<TxTimestampOrID> {
    Some(TxTimestampOrID::TxID(v))
}

// ─── GC unit tests ───────────────────────────────────────────────────────

#[test]
/// Rolled-back transactions leave versions with begin=None, end=None. These are
/// invisible to every transaction and must be removed unconditionally by Rule 1.
fn test_gc_rule1_aborted_garbage_removed() {
    let mut versions = crate::alloc::vec![make_rv(None, None)];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        0,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 1);
    assert!(versions.is_empty());
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// Rule 1 removes only aborted garbage, leaving live and superseded versions intact.
fn test_gc_rule1_aborted_among_live_versions() {
    let mut versions = crate::alloc::vec![
        make_rv(ts(5), None),  // current
        make_rv(None, None),   // aborted
        make_rv(ts(3), ts(5)), // superseded
    ];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        2,
        0,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    // Only aborted removed; superseded has e=5 > lwm=2 so retained
    assert_eq!(dropped, 1);
    assert_eq!(versions.len(), 2);
    assert!(versions
        .iter()
        .all(|rv| rv.begin().is_some() || rv.end().is_some()));
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// A superseded version whose end timestamp is at or below the low-water mark is
/// invisible to all active readers. When a committed current version exists to
/// take over B-tree invalidation, the superseded version is safely removable.
fn test_gc_rule2_superseded_below_lwm_with_current() {
    // Superseded version (end=Timestamp(3)) below LWM=10, and there's a current version.
    let mut versions = crate::alloc::vec![
        make_rv(ts(3), ts(5)), // superseded, e=5 <= lwm=10
        make_rv(ts(5), None),  // current
    ];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        0,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 1);
    assert_eq!(versions.len(), 1);
    assert!(versions[0].end().is_none()); // only current remains
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// A superseded version whose end timestamp exceeds the LWM may still be visible
/// to an active reader. It must be retained regardless of other conditions.
fn test_gc_rule2_superseded_above_lwm_retained() {
    // Superseded version (end=Timestamp(15)) above LWM=10 — must be retained.
    let mut versions = crate::alloc::vec![make_rv(ts(3), ts(15)), make_rv(ts(15), None)];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        0,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 2);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// When a row was deleted but the deletion hasn't been checkpointed to the B-tree
/// yet (e > ckpt_max), the tombstone is the only thing hiding the stale B-tree
/// row. Removing it would resurrect a deleted row. Must be retained.
fn test_gc_rule2_tombstone_guard_uncheckpointed() {
    // Tombstone: end is set, no current version, and e > ckpt_max.
    // Must be retained to prevent row resurrection via dual cursor.
    let mut versions = crate::alloc::vec![
        make_rv(ts(3), ts(5)), // tombstone (sole version, no current)
    ];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        2,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    // e=5 > ckpt_max=2, no current → tombstone guard retains it
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// Once the deletion has been checkpointed (e <= ckpt_max), the B-tree no longer
/// contains the row, so the tombstone is safe to remove.
fn test_gc_rule2_tombstone_guard_checkpointed() {
    // Tombstone with e <= ckpt_max — deletion is checkpointed, safe to remove.
    let mut versions = crate::alloc::vec![make_rv(ts(3), ts(5))];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    // e=5 <= ckpt_max=5, e=5 <= lwm=10 → removable
    assert_eq!(dropped, 1);
    assert!(versions.is_empty());
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// A current version that's been checkpointed to B-tree, with no other versions in
/// the chain and no transaction open at all, is redundant. The dual cursor will
/// fall through to the B-tree which has identical data. Safe to remove.
fn test_gc_rule3_drop_current_when_in_btree() {
    // Single stamped current with b <= ckpt_max, and no transaction open
    // (lwm == u64::MAX).
    let mut versions = crate::alloc::vec![make_materialized_rv(ts(5), None)];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 1);
    assert!(versions.is_empty());
}

/// Truncate Rule 3 matches Passive: sole stamped currents clear only when idle.
#[test]
fn test_gc_rule3_truncate_idle_only() {
    let mut versions = crate::alloc::vec![make_materialized_rv(ts(5), None)];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0, "open snapshot must block Truncate Rule 3 clear");
    assert_eq!(versions.len(), 1);

    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 1);
    assert!(versions.is_empty());
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// A current version not yet checkpointed (b > ckpt_max) cannot be removed —
/// the B-tree doesn't have the data, so fallthrough would return stale results.
fn test_gc_rule3_not_checkpointed_retained() {
    // Single current version with b > ckpt_max — B-tree doesn't have it yet.
    // Nothing is open (lwm == u64::MAX), so ckpt_max is the only thing holding
    // this version back.
    let mut versions = crate::alloc::vec![make_materialized_rv(ts(5), None)];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        3,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// Any finite LWM means some transaction is still open, and an open snapshot may
/// be running a dual-cursor scan that would lose this row if the chain emptied
/// under it. Rule 3 only fires when nothing is open, so this is retained.
fn test_gc_rule3_visible_to_active_tx_retained() {
    // Single stamped current, already inside ckpt_max, but a transaction is open.
    let mut versions = crate::alloc::vec![make_materialized_rv(ts(5), None)];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        5,
        10,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    // lwm=5 != u64::MAX, so a transaction is open and Rule 3 must not fire.
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// A current version cannot be removed before checkpoint has persisted it.
fn test_gc_rule3_current_retained_before_first_checkpoint() {
    let mut versions = crate::alloc::vec![make_materialized_rv(ts(1), None)];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        0,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// Once checkpoint has persisted a sole current version, it becomes GC-eligible.
fn test_gc_rule3_current_collected_after_checkpoint() {
    let mut versions = crate::alloc::vec![make_materialized_rv(ts(1), None)];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 1);
    assert_eq!(versions.len(), 0);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// Rule 3 only drops the last remaining current version. After Rule 2 removes
/// superseded history, Rule 3 can then drop that current.
fn test_gc_rule3_after_history_reclaimed() {
    // Rule 3 only fires when exactly one version remains after rules 1 & 2.
    let mut versions = crate::alloc::vec![make_rv(ts(3), ts(5)), make_materialized_rv(ts(5), None)];
    // Both versions are inside ckpt_max and nothing is open, but the chain
    // starts with 2 entries. Rule 2 removes the superseded one
    // (has_current=true), then Rule 3 drops the remaining current.
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 2);
    assert!(versions.is_empty());
}

/// Rule 3 drops a sole current only once a checkpoint stamped it. A high
/// `ckpt_max` says a checkpoint ran, not that this row reached a B-tree leaf.
#[test]
fn test_gc_rule3_keeps_unstamped_current_and_drops_stamped_one() {
    let mut versions = crate::alloc::vec![make_rv(ts(5), None)];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        10,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);

    versions[0].set_materialized_at(crate::mvcc::database::WalPos {
        checkpoint_seq: 1,
        frame: 7,
    });
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        10,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 1);
    assert!(versions.is_empty());
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// Versions referencing an active transaction (begin=TxID) represent uncommitted
/// inserts. They don't match any removal rule and must always be retained.
fn test_gc_txid_refs_retained() {
    // Versions with TxID (uncommitted) references are never collected.
    let mut versions = crate::alloc::vec![make_rv(txid(99), None)];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        u64::MAX,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// Versions with end=TxID represent an uncommitted deletion. Rule 2 only matches
/// end=Timestamp, so these are never collected until the deleting tx resolves.
fn test_gc_txid_end_retained() {
    // end=TxID means the deletion is uncommitted; rule 2 only matches Timestamp.
    let mut versions = crate::alloc::vec![make_rv(ts(3), txid(50))];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        u64::MAX,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 1);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// A pending insert (begin=TxID) must NOT count as a "committed current version"
/// for the tombstone guard. If it rolled back, the tombstone would be the only
/// thing hiding the stale B-tree row, and removing it would resurrect deleted data.
fn test_gc_rule2_pending_insert_does_not_disable_tombstone_guard() {
    // A pending insert (begin=TxID, end=None) coexists with a tombstone.
    // has_current must NOT count the pending insert — if it rolls back,
    // the tombstone is the only thing hiding the B-tree row.
    let mut versions = crate::alloc::vec![
        make_rv(ts(3), ts(5)), // tombstone: deletion at e=5, not checkpointed (ckpt_max=2)
        make_rv(txid(99), None), // pending insert (uncommitted)
    ];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        2,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    // Tombstone must be retained: e=5 > ckpt_max=2, and pending insert doesn't count.
    // Only nothing changes (pending insert is not aborted garbage either).
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 2);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// When a committed current version exists (begin=Timestamp, end=None), it takes
/// over MVCC visibility from a non-B-tree superseded version. The tombstone guard
/// is no longer needed, so the superseded version can be safely removed.
fn test_gc_rule2_committed_current_disables_non_btree_tombstone_guard() {
    // A committed current version (begin=Timestamp, end=None) means the row
    // has a live successor — the tombstone can safely be removed.
    let mut versions = crate::alloc::vec![
        make_rv(ts(3), ts(5)), // superseded, e=5 <= lwm=10
        make_rv(ts(5), None),  // committed current
    ];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        2,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    // Superseded removed (has_current=true for committed version), current remains.
    assert_eq!(dropped, 1);
    assert_eq!(versions.len(), 1);
    assert!(versions[0].end().is_none());
}

/// A B-tree-resident version whose ending timestamp has not been checkpointed
/// still records a required physical B-tree delete or overwrite. A committed
/// replacement can hide it from readers but cannot make it GC-eligible until
/// checkpoint makes that physical change durable.
#[test]
fn test_gc_rule2_btree_resident_marker_with_current_retained_until_checkpoint() {
    let mut tombstone = make_rv(None, ts(5));
    tombstone.btree_resident = true;
    let current = make_materialized_rv(ts(5), None);
    let mut versions = crate::alloc::vec![tombstone, current.clone()];

    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        2,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 2);
    assert!(versions[0].btree_resident);

    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 2);
    assert!(versions.is_empty());

    let mut rewritten_btree_row = make_rv(ts(3), ts(5));
    rewritten_btree_row.btree_resident = true;
    let mut versions = crate::alloc::vec![rewritten_btree_row, current];

    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        2,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 2);
    assert!(versions[0].btree_resident);

    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 2);
    assert!(versions.is_empty());
}

/// A superseded version whose insert was checkpointed (begin <= ckpt_max) is
/// physically in the B-tree even when its `btree_resident` flag is unset (the
/// flag is only seeded by the dual cursor, not by checkpoint). Until its
/// overwrite/delete is checkpointed (end > ckpt_max), it must be retained even
/// with a committed current replacement: the checkpointer derives DB-file
/// existence from begin/end timestamps against the durable boundary, so
/// removing this version would make a later delete skip the B-tree write and
/// resurrect the row (issue #7638).
#[test]
fn test_gc_rule2_checkpointed_insert_with_current_retained_until_checkpoint() {
    // begin=2 <= ckpt_max=2 (insert checkpointed), end=5 > ckpt_max (overwrite not).
    let checkpointed_btree_row = make_rv(ts(2), ts(5));
    let current = make_materialized_rv(ts(5), None);
    let mut versions = crate::alloc::vec![checkpointed_btree_row, current];

    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        2,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 2);

    // Once the overwrite is checkpointed (end <= ckpt_max), both versions are
    // reclaimable (rule 2 for the superseded, rule 3 for the current).
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 2);
    assert!(versions.is_empty());
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// B-tree tombstones (begin=None, end=e) represent rows that existed in the B-tree
/// before MVCC was enabled and were then deleted. Before checkpoint writes the
/// deletion, the tombstone hides the stale B-tree row. After checkpoint, it's safe
/// to remove. Tests the full lifecycle: retained → checkpointed → collected.
fn test_gc_rule2_btree_tombstone_lifecycle() {
    // B-tree tombstone: begin=None, end=Timestamp(e) where e > 0.
    // Represents a row deleted in MVCC that existed in B-tree before MVCC.
    // Before checkpoint (ckpt_max < e): tombstone must be retained.
    let mut versions = crate::alloc::vec![make_rv(None, ts(5))];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        3,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0, "tombstone retained: e=5 > ckpt_max=3");
    assert_eq!(versions.len(), 1);

    // After checkpoint (ckpt_max >= e): tombstone is collected.
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 1, "tombstone collected: e=5 <= ckpt_max=5");
    assert_eq!(versions.len(), 0);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// Rule 3 must never fire when superseded versions remain in the chain — removing
/// the current version would leave orphaned superseded versions that "poison" the
/// dual cursor, making it hide the B-tree row without providing a replacement.
fn test_gc_rule3_not_firing_with_unremovable_superseded() {
    // Two versions: superseded with e > lwm (can't remove), and current.
    // Rule 2 can't remove the superseded one, so 2 versions remain.
    // Rule 3 needs a single remaining current, so it must NOT fire.
    let mut versions = crate::alloc::vec![
        make_rv(ts(3), ts(15)),             // e=15 > lwm=10 — retained
        make_materialized_rv(ts(15), None), // current
    ];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        20,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
    assert_eq!(versions.len(), 2);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// GC on an empty version chain is a no-op. Verifies no panics or off-by-one errors.
fn test_gc_noop_on_empty() {
    let mut versions: RowVersionChain<TursoAllocator> = crate::alloc::vec![];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        10,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 0);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// All three rules fire together with nothing open: aborted garbage (Rule 1), two
/// checkpointed superseded versions with a committed current (Rule 2), and the sole
/// surviving stamped current inside `ckpt_max` (Rule 3). The chain is fully reclaimed.
fn test_gc_combined_rules() {
    let mut versions = crate::alloc::vec![
        make_rv(None, None),   // aborted → rule 1
        make_rv(ts(1), ts(3)), // superseded, e=3 <= ckpt_max=5 → rule 2 (has_current=true)
        make_rv(ts(3), ts(5)), // superseded, e=5 <= ckpt_max=5 → rule 2
        // current, stamped, b=5 <= ckpt_max=5, nothing open → rule 3
        make_materialized_rv(ts(5), None),
    ];
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        u64::MAX,
        5,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 4);
    assert!(versions.is_empty());
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// End-to-end at the MvStore level: insert a row, commit, and run GC. Without a
/// checkpoint the version is not yet in the B-tree, so Rule 3 doesn't fire and
/// the version survives. Verifies the full insert→commit→GC pipeline.
fn test_gc_integration_insert_commit_gc() {
    let db = MvccTestDb::new();

    // Insert and commit a row.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = generate_simple_string_row((-2).into(), 1, "gc_test");
    db.mvcc_store.insert(tx1, row).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // Row should be in the MvStore.
    assert!(!db.mvcc_store.rows.is_empty());

    // No active transactions → LWM = u64::MAX.
    // ckpt_max = 0 (no checkpoint yet), so rule 3 won't fire (b > ckpt_max).
    let dropped = db.mvcc_store.drop_unused_row_versions();
    assert_eq!(dropped, 0);
    assert!(!db.mvcc_store.rows.is_empty());
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// Rolling back a transaction leaves aborted garbage (begin=None, end=None).
/// GC reclaims the versions. The SkipMap entry stays (lazy removal to avoid
/// TOCTOU with concurrent writers) but the version vec is empty.
fn test_gc_integration_rollback_creates_aborted_garbage() {
    let db = MvccTestDb::new();

    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = generate_simple_string_row((-2).into(), 1, "will_rollback");
    db.mvcc_store.insert(tx1, row).unwrap();
    db.mvcc_store.rollback_tx(
        tx1,
        db.conn.pager.load().clone(),
        &db.conn,
        crate::MAIN_DB_ID,
    );

    // Rollback should leave aborted garbage (begin=None, end=None).
    let entry = db
        .mvcc_store
        .rows
        .get(&RowID::new((-2).into(), RowKey::Int(1)));
    assert!(entry.is_some());
    {
        let versions = entry.as_ref().unwrap().value().read();
        assert_eq!(versions.len(), 1);
        assert!(versions[0].begin().is_none());
        assert!(versions[0].end().is_none());
    }

    // GC should clean up the version. The SkipMap entry stays (lazy removal
    // in background GC avoids TOCTOU), but the version vec should be empty.
    let dropped = db.mvcc_store.drop_unused_row_versions();
    assert_eq!(dropped, 1);
    let entry = db
        .mvcc_store
        .rows
        .get(&RowID::new((-2).into(), RowKey::Int(1)));
    assert!(entry.is_some(), "SkipMap entry stays (lazy removal)");
    assert!(
        entry.unwrap().value().read().is_empty(),
        "but versions should be empty"
    );
}

/// GC trims chains with retain()/clear(), which keeps the Vec's allocation.
/// After a burst of versions is collected, the chain's capacity must be
/// released down to a quarter of its previous value (deliberately not to fit)
/// so a hot row doesn't pin its peak allocation forever.
#[test]
fn test_gc_shrinks_version_chain_capacity() {
    let make_version = |begin, end| RowVersion {
        id: 0,
        begin: crate::mvcc::database::PackedTs::pack(begin),
        end: crate::mvcc::database::PackedTs::pack(end),
        row: generate_simple_string_row((-2).into(), 1, "shrink"),
        btree_resident: false,
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    };

    // One committed current version that survives GC (b=1 > ckpt_max=0, so
    // rule 3 doesn't fire), plus a burst of aborted garbage (always removed).
    let mut versions: RowVersionChain<TursoAllocator> =
        std::iter::once(make_version(Some(TxTimestampOrID::Timestamp(1)), None))
            .chain((0..1023).map(|_| make_version(None, None)))
            .try_collect()
            .unwrap();
    let capacity_before = versions.capacity();
    assert!(capacity_before >= 1024);

    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        0,
        0,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 1023);
    assert_eq!(versions.len(), 1);
    assert!(
        versions.capacity() <= capacity_before / 4,
        "chain capacity should shrink to a quarter of {capacity_before}, got {}",
        versions.capacity()
    );

    // A chain emptied entirely also releases its allocation.
    let mut versions: RowVersionChain<TursoAllocator> = (0..1024)
        .map(|_| make_version(None, None))
        .try_collect()
        .unwrap();
    let capacity_before = versions.capacity();
    let dropped = MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        0,
        0,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert_eq!(dropped, 1024);
    assert!(versions.is_empty());
    assert!(
        versions.capacity() <= capacity_before / 4,
        "empty chain capacity should shrink to a quarter of {capacity_before}, got {}",
        versions.capacity()
    );

    // Small chains are not worth a realloc: capacity at or below the minimum
    // threshold is left untouched even when fully emptied.
    let mut small: RowVersionChain<TursoAllocator> =
        <RowVersionChain<TursoAllocator> as TursoTryWithCapacityExt>::try_with_capacity_ext(16)
            .unwrap();
    small.push(make_version(None, None));
    let capacity_before = small.capacity();
    MvStore::<MvccClock>::gc_version_chain(
        &mut small,
        0,
        0,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    assert!(small.is_empty());
    assert_eq!(small.capacity(), capacity_before);
}

/// `drop_unused_row_versions_and_slots` removes emptied SkipMap entries.
#[test]
fn test_gc_with_slot_removal_drops_empty_skipmap_entries() {
    let db = MvccTestDb::new();

    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = generate_simple_string_row((-2).into(), 1, "will_rollback");
    db.mvcc_store.insert(tx1, row).unwrap();
    db.mvcc_store.rollback_tx(
        tx1,
        db.conn.pager.load().clone(),
        &db.conn,
        crate::MAIN_DB_ID,
    );

    // Rollback leaves aborted garbage behind in the chain.
    let row_id = RowID::new((-2).into(), RowKey::Int(1));
    assert!(db.mvcc_store.rows.get(&row_id).is_some());

    // The slot-removing GC variant collects the garbage AND drops the slot.
    // No concurrent writers exist in this test, satisfying the caller contract.
    let dropped = db.mvcc_store.drop_unused_row_versions_and_slots();
    assert_eq!(dropped, 1);
    assert!(
        db.mvcc_store.rows.get(&row_id).is_none(),
        "empty chain slot should be removed from the SkipMap"
    );
}

/// The low-water mark (LWM) is the minimum begin_ts of all active readers. GC
/// must not remove any version that an active reader might still need. This test
/// opens a reader, writes a new version that supersedes the reader's snapshot,
/// and runs GC — the old version must survive. After the reader closes, GC runs
/// again and reclaims it. This is the core safety property of LWM-based GC.
#[test]
fn test_gc_active_reader_pins_lwm() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    // T1 inserts a row and commits.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row_v1 = generate_simple_string_row(table_id, 1, "version_1");
    db.mvcc_store.insert(tx1, row_v1.clone()).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 begins a read transaction — pins LWM at T2's begin_ts.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let tx2_begin_ts = db.mvcc_store.txs.get(&tx2).unwrap().value().begin_ts;

    // T3 updates the row and commits, creating a superseded version.
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.load().clone()).unwrap();
    let row_v2 = generate_simple_string_row(table_id, 1, "version_2");
    db.mvcc_store.update(tx3, row_v2).unwrap();
    commit_tx(db.mvcc_store.clone(), &conn3, tx3).unwrap();

    // LWM should be T2's begin_ts (the active reader).
    let lwm = db.mvcc_store.compute_lwm();
    assert_eq!(
        lwm, tx2_begin_ts,
        "LWM should equal the active reader's begin_ts"
    );

    // GC should NOT remove the superseded version (its end_ts > lwm).
    let row_id = RowID::new(table_id, RowKey::Int(1));
    let dropped = db.mvcc_store.drop_unused_row_versions();
    assert_eq!(
        dropped, 0,
        "GC should not remove versions visible to active reader"
    );
    {
        let entry = db.mvcc_store.rows.get(&row_id).unwrap();
        let versions = entry.value().read();
        assert_eq!(versions.len(), 2, "both versions should be retained");
    }

    // T2 still sees the old version.
    let read_row = db.mvcc_store.read(tx2, &row_id).unwrap().unwrap();
    assert_eq!(
        read_row, row_v1,
        "active reader should still see the old version"
    );

    // Close the reader transaction.
    db.mvcc_store.remove_tx(tx2).unwrap();

    // LWM should now be u64::MAX.
    assert_eq!(db.mvcc_store.compute_lwm(), u64::MAX);

    // GC should now remove the superseded version.
    let dropped = db.mvcc_store.drop_unused_row_versions();
    assert_eq!(
        dropped, 1,
        "superseded version should be reclaimed after reader closes"
    );
    {
        let entry = db.mvcc_store.rows.get(&row_id).unwrap();
        let versions = entry.value().read();
        assert_eq!(versions.len(), 1, "only current version should remain");
    }
}

/// The live-version counter is the heuristic that gates inline GC. It must
/// increment on every committed insert/update and decrement when GC reclaims a
/// version. Exactness here is convenient for the test but only approximate
/// accuracy is required by the engine.
#[test]
fn test_live_version_count_approx_tracks_inserts_and_gc() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();
    let start = db.mvcc_store.live_version_count_approx();

    // Insert a row + commit -> one live version.
    let tx = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    db.mvcc_store
        .insert(tx, generate_simple_string_row(table_id, 1, "v1"))
        .unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx).unwrap();
    assert_eq!(db.mvcc_store.live_version_count_approx(), start + 1);

    // Update the row + commit -> a second version (old one superseded, not
    // removed, so the count grows to two).
    let tx = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    db.mvcc_store
        .update(tx, generate_simple_string_row(table_id, 1, "v2"))
        .unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx).unwrap();
    assert_eq!(db.mvcc_store.live_version_count_approx(), start + 2);

    // No active readers -> the superseded version is reclaimable. GC drops it
    // and the counter follows.
    let dropped = db.mvcc_store.drop_unused_row_versions();
    assert_eq!(dropped, 1);
    assert_eq!(db.mvcc_store.live_version_count_approx(), start + 1);
}

/// `should_gc` fires once live-version growth since the last pass crosses the
/// threshold; a pass resets the baseline; -1 disables it entirely.
#[test]
fn test_should_gc_threshold_and_reset() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    db.mvcc_store.set_gc_threshold(5);
    assert_eq!(db.mvcc_store.gc_threshold(), 5);
    assert!(!db.mvcc_store.should_gc());

    // Insert 5 versions in a single open (uncommitted) transaction so we drive
    // the counter directly without going through the commit path's own inline
    // GC trigger.
    let tx = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    for i in 1..=5 {
        db.mvcc_store
            .insert(tx, generate_simple_string_row(table_id, i, "x"))
            .unwrap();
    }
    assert!(
        db.mvcc_store.should_gc(),
        "growth of 5 versions should reach the threshold"
    );

    // A pass resets the baseline (regardless of how much it reclaimed — the
    // open txn pins the LWM, so nothing is reclaimable here).
    db.mvcc_store
        .gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC);
    assert!(
        !db.mvcc_store.should_gc(),
        "baseline should reset after a GC pass"
    );

    // A negative threshold disables inline GC even past the old threshold.
    db.mvcc_store.set_gc_threshold(-1);
    for i in 6..=20 {
        db.mvcc_store
            .insert(tx, generate_simple_string_row(table_id, i, "x"))
            .unwrap();
    }
    assert!(
        !db.mvcc_store.should_gc(),
        "negative threshold disables inline GC"
    );
}

/// The `mvcc_gc_threshold` PRAGMA reads back the configured value and feeds
/// `set_gc_threshold`, mirroring `mvcc_checkpoint_threshold`.
#[test]
fn test_mvcc_gc_threshold_pragma_roundtrip() {
    let db = MvccTestDb::new();

    // Default reads back the engine default.
    let rows = get_rows(&db.conn, "PRAGMA mvcc_gc_threshold");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0].as_int().unwrap(),
        MvStore::<MvccClock>::DEFAULT_GC_VERSION_THRESHOLD
    );

    // Setting it updates the store and reads back.
    db.conn.execute("PRAGMA mvcc_gc_threshold = 1000").unwrap();
    assert_eq!(db.mvcc_store.gc_threshold(), 1000);
    let rows = get_rows(&db.conn, "PRAGMA mvcc_gc_threshold");
    assert_eq!(rows[0][0].as_int().unwrap(), 1000);

    // -1 disables inline GC.
    db.conn.execute("PRAGMA mvcc_gc_threshold = -1").unwrap();
    assert_eq!(db.mvcc_store.gc_threshold(), -1);
    assert!(!db.mvcc_store.should_gc());

    // Values below -1 are rejected.
    assert!(db.conn.execute("PRAGMA mvcc_gc_threshold = -2").is_err());
}

/// Incremental GC must reclaim exactly what the full sweep would, just spread
/// across resumable bounded passes whose cursor wraps at the end.
#[test]
fn test_gc_incremental_reclaims_like_full_sweep() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    // Build many chains, each with one reclaimable superseded version
    // (insert+commit then update+commit).
    let n: i64 = 20;
    for i in 1..=n {
        let tx = db
            .mvcc_store
            .begin_tx(db.conn.pager.load().clone())
            .unwrap();
        db.mvcc_store
            .insert(tx, generate_simple_string_row(table_id, i, "v1"))
            .unwrap();
        commit_tx(db.mvcc_store.clone(), &db.conn, tx).unwrap();

        let tx = db
            .mvcc_store
            .begin_tx(db.conn.pager.load().clone())
            .unwrap();
        db.mvcc_store
            .update(tx, generate_simple_string_row(table_id, i, "v2"))
            .unwrap();
        commit_tx(db.mvcc_store.clone(), &db.conn, tx).unwrap();
    }

    // No active readers -> every superseded version is reclaimable.
    assert_eq!(db.mvcc_store.compute_lwm(), u64::MAX);

    // Drive GC in tiny chunks (3 chains/pass) until it converges: no versions
    // reclaimed AND the cursor has wrapped back to the start.
    let mut total = 0;
    let mut passes = 0;
    loop {
        let dropped = db.mvcc_store.gc_incremental(3);
        total += dropped;
        passes += 1;
        if dropped == 0 && db.mvcc_store.gc_table_cursor.lock().is_none() {
            break;
        }
        assert!(passes < 10_000, "incremental GC failed to converge");
    }
    assert!(
        total >= n as usize,
        "incremental GC should reclaim at least the {n} superseded versions, got {total}"
    );

    // A full sweep now finds nothing: incremental GC already reclaimed
    // everything the full sweep would have.
    assert_eq!(
        db.mvcc_store.drop_unused_row_versions(),
        0,
        "full sweep should find nothing left after incremental GC converges"
    );

    // Each chain keeps exactly its surviving current version.
    for i in 1..=n {
        let entry = db
            .mvcc_store
            .rows
            .get(&RowID::new(table_id, RowKey::Int(i)))
            .unwrap();
        assert_eq!(entry.value().read().len(), 1);
    }
}

/// Concurrency safety. Many connections share one `MvStore` and each commit
/// calls `gc_incremental` with no global lock held, so several threads can run
/// GC at once. The single-flight gate keeps only one pass active, but the
/// stronger guarantee is that concurrent passes can never corrupt a chain or
/// the live-version counter. Hammer GC from many threads, then assert the
/// outcome equals a single full sweep and that the (heuristic) counter still
/// exactly matches the live versions — proving no double/missed decrement.
#[test]
fn test_gc_incremental_concurrent_is_safe() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    // Each row: insert+commit (v1) then update+commit (v2) -> one reclaimable
    // superseded version per chain. No readers stay open, so LWM = MAX.
    let n: i64 = 200;
    for i in 1..=n {
        let tx = db
            .mvcc_store
            .begin_tx(db.conn.pager.load().clone())
            .unwrap();
        db.mvcc_store
            .insert(tx, generate_simple_string_row(table_id, i, "v1"))
            .unwrap();
        commit_tx(db.mvcc_store.clone(), &db.conn, tx).unwrap();

        let tx = db
            .mvcc_store
            .begin_tx(db.conn.pager.load().clone())
            .unwrap();
        db.mvcc_store
            .update(tx, generate_simple_string_row(table_id, i, "v2"))
            .unwrap();
        commit_tx(db.mvcc_store.clone(), &db.conn, tx).unwrap();
    }
    assert_eq!(db.mvcc_store.compute_lwm(), u64::MAX);

    // 8 threads each run many small bounded passes concurrently.
    let mut handles = Vec::new();
    for _ in 0..8 {
        let store = db.mvcc_store.clone();
        handles.push(std::thread::spawn(move || {
            for _ in 0..500 {
                store.gc_incremental(8);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Drain anything the bounded passes didn't reach (single-threaded now).
    while db
        .mvcc_store
        .gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC)
        > 0
    {}

    // A full sweep finds nothing left, and each chain kept exactly its current
    // version — concurrent GC reclaimed precisely what one sweep would.
    assert_eq!(db.mvcc_store.drop_unused_row_versions(), 0);
    for i in 1..=n {
        let entry = db
            .mvcc_store
            .rows
            .get(&RowID::new(table_id, RowKey::Int(i)))
            .unwrap();
        assert_eq!(entry.value().read().len(), 1);
    }

    // The heuristic counter exactly matches the live versions still in memory:
    // no decrement was lost or double-applied under concurrency.
    let table_live: usize = db
        .mvcc_store
        .rows
        .iter()
        .map(|e| e.value().read().len())
        .sum();
    let index_live: usize = db
        .mvcc_store
        .index_rows
        .iter()
        .map(|outer| {
            outer
                .value()
                .iter()
                .map(|inner| inner.value().read().len())
                .sum::<usize>()
        })
        .sum();
    assert_eq!(
        db.mvcc_store.live_version_count_approx(),
        table_live + index_live,
        "live-version counter drifted from actual chain contents"
    );
}

/// Index chains are swept by their own bounded, resumable cursor
/// (`gc_index_cursor`), just like table rows — a single huge index can't force
/// an unbounded pass. Aborted index garbage (Rule 1) is reclaimable
/// unconditionally, so it lets us exercise the index sweep without depending on
/// checkpoint timing. This asserts the sweep is resumable mid-pass and that
/// driving it in tiny chunks reclaims exactly what a full sweep would.
#[test]
fn test_gc_incremental_reclaims_index_chains_resumably() {
    let db = MvccTestDb::new();
    let conn = &db.conn;
    // Drive GC manually: disable both the checkpoint and the inline-GC trigger.
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("PRAGMA mvcc_gc_threshold = -1").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_v ON t(v)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'keep')").unwrap();

    // Insert many indexed rows in one transaction, then roll back: each leaves
    // aborted garbage in its own index chain.
    conn.execute("BEGIN").unwrap();
    for i in 100..200 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, 'g{i}')"))
            .unwrap();
    }
    conn.execute("ROLLBACK").unwrap();

    let count_index_versions = || -> usize {
        db.mvcc_store
            .index_rows
            .iter()
            .map(|outer| {
                outer
                    .value()
                    .iter()
                    .map(|inner| inner.value().read().len())
                    .sum::<usize>()
            })
            .sum()
    };
    let before = count_index_versions();
    assert!(
        before > 8,
        "expected accumulated index garbage chains, got {before}"
    );
    assert_eq!(db.mvcc_store.compute_lwm(), u64::MAX);

    // A single tiny pass is bounded and leaves the index sweep mid-flight —
    // the cursor is parked partway through (there are far more than 4 chains).
    db.mvcc_store.gc_incremental(4);
    assert!(
        db.mvcc_store.gc_index_cursor.lock().is_some(),
        "index sweep should resume from a saved cursor, not restart each pass"
    );

    // Drive to convergence in 4-chain chunks: done when a pass reclaims nothing
    // and both cursors have wrapped back to the start.
    let mut passes = 0;
    loop {
        let dropped = db.mvcc_store.gc_incremental(4);
        passes += 1;
        if dropped == 0
            && db.mvcc_store.gc_table_cursor.lock().is_none()
            && db.mvcc_store.gc_index_cursor.lock().is_none()
        {
            break;
        }
        assert!(passes < 100_000, "incremental GC failed to converge");
    }

    // Everything the full sweep would reclaim is already gone (table + index).
    assert_eq!(db.mvcc_store.drop_unused_row_versions(), 0);
    let after = count_index_versions();
    assert!(
        after < before,
        "index versions should shrink: before={before} after={after}"
    );

    // The surviving committed row is still correct and index-readable.
    let rows = get_rows(conn, "SELECT id FROM t WHERE v = 'keep'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
}

/// Inline GC must never run concurrently with a stop-the-world checkpoint:
/// the checkpoint reads version chains to flush them to the B-tree and removes
/// empty slots, so a concurrent GC mutating those chains would corrupt the
/// on-disk image (e.g. drop an index version before the checkpoint flushed it,
/// leaving a table row without its index entry). `gc_incremental` enforces this
/// by holding the checkpoint read lock; if the checkpoint holds the write lock,
/// GC must skip entirely. Regression test for the Antithesis integrity-check
/// failure ("row N missing from index ...").
#[test]
fn test_gc_incremental_skips_while_checkpoint_holds_write_lock() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    // One reclaimable superseded version (insert+commit, then update+commit).
    let tx = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    db.mvcc_store
        .insert(tx, generate_simple_string_row(table_id, 1, "v1"))
        .unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx).unwrap();
    let tx = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    db.mvcc_store
        .update(tx, generate_simple_string_row(table_id, 1, "v2"))
        .unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx).unwrap();
    // No active txns -> all reader read-locks released, LWM = MAX.
    assert_eq!(db.mvcc_store.compute_lwm(), u64::MAX);

    let row_id = RowID::new(table_id, RowKey::Int(1));

    // Simulate a stop-the-world checkpoint by taking the write lock.
    assert!(
        db.mvcc_store.blocking_checkpoint_lock.write(),
        "no readers should remain after commits, so write lock is acquirable"
    );

    // GC cannot take the read lock -> it must skip and reclaim nothing.
    assert_eq!(
        db.mvcc_store
            .gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC),
        0,
        "GC must not run while a checkpoint holds the write lock"
    );
    assert_eq!(
        db.mvcc_store
            .rows
            .get(&row_id)
            .unwrap()
            .value()
            .read()
            .len(),
        2,
        "superseded version must survive while the checkpoint holds the lock"
    );

    // Once the checkpoint releases, GC runs and reclaims the superseded version.
    db.mvcc_store.blocking_checkpoint_lock.unlock();
    assert_eq!(
        db.mvcc_store
            .gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC),
        1,
        "GC runs once the checkpoint lock is released"
    );
    assert_eq!(
        db.mvcc_store
            .rows
            .get(&row_id)
            .unwrap()
            .value()
            .read()
            .len(),
        1
    );
}

/// Passive incremental GC empties a chain's version vec but leaves the (now empty)
/// SkipMap slot; Finalize `unlink_empty` unlinks slots. Truncate incremental unlinks.
#[test]
fn test_gc_incremental_lazy_leaves_empty_slots() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    let table_id: MVTableId = (-2).into();

    // Aborted insert leaves aborted garbage (begin=None, end=None) behind.
    let tx = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
    mvcc_store
        .insert(tx, generate_simple_string_row(table_id, 1, "rollback"))
        .unwrap();
    mvcc_store.rollback_tx(tx, conn.pager.load().clone(), &conn, crate::MAIN_DB_ID);

    let row_id = RowID::new(table_id, RowKey::Int(1));
    assert!(mvcc_store.rows.get(&row_id).is_some());

    // Drive incremental GC to completion.
    for _ in 0..4 {
        mvcc_store.gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC);
    }

    let entry = mvcc_store.rows.get(&row_id);
    assert!(
        entry.is_some(),
        "Passive lazy path keeps the SkipMap slot in place"
    );
    assert!(
        entry.unwrap().value().read().is_empty(),
        "but the version vec is emptied"
    );
}

/// Overlapping transactions: a committed-and-superseded version is reclaimed by
/// inline incremental GC (no checkpoint), but only after the snapshot that
/// pinned the LWM ends. While the reader is open, GC must not touch its version.
#[test]
fn test_gc_incremental_respects_held_snapshot() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();
    let row_id = RowID::new(table_id, RowKey::Int(1));

    // T1 inserts and commits v1.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row_v1 = generate_simple_string_row(table_id, 1, "version_1");
    db.mvcc_store.insert(tx1, row_v1.clone()).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 opens a read snapshot and pins the LWM.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();

    // T3 updates and commits v2, superseding v1.
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.load().clone()).unwrap();
    db.mvcc_store
        .update(tx3, generate_simple_string_row(table_id, 1, "version_2"))
        .unwrap();
    commit_tx(db.mvcc_store.clone(), &conn3, tx3).unwrap();

    // Incremental GC while T2 is open must NOT reclaim v1.
    assert_eq!(
        db.mvcc_store
            .gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC),
        0,
        "held snapshot pins the LWM; nothing reclaimable"
    );
    assert_eq!(
        db.mvcc_store
            .rows
            .get(&row_id)
            .unwrap()
            .value()
            .read()
            .len(),
        2
    );
    // T2 still sees its snapshot version.
    assert_eq!(db.mvcc_store.read(tx2, &row_id).unwrap().unwrap(), row_v1);

    // Close the snapshot; now incremental GC reclaims the superseded v1
    // without any checkpoint having run.
    db.mvcc_store.remove_tx(tx2).unwrap();
    assert_eq!(
        db.mvcc_store
            .gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC),
        1,
        "superseded version reclaimed by inline GC after snapshot ends"
    );
    assert_eq!(
        db.mvcc_store
            .rows
            .get(&row_id)
            .unwrap()
            .value()
            .read()
            .len(),
        1,
        "only the current version remains"
    );
}

/// An older `BEGIN CONCURRENT` reader keeps seeing a row after GC. A newer
/// connection reads the same value from the B-tree. After the older transaction
/// ends, a later sweep reclaims the SkipMap copies.
#[test]
fn test_gc_current_serves_older_reader_then_reclaims() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let mv = db.get_mvcc_store();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL)")
        .unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let older = db.connect();
    older.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(
        get_rows(&older, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Value::from_i64(10)]]
    );

    conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    assert!(
        skipmap_version_count(&mv) > 0,
        "versions an open transaction can see are not removed"
    );
    assert_eq!(
        get_rows(&older, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Value::from_i64(10)]],
        "older reader keeps seeing row 1"
    );

    let newer = db.connect();
    assert_eq!(
        get_rows(&newer, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Value::from_i64(10)]],
        "newer connection reads the same value from the B-tree"
    );

    older.execute("COMMIT").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 30)").unwrap();
    conn.execute("INSERT INTO t VALUES (4, 40)").unwrap();
    assert_eq!(
        skipmap_version_count(&mv),
        0,
        "SkipMap copies are reclaimed once nobody can see them"
    );
    assert_eq!(
        get_rows(&newer, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![Value::from_i64(1), Value::from_i64(10)],
            vec![Value::from_i64(2), Value::from_i64(20)],
            vec![Value::from_i64(3), Value::from_i64(30)],
            vec![Value::from_i64(4), Value::from_i64(40)],
        ]
    );
}

/// Elle list-append: `INSERT ... ON CONFLICT(key) DO UPDATE` against a unique
/// index whose only copy of the key lives in the SkipMap.
///
/// CREATE TABLE is checkpointed so the unique-index root is readable. The
/// user row is not. `durable_txid_max` is then raised, which used to be enough
/// for Truncate Rule 3 to reclaim that unwritten current; fallthrough then hit
/// an empty unique index (`["r", "k9", []]`) and a later insert forked a second
/// rowid (`incompatible-order`). Unique `WHERE key =` can hide that fork, so
/// the assertion is a table scan of `rowid`.
#[test]
fn test_gc_unwritten_text_pk_upsert_does_not_fork() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let mv = db.get_mvcc_store();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    conn.execute("CREATE TABLE t(key TEXT PRIMARY KEY, vals TEXT NOT NULL DEFAULT '')")
        .unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("INSERT INTO t VALUES ('k9', '1')").unwrap();
    assert!(
        unique_index_btree_is_allocated(&mv),
        "CREATE TABLE must publish the unique index so fallthrough can hit an empty btree"
    );

    let older = db.connect();
    older.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(
        get_rows(&older, "SELECT vals FROM t WHERE key = 'k9'"),
        vec![vec![Value::from_text("1")]]
    );

    mv.durable_txid_max.store(u64::MAX, Ordering::SeqCst);
    for _ in 0..4 {
        mv.gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC);
    }
    mv.drop_unused_row_versions();
    assert!(
        skipmap_has_unmaterialized_index_current(&mv),
        "GC must keep a unique-index current that no checkpoint wrote"
    );

    let newer = db.connect();
    newer.execute("BEGIN CONCURRENT").unwrap();
    let read = get_rows(&newer, "SELECT vals FROM t WHERE key = 'k9'");
    assert_eq!(
        read,
        vec![vec![Value::from_text("1")]],
        "unique lookup must still see k9 after GC"
    );
    newer
        .execute(
            "INSERT INTO t (key, vals) VALUES ('k9', '2') \
             ON CONFLICT(key) DO UPDATE SET vals = CASE \
               WHEN vals = '' THEN '2' \
               ELSE vals || ',' || '2' \
             END",
        )
        .unwrap();
    newer.execute("COMMIT").unwrap();
    older.execute("COMMIT").unwrap();

    let rows = get_rows(&conn, "SELECT rowid, key, vals FROM t ORDER BY rowid");
    assert_eq!(rows.len(), 1, "forked into two rows for k9: {rows:?}");
    assert_eq!(rows[0][1].to_string(), "k9");
    let vals = rows[0][2].to_string();
    assert!(vals.starts_with("1,"), "lost the SkipMap prefix: {vals}");
}

/// Same shape as `test_gc_unwritten_text_pk_upsert_does_not_fork`, but a real
/// checkpoint writes the unique-index key first, so GC may reclaim the chain.
/// Elle list-append must then read its own append back in the same txn instead
/// of `[]` from the B-tree.
#[test]
fn test_gc_reclaimed_text_pk_same_txn_sees_own_append() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let mv = db.get_mvcc_store();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    conn.execute("CREATE TABLE t(key TEXT PRIMARY KEY, vals TEXT NOT NULL DEFAULT '')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES ('k6', '1')").unwrap();
    assert!(
        unique_index_btree_is_allocated(&mv),
        "CREATE TABLE must publish the unique index"
    );

    mv.durable_txid_max.store(u64::MAX, Ordering::SeqCst);
    for _ in 0..4 {
        mv.gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC);
    }
    mv.drop_unused_row_versions();

    let newer = db.connect();
    newer.execute("BEGIN CONCURRENT").unwrap();
    let before = get_rows(&newer, "SELECT vals FROM t WHERE key = 'k6'");
    assert_eq!(
        before,
        vec![vec![Value::from_text("1")]],
        "unique lookup must still see k6 after the chain was reclaimed"
    );
    newer
        .execute(
            "INSERT INTO t (key, vals) VALUES ('k6', '2') \
             ON CONFLICT(key) DO UPDATE SET vals = CASE \
               WHEN vals = '' THEN '2' \
               ELSE vals || ',' || '2' \
             END",
        )
        .unwrap();
    let after = get_rows(&newer, "SELECT vals FROM t WHERE key = 'k6'");
    assert_eq!(
        after,
        vec![vec![Value::from_text("1,2")]],
        "same txn must see its own unique append, got {after:?}"
    );
    newer.execute("COMMIT").unwrap();

    let rows = get_rows(&conn, "SELECT rowid, key, vals FROM t ORDER BY rowid");
    let k6: Vec<_> = rows.iter().filter(|r| r[1].to_string() == "k6").collect();
    assert_eq!(k6.len(), 1, "forked k6: {rows:?}");
    let vals = k6[0][2].to_string();
    assert!(vals.starts_with("1,"), "lost the SkipMap prefix: {vals}");
}

#[test]
fn test_gc_unstamped_text_pk_unique_lookup_still_sees_key() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let mv = db.get_mvcc_store();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    conn.execute("CREATE TABLE t(key TEXT PRIMARY KEY, vals TEXT NOT NULL DEFAULT '')")
        .unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("INSERT INTO t VALUES ('k7', '1')").unwrap();
    assert!(
        unique_index_btree_is_allocated(&mv),
        "CREATE TABLE must publish the unique index so fallthrough can hit an empty btree"
    );

    mv.durable_txid_max.store(u64::MAX, Ordering::SeqCst);
    for _ in 0..4 {
        mv.gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC);
    }
    mv.drop_unused_row_versions();
    assert!(
        skipmap_has_unmaterialized_index_current(&mv),
        "Passive GC must not drop an index current that was never written"
    );

    let read = get_rows(&conn, "SELECT vals FROM t WHERE key = 'k7'");
    assert_eq!(
        read,
        vec![vec![Value::from_text("1")]],
        "unique lookup must still see k7 after unstamped Passive GC"
    );
}

#[test]
fn test_passive_finalize_reclaim_text_pk_unique_lookup_still_sees_key() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let mv = db.get_mvcc_store();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(key TEXT PRIMARY KEY, vals TEXT NOT NULL DEFAULT '')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES ('k0', '11')").unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    assert_eq!(
        mv.debug_gc_snapshot().rows_versions,
        0,
        "with no reader open, Passive Finalize must reclaim the materialized table current"
    );

    let read = get_rows(&conn, "SELECT vals FROM t WHERE key = 'k0'");
    assert_eq!(
        read,
        vec![vec![Value::from_text("11")]],
        "unique lookup must see k0 after SkipMap reclaim, got {read:?}"
    );

    let newer = db.connect();
    newer.execute("BEGIN CONCURRENT").unwrap();
    let newer_read = get_rows(&newer, "SELECT vals FROM t WHERE key = 'k0'");
    assert_eq!(
        newer_read,
        vec![vec![Value::from_text("11")]],
        "a later snapshot must see k0 after reclaim, got {newer_read:?}"
    );
    newer
        .execute(
            "INSERT INTO t (key, vals) VALUES ('k0', '62') \
             ON CONFLICT(key) DO UPDATE SET vals = CASE \
               WHEN vals = '' THEN '62' \
               ELSE vals || ',' || '62' \
             END",
        )
        .unwrap();
    let after = get_rows(&newer, "SELECT vals FROM t WHERE key = 'k0'");
    assert_eq!(
        after,
        vec![vec![Value::from_text("11,62")]],
        "same txn must append onto the reclaimed unique row, got {after:?}"
    );
    newer.execute("COMMIT").unwrap();

    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    assert_eq!(
        mv.debug_gc_snapshot().rows_versions,
        0,
        "second Passive Finalize must reclaim the upserted table current"
    );
    let after_second = get_rows(&conn, "SELECT vals FROM t WHERE key = 'k0'");
    assert_eq!(
        after_second,
        vec![vec![Value::from_text("11,62")]],
        "unique lookup must see both appends after the second reclaim, got {after_second:?}"
    );
}

#[test]
fn test_unique_lookup_survives_passive_checkpoint_mid_seek() {
    use crate::StepResult;
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t(key TEXT PRIMARY KEY, vals TEXT NOT NULL DEFAULT '')")
        .unwrap();
    setup.execute("INSERT INTO t VALUES ('k0', '11')").unwrap();
    setup.execute("INSERT INTO t VALUES ('k1', '70')").unwrap();

    let reader = db.connect();
    reader.execute("BEGIN CONCURRENT").unwrap();
    let k1 = get_rows(&reader, "SELECT vals FROM t WHERE key = 'k1'");
    assert_eq!(k1, vec![vec![Value::from_text("70")]]);

    let injector = FixedYieldInjector::new([CursorYieldPoint::SeekStart.point()]);
    reader.set_yield_injector(Some(injector));
    let mut stmt = reader
        .prepare("SELECT vals FROM t WHERE key = 'k0'")
        .unwrap();
    let io = reader.pager.load().io.clone();
    let mut got_yield = false;
    for _ in 0..200_000 {
        match stmt.step().unwrap() {
            StepResult::Yield => {
                got_yield = true;
                break;
            }
            StepResult::IO => io.step().unwrap(),
            StepResult::Row | StepResult::Done => {
                panic!("unique seek finished before SeekStart yield")
            }
            other => panic!("unexpected reader step before yield: {other:?}"),
        }
    }
    assert!(got_yield, "unique seek must pause at SeekStart");

    let ckpt = db.connect();
    ckpt.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    ckpt.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

    reader.set_yield_injector(None);
    let mut read = None;
    for _ in 0..200_000 {
        match stmt.step().unwrap() {
            StepResult::Row => {
                read = Some(
                    stmt.row()
                        .unwrap()
                        .get_values()
                        .cloned()
                        .collect::<Vec<_>>(),
                );
            }
            StepResult::Done => break,
            StepResult::IO | StepResult::Yield => io.step().unwrap(),
            other => panic!("unexpected reader step after checkpoint: {other:?}"),
        }
    }
    reader.execute("COMMIT").unwrap();
    assert_eq!(
        read,
        Some(vec![Value::from_text("11")]),
        "k0 must survive a Passive checkpoint that ran after SeekStart, got {read:?}"
    );
}

#[test]
fn test_same_txn_unique_lookup_stable_across_passive_checkpoint() {
    use crate::StepResult;
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t(key TEXT PRIMARY KEY, vals TEXT NOT NULL DEFAULT '')")
        .unwrap();
    setup.execute("INSERT INTO t VALUES ('k7', '1')").unwrap();
    setup
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    setup.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

    let reader = db.connect();
    reader.execute("BEGIN CONCURRENT").unwrap();
    let before = get_rows(&reader, "SELECT vals FROM t WHERE key = 'k7'");
    assert_eq!(before, vec![vec![Value::from_text("1")]]);

    let writer = db.connect();
    writer
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    let injector = FixedYieldInjector::new([
        CheckpointYieldPoint::BeforePagerCommit.point(),
        CheckpointYieldPoint::AfterDurableBoundaryAdvanced.point(),
    ]);
    writer.set_yield_injector(Some(injector.clone()));
    let mut upsert = writer
        .prepare(
            "INSERT INTO t (key, vals) VALUES ('k7', '2') \
             ON CONFLICT(key) DO UPDATE SET vals = CASE \
               WHEN vals = '' THEN '2' \
               ELSE vals || ',' || '2' \
             END",
        )
        .unwrap();
    let io = writer.pager.load().io.clone();
    let mut yields = 0u32;
    for _ in 0..200_000 {
        match upsert.step().unwrap() {
            StepResult::Yield => {
                yields += 1;
                let mid = get_rows(&reader, "SELECT vals FROM t WHERE key = 'k7'");
                assert_eq!(
                    mid, before,
                    "pinned snapshot must keep k7 across checkpoint yield {yields}, got {mid:?}"
                );
                if injector.remaining_len() == 0 {
                    break;
                }
                io.step().unwrap();
            }
            StepResult::IO => io.step().unwrap(),
            StepResult::Done => break,
            other => panic!("unexpected upsert/checkpoint step: {other:?}"),
        }
    }
    let after = get_rows(&reader, "SELECT vals FROM t WHERE key = 'k7'");
    reader.execute("COMMIT").unwrap();
    assert_eq!(
        after, before,
        "pinned snapshot must keep k7 after checkpoint, got {after:?}"
    );
    assert!(
        yields >= 1,
        "auto-checkpoint must yield so the reader can probe"
    );
}

#[test]
fn test_same_txn_first_unique_lookup_after_aborted_seek_still_sees_reclaimed_key() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(key TEXT PRIMARY KEY, vals TEXT NOT NULL DEFAULT '')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES ('k0', '76')").unwrap();
    conn.execute("INSERT INTO t VALUES ('k5', '1')").unwrap();
    conn.execute("INSERT INTO t VALUES ('k7', '155')").unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(
        get_rows(&conn, "SELECT vals FROM t WHERE key = 'k5'"),
        vec![vec![Value::from_text("1")]]
    );
    conn.execute("ROLLBACK").unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    let first_k7 = get_rows(&conn, "SELECT vals FROM t WHERE key = 'k7'");
    let k0 = get_rows(&conn, "SELECT vals FROM t WHERE key = 'k0'");
    let k5 = get_rows(&conn, "SELECT vals FROM t WHERE key = 'k5'");
    let k0_again = get_rows(&conn, "SELECT vals FROM t WHERE key = 'k0'");
    let last_k7 = get_rows(&conn, "SELECT vals FROM t WHERE key = 'k7'");
    conn.execute("COMMIT").unwrap();

    assert_eq!(k0, vec![vec![Value::from_text("76")]]);
    assert_eq!(k0_again, k0);
    assert_eq!(k5, vec![vec![Value::from_text("1")]]);
    assert_eq!(
        first_k7, last_k7,
        "same snapshot must not see k7 appear after other unique seeks, first={first_k7:?} last={last_k7:?}"
    );
    assert_eq!(
        first_k7,
        vec![vec![Value::from_text("155")]],
        "first unique seek of reclaimed k7 must see the row, got {first_k7:?}"
    );
}

#[test]
fn test_idxdelete_after_truncate_clears_checkpointed_index() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x)")
        .unwrap();
    conn.execute("CREATE INDEX t_x ON t(x)").unwrap();
    conn.execute("INSERT INTO t VALUES (1,1),(2,2)").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.execute("INSERT OR REPLACE INTO t(id,x) VALUES(2,2)")
        .unwrap();
    conn.execute("DELETE FROM t WHERE x>=1").unwrap();
    assert_eq!(
        get_rows(&conn, "SELECT count(*) FROM t"),
        vec![vec![Value::from_i64(0)]],
        "table rows must be gone after DELETE"
    );
    assert_eq!(
        get_rows(&conn, "SELECT count(*) FROM t WHERE x>=1"),
        vec![vec![Value::from_i64(0)]],
        "checkpointed index currents must not survive IdxDelete"
    );
}

#[test]
fn test_delete_via_unique_index_removes_checkpointed_rows() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, c INTEGER UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 400), (2, 500), (3, 600)")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    assert_eq!(
        get_rows(&conn, "SELECT count(*) FROM t"),
        vec![vec![Value::from_i64(3)]],
        "truncate must leave the three checkpointed rows"
    );
    assert_eq!(
        get_rows(&conn, "SELECT id FROM t WHERE id = 1"),
        vec![vec![Value::from_i64(1)]],
        "point lookup of checkpointed INTEGER PK must work"
    );
    conn.execute("INSERT INTO t VALUES (4, 100), (5, 200)")
        .unwrap();
    conn.execute("DELETE FROM t WHERE c < 1000").unwrap();
    assert_eq!(
        get_rows(&conn, "SELECT count(*) FROM t"),
        vec![vec![Value::from_i64(0)]],
        "unique-index DELETE must still visit checkpointed B-tree rows"
    );
}

/// A reader that has already selected a multi-column row must keep seeing every
/// column after GC. No checkpoint ran, so the SkipMap holds the only copy and
/// GC must keep it even though `durable_txid_max` claims everything is durable.
#[test]
fn test_gc_keeps_columns_of_positioned_reader() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let mv = db.get_mvcc_store();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c INT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10, 20, 30)")
        .unwrap();

    let reader = db.connect();
    reader.execute("BEGIN CONCURRENT").unwrap();
    let before = get_rows(&reader, "SELECT a, b, c FROM t WHERE id = 1");
    assert_eq!(
        before,
        vec![vec![
            Value::from_i64(10),
            Value::from_i64(20),
            Value::from_i64(30)
        ]]
    );

    let versions_before_gc = skipmap_version_count(&mv);
    mv.durable_txid_max.store(u64::MAX, Ordering::SeqCst);
    for _ in 0..4 {
        mv.gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC);
    }
    mv.drop_unused_row_versions();
    assert_eq!(
        skipmap_version_count(&mv),
        versions_before_gc,
        "GC must keep live versions no checkpoint ever wrote"
    );

    let after = get_rows(&reader, "SELECT a, b, c FROM t WHERE id = 1");
    assert_eq!(after, before, "must not get NULL or empty columns after GC");
    reader.execute("COMMIT").unwrap();
}

/// SkipMap `read` after incremental GC on a chain no checkpoint wrote. Dropping
/// the sole current here would make this return None.
#[test]
fn test_gc_incremental_keeps_held_reader_row() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();
    let row_id = RowID::new(table_id, RowKey::Int(1));

    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = generate_simple_string_row(table_id, 1, "keep_me");
    db.mvcc_store.insert(tx1, row.clone()).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    db.mvcc_store
        .durable_txid_max
        .store(u64::MAX, Ordering::SeqCst);

    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    assert_eq!(db.mvcc_store.read(tx2, &row_id).unwrap().unwrap(), row);

    db.mvcc_store
        .gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC);
    db.mvcc_store.drop_unused_row_versions();

    assert_eq!(
        db.mvcc_store.read(tx2, &row_id).unwrap().unwrap(),
        row,
        "held reader still reads the current version"
    );
    let versions = db.mvcc_store.rows.get(&row_id).unwrap();
    let versions = versions.value().read();
    assert_eq!(versions.len(), 1);
    assert_eq!(
        versions[0].materialized_at(),
        crate::mvcc::database::WalPos::ORIGIN
    );
    assert_eq!(versions[0].end(), None);
}

/// Two overlapping writers plus GC while a third connection holds a snapshot.
/// Truncate is Busy with the reader open; incremental GC must not change the
/// snapshot values.
#[test]
fn test_gc_retire_snapshot_stable_with_overlapping_writers() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let mv = db.get_mvcc_store();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL)")
        .unwrap();
    setup
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    setup.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let reader = db.connect();
    reader.execute("BEGIN CONCURRENT").unwrap();
    let snap = get_rows(&reader, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(
        snap,
        vec![
            vec![Value::from_i64(1), Value::from_i64(10)],
            vec![Value::from_i64(2), Value::from_i64(20)],
            vec![Value::from_i64(3), Value::from_i64(30)],
        ]
    );

    let w1 = db.connect();
    let w2 = db.connect();
    w1.execute("BEGIN CONCURRENT").unwrap();
    w2.execute("BEGIN CONCURRENT").unwrap();
    w1.execute("UPDATE t SET v = 21 WHERE id = 2").unwrap();
    w2.execute("UPDATE t SET v = 31 WHERE id = 3").unwrap();
    w1.execute("COMMIT").unwrap();
    w2.execute("COMMIT").unwrap();

    assert!(
        matches!(
            setup.execute("PRAGMA wal_checkpoint(TRUNCATE)"),
            Err(LimboError::Busy)
        ),
        "Truncate cannot run while the snapshot is held"
    );
    for _ in 0..4 {
        mv.gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC);
    }
    mv.drop_unused_row_versions();

    let again = get_rows(&reader, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(again, snap, "held snapshot must stay stable across GC");
    reader.execute("COMMIT").unwrap();
}

/// Index rows live in a separate SkipMap from table rows and go through their own
/// GC path (gc_index_row_versions). This SQL-level test creates an indexed table,
/// checkpoints, updates the row (creating superseded index versions), checkpoints
/// again, and verifies the index still returns correct results. Catches regressions
/// where index GC removes versions that the dual cursor still needs.
#[test]
fn test_gc_e2e_index_rows_collected_after_checkpoint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_val ON t(val)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'alpha')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'beta')").unwrap();

    // Checkpoint flushes to B-tree and triggers GC on both table and index rows.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // After GC, reads should still work via B-tree fallthrough.
    let rows = get_rows(&conn, "SELECT val FROM t ORDER BY val");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].to_string(), "alpha");
    assert_eq!(rows[1][0].to_string(), "beta");

    // Index scan should also work.
    let rows = get_rows(&conn, "SELECT id FROM t WHERE val = 'alpha'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);

    // Update a row — creates new index versions.
    conn.execute("UPDATE t SET val = 'gamma' WHERE id = 1")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Old index entry ('alpha') should be gone, new entry ('gamma') visible.
    let rows = get_rows(&conn, "SELECT id FROM t WHERE val = 'alpha'");
    assert_eq!(rows.len(), 0);
    let rows = get_rows(&conn, "SELECT id FROM t WHERE val = 'gamma'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

// ─── GC quickcheck property tests ────────────────────────────────────────

/// Represents a version chain entry for quickcheck.
#[derive(Debug, Clone)]
struct ArbitraryVersionChain {
    versions: RowVersionChain<TursoAllocator>,
    lwm: u64,
    ckpt_max: u64,
}

/// Generates RowVersions matching realistic MVCC states.
/// Only produces valid (begin, end) combinations that can actually occur.
fn arbitrary_row_version(g: &mut Gen) -> RowVersion {
    // Weight toward realistic states:
    // 32% current (Timestamp, None), 24% superseded (Timestamp, Timestamp),
    // 8% aborted (None, None), 8% pending insert (TxID, None),
    // 8% pending delete (Timestamp, TxID), 20% B-tree tombstone (None, Timestamp)
    let kind = u8::arbitrary(g) % 25;
    let (begin, end) = match kind {
        0..=7 => {
            // Current committed version
            let b = u64::arbitrary(g) % 20 + 1;
            (Some(TxTimestampOrID::Timestamp(b)), None)
        }
        8..=13 => {
            // Superseded version
            let b = u64::arbitrary(g) % 15 + 1;
            let e = b + u64::arbitrary(g) % 10 + 1;
            (
                Some(TxTimestampOrID::Timestamp(b)),
                Some(TxTimestampOrID::Timestamp(e)),
            )
        }
        14..=15 => {
            // Aborted garbage
            (None, None)
        }
        16..=17 => {
            // Pending insert
            let t = u64::arbitrary(g) % 20 + 1;
            (Some(TxTimestampOrID::TxID(t)), None)
        }
        18..=19 => {
            // Pending delete
            let b = u64::arbitrary(g) % 15 + 1;
            let t = u64::arbitrary(g) % 20 + 1;
            (
                Some(TxTimestampOrID::Timestamp(b)),
                Some(TxTimestampOrID::TxID(t)),
            )
        }
        20..=24 => {
            // B-tree tombstone (begin=None, end=e) — row existed before MVCC, then deleted
            let e = u64::arbitrary(g) % 20 + 1;
            (None, Some(TxTimestampOrID::Timestamp(e)))
        }
        _ => unreachable!(),
    };

    RowVersion {
        id: 0,
        begin: crate::mvcc::database::PackedTs::pack(begin),
        end: crate::mvcc::database::PackedTs::pack(end),
        row: generate_simple_string_row((-2).into(), 1, "qc"),
        btree_resident: bool::arbitrary(g),
        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
    }
}

impl Arbitrary for ArbitraryVersionChain {
    fn arbitrary(g: &mut Gen) -> Self {
        // 1..8 versions (no empty chains — they trivially pass all properties)
        let len = usize::arbitrary(g) % 8 + 1;
        let versions: RowVersionChain<TursoAllocator> = (0..len)
            .map(|_| arbitrary_row_version(g))
            .try_collect()
            .unwrap();
        // Include boundary values with ~20% probability each.
        let lwm = match u8::arbitrary(g) % 5 {
            0 => 0,
            1 => u64::MAX, // blocking checkpoint case
            _ => u64::arbitrary(g) % 30,
        };
        let ckpt_max = match u8::arbitrary(g) % 5 {
            0 => 0,        // no checkpoint has run
            1 => u64::MAX, // everything checkpointed
            _ => u64::arbitrary(g) % 30,
        };
        Self {
            versions,
            lwm,
            ckpt_max,
        }
    }
}

/// GC only removes versions — it never synthesizes new ones. For any input chain,
/// the output must be a subset (same length or shorter).
#[quickcheck]
fn prop_gc_never_increases_version_count(chain: ArbitraryVersionChain) -> bool {
    let before = chain.versions.len();
    let mut versions = chain.versions;
    MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        chain.lwm,
        chain.ckpt_max,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    versions.len() <= before
}

/// Running GC twice with the same LWM and ckpt_max must produce the same result
/// as running it once. A non-idempotent GC would indicate that GC output triggers
/// further removals on re-evaluation, which means the first pass missed something.
/// Compares actual version content (begin/end), not just chain length.
#[quickcheck]
fn prop_gc_is_idempotent(chain: ArbitraryVersionChain) -> bool {
    let mut v1 = chain.versions.clone();
    MvStore::<MvccClock>::gc_version_chain(
        &mut v1,
        chain.lwm,
        chain.ckpt_max,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    let snapshot = v1.clone();
    MvStore::<MvccClock>::gc_version_chain(
        &mut v1,
        chain.lwm,
        chain.ckpt_max,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    // Compare content, not just length — a swap bug would pass a length-only check.
    v1.len() == snapshot.len()
        && v1
            .iter()
            .zip(snapshot.iter())
            .all(|(a, b)| a.begin() == b.begin() && a.end() == b.end())
}

/// Aborted garbage (begin=None, end=None) is invisible to every transaction and
/// has no B-tree implications. GC must remove all of it unconditionally (Rule 1).
/// No aborted garbage should survive a GC pass, regardless of LWM or ckpt_max.
#[quickcheck]
fn prop_gc_removes_all_aborted_garbage(chain: ArbitraryVersionChain) -> bool {
    let mut versions = chain.versions;
    MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        chain.lwm,
        chain.ckpt_max,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    versions
        .iter()
        .all(|rv| !matches!((&rv.begin(), &rv.end()), (None, None)))
}

/// Uncommitted inserts (begin=TxID, end=None) belong to an in-flight transaction.
/// GC cannot know whether it will commit or abort, so it must never touch them.
/// Verifies all such versions survive GC regardless of other chain contents.
#[quickcheck]
fn prop_gc_retains_txid_begins(chain: ArbitraryVersionChain) -> bool {
    let txid_begins_before: usize = chain
        .versions
        .iter()
        .filter(|rv| matches!(&rv.begin(), Some(TxTimestampOrID::TxID(_))) && rv.end().is_none())
        .count();
    let mut versions = chain.versions;
    MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        chain.lwm,
        chain.ckpt_max,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    let txid_begins_after: usize = versions
        .iter()
        .filter(|rv| matches!(&rv.begin(), Some(TxTimestampOrID::TxID(_))) && rv.end().is_none())
        .count();
    // Active uncommitted versions (begin=TxID, end=None) are never aborted garbage
    // and don't match rule 2 or 3, so they should be retained.
    txid_begins_after == txid_begins_before
}

/// Uncommitted deletions (end=TxID) represent a pending delete by an in-flight
/// transaction. Rule 2 only matches end=Timestamp, so these must be retained.
/// Verifies GC never removes versions with TxID end markers.
#[quickcheck]
fn prop_gc_retains_txid_ends(chain: ArbitraryVersionChain) -> bool {
    // Versions with end=TxID and non-None begin are not matched by any removal
    // rule (rule 1 requires (None,None), rule 2 requires end=Timestamp).
    let filter = |rv: &&RowVersion| {
        matches!(&rv.end(), Some(TxTimestampOrID::TxID(_))) && rv.begin().is_some()
    };
    let txid_ends_before: usize = chain.versions.iter().filter(filter).count();
    let mut versions = chain.versions;
    MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        chain.lwm,
        chain.ckpt_max,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    let txid_ends_after: usize = versions.iter().filter(filter).count();
    txid_ends_after == txid_ends_before
}

/// Current versions (begin=Timestamp(b), end=None) are not removable before they
/// are checkpointed. Forces ckpt_max=0 and verifies all committed current versions
/// survive.
#[quickcheck]
fn prop_gc_current_versions_protected_before_checkpoint(chain: ArbitraryVersionChain) -> bool {
    let current_before: usize = chain
        .versions
        .iter()
        .filter(|rv| {
            matches!(
                (&rv.begin(), &rv.end()),
                (Some(TxTimestampOrID::Timestamp(_)), None)
            )
        })
        .count();
    let mut versions = chain.versions;
    MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        chain.lwm,
        0,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );
    let current_after: usize = versions
        .iter()
        .filter(|rv| {
            matches!(
                (&rv.begin(), &rv.end()),
                (Some(TxTimestampOrID::Timestamp(_)), None)
            )
        })
        .count();
    current_after == current_before
}

/// When a row has been deleted but the deletion isn't checkpointed yet, the
/// tombstone (superseded version with end > ckpt_max) is the only thing preventing
/// the dual cursor from reading a stale B-tree row. If GC empties such a chain,
/// the deleted row reappears. Verifies GC never empties a chain that has
/// uncheckpointed tombstones and no committed current version to take over.
#[quickcheck]
fn prop_gc_tombstone_guard_preserves_btree_safety(chain: ArbitraryVersionChain) -> bool {
    // If a chain has only superseded versions (no committed current) and at
    // least one has e > ckpt_max, GC must not empty the chain — removing all
    // versions would let the dual cursor fall through to a stale B-tree row.
    let mut versions = chain.versions.clone();
    MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        chain.lwm,
        chain.ckpt_max,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );

    // Check: if pre-GC chain had no committed current version AND had a
    // superseded version with e > ckpt_max, post-GC chain must not be empty.
    let had_committed_current = chain
        .versions
        .iter()
        .any(|rv| rv.end().is_none() && matches!(&rv.begin(), Some(TxTimestampOrID::Timestamp(_))));
    let had_uncheckpointed_tombstone = chain
        .versions
        .iter()
        .any(|rv| matches!(&rv.end(), Some(TxTimestampOrID::Timestamp(e)) if *e > chain.ckpt_max));
    // Only non-garbage versions matter (aborted garbage is always removed first)
    let had_non_garbage = chain
        .versions
        .iter()
        .any(|rv| !matches!((&rv.begin(), &rv.end()), (None, None)));

    if !had_committed_current && had_uncheckpointed_tombstone && had_non_garbage {
        !versions.is_empty()
    } else {
        true // no constraint in this case
    }
}

/// Superseded versions without a committed current version are dangerous — their
/// presence tells the dual cursor "this row was modified" but there's no current
/// version to serve reads. GC must only leave such orphans when they're justifiably
/// retained: still visible to a reader (e > lwm), guarding an uncheckpointed
/// deletion (e > ckpt_max).
#[quickcheck]
fn prop_gc_no_orphaned_superseded_versions(chain: ArbitraryVersionChain) -> bool {
    // After GC, if a chain has superseded versions without a committed current
    // version, each superseded version must be justifiably retained:
    // - e > lwm (Rule 2 didn't fire — still visible to some reader)
    // - e > ckpt_max (tombstone guard — deletion not yet in B-tree)
    let mut versions = chain.versions;
    MvStore::<MvccClock>::gc_version_chain(
        &mut versions,
        chain.lwm,
        chain.ckpt_max,
        false,
        crate::mvcc::database::WalPos::STAGED,
        true,
    );

    let has_committed_current = versions
        .iter()
        .any(|rv| rv.end().is_none() && matches!(&rv.begin(), Some(TxTimestampOrID::Timestamp(_))));
    let has_superseded = versions.iter().any(|rv| {
        matches!(
            (&rv.begin(), &rv.end()),
            (
                Some(TxTimestampOrID::Timestamp(_)),
                Some(TxTimestampOrID::Timestamp(_))
            )
        )
    });

    if has_superseded && !has_committed_current {
        versions
            .iter()
            .filter(|rv| {
                matches!(
                    (&rv.begin(), &rv.end()),
                    (
                        Some(TxTimestampOrID::Timestamp(_)),
                        Some(TxTimestampOrID::Timestamp(_))
                    )
                )
            })
            .all(|rv| {
                if let Some(TxTimestampOrID::Timestamp(e)) = &rv.end() {
                    *e > chain.lwm || *e > chain.ckpt_max
                } else {
                    false
                }
            })
    } else {
        true
    }
}

/// Test that a transaction cannot see uncommitted changes from another transaction.
/// This verifies snapshot isolation.
#[test]
fn test_mvcc_snapshot_isolation() {
    let db = MvccTestDbNoConn::new_with_random_db();

    let conn1 = db.connect();
    conn1
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();

    // Start tx1 and read initial values
    conn1.execute("BEGIN CONCURRENT").unwrap();
    let rows1 = get_rows(&conn1, "SELECT value FROM t WHERE id = 2");
    assert_eq!(rows1[0][0].to_string(), "200");

    // Start tx2 and modify the same row
    let conn2 = db.connect();
    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2
        .execute("UPDATE t SET value = 999 WHERE id = 2")
        .unwrap();
    conn2.execute("COMMIT").unwrap();

    // Tx1 should still see the old value (snapshot isolation)
    let rows1_again = get_rows(&conn1, "SELECT value FROM t WHERE id = 2");
    assert_eq!(
        rows1_again[0][0].to_string(),
        "200",
        "Tx1 should not see tx2's committed changes"
    );

    conn1.execute("COMMIT").unwrap();

    // After tx1 commits, new reads should see tx2's changes
    let rows_after = get_rows(&conn1, "SELECT value FROM t WHERE id = 2");
    assert_eq!(rows_after[0][0].to_string(), "999");
}
/// Similar test but with the constraint error happening on the third unique column.
/// This tests that ALL previous index modifications are rolled back.
/// Uses interactive transaction (BEGIN CONCURRENT) to reproduce the bug.
#[test]
fn test_update_three_unique_columns_partial_rollback() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    // Create table with three unique columns
    conn.execute(
        "CREATE TABLE t(
            id INTEGER PRIMARY KEY,
            col_a TEXT UNIQUE,
            col_b REAL UNIQUE,
            col_c INTEGER UNIQUE
        )",
    )
    .unwrap();

    // Insert two rows
    conn.execute("INSERT INTO t VALUES (1, 'a1', 1.0, 100)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'a2', 2.0, 200)")
        .unwrap();

    // Start interactive transaction
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Try to UPDATE row 1 with:
    // - col_a = 'new_a' (index modified)
    // - col_b = 3.0 (index modified)
    // - col_c = 200 (FAIL - conflicts with row 2)
    // BUG: col_a and col_b index changes are NOT rolled back!
    let result =
        conn.execute("UPDATE t SET col_a = 'new_a', col_b = 3.0, col_c = 200 WHERE id = 1");
    assert!(
        result.is_err(),
        "Expected unique constraint violation on col_c"
    );

    // COMMIT - in buggy case, this commits partial index changes
    conn.execute("COMMIT").unwrap();

    // Now try to UPDATE the same row - this should work but may crash
    // if col_a or col_b index entries are inconsistent
    conn.execute("UPDATE t SET col_a = 'updated_a', col_b = 5.0, col_c = 500 WHERE id = 1")
        .unwrap();

    // Verify index lookups work
    let rows = get_rows(&conn, "SELECT * FROM t WHERE col_a = 'updated_a'");
    assert_eq!(rows.len(), 1);

    let rows = get_rows(&conn, "SELECT * FROM t WHERE col_b = 5.0");
    assert_eq!(rows.len(), 1);

    let rows = get_rows(&conn, "SELECT * FROM t WHERE col_c = 500");
    assert_eq!(rows.len(), 1);

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test that simulates the exact sequence from the stress test bug:
/// Multiple interactive transactions updating the same row, with constraint errors.
///
/// From the log:
/// - tx 248: UPDATE row with pk=1.37, sets unique_col='sweet_wind_280' -> COMMIT
/// - tx 1149: BEGIN, UPDATE same row (modifies unique_col index, fails on other_unique), COMMIT
///   BUG: partial index changes from failed UPDATE are committed!
/// - tx 1324: UPDATE same row -> CRASH "IdxDelete: no matching index entry found"
#[test]
fn test_sequential_updates_with_constraint_errors() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute(
        "CREATE TABLE t(
            pk REAL PRIMARY KEY,
            unique_col TEXT UNIQUE,
            other_unique REAL UNIQUE
        )",
    )
    .unwrap();

    // Insert initial rows (simulating the stress test setup)
    conn.execute("INSERT INTO t VALUES (1.37, 'sweet_wind_280', 9.05)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2.13, 'other_value', 2.13)")
        .unwrap();

    // First successful update (like tx 248 in the bug)
    conn.execute("UPDATE t SET unique_col = 'cold_grass_813', other_unique = 3.90 WHERE pk = 1.37")
        .unwrap();

    // Verify the update
    let rows = get_rows(&conn, "SELECT unique_col FROM t WHERE pk = 1.37");
    assert_eq!(rows[0][0].cast_text().unwrap(), "cold_grass_813");

    // Like tx 1149: Start interactive transaction
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Try an update that will fail on other_unique (conflicts with row 2)
    // The UPDATE will:
    // 1. Delete old index entry for unique_col ('cold_grass_813')
    // 2. Insert new index entry for unique_col ('new_value')
    // 3. Delete old index entry for other_unique (3.90)
    // 4. Check constraint for other_unique (2.13) -> FAIL!
    // BUG: Steps 1-3 are NOT rolled back!
    let result =
        conn.execute("UPDATE t SET unique_col = 'new_value', other_unique = 2.13 WHERE pk = 1.37");
    assert!(result.is_err(), "Expected unique constraint violation");

    // COMMIT the transaction (like the stress test does after the error)
    // BUG: This commits the partial index changes!
    conn.execute("COMMIT").unwrap();

    // Like tx 1324: Try another update on the same row
    // If partial changes were committed:
    // - Table row has unique_col = 'cold_grass_813'
    // - But unique_col index has 'new_value' (not 'cold_grass_813')!
    // - This UPDATE reads 'cold_grass_813' from table, tries to delete that index entry
    // - CRASH: "IdxDelete: no matching index entry found"
    conn.execute("UPDATE t SET unique_col = 'fresh_sun_348', other_unique = 5.0 WHERE pk = 1.37")
        .unwrap();

    // Verify final state
    let rows = get_rows(
        &conn,
        "SELECT unique_col, other_unique FROM t WHERE pk = 1.37",
    );
    assert_eq!(rows[0][0].cast_text().unwrap(), "fresh_sun_348");

    // Verify index lookups work
    let rows = get_rows(&conn, "SELECT * FROM t WHERE unique_col = 'fresh_sun_348'");
    assert_eq!(rows.len(), 1);

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test that multiple successful statements in an interactive transaction
/// have their changes preserved when a subsequent statement fails.
/// This tests the statement-level savepoint functionality.
#[test]
fn test_savepoint_multiple_statements_last_fails() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        .unwrap();

    // Start interactive transaction
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Statement 1: Insert row 1 - success
    conn.execute("INSERT INTO t VALUES (1)").unwrap();

    // Statement 2: Insert row 2 - success
    conn.execute("INSERT INTO t VALUES (2)").unwrap();

    // Statement 3: Insert row 1 again - fails with PK violation
    let result = conn.execute("INSERT INTO t VALUES (1)");
    assert!(result.is_err(), "Expected primary key violation");

    // COMMIT - should preserve statements 1 and 2
    conn.execute("COMMIT").unwrap();

    // Verify rows 1 and 2 exist
    let rows = get_rows(&conn, "SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[1][0].as_int().unwrap(), 2);

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test that when the same row is modified by multiple statements,
/// and the second modification fails, the first modification is preserved.
#[test]
fn test_savepoint_same_row_multiple_statements() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER, other_unique INTEGER UNIQUE)")
        .unwrap();

    // Insert initial row and a row to cause conflict
    conn.execute("INSERT INTO t VALUES (1, 100, 1)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 200, 2)").unwrap();

    // Start interactive transaction
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Statement 1: Update row 1's value to 150 - success
    conn.execute("UPDATE t SET v = 150 WHERE id = 1").unwrap();

    // Statement 2: Try to update row 1 with conflicting other_unique - fails
    let result = conn.execute("UPDATE t SET v = 175, other_unique = 2 WHERE id = 1");
    assert!(result.is_err(), "Expected unique constraint violation");

    // COMMIT - should preserve statement 1's change (v = 150)
    conn.execute("COMMIT").unwrap();

    // Verify row 1 has v = 150 (from statement 1), not 175 (from failed statement 2)
    let rows = get_rows(&conn, "SELECT v, other_unique FROM t WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 150);
    assert_eq!(rows[0][1].as_int().unwrap(), 1); // other_unique unchanged

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test that index operations are properly tracked per-statement.
/// When a statement fails after partially modifying indexes,
/// only that statement's index changes are rolled back.
#[test]
fn test_savepoint_index_multiple_statements() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute(
        "CREATE TABLE t(
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            value INTEGER UNIQUE
        )",
    )
    .unwrap();

    // Insert rows
    conn.execute("INSERT INTO t VALUES (1, 'a', 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'b', 20)").unwrap();

    // Start interactive transaction
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Statement 1: Successfully change name for row 1
    conn.execute("UPDATE t SET name = 'c' WHERE id = 1")
        .unwrap();

    // Statement 2: Try to change name to 'b' (conflict with row 2) - fails
    let result = conn.execute("UPDATE t SET name = 'b' WHERE id = 1");
    assert!(
        result.is_err(),
        "Expected unique constraint violation on name"
    );

    // COMMIT
    conn.execute("COMMIT").unwrap();

    // Verify row 1 has name 'c' from statement 1
    let rows = get_rows(&conn, "SELECT name FROM t WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].cast_text().unwrap(), "c");

    // Verify index lookups work correctly
    let rows = get_rows(&conn, "SELECT id FROM t WHERE name = 'c'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);

    // 'a' should no longer be in the index
    let rows = get_rows(&conn, "SELECT id FROM t WHERE name = 'a'");
    assert_eq!(rows.len(), 0);

    // 'b' should still point to row 2
    let rows = get_rows(&conn, "SELECT id FROM t WHERE name = 'b'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 2);

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test INSERT followed by DELETE of same row, then another statement fails.
/// The insert+delete should be preserved (row shouldn't exist).
#[test]
fn test_savepoint_insert_delete_then_fail() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 200)").unwrap();

    // Start interactive transaction
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Statement 1: Insert row 1
    conn.execute("INSERT INTO t VALUES (1, 100)").unwrap();

    // Statement 2: Delete row 1
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();

    // Statement 3: Try to insert with conflicting unique value - fails
    let result = conn.execute("INSERT INTO t VALUES (3, 200)");
    assert!(result.is_err(), "Expected unique constraint violation");

    // COMMIT
    conn.execute("COMMIT").unwrap();

    // Verify row 1 does not exist (was deleted in statement 2)
    let rows = get_rows(&conn, "SELECT * FROM t WHERE id = 1");
    assert_eq!(rows.len(), 0);

    // Row 2 should still exist
    let rows = get_rows(&conn, "SELECT * FROM t WHERE id = 2");
    assert_eq!(rows.len(), 1);

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_delete_row_is_hidden_from_desc_unique_index_scan() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (42, 46)").unwrap();
    conn.execute("DELETE FROM t WHERE id = 42").unwrap();

    let rows = get_rows(&conn, "SELECT id, val FROM t ORDER BY val DESC");
    assert_eq!(rows, Vec::<Vec<Value>>::new());

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_delete_row_is_skipped_by_desc_explicit_index_scan() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX idx_t_val ON t(val)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    conn.execute("DELETE FROM t WHERE id = 2").unwrap();

    let rows = get_rows(&conn, "SELECT id, val FROM t ORDER BY val DESC");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 10);

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_delete_btree_resident_row_is_skipped_by_desc_unique_index_scan() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER UNIQUE)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    db.restart();

    let conn = db.connect();
    conn.execute("DELETE FROM t WHERE id = 2").unwrap();

    let rows = get_rows(&conn, "SELECT id, val FROM t ORDER BY val DESC");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 10);

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Regression test for issue #5935: a reverse (DESC) index scan inside a
/// `BEGIN CONCURRENT` snapshot must not observe rows inserted and committed
/// by another transaction after the snapshot started. The same root cause
/// also produced phantom NULL results from `MAX()` on an indexed column.
#[test]
fn test_desc_index_scan_respects_mvcc_snapshot_for_concurrent_insert() {
    let db = MvccTestDbNoConn::new_with_random_db();

    let setup = db.connect();
    setup.execute("CREATE TABLE t(id INT, val INT)").unwrap();
    setup.execute("CREATE INDEX idx_val ON t(val)").unwrap();
    setup.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    setup.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    setup.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    let reader = db.connect();
    reader.execute("BEGIN CONCURRENT").unwrap();
    let rows = get_rows(
        &reader,
        "SELECT id, val FROM t WHERE val > 10 ORDER BY val DESC",
    );
    assert_eq!(rows.len(), 2);

    let writer = db.connect();
    writer.execute("BEGIN CONCURRENT").unwrap();
    writer.execute("INSERT INTO t VALUES (4, 100)").unwrap();
    writer.execute("COMMIT").unwrap();

    let rows = get_rows(
        &reader,
        "SELECT id, val FROM t WHERE val > 10 ORDER BY val DESC",
    );
    assert_eq!(
        rows.len(),
        2,
        "DESC scan must still see 2 rows from snapshot"
    );
    assert_eq!(rows[0][1].as_int().unwrap(), 30);
    assert_eq!(rows[1][1].as_int().unwrap(), 20);

    let rows = get_rows(&reader, "SELECT MAX(val) FROM t");
    assert_eq!(
        rows[0][0].as_int().unwrap(),
        30,
        "MAX must still be 30 from snapshot"
    );
}

/// Test DELETE all B-tree rows and re-insert with same IDs in MVCC.
/// Verifies tombstones correctly shadow B-tree and new rows are visible.
///
/// This test was initially failing with "UNIQUE constraint failed: t.id"
/// Fixed by implementing dual-peek in the exists() method to check MVCC tombstones.
#[test]
fn test_mvcc_dual_cursor_delete_all_btree_reinsert() {
    let _ = tracing_subscriber::fmt::try_init();
    let mut db = MvccTestDbNoConn::new_with_random_db();

    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'old1')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'old2')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    db.restart();

    let conn = db.connect();
    // Delete all B-tree rows
    conn.execute("DELETE FROM t WHERE id IN (1, 2)").unwrap();
    // Re-insert with new values
    conn.execute("INSERT INTO t VALUES (1, 'new1')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'new2')").unwrap();

    // Should see new values, not old B-tree values
    let rows = get_rows(&conn, "SELECT id, val FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1].to_string(), "new1");
    assert_eq!(rows[1][1].to_string(), "new2");
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_checkpoint_root_page_mismatch_with_index() {
    // Strategy:
    // 1. Create table1 with index, insert many rows to allocate many pages (e.g., pages 2-30)
    // 2. Create table2 with index (will get negative IDs like -35, -36)
    // 3. Insert into table2
    // 4. Checkpoint - table2 will be allocated to pages 32, 33 (after table1's pages)
    // 5. But schema update will do abs(-35) = 35, abs(-36) = 36 (WRONG!)
    // 6. Query table2 using index - will look for page 36 but data is in page 33

    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();

    // Create MULTIPLE tables to consume enough page numbers
    // so that test_table's allocated pages diverge from abs(negative_id)
    for table_num in 1..=30 {
        conn.execute(format!(
            "CREATE TABLE tbl{table_num} (id INTEGER PRIMARY KEY, data TEXT)",
        ))
        .unwrap();
        conn.execute(format!(
            "CREATE INDEX idx{table_num} ON tbl{table_num}(data)",
        ))
        .unwrap();

        // Insert data to force page allocation
        for i in 0..10 {
            let data = format!("data_{table_num}_{i}");
            conn.execute(format!("INSERT INTO tbl{table_num} VALUES ({i}, '{data}')",))
                .unwrap();
        }
    }

    println!("Created 30 tables with indexes and data");

    // Create test_table with UNIQUE index (auto-created for the key)
    conn.execute("CREATE TABLE test_table (key TEXT PRIMARY KEY, value TEXT)")
        .unwrap();

    // Check test_table's root pages (should be negative)
    let rows = get_rows(
        &conn,
        "SELECT name, rootpage FROM sqlite_schema WHERE tbl_name = 'test_table' ORDER BY name",
    );
    let table_root: i64 = rows
        .iter()
        .find(|r| r[0].to_string() == "test_table")
        .unwrap()[1]
        .to_string()
        .parse()
        .unwrap();
    let index_root: i64 = rows
        .iter()
        .find(|r| r[0].to_string().contains("autoindex"))
        .unwrap()[1]
        .to_string()
        .parse()
        .unwrap();
    assert!(
        table_root < 0,
        "test_table should have negative root before checkpoint"
    );
    assert!(
        index_root < 0,
        "test_table index should have negative root before checkpoint"
    );

    // Insert a row into test_table
    conn.execute("INSERT INTO test_table (key, value) VALUES ('test_key', 'test_value')")
        .unwrap();

    // Verify row exists before checkpoint
    let rows = get_rows(&conn, "SELECT value FROM test_table WHERE key = 'test_key'");
    assert_eq!(rows.len(), 1, "Row should exist before checkpoint");
    assert_eq!(rows[0][0].to_string(), "test_value");

    println!("Inserted row into test_table, verified it exists");

    // Run checkpoint
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    println!("Checkpoint complete");

    // Now try to query using the index - this is where the bug manifests
    // The query will use root_page from schema (e.g., abs(index_root) if bug exists)
    // But data is actually in the correct allocated page
    let rows = get_rows(&conn, "SELECT value FROM test_table WHERE key = 'test_key'");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_string(), "test_value", "Value should match");

    println!("Test passed - row found correctly after checkpoint");
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_checkpoint_drop_table() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_t_data ON t(data)").unwrap();

    // Insert data to force page allocation
    for i in 0..10 {
        let data = format!("data_{i}");
        conn.execute(format!("INSERT INTO t VALUES ({i}, '{data}')",))
            .unwrap();
    }
    conn.execute("DROP TABLE t").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(conn);

    db.restart();

    let conn = db.connect();
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// After a DROP TABLE frees pages and a CREATE INDEX reuses one of those
/// freed pages as its new root, a subsequent checkpoint must not use the
/// stale table cursor (which lacks index_info) when writing index rows.
#[test]
fn test_checkpoint_drop_table_then_create_index_page_reuse() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO a VALUES(1,'x')").unwrap();
    conn.execute("INSERT INTO b VALUES(1,'y')").unwrap();
    // First checkpoint writes both tables to the B-tree.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // DROP TABLE a frees its root page; CREATE INDEX may reuse it.
    conn.execute("DROP TABLE a").unwrap();
    conn.execute("CREATE INDEX new_b_v ON b(v)").unwrap();
    // Second checkpoint must handle the page reuse without panicking.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(conn);

    db.restart();

    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT * FROM b");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_string(), "1");
    assert_eq!(rows[0][1].to_string(), "y");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_checkpoint_drop_table_removes_stale_rootpage_mapping() {
    let db = MvccTestDb::new();

    db.conn
        .execute("CREATE TABLE stale_root(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    db.conn
        .execute("INSERT INTO stale_root VALUES(1, 'old')")
        .unwrap();
    db.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(
        &db.conn,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 'stale_root'",
    );
    let rootpage = rows[0][0].as_int().unwrap();
    let table_id = db.mvcc_store.get_table_id_from_root_page(rootpage);
    assert!(db.mvcc_store.table_id_to_rootpage.get(&table_id).is_some());

    db.conn.execute("DROP TABLE stale_root").unwrap();
    db.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    assert!(
        db.mvcc_store.table_id_to_rootpage.get(&table_id).is_none(),
        "dropped checkpointed table mapping must not survive rootpage reuse"
    );
}

#[test]
fn test_checkpoint_post_durable_drop_failure_retry_removes_stale_rootpage_mapping() {
    let db = MvccTestDb::new();

    db.conn
        .execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    db.conn
        .execute("CREATE TABLE stale_root(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    db.conn
        .execute("INSERT INTO stale_root VALUES(1, 'old')")
        .unwrap();
    db.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(
        &db.conn,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 'stale_root'",
    );
    let rootpage = rows[0][0].as_int().unwrap();
    let table_id = db.mvcc_store.get_table_id_from_root_page(rootpage);
    assert!(db.mvcc_store.table_id_to_rootpage.get(&table_id).is_some());

    db.conn.execute("DROP TABLE stale_root").unwrap();
    db.conn
        .set_failure_injector(Some(FixedFailureInjector::new([(
            CheckpointYieldPoint::AfterDurableBoundaryAdvanced.point(),
            LimboError::TxError("synthetic checkpoint failure after pager commit".to_string()),
        )])));
    db.conn
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .expect_err("checkpoint should fail after pager commit");
    db.conn.set_failure_injector(None);

    db.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    assert!(
        db.mvcc_store.table_id_to_rootpage.get(&table_id).is_none(),
        "dropped checkpointed table mapping must be removed after retry"
    );
}

/// Test that inserting a duplicate primary key fails when the existing row
/// was committed before this transaction started (and thus is visible).
#[test]
fn test_mvcc_same_primary_key() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    let conn2 = db.connect();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (666)").unwrap();
    conn.execute("COMMIT").unwrap();

    // conn2 starts AFTER conn1 committed, so conn2 can see the committed row.
    // INSERT should fail with UNIQUE constraint because the row is visible.
    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2
        .execute("INSERT INTO t VALUES (666)")
        .expect_err("duplicate key - visible committed row");
}

/// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
/// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
#[test]
fn test_mvcc_same_primary_key_concurrent() {
    // Pure optimistic concurrency: both transactions can INSERT the same rowid,
    // but only one can commit (first-committer-wins based on end_ts comparison).
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    let conn2 = db.connect();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (666)").unwrap();

    conn2.execute("BEGIN CONCURRENT").unwrap();
    // With pure optimistic CC, INSERT succeeds - conflict detected at commit time
    conn2.execute("INSERT INTO t VALUES (666)").unwrap();

    // First transaction commits successfully (gets lower end_ts)
    conn.execute("COMMIT").unwrap();

    // Second transaction fails at commit time (first-committer-wins)
    conn2
        .execute("COMMIT")
        .expect_err("duplicate key - first committer wins");
}

// ─── End-to-end GC + dual cursor tests ───────────────────────────────────

/// After checkpoint + GC, checkpointed current versions are removed from
/// the SkipMap. Readers must still see the data via B-tree fallthrough.
#[test]
fn test_gc_e2e_checkpointed_row_readable_after_gc() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'world')").unwrap();

    // Checkpoint flushes to B-tree and triggers GC.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // After GC, SkipMap currents that Truncate already wrote to the B-tree are
    // gone; reads fall through to the B-tree.
    let rows = get_rows(&conn, "SELECT id, val FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "hello");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "world");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// After deleting a B-tree row and checkpointing, the tombstone is removed
/// by GC. The deleted row must stay invisible (B-tree no longer has it).
#[turso_macros::test(encryption)]
fn test_gc_e2e_deleted_row_stays_hidden_after_gc() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'keep')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'delete_me')")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    // Restart so rows are only in B-tree.
    db.restart();
    let conn = db.connect();

    // Delete row 2 in MVCC (creates tombstone over B-tree row).
    conn.execute("DELETE FROM t WHERE id = 2").unwrap();

    // Checkpoint writes the deletion to B-tree and GC removes the tombstone.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Row 2 must remain invisible.
    let rows = get_rows(&conn, "SELECT id, val FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "keep");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// After updating a B-tree row and checkpointing, GC removes old versions.
/// The updated value must be visible (from B-tree after GC).
#[turso_macros::test(encryption)]
fn test_gc_e2e_updated_row_correct_after_gc() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'original')")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    db.restart();
    let conn = db.connect();

    // Update in MVCC.
    conn.execute("UPDATE t SET val = 'updated' WHERE id = 1")
        .unwrap();

    // Checkpoint + GC.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Must see updated value.
    let rows = get_rows(&conn, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_string(), "updated");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Multiple checkpoints with interleaved writes. Each checkpoint triggers GC.
/// Verifies cumulative correctness across GC cycles.
#[test]
fn test_gc_e2e_multiple_checkpoint_gc_cycles() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();

    for i in 1..=5 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, {i})"))
            .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Delete rows 2, 4 and update row 3.
    conn.execute("DELETE FROM t WHERE id IN (2, 4)").unwrap();
    conn.execute("UPDATE t SET val = 30 WHERE id = 3").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Insert row 6, delete row 1.
    conn.execute("INSERT INTO t VALUES (6, 6)").unwrap();
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "SELECT id, val FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_int().unwrap(), 3);
    assert_eq!(rows[0][1].as_int().unwrap(), 30);
    assert_eq!(rows[1][0].as_int().unwrap(), 5);
    assert_eq!(rows[1][1].as_int().unwrap(), 5);
    assert_eq!(rows[2][0].as_int().unwrap(), 6);
    assert_eq!(rows[2][1].as_int().unwrap(), 6);

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_mvcc_unique_constraint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id UNIQUE)").unwrap();
    let conn2 = db.connect();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (666)").unwrap();

    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("INSERT INTO t VALUES (666)").unwrap();

    conn.execute("COMMIT").unwrap();
    // conn2 should see conflict with conn1's row where first conneciton changed `begin` to a Timestamp that is < than conn2's end_ts
    conn2
        .execute("COMMIT")
        .expect_err("duplicate unique - first committer wins");
}

/// Regression test for MVCC concurrent commit yield-spin deadlock.
///
/// When the VDBE encounters a yield completion (pager_commit_lock contention),
/// it must return StepResult::Yield to yield control. Previously, it checked
/// `finished()` which is always true for yield completions, causing an infinite
/// spin inside a single step() call — deadlocking cooperative schedulers.
///
/// We simulate lock contention by pre-acquiring pager_commit_lock before
/// calling COMMIT, then verify step() returns Yield instead of hanging.
#[test]
fn test_concurrent_commit_yield_spin() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    // Pre-acquire the pager_commit_lock to simulate another connection
    // holding it mid-commit.
    let mv_store = db.get_mvcc_store();
    let lock = &mv_store.commit_coordinator.pager_commit_lock;
    assert!(lock.write(), "should acquire lock");

    // Prepare COMMIT — step() should yield (return IO), not spin forever.
    let mut stmt = conn.prepare("COMMIT").unwrap();
    let mut returned_io = false;
    for _ in 0..100 {
        match stmt.step().unwrap() {
            crate::StepResult::Yield => {
                returned_io = true;
                break;
            }
            crate::StepResult::Done => break,
            _ => {}
        }
    }
    assert!(
        returned_io,
        "step() should return IO when pager_commit_lock is contended"
    );

    // Release the lock and let the commit finish
    lock.unlock();
    loop {
        match stmt.step().unwrap() {
            crate::StepResult::Done => break,
            crate::StepResult::IO => {}
            _ => {}
        }
    }

    // Verify the insert is visible
    let rows = get_rows(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
}

fn abandon_commit_after_first_io(conn: &Arc<Connection>, mv_store: &Arc<crate::MvStore>) {
    let lock = &mv_store.commit_coordinator.pager_commit_lock;
    assert!(lock.write(), "should acquire commit lock");

    let mut stmt = conn.prepare("COMMIT").unwrap();
    assert!(
        matches!(stmt.step().unwrap(), crate::StepResult::Yield),
        "COMMIT should yield while the commit lock is held",
    );

    drop(stmt);
    lock.unlock();
    conn.close().unwrap();
}

#[test]
fn test_abandoned_commit_rolls_back_insert_with_injected_yield() {
    let db = MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new());
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'new')").unwrap();
    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));

    let mut stmt = conn.prepare("COMMIT").unwrap();
    assert!(
        matches!(stmt.step().unwrap(), crate::StepResult::Yield),
        "MVCC commit should yield before completion",
    );

    drop(stmt);
    conn.close().unwrap();

    let observer = db.connect();
    let rows = get_rows(&observer, "SELECT id FROM t WHERE id = 1");
    assert!(
        rows.is_empty(),
        "row from abandoned INSERT commit remained visible: {rows:?}",
    );
    observer.close().unwrap();
}

/// `PRAGMA journal_mode=mvcc` installs the shared `MvStore` and demotes the
/// connection in `Finalize`, then yields repeatedly while the store bootstraps
/// (reparse, checkpoint, log recovery). If the statement is dropped while
/// parked at one of those bootstrap yields, the abandonment guard must
/// re-promote the connection and uninstall the un-bootstrapped store. Without
/// the guard, `is_mvcc_bootstrap_connection` would stay set forever (silently
/// bypassing MVCC) and other connections on the same `Database` could trip an
/// assertion on the un-bootstrapped store (`global_header = None`) at commit.
///
/// Gated on `io_memory_yield`: the test needs [`crate::MemoryYieldIO`] to defer
/// completions so the bootstrap yields are observable. CI exercises it via the
/// `--all-features` test job.
#[cfg(feature = "io_memory_yield")]
#[test]
fn test_abandoned_journal_mode_mvcc_bootstrap_restores_connection() {
    let _ = tracing_subscriber::fmt::try_init();
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir
        .path()
        .join(format!("test_{}", rand::random::<u64>()));
    // `MemoryYieldIO` defers every completion until the next `io.step()`, so
    // each bootstrap read/write surfaces as a real `StepResult::IO` between
    // `stmt.step()` calls and we can abandon the statement precisely while it
    // is parked at a bootstrap yield (with `PlatformIO` the completions finish
    // synchronously and the whole PRAGMA runs in a single step).
    let io = Arc::new(crate::MemoryYieldIO::new());
    let db = Database::open_file_with_flags(
        io,
        path.as_os_str().to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    // Give the bootstrap real schema/WAL work to span several yields.
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    assert!(
        db.get_mv_store().is_none(),
        "database should start in WAL mode"
    );
    assert!(!conn.is_mvcc_bootstrap_connection());

    let mut stmt = conn.prepare("PRAGMA journal_mode=mvcc").unwrap();
    let mut reached_bootstrap_window = false;
    for _ in 0..10_000 {
        match stmt.step().unwrap() {
            StepResult::IO => {
                // Once `Finalize` has run, the connection is demoted and the
                // store is installed: we are now parked at a bootstrap yield.
                // Abandon here, leaving the IO completion undriven.
                if conn.is_mvcc_bootstrap_connection() {
                    reached_bootstrap_window = true;
                    break;
                }
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(
        reached_bootstrap_window,
        "expected to observe the demoted mid-bootstrap window",
    );
    assert!(
        db.get_mv_store().is_some(),
        "store should be installed during bootstrap",
    );

    // Abandon the statement mid-bootstrap; dropping it drops the
    // `MvccBootstrapGuard` held in its `active_op_state`.
    drop(stmt);

    assert!(
        !conn.is_mvcc_bootstrap_connection(),
        "abandonment guard must re-promote the connection",
    );
    assert!(
        db.get_mv_store().is_none(),
        "abandonment guard must uninstall the un-bootstrapped store",
    );
}

/// `step_build_log_record` chunks the commit's write_set into batches of
/// `MVCC_COMMIT_BATCH_SIZE` rowids and yields between batches so that a
/// large commit (e.g. CREATE INDEX over millions of rows) can't monopolize
/// the executor.
///
/// We bracket the chunked yields with two injected yield points:
/// `BuildLogRecordStart` (fires once on first entry into BuildLogRecord) and
/// `LogRecordPrepared` (fires once after both passes complete). The IOs
/// observed strictly between these two are the chunked yields, so the count
/// is exact.
///
/// With `n_rows = 3 * BATCH_SIZE`, both passes (schema-row + data-row) walk
/// the full write_set, each yielding twice and then transitioning without a
/// final yield. Expected: 4 chunked yields between Start and Prepared.
#[test]
fn test_build_log_record_yields_for_large_write_set() {
    use super::MVCC_COMMIT_BATCH_SIZE;

    /// Yields once at each of the bracketing points and toggles the
    /// corresponding flag so the test can detect when the bracket opens
    /// and closes.
    #[derive(Debug)]
    struct BracketingYieldInjector {
        start: YieldPoint,
        end: YieldPoint,
        started: Arc<AtomicBool>,
        finished: Arc<AtomicBool>,
    }
    impl YieldInjector for BracketingYieldInjector {
        fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
            if point == self.start && !self.started.load(Ordering::SeqCst) {
                self.started.store(true, Ordering::SeqCst);
                return true;
            }
            if point == self.end && !self.finished.load(Ordering::SeqCst) {
                self.finished.store(true, Ordering::SeqCst);
                return true;
            }
            false
        }
    }

    let db = MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new());
    let conn = db.connect();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let n_rows = 3 * MVCC_COMMIT_BATCH_SIZE;
    conn.execute("BEGIN CONCURRENT").unwrap();
    for i in 1..=n_rows {
        conn.execute(format!("INSERT INTO t VALUES ({i}, 'val')"))
            .unwrap();
    }

    let started = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    conn.set_yield_injector(Some(Arc::new(BracketingYieldInjector {
        start: CommitYieldPoint::BuildLogRecordStart.point(),
        end: CommitYieldPoint::LogRecordPrepared.point(),
        started: started.clone(),
        finished: finished.clone(),
    })));

    let mut stmt = conn.prepare("COMMIT").unwrap();
    let mut chunked_io_yields = 0;
    let mut saw_start = false;
    loop {
        match stmt.step().unwrap() {
            crate::StepResult::Yield => {
                if !saw_start {
                    // Wait for the BuildLogRecordStart yield to open the bracket.
                    // IOs before this came from earlier states (Initial → Commit
                    // → WaitForDependencies); they don't count.
                    if started.load(Ordering::SeqCst) {
                        saw_start = true;
                    }
                    continue;
                }
                if finished.load(Ordering::SeqCst) {
                    // The IO we just popped is the LogRecordPrepared injection
                    // closing the bracket. Don't count it.
                    break;
                }
                // Strictly between Start and Prepared: a chunked yield from
                // `Completion::new_yield()` in step_build_log_record's loop.
                chunked_io_yields += 1;
            }
            crate::StepResult::IO => continue,
            crate::StepResult::Done => break,
            other => panic!("unexpected step result: {other:?}"),
        }
    }

    assert!(
        saw_start,
        "BuildLogRecordStart yield never fired — BuildLogRecord state never reached"
    );
    assert!(
        finished.load(Ordering::SeqCst),
        "LogRecordPrepared yield never fired — BuildLogRecord did not complete"
    );
    // n_rows = 3 * BATCH_SIZE → 2 yields per pass × 2 passes = 4 chunked yields.
    assert_eq!(
        chunked_io_yields, 4,
        "with {n_rows} rows, expected exactly 4 chunked IO yields between \
         BuildLogRecordStart and LogRecordPrepared, got {chunked_io_yields}"
    );

    drop(stmt);
    conn.close().unwrap();
}

/// Regression guard for the `mv_store.txs` ↔ `connection.mv_tx_id` divergence
/// originally observed in production as `Transaction <id> not found while
/// releasing savepoint` (panic) and `NoSuchTransactionID(<id>)` (read-path
/// error) — see Antithesis Limbo run, 2026-04-27.
///
/// **Bug shape (pre-fix):** `CommitStateMachine` called `mvcc_store.remove_tx(tx_id)`
/// directly. The connection-cache clear (`conn.set_mv_tx(None)`) lived at the
/// caller (vdbe/mod.rs:1898) and only ran on the success path. If anything
/// between `remove_tx` and that caller-side clear failed or yielded I/O and
/// then the runtime abandoned the task before re-entering, the cache was
/// stranded pointing at a tx that was already gone from `txs`. The natural
/// trigger in production was an IO yield from `CheckpointStateMachine::step`
/// (called after `remove_tx` at the EndCommitLogicalLog site) followed by
/// task abandonment under network partition.
///
/// **Fix:** `MvStore::finish_committed_tx(tx_id, conn, db_id)` clears the
/// connection's mv_tx cache and removes the tx from `txs` together,
/// atomically. All three commit sites that previously called `remove_tx`
/// directly now call `finish_committed_tx`. After this, no in-flight state
/// (Err propagation, IO yield + abandon, success) can produce the divergent
/// `(cache=Some, txs=None)` pair — they're mutated as a single act.
///
/// **What this test exercises:** we inject a `TxError` at the historical
/// post-`remove_tx` boundary (`CommitYieldPoint::AfterRemoveTx`). The abort
/// handler at vdbe/mod.rs:2204 explicitly skips rollback for `TxError`, so
/// pre-fix nothing else would have cleared the cache — the divergence would
/// surface. Post-fix, `finish_committed_tx` already cleared the cache before
/// the injection point fires, so both stores are gone in lock-step and a
/// follow-up read on the connection sees a clean state.
#[test]
fn test_commit_failure_after_remove_tx_does_not_strand_conn_cache() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'new')").unwrap();

    let tx_id = conn.get_mv_tx_id().expect("tx should be open after BEGIN");
    let mv_store = db.get_mvcc_store();
    assert!(
        mv_store.txs.get(&tx_id).is_some(),
        "precondition: tx must be live in txs before COMMIT"
    );

    conn.set_failure_injector(Some(FixedFailureInjector::new([(
        CommitYieldPoint::AfterRemoveTx.point(),
        // `TxError` is in the no-rollback list at vdbe/mod.rs:2204, so the abort
        // handler will not rescue stranded state on its own — the only thing
        // keeping the connection coherent here is `finish_committed_tx`.
        LimboError::TxError("synthetic post-remove_tx failure".to_string()),
    )])));

    let commit_err = conn
        .execute("COMMIT")
        .expect_err("commit must fail at the injected boundary");
    tracing::info!("injected commit failure: {commit_err}");

    // The pairing invariant: `finish_committed_tx` clears both atomically, so
    // after the injected Err we see them gone together — no half-state.
    assert!(
        mv_store.txs.get(&tx_id).is_none(),
        "fix: tx must be gone from txs (finish_committed_tx ran before the \
         injection point)"
    );
    assert_eq!(
        conn.get_mv_tx_id(),
        None,
        "fix: connection mv_tx cache must be cleared in lock-step with the \
         txs removal — pre-fix this stranded the cache"
    );

    // NOTE: we deliberately do not assert anything about the *visibility* of
    // the failed-commit's INSERT here. By the time the injection fires at
    // `AfterRemoveTx`, the commit pipeline has already published
    // `tx.state = Committed(end_ts)` and timestamp-rewritten live versions
    // (mod.rs:1901-1903) — so the row IS visible to subsequent readers. That's
    // a separate "Err on COMMIT but data is durable" semantic concern that
    // predates this fix and applies equally to the production network-partition
    // scenario; it's not what this regression test is guarding. This test
    // verifies only the pairing invariant: `mv_store.txs` and
    // `conn.mv_tx_id` mutate in lock-step, leaving the connection reusable.

    conn.close().unwrap();
}

/// if a txn made some inserts, then aborted (or abandoned due to some IO issue), then those
/// inserted rows should not be visible
#[test]
fn test_abandoned_commit_rolls_back_insert() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'new')").unwrap();

    let mv_store = db.get_mvcc_store();
    abandon_commit_after_first_io(&conn, &mv_store);

    let observer = db.connect();
    let rows = get_rows(&observer, "SELECT id FROM t WHERE id = 1");
    assert!(
        rows.is_empty(),
        "row from abandoned INSERT commit remained visible: {rows:?}",
    );
    observer.close().unwrap();
}

/// if a txn deleted some existing rows, but then aborted (or abandoned due to some IO issue), then
/// those rows should not become deleted
#[test]
fn test_abandoned_commit_rolls_back_delete() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'seed')").unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();

    let mv_store = db.get_mvcc_store();
    abandon_commit_after_first_io(&conn, &mv_store);

    let observer = db.connect();
    let rows = get_rows(&observer, "SELECT id, v FROM t WHERE id = 1");
    assert_eq!(
        rows,
        vec![vec![
            Value::Numeric(Numeric::Integer(1)),
            Value::Text(Text::new("seed".to_string())),
        ]],
        "row disappeared after abandoned DELETE commit: {rows:?}",
    );
    observer.close().unwrap();
}

/// ALTER TABLE RENAME TO on a table with a CREATE INDEX panics on the next
/// session open. Reproduces the issue with 3 separate sessions (DB restarts).
#[test]
fn test_alter_table_rename_with_index_panics_on_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    // Session 1: Create indexed table + checkpoint
    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1")
            .unwrap();
        conn.execute("CREATE TABLE old_name(id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX idx_val ON old_name(val)")
            .unwrap();
        conn.execute("INSERT INTO old_name VALUES (1, 'a')")
            .unwrap();
        conn.close().unwrap();
    }
    // Session 2: Rename table
    db.restart();
    {
        let conn = db.connect();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.execute("ALTER TABLE old_name RENAME TO new_name")
            .unwrap();
        conn.close().unwrap();
    }
    // Session 3: PANIC
    db.restart();
    {
        let conn = db.connect();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        let rows = get_rows(&conn, "SELECT * FROM new_name");
        assert_eq!(rows.len(), 1);
    }
}

/// Same as above but with a UNIQUE constraint (autoindex).
#[test]
fn test_alter_table_rename_with_unique_constraint_panics_on_restart() {
    let _ = tracing_subscriber::fmt::try_init();
    let mut db = MvccTestDbNoConn::new_with_random_db();
    // Session 1
    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1")
            .unwrap();
        conn.execute("CREATE TABLE old_name(id INTEGER PRIMARY KEY, val TEXT UNIQUE)")
            .unwrap();
        conn.execute("INSERT INTO old_name VALUES (1, 'a')")
            .unwrap();
        conn.close().unwrap();
    }
    // Session 2
    db.restart();
    {
        let conn = db.connect();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.execute("ALTER TABLE old_name RENAME TO new_name")
            .unwrap();
        conn.close().unwrap();
    }
    // Session 3: PANIC
    db.restart();
    {
        let conn = db.connect();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        let rows = get_rows(&conn, "SELECT * FROM new_name");
        assert_eq!(rows.len(), 1);
    }
}

#[test]
fn test_checkpoint_skips_uncheckpointed_view_and_trigger_deletes_after_recovery() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, b TEXT)")
            .unwrap();
        conn.execute("CREATE VIEW v_t AS SELECT id, b FROM t")
            .unwrap();
        conn.execute(
            "CREATE TRIGGER tr_t_ai AFTER INSERT ON t
             BEGIN
               UPDATE t SET b = NEW.b || '_tr' WHERE id = NEW.id;
             END",
        )
        .unwrap();
        conn.close().unwrap();
    }

    db.restart();
    {
        db.get_mvcc_store().set_checkpoint_threshold(-1);
        let conn = db.connect();
        conn.execute("BEGIN").unwrap();
        conn.execute("DROP VIEW v_t").unwrap();
        conn.execute("DROP TRIGGER tr_t_ai").unwrap();
        conn.execute("COMMIT").unwrap();
        conn.close().unwrap();
    }

    db.restart();
    let conn = db.connect();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(
        &conn,
        "SELECT type, name FROM sqlite_schema WHERE name NOT LIKE '__turso%' ORDER BY rowid",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_string(), "table");
    assert_eq!(rows[0][1].to_string(), "t");
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_checkpoint_deletes_checkpointed_view_and_trigger_schema_rows_after_recovery() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, b TEXT)")
            .unwrap();
        conn.execute("CREATE VIEW v_t AS SELECT id, b FROM t")
            .unwrap();
        conn.execute(
            "CREATE TRIGGER tr_t_ai AFTER INSERT ON t
             BEGIN
               UPDATE t SET b = NEW.b || '_tr' WHERE id = NEW.id;
             END",
        )
        .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }

    db.restart();
    {
        let conn = db.connect();
        let rows = get_rows(
            &conn,
            "SELECT type, name FROM sqlite_schema WHERE name NOT LIKE '__turso%' ORDER BY rowid",
        );
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0].to_string(), "table");
        assert_eq!(rows[0][1].to_string(), "t");
        assert_eq!(rows[1][0].to_string(), "view");
        assert_eq!(rows[1][1].to_string(), "v_t");
        assert_eq!(rows[2][0].to_string(), "trigger");
        assert_eq!(rows[2][1].to_string(), "tr_t_ai");
        conn.close().unwrap();
    }

    db.get_mvcc_store().set_checkpoint_threshold(-1);
    {
        let conn = db.connect();
        conn.execute("DROP VIEW v_t").unwrap();
        conn.execute("DROP TRIGGER tr_t_ai").unwrap();
        conn.close().unwrap();
    }

    db.restart();
    {
        let conn = db.connect();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }

    db.restart();
    let conn = db.connect();
    let rows = get_rows(
        &conn,
        "SELECT type, name FROM sqlite_schema WHERE name NOT LIKE '__turso%' ORDER BY rowid",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_string(), "table");
    assert_eq!(rows[0][1].to_string(), "t");
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Reproducer for "sqlite_schema contains index for missing table 't'".
///
/// A single transaction deletes a row from t and then runs
/// `ALTER TABLE t ADD COLUMN`. The deleted row already exists in the db file,
/// and idx already has an entry for it. The log therefore contains a
/// DELETE_INDEX op for that one idx entry; it is not deleting idx itself.
/// ADD COLUMN is used because it is a small way to get the general shape that
/// matters here: a schema-row DELETE plus a replacement schema-row UPSERT in the
/// same transaction as table/index row changes.
///
/// The old BuildLogRecord path inserted every committed version into one log
/// vector with `insert_version_raw`. That helper is only valid for one entry in
/// the MVCC maps, but the log vector is replayed in serialized order. Since the
/// old sqlite_schema row for t is already in the db file, ALTER TABLE logs its
/// DELETE with `begin=None` and its replacement UPSERT with `begin=end_ts`.
/// Sorting every touched entry together could replay the schema DELETE, row
/// DELETE, and index-entry DELETE before the schema UPSERT for t's new CREATE
/// TABLE text.
///
/// During replay, the table schema DELETE removes t from `schema_rows` and sets
/// `needs_schema_rebuild=true`. The following DELETE_INDEX op calls
/// `get_index_info` to resolve idx's key format before the table schema UPSERT
/// has been decoded. `get_index_info` sees `needs_schema_rebuild=true` and calls
/// `rebuild_schema(&schema_rows)`, so `populate_indices` sees t missing while
/// the btree-loaded idx schema row is still present and reports
/// "sqlite_schema contains index for missing table".
///
/// Recovery must decode index ops with schema metadata chosen for the whole
/// transaction frame, not with a schema rebuilt halfway through the frame.
#[test]
fn test_alter_add_column_with_index_dml_does_not_corrupt_on_reopen() {
    let _ = tracing_subscriber::fmt::try_init();
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(v INTEGER)").unwrap();
        conn.execute("CREATE INDEX idx ON t(v)").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }
    {
        db.get_mvcc_store().set_checkpoint_threshold(-1);
        let conn = db.connect();
        conn.execute("BEGIN").unwrap();
        conn.execute("DELETE FROM t").unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN x INTEGER").unwrap();
        conn.execute("COMMIT").unwrap();
        conn.close().unwrap();
    }
    db.restart();
    let conn = db.connect();
    let names: Vec<String> = get_rows(&conn, "SELECT name FROM sqlite_schema ORDER BY rowid")
        .iter()
        .map(|r| r[0].to_string())
        .collect();
    assert!(
        names.contains(&"t".to_string()),
        "'t' table missing from sqlite_schema after reopen; got {names:?}"
    );
    assert!(
        names.contains(&"idx".to_string()),
        "index missing from sqlite_schema after reopen; got {names:?}"
    );
}

/// Reproducer for `Index with root page ... not found in schema`.
///
/// A single transaction deletes a row from t and then drops idx. The deleted row
/// already exists in the db file, and idx already has an entry for it. The log
/// therefore contains a DELETE_INDEX op for that one idx entry, plus a
/// sqlite_schema DELETE for idx itself.
///
/// This is the opposite side of the ALTER TABLE ADD COLUMN case above. The
/// index-entry DELETE needs the old idx schema row in order to decode the index
/// key. If BuildLogRecord writes the sqlite_schema DELETE before the
/// DELETE_INDEX op, recovery removes idx from `schema_rows`, rebuilds
/// `connection.schema`, then cannot resolve idx's root page when decoding the
/// later DELETE_INDEX op.
///
/// This is why frame recovery chooses schema metadata from the whole frame.
/// CREATE INDEX insert ops need the final schema; DROP INDEX delete ops need
/// the old schema for entries that existed before the transaction.
#[test]
fn test_delete_then_drop_index_with_index_dml_replays_on_reopen() {
    let _ = tracing_subscriber::fmt::try_init();
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(v INTEGER)").unwrap();
        conn.execute("CREATE INDEX idx ON t(v)").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }
    {
        db.get_mvcc_store().set_checkpoint_threshold(-1);
        let conn = db.connect();
        conn.execute("BEGIN").unwrap();
        conn.execute("DELETE FROM t").unwrap();
        conn.execute("DROP INDEX idx").unwrap();
        conn.execute("COMMIT").unwrap();
        conn.close().unwrap();
    }
    db.restart();
    let conn = db.connect();
    let names: Vec<String> = get_rows(&conn, "SELECT name FROM sqlite_schema ORDER BY rowid")
        .iter()
        .map(|r| r[0].to_string())
        .collect();
    assert!(
        names.contains(&"t".to_string()),
        "'t' table missing from sqlite_schema after reopen; got {names:?}"
    );
    assert!(
        !names.contains(&"idx".to_string()),
        "dropped index still present in sqlite_schema after reopen; got {names:?}"
    );
    let rows = get_rows(&conn, "SELECT v FROM t");
    assert!(
        rows.is_empty(),
        "deleted row should stay deleted after reopen; got {rows:?}"
    );
}

/// A transient index created and dropped in one transaction should leave no
/// logical-log index work behind.
///
/// CREATE INDEX writes sqlite_schema and index entries with `begin=tx_id`.
/// DROP INDEX ends those same versions before commit. Those entries never
/// reached the db file, so recovery cannot depend on their schema existing
/// before or after the frame. The writer must omit them instead of logging a
/// log op for an index that has no durable schema row.
#[test]
fn test_create_then_drop_index_in_one_tx_replays_on_reopen() {
    let _ = tracing_subscriber::fmt::try_init();
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(v INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }
    {
        db.get_mvcc_store().set_checkpoint_threshold(-1);
        let conn = db.connect();
        conn.execute("BEGIN").unwrap();
        conn.execute("CREATE INDEX idx ON t(v)").unwrap();
        conn.execute("DROP INDEX idx").unwrap();
        conn.execute("COMMIT").unwrap();
        conn.close().unwrap();
    }
    db.restart();
    let conn = db.connect();
    let names: Vec<String> = get_rows(&conn, "SELECT name FROM sqlite_schema ORDER BY rowid")
        .iter()
        .map(|r| r[0].to_string())
        .collect();
    assert!(
        names.contains(&"t".to_string()),
        "'t' table missing from sqlite_schema after reopen; got {names:?}"
    );
    assert!(
        !names.contains(&"idx".to_string()),
        "transient index should not remain in sqlite_schema after reopen; got {names:?}"
    );
    let rows = get_rows(&conn, "SELECT v FROM t ORDER BY v");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// These cases came from an adversarial DDL/DML matrix. They did not find a
/// failure, but they cover schema-before/schema-after combinations that the
/// frame-level recovery code must keep working: dropped indexes, newly-created
/// indexes, table recreation, and both checkpointed and uncheckpointed base
/// schemas.
#[test]
fn test_schema_frame_recovery_drop_index_with_remaining_index_matrix() {
    for checkpoint_base in [false, true] {
        let mut db = MvccTestDbNoConn::new_with_random_db();
        {
            let conn = db.connect();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
                .unwrap();
            conn.execute("CREATE INDEX idx_a ON t(a)").unwrap();
            conn.execute("CREATE INDEX idx_b ON t(b)").unwrap();
            conn.execute("INSERT INTO t VALUES (1, 10, 100)").unwrap();
            conn.execute("INSERT INTO t VALUES (2, 20, 200)").unwrap();
            conn.execute("INSERT INTO t VALUES (3, 30, 300)").unwrap();
            if checkpoint_base {
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            }
            conn.close().unwrap();
        }
        {
            db.get_mvcc_store().set_checkpoint_threshold(-1);
            let conn = db.connect();
            conn.execute("BEGIN").unwrap();
            conn.execute("DELETE FROM t WHERE id = 1").unwrap();
            conn.execute("DROP INDEX idx_a").unwrap();
            conn.execute("UPDATE t SET b = 250 WHERE id = 2").unwrap();
            conn.execute("INSERT INTO t VALUES (4, 40, 400)").unwrap();
            conn.execute("COMMIT").unwrap();
            conn.close().unwrap();
        }

        db.restart();
        let conn = db.connect();
        let names: Vec<String> = get_rows(
            &conn,
            "SELECT name FROM sqlite_schema WHERE tbl_name = 't' ORDER BY rowid",
        )
        .iter()
        .map(|r| r[0].to_string())
        .collect();
        assert!(names.contains(&"t".to_string()), "table missing: {names:?}");
        assert!(
            !names.contains(&"idx_a".to_string()),
            "dropped idx_a still present after reopen: {names:?}"
        );
        assert!(
            names.contains(&"idx_b".to_string()),
            "remaining idx_b missing after reopen: {names:?}"
        );
        let rows = get_rows(
            &conn,
            "SELECT id, b FROM t INDEXED BY idx_b WHERE b >= 250 ORDER BY b",
        );
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0].as_int().unwrap(), 2);
        assert_eq!(rows[0][1].as_int().unwrap(), 250);
        assert_eq!(rows[1][0].as_int().unwrap(), 3);
        assert_eq!(rows[1][1].as_int().unwrap(), 300);
        assert_eq!(rows[2][0].as_int().unwrap(), 4);
        assert_eq!(rows[2][1].as_int().unwrap(), 400);
        let rows = get_rows(&conn, "PRAGMA integrity_check");
        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][0].to_string(), "ok");
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();

        db.restart();
        let conn = db.connect();
        let rows = get_rows(
            &conn,
            "SELECT id, b FROM t INDEXED BY idx_b WHERE b >= 250 ORDER BY b",
        );
        assert_eq!(rows.len(), 3);
    }
}

#[test]
fn test_schema_frame_recovery_create_index_with_mixed_dml_matrix() {
    for checkpoint_base in [false, true] {
        let mut db = MvccTestDbNoConn::new_with_random_db();
        {
            let conn = db.connect();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
                .unwrap();
            conn.execute("CREATE INDEX idx_a ON t(a)").unwrap();
            conn.execute("INSERT INTO t VALUES (1, 10, 100)").unwrap();
            conn.execute("INSERT INTO t VALUES (2, 20, 200)").unwrap();
            conn.execute("INSERT INTO t VALUES (3, 30, 300)").unwrap();
            if checkpoint_base {
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            }
            conn.close().unwrap();
        }
        {
            db.get_mvcc_store().set_checkpoint_threshold(-1);
            let conn = db.connect();
            conn.execute("BEGIN").unwrap();
            conn.execute("UPDATE t SET a = 21 WHERE id = 2").unwrap();
            conn.execute("INSERT INTO t VALUES (4, 40, 400)").unwrap();
            conn.execute("DELETE FROM t WHERE id = 1").unwrap();
            conn.execute("CREATE INDEX idx_b ON t(b)").unwrap();
            conn.execute("UPDATE t SET b = 333 WHERE id = 3").unwrap();
            conn.execute("COMMIT").unwrap();
            conn.close().unwrap();
        }

        db.restart();
        let conn = db.connect();
        let names: Vec<String> = get_rows(
            &conn,
            "SELECT name FROM sqlite_schema WHERE tbl_name = 't' ORDER BY rowid",
        )
        .iter()
        .map(|r| r[0].to_string())
        .collect();
        assert!(
            names.contains(&"idx_a".to_string()),
            "idx_a missing: {names:?}"
        );
        assert!(
            names.contains(&"idx_b".to_string()),
            "idx_b missing: {names:?}"
        );
        let rows = get_rows(
            &conn,
            "SELECT id, b FROM t INDEXED BY idx_b WHERE b >= 200 ORDER BY b",
        );
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0].as_int().unwrap(), 2);
        assert_eq!(rows[0][1].as_int().unwrap(), 200);
        assert_eq!(rows[1][0].as_int().unwrap(), 3);
        assert_eq!(rows[1][1].as_int().unwrap(), 333);
        assert_eq!(rows[2][0].as_int().unwrap(), 4);
        assert_eq!(rows[2][1].as_int().unwrap(), 400);
        let rows = get_rows(&conn, "PRAGMA integrity_check");
        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][0].to_string(), "ok");
    }
}

#[test]
fn test_schema_frame_recovery_drop_recreate_table_indexes_matrix() {
    for checkpoint_base in [false, true] {
        let mut db = MvccTestDbNoConn::new_with_random_db();
        {
            let conn = db.connect();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)")
                .unwrap();
            conn.execute("CREATE INDEX idx_a ON t(a)").unwrap();
            conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
            conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
            if checkpoint_base {
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            }
            conn.close().unwrap();
        }
        {
            db.get_mvcc_store().set_checkpoint_threshold(-1);
            let conn = db.connect();
            conn.execute("BEGIN").unwrap();
            conn.execute("DELETE FROM t WHERE id = 1").unwrap();
            conn.execute("DROP TABLE t").unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, b TEXT, c INTEGER)")
                .unwrap();
            conn.execute("CREATE INDEX idx_c ON t(c)").unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'new', 30)").unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'next', 40)")
                .unwrap();
            conn.execute("UPDATE t SET c = 45 WHERE id = 2").unwrap();
            conn.execute("COMMIT").unwrap();
            conn.close().unwrap();
        }

        db.restart();
        let conn = db.connect();
        let names: Vec<String> = get_rows(
            &conn,
            "SELECT name FROM sqlite_schema WHERE tbl_name = 't' ORDER BY rowid",
        )
        .iter()
        .map(|r| r[0].to_string())
        .collect();
        assert!(names.contains(&"t".to_string()), "table missing: {names:?}");
        assert!(
            !names.contains(&"idx_a".to_string()),
            "old idx_a still present after recreate: {names:?}"
        );
        assert!(
            names.contains(&"idx_c".to_string()),
            "new idx_c missing after recreate: {names:?}"
        );
        let rows = get_rows(&conn, "SELECT id, b, c FROM t INDEXED BY idx_c ORDER BY c");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].as_int().unwrap(), 1);
        assert_eq!(rows[0][1].to_string(), "new");
        assert_eq!(rows[0][2].as_int().unwrap(), 30);
        assert_eq!(rows[1][0].as_int().unwrap(), 2);
        assert_eq!(rows[1][1].to_string(), "next");
        assert_eq!(rows[1][2].as_int().unwrap(), 45);
        let rows = get_rows(&conn, "PRAGMA integrity_check");
        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][0].to_string(), "ok");
    }
}

#[test]
fn test_schema_frame_recovery_same_name_partial_index_redefinition() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
            .unwrap();
        conn.execute("CREATE INDEX idx_common ON t(a) WHERE a >= 20")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10, 100)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 20, 200)").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 30, 300)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }
    {
        db.get_mvcc_store().set_checkpoint_threshold(-1);
        let conn = db.connect();
        conn.execute("BEGIN").unwrap();
        conn.execute("UPDATE t SET a = 25 WHERE id = 2").unwrap();
        conn.execute("DROP INDEX idx_common").unwrap();
        conn.execute("CREATE INDEX idx_common ON t(b) WHERE b >= 250")
            .unwrap();
        conn.execute("UPDATE t SET b = 275 WHERE id = 2").unwrap();
        conn.execute("INSERT INTO t VALUES (4, 40, 400)").unwrap();
        conn.execute("COMMIT").unwrap();
        conn.close().unwrap();
    }

    db.restart();
    let conn = db.connect();
    let sql_rows = get_rows(
        &conn,
        "SELECT sql FROM sqlite_schema WHERE name = 'idx_common'",
    );
    assert_eq!(sql_rows.len(), 1);
    let index_sql = sql_rows[0][0].to_string();
    assert!(
        index_sql.contains("ON t (b)") && index_sql.contains("WHERE b >= 250"),
        "idx_common should be recreated on b with the partial predicate; got {index_sql}"
    );
    let rows = get_rows(
        &conn,
        "SELECT id, b FROM t INDEXED BY idx_common WHERE b >= 250 ORDER BY b",
    );
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_int().unwrap(), 2);
    assert_eq!(rows[0][1].as_int().unwrap(), 275);
    assert_eq!(rows[1][0].as_int().unwrap(), 3);
    assert_eq!(rows[1][1].as_int().unwrap(), 300);
    assert_eq!(rows[2][0].as_int().unwrap(), 4);
    assert_eq!(rows[2][1].as_int().unwrap(), 400);
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_schema_rewrites_do_not_drop_table_versions_from_recovery_log() {
    for checkpoint_base in [false, true] {
        let mut db = MvccTestDbNoConn::new_with_random_db();
        {
            let conn = db.connect();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, note TEXT)")
                .unwrap();
            conn.execute("CREATE INDEX idx_a ON t(a)").unwrap();
            conn.execute("CREATE INDEX idx_b ON t(b)").unwrap();
            conn.execute("CREATE UNIQUE INDEX idx_note ON t(note)")
                .unwrap();
            conn.execute("INSERT INTO t VALUES(1, 10, 100, 'n1')")
                .unwrap();
            conn.execute("INSERT INTO t VALUES(2, 20, 200, 'n2')")
                .unwrap();
            conn.execute("INSERT INTO t VALUES(3, 30, 300, 'n3')")
                .unwrap();
            if checkpoint_base {
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            }
            conn.close().unwrap();
        }

        db.restart();
        {
            db.get_mvcc_store().set_checkpoint_threshold(-1);
            let conn = db.connect();
            conn.execute("BEGIN").unwrap();
            conn.execute("ALTER TABLE t RENAME TO tt").unwrap();
            conn.execute("ALTER TABLE tt RENAME COLUMN note TO label")
                .unwrap();
            conn.execute("UPDATE tt SET a = a + 12 WHERE id = 1")
                .unwrap();
            conn.execute("ALTER TABLE tt ADD COLUMN c INTEGER DEFAULT 5")
                .unwrap();
            conn.execute("UPDATE tt SET c = a + b WHERE id = 2")
                .unwrap();
            conn.execute("CREATE INDEX idx_label ON tt(label)").unwrap();
            conn.execute(
                "INSERT INTO tt(id,a,b,label,c) VALUES(4, 40, 472, 'n4', 912)
                 ON CONFLICT(id) DO UPDATE
                 SET a = excluded.a, b = excluded.b, label = excluded.label, c = excluded.c",
            )
            .unwrap();
            conn.execute("DROP INDEX idx_b").unwrap();
            conn.execute("COMMIT").unwrap();
            conn.close().unwrap();
        }

        db.restart();
        let conn = db.connect();
        let table_rows = get_rows(&conn, "SELECT id, a, b, label, c FROM tt ORDER BY id");
        assert_eq!(table_rows.len(), 4, "checkpoint_base={checkpoint_base}");
        assert_eq!(table_rows[0][0].as_int().unwrap(), 1);
        assert_eq!(table_rows[0][1].as_int().unwrap(), 22);
        assert_eq!(table_rows[0][4].as_int().unwrap(), 5);
        assert_eq!(table_rows[1][0].as_int().unwrap(), 2);
        assert_eq!(table_rows[1][2].as_int().unwrap(), 200);
        assert_eq!(table_rows[1][4].as_int().unwrap(), 220);
        assert_eq!(table_rows[2][0].as_int().unwrap(), 3);
        assert_eq!(table_rows[2][4].as_int().unwrap(), 5);
        assert_eq!(table_rows[3][0].as_int().unwrap(), 4);
        assert_eq!(table_rows[3][1].as_int().unwrap(), 40);
        assert_eq!(table_rows[3][3].to_string(), "n4");
        assert_eq!(table_rows[3][4].as_int().unwrap(), 912);

        let indexed_rows = get_rows(
            &conn,
            "SELECT id, label FROM tt INDEXED BY idx_label WHERE label >= 'n1' ORDER BY label, id",
        );
        assert_eq!(indexed_rows.len(), 4, "checkpoint_base={checkpoint_base}");
        assert_eq!(indexed_rows[3][0].as_int().unwrap(), 4);
        assert_eq!(indexed_rows[3][1].to_string(), "n4");

        let rows = get_rows(&conn, "PRAGMA integrity_check");
        assert_eq!(rows.len(), 1, "checkpoint_base={checkpoint_base}");
        assert_eq!(&rows[0][0].to_string(), "ok");
    }
}

/// Updating a row that already exists in the db file creates an MVCC
/// replacement version. If the same transaction deletes that row, recovery must
/// not replay both the old-row delete and a second delete for the replacement:
/// only the old row ever existed in the db file.
#[test]
fn test_btree_resident_update_then_delete_checkpoints_after_reopen() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        conn.execute("CREATE INDEX idx_v ON t(v)").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }
    {
        db.get_mvcc_store().set_checkpoint_threshold(-1);
        let conn = db.connect();
        conn.execute("BEGIN").unwrap();
        conn.execute("UPDATE t SET v = 15 WHERE id = 1").unwrap();
        conn.execute("DELETE FROM t WHERE id = 1").unwrap();
        conn.execute("COMMIT").unwrap();
        conn.close().unwrap();
    }

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t INDEXED BY idx_v ORDER BY v");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 2);
    assert_eq!(rows[0][1].as_int().unwrap(), 20);
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
}

#[test]
fn test_schema_frame_recovery_rename_column_then_drop_index_checkpoints_after_reopen() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX i_b ON t(b)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }
    {
        db.get_mvcc_store().set_checkpoint_threshold(-1);
        let conn = db.connect();
        conn.execute("BEGIN").unwrap();
        conn.execute("ALTER TABLE t RENAME COLUMN b TO bb").unwrap();
        conn.execute("DROP INDEX i_b").unwrap();
        conn.execute("COMMIT").unwrap();
        conn.close().unwrap();
    }

    db.restart();
    let conn = db.connect();
    let schema_rows = get_rows(
        &conn,
        "SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE name NOT LIKE '__turso%' ORDER BY rowid",
    );
    assert_eq!(schema_rows.len(), 1);
    assert_eq!(schema_rows[0][0].to_string(), "table");
    assert_eq!(schema_rows[0][1].to_string(), "t");
    assert_eq!(schema_rows[0][2].to_string(), "t");
    assert_eq!(
        schema_rows[0][3].to_string(),
        "CREATE TABLE t (a INTEGER PRIMARY KEY, bb TEXT)"
    );
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
}

/// Reproducer: DROP TABLE ghost data after restart without explicit checkpoint.
/// Session 1: create + insert + checkpoint. Session 2: drop. Session 3: reopen.
#[test]
fn test_close_persists_drop_table() {
    // Session 1: create table, insert data, checkpoint to DB file
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE todrop(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO todrop VALUES (1, 'data')")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.close().unwrap();

    // Session 2: drop table (no explicit checkpoint, rely on close)
    let conn = db.connect();
    conn.execute("DROP TABLE todrop").unwrap();
    conn.close().unwrap();

    // Session 3: reopen — table must be gone
    db.restart();
    let conn = db.connect();

    // The table must not exist — CREATE should succeed
    let create_result = conn.execute("CREATE TABLE todrop(id INTEGER PRIMARY KEY, newval TEXT)");
    assert!(
        create_result.is_ok(),
        "CREATE TABLE should succeed after DROP, but got: {:?}",
        create_result.unwrap_err()
    );

    // No ghost data from old table
    let rows = get_rows(&conn, "SELECT * FROM todrop");
    assert!(rows.is_empty(), "New table should be empty, got {rows:?}");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_abandoned_drop() {
    let _ = tracing_subscriber::fmt::try_init();
    let io = Arc::new(MemoryIO::new());
    let path = ":memory:";
    let db = Database::open_file(io.clone(), path, Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();

    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, row_number INTEGER, ts INTEGER)")
        .unwrap();
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS t_index \
         ON t (row_number) WHERE ts IS NULL",
    )
    .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    assert!(conn.get_auto_commit());

    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CursorYieldPoint::NextStart.point()
    ])));
    conn.execute("BEGIN").unwrap();
    let mut drop_stmt = conn.prepare("DROP TABLE t").unwrap();
    match drop_stmt.step().unwrap() {
        crate::StepResult::Yield => {}
        other => panic!("expected injected yield mid-DROP TABLE; got {other:?}"),
    }
    conn.set_yield_injector(None);

    drop_stmt.reset().unwrap();
    drop(drop_stmt);

    conn.execute("COMMIT").unwrap();

    drop(conn);
    drop(db);

    let db = Database::open_file(io, path, Arc::new(SqliteDialect)).expect(
        "reopen should not fail; abandoned DROP must not have committed its partial Delete",
    );
    let conn = db.connect().unwrap();
    let after = get_rows(
        &conn,
        "SELECT type, name FROM sqlite_schema \
         WHERE tbl_name = 't' ORDER BY rowid",
    );
    assert!(
        after.len() == 2,
        "schema must not be half-dropped; got rows: {after:?}",
    );
}

/// Reproducer: DROP INDEX ghost pages after restart without explicit checkpoint.
/// Session 1: create table + index + insert + checkpoint. Session 2: drop index. Session 3: reopen.
#[test]
fn test_close_persists_drop_index() {
    // Session 1: create table/index, insert data, checkpoint to DB file
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE tdropidx(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_tdropidx_val ON tdropidx(val)")
        .unwrap();
    conn.execute("INSERT INTO tdropidx VALUES (1, 'data')")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.close().unwrap();

    // Session 2: drop index (no explicit checkpoint, rely on close)
    let conn = db.connect();
    conn.execute("DROP INDEX idx_tdropidx_val").unwrap();
    conn.close().unwrap();

    // Session 3: reopen - index must be gone and integrity check must pass
    db.restart();
    let conn = db.connect();

    let recreate_index = conn.execute("CREATE INDEX idx_tdropidx_val ON tdropidx(val)");
    assert!(
        recreate_index.is_ok(),
        "CREATE INDEX should succeed after DROP INDEX, but got: {:?}",
        recreate_index.unwrap_err()
    );

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_partial_commit_visibility_bug() {
    use crate::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use crate::sync::Arc;
    use std::collections::HashMap;
    use std::thread;
    use std::time::Duration;
    for _ in 0..10 {
        // Setup: Create a table with batch_id and row_num columns
        let db = Arc::new(MvccTestDbNoConn::new_with_random_db());
        {
            let conn = db.connect();
            conn.execute("CREATE TABLE consistency_test (batch_id INTEGER, row_num INTEGER)")
                .unwrap();
        }

        const ROWS_PER_BATCH: i64 = 50; // Large enough to increase race window
        const NUM_BATCHES: u64 = 100;
        const NUM_READER_THREADS: usize = 4;

        let writer_done = Arc::new(AtomicBool::new(false));
        let violation_detected = Arc::new(AtomicBool::new(false));
        let current_batch = Arc::new(AtomicU64::new(0));

        // Writer thread: Insert batches of rows
        let writer_handle = {
            let db = db.clone();
            let writer_done = writer_done.clone();
            let current_batch = current_batch.clone();
            thread::spawn(move || {
                let conn = db.connect();

                for batch_id in 0..NUM_BATCHES {
                    // Start a transaction
                    conn.execute("BEGIN CONCURRENT").unwrap();

                    // Insert ROWS_PER_BATCH rows with the same batch_id
                    // This simulates a multi-row operation like a bank transfer
                    for row_num in 0..ROWS_PER_BATCH {
                        conn.execute(format!(
                            "INSERT INTO consistency_test VALUES ({batch_id}, {row_num})",
                        ))
                        .unwrap();
                    }

                    // Update current batch before committing to allow readers to check
                    current_batch.store(batch_id, Ordering::Release);

                    // Commit the transaction
                    // BUG LOCATION: During commit, the loop at mod.rs:912-984 updates
                    // row timestamps one-by-one while state remains Preparing.
                    // Concurrent readers can see partial updates.
                    conn.execute("COMMIT").unwrap();

                    // Small delay to allow readers to observe the race window
                    thread::sleep(Duration::from_micros(100));
                }

                writer_done.store(true, Ordering::Release);
            })
        };

        // Reader threads: Continuously read and verify batch consistency
        let mut reader_handles = Vec::new();
        for reader_id in 0..NUM_READER_THREADS {
            let db = db.clone();
            let writer_done = writer_done.clone();
            let violation_detected = violation_detected.clone();
            let current_batch = current_batch.clone();

            let handle = thread::spawn(move || {
                let conn = db.connect();
                let mut iteration = 0u64;

                loop {
                    iteration += 1;

                    // Start a new transaction to get a fresh snapshot
                    // Snapshot isolation: This snapshot should see a consistent state
                    conn.execute("BEGIN CONCURRENT").unwrap();

                    // Read all rows grouped by batch_id
                    let rows = get_rows(
                        &conn,
                        "SELECT batch_id, row_num FROM consistency_test ORDER BY batch_id, row_num",
                    );

                    // Group rows by batch_id
                    let mut batches: HashMap<i64, Vec<i64>> = HashMap::new();
                    for row in rows {
                        let batch_id = row[0].as_int().unwrap();
                        let row_num = row[1].as_int().unwrap();
                        batches.entry(batch_id).or_default().push(row_num);
                    }

                    // Check consistency: Each batch must have EITHER all rows OR no rows
                    for (batch_id, row_nums) in &batches {
                        let count = row_nums.len() as i64;

                        // CRITICAL ASSERTION: Snapshot isolation guarantees atomic visibility
                        // A batch is either fully committed (all 50 rows) or not yet committed (0 rows)
                        //
                        // If we see a partial batch (e.g., 23 rows), it means:
                        // 1. The commit loop updated timestamps for rows 0-22 (visible)
                        // 2. Transaction still in Preparing state
                        // 3. Rows 23-49 still have TxID (invisible to us)
                        // 4. We started our snapshot DURING the commit loop
                        //
                        // This is a SNAPSHOT ISOLATION VIOLATION.
                        if count != 0 && count != ROWS_PER_BATCH {
                            eprintln!(
                                "[Reader {reader_id}] VIOLATION DETECTED at iteration {iteration}!",
                            );
                            eprintln!(
                                "  Batch {batch_id} has {count} rows (expected {ROWS_PER_BATCH} or 0)",
                            );
                            eprintln!("  Visible row_nums: {row_nums:?}");
                            eprintln!();
                            eprintln!("  EXPLANATION:");
                            eprintln!(
                                "  - This reader started a snapshot during batch {batch_id}'s commit",
                            );
                            eprintln!(
                                "  - The commit loop (mod.rs:912-984) was updating timestamps"
                            );
                            eprintln!("  - Transaction state was still Preparing(ts)");
                            eprintln!("  - Rows with updated Timestamps became visible");
                            eprintln!("  - Rows with TxID timestamps remained invisible");
                            eprintln!("  - Result: Partial batch visibility (atomicity violation)");
                            eprintln!();
                            eprintln!("  RACE TIMELINE:");
                            eprintln!("  1. Writer: state = Preparing(end_ts)");
                            eprintln!("  2. Writer: Update row 0's timestamp");
                            eprintln!("  3. Writer: Update row 1's timestamp");
                            eprintln!("  ...");
                            eprintln!("  N. Reader: BEGIN (snapshot)");
                            eprintln!(
                                "  N+1. Reader: Read rows 0-{} (visible via Timestamp)",
                                count - 1
                            );
                            eprintln!(
                                "  N+2. Reader: Read rows {}-{} (invisible, still TxID)",
                                count,
                                ROWS_PER_BATCH - 1
                            );
                            eprintln!("  N+3. Writer: Continue updating remaining timestamps...");

                            violation_detected.store(true, Ordering::Release);

                            // Continue to accumulate more evidence
                        }
                    }

                    conn.execute("COMMIT").unwrap();

                    // Exit if writer is done and we've checked a few more times
                    if writer_done.load(Ordering::Acquire) {
                        let final_batch = current_batch.load(Ordering::Acquire);
                        if iteration > final_batch + 10 {
                            break;
                        }
                    }

                    // Small delay to vary timing
                    thread::sleep(Duration::from_micros(50));
                }

                eprintln!("[Reader {reader_id}] Completed {iteration} iterations");
            });

            reader_handles.push(handle);
        }

        // Wait for writer to complete
        writer_handle.join().unwrap();

        // Wait for readers to complete
        for handle in reader_handles {
            handle.join().unwrap();
        }

        // ASSERTION: No violations should be detected
        // With the current bug, this will FAIL because readers observe partial commits
        assert!(
            !violation_detected.load(Ordering::Acquire),
            "Partial commit visibility detected! Transaction atomicity violated.\n\
         \n\
         ROOT CAUSE: Commit loop (mod.rs:912-984) updates row timestamps non-atomically\n\
         while transaction state remains Preparing. Concurrent readers see inconsistent\n\
         snapshots with partial transaction visibility.\n\
         \n\
         FIX REQUIRED: Make timestamp updates atomic, or change visibility logic to\n\
         always dereference transaction state instead of reading row timestamps directly."
        );
    }
}

/// Two concurrent transactions delete the same B-tree-resident row that has a
/// UNIQUE index. Both DELETEs succeed at execute time because tombstones
/// (begin: None) are invisible to is_visible_to(), so both transactions
/// create independent tombstones. However, commit-time validation in
/// check_version_conflicts detects the other transaction's tombstone as a
/// write lock (via its end: TxID field) and rejects the second committer
/// with WriteWriteConflict.
#[test]
fn test_double_delete_btree_resident_row_with_unique_index() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER, uniq TEXT UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES(1, 10, 'a')").unwrap();
    conn.execute("INSERT INTO t VALUES(2, 20, 'b')").unwrap();

    // Checkpoint so rows are only in B-tree, not in MVCC store
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(conn);

    // Two transactions both try to delete the same B-tree-resident row
    let conn1 = db.connect();
    let conn2 = db.connect();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // T1 deletes row 1 — creates tombstone (begin: None, end: TxID(T1))
    conn1.execute("DELETE FROM t WHERE id = 1").unwrap();

    // T2 deletes the same row — creates a second tombstone at execute time
    // (is_visible_to still returns false for tombstones, so operation-time
    // conflict detection is bypassed — that's a separate issue)
    conn2.execute("DELETE FROM t WHERE id = 1").unwrap();

    // T1 commits first — stamps its tombstones with Timestamp
    conn1.execute("COMMIT").unwrap();

    // T2's commit should fail: check_version_conflicts now detects T1's
    // committed tombstone (end: Timestamp >= T2.begin_ts)
    assert!(
        conn2.execute("COMMIT").is_err(),
        "T2's COMMIT should fail with WriteWriteConflict when T1 already \
         committed a tombstone for the same row"
    );
    drop(conn1);
    drop(conn2);

    // Checkpoint: only T1's delete should have gone through
    let conn = db.connect();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        &rows[0][0].to_string(),
        "ok",
        "Index corruption after concurrent double-delete of B-tree-resident row"
    );
}

/// AUTOINCREMENT is not supported in MVCC mode due to sqlite_sequence
/// AUTOINCREMENT is supported in MVCC mode via atomic sequences.
/// Verify that CREATE TABLE with AUTOINCREMENT and INSERT work.
#[test]
fn test_autoincrement_works_in_mvcc() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    // CREATE TABLE with AUTOINCREMENT should succeed in MVCC mode
    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT, b TEXT)")
        .unwrap();

    // INSERT should succeed and auto-generate rowids
    conn.execute("INSERT INTO t(b) VALUES ('hello')").unwrap();
    conn.execute("INSERT INTO t(b) VALUES ('world')").unwrap();

    let rows = get_rows(&conn, "SELECT a, b FROM t ORDER BY a");
    assert_eq!(rows.len(), 2);
    let id1 = rows[0][0].as_int().unwrap();
    let id2 = rows[1][0].as_int().unwrap();
    assert!(
        id1 < id2,
        "rowids must be strictly increasing: {id1}, {id2}"
    );
}

/// If a table with AUTOINCREMENT was created before MVCC was enabled,
/// INSERT into that table should work in MVCC mode using sequences.
#[test]
fn test_autoincrement_insert_works_for_preexisting_table() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir
        .path()
        .join(format!("test_{}", rand::random::<u64>()));
    let path_str = path.to_str().unwrap();
    let io = Arc::new(PlatformIO::new().unwrap());

    // Phase 1: Open in WAL mode, create AUTOINCREMENT table
    {
        let db = crate::Database::open_file_with_flags(
            io.clone(),
            path_str,
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT, b TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t(b) VALUES ('before_mvcc')")
            .unwrap();
        conn.close().unwrap();
    }

    // Clear the database manager to force a fresh open
    {
        let mut manager = crate::DATABASE_MANAGER.lock();
        manager.clear();
    }

    // Phase 2: Reopen in MVCC mode — INSERT should work
    {
        let db = crate::Database::open_file_with_flags(
            io,
            path_str,
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
            .unwrap();

        // Should succeed
        conn.execute("INSERT INTO t(b) VALUES ('in_mvcc')").unwrap();

        let rows = get_rows(&conn, "SELECT a, b FROM t ORDER BY a");
        assert_eq!(rows.len(), 2);
        // The new rowid must be > the previous max (1)
        let new_id = rows[1][0].as_int().unwrap();
        assert!(new_id > 1, "new rowid {new_id} should be > 1");
    }
}

/// Concurrent MVCC transactions inserting into an AUTOINCREMENT table write
/// distinct rowids, so they must not conflict with each other.
#[test]
fn test_concurrent_autoincrement_inserts() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn1 = db.connect();

    conn1
        .execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT, b TEXT)")
        .unwrap();

    // Tx1: begin and insert
    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn1
        .execute("INSERT INTO t(b) VALUES ('from_tx1')")
        .unwrap();

    // Tx2: begin and insert (while tx1 is still open)
    let conn2 = db.connect();
    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2
        .execute("INSERT INTO t(b) VALUES ('from_tx2')")
        .unwrap();

    // Both commits must succeed
    conn1.execute("COMMIT").unwrap();
    conn2.execute("COMMIT").unwrap();

    // Verify both rows are present with distinct, increasing rowids
    let rows = get_rows(&conn1, "SELECT a, b FROM t ORDER BY a");
    assert_eq!(rows.len(), 2, "both inserts should be visible");
    let rowid1 = rows[0][0].as_int().unwrap();
    let rowid2 = rows[1][0].as_int().unwrap();
    assert!(rowid1 < rowid2, "rowids must be strictly increasing");
    assert_eq!(rows[0][1].to_string(), "from_tx1");
    assert_eq!(rows[1][1].to_string(), "from_tx2");
}

/// After concurrent autoincrement inserts and a checkpoint, sqlite_sequence
/// must reflect the true maximum rowid.
#[test]
fn test_autoincrement_sqlite_sequence_after_checkpoint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn1 = db.connect();

    conn1
        .execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT, b TEXT)")
        .unwrap();

    // Insert several rows from separate transactions
    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn1.execute("INSERT INTO t(b) VALUES ('row1')").unwrap();
    conn1.execute("COMMIT").unwrap();

    let conn2 = db.connect();
    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("INSERT INTO t(b) VALUES ('row2')").unwrap();
    conn2.execute("COMMIT").unwrap();

    // Force a checkpoint to flush autoincrement entries
    conn1.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // After checkpoint, sqlite_sequence should have the correct max (2)
    let rows = get_rows(&conn1, "SELECT seq FROM sqlite_sequence WHERE name = 't'");
    assert_eq!(rows.len(), 1, "sqlite_sequence should have entry for 't'");
    let seq = rows[0][0].as_int().unwrap();
    assert_eq!(seq, 2, "sqlite_sequence should reflect the max rowid");

    // A subsequent insert should get rowid 3
    conn1.execute("INSERT INTO t(b) VALUES ('row3')").unwrap();
    let rows = get_rows(&conn1, "SELECT MAX(a) FROM t");
    assert_eq!(rows[0][0].as_int().unwrap(), 3);
}

/// Three concurrent transactions all inserting into the same AUTOINCREMENT table
/// must all succeed and produce unique, increasing rowids.
#[test]
fn test_three_concurrent_autoincrement_inserts() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT, b TEXT)")
        .unwrap();

    let conn1 = db.connect();
    let conn2 = db.connect();
    let conn3 = db.connect();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn1.execute("INSERT INTO t(b) VALUES ('tx1')").unwrap();

    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("INSERT INTO t(b) VALUES ('tx2')").unwrap();

    conn3.execute("BEGIN CONCURRENT").unwrap();
    conn3.execute("INSERT INTO t(b) VALUES ('tx3')").unwrap();

    conn1.execute("COMMIT").unwrap();
    conn2.execute("COMMIT").unwrap();
    conn3.execute("COMMIT").unwrap();

    let rows = get_rows(&conn, "SELECT a FROM t ORDER BY a");
    assert_eq!(rows.len(), 3, "all three inserts should be visible");
    let ids: Vec<i64> = rows.iter().map(|r| r[0].as_int().unwrap()).collect();
    assert!(
        ids[0] < ids[1] && ids[1] < ids[2],
        "rowids must be strictly increasing: {ids:?}"
    );
}

/// Regression: an error between `SequenceBeginInnerTx` and the matching
/// `SequenceCommitInnerTx` (here: sequence exhaustion raised by
/// `SequenceComputeNext`) must not leak the inner tx into
/// `mv_store.txs` or leave the connection's mv_tx pointing at the
/// dead inner. Otherwise the next statement on the same connection
/// inherits the orphaned inner as its outer, and any commit through
/// `WaitForDependencies` may wait forever on the dead tx's deps.
///
/// Whopper reproduces this as a `parse_schema_rows → SELECT →
/// commit_txn → CommitStateMachine::WaitForDependencies` deadlock
/// after a sequence exhaustion error in some prior in-tx nextval.
/// `Statement::cleanup_orphaned_seq_inner_tx` plus the new
/// `ProgramState::sequence_inner_tx_pending` field together close
/// the leak.
#[test]
fn test_inner_tx_cleanup_after_sequence_exhaustion() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    // MAXVALUE = 2: nextval can return 1 and 2, the third call hits
    // SequenceComputeNext's DatabaseFull bail.
    conn.execute("CREATE SEQUENCE tiny START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 2")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    let _ = get_rows(&conn, "SELECT nextval('tiny')");
    let _ = get_rows(&conn, "SELECT nextval('tiny')");
    // Third nextval fails mid-bytecode AFTER SequenceBeginInnerTx
    // swapped conn.mv_tx to the inner, BEFORE SequenceCommitInnerTx
    // could clean it up. Statement reset must roll back the inner
    // and restore the outer mv_tx.
    let exhaust = conn.execute("SELECT nextval('tiny')");
    assert!(exhaust.is_err(), "third nextval must exhaust the sequence");

    // After the abort, the connection must still be usable. Before
    // the fix, this SELECT would inherit the orphaned inner as its
    // outer mv_tx and hang in WaitForDependencies (or worse).
    // Best-effort ROLLBACK — the failing nextval may have already
    // cleared the outer; either way the connection should accept
    // subsequent autocommit statements without hanging.
    let _ = conn.execute("ROLLBACK");
    let rows = get_rows(&conn, "SELECT 1");
    assert_eq!(rows.len(), 1);
}

/// Regression: sequence exhaustion inside an explicit outer transaction must
/// leave the connection's `auto_commit` and `mv_tx` coherent.
///
/// Reproduces a whopper-discovered "row disappeared" failure: an
/// `INSERT … DEFAULT VALUES` with a SERIAL-defaulted column whose sequence is
/// exhausted hits `DatabaseFull` inside `SequenceComputeNext`, AFTER
/// `SequenceBeginInnerTx` has swapped `conn.mv_tx` from the outer (Concurrent
/// tx) to the inner. The vdbe abort path's catch-all for unmatched errors
/// (`vdbe/mod.rs`) calls `rollback_current_txn_state`, which rolls back the
/// inner (currently held in `mv_tx`) and sets `auto_commit = true`.
/// `cleanup_orphaned_seq_inner_tx` then restores `mv_tx` to the outer — but
/// without also restoring `auto_commit = false`, the connection ends up in
/// an inconsistent state where `auto_commit = true` yet `mv_tx = Some(outer)`.
/// Subsequent BEGINs on the connection no-op (the engine sees `mv_tx`
/// already set), so the next "fresh" transaction reuses the outer's stale
/// snapshot and cannot observe rows committed by other connections after
/// the outer's begin timestamp.
#[test]
fn test_auto_commit_coherent_after_sequence_exhaustion_in_outer_tx() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn1 = db.connect();
    let conn2 = db.connect();

    // Sequence exhausted after just two emissions, plus a SERIAL-style
    // table whose default invokes nextval.
    conn1
        .execute("CREATE SEQUENCE tiny START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 2")
        .unwrap();
    conn1
        .execute("CREATE TABLE seq_tbl (id INTEGER DEFAULT (nextval('tiny')), payload TEXT)")
        .unwrap();
    conn1
        .execute("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)")
        .unwrap();

    // Open the outer tx on conn1 and consume the sequence to exhaustion.
    conn1.execute("BEGIN CONCURRENT").unwrap();
    let _ = get_rows(&conn1, "SELECT nextval('tiny')");
    let _ = get_rows(&conn1, "SELECT nextval('tiny')");

    // Sanity: the outer is alive before the failing op.
    assert!(!conn1.get_auto_commit(), "outer tx must be active");
    let outer_mv_tx = conn1
        .get_mv_tx_id()
        .expect("outer tx must have an mv_tx_id");

    // The INSERT … DEFAULT VALUES path invokes nextval on the exhausted
    // sequence and bails with DatabaseFull mid-bytecode, AFTER
    // SequenceBeginInnerTx swapped mv_tx to the inner.
    let exhaust = conn1.execute("INSERT INTO seq_tbl DEFAULT VALUES");
    assert!(
        exhaust.is_err(),
        "INSERT with exhausted sequence default must fail"
    );

    // INVARIANT: auto_commit and mv_tx must agree. Either the connection
    // is back to autocommit (mv_tx = None) or it's still in the outer
    // tx (mv_tx = Some(outer)). Half-states break subsequent BEGINs.
    let auto_commit = conn1.get_auto_commit();
    let mv_tx = conn1.get_mv_tx_id();
    assert_eq!(
        auto_commit,
        mv_tx.is_none(),
        "post-exhaustion state must be coherent: auto_commit={auto_commit}, mv_tx={mv_tx:?}"
    );

    // The outer tx must still be the one mv_tx points to — sequence
    // exhaustion is a per-statement error, not a tx-level abort. The
    // outer survives so the application can decide whether to COMMIT
    // partial work or ROLLBACK.
    assert_eq!(
        mv_tx,
        Some(outer_mv_tx),
        "outer tx should survive sequence exhaustion"
    );

    // Now have a different connection commit a row in autocommit.
    conn2.execute("INSERT INTO kv VALUES ('k1', 'v1')").unwrap();

    // Roll back the outer on conn1 and start a fresh tx. The new tx's
    // snapshot must see conn2's commit. If the bug were present,
    // ROLLBACK would not actually end the outer (because the outer
    // didn't really survive in a usable form), or — more directly —
    // a subsequent BEGIN+SELECT would observe an empty `kv` table
    // because the connection was pinned to the outer's stale snapshot.
    conn1.execute("ROLLBACK").unwrap();
    assert!(conn1.get_auto_commit(), "ROLLBACK must end the outer tx");
    assert_eq!(conn1.get_mv_tx_id(), None, "ROLLBACK must clear mv_tx");

    conn1.execute("BEGIN").unwrap();
    let rows = get_rows(&conn1, "SELECT v FROM kv WHERE k = 'k1'");
    assert_eq!(
        rows.len(),
        1,
        "fresh BEGIN+SELECT must see conn2's committed row \
         (if 0 rows: the connection was pinned to a stale snapshot \
         because the BEGIN no-op'd into the orphaned outer)"
    );
    conn1.execute("COMMIT").unwrap();
}

/// Deterministic reproduction of the sqlite_sequence pollution bug.
///
/// Two concurrent transactions insert into an AUTOINCREMENT table.
/// Tx1 gets data rowid 1, tx2 gets data rowid 2. Tx1 commits first,
/// so its sqlite_sequence row (name='t', seq=1) gets sqlite_sequence
/// rowid 1. Tx2 commits second, so its sqlite_sequence row (name='t',
/// seq=2) gets sqlite_sequence rowid 2.
///
/// After DELETE + checkpoint + restart, the table is empty (btree max = 0).
/// init_autoincrement scans sqlite_sequence by rowid order, finds the FIRST
/// match at sqlite_sequence rowid 1 with seq=1, and uses that.
/// New rowid = max(1, 0) + 1 = 2, which REUSES the previously-used rowid 2.
///
/// This violates AUTOINCREMENT's contract that rowids must never decrease.
#[test]
fn test_autoincrement_no_reuse_after_delete_and_restart() {
    let _ = tracing_subscriber::fmt().try_init();
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn1 = db.connect();

    conn1
        .execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT, b TEXT)")
        .unwrap();

    // Two concurrent transactions: tx1 commits first, tx2 commits second.
    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn1
        .execute("INSERT INTO t(b) VALUES ('from_tx1')")
        .unwrap();

    let conn2 = db.connect();
    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2
        .execute("INSERT INTO t(b) VALUES ('from_tx2')")
        .unwrap();

    // Commit tx1 first: its sqlite_sequence row gets the lower rowid
    conn1.execute("COMMIT").unwrap();
    conn2.execute("COMMIT").unwrap();

    // Verify: data rowids are 1 and 2
    let rows = get_rows(&conn1, "SELECT a FROM t ORDER BY a");
    assert_eq!(rows.len(), 2);
    let max_data_rowid = rows[1][0].as_int().unwrap();
    assert_eq!(max_data_rowid, 2);

    // sqlite_sequence should have duplicate rows (the bug):
    // rowid=1: name=t, seq=1  (from tx1, committed first)
    // rowid=2: name=t, seq=2  (from tx2, committed second)
    let seq_rows = get_rows(
        &conn1,
        "SELECT rowid, seq FROM sqlite_sequence WHERE name = 't' ORDER BY rowid",
    );
    // If there's only 1 row with the correct max, the fix is applied.
    // If there are 2 rows, the bug is present and init_autoincrement will
    // pick the wrong one after restart.
    let seq_count = seq_rows.len();

    // Delete all data rows so btree max becomes 0 after restart
    conn1.execute("DELETE FROM t").unwrap();

    // Checkpoint to flush everything to disk
    conn1.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Drop connections and restart
    drop(conn1);
    drop(conn2);
    db.restart();

    let conn = db.connect();

    // Verify table is empty
    let rows = get_rows(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(rows[0][0].as_int().unwrap(), 0);

    // Insert after restart. The new rowid MUST be > max_data_rowid (2).
    conn.execute("INSERT INTO t(b) VALUES ('after_restart')")
        .unwrap();
    let rows = get_rows(&conn, "SELECT a FROM t");
    let new_rowid = rows[0][0].as_int().unwrap();

    if seq_count > 1 {
        // Bug present: sqlite_sequence has duplicate rows.
        // init_autoincrement picked the first match (seq=1), so new rowid = 2,
        // which reuses a previously-issued rowid.
        eprintln!(
            "sqlite_sequence had {seq_count} rows for 't'. \
             After restart, new rowid = {new_rowid} (previous max was {max_data_rowid})"
        );
    }

    assert!(
        new_rowid > max_data_rowid,
        "AUTOINCREMENT rowid reuse! Previous max was {max_data_rowid}, \
         but new rowid after delete+restart is {new_rowid}. \
         sqlite_sequence had {seq_count} duplicate rows; \
         init_autoincrement picked the stale one (seq=1 instead of seq=2)."
    );
}

/// Same bug as `test_elle_lost_update_exclusive_concurrent` but with simplified SQLs.
/// For this bug to happen, we need deferred conflict detection done at
/// `check_version_conflicts`.
/// Requires UPSERT (INSERT ... ON CONFLICT DO UPDATE) — to hit `insert_btree_resident`
/// and then `check_version_conflicts`.
/// (Note: plain UPDATE eagerly detects conflicts via `delete_from_table_or_index`)
///
/// We need three txns for this bug to happen:
/// Initial state: some row btree resident
/// tx2: starts
/// tx1: upserts the row, commits
/// tx3: upserts the same row, which sets end=TxID(T3) on tx1's version (speculative delete)
/// tx2: upserts the same row, should get WriteWriteConflict at commit time because tx1 committed previously
#[test]
fn test_speculative_delete_hides_committed_version_sql() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t (key TEXT PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1")
            .unwrap();
        conn.execute("INSERT INTO t VALUES ('k1', 'a')").unwrap();
        conn.close().unwrap();
    }
    // lets do this so that row becomes b tree resident
    db.restart();
    {
        let conn = db.connect();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
    }

    let upsert = |val: &str| {
        format!(
            "INSERT INTO t VALUES ('k1', '{val}') \
             ON CONFLICT(key) DO UPDATE SET val = excluded.val"
        )
    };

    // T2: begin early so T1's future commit is invisible under SI.
    let conn2 = db.connect();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // T1: auto-commit UPSERT → insert_btree_resident, commits.
    let conn1 = db.connect();
    conn1.execute(upsert("b")).unwrap();
    conn1.close().unwrap();

    // T3: UPSERT → sets end=TxID(T3) on T1's version.
    let conn3 = db.connect();
    conn3.execute("BEGIN CONCURRENT").unwrap();
    conn3.execute(upsert("d")).unwrap();

    // T2: UPSERT → insert_btree_resident (T1 invisible, T3 invisible).
    conn2.execute(upsert("c")).unwrap();

    // T2: COMMIT → must detect conflict with T1.
    let result = conn2.execute("COMMIT");
    assert!(
        matches!(&result, Err(LimboError::WriteWriteConflict)),
        "Expected WriteWriteConflict, got: {result:?}."
    );
}

/// Regression test for Elle bug (https://github.com/tursodatabase/turso/actions/runs/22855976911/job/66296309873?pr=5819#logs)
/// Previously, `check_version_conflicts` skipped any version with `end.is_some()`,
/// so a speculative delete by T3 (setting end=TxID(T3)) hid T1's committed version
/// from T2's conflict check, allowing a lost update.
///
/// The SQL statements resemble the ones in Elle. For simplified variant, check
/// `test_speculative_delete_hides_committed_version_sql` test
///
/// 1. Row for key "k8" exists in B-tree (B-tree-resident, no MVCC version)
/// 2. T2 starts via BEGIN CONCURRENT
/// 3. T1 does auto-commit UPSERT on "k8" → insert_btree_resident, commits
/// 4. T3 starts via BEGIN CONCURRENT, does UPSERT on "k8" → update path sets
///    end=TxID(T3) on T1's committed version
/// 5. T2 does UPSERT on "k8" → insert_btree_resident (T1's version invisible, T3's invisible)
/// 6. T2 COMMIT → check_version_conflicts SKIPS T1's version because end.is_some()
///    → no WriteWriteConflict detected → lost update!
/// 7. T3 eventually aborts, restoring T1's version, but T2 already committed.
#[test]
fn test_elle_lost_update_exclusive_concurrent() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    // Setup: create the elle-style table and seed initial data into the B-tree
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE elle_lists (key TEXT PRIMARY KEY, vals TEXT DEFAULT '')")
            .unwrap();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1")
            .unwrap();
        conn.execute("INSERT INTO elle_lists (key, vals) VALUES ('k8', '100')")
            .unwrap();
        conn.close().unwrap();
    }
    // Restart: data is only in B-tree, MVCC store is empty.
    db.restart();
    {
        let conn = db.connect();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
    }

    // T2: start concurrent transaction (begin_ts established early)
    let conn2 = db.connect();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // T1: auto-commit UPSERT on k8 → insert_btree_resident, commits immediately
    let conn1 = db.connect();
    conn1
        .execute(
            "INSERT INTO elle_lists (key, vals) VALUES ('k8', '200') \
             ON CONFLICT(key) DO UPDATE SET vals = CASE WHEN vals = '' THEN '200' ELSE vals || ',' || '200' END",
        )
        .unwrap();
    conn1.close().unwrap();

    // T3: start concurrent transaction (begin_ts > T1.end_ts, so T1's version IS visible to T3)
    let conn3 = db.connect();
    conn3.execute("BEGIN CONCURRENT").unwrap();

    // T3: UPSERT on k8 → update path: deletes T1's version (sets end=TxID(T3))
    // and creates T3's own version. This speculatively hides T1's version.
    conn3
        .execute(
            "INSERT INTO elle_lists (key, vals) VALUES ('k8', '400') \
             ON CONFLICT(key) DO UPDATE SET vals = CASE WHEN vals = '' THEN '400' ELSE vals || ',' || '400' END",
        )
        .unwrap();

    // T2: UPSERT on k8 → insert_btree_resident (T1's version invisible under SI,
    // T3's version invisible as Active)
    conn2
        .execute(
            "INSERT INTO elle_lists (key, vals) VALUES ('k8', '300') \
             ON CONFLICT(key) DO UPDATE SET vals = CASE WHEN vals = '' THEN '300' ELSE vals || ',' || '300' END",
        )
        .unwrap();

    // T2: COMMIT → should detect write-write conflict with T1's committed version.
    // BUG: T1's version has end=TxID(T3), and the old code skips versions with
    // end.is_some() → conflict missed → lost update.
    let commit_result = conn2.execute("COMMIT");
    assert!(
        matches!(&commit_result, Err(LimboError::WriteWriteConflict)),
        "Expected WriteWriteConflict, got: {commit_result:?}. \
         T1's committed version was hidden by T3's speculative delete (end=TxID), \
         causing check_version_conflicts to skip it."
    );
}

/// Regression test: speculative delete by an active transaction must not hide a
/// committed version from commit-time conflict checks.
///
/// Previously, `check_version_conflicts` skipped any version with `end.is_some()`,
/// including versions where `end` was `TxID` of an active (uncommitted) transaction.
/// This allowed T2 to commit without detecting the write-write conflict with T1.
///
/// Minimal reproduction using the MvStore API directly (no SQL, no restart, no UPSERT).
/// Note: T2 begins after T1 commits, so T2 *can* see T1 under SI. We call
/// `insert_btree_resident` directly to simulate the UPSERT code path where the
/// cursor doesn't go through normal read visibility.
///
/// Timeline:
///   T1: insert row 1, commit
///   T2: begin (will write later)
///   T3: begin, update row 1 → sets end=TxID(T3) on T1's version
///   T2: insert_btree_resident row 1 (via API, bypassing read visibility)
///   T2: commit → must detect conflict with T1
#[test]
fn test_speculative_delete_hides_committed_version() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    // T1: insert row 1 and commit.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row_v1 = generate_simple_string_row(table_id, 1, "v1");
    db.mvcc_store.insert(tx1, row_v1).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2: begin (will write later).
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();

    // T3: begin, update row 1 → delete sets end=TxID(T3) on T1's version.
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.load().clone()).unwrap();
    let row_v3 = generate_simple_string_row(table_id, 1, "v3");
    assert!(db.mvcc_store.update(tx3, row_v3).unwrap());

    // T2: insert_btree_resident for the same row (called directly via API to
    // simulate the UPSERT code path that bypasses eager conflict detection).
    let row_v2 = generate_simple_string_row(table_id, 1, "v2");
    db.mvcc_store
        .insert_btree_resident_to_table_or_index(tx2, row_v2, None)
        .unwrap();

    // T2: commit → must fail with WriteWriteConflict.
    let result = commit_tx(db.mvcc_store, &conn2, tx2);
    assert!(
        matches!(&result, Err(LimboError::WriteWriteConflict)),
        "Expected WriteWriteConflict, got: {result:?}. \
         T3's speculative delete (end=TxID) on T1's version must not hide it from conflict checks."
    );
}

/// Verify that a committed pure delete (tombstone) is detected as a conflict.
///
/// Scenario: Td deletes a row and commits. Between Td's Commit and CommitEnd
/// (when TxID→Timestamp conversion happens), the tombstone still has
/// end=TxID(Td). T2 does insert_btree_resident for the same row and tries to
/// commit. The tombstone's begin=None, end=TxID(Td) should be caught by the
/// B-tree tombstone check in check_version_conflicts.
///
/// Timeline:
///   T1: insert row 1, commit
///   T2: begin (will write later)
///   Td: delete row 1, commit
///   T2: insert_btree_resident row 1
///   T2: commit → must detect conflict with Td's tombstone
#[test]
fn test_committed_delete_tombstone_conflict() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    // T1: insert row 1 and commit.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row_v1 = generate_simple_string_row(table_id, 1, "v1");
    db.mvcc_store.insert(tx1, row_v1).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2: begin (will write later).
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();

    // Td: delete row 1 and commit.
    let conn_d = db.db.connect().unwrap();
    let tx_d = db.mvcc_store.begin_tx(conn_d.pager.load().clone()).unwrap();
    assert!(db
        .mvcc_store
        .delete(tx_d, RowID::new(table_id, RowKey::Int(1)))
        .unwrap());
    commit_tx(db.mvcc_store.clone(), &conn_d, tx_d).unwrap();

    // T2: insert_btree_resident for the same row.
    let row_v2 = generate_simple_string_row(table_id, 1, "v2");
    db.mvcc_store
        .insert_btree_resident_to_table_or_index(tx2, row_v2, None)
        .unwrap();

    // T2: commit → must detect conflict with Td.
    let result = commit_tx(db.mvcc_store, &conn2, tx2);
    assert!(
        matches!(&result, Err(LimboError::WriteWriteConflict)),
        "Expected WriteWriteConflict, got: {result:?}. \
         Td's committed delete (tombstone) must be detected as a conflict."
    );
}

/// Verify that when a transaction (Td) updates a row and commits, another
/// transaction (T2) that also writes to the same row detects the conflict —
/// even though T1's version has end=TxID(Td) with Td committed.
///
/// This tests the `Committed(_) => continue` branch in `check_version_conflicts`:
/// skipping T1's version is safe because Td's NEW version (begin=TxID(Td)) catches
/// the conflict.
///
/// Timeline:
///   T1: insert row 1, commit
///   T2: begin (will write later)
///   Td: update row 1 (sets end=TxID(Td) on T1's version, creates new version), commit
///   T2: insert_btree_resident row 1
///   T2: commit → must detect conflict with Td's new version
#[test]
fn test_committed_update_version_conflict() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    // T1: insert row 1 and commit.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row_v1 = generate_simple_string_row(table_id, 1, "v1");
    db.mvcc_store.insert(tx1, row_v1).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2: begin (will write later).
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();

    // Td: update row 1 and commit.
    let conn_d = db.db.connect().unwrap();
    let tx_d = db.mvcc_store.begin_tx(conn_d.pager.load().clone()).unwrap();
    let row_vd = generate_simple_string_row(table_id, 1, "vd");
    assert!(db.mvcc_store.update(tx_d, row_vd).unwrap());
    commit_tx(db.mvcc_store.clone(), &conn_d, tx_d).unwrap();

    // T2: insert_btree_resident for the same row.
    let row_v2 = generate_simple_string_row(table_id, 1, "v2");
    db.mvcc_store
        .insert_btree_resident_to_table_or_index(tx2, row_v2, None)
        .unwrap();

    // T2: commit → must detect conflict with Td.
    let result = commit_tx(db.mvcc_store, &conn2, tx2);
    assert!(
        matches!(&result, Err(LimboError::WriteWriteConflict)),
        "Expected WriteWriteConflict, got: {result:?}. \
         Td's committed update must be detected via Td's new version."
    );
}

/// Encrypted MVCC: write rows, restart with same key, verify recovery replays them.
/// Then swap to a wrong key and verify that restart fails.
#[test]
fn test_mvcc_encrypted_log_recovery_and_wrong_key() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);
    write_synthetic_row(&db, "encrypted_value");

    // --- Verify the raw log file is encrypted (no plaintext leakage) ---
    {
        let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");
        let log_bytes = std::fs::read(&log_path).expect("MVCC log file should exist");
        assert!(
            log_bytes.len() > 56,
            "MVCC log should contain data beyond the header"
        );
        let plaintext = b"encrypted_value";
        assert!(
            !log_bytes.windows(plaintext.len()).any(|w| w == plaintext),
            "MVCC log must not contain plaintext data when encryption is enabled"
        );
    }

    // --- Restart with correct key: recovery should replay the encrypted log ---
    db.restart();
    {
        let conn = db.connect();
        let mvcc_store = db.get_mvcc_store();
        let max_root_page = get_rows(
            &conn,
            "SELECT COALESCE(MAX(rootpage), 0) FROM sqlite_schema WHERE rootpage > 0",
        )[0][0]
            .as_int()
            .unwrap();
        let synthetic_table_id = MVTableId::new(-(max_root_page + 100));
        let tx_id = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
        let row = mvcc_store
            .read(tx_id, &RowID::new(synthetic_table_id, RowKey::Int(1)))
            .unwrap()
            .unwrap();
        let record = get_record_value(&row);
        match record.get_value(0).unwrap() {
            ValueRef::Text(text) => assert_eq!(text.as_str(), "encrypted_value"),
            other => panic!("Expected Text, got {other:?}"),
        }
        conn.close().unwrap();
    }

    // --- Restart with wrong key: should fail ---
    let wrong_key = "ff0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    db.enc_opts = Some(crate::EncryptionOpts {
        cipher: "aes256gcm".to_string(),
        hexkey: wrong_key.to_string(),
    });
    assert!(
        db.restart_result().is_err(),
        "Expected error when reopening encrypted MVCC DB with wrong key"
    );
}

/// Enabling MVCC on a file-backed database must still bootstrap durable MVCC
/// metadata even if encryption has only been opted-in and no key/cipher exists yet.
#[test]
fn test_mvcc_late_encryption_setup_keeps_metadata_bootstrapped() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("test.db");
    let io = Arc::new(PlatformIO::new().unwrap());
    let opts = DatabaseOpts::new().with_encryption(true);
    let db = Database::open_file_with_flags(
        io,
        path.as_os_str().to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();

    // Reproduce the deferred-key flow: encryption is enabled as a feature, but
    // the session has not configured any key/cipher when MVCC bootstrap runs.
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();

    let metadata_root = metadata_root_page(&conn);
    assert!(
        metadata_root > 0,
        "metadata table must be present after enabling MVCC on a file-backed db",
    );

    let meta = get_rows(
        &conn,
        "SELECT k, v FROM __turso_internal_mvcc_meta ORDER BY rowid",
    );
    assert_eq!(meta.len(), 1);
    assert_eq!(meta[0][0].to_string(), "persistent_tx_ts_max");
    assert_eq!(meta[0][1].as_int().unwrap(), 0);
}

/// Reopening an encrypted MVCC database without any key material must fail before
/// logical-log recovery, even if there is an outstanding MVCC log tail on disk.
#[test]
fn test_mvcc_encrypted_restart_without_key_fails_before_recovery() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);
    let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");

    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'secret')").unwrap();
        conn.close().unwrap();
    }

    let log_bytes = std::fs::read(&log_path).expect("db-log should exist after MVCC writes");
    assert!(
        log_bytes.len() > LOG_HDR_SIZE,
        "db-log should contain at least one frame before restart"
    );

    db.enc_opts = None;
    assert!(
        matches!(db.restart_result(), Err(LimboError::NotADB)),
        "reopening an encrypted MVCC database without a key must fail during db open, before recovery",
    );
}

#[test]
fn test_encrypted_recovery_large_payload_multi_chunk() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let large_value = "x".repeat(ENCRYPTED_PAYLOAD_CHUNK_SIZE * 3);
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);

    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute(format!("INSERT INTO t VALUES (1, '{large_value}')"))
            .unwrap();
    }

    let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");
    assert!(log_path.exists(), "db-log should exist before restart");
    assert_log_payloads_decrypt(
        &log_path,
        hex_key,
        crate::storage::encryption::CipherMode::Aes256Gcm,
    );

    db.restart();
    let conn = db.connect();
    let rows = get_rows(
        &conn,
        "SELECT id, length(v), substr(v, 1, 16), substr(v, length(v) - 15, 16) FROM t",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), large_value.len() as i64);
    assert_eq!(rows[0][2].to_string(), "xxxxxxxxxxxxxxxx");
    assert_eq!(rows[0][3].to_string(), "xxxxxxxxxxxxxxxx");
}

#[test]
fn test_encrypted_recovery_corrupted_later_chunk_keeps_checkpointed_prefix() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let large_value = "z".repeat(ENCRYPTED_PAYLOAD_CHUNK_SIZE * 3);
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);
    let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");

    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'survives')")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute(format!("INSERT INTO t VALUES (2, '{large_value}')"))
            .unwrap();
    }

    let mut log_bytes = std::fs::read(&log_path).expect("db-log should exist");
    let payload_size = u64::from_le_bytes(
        log_bytes[LOG_HDR_SIZE + 4..LOG_HDR_SIZE + 12]
            .try_into()
            .unwrap(),
    ) as usize;
    let chunk_count = payload_size.div_ceil(ENCRYPTED_PAYLOAD_CHUNK_SIZE);
    assert!(
        chunk_count >= 3,
        "expected multi-chunk encrypted recovery tail"
    );

    let enc_ctx = crate::storage::encryption::EncryptionContext::new(
        crate::storage::encryption::CipherMode::Aes256Gcm,
        &EncryptionKey::from_hex_string(hex_key).unwrap(),
        4096,
    )
    .unwrap();
    let first_chunk_on_disk_size =
        ENCRYPTED_PAYLOAD_CHUNK_SIZE + enc_ctx.tag_size() + enc_ctx.nonce_size();
    let corrupt_offset = LOG_HDR_SIZE + TX_BASE_HEADER_SIZE + first_chunk_on_disk_size + 1;
    log_bytes[corrupt_offset] ^= 0xFF;
    std::fs::write(&log_path, &log_bytes).unwrap();

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "survives");
}

/// Read the raw db-log file and verify every TX frame portable_changes can be decrypted.
/// Panics if the file is missing, has no frames, or any portable_changes fails to decrypt.
fn assert_log_payloads_decrypt(
    log_path: &std::path::Path,
    hex_key: &str,
    cipher: crate::storage::encryption::CipherMode,
) {
    use crate::storage::encryption::EncryptionContext;

    let log_bytes = std::fs::read(log_path).expect("db-log file should exist");
    assert!(
        log_bytes.len() > LOG_HDR_SIZE,
        "db-log should contain data beyond the header"
    );

    let key = EncryptionKey::from_hex_string(hex_key).unwrap();
    let enc_ctx = EncryptionContext::new(cipher, &key, 4096).unwrap();
    let nonce_size = enc_ctx.nonce_size();
    let tag_size = enc_ctx.tag_size();

    // Parse salt from log header (bytes 8..16, little-endian u64)
    let salt = u64::from_le_bytes(log_bytes[8..16].try_into().unwrap());

    let mut offset = LOG_HDR_SIZE;
    let mut frame_count = 0;

    while offset + TX_BASE_HEADER_SIZE + TX_TRAILER_SIZE <= log_bytes.len() {
        // TX Header: frame_magic(4) | payload_size(8) | op_count(4) | commit_ts(8)
        // | extension_size(8) | extension_record_count(4) | frame_flags(4)
        let frame_magic = u32::from_le_bytes(log_bytes[offset..offset + 4].try_into().unwrap());
        let (header_size, extension_size) = if frame_magic == FRAME_MAGIC {
            (TX_BASE_HEADER_SIZE, 0)
        } else if frame_magic == EXT_FRAME_MAGIC {
            if offset + TX_EXT_HEADER_SIZE + TX_TRAILER_SIZE > log_bytes.len() {
                break;
            }
            (
                TX_EXT_HEADER_SIZE,
                u64::from_le_bytes(log_bytes[offset + 24..offset + 32].try_into().unwrap())
                    as usize,
            )
        } else {
            break; // not a valid frame
        };
        let payload_size =
            u64::from_le_bytes(log_bytes[offset + 4..offset + 12].try_into().unwrap()) as usize;
        let op_count = u32::from_le_bytes(log_bytes[offset + 12..offset + 16].try_into().unwrap());
        let commit_ts = u64::from_le_bytes(log_bytes[offset + 16..offset + 24].try_into().unwrap());

        let encrypted_plaintext_size = payload_size + extension_size;
        let mut payload_offset = offset + header_size;
        let chunk_count = if encrypted_plaintext_size == 0 {
            0
        } else {
            encrypted_plaintext_size.div_ceil(ENCRYPTED_PAYLOAD_CHUNK_SIZE)
        };

        let mut frame_complete = true;
        for chunk_index in 0..chunk_count {
            let chunk_plaintext_len = (encrypted_plaintext_size
                - chunk_index * ENCRYPTED_PAYLOAD_CHUNK_SIZE)
                .min(ENCRYPTED_PAYLOAD_CHUNK_SIZE);
            let chunk_on_disk_size = chunk_plaintext_len + tag_size + nonce_size;
            if payload_offset + chunk_on_disk_size + TX_TRAILER_SIZE > log_bytes.len() {
                frame_complete = false;
                break;
            }

            let blob = &log_bytes[payload_offset..payload_offset + chunk_on_disk_size];
            let ciphertext = &blob[..chunk_plaintext_len + tag_size];
            let nonce = &blob[chunk_plaintext_len + tag_size..];

            let mut aad = [0u8; 32];
            aad[..8].copy_from_slice(&salt.to_le_bytes());
            if chunk_index + 1 == chunk_count {
                aad[8..16].copy_from_slice(&(encrypted_plaintext_size as u64).to_le_bytes());
            }
            aad[16..20].copy_from_slice(&op_count.to_le_bytes());
            aad[20..28].copy_from_slice(&commit_ts.to_le_bytes());
            aad[28..32].copy_from_slice(&(chunk_index as u32).to_le_bytes());

            enc_ctx
                .decrypt_chunk(ciphertext, nonce, &aad)
                .unwrap_or_else(|e| {
                    panic!(
                        "failed to decrypt frame {frame_count} chunk {chunk_index} at offset {offset}: {e}"
                    )
                });

            payload_offset += chunk_on_disk_size;
        }
        if !frame_complete {
            break;
        }

        frame_count += 1;
        offset = payload_offset + TX_TRAILER_SIZE; // skip encrypted body and trailer
    }

    assert!(
        frame_count > 0,
        "db-log should contain at least one TX frame"
    );
}

#[cfg(feature = "conn_raw_api")]
fn collect_mvcc_portable_change_bytes(conn: &Arc<Connection>) -> Vec<u8> {
    let mv_store = conn
        .mv_store()
        .as_ref()
        .expect("test database must be in MVCC mode")
        .clone();
    let io = conn.get_pager().io.clone();
    let mut reader = StreamingLogicalLogReader::new(mv_store.get_logical_log_file(), None);
    reader.read_header(&io).unwrap();

    let mut portable_changes = Vec::new();
    while let Some(frame) = io.block(|| reader.next_portable_changes()).unwrap() {
        portable_changes.extend_from_slice(&frame.payload);
    }
    portable_changes
}

#[cfg(feature = "conn_raw_api")]
fn collect_mvcc_portable_change_bytes_with_encryption(
    conn: &Arc<Connection>,
    encryption_ctx: crate::storage::encryption::EncryptionContext,
) -> Vec<u8> {
    let mv_store = conn
        .mv_store()
        .as_ref()
        .expect("test database must be in MVCC mode")
        .clone();
    let io = conn.get_pager().io.clone();
    let mut reader =
        StreamingLogicalLogReader::new(mv_store.get_logical_log_file(), Some(encryption_ctx));
    reader.read_header(&io).unwrap();

    let mut portable_changes = Vec::new();
    while let Some(frame) = io.block(|| reader.next_portable_changes()).unwrap() {
        portable_changes.extend_from_slice(&frame.payload);
    }
    portable_changes
}

#[cfg(feature = "conn_raw_api")]
fn collect_mvcc_recovery_ops(conn: &Arc<Connection>) -> Vec<ParsedOp> {
    let mv_store = conn
        .mv_store()
        .as_ref()
        .expect("test database must be in MVCC mode")
        .clone();
    let io = conn.get_pager().io.clone();
    let mut reader = StreamingLogicalLogReader::new(mv_store.get_logical_log_file(), None);
    reader.read_header(&io).unwrap();

    let mut ops = Vec::new();
    while let Some(frame_ops) = reader.next_frame_blocking(&io).unwrap() {
        ops.extend(frame_ops);
    }
    ops
}

#[cfg(feature = "conn_raw_api")]
fn bytes_contain(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

#[cfg(feature = "conn_raw_api")]
fn read_proto_varint(bytes: &[u8], offset: &mut usize) -> u64 {
    let mut value = 0u64;
    let mut shift = 0;
    loop {
        let byte = bytes[*offset];
        *offset += 1;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return value;
        }
        shift += 7;
    }
}

#[cfg(feature = "conn_raw_api")]
fn decode_proto_sint64(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

#[cfg(feature = "conn_raw_api")]
fn skip_proto_field(bytes: &[u8], offset: &mut usize, wire_type: u64) {
    match wire_type {
        0 => {
            let _ = read_proto_varint(bytes, offset);
        }
        2 => {
            let len = read_proto_varint(bytes, offset) as usize;
            *offset += len;
        }
        other => panic!("unsupported protobuf wire type in test decoder: {other}"),
    }
}

#[cfg(feature = "conn_raw_api")]
#[derive(Clone, Debug)]
struct DecodedObjectMap {
    mv_table_id: i64,
    name: String,
}

#[cfg(feature = "conn_raw_api")]
#[derive(Clone, Debug, Default)]
struct DecodedPortableTxn {
    objects: Vec<DecodedObjectMap>,
    metadata: std::collections::HashMap<String, String>,
}

#[cfg(feature = "conn_raw_api")]
fn decode_object_map(bytes: &[u8], strings: &[String]) -> Option<DecodedObjectMap> {
    let mut offset = 0usize;
    let mut mv_table_id = None;
    let mut name = String::new();
    while offset < bytes.len() {
        let key = read_proto_varint(bytes, &mut offset);
        let field = key >> 3;
        let wire_type = key & 7;
        match (field, wire_type) {
            (1, 0) => {
                mv_table_id = Some(decode_proto_sint64(read_proto_varint(bytes, &mut offset)))
            }
            (2, 0) => {
                let idx = read_proto_varint(bytes, &mut offset) as usize;
                name = strings.get(idx).cloned().unwrap_or_default();
            }
            _ => skip_proto_field(bytes, &mut offset, wire_type),
        }
    }
    Some(DecodedObjectMap {
        mv_table_id: mv_table_id?,
        name,
    })
}

#[cfg(feature = "conn_raw_api")]
fn decode_metadata(bytes: &[u8], strings: &[String]) -> Option<(String, String)> {
    let mut offset = 0usize;
    let mut key = None;
    let mut value = None;
    while offset < bytes.len() {
        let field_key = read_proto_varint(bytes, &mut offset);
        let field = field_key >> 3;
        let wire_type = field_key & 7;
        match (field, wire_type) {
            (1, 0) => {
                let idx = read_proto_varint(bytes, &mut offset) as usize;
                key = strings.get(idx).cloned();
            }
            (2, 0) => {
                let idx = read_proto_varint(bytes, &mut offset) as usize;
                value = strings.get(idx).cloned();
            }
            _ => skip_proto_field(bytes, &mut offset, wire_type),
        }
    }
    Some((key?, value?))
}

#[cfg(feature = "conn_raw_api")]
fn decode_portable_change_txns(portable_changes: &[u8]) -> Vec<DecodedPortableTxn> {
    let mut txns = Vec::new();
    let mut offset = 0usize;
    while offset < portable_changes.len() {
        let txn_len = read_proto_varint(portable_changes, &mut offset) as usize;
        let txn_end = offset + txn_len;
        let txn = &portable_changes[offset..txn_end];
        offset = txn_end;

        let mut strings = Vec::new();
        let mut object_blobs = Vec::new();
        let mut meta_blobs = Vec::new();
        let mut txn_offset = 0usize;
        while txn_offset < txn.len() {
            let key = read_proto_varint(txn, &mut txn_offset);
            let field = key >> 3;
            let wire_type = key & 7;
            match (field, wire_type) {
                (12, 2) => {
                    let len = read_proto_varint(txn, &mut txn_offset) as usize;
                    strings.push(
                        String::from_utf8(txn[txn_offset..txn_offset + len].to_vec()).unwrap(),
                    );
                    txn_offset += len;
                }
                (13, 2) => {
                    let len = read_proto_varint(txn, &mut txn_offset) as usize;
                    object_blobs.push(txn[txn_offset..txn_offset + len].to_vec());
                    txn_offset += len;
                }
                (14, 2) => {
                    let len = read_proto_varint(txn, &mut txn_offset) as usize;
                    meta_blobs.push(txn[txn_offset..txn_offset + len].to_vec());
                    txn_offset += len;
                }
                _ => skip_proto_field(txn, &mut txn_offset, wire_type),
            }
        }
        let objects = object_blobs
            .iter()
            .filter_map(|object| decode_object_map(object, &strings))
            .collect();
        let mut metadata = std::collections::HashMap::new();
        for meta in &meta_blobs {
            if let Some((key, value)) = decode_metadata(meta, &strings) {
                metadata.insert(key, value);
            }
        }
        txns.push(DecodedPortableTxn { objects, metadata });
    }
    txns
}

#[cfg(feature = "conn_raw_api")]
fn decoded_object_maps(txns: &[DecodedPortableTxn]) -> Vec<&DecodedObjectMap> {
    txns.iter().flat_map(|txn| txn.objects.iter()).collect()
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_encoder_matches_metadata_wire_golden() {
    let mut builder = PortableLogicalBuilder::new();
    builder.add_metadata("client", "client-a").unwrap();
    builder
        .add_object_map(PortableObjectMapEntry {
            mv_table_id: -5,
            name: "items",
        })
        .unwrap();
    let encoded = builder.finish().unwrap();

    assert_eq!(
        encoded,
        vec![
            0x62, 0x06, b'c', b'l', b'i', b'e', b'n', b't', 0x62, 0x08, b'c', b'l', b'i', b'e',
            b'n', b't', b'-', b'a', 0x62, 0x05, b'i', b't', b'e', b'm', b's', 0x6a, 0x04, 0x08,
            0x09, // mv_table_id = -5
            0x10, 0x02, // name_ref = "items"
            0x72, 0x04, 0x08, 0x00, // metadata key_ref = "client"
            0x10, 0x01, // metadata value_ref = "client-a"
        ]
    );
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_disabled_by_default() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, portable_changes TEXT)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1, 'alpha')")
        .unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&conn);

    assert!(portable_changes.is_empty());
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_contains_user_schema_and_rows() {
    let db = MvccTestDb::new_with_portable_logical_changes();
    db.conn
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, portable_changes TEXT)")
        .unwrap();
    db.conn
        .execute("INSERT INTO items VALUES (1, 'alpha')")
        .unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&db.conn);
    let txns = decode_portable_change_txns(&portable_changes);
    let objects = decoded_object_maps(&txns);

    assert!(objects
        .iter()
        .any(|object| object.name == "items" && object.mv_table_id < 0));
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_updates_name_mapping_across_rename() {
    let db = MvccTestDb::new_with_portable_logical_changes();
    db.conn
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, payload TEXT)")
        .unwrap();
    db.conn
        .execute("INSERT INTO items VALUES (1, 'before')")
        .unwrap();
    db.conn
        .execute("ALTER TABLE items RENAME TO things")
        .unwrap();
    db.conn
        .execute("INSERT INTO things VALUES (2, 'after')")
        .unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&db.conn);
    let txns = decode_portable_change_txns(&portable_changes);
    let objects = decoded_object_maps(&txns);
    assert!(objects.iter().any(|object| object.name == "items"));
    assert!(objects.iter().any(|object| object.name == "things"));
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_emit_refresh_for_same_rowid_schema_update() {
    let db = MvccTestDb::new_with_portable_logical_changes();
    db.conn
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, portable_changes TEXT)")
        .unwrap();
    db.conn
        .execute("ALTER TABLE items ADD COLUMN note TEXT")
        .unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&db.conn);
    let txns = decode_portable_change_txns(&portable_changes);
    let objects = decoded_object_maps(&txns);

    assert!(objects.iter().any(|object| object.name == "items"));
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_emit_drop_and_create_for_drop_recreate_same_name() {
    let db = MvccTestDb::new_with_portable_logical_changes();
    db.conn
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, portable_changes TEXT)")
        .unwrap();
    db.conn
        .execute("INSERT INTO items VALUES (1, 'old')")
        .unwrap();

    db.conn.execute("BEGIN").unwrap();
    db.conn.execute("DROP TABLE items").unwrap();
    db.conn
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, note TEXT)")
        .unwrap();
    db.conn
        .execute("INSERT INTO items VALUES (2, 'new')")
        .unwrap();
    db.conn.execute("COMMIT").unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&db.conn);
    let txns = decode_portable_change_txns(&portable_changes);
    let objects = decoded_object_maps(&txns);
    let item_table_ids = objects
        .iter()
        .filter(|object| object.name == "items")
        .map(|object| object.mv_table_id)
        .collect::<HashSet<_>>();

    assert!(
        item_table_ids.len() >= 2,
        "drop/recreate should expose old and new table identities"
    );
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_resolve_rows_through_object_map_in_same_txn() {
    let db = MvccTestDb::new_with_portable_logical_changes();
    db.conn.execute("BEGIN").unwrap();
    db.conn
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, payload TEXT)")
        .unwrap();
    db.conn
        .execute("INSERT INTO items VALUES (1, 'alpha')")
        .unwrap();
    db.conn.execute("COMMIT").unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&db.conn);
    let txns = decode_portable_change_txns(&portable_changes);
    let objects = decoded_object_maps(&txns);
    let object = objects
        .iter()
        .find(|object| object.name == "items")
        .expect("object map should resolve same-transaction table writes");

    assert!(object.mv_table_id < 0);
}

#[test]
fn test_mvcc_mode_supports_cdc_for_client_push() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full,turso_cdc')")
        .unwrap();

    conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, payload TEXT)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1, 'alpha')")
        .unwrap();
    conn.execute("UPDATE items SET payload = 'beta' WHERE id = 1")
        .unwrap();
    conn.execute("DELETE FROM items WHERE id = 1").unwrap();

    let item_rows = get_rows(
        &conn,
        "SELECT change_id, change_txn_id, change_type
         FROM turso_cdc
         WHERE table_name = 'items'
         ORDER BY change_id",
    );
    let item_change_types = item_rows
        .iter()
        .map(|row| row[2].as_int().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(item_change_types, vec![1, 0, -1]);
    assert!(item_rows.iter().all(|row| row[1].as_int().unwrap() > 0));

    let all_rows = get_rows(
        &conn,
        "SELECT change_id, change_type
         FROM turso_cdc
         ORDER BY change_id",
    );
    let change_ids = all_rows
        .iter()
        .map(|row| row[0].as_int().unwrap())
        .collect::<Vec<_>>();
    let commit_count = all_rows
        .iter()
        .filter(|row| row[1].as_int() == Some(2))
        .count();

    assert_eq!(change_ids, (1..=all_rows.len() as i64).collect::<Vec<_>>());
    assert_eq!(commit_count, 4);
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_emit_index_drop_for_drop_table() {
    let db = MvccTestDb::new_with_portable_logical_changes();
    db.conn
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, portable_changes TEXT)")
        .unwrap();
    db.conn
        .execute("CREATE INDEX items_payload_idx ON items(portable_changes)")
        .unwrap();
    db.conn.execute("DROP TABLE items").unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&db.conn);
    let txns = decode_portable_change_txns(&portable_changes);
    let objects = decoded_object_maps(&txns);

    assert!(objects.iter().any(|object| object.name == "items"));
    assert!(!bytes_contain(&portable_changes, b"items_payload_idx"));
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_emit_index_trigger_and_view_schema_ops() {
    let db = MvccTestDb::new_with_portable_logical_changes();
    db.conn
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, portable_changes TEXT)")
        .unwrap();
    db.conn
        .execute("CREATE INDEX items_payload_idx ON items(portable_changes)")
        .unwrap();
    db.conn
        .execute("CREATE VIEW items_view AS SELECT id, portable_changes FROM items")
        .unwrap();
    db.conn
        .execute(
            "CREATE TRIGGER items_ai AFTER INSERT ON items BEGIN UPDATE items SET portable_changes = NEW.portable_changes WHERE id = NEW.id; END",
        )
        .unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&db.conn);
    let txns = decode_portable_change_txns(&portable_changes);
    let objects = decoded_object_maps(&txns);

    assert!(objects.iter().any(|object| object.name == "items"));
    assert!(!bytes_contain(&portable_changes, b"items_payload_idx"));
    assert!(!bytes_contain(&portable_changes, b"CREATE VIEW items_view"));
    assert!(!bytes_contain(
        &portable_changes,
        b"CREATE TRIGGER items_ai"
    ));
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_emit_trigger_and_view_lifecycle_ops() {
    let db = MvccTestDb::new_with_portable_logical_changes();
    db.conn
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, portable_changes TEXT)")
        .unwrap();
    db.conn
        .execute("CREATE VIEW items_view AS SELECT id, portable_changes FROM items")
        .unwrap();
    db.conn
        .execute(
            "CREATE TRIGGER items_ai AFTER INSERT ON items BEGIN UPDATE items SET portable_changes = NEW.portable_changes WHERE id = NEW.id; END",
        )
        .unwrap();

    db.conn.execute("BEGIN").unwrap();
    db.conn.execute("DROP VIEW items_view").unwrap();
    db.conn.execute("DROP TRIGGER items_ai").unwrap();
    db.conn
        .execute("CREATE VIEW items_view AS SELECT id FROM items")
        .unwrap();
    db.conn
        .execute("CREATE TRIGGER items_ai AFTER INSERT ON items BEGIN SELECT NEW.id; END")
        .unwrap();
    db.conn.execute("COMMIT").unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&db.conn);

    assert!(!bytes_contain(&portable_changes, b"items_view"));
    assert!(!bytes_contain(&portable_changes, b"items_ai"));
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_emit_header_only_commits() {
    let db = MvccTestDb::new_with_portable_logical_changes();
    db.conn.execute("PRAGMA user_version = 42").unwrap();
    db.conn.execute("PRAGMA application_id = 1337").unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&db.conn);
    let recovery_ops = collect_mvcc_recovery_ops(&db.conn);

    // Header-only commits touch no user object, but a portable-enabled writer
    // still emits the extension block so readers can tell an empty change set
    // apart from pre-portable history. The payload therefore carries the frame
    // cursor and commit timestamp and nothing else.
    let txns = decode_portable_change_txns(&portable_changes);
    assert_eq!(txns.len(), 2);
    assert!(decoded_object_maps(&txns).is_empty());
    assert!(txns.iter().all(|txn| txn.metadata.is_empty()));
    assert_eq!(
        recovery_ops
            .iter()
            .filter(|op| matches!(op, ParsedOp::UpdateHeader { .. }))
            .count(),
        2
    );
}

#[test]
#[cfg(feature = "conn_raw_api")]
fn test_mvcc_portable_changes_use_checkpointed_schema_after_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.set_portable_logical_changes_enabled(true);
        conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, portable_changes TEXT)")
            .unwrap();
        conn.execute("INSERT INTO items VALUES (1, 'before')")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }

    db.restart();
    {
        let conn = db.connect();
        conn.set_portable_logical_changes_enabled(true);
        conn.execute("INSERT INTO items VALUES (2, 'after')")
            .unwrap();
        conn.execute("ALTER TABLE items ADD COLUMN note TEXT")
            .unwrap();
        conn.execute("UPDATE items SET note = 'backfill' WHERE id = 2")
            .unwrap();

        let portable_changes = collect_mvcc_portable_change_bytes(&conn);
        let txns = decode_portable_change_txns(&portable_changes);
        let objects = decoded_object_maps(&txns);

        assert!(objects.iter().any(|object| object.name == "items"));
        conn.close().unwrap();
    }
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_resolve_user_table_after_cross_connection_checkpoint() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let creator = db.connect().unwrap();
    creator.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    creator.set_portable_logical_changes_enabled(true);
    creator
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, payload TEXT)")
        .unwrap();
    let rows = get_rows(
        &creator,
        "SELECT rootpage FROM sqlite_schema WHERE name = 'items'",
    );
    let initial_rootpage = rows[0][0].as_int().unwrap();
    assert!(initial_rootpage < 0);

    let checkpoint = db.connect().unwrap();
    checkpoint
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    let rows = get_rows(
        &checkpoint,
        "SELECT rootpage FROM sqlite_schema WHERE name = 'items'",
    );
    let rootpage = rows[0][0].as_int().unwrap();
    assert!(rootpage > 0);
    {
        let schema = checkpoint.db.schema.lock();
        assert_eq!(schema.table_name_for_root_page(rootpage), Some("items"));
        assert_eq!(schema.table_name_for_root_page(initial_rootpage), None);
    }
    {
        let schema = checkpoint.schema.read();
        assert_eq!(schema.table_name_for_root_page(rootpage), Some("items"));
        assert_eq!(schema.table_name_for_root_page(initial_rootpage), None);
    }

    let writer = db.connect().unwrap();
    writer.set_portable_logical_changes_enabled(true);
    writer
        .execute("INSERT INTO items(id, payload) VALUES (1, 'after-checkpoint')")
        .unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&writer);
    let txns = decode_portable_change_txns(&portable_changes);
    let objects = decoded_object_maps(&txns);

    assert!(
        objects.iter().any(|object| object.name == "items"),
        "DML-only portable frame should resolve user table after cross-connection checkpoint"
    );
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_resolve_table_after_alter_backfill() {
    let db = MvccTestDb::new_with_portable_logical_changes();
    db.conn
        .execute(
            "CREATE TABLE items(
                id INTEGER PRIMARY KEY,
                owner TEXT NOT NULL,
                payload TEXT NOT NULL,
                rev INTEGER NOT NULL DEFAULT 0
            )",
        )
        .unwrap();
    db.conn
        .execute("CREATE INDEX items_owner_rev_idx ON items(owner, rev)")
        .unwrap();
    db.conn
        .execute("INSERT INTO items (id, owner, payload, rev) VALUES (1, 'seed-a', 'alpha', 1)")
        .unwrap();

    db.conn
        .execute("ALTER TABLE items ADD COLUMN note TEXT")
        .unwrap();
    db.conn
        .execute("UPDATE items SET note = 'schema-note'")
        .unwrap();

    db.conn
        .execute(
            "INSERT INTO items (id, owner, payload, rev, note)
             VALUES (1000000, 'remote-owner', 'remote-bootstrap-5', 1, 'remote-owner-note-1000000')",
        )
        .unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&db.conn);
    let txns = decode_portable_change_txns(&portable_changes);
    assert!(
        txns.len() >= 6,
        "expected every portable-enabled commit to be encoded, got {} txns",
        txns.len()
    );
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_emit_ddl_and_backfill_rows_in_same_transaction() {
    let db = MvccTestDb::new_with_portable_logical_changes();
    db.conn
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, portable_changes TEXT)")
        .unwrap();
    db.conn
        .execute("INSERT INTO items VALUES (1, 'alpha'), (2, 'beta')")
        .unwrap();

    db.conn.execute("BEGIN").unwrap();
    db.conn
        .execute("ALTER TABLE items ADD COLUMN note TEXT")
        .unwrap();
    db.conn
        .execute("UPDATE items SET note = 'backfilled'")
        .unwrap();
    db.conn.execute("COMMIT").unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&db.conn);
    let txns = decode_portable_change_txns(&portable_changes);
    let objects = decoded_object_maps(&txns);

    assert!(objects.iter().any(|object| object.name == "items"));
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_delete_carries_pk_projection_not_old_record() {
    let db = MvccTestDb::new_with_portable_logical_changes();
    db.conn
        .execute("CREATE TABLE items(id TEXT PRIMARY KEY, payload TEXT)")
        .unwrap();
    db.conn
        .execute("INSERT INTO items VALUES ('item-a', 'alpha')")
        .unwrap();
    db.conn
        .execute("DELETE FROM items WHERE id = 'item-a'")
        .unwrap();

    let recovery_ops = collect_mvcc_recovery_ops(&db.conn);

    let delete_pk_record = recovery_ops.iter().find_map(|op| match op {
        ParsedOp::DeleteTable {
            rowid,
            record_bytes,
            pk_record_bytes,
            ..
        } if rowid.table_id != SQLITE_SCHEMA_MVCC_TABLE_ID => {
            assert!(
                record_bytes.is_empty(),
                "data DELETE must not duplicate the full row record"
            );
            Some(pk_record_bytes.clone())
        }
        _ => None,
    });
    let delete_pk_record = delete_pk_record.expect("expected data DELETE op");
    let pk_values = ImmutableRecord::from_bin_record(
        crate::types::value_blob_from_slice(&delete_pk_record).expect(crate::alloc::ALLOC_ERR_MSG),
    )
    .get_values_owned()
    .unwrap();
    assert_eq!(
        pk_values,
        vec![Value::Text(Text::new("item-a".to_string()))]
    );
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_do_not_infer_origin_from_application_table() {
    let db = MvccTestDb::new_with_portable_logical_changes();
    db.conn
        .execute(
            "CREATE TABLE turso_sync_last_change_id(client_id TEXT PRIMARY KEY, pull_gen INTEGER, change_id INTEGER)",
        )
        .unwrap();
    db.conn
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, portable_changes TEXT)")
        .unwrap();
    db.conn.execute("BEGIN").unwrap();
    db.conn
        .execute("INSERT INTO turso_sync_last_change_id VALUES ('client-a', 1, 10)")
        .unwrap();
    db.conn
        .execute("INSERT INTO items VALUES (1, 'visible')")
        .unwrap();
    db.conn.execute("COMMIT").unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&db.conn);
    let txns = decode_portable_change_txns(&portable_changes);
    let objects = decoded_object_maps(&txns);

    assert!(!bytes_contain(
        &portable_changes,
        b"turso_sync_last_change_id"
    ));
    assert!(txns.iter().all(|txn| !txn.metadata.contains_key("client")));
    assert!(objects.iter().any(|object| object.name == "items"));
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_mark_internal_only_commits_explicitly() {
    // A push whose user statements match no row commits only the sync
    // bookkeeping upsert. That transaction has recovery ops but no user-visible
    // change, and it must still be written as an extension frame: a plain frame
    // is indistinguishable from pre-portable history, so a sync server planning
    // a logical pull has to refuse the range, which wedges the database into a
    // replace-base loop (no fresh replica can bootstrap again).
    let db = MvccTestDb::new_with_portable_logical_changes();
    db.conn
        .execute(
            "CREATE TABLE turso_sync_last_change_id(client_id TEXT PRIMARY KEY, change_id INTEGER)",
        )
        .unwrap();
    db.conn
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, payload TEXT)")
        .unwrap();
    db.conn
        .execute("INSERT INTO items VALUES (1, 'alpha')")
        .unwrap();

    let before = decode_portable_change_txns(&collect_mvcc_portable_change_bytes(&db.conn)).len();

    db.conn.execute("BEGIN").unwrap();
    db.conn
        .execute("DELETE FROM items WHERE payload = 'no-such-row'")
        .unwrap();
    db.conn
        .execute("INSERT INTO turso_sync_last_change_id VALUES ('client-a', 7)")
        .unwrap();
    db.conn.execute("COMMIT").unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&db.conn);
    let txns = decode_portable_change_txns(&portable_changes);

    assert_eq!(
        txns.len(),
        before + 1,
        "internal-only commit must still emit a portable extension frame"
    );
    let internal_only = txns.last().unwrap();
    assert!(internal_only.objects.is_empty());
    assert!(!bytes_contain(
        &portable_changes,
        b"turso_sync_last_change_id"
    ));
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_metadata_does_not_auto_enable_or_get_consumed() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.set_mvcc_log_meta("client".to_string(), Some("client-a".to_string()));
    conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, payload TEXT)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1, 'alpha')")
        .unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&conn);
    assert!(portable_changes.is_empty());

    conn.set_portable_logical_changes_enabled(true);
    conn.execute("INSERT INTO items VALUES (2, 'beta')")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (3, 'gamma')")
        .unwrap();

    let portable_changes = collect_mvcc_portable_change_bytes(&conn);
    let txns = decode_portable_change_txns(&portable_changes);
    let clients = txns
        .iter()
        .filter_map(|txn| txn.metadata.get("client").cloned())
        .collect::<Vec<_>>();
    assert_eq!(
        clients,
        vec!["client-a".to_string(), "client-a".to_string()]
    );
}

#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_are_encrypted_with_log_body() {
    use crate::storage::encryption::{CipherMode, EncryptionContext};

    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let db = MvccTestDbNoConn::new_encrypted(hex_key);
    let conn = db.connect();
    conn.set_portable_logical_changes_enabled(true);
    conn.execute("CREATE TABLE secret_items(id INTEGER PRIMARY KEY, payload TEXT)")
        .unwrap();
    conn.execute("INSERT INTO secret_items VALUES (1, 'secret-alpha')")
        .unwrap();

    let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");
    let log_bytes = std::fs::read(log_path).unwrap();
    assert!(!bytes_contain(&log_bytes, b"secret_items"));
    assert!(!bytes_contain(&log_bytes, b"secret-alpha"));

    let key = EncryptionKey::from_hex_string(hex_key).unwrap();
    let enc_ctx = EncryptionContext::new(CipherMode::Aes256Gcm, &key, 4096).unwrap();
    let portable_changes = collect_mvcc_portable_change_bytes_with_encryption(&conn, enc_ctx);
    let txns = decode_portable_change_txns(&portable_changes);
    let objects = decoded_object_maps(&txns);

    assert!(objects.iter().any(|object| object.name == "secret_items"));
}

/// After a checkpoint, log records reference tables by the canonical
/// -(root_page) id, but `table_id_to_rootpage` keeps entries keyed by the
/// original in-memory counter ids. Root pages drift away from the
/// -(counter id) alignment as `sqlite_schema` page splits interleave with
/// btree creation (the fat multi-column DDL below forces those splits), so a
/// canonical id aliases the still-live counter-id entry of an unrelated
/// object (typically an autoindex). Resolving portable object-map names
/// through the map first then followed the wrong root page, failing the
/// commit with "portable changes cannot resolve user data table id ...".
/// Post-checkpoint inserts must resolve every table to its own name.
#[cfg(feature = "conn_raw_api")]
#[test]
fn test_mvcc_portable_changes_resolve_checkpointed_table_despite_counter_id_alias() {
    let db = MvccTestDb::new_with_portable_logical_changes();
    let tables = 40;
    let columns = (0..12)
        .map(|c| format!("column_with_a_long_name_{c:02} TEXT"))
        .collect::<Vec<_>>()
        .join(", ");
    for i in 0..tables {
        db.conn
            .execute(format!(
                "CREATE TABLE t{i}({columns}, \
                 UNIQUE(column_with_a_long_name_00), \
                 UNIQUE(column_with_a_long_name_01), \
                 UNIQUE(column_with_a_long_name_02))"
            ))
            .unwrap();
        db.conn
            .execute(format!(
                "INSERT INTO t{i} VALUES ('a{i}', 'b{i}', 'c{i}', 'd{i}', 'e{i}', 'f{i}', \
                 'g{i}', 'h{i}', 'i{i}', 'j{i}', 'k{i}', 'l{i}')"
            ))
            .unwrap();
    }
    db.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    for i in 0..tables {
        db.conn
            .execute(format!(
                "INSERT INTO t{i} VALUES ('A{i}', 'B{i}', 'C{i}', 'D{i}', 'E{i}', 'F{i}', \
                 'G{i}', 'H{i}', 'I{i}', 'J{i}', 'K{i}', 'L{i}')"
            ))
            .unwrap();
    }

    let portable_changes = collect_mvcc_portable_change_bytes(&db.conn);
    let txns = decode_portable_change_txns(&portable_changes);
    let objects = decoded_object_maps(&txns);
    for i in 0..tables {
        let name = format!("t{i}");
        assert!(
            objects.iter().any(|object| object.name == name),
            "{name} missing from portable object maps"
        );
    }
}

/// Encrypted version of test_recovery_checkpoint_then_more_writes.
/// Checkpoint some rows, write more without checkpointing, restart, verify all rows survive.
#[test]
fn test_encrypted_recovery_checkpoint_then_more_writes() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
    }

    let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");
    assert!(log_path.exists(), "db-log file should exist before restart");
    assert_log_payloads_decrypt(
        &log_path,
        hex_key,
        crate::storage::encryption::CipherMode::Aes256Gcm,
    );

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "b");
    assert_eq!(rows[2][0].as_int().unwrap(), 3);
    assert_eq!(rows[2][1].to_string(), "c");
}

/// Write, restart, write more, restart again, verify all data accumulates correctly.
#[test]
fn test_encrypted_recovery_multiple_restart_cycles() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);
    let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");

    // Cycle 1: create table + insert
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'first')").unwrap();
    }

    assert!(log_path.exists(), "db-log file should exist after cycle 1");
    assert_log_payloads_decrypt(
        &log_path,
        hex_key,
        crate::storage::encryption::CipherMode::Aes256Gcm,
    );
    db.restart();

    // Cycle 2: insert more rows
    {
        let conn = db.connect();
        conn.execute("INSERT INTO t VALUES (2, 'second')").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'third')").unwrap();
    }

    db.restart();

    // Verify all rows survived two restart cycles
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][1].to_string(), "first");
    assert_eq!(rows[1][1].to_string(), "second");
    assert_eq!(rows[2][1].to_string(), "third");
}

/// Corrupt ciphertext bytes in the encrypted log payload. Recovery should treat the
/// corrupted frame as a torn tail and stop cleanly without losing earlier valid frames.
#[test]
fn test_encrypted_recovery_corrupted_ciphertext() {
    let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let mut db = MvccTestDbNoConn::new_encrypted(hex_key);
    let log_path = std::path::PathBuf::from(db.path.as_ref().unwrap()).with_extension("db-log");

    // Write two transactions: checkpoint the first, leave the second only in the log.
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'survives')")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'corrupted')")
            .unwrap();
    }

    assert!(
        log_path.exists(),
        "db-log file should exist before corruption"
    );
    assert_log_payloads_decrypt(
        &log_path,
        hex_key,
        crate::storage::encryption::CipherMode::Aes256Gcm,
    );

    // Corrupt the portable_changes of the second (non-checkpointed) frame in the log file.
    // The log header is 56 bytes, then the TX header is 24 bytes. Flip a byte
    // in the encrypted payload area right after that.
    {
        let mut log_bytes = std::fs::read(&log_path).expect("log file should exist");
        assert!(
            log_bytes.len() > 56 + 24 + 1,
            "log should have data beyond header + tx header"
        );
        // Flip a byte in the encrypted payload region
        let corrupt_offset = 56 + 24 + 1;
        log_bytes[corrupt_offset] ^= 0xFF;
        std::fs::write(&log_path, &log_bytes).unwrap();
    }

    // Restart: recovery should discard the corrupted frame but the checkpointed
    // row (id=1) must survive because it's already in the DB file.
    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "survives");
}

/// Reproducer for a bug where log replay after checkpoint-restart-checkpoint-restart
/// panics with "table id that does not exist in the table_id_to_rootpage map".
///
/// The scenario from the simulator:
/// 1. Create many tables, insert data, checkpoint (tables get positive root pages)
/// 2. Restart (recovery rebuilds table_id_to_rootpage from btree schema)
/// 3. Create more tables + insert into old and new tables
/// 4. Checkpoint (all tables now have positive root pages, log is truncated)
/// 5. Insert more data into all tables (un-checkpointed, written to log with
///    table IDs assigned in this server incarnation)
/// 6. Restart → bootstrap rebuilds map from btree root pages, then log replay
///    sees row inserts for table IDs that may not match the bootstrap mapping
#[test]
fn test_recovery_many_tables_checkpoint_restart_checkpoint_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let num_initial_tables = 50;
    let num_extra_tables = 30;

    // Step 1: Create many tables, insert data, checkpoint
    {
        let conn = db.connect();
        for i in 0..num_initial_tables {
            conn.execute(format!("CREATE TABLE t{i}(id INTEGER PRIMARY KEY, v TEXT)"))
                .unwrap();
            conn.execute(format!("INSERT INTO t{i} VALUES (1, 'init')"))
                .unwrap();
        }
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }

    // Step 2: Restart (simulates server redeploy)
    db.restart();

    // Step 3: Create more tables + insert into old tables, then checkpoint
    {
        let conn = db.connect();
        // Create new tables (these get new negative table IDs)
        for i in 0..num_extra_tables {
            conn.execute(format!(
                "CREATE TABLE extra{i}(id INTEGER PRIMARY KEY, v TEXT)"
            ))
            .unwrap();
            conn.execute(format!("INSERT INTO extra{i} VALUES (1, 'extra')"))
                .unwrap();
        }
        // Insert into the original tables
        for i in 0..num_initial_tables {
            conn.execute(format!("INSERT INTO t{i} VALUES (2, 'after_restart')"))
                .unwrap();
        }
        // Step 4: Checkpoint - all tables get positive root pages, log truncated
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        // Step 5: More writes after checkpoint (un-checkpointed, in the log)
        for i in 0..num_initial_tables {
            conn.execute(format!("INSERT INTO t{i} VALUES (3, 'post_ckpt2')"))
                .unwrap();
        }
        for i in 0..num_extra_tables {
            conn.execute(format!(
                "INSERT INTO extra{i} VALUES (2, 'extra_post_ckpt')"
            ))
            .unwrap();
        }
        conn.close().unwrap();
    }

    // Step 6: Restart again - log replay should not panic
    db.restart();

    // Verify data integrity
    {
        let conn = db.connect();
        for i in 0..num_initial_tables {
            let rows = get_rows(&conn, &format!("SELECT id, v FROM t{i} ORDER BY id"));
            assert_eq!(
                rows.len(),
                3,
                "table t{i} should have 3 rows, got {}",
                rows.len()
            );
        }
        for i in 0..num_extra_tables {
            let rows = get_rows(&conn, &format!("SELECT id, v FROM extra{i} ORDER BY id"));
            assert_eq!(
                rows.len(),
                2,
                "table extra{i} should have 2 rows, got {}",
                rows.len()
            );
        }
    }
}

/// Variant that does 3 restart cycles with tables created across each incarnation.
/// This stresses the table_id_to_rootpage mapping more aggressively.
#[test]
fn test_recovery_three_restarts_with_table_creation() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    // Incarnation 1: create tables, checkpoint
    {
        let conn = db.connect();
        for i in 0..20 {
            conn.execute(format!("CREATE TABLE a{i}(id INTEGER PRIMARY KEY, v TEXT)"))
                .unwrap();
            conn.execute(format!("INSERT INTO a{i} VALUES (1, 'a')"))
                .unwrap();
        }
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }

    db.restart();

    // Incarnation 2: create more tables, insert into old, checkpoint, then more writes
    {
        let conn = db.connect();
        for i in 0..20 {
            conn.execute(format!("CREATE TABLE b{i}(id INTEGER PRIMARY KEY, v TEXT)"))
                .unwrap();
            conn.execute(format!("INSERT INTO b{i} VALUES (1, 'b')"))
                .unwrap();
        }
        for i in 0..20 {
            conn.execute(format!("INSERT INTO a{i} VALUES (2, 'a2')"))
                .unwrap();
        }
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        // Un-checkpointed writes
        for i in 0..20 {
            conn.execute(format!("INSERT INTO a{i} VALUES (3, 'a3')"))
                .unwrap();
            conn.execute(format!("INSERT INTO b{i} VALUES (2, 'b2')"))
                .unwrap();
        }
        conn.close().unwrap();
    }

    db.restart();

    // Incarnation 3: create even more tables, insert everywhere, checkpoint, more writes
    {
        let conn = db.connect();
        for i in 0..20 {
            conn.execute(format!("CREATE TABLE c{i}(id INTEGER PRIMARY KEY, v TEXT)"))
                .unwrap();
            conn.execute(format!("INSERT INTO c{i} VALUES (1, 'c')"))
                .unwrap();
        }
        for i in 0..20 {
            conn.execute(format!("INSERT INTO a{i} VALUES (4, 'a4')"))
                .unwrap();
            conn.execute(format!("INSERT INTO b{i} VALUES (3, 'b3')"))
                .unwrap();
        }
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        // Un-checkpointed writes to all tables
        for i in 0..20 {
            conn.execute(format!("INSERT INTO a{i} VALUES (5, 'a5')"))
                .unwrap();
            conn.execute(format!("INSERT INTO b{i} VALUES (4, 'b4')"))
                .unwrap();
            conn.execute(format!("INSERT INTO c{i} VALUES (2, 'c2')"))
                .unwrap();
        }
        conn.close().unwrap();
    }

    // Final restart - should not panic during log replay
    db.restart();

    {
        let conn = db.connect();
        for i in 0..20 {
            let rows = get_rows(&conn, &format!("SELECT id FROM a{i} ORDER BY id"));
            assert_eq!(rows.len(), 5, "table a{i} should have 5 rows");
            let rows = get_rows(&conn, &format!("SELECT id FROM b{i} ORDER BY id"));
            assert_eq!(rows.len(), 4, "table b{i} should have 4 rows");
            let rows = get_rows(&conn, &format!("SELECT id FROM c{i} ORDER BY id"));
            assert_eq!(rows.len(), 2, "table c{i} should have 2 rows");
        }
    }
}

fn create_wide_table_like_schema(conn: &Arc<Connection>) {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS core(
            id INTEGER PRIMARY KEY,
            row_number INTEGER NOT NULL,
            sheet_id INTEGER NOT NULL,
            created_by TEXT,
            updated_by TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            col_1 TEXT,
            col_2 TEXT,
            col_3 TEXT,
            col_4 TEXT,
            col_5 TEXT,
            col_6 TEXT,
            col_7 TEXT,
            col_8 TEXT
        )",
    )
    .unwrap();
    conn.execute("CREATE INDEX IF NOT EXISTS idx_core_sheet_row ON core(sheet_id, row_number)")
        .unwrap();
    conn.execute("CREATE INDEX IF NOT EXISTS idx_core_created ON core(created_at)")
        .unwrap();
    conn.execute("CREATE INDEX IF NOT EXISTS idx_core_updated ON core(updated_at, sheet_id)")
        .unwrap();
    conn.execute("CREATE INDEX IF NOT EXISTS idx_core_created_by ON core(created_by, sheet_id)")
        .unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS metadata(
            sheet_id INTEGER PRIMARY KEY,
            next_row_number INTEGER NOT NULL DEFAULT 1,
            row_count INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT DEFAULT (datetime('now'))
        )",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS audit_log(
            id INTEGER PRIMARY KEY,
            sheet_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            row_id INTEGER,
            row_number INTEGER,
            created_at TEXT DEFAULT (datetime('now')),
            details TEXT
        )",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS trigger_gate(
            id INTEGER PRIMARY KEY,
            sheet_id INTEGER NOT NULL,
            trigger_type TEXT NOT NULL,
            portable_changes TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )",
    )
    .unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO metadata(sheet_id, next_row_number, row_count, updated_at)
         VALUES (1, 1, 0, datetime('now'))",
    )
    .unwrap();
}

fn drop_wide_table_like_schema(conn: &Arc<Connection>) {
    conn.execute("DROP TABLE IF EXISTS trigger_gate").unwrap();
    conn.execute("DROP TABLE IF EXISTS audit_log").unwrap();
    conn.execute("DROP TABLE IF EXISTS metadata").unwrap();
    conn.execute("DROP INDEX IF EXISTS idx_core_sheet_row")
        .unwrap();
    conn.execute("DROP INDEX IF EXISTS idx_core_created")
        .unwrap();
    conn.execute("DROP INDEX IF EXISTS idx_core_updated")
        .unwrap();
    conn.execute("DROP INDEX IF EXISTS idx_core_created_by")
        .unwrap();
    conn.execute("DROP TABLE IF EXISTS core").unwrap();
}

fn insert_wide_table_like_batch(conn: &Arc<Connection>, start_row_number: i64, rows: usize) {
    conn.execute("BEGIN").unwrap();

    for offset in 0..rows {
        let row_number = start_row_number + offset as i64;
        conn.execute(format!(
            "INSERT INTO core(
                row_number, sheet_id, created_by, updated_by,
                created_at, updated_at,
                col_1, col_2, col_3, col_4, col_5, col_6, col_7, col_8
             ) VALUES (
                {row_number}, 1, 'seed', 'seed',
                datetime('now'), datetime('now'),
                hex(randomblob(8)), hex(randomblob(8)), hex(randomblob(8)), hex(randomblob(8)),
                hex(randomblob(8)), hex(randomblob(8)), hex(randomblob(8)), hex(randomblob(8))
             )",
        ))
        .unwrap();

        conn.execute(format!(
            "INSERT INTO audit_log(sheet_id, action, row_number, details, created_at)
             VALUES (1, 'INSERT', {row_number}, 'wide table repro', datetime('now'))",
        ))
        .unwrap();
    }

    conn.execute(format!(
        "UPDATE metadata
         SET next_row_number = next_row_number + {rows},
             row_count = row_count + {rows},
             updated_at = datetime('now')
         WHERE sheet_id = 1",
    ))
    .unwrap();
    conn.execute(
        "INSERT INTO trigger_gate(sheet_id, trigger_type, portable_changes, created_at)
         VALUES (1, 'ROW_INSERT', '{\"count\": 1}', datetime('now'))",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO trigger_gate(sheet_id, trigger_type, portable_changes, created_at)
         VALUES (1, 'RECALC', '{\"sheet_id\": 1}', datetime('now'))",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO trigger_gate(sheet_id, trigger_type, portable_changes, created_at)
         VALUES (1, 'WEBHOOK', '{\"event\": \"rows_added\"}', datetime('now'))",
    )
    .unwrap();

    conn.execute("COMMIT").unwrap();
}

/// Reproducer for an MVCC crash-restart bug in checkpointing.
///
/// Sequence:
/// 1. Create a wide-table style schema and write one row.
/// 2. Simulate an abrupt process death (no clean connection close).
/// 3. Restart, drop the old schema, recreate it, write one new row.
/// 4. Checkpoint.
///
/// Checkpoint should retire the dropped table before creating the replacement table,
/// even when sqlite_schema rowids are reused across a crash + restart cycle.
#[test]
fn test_checkpoint_recovers_after_crash_restart_drop_recreate_table() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
            .unwrap();
        create_wide_table_like_schema(&conn);
        insert_wide_table_like_batch(&conn, 1, 1);
    }

    force_close_for_artifact_tamper(&mut db);
    db.restart();

    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
        .unwrap();
    drop_wide_table_like_schema(&conn);
    create_wide_table_like_schema(&conn);
    insert_wide_table_like_batch(&conn, 1, 1);
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(
        &conn,
        "SELECT row_number, sheet_id, created_by FROM core ORDER BY id",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 1);
    assert_eq!(rows[0][2].to_string(), "seed");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");

    conn.close().unwrap();
    db.restart();

    let conn = db.connect();
    let rows = get_rows(
        &conn,
        "SELECT row_number, sheet_id, created_by FROM core ORDER BY id",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 1);
    assert_eq!(rows[0][2].to_string(), "seed");
}

/// Reproducer for the original index-side panic:
/// "Index struct for index_id ... must exist when checkpointing index rows".
///
/// Sequence:
/// 1. Create and checkpoint a table with one row.
/// 2. Create an index on that existing table.
/// 3. Simulate an abrupt process death before the index is checkpointed.
/// 4. Restart, drop and recreate the index, insert one more row.
/// 5. Checkpoint.
///
/// Checkpoint should retire the dropped index before processing recovered index rows,
/// even when sqlite_schema reuses the same rowid for the recreated index.
#[test]
fn test_checkpoint_recovers_after_crash_restart_drop_recreate_index() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
            .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, portable_changes TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'seed_1', hex(randomblob(16)))")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("CREATE INDEX idx_t_v ON t(v)").unwrap();
    }

    force_close_for_artifact_tamper(&mut db);
    db.restart();

    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
        .unwrap();
    conn.execute("DROP INDEX IF EXISTS idx_t_v").unwrap();
    conn.execute("CREATE INDEX idx_t_v ON t(v)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'post_2', hex(randomblob(16)))")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "seed_1");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "post_2");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");

    conn.close().unwrap();
    db.restart();

    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "seed_1");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "post_2");
}

/// Regression for the production panic:
/// "Index struct for index_id ... must exist when checkpointing index rows".
///
/// The delayed checkpoint comes from the normal MVCC auto-checkpoint-on-commit
/// path:
/// 1. An INSERT commits with `mvcc_checkpoint_threshold = 0` and yields at the
///    checkpoint's `BeforeAcquireLock` point, after the checkpoint has captured
///    the schema but before it has collected rows.
/// 2. A second connection creates a new index while auto-checkpointing remains
///    enabled, making the index schema row durable after the delayed checkpoint's
///    schema snapshot.
/// 3. A later INSERT writes a fresh index row version for that now-durable index.
/// 4. The background connection observes the new schema through normal SQL, so
///    the resumed checkpoint no longer fails early as a stale-schema write.
/// 5. Resuming the delayed checkpoint refreshes its durable watermark, skips the
///    durable CREATE INDEX schema row, but still collects the fresh index row.
///    The checkpoint-local `index_id_to_index` map must have been refreshed
///    alongside the durable watermark so `WriteIndexRow` can open the index.
#[test]
fn test_auto_checkpoint_refreshes_index_metadata_after_schema_change() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = MvccTestDbNoConn::new_with_random_db();

    let conn_a = db.connect();
    conn_a
        .execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
        .unwrap();
    conn_a
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn_a.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    let conn_b = db.connect();
    conn_b
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    conn_b.set_yield_injector(Some(FixedYieldInjector::new([
        CheckpointYieldPoint::BeforeAcquireLock.point(),
    ])));

    let mut delayed_ckpt = conn_b
        .prepare("INSERT INTO t VALUES (10, 'pre_idx')")
        .unwrap();
    let mut ckpt_yielded = false;
    for _ in 0..1000 {
        match delayed_ckpt.step().unwrap() {
            StepResult::Yield => {
                ckpt_yielded = true;
                break;
            }
            StepResult::Done => break,
            StepResult::IO | StepResult::Sleep { .. } => conn_b.db.io.step().unwrap(),
            StepResult::Row | StepResult::Busy | StepResult::Interrupt => {}
        }
    }
    conn_b.set_yield_injector(None);
    assert!(
        ckpt_yielded,
        "auto-checkpoint should yield at BeforeAcquireLock with its schema snapshot captured"
    );

    conn_a.execute("CREATE INDEX idx ON t(v)").unwrap();
    conn_a
        .execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
        .unwrap();
    conn_a.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    let rows = get_rows(
        &conn_b,
        "SELECT name FROM sqlite_schema WHERE type = 'index' AND name = 'idx'",
    );
    assert_eq!(rows.len(), 1);

    delayed_ckpt.run_ignore_rows().unwrap();

    let rows = get_rows(&conn_a, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "b");
    assert_eq!(rows[2][0].as_int().unwrap(), 10);
    assert_eq!(rows[2][1].to_string(), "pre_idx");

    let rows = get_rows(&conn_a, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Reproducer for recovery of a dropped checkpointed index.
///
/// Sequence:
/// 1. Create and checkpoint a table plus index.
/// 2. Drop the checkpointed index.
/// 3. Simulate an abrupt process death before checkpoint.
/// 4. Restart and checkpoint.
///
/// Recovery must preserve the deleted sqlite_schema record so checkpoint can
/// retire the dropped index without losing its object identity.
#[test]
fn test_checkpoint_recovers_after_restart_drop_checkpointed_index() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
            .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX idx_t_v ON t(v)").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'seed_1')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("DROP INDEX idx_t_v").unwrap();
    }

    force_close_for_artifact_tamper(&mut db);
    db.restart();

    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "seed_1");

    let rows = get_rows(
        &conn,
        "SELECT name FROM sqlite_schema WHERE type = 'index' AND name = 'idx_t_v'",
    );
    assert_eq!(rows.len(), 0);

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_recovery_after_drop_table_with_uncheckpointed_index() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
            .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX idx_t_v ON t(v)").unwrap();
        conn.execute("DROP TABLE t").unwrap();
    }

    force_close_for_artifact_tamper(&mut db);
    db.restart();

    let conn = db.connect();
    let rows = get_rows(
        &conn,
        "SELECT type, name, tbl_name FROM sqlite_schema WHERE name IN ('t', 'idx_t_v') ORDER BY type, name",
    );
    assert_eq!(rows, Vec::<Vec<Value>>::new());
}

#[test]
fn test_recovery_after_drop_checkpointed_table_with_index() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
            .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX idx_t_v ON t(v)").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'seed')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("DROP TABLE t").unwrap();
    }

    force_close_for_artifact_tamper(&mut db);
    db.restart();

    let conn = db.connect();
    let rows = get_rows(
        &conn,
        "SELECT type, name, tbl_name FROM sqlite_schema WHERE name IN ('t', 'idx_t_v') ORDER BY type, name",
    );
    assert_eq!(rows, Vec::<Vec<Value>>::new());
}

#[test]
fn test_recovery_after_drop_checkpointed_table_with_if_not_exists_index() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let table = "mvcc_sync_items_1777416431036232886";
    let index = "mvcc_sync_items_1777416431036232886_owner_rev_idx";

    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
            .unwrap();
        conn.execute(format!(
            "CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, portable_changes TEXT NOT NULL, rev INTEGER NOT NULL DEFAULT 0)"
        ))
        .unwrap();
        conn.execute(format!(
            "CREATE INDEX IF NOT EXISTS {index} ON {table} (owner, rev)"
        ))
        .unwrap();
        conn.execute(format!(
            "INSERT INTO {table} (id, owner, portable_changes, rev) VALUES (1, 'seed-a', 'alpha', 1)"
        ))
        .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute(format!("DROP TABLE IF EXISTS {table}"))
            .unwrap();
    }

    force_close_for_artifact_tamper(&mut db);
    db.restart();

    let conn = db.connect();
    let rows = get_rows(
        &conn,
        &format!(
            "SELECT type, name, tbl_name FROM sqlite_schema WHERE name IN ('{table}', '{index}') ORDER BY type, name"
        ),
    );
    assert_eq!(rows, Vec::<Vec<Value>>::new());
}

#[test]
fn test_recovery_after_drop_table_with_many_schema_rows() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let target = "mvcc_sync_items_1777416431036232886";
    let target_index = "mvcc_sync_items_1777416431036232886_owner_rev_idx";

    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
            .unwrap();
        for i in 0..350 {
            conn.execute(format!(
                "CREATE TABLE filler_{i}(id INTEGER PRIMARY KEY, owner TEXT, rev INTEGER)"
            ))
            .unwrap();
            conn.execute(format!(
                "CREATE INDEX filler_{i}_idx ON filler_{i}(owner, rev)"
            ))
            .unwrap();
        }
        conn.execute(format!(
            "CREATE TABLE IF NOT EXISTS {target} (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, portable_changes TEXT NOT NULL, rev INTEGER NOT NULL DEFAULT 0)"
        ))
        .unwrap();
        conn.execute(format!(
            "CREATE INDEX IF NOT EXISTS {target_index} ON {target} (owner, rev)"
        ))
        .unwrap();
        conn.execute(format!(
            "INSERT INTO {target} (id, owner, portable_changes, rev) VALUES (1, 'seed-a', 'alpha', 1)"
        ))
        .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute(format!("DROP TABLE IF EXISTS {target}"))
            .unwrap();
    }

    force_close_for_artifact_tamper(&mut db);
    db.restart();

    let conn = db.connect();
    let rows = get_rows(
        &conn,
        &format!(
            "SELECT type, name, tbl_name FROM sqlite_schema WHERE name IN ('{target}', '{target_index}') ORDER BY type, name"
        ),
    );
    assert_eq!(rows, Vec::<Vec<Value>>::new());
}

#[test]
fn test_drop_recreate_indexed_table_many_inserts_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();

    for round in 0..2 {
        {
            let conn = db.connect();
            let mv_store = db.get_mvcc_store();
            mv_store.set_checkpoint_threshold(4096);

            if round > 0 {
                conn.execute("DROP TABLE IF EXISTS t").unwrap();
            }

            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c INTEGER)")
                .unwrap();
            conn.execute("CREATE INDEX idx_a ON t(a)").unwrap();
            conn.execute("CREATE INDEX idx_b ON t(b)").unwrap();
            conn.execute("CREATE INDEX idx_c ON t(c)").unwrap();

            for i in 0..1000 {
                conn.execute(format!("INSERT INTO t VALUES({i}, 'a_{i}', 'b_{i}', {i})"))
                    .unwrap();
            }

            conn.close().unwrap();
        }

        db.restart();

        {
            let conn = db.connect();
            let rows = get_rows(&conn, "SELECT count(*) FROM t");
            assert_eq!(
                rows[0][0].as_int().unwrap(),
                1000,
                "round {round}: expected 1000 rows"
            );
            conn.close().unwrap();
        }
    }
}

/// What this test checks: CREATE TYPE (which writes to __turso_internal_types,
/// not sqlite_schema) is visible to a second connection under MVCC.
/// Why this matters: The commit phase must detect schema changes even when no
/// rows are written to sqlite_schema. Without the fix, did_commit_schema_change
/// stayed false and the second connection never reloaded the schema.
#[test]
fn test_create_type_visible_to_second_connection_under_mvcc() {
    let db =
        MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new().with_custom_types(true));

    // conn1: define a custom type
    let conn1 = db.connect();
    conn1
        .execute("CREATE TYPE my_uint(value any) BASE text ENCODE my_uint_enc(value) DECODE my_uint_dec(value)")
        .unwrap();
    conn1.close().unwrap();

    // conn2: the type should be visible without reopening the database
    let conn2 = db.connect();
    let rows = get_rows(
        &conn2,
        "SELECT name FROM sqlite_turso_types WHERE name LIKE 'my_uint%'",
    );
    assert_eq!(rows.len(), 1, "CREATE TYPE should be visible to conn2");
    assert_eq!(rows[0][0].to_string(), "my_uint(value any)");
    conn2.close().unwrap();
}

/// Dropped roots that are still live roots must not be walked twice.
#[test]
fn test_integrity_check_ignores_dropped_root_that_is_live_after_recovery() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }

    db.restart();

    let conn = db.connect();

    let rows = get_rows(
        &conn,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 't'",
    );
    let root_page = rows[0][0].as_int().unwrap();
    assert!(root_page > 0);

    conn.with_schema_mut(|schema| {
        schema.dropped_root_pages.insert(root_page);
    })
    .unwrap();

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Passive mode: a stale dropped-root entry for a page already walked as a btree child must not
/// report double-reference.
#[test]
fn test_integrity_check_tolerates_dropped_root_reused_as_btree_child() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    for i in 0..1000 {
        conn.execute(format!(
            "INSERT INTO t VALUES ({i}, 'wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww')"
        ))
        .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let page_count = get_rows(&conn, "PRAGMA page_count")[0][0].as_int().unwrap();
    let root_page = get_rows(
        &conn,
        "SELECT rootpage FROM sqlite_schema WHERE type='table' AND name='t'",
    )[0][0]
        .as_int()
        .unwrap();
    assert!(page_count > root_page, "t should span multiple pages");
    conn.with_schema_mut(|schema| {
        schema.dropped_root_pages.insert(page_count);
    })
    .unwrap();

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        &rows[0][0].to_string(),
        "ok",
        "a reused dropped root must not be reported as doubly-referenced"
    );
}

/// `begin_tx`'s schema-generation gate: a begin whose caller validated its prepared schema at a
/// generation that no longer matches the store's current `schema_generation` (a passive checkpoint
/// republished roots in the begin window) must fail with `SchemaUpdated` so the statement
/// reprepares rather than begin against stale physical roots. A matching generation (or no gate)
/// begins normally.
#[test]
fn test_begin_tx_schema_generation_gate() {
    let db = MvccTestDb::new();
    let pager = db.conn.pager.load().clone();
    let generation = db.mvcc_store.schema_generation();

    // Matching generation: begins normally.
    let tx = db
        .mvcc_store
        .begin_tx_with_schema_generation(pager.clone(), Some(generation))
        .unwrap();
    db.mvcc_store
        .rollback_tx(tx, pager.clone(), &db.conn, crate::MAIN_DB_ID);

    // Stale (mismatched) generation: forced reprepare.
    let err = db
        .mvcc_store
        .begin_tx_with_schema_generation(pager.clone(), Some(generation + 1))
        .unwrap_err();
    assert!(
        matches!(err, LimboError::SchemaUpdated),
        "stale schema generation should force reprepare, got {err:?}"
    );

    // No gate: begins normally.
    let tx = db
        .mvcc_store
        .begin_tx_with_schema_generation(pager.clone(), None)
        .unwrap();
    db.mvcc_store
        .rollback_tx(tx, pager, &db.conn, crate::MAIN_DB_ID);
}

/// Passive mode: freelist fields must come from the pager's live page 1, not a stale MVCC header.
#[test]
fn test_integrity_check_passive_reads_freelist_from_pager_not_stale_mvcc_header() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    for i in 0..500 {
        conn.execute(format!(
            "INSERT INTO t VALUES ({i}, 'wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww')"
        ))
        .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    let page_count = get_rows(&conn, "PRAGMA page_count")[0][0].as_int().unwrap();
    assert!(page_count > 2, "t should span multiple pages");

    {
        let mv_guard = conn.db.get_mv_store();
        let mv = mv_guard.as_ref().expect("mvcc store");
        let mut gh = mv.global_header.write();
        let h = gh.as_mut().expect("global_header initialized");
        h.freelist_trunk_page = pack1::U32BE::new(page_count as u32);
        h.freelist_pages = pack1::U32BE::new(1);
    }

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        &rows[0][0].to_string(),
        "ok",
        "passive integrity_check must read the freelist from the pager's live page 1, not the stale MVCC header"
    );
}

/// PASSIVE port of PR #7620's reproducer: a checkpointed row gets an INSERT OR REPLACE (new
/// btree-resident marker + replacement) then the replacement is deleted. GC must retain the
/// btree-resident marker until checkpoint applies the physical delete, or the stale table row
/// survives while its index entry is removed -> "row missing from index".
#[test]
fn test_mvcc_passive_replace_then_delete_keeps_table_and_index_consistent() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name, iq, year)")
        .unwrap();
    conn.execute("CREATE INDEX t_iq ON t(iq)").unwrap();
    conn.execute(
        "INSERT INTO t VALUES (1,'v',100,2024),(2,'einstein',150,1950),(3,'newton',140,1850)",
    )
    .unwrap();
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    conn.execute("INSERT OR REPLACE INTO t(id,name,iq,year) VALUES(1,'v',120,2025)")
        .unwrap();
    conn.execute("DELETE FROM t WHERE id=1").unwrap();
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(
        &rows[0][0].to_string(),
        "ok",
        "passive replace-then-delete must keep table/index consistent, got {rows:?}"
    );
}

/// Snapshot stability under all of: nested-savepoint rollbacks, checkpoints,
/// CREATE/DROP INDEX, and concurrent committed writers.
///
/// One reader holds a long BEGIN CONCURRENT and repeatedly samples
/// `SELECT count(*) FROM t`; *every sample within the tx must be equal*.
/// If any pair differs, MVCC snapshot isolation is violated — the case the
/// original analysis points at (`gc_version_chain` Rule 3 reaping `V_old`
/// after a savepoint-thread's rollback restores `end=None`, while the
/// reader's snapshot still depends on it).
///
/// Disruptor threads (each toggleable via env):
///   REPRO_SP=1     — runs the nested-savepoint driver (BEGIN CONCURRENT;
///                    SAVEPOINT × depth with INSERTs and DELETEs of
///                    pre-existing rows; ROLLBACK TO sp_<rb>; RELEASE; COMMIT)
///   REPRO_CKPT=1   — cycles PRAGMA wal_checkpoint(PASSIVE/FULL/RESTART/TRUNCATE)
///   REPRO_DDL=1    — CREATE/DROP INDEX cycle
///   REPRO_WRITER=1 — committed INSERT/DELETE in BEGIN CONCURRENT/COMMIT
///   (defaults: SP, CKPT, WRITER on; DDL off because it currently hangs.)
///
/// Other knobs:
///   REPRO_DURATION_SECS=N  total wall-clock cap (default 30)
///   REPRO_READER_OPS=N     count samples per reader transaction (default 8)
#[test]
fn test_snapshot_stability_full() {
    use crate::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    // Honor RUST_LOG when set; ignored if a subscriber is already installed.
    // NOTE: deliberately NOT using `with_test_writer()` — that routes through
    // libtest's stdout capture, which serializes events behind a mutex and is
    // slow enough under heavy concurrency to suppress the very race we're
    // trying to log.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();

    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, b BLOB)")
            .unwrap();
        conn.execute("CREATE INDEX idx_v ON t(v)").unwrap();
        // Pre-existing rows so V_old candidates exist before any tx starts.
        for i in 0..500 {
            conn.execute(format!("INSERT INTO t VALUES ({i}, 'v_{i}', NULL)"))
                .unwrap();
        }
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let mismatch = Arc::new(AtomicBool::new(false));
    let reader_iters = Arc::new(AtomicU64::new(0));
    let reader_samples = Arc::new(AtomicU64::new(0));
    let sp_iters = Arc::new(AtomicU64::new(0));
    let writer_iters = Arc::new(AtomicU64::new(0));
    let ckpt_iters = Arc::new(AtomicU64::new(0));
    let ddl_iters = Arc::new(AtomicU64::new(0));
    let next_id = Arc::new(AtomicU64::new(10_000_000));

    let mismatch_first = Arc::new(AtomicI64::new(0));
    let mismatch_second = Arc::new(AtomicI64::new(0));
    let mismatch_idx_a = Arc::new(AtomicU64::new(0));
    let mismatch_idx_b = Arc::new(AtomicU64::new(0));

    let duration = Duration::from_secs(
        std::env::var("REPRO_DURATION_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5),
    );
    let reader_ops: usize = std::env::var("REPRO_READER_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);

    let enable_sp = std::env::var("REPRO_SP").map(|s| s != "0").unwrap_or(true);
    let enable_writer = std::env::var("REPRO_WRITER")
        .map(|s| s != "0")
        .unwrap_or(true);
    let enable_ckpt = std::env::var("REPRO_CKPT")
        .map(|s| s != "0")
        .unwrap_or(true);
    let enable_ddl = std::env::var("REPRO_DDL")
        .map(|s| s != "0")
        .unwrap_or(false);

    // --- Reader: snapshot-stability assertion ---
    let reader = {
        let db_arc = db.get_db();
        let stop = stop.clone();
        let mismatch = mismatch.clone();
        let reader_iters = reader_iters.clone();
        let reader_samples = reader_samples.clone();
        let mismatch_first = mismatch_first.clone();
        let mismatch_second = mismatch_second.clone();
        let mismatch_idx_a = mismatch_idx_a.clone();
        let mismatch_idx_b = mismatch_idx_b.clone();
        std::thread::spawn(move || {
            let conn = db_arc.connect().unwrap();
            while !stop.load(Ordering::Relaxed) && !mismatch.load(Ordering::Relaxed) {
                if conn.execute("BEGIN CONCURRENT").is_err() {
                    std::thread::yield_now();
                    continue;
                }
                let mut samples: Vec<i64> = Vec::with_capacity(reader_ops);
                for _ in 0..reader_ops {
                    let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
                    let rows = stmt.run_collect_rows().unwrap();
                    let c = rows[0][0].as_int().unwrap();
                    samples.push(c);
                    reader_samples.fetch_add(1, Ordering::Relaxed);
                }
                // All samples within one snapshot must be equal.
                if let Some((i, &c)) = samples.iter().enumerate().find(|(_, &c)| c != samples[0]) {
                    mismatch_first.store(samples[0], Ordering::Relaxed);
                    mismatch_second.store(c, Ordering::Relaxed);
                    mismatch_idx_a.store(0, Ordering::Relaxed);
                    mismatch_idx_b.store(i as u64, Ordering::Relaxed);
                    mismatch.store(true, Ordering::Relaxed);
                    let _ = conn.execute("ROLLBACK");
                    return;
                }
                let _ = conn.execute("COMMIT");
                reader_iters.fetch_add(1, Ordering::Relaxed);
            }
        })
    };

    // --- Savepoint-rollback driver: tombstones pre-existing rows inside a
    //     savepoint, then rolls back, restoring V_old.end = None. ---
    let sp_thread = enable_sp.then(|| {
        let db_arc = db.get_db();
        let stop = stop.clone();
        let mismatch = mismatch.clone();
        let sp_iters = sp_iters.clone();
        let next_id = next_id.clone();
        std::thread::spawn(move || {
            let conn = db_arc.connect().unwrap();
            let mut rng = ChaCha8Rng::seed_from_u64(0xCAFEF00D);
            while !stop.load(Ordering::Relaxed) && !mismatch.load(Ordering::Relaxed) {
                if conn.execute("BEGIN CONCURRENT").is_err() {
                    std::thread::yield_now();
                    continue;
                }
                let depth = 2 + (rng.random::<u8>() % 3) as usize;
                let mut sps = Vec::with_capacity(depth);
                let mut aborted = false;
                'sp: for i in 0..depth {
                    let name = format!("sp_{i}_{}", rng.random::<u32>() % 100_000);
                    if conn.execute(format!("SAVEPOINT {name}")).is_err() {
                        aborted = true;
                        break 'sp;
                    }
                    sps.push(name);
                    let muts = 1 + (rng.random::<u8>() % 4) as u64;
                    for _ in 0..muts {
                        let op = rng.random::<u8>() % 3;
                        let sql = if op == 0 {
                            // Tombstone a pre-existing baseline row inside SP.
                            let target = (rng.random::<u32>() % 500) as i64;
                            format!("DELETE FROM t WHERE id = {target}")
                        } else {
                            let id = next_id.fetch_add(1, Ordering::Relaxed) as i64;
                            format!("INSERT INTO t VALUES ({id}, 'sp_{id}', NULL)")
                        };
                        match conn.execute(&sql) {
                            Ok(_) => {}
                            Err(LimboError::Constraint(_)) => {}
                            Err(LimboError::WriteWriteConflict)
                            | Err(LimboError::Busy)
                            | Err(LimboError::TxTerminated) => {
                                aborted = true;
                                break 'sp;
                            }
                            Err(e) => panic!("sp mutation failed: {e:?}"),
                        }
                    }
                }
                if aborted {
                    let _ = conn.execute("ROLLBACK");
                    continue;
                }
                let rb = (rng.random::<u8>() as usize) % depth;
                let target = sps[rb].clone();
                let _ = conn.execute(format!("ROLLBACK TO {target}"));
                let _ = conn.execute(format!("RELEASE {target}"));
                let _ = conn.execute("COMMIT");
                sp_iters.fetch_add(1, Ordering::Relaxed);
            }
        })
    });

    // --- Independent committed writer: drives ckpt_max + GC. ---
    let writer_thread = enable_writer.then(|| {
        let db_arc = db.get_db();
        let stop = stop.clone();
        let mismatch = mismatch.clone();
        let writer_iters = writer_iters.clone();
        let next_id = next_id.clone();
        std::thread::spawn(move || {
            let conn = db_arc.connect().unwrap();
            let mut rng = ChaCha8Rng::seed_from_u64(0xDEADBEEF);
            while !stop.load(Ordering::Relaxed) && !mismatch.load(Ordering::Relaxed) {
                if conn.execute("BEGIN CONCURRENT").is_err() {
                    std::thread::yield_now();
                    continue;
                }
                let id = next_id.fetch_add(1, Ordering::Relaxed) as i64;
                let sql = if rng.random::<u8>() & 3 == 0 {
                    let target = (rng.random::<u32>() % 500) as i64;
                    format!("DELETE FROM t WHERE id = {target}")
                } else {
                    format!("INSERT INTO t VALUES ({id}, 'w_{id}', NULL)")
                };
                if conn.execute(&sql).is_err() {
                    let _ = conn.execute("ROLLBACK");
                    continue;
                }
                if conn.execute("COMMIT").is_ok() {
                    writer_iters.fetch_add(1, Ordering::Relaxed);
                }
            }
        })
    });

    // --- Checkpoint thread: drives drop_unused_row_versions. ---
    let ckpt_thread = enable_ckpt.then(|| {
        let db_arc = db.get_db();
        let stop = stop.clone();
        let mismatch = mismatch.clone();
        let ckpt_iters = ckpt_iters.clone();
        std::thread::spawn(move || {
            let conn = db_arc.connect().unwrap();
            let modes = ["PASSIVE", "FULL", "RESTART", "TRUNCATE"];
            let mut idx = 0usize;
            while !stop.load(Ordering::Relaxed) && !mismatch.load(Ordering::Relaxed) {
                let _ = conn.execute(format!(
                    "PRAGMA wal_checkpoint({})",
                    modes[idx % modes.len()]
                ));
                idx = idx.wrapping_add(1);
                ckpt_iters.fetch_add(1, Ordering::Relaxed);
            }
        })
    });

    // --- DDL thread: CREATE INDEX / DROP INDEX cycle (default OFF). ---
    let ddl_thread = enable_ddl.then(|| {
        let db_arc = db.get_db();
        let stop = stop.clone();
        let mismatch = mismatch.clone();
        let ddl_iters = ddl_iters.clone();
        std::thread::spawn(move || {
            let conn = db_arc.connect().unwrap();
            let mut i = 0u32;
            while !stop.load(Ordering::Relaxed) && !mismatch.load(Ordering::Relaxed) {
                let name = format!("idx_dyn_{}", i % 4);
                let _ = conn.execute(format!("CREATE INDEX {name} ON t(v)"));
                let _ = conn.execute(format!("DROP INDEX {name}"));
                i = i.wrapping_add(1);
                ddl_iters.fetch_add(1, Ordering::Relaxed);
            }
        })
    });

    let started = Instant::now();
    while started.elapsed() < duration && !mismatch.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(50));
    }
    stop.store(true, Ordering::Relaxed);

    reader.join().unwrap();
    if let Some(h) = sp_thread {
        h.join().unwrap();
    }
    if let Some(h) = writer_thread {
        h.join().unwrap();
    }
    if let Some(h) = ckpt_thread {
        h.join().unwrap();
    }
    if let Some(h) = ddl_thread {
        h.join().unwrap();
    }

    let r = reader_iters.load(Ordering::Relaxed);
    let rs = reader_samples.load(Ordering::Relaxed);
    let s = sp_iters.load(Ordering::Relaxed);
    let w = writer_iters.load(Ordering::Relaxed);
    let c = ckpt_iters.load(Ordering::Relaxed);
    let d = ddl_iters.load(Ordering::Relaxed);
    eprintln!(
        "reader_iters={r} reader_samples={rs} sp_iters={s} writer_iters={w} ckpt_iters={c} ddl_iters={d} elapsed={:?}",
        started.elapsed()
    );

    if mismatch.load(Ordering::Relaxed) {
        let a = mismatch_first.load(Ordering::Relaxed);
        let b = mismatch_second.load(Ordering::Relaxed);
        let ia = mismatch_idx_a.load(Ordering::Relaxed);
        let ib = mismatch_idx_b.load(Ordering::Relaxed);
        panic!(
            "snapshot count drifted within a single BEGIN CONCURRENT: \
             samples[{ia}]={a} samples[{ib}]={b} \
             (reader_iters={r}, sp_iters={s}, writer_iters={w}, ckpt_iters={c}, ddl_iters={d})"
        );
    }
    assert!(rs > 0, "reader made no progress");
}

#[test]
fn test_read_lock_leak_deferred_then_concurrent() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn0 = db.connect();
    conn0
        .execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn0.execute("INSERT INTO t1 VALUES(1, 'v1')").unwrap();
    conn0.close().unwrap();

    let conn1 = db.connect();
    conn1.execute("BEGIN DEFERRED").unwrap();
    // BEGIN CONCURRENT after BEGIN DEFERRED should error but not leak state
    let result = conn1.execute("BEGIN CONCURRENT");
    assert!(result.is_err());

    // After the error, SELECT should work without panicking
    let rows = get_rows(&conn1, "SELECT * FROM t1");
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_schema_change_succeeds_while_concurrent_writer_aborts_at_commit() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, c TEXT)")
        .unwrap();
    setup.execute("INSERT INTO t VALUES(1, 'a')").unwrap();
    setup.close().unwrap();

    let ddl = db.connect();
    let writer = db.connect();
    writer.execute("BEGIN CONCURRENT").unwrap();
    writer
        .execute("UPDATE t SET c = 'writer' WHERE id = 1")
        .unwrap();

    ddl.execute("ALTER TABLE t ADD COLUMN extra INTEGER")
        .unwrap();

    let commit_err = writer
        .execute("COMMIT")
        .expect_err("writer snapshot predates committed schema change");
    assert!(matches!(commit_err, LimboError::SchemaConflict));
    assert!(
        writer.get_auto_commit(),
        "SchemaConflict should roll back the stale writer transaction"
    );

    let verify = db.connect();
    let columns = get_rows(&verify, "PRAGMA table_info(t)");
    let column_names: Vec<String> = columns.iter().map(|row| row[1].to_string()).collect();
    assert_eq!(column_names, vec!["id", "c", "extra"]);
    let rows = get_rows(&verify, "SELECT id, c, extra FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1].to_string(), "a");
    assert!(matches!(&rows[0][2], crate::types::Value::Null));
}

#[test]
fn test_create_index_succeeds_while_concurrent_writer_aborts_at_commit() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, c TEXT, keep INTEGER)")
        .unwrap();
    setup.execute("INSERT INTO t VALUES(1, 'a', 10)").unwrap();
    setup.execute("INSERT INTO t VALUES(2, 'b', 20)").unwrap();
    setup.close().unwrap();

    let writer = db.connect();
    writer.execute("BEGIN CONCURRENT").unwrap();
    writer.execute("INSERT INTO t VALUES(3, 'c', 30)").unwrap();

    let ddl = db.connect();
    ddl.execute("CREATE INDEX idx_t_c ON t(c)").unwrap();

    let commit_err = writer
        .execute("COMMIT")
        .expect_err("writer snapshot predates committed schema change");
    assert!(matches!(commit_err, LimboError::SchemaConflict));
    assert!(writer.get_auto_commit());

    let verify = db.connect();
    let rows = get_rows(&verify, "SELECT id, c, keep FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    let indexes = get_rows(
        &verify,
        "SELECT name FROM sqlite_schema WHERE type = 'index' AND name = 'idx_t_c'",
    );
    assert_eq!(indexes.len(), 1);
    let rows = get_rows(&verify, "PRAGMA integrity_check");
    assert_eq!(rows[0][0].to_string(), "ok");
}

#[test]
fn test_exclusive_update_conflicts_with_concurrent_delete_without_replacing_marker() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, text TEXT)")
        .unwrap();
    setup
        .execute("INSERT INTO t VALUES(1, 'original')")
        .unwrap();
    setup.close().unwrap();

    let concurrent = db.connect();
    let exclusive = db.connect();
    concurrent.execute("BEGIN CONCURRENT").unwrap();
    concurrent.execute("DELETE FROM t WHERE id = 1").unwrap();

    let update_err = exclusive
        .execute("UPDATE t SET text = 'exclusive' WHERE id = 1")
        .expect_err("exclusive writer must not replace another transaction's delete marker");
    assert!(matches!(update_err, LimboError::WriteWriteConflict));
    assert!(exclusive.get_auto_commit());

    let rows = get_rows(&concurrent, "SELECT * FROM t");
    assert!(
        rows.is_empty(),
        "concurrent tx should still see its own delete"
    );
    concurrent.execute("COMMIT").unwrap();
    assert!(
        concurrent.get_auto_commit(),
        "concurrent transaction should remain usable after rejected exclusive write"
    );

    let rows = get_rows(&exclusive, "SELECT id, text FROM t");
    assert!(rows.is_empty());
}

#[test]
fn test_explicit_delete_conflicts_with_concurrent_delete_without_replacing_marker() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, text TEXT)")
        .unwrap();
    setup
        .execute("INSERT INTO t VALUES(1, 'original')")
        .unwrap();
    setup.execute("INSERT INTO t VALUES(2, 'keep')").unwrap();
    setup.close().unwrap();

    let concurrent = db.connect();
    let exclusive = db.connect();
    concurrent.execute("BEGIN CONCURRENT").unwrap();
    concurrent.execute("DELETE FROM t WHERE id = 1").unwrap();

    exclusive.execute("BEGIN").unwrap();
    let rows = get_rows(&exclusive, "SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    let delete_err = exclusive
        .execute("DELETE FROM t WHERE id = 1")
        .expect_err("exclusive delete must conflict instead of stealing row marker");
    assert!(matches!(delete_err, LimboError::WriteWriteConflict));
    assert!(exclusive.get_auto_commit());

    let rows = get_rows(&concurrent, "SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 2);

    concurrent.execute("COMMIT").unwrap();
    let rows = get_rows(&exclusive, "SELECT id, text FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 2);
}

/// Regression for #6754: dropping a Statement that paused mid-IO inside
/// op_new_rowid leaks the per-table RowidAllocator lock. With the Drop
/// impl on MvccLazyCursor, end_new_rowid runs on cursor teardown so the
/// next INSERT into the same table from any connection makes progress.
#[test]
fn rowid_allocator_lock_released_when_statement_dropped_at_seek_yield() {
    use std::time::{Duration, Instant};

    let db = MvccTestDbNoConn::new_with_random_db();

    let setup = db.connect();
    setup
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    setup.close().unwrap();

    let leaker = db.connect();
    let victim = db.connect();

    // Force the seek that runs from inside op_new_rowid's SeekingToLast
    // to yield IO at SeekStart. At that moment the rowid allocator lock
    // is held.
    leaker.set_yield_injector(Some(FixedYieldInjector::new([
        CursorYieldPoint::SeekStart.point()
    ])));

    let mut leak_stmt = leaker
        .prepare("INSERT INTO t VALUES (NULL, 'leaker')")
        .unwrap();
    match leak_stmt.step().unwrap() {
        crate::StepResult::Yield => {}
        other => panic!("expected yield from injected seek_start; got {other:?}"),
    }

    // Drop the statement without advancing past the yield. The Drop impl
    // on MvccLazyCursor must release the rowid allocator lock.
    drop(leak_stmt);
    leaker.set_yield_injector(None);

    // A different connection must now be able to INSERT into the same
    // table within a small budget.
    let mut victim_stmt = victim
        .prepare("INSERT INTO t VALUES (NULL, 'victim')")
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if Instant::now() >= deadline {
            panic!("victim INSERT did not complete within 5s — rowid allocator lock leaked");
        }
        match victim_stmt.step().unwrap() {
            crate::StepResult::Done => break,
            crate::StepResult::IO | crate::StepResult::Yield => continue,
            other => panic!("unexpected step result on victim INSERT: {other:?}"),
        }
    }
}

// https://github.com/tursodatabase/turso/issues/6752
#[test]
fn exclusive_commit_failure_at_after_remove_tx_strands_exclusive_atom() {
    let db = MvccTestDbNoConn::new();
    let conn_a = db.connect();
    let conn_b = db.connect();

    conn_a
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn_a.execute("BEGIN IMMEDIATE").unwrap();
    conn_a.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    conn_a.set_failure_injector(Some(FixedFailureInjector::new([(
        CommitYieldPoint::AfterRemoveTx.point(),
        LimboError::TxError("synthetic AfterRemoveTx failure".to_string()),
    )])));

    conn_a
        .execute("COMMIT")
        .expect_err("COMMIT must surface the injected TxError");

    conn_b.execute("BEGIN CONCURRENT").unwrap();
    conn_b.execute("INSERT INTO t VALUES (2, 'b')").unwrap();

    let mut commit_b = conn_b.prepare("COMMIT").unwrap();
    let step_result = loop {
        match commit_b.step() {
            Ok(StepResult::IO | StepResult::Yield) => continue,
            other => break other,
        }
    };

    match step_result {
        Ok(StepResult::Done) => {}
        Ok(other) => panic!("stage 3: unexpected step result: {other:?}"),
        Err(err) => panic!("INSERT after failed commit must not return error, got {err}"),
    }
}

/// Regression for #6757: a Statement driving a CONCURRENT `COMMIT` that
/// yields at `LogRecordPrepared` and is then dropped used to leave the tx
/// in `Preparing`. The next statement on that connection would trip a
/// `turso_assert_eq!(Active)` in `read_from_table_or_index` (process
/// panic). The abort-side `cleanup_abandoned_mvcc_commit` hook now rolls
/// back the orphan tx so a follow-up INSERT works against a fresh tx.
#[test]
fn dropped_concurrent_commit_does_not_strand_connection() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    setup.close().unwrap();

    let conn = db.connect();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    let mv_store = db.get_mvcc_store();
    let tx_id = conn
        .get_mv_tx_id()
        .expect("tx must be open after INSERT inside BEGIN CONCURRENT");

    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));
    {
        let mut commit = conn.prepare("COMMIT").unwrap();
        match commit.step().unwrap() {
            crate::StepResult::Yield => {}
            other => panic!("expected yield at LogRecordPrepared; got {other:?}"),
        }
    }
    conn.set_yield_injector(None);

    // Abort hook ran cleanup_abandoned_mvcc_commit → rollback_tx → tx is
    // gone from `txs`, connection's mv_tx slot is cleared, AND
    // transaction_state is reset to None so the next op_transaction takes
    // the fresh-tx path instead of inheriting stale Write state.
    assert!(
        !mv_store.txs.contains_key(&tx_id),
        "orphan tx must be rolled back by abort-side cleanup"
    );
    assert!(
        conn.get_mv_tx_id().is_none(),
        "connection's mv_tx slot must be cleared"
    );
    assert_eq!(
        conn.get_tx_state(),
        crate::connection::TransactionState::None,
        "transaction_state must be reset after abort-side rollback"
    );

    // The next op must not panic on the would-be-Preparing tx — it should
    // start a fresh autocommit tx instead.
    conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    let rows = get_rows(&conn, "SELECT id FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 2);
}

/// Regression for #6755: dropping a Statement driving an EXCLUSIVE
/// (BEGIN IMMEDIATE) COMMIT at `LogRecordPrepared` used to leak both
/// `pager_commit_lock` and the `exclusive_tx` atomic. With abort-side
/// `cleanup_abandoned_mvcc_commit` calling `rollback_tx`, both are
/// released and a second connection's BEGIN IMMEDIATE makes progress.
#[test]
fn dropped_exclusive_commit_releases_locks() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    setup.close().unwrap();

    let conn_a = db.connect();
    let conn_b = db.connect();

    conn_a.execute("BEGIN IMMEDIATE").unwrap();
    conn_a.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    let mv_store = db.get_mvcc_store();
    let tx_a = conn_a
        .get_mv_tx_id()
        .expect("EXCLUSIVE tx_a must be open after INSERT");
    assert!(
        mv_store.is_exclusive_tx(&tx_a),
        "EXCLUSIVE tx_a must own the exclusive_tx atomic"
    );

    conn_a.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));
    {
        let mut commit = conn_a.prepare("COMMIT").unwrap();
        match commit.step().unwrap() {
            crate::StepResult::Yield => {}
            other => panic!("expected yield at LogRecordPrepared; got {other:?}"),
        }
    }
    conn_a.set_yield_injector(None);

    // After abort, exclusive_tx must be released.
    assert!(
        !mv_store.is_exclusive_tx(&tx_a),
        "exclusive_tx must be released by abort-side cleanup"
    );

    // conn_b must be able to take the exclusive lock.
    conn_b.execute("BEGIN IMMEDIATE").unwrap();
    conn_b.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    conn_b.execute("COMMIT").unwrap();

    let rows = get_rows(&conn_b, "SELECT id FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 2);
}

/// Regression for abandoned COMMIT cleanup with attached MVCC databases.
/// Dropping the COMMIT while the main-db CommitStateMachine is paused must
/// also roll back attached MVCC txs opened by the same SQL transaction.
#[test]
fn dropped_main_commit_rolls_back_attached_mvcc_txs() {
    let db = MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new().with_attach(true));
    let aux_dir = tempfile::TempDir::new().unwrap();
    let aux_path = aux_dir.path().join("aux.db");

    let conn = db.connect();
    drive_attach(&conn, aux_path.to_str().unwrap(), "aux");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE aux.u (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'main')").unwrap();
    conn.execute("INSERT INTO aux.u VALUES (1, 'aux')").unwrap();

    let aux_db_id = conn.get_database_id_by_name("aux").unwrap();
    let aux_mv_store = conn
        .mv_store_for_db(aux_db_id)
        .expect("attached aux database must be MVCC");
    let aux_pager = conn.get_pager_from_database_index(&aux_db_id).unwrap();
    let aux_tx_id = conn
        .get_mv_tx_id_for_db(aux_db_id)
        .expect("attached MVCC tx must be open after INSERT");

    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));
    {
        let mut commit = conn.prepare("COMMIT").unwrap();
        match commit.step().unwrap() {
            crate::StepResult::Yield => {}
            other => panic!("expected yield at LogRecordPrepared; got {other:?}"),
        }
    }
    conn.set_yield_injector(None);

    assert!(
        conn.get_mv_tx_id_for_db(aux_db_id).is_none(),
        "attached MVCC tx slot must be cleared when abandoned COMMIT is rolled back"
    );
    assert!(
        !aux_mv_store.txs.contains_key(&aux_tx_id),
        "attached MVCC tx must be removed from txs"
    );
    assert!(
        !aux_pager.holds_read_lock(),
        "attached pager read lock must be released"
    );

    let rows = get_rows(&conn, "SELECT id FROM aux.u ORDER BY id");
    assert!(
        rows.is_empty(),
        "abandoned attached INSERT must not become visible"
    );
}

#[test]
fn dropped_main_commit_rolls_back_temp_schema_changes() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'main')").unwrap();
    conn.execute("CREATE TEMP TABLE temp_only (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX temp_only_v_idx ON temp_only(v)")
        .unwrap();

    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));
    {
        let mut commit = conn.prepare("COMMIT").unwrap();
        match commit.step().unwrap() {
            crate::StepResult::Yield => {}
            other => panic!("expected yield at LogRecordPrepared; got {other:?}"),
        }
    }
    conn.set_yield_injector(None);

    let temp_err = match conn.prepare("SELECT * FROM temp_only") {
        Ok(_) => {
            panic!("abandoned COMMIT must roll back temp schema created inside the transaction")
        }
        Err(err) => err,
    };
    assert!(
        temp_err.to_string().contains("no such table"),
        "expected rolled-back temp table to be absent, got {temp_err}",
    );
    let rows = get_rows(&conn, "SELECT id FROM t ORDER BY id");
    assert!(
        rows.is_empty(),
        "abandoned COMMIT must roll back main MVCC writes too",
    );
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Regression for abandoning COMMIT after it has advanced from the main
/// MVCC phase into an attached MVCC CommitStateMachine.
#[test]
fn dropped_attached_commit_releases_attached_read_lock() {
    let db = MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new().with_attach(true));
    let aux_dir = tempfile::TempDir::new().unwrap();
    let aux_path = aux_dir.path().join("aux.db");

    let conn = db.connect();
    drive_attach(&conn, aux_path.to_str().unwrap(), "aux");
    conn.execute("CREATE TABLE aux.u (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO aux.u VALUES (1, 'aux')").unwrap();

    let aux_db_id = conn.get_database_id_by_name("aux").unwrap();
    let aux_mv_store = conn
        .mv_store_for_db(aux_db_id)
        .expect("attached aux database must be MVCC");
    let aux_pager = conn.get_pager_from_database_index(&aux_db_id).unwrap();
    let aux_tx_id = conn
        .get_mv_tx_id_for_db(aux_db_id)
        .expect("attached MVCC tx must be open after INSERT");

    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));
    {
        let mut commit = conn.prepare("COMMIT").unwrap();
        match commit.step().unwrap() {
            crate::StepResult::Yield => {}
            other => panic!("expected yield at attached LogRecordPrepared; got {other:?}"),
        }
    }
    conn.set_yield_injector(None);

    assert!(
        conn.get_mv_tx_id_for_db(aux_db_id).is_none(),
        "attached MVCC tx slot must be cleared"
    );
    assert!(
        !aux_mv_store.txs.contains_key(&aux_tx_id),
        "attached MVCC tx must be removed from txs"
    );
    assert!(
        !aux_pager.holds_read_lock(),
        "attached pager read lock must be released"
    );
}

/// Regression for abandoning COMMIT while one attached MVCC database is
/// paused mid-commit and another attached MVCC transaction is still pending.
#[test]
fn dropped_attached_commit_rolls_back_remaining_attached_mvcc_txs() {
    let db = MvccTestDbNoConn::new_with_random_db_with_opts(DatabaseOpts::new().with_attach(true));
    let aux_dir = tempfile::TempDir::new().unwrap();
    let aux1_path = aux_dir.path().join("aux1.db");
    let aux2_path = aux_dir.path().join("aux2.db");

    let conn = db.connect();
    drive_attach(&conn, aux1_path.to_str().unwrap(), "aux1");
    drive_attach(&conn, aux2_path.to_str().unwrap(), "aux2");
    conn.execute("CREATE TABLE aux1.u (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE aux2.v (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO aux1.u VALUES (1, 'aux1')")
        .unwrap();
    conn.execute("INSERT INTO aux2.v VALUES (1, 'aux2')")
        .unwrap();

    let aux1_db_id = conn.get_database_id_by_name("aux1").unwrap();
    let aux2_db_id = conn.get_database_id_by_name("aux2").unwrap();
    let aux1_mv_store = conn
        .mv_store_for_db(aux1_db_id)
        .expect("attached aux1 database must be MVCC");
    let aux2_mv_store = conn
        .mv_store_for_db(aux2_db_id)
        .expect("attached aux2 database must be MVCC");
    let aux1_pager = conn.get_pager_from_database_index(&aux1_db_id).unwrap();
    let aux2_pager = conn.get_pager_from_database_index(&aux2_db_id).unwrap();
    let aux1_tx_id = conn
        .get_mv_tx_id_for_db(aux1_db_id)
        .expect("attached aux1 MVCC tx must be open after INSERT");
    let aux2_tx_id = conn
        .get_mv_tx_id_for_db(aux2_db_id)
        .expect("attached aux2 MVCC tx must be open after INSERT");

    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));
    {
        let mut commit = conn.prepare("COMMIT").unwrap();
        match commit.step().unwrap() {
            crate::StepResult::Yield => {}
            other => panic!("expected yield at attached LogRecordPrepared; got {other:?}"),
        }
    }
    conn.set_yield_injector(None);

    assert!(
        conn.get_mv_tx_id_for_db(aux1_db_id).is_none(),
        "attached aux1 MVCC tx slot must be cleared"
    );
    assert!(
        conn.get_mv_tx_id_for_db(aux2_db_id).is_none(),
        "attached aux2 MVCC tx slot must be cleared"
    );
    assert!(
        !aux1_mv_store.txs.contains_key(&aux1_tx_id),
        "attached aux1 MVCC tx must be removed from txs"
    );
    assert!(
        !aux2_mv_store.txs.contains_key(&aux2_tx_id),
        "attached aux2 MVCC tx must be removed from txs"
    );
    assert!(
        !aux1_pager.holds_read_lock(),
        "attached aux1 pager read lock must be released"
    );
    assert!(
        !aux2_pager.holds_read_lock(),
        "attached aux2 pager read lock must be released"
    );
}

/// DurableStorage::log_tx returning Busy should not leak pager_commit_lock.
/// https://github.com/tursodatabase/turso/issues/6753.
#[test]
fn busy_from_log_tx_strands_pager_commit_lock_then_blocks_subsequent_commit() {
    use crate::io::FileSyncType;
    use crate::mvcc;
    use crate::mvcc::database::{LogRecord, RowVersion};
    use crate::mvcc::persistent_storage::logical_log::{LogHeader, OnSerializationComplete};
    use crate::mvcc::persistent_storage::DurableStorage;
    use crate::storage::encryption::EncryptionContext;
    use crate::storage::sqlite3_ondisk::DatabaseHeader;
    use crate::{CheckpointResult, File, Result, IO};
    use std::time::Duration;

    /// BusyOnLogTxStorage is a test double that can be stubbed to return [LimboError::Busy] from log_tx.
    #[derive(Debug)]
    struct BusyOnLogTxStorage {
        inner: Arc<dyn DurableStorage>,
        arm_log_tx_busy: AtomicBool,
    }
    impl BusyOnLogTxStorage {
        fn new(inner: Arc<dyn DurableStorage>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                arm_log_tx_busy: AtomicBool::new(false),
            })
        }
        fn arm(&self) {
            self.arm_log_tx_busy.store(true, Ordering::Release);
        }
    }
    impl DurableStorage for BusyOnLogTxStorage {
        fn serialize_row_version(
            &self,
            log_record: &mut LogRecord,
            row_version: &RowVersion,
            portable_extension: Option<&[u8]>,
        ) -> Result<()> {
            self.inner
                .serialize_row_version(log_record, row_version, portable_extension)
        }
        fn serialize_database_header(
            &self,
            log_record: &mut LogRecord,
            header: &DatabaseHeader,
        ) -> Result<()> {
            self.inner.serialize_database_header(log_record, header)
        }
        fn log_tx(
            &self,
            m: LogRecord,
            c: OnSerializationComplete<'_>,
        ) -> Result<(Completion, u64)> {
            if self.arm_log_tx_busy.swap(false, Ordering::AcqRel) {
                return Err(LimboError::Busy);
            }
            self.inner.log_tx(m, c)
        }
        fn upgrade_header_for_log_tx(&self, m: &LogRecord) -> Result<Option<Completion>> {
            self.inner.upgrade_header_for_log_tx(m)
        }
        fn sync(&self, t: FileSyncType) -> Result<Completion> {
            self.inner.sync(t)
        }
        fn update_header(&self) -> Result<Completion> {
            self.inner.update_header()
        }
        fn truncate(
            &self,
            checkpointed_through_ts: u64,
        ) -> Result<(
            Completion,
            crate::mvcc::persistent_storage::LogicalLogTruncateOutcome,
        )> {
            self.inner.truncate(checkpointed_through_ts)
        }
        fn reset_to_fresh_header(&self) -> Result<Completion> {
            self.inner.reset_to_fresh_header()
        }
        fn get_logical_log_file(&self) -> Arc<dyn File> {
            self.inner.get_logical_log_file()
        }
        fn logical_log_offset(&self) -> u64 {
            self.inner.logical_log_offset()
        }
        fn should_checkpoint(&self) -> bool {
            self.inner.should_checkpoint()
        }
        fn set_checkpoint_threshold(&self, t: i64) {
            self.inner.set_checkpoint_threshold(t)
        }
        fn checkpoint_threshold(&self) -> i64 {
            self.inner.checkpoint_threshold()
        }
        fn advance_logical_log_offset_after_success(&self, b: u64) -> Result<()> {
            self.inner.advance_logical_log_offset_after_success(b)
        }
        fn discard_pending_log_write(&self) -> Result<()> {
            self.inner.discard_pending_log_write()
        }
        fn restore_logical_log_state_after_recovery(&self, o: u64, c: u32) {
            self.inner.restore_logical_log_state_after_recovery(o, c)
        }
        fn set_header(&self, h: LogHeader) {
            self.inner.set_header(h)
        }
        fn on_checkpoint_start(&self) -> Result<()> {
            self.inner.on_checkpoint_start()
        }
        fn on_checkpoint_end(&self, r: Result<&CheckpointResult>) -> Result<()> {
            self.inner.on_checkpoint_end(r)
        }
        fn encryption_ctx(&self) -> Option<EncryptionContext> {
            self.inner.encryption_ctx()
        }
    }

    fn drive_to_done_or_timeout(stmt: &mut Statement, budget: usize) {
        for _ in 0..budget {
            match stmt.step() {
                Ok(StepResult::Done) => return,
                Ok(StepResult::IO | StepResult::Yield) => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                Ok(other) => panic!("unexpected step: {other:?}"),
                Err(error) => panic!("received error: {error}"),
            }
        }
        panic!("budged elapsed: {budget} iterations");
    }

    // Step 1: open normally so PRAGMA journal_mode=mvcc creates the logical log.
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir
        .path()
        .join(format!("test_{}.db", rand::random::<u64>()));
    let path_str = path.to_str().unwrap().to_string();
    {
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            &path_str,
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
        DATABASE_MANAGER.lock().clear();
    }

    // Step 3: re-open with the busy-on-log_tx storage wrapper.
    let log_path = path.with_extension("db-log");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let log_file = io
        .open_file(log_path.to_str().unwrap(), OpenFlags::default(), false)
        .unwrap();
    let inner_storage: Arc<dyn DurableStorage> = Arc::new(mvcc::persistent_storage::Storage::new(
        log_file,
        io.clone(),
        None,
    ));
    let busy_storage = BusyOnLogTxStorage::new(inner_storage);
    let db = Database::open(
        io,
        &path_str,
        crate::OpenOptions::new(Arc::new(SqliteDialect))
            .durable_storage(busy_storage.clone() as Arc<dyn DurableStorage>),
    )
    .unwrap();

    let conn_a = db.connect().unwrap();
    let conn_b = db.connect().unwrap();
    conn_a
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let mv_store: Arc<crate::MvStore> = db.get_mv_store().clone().unwrap();

    // Step 3: open a CONCURRENT tx, do an INSERT, then arm log_tx Busy.
    conn_a.execute("BEGIN CONCURRENT").unwrap();
    conn_a.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let tx_a = conn_a
        .get_mv_tx_id()
        .expect("tx_a must be open after INSERT");
    assert!(
        !mv_store.is_exclusive_tx(&tx_a),
        "tx_a must be CONCURRENT (non-exclusive) so it goes through BeginCommitLogicalLog"
    );
    busy_storage.arm();

    conn_a
        .execute("COMMIT")
        .expect_err("COMMIT must surface the injected Busy from log_tx");

    // Step 4: from another CONCURRENT tx, do an INSERT; the INSERT should go through.
    conn_b.execute("BEGIN CONCURRENT").unwrap();
    conn_b.execute("INSERT INTO t VALUES (2, 'b')").unwrap();

    let mut commit_b = conn_b.prepare("COMMIT").unwrap();
    drive_to_done_or_timeout(&mut commit_b, 30); // this times out if pager_commit_lock is leaked
}

// https://github.com/tursodatabase/turso/issues/6757
#[test]
fn test_dropped_commit_corrupts_subsequent_insert() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.close().unwrap();
    }

    let conn = db.connect();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'first')").unwrap();
    conn.set_yield_injector(Some(FixedYieldInjector::new([LogRecordPrepared.point()])));

    {
        let mut commit = conn.prepare("COMMIT").unwrap();
        match commit.step().unwrap() {
            StepResult::IO | StepResult::Yield | StepResult::Done => {}
            other => panic!("unexpected step result: {other:?}"),
        };
    }

    conn.execute("INSERT INTO t VALUES (2, 'second')").unwrap();
}

// https://github.com/tursodatabase/turso/issues/6755
#[test]
fn abandoned_exclusive_commit_should_not_block_subsequent_concurrent_writer() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn_a = db.connect();
    let conn_b = db.connect();

    conn_a
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn_a.execute("BEGIN IMMEDIATE").unwrap();
    conn_a.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    conn_a.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));

    match conn_a.prepare("COMMIT").unwrap().step().unwrap() {
        StepResult::Yield => {} // tx will immediately hit injected yield point
        other => panic!("tx should yield, got: {other:?}"),
    }

    assert!(
        matches!(conn_a.prepare("COMMIT").unwrap().step().err().unwrap(),
            LimboError::TxError(msg) if msg == "cannot commit - no transaction is active")
    );

    conn_b.execute("BEGIN CONCURRENT").unwrap();
    conn_b.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    match conn_b.prepare("COMMIT").unwrap().step() {
        Ok(StepResult::IO) => {}
        Err(err) => panic!("conn_b COMMIT must not error; got: {err:?}"),
        _ => {}
    }
}

// https://github.com/tursodatabase/turso/issues/6751
#[test]
fn abandoned_commit_in_committed_state_should_not_block_subsequent_checkpoint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn_a = db.connect();
    let conn_b = db.connect();

    conn_a
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn_a.execute("BEGIN IMMEDIATE").unwrap();
    conn_a.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    conn_a.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::BeforeFinishCommittedTx.point(),
    ])));

    match conn_a.prepare("COMMIT").unwrap().step().unwrap() {
        StepResult::Yield => {}
        other => panic!("tx should yield, got: {other:?}"),
    }

    let _ = conn_a.prepare("COMMIT").unwrap().step();

    conn_b.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
}

/// A concurrent explicit rowid insert must raise the allocator watermark before
/// another transaction performs auto-rowid allocation. Otherwise later auto
/// inserts can overwrite the explicit row and leave secondary indexes stale.
#[test]
fn test_concurrent_explicit_rowid_high_watermark_not_clobbered() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn0 = db.connect();
    let conn1 = db.connect();
    let conn2 = db.connect();

    conn0
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn1
        .execute("INSERT INTO t(id, v) VALUES (1000, 'A-explicit')")
        .unwrap();

    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("INSERT INTO t(v) VALUES ('B-auto')").unwrap();
    conn2.execute("COMMIT").unwrap();
    conn1.execute("COMMIT").unwrap();

    let rows = get_rows(&conn0, "SELECT rowid, v FROM t ORDER BY rowid");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1000);
    assert_eq!(rows[0][1].to_string(), "A-explicit");
    assert_eq!(rows[1][0].as_int().unwrap(), 1001);
    assert_eq!(rows[1][1].to_string(), "B-auto");
}

#[test]
fn test_concurrent_explicit_rowid_auto_rowid_does_not_walk_back_into_collision() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn0 = db.connect();
    let conn1 = db.connect();
    let conn2 = db.connect();

    conn0
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn1
        .execute("INSERT INTO t(id, v) VALUES (5, 'A')")
        .unwrap();

    conn2.execute("BEGIN CONCURRENT").unwrap();
    for i in 0..5 {
        conn2
            .execute(format!("INSERT INTO t(v) VALUES ('B{i}')"))
            .unwrap();
    }
    conn2.execute("COMMIT").unwrap();
    conn1
        .execute("COMMIT")
        .expect("explicit rowid transaction should not conflict with auto rowids");

    let rows = get_rows(&conn0, "SELECT rowid, v FROM t ORDER BY rowid");
    assert_eq!(rows.len(), 6);
    assert_eq!(rows[0][0].as_int().unwrap(), 5);
    assert_eq!(rows[0][1].to_string(), "A");
    for i in 0..5 {
        assert_eq!(rows[i + 1][0].as_int().unwrap(), 6 + i as i64);
        assert_eq!(rows[i + 1][1].to_string(), format!("B{i}"));
    }
}

#[test]
fn test_concurrent_explicit_rowid_preserves_auto_rowid_watermark() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn1 = db.connect();
    let conn2 = db.connect();

    conn1
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, k INTEGER)")
        .unwrap();
    conn1.execute("CREATE INDEX t_k ON t(k)").unwrap();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn1
        .execute("INSERT INTO t(id, v, k) VALUES (5, 'A', 999)")
        .unwrap();

    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2
        .execute("INSERT INTO t(v, k) VALUES ('B', 100)")
        .unwrap();
    conn2.execute("COMMIT").unwrap();
    conn1.execute("COMMIT").unwrap();

    for (v, k) in [("p2", 200), ("p3", 300), ("p4", 400), ("p5", 500)] {
        conn1
            .execute(format!("INSERT INTO t(v, k) VALUES ('{v}', {k})"))
            .unwrap();
    }

    let integrity = get_rows(&conn1, "PRAGMA integrity_check");
    assert_eq!(integrity.len(), 1);
    assert_eq!(
        integrity[0][0].to_string(),
        "ok",
        "integrity_check should not report stale secondary index entries"
    );

    let indexed = get_rows(
        &conn1,
        "SELECT rowid, v, k FROM t INDEXED BY t_k WHERE k = 999",
    );
    assert_eq!(indexed.len(), 1);
    assert_eq!(indexed[0][0].as_int().unwrap(), 5);
    assert_eq!(indexed[0][1].to_string(), "A");
    assert_eq!(indexed[0][2].as_int().unwrap(), 999);
}

#[test]
fn test_auto_rowid_after_negative_explicit_rowid_uses_next_negative() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t(id, v) VALUES(-5, 'manual')")
        .unwrap();
    conn.execute("INSERT INTO t(v) VALUES('auto')").unwrap();

    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), -5);
    assert_eq!(rows[0][1].to_string(), "manual");
    assert_eq!(rows[1][0].as_int().unwrap(), -4);
    assert_eq!(rows[1][1].to_string(), "auto");
}
/// What this test checks: CREATE SEQUENCE + DROP SEQUENCE between checkpoints must not
/// crash the checkpoint when it tries to delete the sqlite_schema row from the B-tree.
///
/// Why this matters: Sequence schema rows have type="sequence" and rootpage=0, so they
/// are not recognized by `sqlite_schema_btree_identity()`. Without a fix, the checkpoint
/// adds them to the write_set via the `is_schema_delete` path (for tracking destroyed
/// tables), but the WriteRow handler then tries to B-tree-delete a row that was never
/// checkpointed, causing "MVCC delete: rowid N not found".
#[test]
fn test_checkpoint_after_create_and_drop_sequence() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE SEQUENCE seq1").unwrap();
    conn.execute("DROP SEQUENCE seq1").unwrap();

    // This checkpoint should not crash. The sqlite_schema row for seq1 was
    // created and deleted without an intervening checkpoint, so it does not
    // exist in the B-tree.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// A passive checkpoint must not panic when collecting or writing a user-data tombstone
/// for a table whose B-tree was destroyed in a prior checkpoint (e.g. DROP SEQUENCE after
/// the backing table was materialized).
#[test]
fn test_passive_checkpoint_skips_late_tombstone_after_prior_destroy() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mv_store = db.get_db().get_mv_store().clone().unwrap();
    let rootpage = get_rows(
        &conn,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 't'",
    )[0][0]
        .as_int()
        .unwrap();
    let table_id = mv_store.get_table_id_from_root_page(rootpage);

    conn.execute("DROP TABLE t").unwrap();
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    let tx = conn.get_mv_tx_id().unwrap();
    mv_store
        .delete(tx, RowID::new(table_id, RowKey::Int(1)))
        .unwrap();
    conn.execute("COMMIT").unwrap();

    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Descending sequence compaction must keep the most-advanced (lowest) value.
///
/// A descending sequence (INCREMENT BY -1, START WITH 100) produces values 100, 99, 98...
/// In MVCC mode, each commit appends a new sqlite_sequence row. On checkpoint, compaction
/// should keep the minimum (most advanced for descending) and delete the rest.
/// After restart, nextval should resume from the most advanced value.
#[test]
fn test_descending_sequence_compaction() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    // Create an autoincrement table to force sqlite_sequence table creation.
    conn.execute("CREATE TABLE dummy(id INTEGER PRIMARY KEY AUTOINCREMENT)")
        .unwrap();
    conn.execute("CREATE SEQUENCE desc_seq START WITH 100 INCREMENT BY -1 MINVALUE 1 MAXVALUE 100")
        .unwrap();

    // Call nextval 5 times across separate transactions.
    // Descending: produces 100, 99, 98, 97, 96
    for _ in 0..5 {
        conn.execute("BEGIN CONCURRENT").unwrap();
        let rows = get_rows(&conn, "SELECT nextval('desc_seq')");
        assert_eq!(rows.len(), 1);
        conn.execute("COMMIT").unwrap();
    }

    // Verify last nextval returned 96
    let rows = get_rows(&conn, "SELECT nextval('desc_seq')");
    let last_val = rows[0][0].as_int().unwrap();
    assert_eq!(last_val, 95, "6th call should return 95");

    // Checkpoint → compaction runs. Should keep the most advanced (lowest) value.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");

    // After compaction, the backing table should have exactly 1 row with the current value
    let rows = get_rows(&conn, "SELECT value FROM __turso_internal_seq_desc_seq");
    assert_eq!(rows.len(), 1, "compaction should leave exactly 1 row");
    let compacted_val = rows[0][0].as_int().unwrap();
    assert_eq!(
        compacted_val, 95,
        "compaction should keep the most advanced (lowest) value for descending"
    );

    // Close and restart the database
    conn.close().unwrap();
    db.restart();
    let conn = db.connect();

    // After restart, nextval should resume from the most advanced value (95)
    let rows = get_rows(&conn, "SELECT nextval('desc_seq')");
    let resumed_val = rows[0][0].as_int().unwrap();
    assert_eq!(
        resumed_val, 94,
        "after restart, descending seq should resume from most advanced value"
    );
}

/// Autoincrement in an ATTACH'd MVCC database must persist across checkpoint + restart.
///
/// Before the fix, dirty_sequences lacked a database_id so flush always targeted main,
/// and the checkpoint compaction only read main's schema — values silently reset on restart.
#[test]
fn test_autoincrement_in_attached_mvcc_database() {
    let _ = tracing_subscriber::fmt().try_init();
    let opts = DatabaseOpts::new().with_attach(true);
    let mut db = MvccTestDbNoConn::new_with_random_db_with_opts(opts);

    // Create a second temp file for the attached database.
    let aux_dir = tempfile::TempDir::new().unwrap();
    let aux_path = aux_dir
        .path()
        .join(format!("aux_{}.db", rand::random::<u64>()));
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    // Phase 1: attach, create table, insert, checkpoint
    {
        let conn = db.connect();
        conn.execute(format!("ATTACH '{aux_path_str}' AS aux"))
            .unwrap();
        conn.execute("PRAGMA aux.journal_mode = 'experimental_mvcc'")
            .unwrap();
        conn.execute("CREATE TABLE aux.t(id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO aux.t(val) VALUES ('a')").unwrap();
        conn.execute("INSERT INTO aux.t(val) VALUES ('b')").unwrap();
        conn.execute("INSERT INTO aux.t(val) VALUES ('c')").unwrap();

        // Checkpoint the attached db
        conn.execute("PRAGMA aux.wal_checkpoint(TRUNCATE)").unwrap();

        conn.close().unwrap();
    }

    // Phase 2: restart main, re-attach, insert — id must be 4
    drop(db.db.take());
    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }
    db.restart();

    {
        let conn = db.connect();
        conn.execute(format!("ATTACH '{aux_path_str}' AS aux"))
            .unwrap();
        conn.execute("PRAGMA aux.journal_mode = 'experimental_mvcc'")
            .unwrap();

        conn.execute("INSERT INTO aux.t(val) VALUES ('d')").unwrap();
        let rows = get_rows(&conn, "SELECT MAX(id) FROM aux.t");
        let max_id = rows[0][0].as_int().unwrap();
        assert_eq!(
            max_id, 4,
            "after restart, next autoincrement id must be 4, got {max_id}"
        );
    }
}

/// Explicit sequences in an ATTACH'd MVCC database must persist across checkpoint + restart.
#[test]
fn test_create_sequence_in_attached_mvcc_database() {
    let _ = tracing_subscriber::fmt().try_init();
    let opts = DatabaseOpts::new().with_attach(true);
    let mut db = MvccTestDbNoConn::new_with_random_db_with_opts(opts);

    let aux_dir = tempfile::TempDir::new().unwrap();
    let aux_path = aux_dir
        .path()
        .join(format!("aux_{}.db", rand::random::<u64>()));
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    // Phase 1: attach, create sequence, advance it, checkpoint
    {
        let conn = db.connect();
        conn.execute(format!("ATTACH '{aux_path_str}' AS aux"))
            .unwrap();
        conn.execute("PRAGMA aux.journal_mode = 'experimental_mvcc'")
            .unwrap();

        // Create an autoincrement table in aux to ensure sqlite_sequence exists
        conn.execute("CREATE TABLE aux.dummy(id INTEGER PRIMARY KEY AUTOINCREMENT)")
            .unwrap();

        conn.execute("CREATE SEQUENCE aux.my_seq").unwrap();

        // Advance the sequence 3 times
        for _ in 0..3 {
            conn.execute("BEGIN CONCURRENT").unwrap();
            let rows = get_rows(&conn, "SELECT nextval('aux.my_seq')");
            assert_eq!(rows.len(), 1);
            conn.execute("COMMIT").unwrap();
        }

        // Checkpoint aux
        conn.execute("PRAGMA aux.wal_checkpoint(TRUNCATE)").unwrap();

        conn.close().unwrap();
    }

    // Phase 2: restart, re-attach, nextval must resume from 4
    drop(db.db.take());
    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
    }
    db.restart();

    {
        let conn = db.connect();
        conn.execute(format!("ATTACH '{aux_path_str}' AS aux"))
            .unwrap();
        conn.execute("PRAGMA aux.journal_mode = 'experimental_mvcc'")
            .unwrap();

        let rows = get_rows(&conn, "SELECT nextval('aux.my_seq')");
        let val = rows[0][0].as_int().unwrap();
        assert_eq!(
            val, 4,
            "after restart, nextval should resume from 4, got {val}"
        );
    }
}

/// Regression: a non-CYCLE `nextval` issued inside a `BEGIN CONCURRENT`
/// transaction must never drive `op_sequence_commit_inner_tx` to retry.
/// The autonomous inner tx is only allowed to absorb conflicts that
/// are *inherent* to two concurrent allocations of the same sequence
/// value (PK collision on the new watermark row). Any additional
/// contention surface — most notably inline backing-table compaction
/// (Delete of the prior watermark row that concurrent allocators also
/// touch) — is a design regression and is forbidden on this hot path.
///
/// Autocommit nextval is uninteresting here: `begin_write_on_database`
/// opens an *exclusive* outer tx, so `op_sequence_begin_inner_tx`
/// takes the `SEQ_PATH_SKIPPED` branch and no inner tx ever wraps.
/// Concurrent execution is only possible via explicit
/// `BEGIN CONCURRENT`, which is the path this test exercises.
///
/// Staging: tx A begins concurrent and runs `nextval`, which wraps in
/// an autonomous inner tx. We pin that inner tx's commit at a yield
/// point so its writes are still uncommitted. Tx B then runs a full
/// autocommit `nextval` end-to-end (it gets the watermark before A
/// because A's writes are invisible). When A resumes, its inner-tx
/// commit must complete without retry: the only allowed retry source
/// (new-value PK collision) is avoided because B's commit has already
/// advanced disk past A's chosen target.
///
/// `Connection::sequence_inner_retries` is the cross-statement
/// observability hook (see its doc): assert it is zero across the
/// scenario.
#[test]
fn test_nextval_no_inner_tx_retry_on_concurrent_mvcc() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let setup = db.connect();
        setup.execute("CREATE SEQUENCE s START WITH 1").unwrap();
        // Prime the backing table so the first observed allocation is
        // not the special start-row overwrite (which doesn't exercise
        // the compaction Delete). After this, the table is in the
        // "1 historical row + 1 new row per nextval" steady state.
        for _ in 0..3 {
            setup.execute("SELECT nextval('s')").unwrap();
        }
        setup.close().unwrap();
    }

    let conn_a = db.connect();
    let conn_b = db.connect();
    conn_a.reset_sequence_inner_retries();
    conn_b.reset_sequence_inner_retries();

    // A: BEGIN CONCURRENT so the outer tx is Concurrent, not exclusive.
    // The next `nextval` will wrap in an autonomous inner Concurrent tx
    // (path = SEQ_PATH_WRAPPED). That inner tx is the one whose commit
    // path must never retry.
    conn_a.execute("BEGIN CONCURRENT").unwrap();

    // Pin A AFTER its inner-tx commit publishes the new watermark, but
    // before the post-commit cleanup that removes the tx from `txs`.
    // At this point A's row is visible to any snapshot taken later —
    // so B's BEGIN CONCURRENT below sees A's K+1 and picks K+2 instead
    // of K+1, sidestepping the unavoidable "two readers see same MAX,
    // both target MAX+1" PK collision. With that collision removed,
    // any retry observed is from contention on a *shared* row written
    // by the nextval path (the canary the test exists to enforce).
    let injector = FixedYieldInjector::new([CommitYieldPoint::BeforeFinishCommittedTx.point()]);
    conn_a.set_yield_injector(Some(injector.clone()));
    let mut next_a = conn_a.prepare("SELECT nextval('s')").unwrap();
    // Drive past real cursor-read IOs (page cache misses on the backing
    // table) and stop precisely when the injector has fired. `nextval`
    // emits many cursor instructions before the inner-tx commit, so a
    // "break on first StepResult::IO" loop would exit on a real IO
    // rather than the synthetic injected yield. The FixedYieldInjector
    // consumes its entry on the first matching `should_yield` call, so
    // `is_empty()` after a step is the unambiguous signal that the
    // inject point was reached.
    let injected = loop {
        match next_a.step().unwrap() {
            StepResult::IO | StepResult::Yield => {
                if injector.is_empty() {
                    break true;
                }
                conn_a.pager.load().io.step().unwrap();
            }
            StepResult::Done | StepResult::Row => break false,
            other => panic!(
                "unexpected step result while driving A's nextval to its \
                 inner-tx commit yield: {other:?}"
            ),
        }
    };
    assert!(
        injected,
        "A's nextval inner-tx commit should yield at \
         BeforeFinishCommittedTx; injector did not fire — yield-point \
         lineup likely shifted."
    );

    // B also goes Concurrent (an autocommit nextval would try to open an
    // exclusive tx and busy out against A's in-flight Concurrent inner
    // tx). B's snapshot does not see A's not-yet-committed inner-tx
    // writes, so B inserts watermark+1 in its own inner tx. With the
    // design invariant (no hot-path writes of shared rows in nextval),
    // B's nextval and commit both succeed cleanly. If anything in this
    // sequence fails with WriteWriteConflict, it is the canary: A and
    // B touched a shared row — almost certainly the prior watermark
    // that inline backing-table compaction deletes.
    conn_b.execute("BEGIN CONCURRENT").unwrap();
    let b_nextval = conn_b.execute("SELECT nextval('s')");
    assert!(
        !matches!(b_nextval, Err(LimboError::WriteWriteConflict)),
        "B's `SELECT nextval('s')` returned WriteWriteConflict against \
         A's parked inner tx. Two concurrent nextvals on a non-CYCLE \
         seq must not share a written row — this is the canary for \
         inline backing-table compaction or any other hot-path write \
         of a shared row. See PR #7137: inline compaction is not \
         allowed."
    );
    b_nextval.unwrap();
    let b_commit = conn_b.execute("COMMIT");
    assert!(
        !matches!(b_commit, Err(LimboError::WriteWriteConflict)),
        "B's COMMIT returned WriteWriteConflict against A's parked \
         inner tx. Same canary as the nextval assertion above — \
         a shared written row exists somewhere on the nextval path."
    );
    b_commit.unwrap();

    // Resume A's inner-tx commit. Any retry here is a regression — the
    // test's whole point is that the inner-tx commit path is free of
    // contended-write surfaces beyond the (here-avoided) new-watermark
    // PK.
    conn_a.set_yield_injector(None);
    let a_finish = next_a.run_collect_rows();
    drop(next_a);
    assert!(
        !matches!(a_finish, Err(LimboError::WriteWriteConflict)),
        "A's resumed inner-tx commit returned WriteWriteConflict — \
         same canary as B above; the inner tx wrote a row B's commit \
         already touched. See PR #7137."
    );
    a_finish.unwrap();
    conn_a.execute("COMMIT").unwrap();

    let a_retries = conn_a.sequence_inner_retries();
    let b_retries = conn_b.sequence_inner_retries();
    assert_eq!(
        a_retries, 0,
        "A's inner tx retried — `Connection::sequence_inner_retries` \
         is the canary for inline backing-table compaction or any \
         other hot-path write of a shared row. See PR #7137: inline \
         compaction is not allowed."
    );
    assert_eq!(
        b_retries, 0,
        "B's inner tx retried — same canary as A. See PR #7137."
    );
}

#[test]
fn test_sequence_write_conflict_rolls_back_outer_tx_and_rejects_commit() {
    let db = MvccTestDbNoConn::new();
    let setup = db.connect();
    setup.execute("CREATE TABLE t(a INTEGER)").unwrap();
    setup.execute("CREATE SEQUENCE s").unwrap();
    setup.execute("INSERT INTO t VALUES (1)").unwrap();

    let nextval_conn = db.connect();
    let setval_conn = db.connect();

    nextval_conn.execute("BEGIN CONCURRENT").unwrap();
    setval_conn.execute("BEGIN CONCURRENT").unwrap();
    nextval_conn.execute("DELETE FROM t WHERE TRUE").unwrap();

    let setval_rows = get_rows(&setval_conn, "SELECT setval('s', 100, true)");
    assert_eq!(setval_rows[0][0].as_int().unwrap(), 100);

    let nextval = nextval_conn.execute("SELECT nextval('s')");
    assert!(
        matches!(nextval, Err(LimboError::WriteWriteConflict)),
        "conflicting nextval must abort with WriteWriteConflict, got {nextval:?}"
    );

    setval_conn.execute("COMMIT").unwrap();

    assert!(
        nextval_conn.get_auto_commit(),
        "write-write conflict must leave the losing connection in autocommit"
    );
    assert_eq!(
        nextval_conn.get_mv_tx_id(),
        None,
        "write-write conflict must clear the losing connection's MVCC tx"
    );

    let commit = nextval_conn.execute("COMMIT");
    let expected = "cannot commit - no transaction is active";
    assert!(
        matches!(commit, Err(LimboError::TxError(ref msg)) if msg == expected),
        "COMMIT after conflict rollback must report no active transaction, got {commit:?}"
    );

    let rows = get_rows(&setup, "SELECT count(*) FROM t");
    assert_eq!(
        rows[0][0].as_int().unwrap(),
        1,
        "the DELETE from the rolled-back transaction must not persist"
    );
}

/// Regression: a multi-row `INSERT INTO autoinc_table VALUES (...), (...)`
/// inside `BEGIN CONCURRENT` whose second-row nextval exhausts the
/// AUTOINCREMENT sequence must leave NO partial row committed — even
/// though the autonomous inner-tx pattern means the first row's
/// nextval already committed its sequence advance independently of the
/// outer tx.
///
/// The bug Nikita reported on PR #7137 (2026-05-26): after setting the
/// AUTOINCREMENT seq one step below i64::MAX and trying to insert two
/// rows, the first row's table insert leaked through to the post-COMMIT
/// state despite the statement returning `DatabaseFull`. Root cause was
/// twofold:
///
///   * `set_insert_stmt_journal_flags` did not flag AUTOINCREMENT inserts
///     as `may_abort`, so the statement-level MVCC savepoint was never
///     opened and the outer tx's table-row write had nothing to roll back
///     against on error.
///   * The vdbe abort path called `end_statement(RollbackSavepoint)` via
///     `connection.get_mv_tx_id()`, but that returned the still-pending
///     autonomous inner-tx id (the inner tx began but never reached
///     `op_sequence_commit_inner_tx`). The savepoint rollback ran against
///     the wrong tx and left the outer tx's row in its write_set.
///
/// Both halves of the fix must hold for this test to pass.
#[test]
fn test_multi_row_autoincrement_insert_atomic_on_sequence_exhaustion() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE autoinc(x INTEGER PRIMARY KEY AUTOINCREMENT, y)")
        .unwrap();
    setup
        .execute(
            "SELECT setval('__turso_internal_autoincrement_autoinc', \
             9223372036854775807 - 1)",
        )
        .unwrap();
    setup.close().unwrap();

    let conn = db.connect();
    conn.execute("BEGIN CONCURRENT").unwrap();
    // Second row's nextval exhausts the seq → DatabaseFull. The first
    // row's table write must be rolled back at the statement level.
    let insert = conn.execute("INSERT INTO autoinc(y) VALUES (1), (2)");
    assert!(
        matches!(insert, Err(LimboError::DatabaseFull(_))),
        "expected DatabaseFull on the second nextval, got {insert:?}"
    );
    conn.execute("COMMIT").unwrap();

    let rows = get_rows(&conn, "SELECT x, y FROM autoinc");
    assert!(
        rows.is_empty(),
        "INSERT VALUES (1), (2) errored mid-statement on sequence exhaustion; \
         per-statement atomicity requires zero rows to land in the table, but \
         saw {} row(s): {rows:?}",
        rows.len(),
    );
}

/// Companion to `test_multi_row_autoincrement_insert_atomic_on_sequence_exhaustion`:
/// when a `BEGIN CONCURRENT` block runs several *single-statement*
/// INSERTs into the same AUTOINCREMENT table, each statement is either
/// fully committed or fully rolled back (no partial state). Earlier
/// successful statements must NOT be undone by a later statement's
/// sequence-exhaustion failure — only the failing statement is.
///
/// Setup arranges the seq with exactly two values remaining: the third
/// INSERT exhausts. After COMMIT, the first two successful inserts
/// must survive and the failed third must contribute nothing.
///
/// Uses a single-column AUTOINCREMENT table so every INSERT is purely
/// a sequence-driven row allocation — there is no other column whose
/// value could mask a per-row write surviving its statement's
/// rollback.
#[test]
fn test_per_statement_atomicity_across_multi_statement_autoincrement_tx() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE only_id(x INTEGER PRIMARY KEY AUTOINCREMENT)")
        .unwrap();
    // Leave room for exactly two more emissions: nextval will yield
    // MAX-1 and MAX, then the third call returns DatabaseFull.
    setup
        .execute(
            "SELECT setval('__turso_internal_autoincrement_only_id', \
             9223372036854775807 - 2)",
        )
        .unwrap();
    setup.close().unwrap();

    let conn = db.connect();
    conn.execute("BEGIN CONCURRENT").unwrap();

    // First two single-statement INSERTs must succeed end-to-end.
    let r1 = conn.execute("INSERT INTO only_id DEFAULT VALUES");
    assert!(r1.is_ok(), "first INSERT must succeed, got {r1:?}");
    let r2 = conn.execute("INSERT INTO only_id DEFAULT VALUES");
    assert!(r2.is_ok(), "second INSERT must succeed, got {r2:?}");

    // The third statement exhausts the seq mid-flight and must fail.
    let r3 = conn.execute("INSERT INTO only_id DEFAULT VALUES");
    assert!(
        matches!(r3, Err(LimboError::DatabaseFull(_))),
        "third INSERT must exhaust the seq and return DatabaseFull, got {r3:?}"
    );

    // The outer tx is still alive — its first two successful statements
    // stay in the write_set, the failing third contributed nothing,
    // and COMMIT publishes exactly two rows.
    conn.execute("COMMIT").unwrap();

    let rows = get_rows(&conn, "SELECT x FROM only_id ORDER BY x");
    assert_eq!(
        rows.len(),
        2,
        "two committed inserts must survive a later failing statement; \
         got {} row(s): {rows:?}",
        rows.len(),
    );
    assert_eq!(
        rows[0][0].as_int().unwrap(),
        9223372036854775806,
        "first surviving row id"
    );
    assert_eq!(
        rows[1][0].as_int().unwrap(),
        9223372036854775807,
        "second surviving row id"
    );
}

/// Regression: out-of-order MVCC commit finalization must not let an older
/// transaction replace `global_header` with a stale header.
///
/// tx_a is paused in FinalizeCommit after it has been marked Committed but
/// before publishing its header/watermark. tx_b then commits newer DDL and
/// publishes a bumped schema cookie. When tx_a resumes, its older header must
/// not move `global_header.schema_cookie` backward.
#[test]
fn test_global_header_cookie_no_regression_on_out_of_order_finalize() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let setup = db.connect();
        setup
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        setup
            .execute("INSERT INTO t VALUES (1, 'initial')")
            .unwrap();
        setup.close().unwrap();
    }
    let mvcc_store = db.get_mvcc_store();
    let cookie_before = mvcc_store
        .with_header(|h| h.schema_cookie.get(), None)
        .unwrap();

    let conn_a = db.connect();
    let conn_b = db.connect();

    // tx_a: CONCURRENT update. Pin its commit inside FinalizeCommit, after
    // CommitEnd has already marked the tx Committed but before the
    // watermark / global_header writes. tx_a is no longer Preparing at the
    // yield, so `acquire_exclusive_tx`'s `has_preparing_tx_other_than` check
    // lets tx_b take the slot.
    conn_a.execute("BEGIN CONCURRENT").unwrap();
    conn_a
        .execute("UPDATE t SET v = 'a-mod' WHERE id = 1")
        .unwrap();
    let tx_a_id = conn_a.get_mv_tx_id().expect("tx_a should be active");

    conn_a.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::BeforeGlobalHeaderUpdate.point(),
    ])));
    let mut commit_a = conn_a.prepare("COMMIT").unwrap();
    let mut yielded = false;
    for _ in 0..200 {
        match commit_a.step().unwrap() {
            StepResult::Yield => {
                yielded = true;
                break;
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(
        yielded,
        "tx_a's COMMIT should yield before publishing global_header"
    );
    assert!(
        matches!(
            mvcc_store
                .txs
                .get(&tx_a_id)
                .expect("tx_a should be tracked")
                .value()
                .state
                .load(),
            TransactionState::Committed(_)
        ),
        "tx_a should be Committed (set by CommitEnd) by the time we yield in FinalizeCommit"
    );

    // tx_b: exclusive DDL. tx_a is Committed so `acquire_exclusive_tx`
    // does not see a Preparing other-than. tx_b runs end-to-end and its
    // FinalizeCommit writes the bumped cookie into global_header.
    conn_b.execute("BEGIN").unwrap();
    conn_b.execute("CREATE TABLE foo(x INTEGER)").unwrap();
    conn_b.execute("COMMIT").unwrap();
    let cookie_after_b = mvcc_store
        .with_header(|h| h.schema_cookie.get(), None)
        .unwrap();
    assert!(
        cookie_after_b > cookie_before,
        "tx_b's CREATE TABLE should bump global_header.schema_cookie \
         (before={cookie_before} after_b={cookie_after_b})"
    );

    // Resume tx_a. Its FinalizeCommit's global_header write must not
    // overwrite tx_b's newer cookie.
    conn_a.set_yield_injector(None);
    commit_a.run_ignore_rows().unwrap();
    drop(commit_a);

    let cookie_final = mvcc_store
        .with_header(|h| h.schema_cookie.get(), None)
        .unwrap();
    assert_eq!(
        cookie_final, cookie_after_b,
        "global_header.schema_cookie regressed after older tx_a finalized \
         after newer DDL tx_b — before={cookie_before} after_b={cookie_after_b} \
         final={cookie_final}"
    );
}

/// Regression: the same stale `global_header` overwrite can lose user-visible
/// database-header state, not just regress the internal schema cookie.
///
/// `PRAGMA user_version` is committed through the MVCC header path. If an older
/// transaction resumes after that newer header-only commit and overwrites
/// `global_header` with its stale header snapshot, users observe the committed
/// user_version move backward.
#[test]
fn test_global_header_regression_would_lose_committed_user_version() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let setup = db.connect();
        setup
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        setup.execute("PRAGMA user_version = 7").unwrap();
        setup
            .execute("INSERT INTO t VALUES (1, 'initial')")
            .unwrap();
        setup.close().unwrap();
    }

    let older = db.connect();
    let header_writer = db.connect();
    let observer = db.connect();

    older.execute("BEGIN CONCURRENT").unwrap();
    older
        .execute("UPDATE t SET v = 'older' WHERE id = 1")
        .unwrap();
    older.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::BeforeGlobalHeaderUpdate.point(),
    ])));
    let mut older_commit = older.prepare("COMMIT").unwrap();
    let mut yielded_older = false;
    for _ in 0..200 {
        match older_commit.step().unwrap() {
            StepResult::Yield => {
                yielded_older = true;
                break;
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(
        yielded_older,
        "older COMMIT should yield before publishing global_header"
    );

    header_writer.execute("BEGIN").unwrap();
    header_writer.execute("PRAGMA user_version = 42").unwrap();
    header_writer.execute("COMMIT").unwrap();

    let rows = get_rows(&observer, "PRAGMA user_version");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0].as_int().unwrap(),
        42,
        "newer header-only commit should publish user_version before older resumes"
    );

    older.set_yield_injector(None);
    older_commit.run_ignore_rows().unwrap();
    drop(older_commit);

    let rows = get_rows(&observer, "PRAGMA user_version");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0].as_int().unwrap(),
        42,
        "older out-of-order FinalizeCommit regressed committed PRAGMA user_version"
    );
}

#[test]
fn test_create_index_exclusive_acquire_rechecks_timestamp_after_cas() {
    let db = MvccTestDbNoConn::new_with_random_db();

    let setup = db.connect();
    setup
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    setup.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    setup.close().unwrap();

    let writer = db.connect();
    writer.execute("BEGIN CONCURRENT").unwrap();
    writer.execute("INSERT INTO t VALUES (2, 200)").unwrap();

    let ddl = db.connect();
    ddl.execute("BEGIN DEFERRED").unwrap();
    let mut read = ddl.prepare("SELECT COUNT(*) FROM t").unwrap();
    read.run_ignore_rows().unwrap();
    drop(read);

    let mvcc_store = db.get_mvcc_store();
    let ddl_tx_id = ddl
        .get_mv_tx_id()
        .expect("DDL connection should have an active MVCC transaction");
    let ddl_begin_ts = mvcc_store
        .txs
        .get(&ddl_tx_id)
        .expect("DDL transaction should be tracked")
        .value()
        .begin_ts;
    assert!(
        ddl_begin_ts >= mvcc_store.last_committed_tx_ts.load(Ordering::Acquire),
        "pre-CAS timestamp check should pass before the injected writer commit"
    );

    let injector = CommitWriterOnExclusiveAcquireInjector::new(
        ExclusiveTxYieldPoint::AfterTimestampCheckBeforeCas.point(),
        ddl_tx_id,
        writer,
    );
    ddl.set_yield_injector(Some(injector.clone()));

    let result = ddl.execute("CREATE INDEX idx_v ON t(v)");
    assert!(
        injector.fired(),
        "writer should commit in the exclusive-acquire CAS window"
    );
    assert!(
        matches!(result, Err(crate::LimboError::Busy)),
        "stale DDL transaction should release exclusive and return Busy after post-CAS recheck: {result:?}"
    );
    assert!(
        !mvcc_store.is_exclusive_tx(&ddl_tx_id),
        "failed stale DDL should not keep the exclusive slot"
    );
    ddl.set_yield_injector(None);
    if ddl.get_mv_tx_id().is_some() {
        ddl.execute("ROLLBACK").unwrap();
    }

    let observer = db.connect();
    observer.execute("CREATE INDEX idx_v ON t(v)").unwrap();
    observer.execute("DELETE FROM t WHERE id = 2").unwrap();

    let integrity = get_rows(&observer, "PRAGMA integrity_check");
    assert_eq!(integrity.len(), 1);
    assert_eq!(integrity[0][0].to_string(), "ok");
}

/// Contract test for [`crate::StepResult::Yield`]: an explicit yield (here an
/// injected one) must surface as `Yield`, not as `IO` — there are no pending
/// completions to drive, so the caller can simply step the statement again,
/// possibly after doing other work first.
#[test]
fn injected_yield_surfaces_as_step_result_yield() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));

    let mut commit = conn.prepare("COMMIT").unwrap();
    let first = commit.step().unwrap();
    assert!(
        matches!(first, crate::StepResult::Yield),
        "injected yield must surface as StepResult::Yield, got {first:?}"
    );
    conn.set_yield_injector(None);

    // After a yield the statement just resumes at the same point.
    commit.run_ignore_rows().unwrap();

    let rows = get_rows(&conn, "SELECT id, v FROM t");
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_checkpoint_seek_skip_divider_reinsert_loses_row() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    // Same shape as the stress table hot_floor_424: tiny rows, rowid-alias PK.
    conn.execute("CREATE TABLE t (v REAL NOT NULL, pk INTEGER PRIMARY KEY)")
        .unwrap();
    // Bulk-load in ONE transaction => one checkpoint pass bulk-inserts ascending and
    // splits the btree, creating dividers (~every 250 rows at this record size).
    conn.execute("BEGIN").unwrap();
    for i in 0..1000 {
        conn.execute(format!("INSERT INTO t VALUES ({}.5, {})", i % 7, i))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    conn.execute("BEGIN").unwrap();
    for i in (0..1000).filter(|i| i % 5 == 3) {
        conn.execute(format!("DELETE FROM t WHERE pk = {i}"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    for g in 1..400i64 {
        conn.execute(format!("DELETE FROM t WHERE pk = {g}"))
            .unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute(format!("INSERT OR REPLACE INTO t VALUES (1.25, {})", g - 1))
            .unwrap();
        conn.execute(format!("INSERT INTO t VALUES (2.5, {g})"))
            .unwrap();
        conn.execute("COMMIT").unwrap();
        let rows = get_rows(&conn, &format!("SELECT pk FROM t WHERE pk = {g}"));
        assert_eq!(
            rows.len(),
            1,
            "rowid {g} vanished from point lookup after re-insert \
             (checkpoint seek-skip wrote it on the wrong side of the divider)"
        );
    }
}

/// Regression test for https://github.com/tursodatabase/turso/issues/7477.
///
/// A large committed DELETE whose commit statement is dropped mid-flight
/// (after `LogRecordPrepared`, before finishing tombstone TxID rewriting)
/// must not leave tombstones pointing at the removed TxID; otherwise a
/// later writer panics with
/// "check_version_conflicts: tombstone end TxID not found in txn map".
#[test]
fn mvcc_bug_repro_dropped_committed_delete_rewrites_all_tombstone_txids() {
    let db = MvccTestDbNoConn::new_with_random_db();

    let setup = db.connect();
    setup
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let n_rows = MVCC_COMMIT_BATCH_SIZE + 476;
    let values = (1..=n_rows)
        .map(|i| format!("({i}, 'v{i}')"))
        .collect::<Vec<_>>()
        .join(", ");

    setup
        .execute(format!("INSERT INTO t(id, v) VALUES {values}"))
        .unwrap();

    setup.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    setup.close().unwrap();

    let conn_a = db.connect();
    conn_a.execute("BEGIN CONCURRENT").unwrap();
    conn_a.execute("DELETE FROM t").unwrap();

    let log_record_prepared =
        FixedYieldInjector::new([CommitYieldPoint::LogRecordPrepared.point()]);
    conn_a.set_yield_injector(Some(log_record_prepared.clone()));

    let mut commit_a = conn_a.prepare("COMMIT").unwrap();

    for _ in 0..10_000 {
        match commit_a.step().unwrap() {
            StepResult::IO | StepResult::Yield if log_record_prepared.is_empty() => break,
            StepResult::IO | StepResult::Yield => {}
            StepResult::Done => panic!("COMMIT completed before LogRecordPrepared yielded"),
            other => panic!("unexpected COMMIT result before LogRecordPrepared: {other:?}"),
        }
    }

    conn_a.set_yield_injector(None);

    match commit_a.step().unwrap() {
        StepResult::IO | StepResult::Yield => {}
        StepResult::Done => panic!("COMMIT completed before RewriteLiveVersions yielded"),
        other => panic!("unexpected COMMIT result after LogRecordPrepared: {other:?}"),
    }

    drop(commit_a);

    let conn_b = db.connect();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        conn_b.execute("BEGIN CONCURRENT").unwrap();
        conn_b
            .execute(format!(
                "INSERT INTO t(id, v) VALUES ({n_rows}, 'replacement')"
            ))
            .unwrap();
        conn_b.execute("COMMIT")
    }));

    assert!(
        result.is_ok(),
        "later public writer must not panic on a stale removed tombstone TxID"
    );

    result
        .unwrap()
        .expect("later public writer must not conflict on a stale removed tombstone TxID");
}

/// Creates and drops a checkpointed table so the following schema changes reuse
/// physical roots and expose a root mapping left behind by a failed checkpoint.
fn prepare_recycled_root_pages_for_failed_checkpoint(conn: &Arc<Connection>) {
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("CREATE TABLE old(id INTEGER PRIMARY KEY, x TEXT, b BLOB)")
        .unwrap();
    conn.execute(
        "INSERT INTO old
         SELECT value, 'old' || value, zeroblob(1000)
           FROM generate_series(1, 50)",
    )
    .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.execute("DROP TABLE old").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
}

fn expect_database_full_checkpoint(result: crate::Result<()>) {
    let error = result.expect_err("checkpoint must fail when max_page_count is exhausted");
    assert!(
        error.to_string().contains("Database is full"),
        "checkpoint must report that the database is full, got {error:?}"
    );
}

fn assert_table_row(conn: &Arc<Connection>, table: &str, id: i64, expected_x: Option<&str>) {
    let rows = get_rows(conn, &format!("SELECT id, x FROM {table} WHERE id = {id}"));
    match expected_x {
        Some(expected_x) => {
            assert_eq!(
                rows.len(),
                1,
                "expected {table} to contain id {id}, got {rows:?}"
            );
            assert_eq!(rows[0][0].as_int().unwrap(), id);
            assert_eq!(rows[0][1].to_string(), expected_x);
        }
        None => assert!(
            rows.is_empty(),
            "expected {table} not to contain id {id}, got {rows:?}"
        ),
    }
}

fn assert_table_counts(conn: &Arc<Connection>, expected_t: i64, expected_u: i64) {
    let rows = get_rows(
        conn,
        "SELECT (SELECT count(*) FROM t), (SELECT count(*) FROM u)",
    );
    assert_eq!(rows.len(), 1, "count query returned {rows:?}");
    assert_eq!(rows[0][0].as_int().unwrap(), expected_t, "t row count");
    assert_eq!(rows[0][1].as_int().unwrap(), expected_u, "u row count");
}

/// Regression test for https://github.com/tursodatabase/turso/issues/7642.
///
/// A checkpoint that hits `max_page_count` must not leave the connection with
/// a physical root mapping for a page that was never written to the database.
#[test]
fn test_read_after_database_full_checkpoint_remains_usable() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("PRAGMA max_page_count = 5").unwrap();
    conn.execute("CREATE TABLE t(x TEXT)").unwrap();
    conn.execute(
        "INSERT INTO t
         SELECT printf('%.*c', 1800, 'x')
           FROM generate_series(1, 60)",
    )
    .unwrap();

    expect_database_full_checkpoint(conn.execute("PRAGMA wal_checkpoint(TRUNCATE)"));

    let mut read = conn.prepare("SELECT count(*) FROM t").unwrap();
    let rows = read
        .run_collect_rows()
        .expect("read after failed checkpoint must not short-read an unwritten root page");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 60);
}

/// Regression test for https://github.com/tursodatabase/turso/issues/7642.
///
/// A failed automatic checkpoint must not persist an UPDATE against a recycled
/// root belonging to a different table when the database is reopened.
#[test]
fn test_auto_checkpoint_update_stays_with_source_table_after_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        prepare_recycled_root_pages_for_failed_checkpoint(&conn);
        conn.execute("PRAGMA max_page_count = 6").unwrap();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT, b BLOB)")
            .unwrap();
        conn.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT, b BLOB)")
            .unwrap();
        conn.execute(
            "INSERT INTO t
             SELECT value, 't' || value, zeroblob(1800)
               FROM generate_series(1, 60)",
        )
        .unwrap();
        conn.execute("INSERT INTO t VALUES(1000, 't_old', zeroblob(1800))")
            .unwrap();
        conn.execute("INSERT INTO u VALUES(1000, 'u_old', zeroblob(1800))")
            .unwrap();
        conn.execute("COMMIT").unwrap();
        conn.execute("UPDATE t SET x = 't_updated_by_auto_case' WHERE id = 1000")
            .unwrap();

        assert_table_row(&conn, "t", 1000, Some("t_updated_by_auto_case"));
        assert_table_row(&conn, "u", 1000, Some("u_old"));
    }

    db.restart();
    let conn = db.connect();
    assert_table_row(&conn, "t", 1000, Some("t_updated_by_auto_case"));
    assert_table_row(&conn, "u", 1000, Some("u_old"));
    assert_table_counts(&conn, 61, 1);
    assert_integrity_ok(&conn);
}

/// Regression test for https://github.com/tursodatabase/turso/issues/7642.
///
/// A DELETE issued after a failed checkpoint must remain attached to its
/// source table both before and after recovery.
#[test]
fn test_delete_after_failed_checkpoint_stays_with_source_table() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        prepare_recycled_root_pages_for_failed_checkpoint(&conn);
        conn.execute("PRAGMA max_page_count = 6").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT, b BLOB)")
            .unwrap();
        conn.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT, b BLOB)")
            .unwrap();
        conn.execute(
            "INSERT INTO t
             SELECT value, 't' || value, zeroblob(1800)
               FROM generate_series(1, 60)",
        )
        .unwrap();
        conn.execute("INSERT INTO t VALUES(1000, 't_victim', zeroblob(1800))")
            .unwrap();
        conn.execute("INSERT INTO u VALUES(1000, 'u_victim', zeroblob(1800))")
            .unwrap();

        expect_database_full_checkpoint(conn.execute("PRAGMA wal_checkpoint(TRUNCATE)"));
        conn.execute("DELETE FROM t WHERE id = 1000").unwrap();

        assert_table_row(&conn, "t", 1000, None);
        assert_table_row(&conn, "u", 1000, Some("u_victim"));
        assert_table_counts(&conn, 60, 1);
    }

    db.restart();
    let conn = db.connect();
    assert_table_row(&conn, "t", 1000, None);
    assert_table_row(&conn, "u", 1000, Some("u_victim"));
    assert_table_counts(&conn, 60, 1);
    assert_integrity_ok(&conn);
}

/// Regression test for https://github.com/tursodatabase/turso/issues/7642.
///
/// A failed checkpoint on one connection must not poison the shared root map
/// used by a subsequent writer connection.
#[test]
fn test_failed_checkpoint_does_not_move_other_connection_insert() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn1 = db.connect();
        let conn2 = db.connect();
        prepare_recycled_root_pages_for_failed_checkpoint(&conn1);
        conn1.execute("PRAGMA max_page_count = 6").unwrap();
        conn1
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT, b BLOB)")
            .unwrap();
        conn1
            .execute("CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT, b BLOB)")
            .unwrap();
        conn1
            .execute(
                "INSERT INTO t
                 SELECT value, 't' || value, zeroblob(1800)
                   FROM generate_series(1, 60)",
            )
            .unwrap();

        expect_database_full_checkpoint(conn1.execute("PRAGMA wal_checkpoint(TRUNCATE)"));
        conn2
            .execute("INSERT INTO t VALUES(1000, 'conn2_after_leak', zeroblob(1800))")
            .unwrap();

        assert_table_row(&conn2, "t", 1000, Some("conn2_after_leak"));
        assert_table_row(&conn2, "u", 1000, None);
        assert_table_counts(&conn2, 61, 0);
    }

    db.restart();
    let conn = db.connect();
    assert_table_row(&conn, "t", 1000, Some("conn2_after_leak"));
    assert_table_row(&conn, "u", 1000, None);
    assert_table_counts(&conn, 61, 0);
    assert_integrity_ok(&conn);
}

/// Regression test for https://github.com/tursodatabase/turso/issues/7642.
///
/// Replaying a row against the wrong table must not create a table/index
/// split-brain that only becomes visible after a later successful checkpoint.
#[test]
fn test_failed_checkpoint_preserves_secondary_index_consistency() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        prepare_recycled_root_pages_for_failed_checkpoint(&conn);
        conn.execute("PRAGMA max_page_count = 6").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT, b BLOB)")
            .unwrap();
        conn.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT, b BLOB)")
            .unwrap();
        conn.execute("CREATE INDEX ux ON u(x)").unwrap();
        conn.execute(
            "INSERT INTO t
             SELECT value, 't' || value, zeroblob(1800)
               FROM generate_series(1, 60)",
        )
        .unwrap();

        expect_database_full_checkpoint(conn.execute("PRAGMA wal_checkpoint(TRUNCATE)"));
        conn.execute("INSERT INTO t VALUES(1000, 'missing_u_index', zeroblob(1800))")
            .unwrap();
    }

    db.restart();
    let conn = db.connect();
    let indexed = get_rows(
        &conn,
        "SELECT id, x FROM u INDEXED BY ux WHERE x = 'missing_u_index'",
    );
    let scanned = get_rows(
        &conn,
        "SELECT id, x FROM u NOT INDEXED WHERE x = 'missing_u_index'",
    );
    assert_eq!(
        indexed, scanned,
        "secondary-index lookup and table scan must agree after recovery"
    );
    assert!(
        scanned.is_empty(),
        "a row inserted into t must not recover under u: {scanned:?}"
    );
    assert_table_row(&conn, "t", 1000, Some("missing_u_index"));
    assert_table_row(&conn, "u", 1000, None);
    assert_integrity_ok(&conn);

    conn.execute("PRAGMA max_page_count = 1000").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    assert_integrity_ok(&conn);
}

/// Concurrent DROP of a checkpointed table during a parked passive checkpoint must not panic.
#[test]
fn test_passive_checkpoint_truncate_wal_tolerates_concurrent_drop_of_checkpointed_table() {
    use crate::StepResult;
    let _ = tracing_subscriber::fmt::try_init();
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let conn_keep = db.connect();
    conn_keep
        .execute("CREATE TABLE keep(x INTEGER PRIMARY KEY)")
        .unwrap();
    conn_keep.execute("INSERT INTO keep VALUES (1)").unwrap();
    conn_keep
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    let keep_root = get_rows(
        &conn_keep,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 'keep'",
    )[0][0]
        .as_int()
        .unwrap();
    assert!(
        keep_root > 0,
        "keep must be checkpointed (positive root) for the DROP to record it, got {keep_root}"
    );
    let conn_c = db.connect();
    conn_c
        .execute("CREATE TABLE other(y INTEGER PRIMARY KEY)")
        .unwrap();
    conn_c
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    let injector =
        FixedYieldInjector::new([CheckpointYieldPoint::AfterDurableBoundaryAdvanced.point()]);
    conn_c.set_yield_injector(Some(injector.clone()));
    let mut checkpoint = conn_c.prepare("INSERT INTO other VALUES (1)").unwrap();
    let pager_io = conn_c.pager.load().io.clone();
    let step_to_next_yield = |checkpoint: &mut crate::Statement, expect_remaining: usize| {
        for _ in 0..200_000 {
            match checkpoint.step().unwrap() {
                StepResult::IO | StepResult::Yield => {
                    if injector.remaining_len() == expect_remaining {
                        return true;
                    }
                    pager_io.step().unwrap();
                }
                StepResult::Done => return false,
                other => panic!("unexpected checkpoint step: {other:?}"),
            }
        }
        false
    };
    assert!(
        step_to_next_yield(&mut checkpoint, 0),
        "passive checkpoint must yield at AfterDurableBoundaryAdvanced (publish window done)"
    );
    assert!(
        conn_c.db.schema.lock().dropped_root_pages.is_empty(),
        "parked checkpoint should have published its own pages; live set must be clean"
    );
    let conn_d = db.connect();
    conn_d.execute("DROP TABLE keep").unwrap();
    assert!(
        conn_c
            .db
            .schema
            .lock()
            .dropped_root_pages
            .contains(&keep_root),
        "concurrent DROP must record keep's root in the live shared dropped_root_pages"
    );
    for _ in 0..200_000 {
        match checkpoint.step().unwrap() {
            StepResult::Done => break,
            StepResult::IO | StepResult::Yield => pager_io.step().unwrap(),
            other => panic!("unexpected checkpoint step after resume: {other:?}"),
        }
    }
    assert_integrity_ok(&conn_c);
}

/// Repro for https://github.com/tursodatabase/turso/issues/7956.
///
/// A passive checkpoint must materialize CREATE at its snapshot, but publishing
/// the positive rootpage must not clobber a concurrent DROP's end stamp on that
/// schema version (which would resurrect the table).
#[test]
fn test_passive_checkpoint_preserves_drop_committed_after_collection() {
    use crate::StepResult;

    let _ = tracing_subscriber::fmt::try_init();
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let setup = db.connect();
    setup
        .execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    setup
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    setup
        .execute("CREATE TABLE driver(id INTEGER PRIMARY KEY)")
        .unwrap();
    setup.execute("INSERT INTO t VALUES (1, 'live')").unwrap();
    let root_before = get_rows(
        &setup,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 't'",
    )[0][0]
        .as_int()
        .unwrap();
    assert!(
        root_before < 0,
        "t must still have an unpublished MVCC root, got {root_before}"
    );

    let checkpoint_conn = db.connect();
    checkpoint_conn
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    let injector = FixedYieldInjector::new([
        CheckpointYieldPoint::AfterCollectTableRows.point(),
        CheckpointYieldPoint::BeforeAcquireLock.point(),
    ]);
    checkpoint_conn.set_yield_injector(Some(injector.clone()));
    let mut checkpoint = checkpoint_conn
        .prepare("INSERT INTO driver VALUES (1)")
        .unwrap();
    let pager_io = checkpoint_conn.pager.load().io.clone();

    let step_to_next_yield = |checkpoint: &mut crate::Statement, expect_remaining: usize| {
        for _ in 0..200_000 {
            match checkpoint.step().unwrap() {
                StepResult::IO | StepResult::Yield => {
                    if injector.remaining_len() == expect_remaining {
                        return true;
                    }
                    pager_io.step().unwrap();
                }
                StepResult::Done => return false,
                other => panic!("unexpected checkpoint step: {other:?}"),
            }
        }
        false
    };

    assert!(
        step_to_next_yield(&mut checkpoint, 1),
        "passive checkpoint must park after collecting its table-row snapshot"
    );

    let dropper = db.connect();
    dropper.execute("DROP TABLE t").unwrap();
    let dropped_schema_rows = get_rows(
        &dropper,
        "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 't'",
    );
    assert!(
        dropped_schema_rows.is_empty(),
        "DROP must be visible before the parked checkpoint resumes"
    );

    assert!(
        step_to_next_yield(&mut checkpoint, 0),
        "passive checkpoint must reach the pre-lock yield after DROP"
    );
    let mut checkpoint_done = false;
    for _ in 0..200_000 {
        match checkpoint.step().unwrap() {
            StepResult::Done => {
                checkpoint_done = true;
                break;
            }
            StepResult::IO | StepResult::Yield => pager_io.step().unwrap(),
            other => panic!("unexpected checkpoint step after DROP: {other:?}"),
        }
    }
    assert!(checkpoint_done, "passive checkpoint did not finish");
    checkpoint_conn.set_yield_injector(None);
    drop(checkpoint);

    let observer = db.connect();
    let schema_rows_after_racing_checkpoint = get_rows(
        &observer,
        "SELECT name, rootpage FROM sqlite_schema WHERE type = 'table' AND name = 't'",
    );
    let integrity_after_racing_checkpoint = get_rows(&observer, "PRAGMA integrity_check");

    observer.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    let schema_rows_after_followup_checkpoint = get_rows(
        &observer,
        "SELECT name, rootpage FROM sqlite_schema WHERE type = 'table' AND name = 't'",
    );
    let integrity_after_followup_checkpoint = get_rows(&observer, "PRAGMA integrity_check");

    assert!(
        schema_rows_after_racing_checkpoint.is_empty()
            && schema_rows_after_followup_checkpoint.is_empty(),
        "committed DROP was lost: after racing checkpoint={schema_rows_after_racing_checkpoint:?}, \
         after follow-up checkpoint={schema_rows_after_followup_checkpoint:?}; integrity results: \
         racing={integrity_after_racing_checkpoint:?}, \
         follow-up={integrity_after_followup_checkpoint:?}"
    );
    let ok = vec![vec![Value::build_text("ok")]];
    assert_eq!(integrity_after_racing_checkpoint, ok);
    assert_eq!(integrity_after_followup_checkpoint, ok);
}

/// Repro for https://github.com/tursodatabase/turso/issues/7957.
///
/// A late DROP of an already-materialized table while a passive checkpoint is
/// parked after collection must keep the dropped root in `dropped_root_pages`
/// until a later checkpoint actually frees that btree.
#[test]
fn test_passive_checkpoint_preserves_late_dropped_root_tracking() {
    use crate::StepResult;

    let _ = tracing_subscriber::fmt::try_init();
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let setup = db.connect();
    setup
        .execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    setup
        .execute("CREATE TABLE keep(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    setup
        .execute("INSERT INTO keep VALUES (1, 'live')")
        .unwrap();
    setup.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let keep_root = get_rows(
        &setup,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 'keep'",
    )[0][0]
        .as_int()
        .unwrap();
    assert!(
        keep_root > 0,
        "keep must have a materialized root, got {keep_root}"
    );

    setup
        .execute("CREATE TABLE work(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    setup
        .execute("INSERT INTO work VALUES (1, 'work')")
        .unwrap();
    setup
        .execute("CREATE TABLE driver(id INTEGER PRIMARY KEY)")
        .unwrap();

    let checkpoint_conn = db.connect();
    checkpoint_conn
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    let injector = FixedYieldInjector::new([
        CheckpointYieldPoint::AfterCollectTableRows.point(),
        CheckpointYieldPoint::BeforeAcquireLock.point(),
    ]);
    checkpoint_conn.set_yield_injector(Some(injector.clone()));
    let mut checkpoint = checkpoint_conn
        .prepare("INSERT INTO driver VALUES (1)")
        .unwrap();
    let pager_io = checkpoint_conn.pager.load().io.clone();

    let step_to_next_yield = |checkpoint: &mut crate::Statement, expect_remaining: usize| {
        for _ in 0..200_000 {
            match checkpoint.step().unwrap() {
                StepResult::IO | StepResult::Yield => {
                    if injector.remaining_len() == expect_remaining {
                        return true;
                    }
                    pager_io.step().unwrap();
                }
                StepResult::Done => return false,
                other => panic!("unexpected checkpoint step: {other:?}"),
            }
        }
        false
    };

    assert!(
        step_to_next_yield(&mut checkpoint, 1),
        "passive checkpoint must park after collecting its table-row snapshot"
    );

    let dropper = db.connect();
    dropper.execute("DROP TABLE keep").unwrap();
    let dropped_schema_rows = get_rows(
        &dropper,
        "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'keep'",
    );
    assert!(
        dropped_schema_rows.is_empty(),
        "DROP must be visible before the parked checkpoint resumes"
    );
    let expected_integrity = vec![vec![Value::build_text("ok")]];
    let integrity_before_resume = get_rows(&dropper, "PRAGMA integrity_check");
    assert_eq!(
        integrity_before_resume, expected_integrity,
        "the committed DROP must keep its still-allocated root accounted for"
    );

    assert!(
        step_to_next_yield(&mut checkpoint, 0),
        "passive checkpoint must reach the pre-lock yield after DROP"
    );
    let mut checkpoint_done = false;
    for _ in 0..200_000 {
        match checkpoint.step().unwrap() {
            StepResult::Done => {
                checkpoint_done = true;
                break;
            }
            StepResult::IO | StepResult::Yield => pager_io.step().unwrap(),
            other => panic!("unexpected checkpoint step after DROP: {other:?}"),
        }
    }
    assert!(checkpoint_done, "passive checkpoint did not finish");
    checkpoint_conn.set_yield_injector(None);
    drop(checkpoint);

    let observer = db.connect();
    let schema_rows_after_checkpoint = get_rows(
        &observer,
        "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'keep'",
    );
    assert!(
        schema_rows_after_checkpoint.is_empty(),
        "keep must remain dropped"
    );
    let integrity_after_checkpoint = get_rows(&observer, "PRAGMA integrity_check");
    assert_eq!(
        integrity_after_checkpoint, expected_integrity,
        "checkpoint lost the late DROP's still-allocated root {keep_root}"
    );
}

/// Unlocking before `on_checkpoint_end` races writers and the next checkpoint.
#[test]
fn on_checkpoint_end_runs_before_blocking_checkpoint_unlock() {
    use crate::io::FileSyncType;
    use crate::mvcc;
    use crate::mvcc::database::{LogRecord, RowVersion};
    use crate::mvcc::persistent_storage::logical_log::{LogHeader, OnSerializationComplete};
    use crate::mvcc::persistent_storage::DurableStorage;
    use crate::storage::encryption::EncryptionContext;
    use crate::storage::sqlite3_ondisk::DatabaseHeader;
    use crate::storage::wal::{CheckpointMode, TursoRwLock};
    use crate::{CheckpointResult, File, Result, IO};

    #[derive(Debug)]
    struct ObserveCheckpointEndStorage {
        inner: Arc<dyn DurableStorage>,
        /// Set after open; probed from `on_checkpoint_end`.
        lock: Mutex<Option<Arc<TursoRwLock>>>,
        lock_held_during_end: AtomicBool,
        end_called: AtomicBool,
    }

    impl ObserveCheckpointEndStorage {
        fn new(inner: Arc<dyn DurableStorage>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                lock: Mutex::new(None),
                lock_held_during_end: AtomicBool::new(false),
                end_called: AtomicBool::new(false),
            })
        }

        fn set_lock(&self, lock: Arc<TursoRwLock>) {
            *self.lock.lock() = Some(lock);
        }
    }

    impl DurableStorage for ObserveCheckpointEndStorage {
        fn serialize_row_version(
            &self,
            log_record: &mut LogRecord,
            row_version: &RowVersion,
            portable_extension: Option<&[u8]>,
        ) -> Result<()> {
            self.inner
                .serialize_row_version(log_record, row_version, portable_extension)
        }
        fn serialize_database_header(
            &self,
            log_record: &mut LogRecord,
            header: &DatabaseHeader,
        ) -> Result<()> {
            self.inner.serialize_database_header(log_record, header)
        }
        fn log_tx(
            &self,
            m: LogRecord,
            c: OnSerializationComplete<'_>,
        ) -> Result<(Completion, u64)> {
            self.inner.log_tx(m, c)
        }
        fn upgrade_header_for_log_tx(&self, m: &LogRecord) -> Result<Option<Completion>> {
            self.inner.upgrade_header_for_log_tx(m)
        }
        fn sync(&self, t: FileSyncType) -> Result<Completion> {
            self.inner.sync(t)
        }
        fn update_header(&self) -> Result<Completion> {
            self.inner.update_header()
        }
        fn truncate(
            &self,
            checkpointed_through_ts: u64,
        ) -> Result<(
            Completion,
            crate::mvcc::persistent_storage::LogicalLogTruncateOutcome,
        )> {
            self.inner.truncate(checkpointed_through_ts)
        }
        fn reset_to_fresh_header(&self) -> Result<Completion> {
            self.inner.reset_to_fresh_header()
        }
        fn get_logical_log_file(&self) -> Arc<dyn File> {
            self.inner.get_logical_log_file()
        }
        fn logical_log_offset(&self) -> u64 {
            self.inner.logical_log_offset()
        }
        fn should_checkpoint(&self) -> bool {
            self.inner.should_checkpoint()
        }
        fn set_checkpoint_threshold(&self, t: i64) {
            self.inner.set_checkpoint_threshold(t)
        }
        fn checkpoint_threshold(&self) -> i64 {
            self.inner.checkpoint_threshold()
        }
        fn advance_logical_log_offset_after_success(&self, b: u64) -> Result<()> {
            self.inner.advance_logical_log_offset_after_success(b)
        }
        fn discard_pending_log_write(&self) -> Result<()> {
            self.inner.discard_pending_log_write()
        }
        fn restore_logical_log_state_after_recovery(&self, o: u64, c: u32) {
            self.inner.restore_logical_log_state_after_recovery(o, c)
        }
        fn set_header(&self, h: LogHeader) {
            self.inner.set_header(h)
        }
        fn on_checkpoint_start(&self) -> Result<()> {
            self.inner.on_checkpoint_start()
        }
        fn on_checkpoint_end(&self, r: Result<&CheckpointResult>) -> Result<()> {
            self.end_called.store(true, Ordering::Release);
            let held = match self.lock.lock().as_ref() {
                Some(lock) => {
                    // write() fails if the checkpoint still holds the lock.
                    if lock.write() {
                        lock.unlock();
                        false
                    } else {
                        true
                    }
                }
                None => false,
            };
            self.lock_held_during_end.store(held, Ordering::Release);
            self.inner.on_checkpoint_end(r)
        }
        fn encryption_ctx(&self) -> Option<EncryptionContext> {
            self.inner.encryption_ctx()
        }
    }

    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir
        .path()
        .join(format!("test_{}.db", rand::random::<u64>()));
    let path_str = path.to_str().unwrap().to_string();
    {
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            &path_str,
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
        DATABASE_MANAGER.lock().clear();
    }

    let log_path = path.with_extension("db-log");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let log_file = io
        .open_file(log_path.to_str().unwrap(), OpenFlags::default(), false)
        .unwrap();
    let inner_storage: Arc<dyn DurableStorage> = Arc::new(mvcc::persistent_storage::Storage::new(
        log_file,
        io.clone(),
        None,
    ));
    let observe = ObserveCheckpointEndStorage::new(inner_storage);
    let db = Database::open(
        io,
        &path_str,
        crate::OpenOptions::new(Arc::new(SqliteDialect))
            .durable_storage(observe.clone() as Arc<dyn DurableStorage>),
    )
    .unwrap();

    let mv_store = db.get_mv_store().clone().unwrap();
    observe.set_lock(mv_store.blocking_checkpoint_lock.clone());

    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn.checkpoint(CheckpointMode::Truncate {
        upper_bound_inclusive: None,
    })
    .unwrap();

    assert!(
        observe.end_called.load(Ordering::Acquire),
        "on_checkpoint_end must run for a successful truncate checkpoint"
    );
    assert!(
        observe.lock_held_during_end.load(Ordering::Acquire),
        "blocking_checkpoint_lock must still be held during on_checkpoint_end"
    );
    assert!(
        mv_store.blocking_checkpoint_lock.write(),
        "blocking_checkpoint_lock must be released after checkpoint returns"
    );
    mv_store.blocking_checkpoint_lock.unlock();
}

/// Documents *why* Truncate Finalize always sees an idle LWM, which is what lets its
/// Rule 3 pass drop sole current versions: acquiring `blocking_checkpoint_lock` for
/// write cannot succeed while any MVCC transaction — even a read-only one — is still
/// open, because `begin_tx` holds the read side of that same lock for the
/// transaction's entire lifetime. Passive never takes this lock around its write phase
/// (that exclusion is exactly the throughput it exists to avoid), so it has to test
/// `lwm == u64::MAX` directly instead — see
/// `passive_reader_snapshot_survives_later_write_after_row_versions_gc` and the
/// `gc_version_chain` doc comment.
#[test]
fn truncate_checkpoint_is_busy_while_a_reader_transaction_is_open() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, bal INTEGER NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1000)").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let reader = db.connect();
    reader.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(
        get_rows(&reader, "SELECT bal FROM t WHERE id = 1"),
        vec![vec![Value::from_i64(1000)]]
    );

    let writer = db.connect();
    writer.execute("BEGIN CONCURRENT").unwrap();
    writer
        .execute("UPDATE t SET bal = 2000 WHERE id = 1")
        .unwrap();
    writer.execute("COMMIT").unwrap();
    assert!(
        matches!(
            writer.execute("PRAGMA wal_checkpoint(TRUNCATE)"),
            Err(LimboError::Busy)
        ),
        "a second Truncate must not be able to run while the earlier reader transaction is open"
    );

    reader.execute("COMMIT").unwrap();
    // Now that the reader is gone, Truncate can proceed.
    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
}

/// What this test checks: commit-time conflict validation reports a
/// write-write conflict, instead of panicking, when it meets an in-flight
/// B-tree tombstone whose writer has already been evicted from the live
/// transaction map into `finalized_tx_states`.
/// Why this matters: eviction (`remove_tx`) runs concurrently with other
/// transactions' commit validation. Validation reads the tombstone's TxID
/// marker first and looks the writer up second, so the writer can move maps
/// in between; this used to panic with "tombstone end TxID not found in txn
/// map" and take the process down. Found by the FTS concurrent-writers fuzz
/// soak.
#[test]
fn commit_validation_reports_conflict_for_evicted_tombstone_writer() {
    let db = MvccTestDb::new();

    // T1 creates the row so a version chain exists.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    db.mvcc_store
        .insert(tx1, generate_simple_string_row((-2).into(), 1, "original"))
        .unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 updates the row, putting it into T2's write set so T2's commit
    // walks this version chain.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    assert!(db
        .mvcc_store
        .update(tx2, generate_simple_string_row((-2).into(), 1, "updated"))
        .unwrap());

    // Recreate the state T2's validation observes mid-race: another
    // transaction deleted the B-tree-resident row, its tombstone still
    // carries the in-flight TxID marker, and the writer itself has already
    // committed and been evicted from `txs` into `finalized_tx_states`.
    let evicted_writer: TxID = 9999;
    db.mvcc_store
        .insert_finalized_tx_state(evicted_writer, 1000)
        .unwrap();
    let row_id = RowID {
        table_id: (-2).into(),
        row_id: RowKey::Int(1),
    };
    let entry = db.mvcc_store.rows.get(&row_id).unwrap();
    entry.value().write().push(RowVersion {
        id: 0,
        begin: PackedTs::pack(None),
        end: PackedTs::pack(Some(TxTimestampOrID::TxID(evicted_writer))),
        row: generate_simple_string_row((-2).into(), 1, "original"),
        btree_resident: true,
        materialized_at: WalPos::ORIGIN,
    });
    drop(entry);

    // The evicted writer committed, so T2 must lose with a write-write
    // conflict — not a panic.
    assert!(matches!(
        commit_tx(db.mvcc_store.clone(), &conn2, tx2),
        Err(LimboError::WriteWriteConflict)
    ));
}

#[path = "group_commit_tests.rs"]
mod group_commit_tests;
