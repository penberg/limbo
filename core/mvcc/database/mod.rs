use crate::alloc::{
    ConcurrentAllocator, DynAllocator, DynVec, TryReserveError, TursoAllocator,
    TursoTryWithCapacityExt, TursoVecInExt, ALLOC_ERR_MSG,
};
use crate::mvcc::clock::LogicalClock;
use crate::mvcc::cursor::{static_iterator_hack, MvccIterator};
#[cfg(any(test, injected_yields))]
use crate::mvcc::yield_hooks::{ProvidesYieldContext, YieldContext, YieldPointMarker};
use crate::mvcc::yield_points::{inject_transition_failure, inject_transition_yield};
use crate::schema::{Schema, Sequence, Table};
use crate::skiplist::comparator::BasicComparator;
use crate::skiplist::map::Entry;
use crate::skiplist::SkipMap;
use crate::state_machine::StateMachine;
use crate::state_machine::StateTransition;
use crate::state_machine::TransitionResult;
use crate::storage::btree::BTreeCursor;
use crate::storage::btree::BTreeKey;
use crate::storage::btree::CursorTrait;
use crate::storage::btree::CursorValidState;
use crate::storage::pager::SavepointResult;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::storage::wal::{CheckpointMode, CheckpointResult, TursoRwLock};
use crate::sync::atomic::{AtomicBool, AtomicI64};
use crate::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use crate::sync::Arc;
use crate::sync::{Mutex, RwLock};
use crate::translate::plan::IterationDirection;
use crate::types::compare_immutable;
use crate::types::IOCompletions;
use crate::types::IOResult;
use crate::types::IOResultOr;
use crate::types::ImmutableRecord;
use crate::types::ImmutableRecordRef;
use crate::types::IndexInfo;
use crate::types::SeekResult;
use crate::Completion;
use crate::File;
use crate::IOExt;
use crate::LimboError;
use crate::PageSize;
use crate::Result;
#[cfg(feature = "conn_raw_api")]
use crate::Value;
use crate::ValueRef;
use crate::{io::FileSyncType, io_yield_one, return_if_io};
use crate::{
    turso_assert, turso_assert_eq, turso_assert_less_than, turso_assert_reachable, Numeric,
};
use crate::{Connection, Pager, SyncMode};
use rustc_hash::FxHashMap as HashMap;
use rustc_hash::FxHashSet as HashSet;
use std::collections::{BTreeSet, HashMap as StdHashMap};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Bound;
#[cfg(any(test, injected_yields))]
use strum::EnumCount;
use tracing::instrument;
use tracing::Level;

pub mod checkpoint_state_machine;
pub use checkpoint_state_machine::{
    sqlite_schema_btree_identity, CheckpointState, CheckpointStateMachine,
};

mod group_commit;
pub(crate) use group_commit::{CommitCoordinator, GroupBatch, GroupWork};

#[cfg(feature = "conn_raw_api")]
use super::persistent_storage::logical_log::{
    parse_ops_from_plaintext, LogSerializer, LOG_RECORD_PREFIX_SIZE,
};
use super::persistent_storage::logical_log::{
    HeaderReadResult, IndexOpKind, ParsedOp, StreamingLogicalLogReader, StreamingResult,
    LOG_HDR_SIZE,
};
#[cfg(feature = "conn_raw_api")]
use super::portable_logical::{
    is_portable_logical_name, is_portable_schema_row, is_portable_table_schema_row,
    portable_schema_row_from_record, PortableLogicalBuilder, PortableObjectMapEntry,
};

#[cfg(test)]
pub mod hermitage_tests;
#[cfg(test)]
pub mod tests;

/// Sentinel value for `MvStore::exclusive_tx` indicating no exclusive transaction is active.
const NO_EXCLUSIVE_TX: u64 = 0;

/// Whether a caller already holds the blocking checkpoint read guard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CheckpointReadLockState {
    NotHeld,
    Held,
}

impl CheckpointReadLockState {
    fn is_held(self) -> bool {
        matches!(self, Self::Held)
    }
}

/// Convert a sequence backing-table row into the exclusive upper bound used by
/// sync scans. This is intentionally tailored to ascending non-CYCLE sequences,
/// which is the shape used by AUTOINCREMENT CDC ids.
pub(crate) fn first_unsafe_sequence_watermark(seq: &Sequence, value: i64, is_called: bool) -> i64 {
    if !is_called {
        return value;
    }
    if seq.increment_by > 0 && !seq.cycle {
        value.checked_add(seq.increment_by).unwrap_or(value)
    } else {
        value
    }
}

#[cfg(not(any(test, injected_yields)))]
struct YieldContext;

/// A table ID for MVCC.
/// MVCC table IDs are always negative. Their corresponding rootpage entry in sqlite_schema
/// is the same negative value if the table has not been checkpointed yet. Otherwise, the root page
/// will be positive and corresponds to the actual physical page.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct MVTableId(i64);

/// The versions of a single row.
///
/// This associated type keeps `RowVersionChain<A>` allocator-shaped in MVCC
/// code without scattering `cfg(nightly)` branches. Stable Rust cannot define
/// a `Vec<T, A>` type alias that ignores `A`, so the stable implementation
/// maps every allocator to the allocator-free `Vec<RowVersion>`.
pub trait RowVersionAllocator: ConcurrentAllocator {
    type RowVersionChain: std::fmt::Debug;
}

#[cfg(not(nightly))]
impl<A: ConcurrentAllocator> RowVersionAllocator for A {
    type RowVersionChain = crate::alloc::Vec<RowVersion>;
}

#[cfg(nightly)]
impl<A: ConcurrentAllocator> RowVersionAllocator for A {
    type RowVersionChain = crate::alloc::Vec<RowVersion, A>;
}

pub type RowVersionChain<A = TursoAllocator> = <A as RowVersionAllocator>::RowVersionChain;
pub type RowVersions<A = TursoAllocator> = Arc<RwLock<RowVersionChain<A>>>;
type TableRowEntry<'a, A = TursoAllocator> = Entry<'a, RowID, RowVersions<A>, BasicComparator, A>;
type IndexRowEntry<'a, A = TursoAllocator> =
    Entry<'a, Arc<SortableIndexKey>, RowVersions<A>, BasicComparator, A>;
type IndexRowsEntry<'a, A = TursoAllocator> =
    Entry<'a, MVTableId, IndexRowsMap<A>, BasicComparator, A>;
type TableRowIterator<'a, A = TursoAllocator> =
    Box<dyn Iterator<Item = TableRowEntry<'a, A>> + Send + Sync + 'a>;
type IndexRowIterator<'a, A = TursoAllocator> =
    Box<dyn Iterator<Item = IndexRowEntry<'a, A>> + Send + Sync + 'a>;

/// Per-index map of sortable keys to their version chains, stored as the
/// values of [`MvStore::index_rows`].
pub type IndexRowsMap<A = TursoAllocator> =
    SkipMap<Arc<SortableIndexKey>, RowVersions<A>, BasicComparator, A>;

impl MVTableId {
    pub fn new(value: i64) -> Self {
        turso_assert_less_than!(value, 0, "MVCC table IDs are always negative");
        Self(value)
    }
}

impl From<i64> for MVTableId {
    fn from(value: i64) -> Self {
        turso_assert_less_than!(value, 0, "MVCC table IDs are always negative");
        Self(value)
    }
}

impl From<MVTableId> for i64 {
    fn from(value: MVTableId) -> Self {
        value.0
    }
}

impl std::fmt::Display for MVTableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MVTableId({})", self.0)
    }
}

/// Wrapper for index keys that implements collation-aware, ASC/DESC-aware ordering.
#[derive(Debug, Clone)]
pub struct SortableIndexKey {
    /// The key as bytes.
    pub key: ImmutableRecordRef<'static>,
    /// Index metadata containing sort orders and collations
    pub metadata: Arc<IndexInfo>,
}

impl SortableIndexKey {
    pub fn new_from_payload_in<A: ConcurrentAllocator>(
        payload: impl AsRef<[u8]>,
        metadata: Arc<IndexInfo>,
        alloc: A,
    ) -> Result<Self, TryReserveError> {
        Ok(Self {
            key: ImmutableRecordRef::from_shared_record(crate::alloc::try_arc_slice_from_slice_in(
                payload.as_ref(),
                alloc,
            )?),
            metadata,
        })
    }

    fn compare(&self, other: &Self) -> Result<std::cmp::Ordering> {
        // We sometimes need to compare a shorter key to a longer one,
        // for example when seeking with an index key that is a prefix of the full key.
        let num_cols = self.metadata.num_cols.min(other.metadata.num_cols);

        let mut lhs = self.key.iter()?;
        let mut rhs = other.key.iter()?;

        for i in 0..num_cols {
            let lhs_value = lhs.next().expect("we already checked length")?;
            let rhs_value = rhs.next().expect("we already checked length")?;

            let cmp = compare_immutable(
                std::iter::once(&lhs_value),
                std::iter::once(&rhs_value),
                &self.metadata.key_info[i..i + 1],
            );

            if cmp != std::cmp::Ordering::Equal {
                return Ok(cmp);
            }
        }

        Ok(std::cmp::Ordering::Equal)
    }

    /// Check if the index key contains any NULL values (excluding the rowid column).
    /// In SQLite, NULLs don't violate UNIQUE constraints, so we skip conflict checks for NULL keys.
    pub fn contains_null(&self, num_indexed_cols: usize) -> Result<bool> {
        let mut iter = self.key.iter()?;
        // Only check the indexed columns, not the rowid at the end
        for _ in 0..num_indexed_cols {
            if let Some(value) = iter.next() {
                if matches!(value?, crate::types::ValueRef::Null) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Check if the first `num_cols` columns of this key match another key.
    /// Used for UNIQUE index conflict detection where we need to compare only
    /// the indexed columns, not the rowid suffix.
    pub fn matches_prefix(&self, other: &Self, num_cols: usize) -> Result<bool> {
        let mut lhs = self.key.iter()?;
        let mut rhs = other.key.iter()?;

        for i in 0..num_cols {
            let lhs_value = match lhs.next() {
                Some(v) => v?,
                None => return Ok(false),
            };
            let rhs_value = match rhs.next() {
                Some(v) => v?,
                None => return Ok(false),
            };

            let cmp = compare_immutable(
                std::iter::once(&lhs_value),
                std::iter::once(&rhs_value),
                &self.metadata.key_info[i..i + 1],
            );

            if cmp != std::cmp::Ordering::Equal {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

impl PartialEq for SortableIndexKey {
    fn eq(&self, other: &Self) -> bool {
        if self.key.get_payload() == other.key.get_payload() {
            return true;
        }

        self.compare(other)
            .map(|ord| ord == std::cmp::Ordering::Equal)
            .unwrap_or(false)
    }
}

impl Eq for SortableIndexKey {}

impl PartialOrd for SortableIndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortableIndexKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.compare(other).expect("Failed to compare IndexKeys")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RowKey {
    Int(i64),
    Record(Arc<SortableIndexKey>),
}

impl RowKey {
    pub fn to_int_or_panic(&self) -> i64 {
        match self {
            RowKey::Int(row_id) => *row_id,
            _ => panic!("RowKey is not an integer"),
        }
    }

    pub fn is_int_key(&self) -> bool {
        matches!(self, RowKey::Int(_))
    }
}

impl std::fmt::Display for RowKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RowKey::Int(row_id) => write!(f, "{row_id}"),
            RowKey::Record(record) => write!(f, "{record:?}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowID {
    /// The table ID. Analogous to table's root page number.
    pub table_id: MVTableId,
    pub row_id: RowKey,
}

impl RowID {
    pub fn new(table_id: MVTableId, row_id: RowKey) -> Self {
        Self { table_id, row_id }
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]

pub struct Row {
    pub id: RowID,
    /// Data is None for index rows because the key holds all the data.
    pub data: Option<crate::alloc::ArcSlice<u8>>,
    pub column_count: usize,
}

impl Row {
    pub fn new_table_row(
        id: RowID,
        data: &[u8],
        column_count: usize,
    ) -> Result<Self, TryReserveError> {
        Self::new_table_row_in(id, data, column_count, TursoAllocator)
    }

    pub fn new_table_row_in<A: ConcurrentAllocator>(
        id: RowID,
        data: &[u8],
        column_count: usize,
        alloc: A,
    ) -> Result<Self, TryReserveError> {
        Ok(Self {
            id,
            data: Some(crate::alloc::try_arc_slice_from_slice_in(data, alloc)?),
            column_count,
        })
    }

    pub fn new_index_row(id: RowID, column_count: usize) -> Self {
        Self {
            id,
            data: None,
            column_count,
        }
    }

    pub fn is_index_row(&self) -> bool {
        self.data.is_none()
    }

    pub fn payload(&self) -> &[u8] {
        match self.id.row_id {
            RowKey::Int(_) => self.data.as_deref().expect("table rows should have data"),
            RowKey::Record(ref sortable_key) => sortable_key.key.get_payload(),
        }
    }
}

/// Packed representation of `Option<TxTimestampOrID>` in a single `u64`,
/// halving the size of the `begin`/`end` fields (16 bytes each → 8).
///
/// Layout (top two bits are the tag):
/// * `0`                  → `None`
/// * `(1 << 62) | value`  → `Some(Timestamp(value))`
/// * `(1 << 63) | value`  → `Some(TxID(value))`
///
/// `value` occupies the low 62 bits. Timestamps and transaction IDs are
/// monotonic counters that start near zero, so 62 bits (~4.6e18) is never
/// exhausted; `pack` asserts this invariant. The two distinct tag bits (rather
/// than a zero sentinel) are required because `Timestamp(0)` is a real value —
/// the logical clock hands out timestamp 0 to the first transaction.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct PackedTs(u64);

impl PackedTs {
    const TIMESTAMP_TAG: u64 = 1 << 62;
    const TXID_TAG: u64 = 1 << 63;
    const VALUE_MASK: u64 = (1 << 62) - 1;
    const NONE: PackedTs = PackedTs(0);

    #[inline]
    pub(crate) fn pack(value: Option<TxTimestampOrID>) -> Self {
        match value {
            None => Self::NONE,
            Some(TxTimestampOrID::Timestamp(ts)) => {
                turso_assert!(ts <= Self::VALUE_MASK, "timestamp exceeds 62-bit range");
                PackedTs(Self::TIMESTAMP_TAG | ts)
            }
            Some(TxTimestampOrID::TxID(id)) => {
                turso_assert!(id <= Self::VALUE_MASK, "tx id exceeds 62-bit range");
                PackedTs(Self::TXID_TAG | id)
            }
        }
    }

    #[inline]
    fn unpack(self) -> Option<TxTimestampOrID> {
        if self.0 == 0 {
            None
        } else if self.0 & Self::TXID_TAG != 0 {
            Some(TxTimestampOrID::TxID(self.0 & Self::VALUE_MASK))
        } else {
            Some(TxTimestampOrID::Timestamp(self.0 & Self::VALUE_MASK))
        }
    }
}

impl std::fmt::Debug for PackedTs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.unpack(), f)
    }
}

/// A row version.
#[derive(Clone, Debug, PartialEq)]
pub struct RowVersion {
    /// Unique identifier for this version within the MvStore.
    /// Used for savepoint tracking to identify specific versions to rollback.
    pub id: u64,
    /// `begin`/`end` timestamps are bit-packed. Read them through the
    /// [`RowVersion::begin`]/[`RowVersion::end`] accessors and write them with
    /// [`RowVersion::set_begin`]/[`RowVersion::set_end`]; the raw `PackedTs`
    /// fields are `pub(crate)` only so they can be set in struct literals (via
    /// `PackedTs::pack`).
    pub(crate) begin: PackedTs,
    pub(crate) end: PackedTs,
    pub row: Row,
    /// Indicates this version was created for a row that existed in B-tree before
    /// MVCC was enabled (e.g., after switching from WAL to MVCC journal mode).
    /// This flag helps the checkpoint logic determine if a delete should be
    /// checkpointed to the B-tree file.
    pub btree_resident: bool,
    /// The WAL position at which this version's *current* (begin, end) state was last
    /// materialized to the B-tree by a checkpoint. [`WalPos::ORIGIN`] means "not yet in the
    /// B-tree" — either never checkpointed, or its state changed (e.g. a delete set `end`) and
    /// the new state is not materialized yet. The version-store GC (`gc_version_chain` Rules 2/3)
    /// may only reclaim a version once its state is materialized (`!= ORIGIN`) AND every reader's
    /// read mark has reached that position — otherwise a reader pinned below it reads the stale
    /// B-tree and the version it needed is gone. Set by the checkpoint at materialization
    /// ([`MvStore::stamp_materialized`]); reset to ORIGIN when a delete supersedes the row.
    pub(crate) materialized_at: WalPos,
}

#[derive(Debug)]
pub enum RowVersionState {
    LiveVersion,
    NotFound,
    Deleted,
}
pub type TxID = u64;

/// A log record contains all durable effects of a committed transaction,
/// pre-serialized into a frame buffer that the logical-log flush path
/// finalizes (backfills the TX header, appends the CRC trailer, optionally
/// chunk-encrypts the payload) and writes to disk.
#[derive(Debug)]
pub struct LogRecord {
    pub(crate) tx_timestamp: TxID,
    /// Frame buffer that grows in place into the on-disk representation.
    /// The first `LOG_HDR_SIZE + TX_HEADER_SIZE` bytes are pre-reserved
    /// (zeros) so that op-entry appends land at the correct on-disk
    /// offset; the flush path backfills the framing prefix and appends
    /// the trailer.
    pub buf: DynVec<u8>,
    /// Number of op entries appended to `buf`. Includes any header op.
    pub op_count: u32,
    /// True once a `DatabaseHeader` op has been appended. At most one
    /// header op is allowed per transaction.
    pub has_header: bool,
    /// Portable logical-change metadata stored alongside the MVCC recovery log.
    ///
    /// Recovery ignores this field. Raw-log consumers use it to resolve the
    /// recovery ops' MVCC table ids and read transaction-level metadata.
    #[cfg(feature = "conn_raw_api")]
    pub portable_changes: crate::alloc::Vec<u8>,
    /// True when the committing connection requested portable logical-change
    /// frames, even if this transaction has no client-visible metadata.
    #[cfg(feature = "conn_raw_api")]
    pub portable_changes_enabled: bool,
    /// True when a frame must carry a portable transaction wrapper even if
    /// the wrapper metadata itself is empty.
    #[cfg(feature = "conn_raw_api")]
    pub portable_changes_required: bool,
}

impl LogRecord {
    #[cfg_attr(
        feature = "aristo-instr",
        aristo::instrument::expose_pub(as = "new_for_test")
    )]
    pub(crate) fn new(tx_timestamp: TxID, alloc: DynAllocator) -> Result<Self> {
        let buf: DynVec<u8> = crate::alloc::try_vec![
            0;
            crate::mvcc::persistent_storage::logical_log::LOG_RECORD_PREFIX_SIZE;
            alloc
        ]?;
        Ok(Self {
            tx_timestamp,
            // Pre-reserve the framing prefix at the front of buf:
            //   [LOG_HDR slot (56B) | TX_HEADER slot (24B) | <ops here>]
            // The log-header slot is only filled on the very first write to
            // a log file; otherwise it stays zero and the flush path exposes
            // a shared view starting at `LOG_HDR_SIZE`, so those 56 bytes never
            // reach disk.
            buf,
            op_count: 0,
            has_header: false,
            #[cfg(feature = "conn_raw_api")]
            portable_changes: crate::alloc::vec![],
            #[cfg(feature = "conn_raw_api")]
            portable_changes_enabled: false,
            #[cfg(feature = "conn_raw_api")]
            portable_changes_required: false,
        })
    }

    fn empty(tx_timestamp: TxID, alloc: DynAllocator) -> Self {
        Self {
            tx_timestamp,
            buf: <DynVec<u8> as TursoVecInExt<u8, DynAllocator>>::new_in(alloc),
            op_count: 0,
            has_header: false,
            #[cfg(feature = "conn_raw_api")]
            portable_changes: crate::alloc::vec![],
            #[cfg(feature = "conn_raw_api")]
            portable_changes_enabled: false,
            #[cfg(feature = "conn_raw_api")]
            portable_changes_required: false,
        }
    }

    /// True iff no ops (row versions or header) have been appended.
    pub fn is_empty(&self) -> bool {
        let empty = self.op_count == 0;
        turso_assert!(
            !empty || !self.has_header,
            "header shouldn't have been written"
        );
        empty
    }

    /// Test-only constructor that eagerly serializes a list of row versions
    /// and an optional `DatabaseHeader` into the payload buffer using the
    /// production wire format. Production code uses
    /// [`DurableStorage::serialize_row_version`] and
    /// [`DurableStorage::serialize_database_header`] instead so the bytes are
    /// appended one op at a time.
    #[cfg(test)]
    pub(crate) fn for_test(
        tx_timestamp: TxID,
        row_versions: &[RowVersion],
        header: Option<DatabaseHeader>,
    ) -> Self {
        let mut record = Self::new(tx_timestamp, DynAllocator::default())
            .expect("failed to allocate logical log record in test");
        for rv in row_versions {
            record.push_row_version_for_test(rv);
        }
        if let Some(hdr) = header {
            record.set_header_for_test(&hdr);
        }
        record
    }

    /// Test-only: append one row-version op to the payload buffer.
    #[cfg(test)]
    pub(crate) fn push_row_version_for_test(&mut self, row_version: &RowVersion) {
        crate::mvcc::persistent_storage::logical_log::LogSerializer::new(&mut self.buf)
            .serialize_op_entry(row_version, None)
            .expect("failed to serialize row version in test");
        self.op_count += 1;
    }

    /// Test-only: append a `DatabaseHeader` op to the payload buffer.
    #[cfg(test)]
    pub(crate) fn set_header_for_test(&mut self, header: &DatabaseHeader) {
        assert!(!self.has_header, "header op appended twice in test");
        crate::mvcc::persistent_storage::logical_log::LogSerializer::new(&mut self.buf)
            .serialize_header_entry(header)
            .expect("failed to serialize database header in test");
        self.has_header = true;
        self.op_count += 1;
    }
}

#[cfg(feature = "conn_raw_api")]
fn portable_table_id_from_rootpage(rootpage: i64) -> MVTableId {
    if rootpage > 0 {
        MVTableId::from(-rootpage)
    } else {
        MVTableId::from(rootpage)
    }
}

#[derive(Clone, Debug)]
#[cfg(feature = "conn_raw_api")]
struct PortableTableRef {
    name: String,
}

#[cfg(feature = "conn_raw_api")]
fn rootpage_for_mv_table_id<Clock: LogicalClock, A: ConcurrentAllocator>(
    mvcc_store: &MvStore<Clock, A>,
    table_id: MVTableId,
) -> i64 {
    mvcc_store
        .table_id_to_rootpage
        .get(&table_id)
        .and_then(|entry| entry.value().root_page)
        .map(|rootpage| rootpage as i64)
        .unwrap_or_else(|| i64::from(table_id))
}

#[cfg(feature = "conn_raw_api")]
fn table_name_for_rootpage_in_schema(schema: &Schema, rootpage: i64) -> Option<String> {
    if rootpage == 0 {
        return None;
    }
    if let Some(name) = schema.table_name_for_root_page(rootpage) {
        return Some(name.to_string());
    }
    let alternate_rootpage = -rootpage;
    if alternate_rootpage == 0 {
        return None;
    }
    schema
        .table_name_for_root_page(alternate_rootpage)
        .map(ToString::to_string)
}

#[cfg(feature = "conn_raw_api")]
fn table_name_for_rootpage(connection: &Connection, rootpage: i64) -> Option<String> {
    {
        let schema = connection.schema.read();
        if let Some(name) = table_name_for_rootpage_in_schema(&schema, rootpage) {
            return Some(name);
        }
    }

    let schema = connection.db.schema.lock();
    table_name_for_rootpage_in_schema(&schema, rootpage)
}

#[cfg(feature = "conn_raw_api")]
fn table_name_for_rootpage_in_mvcc_schema<Clock: LogicalClock, A: ConcurrentAllocator>(
    mvcc_store: &MvStore<Clock, A>,
    rootpage: i64,
) -> Option<String> {
    if rootpage == 0 {
        return None;
    }
    let alternate_rootpage = -rootpage;
    for entry in mvcc_store.rows.iter() {
        if entry.key().table_id != SQLITE_SCHEMA_MVCC_TABLE_ID {
            continue;
        }
        let row_versions = entry.value().read();
        for row_version in row_versions.iter().rev() {
            if row_version.end().is_some() {
                continue;
            }
            let Ok(row) = portable_schema_row_from_record(row_version.row.payload()) else {
                continue;
            };
            if row.rootpage == rootpage || row.rootpage == alternate_rootpage {
                return Some(row.name);
            }
        }
    }
    None
}

#[cfg(feature = "conn_raw_api")]
fn portable_table_name_for_mv_table_id<Clock: LogicalClock, A: ConcurrentAllocator>(
    connection: &Connection,
    mvcc_store: &MvStore<Clock, A>,
    table_id: MVTableId,
) -> Option<String> {
    // `table_id` is the id as serialized into the log (see
    // `canonicalize_table_id`), so interpret it directly as a rootpage first;
    // `table_id_to_rootpage` is keyed by in-memory counter ids and a canonical
    // -(root_page) id can alias an unrelated object's counter id. The map is
    // only a fallback for a stale id serialized before a concurrent checkpoint
    // published new roots.
    let direct = i64::from(table_id);
    if let Some(name) = table_name_for_rootpage(connection, direct)
        .or_else(|| table_name_for_rootpage_in_mvcc_schema(mvcc_store, direct))
    {
        return Some(name);
    }
    let rootpage = rootpage_for_mv_table_id(mvcc_store, table_id);
    if rootpage == direct {
        return None;
    }
    table_name_for_rootpage(connection, rootpage)
        .or_else(|| table_name_for_rootpage_in_mvcc_schema(mvcc_store, rootpage))
}

#[cfg(feature = "conn_raw_api")]
fn portable_delete_op_extension_for_row_version<Clock: LogicalClock, A: ConcurrentAllocator>(
    connection: &Connection,
    mvcc_store: &MvStore<Clock, A>,
    row_version: &RowVersion,
) -> Result<Option<Vec<u8>>> {
    if !connection.portable_logical_changes_enabled() {
        return Ok(None);
    }
    if !matches!(row_version.end(), Some(TxTimestampOrID::Timestamp(_))) {
        return Ok(None);
    }
    let RowKey::Int(rowid) = row_version.row.id.row_id else {
        return Ok(None);
    };

    if row_version.row.id.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID {
        let extension = LogSerializer::encode_delete_portable_extension(
            Some(row_version.row.payload()),
            None,
            Some(rowid),
        )?;
        return Ok((!extension.is_empty()).then_some(extension));
    }

    let Some(table_name) =
        portable_table_name_for_mv_table_id(connection, mvcc_store, row_version.row.id.table_id)
    else {
        return Ok(None);
    };
    if !is_portable_logical_name(&table_name) {
        return Ok(None);
    }

    let schema = connection.schema.read();
    let Some(table) = schema.get_btree_table(&table_name) else {
        return Ok(None);
    };

    let mut record_values = None;
    let mut pk_values = Vec::with_capacity(table.primary_key_columns.len());
    for (pk_name, _) in &table.primary_key_columns {
        let Some((logical_column, column)) = table.get_column(pk_name) else {
            return Err(LimboError::InternalError(format!(
                "primary key column {pk_name} not found for table {table_name}"
            )));
        };
        if column.is_rowid_alias() {
            pk_values.push(Value::from_i64(rowid));
            continue;
        }
        let values = match &record_values {
            Some(values) => values,
            None => record_values.insert(
                ImmutableRecordRef::from_bin_record(row_version.row.payload())
                    .get_values_owned()?,
            ),
        };
        let physical_column = table.logical_to_physical_column(logical_column);
        let Some(value) = values.get(physical_column).cloned() else {
            return Err(LimboError::Corrupt(format!(
                "DELETE_TABLE record for {table_name} missing primary key column {pk_name}"
            )));
        };
        pk_values.push(value);
    }

    let pk_record = if pk_values.is_empty() {
        crate::alloc::vec![]
    } else {
        ImmutableRecord::from_values(&pk_values, pk_values.len())?.into_payload()
    };
    let extension =
        LogSerializer::encode_delete_portable_extension(None, Some(&pk_record), Some(rowid))?;
    Ok((!extension.is_empty()).then_some(extension))
}

/// A transaction timestamp or ID.
///
/// Versions either track a timestamp or a transaction ID, depending on the
/// phase of the transaction. During the active phase, new versions track the
/// transaction ID in the `begin` and `end` fields. After a transaction commits,
/// versions switch to tracking timestamps.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub enum TxTimestampOrID {
    /// A committed transaction's timestamp.
    Timestamp(u64),
    /// The ID of a non-committed transaction.
    TxID(TxID),
}

/// Tracks versions created/modified during a savepoint for rollback.
/// Used for statement-level savepoints in interactive transactions.
#[derive(Debug, Default)]
enum SavepointKind {
    /// Internal savepoint used for statement-level rollback.
    #[default]
    Statement,
    /// User-visible named savepoint.
    Named {
        name: String,
        starts_transaction: bool,
    },
}

/// Tracks row/index version deltas created inside a single savepoint scope.
#[derive(Debug)]
pub struct Savepoint<A: RowVersionAllocator = TursoAllocator> {
    kind: SavepointKind,
    deferred_fk_violations: isize,
    header: DatabaseHeader,
    header_dirty: bool,
    /// Versions CREATED during this savepoint (insert operations).
    /// On rollback: these versions are removed from their chains.
    created_table_versions: Vec<(RowID, u64)>,
    created_index_versions: Vec<((MVTableId, Arc<SortableIndexKey>), u64)>,
    /// Versions DELETED during this savepoint (end timestamp set).
    /// On rollback: clear end timestamp to restore visibility.
    deleted_table_versions: Vec<(RowID, u64)>,
    deleted_index_versions: Vec<((MVTableId, Arc<SortableIndexKey>), u64)>,
    /// RowIDs that were NEWLY added to write_set by this savepoint.
    /// On rollback: only these should be removed from write_set.
    newly_added_to_write_set: Vec<(RowID, RowVersions<A>)>,
}

impl<A: RowVersionAllocator> Default for Savepoint<A> {
    fn default() -> Self {
        Self {
            kind: SavepointKind::default(),
            deferred_fk_violations: 0,
            header: DatabaseHeader::default(),
            header_dirty: false,
            created_table_versions: Vec::new(),
            created_index_versions: Vec::new(),
            deleted_table_versions: Vec::new(),
            deleted_index_versions: Vec::new(),
            newly_added_to_write_set: Vec::new(),
        }
    }
}

impl<A: RowVersionAllocator> Savepoint<A> {
    /// Creates an internal statement savepoint used for per-statement rollback.
    fn statement(header: DatabaseHeader, header_dirty: bool) -> Self {
        Self {
            header,
            header_dirty,
            ..Default::default()
        }
    }

    /// Creates a user-visible named savepoint snapshot.
    fn named(
        name: String,
        starts_transaction: bool,
        deferred_fk_violations: isize,
        header: DatabaseHeader,
        header_dirty: bool,
    ) -> Self {
        Self {
            kind: SavepointKind::Named {
                name,
                starts_transaction,
            },
            deferred_fk_violations,
            header,
            header_dirty,
            ..Default::default()
        }
    }

    /// Merges child savepoint deltas into this savepoint.
    ///
    /// Called when releasing nested savepoints so outer rollback still has a full undo set.
    fn merge_from(&mut self, mut other: Savepoint<A>) {
        self.created_table_versions
            .append(&mut other.created_table_versions);
        self.created_index_versions
            .append(&mut other.created_index_versions);
        self.deleted_table_versions
            .append(&mut other.deleted_table_versions);
        self.deleted_index_versions
            .append(&mut other.deleted_index_versions);
        self.newly_added_to_write_set
            .append(&mut other.newly_added_to_write_set);
    }
}

struct SavepointRollbackResult<A: RowVersionAllocator = TursoAllocator> {
    /// Savepoints that were rolled back, in the order they were created (oldest to newest).
    rolledback_savepoints: Vec<Savepoint<A>>,
    /// Deferred FK counter snapshot captured at the target named savepoint.
    deferred_fk_violations: isize,
}

#[derive(Debug)]
struct WriteSet<A: RowVersionAllocator = TursoAllocator> {
    entries: Vec<(RowID, RowVersions<A>)>,
    /// A set of the pointer addresses of the `RowVersions`. Used to deduplicate entries.
    ///
    /// This is correct because instances of [RowVersions] are created once per [RowID] and then
    /// reused by cloning the [Arc]. It would be nice to encode this in the type system, but I'm
    /// not sure how.
    seen: HashSet<usize>,
}

impl<A: RowVersionAllocator> Default for WriteSet<A> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            seen: HashSet::default(),
        }
    }
}

impl<A: RowVersionAllocator> WriteSet<A> {
    fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if this `RowVersions` was not already contained in the write set.
    fn insert(&mut self, id: RowID, row_versions: RowVersions<A>) -> bool {
        let ptr = Arc::as_ptr(&row_versions) as usize;
        if self.seen.insert(ptr) {
            self.entries.push((id, row_versions));
            true
        } else {
            false
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn iter(&self) -> std::slice::Iter<'_, (RowID, RowVersions<A>)> {
        self.entries.iter()
    }

    fn take(&mut self) -> Self {
        std::mem::take(self)
    }

    /// Retain entries where `keep(rowid, row_versions)` returns true.
    fn retain<F: FnMut(&RowID, &RowVersions<A>) -> bool>(&mut self, mut keep: F) {
        let seen = &mut self.seen;
        self.entries.retain(|(rowid, rv)| {
            if keep(rowid, rv) {
                true
            } else {
                seen.remove(&(Arc::as_ptr(rv) as usize));
                false
            }
        });
    }
}

/// Transaction
#[derive(Debug)]
pub struct Transaction<A: RowVersionAllocator = TursoAllocator> {
    /// The state of the transaction.
    state: AtomicTransactionState,
    /// The transaction ID.
    tx_id: u64,
    /// The transaction begin timestamp.
    begin_ts: u64,
    /// The transaction write set. Only writer is the [Transaction]'s own connection.
    write_set: Mutex<WriteSet<A>>,
    /// The transaction header.
    header: RwLock<DatabaseHeader>,
    /// True when the transaction mutated its local database header snapshot.
    header_dirty: AtomicBool,
    /// Stack of savepoints for statement-level rollback.
    /// Each savepoint tracks versions created/deleted during that statement.
    savepoint_stack: RwLock<Vec<Savepoint<A>>>,
    /// True when this transaction currently holds the serialized logical-log commit lock.
    pager_commit_lock_held: AtomicBool,
    /// True once this transaction's commit record is in the logical log.
    /// From then on the transaction commits whatever happens to the
    /// statement driving it: later records are chained after its record
    /// and recovery replays it.
    log_appended: AtomicBool,
    /// Number of unresolved commit dependencies (must reach 0 before commit).
    /// i.e the number of transactions this transaction is dependent on and waiting for
    /// commit or abort.
    /// Hekaton Section 2.7: "A transaction cannot commit until this counter is zero."
    commit_dep_counter: AtomicU64,
    /// Flag: a depended-on transaction aborted; this transaction must abort too.
    /// Hekaton Section 2.7: "AbortNow that other transactions can set to tell T to abort."
    abort_now: AtomicBool,
    /// Transaction IDs that depend on this transaction (notified on commit/abort).
    /// Hekaton Section 2.7: "CommitDepSet, that stores transaction IDs of the
    /// transactions that depend on T."
    commit_dep_set: Mutex<HashSet<TxID>>,
    /// True when this transaction holds `blocking_checkpoint_lock` in read mode
    /// (truncate / flag-off path only). Passive `begin_tx` does not pin the lock.
    holds_blocking_checkpoint_read: AtomicBool,
    /// `MvStore::schema_generation` captured at `begin_tx` (passive root publication gate).
    schema_generation_at_begin: u64,
    /// This transaction's frozen WAL read mark `(checkpoint_seq, max_frame)`, captured when it
    /// pinned its read transaction at begin. A checkpoint-materialized B-tree is physically
    /// reachable by this transaction only if `materialized_at <= read_mark` — i.e. the
    /// materialization's frames are at-or-below this read mark (or in an earlier, backfilled WAL
    /// epoch). See [`MvStore::is_btree_readable_at`] / [`MvStore::compute_min_reader_mark`].
    read_mark: WalPos,
}

impl<A: RowVersionAllocator> Transaction<A> {
    fn new(
        tx_id: u64,
        begin_ts: u64,
        header: DatabaseHeader,
        read_mark: WalPos,
        schema_generation_at_begin: u64,
    ) -> Transaction<A> {
        Transaction {
            state: TransactionState::Active.into(),
            tx_id,
            begin_ts,
            read_mark,
            schema_generation_at_begin,
            write_set: Mutex::new(WriteSet::new()),
            header: RwLock::new(header),
            header_dirty: AtomicBool::new(false),
            savepoint_stack: RwLock::new(Vec::new()),
            pager_commit_lock_held: AtomicBool::new(false),
            log_appended: AtomicBool::new(false),
            commit_dep_counter: AtomicU64::new(0),
            abort_now: AtomicBool::new(false),
            commit_dep_set: Mutex::new(HashSet::default()),
            holds_blocking_checkpoint_read: AtomicBool::new(false),
        }
    }

    fn insert_to_write_set(&self, id: RowID, row_versions: RowVersions<A>) {
        turso_assert_eq!(
            self.state,
            TransactionState::Active,
            "write set cannot be modified unless transaction is active"
        );
        // Always record in the current savepoint's `newly_added_to_write_set`.
        // Duplicates here are harmless: `rollback_savepoint_changes` collects
        // touched rowids into a BTreeSet (dedup), and the actual write_set
        // removal is gated by `row_has_uncommitted_version_for_tx`, so a row
        // already pinned by a parent savepoint won't be evicted on inner
        // rollback.
        if let Some(savepoint) = self.savepoint_stack.write().last_mut() {
            savepoint
                .newly_added_to_write_set
                .push((id.clone(), row_versions.clone()));
        }
        self.write_set.lock().insert(id, row_versions);
    }

    /// Begin a new savepoint for statement-level tracking.
    fn begin_savepoint(&self) {
        let depth = self.savepoint_stack.read().len();
        tracing::debug!("begin_savepoint(tx_id={}, depth={})", self.tx_id, depth);
        let header = *self.header.read();
        let header_dirty = self.header_dirty.load(Ordering::Acquire);
        self.savepoint_stack
            .write()
            .push(Savepoint::statement(header, header_dirty));
    }

    /// Begin a new named savepoint. If `starts_transaction` is true, this savepoint represents the
    /// beginning of an interactive transaction and will be used to track deferred FK violations
    /// for that transaction.
    fn begin_named_savepoint(
        &self,
        name: String,
        starts_transaction: bool,
        deferred_fk_violations: isize,
    ) {
        let depth = self.savepoint_stack.read().len();
        tracing::debug!(
            "begin_named_savepoint(tx_id={}, depth={}, name={})",
            self.tx_id,
            depth,
            name
        );
        let header = *self.header.read();
        let header_dirty = self.header_dirty.load(Ordering::Acquire);
        self.savepoint_stack.write().push(Savepoint::named(
            name,
            starts_transaction,
            deferred_fk_violations,
            header,
            header_dirty,
        ));
    }

    /// Release the newest savepoint (statement completed successfully).
    fn release_savepoint(&self) {
        let depth = self.savepoint_stack.read().len();
        tracing::debug!("release_savepoint(tx_id={}, depth={})", self.tx_id, depth);
        let mut savepoints = self.savepoint_stack.write();
        if !matches!(
            savepoints.last().map(|savepoint| &savepoint.kind),
            Some(SavepointKind::Statement)
        ) {
            return;
        }
        let savepoint = savepoints.pop().expect("savepoint must exist");
        if let Some(parent) = savepoints.last_mut() {
            parent.merge_from(savepoint);
        }
    }

    fn pop_statement_savepoint(&self) -> Option<Savepoint<A>> {
        let mut savepoints = self.savepoint_stack.write();
        if !matches!(
            savepoints.last().map(|savepoint| &savepoint.kind),
            Some(SavepointKind::Statement)
        ) {
            return None;
        }
        savepoints.pop()
    }

    /// Release a named savepoint. If this savepoint starts a transaction, returns
    /// [SavepointResult::Commit] to indicate the transaction should be committed.
    fn release_named_savepoint(&self, name: &str) -> SavepointResult {
        let mut savepoints = self.savepoint_stack.write();
        let Some(target_idx) = savepoints.iter().rposition(|savepoint| {
            matches!(
                savepoint.kind,
                SavepointKind::Named {
                    name: ref savepoint_name,
                    ..
                } if savepoint_name == name
            )
        }) else {
            return SavepointResult::NotFound;
        };

        let commits_transaction = if matches!(
            savepoints[target_idx].kind,
            SavepointKind::Named {
                starts_transaction: true,
                ..
            }
        ) && target_idx == 0
        {
            SavepointResult::Commit
        } else {
            SavepointResult::Release
        };
        if matches!(commits_transaction, SavepointResult::Commit) {
            // Defer mutation until transaction commit succeeds. If commit fails
            // (e.g. deferred FK violation), savepoints must remain intact.
            return commits_transaction;
        }

        let drained: Vec<Savepoint<A>> = savepoints.drain(target_idx..).collect();
        if let Some(parent) = savepoints.last_mut() {
            for savepoint in drained {
                parent.merge_from(savepoint);
            }
        }
        commits_transaction
    }

    /// Find the named savepoint to rollback to and pop all savepoints above it. Returns the rolled
    /// back savepoints and net change in deferred FK violations for undoing changes to transaction
    /// state.
    fn rollback_to_named_savepoint(&self, name: &str) -> Option<SavepointRollbackResult<A>> {
        let mut savepoints = self.savepoint_stack.write();
        let target_idx = savepoints.iter().rposition(|savepoint| {
            matches!(
                savepoint.kind,
                SavepointKind::Named {
                    name: ref savepoint_name,
                    ..
                } if savepoint_name == name
            )
        })?;

        let target_name = match &savepoints[target_idx].kind {
            SavepointKind::Named { name, .. } => name.clone(),
            SavepointKind::Statement => unreachable!("target idx points to named savepoint"),
        };
        let starts_transaction = matches!(
            savepoints[target_idx].kind,
            SavepointKind::Named {
                starts_transaction: true,
                ..
            }
        );
        let deferred_fk_violations = savepoints[target_idx].deferred_fk_violations;
        let header = savepoints[target_idx].header;
        let header_dirty = savepoints[target_idx].header_dirty;

        let drained: Vec<Savepoint<A>> = savepoints.drain(target_idx..).collect();
        savepoints.push(Savepoint::named(
            target_name,
            starts_transaction,
            deferred_fk_violations,
            header,
            header_dirty,
        ));
        Some(SavepointRollbackResult {
            rolledback_savepoints: drained,
            deferred_fk_violations,
        })
    }

    /// Record a version that was created during the current savepoint.
    fn record_created_table_version(&self, rowid: RowID, version_id: u64) {
        if let Some(savepoint) = self.savepoint_stack.write().last_mut() {
            tracing::debug!(
                "record_created_table_version(tx_id={}, table_id={}, row_id={}, version_id={})",
                self.tx_id,
                rowid.table_id,
                rowid.row_id,
                version_id
            );
            savepoint.created_table_versions.push((rowid, version_id));
        }
    }

    /// Record an index version that was created during the current savepoint.
    fn record_created_index_version(
        &self,
        key: (MVTableId, Arc<SortableIndexKey>),
        version_id: u64,
    ) {
        if let Some(savepoint) = self.savepoint_stack.write().last_mut() {
            tracing::debug!(
                "record_created_index_version(tx_id={}, table_id={}, version_id={})",
                self.tx_id,
                key.0,
                version_id
            );
            savepoint.created_index_versions.push((key, version_id));
        }
    }

    /// Record a version that was deleted during the current savepoint.
    fn record_deleted_table_version(&self, rowid: RowID, version_id: u64) {
        if let Some(savepoint) = self.savepoint_stack.write().last_mut() {
            tracing::debug!(
                "record_deleted_table_version(tx_id={}, table_id={}, row_id={}, version_id={})",
                self.tx_id,
                rowid.table_id,
                rowid.row_id,
                version_id
            );
            savepoint.deleted_table_versions.push((rowid, version_id));
        }
    }

    /// Record an index version that was deleted during the current savepoint.
    fn record_deleted_index_version(
        &self,
        key: (MVTableId, Arc<SortableIndexKey>),
        version_id: u64,
    ) {
        if let Some(savepoint) = self.savepoint_stack.write().last_mut() {
            tracing::debug!(
                "record_deleted_index_version(tx_id={}, table_id={}, version_id={})",
                self.tx_id,
                key.0,
                version_id
            );
            savepoint.deleted_index_versions.push((key, version_id));
        }
    }
}

impl<A: RowVersionAllocator> std::fmt::Display for Transaction<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "{{ state: {}, id: {}, begin_ts: {}, write_set: ",
            self.state.load(),
            self.tx_id,
            self.begin_ts,
        )?;

        match self.write_set.try_lock() {
            Some(write_set) => {
                write!(f, "[")?;
                for (i, (id, _chain)) in write_set.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?
                    }
                    write!(f, "{id:?}")?;
                }
                write!(f, "]")?;
            }
            None => write!(f, "<locked>")?,
        }

        write!(f, " }}")
    }
}

/// Transaction state.
#[derive(Debug, Clone, PartialEq, Copy)]
enum TransactionState {
    Active,
    /// Preparing state includes the end_ts so other transactions can compare
    /// timestamps during validation to resolve races (first-committer-wins).
    Preparing(u64),
    Aborted,
    Terminated,
    Committed(u64),
}

impl TransactionState {
    // Bit patterns for encoding states with timestamps
    const PREPARING_BIT: u64 = 0x4000_0000_0000_0000;
    const COMMITTED_BIT: u64 = 0x8000_0000_0000_0000;
    const TIMESTAMP_MASK: u64 = 0x3fff_ffff_ffff_ffff;

    pub fn encode(&self) -> u64 {
        match self {
            TransactionState::Active => 0,
            TransactionState::Preparing(ts) => {
                // We only support 2^62 - 1 timestamps
                assert!(ts & !Self::TIMESTAMP_MASK == 0);
                Self::PREPARING_BIT | ts
            }
            TransactionState::Aborted => 1,
            TransactionState::Terminated => 2,
            TransactionState::Committed(ts) => {
                // We only support 2^62 - 1 timestamps
                turso_assert_eq!(ts & !Self::TIMESTAMP_MASK, 0);
                Self::COMMITTED_BIT | ts
            }
        }
    }

    pub fn decode(v: u64) -> Self {
        match v {
            0 => TransactionState::Active,
            1 => TransactionState::Aborted,
            2 => TransactionState::Terminated,
            v if v & Self::COMMITTED_BIT != 0 => {
                TransactionState::Committed(v & Self::TIMESTAMP_MASK)
            }
            v if v & Self::PREPARING_BIT != 0 => {
                TransactionState::Preparing(v & Self::TIMESTAMP_MASK)
            }
            _ => panic!("Invalid transaction state"),
        }
    }
}

// Transaction state encoded into a single 64-bit atomic.
#[derive(Debug)]
pub(crate) struct AtomicTransactionState {
    pub(crate) state: AtomicU64,
}

impl From<TransactionState> for AtomicTransactionState {
    fn from(state: TransactionState) -> Self {
        Self {
            state: AtomicU64::new(state.encode()),
        }
    }
}

impl From<AtomicTransactionState> for TransactionState {
    fn from(state: AtomicTransactionState) -> Self {
        let encoded = state.state.load(Ordering::Acquire);
        TransactionState::decode(encoded)
    }
}

impl std::cmp::PartialEq<TransactionState> for AtomicTransactionState {
    fn eq(&self, other: &TransactionState) -> bool {
        &self.load() == other
    }
}

impl std::fmt::Display for TransactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            TransactionState::Active => write!(f, "Active"),
            TransactionState::Preparing(ts) => write!(f, "Preparing({ts})"),
            TransactionState::Committed(ts) => write!(f, "Committed({ts})"),
            TransactionState::Aborted => write!(f, "Aborted"),
            TransactionState::Terminated => write!(f, "Terminated"),
        }
    }
}

impl AtomicTransactionState {
    fn store(&self, state: TransactionState) {
        self.state.store(state.encode(), Ordering::Release);
    }

    fn load(&self) -> TransactionState {
        TransactionState::decode(self.state.load(Ordering::Acquire))
    }
}

#[allow(clippy::large_enum_variant)]
pub enum CommitState<Clock: LogicalClock, A: ConcurrentAllocator = TursoAllocator> {
    Initial,
    Commit {
        end_ts: u64,
    },
    /// Wait for unresolved commit dependencies before building the durable
    /// committed view for the logical log.
    /// Hekaton Section 3.2: "If T passes validation, it must wait for outstanding
    /// commit dependencies to be resolved."
    WaitForDependencies {
        end_ts: u64,
    },
    /// Build the committed log record incrementally, yielding every
    /// `MVCC_COMMIT_BATCH_SIZE` rowids so that very large write sets
    /// (e.g. CREATE INDEX on a multi-million row table) don't monopolize
    /// the executor.
    BuildLogRecord(BuildLogRecordCtx),
    BeginCommitLogicalLog {
        end_ts: u64,
        log_record: LogRecord,
    },
    /// Enqueued behind a group-commit leader, or waiting for
    /// `durable_through` to cover this ticket.
    AwaitGroupCommit {
        end_ts: u64,
        ticket: u64,
    },
    UpgradeLogicalLogHeader {
        end_ts: u64,
        log_record: LogRecord,
    },
    WriteLogicalLog {
        end_ts: u64,
        log_record: LogRecord,
    },
    FinishLogicalLogWrite {
        end_ts: u64,
    },
    SyncLogicalLog {
        end_ts: u64,
    },
    /// Fsync the log through `written_through` after a leader dropped,
    /// then return to [`CommitState::AwaitGroupCommit`].
    SyncGroupPrefix {
        end_ts: u64,
        ticket: u64,
    },
    GroupPrefixSynced {
        end_ts: u64,
        ticket: u64,
    },
    EndCommitLogicalLog {
        end_ts: u64,
    },
    Checkpoint {
        // TODO: if and when we transform this code to async we won't be needing this explicit state machine nor
        // the mutex
        state_machine: Mutex<StateMachine<CheckpointStateMachine<Clock, A>>>,
    },
    CommitEnd {
        end_ts: u64,
    },
    /// Publish committed timestamps into the live MVCC chains in chunks
    /// of `MVCC_COMMIT_BATCH_SIZE` rowids, yielding between chunks. The
    /// transaction is already in the Committed state at this point, so
    /// readers consult `txs[tx_id]` to resolve any TxID references that
    /// haven't been rewritten yet.
    RewriteLiveVersions(RewriteLiveVersionsCtx),
    /// Final post-rewrite cleanup: drain commit dependents, release the
    /// commit lock, update the global header, finish the tx, and start
    /// auto-checkpoint if needed.
    FinalizeCommit {
        end_ts: u64,
    },
}

/// Iteration state for the chunked `BuildLogRecord` step.
#[derive(Debug)]
pub struct BuildLogRecordCtx {
    pub end_ts: u64,
    pub log_record: LogRecord,
    /// Index into `CommitStateMachine::write_set` for the current pass.
    pub cursor: usize,
    /// True while emitting schema rows (sqlite_schema), false during the
    /// data-row pass. Schema rows are emitted first so log replay sees
    /// CREATE TABLE before related INSERTs.
    pub schema_process: bool,
    /// Snapshot of `tx.header` taken when `header_dirty` was observed
    /// at the start of BuildLogRecord. Appended as an `OP_UPDATE_HEADER`
    /// op after both row-version passes complete (preserves on-disk
    /// order: ops first, header last).
    pub pending_header: Option<DatabaseHeader>,
}

/// Iteration state for the chunked `RewriteLiveVersions` step.
#[derive(Debug)]
pub struct RewriteLiveVersionsCtx {
    pub end_ts: u64,
    /// Index into `CommitStateMachine::write_set`.
    pub cursor: usize,
}

/// How many rowids `BuildLogRecord` / `RewriteLiveVersions` process before
/// yielding to the executor. Picked to amortize state-machine overhead
/// while keeping a CREATE INDEX on a 2M-row table responsive.
const MVCC_COMMIT_BATCH_SIZE: usize = 1024;

#[derive(Debug)]
pub enum WriteRowState {
    Initial,
    Seek,
    /// After seek returns TryAdvance for an index key stored in an interior node,
    /// advance the cursor to that interior cell so insert overwrites it.
    Advance,
    Insert,
    /// Move to the next record in order to leave the cursor in the next position, this is used for inserting multiple rows for optimizations.
    Next,
}

#[cfg(any(test, injected_yields))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum_macros::EnumCount)]
#[repr(u8)]
pub(crate) enum CommitYieldPoint {
    CommitValidation,
    WaitForDependencies,
    /// Fires once on the first entry into `step_build_log_record` (cursor=0,
    /// schema_process=true), before any chunk processing. Pairs with
    /// `LogRecordPrepared` to bracket the BuildLogRecord chunked yields.
    BuildLogRecordStart,
    LogRecordPrepared,
    /// Fires after commit dependencies are released and the commit lock is
    /// dropped, but before publishing the cached global header / committed
    /// timestamp watermark.
    BeforeGlobalHeaderUpdate,
    BeforeFinishCommittedTx,
    /// Boundary right after `remove_tx` runs but before the connection cache
    /// is cleared by the caller at vdbe/mod.rs. Used for failure injection
    /// to reproduce divergence between `mv_store.txs` and `connection.mv_tx_id`.
    AfterRemoveTx,
    /// Fires after this transaction's logical-log record is owned (offset
    /// advanced, `log_appended` set), including the last record of a group
    /// batch, and before the covering fsync.
    LogicalLogOwned,
    /// Fires after `log_tx` is issued for another transaction's record and
    /// before the offset is advanced. Dropping the waiter here must not
    /// roll back a write the leader still owns.
    LogicalLogWriteIssued,
}

#[cfg(any(test, injected_yields))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum_macros::EnumCount)]
#[repr(u8)]
pub(crate) enum ExclusiveTxYieldPoint {
    AfterTimestampCheckBeforeCas,
}

#[cfg(any(test, injected_yields))]
impl YieldPointMarker for ExclusiveTxYieldPoint {
    const POINT_COUNT: u8 = Self::COUNT as u8;

    fn ordinal(self) -> u8 {
        self as u8
    }
}

#[cfg(any(test, injected_yields))]
impl YieldPointMarker for CommitYieldPoint {
    const POINT_COUNT: u8 = Self::COUNT as u8;

    fn ordinal(self) -> u8 {
        self as u8
    }
}

#[cfg(any(test, injected_yields))]
fn commit_yield_key(tx_id: u64) -> u64 {
    // any large number will do
    const COMMIT_SELECTION_TAG: u64 = 0xC011_C011_C011_C011;
    tx_id ^ COMMIT_SELECTION_TAG
}

#[cfg(any(test, injected_yields))]
impl<Clock: LogicalClock, A: ConcurrentAllocator> ProvidesYieldContext
    for CommitStateMachine<Clock, A>
{
    fn yield_context(&self) -> YieldContext {
        YieldContext::new(
            self.connection.yield_injector(),
            self.connection.failure_injector(),
            self.yield_instance_id,
            commit_yield_key(self.tx_id),
        )
    }
}

pub struct CommitStateMachine<Clock: LogicalClock, A: ConcurrentAllocator = TursoAllocator> {
    state: CommitState<Clock, A>,
    is_finalized: bool,
    #[cfg(any(test, injected_yields))]
    yield_instance_id: u64,
    did_commit_schema_change: bool,
    tx_id: TxID,
    mvcc_store: Arc<MvStore<Clock, A>>,
    connection: Arc<Connection>,
    /// Database index this commit is for (`MAIN_DB_ID` or an attached-db id).
    /// Threaded through so that `finish_committed_tx` can clear the matching
    /// connection-level mv_tx slot atomically with `remove_tx`.
    db_id: usize,
    commit_coordinator: Arc<CommitCoordinator>,
    header: Arc<RwLock<Option<DatabaseHeader>>>,
    pager: Arc<Pager>,
    /// Bytes appended to the logical log for this commit; applied to writer offset as soon as the write succeeds.
    pending_log_append_bytes: Option<u64>,
    group_batch: Option<GroupBatch>,
    /// This state machine issued at least one `log_tx` (leader or exclusive).
    wrote_logical_log: bool,
    /// The synchronous mode for fsync operations. When set to Off, fsync is skipped.
    sync_mode: SyncMode,
    _phantom: PhantomData<Clock>,
}

impl<Clock: LogicalClock, A: ConcurrentAllocator> Debug for CommitStateMachine<Clock, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitStateMachine")
            .field("state", &self.state)
            .field("is_finalized", &self.is_finalized)
            .finish()
    }
}

impl<Clock: LogicalClock, A: ConcurrentAllocator> Drop for CommitStateMachine<Clock, A> {
    fn drop(&mut self) {
        self.cleanup_unfinished_commit();
    }
}

pub struct WriteRowStateMachine {
    state: WriteRowState,
    is_finalized: bool,
    row: Row,
    record: Option<ImmutableRecord>,
    cursor: Arc<RwLock<BTreeCursor>>,
    requires_seek: bool,
}

#[derive(Debug)]
pub enum DeleteRowState {
    Initial,
    Seek,
    /// After seek returns TryAdvance (key found in interior node, not leaf),
    /// advance the cursor to position it on the interior cell.
    Advance,
    Delete,
}

pub struct DeleteRowStateMachine {
    state: DeleteRowState,
    is_finalized: bool,
    rowid: RowID,
    cursor: Arc<RwLock<BTreeCursor>>,
}

impl<Clock: LogicalClock, A: ConcurrentAllocator> CommitStateMachine<Clock, A> {
    pub(crate) fn cleanup_mvcc_checkpoint_state(&mut self) {
        if let CommitState::Checkpoint { state_machine } = &mut self.state {
            let _ = state_machine
                .lock()
                .inner_mut()
                .cleanup_after_external_io_error(LimboError::InternalError(
                    "mvcc: cleanup_unfinished_commit".to_string(),
                ))
                .inspect_err(|e| tracing::error!("cleanup_after_external_io_error failed: {e}"));
        }
    }

    fn new(
        state: CommitState<Clock, A>,
        tx_id: TxID,
        mvcc_store: Arc<MvStore<Clock, A>>,
        connection: Arc<Connection>,
        db_id: usize,
        commit_coordinator: Arc<CommitCoordinator>,
        header: Arc<RwLock<Option<DatabaseHeader>>>,
    ) -> Result<Self> {
        let pager = connection.get_pager_from_database_index(&db_id)?;
        let sync_mode = connection.get_sync_mode_for_database(db_id)?;
        // Use the connection's tx-level schema_did_change flag as the
        // single source of truth.  This flag is set by SetCookie(SchemaVersion)
        // which every DDL emits, so it covers all schema-changing operations
        // including ones that don't write to sqlite_schema (e.g. AddType
        // writing to __turso_internal_types for custom types).
        let schema_did_change_from_tx = matches!(
            connection.get_tx_state(),
            crate::connection::TransactionState::Write {
                schema_did_change: true
            }
        );
        Ok(Self {
            state,
            is_finalized: false,
            #[cfg(any(test, injected_yields))]
            yield_instance_id: connection.next_yield_instance_id(),
            did_commit_schema_change: schema_did_change_from_tx,
            tx_id,
            mvcc_store,
            connection,
            db_id,
            commit_coordinator,
            pager,
            header,
            pending_log_append_bytes: None,
            group_batch: None,
            wrote_logical_log: false,
            sync_mode,
            _phantom: PhantomData,
        })
    }

    fn release_group_claim(&mut self) {
        if let Some(mut batch) = self.group_batch.take() {
            if let Some(advanced) = batch.advanced_through {
                self.commit_coordinator.note_written(advanced);
            }
            let owned = batch.advanced_through == Some(batch.writing.ticket);
            let submitted = self.pending_log_append_bytes.is_some();
            let before_log_tx = !submitted
                && matches!(
                    self.state,
                    CommitState::UpgradeLogicalLogHeader { .. }
                        | CommitState::WriteLogicalLog { .. }
                );
            if before_log_tx {
                if let CommitState::UpgradeLogicalLogHeader { log_record, .. }
                | CommitState::WriteLogicalLog { log_record, .. } = &mut self.state
                {
                    std::mem::swap(&mut batch.writing.log_record, log_record);
                }
                self.commit_coordinator
                    .requeue(std::iter::once(batch.writing).chain(batch.rest));
            } else if !owned
                && matches!(
                    self.state,
                    CommitState::WriteLogicalLog { .. } | CommitState::FinishLogicalLogWrite { .. }
                )
            {
                self.commit_coordinator.clear_issued();
                if self.commit_coordinator.take_abandoned(batch.writing.tx_id) {
                    if self.mvcc_store.txs.get(&batch.writing.tx_id).is_some() {
                        self.mvcc_store
                            .rollback_tx_inner(batch.writing.tx_id, None, self.db_id);
                    }
                } else {
                    self.commit_coordinator.request_retry(batch.writing.ticket);
                }
                self.commit_coordinator.requeue(batch.rest.into_iter());
            } else {
                self.commit_coordinator.requeue(batch.rest.into_iter());
            }
        }
        if let CommitState::AwaitGroupCommit { ticket, .. } = self.state {
            let _ = self.commit_coordinator.drop_pending(ticket);
        }
    }

    fn cleanup_unfinished_commit(&mut self) {
        if !self.is_finalized {
            self.release_group_claim();
            self.cleanup_mvcc_checkpoint_state();
            if self.pending_log_append_bytes.take().is_some() {
                if let Err(err) = self.mvcc_store.storage.discard_pending_log_write() {
                    tracing::error!("failed to discard pending MVCC logical-log write: {err}");
                }
            }
            if !matches!(self.state, CommitState::Checkpoint { .. }) {
                self.mvcc_store.cleanup_dropped_commit(
                    self.tx_id,
                    self.connection.as_ref(),
                    self.db_id,
                );
            }
            self.end_read_tx_for_db();
            if self.db_id == crate::MAIN_DB_ID {
                self.connection
                    .set_tx_state(crate::connection::TransactionState::None);
            }
        }

        let tx_id = self.tx_id;
        let db_id = self.db_id;
        turso_assert!(
            self.mvcc_store.txs.get(&tx_id).is_none()
                || self.commit_coordinator.is_abandoned(tx_id),
            "MVCC tx should be removed from txs after a successful commit",
            { "tx_id": tx_id }
        );
        turso_assert!(
            !self.mvcc_store.is_exclusive_tx(&tx_id),
            "MVCC tx should not still hold the exclusive slot after a successful commit",
            { "tx_id": tx_id }
        );
        turso_assert!(
            self.connection.get_mv_tx_id_for_db(db_id) != Some(tx_id),
            "Connection should not still reference an MVCC tx after a successful commit",
            { "tx_id": tx_id, "db_id": db_id }
        );
    }

    fn take_next_group_record(&mut self) -> bool {
        let Some(batch) = self.group_batch.as_mut() else {
            return false;
        };
        let Some(next) = batch.rest.pop_front() else {
            return false;
        };
        batch.writing = next;
        true
    }

    fn own_logical_log_record(
        &mut self,
        mvcc_store: &Arc<MvStore<Clock, A>>,
        ticket: Option<u64>,
        owner_tx: TxID,
    ) -> Result<bool> {
        let append_bytes = self.pending_log_append_bytes.take().ok_or_else(|| {
            LimboError::InternalError(
                "logical log record completed without pending byte count".to_string(),
            )
        })?;
        mvcc_store
            .storage
            .advance_logical_log_offset_after_success(append_bytes)?;
        if let Some(ticket) = ticket {
            self.commit_coordinator.note_written(ticket);
            if let Some(batch) = self.group_batch.as_mut() {
                batch.advanced_through = Some(ticket);
            }
        }
        if let Some(tx) = mvcc_store.txs.get(&owner_tx) {
            tx.value().log_appended.store(true, Ordering::Release);
        }
        self.commit_coordinator.clear_issued();
        if owner_tx != self.tx_id && self.commit_coordinator.take_abandoned(owner_tx) {
            self.finish_abandoned_group_waiter(mvcc_store, owner_tx)?;
        }
        self.wrote_logical_log = true;
        Ok(owner_tx == self.tx_id)
    }

    fn finish_abandoned_group_waiter(
        &self,
        mvcc_store: &Arc<MvStore<Clock, A>>,
        owner_tx: TxID,
    ) -> Result<()> {
        let Some(tx) = mvcc_store.txs.get(&owner_tx) else {
            return Ok(());
        };
        let end_ts = match tx.value().state.load() {
            TransactionState::Preparing(end_ts) | TransactionState::Committed(end_ts) => end_ts,
            other => {
                return Err(LimboError::InternalError(format!(
                    "abandoned group waiter {owner_tx} in {other:?}"
                )));
            }
        };
        tx.value().state.store(TransactionState::Committed(end_ts));
        drop(tx);
        mvcc_store.rewrite_live_versions_for_committed_tx(owner_tx, end_ts);
        if let Some(tx) = mvcc_store.txs.get(&owner_tx) {
            mvcc_store.unlock_commit_lock_if_held(tx.value());
        }
        crate::without_allocation_faults!(mvcc_store.remove_tx(owner_tx).expect(ALLOC_ERR_MSG));
        Ok(())
    }

    fn take_writing_log_record(&mut self, end_ts: u64, alloc: DynAllocator) -> LogRecord {
        let batch = self
            .group_batch
            .as_mut()
            .expect("group batch holds the record being written");
        std::mem::replace(
            &mut batch.writing.log_record,
            LogRecord::empty(end_ts, alloc),
        )
    }

    fn advance_group_or_sync(&mut self, end_ts: u64, alloc: DynAllocator) -> CommitState<Clock, A> {
        if self.take_next_group_record() {
            let log_record = self.take_writing_log_record(end_ts, alloc);
            CommitState::UpgradeLogicalLogHeader { end_ts, log_record }
        } else {
            CommitState::SyncLogicalLog { end_ts }
        }
    }

    fn rebuild_log_record(
        &mut self,
        mvcc_store: &Arc<MvStore<Clock, A>>,
        end_ts: u64,
    ) -> Result<()> {
        let tx = mvcc_store
            .txs
            .get(&self.tx_id)
            .ok_or_else(|| LimboError::NoSuchTransactionID(self.tx_id.to_string()))?;
        let tx = tx.value();
        let pending_header = if tx.header_dirty.load(Ordering::Acquire) {
            Some(*tx.header.read())
        } else {
            None
        };
        self.state = CommitState::BuildLogRecord(BuildLogRecordCtx {
            end_ts,
            log_record: LogRecord::new(end_ts, mvcc_store.logical_log_allocator())?,
            cursor: 0,
            schema_process: true,
            pending_header,
        });
        Ok(())
    }

    fn step_await_group_commit(
        &mut self,
        mvcc_store: &Arc<MvStore<Clock, A>>,
        end_ts: u64,
        ticket: u64,
    ) -> Result<TransitionResult<()>> {
        if self.commit_coordinator.durable_through() >= ticket {
            self.state = CommitState::EndCommitLogicalLog { end_ts };
            return Ok(TransitionResult::Continue);
        }
        if self.commit_coordinator.take_retry(ticket) {
            self.rebuild_log_record(mvcc_store, end_ts)?;
            return Ok(TransitionResult::Continue);
        }
        if !self.commit_coordinator.pager_commit_lock.write() {
            return Ok(TransitionResult::Io(IOCompletions(Completion::new_yield())));
        }
        if self.commit_coordinator.take_retry(ticket) {
            self.commit_coordinator.pager_commit_lock.unlock();
            self.rebuild_log_record(mvcc_store, end_ts)?;
            return Ok(TransitionResult::Continue);
        }
        let tx = match mvcc_store.txs.get(&self.tx_id) {
            Some(tx) => tx,
            None => {
                self.commit_coordinator.pager_commit_lock.unlock();
                return Err(LimboError::NoSuchTransactionID(self.tx_id.to_string()));
            }
        };
        match self.commit_coordinator.take_work() {
            GroupWork::Lead { writing, rest } => {
                tx.value()
                    .pager_commit_lock_held
                    .store(true, Ordering::Release);
                self.group_batch = Some(GroupBatch::from_lead(writing, rest));
                let log_record =
                    self.take_writing_log_record(end_ts, mvcc_store.logical_log_allocator());
                self.state = CommitState::UpgradeLogicalLogHeader { end_ts, log_record };
                Ok(TransitionResult::Continue)
            }
            GroupWork::SyncPrefix => {
                tx.value()
                    .pager_commit_lock_held
                    .store(true, Ordering::Release);
                self.state = CommitState::SyncGroupPrefix { end_ts, ticket };
                Ok(TransitionResult::Continue)
            }
            GroupWork::None => {
                self.commit_coordinator.pager_commit_lock.unlock();
                Ok(TransitionResult::Io(IOCompletions(Completion::new_yield())))
            }
        }
    }

    fn end_read_tx_for_db(&self) {
        if let Ok(pager) = self.connection.get_pager_from_database_index(&self.db_id) {
            pager.end_read_tx();
            return;
        }
        self.pager.end_read_tx();
    }

    /// Validates commit-time write-write conflicts for one table row key.
    ///
    /// Returns [LimboError::WriteWriteConflict] when another transaction committed or is
    /// preparing a conflicting version according to first-committer-wins.
    fn check_rowid_for_conflicts(
        &self,
        rowid: &RowID,
        end_ts: u64,
        tx: &Transaction<A>,
        mvcc_store: &Arc<MvStore<Clock, A>>,
    ) -> Result<()> {
        let row_versions = mvcc_store.rows.get(rowid);
        if row_versions.is_none() {
            return Ok(());
        }

        let row_versions = row_versions.unwrap();
        let row_versions = row_versions.value();
        let row_versions = row_versions.read();

        self.check_version_conflicts(end_ts, tx, mvcc_store, &row_versions)?;
        Ok(())
    }

    /// Validates commit-time write-write conflicts for one index key.
    ///
    /// Returns [LimboError::WriteWriteConflict] when another transaction committed or is
    /// preparing a conflicting index version according to first-committer-wins.
    fn check_index_for_conflicts(
        &self,
        rowid: &RowID,
        end_ts: u64,
        tx: &Transaction<A>,
        mvcc_store: &Arc<MvStore<Clock, A>>,
    ) -> Result<()> {
        let RowKey::Record(record) = &rowid.row_id else {
            panic!("invalid index row_id type, should be Record")
        };
        if !record.metadata.is_unique {
            // Skip indexes which are not unique or not primary key
            return Ok(());
        }
        // A key without an appended rowid (an index method's backing B-tree)
        // is unique over the whole key; keys with a rowid are unique over
        // everything before it.
        let num_indexed_cols = if record.metadata.has_rowid {
            record.metadata.num_cols.saturating_sub(1) // exclude rowid column
        } else {
            record.metadata.num_cols
        };
        // In SQLite, NULLs don't violate UNIQUE constraints - skip conflict check for keys containing NULL
        if record.contains_null(num_indexed_cols)? {
            return Ok(());
        }

        // Create a prefix key over the indexed columns for range lookup.
        // Due to SortableIndexKey's Ord using min(num_cols), this key compares Equal
        // to all entries with the same indexed columns (regardless of rowid).
        let prefix_key = {
            let mut index_info = record.metadata.as_ref().clone();
            index_info.num_cols = num_indexed_cols;
            SortableIndexKey {
                key: record.key.clone(),
                metadata: Arc::new(index_info),
            }
        };

        let table_id = rowid.table_id;
        let index_rows = mvcc_store
            .index_rows
            .get(&table_id)
            .unwrap_or_else(|| panic!("expected index {table_id:?}"));
        let index_rows = index_rows.value();

        // Use range to efficiently find all entries that match the prefix.
        // Since entries are ordered by Ord, all entries with the same indexed columns
        // are contiguous. We start from the prefix_key and stop when prefix no longer matches.
        for entry in index_rows.range::<SortableIndexKey, _>(&prefix_key..) {
            let other_key = entry.key();
            // Check if prefix still matches - if not, we've passed all matching entries
            if !record.matches_prefix(other_key, num_indexed_cols)? {
                break;
            }
            let row_versions = entry.value();
            let row_versions = row_versions.read();
            self.check_version_conflicts(end_ts, tx, mvcc_store, &row_versions)?;
        }

        Ok(())
    }

    /// Validates a single version chain against the current transaction's commit timestamp.
    ///
    /// This enforces snapshot-isolation conflict checks for both:
    /// 1. versions ended by concurrent commits (`end > tx.begin_ts`), and
    /// 2. live versions owned by concurrent transactions (state/ts tie-breaking).
    fn check_version_conflicts(
        &self,
        end_ts: u64,
        tx: &Transaction<A>,
        mvcc_store: &Arc<MvStore<Clock, A>>,
        row_versions: &[RowVersion],
    ) -> Result<()> {
        // Check for conflicts - iterate in reverse for faster early termination
        for version in row_versions.iter().rev() {
            // A row that we are trying to commit was deleted/updated by another
            // committed transaction after our begin timestamp. Even if that
            // version is now "ended", this is still a write-write conflict.
            if let Some(TxTimestampOrID::Timestamp(end_ts)) = version.end() {
                turso_assert!(
                    end_ts != tx.begin_ts,
                    "committed end_ts and begin_ts cannot be equal: txn timestamps are strictly monotonic"
                );
                if end_ts > tx.begin_ts {
                    return Err(LimboError::WriteWriteConflict);
                }
            }

            // B-tree tombstones (begin: None, end: TxID) act as write locks.
            // When another transaction has created a tombstone to delete a
            // B-tree-resident row, that tombstone is effectively a write lock
            // on the row — same as Hekaton's End field. We must detect this
            // as a write-write conflict using the same state-based logic used
            // for begin: TxID checks below.
            if version.begin().is_none() {
                // Committed tombstones (end: Timestamp) are already handled by
                // the check above at lines 1070-1074. Here we only need to check
                // in-flight tombstones (end: TxID) from other transactions.
                if let Some(TxTimestampOrID::TxID(other_tx_id)) = version.end() {
                    if other_tx_id != self.tx_id {
                        // The other transaction may already have moved from
                        // `txs` to `finalized_tx_states`; consult both
                        // instead of panicking on the race.
                        let other_state = lookup_tx_state(
                            &mvcc_store.txs,
                            &mvcc_store.finalized_tx_states,
                            other_tx_id,
                        );
                        match other_state {
                            Some(TransactionState::Committed(_)) => {
                                return Err(LimboError::WriteWriteConflict);
                            }
                            Some(TransactionState::Preparing(other_end_ts)) => {
                                if other_end_ts < end_ts {
                                    return Err(LimboError::WriteWriteConflict);
                                }
                            }
                            Some(TransactionState::Active) => {}
                            Some(TransactionState::Aborted | TransactionState::Terminated)
                            | None => {}
                        }
                    }
                }
                // Tombstones have no meaningful begin field — skip begin checks
                continue;
            }

            match version.end() {
                Some(TxTimestampOrID::Timestamp(end_ts)) => {
                    // Committed deletion. If end_ts > our begin_ts, the conflict
                    // would have been already caught earlier when we iterate through
                    // the row versions in reverse. If end_ts < our
                    // begin_ts, the deletion predates our snapshot — no conflict.
                    turso_assert!(
                        end_ts < tx.begin_ts,
                        "row version's end_ts cannot be greater than txns begin_ts"
                    );
                    continue;
                }
                Some(TxTimestampOrID::TxID(end_tx_id)) => {
                    // Deletion not yet finalized; the deleting transaction may still be in Preparing.
                    if end_tx_id == self.tx_id {
                        // We deleted this version ourselves, so it cannot conflict with our commit.
                        continue;
                    }

                    match lookup_tx_state(
                        &mvcc_store.txs,
                        &mvcc_store.finalized_tx_states,
                        end_tx_id,
                    ) {
                        Some(TransactionState::Committed(committed_end_ts)) => {
                            turso_assert!(
                                committed_end_ts != tx.begin_ts,
                                "committed end_ts and begin_ts cannot be equal: txn timestamps are strictly monotonic"
                            );
                            if committed_end_ts > tx.begin_ts {
                                return Err(LimboError::WriteWriteConflict);
                            }
                            continue;
                        }
                        _ => {
                            // Deleting tx is Active, Preparing, Aborted, or gone.
                            // The deletion may not stick, so this version may still be live.
                            // Fall through to check begin for conflicts.
                        }
                    }
                }
                None => {
                    // No end — version is live. Fall through to check begin.
                }
            }

            match version.begin() {
                Some(TxTimestampOrID::TxID(other_tx_id)) => {
                    // Skip our own version
                    if other_tx_id == self.tx_id {
                        continue;
                    }
                    // Another transaction's uncommitted version - check their state
                    match lookup_tx_state(
                        &mvcc_store.txs,
                        &mvcc_store.finalized_tx_states,
                        other_tx_id,
                    ) {
                        // Other tx already committed = conflict
                        Some(TransactionState::Committed(_)) => {
                            return Err(LimboError::WriteWriteConflict);
                        }
                        // Both preparing - compare end_ts (lower wins)
                        Some(TransactionState::Preparing(other_end_ts)) => {
                            if other_end_ts < end_ts {
                                // Other tx has lower end_ts, they win
                                return Err(LimboError::WriteWriteConflict);
                            }
                            // We have lower end_ts, we win - they'll abort when they validate
                        }
                        // Other tx still active - we're already Preparing so we're ahead
                        // They'll see us in Preparing/Committed when they try to commit
                        Some(TransactionState::Active) => {}
                        // Other tx aborted - no conflict
                        Some(TransactionState::Aborted) | Some(TransactionState::Terminated) => {}
                        None => {
                            // TODO: an aborted txn should not affect another one.. properly handle
                            // this case, but for now be conservative and treat as conflict to avoid
                            // potential correctness issues
                            tracing::debug!(
                                "check_version_conflicts: missing tx {} for row version {:?}; conservatively treating as conflict",
                                other_tx_id,
                                version
                            );
                            return Err(LimboError::WriteWriteConflict);
                        }
                    }
                }
                Some(TxTimestampOrID::Timestamp(begin_ts)) => {
                    // A live committed version with this rowid exists.
                    // begin_ts >= tx.begin_ts: a concurrent transaction committed a row
                    //   with this rowid after our snapshot — invisible to NotExists.
                    // begin_ts < tx.begin_ts: the row predates our snapshot. NotExists
                    //   should have seen it at INSERT time, so this is a defensive guard.
                    let _ = begin_ts;
                    return Err(LimboError::WriteWriteConflict);
                }
                None => {
                    // Invalid version
                }
            }
        }
        Ok(())
    }

    /// Run one chunked step of `BuildLogRecord`. Processes up to
    /// `MVCC_COMMIT_BATCH_SIZE` rowids per call, then yields. Schema rows
    /// (table_id == SQLITE_SCHEMA_MVCC_TABLE_ID) are emitted before data rows
    /// in two passes so that log replay sees CREATE TABLE before INSERTs.
    fn step_build_log_record(
        &mut self,
        mvcc_store: &Arc<MvStore<Clock, A>>,
    ) -> Result<TransitionResult<()>> {
        // First entry into BuildLogRecord (no chunk processed yet): a yield-
        // point to bracket the chunked yields so tests can count them exactly.
        // Must run before the `&mut self.state` re-bind below — the macro
        // calls `self.yield_context()` which needs `&self`.
        let is_first_entry = matches!(
            self.state,
            CommitState::BuildLogRecord(BuildLogRecordCtx {
                cursor: 0,
                schema_process: true,
                ..
            })
        );
        if is_first_entry {
            inject_transition_yield!(self, CommitYieldPoint::BuildLogRecordStart);
        }

        let tx_id = self.tx_id;
        let tx_entry = mvcc_store.txs.get(&tx_id);
        let tx = tx_entry
            .as_ref()
            .map(|entry| entry.value())
            .ok_or_else(|| {
                LimboError::NoSuchTransactionID(format!(
                    "tx id {tx_id} not found in step_build_logical_record"
                ))
            })?;
        let write_set_len = tx.write_set.lock().entries.len();
        #[cfg(feature = "conn_raw_api")]
        let connection = Arc::clone(&self.connection);
        let CommitState::BuildLogRecord(ctx) = &mut self.state else {
            unreachable!("step_build_log_record requires BuildLogRecord state")
        };
        let end_ts = ctx.end_ts;

        // A sqlite_schema row that is inserted and deleted inside one
        // transaction does not always mean the underlying table or index was
        // inserted and deleted. ALTER TABLE can rewrite sqlite_schema several
        // times for an existing root page. We only treat a root page as
        // transaction-local when it has no schema row before the transaction and
        // no schema row after it.

        let is_our_begin = |row_version: &RowVersion| {
            matches!(
                row_version.begin(),
                Some(TxTimestampOrID::TxID(vid)) if vid == tx_id
            )
        };
        let is_our_end = |row_version: &RowVersion| {
            matches!(
                row_version.end(),
                Some(TxTimestampOrID::TxID(vid)) if vid == tx_id
            )
        };

        let mut btree_ids_created_and_dropped_in_tx: HashSet<MVTableId> = HashSet::default();
        let mut btree_ids_removed_from_schema_by_tx: HashSet<MVTableId> = HashSet::default();
        {
            let write_set = tx.write_set.lock();
            let frame_writes_schema = write_set
                .entries
                .iter()
                .any(|(id, _)| id.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID);

            // Decoding sqlite_schema records is only needed for DDL frames.
            // Normal DML frames do not touch sqlite_schema, so avoid parsing
            // schema records while committing the common path.
            if frame_writes_schema {
                let mut schema_roots_created_and_deleted_in_tx: HashSet<i64> = HashSet::default();
                let mut schema_roots_deleted_by_tx: HashSet<i64> = HashSet::default();
                let mut schema_roots_present_before_tx: HashSet<i64> = HashSet::default();
                let mut schema_roots_present_after_tx: HashSet<i64> = HashSet::default();

                for (id, row_versions) in &write_set.entries {
                    // Only sqlite_schema entries can yield a btree identity. The
                    // common path (CREATE INDEX on a populated table, bulk DML)
                    // has tens of thousands of write_set entries that are NOT
                    // sqlite_schema; locking each and parsing every version was
                    // the dominant cost of MVCC commit on this branch.
                    if id.table_id != SQLITE_SCHEMA_MVCC_TABLE_ID {
                        continue;
                    }
                    for row_version in row_versions.read().iter() {
                        let Some(identity) = sqlite_schema_btree_identity(row_version) else {
                            continue;
                        };
                        let our_begin = is_our_begin(row_version);
                        let our_end = is_our_end(row_version);
                        if !our_begin {
                            schema_roots_present_before_tx.insert(identity.root_page);
                        }
                        if our_end {
                            schema_roots_deleted_by_tx.insert(identity.root_page);
                        }
                        if !our_end {
                            schema_roots_present_after_tx.insert(identity.root_page);
                        }
                        if our_begin && our_end && !row_version.btree_resident {
                            schema_roots_created_and_deleted_in_tx.insert(identity.root_page);
                        }
                    }
                }

                for root_page in schema_roots_deleted_by_tx {
                    if !schema_roots_present_after_tx.contains(&root_page) {
                        btree_ids_removed_from_schema_by_tx
                            .insert(mvcc_store.get_table_id_from_root_page(root_page));
                    }
                }

                for root_page in schema_roots_created_and_deleted_in_tx {
                    if !schema_roots_present_before_tx.contains(&root_page)
                        && !schema_roots_present_after_tx.contains(&root_page)
                    {
                        btree_ids_created_and_dropped_in_tx
                            .insert(mvcc_store.get_table_id_from_root_page(root_page));
                    }
                }
            }
        }

        // Remap a table_id to its canonical form for the log. After checkpoint,
        // a table's in-memory table_id (e.g. -53) may differ from -(root_page)
        // (e.g. -58). On recovery, bootstrap reconstructs the map using
        // -(root_page), so log records must use that canonical form to be found.
        let canonicalize_table_id = |version: &mut RowVersion| {
            let table_id = version.row.id.table_id;
            if table_id == SQLITE_SCHEMA_MVCC_TABLE_ID {
                return;
            }
            if let Some(entry) = mvcc_store.table_id_to_rootpage.get(&table_id) {
                if let Some(root_page) = entry.value().root_page {
                    let canonical = MVTableId::from(-(root_page as i64));
                    if canonical != table_id {
                        version.row.id.table_id = canonical;
                    }
                }
            }
        };

        let collect_versions = |row_versions: &RowVersions<A>,
                                log_record: &mut LogRecord|
         -> Result<()> {
            // `log_record.row_versions` is the logical transaction log. Recovery
            // replays it in this order. `insert_version_raw` is for the versions
            // of one MVCC entry: one table row, one sqlite_schema row, or one
            // index entry. Its timestamp sort is correct for that one entry, but
            // it is not a rule for ordering the whole transaction.
            //
            // Example: the db file already has table t, index idx, and
            // t(rowid=1). One transaction runs `DELETE FROM t`, then
            // `ALTER TABLE t ADD COLUMN x`. ALTER TABLE records a DELETE for the
            // old sqlite_schema row for t and an UPSERT for the replacement row.
            // DELETE FROM t records a DELETE for t(rowid=1) and a DELETE_INDEX
            // for the idx entry that pointed at that row. The schema DELETE,
            // table-row DELETE, and DELETE_INDEX are all for entries already in
            // the db file, so their `begin` is None. Sorting the whole
            // transaction with `insert_version_raw` can put those three deletes
            // before the schema UPSERT:
            // `[DELETE schema(t), DELETE t(rowid=1), DELETE_INDEX idx(rowid=1),
            //   UPSERT schema(t)]`.
            //
            // Index log ops contain serialized index keys, not CREATE INDEX SQL.
            // Recovery now decodes every index op using schema snapshots for the
            // whole transaction frame, so it does not install a half-updated
            // schema while the frame is still being replayed. The writer still
            // must not scramble a sqlite_schema DELETE+UPSERT pair with unrelated
            // table/index entries; keeping each write-set entry together gives
            // recovery a frame whose final schema can be understood.
            //
            // The filtering below has four separate jobs:
            //
            // 1. Omit all entries for a table/index root page that had no
            //    sqlite_schema row before this transaction and has no
            //    sqlite_schema row after it. Example: CREATE INDEX followed by
            //    DROP INDEX in one transaction.
            // 2. Omit inserts and updates for a table/index root page that is
            //    removed from sqlite_schema by this transaction. Example:
            //    UPDATE writes a new entry into idx_old, then DROP INDEX
            //    idx_old runs before COMMIT. The old index-entry deletes still
            //    matter, but new entries for idx_old cannot survive the frame.
            // 3. Omit one version that was created and deleted by this
            //    transaction before it reached the database file. Such a version
            //    does not change durable state, and recovery may not have enough
            //    schema information to decode a delete for it.
            // 4. If this entry has a delete for the row that already existed in
            //    the database file, do not also log a same-transaction
            //    create/delete replacement for the same write-set entry. Example:
            //    ALTER TABLE rewrites an index sqlite_schema row, then DROP INDEX
            //    deletes that replacement in the same transaction. The durable
            //    change is one delete of the original sqlite_schema row.
            //
            // What the code below does after that filtering:
            // - look only at this one write-set entry (`row_versions`);
            // - copy the versions written or ended by the committing transaction;
            // - if this same entry appears twice with the same
            //   `begin=Timestamp(...)`, keep the later one, because recovery should
            //   not replay an intermediate value for the same table row,
            //   sqlite_schema row, or index entry;
            // - append those versions to `log_record.row_versions` without sorting
            //   them against versions from other write-set entries.

            let mut entry_versions: Vec<RowVersion> = Vec::new();
            let row_versions = row_versions.read();

            // A tombstone over a row that was already in the B-tree before this
            // tx (begin=None, end=tx_id, btree_resident=true) is the canonical
            // log record for deleting that durable row. If the same entry also
            // contains a version this tx both began and ended over a B-tree row
            // (e.g. DELETE; INSERT; DELETE on a btree-resident rowid), the
            // tombstone already covers the durable delete and the begun+ended
            // version must be suppressed to avoid logging the same delete twice.
            let has_tombstone_for_btree_row = row_versions.iter().any(|row_version| {
                !is_our_begin(row_version) && is_our_end(row_version) && row_version.btree_resident
            });

            // Helper that returns Some(row_version) if our tx contributed to it and if we must therefore log it.
            let our_committed_image = |row_version: &RowVersion| -> Option<RowVersion> {
                let our_begin = is_our_begin(row_version);
                let our_end = is_our_end(row_version);
                if !our_begin && !our_end {
                    // row_version belongs to another tx
                    return None;
                }
                if btree_ids_created_and_dropped_in_tx.contains(&row_version.row.id.table_id) {
                    // This table or index has no sqlite_schema row before the
                    // transaction and no sqlite_schema row after it. It was created
                    // and dropped inside this commit, so its table/index entries do
                    // not change durable state.
                    return None;
                }
                if btree_ids_removed_from_schema_by_tx.contains(&row_version.row.id.table_id)
                    && our_begin
                    && !our_end
                {
                    // This transaction created or rewrote an entry for a table or
                    // index that is gone from sqlite_schema by COMMIT. Example:
                    // UPDATE writes a new entry into idx_old, then DROP INDEX
                    // idx_old runs before COMMIT. Keep deletes for entries that
                    // existed before the transaction, but do not log new entries
                    // that cannot exist after the transaction.
                    return None;
                }
                if our_begin && our_end {
                    // A begun+ended version is purely in-memory unless it
                    // shadows a B-tree row (insert_btree_resident_to_table_or_index
                    // then delete). For btree_resident=true we still need to log
                    // the delete of the durable row, UNLESS a sibling tombstone
                    // in this same entry already covers it.
                    if !row_version.btree_resident || has_tombstone_for_btree_row {
                        return None;
                    }
                }

                let mut committed = row_version.clone();
                if our_begin {
                    // New version is valid STARTING FROM the committing
                    // transaction's end timestamp. See Hekaton page 299.
                    committed.set_begin(Some(TxTimestampOrID::Timestamp(end_ts)));

                    if !our_end {
                        // A version row_version we inserted may have row_version.end() == tx_b.tx_id,
                        // where tx_b is a concurrent tx. This is because when a tx transitions to
                        // Preparing, its row_version becomes *speculatively updatable*, and a tx tx_b
                        // is allowed to change row_version.end() from None to tx_b.tx_id to delete it
                        // (see the Hekaton paper, §3.1, heading "check updatability").
                        //
                        // That deletion is tx_b's contribution, and tx_b will log it on its own commit.
                        // Our log record must capture our own contribution, but if the `end` field is
                        // set, it will be serialized as a OP_DELETE_* in the logical log, so we unset
                        // it so that it will be serialized as an OP_UPSERT_*. tx_b will take care of
                        // logging the deletion.
                        committed.set_end(None);
                    }
                }
                if our_end {
                    // Old version is valid UNTIL the committing
                    // transaction's end timestamp. See Hekaton page 299.
                    committed.set_end(Some(TxTimestampOrID::Timestamp(end_ts)));
                }
                Some(committed)
            };

            for row_version in row_versions.iter() {
                let Some(mut committed_version) = our_committed_image(row_version) else {
                    continue;
                };
                canonicalize_table_id(&mut committed_version);
                let is_btree_resident_delete_marker =
                    |version: &RowVersion| version.btree_resident && version.end().is_some();
                let replaces_last = entry_versions.last().is_some_and(|last| {
                    last.row.id == committed_version.row.id
                        && !is_btree_resident_delete_marker(last)
                        && matches!(
                            (&last.begin(), &committed_version.begin()),
                            (
                                Some(TxTimestampOrID::Timestamp(existing)),
                                Some(TxTimestampOrID::Timestamp(new))
                            ) if existing == new
                        )
                });
                if replaces_last {
                    *entry_versions
                        .last_mut()
                        .expect("last version checked above") = committed_version;
                    continue;
                }
                #[cfg(debug_assertions)]
                {
                    let same_row_and_begin = |existing: &RowVersion| {
                        existing.row.id == committed_version.row.id
                            && !is_btree_resident_delete_marker(existing)
                            && !is_btree_resident_delete_marker(&committed_version)
                            && matches!(
                                (&existing.begin(), &committed_version.begin()),
                                (
                                    Some(TxTimestampOrID::Timestamp(existing)),
                                    Some(TxTimestampOrID::Timestamp(new))
                                ) if existing == new
                            )
                    };
                    turso_assert!(
                        !entry_versions.iter().any(same_row_and_begin),
                        "one write-set entry produced non-adjacent log versions with the same row id and commit timestamp"
                    );
                }
                entry_versions.push(committed_version);
            }
            for committed_version in &entry_versions {
                #[cfg(feature = "conn_raw_api")]
                let portable_extension = portable_delete_op_extension_for_row_version(
                    &connection,
                    mvcc_store,
                    committed_version,
                )?;
                #[cfg(not(feature = "conn_raw_api"))]
                let portable_extension: Option<Vec<u8>> = None;
                mvcc_store.storage.serialize_row_version(
                    log_record,
                    committed_version,
                    portable_extension.as_deref(),
                )?;
            }
            Ok(())
        };

        // Process schema rows (sqlite_schema) before data/index rows so that
        // replay sees table_id_to_rootpage updates before row ops reference
        // those ids. `tx.write_set` preserves first-touch order, so mixed DDL
        // and DML in one transaction cannot rely on write-set order alone.
        let mut iterations = 0;

        let write_set = tx.write_set.lock();
        while ctx.cursor < write_set_len && iterations < MVCC_COMMIT_BATCH_SIZE {
            let (id, row_versions) = &write_set.entries[ctx.cursor];
            let is_schema = id.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID;
            // schema_process=true: schema rows only, false: data rows only.
            let process = if ctx.schema_process {
                is_schema
            } else {
                !is_schema
            };
            if process {
                collect_versions(row_versions, &mut ctx.log_record)?;
            }
            ctx.cursor += 1;
            iterations += 1;
        }
        if ctx.cursor < write_set_len {
            // More work remains in the current pass: yield and resume.
            return Ok(TransitionResult::Io(IOCompletions(Completion::new_yield())));
        }

        if ctx.schema_process {
            // Schema pass done; start the data pass from the top.
            ctx.schema_process = false;
            ctx.cursor = 0;
            return Ok(TransitionResult::Continue);
        }

        if let Some(header) = ctx.pending_header.take() {
            mvcc_store
                .storage
                .serialize_database_header(&mut ctx.log_record, &header)?;
        }

        // Move the assembled log record out and transition to
        // BeginCommitLogicalLog (or directly to CommitEnd if there is nothing
        // to log).
        let mut log_record = std::mem::replace(
            &mut ctx.log_record,
            LogRecord::empty(end_ts, mvcc_store.logical_log_allocator()),
        );
        self.populate_portable_changes(mvcc_store, &mut log_record)?;
        tracing::trace!("prepared_log_record(tx_id={})", self.tx_id);

        if log_record.is_empty() {
            // Nothing to log. We still need to release the commit lock here
            // if this is an exclusive tx, mirroring the pre-chunk path
            // through WaitForDependencies.
            if mvcc_store.is_exclusive_tx(&self.tx_id) {
                if let Some(tx_entry) = mvcc_store.txs.get(&self.tx_id) {
                    mvcc_store.unlock_commit_lock_if_held(tx_entry.value());
                }
            }
            self.state = CommitState::CommitEnd { end_ts };
        } else {
            self.state = CommitState::BeginCommitLogicalLog { end_ts, log_record };
        }
        inject_transition_yield!(self, CommitYieldPoint::LogRecordPrepared);
        Ok(TransitionResult::Continue)
    }

    fn populate_portable_changes(
        &self,
        mvcc_store: &Arc<MvStore<Clock, A>>,
        log_record: &mut LogRecord,
    ) -> Result<()> {
        #[cfg(not(feature = "conn_raw_api"))]
        {
            let _ = mvcc_store;
            let _ = log_record;
            Ok(())
        }

        #[cfg(feature = "conn_raw_api")]
        {
            if !self.connection.portable_logical_changes_enabled() {
                return Ok(());
            }
            log_record.portable_changes_enabled = true;

            let mut builder = PortableLogicalBuilder::new();
            let mut metadata: Vec<_> = self
                .connection
                .mvcc_log_meta_snapshot()
                .into_iter()
                .collect();
            metadata.sort_by(|a, b| a.0.cmp(&b.0));
            for (key, value) in &metadata {
                builder.add_metadata(key, value)?;
            }

            // The recovery payload is the single durable operation stream.
            // The portable extension only adds the metadata required to interpret
            // recovery table ids outside this database instance.
            let mut table_refs_by_id = HashMap::default();
            let recovery_payload = &log_record.buf[LOG_RECORD_PREFIX_SIZE..];
            let parsed_ops = parse_ops_from_plaintext(
                recovery_payload,
                recovery_payload.len(),
                log_record.op_count,
                log_record.tx_timestamp,
            )?;

            let mut schema_upserts = HashMap::default();
            let mut schema_deletes = HashMap::default();
            let mut schema_rowids = Vec::new();
            let mut data_table_ids = HashSet::default();
            let mut has_portable_schema_changes = false;
            for op in &parsed_ops {
                match op {
                    ParsedOp::UpsertTable {
                        table_id,
                        rowid,
                        record_bytes,
                        ..
                    } if *table_id == SQLITE_SCHEMA_MVCC_TABLE_ID => {
                        let rowid = rowid.row_id.to_int_or_panic();
                        let row = portable_schema_row_from_record(record_bytes)?;
                        has_portable_schema_changes |= is_portable_schema_row(&row);
                        schema_rowids.push(rowid);
                        schema_upserts.insert(rowid, row);
                    }
                    ParsedOp::DeleteTable {
                        rowid,
                        record_bytes,
                        ..
                    } if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID => {
                        if record_bytes.is_empty() {
                            return Err(LimboError::Corrupt(
                                "sqlite_schema DELETE_TABLE missing old record".to_string(),
                            ));
                        }
                        let schema_rowid = rowid.row_id.to_int_or_panic();
                        let row = portable_schema_row_from_record(record_bytes)?;
                        has_portable_schema_changes |= is_portable_schema_row(&row);
                        schema_rowids.push(schema_rowid);
                        schema_deletes.insert(schema_rowid, row);
                    }
                    ParsedOp::UpsertTable { table_id, .. } => {
                        data_table_ids.insert(*table_id);
                    }
                    ParsedOp::DeleteTable { rowid, .. } => {
                        if rowid.table_id != SQLITE_SCHEMA_MVCC_TABLE_ID {
                            data_table_ids.insert(rowid.table_id);
                        }
                    }
                    ParsedOp::UpsertIndex { .. }
                    | ParsedOp::DeleteIndex { .. }
                    | ParsedOp::UpdateHeader { .. } => {}
                }
            }

            schema_rowids.sort_unstable();
            schema_rowids.dedup();
            for rowid in schema_rowids {
                let old_row = schema_deletes.get(&rowid);
                let new_row = schema_upserts.get(&rowid);
                match (old_row, new_row) {
                    (Some(old_row), Some(new_row)) => {
                        if is_portable_table_schema_row(old_row) {
                            table_refs_by_id.insert(
                                portable_table_id_from_rootpage(old_row.rootpage),
                                PortableTableRef {
                                    name: old_row.name.clone(),
                                },
                            );
                        }
                        if is_portable_table_schema_row(new_row) {
                            table_refs_by_id.insert(
                                portable_table_id_from_rootpage(new_row.rootpage),
                                PortableTableRef {
                                    name: new_row.name.clone(),
                                },
                            );
                        }
                    }
                    (None, Some(new_row)) => {
                        if is_portable_table_schema_row(new_row) {
                            table_refs_by_id.insert(
                                portable_table_id_from_rootpage(new_row.rootpage),
                                PortableTableRef {
                                    name: new_row.name.clone(),
                                },
                            );
                        }
                    }
                    (Some(old_row), None) => {
                        if is_portable_table_schema_row(old_row) {
                            table_refs_by_id.insert(
                                portable_table_id_from_rootpage(old_row.rootpage),
                                PortableTableRef {
                                    name: old_row.name.clone(),
                                },
                            );
                        }
                    }
                    (None, None) => {}
                }
            }

            // A data table id parsed back out of the serialized payload is
            // already in its log form: either the schema's negative rootpage
            // placeholder (table not yet checkpointed) or the canonical
            // -(root_page) written by `canonicalize_table_id`. Interpret it
            // directly as a rootpage first. Consulting `table_id_to_rootpage`
            // first is wrong: its keys are the original in-memory counter ids,
            // so a canonical id like -159 can alias an unrelated object whose
            // counter id happened to be -159 (and now has a checkpointed root
            // page), sending resolution to the wrong rootpage. The map is only
            // a fallback for a payload id serialized before a concurrent
            // checkpoint published new roots.
            let rootpage_for_table_id = |table_id: MVTableId| -> i64 {
                let direct = i64::from(table_id);
                if table_name_for_rootpage(&self.connection, direct)
                    .or_else(|| table_name_for_rootpage_in_mvcc_schema(mvcc_store, direct))
                    .is_some()
                {
                    return direct;
                }
                mvcc_store
                    .table_id_to_rootpage
                    .get(&table_id)
                    .and_then(|entry| entry.value().root_page)
                    .map(|rootpage| rootpage as i64)
                    .unwrap_or(direct)
            };

            let mut unresolved_data_tables = Vec::new();
            let mut needed_rootpages = HashSet::default();
            for table_id in &data_table_ids {
                if table_refs_by_id.contains_key(table_id) {
                    continue;
                }
                let rootpage = rootpage_for_table_id(*table_id);
                if rootpage == 0 {
                    continue;
                }
                let root_table_id = portable_table_id_from_rootpage(rootpage);
                if let Some(table_ref) = table_refs_by_id.get(&root_table_id).cloned() {
                    table_refs_by_id.insert(*table_id, table_ref);
                    continue;
                }
                needed_rootpages.insert(rootpage);
                unresolved_data_tables.push((*table_id, root_table_id));
            }

            if !needed_rootpages.is_empty() {
                for rootpage in needed_rootpages {
                    let Some(name) = table_name_for_rootpage(&self.connection, rootpage)
                        .or_else(|| table_name_for_rootpage_in_mvcc_schema(mvcc_store, rootpage))
                    else {
                        continue;
                    };
                    let resolved_rootpage = if rootpage < 0 { -rootpage } else { rootpage };
                    let resolved_table_id = portable_table_id_from_rootpage(resolved_rootpage);
                    let table_ref = PortableTableRef { name };
                    table_refs_by_id.insert(resolved_table_id, table_ref);
                }

                for (table_id, root_table_id) in unresolved_data_tables {
                    if let Some(table_ref) = table_refs_by_id.get(&root_table_id).cloned() {
                        table_refs_by_id.insert(table_id, table_ref);
                    }
                }
            }

            for (table_id, table_ref) in &table_refs_by_id {
                if !is_portable_logical_name(&table_ref.name) {
                    continue;
                }
                let added = builder.add_object_map(PortableObjectMapEntry {
                    mv_table_id: i64::from(*table_id),
                    name: &table_ref.name,
                })?;
                turso_assert!(
                    added,
                    "portable object map unexpectedly rejected a user object"
                );
            }

            for table_id in data_table_ids {
                let Some(table_ref) = table_refs_by_id.get(&table_id) else {
                    return Err(LimboError::Corrupt(format!(
                        "portable changes cannot resolve user data table id {table_id}"
                    )));
                };
                if !is_portable_logical_name(&table_ref.name) {
                    continue;
                }
            }

            log_record.portable_changes = builder.finish()?;
            log_record.portable_changes_required = has_portable_schema_changes;
            Ok(())
        }
    }

    /// Run one chunked step of `RewriteLiveVersions`. Processes up to
    /// `MVCC_COMMIT_BATCH_SIZE` rowids per call, then yields. The transaction
    /// is already in the Committed state at this point; un-rewritten TxID
    /// references resolve via `txs[tx_id]` for visibility/conflict checks.
    fn step_rewrite_live_versions(
        &mut self,
        mvcc_store: &Arc<MvStore<Clock, A>>,
    ) -> Result<TransitionResult<()>> {
        let tx_id = self.tx_id;
        let tx_entry = mvcc_store.txs.get(&tx_id);
        let tx = tx_entry
            .as_ref()
            .map(|entry| entry.value())
            .ok_or_else(|| {
                LimboError::NoSuchTransactionID(format!(
                    "tx id {tx_id} not found in step_build_logical_record"
                ))
            })?;
        let write_set = tx.write_set.lock();
        let write_set_len = write_set.entries.len();
        let CommitState::RewriteLiveVersions(ctx) = &mut self.state else {
            unreachable!("step_rewrite_live_versions requires RewriteLiveVersions state")
        };
        let end_ts = ctx.end_ts;
        if ctx.cursor == 0 {
            let tx_state = mvcc_store
                .txs
                .get(&tx_id)
                .map(|entry| entry.value().state.load());
            turso_assert!(
                matches!(tx_state, Some(TransactionState::Committed(ts)) if ts == end_ts),
                "RewriteLiveVersions requires a committed transaction state"
            );
        }
        let mut iterations = 0;
        while ctx.cursor < write_set_len && iterations < MVCC_COMMIT_BATCH_SIZE {
            let (_id, row_versions) = &write_set.entries[ctx.cursor];
            let mut row_versions = row_versions.write();
            for row_version in row_versions.iter_mut() {
                row_version.rewrite_txid_to_timestamp(tx_id, end_ts);
            }
            ctx.cursor += 1;
            iterations += 1;
        }
        if ctx.cursor < write_set_len {
            return Ok(TransitionResult::Io(IOCompletions(Completion::new_yield())));
        }
        self.state = CommitState::FinalizeCommit { end_ts };
        Ok(TransitionResult::Continue)
    }
}

impl WriteRowStateMachine {
    fn new(row: Row, cursor: Arc<RwLock<BTreeCursor>>, requires_seek: bool) -> Self {
        Self {
            state: WriteRowState::Initial,
            is_finalized: false,
            row,
            record: None,
            cursor,
            requires_seek,
        }
    }
}

impl<Clock: LogicalClock, A: ConcurrentAllocator> StateTransition for CommitStateMachine<Clock, A> {
    type Context = Arc<MvStore<Clock, A>>;
    type SMResult = ();

    #[tracing::instrument(fields(state = ?self.state), skip(self, mvcc_store), level = Level::DEBUG)]
    fn step(&mut self, mvcc_store: &Self::Context) -> Result<TransitionResult<Self::SMResult>> {
        tracing::trace!("step(state={:?})", self.state);
        match &self.state {
            CommitState::Initial => {
                // NOTICE: the first shadowed tx keeps the entry alive in the map
                // for the duration of this whole function, which is important for correctness!
                let tx = mvcc_store
                    .txs
                    .get(&self.tx_id)
                    .ok_or(LimboError::TxTerminated)?;
                let tx = tx.value();
                match tx.state.load() {
                    TransactionState::Terminated => {
                        return Err(LimboError::TxTerminated);
                    }
                    _ => {
                        turso_assert_eq!(tx.state, TransactionState::Active);
                    }
                }

                // Atomically generate end_ts and publish Preparing(end_ts) while the
                // clock lock is held. This closes the TOCTOU window
                // Consider the example:
                //
                // tx1 (Active): get_ts for end - 10
                // tx2 (Active): got begin_ts - 11
                // tx2 (Active): does queries but does not see changes by tx1
                // tx1 (Preparing): now stores `end_ts(10)`
                // tx2 (Active): queries again, but now it can see changes by tx1
                //
                // hence we want to guard the timestamp generation by a mutex, only allow next
                // ts to generate when the previous one is used / discarded

                let write_set_is_empty = tx.write_set.lock().is_empty();
                let header_write = tx.header_dirty.load(Ordering::Acquire);
                // Read only is not only exclusive to empty write set, we could be writing the
                // database header here.
                let read_only = write_set_is_empty && !header_write;

                let mut schema_conflict = false;
                let mut exclusive_conflict = false;

                let end_ts = mvcc_store.get_commit_timestamp(|ts| {
                    turso_assert!(
                        ts > tx.begin_ts,
                        "end_ts must be strictly greater than begin_ts"
                    );

                    // First we check if there is exclusive conflict, if there is then we won't
                    // commit txn.
                    if !mvcc_store.is_exclusive_tx(&self.tx_id) && mvcc_store.has_exclusive_tx() {
                        // A non-CONCURRENT transaction is holding the exclusive lock, we must abort.
                        turso_assert_reachable!("commit aborted due to exclusive tx conflict");
                        exclusive_conflict = true;
                    }
                    // Now check if we saw schema change which would require reprepare. Let's note
                    // that we check this right after exlusive tx because we update this before we release
                    // exclusive lock.
                    let schema_updated = mvcc_store
                        .last_committed_schema_change_ts
                        .load(Ordering::Acquire)
                        > tx.begin_ts;
                    // last_committed_schema_ts is not enough, we need to check schema cookie
                    // is the same because e.g:
                    // T1 CREATE INDEX
                    // T1 Yield somewhere in middle of commit
                    // T1 end_ts = x
                    // T2 BEGIN CONCURRENT; INSERT (same table); COMMIT
                    // T2 begin_ts = x+1
                    // T2 begin_ts > end_ts
                    //
                    // Therefore even if exclusive tx (T1) finishes right in time
                    // we will see schema was not updated because we see an younger timestamp and
                    // ours is older!
                    //
                    let our_cookie = tx.header.read().schema_cookie.get();
                    let global_cookie = {
                        let h = mvcc_store.global_header.read();
                        let h = h.as_ref();
                        turso_assert!(h.is_some(), "global_header should be initialized");
                        h.unwrap().schema_cookie.get()
                    };

                    let header_dirty = tx.header_dirty.load(Ordering::Acquire);
                    turso_assert!(!header_dirty || mvcc_store.is_exclusive_tx(&tx.tx_id), "header_dirty=true implies that tx is exclusive");
                    if our_cookie != global_cookie && !header_dirty {
                        tracing::debug!("cookie mismatch in CommitState::Initial tx({our_cookie}) != global(!{global_cookie})");
                        schema_conflict = true;
                    }

                    if schema_updated {
                        tracing::debug!("schema ts is older than our ts");
                        // Schema changes made after the transaction began always cause a [SchemaConflict] error and the tx must abort.
                        schema_conflict = true;
                    }

                    let can_commit_tx = !(exclusive_conflict || schema_conflict);
                    if can_commit_tx || read_only {
                        tx.state.store(TransactionState::Preparing(ts));
                    }
                });
                // We allow reads from happening. Exlusive means there is a single writer.
                if exclusive_conflict && !read_only {
                    return Err(LimboError::WriteWriteConflict);
                }
                if schema_conflict && !read_only {
                    return Err(LimboError::SchemaConflict);
                }
                tracing::trace!("prepare_tx(tx_id={}, end_ts={})", self.tx_id, end_ts);
                /* In order to implement serializability, we need the following steps:
                **
                ** 1. Validate if all read versions are still visible by inspecting the read_set
                ** 2. Validate if there are no phantoms by walking the scans from scan_set (which we don't even have yet)
                **    - a phantom is a version that became visible in the middle of our transaction,
                **      but wasn't taken into account during one of the scans from the scan_set
                ** 3. Wait for commit dependencies, which we don't even track yet...
                **    Excerpt from what's a commit dependency and how it's tracked in the original paper:
                **    """
                        A transaction T1 has a commit dependency on another transaction
                        T2, if T1 is allowed to commit only if T2 commits. If T2 aborts,
                        T1 must also abort, so cascading aborts are possible. T1 acquires a
                        commit dependency either by speculatively reading or speculatively ignoring a version,
                        instead of waiting for T2 to commit.
                        We implement commit dependencies by a register-and-report
                        approach: T1 registers its dependency with T2 and T2 informs T1
                        when it has committed or aborted. Each transaction T contains a
                        counter, CommitDepCounter, that counts how many unresolved
                        commit dependencies it still has. A transaction cannot commit
                        until this counter is zero. In addition, T has a Boolean variable
                        AbortNow that other transactions can set to tell T to abort. Each
                        transaction T also has a set, CommitDepSet, that stores transaction IDs
                        of the transactions that depend on T.
                        To take a commit dependency on a transaction T2, T1 increments
                        its CommitDepCounter and adds its transaction ID to T2’s CommitDepSet.
                        When T2 has committed, it locates each transaction in
                        its CommitDepSet and decrements their CommitDepCounter. If
                        T2 aborted, it tells the dependent transactions to also abort by
                        setting their AbortNow flags. If a dependent transaction is not
                        found, this means that it has already aborted.
                        Note that a transaction with commit dependencies may not have to
                        wait at all - the dependencies may have been resolved before it is
                        ready to commit. Commit dependencies consolidate all waits into
                        a single wait and postpone the wait to just before commit.
                        Some transactions may have to wait before commit.
                        Waiting raises a concern of deadlocks.
                        However, deadlocks cannot occur because an older transaction never
                        waits on a younger transaction. In
                        a wait-for graph the direction of edges would always be from a
                        younger transaction (higher end timestamp) to an older transaction
                        (lower end timestamp) so cycles are impossible.
                    """
                **  If you're wondering when a speculative read happens, here you go:
                **  Case 1: speculative read of TB:
                    """If transaction TB is in the Preparing state, it has acquired an end
                        timestamp TS which will be V’s begin timestamp if TB commits.
                        A safe approach in this situation would be to have transaction T
                        wait until transaction TB commits. However, we want to avoid all
                        blocking during normal processing so instead we continue with
                        the visibility test and, if the test returns true, allow T to
                        speculatively read V. Transaction T acquires a commit dependency on
                        TB, restricting the serialization order of the two transactions. That
                        is, T is allowed to commit only if TB commits.
                    """
                **  Case 2: speculative ignore of TE:
                    """
                        If TE’s state is Preparing, it has an end timestamp TS that will become
                        the end timestamp of V if TE does commit. If TS is greater than the read
                        time RT, it is obvious that V will be visible if TE commits. If TE
                        aborts, V will still be visible, because any transaction that updates
                        V after TE has aborted will obtain an end timestamp greater than
                        TS. If TS is less than RT, we have a more complicated situation:
                        if TE commits, V will not be visible to T but if TE aborts, it will
                        be visible. We could handle this by forcing T to wait until TE
                        commits or aborts but we want to avoid all blocking during normal processing.
                        Instead we allow T to speculatively ignore V and
                        proceed with its processing. Transaction T acquires a commit
                        dependency (see Section 2.7) on TE, that is, T is allowed to commit
                        only if TE commits.
                    """
                */
                /* NOTE: Commit dependencies (Hekaton Section 2.7) are implemented via
                 ** the register-and-report protocol:
                 ** - Speculative reads/ignores call register_commit_dependency, which
                 **   increments CommitDepCounter and adds to CommitDepSet.
                 ** - WaitForDependencies checks AbortNow and waits for counter == 0.
                 ** - CommitEnd / rollback_tx drain CommitDepSet, notifying dependents.
                 **
                 ** TODO: For full serializability (beyond snapshot isolation), we still need:
                 ** 1. Validate if all read versions are still visible by inspecting the read_set
                 ** 2. Validate if there are no phantoms by walking the scans from scan_set
                 */
                tracing::trace!("commit_tx(tx_id={})", self.tx_id);
                // Header-only writes must not take this fast path; they need durable log records.
                if read_only {
                    turso_assert!(
                        tx.commit_dep_set.lock().is_empty(),
                        "MVCC read only transaction should not have commit dependencies on other txns"
                    );
                    // Abort eagerly if requested
                    if tx.abort_now.load(Ordering::Acquire) {
                        return Err(LimboError::CommitDependencyAborted);
                    }
                    // Even read-only transactions must honour commit dependencies.
                    // A SELECT during normal processing may have speculatively read
                    // from a Preparing transaction (Hekaton §2.7), incrementing our
                    // CommitDepCounter. We must wait for those to resolve.
                    if tx.commit_dep_counter.load(Ordering::Acquire) > 0 {
                        // Unresolved dependencies — skip validation (no writes)
                        // and go straight to WaitForDependencies.
                        self.state = CommitState::WaitForDependencies { end_ts };
                        return Ok(TransitionResult::Continue);
                    }
                    // Check abort_now AFTER counter: rollback_tx stores abort_now
                    // (Release) before fetch_sub (AcqRel). Once counter == 0, all
                    // decrements have completed and the abort_now flag is visible.
                    if tx.abort_now.load(Ordering::Acquire) {
                        return Err(LimboError::CommitDependencyAborted);
                    }
                    tx.state.store(TransactionState::Committed(end_ts));
                    if mvcc_store.is_exclusive_tx(&self.tx_id) {
                        mvcc_store.release_exclusive_tx(&self.tx_id);
                    }
                    mvcc_store.unlock_commit_lock_if_held(tx);
                    mvcc_store.finish_committed_tx(self.tx_id, &self.connection, self.db_id)?;
                    inject_transition_failure!(self, CommitYieldPoint::AfterRemoveTx);
                    self.finalize(mvcc_store)?;
                    return Ok(TransitionResult::Done(()));
                }
                self.state = CommitState::Commit { end_ts };
                inject_transition_yield!(self, CommitYieldPoint::CommitValidation);
                Ok(TransitionResult::Continue)
            }
            CommitState::Commit { end_ts } => {
                // Check for rowid conflicts before committing (pure optimistic, first-committer-wins)
                // Ref: Hekaton paper Section 3.2 - validation uses end_ts comparison
                let tx = mvcc_store
                    .txs
                    .get(&self.tx_id)
                    .ok_or(LimboError::TxTerminated)?;
                let tx = tx.value();

                for (id, _chain) in tx.write_set.lock().iter() {
                    if id.row_id.is_int_key() {
                        self.check_rowid_for_conflicts(id, *end_ts, tx, mvcc_store)?;
                    } else {
                        self.check_index_for_conflicts(id, *end_ts, tx, mvcc_store)?;
                    }
                }

                // Validation passed. Wait for commit dependencies before building
                // the durable commit record. The live row versions must stay on
                // TxID references until CommitEnd so an abandoned commit can
                // still be rolled back by matching on TxID(self.tx_id).
                self.state = CommitState::WaitForDependencies { end_ts: *end_ts };
                inject_transition_yield!(self, CommitYieldPoint::WaitForDependencies);
                return Ok(TransitionResult::Continue);
            }
            CommitState::WaitForDependencies { end_ts } => {
                let end_ts = *end_ts;
                let tx = mvcc_store
                    .txs
                    .get(&self.tx_id)
                    .ok_or(LimboError::TxTerminated)?;
                let tx = tx.value();

                // Eagarly check for abort_now
                if tx.abort_now.load(Ordering::Acquire) {
                    return Err(LimboError::CommitDependencyAborted);
                }
                // Hekaton Section 2.7: "A transaction cannot commit until this
                // counter is zero." Deadlock impossible: edges always go from higher
                // end_ts to lower end_ts, so the wait graph is acyclic.
                if tx.commit_dep_counter.load(Ordering::Acquire) > 0 {
                    return Ok(TransitionResult::Io(IOCompletions(Completion::new_yield())));
                }

                // Check abort_now AFTER counter reaches 0. Memory ordering:
                // rollback_tx does abort_now.store(true, Release) BEFORE
                // counter.fetch_sub(1, AcqRel). Our Acquire load of counter==0
                // synchronizes-with that fetch_sub, making the abort_now store
                // visible. Checking in the opposite order (abort_now first) has a
                // TOCTOU race: an aborting dep can set abort_now and decrement
                // between our two reads, letting us see (false, 0) and commit.
                if tx.abort_now.load(Ordering::Acquire) {
                    return Err(LimboError::CommitDependencyAborted);
                }

                // Read-only fast path: if write_set is empty and header was not mutated, commit without
                // going through CommitEnd. CommitEnd updates last_committed_tx_ts
                // which would make a read-only transaction look like a write,
                // causing spurious Busy errors from acquire_exclusive_tx.
                if tx.write_set.lock().is_empty() && !tx.header_dirty.load(Ordering::Acquire) {
                    turso_assert!(
                        tx.commit_dep_set.lock().is_empty(),
                        "MVCC read-only transaction should not have other transactions depending on it"
                    );
                    tx.state.store(TransactionState::Committed(end_ts));
                    if mvcc_store.is_exclusive_tx(&self.tx_id) {
                        mvcc_store.release_exclusive_tx(&self.tx_id);
                        self.commit_coordinator.pager_commit_lock.unlock();
                    }
                    mvcc_store.finish_committed_tx(self.tx_id, &self.connection, self.db_id)?;
                    inject_transition_failure!(self, CommitYieldPoint::AfterRemoveTx);
                    self.finalize(mvcc_store)?;
                    return Ok(TransitionResult::Done(()));
                }

                // All dependencies resolved. Initialize an empty log record
                // (`buf` is grown incrementally during BuildLogRecord) and
                // snapshot the dirty header for the tail of the payload.
                // Live row versions stay on TxID references until CommitEnd
                // so rollback of an abandoned commit can still match them.
                let pending_header = if tx.header_dirty.load(Ordering::Acquire) {
                    Some(*tx.header.read())
                } else {
                    None
                };
                self.state = CommitState::BuildLogRecord(BuildLogRecordCtx {
                    end_ts,
                    log_record: LogRecord::new(end_ts, mvcc_store.logical_log_allocator())?,
                    cursor: 0,
                    schema_process: true,
                    pending_header,
                });
                return Ok(TransitionResult::Continue);
            }
            // Chunked: yields every MVCC_COMMIT_BATCH_SIZE rowids. Pulled out
            // to a helper because the arm body needs `&mut self` to mutate
            // the cursor / pass / log_record inside the variant, which
            // conflicts with the outer `&self.state` match borrow.
            CommitState::BuildLogRecord(_) => self.step_build_log_record(mvcc_store),
            CommitState::BeginCommitLogicalLog { end_ts, .. } => {
                let is_exclusive = mvcc_store.is_exclusive_tx(&self.tx_id);
                if !is_exclusive && self.commit_coordinator.group_commit_enabled() {
                    let end_ts = *end_ts;
                    let log_record = match std::mem::replace(&mut self.state, CommitState::Initial)
                    {
                        CommitState::BeginCommitLogicalLog { log_record, .. } => log_record,
                        _ => unreachable!(),
                    };
                    let ticket = self.commit_coordinator.enqueue(self.tx_id, log_record);
                    self.state = CommitState::AwaitGroupCommit { end_ts, ticket };
                    return Ok(TransitionResult::Continue);
                }
                if !is_exclusive {
                    // logical log needs to be serialized.
                    let tx = mvcc_store
                        .txs
                        .get(&self.tx_id)
                        .ok_or_else(|| LimboError::NoSuchTransactionID(self.tx_id.to_string()))?;
                    let locked = self.commit_coordinator.pager_commit_lock.write();
                    if !locked {
                        return Ok(TransitionResult::Io(IOCompletions(Completion::new_yield())));
                    }
                    tx.value()
                        .pager_commit_lock_held
                        .store(true, Ordering::Release);
                }
                let end_ts = *end_ts;
                let log_record = match std::mem::replace(
                    &mut self.state,
                    CommitState::UpgradeLogicalLogHeader {
                        end_ts,
                        log_record: LogRecord::empty(end_ts, mvcc_store.logical_log_allocator()),
                    },
                ) {
                    CommitState::BeginCommitLogicalLog { log_record, .. } => log_record,
                    _ => unreachable!(),
                };
                self.state = CommitState::UpgradeLogicalLogHeader { end_ts, log_record };
                Ok(TransitionResult::Continue)
            }
            CommitState::AwaitGroupCommit { end_ts, ticket } => {
                self.step_await_group_commit(mvcc_store, *end_ts, *ticket)
            }
            CommitState::UpgradeLogicalLogHeader { end_ts, log_record } => {
                let skip_dropped = self
                    .group_batch
                    .as_ref()
                    .is_some_and(|batch| mvcc_store.txs.get(&batch.writing.tx_id).is_none());
                if skip_dropped {
                    let end_ts = *end_ts;
                    self.state =
                        self.advance_group_or_sync(end_ts, mvcc_store.logical_log_allocator());
                    return Ok(TransitionResult::Continue);
                }
                let header_c = mvcc_store.storage.upgrade_header_for_log_tx(log_record)?;
                if let Some(c) = header_c {
                    if !c.succeeded() {
                        return Ok(TransitionResult::Io(IOCompletions(c)));
                    }
                }
                let end_ts = *end_ts;
                let log_record = match std::mem::replace(
                    &mut self.state,
                    CommitState::WriteLogicalLog {
                        end_ts,
                        log_record: LogRecord::empty(end_ts, mvcc_store.logical_log_allocator()),
                    },
                ) {
                    CommitState::UpgradeLogicalLogHeader { log_record, .. } => log_record,
                    _ => unreachable!(),
                };
                self.state = CommitState::WriteLogicalLog { end_ts, log_record };
                Ok(TransitionResult::Continue)
            }
            CommitState::WriteLogicalLog { end_ts, .. } => {
                let end_ts = *end_ts;
                let log_record = match std::mem::replace(
                    &mut self.state,
                    CommitState::FinishLogicalLogWrite { end_ts },
                ) {
                    CommitState::WriteLogicalLog { log_record, .. } => log_record,
                    _ => unreachable!(),
                };
                if self
                    .group_batch
                    .as_ref()
                    .is_some_and(|batch| mvcc_store.txs.get(&batch.writing.tx_id).is_none())
                {
                    self.state =
                        self.advance_group_or_sync(end_ts, mvcc_store.logical_log_allocator());
                    return Ok(TransitionResult::Continue);
                }
                let (c, append_bytes) = mvcc_store.storage.log_tx(log_record, None)?;
                if let Some(batch) = self.group_batch.as_ref() {
                    self.commit_coordinator
                        .note_write_issued(batch.writing.tx_id);
                }
                self.pending_log_append_bytes = Some(append_bytes);
                if self
                    .group_batch
                    .as_ref()
                    .is_some_and(|batch| batch.writing.tx_id != self.tx_id)
                {
                    inject_transition_yield!(self, CommitYieldPoint::LogicalLogWriteIssued);
                }
                if c.succeeded() {
                    Ok(TransitionResult::Continue)
                } else {
                    Ok(TransitionResult::Io(IOCompletions(c)))
                }
            }

            CommitState::FinishLogicalLogWrite { end_ts } => {
                let end_ts = *end_ts;
                let c = mvcc_store.storage.on_log_write_complete()?;
                let (ticket, owner_tx) = match self.group_batch.as_ref() {
                    Some(batch) => (Some(batch.writing.ticket), batch.writing.tx_id),
                    None => (None, self.tx_id),
                };
                let owned_self = self.own_logical_log_record(mvcc_store, ticket, owner_tx)?;
                self.state = self.advance_group_or_sync(end_ts, mvcc_store.logical_log_allocator());
                if !c.succeeded() {
                    return Ok(TransitionResult::Io(IOCompletions(c)));
                }
                if owned_self {
                    inject_transition_yield!(self, CommitYieldPoint::LogicalLogOwned);
                }
                Ok(TransitionResult::Continue)
            }

            CommitState::SyncLogicalLog { end_ts } => {
                // Skip fsync when synchronous mode is not FULL.
                // NORMAL mode skips fsync on commit (but still fsyncs on checkpoint).
                if self.sync_mode != SyncMode::Full {
                    tracing::debug!("Skipping fsync of logical log (synchronous!=full)");
                    self.state = CommitState::EndCommitLogicalLog { end_ts: *end_ts };
                    return Ok(TransitionResult::Continue);
                }
                let c = mvcc_store.storage.sync(self.pager.get_sync_type())?;
                self.state = CommitState::EndCommitLogicalLog { end_ts: *end_ts };
                if c.succeeded() {
                    Ok(TransitionResult::Continue)
                } else {
                    Ok(TransitionResult::Io(IOCompletions(c)))
                }
            }
            CommitState::SyncGroupPrefix { end_ts, ticket } => {
                let (end_ts, ticket) = (*end_ts, *ticket);
                if self.sync_mode != SyncMode::Full {
                    self.state = CommitState::GroupPrefixSynced { end_ts, ticket };
                    return Ok(TransitionResult::Continue);
                }
                let c = mvcc_store.storage.sync(self.pager.get_sync_type())?;
                self.state = CommitState::GroupPrefixSynced { end_ts, ticket };
                if c.succeeded() {
                    Ok(TransitionResult::Continue)
                } else {
                    Ok(TransitionResult::Io(IOCompletions(c)))
                }
            }
            CommitState::GroupPrefixSynced { end_ts, ticket } => {
                self.commit_coordinator
                    .mark_durable(self.commit_coordinator.written_through());
                if let Some(tx) = mvcc_store.txs.get(&self.tx_id) {
                    mvcc_store.unlock_commit_lock_if_held(tx.value());
                } else {
                    self.commit_coordinator.pager_commit_lock.unlock();
                }
                self.state = CommitState::AwaitGroupCommit {
                    end_ts: *end_ts,
                    ticket: *ticket,
                };
                Ok(TransitionResult::Continue)
            }
            CommitState::EndCommitLogicalLog { end_ts } => {
                let tx = mvcc_store
                    .txs
                    .get(&self.tx_id)
                    .ok_or_else(|| LimboError::NoSuchTransactionID(self.tx_id.to_string()))?;
                let tx_unlocked = tx.value();
                if self.group_batch.take().is_some() {
                    self.commit_coordinator
                        .mark_durable(self.commit_coordinator.written_through());
                }
                let tx_header = *tx_unlocked.header.read();
                let schema_did_change = self.did_commit_schema_change
                    || self
                        .header
                        .read()
                        .as_ref()
                        .map(|header| header.schema_cookie.get())
                        != Some(tx_header.schema_cookie.get());
                self.did_commit_schema_change = schema_did_change;
                if schema_did_change {
                    let schema = self.connection.schema.read().clone();
                    self.connection.db.update_schema_if_newer(schema);
                }
                // Guard the global_header write against out-of-order
                // completion. An exclusive tx can bypass `pager_commit_lock`
                // (the conditional at the top of BeginCommitLogicalLog) and
                // race past us into EndCommitLogicalLog; if its end_ts is
                // higher, its header has already been published, and our
                // older write would regress `global_header.schema_cookie`
                // below the latest committed value. The same monotonicity
                // applies in FinalizeCommit below.
                let prev_hdr_ts = mvcc_store
                    .last_global_header_ts
                    .fetch_max(*end_ts, Ordering::AcqRel);
                if prev_hdr_ts <= *end_ts {
                    self.header.write().replace(tx_header);
                }
                tracing::trace!("end_commit_logical_log(tx_id={})", self.tx_id);
                self.state = CommitState::CommitEnd { end_ts: *end_ts };
                return Ok(TransitionResult::Continue);
            }
            CommitState::CommitEnd { end_ts } => {
                // The record is in the log and owned since FinishLogicalLogWrite.
                // Order of operations from here:
                // 1. Mark transaction Committed
                // 2. Rewrite live row versions from TxID to Timestamp (chunked
                //    in `RewriteLiveVersions` so 2M-row write sets don't stall)
                // 3. Notify dependents
                // 4. Release commit lock
                // 5. Update cached global header
                //
                // (1) must precede (2): rewriting before marking Committed would
                // publish the transaction's effects to readers before its fate is
                // decided, which breaks rollback of abandoned commits.
                let tx = mvcc_store
                    .txs
                    .get(&self.tx_id)
                    .ok_or_else(|| LimboError::NoSuchTransactionID(self.tx_id.to_string()))?;
                let tx_unlocked = tx.value();
                tx_unlocked
                    .state
                    .store(TransactionState::Committed(*end_ts));

                // Hand off to the chunked rewriter. Between chunks readers
                // resolve any unwritten TxID refs via `txs[tx_id]` which now
                // reports Committed(end_ts).
                self.state = CommitState::RewriteLiveVersions(RewriteLiveVersionsCtx {
                    end_ts: *end_ts,
                    cursor: 0,
                });
                Ok(TransitionResult::Continue)
            }
            // Chunked: yields every MVCC_COMMIT_BATCH_SIZE rowids. Same
            // helper-dispatch reason as BuildLogRecord above.
            CommitState::RewriteLiveVersions(_) => self.step_rewrite_live_versions(mvcc_store),
            CommitState::FinalizeCommit { end_ts } => {
                let tx = mvcc_store
                    .txs
                    .get(&self.tx_id)
                    .ok_or_else(|| LimboError::NoSuchTransactionID(self.tx_id.to_string()))?;
                let tx_unlocked = tx.value();

                // Hekaton Section 3.3: "The transaction then processes all outgoing
                // commit dependencies listed in its CommitDepSet. If it committed, it
                // decrements the target transaction's CommitDepCounter."
                // IOW since this txn committed, let's signal waiting transactions.
                let dependents = std::mem::take(&mut *tx_unlocked.commit_dep_set.lock());
                for dep_tx_id in dependents {
                    if let Some(dep_tx_entry) = mvcc_store.txs.get(&dep_tx_id) {
                        dep_tx_entry
                            .value()
                            .commit_dep_counter
                            .fetch_sub(1, Ordering::AcqRel);
                    }
                }

                let appended_to_log = self.wrote_logical_log;
                mvcc_store.unlock_commit_lock_if_held(tx_unlocked);

                inject_transition_yield!(self, CommitYieldPoint::BeforeGlobalHeaderUpdate);

                let tx_header = *tx_unlocked.header.read();
                {
                    // Hold the header lock across the watermark update and header
                    // publish so the guard decision and replacement are serialized.
                    let mut global_header = mvcc_store.global_header.write();
                    // Since we assign a commit timestamp and then we drive the commit to completion,
                    // it is totally possible for so an older transaction can finish after a newer one.
                    // In such case, we should not let older commit to set lower value than previous.
                    // This value is used in checkpointing as a watermark boundary, and an incorrect
                    // lower value can cause data loss / corruption.
                    let last_committed_ts = mvcc_store
                        .last_committed_tx_ts
                        .fetch_max(*end_ts, Ordering::AcqRel);
                    if last_committed_ts <= *end_ts {
                        global_header.replace(tx_header);
                    }
                }
                if self.did_commit_schema_change {
                    mvcc_store
                        .last_committed_schema_change_ts
                        .fetch_max(*end_ts, Ordering::AcqRel);
                }

                // We have now updated all the versions with a reference to the
                // transaction ID to a timestamp and can, therefore, remove the
                // transaction. Pair removal with the connection cache clear so
                // an IO yield + abandon during the upcoming checkpoint cannot
                // strand `conn.mv_tx_id` referencing a tx that's gone from `txs`.
                //
                // Release `exclusive_tx` BEFORE `finish_committed_tx` /
                // `inject_transition_failure!` so an Err at `AfterRemoveTx`
                // cannot strand the atomic — matches the ordering at the
                // two fast-path CommitEnd sites.
                if mvcc_store.is_exclusive_tx(&self.tx_id) {
                    mvcc_store.release_exclusive_tx(&self.tx_id);
                }
                inject_transition_yield!(self, CommitYieldPoint::BeforeFinishCommittedTx);
                mvcc_store.finish_committed_tx(self.tx_id, &self.connection, self.db_id)?;
                inject_transition_failure!(self, CommitYieldPoint::AfterRemoveTx);
                if appended_to_log && mvcc_store.storage.should_checkpoint() {
                    let auto_checkpoint_mode = if mvcc_store.uses_passive_checkpoint() {
                        crate::storage::wal::CheckpointMode::Passive {
                            upper_bound_inclusive: None,
                        }
                    } else {
                        crate::storage::wal::CheckpointMode::Truncate {
                            upper_bound_inclusive: None,
                        }
                    };
                    let state_machine = StateMachine::new(CheckpointStateMachine::new(
                        self.pager.clone(),
                        mvcc_store.clone(),
                        self.connection.clone(),
                        false,
                        self.sync_mode,
                        self.db_id,
                        auto_checkpoint_mode,
                    ));
                    let state_machine = Mutex::new(state_machine);
                    self.state = CommitState::Checkpoint { state_machine };
                    return Ok(TransitionResult::Continue);
                }
                // Not checkpointing this commit. Reclaim invisible versions
                // inline with a bounded, non-blocking GC pass so steady-state
                // memory stays flat between checkpoints (which only fire on
                // logical-log byte growth, not version accumulation). The pass
                // does no I/O and is capped at MAX_CHAINS_PER_GC chains, so it
                // doesn't meaningfully slow this committing connection. See
                // `gc_incremental`.
                if mvcc_store.should_gc() {
                    mvcc_store.gc_incremental(MvStore::<Clock>::MAX_CHAINS_PER_GC);
                }
                tracing::trace!("logged(tx_id={}, end_ts={})", self.tx_id, *end_ts);
                self.finalize(mvcc_store)?;
                Ok(TransitionResult::Done(()))
            }
            CommitState::Checkpoint { state_machine } => {
                let step_result = {
                    let mut sm = state_machine.lock();
                    // Step the checkpoint SM directly so a passive publish retry can return
                    // `Continue` and yield the commit executor (the `StateMachine` wrapper would
                    // spin on `Continue` internally and starve pinned readers on the same thread).
                    sm.inner_mut().step(&())
                };
                match step_result {
                    Ok(TransitionResult::Continue) => {
                        return Ok(TransitionResult::Continue);
                    }
                    Ok(TransitionResult::Io(iocompletions)) => {
                        return Ok(TransitionResult::Io(iocompletions));
                    }
                    Ok(TransitionResult::Done(_)) => {
                        state_machine.lock().finalize(&())?;
                    }
                    Err(err) => {
                        // Auto-checkpoint errors should not surface to the committed statement.
                        tracing::info!("MVCC auto-checkpoint failed: {err}");
                        self.finalize(mvcc_store)?;
                        return Ok(TransitionResult::Done(()));
                    }
                }
                self.finalize(mvcc_store)?;
                return Ok(TransitionResult::Done(()));
            }
        }
    }

    fn finalize(&mut self, _context: &Self::Context) -> Result<()> {
        self.is_finalized = true;
        Ok(())
    }

    fn is_finalized(&self) -> bool {
        self.is_finalized
    }
}

impl StateTransition for WriteRowStateMachine {
    type Context = ();
    type SMResult = ();

    #[tracing::instrument(fields(state = ?self.state), skip(self, _context), level = Level::DEBUG)]
    fn step(&mut self, _context: &Self::Context) -> Result<TransitionResult<Self::SMResult>> {
        use crate::types::{IOResult, SeekKey, SeekOp};

        match self.state {
            WriteRowState::Initial => {
                // Create the record and key
                self.record = if self.row.is_index_row() {
                    None
                } else {
                    let row_data = self.row.data.as_ref().expect("table rows should have data");
                    let mut record = ImmutableRecord::new(row_data.len())?;
                    record.start_serialization(row_data)?;
                    Some(record)
                };
                // `requires_seek == false` is a write_set-level *candidate* for the
                // sequential-write optimization (this row's key is exactly previous
                // row's key + 1, so the cursor — left on the previous row and advanced
                // by WriteRowState::Next — is usually already at the insert position).
                // It is only sound if the cursor is still PAST the start of its leaf:
                // if the previous row was the last cell of its leaf, next() crossed
                // into the following leaf (cell 0), and this row may belong on the
                // other side of the parent divider. Dividers keep their key when the
                // checkpoint deletes their row, so the divider can
                // be >= this row's key, meaning the row MUST go into the left leaf
                // even though the cursor is in the right one. Writing it at the
                // cursor would keep the leaf locally sorted but break the interior
                // ordering invariant, making the row invisible to point lookups
                // ("Rowid N out of order" under sqlite3 integrity_check). Fall back
                // to a real seek, which resolves the divider comparison correctly.
                //
                // The other sound position is the end of the rightmost leaf: when
                // the previous row was the last row of the table, next() ran off
                // the end and left the cursor one past it, with no divider to the
                // right. Every append-only workload sits here for every row, so
                // without this case each row would seek from the root.
                let positioned_for_next_key = {
                    let cursor = self.cursor.read();
                    cursor.is_positioned_past_page_start() || cursor.is_at_end_of_rightmost_leaf()
                };
                if self.requires_seek || !positioned_for_next_key {
                    self.state = WriteRowState::Seek;
                } else {
                    self.state = WriteRowState::Insert;
                }
                Ok(TransitionResult::Continue)
            }
            WriteRowState::Seek => {
                // Position the cursor by seeking to the row position
                let seek_key = match &self.row.id.row_id {
                    RowKey::Int(row_id) => SeekKey::TableRowId(*row_id),
                    RowKey::Record(record) => SeekKey::IndexKey(record.key.reborrow()),
                };

                match self
                    .cursor
                    .write()
                    .seek(seek_key, SeekOp::GE { eq_only: true })?
                {
                    IOResult::Done(seek_result) => {
                        if self.row.is_index_row() && matches!(seek_result, SeekResult::TryAdvance)
                        {
                            self.state = WriteRowState::Advance;
                            return Ok(TransitionResult::Continue);
                        }
                    }
                    IOResult::IO(io) => {
                        return Ok(TransitionResult::Io(io));
                    }
                }
                turso_assert_eq!(self.cursor.write().valid_state, CursorValidState::Valid);
                self.state = WriteRowState::Insert;
                Ok(TransitionResult::Continue)
            }
            WriteRowState::Advance => {
                match self.cursor.write().next()? {
                    IOResult::Done(_) => {}
                    IOResult::IO(io) => {
                        return Ok(TransitionResult::Io(io));
                    }
                }
                turso_assert!(
                    self.cursor.read().has_record(),
                    "MVCC checkpoint index insert did not land on the matched interior record"
                );
                self.state = WriteRowState::Insert;
                Ok(TransitionResult::Continue)
            }
            WriteRowState::Insert => {
                // Insert the record into the B-tree
                let key = match &self.row.id.row_id {
                    RowKey::Int(row_id) => BTreeKey::new_table_rowid(*row_id, self.record.as_ref()),
                    RowKey::Record(record) => BTreeKey::new_index_key(record.key.reborrow()),
                };

                match self.cursor.write().insert(&key)? {
                    IOResult::Done(()) => {}
                    IOResult::IO(io) => {
                        return Ok(TransitionResult::Io(io));
                    }
                }
                self.state = WriteRowState::Next;
                Ok(TransitionResult::Continue)
            }
            WriteRowState::Next => {
                match self.cursor.write().next()? {
                    IOResult::Done(_) => {}
                    IOResult::IO(io) => {
                        return Ok(TransitionResult::Io(io));
                    }
                }
                self.finalize(&())?;
                Ok(TransitionResult::Done(()))
            }
        }
    }

    fn finalize(&mut self, _context: &Self::Context) -> Result<()> {
        self.is_finalized = true;
        Ok(())
    }

    fn is_finalized(&self) -> bool {
        self.is_finalized
    }
}

impl StateTransition for DeleteRowStateMachine {
    type Context = ();
    type SMResult = ();

    #[tracing::instrument(fields(state = ?self.state), skip(self, _context), level = Level::TRACE)]
    fn step(&mut self, _context: &Self::Context) -> Result<TransitionResult<Self::SMResult>> {
        use crate::types::{IOResult, SeekKey, SeekOp};

        match self.state {
            DeleteRowState::Initial => {
                self.state = DeleteRowState::Seek;
                Ok(TransitionResult::Continue)
            }
            DeleteRowState::Seek => {
                let seek_key = match &self.rowid.row_id {
                    RowKey::Int(row_id) => SeekKey::TableRowId(*row_id),
                    RowKey::Record(record) => SeekKey::IndexKey(record.key.reborrow()),
                };

                match self
                    .cursor
                    .write()
                    .seek(seek_key, SeekOp::GE { eq_only: true })?
                {
                    IOResult::Done(seek_res) => {
                        match seek_res {
                            SeekResult::Found => {
                                self.state = DeleteRowState::Delete;
                            }
                            SeekResult::TryAdvance => {
                                // In index B-trees, the key can reside in an interior node
                                // rather than a leaf. The seek descends to the leaf but
                                // doesn't find it there, returning TryAdvance. Advancing
                                // the cursor will move up to the interior cell.
                                self.state = DeleteRowState::Advance;
                            }
                            SeekResult::NotFound => {
                                crate::bail_corrupt_error!(
                                    "MVCC delete: rowid {} not found",
                                    self.rowid.row_id
                                );
                            }
                        }
                        Ok(TransitionResult::Continue)
                    }
                    IOResult::IO(io) => {
                        return Ok(TransitionResult::Io(io));
                    }
                }
            }
            DeleteRowState::Advance => {
                let next_result = self.cursor.write().next()?;
                match next_result {
                    IOResult::Done(()) => {
                        if !self.cursor.read().has_record() {
                            crate::bail_corrupt_error!(
                                "MVCC delete: rowid {} not found after advance",
                                self.rowid.row_id
                            );
                        }
                        self.state = DeleteRowState::Delete;
                        Ok(TransitionResult::Continue)
                    }
                    IOResult::IO(io) => {
                        return Ok(TransitionResult::Io(io));
                    }
                }
            }
            DeleteRowState::Delete => {
                // Insert the record into the B-tree

                match self.cursor.write().delete()? {
                    IOResult::Done(()) => {}
                    IOResult::IO(io) => {
                        return Ok(TransitionResult::Io(io));
                    }
                }
                tracing::trace!(
                    "delete_row_from_pager(table_id={}, row_id={})",
                    self.rowid.table_id,
                    self.rowid.row_id
                );
                self.finalize(&())?;
                Ok(TransitionResult::Done(()))
            }
        }
    }

    fn finalize(&mut self, _context: &Self::Context) -> Result<()> {
        self.is_finalized = true;
        Ok(())
    }

    fn is_finalized(&self) -> bool {
        self.is_finalized
    }
}

impl DeleteRowStateMachine {
    fn new(rowid: RowID, cursor: Arc<RwLock<BTreeCursor>>) -> Self {
        Self {
            state: DeleteRowState::Initial,
            is_finalized: false,
            rowid,
            cursor,
        }
    }
}

pub const SQLITE_SCHEMA_MVCC_TABLE_ID: MVTableId = MVTableId(-1);
pub(crate) const MVCC_META_TABLE_NAME: &str = "__turso_internal_mvcc_meta";
/// Indicates the maximum transaction timestamp that has been made durable in the WAL.
/// Used to determine the replay boundary for recovery; only records with a higher timestamp
/// are replayed.
pub(crate) const MVCC_META_KEY_PERSISTENT_TX_TS_MAX: &str = "persistent_tx_ts_max";

#[derive(Debug)]
pub struct RowidAllocator {
    /// Exclusive lock serializing initialization (btree max read → store).
    /// Only held during the first NewRowid for a table; after that, the
    /// fast path is lock-free (atomic CAS on max_rowid).
    lock: TursoRwLock,
    /// Monotonically increasing counter. 0 = empty table (rowids start at 1).
    /// Updated via atomic CAS — no RwLock needed on the fast path.
    max_rowid: AtomicI64,
    /// True after the first btree-max scan. Never reset to false.
    initialized: AtomicBool,
}

/// Sub state machine for [`MvStore::bootstrap_nonblock`]. Carried by the
/// open state machine (`OpenDbAsyncPhase::BootstrapMvStore`) across the
/// metadata-bootstrap IO chain so opening an MVCC database does not block on
/// the log-header truncate / write / fsync sequence, the interrupted-checkpoint
/// reconciliation, the schema reparse, or the metadata-table reads/writes.
#[derive(Default)]
pub enum BootstrapState {
    #[default]
    Start,
    /// Pre-metadata: reconcile an interrupted checkpoint (non-blocking).
    PreCheckpoint {
        tvfs: Vec<Arc<crate::vtab::VirtualTable>>,
        checkpoint_st: CompleteCheckpointState,
    },
    /// Pre-metadata: reparse the schema (non-blocking).
    PreReparse {
        tvfs: Vec<Arc<crate::vtab::VirtualTable>>,
        reparse_st: crate::connection::ReparseSchemaState,
    },
    /// Reading the persistent tx-ts-max from the MVCC metadata table to decide
    /// whether the metadata-bootstrap IO chain is needed.
    BeginReadTxTs {
        read_st: ReadPersistentTxTsMaxState,
    },
    MetadataIo(MetadataIoInFlight),
    /// Finish: create/seed the MVCC metadata table (non-blocking).
    FinishInit {
        init_st: InitMetadataTableState,
    },
    /// Finish: reconcile the interrupted checkpoint again after metadata writes.
    FinishCheckpoint {
        checkpoint_st: CompleteCheckpointState,
    },
    /// Replay the logical log into the MVCC store (non-blocking), then promote
    /// the bootstrap connection to a regular MVCC connection.
    Recover {
        recover_st: RecoverLogicalLogState,
    },
    /// Recover sequence descriptors from each backing table (non-blocking).
    /// The pre-replay reparse missed backing tables created by committed-but-
    /// not-checkpointed CREATE SEQUENCE statements; this pass registers pure
    /// descriptors via the MVCC-aware SQL path. The runtime watermark is never
    /// read — every nextval queries disk on demand.
    LoadSequences {
        load_st: crate::connection::LoadSequenceDescriptorsState,
    },
    /// Compatibility sync for AUTOINCREMENT tables created in WAL mode (where
    /// the watermark lives in sqlite_sequence) and then reopened in MVCC mode
    /// (where the disk-only allocation path reads backing tables). Non-blocking.
    SyncAutoincrement {
        sync_st: crate::connection::SyncAutoincrementState,
    },
    AwaitingGlobalHeader,
}

#[doc(hidden)]
pub struct MetadataIoInFlight {
    pub completion: Completion,
    pub sync_type: FileSyncType,
    pub next: MetadataIoStep,
}

#[doc(hidden)]
#[derive(Clone, Copy)]
pub enum MetadataIoStep {
    /// `log_file.truncate(0)` just finished. If the log was <= header size we
    /// still need to write a fresh header (and maybe fsync). `log_size` is the
    /// original on-disk size; `sync_mode_off` records whether sync is disabled.
    AfterTruncate { log_size: u64, sync_mode_off: bool },
    /// `storage.update_header` just finished. Maybe issue `storage.sync`.
    AfterUpdateHeader { sync_mode_off: bool },
    /// `storage.sync` just finished — metadata IO chain done.
    AfterSync,
}

/// Sub state machine for
/// [`MvStore::maybe_complete_interrupted_checkpoint_nonblock`]. Tracks the
/// sequence of IO yields needed to reconcile an interrupted MVCC checkpoint
/// without blocking: read log header → optional early WAL truncate, or
/// WAL→DB backfill + db_file.sync + log-header rewrite (with a single-shot
/// CRC retry) + final WAL truncate.
#[derive(Default)]
pub enum CompleteCheckpointState {
    #[default]
    Start,
    /// Reading the log header via the streaming reader.
    ReadingHeader {
        reader: Box<StreamingLogicalLogReader>,
    },
    /// `wal_max_frame == 0` branch: driving the early `wal.truncate_wal`.
    /// `checkpoint_result` must persist across IO yields because
    /// `truncate_wal`/`truncate_log` tracks its truncate/sync progress through
    /// its `wal_truncate_sent` / `wal_sync_sent` flags; recreating it each
    /// re-entry would re-issue the truncate forever.
    DriveEarlyTruncate {
        header_result: HeaderReadResult,
        checkpoint_result: CheckpointResult,
    },
    /// Main path: driving `wal.checkpoint(Truncate)`. Reached from a Valid header (reused
    /// via `set_header`) or a `NoLog` log (the Passive steady state); the fresh header is
    /// (re)written later in `RetryHeader`.
    DriveCheckpoint,
    /// Awaiting the `db_file.sync` completion after a successful backfill.
    AwaitDbFileSync {
        completion: Completion,
        checkpoint_result: CheckpointResult,
    },
    /// Retry loop: rewriting the log header + verifying CRC. `retried_crc`
    /// allows a single retry on torn-tail mismatch before failing closed.
    RetryHeader {
        checkpoint_result: CheckpointResult,
        retried_crc: bool,
        phase: RetryHeaderPhase,
    },
    /// Driving the final `wal.truncate_wal`.
    DriveFinalTruncate { checkpoint_result: CheckpointResult },
}

#[doc(hidden)]
pub enum RetryHeaderPhase {
    /// Need to issue `storage.update_header`.
    NeedUpdateHeader,
    /// Awaiting the `storage.update_header` write.
    AwaitUpdateHeader(Completion),
    /// Awaiting `storage.sync` (only when `sync_mode != Off`).
    AwaitLogSync(Completion),
    /// Reading the log header to verify the CRC.
    AwaitCrcCheck {
        reader: Box<StreamingLogicalLogReader>,
    },
}

/// Sub state machine for [`MvStore::try_read_persistent_tx_ts_max_nonblock`].
/// Holds the prepared metadata-read statement + accumulated value across IO
/// yields while the SELECT runs cooperatively.
#[derive(Default)]
pub enum ReadPersistentTxTsMaxState {
    #[default]
    Start,
    Running {
        stmt: Box<crate::Statement>,
        value: Option<i64>,
    },
}

/// Sub state machine for [`MvStore::initialize_mvcc_metadata_table_nonblock`].
/// Sequences the CREATE TABLE then INSERT statements, holding each prepared
/// statement across IO yields.
#[derive(Default)]
pub enum InitMetadataTableState {
    #[default]
    Start,
    CreateTable {
        stmt: Box<crate::Statement>,
    },
    Insert {
        stmt: Box<crate::Statement>,
    },
}

/// Sub state machine for [`MvStore::maybe_recover_logical_log`]. The
/// setup phases (header read, persistent-tx-ts read, schema-cookie read,
/// sqlite_schema scan) each yield IO; the `Replay` phase carries the loop
/// accumulators in [`RecoverCtx`] across per-frame `next_frame` yields.
#[derive(Default)]
pub enum RecoverLogicalLogState {
    #[default]
    Start,
    ReadHeader {
        reader: Box<StreamingLogicalLogReader>,
        preserved_tvfs: Vec<Arc<crate::vtab::VirtualTable>>,
    },
    ReadTxTs {
        reader: Box<StreamingLogicalLogReader>,
        preserved_tvfs: Vec<Arc<crate::vtab::VirtualTable>>,
        header_present: bool,
        txts_st: ReadPersistentTxTsMaxState,
    },
    ReadCookie {
        reader: Box<StreamingLogicalLogReader>,
        preserved_tvfs: Vec<Arc<crate::vtab::VirtualTable>>,
        persistent_tx_ts_max: u64,
    },
    QuerySchema {
        reader: Box<StreamingLogicalLogReader>,
        preserved_tvfs: Vec<Arc<crate::vtab::VirtualTable>>,
        persistent_tx_ts_max: u64,
        cookie: u32,
        stmt: Option<Box<crate::Statement>>,
        schema_rows: HashMap<i64, ImmutableRecord>,
    },
    Replay {
        ctx: Box<RecoverCtx>,
    },
    Done,
}

/// Accumulated state of the logical-log replay loop, persisted across
/// `next_frame` IO yields. See [`MvStore::maybe_recover_logical_log`].
pub struct RecoverCtx {
    reader: Box<StreamingLogicalLogReader>,
    preserved_table_valued_functions: Vec<Arc<crate::vtab::VirtualTable>>,
    /// Fallback schema cookie (pre-read) used by `recover_build_schema` only
    /// when `global_header` is unset.
    cookie: u32,
    persistent_tx_ts_max: u64,
    replay_cutoff_ts: u64,
    max_commit_ts_seen: u64,
    schema_rows: HashMap<i64, ImmutableRecord>,
    dropped_root_pages: HashSet<i64>,
    current_schema: Arc<Schema>,
    index_infos: HashMap<(MVTableId, IndexOpKind), Arc<IndexInfo>>,
}

/// WAL position `(checkpoint_seq, frame)`. Ordered lexicographically for physical reachability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WalPos {
    pub checkpoint_seq: u32,
    pub frame: u64,
}

impl WalPos {
    /// In the durable base from the very beginning — reachable by every reader.
    pub const ORIGIN: WalPos = WalPos {
        checkpoint_seq: 0,
        frame: 0,
    };
    /// Staged / not-yet-committed sentinel: greater than any real reader's mark, so unreachable.
    pub const STAGED: WalPos = WalPos {
        checkpoint_seq: u32::MAX,
        frame: u64::MAX,
    };

    pub fn from_pair((checkpoint_seq, frame): (u32, u64)) -> Self {
        Self {
            checkpoint_seq,
            frame,
        }
    }
}

/// Versioned `table_id -> root_page` binding (`begin`/`end` = snapshot lifetime).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RootEntry {
    pub root_page: Option<u64>,
    pub begin: u64,
    pub end: u64,
    /// When this binding's B-tree pages became durable in the WAL (`ORIGIN` = always in base).
    pub materialized_at: WalPos,
}

impl RootEntry {
    /// A live binding visible to every snapshot (bootstrap/recovery/uncheckpointed-create).
    pub fn live(root_page: Option<u64>) -> Self {
        Self {
            root_page,
            begin: 0,
            end: u64::MAX,
            materialized_at: WalPos::ORIGIN,
        }
    }

    /// Still live (not yet dropped).
    pub fn is_live(&self) -> bool {
        self.end == u64::MAX
    }

    /// Whether this binding is logically visible to a transaction at snapshot `ts`
    /// (`begin <= ts` and, unless live, `ts < end`). This is base-validity only; for a btree
    /// *read* also require physical reachability via [`Self::materialized_at`].
    pub fn covers(&self, ts: u64) -> bool {
        self.begin <= ts && (self.end == u64::MAX || ts < self.end)
    }
}

/// SkipMap / GC counters for debugging (see [`MvStore::debug_gc_snapshot`]).
/// Test-only harness data, not part of the public MVCC API.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GcDebugSnapshot {
    pub rows_slots: usize,
    pub rows_empty_slots: usize,
    pub rows_versions: usize,
    pub index_slots: usize,
    pub index_empty_slots: usize,
    pub index_versions: usize,
    pub live_version_count_approx: usize,
    pub live_versions_at_last_gc: usize,
    pub lwm: u64,
    pub durable_txid_max: u64,
    pub logical_log_offset: u64,
    pub logical_log_size: u64,
    pub active_txs: usize,
    pub min_reader_mark: WalPos,
    pub backfill_floor: WalPos,
}

/// One custom index's writer lease plus the commit timestamp of its last
/// publication. See `MvStore::index_method_write_leases`.
///
/// With segment-registry FTS storage, plain document inserts never take the
/// lease — they only append rows under fresh segment ids and commute freely.
/// The lease is held only by maintenance work (merge/OPTIMIZE, index
/// teardown), which retires other transactions' rows. Deletes and updates
/// sit in between: they insert tombstone rows against existing segments, so
/// a merge that retires those segments concurrently would silently lose the
/// tombstones. `active_deleters` and `last_delete_commit_ts` make deleters
/// and the lease holder mutually excluded without serializing deleters
/// against each other or against inserters.
#[derive(Debug, Default)]
struct IndexMethodWriteLease {
    /// Transaction currently allowed to write the index, if any.
    holder: Option<TxID>,
    /// Commit timestamp of the last transaction that published this index.
    last_publish_ts: Option<u64>,
    /// Transactions that inserted (or may insert) tombstone rows against
    /// this index's existing segments and have not finished yet.
    active_deleters: HashSet<TxID>,
    /// Commit timestamp of the last transaction that inserted tombstone
    /// rows. A merge whose read snapshot predates this cannot commit: it
    /// would retire segments without carrying those tombstones forward.
    last_delete_commit_ts: Option<u64>,
}

/// A multi-version concurrency control database.
#[derive(Debug)]
pub struct MvStore<Clock: LogicalClock, A: ConcurrentAllocator = TursoAllocator> {
    pub rows: SkipMap<RowID, RowVersions<A>, BasicComparator, A>,
    /// Table ID is an opaque identifier that is only meaningful to the MV store.
    /// Each checkpointed MVCC table corresponds to a single B-tree on the pager,
    /// which naturally has a root page.
    /// We cannot use root page as the MVCC table ID directly because:
    /// - We assign table IDs during MVCC commit, but
    /// - we commit pages to the pager only during checkpoint
    ///
    /// which means the root page is not easily knowable ahead of time.
    /// Hence, we store the mapping here.
    /// The value is Option because tables created in an MVCC commit that have not
    /// been checkpointed yet have no real root page assigned yet.
    ///
    /// Versioned root bindings; passive checkpoints update these at publish, not during collection.
    pub table_id_to_rootpage: SkipMap<MVTableId, RootEntry, BasicComparator, A>,
    /// Unlike table rows which are stored in a single map, we have a separate map for every index
    /// because operations like last() on an index are much easier when we don't have to take the
    /// table identifier into account.
    pub index_rows: SkipMap<MVTableId, IndexRowsMap<A>, BasicComparator, A>,
    /// Bumped whenever the key set of `index_rows` may change (every
    /// [`Self::insert_index_version`], which is the single funnel through which
    /// new index keys are created). Forward-scan cursors snapshot this next to
    /// their [`crate::mvcc::cursor::IndexShadowFinger`] and reset the finger on
    /// a mismatch, since a key inserted at or behind an already-positioned
    /// finger would otherwise be skipped (#7578).
    index_rows_epoch: AtomicU64,
    txs: SkipMap<TxID, Transaction<A>, BasicComparator, A>,
    /// Final state for removed transactions. Readers may still race with stale TxID
    /// references in row versions after a transaction is removed from `txs`.
    finalized_tx_states: SkipMap<TxID, TransactionState, BasicComparator, A>,
    /// Allocator backing every skiplist in this store, including lazily
    /// created per-index maps in `index_rows`.
    alloc: A,
    /// Type-erased clone of `alloc` used by logical-log buffers that cross the
    /// durable-storage trait-object and I/O ownership boundaries.
    logical_log_alloc: DynAllocator,
    tx_ids: AtomicU64,
    version_id_counter: AtomicU64,
    next_rowid: AtomicU64,
    next_table_id: AtomicI64,
    clock: Clock,

    /// MVCC durable storage (logical log writes, checkpoint thresholding, recovery state).
    ///
    /// Stored behind a trait object so callers can inject their own implementation
    /// per database (via `Database::durable_storage`) for testing or custom durability.
    storage: Arc<dyn crate::mvcc::persistent_storage::DurableStorage>,

    /// The transaction ID of a transaction that has acquired an exclusive write lock, if any.
    ///
    /// An exclusive MVCC transaction is one that has a write lock on the pager, which means
    /// every other MVCC transaction must wait for it to commit before they can commit. We have
    /// exclusive transactions to support single-writer semantics for compatibility with SQLite.
    ///
    /// If there is no exclusive transaction, the field is set to `NO_EXCLUSIVE_TX`.
    exclusive_tx: AtomicU64,
    /// Custom-index writer leases keyed by the backing object's stable MVCC
    /// table ID. Leases reject contention instead of waiting, so acquiring
    /// several leases cannot deadlock. Entries outlive their holder: each one
    /// remembers when the index was last published, so a transaction whose
    /// read snapshot predates that publication is refused — its rebuild of
    /// the index state starts from a superseded base and committing it would
    /// overwrite the newer publication.
    ///
    /// One writer per index is the intended concurrency model, not a stopgap:
    /// two transactions cannot merge the index state each rebuilt from its
    /// own snapshot, so the later one would silently overwrite the earlier
    /// one's work. The lease makes the second writer fail fast instead.
    /// Writers on different indexes, and all readers, still run concurrently.
    index_method_write_leases: Mutex<HashMap<MVTableId, IndexMethodWriteLease>>,
    commit_coordinator: Arc<CommitCoordinator>,
    global_header: Arc<RwLock<Option<DatabaseHeader>>>,
    /// Held by checkpoints only during the brief in-memory publish phase; the I/O-heavy
    /// MvStore → WAL write-out runs unlocked, so concurrent `BEGIN CONCURRENT`s aren't
    /// blocked. Phases: (unlocked) snapshot + collect + write + commit pager txn;
    /// (locked) publish durable_txid_max / global_header / schema roots; (unlocked) GC,
    /// CheckpointWal, truncate logical log, TruncateWal.
    blocking_checkpoint_lock: Arc<TursoRwLock>,
    /// Passive publish drain: set for the brief in-memory publish window so new `begin_tx`
    /// calls contend out instead of pinning a lifetime checkpoint read guard.
    checkpoint_publish_in_progress: AtomicBool,
    /// Bumped when a passive checkpoint publishes physical btree roots into the shared schema.
    /// Open transactions compare their captured value and get [`LimboError::SchemaUpdated`].
    schema_generation: AtomicU64,
    /// Single-orchestrator gate: set while a CheckpointStateMachine runs its unlocked
    /// write-out phase, cleared on completion/error. Commits racing `should_checkpoint()`
    /// contend on it; only one wins. Needed because the lock no longer guards the start
    /// of the checkpoint (it's acquired after the pager-write phase, not before).
    checkpoint_in_progress: AtomicBool,
    /// The highest transaction ID that has been made durable in the WAL.
    /// Used to skip checkpointing transactions from mv store to WAL that have already been processed.
    durable_txid_max: AtomicU64,
    /// The WAL backfill boundary published by the most recent checkpoint: a version materialized at
    /// or below this `WalPos` is durably in the DB file, so reachable by EVERY snapshot (including
    /// a db-file reader pinned at the boundary). The passive checkpoint GC reclaims a materialized version
    /// only once its `materialized_at <= backfill_floor` — otherwise a low-frame reader that
    /// cannot reach the un-backfilled WAL frame still needs the version-store copy. See
    /// `gc_version_chain` / `gc_floor_reader_mark`. `RwLock<WalPos>` mirrors `global_header`.
    backfill_floor: Arc<RwLock<WalPos>>,
    /// The timestamp of the last committed schema change.
    /// Schema changes always cause a [SchemaUpdated] error.
    last_committed_schema_change_ts: AtomicU64,
    /// The timestamp of the last committed transaction.
    /// If there are two concurrent BEGIN (non-CONCURRENT) transactions, and one tries to promote
    /// to exclusive, it will abort if another transaction committed after its begin timestamp.
    last_committed_tx_ts: AtomicU64,
    /// `end_ts` of the most recent tx whose header was written into
    /// `global_header`. Used to gate header writes at both
    /// `EndCommitLogicalLog` and `FinalizeCommit` so an older commit
    /// finishing after a newer one cannot regress
    /// `global_header.schema_cookie` below the latest committed value
    /// — which would break `maybe_reparse_schema`'s cookie-mismatch
    /// early-exit and strand readers in a reparse-WaitForDependencies
    /// deadlock.
    last_global_header_ts: AtomicU64,
    table_id_to_last_rowid: RwLock<HashMap<MVTableId, Arc<RowidAllocator>>>,
    /// Per-sequence first value not guaranteed safe to read past based only on
    /// durable/current sequence state. Active allocations can lower this.
    sequence_watermarks: Mutex<HashMap<String, i64>>,
    /// Per-sequence minimum allocated value for each active transaction.
    ///
    /// This is in-memory and therefore only correct while all MVCC writers for a
    /// database live in one process. Multi-process MVCC will need a shared
    /// coordination mechanism before sync can rely on this watermark.
    sequence_allocations: Mutex<HashMap<String, StdHashMap<TxID, i64>>>,

    /// Approximate count of live row versions across `rows` + `index_rows`.
    /// Incremented on every inserted version, decremented when versions are
    /// reclaimed (GC) or physically removed (savepoint rollback, checkpoint
    /// purge). This is a *heuristic* used only to decide when to run an
    /// incremental GC pass (`should_gc`) — never a correctness input. Aborted
    /// versions are left counted until GC reclaims them (they still occupy
    /// memory). See [`Self::live_version_count_approx`].
    live_version_count_approx: AtomicUsize,
    /// Snapshot of `live_version_count_approx` taken at the end of the last
    /// `gc_incremental` pass. `should_gc` fires once growth since this snapshot
    /// crosses `gc_version_threshold`, giving a roughly fixed GC cadence per N
    /// new versions regardless of how many a single pass reclaims.
    live_versions_at_last_gc: AtomicUsize,
    /// Growth in `live_version_count_approx` (number of newly inserted versions since
    /// the last GC pass) that triggers an incremental GC pass on the commit
    /// path. Negative disables inline GC entirely. Configurable via the
    /// `mvcc_gc_threshold` PRAGMA; mirrors `checkpoint_threshold`.
    gc_version_threshold: AtomicI64,
    /// Resume cursor for the incremental table-row GC sweep: the last `RowID`
    /// processed by the previous `gc_incremental` pass. `None` restarts the
    /// sweep from the beginning of `rows` (and marks the point where index
    /// chains are swept — see `gc_incremental`).
    gc_table_cursor: Mutex<Option<RowID>>,
    /// Resume cursor for the incremental index-row GC sweep: the last
    /// `(index id, key)` processed by the previous `gc_incremental` pass.
    /// `None` restarts from the beginning of `index_rows`. Mirrors
    /// `gc_table_cursor` but over the nested `index_rows` maps, so a single
    /// huge index can't force an unbounded pass.
    gc_index_cursor: Mutex<Option<(MVTableId, Arc<SortableIndexKey>)>>,
    /// Single-flight gate for inline GC. Many connections commit concurrently
    /// (each shares this `MvStore`), and the commit path calls
    /// `gc_incremental` without holding any global lock — so without this gate
    /// several threads would run overlapping GC passes at once. That is *safe*
    /// (per-chain write locks + idempotent reclamation) but wasteful: redundant
    /// scans plus thrashing of `gc_table_cursor`. This flag ensures at most one
    /// inline pass runs at a time; a committer that loses the race simply skips
    /// GC for that commit.
    gc_in_progress: AtomicBool,
    /// LWM observed by the last inline GC pass that actually ran. When a
    /// long-running transaction pins the LWM, re-scanning at the same LWM
    /// reclaims nothing new — any version superseded since then has
    /// `end_ts > LWM` (the pinning txn is older) and stays visible. So an inline
    /// pass short-circuits when the LWM hasn't advanced, avoiding wasted scans
    /// while a long txn is open. `u64::MAX` means "no pass has run yet" and also
    /// the no-active-txn state, which never short-circuits (there is always
    /// potential garbage to reclaim then). Aborted garbage and redundant current
    /// versions that a skipped pass leaves behind are still collected by the
    /// checkpoint's full sweep.
    gc_last_lwm: AtomicU64,
    experimental_mvcc_passive_checkpoint: bool,
}

impl<Clock: LogicalClock> MvStore<Clock> {
    /// Creates a new database backed by the default [`TursoAllocator`].
    pub fn new(
        clock: Clock,
        storage: Arc<dyn crate::mvcc::persistent_storage::DurableStorage>,
        experimental_mvcc_passive_checkpoint: bool,
    ) -> Result<Self> {
        Self::new_in(
            clock,
            storage,
            TursoAllocator,
            experimental_mvcc_passive_checkpoint,
        )
    }
}

impl<Clock: LogicalClock, A: ConcurrentAllocator> MvStore<Clock, A> {
    pub(crate) fn allocator(&self) -> A {
        self.alloc.clone()
    }

    fn logical_log_allocator(&self) -> DynAllocator {
        self.logical_log_alloc.clone()
    }

    fn uses_durable_mvcc_metadata(&self, connection: &Arc<Connection>) -> bool {
        !connection.db.is_in_memory_db()
    }

    /// Captures table-valued functions (e.g. generate_series) from the schema before
    /// reparse_schema() drops them. Built-in TVFs are registered programmatically and
    /// don't survive schema re-parsing from sqlite_schema; we save and re-inject them.
    fn capture_table_valued_functions(schema: &Schema) -> Vec<Arc<crate::vtab::VirtualTable>> {
        schema
            .tables
            .values()
            .filter_map(|table| match table.as_ref() {
                Table::Virtual(vtab)
                    if matches!(vtab.kind, turso_ext::VTabKind::TableValuedFunction) =>
                {
                    Some(vtab.clone())
                }
                _ => None,
            })
            .collect()
    }

    fn rehydrate_table_valued_functions(
        schema: &mut Schema,
        table_valued_functions: &[Arc<crate::vtab::VirtualTable>],
    ) {
        for vtab in table_valued_functions {
            let normalized_name = crate::util::normalize_ident(&vtab.name);
            schema
                .tables
                .entry(normalized_name)
                .or_insert_with(|| Arc::new(Table::Virtual(vtab.clone())));
        }
    }

    fn rehydrate_connection_table_valued_functions(
        &self,
        connection: &Arc<Connection>,
        table_valued_functions: &[Arc<crate::vtab::VirtualTable>],
    ) -> Result<()> {
        connection.with_schema_mut(|schema| {
            Self::rehydrate_table_valued_functions(schema, table_valued_functions);
        })?;
        *connection.db.schema.lock() = connection.schema.read().clone();
        Ok(())
    }

    /// Creates a new database whose skiplists allocate through `alloc`.
    pub fn new_in(
        clock: Clock,
        storage: Arc<dyn crate::mvcc::persistent_storage::DurableStorage>,
        alloc: A,
        experimental_mvcc_passive_checkpoint: bool,
    ) -> Result<Self> {
        let logical_log_alloc = DynAllocator::new(alloc.clone());
        let table_id_to_rootpage = SkipMap::new_in(alloc.clone());
        // table id 1 / root page 1 is always sqlite_schema.
        table_id_to_rootpage.try_insert(SQLITE_SCHEMA_MVCC_TABLE_ID, RootEntry::live(Some(1)))?;
        Ok(Self {
            rows: SkipMap::new_in(alloc.clone()),
            table_id_to_rootpage,
            index_rows: SkipMap::new_in(alloc.clone()),
            index_rows_epoch: AtomicU64::new(0),
            txs: SkipMap::new_in(alloc.clone()),
            finalized_tx_states: SkipMap::new_in(alloc.clone()),
            alloc,
            logical_log_alloc,
            tx_ids: AtomicU64::new(1), // let's reserve transaction 0 for special purposes
            version_id_counter: AtomicU64::new(1), // Reserve 0 for special purposes
            next_rowid: AtomicU64::new(0), // TODO: determine this from B-Tree
            next_table_id: AtomicI64::new(-2), // table id -1 / root page 1 is always sqlite_schema.
            clock,
            storage,
            exclusive_tx: AtomicU64::new(NO_EXCLUSIVE_TX),
            index_method_write_leases: Mutex::new(HashMap::default()),
            commit_coordinator: Arc::new(CommitCoordinator::new()),
            global_header: Arc::new(RwLock::new(None)),
            backfill_floor: Arc::new(RwLock::new(WalPos::ORIGIN)),
            blocking_checkpoint_lock: Arc::new(TursoRwLock::new()),
            checkpoint_publish_in_progress: AtomicBool::new(false),
            schema_generation: AtomicU64::new(0),
            checkpoint_in_progress: AtomicBool::new(false),
            durable_txid_max: AtomicU64::new(0),
            last_committed_schema_change_ts: AtomicU64::new(0),
            last_committed_tx_ts: AtomicU64::new(0),
            last_global_header_ts: AtomicU64::new(0),
            table_id_to_last_rowid: RwLock::new(HashMap::default()),
            sequence_watermarks: Mutex::new(HashMap::default()),
            sequence_allocations: Mutex::new(HashMap::default()),
            live_version_count_approx: AtomicUsize::new(0),
            live_versions_at_last_gc: AtomicUsize::new(0),
            gc_version_threshold: AtomicI64::new(Self::DEFAULT_GC_VERSION_THRESHOLD),
            gc_table_cursor: Mutex::new(None),
            gc_index_cursor: Mutex::new(None),
            gc_in_progress: AtomicBool::new(false),
            gc_last_lwm: AtomicU64::new(u64::MAX),
            experimental_mvcc_passive_checkpoint,
        })
    }

    /// Get the table ID from the root page, resolving against the current (live) mapping.
    /// Equivalent to `get_table_id_from_root_page_at(root_page, u64::MAX)`.
    pub fn get_table_id_from_root_page(&self, root_page: i64) -> MVTableId {
        self.get_table_id_from_root_page_at(root_page, u64::MAX)
    }

    /// Get the table ID for `root_page` as seen by a transaction at `snapshot_ts`.
    ///
    /// Negative root pages are non-checkpointed objects whose table ID equals the root page;
    /// they are never reused or versioned, so the snapshot is irrelevant.
    ///
    /// For a positive (checkpointed) root page, a PASSIVE checkpoint may have dropped the
    /// object — and possibly reused the page for a new btree — while this transaction still
    /// references it at an older snapshot. Successive owners of a page hold disjoint,
    /// back-to-back lifetimes; we return the owner whose binding has not yet ended at the
    /// snapshot (smallest `end > ts`, live counting as `+inf`). We deliberately do NOT gate on
    /// `begin` here: a transaction's *physical* schema (root pages) can run ahead of its *data*
    /// snapshot, because a checkpoint allocating a root page is not a logical schema change.
    /// Whether the btree should actually be read at the snapshot is decided separately by
    /// [`Self::is_btree_allocated_at`] / [`Self::resolve_root_page_at`], which do gate on
    /// `begin`. `u64::MAX` resolves the current live owner.
    pub fn get_table_id_from_root_page_at(&self, root_page: i64, snapshot_ts: u64) -> MVTableId {
        self.try_get_table_id_from_root_page_at(root_page, snapshot_ts)
            .unwrap_or_else(|| {
                panic!("Positive root page is not mapped to a table id: {root_page}")
            })
    }

    /// Fallible variant of [`Self::get_table_id_from_root_page_at`]: returns `None` when a
    /// positive root page has no binding that covers `snapshot_ts`. Under a PASSIVE checkpoint
    /// this is not an invariant violation but a stale-schema read: the transaction captured an
    /// older `schema_cookie` (the commit that dropped this object published its cookie after the
    /// transaction read the header, even though the drop's commit ts precedes the transaction's
    /// begin ts) and compiled a cursor against a table its own snapshot already sees dropped.
    /// The caller turns this into [`LimboError::SchemaUpdated`] so the statement reprepares
    /// against the current schema. See the begin/commit schema-coherence note in the passive
    /// checkpoint design.
    pub fn try_get_table_id_from_root_page_at(
        &self,
        root_page: i64,
        snapshot_ts: u64,
    ) -> Option<MVTableId> {
        if root_page < 0 {
            // Not checkpointed table - table ID and root_page are both the same negative value
            return Some(root_page.into());
        }
        let root_page = root_page as u64;
        self.table_id_to_rootpage
            .iter()
            .filter(|entry| {
                let e = entry.value();
                e.root_page == Some(root_page) && (e.is_live() || snapshot_ts < e.end)
            })
            .min_by_key(|entry| entry.value().end)
            .map(|entry| *entry.key())
    }

    /// Snapshot timestamp (`begin_ts`) of the given transaction, or `u64::MAX` if it is not
    /// tracked (resolving the live root-page binding). Used to make a transaction's root-page
    /// lookups snapshot-consistent.
    pub fn read_snapshot_ts(&self, tx_id: TxID) -> u64 {
        self.txs
            .get(&tx_id)
            .map(|tx| tx.value().begin_ts)
            .unwrap_or(u64::MAX)
    }

    /// This transaction's frozen WAL read mark, or [`WalPos::STAGED`] (sees everything published)
    /// if untracked. The physical-reachability coordinate of the btree-read gate. See
    /// [`Self::is_btree_readable_at`].
    pub fn read_tx_mark(&self, tx_id: TxID) -> WalPos {
        self.txs
            .get(&tx_id)
            .map(|tx| tx.value().read_mark)
            .unwrap_or(WalPos::STAGED)
    }

    /// Bump `next_table_id` below `table_id` (and below `-root_page` for a checkpointed root) so
    /// recovery's `table_id = -root_page` assignment can never collide with an existing id.
    fn bump_next_table_id_below(&self, table_id: MVTableId, root_page: Option<u64>) {
        let minimum: i64 = if let Some(root_page) = root_page {
            let root_page_as_table_id = MVTableId::from(-(root_page as i64));
            table_id.min(root_page_as_table_id).into()
        } else {
            table_id.into()
        };
        if minimum <= self.next_table_id.load(Ordering::SeqCst) {
            self.next_table_id.store(minimum - 1, Ordering::SeqCst);
        }
    }

    /// Insert a live `table_id -> root_page` binding (bootstrap/recovery, or an
    /// uncheckpointed-create with `None`). Visible to every snapshot. Checkpoint-time
    /// allocation of a real root page goes through [`Self::record_rootpage_alloc`] instead.
    pub fn insert_table_id_to_rootpage(&self, table_id: MVTableId, root_page: Option<u64>) {
        self.table_id_to_rootpage
            .insert(table_id, RootEntry::live(root_page));
        self.bump_next_table_id_below(table_id, root_page);
    }

    pub fn remove_table_id_to_rootpage(&self, table_id: &MVTableId) {
        self.table_id_to_rootpage.remove(table_id);
        self.table_id_to_last_rowid.write().remove(table_id);
    }

    /// The current physical root page of `table_id`, if it is checkpointed and live.
    pub fn current_root_page(&self, table_id: &MVTableId) -> Option<u64> {
        self.table_id_to_rootpage
            .get(table_id)
            .and_then(|entry| entry.value().root_page)
    }

    /// Record that a PASSIVE checkpoint allocated `root_page` for `table_id`. `begin_ts` is the
    /// checkpoint's snapshot ts (base-validity lower bound); `materialized_at` is the WAL position
    /// the pages reach durability at — [`WalPos::STAGED`] at `btree_create` (pages not committed
    /// yet), lowered to the real position by [`Self::publish_rootpage_visible`] in the
    /// post-`CommitPagerTxn` publish window.
    pub fn record_rootpage_alloc(
        &self,
        table_id: MVTableId,
        root_page: u64,
        begin_ts: u64,
        materialized_at: WalPos,
    ) {
        self.table_id_to_rootpage.insert(
            table_id,
            RootEntry {
                root_page: Some(root_page),
                begin: begin_ts,
                end: u64::MAX,
                materialized_at,
            },
        );
        // A page has one live owner. Claiming this page means it was freed+reused; retire any
        // stale prior owner still marked live for it (drop-time retire raced collection),
        // else two live bindings resolve to one page (integrity_check: referenced twice).
        let stale: Vec<(MVTableId, RootEntry)> = self
            .table_id_to_rootpage
            .iter()
            .filter(|entry| {
                let e = entry.value();
                e.is_live() && e.root_page == Some(root_page) && *entry.key() != table_id
            })
            .map(|entry| (*entry.key(), *entry.value()))
            .collect();
        for (key, mut entry) in stale {
            entry.end = begin_ts;
            self.table_id_to_rootpage.insert(key, entry);
        }
        self.bump_next_table_id_below(table_id, Some(root_page));
    }

    /// Publish a staged root-page binding: set its `materialized_at` from [`WalPos::STAGED`] to the
    /// WAL position the pages were committed at, making the btree physically readable by any
    /// transaction whose read mark reaches that position. Called in the checkpoint's post-commit
    /// publish window. No-op if the entry is gone (e.g. dropped same checkpoint).
    pub fn publish_rootpage_visible(&self, table_id: MVTableId, materialized_at: WalPos) {
        if let Some(entry) = self.table_id_to_rootpage.get(&table_id) {
            let mut e = *entry.value();
            e.materialized_at = materialized_at;
            self.table_id_to_rootpage.insert(table_id, e);
        }
    }

    /// Close `table_id`'s binding at `end_ts` (the drop tombstone commit ts) but keep it, so a
    /// transaction whose snapshot predates the drop can still resolve the (read-mark-protected)
    /// root page. Reclaimed by [`Self::gc_rootpage_entries`] once `end_ts <= lwm`.
    pub fn retire_rootpage(&self, table_id: MVTableId, end_ts: u64) {
        if let Some(entry) = self.table_id_to_rootpage.get(&table_id) {
            let mut e = *entry.value();
            e.end = end_ts;
            self.table_id_to_rootpage.insert(table_id, e);
        }
    }

    /// Drop closed (retired) bindings no transaction can still see (dropped, with
    /// `end <= lwm`). Live bindings have `end == u64::MAX` and are never reclaimed — important
    /// because `compute_lwm()` is `u64::MAX` when no transactions are active. Returns count.
    pub fn gc_rootpage_entries(&self, lwm: u64) -> usize {
        let stale: Vec<MVTableId> = self
            .table_id_to_rootpage
            .iter()
            .filter(|entry| {
                let e = entry.value();
                !e.is_live() && e.end <= lwm
            })
            .map(|entry| *entry.key())
            .collect();
        for key in &stale {
            self.table_id_to_rootpage.remove(key);
            self.table_id_to_last_rowid.write().remove(key);
        }
        stale.len()
    }

    /// Acquire MVCC's stop-the-world gate for VACUUM.
    ///
    /// This is the same lock used by MVCC checkpointing. All MVCC transactions
    /// hold it in read mode for their whole lifetime, so acquiring it in write
    /// mode proves there are no active MVCC transactions and prevents new ones
    /// from starting until VACUUM releases it.
    pub(crate) fn try_begin_vacuum_gate(&self) -> Result<()> {
        if !self.blocking_checkpoint_lock.write() {
            return Err(LimboError::Busy);
        }
        turso_assert!(
            self.txs.is_empty(),
            "MVCC vacuum gate acquired while transactions are still active"
        );
        Ok(())
    }

    /// Release the MVCC stop-the-world gate acquired by `try_begin_vacuum_gate`.
    pub(crate) fn release_vacuum_gate(&self) {
        self.blocking_checkpoint_lock.unlock();
    }

    /// VACUUM copies the physical DB image, so any MVCC logical-log bytes must
    /// be checkpointed first.
    pub(crate) fn has_uncheckpointed_log(&self) -> Result<bool> {
        Ok(self.get_logical_log_file().size()? != 0)
    }

    /// Rebuild MVCC's physical root-page metadata after in-place VACUUM
    /// reparses the rewritten B-tree image and stages the committed page-1
    /// header that now owns the physical schema cookie.
    ///
    /// The caller must hold the MVCC vacuum gate and pass both the committed
    /// page-1 header and the schema parsed from the post-VACUUM physical
    /// database image.
    pub(crate) fn reset_after_vacuum(&self, header: DatabaseHeader, schema: &Schema) {
        turso_assert!(
            self.txs.is_empty(),
            "MVCC VACUUM reset requires no active transactions"
        );
        // see the test `test_mvcc_plain_vacuum_active_write_tx_returns_busy`
        self.drop_unused_row_versions();
        let has_table_versions = self
            .rows
            .iter()
            .any(|entry| !entry.value().read().is_empty());
        turso_assert!(
            !has_table_versions,
            "MVCC VACUUM reset requires checkpointed table versions to be cleared"
        );
        let has_index_versions = self.index_rows.iter().any(|index_entry| {
            index_entry
                .value()
                .iter()
                .any(|entry| !entry.value().read().is_empty())
        });
        turso_assert!(
            !has_index_versions,
            "MVCC VACUUM reset requires checkpointed index versions to be cleared"
        );
        turso_assert!(
            self.finalized_tx_states.is_empty(),
            "MVCC VACUUM reset requires finalized transaction cache to be cleared"
        );
        // Drop empty buckets left by checkpoint GC: their table_ids reference
        // pre-VACUUM root pages and can alias new objects after root-page
        // reuse, corrupting `index_rows` lookups and SkipMap ordering.
        self.rows.clear();
        self.index_rows.clear();
        let root_pages = schema
            .tables
            .values()
            .filter_map(|table| match table.as_ref() {
                Table::BTree(btree) => Some(btree.root_page),
                _ => None,
            })
            .chain(
                schema
                    .indexes
                    .values()
                    .flatten()
                    .filter(|index| index.is_btree_backed())
                    .map(|index| index.root_page),
            )
            .collect::<Vec<_>>();
        for &root_page in &root_pages {
            turso_assert!(
                root_page > 0,
                "post-VACUUM B-tree root page must be positive"
            );
        }
        // Clears live and retired bindings alike: both reference pre-VACUUM root pages that
        // would alias new objects after root-page reuse.
        self.table_id_to_rootpage.clear();
        self.table_id_to_last_rowid.write().clear();
        // TODO: vacuum related code, not handling alloc errors for now
        self.insert_table_id_to_rootpage(SQLITE_SCHEMA_MVCC_TABLE_ID, Some(1));
        for root_page in root_pages {
            let table_id = MVTableId::from(-root_page);
            self.insert_table_id_to_rootpage(table_id, Some(root_page as u64));
        }
        self.global_header.write().replace(header);
    }

    /// Creates the `__turso_internal_mvcc_meta` table and seeds it with
    /// `persistent_tx_ts_max` (initialized to 0). This table stores the durable replay
    /// boundary: on recovery, only logical-log frames with `commit_ts > persistent_tx_ts_max`
    /// are replayed. Called once during first MVCC bootstrap.
    /// Creates and seeds the MVCC metadata table, driving the CREATE TABLE then
    /// INSERT statements cooperatively via the supplied [`InitMetadataTableState`]
    /// so bootstrap does not block on backends without a synchronous IO pump.
    fn initialize_mvcc_metadata_table_nonblock(
        &self,
        connection: &Arc<Connection>,
        st: &mut InitMetadataTableState,
    ) -> IOResultOr<()> {
        loop {
            match st {
                InitMetadataTableState::Start => {
                    let stmt = connection.prepare(format!(
                        "CREATE TABLE IF NOT EXISTS {MVCC_META_TABLE_NAME}(k TEXT, v INTEGER NOT NULL)"
                    ))?;
                    *st = InitMetadataTableState::CreateTable {
                        stmt: Box::new(stmt),
                    };
                }
                InitMetadataTableState::CreateTable { stmt } => {
                    return_if_io!(stmt.run_ignore_rows_nonblock());
                    let stmt = connection.prepare(format!(
                        "INSERT OR IGNORE INTO {MVCC_META_TABLE_NAME}(rowid, k, v) VALUES (1, '{MVCC_META_KEY_PERSISTENT_TX_TS_MAX}', 0)"
                    ))?;
                    *st = InitMetadataTableState::Insert {
                        stmt: Box::new(stmt),
                    };
                }
                InitMetadataTableState::Insert { stmt } => {
                    return_if_io!(stmt.run_ignore_rows_nonblock());
                    *st = InitMetadataTableState::Start;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    /// Shared validation/normalization for the persistent-tx-ts-max metadata
    /// value used by both the blocking and non-blocking readers.
    fn validate_persistent_tx_ts_max(value: Option<i64>) -> Result<Option<u64>> {
        let value = value.ok_or_else(|| {
            LimboError::Corrupt(format!(
                "Missing MVCC metadata row for key {MVCC_META_KEY_PERSISTENT_TX_TS_MAX}"
            ))
        })?;

        if value < 0 {
            return Err(LimboError::Corrupt(format!(
                "Invalid MVCC metadata value for {MVCC_META_KEY_PERSISTENT_TX_TS_MAX}: {value}"
            )));
        }
        Ok(Some(value as u64))
    }

    /// Non-blocking variant of [`Self::try_read_persistent_tx_ts_max`]: runs the
    /// metadata SELECT cooperatively via the supplied
    /// [`ReadPersistentTxTsMaxState`], yielding IO instead of pumping it. Returns
    /// `None` if the metadata table does not exist yet.
    fn try_read_persistent_tx_ts_max_nonblock(
        &self,
        connection: &Arc<Connection>,
        st: &mut ReadPersistentTxTsMaxState,
    ) -> IOResultOr<Option<u64>> {
        loop {
            match st {
                ReadPersistentTxTsMaxState::Start => {
                    let query_result = connection.query(format!(
                        "SELECT v FROM {MVCC_META_TABLE_NAME}
                         WHERE k = '{MVCC_META_KEY_PERSISTENT_TX_TS_MAX}'"
                    ));
                    let maybe_stmt = match query_result {
                        Ok(stmt) => stmt,
                        Err(LimboError::ParseError(msg)) if msg.contains("no such table") => {
                            return Ok(IOResult::Done(None));
                        }
                        Err(err) => {
                            return Err(LimboError::Corrupt(format!(
                                "Failed to read MVCC metadata table: {err}"
                            ))
                            .into());
                        }
                    };
                    match maybe_stmt {
                        Some(stmt) => {
                            *st = ReadPersistentTxTsMaxState::Running {
                                stmt: Box::new(stmt),
                                value: None,
                            };
                        }
                        // No statement to run — fall through to the missing-row
                        // error path (matches the blocking variant).
                        None => {
                            return Self::validate_persistent_tx_ts_max(None)
                                .map(IOResult::Done)
                                .map_err(Into::into);
                        }
                    }
                }
                ReadPersistentTxTsMaxState::Running { stmt, value } => {
                    return_if_io!(stmt.run_with_row_callback_nonblock(|row| {
                        *value = Some(row.get::<i64>(0)?);
                        Ok(())
                    }));
                    let value = *value;
                    *st = ReadPersistentTxTsMaxState::Start;
                    return Self::validate_persistent_tx_ts_max(value)
                        .map(IOResult::Done)
                        .map_err(Into::into);
                }
            }
        }
    }

    /// Bootstrap the MV store from the SQLite schema table and logical log.
    /// 1. Get all root pages from the already parsed schema object
    /// 2. Assign table IDs to the root pages (table_id = -1 * root_page)
    /// 3. Complete interrupted WAL/log checkpoint reconciliation, if needed
    /// 4. Promote the bootstrap connection to a regular connection so that it reads from the MV store again
    /// 5. Recover the logical log
    /// 6. Make sure schema changes reflected from deserialized logical log are captured in the schema
    ///
    /// Blocking shim retained for synchronous callers. The open, attach, and
    /// journal-mode state machines drive
    /// [`MvStore::bootstrap_nonblock`] directly.
    pub fn bootstrap(&self, bootstrap_conn: Arc<Connection>) -> Result<()> {
        let mut st = BootstrapState::default();
        let io = bootstrap_conn.db.io.clone();
        io.block(|| self.bootstrap_nonblock(&bootstrap_conn, &mut st))
    }

    #[doc(hidden)]
    pub fn bootstrap_nonblock(
        &self,
        bootstrap_conn: &Arc<Connection>,
        st: &mut BootstrapState,
    ) -> IOResultOr<()> {
        loop {
            match st {
                BootstrapState::Start => {
                    // Capture built-in table-valued functions before the schema
                    // reparse drops them (sync, no IO).
                    let tvfs = Self::capture_table_valued_functions(&bootstrap_conn.schema.read());
                    *st = BootstrapState::PreCheckpoint {
                        tvfs,
                        checkpoint_st: CompleteCheckpointState::default(),
                    };
                }
                BootstrapState::PreCheckpoint {
                    tvfs,
                    checkpoint_st,
                } => {
                    return_if_io!(self.maybe_complete_interrupted_checkpoint_nonblock(
                        bootstrap_conn,
                        checkpoint_st
                    ));
                    let tvfs = std::mem::take(tvfs);
                    *st = BootstrapState::PreReparse {
                        tvfs,
                        reparse_st: crate::connection::ReparseSchemaState::default(),
                    };
                }
                BootstrapState::PreReparse { tvfs, reparse_st } => {
                    return_if_io!(bootstrap_conn.reparse_schema_nonblock(reparse_st));
                    self.rehydrate_connection_table_valued_functions(bootstrap_conn, tvfs)?;
                    // pre_metadata done. Decide whether metadata bootstrap IO is needed.
                    if !self.uses_durable_mvcc_metadata(bootstrap_conn) {
                        self.bootstrap_map_root_pages(bootstrap_conn)?;
                        *st = BootstrapState::Recover {
                            recover_st: RecoverLogicalLogState::default(),
                        };
                    } else {
                        *st = BootstrapState::BeginReadTxTs {
                            read_st: ReadPersistentTxTsMaxState::default(),
                        };
                    }
                }
                BootstrapState::BeginReadTxTs { read_st } => {
                    let persistent_tx_ts = return_if_io!(
                        self.try_read_persistent_tx_ts_max_nonblock(bootstrap_conn, read_st)
                    );
                    if persistent_tx_ts.is_some() {
                        // Metadata already durable — no bootstrap IO chain needed.
                        self.bootstrap_map_root_pages(bootstrap_conn)?;
                        *st = BootstrapState::Recover {
                            recover_st: RecoverLogicalLogState::default(),
                        };
                    } else if let Some(next) = self.bootstrap_issue_metadata_io(bootstrap_conn)? {
                        *st = BootstrapState::MetadataIo(next);
                    } else {
                        self.bootstrap_map_root_pages(bootstrap_conn)?;
                        *st = BootstrapState::Recover {
                            recover_st: RecoverLogicalLogState::default(),
                        };
                    }
                }
                BootstrapState::MetadataIo(phase) => {
                    if !phase.completion.succeeded() {
                        let c = phase.completion.clone();
                        io_yield_one!(c);
                    }
                    let sync_type = phase.sync_type;
                    match phase.next {
                        MetadataIoStep::AfterTruncate {
                            log_size,
                            sync_mode_off,
                        } => {
                            // Truncate done. Maybe issue update_header next.
                            if log_size <= LOG_HDR_SIZE as u64 {
                                let c = self.storage.update_header()?;
                                *phase = MetadataIoInFlight {
                                    completion: c,
                                    sync_type,
                                    next: MetadataIoStep::AfterUpdateHeader { sync_mode_off },
                                };
                                // Loop to re-check succeeded immediately
                            } else {
                                *st = BootstrapState::FinishInit {
                                    init_st: InitMetadataTableState::default(),
                                };
                            }
                        }
                        MetadataIoStep::AfterUpdateHeader { sync_mode_off } => {
                            if !sync_mode_off {
                                let c = self.storage.sync(sync_type)?;
                                *phase = MetadataIoInFlight {
                                    completion: c,
                                    sync_type,
                                    next: MetadataIoStep::AfterSync,
                                };
                            } else {
                                *st = BootstrapState::FinishInit {
                                    init_st: InitMetadataTableState::default(),
                                };
                            }
                        }
                        MetadataIoStep::AfterSync => {
                            *st = BootstrapState::FinishInit {
                                init_st: InitMetadataTableState::default(),
                            };
                        }
                    }
                }
                BootstrapState::FinishInit { init_st } => {
                    return_if_io!(
                        self.initialize_mvcc_metadata_table_nonblock(bootstrap_conn, init_st)
                    );
                    *st = BootstrapState::FinishCheckpoint {
                        checkpoint_st: CompleteCheckpointState::default(),
                    };
                }
                BootstrapState::FinishCheckpoint { checkpoint_st } => {
                    return_if_io!(self.maybe_complete_interrupted_checkpoint_nonblock(
                        bootstrap_conn,
                        checkpoint_st
                    ));
                    self.bootstrap_map_root_pages(bootstrap_conn)?;
                    *st = BootstrapState::Recover {
                        recover_st: RecoverLogicalLogState::default(),
                    };
                }
                BootstrapState::Recover { recover_st } => {
                    // Recover the logical log while the bootstrap connection still
                    // reads from the pager-backed schema, so recovery can merge
                    // checkpointed sqlite_schema rows with non-checkpointed rows
                    // from log replay.
                    return_if_io!(self.maybe_recover_logical_log(bootstrap_conn, recover_st));
                    // Recovery done; switch back to regular MVCC reads.
                    bootstrap_conn.promote_to_regular_connection();
                    *st = BootstrapState::LoadSequences {
                        load_st: crate::connection::LoadSequenceDescriptorsState::default(),
                    };
                }
                BootstrapState::LoadSequences { load_st } => {
                    // After log replay, recover sequence descriptors from each
                    // backing table (non-blocking). A read failure on an internal
                    // backing table is on-disk corruption, surfaced as a hard
                    // bootstrap failure rather than a misleading "sequence does
                    // not exist" on the next nextval.
                    return_if_io!(
                        bootstrap_conn.load_sequence_descriptors_via_sql_nonblock(load_st)
                    );
                    *st = BootstrapState::SyncAutoincrement {
                        sync_st: crate::connection::SyncAutoincrementState::default(),
                    };
                }
                BootstrapState::SyncAutoincrement { sync_st } => {
                    // WAL→MVCC AUTOINCREMENT watermark compatibility sync (non-
                    // blocking). A failure here cannot be downgraded: it would
                    // leave the next AUTOINCREMENT INSERT able to re-emit a rowid
                    // already on disk, so it propagates as a bootstrap failure.
                    return_if_io!(bootstrap_conn
                        .sync_autoincrement_backing_tables_from_sqlite_sequence_nonblock(sync_st));
                    *bootstrap_conn.db.schema.lock() = bootstrap_conn.schema.read().clone();
                    *st = BootstrapState::AwaitingGlobalHeader;
                }
                BootstrapState::AwaitingGlobalHeader => {
                    if self.global_header.read().is_none() {
                        let pager = bootstrap_conn.pager.load();
                        let header = return_if_io!(pager.with_header(|header| *header));
                        self.global_header.write().replace(header);
                    }
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    /// Issue the first completion of the metadata-bootstrap IO chain, if one is
    /// needed. Called from the bootstrap state machine only after it has already
    /// confirmed durable MVCC metadata is in use and the persistent tx-ts max is
    /// absent. Returns `Some(MetadataIoInFlight)` with the in-flight completion
    /// and next step, or `None` if the log is already in a clean state.
    fn bootstrap_issue_metadata_io(
        &self,
        bootstrap_conn: &Arc<Connection>,
    ) -> Result<Option<MetadataIoInFlight>> {
        let log_size = self.get_logical_log_file().size()?;
        let pager = bootstrap_conn.pager.load().clone();
        if bootstrap_conn.db.is_readonly() {
            return Err(LimboError::Corrupt(
                "Missing MVCC metadata table in read-only mode".to_string(),
            ));
        }
        if log_size > LOG_HDR_SIZE as u64 {
            return Err(LimboError::Corrupt(
                "Missing MVCC metadata table while logical log state exists".to_string(),
            ));
        }
        let sync_type = pager.get_sync_type();
        let sync_mode_off = bootstrap_conn.get_sync_mode() == SyncMode::Off;

        // First-time MVCC bootstrap: ensure a durable logical-log header exists
        // before any metadata-table writes can commit into WAL. If a previous
        // crash left a torn header tail (0 < size < LOG_HDR_SIZE), clear it
        // before rewriting the header.
        if log_size > 0 && log_size < LOG_HDR_SIZE as u64 {
            let log_file = self.get_logical_log_file();
            let completion = log_file.truncate(0, Completion::new_trunc(|_| {}))?;
            return Ok(Some(MetadataIoInFlight {
                completion,
                sync_type,
                next: MetadataIoStep::AfterTruncate {
                    log_size,
                    sync_mode_off,
                },
            }));
        }
        if log_size <= LOG_HDR_SIZE as u64 {
            let completion = self.storage.update_header()?;
            return Ok(Some(MetadataIoInFlight {
                completion,
                sync_type,
                next: MetadataIoStep::AfterUpdateHeader { sync_mode_off },
            }));
        }
        // Shouldn't reach here given the earlier error returns, but be safe.
        Ok(None)
    }

    /// Sync prelude to logical-log recovery: map all existing checkpointed
    /// sqlite_schema root pages to MVCC table ids (root_page=R → table_id=-R).
    /// Recovery itself (and the connection promotion) is driven separately by
    /// the bootstrap state machine's `Recover` phase so it can yield IO.
    fn bootstrap_map_root_pages(&self, bootstrap_conn: &Arc<Connection>) -> Result<()> {
        let schema = bootstrap_conn.schema.read();
        let sqlite_schema_root_pages = {
            schema
                .tables
                .values()
                .filter_map(|t| {
                    if let Table::BTree(btree) = t.as_ref() {
                        Some(btree.root_page)
                    } else {
                        None
                    }
                })
                .chain(
                    schema
                        .indexes
                        .values()
                        .flatten()
                        .filter(|index| index.is_btree_backed())
                        .map(|index| index.root_page),
                )
        };
        for root_page in sqlite_schema_root_pages {
            turso_assert!(root_page > 0, "root_page={root_page} must be positive");
            let root_page_as_table_id = MVTableId::from(-(root_page));
            self.insert_table_id_to_rootpage(root_page_as_table_id, Some(root_page as u64));
        }
        Ok(())
    }

    /// MVCC does not use the pager/btree cursors to create pages until checkpoint.
    /// This method is used to assign root page numbers when Insn::CreateBtree is used.
    /// MVCC table ids are always negative. Their corresponding rootpage entry in sqlite_schema
    /// is the same negative value if the table has not been checkpointed yet. Otherwise, the root page
    /// will be positive and corresponds to the actual physical page.
    pub fn get_next_table_id(&self) -> i64 {
        self.next_table_id.fetch_sub(1, Ordering::SeqCst)
    }

    pub fn get_next_rowid(&self) -> i64 {
        self.next_rowid.fetch_add(1, Ordering::SeqCst) as i64
    }

    /// Inserts a new row into a table in the database.
    ///
    /// This function inserts a new `row` into the database within the context
    /// of the transaction `tx_id`.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - the ID of the transaction in which to insert the new row.
    /// * `row` - the row object containing the values to be inserted.
    ///
    pub fn insert(&self, tx_id: TxID, row: Row) -> Result<()> {
        self.insert_to_table_or_index(tx_id, row, None)
    }

    /// Same as insert() but can insert to a table or an index, indicated by the `maybe_index_id` argument.
    pub fn insert_to_table_or_index(
        &self,
        tx_id: TxID,
        row: Row,
        maybe_index_id: Option<MVTableId>,
    ) -> Result<()> {
        tracing::trace!("insert(tx_id={}, row.id={:?})", tx_id, row.id);
        let tx = self
            .txs
            .get(&tx_id)
            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
        let tx = tx.value();
        turso_assert_eq!(tx.state, TransactionState::Active);
        let id = row.id.clone();
        match maybe_index_id {
            Some(index_id) => {
                let version_id = self.get_version_id();
                let row_version = RowVersion {
                    id: version_id,
                    begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(
                        tx.tx_id,
                    ))),
                    end: crate::mvcc::database::PackedTs::pack(None),
                    row: row.clone(),
                    btree_resident: false,
                    materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                };
                let RowKey::Record(sortable_key) = row.id.row_id else {
                    panic!("Index writes must be to a record");
                };
                // Single SkipMap traversal: pass in a fresh Arc; the SkipMap
                // returns the canonical Arc (ours on miss, an existing one
                // on hit), which we hand to savepoint tracking.
                let (canonical_key, row_versions) =
                    self.insert_index_version(index_id, sortable_key, row_version)?;
                tx.insert_to_write_set(
                    RowID::new(id.table_id, RowKey::Record(canonical_key.clone())),
                    row_versions,
                );
                tx.record_created_index_version((index_id, canonical_key), version_id);
            }
            None => {
                // NOTE: We do NOT check for conflicts at insert time (pure optimistic).
                // Conflicts are detected at commit time using end_ts comparison.
                // This allows multiple transactions to insert the same rowid,
                // with first-committer-wins semantics.

                let version_id = self.get_version_id();
                let row_version = RowVersion {
                    id: version_id,
                    begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(
                        tx.tx_id,
                    ))),
                    end: crate::mvcc::database::PackedTs::pack(None),
                    row,
                    btree_resident: false,
                    materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                };
                let row_versions = self.insert_version(id.clone(), row_version)?;
                let allocator = self.get_rowid_allocator(&id.table_id);
                allocator.insert_row_id_maybe_update(id.row_id.to_int_or_panic());
                tx.record_created_table_version(id.clone(), version_id);
                tx.insert_to_write_set(id, row_versions);
            }
        }
        Ok(())
    }

    /// Inserts a deletion record for a row that does not currently have any versions in the MV store.
    /// This is used in cases where the BTree contains that record, but it is logically deleted.
    pub fn insert_tombstone_to_table_or_index(
        &self,
        tx_id: TxID,
        id: RowID,
        row: Row,
        maybe_index_id: Option<MVTableId>,
    ) -> Result<()> {
        let version_id = self.get_version_id();
        let row_version = RowVersion {
            id: version_id,
            // Tombstones over B-tree-resident rows have no MVCC creator begin.
            // They invalidate B-tree visibility via end timestamp only.
            begin: crate::mvcc::database::PackedTs::pack(None),
            end: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(tx_id))),
            row: row.clone(),
            btree_resident: true,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        };
        let tx = self
            .txs
            .get(&tx_id)
            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
        let tx = tx.value();
        match maybe_index_id {
            Some(index_id) => {
                let RowKey::Record(sortable_key) = row.id.row_id else {
                    panic!("Index writes must be to a record");
                };
                let (canonical_key, row_versions) =
                    self.insert_index_version(index_id, sortable_key, row_version)?;
                tx.insert_to_write_set(
                    RowID::new(id.table_id, RowKey::Record(canonical_key.clone())),
                    row_versions,
                );
                tx.record_created_index_version((index_id, canonical_key), version_id);
            }
            None => {
                let row_versions = self.insert_version(id.clone(), row_version)?;
                tx.record_created_table_version(id.clone(), version_id);
                tx.insert_to_write_set(id, row_versions);
            }
        }
        Ok(())
    }

    /// Inserts a row that was read from the B-tree (not in MvStore).
    /// This is used when updating a row that exists in B-tree but hasn't been
    /// modified in MVCC yet. The btree_resident flag helps the checkpoint logic
    /// determine if subsequent deletes should be checkpointed to the B-tree file.
    pub fn insert_btree_resident_to_table_or_index(
        &self,
        tx_id: TxID,
        row: Row,
        maybe_index_id: Option<MVTableId>,
    ) -> Result<()> {
        tracing::trace!(
            "insert_btree_resident(tx_id={}, row.id={:?})",
            tx_id,
            row.id
        );
        let tx = self
            .txs
            .get(&tx_id)
            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
        let tx = tx.value();
        turso_assert_eq!(tx.state, TransactionState::Active);
        let id = row.id.clone();
        match maybe_index_id {
            Some(index_id) => {
                let version_id = self.get_version_id();
                let row_version = RowVersion {
                    id: version_id,
                    begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(
                        tx.tx_id,
                    ))),
                    end: crate::mvcc::database::PackedTs::pack(None),
                    row: row.clone(),
                    btree_resident: true,
                    materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                };
                let RowKey::Record(sortable_key) = row.id.row_id else {
                    panic!("Index writes must be to a record");
                };
                let (canonical_key, row_versions) =
                    self.insert_index_version(index_id, sortable_key, row_version)?;
                tx.insert_to_write_set(
                    RowID::new(id.table_id, RowKey::Record(canonical_key.clone())),
                    row_versions,
                );
                tx.record_created_index_version((index_id, canonical_key), version_id);
            }
            None => {
                let version_id = self.get_version_id();
                let row_version = RowVersion {
                    id: version_id,
                    begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(
                        tx.tx_id,
                    ))),
                    end: crate::mvcc::database::PackedTs::pack(None),
                    row,
                    btree_resident: true,
                    materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                };
                let row_versions = self.insert_version(id.clone(), row_version)?;
                tx.record_created_table_version(id.clone(), version_id);
                tx.insert_to_write_set(id, row_versions);
            }
        }
        Ok(())
    }

    /// Updates a row in a table in the database with new values.
    ///
    /// This function updates an existing row in the database within the
    /// context of the transaction `tx_id`. The `row` argument identifies the
    /// row to be updated as `id` and contains the new values to be inserted.
    ///
    /// If the row identified by the `id` does not exist, this function does
    /// nothing and returns `false`. Otherwise, the function updates the row
    /// with the new values and returns `true`.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - the ID of the transaction in which to update the new row.
    /// * `row` - the row object containing the values to be updated.
    ///
    /// # Returns
    ///
    /// Returns `true` if the row was successfully updated, and `false` otherwise.
    pub fn update(&self, tx_id: TxID, row: Row) -> Result<bool> {
        self.update_to_table_or_index(tx_id, row, None)
    }

    /// Same as update() but can update a table or an index, indicated by the `maybe_index_id` argument.
    pub fn update_to_table_or_index(
        &self,
        tx_id: TxID,
        row: Row,
        maybe_index_id: Option<MVTableId>,
    ) -> Result<bool> {
        tracing::trace!("update(tx_id={}, row.id={:?})", tx_id, row.id);
        if !self.delete_from_table_or_index(tx_id, row.id.clone(), maybe_index_id)? {
            return Ok(false);
        }
        self.insert_to_table_or_index(tx_id, row, maybe_index_id)?;
        Ok(true)
    }

    /// Inserts a row into a table in the database with new values, previously deleting
    /// any old data if it existed. Bails on a delete error, e.g. write-write conflict.
    pub fn upsert(&self, tx_id: TxID, row: Row) -> Result<()> {
        self.upsert_to_table_or_index(tx_id, row, None)
    }

    /// Same as upsert() but can upsert to a table or an index, indicated by the `maybe_index_id` argument.
    pub fn upsert_to_table_or_index(
        &self,
        tx_id: TxID,
        row: Row,
        maybe_index_id: Option<MVTableId>,
    ) -> Result<()> {
        tracing::trace!("upsert(tx_id={}, row.id={:?})", tx_id, row.id);
        self.delete_from_table_or_index(tx_id, row.id.clone(), maybe_index_id)?;
        self.insert_to_table_or_index(tx_id, row, maybe_index_id)?;
        Ok(())
    }

    /// Deletes a row from the table with the given `id`.
    ///
    /// This function deletes an existing row `id` in the database within the
    /// context of the transaction `tx_id`.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - the ID of the transaction in which to delete the new row.
    /// * `id` - the ID of the row to delete.
    ///
    /// # Returns
    ///
    /// Returns `true` if the row was successfully deleted, and `false` otherwise.
    ///
    pub fn delete(&self, tx_id: TxID, id: RowID) -> Result<bool> {
        self.delete_from_table_or_index(tx_id, id, None)
    }

    /// Same as delete() but can delete from a table or an index, indicated by the `maybe_index_id` argument.
    pub fn delete_from_table_or_index(
        &self,
        tx_id: TxID,
        id: RowID,
        maybe_index_id: Option<MVTableId>,
    ) -> Result<bool> {
        tracing::trace!("delete(tx_id={}, id={:?})", tx_id, id);
        match maybe_index_id {
            Some(index_id) => {
                let rows = self.get_or_create_index_rows(index_id)?;
                let rows = rows.value();
                let RowKey::Record(sortable_key) = id.row_id.clone() else {
                    panic!("Index deletes must have a record row_id");
                };
                if let Some(ref row_versions_entry) = rows.get(&sortable_key) {
                    // Get the Arc key from the map entry for savepoint tracking
                    let arc_key = row_versions_entry.key().clone();
                    let row_versions = row_versions_entry.value().clone();
                    for rv in row_versions.write().iter_mut().rev() {
                        let tx = self
                            .txs
                            .get(&tx_id)
                            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
                        let tx = tx.value();
                        turso_assert_eq!(tx.state, TransactionState::Active);
                        // A transaction cannot delete a version that it cannot see,
                        // nor can it conflict with it.
                        if !rv.is_visible_to(tx, &self.txs, &self.finalized_tx_states) {
                            continue;
                        }
                        if is_write_write_conflict(&self.txs, &self.finalized_tx_states, tx, rv) {
                            turso_assert_reachable!("write-write conflict on delete");
                            return Err(LimboError::WriteWriteConflict);
                        }

                        let version_id = rv.id;
                        rv.set_end(Some(TxTimestampOrID::TxID(tx.tx_id)));
                        let tx = self
                            .txs
                            .get(&tx_id)
                            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
                        let tx = tx.value();
                        tx.insert_to_write_set(id, row_versions.clone());
                        tx.record_deleted_index_version((index_id, arc_key), version_id);
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            None => {
                let row_versions_opt = self.rows.get(&id);
                if let Some(ref row_versions_entry) = row_versions_opt {
                    let row_versions = row_versions_entry.value().clone();
                    let mut locked_row_versions = row_versions.write();
                    for rv in locked_row_versions.iter_mut().rev() {
                        let tx = self
                            .txs
                            .get(&tx_id)
                            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
                        let tx = tx.value();
                        turso_assert_eq!(tx.state, TransactionState::Active);
                        // A transaction cannot delete a version that it cannot see,
                        // nor can it conflict with it.
                        if !rv.is_visible_to(tx, &self.txs, &self.finalized_tx_states) {
                            continue;
                        }
                        if is_write_write_conflict(&self.txs, &self.finalized_tx_states, tx, rv) {
                            turso_assert_reachable!("write-write conflict on delete");
                            return Err(LimboError::WriteWriteConflict);
                        }

                        let version_id = rv.id;
                        rv.set_end(Some(TxTimestampOrID::TxID(tx.tx_id)));
                        drop(locked_row_versions);
                        drop(row_versions_opt);
                        let tx = self
                            .txs
                            .get(&tx_id)
                            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
                        let tx = tx.value();
                        tx.insert_to_write_set(id.clone(), row_versions.clone());
                        tx.record_deleted_table_version(id.clone(), version_id);
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    /// Retrieves a row from the table with the given `id`.
    ///
    /// This operation is performed within the scope of the transaction identified
    /// by `tx_id`.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The ID of the transaction to perform the read operation in.
    /// * `id` - The ID of the row to retrieve.
    ///
    /// # Returns
    ///
    /// Returns `Some(row)` with the row data if the row with the given `id` exists,
    /// and `None` otherwise.
    pub fn read(&self, tx_id: TxID, id: &RowID) -> Result<Option<Row>> {
        self.read_from_table_or_index(tx_id, id, None)
    }

    /// Same as read() but can read from a table or an index, indicated by the `maybe_index_id` argument.
    pub fn read_from_table_or_index(
        &self,
        tx_id: TxID,
        id: &RowID,
        maybe_index_id: Option<MVTableId>,
    ) -> Result<Option<Row>> {
        tracing::trace!("read(tx_id={}, id={:?})", tx_id, id);

        let tx = self
            .txs
            .get(&tx_id)
            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
        let tx = tx.value();
        turso_assert_eq!(tx.state, TransactionState::Active);
        match maybe_index_id {
            Some(index_id) => {
                let rows = self.get_or_create_index_rows(index_id)?;
                let rows = rows.value();
                let RowKey::Record(sortable_key) = &id.row_id else {
                    panic!("Index reads must have a record row_id");
                };
                let row_versions_opt = rows.get(sortable_key);
                if let Some(ref row_versions) = row_versions_opt {
                    let row_versions = row_versions.value().read();
                    if let Some(rv) = row_versions
                        .iter()
                        .rev()
                        .find(|rv| rv.is_visible_to(tx, &self.txs, &self.finalized_tx_states))
                    {
                        return Ok(Some(rv.row.clone()));
                    }
                }
                Ok(None)
            }
            None => {
                if let Some(row_versions) = self.rows.get(id) {
                    let row_versions = row_versions.value().read();
                    if let Some(rv) = row_versions
                        .iter()
                        .rev()
                        .find(|rv| rv.is_visible_to(tx, &self.txs, &self.finalized_tx_states))
                    {
                        return Ok(Some(rv.row.clone()));
                    }
                }
                Ok(None)
            }
        }
    }

    /// Like the table branch of [`read_from_table_or_index`], but reads from an
    /// already-resolved version chain instead of looking the row up in
    /// `self.rows`. Used on the scan path where the cursor's range iterator
    /// already located the `Arc`, so the second skiplist traversal is avoided.
    pub(crate) fn read_visible_from_versions(
        &self,
        tx_id: TxID,
        versions: &RowVersions<A>,
    ) -> Result<Option<Row>> {
        let tx = self
            .txs
            .get(&tx_id)
            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
        let tx = tx.value();
        turso_assert_eq!(tx.state, TransactionState::Active);
        let versions = versions.read();
        if let Some(rv) = versions
            .iter()
            .rev()
            .find(|rv| rv.is_visible_to(tx, &self.txs, &self.finalized_tx_states))
        {
            return Ok(Some(rv.row.clone()));
        }
        Ok(None)
    }

    /// Like [`read_visible_from_versions`] but serializes the visible row's
    /// payload directly into `record` instead of cloning a `Row`. Mirrors the
    /// btree cursor, which serializes a cell straight into its reusable record.
    /// Returns true if a visible version was found. The version-chain read lock
    /// is held only for the serialization copy.
    pub(crate) fn read_visible_into_record(
        &self,
        tx_id: TxID,
        versions: &RowVersions<A>,
        record: &mut ImmutableRecord,
    ) -> Result<bool> {
        let tx = self
            .txs
            .get(&tx_id)
            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
        let tx = tx.value();
        turso_assert_eq!(tx.state, TransactionState::Active);
        let versions = versions.read();
        if let Some(rv) = versions
            .iter()
            .rev()
            .find(|rv| rv.is_visible_to(tx, &self.txs, &self.finalized_tx_states))
        {
            record.invalidate();
            record.start_serialization(rv.row.payload())?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Gets all row ids in the database.
    pub fn scan_row_ids(&self) -> Result<Vec<RowID>> {
        tracing::trace!("scan_row_ids");
        let keys = self.rows.iter().map(|entry| entry.key().clone());
        Ok(keys.collect())
    }

    pub fn get_row_id_range(
        &self,
        table_id: MVTableId,
        start: i64,
        bucket: &mut Vec<RowID>,
        max_items: u64,
    ) -> Result<()> {
        tracing::trace!(
            "get_row_id_in_range(table_id={}, range_start={})",
            table_id,
            start,
        );
        let start_id = RowID {
            table_id,
            row_id: RowKey::Int(start),
        };

        let end_id = RowID {
            table_id,
            row_id: RowKey::Int(i64::MAX),
        };

        self.rows
            .range(start_id..end_id)
            .take(max_items as usize)
            .for_each(|entry| bucket.push(entry.key().clone()));

        Ok(())
    }

    pub(crate) fn advance_cursor_and_get_row_id_for_table(
        &self,
        table_id: MVTableId,
        mv_store_iterator: &mut Option<MvccIterator<'static, RowID, A>>,
        tx_id: TxID,
    ) -> Option<(RowID, RowVersions<A>)> {
        let mv_store_iterator = mv_store_iterator.as_mut().expect(
            "mv_store_iterator must be initialized when calling get_row_id_for_table_in_direction",
        );

        let tx = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx.value();
        loop {
            // We are moving forward, so if a row was deleted we just need to skip it. Therefore, we need
            // to loop either until we find a row that is not deleted or until we reach the end of the table.
            let next_row = mv_store_iterator.next();
            let row = next_row?;
            if row.key().table_id != table_id {
                // In case of table rows, we store the rows of all tables in a single map,
                // so we must stop iteration if we reach a row that is on a different table.
                // In the case of indexes we have a separate map per table so this is not
                // relevant.
                return None;
            }

            // We found a row, let's check if it's visible to the transaction.
            if let Some(visible_row) = self.find_last_visible_version(tx, &row) {
                return Some(visible_row);
            }
            // If this row is not visible, continue to the next row
        }
    }

    pub(crate) fn advance_cursor_and_get_row_id_for_index(
        &self,
        mv_store_iterator: &mut Option<MvccIterator<'static, Arc<SortableIndexKey>, A>>,
        tx_id: TxID,
    ) -> Option<RowID> {
        let mv_store_iterator = mv_store_iterator.as_mut().expect(
            "mv_store_iterator must be initialized when calling get_row_id_for_index_in_direction",
        );

        let tx = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx.value();

        self.find_next_visible_index_row(tx, mv_store_iterator)
    }

    /// Whether a SkipMap chain must still participate in MVCC merge/shadow for `tx`.
    ///
    /// False only for a sole materialized current inside the published durable
    /// boundary (Rule 3 shape). Longer chains, pending TxIDs, and unmaterialized
    /// versions stay on the SkipMap path.
    fn chain_is_write_buffer_for(
        &self,
        tx: &Transaction<A>,
        versions: &[RowVersion],
        ckpt_max: u64,
        reader_mark: WalPos,
    ) -> bool {
        if versions.is_empty() {
            return false;
        }
        if versions.len() != 1 {
            return true;
        }
        let rv = &versions[0];
        let Some(TxTimestampOrID::Timestamp(begin_ts)) = rv.begin() else {
            return true;
        };
        if rv.end().is_some() || !rv.is_visible_to(tx, &self.txs, &self.finalized_tx_states) {
            return true;
        }
        // Passive may stamp materialized_at during write-out before publish.
        if begin_ts > ckpt_max {
            return true;
        }
        let mat = rv.materialized_at();
        if mat == WalPos::ORIGIN || reader_mark < mat {
            return true;
        }
        false
    }

    /// True when `tx` should ignore this SkipMap chain and read the key from B-tree.
    ///
    /// Passive keeps SkipMap cover for all chains (table/index views can disagree
    /// under concurrent Passive). Truncate may fall through for sole materialized
    /// currents when no checkpoint is in progress.
    fn btree_covers_chain_for_tx(
        &self,
        tx: &Transaction<A>,
        table_id: MVTableId,
        versions: &[RowVersion],
    ) -> bool {
        if self.experimental_mvcc_passive_checkpoint {
            return false;
        }
        if self.checkpoint_in_progress.load(Ordering::Acquire) {
            return false;
        }
        if !self.is_btree_readable_at(&table_id, tx.begin_ts, tx.read_mark) {
            return false;
        }
        let ckpt_max = self.durable_txid_max.load(Ordering::SeqCst);
        !self.chain_is_write_buffer_for(tx, versions, ckpt_max, tx.read_mark)
    }

    /// Whether an already-resolved index version chain shadows (invalidates) the
    /// corresponding B-tree row for `tx_id`.
    ///
    /// This is exactly the predicate used by the `RowKey::Record` branch of
    /// [`Self::query_btree_version_is_valid`], but it takes the version chain
    /// directly instead of looking it up by key. A forward index scan keeps a
    /// skiplist finger co-positioned with the B-tree and calls this on the
    /// chain the finger already points at, replacing one `index_rows.get()`
    /// (O(log N)) per scanned row with an amortized-O(1) merge step.
    pub(crate) fn index_chain_invalidates_btree(
        &self,
        versions: &RwLock<RowVersionChain<A>>,
        tx_id: TxID,
    ) -> bool {
        let tx = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx.value();
        let versions = versions.read();
        if versions.is_empty() {
            return false;
        }
        let table_id = versions[0].row.id.table_id;
        if self.btree_covers_chain_for_tx(tx, table_id, &versions) {
            return false;
        }
        versions.iter().rev().any(|version| {
            version.is_btree_invalidating_version(tx, &self.txs, &self.finalized_tx_states)
        })
    }

    /// Check if the B-tree version of a row should be shown to the given transaction.
    ///
    /// Returns true if the B-tree version is valid (should be shown).
    /// Returns false if the B-tree version is shadowed or deleted by MVCC.
    pub fn query_btree_version_is_valid(
        &self,
        table_id: MVTableId,
        row_id: &RowKey,
        tx_id: TxID,
    ) -> bool {
        let tx = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx.value();

        match row_id {
            RowKey::Int(_) => {
                let row_id_full = RowID {
                    table_id,
                    row_id: row_id.clone(),
                };
                let Some(versions) = self.rows.get(&row_id_full) else {
                    // No MVCC version -> B-tree is valid
                    return true;
                };
                let versions = versions.value().read();
                if self.btree_covers_chain_for_tx(tx, table_id, &versions) {
                    return true;
                }

                // Check if any version invalidates the B-tree row
                let btree_is_invalid = versions.iter().rev().any(|version| {
                    version.is_btree_invalidating_version(tx, &self.txs, &self.finalized_tx_states)
                });

                !btree_is_invalid
            }
            RowKey::Record(record) => {
                // Dont allocate new SkipList here to avoid introducing concerns around error handling
                let Some(index_rows) = self.index_rows.get(&table_id) else {
                    return true;
                };
                let index_rows = index_rows.value();
                let Some(versions) = index_rows.get(record.as_ref()) else {
                    // No MVCC version -> B-tree is valid
                    return true;
                };
                let versions = versions.value().read();
                if self.btree_covers_chain_for_tx(tx, table_id, &versions) {
                    return true;
                }

                // Check if any version invalidates the B-tree row
                let btree_is_invalid = versions.iter().rev().any(|version| {
                    version.is_btree_invalidating_version(tx, &self.txs, &self.finalized_tx_states)
                });

                !btree_is_invalid
            }
        }
    }

    fn find_last_visible_version(
        &self,
        tx: &Transaction<A>,
        row: &TableRowEntry<'_, A>,
    ) -> Option<(RowID, RowVersions<A>)> {
        let versions_arc = row.value();
        {
            let versions = versions_arc.read();
            let has_visible = versions
                .iter()
                .rev()
                .any(|version| version.is_visible_to(tx, &self.txs, &self.finalized_tx_states));
            if !has_visible {
                return None;
            }
            if self.btree_covers_chain_for_tx(tx, row.key().table_id, &versions) {
                return None;
            }
        }
        Some((row.key().clone(), versions_arc.clone()))
    }

    fn find_last_visible_index_version(
        &self,
        tx: &Transaction<A>,
        row: IndexRowEntry<'_, A>,
    ) -> Option<RowID> {
        let versions = row.value().read();
        let visible = versions
            .iter()
            .rev()
            .find(|version| version.is_visible_to(tx, &self.txs, &self.finalized_tx_states))?;
        let table_id = visible.row.id.table_id;
        if self.btree_covers_chain_for_tx(tx, table_id, &versions) {
            return None;
        }
        Some(visible.row.id.clone())
    }

    fn find_next_visible_index_row<'a, I>(&self, tx: &Transaction<A>, mut rows: I) -> Option<RowID>
    where
        I: Iterator<Item = IndexRowEntry<'a, A>>,
    {
        loop {
            let row = rows.next()?;
            if let Some(visible_row) = self.find_last_visible_index_version(tx, row) {
                return Some(visible_row);
            }
        }
    }

    fn find_next_visible_table_row<'a, I>(
        &self,
        tx: &Transaction<A>,
        mut rows: I,
        table_id: MVTableId,
    ) -> Option<(RowID, RowVersions<A>)>
    where
        I: Iterator<Item = TableRowEntry<'a, A>>,
    {
        loop {
            let row = rows.next()?;
            if row.key().table_id != table_id {
                return None;
            }
            if let Some(visible_row) = self.find_last_visible_version(tx, &row) {
                return Some(visible_row);
            }
        }
    }

    pub fn seek_rowid(
        &self,
        start: RowID,
        inclusive: bool,
        eq_only: bool,
        direction: IterationDirection,
        tx_id: TxID,
        table_iterator: &mut Option<MvccIterator<'static, RowID, A>>,
    ) -> Option<RowID> {
        let table_id = start.table_id;
        let iter_box = {
            let range = if eq_only {
                // An eq-only rowid seek (point lookup, NotExists / rowid-uniqueness
                // probe) only cares about the single key `start`. Bound BOTH ends of
                // the range to that key so the skiplist walk stops immediately instead
                // of scanning forward over every invisible neighbor until it happens to
                // find the next visible row. Without this bound a single probe costs
                // O(pending invisible versions), which compounds into quadratic work
                // when each row of a concurrent batch insert pays it. This mirrors the
                // eq-only bound in `seek_index`. The cursor's final position is still
                // decided by `PickWinner` from the merged MVCC/B-tree peeks, so bounding
                // the range here does not change where the cursor lands.
                (Bound::Included(start.clone()), Bound::Included(start))
            } else {
                let start = if inclusive {
                    Bound::Included(start)
                } else {
                    Bound::Excluded(start)
                };
                create_seek_range(start, direction)
            };
            match direction {
                IterationDirection::Forwards => {
                    Box::new(self.rows.range(range)) as TableRowIterator<'_, A>
                }
                IterationDirection::Backwards => {
                    Box::new(self.rows.range(range).rev()) as TableRowIterator<'_, A>
                }
            }
        };
        *table_iterator = Some(static_iterator_hack!(iter_box, RowID, A));

        let mv_store_iterator = table_iterator
            .as_mut()
            .expect("table_iterator was assigned above if it was None");

        let tx = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx.value();

        self.find_next_visible_table_row(tx, mv_store_iterator, table_id)
            .map(|(row_id, _versions)| row_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn seek_index(
        &self,
        index_id: MVTableId,
        start: SortableIndexKey,
        inclusive: bool,
        eq_only: bool,
        direction: IterationDirection,
        tx_id: TxID,
        index_iterator: &mut Option<MvccIterator<'static, Arc<SortableIndexKey>, A>>,
    ) -> Result<Option<RowID>> {
        let index_rows = self.get_or_create_index_rows(index_id)?;
        let index_rows = index_rows.value();
        let range = if eq_only {
            // An eq-only seek (point lookup, NoConflict, unique-constraint probe,
            // index delete) only cares about entries whose key matches `start`.
            // Bound BOTH ends of the range to the probe key so the skiplist walk
            // stops at the matching cluster instead of scanning forward over every
            // invisible neighbor until it happens to find the next visible row.
            //
            // `SortableIndexKey` ordering compares only the probe's columns (see
            // `SortableIndexKey::compare`, which clamps to `min(num_cols)`), so
            // `start..=start` captures all entries sharing the probed prefix
            // regardless of their trailing rowid — exactly the set an eq-only seek
            // may match. Without this bound a single seek costs O(pending invisible
            // versions), which compounds into quadratic work when each row of a
            // concurrent batch insert pays it. The cursor's position after the seek
            // is still set by `PickWinner` from the merged MVCC/B-tree peeks, so
            // returning early here does not change where the cursor lands.
            (Bound::Included(start.clone()), Bound::Included(start))
        } else {
            let start = if inclusive {
                Bound::Included(start)
            } else {
                Bound::Excluded(start)
            };
            create_seek_range(start, direction)
        };
        let iter_box = match direction {
            IterationDirection::Forwards => {
                Box::new(index_rows.range(range)) as IndexRowIterator<'_, A>
            }
            IterationDirection::Backwards => {
                Box::new(index_rows.range(range).rev()) as IndexRowIterator<'_, A>
            }
        };
        *index_iterator = Some(static_iterator_hack!(iter_box, Arc<SortableIndexKey>, A));
        let mv_store_iterator = index_iterator
            .as_mut()
            .expect("index_iterator was assigned above if it was None");

        let tx = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx.value();
        Ok(self.find_next_visible_index_row(tx, mv_store_iterator))
    }

    /// Begins an exclusive write transaction that prevents concurrent writes.
    ///
    /// This is used for IMMEDIATE and EXCLUSIVE transaction types where we need
    /// to ensure exclusive write access as per SQLite semantics.
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn begin_exclusive_tx(
        &self,
        pager: Arc<Pager>,
        maybe_existing_tx_id: Option<TxID>,
        connection: &Connection,
        expected_schema_generation: Option<u64>,
    ) -> Result<TxID> {
        self.begin_exclusive_tx_with_checkpoint_read_guard(
            pager,
            maybe_existing_tx_id,
            connection,
            expected_schema_generation,
            CheckpointReadLockState::NotHeld,
        )
    }

    /// Try to acquire the blocking checkpoint read guard for a fresh MVCC
    /// begin.
    ///
    /// Use this only for a fresh MVCC begin that must preserve this order:
    /// checkpoint gate, pager read mark, MVCC transaction. If this returns
    /// `CheckpointReadLockState::Held` and the caller fails before handing the
    /// guard to a `begin_*_with_checkpoint_read_guard` helper, call
    /// `release_unadopted_checkpoint_read_guard`.
    ///
    /// Do not use this for read-to-write upgrades. An existing MVCC
    /// transaction already owns its checkpoint guard.
    pub(crate) fn try_acquire_checkpoint_read_guard_for_fresh_mvcc_begin(
        &self,
    ) -> Result<CheckpointReadLockState> {
        if self.experimental_mvcc_passive_checkpoint {
            return Ok(CheckpointReadLockState::NotHeld);
        }
        if !self.blocking_checkpoint_lock.read() {
            return Err(LimboError::Busy);
        }
        Ok(CheckpointReadLockState::Held)
    }

    /// Release a checkpoint gate acquired by
    /// `try_acquire_checkpoint_read_guard_for_fresh_mvcc_begin` before MVCC
    /// has adopted it.
    ///
    /// Do not call this after a `begin_*_with_checkpoint_read_guard` helper
    /// succeeds. At that point the transaction owns the guard and the normal
    /// transaction finish path releases it.
    pub(crate) fn release_unadopted_checkpoint_read_guard(&self, guard: CheckpointReadLockState) {
        if guard.is_held() {
            self.blocking_checkpoint_lock.unlock();
        }
    }

    /// Begin or upgrade a write MVCC transaction, optionally adopting a
    /// checkpoint gate that the VDBE already acquired.
    ///
    /// Ordinary MvStore callers should use `begin_exclusive_tx`. Use this
    /// helper only when the caller is preserving the fresh-begin order itself:
    /// checkpoint gate, pager read mark, MVCC transaction. Pass
    /// `CheckpointReadLockState::NotHeld` for upgrades because the existing
    /// transaction already owns the gate.
    pub(crate) fn begin_exclusive_tx_with_checkpoint_read_guard(
        &self,
        pager: Arc<Pager>,
        maybe_existing_tx_id: Option<TxID>,
        connection: &Connection,
        expected_schema_generation: Option<u64>,
        checkpoint_read_guard: CheckpointReadLockState,
    ) -> Result<TxID> {
        #[cfg(not(any(test, injected_yields)))]
        let _ = connection;
        turso_assert!(
            maybe_existing_tx_id.is_none() || !checkpoint_read_guard.is_held(),
            "checkpoint read guard is only passed for fresh MVCC begins"
        );
        turso_assert!(
            !checkpoint_read_guard.is_held() || !self.experimental_mvcc_passive_checkpoint,
            "passive MVCC begin must not hold the blocking checkpoint read guard"
        );
        turso_assert!(
            !checkpoint_read_guard.is_held() || pager.holds_read_lock(),
            "checkpoint read guard must be paired with a pager read mark before MVCC begin"
        );
        // Existing transactions already hold one blocking-checkpoint read guard
        // from begin_tx() (truncate path only). When upgrading read->write, do not acquire another one.
        let passive = self.experimental_mvcc_passive_checkpoint;
        let acquires_checkpoint_guard = maybe_existing_tx_id.is_none() && !passive;
        // Fresh write begins gate on the connection's prepared schema generation (same as
        // begin_tx); upgrades keep the original snapshot, so the caller passes None. See begin_tx
        // for why this closes the passive publish/begin race without a Busy.
        let expected_schema_generation = if maybe_existing_tx_id.is_none() {
            expected_schema_generation
        } else {
            None
        };
        if acquires_checkpoint_guard
            && !checkpoint_read_guard.is_held()
            && !self.blocking_checkpoint_lock.read()
        {
            // If there is a stop-the-world checkpoint in progress, we cannot begin any transaction at all.
            return Err(LimboError::Busy);
        }
        let unlock_checkpoint_guard = || {
            if acquires_checkpoint_guard {
                self.blocking_checkpoint_lock.unlock();
            }
        };
        let tx_id = maybe_existing_tx_id.unwrap_or_else(|| self.get_tx_id());
        let begin_ts = if let Some(tx_id) = maybe_existing_tx_id {
            // Upgrade path: the transaction is already published in `txs`
            // (from begin_tx), so it is already visible to compute_lwm().
            self.txs
                .get(&tx_id)
                .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?
                .value()
                .begin_ts
        } else {
            // Fresh path: publish the transaction into `txs` atomically with
            // begin_ts allocation, under the clock lock — same begin-publish
            // window fix as begin_tx(). Without this, inline GC on a concurrent
            // committer's commit path could compute an LWM above our begin_ts
            // and reclaim a version this snapshot still needs. The header read
            // (possibly blocking I/O) happens before the clock lock is taken;
            // the error paths below remove this tx if begin fails. The real
            // header is set here, so the tail only flips `pager_commit_lock_held`.
            // Ensure page 1 / global_header is initialized (pager I/O only on first init). The
            // header + schema_generation are re-read inside the clock callback below, not captured
            // here, so a passive publish (which swaps global_header + bumps schema_generation under
            // this same clock) cannot interleave between the capture and `txs.insert`.
            self.get_new_transaction_database_header(&pager);
            pager.mvcc_refresh_if_db_changed();
            let read_mark = WalPos::from_pair(pager.wal_pos());
            let mut schema_stale = false;
            let begin_ts = self.clock.get_timestamp(|ts| {
                let schema_generation = self.schema_generation();
                if expected_schema_generation.is_some_and(|exp| exp != schema_generation) {
                    schema_stale = true;
                    return;
                }
                let header = self
                    .global_header
                    .read()
                    .expect("global_header initialized above");
                self.txs.insert(
                    tx_id,
                    Transaction::new(tx_id, ts, header, read_mark, schema_generation),
                );
            });
            if schema_stale {
                unlock_checkpoint_guard();
                return Err(LimboError::SchemaUpdated);
            }
            if acquires_checkpoint_guard {
                if let Some(entry) = self.txs.get(&tx_id) {
                    entry
                        .value()
                        .holds_blocking_checkpoint_read
                        .store(true, Ordering::Release);
                }
            }
            begin_ts
        };
        #[cfg(any(test, injected_yields))]
        let exclusive_yield_context = YieldContext::new(
            connection.yield_injector(),
            None,
            connection.next_yield_instance_id(),
            tx_id,
        );
        #[cfg(any(test, injected_yields))]
        let exclusive_yield_context = Some(&exclusive_yield_context);
        #[cfg(not(any(test, injected_yields)))]
        let exclusive_yield_context: Option<&YieldContext> = None;

        let already_exclusive = self.is_exclusive_tx(&tx_id);
        if !already_exclusive {
            self.acquire_exclusive_tx(&tx_id, exclusive_yield_context)
                .inspect_err(|_| {
                    // Fresh txns were already published into `txs` above; undo
                    // that so a failed begin doesn't leave a phantom Active txn
                    // pinning the LWM forever.
                    if maybe_existing_tx_id.is_none() {
                        self.txs.remove(&tx_id);
                    }
                    unlock_checkpoint_guard();
                })?;
        }

        // Hoist: validate the existing tx still exists and snapshot the
        // `pager_commit_lock_held` flag BEFORE acquiring `pager_commit_lock`,
        // so a vanished tx cannot strand the lock (#6905).
        let already_holds_commit_lock = match maybe_existing_tx_id {
            Some(existing_tx_id) => {
                let tx = self.txs.get(&existing_tx_id).ok_or_else(|| {
                    if !already_exclusive {
                        self.release_exclusive_tx(&tx_id);
                    }
                    unlock_checkpoint_guard();
                    LimboError::NoSuchTransactionID(existing_tx_id.to_string())
                })?;
                tx.value().pager_commit_lock_held.load(Ordering::Acquire)
            }
            None => false,
        };

        if !already_holds_commit_lock {
            let locked = self.commit_coordinator.pager_commit_lock.write();
            if !locked {
                tracing::debug!(
                    "begin_exclusive_tx: tx_id={} failed with Busy on pager_commit_lock",
                    tx_id
                );
                if maybe_existing_tx_id.is_none() {
                    self.txs.remove(&tx_id);
                }
                if !already_exclusive {
                    self.release_exclusive_tx(&tx_id);
                }
                unlock_checkpoint_guard();
                return Err(LimboError::Busy);
            }
        }

        let handle_err = || {
            if !already_holds_commit_lock {
                self.commit_coordinator.pager_commit_lock.unlock();
            }
            if !already_exclusive {
                self.release_exclusive_tx(&tx_id);
            }
            unlock_checkpoint_guard();
        };

        if let Some(existing_tx_id) = maybe_existing_tx_id {
            // Upgrade path: read the (possibly blocking) header now that all
            // locks are held, then apply it to the already-published txn.
            let header = self.get_new_transaction_database_header(&pager);
            // Re-fetch the Ref now that all blocking I/O is done. If the tx
            // vanished between the earlier validation and now (extraordinarily
            // narrow window — only checkpoint can remove a tx and it cannot
            // race a commit-lock holder), release what we acquired and bail.
            let tx = self.txs.get(&existing_tx_id).ok_or_else(|| {
                handle_err();
                LimboError::NoSuchTransactionID(existing_tx_id.to_string())
            })?;
            tx.value()
                .pager_commit_lock_held
                .store(true, Ordering::Release);
            *tx.value().header.write() = header;
            tracing::trace!(
                "begin_exclusive_tx(tx_id={}, begin_ts={}) - upgraded existing transaction",
                tx_id,
                begin_ts
            );
            tracing::debug!("begin_exclusive_tx: tx_id={} succeeded", tx_id);
            return Ok(tx_id);
        }

        // Fresh path: the transaction (with its header) was already published
        // into `txs` under the clock lock during begin_ts allocation. Now that
        // we hold the commit lock, just record that.
        let tx = self
            .txs
            .get(&tx_id)
            .expect("fresh exclusive tx was published during begin_ts allocation");
        tx.value()
            .pager_commit_lock_held
            .store(true, Ordering::Release);
        tracing::trace!(
            "begin_exclusive_tx(tx_id={}, begin_ts={}) - exclusive write logical log transaction",
            tx_id,
            begin_ts
        );
        tracing::debug!("begin_exclusive_tx: tx_id={} succeeded", tx_id);
        Ok(tx_id)
    }

    /// Begins a new transaction in the database.
    ///
    /// This function starts a new transaction in the database and returns a `TxID` value
    /// that you can use to perform operations within the transaction. All changes made within the
    /// transaction are isolated from other transactions until you commit the transaction.
    pub fn begin_tx(&self, pager: Arc<Pager>) -> Result<TxID> {
        self.begin_tx_with_schema_generation(pager, None)
    }

    /// `begin_tx` with the connection's validated `schema_generation` gate (see
    /// [`Connection::mvcc_begin_schema_generation`]). Used by the statement begin path so a passive
    /// checkpoint that republishes physical roots into the begin window forces a reprepare instead
    /// of a transaction beginning against stale roots.
    pub fn begin_tx_with_schema_generation(
        &self,
        pager: Arc<Pager>,
        expected_schema_generation: Option<u64>,
    ) -> Result<TxID> {
        self.begin_tx_with_schema_generation_and_checkpoint_read_guard(
            pager,
            expected_schema_generation,
            CheckpointReadLockState::NotHeld,
        )
    }

    /// Begin a read MVCC transaction, optionally adopting a checkpoint gate
    /// that the VDBE already acquired.
    ///
    /// Ordinary MvStore callers should use `begin_tx_with_schema_generation`.
    /// Use this helper only after
    /// `try_acquire_checkpoint_read_guard_for_fresh_mvcc_begin` succeeds and
    /// the caller has successfully pinned the pager read mark. On success, the
    /// MVCC transaction owns the checkpoint guard. On error, this helper
    /// releases the guard, but the caller still owns any pager read mark it
    /// opened.
    pub(crate) fn begin_tx_with_schema_generation_and_checkpoint_read_guard(
        &self,
        pager: Arc<Pager>,
        expected_schema_generation: Option<u64>,
        checkpoint_read_guard: CheckpointReadLockState,
    ) -> Result<TxID> {
        turso_assert!(
            !checkpoint_read_guard.is_held() || !self.experimental_mvcc_passive_checkpoint,
            "passive MVCC begin must not hold the blocking checkpoint read guard"
        );
        turso_assert!(
            !checkpoint_read_guard.is_held() || pager.holds_read_lock(),
            "checkpoint read guard must be paired with a pager read mark before MVCC begin"
        );
        let passive = self.experimental_mvcc_passive_checkpoint;
        if !passive && !checkpoint_read_guard.is_held() && !self.blocking_checkpoint_lock.read() {
            // Stop-the-world truncate checkpoint in progress.
            return Err(LimboError::Busy);
        }
        let tx_id = self.get_tx_id();

        // Ensure page 1 / global_header is initialized. The (possibly blocking) init I/O
        // happens here, BEFORE the clock lock; the header value itself is re-read inside the
        // clock callback below so it pairs atomically with begin_ts + schema_generation.
        self.get_new_transaction_database_header(&pager);

        // Allocate begin_ts and publish the transaction into `txs` atomically
        // under the clock lock. This closes the "begin-publish window": between
        // allocating a snapshot timestamp and inserting into `txs`, the txn is
        // invisible to `compute_lwm`. Inline GC runs on the commit path holding
        // only `blocking_checkpoint_lock.read()` (truncate path), so a writer that
        // commits in that window could compute an LWM above our begin_ts and
        // reclaim a version this snapshot still needs — a snapshot-isolation
        // violation. Publishing under the clock lock orders us strictly before
        // or after any commit timestamp (commits also take the clock lock), so
        // any GC that runs after a later commit already sees our begin_ts.
        //
        // Truncate mode also holds `blocking_checkpoint_lock.read()` for the txn lifetime so
        // publish cannot interleave with read_mark capture. Passive mode relies on the clock:
        // its publish window runs under `get_timestamp` too, so begin and publish serialize on
        // the clock and never need to block each other — a begin that orders after a publish
        // simply observes the bumped `schema_generation` and reprepares (below).
        pager.mvcc_refresh_if_db_changed();
        let read_mark = WalPos::from_pair(pager.wal_pos());
        let mut schema_stale = false;
        let begin_ts = self.clock.get_timestamp(|ts| {
            // Capture header (cookie) + schema_generation INSIDE the clock so they are
            // consistent with the root map at insert time: a passive publish runs under this
            // same clock, so it cannot interleave between this capture and the insert.
            let schema_generation = self.schema_generation();
            // A publish that ordered into our begin window bumped the generation past the value
            // the caller validated its prepared schema against: reprepare instead of beginning
            // with stale physical roots.
            if expected_schema_generation.is_some_and(|exp| exp != schema_generation) {
                schema_stale = true;
                return;
            }
            let header = self
                .global_header
                .read()
                .expect("global_header initialized above");
            self.txs.insert(
                tx_id,
                Transaction::new(tx_id, ts, header, read_mark, schema_generation),
            );
        });
        if schema_stale {
            if !passive {
                self.blocking_checkpoint_lock.unlock();
            }
            return Err(LimboError::SchemaUpdated);
        }
        if !passive {
            if let Some(entry) = self.txs.get(&tx_id) {
                entry
                    .value()
                    .holds_blocking_checkpoint_read
                    .store(true, Ordering::Release);
            }
        }
        tracing::trace!("begin_tx(tx_id={}, begin_ts={})", tx_id, begin_ts);

        Ok(tx_id)
    }

    #[turso_macros::allocation_site(crate::alloc::MvStoreAllocationSite::TxInsert)]
    fn insert_tx_entry(&self, tx_id: TxID, tx: Transaction<A>) -> Result<(), TryReserveError> {
        self.txs.try_insert(tx_id, tx)?;
        Ok(())
    }

    pub fn remove_tx(&self, tx_id: TxID) -> Result<(), TryReserveError> {
        self.release_index_method_write_leases(tx_id);
        self.remove_sequence_allocations(tx_id);
        if let Some(entry) = self.txs.get(&tx_id) {
            let tx = entry.value();
            let held_checkpoint_read = tx.holds_blocking_checkpoint_read.load(Ordering::Acquire);
            if let TransactionState::Committed(commit_ts) = tx.state.load() {
                // Read-only transactions cannot leave row versions with stale TxID
                // references, so they do not need finalized-state caching.
                if !tx.write_set.lock().is_empty() {
                    crate::without_allocation_faults!(self
                        .insert_finalized_tx_state(tx_id, commit_ts)
                        .expect(ALLOC_ERR_MSG));
                }
            }
            let dep_set = std::mem::take(&mut *tx.commit_dep_set.lock());
            // Invariant: commit_dep_set must be drained before removing the transaction.
            // CommitEnd and rollback_tx both drain the commit_dep_set to notify dependencies.
            // If we remove a transaction with non-empty commit_dep_set, those dependencies will wait
            // forever (deadlock).
            turso_assert!(
                dep_set.is_empty(),
                "remove_tx({tx_id}): commit_dep_set is not empty"
            );
            // Invariant: a committed writer must be findable in
            // `finalized_tx_states` before it leaves `txs`. Other transactions'
            // commit validation (`check_version_conflicts`) and visibility
            // checks (`is_end_visible`) look a tombstone's TxID marker up in
            // `txs` first and `finalized_tx_states` second; a writer absent
            // from both is treated as "no conflict" / "visible", so reordering
            // the insert above after this remove would let a conflicting
            // commit through instead of returning WriteWriteConflict.
            turso_assert!(
                !matches!(tx.state.load(), TransactionState::Committed(_))
                    || tx.write_set.lock().is_empty()
                    || lookup_finalized_tx_state(&self.finalized_tx_states, tx_id).is_some(),
                "committed writer must be visible in finalized_tx_states before leaving txs",
                { "tx_id": tx_id }
            );
            self.txs.remove(&tx_id);
            if held_checkpoint_read {
                self.blocking_checkpoint_lock.unlock();
            }
            return Ok(());
        }
        self.txs.remove(&tx_id);
        Ok(())
    }

    /// Acquire a transaction-scoped custom-index writer lease.
    ///
    /// Acquisition is reentrant for the owning transaction. Contention with a
    /// live holder is `Busy` — the caller can retry once the holder finishes,
    /// exactly what `busy_timeout` handles. A transaction whose read snapshot
    /// predates the index's last publication gets `WriteWriteConflict`
    /// instead: it would rebuild the index from a superseded base, and no
    /// amount of retrying inside the same transaction can fix that.
    pub(crate) fn acquire_index_method_write_lease(
        &self,
        tx_id: TxID,
        index_id: MVTableId,
    ) -> Result<()> {
        if !self.is_tx_rollbackable(tx_id) {
            return Err(LimboError::NoSuchTransactionID(tx_id.to_string()));
        }

        let snapshot_ts = self.read_snapshot_ts(tx_id);
        let mut leases = self.index_method_write_leases.lock();
        let lease = leases.entry(index_id).or_default();
        match lease.holder {
            Some(owner) if owner == tx_id => Ok(()),
            Some(_) => Err(LimboError::Busy),
            None => {
                if lease
                    .active_deleters
                    .iter()
                    .any(|deleter| *deleter != tx_id)
                {
                    // A live transaction may still insert tombstone rows
                    // against segments this lease holder would retire. Wait
                    // for it to finish instead of losing its deletes.
                    return Err(LimboError::Busy);
                }
                if lease
                    .last_publish_ts
                    .is_some_and(|publish_ts| publish_ts > snapshot_ts)
                    || lease
                        .last_delete_commit_ts
                        .is_some_and(|delete_ts| delete_ts > snapshot_ts)
                {
                    return Err(LimboError::WriteWriteConflict);
                }
                lease.holder = Some(tx_id);
                Ok(())
            }
        }
    }

    /// Announce that `tx_id` will insert index-method tombstone rows (an FTS
    /// delete or update) against `index_id`'s existing segments.
    ///
    /// Refused with `Busy` while another transaction holds the index's lease
    /// (a merge in flight could retire the tombstoned segments and lose the
    /// deletes), and with `WriteWriteConflict` when a lease holder already
    /// published after this transaction's snapshot: the transaction's visible
    /// segment set predates the merge, so its tombstones would target retired
    /// segments and the deleted postings would resurrect in the merged one.
    /// Registration is idempotent and lasts until the transaction finishes;
    /// `release_index_method_write_leases` retires it and, on commit, records
    /// the commit timestamp so an overlapping merge is refused at its own
    /// commit.
    pub(crate) fn register_index_method_deleter(
        &self,
        tx_id: TxID,
        index_id: MVTableId,
    ) -> Result<()> {
        if !self.is_tx_rollbackable(tx_id) {
            return Err(LimboError::NoSuchTransactionID(tx_id.to_string()));
        }
        let snapshot_ts = self.read_snapshot_ts(tx_id);
        let mut leases = self.index_method_write_leases.lock();
        let lease = leases.entry(index_id).or_default();
        match lease.holder {
            Some(owner) if owner != tx_id => Err(LimboError::Busy),
            _ => {
                if lease
                    .last_publish_ts
                    .is_some_and(|publish_ts| publish_ts > snapshot_ts)
                {
                    return Err(LimboError::WriteWriteConflict);
                }
                lease.active_deleters.insert(tx_id);
                Ok(())
            }
        }
    }

    /// Re-check, at the lease holder's commit, that no delete overlapped the
    /// merge: any still-active deleter is `Busy`, and a deleter that
    /// committed after the holder's read snapshot is `WriteWriteConflict`
    /// (its tombstones are invisible to the merge and would be lost).
    pub(crate) fn check_index_method_merge_admissible(
        &self,
        tx_id: TxID,
        index_id: MVTableId,
    ) -> Result<()> {
        let snapshot_ts = self.read_snapshot_ts(tx_id);
        let mut leases = self.index_method_write_leases.lock();
        let lease = leases.entry(index_id).or_default();
        if lease
            .active_deleters
            .iter()
            .any(|deleter| *deleter != tx_id)
        {
            return Err(LimboError::Busy);
        }
        if lease
            .last_delete_commit_ts
            .is_some_and(|delete_ts| delete_ts > snapshot_ts)
        {
            return Err(LimboError::WriteWriteConflict);
        }
        Ok(())
    }

    fn release_index_method_write_leases(&self, tx_id: TxID) {
        // A committed holder published new index state: remember its commit
        // timestamp so later writers with older snapshots are refused.
        let committed_at =
            self.txs
                .get(&tx_id)
                .and_then(|entry| match entry.value().state.load() {
                    TransactionState::Committed(commit_ts) => Some(commit_ts),
                    _ => None,
                });
        let mut leases = self.index_method_write_leases.lock();
        for lease in leases.values_mut() {
            if lease.holder == Some(tx_id) {
                lease.holder = None;
                if committed_at.is_some() {
                    lease.last_publish_ts = committed_at;
                }
            }
            if lease.active_deleters.remove(&tx_id) {
                if let Some(commit_ts) = committed_at {
                    lease.last_delete_commit_ts = Some(
                        lease
                            .last_delete_commit_ts
                            .map_or(commit_ts, |previous| previous.max(commit_ts)),
                    );
                }
            }
        }
    }

    #[turso_macros::allocation_site(crate::alloc::MvStoreAllocationSite::FinalizedTxStateInsert)]
    fn insert_finalized_tx_state(
        &self,
        tx_id: TxID,
        commit_ts: u64,
    ) -> Result<(), TryReserveError> {
        self.finalized_tx_states
            .try_insert(tx_id, TransactionState::Committed(commit_ts))?;
        Ok(())
    }

    pub fn register_sequence_allocation(
        &self,
        tx_id: TxID,
        sequence_name: &str,
        sequence_value: i64,
    ) -> Result<()> {
        let Some(tx) = self.txs.get(&tx_id) else {
            return Err(LimboError::NoSuchTransactionID(tx_id.to_string()));
        };
        turso_assert!(
            matches!(
                tx.value().state.load(),
                TransactionState::Active | TransactionState::Preparing(_)
            ),
            "sequence allocation must be registered while the transaction is active or preparing"
        );

        let sequence_name = crate::util::normalize_ident(sequence_name);
        let mut allocations = self.sequence_allocations.lock();
        let tx_allocations = allocations.entry(sequence_name).or_default();
        tx_allocations
            .entry(tx_id)
            .and_modify(|value| *value = (*value).min(sequence_value))
            .or_insert(sequence_value);
        Ok(())
    }

    pub fn set_sequence_watermark(&self, sequence_name: &str, watermark: i64) {
        let sequence_name = crate::util::normalize_ident(sequence_name);
        self.sequence_watermarks
            .lock()
            .insert(sequence_name, watermark);
    }

    /// Returns the first sequence value that is not safe for cursor scans to pass.
    ///
    /// Readers can safely consume rows with sequence values less than this
    /// watermark. The value is the minimum of the current sequence boundary and
    /// any lower value already allocated by an active transaction.
    pub fn sequence_watermark(&self, sequence_name: &str) -> Option<i64> {
        let sequence_name = crate::util::normalize_ident(sequence_name);
        let mut allocations = self.sequence_allocations.lock();
        let mut remove_allocations = false;
        let active_watermark = {
            allocations
                .get_mut(&sequence_name)
                .and_then(|tx_allocations| {
                    tx_allocations.retain(|tx_id, _| {
                        self.txs.get(tx_id).is_some_and(|tx| {
                            matches!(
                                tx.value().state.load(),
                                TransactionState::Active | TransactionState::Preparing(_)
                            )
                        })
                    });
                    let watermark = tx_allocations.values().copied().min();
                    if tx_allocations.is_empty() {
                        remove_allocations = true;
                    }
                    watermark
                })
        };
        if remove_allocations {
            allocations.remove(&sequence_name);
        }
        let current_watermark = self.sequence_watermarks.lock().get(&sequence_name).copied();
        match (current_watermark, active_watermark) {
            (Some(current), Some(active)) => Some(current.min(active)),
            (Some(current), None) => Some(current),
            (None, active) => active,
        }
    }

    fn remove_sequence_allocations(&self, tx_id: TxID) {
        let mut allocations = self.sequence_allocations.lock();
        allocations.retain(|_, tx_allocations| {
            tx_allocations.remove(&tx_id);
            !tx_allocations.is_empty()
        });
    }

    /// Atomically retire a committed tx: clear the connection's mv_tx_id cache
    /// for `db_id`, then remove the tx from `txs`. Pairs the two mutations so
    /// no concurrent observer (or in-flight statement) can see the divergent
    /// `(cache=Some, txs=None)` state — the production-panic shape from
    /// `release_named_savepoint` and `NoSuchTransactionID` read-path errors.
    ///
    /// Order matches `rollback_tx` (cache cleared before `remove_tx`) so the
    /// commit and rollback paths are symmetric.
    ///
    /// Use this anywhere the commit state machine would otherwise call
    /// `remove_tx` directly. Other call sites that don't have a connection
    /// context (e.g. tests poking internal state) keep using `remove_tx`.
    pub fn finish_committed_tx(
        &self,
        tx_id: TxID,
        conn: &Connection,
        db_id: usize,
    ) -> Result<(), TryReserveError> {
        conn.set_mv_tx_for_db(db_id, None);
        self.remove_tx(tx_id)
    }

    fn get_new_transaction_database_header(&self, pager: &Arc<Pager>) -> DatabaseHeader {
        if self.global_header.read().is_none() {
            pager
                .io
                .block(|| pager.maybe_allocate_page1())
                .expect("failed to allocate page1");
            let header = pager
                .io
                .block(|| pager.with_header(|header| *header))
                .expect("failed to read database header");
            // TODO: We initialize header here, maybe this needs more careful handling
            self.global_header.write().replace(header);
            tracing::debug!(
                "get_transaction_database_header create: header={:?}",
                header
            );
            header
        } else {
            let header = self
                .global_header
                .read()
                .expect("global_header should be initialized");
            // The header could be stored, but not persisted yet
            pager
                .io
                .block(|| pager.maybe_allocate_page1())
                .expect("failed to allocate page1");
            tracing::debug!("get_transaction_database_header read: header={:?}", header);
            header
        }
    }

    pub fn get_transaction_database_header(&self, tx_id: &TxID) -> DatabaseHeader {
        let tx = self
            .txs
            .get(tx_id)
            .expect("transaction not found when trying to get header");
        let header = tx.value();
        let header = header.header.read();
        tracing::debug!("get_transaction_database_header read: header={:?}", header);
        *header
    }

    /// Update the cached global header's page size to match a fresh `PRAGMA page_size`.
    ///
    /// `global_header` is captured from the pager during MVCC bootstrap, before any PRAGMA
    /// can run, so it always starts at the default 4 KiB. Subsequent transactions copy from
    /// it, which means a `PRAGMA page_size = N` issued on the connection would otherwise be
    /// invisible to MVCC header lookups even though the pager itself honors `N` for on-disk
    /// page allocation. Only valid before any data has been written; matches the same
    /// precondition `Connection::reset_page_size` enforces via `db.initialized()`.
    pub fn set_global_page_size(&self, size: PageSize) {
        let mut header = self.global_header.write();
        if let Some(header) = header.as_mut() {
            header.page_size = size;
        }
    }

    pub fn with_header<T, F>(&self, f: F, tx_id: Option<&TxID>) -> Result<T>
    where
        F: Fn(&DatabaseHeader) -> T,
    {
        if let Some(tx_id) = tx_id {
            let tx = self
                .txs
                .get(tx_id)
                .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
            let header = tx.value();
            let header = header.header.read();
            tracing::debug!("with_header read: header={:?}", header);
            Ok(f(&header))
        } else {
            let header = self.global_header.read();
            tracing::debug!("with_header read: header={:?}", header);
            Ok(f(header.as_ref().ok_or_else(|| {
                LimboError::InternalError("global_header not initialized".to_string())
            })?))
        }
    }

    pub fn with_header_mut<T, F>(&self, f: F, tx_id: Option<&TxID>) -> Result<T>
    where
        F: Fn(&mut DatabaseHeader) -> T,
    {
        if let Some(tx_id) = tx_id {
            let tx = self
                .txs
                .get(tx_id)
                .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
            let header = tx.value();
            let mut header = header.header.write();
            tracing::debug!("with_header_mut read: header={:?}", header);
            let out = f(&mut header);
            // Commit path consults this flag to decide whether a header-only logical-log record
            // is required even when write_set stays empty.
            tx.value().header_dirty.store(true, Ordering::Release);
            Ok(out)
        } else {
            let mut header = self.global_header.write();
            let header = header.as_mut().ok_or_else(|| {
                LimboError::InternalError("global_header not initialized".to_string())
            })?;
            tracing::debug!("with_header_mut write: header={:?}", header);
            Ok(f(header))
        }
    }

    /// Commits a transaction with the specified transaction ID.
    ///
    /// This function commits the changes made within the specified transaction and finalizes the
    /// transaction. Once a transaction has been committed, all changes made within the transaction
    /// are visible to other transactions that access the same data.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The ID of the transaction to commit.
    pub fn commit_tx(
        self: &Arc<Self>,
        tx_id: TxID,
        connection: &Arc<Connection>,
        db_id: usize,
    ) -> Result<StateMachine<Box<CommitStateMachine<Clock, A>>>> {
        let state = Box::new(CommitStateMachine::new(
            CommitState::Initial,
            tx_id,
            self.clone(),
            connection.clone(),
            db_id,
            self.commit_coordinator.clone(),
            self.global_header.clone(),
        )?);
        let state_machine = StateMachine::new(state);
        Ok(state_machine)
    }

    /// Returns true if the transaction can be rolled back (Active or Preparing).
    pub fn is_tx_rollbackable(&self, tx_id: TxID) -> bool {
        self.txs.get(&tx_id).is_some_and(|tx| {
            matches!(
                tx.value().state.load(),
                TransactionState::Active | TransactionState::Preparing(_)
            )
        })
    }

    /// Rolls back a transaction with the specified ID.
    ///
    /// This function rolls back a transaction with the specified `tx_id` by
    /// discarding any changes made by the transaction.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The ID of the transaction to abort.
    /// * `db` - The database index this transaction belongs to.
    pub fn rollback_tx(&self, tx_id: TxID, _pager: Arc<Pager>, connection: &Connection, db: usize) {
        self.rollback_tx_inner(tx_id, Some(connection), db);
    }

    #[aristo::intent(
        "Rollback freezes the transaction before transferring its complete write set; every \
         recorded row-version chain is then visited from the transferred storage without cloning \
         or allocating a snapshot.",
        verify = "test",
        id = "rollback_transfers_frozen_write_set"
    )]
    fn rollback_tx_inner(&self, tx_id: TxID, connection: Option<&Connection>, db: usize) {
        let tx_unlocked = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx_unlocked.value();
        if let Some(connection) = connection {
            connection.set_mv_tx_for_db(db, None);
        }
        turso_assert!(matches!(
            tx.state.load(),
            TransactionState::Active | TransactionState::Preparing(_)
        ));
        tx.state.store(TransactionState::Aborted);
        tracing::trace!("abort(tx_id={})", tx_id);
        self.unlock_commit_lock_if_held(tx);

        // Hekaton Section 3.3: "If it aborted, it forces the dependent transactions
        // to also abort by setting their AbortNow flags."
        let dependents = std::mem::take(&mut *tx.commit_dep_set.lock());
        // a txn cannot depend on itself
        turso_assert!(
            !dependents.contains(&tx_id),
            "rollback_tx: transaction has itself in its own commit_dep_set"
        );
        for dep_tx_id in dependents {
            if let Some(dep_tx_entry) = self.txs.get(&dep_tx_id) {
                let dep_tx = dep_tx_entry.value();
                dep_tx.abort_now.store(true, Ordering::Release);
                dep_tx.commit_dep_counter.fetch_sub(1, Ordering::AcqRel);
            }
        }

        if self.is_exclusive_tx(&tx_id) {
            self.release_exclusive_tx(&tx_id);
        }

        // Transfer ownership under the lock so we can drop it before taking
        // row-version-chain locks.
        let write_set = tx.write_set.lock().take();
        for (_rowid, row_versions) in write_set.entries {
            for rv in row_versions.write().iter_mut() {
                rollback_row_version(tx_id, rv);
            }
        }

        if let Some(connection) = connection {
            if connection.schema.read().schema_version > connection.db.schema.lock().schema_version
            {
                // Connection made schema changes during tx and rolled back -> revert connection-local schema.
                *connection.schema.write() = connection.db.clone_schema();
            }
        }

        let tx = tx_unlocked.value();
        tx.state.store(TransactionState::Terminated);
        tracing::trace!("terminate(tx_id={})", tx_id);
        // Safe to remove here: the rollback loop above acquired the write lock on
        // every row version chain in the write set, clearing all TxID references.
        // Any concurrent reader that held a read lock on one of those chains has
        // already completed its register_commit_dependency call (it runs under the
        // read lock), so no future txs.get() for this tx_id can come from a
        // speculative read path.
        crate::without_allocation_faults!(self.remove_tx(tx_id).expect(ALLOC_ERR_MSG));
    }

    fn cleanup_dropped_commit(&self, tx_id: TxID, connection: &Connection, db_id: usize) {
        let tx_state = self.txs.get(&tx_id).map(|tx| {
            let tx = tx.value();
            match tx.state.load() {
                TransactionState::Preparing(end_ts) if tx.log_appended.load(Ordering::Acquire) => {
                    tx.state.store(TransactionState::Committed(end_ts));
                    TransactionState::Committed(end_ts)
                }
                state => state,
            }
        });
        match tx_state {
            Some(TransactionState::Active | TransactionState::Preparing(_)) => {
                if self.commit_coordinator.abandon_if_issued(tx_id) {
                    if connection.get_mv_tx_id_for_db(db_id) == Some(tx_id) {
                        connection.set_mv_tx_for_db(db_id, None);
                    }
                    return;
                }
                self.rollback_tx_inner(tx_id, Some(connection), db_id);
            }
            Some(TransactionState::Committed(end_ts)) => {
                // The dropped statement may have been interrupted mid
                // `RewriteLiveVersions`, leaving live row versions (e.g. b-tree
                // tombstones) that still reference this TxID. Finish the rewrite
                // synchronously before removing the tx from `txs`, otherwise later
                // visibility/conflict checks would find versions pointing at a
                // removed TxID (https://github.com/tursodatabase/turso/issues/7477).
                self.rewrite_live_versions_for_committed_tx(tx_id, end_ts);
                if let Some(tx) = self.txs.get(&tx_id) {
                    self.unlock_commit_lock_if_held(tx.value());
                }
                if self.is_exclusive_tx(&tx_id) {
                    self.release_exclusive_tx(&tx_id);
                }
                crate::without_allocation_faults!(self
                    .finish_committed_tx(tx_id, connection, db_id)
                    .expect(ALLOC_ERR_MSG));
            }
            Some(TransactionState::Aborted | TransactionState::Terminated) | None => {
                if connection.get_mv_tx_id_for_db(db_id) == Some(tx_id) {
                    connection.set_mv_tx_for_db(db_id, None);
                }
                if self.is_exclusive_tx(&tx_id) {
                    self.release_exclusive_tx(&tx_id);
                }
            }
        }
    }

    /// Rewrite every live row version in `tx_id`'s write set that still
    /// references the TxID to the committed timestamp `end_ts`.
    ///
    /// Synchronous (unchunked) counterpart of `step_rewrite_live_versions`:
    /// a commit statement dropped mid-`RewriteLiveVersions` must finish
    /// publishing its timestamps before the tx is removed from `txs`, so no
    /// row version is left referencing a TxID that no longer resolves.
    fn rewrite_live_versions_for_committed_tx(&self, tx_id: TxID, end_ts: u64) {
        let Some(tx_entry) = self.txs.get(&tx_id) else {
            return;
        };
        turso_assert!(
            matches!(tx_entry.value().state.load(), TransactionState::Committed(ts) if ts == end_ts),
            "rewrite_live_versions_for_committed_tx requires a committed transaction state"
        );
        let write_set = tx_entry.value().write_set.lock();
        for (_id, row_versions) in write_set.iter() {
            let mut row_versions = row_versions.write();
            for row_version in row_versions.iter_mut() {
                row_version.rewrite_txid_to_timestamp(tx_id, end_ts);
            }
        }
    }

    fn unlock_commit_lock_if_held(&self, tx: &Transaction<A>) {
        if tx.pager_commit_lock_held.swap(false, Ordering::AcqRel) {
            self.commit_coordinator.pager_commit_lock.unlock();
        }
    }

    /// Begin a savepoint for the transaction.
    /// This should be called at the start of a statement in an interactive transaction.
    pub fn begin_savepoint(&self, tx_id: TxID) {
        let tx = self
            .txs
            .get(&tx_id)
            .unwrap_or_else(|| panic!("Transaction {tx_id} not found while beginning savepoint"));
        tx.value().begin_savepoint();
    }

    /// Begin a user-visible named savepoint inside an existing transaction.
    ///
    /// `starts_transaction` is true when the savepoint was opened in autocommit mode and therefore
    /// releasing the root savepoint should commit the transaction.
    pub fn begin_named_savepoint(
        &self,
        tx_id: TxID,
        name: String,
        starts_transaction: bool,
        deferred_fk_violations: isize,
    ) {
        let tx = self.txs.get(&tx_id).unwrap_or_else(|| {
            panic!("Transaction {tx_id} not found while beginning named savepoint")
        });
        tx.value()
            .begin_named_savepoint(name, starts_transaction, deferred_fk_violations);
    }

    /// Release the newest savepoint for the transaction.
    /// This should be called when a statement completes successfully.
    /// Silently returns if the transaction doesn't exist (e.g., already committed).
    pub fn release_savepoint(&self, tx_id: TxID) {
        if let Some(tx) = self.txs.get(&tx_id) {
            tx.value().release_savepoint();
        }
        // If transaction doesn't exist, it was already committed - nothing to release
    }

    /// Releases a named savepoint and nested savepoints above it.
    ///
    /// Returns [SavepointResult::Commit] when releasing the root savepoint should commit the
    /// transaction.
    pub fn release_named_savepoint(&self, tx_id: TxID, name: &str) -> Result<SavepointResult> {
        let tx = self
            .txs
            .get(&tx_id)
            .unwrap_or_else(|| panic!("Transaction {tx_id} not found while releasing savepoint"));
        Ok(tx.value().release_named_savepoint(name))
    }

    /// Rolls back a savepoint within a transaction.
    /// Returns true if a savepoint was rolled back, false if no savepoint was active.
    pub fn rollback_first_savepoint(&self, tx_id: u64) -> Result<bool> {
        let tx = self.txs.get(&tx_id).unwrap_or_else(|| {
            panic!("Transaction {tx_id} not found while rolling back savepoint")
        });

        let tx = tx.value();
        let savepoint = tx.pop_statement_savepoint();

        if let Some(savepoint) = savepoint {
            self.rollback_savepoint_changes(tx_id, savepoint);
            Ok(true)
        } else {
            tracing::debug!(
                "rollback_savepoint(tx_id={}): no savepoint was active",
                tx_id
            );
            Ok(false)
        }
    }

    /// Rolls back to the newest matching named savepoint while keeping that savepoint active.
    ///
    /// Returns the deferred FK snapshot stored on the named savepoint, or `None` if no matching
    /// savepoint exists.
    pub fn rollback_to_named_savepoint(&self, tx_id: TxID, name: &str) -> Result<Option<isize>> {
        let tx = self.txs.get(&tx_id).unwrap_or_else(|| {
            panic!("Transaction {tx_id} not found while rolling back named savepoint")
        });
        let Some(SavepointRollbackResult {
            rolledback_savepoints,
            deferred_fk_violations,
        }) = tx.value().rollback_to_named_savepoint(name)
        else {
            return Ok(None);
        };

        for savepoint in rolledback_savepoints.into_iter().rev() {
            self.rollback_savepoint_changes(tx_id, savepoint);
        }

        Ok(Some(deferred_fk_violations))
    }

    fn rollback_savepoint_changes(&self, tx_id: TxID, savepoint: Savepoint<A>) {
        let Savepoint {
            header,
            header_dirty,
            created_table_versions,
            created_index_versions,
            deleted_table_versions,
            deleted_index_versions,
            newly_added_to_write_set,
            ..
        } = savepoint;

        tracing::debug!(
            "rollback_savepoint(tx_id={}, created_table={}, created_index={}, deleted_table={}, deleted_index={})",
            tx_id,
            created_table_versions.len(),
            created_index_versions.len(),
            deleted_table_versions.len(),
            deleted_index_versions.len()
        );

        let mut touched_rowids = BTreeSet::new();

        for (rowid, version_id) in created_table_versions {
            touched_rowids.insert(rowid.clone());
            if let Some(entry) = self.rows.get(&rowid) {
                let mut versions = entry.value().write();
                let before = versions.len();
                versions.retain(|rv| rv.id != version_id);
                self.dec_live_version_count_approx(before - versions.len());
                tracing::debug!(
                    "rollback_savepoint: removed table version(table_id={}, row_id={}, version_id={})",
                    rowid.table_id,
                    rowid.row_id,
                    version_id
                );
            }
        }

        for ((table_id, key), version_id) in created_index_versions {
            if let Some(index) = self.index_rows.get(&table_id) {
                if let Some(entry) = index.value().get(&key) {
                    let mut versions = entry.value().write();
                    let before = versions.len();
                    versions.retain(|rv| rv.id != version_id);
                    self.dec_live_version_count_approx(before - versions.len());
                    tracing::debug!(
                        "rollback_savepoint: removed index version(table_id={}, version_id={})",
                        table_id,
                        version_id
                    );
                }
            }
        }

        for (rowid, version_id) in deleted_table_versions {
            touched_rowids.insert(rowid.clone());
            if let Some(entry) = self.rows.get(&rowid) {
                let mut versions = entry.value().write();
                for rv in versions.iter_mut() {
                    if rv.id == version_id {
                        rv.set_end(None);
                        tracing::debug!(
                            "rollback_savepoint: restored table version(table_id={}, row_id={}, version_id={})",
                            rowid.table_id,
                            rowid.row_id,
                            version_id
                        );
                        break;
                    }
                }
            }
        }

        for ((table_id, key), version_id) in deleted_index_versions {
            if let Some(index) = self.index_rows.get(&table_id) {
                if let Some(entry) = index.value().get(&key) {
                    let mut versions = entry.value().write();
                    for rv in versions.iter_mut() {
                        if rv.id == version_id {
                            rv.set_end(None);
                            tracing::debug!(
                                "rollback_savepoint: restored index version(table_id={}, version_id={})",
                                table_id,
                                version_id
                            );
                            break;
                        }
                    }
                }
            }
        }

        touched_rowids.extend(newly_added_to_write_set.into_iter().map(|(id, _)| id));
        self.remove_rolled_back_rows_from_write_set(tx_id, touched_rowids.clone());

        let tx = self
            .txs
            .get(&tx_id)
            .unwrap_or_else(|| panic!("Transaction {tx_id} not found while restoring savepoint"));
        let tx = tx.value();
        *tx.header.write() = header;
        tx.header_dirty.store(header_dirty, Ordering::Release);
    }

    fn row_has_uncommitted_version_for_tx(&self, rowid: &RowID, tx_id: TxID) -> bool {
        if rowid.row_id.is_int_key() {
            let Some(entry) = self.rows.get(rowid) else {
                return false;
            };
            let versions = entry.value().read();
            return versions.iter().any(|rv| {
                rv.begin() == Some(TxTimestampOrID::TxID(tx_id))
                    || rv.end() == Some(TxTimestampOrID::TxID(tx_id))
            });
        }

        let RowKey::Record(ref record) = rowid.row_id else {
            return false;
        };
        let Some(index) = self.index_rows.get(&rowid.table_id) else {
            return false;
        };
        let Some(entry) = index.value().get(record.as_ref()) else {
            return false;
        };
        let versions = entry.value().read();
        versions.iter().any(|rv| {
            rv.begin() == Some(TxTimestampOrID::TxID(tx_id))
                || rv.end() == Some(TxTimestampOrID::TxID(tx_id))
        })
    }

    fn remove_rolled_back_rows_from_write_set(&self, tx_id: TxID, rowids: BTreeSet<RowID>) {
        if rowids.is_empty() {
            return;
        }
        let Some(tx) = self.txs.get(&tx_id) else {
            return;
        };
        let tx = tx.value();
        // Single pass: drop entries that appear in `rowids` AND have no
        // surviving uncommitted version (parent savepoints may still pin
        // them).
        let mut write_set = tx.write_set.lock();
        write_set.retain(|rowid, _rv| {
            if !rowids.contains(rowid) {
                return true;
            }
            self.row_has_uncommitted_version_for_tx(rowid, tx_id)
        });
    }

    /// Returns true if the given transaction is the exclusive transaction.
    #[inline]
    pub fn is_exclusive_tx(&self, tx_id: &TxID) -> bool {
        self.exclusive_tx.load(Ordering::Acquire) == *tx_id
    }

    /// Returns true if there is an exclusive transaction ongoing.
    #[inline]
    fn has_exclusive_tx(&self) -> bool {
        self.exclusive_tx.load(Ordering::Acquire) != NO_EXCLUSIVE_TX
    }

    fn has_preparing_tx_other_than(&self, tx_id: TxID) -> bool {
        self.txs.iter().any(|entry| {
            *entry.key() != tx_id
                && matches!(entry.value().state.load(), TransactionState::Preparing(_))
        })
    }

    /// Acquires the exclusive transaction lock to the given transaction ID.
    fn acquire_exclusive_tx(
        &self,
        tx_id: &TxID,
        yield_context: Option<&YieldContext>,
    ) -> Result<()> {
        #[cfg(not(any(test, injected_yields)))]
        let _ = yield_context;
        if self.exclusive_tx.load(Ordering::Acquire) == *tx_id {
            // Re-entrant upgrade attempt for the same transaction.
            return Ok(());
        }
        // if some other transaction is in preparing state, then we cannot let this tx to
        // continue, as the preparing txn will eventually commit
        if self.has_preparing_tx_other_than(*tx_id) {
            return Err(LimboError::Busy);
        }
        // after we acquired begin_ts, we will check if some other txn committed in the meantime.
        // If so, no point in letting this txn to progress as it's begin_ts is less than
        // other txn's commit ts.
        // do note that this is an optimistic / early check. We need to check this again after this
        // txn gets exclusive txn status. check below after the CAS
        if let Some(tx) = self.txs.get(tx_id) {
            let tx = tx.value();
            if tx.begin_ts < self.last_committed_tx_ts.load(Ordering::Acquire) {
                // Another transaction committed after this transaction's begin timestamp, do not allow exclusive lock.
                // This mimics regular (non-CONCURRENT) sqlite transaction behavior.
                return Err(LimboError::Busy);
            }
        }
        #[cfg(any(test, injected_yields))]
        if let Some(yield_context) = yield_context {
            if yield_context.injector.as_ref().is_some_and(|injector| {
                injector.should_yield(
                    yield_context.instance_id,
                    yield_context.selection_key,
                    ExclusiveTxYieldPoint::AfterTimestampCheckBeforeCas.point(),
                )
            }) {
                tracing::debug!(
                    tx_id,
                    "injected exclusive acquisition interleaving before CAS"
                );
            }
        }
        match self.exclusive_tx.compare_exchange(
            NO_EXCLUSIVE_TX,
            *tx_id,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                if self.has_preparing_tx_other_than(*tx_id) {
                    self.release_exclusive_tx(tx_id);
                    return Err(LimboError::Busy);
                }
                // we will check again, if some other txn committed in the meantime.
                // we did this check previously too, but we will have to do this again.
                // consider this timeline of events:
                //
                // t0 - no txn is preparing, and last_committed_ts is smaller than begin_ts
                //      we proceed to do CAS
                // t1 - we are yet to do CAS, but another txn comes in, goes into
                //      preparing state, and commits with commit_ts greater than our begin_ts
                // t3 - we proceed with CAS and get exclusive txn status
                //
                // at this point, we have got exclusive txn but last_committed_tx_ts is greater than
                // our begin_ts, violating the isolation guarantee.
                //
                // we want to prevent this. so we acquire the exclusive txn and then check again.
                // we can also be sure that once we get the exclusive txn status, no other txn can sneak in and commit,
                // because to commit, we make sure that there is no other exclusive txn. Check
                // `CommitState::Initial`.
                if let Some(tx) = self.txs.get(tx_id) {
                    let tx = tx.value();
                    if tx.begin_ts < self.last_committed_tx_ts.load(Ordering::Acquire) {
                        self.release_exclusive_tx(tx_id);
                        return Err(LimboError::Busy);
                    }
                }
                Ok(())
            }
            Err(_) => {
                // Another transaction already holds the exclusive lock
                Err(LimboError::Busy)
            }
        }
    }

    /// Release the exclusive transaction lock if held by the this transaction.
    fn release_exclusive_tx(&self, tx_id: &TxID) {
        tracing::trace!("release_exclusive_tx(tx_id={})", tx_id);
        let prev = self.exclusive_tx.swap(NO_EXCLUSIVE_TX, Ordering::Release);
        turso_assert_eq!(prev, *tx_id, "exclusive lock released by wrong tx", { "expected_tx_id": *tx_id, "actual_tx_id": prev });
    }

    /// Generates next unique transaction id
    pub fn get_tx_id(&self) -> u64 {
        self.tx_ids.fetch_add(1, Ordering::SeqCst)
    }

    /// Generates next unique version ID for RowVersion tracking.
    pub fn get_version_id(&self) -> u64 {
        self.version_id_counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Generate a begin timestamp. No side-effect needed alongside generation.
    pub fn get_begin_timestamp(&self) -> u64 {
        self.clock.get_timestamp(crate::mvcc::clock::no_op)
    }

    /// Generate a commit timestamp and call `f` with it while the clock
    /// lock is held, atomically publishing the timestamp before release.
    /// See [`MvccClock`] for the full explanation.
    pub fn get_commit_timestamp<F: FnOnce(u64)>(&self, f: F) -> u64 {
        self.clock.get_timestamp(f)
    }

    /// Snapshot timestamp for a checkpoint, clamped below any in-flight (`Preparing`)
    /// commit. The published durable boundary (`durable_txid_max_new`) derives from this.
    /// `last_committed_tx_ts` is a `fetch_max` high-water mark, so a transaction that
    /// already assigned a *lower* `end_ts` and is still `Preparing` can sit below it; the
    /// checkpoint would skip that transaction (not yet `Committed`) yet publish a boundary
    /// above its `end_ts`, and a crash after it finalizes would discard its log frame
    /// (`commit_ts <= boundary`) even though it was never written to the B-tree — silent
    /// data loss. Clamping below the lowest `Preparing` end_ts prevents the straddle.
    ///
    /// Computed while holding the clock lock (via `get_timestamp`) so a transaction
    /// mid-(end_ts assignment + `Preparing` publish, which happen together under that
    /// lock) cannot be missed by the scan. Active transactions need no clamp: their future
    /// `end_ts` is drawn from the monotonic clock and is therefore `> snapshot_ts`.
    pub fn checkpoint_snapshot_ts(&self) -> u64 {
        let mut snapshot_ts = 0;
        self.clock.get_timestamp(|_now| {
            let last_committed = self.last_committed_tx_ts.load(Ordering::Acquire);
            let inflight_floor = self
                .txs
                .iter()
                .filter_map(|entry| match entry.value().state.load() {
                    TransactionState::Preparing(ts) => Some(ts),
                    _ => None,
                })
                .min()
                .unwrap_or(u64::MAX);
            snapshot_ts = last_committed.min(inflight_floor.saturating_sub(1));
        });
        snapshot_ts
    }

    pub(crate) fn uses_passive_checkpoint(&self) -> bool {
        self.experimental_mvcc_passive_checkpoint
    }

    /// Try to enter the passive publish window. Returns false if another publish is in flight.
    pub(crate) fn try_begin_passive_publish_window(&self) -> bool {
        debug_assert!(self.experimental_mvcc_passive_checkpoint);
        self.checkpoint_publish_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Release the passive publish drain bit after a successful or failed publish attempt.
    pub(crate) fn end_passive_publish_window(&self) {
        self.checkpoint_publish_in_progress
            .store(false, Ordering::Release);
    }

    pub(crate) fn schema_generation(&self) -> u64 {
        self.schema_generation.load(Ordering::Acquire)
    }

    pub(crate) fn bump_schema_generation(&self) {
        self.schema_generation.fetch_add(1, Ordering::Release);
    }

    /// Passive checkpoint published new physical roots after this transaction began.
    pub fn schema_still_valid_for_tx(&self, tx_id: TxID) -> Result<()> {
        if !self.experimental_mvcc_passive_checkpoint {
            return Ok(());
        }
        let Some(entry) = self.txs.get(&tx_id) else {
            return Ok(());
        };
        if entry.value().schema_generation_at_begin != self.schema_generation() {
            return Err(LimboError::SchemaUpdated);
        }
        Ok(())
    }

    /// Compute the low-water mark: the minimum begin_ts of all active or
    /// preparing transactions. Returns u64::MAX if no transactions are active.
    /// Used by GC to determine which row versions are safe to reclaim.
    pub fn compute_lwm(&self) -> u64 {
        self.txs
            .iter()
            .filter_map(|entry| {
                let tx = entry.value();
                match tx.state.load() {
                    TransactionState::Active | TransactionState::Preparing(_) => Some(tx.begin_ts),
                    _ => None,
                }
            })
            .min()
            .unwrap_or(u64::MAX)
    }

    /// Default `mvcc_gc_threshold`: run an incremental GC pass roughly every
    /// this many newly inserted versions. Small enough that steady-state
    /// memory stays bounded under heavy short-txn concurrency, large enough
    /// that small workloads (and most unit tests) never trigger a pass.
    pub const DEFAULT_GC_VERSION_THRESHOLD: i64 = 16 * 1024;

    /// Upper bound on table-row chains scanned by one inline `gc_incremental`
    /// pass on the commit path. Keeps a pass cheap (sub-millisecond) so it
    /// doesn't noticeably slow the committing connection; steady state relies
    /// on frequent passes resuming via `gc_table_cursor`.
    pub const MAX_CHAINS_PER_GC: usize = 4096;

    /// Current approximate live row-version count. Heuristic only (see the
    /// `live_version_count_approx` field) — never use for correctness decisions.
    pub fn live_version_count_approx(&self) -> usize {
        self.live_version_count_approx.load(Ordering::Relaxed)
    }

    /// Debug SkipMap / GC counters. Walks every chain; not for hot paths.
    #[cfg(test)]
    pub(crate) fn debug_gc_snapshot(&self) -> GcDebugSnapshot {
        let mut rows_empty_slots = 0;
        let mut rows_versions = 0;
        for entry in self.rows.iter() {
            let n = entry.value().read().len();
            rows_versions += n;
            if n == 0 {
                rows_empty_slots += 1;
            }
        }

        let mut index_slots = 0;
        let mut index_empty_slots = 0;
        let mut index_versions = 0;
        for index in self.index_rows.iter() {
            for entry in index.value().iter() {
                index_slots += 1;
                let n = entry.value().read().len();
                index_versions += n;
                if n == 0 {
                    index_empty_slots += 1;
                }
            }
        }

        GcDebugSnapshot {
            rows_slots: self.rows.len(),
            rows_empty_slots,
            rows_versions,
            index_slots,
            index_empty_slots,
            index_versions,
            live_version_count_approx: self.live_version_count_approx(),
            live_versions_at_last_gc: self.live_versions_at_last_gc.load(Ordering::Relaxed),
            lwm: self.compute_lwm(),
            durable_txid_max: self.durable_txid_max.load(Ordering::SeqCst),
            logical_log_offset: self.logical_log_offset(),
            logical_log_size: self.get_logical_log_file().size().unwrap_or(0),
            active_txs: self.txs.len(),
            min_reader_mark: self.compute_min_reader_mark(),
            backfill_floor: *self.backfill_floor.read(),
        }
    }

    /// Saturating decrement of the live-version heuristic. The counter is
    /// approximate, so clamp at zero rather than risk an underflow wrap that
    /// would make `should_gc` fire on every commit.
    fn dec_live_version_count_approx(&self, n: usize) {
        if n == 0 {
            return;
        }
        let mut current = self.live_version_count_approx.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_sub(n);
            match self.live_version_count_approx.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Set the inline-GC trigger threshold (growth in live versions since the
    /// last GC pass). Negative disables inline GC. Mirrors
    /// `set_checkpoint_threshold`; wired to the `mvcc_gc_threshold` PRAGMA.
    pub fn set_gc_threshold(&self, threshold: i64) {
        self.gc_version_threshold
            .store(threshold, Ordering::Relaxed);
    }

    pub fn gc_threshold(&self) -> i64 {
        self.gc_version_threshold.load(Ordering::Relaxed)
    }

    pub fn set_group_commit_enabled(&self, enabled: bool) {
        self.commit_coordinator.set_group_commit_enabled(enabled);
    }

    pub fn group_commit_enabled(&self) -> bool {
        self.commit_coordinator.group_commit_enabled()
    }

    #[cfg(test)]
    pub fn last_group_commit_size(&self) -> usize {
        self.commit_coordinator.last_group_size()
    }

    /// Whether an incremental GC pass should run now: inline GC is enabled
    /// (threshold >= 0) and `live_version_count_approx` has grown past the threshold
    /// since the last pass. Heuristic — drift only changes GC frequency.
    pub fn should_gc(&self) -> bool {
        let threshold = self.gc_version_threshold.load(Ordering::Relaxed);
        if threshold < 0 {
            return false;
        }
        let current = self.live_version_count_approx.load(Ordering::Relaxed);
        let at_last = self.live_versions_at_last_gc.load(Ordering::Relaxed);
        current.saturating_sub(at_last) >= threshold as usize
    }

    /// Garbage-collects row versions that are invisible to all active transactions.
    /// Uses the low-water mark (LWM) to determine reclaimability in O(1) per version.
    /// Covers both table rows (`self.rows`) and index rows (`self.index_rows`).
    /// Returns the number of removed versions.
    pub fn drop_unused_row_versions(&self) -> usize {
        self.drop_unused_row_versions_inner(
            false,
            !self.experimental_mvcc_passive_checkpoint,
            WalPos::STAGED,
        )
    }

    /// Like [`Self::drop_unused_row_versions`], and remove emptied SkipMap slots.
    /// Also drops last currents already in the B-tree (Truncate Finalize).
    /// Writers retry if GC unlinks their Arc (`insert_version` / `insert_index_version`).
    pub fn drop_unused_row_versions_and_slots(&self) -> usize {
        self.drop_unused_row_versions_inner(true, true, WalPos::STAGED)
    }

    /// Drop old versions and empty SkipMap slots, but keep the latest copy of each
    /// row (Rule 3 off). Passive Finalize uses this. `reader_mark_floor` should
    /// include pager-held readers, not only `txs`.
    pub fn drop_unused_row_versions_unlink_empty_at(&self, reader_mark_floor: WalPos) -> usize {
        self.drop_unused_row_versions_inner(true, false, reader_mark_floor)
    }

    /// Incremental GC on the commit path: reclaim up to `max_chains` table chains
    /// (resuming via `gc_table_cursor`). Truncate mode unlinks empty SkipMap slots;
    /// Passive leaves them for Finalize `unlink_empty`. Skips `finalized_tx_states`
    /// pruning (checkpoint still does that).
    pub fn gc_incremental(&self, max_chains: usize) -> usize {
        // Truncate checkpoints hold the write side for the whole pass; pin a read
        // guard so inline GC cannot race them. Passive checkpoints use the publish
        // drain bit instead, so skip the lifetime-style pin here.
        let _ckpt_guard = if self.experimental_mvcc_passive_checkpoint {
            None
        } else if self.blocking_checkpoint_lock.read() {
            struct CheckpointReadGuard<'a>(&'a TursoRwLock);
            impl Drop for CheckpointReadGuard<'_> {
                fn drop(&mut self) {
                    self.0.unlock();
                }
            }
            Some(CheckpointReadGuard(&self.blocking_checkpoint_lock))
        } else {
            return 0;
        };

        // Single-flight: only one inline GC pass runs at a time across all
        // connections (see `gc_in_progress`). Losing the race is a no-op — the
        // growth that triggered this commit's `should_gc` will retrigger soon,
        // or the in-flight pass already covers the same chains. The RAII guard
        // releases the gate even if the body panics, so a stuck flag can never
        // wedge GC for the lifetime of the store.
        if self
            .gc_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return 0;
        }
        struct GcGate<'a>(&'a AtomicBool);
        impl Drop for GcGate<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _gate = GcGate(&self.gc_in_progress);

        let passive = self.experimental_mvcc_passive_checkpoint;
        // Passive: keep the last current SkipMap version (B-trees may still be mid-write).
        // Blocking Truncate: safe to drop it once the B-tree already has the row.
        let drop_current_if_in_btree = !passive;
        let lwm = if passive {
            let mut sampled = u64::MAX;
            self.clock.get_timestamp(|_| sampled = self.compute_lwm());
            sampled
        } else {
            self.compute_lwm()
        };

        // Short-circuit when a long-running transaction has pinned the LWM at
        // the same value since the last pass: nothing newly reclaimable can
        // exist below it (any version superseded since then ends above the
        // pinning txn's begin_ts). `u64::MAX` (no active txns) never
        // short-circuits — that is exactly when the most is reclaimable. Reset
        // the trigger baseline so `should_gc` doesn't spin on every commit
        // while the LWM is stuck.
        if lwm != u64::MAX && lwm == self.gc_last_lwm.load(Ordering::Relaxed) {
            self.live_versions_at_last_gc.store(
                self.live_version_count_approx.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            return 0;
        }

        let ckpt_max = self.durable_txid_max.load(Ordering::SeqCst);
        // Bound by the backfill boundary: never reclaim a version materialized in un-backfilled
        // WAL frames — a db-file reader (present or future) needs the version-store copy.
        let min_reader_mark = self
            .compute_min_reader_mark()
            .min(*self.backfill_floor.read());

        let mut dropped = 0;
        let mut processed = 0;
        let mut last_key: Option<RowID> = None;

        // Resume strictly after the last key processed by the previous pass.
        let start_bound = match self.gc_table_cursor.lock().clone() {
            Some(k) => Bound::Excluded(k),
            None => Bound::Unbounded,
        };
        for entry in self.rows.range((start_bound, Bound::Unbounded)) {
            if processed >= max_chains {
                break;
            }
            // GC floor: retain rows of a freshly-materialized btree not yet visible to all readers.
            if !self.rootpage_gc_protected(&entry.key().table_id, min_reader_mark) {
                if passive {
                    self.clock.get_timestamp(|_| {
                        let lwm = self.compute_lwm();
                        let mut versions = entry.value().write();
                        dropped += Self::gc_version_chain(
                            &mut versions,
                            lwm,
                            ckpt_max,
                            true,
                            min_reader_mark,
                            drop_current_if_in_btree,
                        );
                    });
                } else {
                    let mut versions = entry.value().write();
                    dropped += Self::gc_version_chain(
                        &mut versions,
                        lwm,
                        ckpt_max,
                        false,
                        min_reader_mark,
                        drop_current_if_in_btree,
                    );
                    if versions.is_empty() {
                        entry.remove();
                    }
                }
            }
            last_key = Some(entry.key().clone());
            processed += 1;
        }

        // If we processed fewer chains than the budget, the range was
        // exhausted — wrap the cursor to the start for the next pass.
        let table_wrapped = processed < max_chains;
        *self.gc_table_cursor.lock() = if table_wrapped { None } else { last_key };

        // Sweep index chains with their own bounded, resumable budget (see
        // `gc_index_incremental`). Each map has an independent cursor so a
        // single huge index can't force an unbounded pass.
        dropped += self.gc_index_incremental(lwm, ckpt_max, max_chains);

        self.dec_live_version_count_approx(dropped);
        // Reset the trigger baseline so `should_gc` measures growth from here.
        self.live_versions_at_last_gc.store(
            self.live_version_count_approx.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );

        // Record the LWM only once a full cycle (both cursors wrapped back to
        // the start) has completed at this LWM, so the short-circuit above
        // never skips chains we haven't yet swept at the current LWM. Until the
        // cycle finishes, leave `gc_last_lwm` unchanged so subsequent passes
        // keep scanning.
        let full_cycle =
            self.gc_table_cursor.lock().is_none() && self.gc_index_cursor.lock().is_none();
        if full_cycle {
            self.gc_last_lwm.store(lwm, Ordering::Relaxed);
        }

        if dropped > 0 {
            tracing::trace!(
                "gc_incremental: reclaimed {dropped} versions ({processed} table chains, table_wrapped={table_wrapped}), live~{}",
                self.live_version_count_approx.load(Ordering::Relaxed)
            );
        }
        dropped
    }

    /// Resumable index-chain GC: the index-map counterpart to the table sweep
    /// in [`Self::gc_incremental`]. Applies `gc_version_chain` to up to
    /// `max_chains` index version chains, resuming strictly after the
    /// `(index id, key)` the previous pass stopped at (`gc_index_cursor`) and
    /// wrapping to the start when the nested maps are exhausted. Truncate mode
    /// unlinks empty slots; Passive leaves them for Finalize `unlink_empty`.
    /// Returns the number of versions reclaimed.
    ///
    /// `index_rows` is nested (`MVTableId -> key -> chain`), so the cursor is a
    /// `(MVTableId, key)` pair: the outer scan resumes at the saved index id
    /// (inclusive, to finish its remaining keys) and the inner scan resumes
    /// after the saved key; later indexes start from their first key.
    fn gc_index_incremental(&self, lwm: u64, ckpt_max: u64, max_chains: usize) -> usize {
        let passive = self.experimental_mvcc_passive_checkpoint;
        let drop_current_if_in_btree = !passive;
        let mut dropped = 0;
        let mut processed = 0;
        let mut last: Option<(MVTableId, Arc<SortableIndexKey>)> = None;
        // Bound by the backfill boundary: never reclaim a version materialized in un-backfilled
        // WAL frames — a db-file reader (present or future) needs the version-store copy.
        let min_reader_mark = self
            .compute_min_reader_mark()
            .min(*self.backfill_floor.read());

        let cursor = self.gc_index_cursor.lock().clone();
        let outer_start = match &cursor {
            Some((index_id, _)) => Bound::Included(*index_id),
            None => Bound::Unbounded,
        };
        'outer: for outer in self.index_rows.range((outer_start, Bound::Unbounded)) {
            if processed >= max_chains {
                break;
            }
            let index_id = *outer.key();
            // GC floor: retain a freshly-materialized index not yet visible to all readers.
            if self.rootpage_gc_protected(&index_id, min_reader_mark) {
                continue;
            }
            let inner = outer.value();
            // Resume after the saved key only within the index that the cursor
            // pointed into; every later index starts from its first key.
            let inner_start = match &cursor {
                Some((cursor_id, key)) if *cursor_id == index_id => Bound::Excluded(key.clone()),
                _ => Bound::Unbounded,
            };
            for inner_entry in inner.range((inner_start, Bound::Unbounded)) {
                if processed >= max_chains {
                    break 'outer;
                }
                if passive {
                    self.clock.get_timestamp(|_| {
                        let lwm = self.compute_lwm();
                        let mut versions = inner_entry.value().write();
                        dropped += Self::gc_version_chain(
                            &mut versions,
                            lwm,
                            ckpt_max,
                            true,
                            min_reader_mark,
                            drop_current_if_in_btree,
                        );
                    });
                } else {
                    let mut versions = inner_entry.value().write();
                    dropped += Self::gc_version_chain(
                        &mut versions,
                        lwm,
                        ckpt_max,
                        false,
                        min_reader_mark,
                        drop_current_if_in_btree,
                    );
                    if versions.is_empty() {
                        self.bump_index_rows_epoch();
                        inner_entry.remove();
                    }
                }
                last = Some((index_id, inner_entry.key().clone()));
                processed += 1;
            }
        }

        // Fewer chains than the budget => nested maps exhausted; wrap to start.
        let wrapped = processed < max_chains;
        *self.gc_index_cursor.lock() = if wrapped { None } else { last };
        dropped
    }

    fn drop_unused_row_versions_inner(
        &self,
        remove_empty_slots: bool,
        drop_current_if_in_btree: bool,
        reader_mark_floor: WalPos,
    ) -> usize {
        let lwm = self.compute_lwm();
        let ckpt_max = self.durable_txid_max.load(Ordering::SeqCst);
        let mut referenced_tx_ids = HashSet::default();

        let dropped = self.gc_table_row_versions(
            lwm,
            ckpt_max,
            &mut referenced_tx_ids,
            remove_empty_slots,
            drop_current_if_in_btree,
            reader_mark_floor,
        ) + self.gc_index_row_versions(
            lwm,
            ckpt_max,
            &mut referenced_tx_ids,
            remove_empty_slots,
            drop_current_if_in_btree,
            reader_mark_floor,
        );
        self.dec_live_version_count_approx(dropped);
        let pruned_finalized = self.prune_finalized_tx_states(&referenced_tx_ids);

        tracing::trace!(
            "drop_unused_row_versions() -> dropped {dropped}, pruned_finalized={pruned_finalized}, txs: {}, finalized_tx_states: {}, rows: {}",
            self.txs.len(),
            self.finalized_tx_states.len(),
            self.rows.len()
        );
        dropped
    }

    fn gc_table_row_versions(
        &self,
        lwm: u64,
        ckpt_max: u64,
        referenced_tx_ids: &mut HashSet<TxID>,
        remove_empty_slots: bool,
        drop_current_if_in_btree: bool,
        reader_mark_floor: WalPos,
    ) -> usize {
        let mut dropped = 0;
        // Bound by the backfill boundary: never reclaim a version materialized in un-backfilled
        // WAL frames — a db-file reader (present or future) needs the version-store copy.
        // `reader_mark_floor` additionally covers pager-pinned readers not yet published as
        // MVCC transactions (see `drop_unused_row_versions_unlink_empty_at`).
        let min_reader_mark = self
            .compute_min_reader_mark()
            .min(*self.backfill_floor.read())
            .min(reader_mark_floor);

        for entry in self.rows.iter() {
            // GC floor: retain rows of a freshly-materialized btree not yet visible to all readers.
            if self.rootpage_gc_protected(&entry.key().table_id, min_reader_mark) {
                continue;
            }
            let mut versions = entry.value().write();
            dropped += Self::gc_version_chain(
                &mut versions,
                lwm,
                ckpt_max,
                self.experimental_mvcc_passive_checkpoint,
                min_reader_mark,
                drop_current_if_in_btree,
            );
            Self::collect_referenced_txids(&versions, referenced_tx_ids);
            if remove_empty_slots && versions.is_empty() {
                entry.remove();
            }
        }
        dropped
    }

    fn gc_index_row_versions(
        &self,
        lwm: u64,
        ckpt_max: u64,
        referenced_tx_ids: &mut HashSet<TxID>,
        remove_empty_slots: bool,
        drop_current_if_in_btree: bool,
        reader_mark_floor: WalPos,
    ) -> usize {
        let mut dropped = 0;
        // Bound by the backfill boundary: never reclaim a version materialized in un-backfilled
        // WAL frames — a db-file reader (present or future) needs the version-store copy.
        // `reader_mark_floor` additionally covers pager-pinned readers not yet published as
        // MVCC transactions (see `drop_unused_row_versions_unlink_empty_at`).
        let min_reader_mark = self
            .compute_min_reader_mark()
            .min(*self.backfill_floor.read())
            .min(reader_mark_floor);

        for outer_entry in self.index_rows.iter() {
            // GC floor: retain a freshly-materialized index not yet visible to all readers.
            if self.rootpage_gc_protected(outer_entry.key(), min_reader_mark) {
                continue;
            }
            let inner_map = outer_entry.value();

            for inner_entry in inner_map.iter() {
                let mut versions = inner_entry.value().write();
                dropped += Self::gc_version_chain(
                    &mut versions,
                    lwm,
                    ckpt_max,
                    self.experimental_mvcc_passive_checkpoint,
                    min_reader_mark,
                    drop_current_if_in_btree,
                );
                Self::collect_referenced_txids(&versions, referenced_tx_ids);
                if remove_empty_slots && versions.is_empty() {
                    self.bump_index_rows_epoch();
                    // Inner key only — the outer per-index map stays (bounded by index count).
                    inner_entry.remove();
                }
            }
        }
        dropped
    }

    fn collect_referenced_txids(versions: &[RowVersion], referenced_tx_ids: &mut HashSet<TxID>) {
        for version in versions {
            if let Some(TxTimestampOrID::TxID(tx_id)) = version.begin() {
                referenced_tx_ids.insert(tx_id);
            }
            if let Some(TxTimestampOrID::TxID(tx_id)) = version.end() {
                referenced_tx_ids.insert(tx_id);
            }
        }
    }

    fn prune_finalized_tx_states(&self, referenced_tx_ids: &HashSet<TxID>) -> usize {
        if self.finalized_tx_states.is_empty() {
            return 0;
        }

        let to_remove: Vec<TxID> = self
            .finalized_tx_states
            .iter()
            .filter_map(|entry| {
                let tx_id = *entry.key();
                (!referenced_tx_ids.contains(&tx_id)).then_some(tx_id)
            })
            .collect();

        for tx_id in &to_remove {
            self.finalized_tx_states.remove(tx_id);
        }

        to_remove.len()
    }

    /// Apply GC rules to a single version chain. Returns the number removed.
    ///
    /// Rule 1: aborted garbage (begin=None, end=None) — always remove.
    /// Rule 2: superseded (end=Timestamp(e)) — remove once no reader can see it,
    ///         unless it's a tombstone (no committed current) whose delete isn't
    ///         checkpointed yet, or a B-tree-resident version whose physical
    ///         delete/overwrite hasn't been checkpointed.
    /// Rule 3: last remaining current (end=None) — remove only when
    ///         `drop_current_if_in_btree` is true and the B-tree already has it.
    ///
    /// Passive gates Rule 2 on `materialized_at` + `min_reader_mark`. Blocking
    /// Truncate uses `ckpt_max` instead.
    ///
    /// Leaving Rule 3 off keeps a SkipMap copy so an older reader cannot fall
    /// through to a B-tree page a later checkpoint already rewrote. Truncate can
    /// turn it on under the blocking lock (no open MVCC txs). Callers set
    /// `drop_current_if_in_btree` when they want Rule 3.
    fn gc_version_chain(
        versions: &mut RowVersionChain<A>,
        lwm: u64,
        ckpt_max: u64,
        passive: bool,
        min_reader_mark: WalPos,
        drop_current_if_in_btree: bool,
    ) -> usize {
        let before = versions.len();

        // Rule 1: aborted garbage
        versions.retain(|rv| !matches!((&rv.begin(), &rv.end()), (None, None)));

        let has_current = versions.iter().any(|rv| {
            matches!(rv.begin(), Some(TxTimestampOrID::Timestamp(_))) && rv.end().is_none()
        });

        // A version's current state is reclaimable iff it is materialized in the B-tree and no
        // active reader is pinned below that materialization frame.
        let materialized_for_readers = |rv: &RowVersion| {
            rv.materialized_at() != WalPos::ORIGIN && min_reader_mark >= rv.materialized_at()
        };

        // Rule 2: superseded version (end=Timestamp(e)) no reader can see (e <= lwm).
        versions.retain(|rv| match &rv.end() {
            Some(TxTimestampOrID::Timestamp(e)) if *e <= lwm => {
                if passive {
                    // Keep until this delete is in the B-tree and reachable by every reader.
                    !materialized_for_readers(rv)
                } else {
                    // Keep until the delete is checkpointed. Tombstones without a committed
                    // current successor must survive, as must versions already in the B-tree
                    // (btree_resident, or begin <= ckpt_max). Dropping the latter erases the
                    // only evidence that a later delete must be written (#7638).
                    let in_btree = rv.btree_resident
                        || matches!(&rv.begin(), Some(TxTimestampOrID::Timestamp(b)) if *b <= ckpt_max);
                    *e > ckpt_max && (in_btree || !has_current)
                }
            }
            _ => true,
        });

        // Rule 3: optionally drop the last current version when the B-tree already has it.
        if drop_current_if_in_btree && versions.len() == 1 {
            if let (Some(TxTimestampOrID::Timestamp(b)), None) =
                (&versions[0].begin(), &versions[0].end())
            {
                let removable = if passive {
                    materialized_for_readers(&versions[0]) && *b < lwm
                } else {
                    *b <= ckpt_max && *b < lwm
                };
                if removable {
                    versions.clear();
                }
            }
        }

        Self::shrink_version_chain_allocation(versions);

        before - versions.len()
    }

    /// Stamp each version this checkpoint materialized with the WAL `frame` it became durable at.
    /// A version's current state is in the B-tree iff its terminal event (delete `end`, else
    /// insert `begin`) committed at or before `snapshot_ts`. `gc_version_chain` then reclaims it
    /// only once every reader's mark reaches `frame`.
    fn stamp_chain_materialized(
        &self,
        versions: &mut [RowVersion],
        frame: WalPos,
        snapshot_ts: u64,
    ) {
        let resolve = |t: Option<TxTimestampOrID>| -> Option<u64> {
            match t {
                Some(TxTimestampOrID::Timestamp(ts)) => Some(ts),
                Some(TxTimestampOrID::TxID(id)) => {
                    match lookup_tx_state(&self.txs, &self.finalized_tx_states, id) {
                        Some(TransactionState::Committed(ts)) => Some(ts),
                        _ => None,
                    }
                }
                None => None,
            }
        };
        for v in versions.iter_mut() {
            let terminal = if v.end().is_some() {
                resolve(v.end())
            } else {
                resolve(v.begin())
            };
            if terminal.is_some_and(|t| t <= snapshot_ts) {
                v.set_materialized_at(frame);
            }
        }
    }

    /// Chains with capacity at or below this are never shrunk — the
    /// allocation is too small to be worth a realloc.
    const CHAIN_SHRINK_MIN_CAPACITY: usize = 16;

    /// Release excess version-chain capacity after GC trimmed the chain.
    /// `retain`/`clear` keep the Vec's allocation, so a one-off burst of
    /// versions (e.g. a hot row between checkpoints) would otherwise pin its
    /// peak allocation forever. Capacity drops to a quarter of its current
    /// value — deliberately not to fit — when the survivors occupy less than
    /// a quarter of it, so steady-state chains keep slack for new versions.
    fn shrink_version_chain_allocation(versions: &mut RowVersionChain<A>) {
        let capacity = versions.capacity();
        if capacity > Self::CHAIN_SHRINK_MIN_CAPACITY && versions.len() < capacity / 4 {
            versions.shrink_to(capacity / 4);
        }
    }

    // Extracts the begin timestamp from a transaction
    #[inline]
    fn resolve_begin_timestamp(&self, ts_or_id: &Option<TxTimestampOrID>) -> u64 {
        match ts_or_id {
            Some(TxTimestampOrID::Timestamp(ts)) => *ts,
            Some(TxTimestampOrID::TxID(tx_id)) => {
                self.txs
                    .get(tx_id)
                    .expect("transaction should exist in txs map")
                    .value()
                    .begin_ts
            }
            // This function is intended to be used in the ordering of row versions within the row version chain in `insert_version_raw`.
            //
            // The row version chain should be append-only (aside from garbage collection),
            // so the specific ordering handled by this function may not be critical. We might
            // be able to append directly to the row version chain in the future.
            //
            // The value 0 is used here to represent an infinite timestamp value. This is a deliberate
            // choice for a planned future bitpacking optimization, reserving 0 for this purpose,
            // while actual timestamps will start from 1.
            None => 0,
        }
    }

    /// Inserts a new row version into the database, while making sure that the row version
    /// is inserted in the correct order. Returns a reference to the modified version chain.
    fn insert_version(
        &self,
        id: RowID,
        row_version: RowVersion,
    ) -> Result<RowVersions<A>, TryReserveError> {
        // Retry if GC unlinked this slot while we waited for the write lock.
        loop {
            let row_versions = self.get_or_create_table_row_versions(id.clone())?;
            let mut versions = row_versions.write();
            if !self.table_versions_still_mapped(&id, &row_versions) {
                continue;
            }
            self.insert_version_raw(&mut versions, row_version)?;
            drop(versions);
            return Ok(row_versions);
        }
    }

    /// True if `arc` is still the mapped value for `id`.
    fn table_versions_still_mapped(&self, id: &RowID, arc: &RowVersions<A>) -> bool {
        self.rows
            .get(id)
            .is_some_and(|entry| Arc::ptr_eq(entry.value(), arc))
    }

    /// True if `arc` is still the mapped value for `key`.
    fn index_versions_still_mapped(
        &self,
        index: &IndexRowsMap<A>,
        key: &SortableIndexKey,
        arc: &RowVersions<A>,
    ) -> bool {
        index
            .get(key)
            .is_some_and(|entry| Arc::ptr_eq(entry.value(), arc))
    }

    #[turso_macros::allocation_site(crate::alloc::MvStoreAllocationSite::TableRowsEntry)]
    fn get_or_create_table_row_versions(
        &self,
        id: RowID,
    ) -> Result<RowVersions<A>, TryReserveError> {
        let alloc = self.alloc.clone();
        let versions = self.rows.try_get_or_insert_with(id, move || {
            Arc::new(RwLock::new(<RowVersionChain<A> as TursoVecInExt<
                RowVersion,
                A,
            >>::new_in(alloc)))
        })?;
        Ok(versions.value().clone())
    }

    /// Gets an existing Arc<SortableIndexKey> from the index if the key exists,
    /// otherwise creates a new Arc. This ensures we reuse Arc instances for the same key.
    fn get_or_create_index_key_arc(
        &self,
        index_id: MVTableId,
        key: Arc<SortableIndexKey>,
    ) -> Result<Arc<SortableIndexKey>> {
        let index = self.get_or_create_index_rows(index_id)?;
        let index = index.value();
        let existing = index.get(&*key).map(|entry| entry.key().clone());
        Ok(existing.unwrap_or(key))
    }

    /// Inserts (or appends to) the version chain for an index entry and returns
    /// the id and versions of the modified row.
    pub fn insert_index_version(
        &self,
        index_id: MVTableId,
        key: Arc<SortableIndexKey>,
        mut row_version: RowVersion,
    ) -> Result<(Arc<SortableIndexKey>, RowVersions<A>)> {
        // Publish the key-set mutation *before* the key becomes visible in the
        // map: a concurrent shadow finger that races with this insert may then
        // reset spuriously, but can never miss the new key (#7578).
        self.bump_index_rows_epoch();
        let index = self.get_or_create_index_rows(index_id)?;
        let index = index.value();
        // Same drain-retry as `insert_version`.
        loop {
            let entry = self.get_or_create_index_key_entry(index, key.clone())?;
            // SkipMap may keep our Arc (miss) or a pre-existing one (hit); return that
            // canonical Arc so savepoint tracking and the map stay in sync.
            let canonical_key = entry.key().clone();
            let row_versions = entry.value().clone();
            let mut versions = row_versions.write();
            if !self.index_versions_still_mapped(index, canonical_key.as_ref(), &row_versions) {
                continue;
            }
            row_version.row.id.row_id = RowKey::Record(canonical_key.clone());
            self.insert_version_raw(&mut versions, row_version)?;
            drop(versions);
            return Ok((canonical_key, row_versions));
        }
    }

    /// Current epoch of `index_rows` key-set mutations; see the field docs.
    pub(crate) fn index_rows_epoch(&self) -> u64 {
        self.index_rows_epoch.load(Ordering::SeqCst)
    }

    /// Key-set mutation of `index_rows` (insert or empty-slot remove); see field docs.
    pub(crate) fn bump_index_rows_epoch(&self) {
        self.index_rows_epoch.fetch_add(1, Ordering::SeqCst);
    }

    #[turso_macros::allocation_site(crate::alloc::MvStoreAllocationSite::IndexRowsEntry)]
    pub(crate) fn get_or_create_index_rows(
        &self,
        index_id: MVTableId,
    ) -> Result<IndexRowsEntry<'_, A>, TryReserveError> {
        let alloc = self.alloc.clone();
        let index = self
            .index_rows
            .try_get_or_insert_with(index_id, move || SkipMap::new_in(alloc))?;
        Ok(index)
    }

    #[turso_macros::allocation_site(crate::alloc::MvStoreAllocationSite::IndexKeyEntry)]
    fn get_or_create_index_key_entry<'a>(
        &self,
        index: &'a IndexRowsMap<A>,
        key: Arc<SortableIndexKey>,
    ) -> Result<IndexRowEntry<'a, A>, TryReserveError> {
        let alloc = self.alloc.clone();
        let entry = index.try_get_or_insert_with(key, move || {
            Arc::new(RwLock::new(<RowVersionChain<A> as TursoVecInExt<
                RowVersion,
                A,
            >>::new_in(alloc)))
        })?;
        Ok(entry)
    }

    /// Inserts a new row version into the internal data structure for versions,
    /// while making sure that the row version is inserted in the correct order.
    #[turso_macros::allocation_site(crate::alloc::MvStoreAllocationSite::RowVersionReserve)]
    pub fn insert_version_raw(
        &self,
        versions: &mut RowVersionChain<A>,
        row_version: RowVersion,
    ) -> Result<(), TryReserveError> {
        // NOTICE: this is an insert a'la insertion sort, with pessimistic linear complexity.
        // However, we expect the number of versions to be nearly sorted, so we deem it worthy
        // to search linearly for the insertion point instead of paying the price of using
        // another data structure, e.g. a BTreeSet. If it proves to be too quadratic empirically,
        // we can either switch to a tree-like structure, or at least use partition_point()
        // which performs a binary search for the insertion point.
        versions.try_reserve(1)?;
        let mut position = 0_usize;
        for (i, v) in versions.iter().enumerate().rev() {
            let existing_begin = self.resolve_begin_timestamp(&v.begin());
            let new_begin = self.resolve_begin_timestamp(&row_version.begin());
            if existing_begin <= new_begin {
                // Recovery can replay multiple operations for the same row from one transaction
                // (e.g. insert then delete), which share the same begin timestamp.
                // Keep only the latest version for that begin timestamp so visibility checks don't
                // surface a stale intermediate version.
                // Only collapse duplicate "begin" values when both are concrete begins.
                // `begin=None` is used for committed tombstones over B-tree-resident rows and
                // must never be conflated with a later statement's transient tombstone.
                if versions[i].row.id == row_version.row.id
                    && matches!(
                        (&versions[i].begin(), &row_version.begin()),
                        (
                            Some(TxTimestampOrID::Timestamp(existing)),
                            Some(TxTimestampOrID::Timestamp(new))
                        ) if existing == new
                    )
                {
                    versions[i] = row_version;
                    return Ok(());
                }
                position = i + 1;
                break;
            }
        }
        // Memory pre allocated already
        versions.insert(position, row_version);
        // A genuine insert (the collapse branch above `return`s without
        // reaching here). Track it for the GC trigger heuristic.
        self.live_version_count_approx
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn write_row_to_pager(
        &self,
        row: &Row,
        cursor: Arc<RwLock<BTreeCursor>>,
        requires_seek: bool,
    ) -> Result<StateMachine<WriteRowStateMachine>> {
        let state_machine: StateMachine<WriteRowStateMachine> =
            StateMachine::<WriteRowStateMachine>::new(WriteRowStateMachine::new(
                row.clone(),
                cursor,
                requires_seek,
            ));

        Ok(state_machine)
    }

    pub fn delete_row_from_pager(
        &self,
        rowid: RowID,
        cursor: Arc<RwLock<BTreeCursor>>,
    ) -> Result<StateMachine<DeleteRowStateMachine>> {
        let state_machine: StateMachine<DeleteRowStateMachine> =
            StateMachine::<DeleteRowStateMachine>::new(DeleteRowStateMachine::new(rowid, cursor));

        Ok(state_machine)
    }

    /// Clear every version-chain entry for `rowid`. Used by checkpoint-time
    /// compaction that deletes the corresponding B-tree row outside the
    /// normal MVCC delete path (e.g. `SeqCompactDriver` for sequence
    /// backing tables) so the two layers stay in sync — without this the
    /// version chain would keep `RowVersion { begin: Timestamp(T), end:
    /// None, btree_resident: true }` entries pointing at B-tree rows that
    /// no longer exist, until `drop_unused_row_versions` Rule 3 caught up.
    ///
    /// **Caller contract** — the caller must hold a guarantee that no
    /// concurrent reader can observe mid-purge state. Today the only
    /// caller is `SeqCompactDriver`, which runs inside the checkpoint
    /// while the `pager_commit_lock` is held; nextval allocators serialize
    /// through that same lock, so they cannot see the chain in a partially
    /// purged state. Callers without that guarantee must add proper
    /// tombstones via the normal write path instead.
    ///
    /// Clears the chain but keeps the empty SkipMap slot (write-set GC still looks it up).
    pub fn purge_row_versions_during_checkpoint(&self, rowid: RowID) {
        if let Some(entry) = self.rows.get(&rowid) {
            let mut versions = entry.value().write();
            self.dec_live_version_count_approx(versions.len());
            versions.clear();
            Self::shrink_version_chain_allocation(&mut versions);
        }
    }

    /// Passive sequence compaction: record end-stamped deletes instead of inline B-tree purge.
    pub fn seqcompact_commit_delete(&self, rowid: RowID, num_cols: usize, end_ts: u64) {
        loop {
            let Ok(row_versions) = self.get_or_create_table_row_versions(rowid.clone()) else {
                return;
            };
            let mut versions = row_versions.write();
            if !self.table_versions_still_mapped(&rowid, &row_versions) {
                continue;
            }
            // End-stamp the live committed version, if any — collection then
            // materializes the B-tree delete (begin <= durable_max => exists_in_db_file).
            if let Some(rv) = versions.iter_mut().find(|rv| {
                matches!(rv.begin(), Some(TxTimestampOrID::Timestamp(_))) && rv.end().is_none()
            }) {
                rv.set_end(Some(TxTimestampOrID::Timestamp(end_ts)));
                return;
            }
            // Already tombstoned / no live version: nothing to delete again.
            if versions.iter().any(|rv| rv.end().is_some()) {
                return;
            }
            // B-tree-only row: btree-resident tombstone so collection materializes the delete.
            let version_id = self.get_version_id();
            let row = Row::new_table_row_in(rowid, &[], num_cols, self.alloc.clone())
                .expect("empty tombstone row");
            let _ = self.insert_version_raw(
                &mut versions,
                RowVersion {
                    id: version_id,
                    begin: PackedTs::pack(None),
                    end: PackedTs::pack(Some(TxTimestampOrID::Timestamp(end_ts))),
                    row,
                    btree_resident: true,
                    materialized_at: WalPos::ORIGIN,
                },
            );
            return;
        }
    }

    pub fn get_last_table_rowid(
        &self,
        table_id: MVTableId,
        table_iterator: &mut Option<MvccIterator<'static, RowID, A>>,
        tx_id: TxID,
    ) -> Option<RowKey> {
        let tx = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx.value();
        let max_rowid = RowID {
            table_id,
            row_id: RowKey::Int(i64::MAX),
        };
        let range = create_seek_range(Bound::Included(max_rowid), IterationDirection::Backwards);
        let iter_box = Box::new(self.rows.range(range).rev());
        *table_iterator = Some(static_iterator_hack!(iter_box, RowID, A));
        let iter = table_iterator
            .as_mut()
            .expect("table_iterator was assigned above");
        loop {
            let entry = iter.next()?;
            // Rowid is not part of the table, therefore we already reached the end of the table.
            // NOTE: Shouldn't range already prevent this?
            tracing::trace!(
                "get_last_table_rowid: entry.key().table_id={}, table_id={}, row_id={}",
                entry.key().table_id,
                table_id,
                entry.key().row_id
            );
            if entry.key().table_id != table_id {
                tracing::trace!("get_last_table_rowid: reached end of table");
                return None;
            }
            if let Some(_visible_row) = self.find_last_visible_version(tx, &entry) {
                tracing::trace!(
                    "get_last_table_rowid: found visible row: {:?}",
                    _visible_row
                );
                // There is a visible version for this rowid, so we return it
                return Some(RowKey::Int(match &entry.key().row_id {
                    RowKey::Int(i) => *i,
                    _ => panic!("Expected RowKey::Int for table rowid"),
                }));
            }
        }
    }

    pub fn get_last_table_rowid_without_visibility_check(
        &self,
        table_id: MVTableId,
    ) -> Option<RowKey> {
        let max_rowid = RowID {
            table_id,
            row_id: RowKey::Int(i64::MAX),
        };
        let range = create_seek_range(Bound::Included(max_rowid), IterationDirection::Backwards);
        let mut range = self.rows.range(range).rev();
        let entry = range.next()?;
        if entry.key().table_id != table_id {
            return None;
        }
        Some(entry.key().row_id.clone())
    }

    pub fn get_last_index_rowid(
        &self,
        index_id: MVTableId,
        tx_id: TxID,
        index_iterator: &mut Option<MvccIterator<'static, Arc<SortableIndexKey>, A>>,
    ) -> Result<Option<RowKey>> {
        let index = self.get_or_create_index_rows(index_id)?;
        let index = index.value();
        let iter_box = Box::new(index.iter().rev());
        *index_iterator = Some(static_iterator_hack!(iter_box, Arc<SortableIndexKey>, A));
        let iter = index_iterator
            .as_mut()
            .expect("index_iterator was assigned above");
        let tx = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx.value();
        Ok(self
            .find_next_visible_index_row(tx, iter)
            .map(|row| row.row_id))
    }

    pub fn get_logical_log_file(&self) -> Arc<dyn File> {
        self.storage.get_logical_log_file()
    }

    pub fn logical_log_offset(&self) -> u64 {
        self.storage.logical_log_offset()
    }

    /// Replace the logical log with a fresh valid header after the database
    /// file was restored outside MVCC.
    ///
    /// The returned completion must finish before reopening/recovering MVCC
    /// state. Otherwise recovery could replay stale local logical-log frames on
    /// top of the restored database image.
    pub fn reset_logical_log_after_external_restore(&self) -> Result<Completion> {
        self.storage.reset_to_fresh_header()
    }

    /// Return the durable sync completion for the freshly reset logical log.
    ///
    /// This is separate from `reset_logical_log_after_external_restore` so
    /// callers can drive the reset completion cooperatively, then issue the
    /// ordered sync only after the header/truncate group has completed.
    pub fn sync_logical_log_after_external_restore(
        &self,
        connection: &Arc<Connection>,
    ) -> Result<Option<Completion>> {
        if connection.get_sync_mode() != SyncMode::Off {
            let pager = connection.pager.load().clone();
            return Ok(Some(self.storage.sync(pager.get_sync_type())?));
        }
        Ok(None)
    }

    /// Reconcile WAL state left by a prior crash or incomplete checkpoint.
    /// Classifies startup state by WAL frame count and logical-log header
    /// validity, then either completes the interrupted checkpoint
    /// (backfill WAL → DB, sync, truncate) or fails closed on corrupt /
    /// inconsistent artifacts. Non-blocking: all IO yields through the
    /// supplied [`CompleteCheckpointState`].
    /// See RECOVERY_SEMANTICS.md "Startup Case Classification" for the full case table.
    pub(crate) fn maybe_complete_interrupted_checkpoint_nonblock(
        &self,
        connection: &Arc<Connection>,
        st: &mut CompleteCheckpointState,
    ) -> IOResultOr<()> {
        let pager = connection.pager.load().clone();
        let Some(wal) = &pager.wal else {
            return Ok(IOResult::Done(()));
        };
        loop {
            match st {
                CompleteCheckpointState::Start => {
                    // The bootstrap connection may have acquired a WAL read lock during
                    // earlier bootstrap steps (e.g. schema parsing). Drop it so the
                    // TRUNCATE checkpoint below isn't blocked by our own read lock.
                    // Idempotent across re-entry (holds_read_lock is checked first).
                    if wal.holds_read_lock() {
                        wal.end_read_tx();
                    }
                    let file = self.get_logical_log_file();
                    // Header is never encrypted; no EncryptionContext needed.
                    *st = CompleteCheckpointState::ReadingHeader {
                        reader: Box::new(StreamingLogicalLogReader::new(file, None)),
                    };
                }
                CompleteCheckpointState::ReadingHeader { reader } => {
                    let header_result = return_if_io!(reader.try_read_header_nonblock());
                    let wal_max_frame = wal.get_max_frame_in_wal();
                    let is_readonly = connection.db.is_readonly();
                    if wal_max_frame == 0 {
                        if is_readonly {
                            // Nothing to reconcile in read-only mode with no committed frames.
                            *st = CompleteCheckpointState::Start;
                            return Ok(IOResult::Done(()));
                        }
                        *st = CompleteCheckpointState::DriveEarlyTruncate {
                            header_result,
                            checkpoint_result: CheckpointResult::new(0, 0, 0),
                        };
                        continue;
                    }
                    if is_readonly {
                        return Err(LimboError::Corrupt(
                            "Cannot reconcile interrupted MVCC checkpoint in read-only mode"
                                .to_string(),
                        )
                        .into());
                    }
                    match header_result {
                        HeaderReadResult::Valid(header) => {
                            // Interrupted checkpoint with the logical log still
                            // present: reuse its header so the fresh-header write in
                            // RetryHeader keeps the existing salt chain.
                            self.storage.set_header(header);
                        }
                        HeaderReadResult::NoLog => {
                            if !connection.experimental_mvcc_passive_checkpoint_enabled() {
                                return Err(LimboError::Corrupt(
                                    "WAL has committed frames but logical log header is missing"
                                        .to_string(),
                                )
                                .into());
                            }
                        }
                        HeaderReadResult::Invalid => {
                            // Present but undecodable: a torn header write / genuine
                            // corruption, not a clean truncation — fail closed.
                            return Err(LimboError::Corrupt(
                                "WAL has committed frames but logical log header is invalid"
                                    .to_string(),
                            )
                            .into());
                        }
                    }
                    // Enter the checkpoint lifecycle before any WAL→DB backfill so
                    // that `DurableStorage` implementations observing the
                    // start/end pairing (e.g. the diskless server, which arms its
                    // next-generation metadata here) see a checkpoint in progress
                    // when `wal.checkpoint` writes pages into the DB file. Called
                    // exactly once at this transition (never on `DriveCheckpoint`
                    // re-entry) since `on_checkpoint_start` is not idempotent.
                    self.storage.on_checkpoint_start()?;
                    *st = CompleteCheckpointState::DriveCheckpoint;
                }
                CompleteCheckpointState::DriveEarlyTruncate {
                    header_result,
                    checkpoint_result,
                } => {
                    return_if_io!(wal.truncate_wal(checkpoint_result, pager.get_sync_type()));
                    if let HeaderReadResult::Valid(header) = header_result {
                        self.storage.set_header(header.clone());
                    }
                    *st = CompleteCheckpointState::Start;
                    return Ok(IOResult::Done(()));
                }
                CompleteCheckpointState::DriveCheckpoint => {
                    // NOTE: uses `CheckpointMode::Truncate` to drive WAL backfill
                    // only; we still truncate the WAL explicitly below to preserve
                    // WAL-last ordering in recovery.
                    let checkpoint_result = return_if_io!(wal.checkpoint(
                        &pager,
                        CheckpointMode::Truncate {
                            upper_bound_inclusive: None,
                        },
                        connection.get_sync_mode(),
                    ));
                    if !checkpoint_result.everything_backfilled() {
                        let err = LimboError::Corrupt(
                            "Unable to fully backfill committed WAL frames during MVCC recovery"
                                .to_string(),
                        );
                        // Close out the lifecycle opened in `ReadingHeader` so the
                        // start/end pairing stays balanced on this error path.
                        self.storage.on_checkpoint_end(Err(err.clone()))?;
                        return Err(err.into());
                    }
                    let need_db_sync = connection.get_sync_mode() != SyncMode::Off
                        && checkpoint_result.wal_checkpoint_backfilled > 0;
                    if need_db_sync {
                        let c = match pager
                            .db_file
                            .sync(Completion::new_sync(|_| {}), pager.get_sync_type())
                        {
                            Ok(c) => c,
                            Err(err) => {
                                self.storage.on_checkpoint_end(Err(err.clone()))?;
                                return Err(err.into());
                            }
                        };
                        *st = CompleteCheckpointState::AwaitDbFileSync {
                            completion: c,
                            checkpoint_result,
                        };
                    } else {
                        *st = CompleteCheckpointState::RetryHeader {
                            checkpoint_result,
                            retried_crc: false,
                            phase: RetryHeaderPhase::NeedUpdateHeader,
                        };
                    }
                }
                CompleteCheckpointState::AwaitDbFileSync {
                    completion,
                    checkpoint_result,
                } => {
                    if !completion.succeeded() {
                        let c = completion.clone();
                        io_yield_one!(c);
                    }
                    let checkpoint_result =
                        std::mem::replace(checkpoint_result, CheckpointResult::new(0, 0, 0));
                    *st = CompleteCheckpointState::RetryHeader {
                        checkpoint_result,
                        retried_crc: false,
                        phase: RetryHeaderPhase::NeedUpdateHeader,
                    };
                }
                CompleteCheckpointState::RetryHeader {
                    checkpoint_result,
                    retried_crc,
                    phase,
                } => match phase {
                    RetryHeaderPhase::NeedUpdateHeader => {
                        let c = match self.storage.update_header() {
                            Ok(c) => c,
                            Err(err) => {
                                self.storage.on_checkpoint_end(Err(err.clone()))?;
                                return Err(err.into());
                            }
                        };
                        *phase = RetryHeaderPhase::AwaitUpdateHeader(c);
                    }
                    RetryHeaderPhase::AwaitUpdateHeader(completion) => {
                        if !completion.succeeded() {
                            let c = completion.clone();
                            io_yield_one!(c);
                        }
                        if connection.get_sync_mode() != SyncMode::Off {
                            let c = match self.storage.sync(pager.get_sync_type()) {
                                Ok(c) => c,
                                Err(err) => {
                                    self.storage.on_checkpoint_end(Err(err.clone()))?;
                                    return Err(err.into());
                                }
                            };
                            *phase = RetryHeaderPhase::AwaitLogSync(c);
                        } else {
                            let file = self.get_logical_log_file();
                            *phase = RetryHeaderPhase::AwaitCrcCheck {
                                reader: Box::new(StreamingLogicalLogReader::new(file, None)),
                            };
                        }
                    }
                    RetryHeaderPhase::AwaitLogSync(completion) => {
                        if !completion.succeeded() {
                            let c = completion.clone();
                            io_yield_one!(c);
                        }
                        let file = self.get_logical_log_file();
                        *phase = RetryHeaderPhase::AwaitCrcCheck {
                            reader: Box::new(StreamingLogicalLogReader::new(file, None)),
                        };
                    }
                    RetryHeaderPhase::AwaitCrcCheck { reader } => {
                        let header_result = return_if_io!(reader.try_read_header_nonblock());
                        let crc_valid = matches!(header_result, HeaderReadResult::Valid(_));
                        if crc_valid {
                            let checkpoint_result = std::mem::replace(
                                checkpoint_result,
                                CheckpointResult::new(0, 0, 0),
                            );
                            *st = CompleteCheckpointState::DriveFinalTruncate { checkpoint_result };
                        } else {
                            if *retried_crc {
                                let err = LimboError::Corrupt(
                                    "Logical log header CRC mismatch after retry".to_string(),
                                );
                                self.storage.on_checkpoint_end(Err(err.clone()))?;
                                return Err(err.into());
                            }
                            *retried_crc = true;
                            *phase = RetryHeaderPhase::NeedUpdateHeader;
                        }
                    }
                },
                CompleteCheckpointState::DriveFinalTruncate { checkpoint_result } => {
                    match wal.truncate_wal(checkpoint_result, pager.get_sync_type()) {
                        Ok(IOResult::Done(())) => {
                            self.storage.on_checkpoint_end(Ok(checkpoint_result))?;
                        }
                        Ok(IOResult::IO(c)) => {
                            return Ok(IOResult::IO(c));
                        }
                        Err(err) => {
                            self.storage.on_checkpoint_end(Err((*err).clone()))?;
                            return Err(err);
                        }
                    }
                    *st = CompleteCheckpointState::Start;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    /// Rewrite placeholder (negative) root pages in `schema` to the real positive roots a
    /// checkpoint has since materialized. A negative root whose `table_id_to_rootpage` entry
    /// resolves to a positive page means the object's btree is durable; pointing the schema at
    /// it keeps consumers that ignore negative roots (e.g. `integrity_check`, which skips them)
    /// from treating the live page as orphaned. Objects not yet materialized stay negative.
    pub(crate) fn resolve_schema_negative_roots(&self, schema: &mut crate::schema::Schema) {
        let resolve = |root_page: i64| -> Option<i64> {
            if root_page >= 0 {
                return None;
            }
            self.table_id_to_rootpage
                .get(&MVTableId::from(root_page))
                .filter(|e| e.value().is_live())
                .and_then(|e| e.value().root_page)
                .map(|r| r as i64)
        };
        for table in schema.tables.values_mut() {
            let needs = table
                .btree()
                .is_some_and(|b| resolve(b.root_page).is_some());
            if !needs {
                continue;
            }
            let table = Arc::make_mut(table);
            if let Some(btree_table) = table.btree_mut() {
                let btree_table = Arc::make_mut(btree_table);
                if let Some(rp) = resolve(btree_table.root_page) {
                    btree_table.root_page = rp;
                }
            }
        }
        for table_index_list in schema.indexes.values_mut() {
            for index in table_index_list.iter_mut() {
                if let Some(rp) = resolve(index.root_page) {
                    Arc::make_mut(index).root_page = rp;
                }
            }
        }
    }

    /// Resolve a single (possibly placeholder/negative) root page to the real positive page a
    /// checkpoint has materialized for it, or return it unchanged if not yet materialized.
    pub(crate) fn resolve_root_page(&self, root_page: i64) -> i64 {
        if root_page >= 0 {
            return root_page;
        }
        self.table_id_to_rootpage
            .get(&MVTableId::from(root_page))
            .filter(|e| e.value().is_live())
            .and_then(|e| e.value().root_page)
            .map(|r| r as i64)
            .unwrap_or(root_page)
    }

    /// Build an `Arc<Schema>` from a set of sqlite_schema rows (keyed by rowid).
    /// Shared by recovery (replayed rows) and the checkpoint's snapshot-consistent
    /// `BuildLocalSchemaView` (btree + MVCC-delta merge at snapshot_ts).
    pub(crate) fn build_schema_from_rows(
        &self,
        connection: &Arc<Connection>,
        schema_rows: &HashMap<i64, ImmutableRecordRef<'static>>,
        preserved_table_valued_functions: &[Arc<crate::vtab::VirtualTable>],
    ) -> Result<Arc<Schema>> {
        let pager = connection.pager.load().clone();
        let cookie = self
            .global_header
            .read()
            .as_ref()
            .map(|header| header.schema_cookie.get())
            .unwrap_or(
                pager
                    .io
                    .block(|| pager.with_header(|header| header.schema_cookie))?
                    .get(),
            );
        let mut fresh = Schema::with_options(
            connection.db.experimental_custom_types_enabled(),
            connection.db.dialect().as_ref(),
        )?;
        fresh.generated_columns_enabled = connection.db.experimental_generated_columns_enabled();
        fresh.schema_version = cookie;
        let mut from_sql_indexes = crate::alloc::vec![];
        let mut automatic_indices = HashMap::default();
        let mut dbsp_state_roots: HashMap<String, i64> = HashMap::default();
        let mut dbsp_state_index_roots: HashMap<String, i64> = HashMap::default();
        let mut materialized_view_info: HashMap<String, (String, i64)> = HashMap::default();
        let syms = connection.syms.read();
        let mv_store = connection.db.get_mv_store().clone();

        let mut sorted_rowids: Vec<i64> = schema_rows.keys().copied().collect();
        sorted_rowids.sort_unstable();
        for rowid in &sorted_rowids {
            let record = &schema_rows[rowid];
            let ty = match record.get_value_opt(0) {
                Some(ValueRef::Text(v)) => v.as_str(),
                _ => {
                    return Err(LimboError::Corrupt(
                        "sqlite_schema type must be text".to_string(),
                    ));
                }
            };
            let name = match record.get_value_opt(1) {
                Some(ValueRef::Text(v)) => v.as_str(),
                _ => {
                    return Err(LimboError::Corrupt(
                        "sqlite_schema name must be text".to_string(),
                    ));
                }
            };
            let table_name = match record.get_value_opt(2) {
                Some(ValueRef::Text(v)) => v.as_str(),
                _ => {
                    return Err(LimboError::Corrupt(
                        "sqlite_schema tbl_name must be text".to_string(),
                    ));
                }
            };
            let root_page = match record.get_value_opt(3) {
                Some(ValueRef::Numeric(Numeric::Integer(v))) => v,
                _ => {
                    return Err(LimboError::Corrupt(
                        "sqlite_schema root_page must be integer".to_string(),
                    ));
                }
            };
            // A negative root is a not-yet-materialized placeholder. If a (passive) checkpoint
            // has since materialized this object, resolve to its real positive root so the
            // rebuilt schema reflects the btree page — otherwise integrity_check skips the
            // negative root and reports its materialized page as orphaned ("never used").
            let root_page = if root_page < 0 {
                self.table_id_to_rootpage
                    .get(&MVTableId::from(root_page))
                    .filter(|e| e.value().is_live())
                    .and_then(|e| e.value().root_page)
                    .map(|r| r as i64)
                    .unwrap_or(root_page)
            } else {
                root_page
            };
            let sql = match record.get_value_opt(4) {
                Some(ValueRef::Text(v)) => Some(v.as_str()),
                _ => None,
            };
            let attached_resolver = |alias: &str| -> Option<usize> {
                connection
                    .attached_databases()
                    .read()
                    .get_database_by_name(&crate::util::normalize_ident(alias))
                    .map(|(idx, _)| idx)
            };
            fresh.handle_schema_row(
                ty,
                name,
                table_name,
                root_page,
                sql,
                &syms,
                &mut from_sql_indexes,
                &mut automatic_indices,
                &mut dbsp_state_roots,
                &mut dbsp_state_index_roots,
                &mut materialized_view_info,
                &attached_resolver,
                connection.dialect().as_ref(),
            )?;
        }
        fresh.populate_indices(
            &syms,
            from_sql_indexes,
            automatic_indices,
            mv_store.is_some(),
        )?;
        fresh.populate_materialized_views(
            materialized_view_info,
            dbsp_state_roots,
            dbsp_state_index_roots,
        )?;
        Self::rehydrate_table_valued_functions(&mut fresh, preserved_table_valued_functions);

        Ok(Arc::new(fresh))
    }

    /// Replays committed logical-log frames into the in-memory MVCC store.
    /// Only frames with `commit_ts > persistent_tx_ts_max` (the durable replay
    /// boundary from the metadata table) are applied; earlier frames were
    /// already checkpointed. On success, reseeds the MVCC clock and sets the log
    /// writer offset so torn-tail bytes are overwritten. Returns true if any
    /// frames were replayed.
    pub fn maybe_recover_logical_log(
        &self,
        connection: &Arc<Connection>,
        st: &mut RecoverLogicalLogState,
    ) -> IOResultOr<bool> {
        loop {
            match st {
                RecoverLogicalLogState::Start => {
                    let file = self.get_logical_log_file();
                    let enc_ctx = self.storage.encryption_ctx();
                    let reader = Box::new(StreamingLogicalLogReader::new(file, enc_ctx));
                    let preserved_tvfs =
                        Self::capture_table_valued_functions(&connection.schema.read());
                    *st = RecoverLogicalLogState::ReadHeader {
                        reader,
                        preserved_tvfs,
                    };
                }
                RecoverLogicalLogState::ReadHeader { reader, .. } => {
                    let header = match return_if_io!(reader.try_read_header_nonblock()) {
                        HeaderReadResult::Valid(header) => Some(header),
                        HeaderReadResult::NoLog => None,
                        HeaderReadResult::Invalid => {
                            return Err(LimboError::Corrupt(
                                "Logical log header corrupt and no WAL recovery available"
                                    .to_string(),
                            )
                            .into());
                        }
                    };
                    if let Some(header) = &header {
                        self.storage.set_header(header.clone());
                    }
                    let header_present = header.is_some();
                    let RecoverLogicalLogState::ReadHeader {
                        reader,
                        preserved_tvfs,
                    } = std::mem::take(st)
                    else {
                        unreachable!("state is ReadHeader");
                    };
                    *st = RecoverLogicalLogState::ReadTxTs {
                        reader,
                        preserved_tvfs,
                        header_present,
                        txts_st: ReadPersistentTxTsMaxState::default(),
                    };
                }
                RecoverLogicalLogState::ReadTxTs {
                    header_present,
                    txts_st,
                    ..
                } => {
                    let header_present = *header_present;
                    let persistent_tx_ts_max = if self.uses_durable_mvcc_metadata(connection) {
                        match return_if_io!(
                            self.try_read_persistent_tx_ts_max_nonblock(connection, txts_st)
                        ) {
                            Some(ts) => ts,
                            None if !header_present => 0,
                            None => {
                                return Err(LimboError::Corrupt(
                                    "Missing MVCC metadata table".to_string(),
                                )
                                .into());
                            }
                        }
                    } else {
                        0
                    };
                    self.durable_txid_max
                        .store(persistent_tx_ts_max, Ordering::SeqCst);
                    self.clock.reset(persistent_tx_ts_max + 1);

                    if !header_present || self.get_logical_log_file().size()? <= LOG_HDR_SIZE as u64
                    {
                        *st = RecoverLogicalLogState::Done;
                        return Ok(IOResult::Done(false));
                    }
                    let RecoverLogicalLogState::ReadTxTs {
                        reader,
                        preserved_tvfs,
                        ..
                    } = std::mem::take(st)
                    else {
                        unreachable!("state is ReadTxTs");
                    };
                    *st = RecoverLogicalLogState::ReadCookie {
                        reader,
                        preserved_tvfs,
                        persistent_tx_ts_max,
                    };
                }
                RecoverLogicalLogState::ReadCookie { .. } => {
                    let fallback_cookie = if self.global_header.read().is_some() {
                        0
                    } else {
                        let pager = connection.pager.load().clone();
                        return_if_io!(pager.with_header(|header| header.schema_cookie)).get()
                    };
                    let RecoverLogicalLogState::ReadCookie {
                        reader,
                        preserved_tvfs,
                        persistent_tx_ts_max,
                    } = std::mem::take(st)
                    else {
                        unreachable!("state is ReadCookie");
                    };
                    let stmt = connection
                        .query(
                            "SELECT rowid, type, name, tbl_name, rootpage, sql FROM sqlite_schema",
                        )?
                        .map(Box::new);
                    *st = RecoverLogicalLogState::QuerySchema {
                        reader,
                        preserved_tvfs,
                        persistent_tx_ts_max,
                        cookie: fallback_cookie,
                        stmt,
                        schema_rows: HashMap::default(),
                    };
                }
                RecoverLogicalLogState::QuerySchema {
                    stmt, schema_rows, ..
                } => {
                    if let Some(stmt) = stmt {
                        return_if_io!(stmt.run_with_row_callback_nonblock(|row| {
                            let rowid = row.get::<i64>(0)?;
                            let values = (1..=5)
                                .map(|i| row.get_value(i).clone())
                                .collect::<Vec<_>>();
                            schema_rows.insert(
                                rowid,
                                ImmutableRecord::from_values(&values, values.len())?,
                            );
                            Ok(())
                        }));
                    }
                    let RecoverLogicalLogState::QuerySchema {
                        reader,
                        preserved_tvfs,
                        persistent_tx_ts_max,
                        cookie,
                        schema_rows,
                        ..
                    } = std::mem::take(st)
                    else {
                        unreachable!("state is QuerySchema");
                    };
                    let current_schema = connection.schema.read().clone();
                    *st = RecoverLogicalLogState::Replay {
                        ctx: Box::new(RecoverCtx {
                            reader,
                            preserved_table_valued_functions: preserved_tvfs,
                            cookie,
                            persistent_tx_ts_max,
                            replay_cutoff_ts: persistent_tx_ts_max,
                            max_commit_ts_seen: persistent_tx_ts_max,
                            schema_rows,
                            dropped_root_pages: HashSet::default(),
                            current_schema,
                            index_infos: HashMap::default(),
                        }),
                    };
                }
                RecoverLogicalLogState::Replay { ctx } => {
                    turso_assert_reachable!("MVCC recovery (replaying a frame)");
                    loop {
                        let Some(frame) = return_if_io!(ctx.reader.next_frame()) else {
                            let recovered_offset = ctx.reader.last_valid_offset() as u64;
                            let recovered_running_crc = ctx.reader.running_crc();
                            self.storage.restore_logical_log_state_after_recovery(
                                recovered_offset,
                                recovered_running_crc,
                            );
                            break;
                        };
                        self.recover_process_frame(connection, ctx, frame)?;
                    }
                    let max_commit_ts_seen = ctx.max_commit_ts_seen;
                    let persistent_tx_ts_max = ctx.persistent_tx_ts_max;
                    let dropped_root_pages = std::mem::take(&mut ctx.dropped_root_pages);
                    assert!(
                        max_commit_ts_seen >= persistent_tx_ts_max,
                        "replay clock would rewind below metadata boundary: max_commit_ts_seen={max_commit_ts_seen} persistent_tx_ts_max={persistent_tx_ts_max}"
                    );
                    connection.with_schema_mut(|schema| {
                        schema.dropped_root_pages = dropped_root_pages;
                    })?;
                    if let Some(header) = self.global_header.read().as_ref() {
                        connection.with_schema_mut(|schema| {
                            schema.schema_version = header.schema_cookie.get();
                        })?;
                    }
                    *connection.db.schema.lock() = connection.schema.read().clone();
                    self.clock.reset(max_commit_ts_seen + 1);
                    self.last_committed_tx_ts
                        .store(max_commit_ts_seen, Ordering::SeqCst);
                    *st = RecoverLogicalLogState::Done;
                    return Ok(IOResult::Done(true));
                }
                RecoverLogicalLogState::Done => {
                    return Ok(IOResult::Done(false));
                }
            }
        }
    }

    /// Replay a single committed logical-log transaction frame into the MVCC
    /// store. Fully synchronous (the only recovery IO is reading the next frame,
    /// driven by the caller); operates on accumulators borrowed from `ctx`.
    ///
    /// A same-transaction create-then-drop leaves a delete of a sqlite_schema rowid absent from
    /// the merged state; replay treats that as a legal no-op, not corruption.
    #[aristo::intent(
        "Replaying a committed logical-log transaction never aborts with a corruption error",
        id = "mvcc_recovery_total_on_committed_log",
        verify = "full"
    )]
    fn recover_process_frame(
        &self,
        connection: &Arc<Connection>,
        ctx: &mut RecoverCtx,
        frame: Vec<ParsedOp>,
    ) -> Result<()> {
        let mut max_commit_ts_seen = ctx.max_commit_ts_seen;
        let replay_cutoff_ts = ctx.replay_cutoff_ts;
        let cookie = ctx.cookie;
        let mut schema_rows = std::mem::take(&mut ctx.schema_rows);
        let mut dropped_root_pages = std::mem::take(&mut ctx.dropped_root_pages);
        let mut current_schema = ctx.current_schema.clone();
        let mut index_infos = std::mem::take(&mut ctx.index_infos);

        let install_schema = |schema: Arc<Schema>| {
            *connection.schema.write() = schema.clone();
            *connection.db.schema.lock() = schema;
        };

        let root_page_for_index = |index_id: MVTableId| -> i64 {
            self.current_root_page(&index_id)
                .map(|value| value as i64)
                .unwrap_or_else(|| i64::from(index_id))
        };

        let find_index_info =
            |schema: &Schema, root_page: i64| -> Result<Option<Arc<IndexInfo>>, TryReserveError> {
                schema
                    .indexes
                    .values()
                    .flatten()
                    .find(|idx| idx.root_page == root_page)
                    .map(|idx| {
                        IndexInfo::new_from_index_in(idx.as_ref(), self.alloc.clone()).map(Arc::new)
                    })
                    .transpose()
            };

        let schema_has_index_root = |schema: &Schema, root_page: i64| -> bool {
            schema
                .indexes
                .values()
                .flatten()
                .any(|idx| idx.root_page == root_page)
        };

        let parsed_op_commit_ts = |op: &ParsedOp| match op {
            ParsedOp::UpsertTable { commit_ts, .. }
            | ParsedOp::DeleteTable { commit_ts, .. }
            | ParsedOp::UpsertIndex { commit_ts, .. }
            | ParsedOp::DeleteIndex { commit_ts, .. }
            | ParsedOp::UpdateHeader { commit_ts, .. } => *commit_ts,
        };
        'frame: {
            let frame_commit_ts = parsed_op_commit_ts(
                frame
                    .first()
                    .expect("next_frame should not return an empty frame"),
            );
            max_commit_ts_seen = max_commit_ts_seen.max(frame_commit_ts);
            if frame_commit_ts <= replay_cutoff_ts {
                break 'frame;
            }

            // Work out what sqlite_schema will look like after this transaction,
            // before applying any row/index changes from the transaction.
            //
            // Why this is necessary:
            // - CREATE INDEX can log index-entry inserts in the same frame as the
            //   sqlite_schema insert for the new index. Those index-entry inserts
            //   need the post-frame schema.
            // - DROP INDEX can log DELETE_INDEX entries for b-tree index entries
            //   that existed before the transaction. Those deletes still need the
            //   pre-frame schema, because the final schema no longer has idx.
            // - ALTER TABLE can delete and reinsert the same table's sqlite_schema
            //   row while also logging table/index DML. Installing the schema after
            //   only the delete would create an impossible in-between schema:
            //   sqlite_schema may still contain idx while the table row for t is
            //   temporarily absent.
            //
            // So recovery stages sqlite_schema into `schema_rows_after`, builds a
            // post-frame Schema from that staged map, and keeps the installed
            // `current_schema` unchanged until every op in the frame has replayed.
            // Most transaction frames do not change sqlite_schema, so clone the
            // schema row map only if this frame actually writes sqlite_schema.
            let mut schema_rows_after: Option<HashMap<i64, ImmutableRecord>> = None;
            for parsed_op in &frame {
                match parsed_op {
                    ParsedOp::UpsertTable {
                        rowid,
                        record_bytes,
                        ..
                    } if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID => {
                        let schema_rows_after =
                            schema_rows_after.get_or_insert_with(|| schema_rows.clone());
                        let record = ImmutableRecordRef::from_bin_record(record_bytes);
                        let column_count = record.column_count();
                        if column_count < 5 {
                            return Err(LimboError::Corrupt(format!(
                                "sqlite_schema row must have at least 5 columns, got {column_count}",
                            )));
                        }
                        crate::with_mv_store_allocation_site!(SchemaRowPayload, {
                            schema_rows_after.insert(
                                rowid.row_id.to_int_or_panic(),
                                ImmutableRecord::from_bin_record(
                                    crate::types::value_blob_from_slice(record_bytes)?,
                                ),
                            );
                        });
                    }
                    ParsedOp::DeleteTable { rowid, .. }
                        if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID =>
                    {
                        let schema_rows_after =
                            schema_rows_after.get_or_insert_with(|| schema_rows.clone());
                        schema_rows_after.remove(&rowid.row_id.to_int_or_panic());
                    }
                    _ => {}
                }
            }
            // schema_rows_after is Some if the frame changes the schema
            let schema_rows_after = schema_rows_after;

            let schema_after = match schema_rows_after.as_ref() {
                Some(schema_rows_after) => Some(self.recover_build_schema(
                    connection,
                    schema_rows_after,
                    cookie,
                    &ctx.preserved_table_valued_functions,
                )?),
                None => None,
            };

            if schema_rows_after.is_some() {
                // Cached IndexInfo values are tied to a specific schema. Clear
                // before decoding a schema-changing frame so this frame chooses
                // from its own before/after schema pair.
                index_infos.clear();
            }

            {
                let should_skip_index_op = |parsed_op: &ParsedOp| -> bool {
                    let Some(schema_after) = schema_after.as_ref() else {
                        return false;
                    };

                    match parsed_op {
                        ParsedOp::UpsertIndex { table_id, .. } => {
                            let root_page = root_page_for_index(*table_id);
                            !schema_has_index_root(schema_after, root_page)
                        }
                        ParsedOp::DeleteIndex { table_id, .. } => {
                            let root_page = root_page_for_index(*table_id);
                            !schema_has_index_root(&current_schema, root_page)
                        }
                        _ => false,
                    }
                };

                let mut get_index_info = |index_id: MVTableId,
                                          op_kind: IndexOpKind|
                 -> Result<Arc<IndexInfo>> {
                    if let Some(index_info) = index_infos.get(&(index_id, op_kind)) {
                        return Ok(index_info.clone());
                    }

                    let root_page = root_page_for_index(index_id);
                    let before = find_index_info(&current_schema, root_page)?;
                    let after = schema_after
                        .as_ref()
                        .map(|schema| find_index_info(schema, root_page))
                        .transpose()?
                        .flatten();

                    // The logical log tells us whether an index entry is being
                    // inserted or deleted, but it stores only encoded key bytes
                    // plus the index root page. The recovery loop below skips
                    // index ops that cannot affect the final state of this
                    // frame before those bytes are decoded. For the remaining
                    // ops, pick the schema view that owns the entry at the
                    // frame boundary:
                    //
                    // - UPSERT_INDEX writes an entry that survives after the
                    //   transaction. In a schema-changing frame, the index must
                    //   exist in the post-frame schema.
                    // - DELETE_INDEX removes an entry that existed before the
                    //   transaction. In a schema-changing frame, the index must
                    //   exist in the pre-frame schema.
                    // - If the frame does not change schema, `current_schema` is
                    //   both the before and after schema.
                    let index_info = match op_kind {
                        IndexOpKind::Upsert if schema_after.is_some() => after,
                        IndexOpKind::Delete if schema_after.is_some() => before,
                        IndexOpKind::Upsert | IndexOpKind::Delete => before,
                    }
                    .ok_or_else(|| {
                        let expected_schema = match op_kind {
                            IndexOpKind::Upsert if schema_after.is_some() => "post-frame",
                            IndexOpKind::Delete if schema_after.is_some() => "pre-frame",
                            IndexOpKind::Upsert | IndexOpKind::Delete => "current",
                        };
                        LimboError::InternalError(format!(
                            "Index with root page {root_page} not found in {expected_schema} schema while recovering logical log",
                        ))
                    })?;
                    index_infos.insert((index_id, op_kind), index_info.clone());
                    Ok(index_info)
                };

                for parsed_op in frame {
                    // Some index writes are real while the transaction is
                    // running, but have no meaning at either durable boundary.
                    //
                    // Example: UPDATE a row so it writes a new entry into
                    // idx_old, then DROP INDEX idx_old before COMMIT. The
                    // UPSERT_INDEX for idx_old is not part of the database
                    // after the transaction, and the post-frame schema no
                    // longer has CREATE INDEX text for idx_old. Decoding it
                    // against the pre-frame schema would preserve an index
                    // entry for an index that was dropped. Decoding it against
                    // the post-frame schema is impossible. The correct action
                    // is to skip it.
                    //
                    // The opposite case is a DELETE_INDEX for an index created
                    // earlier in the same frame. There was no pre-frame index
                    // entry in the database file, so that delete also has no
                    // durable work to do.
                    if should_skip_index_op(&parsed_op) {
                        continue;
                    }

                    let next_rec = ctx.reader.parsed_op_to_streaming_in(
                        parsed_op,
                        &mut get_index_info,
                        self.alloc.clone(),
                    )?;

                    tracing::trace!("next_rec {next_rec:?}");

                    match next_rec {
                        StreamingResult::UpsertTableRow {
                            row,
                            rowid,
                            commit_ts,
                            btree_resident,
                        } => {
                            max_commit_ts_seen = max_commit_ts_seen.max(commit_ts);
                            if commit_ts <= replay_cutoff_ts {
                                continue;
                            }
                            let is_schema_row = rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID;
                            if is_schema_row {
                                let record = ImmutableRecordRef::from_bin_record(row.payload());
                                let column_count = record.column_count();
                                if column_count < 5 {
                                    return Err(LimboError::Corrupt(format!(
                                        "sqlite_schema row must have at least 5 columns, got {column_count}",
                                    )));
                                }
                                let Some(ValueRef::Text(row_type)) = record.get_value_opt(0) else {
                                    return Err(LimboError::Corrupt(
                                        "sqlite_schema type must be text".to_string(),
                                    ));
                                };
                                let row_type = row_type.as_str();
                                let val = match record.get_value_opt(3) {
                                    Some(v) => v,
                                    None => {
                                        return Err(LimboError::InternalError(
                                            "Expected at least 5 columns in sqlite_schema"
                                                .to_string(),
                                        ));
                                    }
                                };
                                let ValueRef::Numeric(crate::numeric::Numeric::Integer(root_page)) =
                                    val
                                else {
                                    panic!("Expected integer value for root page, got {val:?}");
                                };
                                let sql = match record.get_value_opt(4) {
                                    Some(ValueRef::Text(v)) => Some(v.as_str()),
                                    _ => None,
                                };
                                let has_btree = match row_type {
                                    "index" => root_page != 0,
                                    "table" => {
                                        !sql.is_some_and(crate::util::sql_is_create_virtual_table)
                                    }
                                    _ => false,
                                };
                                if has_btree {
                                    if root_page == 0 {
                                        return Err(LimboError::Corrupt(format!(
                                            "sqlite_schema root_page=0 for btree {row_type}"
                                        )));
                                    }
                                    if root_page < 0 {
                                        let table_id = self.get_table_id_from_root_page(root_page);
                                        if let Some(entry) =
                                            self.table_id_to_rootpage.get(&table_id)
                                        {
                                            if let Some(value) = entry.value().root_page {
                                                panic!(
                                                    "Logical log contains an insertion of a sqlite_schema record that has both a negative root page and a positive root page: {root_page} & {value}"
                                                );
                                            }
                                        }
                                        self.insert_table_id_to_rootpage(table_id, None);
                                    } else {
                                        dropped_root_pages.remove(&root_page);
                                        let table_id = self.get_table_id_from_root_page(root_page);
                                        let Some(entry) = self.table_id_to_rootpage.get(&table_id)
                                        else {
                                            panic!(
                                                "Logical log contains root page reference {root_page} that does not exist in the table_id_to_rootpage map"
                                            );
                                        };
                                        let Some(value) = entry.value().root_page else {
                                            panic!(
                                                "Logical log contains root page reference {root_page} that does not have a root page in the table_id_to_rootpage map"
                                            );
                                        };
                                        turso_assert_eq!(value, root_page as u64, "logical log root page does not match table_id_to_rootpage map", { "root_page": root_page, "map_value": value });
                                    }
                                } else if root_page != 0 {
                                    return Err(LimboError::Corrupt(format!(
                                        "sqlite_schema root_page must be 0 for {row_type}, got {root_page}"
                                    )));
                                }
                                let rowid_int = rowid.row_id.to_int_or_panic();
                                crate::with_mv_store_allocation_site!(SchemaRowPayload, {
                                    schema_rows.insert(
                                        rowid_int,
                                        ImmutableRecord::from_bin_record(
                                            crate::types::value_blob_from_slice(row.payload())?,
                                        ),
                                    );
                                });
                            } else if self.table_id_to_rootpage.get(&rowid.table_id).is_none() {
                                // Data row references a table_id not yet in the map. This can happen
                                // with logs written before the schema-first serialization fix: in a
                                // same-transaction CREATE TABLE + INSERT + DROP TABLE, data rows were
                                // serialized before the schema INSERT that registers the table_id.
                                // The schema INSERT (or DELETE) for this table will follow later in
                                // this transaction frame, so we register the table_id now.
                                self.insert_table_id_to_rootpage(rowid.table_id, None);
                            }

                            let version_id = self.get_version_id();
                            let row_version = RowVersion {
                                id: version_id,
                                begin: crate::mvcc::database::PackedTs::pack(Some(
                                    TxTimestampOrID::Timestamp(commit_ts),
                                )),
                                end: crate::mvcc::database::PackedTs::pack(None),
                                row: row.clone(),
                                btree_resident,
                                materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                            };
                            {
                                let versions =
                                    self.get_or_create_table_row_versions(rowid.clone())?;
                                let mut versions = versions.write();
                                self.insert_version_raw(&mut versions, row_version)?;
                            }
                            let allocator = self.get_rowid_allocator(&rowid.table_id);
                            allocator.insert_row_id_maybe_update(rowid.row_id.to_int_or_panic());
                        }
                        StreamingResult::DeleteTableRow {
                            rowid, commit_ts, ..
                        } => {
                            max_commit_ts_seen = max_commit_ts_seen.max(commit_ts);
                            if commit_ts <= replay_cutoff_ts {
                                continue;
                            }
                            if self.table_id_to_rootpage.get(&rowid.table_id).is_none() {
                                // See comment in UpsertTableRow: old logs may have data rows
                                // serialized before the schema INSERT that registers the table_id.
                                self.insert_table_id_to_rootpage(rowid.table_id, None);
                            }
                            let tombstone_row = crate::with_mv_store_allocation_site!(
                                RowPayload,
                                if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID {
                                    let rowid_int = rowid.row_id.to_int_or_panic();
                                    if let Some(record) = schema_rows.get(&rowid_int) {
                                        // Preserve the pre-delete sqlite_schema record in recovered
                                        // tombstones so checkpoint can still recover B-tree identity.
                                        Row::new_table_row_in(
                                            rowid.clone(),
                                            record.as_blob(),
                                            record.column_count(),
                                            self.alloc.clone(),
                                        )?
                                    } else {
                                        Row::new_table_row_in(
                                            rowid.clone(),
                                            &[],
                                            0,
                                            self.alloc.clone(),
                                        )?
                                    }
                                } else {
                                    Row::new_table_row_in(
                                        rowid.clone(),
                                        &[],
                                        0,
                                        self.alloc.clone(),
                                    )?
                                }
                            );
                            if let Some(versions) = self.rows.get(&rowid) {
                                // Row exists in memory — try to find the current (non-ended) version
                                // that was committed before this delete, and mark it as ended. If no
                                // such version exists (e.g. it was already GC'd or this is a B-tree
                                // resident row not yet in memory), insert a tombstone instead.
                                let mut versions = versions.value().write();
                                if let Some(existing) = versions.iter_mut().rev().find(|rv| {
                                    rv.end().is_none()
                                        && matches!(rv.begin(), Some(TxTimestampOrID::Timestamp(b)) if b < commit_ts)
                                }) {
                                    existing.set_end(Some(TxTimestampOrID::Timestamp(commit_ts)));
                                } else {
                                    let version_id = self.get_version_id();
                                    let row_version = RowVersion {
                                        id: version_id,
                                        begin: crate::mvcc::database::PackedTs::pack(None),
                                        end: crate::mvcc::database::PackedTs::pack(Some(
                                            TxTimestampOrID::Timestamp(commit_ts),
                                        )),
                                        row: tombstone_row.clone(),
                                        // The version this delete ends was not replayed, so its
                                        // frame is at or below the durable replay boundary: the
                                        // deleted row is in the DB file. Mark the tombstone
                                        // btree-resident so checkpoint applies the delete even
                                        // though the logged flag predates the row becoming durable.
                                        btree_resident: true,
                                        materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                                    };
                                    self.insert_version_raw(&mut versions, row_version)?;
                                }
                            } else {
                                let version_id = self.get_version_id();
                                let row_version = RowVersion {
                                    id: version_id,
                                    begin: crate::mvcc::database::PackedTs::pack(None),
                                    end: crate::mvcc::database::PackedTs::pack(Some(
                                        TxTimestampOrID::Timestamp(commit_ts),
                                    )),
                                    row: tombstone_row,
                                    // Same invariant as above: no replayed version means the
                                    // deleted row is already durable in the DB file.
                                    btree_resident: true,
                                    materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                                };
                                let versions =
                                    self.get_or_create_table_row_versions(rowid.clone())?;
                                let mut versions = versions.write();
                                self.insert_version_raw(&mut versions, row_version)?;
                            }
                            if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID {
                                let rowid_int = rowid.row_id.to_int_or_panic();
                                let Some(record) = schema_rows.get(&rowid_int) else {
                                    // this can happen if a row in sqlite_schema was inserted and then
                                    // deleted in the same transaction (ex: a CREATE TABLE followed by a DROP TABLE)
                                    continue;
                                };
                                if record.column_count() < 5 {
                                    return Err(LimboError::Corrupt(format!(
                                        "sqlite_schema row must have at least 5 columns, got {}",
                                        record.column_count()
                                    )));
                                }
                                let (
                                    ValueRef::Text(row_type),
                                    ValueRef::Numeric(Numeric::Integer(root_page)),
                                ) = record.get_two_values(0, 3)?
                                else {
                                    return Err(LimboError::Corrupt(
                                        "sqlite_schema type and root_page must be text and integer"
                                            .to_string(),
                                    ));
                                };
                                let row_type = row_type.as_str();
                                if (row_type == "table" || row_type == "index") && root_page > 0 {
                                    dropped_root_pages.insert(root_page);
                                }
                                schema_rows.remove(&rowid_int);
                            }
                        }
                        StreamingResult::UpsertIndexRow {
                            row,
                            rowid,
                            commit_ts,
                            btree_resident,
                        } => {
                            max_commit_ts_seen = max_commit_ts_seen.max(commit_ts);
                            if commit_ts <= replay_cutoff_ts {
                                continue;
                            }
                            let version_id = self.get_version_id();
                            let row_version = RowVersion {
                                id: version_id,
                                begin: crate::mvcc::database::PackedTs::pack(Some(
                                    TxTimestampOrID::Timestamp(commit_ts),
                                )),
                                end: crate::mvcc::database::PackedTs::pack(None),
                                row: row.clone(),
                                btree_resident,
                                materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                            };
                            let RowKey::Record(sortable_key) = rowid.row_id.clone() else {
                                panic!("Index writes must be to a record");
                            };
                            self.insert_index_version(rowid.table_id, sortable_key, row_version)?;
                        }
                        StreamingResult::DeleteIndexRow {
                            row,
                            rowid,
                            commit_ts,
                            ..
                        } => {
                            max_commit_ts_seen = max_commit_ts_seen.max(commit_ts);
                            if commit_ts <= replay_cutoff_ts {
                                continue;
                            }
                            let RowKey::Record(sortable_key) = rowid.row_id.clone() else {
                                panic!("Index writes must be to a record");
                            };
                            let sortable_key =
                                self.get_or_create_index_key_arc(rowid.table_id, sortable_key)?;
                            if let Some(index_map) = self.index_rows.get(&rowid.table_id) {
                                if let Some(versions) = index_map.value().get(&sortable_key) {
                                    let mut versions = versions.value().write();
                                    if let Some(existing) = versions.iter_mut().rev().find(|rv| {
                                rv.end().is_none()
                                    && matches!(rv.begin(), Some(TxTimestampOrID::Timestamp(b)) if b < commit_ts)
                            }) {
                                existing.set_end(Some(TxTimestampOrID::Timestamp(commit_ts)));
                                continue;
                            }
                                }
                            }
                            let version_id = self.get_version_id();
                            let row_version = RowVersion {
                                id: version_id,
                                begin: crate::mvcc::database::PackedTs::pack(None),
                                end: crate::mvcc::database::PackedTs::pack(Some(
                                    TxTimestampOrID::Timestamp(commit_ts),
                                )),
                                row: row.clone(),
                                // The index entry this delete ends was not replayed, so its
                                // frame is at or below the durable replay boundary: the entry
                                // is in the DB file. Mark the tombstone btree-resident so
                                // checkpoint applies the delete even though the logged flag
                                // predates the entry becoming durable (e.g. a checkpoint that
                                // failed after its pager commit).
                                btree_resident: true,
                                materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                            };
                            self.insert_index_version(rowid.table_id, sortable_key, row_version)?;
                        }
                        StreamingResult::UpdateHeader { header, commit_ts } => {
                            max_commit_ts_seen = max_commit_ts_seen.max(commit_ts);
                            if commit_ts <= replay_cutoff_ts {
                                continue;
                            }
                            // Recovery applies only post-boundary header ops; the same value is later
                            // staged to pager page-1 during checkpoint.
                            self.global_header.write().replace(header);
                        }
                        StreamingResult::Eof => {
                            unreachable!("next_frame does not return EOF records");
                        }
                    }
                }
            }

            if schema_rows_after.is_some() {
                // Now that all table and index ops from this transaction have
                // been replayed, publish the frame's final schema. No later index
                // op from this frame can observe a half-applied sqlite_schema.
                let schema_rows_after =
                    schema_rows_after.expect("schema_rows_after must exist when schema changed");
                let schema_after = schema_after
                    .expect("schema_after must exist when frame_changes_schema is true");
                schema_rows = schema_rows_after;
                install_schema(schema_after.clone());
                current_schema = schema_after;
                // The frame may have decoded DROP INDEX entries using the
                // pre-frame schema. Do not carry those IndexInfo values into
                // later frames after current_schema has changed.
                index_infos.clear();
            }
        }

        ctx.max_commit_ts_seen = max_commit_ts_seen;
        ctx.schema_rows = schema_rows;
        ctx.dropped_root_pages = dropped_root_pages;
        ctx.current_schema = current_schema;
        ctx.index_infos = index_infos;
        Ok(())
    }

    /// Build a fresh `Schema` from the recovered `sqlite_schema` rows. Sync: the
    /// schema cookie comes from `global_header` (in-memory) or `fallback_cookie`
    /// (pre-read by the caller); no IO here.
    fn recover_build_schema(
        &self,
        connection: &Arc<Connection>,
        schema_rows: &HashMap<i64, ImmutableRecord>,
        fallback_cookie: u32,
        preserved_table_valued_functions: &[Arc<crate::vtab::VirtualTable>],
    ) -> Result<Arc<Schema>> {
        let cookie = self
            .global_header
            .read()
            .as_ref()
            .map(|header| header.schema_cookie.get())
            .unwrap_or(fallback_cookie);
        let mut fresh = Schema::with_options(
            connection.db.experimental_custom_types_enabled(),
            connection.db.dialect().as_ref(),
        )?;
        fresh.generated_columns_enabled = connection.db.experimental_generated_columns_enabled();
        fresh.schema_version = cookie;
        let mut from_sql_indexes =
            crate::alloc::Vec::try_with_capacity_ext(10).expect(crate::alloc::ALLOC_ERR_MSG);
        let mut automatic_indices: HashMap<String, crate::alloc::Vec<(String, i64)>> =
            HashMap::default();
        let mut dbsp_state_roots: HashMap<String, i64> = HashMap::default();
        let mut dbsp_state_index_roots: HashMap<String, i64> = HashMap::default();
        let mut materialized_view_info: HashMap<String, (String, i64)> = HashMap::default();
        let syms = connection.syms.read();
        let mv_store = connection.db.get_mv_store().clone();

        let mut sorted_rowids: Vec<i64> = schema_rows.keys().copied().collect();
        sorted_rowids.sort_unstable();
        for rowid in &sorted_rowids {
            let record = &schema_rows[rowid];
            let ty = match record.get_value_opt(0) {
                Some(ValueRef::Text(v)) => v.as_str(),
                _ => {
                    return Err(LimboError::Corrupt(
                        "sqlite_schema type must be text".to_string(),
                    ));
                }
            };
            let name = match record.get_value_opt(1) {
                Some(ValueRef::Text(v)) => v.as_str(),
                _ => {
                    return Err(LimboError::Corrupt(
                        "sqlite_schema name must be text".to_string(),
                    ));
                }
            };
            let table_name = match record.get_value_opt(2) {
                Some(ValueRef::Text(v)) => v.as_str(),
                _ => {
                    return Err(LimboError::Corrupt(
                        "sqlite_schema tbl_name must be text".to_string(),
                    ));
                }
            };
            let root_page = match record.get_value_opt(3) {
                Some(ValueRef::Numeric(Numeric::Integer(v))) => v,
                _ => {
                    return Err(LimboError::Corrupt(
                        "sqlite_schema root_page must be integer".to_string(),
                    ));
                }
            };
            // A negative root is a not-yet-materialized placeholder. If a (passive) checkpoint
            // has since materialized this object, resolve to its real positive root so the
            // rebuilt schema reflects the btree page — otherwise integrity_check skips the
            // negative root and reports its materialized page as orphaned ("never used").
            let root_page = if root_page < 0 {
                self.table_id_to_rootpage
                    .get(&MVTableId::from(root_page))
                    .filter(|e| e.value().is_live())
                    .and_then(|e| e.value().root_page)
                    .map(|r| r as i64)
                    .unwrap_or(root_page)
            } else {
                root_page
            };
            let sql = match record.get_value_opt(4) {
                Some(ValueRef::Text(v)) => Some(v.as_str()),
                _ => None,
            };
            let attached_resolver = |alias: &str| -> Option<usize> {
                connection
                    .attached_databases()
                    .read()
                    .get_database_by_name(&crate::util::normalize_ident(alias))
                    .map(|(idx, _)| idx)
            };
            fresh.handle_schema_row(
                ty,
                name,
                table_name,
                root_page,
                sql,
                &syms,
                &mut from_sql_indexes,
                &mut automatic_indices,
                &mut dbsp_state_roots,
                &mut dbsp_state_index_roots,
                &mut materialized_view_info,
                &attached_resolver,
                connection.dialect().as_ref(),
            )?;
        }
        fresh.populate_indices(
            &syms,
            from_sql_indexes,
            automatic_indices,
            mv_store.is_some(),
        )?;
        fresh.populate_materialized_views(
            materialized_view_info,
            dbsp_state_roots,
            dbsp_state_index_roots,
        )?;
        Self::rehydrate_table_valued_functions(&mut fresh, preserved_table_valued_functions);

        Ok(Arc::new(fresh))
    }

    pub fn set_checkpoint_threshold(&self, threshold: i64) {
        self.storage.set_checkpoint_threshold(threshold)
    }

    pub fn checkpoint_threshold(&self) -> i64 {
        self.storage.checkpoint_threshold()
    }

    pub fn get_real_table_id(&self, table_id: i64) -> i64 {
        self.current_root_page(&MVTableId::from(table_id))
            .map_or(table_id, |root_page| root_page as i64)
    }

    pub fn get_rowid_allocator(&self, table_id: &MVTableId) -> Arc<RowidAllocator> {
        let mut map = self.table_id_to_last_rowid.write();
        if map.contains_key(table_id) {
            map.get(table_id).unwrap().clone()
        } else {
            let allocator = Arc::new(RowidAllocator {
                lock: TursoRwLock::new(),
                max_rowid: AtomicI64::new(0),
                initialized: AtomicBool::new(false),
            });
            map.insert(*table_id, allocator.clone());
            allocator
        }
    }

    /// Whether `table_id` has a *currently live* checkpointed B-tree. Snapshot-agnostic; for
    /// transaction reads use [`Self::is_btree_readable_at`].
    pub fn is_btree_allocated(&self, table_id: &MVTableId) -> bool {
        self.table_id_to_rootpage
            .get(table_id)
            .is_some_and(|entry| entry.value().root_page.is_some() && entry.value().is_live())
    }

    /// Whether a transaction may **read** `table_id`'s B-tree, given its logical snapshot
    /// `begin_ts` and its frozen WAL `read_mark`. Requires BOTH:
    /// - **logical (base validity):** the binding `covers(begin_ts)` — `begin <= begin_ts < end`; and
    /// - **physical reachability:** `materialized_at <= read_mark` — the btree's pages are at-or-below
    ///   this transaction's read mark (same WAL epoch and frame ≤ mark, or an earlier backfilled
    ///   epoch). Without this a transaction that opened before an checkpoint materialization would
    ///   seek a page its read mark cannot reach (a torn/foreign/zeroed-page read).
    ///
    /// When this is false the transaction stays version-store-only; the GC floor
    /// ([`Self::compute_min_reader_mark`]) guarantees the version-store copy is still present.
    pub fn is_btree_readable_at(
        &self,
        table_id: &MVTableId,
        begin_ts: u64,
        read_mark: WalPos,
    ) -> bool {
        self.table_id_to_rootpage
            .get(table_id)
            .is_some_and(|entry| {
                let e = entry.value();
                // A not-yet-committed binding (materialized_at == STAGED) is never readable, even
                // by an untracked/no-WAL reader whose mark is also STAGED.
                e.root_page.is_some()
                    && e.materialized_at != WalPos::STAGED
                    && e.covers(begin_ts)
                    && e.materialized_at <= read_mark
            })
    }

    /// Lexicographic minimum WAL read mark over all active/preparing transactions
    /// ([`WalPos::STAGED`] if none). A freshly-materialized object's version-store rows may be GC'd
    /// only once `materialized_at <= this`, i.e. every live reader can now physically reach it —
    /// otherwise a reader whose read mark predates the materialization would lose the rows.
    pub fn compute_min_reader_mark(&self) -> WalPos {
        self.txs
            .iter()
            .filter_map(|entry| {
                let tx = entry.value();
                match tx.state.load() {
                    TransactionState::Active | TransactionState::Preparing(_) => Some(tx.read_mark),
                    _ => None,
                }
            })
            .min()
            .unwrap_or(WalPos::STAGED)
    }

    /// GC floor for a freshly *created* btree: while a reader's read mark predates the binding's
    /// `materialized_at` (the creation frame) it cannot reach the btree and relies on the version
    /// store. Incremental re-materialization of existing rows is handled precisely per-version via
    /// [`RowVersion::materialized_at`] in [`Self::gc_version_chain`], not here.
    fn rootpage_gc_protected(&self, table_id: &MVTableId, min_reader_mark: WalPos) -> bool {
        self.table_id_to_rootpage
            .get(table_id)
            .is_some_and(|e| e.value().materialized_at > min_reader_mark)
    }

    pub fn tx_should_abort(&self, tx_id: u64) -> bool {
        if let Some(tx) = self.txs.get(&tx_id) {
            tx.value().abort_now.load(Ordering::Acquire)
        } else {
            false
        }
    }
}

fn rollback_row_version(tx_id: u64, rv: &mut RowVersion) {
    if rv.begin() == Some(TxTimestampOrID::TxID(tx_id)) {
        // If the transaction has aborted,
        // it marks all its new versions as garbage and sets their Begin
        // and End timestamps to infinity to make them invisible
        // See section 2.4: https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf
        rv.set_begin(None);
        rv.set_end(None);
    } else if rv.end() == Some(TxTimestampOrID::TxID(tx_id)) {
        // undo deletions by this transaction
        rv.set_end(None);
    }
}

impl RowidAllocator {
    /// Lock-free rowid allocation via atomic CAS.
    /// Returns None only when at i64::MAX (triggers random fallback).
    /// Returns Some((new_rowid, prev_rowid)) where prev_rowid is None if table was empty.
    pub fn get_next_rowid(&self) -> Option<(i64, Option<i64>)> {
        loop {
            let cur = self.max_rowid.load(Ordering::SeqCst);
            if cur == i64::MAX {
                tracing::trace!("get_next_rowid(max)");
                return None;
            }
            let next = cur + 1;
            if self
                .max_rowid
                .compare_exchange(cur, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                let prev = if cur == 0 { None } else { Some(cur) };
                tracing::trace!("get_next_rowid({next})");
                return Some((next, prev));
            }
        }
    }

    /// Bump the counter to at least `rowid`. Used for user-specified rowids
    /// (e.g. INSERT INTO t(rowid,...) VALUES(1000,...)).
    pub fn insert_row_id_maybe_update(&self, rowid: i64) {
        loop {
            let cur = self.max_rowid.load(Ordering::SeqCst);
            if rowid <= cur {
                return;
            }
            if self
                .max_rowid
                .compare_exchange(cur, rowid, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return;
            }
        }
    }

    pub fn is_uninitialized(&self) -> bool {
        !self.initialized.load(Ordering::SeqCst)
    }

    /// Initialize from btree max. Called once per table, under lock.
    pub fn initialize(&self, rowid: Option<i64>) {
        tracing::trace!("initialize({rowid:?})");
        let _ = self
            .max_rowid
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |cur| {
                let next = match rowid {
                    // max_rowid starts at 0, but a B-tree whose largest rowid is
                    // negative still needs to seed automatic allocation from that value.
                    Some(rowid) if cur == 0 => rowid,
                    Some(rowid) => cur.max(rowid),
                    None => cur,
                };
                (next != cur).then_some(next)
            });
        self.initialized.store(true, Ordering::SeqCst);
    }

    pub fn lock(&self) -> bool {
        self.lock.write()
    }

    pub fn unlock(&self) {
        self.lock.unlock()
    }
}

pub fn create_seek_range<K: Ord>(
    limit_boundary: Bound<K>,
    direction: IterationDirection,
) -> (Bound<K>, Bound<K>) {
    if direction == IterationDirection::Forwards {
        (limit_boundary, Bound::Unbounded)
    } else {
        (Bound::Unbounded, limit_boundary)
    }
}

/// A write-write conflict happens when transaction T_current attempts to update a
/// row version that is:
/// a) currently being updated by an active transaction T_previous, or
/// b) was updated by an ended transaction T_previous that committed AFTER T_current started
/// but BEFORE T_previous commits.
///
/// "Suppose transaction T wants to update a version V. V is updatable
/// only if it is the latest version, that is, it has an end timestamp equal
/// to infinity or its End field contains the ID of a transaction TE and
/// TE’s state is Aborted"
/// Ref: https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf , page 301,
/// 2.6. Updating a Version.
fn is_write_write_conflict<A: ConcurrentAllocator>(
    txs: &SkipMap<TxID, Transaction<A>, BasicComparator, A>,
    finalized_tx_states: &SkipMap<TxID, TransactionState, BasicComparator, A>,
    tx: &Transaction<A>,
    rv: &RowVersion,
) -> bool {
    match rv.end() {
        Some(TxTimestampOrID::TxID(rv_end)) => {
            if rv_end == tx.tx_id {
                return false;
            }
            match lookup_tx_state(txs, finalized_tx_states, rv_end) {
                Some(TransactionState::Aborted) | Some(TransactionState::Terminated) => false,
                Some(TransactionState::Active)
                | Some(TransactionState::Preparing(_))
                | Some(TransactionState::Committed(_)) => true,
                None => {
                    tracing::debug!(
                        "is_write_write_conflict: missing tx {} for row version {:?}; treating as conflict",
                        rv_end,
                        rv
                    );
                    true
                }
            }
        }
        // A non-"infinity" end timestamp (here modeled by Some(ts)) functions as a write lock
        // on the row, so it can never be updated by another transaction.
        // Ref: https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf , page 301,
        // 2.6. Updating a Version.
        Some(TxTimestampOrID::Timestamp(_)) => true,
        None => false,
    }
}

impl RowVersion {
    /// Construct a row version. `begin`/`end` are bit-packed internally.
    pub fn new(
        id: u64,
        begin: Option<TxTimestampOrID>,
        end: Option<TxTimestampOrID>,
        row: Row,
        btree_resident: bool,
    ) -> Self {
        Self {
            id,
            begin: crate::mvcc::database::PackedTs::pack(begin),
            end: crate::mvcc::database::PackedTs::pack(end),
            row,
            btree_resident,
            materialized_at: WalPos::ORIGIN,
        }
    }

    /// WAL position at which this version's current state is materialized in the B-tree
    /// ([`WalPos::ORIGIN`] = not materialized). See the field doc.
    #[inline]
    pub(crate) fn materialized_at(&self) -> WalPos {
        self.materialized_at
    }

    #[inline]
    pub(crate) fn set_materialized_at(&mut self, pos: WalPos) {
        self.materialized_at = pos;
    }

    /// The begin timestamp/tx-id, or `None`.
    #[inline]
    pub fn begin(&self) -> Option<TxTimestampOrID> {
        self.begin.unpack()
    }

    /// The end timestamp/tx-id, or `None`.
    #[inline]
    pub fn end(&self) -> Option<TxTimestampOrID> {
        self.end.unpack()
    }

    #[inline]
    pub fn set_begin(&mut self, value: Option<TxTimestampOrID>) {
        self.begin = crate::mvcc::database::PackedTs::pack(value);
        // The version's state changed, so any prior B-tree materialization no longer reflects
        // it (over-resetting to ORIGIN is safe: it only delays GC, never reclaims early).
        self.materialized_at = WalPos::ORIGIN;
    }

    #[inline]
    pub fn set_end(&mut self, value: Option<TxTimestampOrID>) {
        self.end = crate::mvcc::database::PackedTs::pack(value);
        // A delete (or any end change) means the B-tree no longer reflects this version's current
        // state until a checkpoint re-materializes it; the GC must not reclaim it before then.
        self.materialized_at = WalPos::ORIGIN;
    }

    /// Replace `begin`/`end` references to `tx_id` with the committed
    /// timestamp `end_ts`. Shared by the chunked commit step
    /// (`step_rewrite_live_versions`) and the dropped-statement cleanup path
    /// (`rewrite_live_versions_for_committed_tx`) so the two cannot drift.
    #[inline]
    fn rewrite_txid_to_timestamp(&mut self, tx_id: TxID, end_ts: u64) {
        if let Some(TxTimestampOrID::TxID(rv_id)) = self.begin() {
            if rv_id == tx_id {
                self.set_begin(Some(TxTimestampOrID::Timestamp(end_ts)));
            }
        }
        if let Some(TxTimestampOrID::TxID(rv_id)) = self.end() {
            if rv_id == tx_id {
                self.set_end(Some(TxTimestampOrID::Timestamp(end_ts)));
            }
        }
    }

    /// A row is visible to a transaction if:
    /// * Begin is visible to the transaction
    /// * End timestamp is not applicable yet, meaning deletion of row is not visible to this transaction
    fn is_visible_to<A: ConcurrentAllocator>(
        &self,
        tx: &Transaction<A>,
        txs: &SkipMap<TxID, Transaction<A>, BasicComparator, A>,
        finalized_tx_states: &SkipMap<TxID, TransactionState, BasicComparator, A>,
    ) -> bool {
        is_begin_visible(txs, finalized_tx_states, tx, self)
            && is_end_visible(txs, finalized_tx_states, tx, self)
    }

    /// Check if this version indicates the B-tree row has been modified (updated or deleted).
    ///
    /// A version is "relevant" to a transaction if:
    /// 1. The version is fully visible (begin visible AND end visible), OR
    /// 2. The version has an end timestamp that indicates the row was deleted before/at the transaction's begin, OR
    /// 3. The current transaction itself has deleted/updated this row (end = current tx_id)
    ///
    /// This is used by dual-cursor to determine if a B-tree row should be shown or hidden.
    fn is_btree_invalidating_version<A: ConcurrentAllocator>(
        &self,
        tx: &Transaction<A>,
        txs: &SkipMap<TxID, Transaction<A>, BasicComparator, A>,
        finalized_tx_states: &SkipMap<TxID, TransactionState, BasicComparator, A>,
    ) -> bool {
        // If the version is fully visible, it invalidates the B-tree
        if self.is_visible_to(tx, txs, finalized_tx_states) {
            return true;
        }

        // Check if this version represents a deletion/update that affects us
        match self.end() {
            Some(TxTimestampOrID::Timestamp(end_ts)) => {
                // Row was deleted at end_ts. If we started after end_ts, we shouldn't see it
                turso_assert!(
                    tx.begin_ts != end_ts,
                    "begin_ts and committed end_ts cannot be equal: txn timestamps are strictly monotonic"
                );
                tx.begin_ts > end_ts
            }
            Some(TxTimestampOrID::TxID(end_tx_id)) => {
                // Row is being deleted/updated by another transaction.
                // Consult the deleting tx's state so we don't race with the
                // post-commit rewrite that turns TxID(W) into Timestamp(W.end_ts).
                if end_tx_id == tx.tx_id {
                    return true;
                }
                match lookup_tx_state(txs, finalized_tx_states, end_tx_id) {
                    Some(TransactionState::Committed(committed_ts)) => {
                        // Same predicate as the Timestamp arm above.
                        tx.begin_ts > committed_ts
                    }
                    Some(TransactionState::Preparing(end_ts)) => {
                        // Hekaton speculative read: treat as if W will commit at
                        // its prepared end_ts. When we speculatively invalidate
                        // the B-tree row, register a commit dependency on W —
                        // for tombstones (begin=None) we are the only place that
                        // decides this, since `is_visible_to` short-circuits at
                        // `is_begin_visible` and never calls `is_end_visible`.
                        // If W aborts, we must cascade-abort to avoid letting
                        // the reader observe the row reappear.
                        let speculatively_invalidated = tx.begin_ts > end_ts;
                        if speculatively_invalidated {
                            register_commit_dependency(txs, tx, end_tx_id);
                        }
                        speculatively_invalidated
                    }
                    Some(TransactionState::Active) => false,
                    Some(TransactionState::Aborted) | Some(TransactionState::Terminated) => false,
                    None => false,
                }
            }
            None => false,
        }
    }
}

/// Hekaton Section 2.7 — register-and-report protocol:
/// "To take a commit dependency on a transaction T2, T1 increments its
/// CommitDepCounter and adds its transaction ID to T2's CommitDepSet."
///
/// The lock on `commit_dep_set` serializes with the drain in commit/abort
/// resolution, preventing the race where we push an entry after the drain.
fn register_commit_dependency<A: ConcurrentAllocator>(
    txs: &SkipMap<TxID, Transaction<A>, BasicComparator, A>,
    dependent_tx: &Transaction<A>,
    depended_on_tx_id: TxID,
) {
    turso_assert!(
        dependent_tx.tx_id != depended_on_tx_id,
        "transaction cannot depend on itself"
    );
    let Some(depended_on) = txs.get(&depended_on_tx_id) else {
        // Transaction was already committed and removed from the map
        // (CommitEnd calls remove_tx after setting Committed and draining
        // CommitDepSet). Dependency is trivially resolved.
        return;
    };
    let depended_on = depended_on.value();

    // Hold lock while checking state to serialize with the drain in
    // commit/abort postprocessing.
    let mut dep_set = depended_on.commit_dep_set.lock();
    match depended_on.state.load() {
        TransactionState::Preparing(_) => {
            // Increment counter BEFORE inserting into dep_set and BEFORE dropping
            // the lock. This prevents underflow: if we inserted first and
            // released the lock, the depended-on tx could drain the dep_set
            // and call fetch_sub before we increment, wrapping the counter
            // from 0 to u64::MAX. Only increment on first insertion (dedup).
            if dep_set.insert(dependent_tx.tx_id) {
                dependent_tx
                    .commit_dep_counter
                    .fetch_add(1, Ordering::AcqRel);
            }
            drop(dep_set);
            tracing::trace!(
                "register_commit_dependency: tx {} depends on tx {}",
                dependent_tx.tx_id,
                depended_on_tx_id
            );
        }
        TransactionState::Active => {
            turso_assert!(false, "a txn found dependent on active txn");
        }
        TransactionState::Committed(_) => {
            // Already committed — dependency trivially resolved.
        }
        TransactionState::Aborted | TransactionState::Terminated => {
            // Already aborted — cascade abort to dependent.
            drop(dep_set);
            dependent_tx.abort_now.store(true, Ordering::Release);
            tracing::trace!(
                "register_commit_dependency: tx {} must abort (dep tx {} aborted)",
                dependent_tx.tx_id,
                depended_on_tx_id
            );
        }
    }
}

fn lookup_tx_state<A: ConcurrentAllocator>(
    txs: &SkipMap<TxID, Transaction<A>, BasicComparator, A>,
    finalized_tx_states: &SkipMap<TxID, TransactionState, BasicComparator, A>,
    tx_id: TxID,
) -> Option<TransactionState> {
    txs.get(&tx_id)
        .map(|entry| entry.value().state.load())
        .or_else(|| finalized_tx_states.get(&tx_id).map(|entry| *entry.value()))
}

fn lookup_finalized_tx_state<A: ConcurrentAllocator>(
    finalized_tx_states: &SkipMap<TxID, TransactionState, BasicComparator, A>,
    tx_id: TxID,
) -> Option<TransactionState> {
    finalized_tx_states.get(&tx_id).map(|entry| {
        let state = *entry.value();
        turso_assert!(
            !matches!(
                state,
                TransactionState::Active | TransactionState::Preparing(_)
            ),
            "finalized_tx_states contains non-final state for tx {tx_id}: {state:?}"
        );
        state
    })
}

fn is_begin_visible<A: ConcurrentAllocator>(
    txs: &SkipMap<TxID, Transaction<A>, BasicComparator, A>,
    finalized_tx_states: &SkipMap<TxID, TransactionState, BasicComparator, A>,
    tx: &Transaction<A>,
    rv: &RowVersion,
) -> bool {
    match rv.begin() {
        Some(TxTimestampOrID::Timestamp(rv_begin_ts)) => {
            turso_assert!(
                tx.begin_ts != rv_begin_ts,
                "begin_ts and committed rv_begin_ts cannot be equal: txn timestamps are strictly monotonic"
            );
            tx.begin_ts > rv_begin_ts
        }
        Some(TxTimestampOrID::TxID(rv_begin)) => {
            let visible = match txs.get(&rv_begin) {
                Some(tb_entry) => {
                    let tb = tb_entry.value();
                    let visible = match tb.state.load() {
                        TransactionState::Active => tx.tx_id == tb.tx_id && rv.end().is_none(),
                        TransactionState::Preparing(end_ts) => {
                            // Hekaton Table 1 / Section 2.5: speculative read of TB.
                            // If begin_ts > end_ts, the version would be visible once TB
                            // commits. Speculatively return true and register a dependency.
                            // Fixes partial commit visibility (Bug #8).
                            turso_assert!(
                                tx.tx_id != tb.tx_id,
                                "a txn cannot read its own row versions during prepare"
                            );
                            turso_assert!(
                                tx.begin_ts != end_ts,
                                "begin_ts and preparing end_ts cannot be equal: txn timestamps are strictly monotonic"
                            );
                            if tx.begin_ts > end_ts {
                                register_commit_dependency(txs, tx, rv_begin);
                                true
                            } else {
                                false
                            }
                        }
                        TransactionState::Committed(committed_ts) => {
                            turso_assert!(
                                tx.begin_ts != committed_ts,
                                "begin_ts and committed_ts cannot be equal: txn timestamps are strictly monotonic"
                            );
                            tx.begin_ts > committed_ts
                        }
                        TransactionState::Aborted => false,
                        TransactionState::Terminated => {
                            tracing::debug!(
                                "TODO: should reread rv's end field - it should have updated the timestamp in the row version by now"
                            );
                            false
                        }
                    };
                    tracing::trace!(
                        "is_begin_visible: tx={tx}, tb={tb} rv = {:?}-{:?} visible = {visible}",
                        rv.begin(),
                        rv.end()
                    );
                    visible
                }
                None => match lookup_finalized_tx_state(finalized_tx_states, rv_begin) {
                    Some(TransactionState::Committed(committed_ts)) => {
                        turso_assert!(
                            tx.begin_ts != committed_ts,
                            "begin_ts and committed_ts cannot be equal: txn timestamps are strictly monotonic"
                        );
                        tx.begin_ts > committed_ts
                    }
                    Some(TransactionState::Aborted) | Some(TransactionState::Terminated) => false,
                    Some(TransactionState::Active) | Some(TransactionState::Preparing(_)) => {
                        unreachable!(
                            "is_begin_visible: live tx {} missing from txs but present in finalized cache",
                            rv_begin
                        );
                    }
                    None => {
                        // Transaction was removed from the map after converting its TxID refs
                        // to Timestamps. The begin field should have been updated but we still
                        // see the stale TxID. Conservative fallback.
                        false
                    }
                },
            };
            visible
        }
        None => false,
    }
}

fn is_end_visible<A: ConcurrentAllocator>(
    txs: &SkipMap<TxID, Transaction<A>, BasicComparator, A>,
    finalized_tx_states: &SkipMap<TxID, TransactionState, BasicComparator, A>,
    current_tx: &Transaction<A>,
    row_version: &RowVersion,
) -> bool {
    match row_version.end() {
        Some(TxTimestampOrID::Timestamp(rv_end_ts)) => current_tx.begin_ts < rv_end_ts,
        Some(TxTimestampOrID::TxID(rv_end)) => {
            let visible = match txs.get(&rv_end) {
                Some(other_tx_entry) => {
                    let other_tx = other_tx_entry.value();
                    let visible = match other_tx.state.load() {
                        // V's sharp mind discovered an issue with the hekaton paper which basically states that a
                        // transaction can see a row version if the end is a TXId only if it isn't the same transaction.
                        // Source: https://avi.im/blag/2023/hekaton-paper-typo/
                        TransactionState::Active => current_tx.tx_id != other_tx.tx_id,
                        // Hekaton Table 2: speculative ignore of TE. If end_ts < begin_ts,
                        // we speculatively ignore V (treat deletion as committed). Register a
                        // dependency in case TE aborts (then V should have been visible).
                        TransactionState::Preparing(end_ts) => {
                            turso_assert!(
                                current_tx.tx_id != other_tx.tx_id,
                                "a txn is reading itself while preparing"
                            );
                            let visible = current_tx.begin_ts < end_ts;
                            if !visible {
                                register_commit_dependency(txs, current_tx, rv_end);
                            }
                            visible
                        }
                        TransactionState::Committed(committed_ts) => {
                            current_tx.begin_ts < committed_ts
                        }
                        TransactionState::Aborted => true,
                        // Table 2 (Hekaton): Reread V's End field. In this codebase Terminated is only
                        // reachable from Aborted, and abort rollback resets end to None → visible.
                        TransactionState::Terminated => true,
                    };
                    tracing::trace!(
                        "is_end_visible: tx={current_tx}, te={other_tx} rv = {:?}-{:?}  visible = {visible}",
                        row_version.begin(),
                        row_version.end()
                    );
                    visible
                }
                None => match lookup_finalized_tx_state(finalized_tx_states, rv_end) {
                    Some(TransactionState::Committed(committed_ts)) => {
                        current_tx.begin_ts < committed_ts
                    }
                    Some(TransactionState::Aborted) | Some(TransactionState::Terminated) => true,
                    Some(TransactionState::Active) | Some(TransactionState::Preparing(_)) => {
                        unreachable!(
                            "is_end_visible: live tx {rv_end} missing from txs but present in finalized cache"
                        );
                    }
                    None => {
                        // Transaction was removed after converting its TxID refs to Timestamps.
                        // The end field should have been updated. Conservative fallback.
                        true
                    }
                },
            };
            visible
        }
        None => true,
    }
}

impl<Clock: LogicalClock, A: ConcurrentAllocator> Debug for CommitState<Clock, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initial => write!(f, "Initial"),
            Self::Commit { end_ts } => f.debug_struct("Commit").field("end_ts", end_ts).finish(),
            Self::WaitForDependencies { end_ts } => f
                .debug_struct("WaitForDependencies")
                .field("end_ts", end_ts)
                .finish(),
            Self::BuildLogRecord(ctx) => f.debug_tuple("BuildLogRecord").field(ctx).finish(),
            Self::BeginCommitLogicalLog { end_ts, log_record } => f
                .debug_struct("BeginCommitLogicalLog")
                .field("end_ts", end_ts)
                .field("log_record", log_record)
                .finish(),
            Self::AwaitGroupCommit { end_ts, ticket } => f
                .debug_struct("AwaitGroupCommit")
                .field("end_ts", end_ts)
                .field("ticket", ticket)
                .finish(),
            Self::UpgradeLogicalLogHeader { end_ts, log_record } => f
                .debug_struct("UpgradeLogicalLogHeader")
                .field("end_ts", end_ts)
                .field("log_record", log_record)
                .finish(),
            Self::WriteLogicalLog { end_ts, log_record } => f
                .debug_struct("WriteLogicalLog")
                .field("end_ts", end_ts)
                .field("log_record", log_record)
                .finish(),
            Self::FinishLogicalLogWrite { end_ts } => f
                .debug_struct("FinishLogicalLogWrite")
                .field("end_ts", end_ts)
                .finish(),
            Self::SyncLogicalLog { end_ts } => f
                .debug_struct("SyncLogicalLog")
                .field("end_ts", end_ts)
                .finish(),
            Self::SyncGroupPrefix { end_ts, ticket } => f
                .debug_struct("SyncGroupPrefix")
                .field("end_ts", end_ts)
                .field("ticket", ticket)
                .finish(),
            Self::GroupPrefixSynced { end_ts, ticket } => f
                .debug_struct("GroupPrefixSynced")
                .field("end_ts", end_ts)
                .field("ticket", ticket)
                .finish(),
            Self::EndCommitLogicalLog { end_ts } => f
                .debug_struct("EndCommitLogicalLog")
                .field("end_ts", end_ts)
                .finish(),
            Self::Checkpoint { state_machine: _ } => f.debug_struct("Checkpoint").finish(),
            Self::CommitEnd { end_ts } => {
                f.debug_struct("CommitEnd").field("end_ts", end_ts).finish()
            }
            Self::RewriteLiveVersions(ctx) => {
                f.debug_tuple("RewriteLiveVersions").field(ctx).finish()
            }
            Self::FinalizeCommit { end_ts } => f
                .debug_struct("FinalizeCommit")
                .field("end_ts", end_ts)
                .finish(),
        }
    }
}

impl PartialOrd for RowID {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowID {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Make sure table id is first comparison so that we sort first by table_id and then by
        // rowid. Due to order of the struct, table_id is first which is fine but if we were to
        // change it we would bring chaos.
        match self.table_id.cmp(&other.table_id) {
            std::cmp::Ordering::Equal => self.row_id.cmp(&other.row_id),
            ord => ord,
        }
    }
}
