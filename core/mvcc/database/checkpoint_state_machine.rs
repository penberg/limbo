use crate::alloc::{
    ConcurrentAllocator, TryReserveError, TursoAllocator, TursoIteratorExt, TursoVecExt, Vec,
    ALLOC_ERR_MSG,
};
use crate::mvcc::clock::LogicalClock;
use crate::mvcc::database::{
    DeleteRowStateMachine, MVTableId, MvStore, Row, RowID, RowKey, RowVersion, SortableIndexKey,
    TxTimestampOrID, WalPos, WriteRowStateMachine, MVCC_META_KEY_PERSISTENT_TX_TS_MAX,
    MVCC_META_TABLE_NAME, SQLITE_SCHEMA_MVCC_TABLE_ID,
};
#[cfg(any(test, injected_yields))]
use crate::mvcc::yield_hooks::{ProvidesYieldContext, YieldContext, YieldPointMarker};
use crate::mvcc::yield_points::{inject_transition_failure, inject_transition_yield};
use crate::schema::{Index, Schema};
use crate::state_machine::{StateMachine, StateTransition, TransitionResult};
use crate::storage::btree::{BTreeCursor, CursorTrait};
use crate::storage::pager::CreateBTreeFlags;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::storage::wal::{CheckpointMode, TursoRwLock, WalAutoActions};
use crate::sync::atomic::Ordering;
use crate::sync::Arc;
use crate::sync::RwLock;
use crate::types::IOResultOr;
use crate::types::{IOCompletions, IOResult, ImmutableRecord, ImmutableRecordRef};
use crate::{turso_assert, turso_assert_eq};
use crate::{
    CheckpointResult, Completion, Connection, Database, IOExt, LimboError, Numeric, Pager, Result,
    SyncMode, TransactionState, Value, ValueRef,
};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::num::NonZeroU64;
use std::ops::Bound;
#[cfg(any(test, injected_yields))]
use strum::EnumCount;

use super::lookup_tx_state;
const COLLECT_PREEMPTION_THRESHOLD: usize = 1024;

macro_rules! with_mvcc_checkpoint_allocation_site {
    ($site:ident, $expr:expr) => {{
        #[cfg(feature = "allocation_metric")]
        let _turso_allocation_site_guard =
            crate::alloc::enter_allocation_site(crate::alloc::MvccCheckpointAllocationSite::$site);
        $expr
    }};
}

/// Root page of the `sqlite_schema` B-tree in the database file.
const SQLITE_SCHEMA_ROOT_PAGE: i64 = 1;
/// Column count of a `sqlite_schema` record (type, name, tbl_name, rootpage, sql).
const SQLITE_SCHEMA_COLUMN_COUNT: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointState {
    PrepareCheckpoint,
    AcquireLock,
    BuildLocalSchemaView,
    CollectTableRows,
    CollectIndexRows,
    BeginPagerTxn,
    WriteRow {
        write_set_index: usize,
        requires_seek: bool,
    },
    WriteRowStateMachine {
        write_set_index: usize,
    },
    DeleteRowStateMachine {
        write_set_index: usize,
    },
    WriteIndexRow {
        index_write_set_index: usize,
        requires_seek: bool,
    },
    WriteIndexRowStateMachine {
        index_write_set_index: usize,
    },
    DeleteIndexRowStateMachine {
        index_write_set_index: usize,
    },
    /// Compact each non-CYCLE sequence backing table down to a single
    /// watermark row. CYCLE seqs are skipped — they manage wrap
    /// correctness via inline compaction in the nextval bytecode and
    /// already stay at one row in steady state. Non-CYCLE seqs grow
    /// monotonically (one row per nextval) since inline compaction
    /// was removed from the hot path to eliminate shared-row WW
    /// conflicts; checkpoint reclaims the historical rows here, via
    /// `SeqCompactDriver` which drives `BTreeCursor` ops with normal
    /// `IOResult` propagation (no `io.block` / `wait_for_completion`).
    CompactSequences,
    CommitPagerTxn,
    CheckpointWal,
    /// Fsync the database file after checkpoint, before truncating WAL.
    /// This ensures durability: if we crash after WAL truncation but before DB fsync,
    /// the data would be lost.
    SyncDbFile,
    TruncateLogicalLog,
    FsyncLogicalLog,
    /// Truncate the WAL file after DB file and logical-log cleanup are safely durable.
    TruncateWal,
    GcTableRows {
        next_index: usize,
        lwm: u64,
    },
    GcIndexRows {
        next_index: usize,
        lwm: u64,
    },
    Finalize,
}

#[cfg(any(test, injected_yields))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum_macros::EnumCount)]
#[repr(u8)]
pub(crate) enum CheckpointYieldPoint {
    BeforeAcquireLock,
    AfterDurableBoundaryAdvanced,
    AfterCollectTableRows,
    BeforePagerCommit,
}

#[cfg(any(test, injected_yields))]
impl YieldPointMarker for CheckpointYieldPoint {
    const POINT_COUNT: u8 = Self::COUNT as u8;

    fn ordinal(self) -> u8 {
        self as u8
    }
}

#[cfg(any(test, injected_yields))]
fn checkpoint_yield_key() -> u64 {
    const CHECKPOINT_SELECTION_TAG: u64 = 0xC4EC_9011_C4EC_9011;
    CHECKPOINT_SELECTION_TAG
}

/// Root-map mutation staged during collection; applied in the publish window.
enum RootMapOp {
    /// Insert a new (checkpointed) root binding for `id` at `root` (STAGED → published).
    Alloc { id: MVTableId, root: u64 },
    /// Set the `end` ts of `id`'s binding (DROP of a checkpointed object).
    Retire { id: MVTableId, end_ts: u64 },
    /// Remove `id`'s binding entirely (DROP of a never-checkpointed object).
    Remove { id: MVTableId },
}

/// The states of the locks held by the state machine - these are tracked for error handling so that they are
/// released if the state machine fails.
pub struct LockStates {
    blocking_checkpoint_lock_held: bool,
    pager_read_tx: bool,
    pager_write_tx: bool,
}

/// A state machine that performs a complete checkpoint operation on the MVCC store.
///
/// The checkpoint process:
/// 1. Takes a blocking lock on the database so that no other transactions can run during the checkpoint.
/// 2. Determines which row versions should be written to the B-tree.
/// 3. Begins a pager transaction
/// 4. Writes all the selected row versions to the B-tree.
/// 5. Commits the pager transaction, effectively flushing to the WAL
/// 6. Immediately does a TRUNCATE checkpoint from the WAL to the DB
/// 7. Fsync the DB file
/// 8. Truncate logical log to 0 (salt regenerated in memory), fsync, then truncate WAL
/// 9. Calls `on_checkpoint_end`, then releases checkpoint locks
///
/// Passive mode defers step 1 until publish and runs collection/write concurrently; the durable
/// outcome (WAL backfill, log truncate, metadata) is the same.
pub struct CheckpointStateMachine<Clock: LogicalClock, A: ConcurrentAllocator = TursoAllocator> {
    /// The current state of the state machine
    state: CheckpointState,
    /// The states of the locks held by the state machine - these are tracked for error handling so that they are
    /// released if the state machine fails.
    lock_states: LockStates,
    /// The highest transaction ID that has been made durable in the WAL in a previous checkpoint.
    durable_txid_max_old: Option<NonZeroU64>,
    /// The highest transaction ID that will be made durable in the WAL in the current checkpoint.
    durable_txid_max_new: u64,
    /// Pager used for writing to the B-tree
    pager: Arc<Pager>,
    /// MVCC store containing the row versions.
    mvstore: Arc<MvStore<Clock, A>>,
    /// Connection to the database
    connection: Arc<Connection>,
    /// Database whose pager and schema this checkpoint is writing.
    database: Arc<Database>,
    database_id: usize,
    #[cfg(any(test, injected_yields))]
    yield_instance_id: u64,
    /// Lock used to block other transactions from running during the checkpoint
    checkpoint_lock: Arc<TursoRwLock>,
    /// All committed versions to write to the B-tree.
    /// In the case of CREATE TABLE / DROP TABLE ops, contains a [SpecialWrite] to create/destroy the B-tree.
    write_set: Vec<(RowVersion, Option<SpecialWrite>)>,
    /// State machine for writing rows to the B-tree
    write_row_state_machine: Option<StateMachine<WriteRowStateMachine>>,
    /// State machine for deleting rows from the B-tree
    delete_row_state_machine: Option<StateMachine<DeleteRowStateMachine>>,
    /// Cursors for the B-trees
    cursors: HashMap<u64, Arc<RwLock<BTreeCursor>>>,
    /// Tables or indexes that were created in this checkpoint
    /// key is the rowid in the sqlite_schema table
    created_btrees: HashMap<i64, (MVTableId, RowVersion)>,
    /// Tables that were destroyed in this checkpoint
    destroyed_tables: HashSet<MVTableId>,
    /// Indexes that were destroyed in this checkpoint
    destroyed_indexes: HashSet<MVTableId>,
    /// Index row changes to write: (index_id, row_version, is_delete)
    index_write_set: Vec<(MVTableId, RowVersion, bool)>,
    /// Map from index_id to Index struct (for creating cursors)
    /// This is populated when we process sqlite_schema rows for indexes
    index_id_to_index: HashMap<MVTableId, Arc<Index>>,
    /// Result of the checkpoint
    checkpoint_result: Option<CheckpointResult>,
    /// Update connection's transaction state on checkpoint. If checkpoint was called as automatic
    /// process in a transaction we don't want to change the state as we assume we are already on a
    /// write transaction and any failure will be cleared on vdbe error handling.
    update_transaction_state: bool,
    /// The synchronous mode for fsync operations. When set to Off, fsync is skipped.
    sync_mode: SyncMode,
    /// Checkpoint mode. `should_restart_log()` (Truncate/Restart) gates the WAL
    /// truncation in `TruncateWal`; Passive leaves the WAL non-empty (restart-on-write).
    mode: CheckpointMode,
    /// Internal metadata table info for persisting `persistent_tx_ts_max` atomically with pager commit.
    mvcc_meta_table: Option<(MVTableId, usize)>,
    /// File-backed databases must persist replay boundary durably.
    durable_mvcc_metadata: bool,
    /// Header staged into pager page 1 before commit; published to global_header on success.
    staged_checkpoint_header: Option<DatabaseHeader>,
    /// Guard to avoid restaging page 1 across CommitPagerTxn async retries.
    header_staged_for_commit: bool,
    /// Set after `pager.commit_tx` succeeds; the publish window may still be pending (auto passive
    /// retries acquiring the brief write lock without re-committing).
    pager_commit_done: bool,
    /// Root-map ops staged during collection; applied at publish.
    pending_rootmap_ops: Vec<RootMapOp>,
    /// Roots allocated this checkpoint; resolved until publish.
    pending_alloc_roots: std::collections::HashMap<MVTableId, u64>,
    collect_table_cursor: Option<RowID>,
    collect_index_tableid_cursor: Option<MVTableId>,
    collect_index_key_cursor: Option<Arc<SortableIndexKey>>,
    /// Async driver for `CheckpointState::CompactSequences`. Lazily set
    /// on first entry to that state; cleared when the driver completes.
    seq_compact: Option<SeqCompactDriver<Clock, A>>,
    /// Sequence deletes recorded in passive mode; applied in the publish window.
    pending_seq_deletes: Vec<(RowID, usize)>,
    /// Collection upper bound (`last_committed_tx_ts` at snapshot). `u64::MAX` = no bound.
    snapshot_ts: u64,
    build_local_schema_sm: Option<StateMachine<BuildLocalSchemaViewStateMachine<Clock, A>>>,
    build_local_schema_began_read_tx: bool,
    /// Snapshot-consistent schema built at `snapshot_ts`; drives `index_id_to_index` in PASSIVE mode.
    local_schema: Option<Arc<Schema>>,
    owns_checkpoint_in_progress: bool,
    /// Roots allocated this checkpoint; published with `visible_from = durable_txid_max_new`.
    staged_roots: Vec<MVTableId>,
    /// Positive roots materialized this pass whose schema version was already ended
    /// (e.g. DROP after our snapshot). Tracked in `dropped_root_pages` until a later
    /// checkpoint frees the btree.
    ended_created_roots: HashSet<i64>,
    /// Positive roots whose btrees were destroyed in this checkpoint's pager write.
    /// Only these may be removed from `dropped_root_pages` at publish.
    freed_root_pages: HashSet<i64>,
    /// Table rowids whose pager write or delete finished this checkpoint.
    /// GC stamps only these, after pager commit. Stamping a skip-write makes
    /// scans fall through to a B-tree that never got the row.
    written_table_rowids: HashSet<(MVTableId, i64)>,
    /// `index_write_set` slots whose pager write or delete finished. Slots, not
    /// keys: `SortableIndexKey` is not `Hash`.
    written_index_slots: HashSet<usize>,
}

/// One pending compaction job in the per-checkpoint sequence sweep.
#[derive(Debug, Clone, Copy)]
struct SeqCompaction {
    backing_root: i64,
    backing_num_cols: usize,
    /// MVCC table id derived from `backing_root` at sweep-plan time.
    /// Cached here so the per-row purge in `ScanDelete` doesn't re-scan
    /// `mvstore.table_id_to_rootpage` on every deletion. The driver uses
    /// it together with the deleted row's `value` (= rowid alias) to
    /// build the `RowID` it passes to
    /// `MvStore::purge_row_versions_during_checkpoint`.
    table_id: MVTableId,
    /// `true` for ascending sequences (watermark = `Last`), `false` for
    /// descending (watermark = `Rewind`). Direction-aware because the
    /// "current value" of a sequence is the max for ascending and the
    /// min for descending — keeping the wrong end as the watermark
    /// after compaction would lose the last emitted value across
    /// restart.
    increment_positive: bool,
}

/// Per-row scan phase within `SeqCompactDriver`. Each backing table is
/// walked end-to-end: a watermark seek (Last/Rewind) followed by a
/// from-start scan that deletes every row whose key (= value = rowid,
/// since `value` is `INTEGER PRIMARY KEY`) differs from the watermark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SeqCompactPhase {
    /// `cursor.last()` for ascending, `cursor.rewind()` for descending.
    /// Yields IOResult::IO on page reads.
    SeekWatermark,
    /// `cursor.rowid()` to capture the watermark key. Yields IO on
    /// further reads. If the cursor has no record (empty backing
    /// table), advances to next sequence without scanning.
    ReadWatermarkRowid,
    /// `cursor.rewind()` to start the from-start scan. Yields IO.
    ScanRewind,
    /// `cursor.rowid()` on the current scan row. Yields IO.
    ScanReadRowid,
    /// `cursor.delete()` when the current row's key differs from the
    /// watermark. Yields IO.
    ScanDelete,
    /// `cursor.next()` to advance the scan. Yields IO. Re-enters
    /// `ScanReadRowid` when the cursor still has a record, otherwise
    /// advances to the next sequence.
    ScanNext,
}

/// Walks each pending sequence backing table and deletes every row
/// that is not the current watermark. Pure `IOResult` plumbing — every
/// cursor op yields up to the caller on page IO, so a `step()` call
/// from inside the checkpoint state machine can propagate a yield
/// upward without ever blocking the executor.
///
/// Generic over `Clock` so it can hold an `Arc<MvStore<Clock, A>>` — the
/// driver paired-deletes from the B-tree (via the cursor) AND from the
/// MVCC version chain (via `purge_row_versions_during_checkpoint`) so
/// the two layers stay consistent. Skipping the version-chain purge
/// would leave entries with `btree_resident: true` pointing at B-tree
/// rows that no longer exist, surviving until `drop_unused_row_versions`
/// Rule 3 catches up.
struct SeqCompactDriver<Clock: LogicalClock, A: ConcurrentAllocator = TursoAllocator> {
    /// Remaining backing tables to compact, in arbitrary order.
    pending: Vec<SeqCompaction>,
    /// Index of the in-flight compaction within `pending`.
    current_idx: usize,
    /// Cursor on the in-flight backing table. Constructed on entry to
    /// `SeekWatermark`, dropped on transition to the next sequence.
    cursor: Option<BTreeCursor>,
    /// Current scan phase for the in-flight backing table.
    phase: SeqCompactPhase,
    /// The watermark key captured in `ReadWatermarkRowid`. The scan
    /// keeps the row at this key and deletes all others.
    watermark_key: Option<i64>,
    /// Row key (= `value` column, since `value INTEGER PRIMARY KEY`)
    /// captured in `ScanReadRowid` when we decide the current row must
    /// go. Consumed by `ScanDelete` AFTER `cursor.delete()` returns
    /// `Done` to build the matching `RowID` for the MVCC purge call.
    /// Stored on the driver (not as a phase payload) so a yield mid-
    /// `cursor.delete()` doesn't lose the rowid across re-entry.
    pending_delete_rowid: Option<i64>,
    /// Cached pager handle so cursor construction matches the original
    /// `BTreeCursor::new_table` signature.
    pager: Arc<Pager>,
    /// MVCC store used to purge version-chain entries paired with each
    /// B-tree delete. See the struct-level comment for the invariant.
    mvstore: Arc<MvStore<Clock, A>>,
    /// Passive mode: record deletes for `seqcompact_commit_delete` instead of purging inline.
    passive: bool,
    /// Rows recorded for deletion in passive mode (drained by the checkpoint
    /// state machine into its publish window).
    compacted: Vec<(RowID, usize)>,
}

#[cfg(any(test, injected_yields))]
impl<Clock: LogicalClock, A: ConcurrentAllocator> ProvidesYieldContext
    for CheckpointStateMachine<Clock, A>
{
    fn yield_context(&self) -> YieldContext {
        YieldContext::new(
            self.connection.yield_injector(),
            self.connection.failure_injector(),
            self.yield_instance_id,
            checkpoint_yield_key(),
        )
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
/// Special writes for CREATE TABLE / DROP TABLE / CREATE INDEX / DROP INDEX ops.
/// These are used to create/destroy B-trees during pager ops.
pub enum SpecialWrite {
    BTreeCreate {
        table_id: MVTableId,
        sqlite_schema_rowid: i64,
    },
    BTreeDestroy {
        table_id: MVTableId,
        root_page: u64,
        num_columns: usize,
    },
    BTreeCreateIndex {
        index_id: MVTableId,
        sqlite_schema_rowid: i64,
    },
    BTreeDestroyIndex {
        index_id: MVTableId,
        root_page: u64,
        num_columns: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqliteSchemaBtreeKind {
    Table,
    Index,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqliteSchemaBtreeIdentity {
    kind: SqliteSchemaBtreeKind,
    pub root_page: i64,
}

/// Identity of a sqlite_schema row version that refers to a B-tree-backed object.
/// Schema rewrites that preserve this identity are metadata-only and should not be
/// treated as create/drop lifecycle changes.
#[aristo::intent(
    "Recovery never replays a sqlite_schema delete as an empty payload; it keeps the row's pre-delete record, so every recovered schema row still has a decodable (type, rootpage)",
    id = "mvcc_recovered_schema_record_wellformed",
    verify = "full"
)]
pub fn sqlite_schema_btree_identity(version: &RowVersion) -> Option<SqliteSchemaBtreeIdentity> {
    if version.row.id.table_id != SQLITE_SCHEMA_MVCC_TABLE_ID {
        return None;
    }

    // Recovery can synthesize payload-less sqlite_schema tombstones when the
    // pre-delete record is no longer available. Those versions do not carry
    // enough information to recover B-tree identity.
    if version.row.payload().is_empty() {
        return None;
    }

    let row_data = ImmutableRecordRef::from_bin_record(version.row.payload());
    let Ok((col0, col3)) = row_data.get_two_values(0, 3) else {
        return None;
    };

    let kind = match col0 {
        ValueRef::Text(type_str) => match type_str.as_str() {
            "table" => SqliteSchemaBtreeKind::Table,
            "index" => SqliteSchemaBtreeKind::Index,
            _ => return None,
        },
        _ => panic!("sqlite_schema.type column must be TEXT, got {col0:?}"),
    };

    let ValueRef::Numeric(Numeric::Integer(root_page)) = col3 else {
        panic!("sqlite_schema.rootpage column must be INTEGER, got {col3:?}");
    };

    if root_page == 0 {
        return None;
    }

    Some(SqliteSchemaBtreeIdentity { kind, root_page })
}

fn sqlite_schema_versions_refer_to_btree(lhs: &RowVersion, rhs: &RowVersion) -> bool {
    sqlite_schema_btree_identity(lhs)
        .zip(sqlite_schema_btree_identity(rhs))
        .is_some_and(|(lhs_id, rhs_id)| lhs_id == rhs_id)
}

/// A single `sqlite_schema` rowid can be reused across multiple row versions. Some of those
/// transitions are metadata-only rewrites of the same B-tree object, while others represent a
/// real change that mutates the BTREE.
///
/// Checkpoint needs to preserve ended schema versions only for the types of changes so it can
/// register destroyed tables/indexes and skip stale recovered rows. Same-object rewrites, such as
/// `ALTER TABLE ... RENAME COLUMN`, must collapse to the latest version; otherwise checkpoint
/// treats one schema row chain as a DROP+CREATE pair and emits duplicate work for the same rowid.
fn is_schema_metadata_only_rewrite(current: &RowVersion, next: Option<&RowVersion>) -> bool {
    if current.end().is_none() {
        return false;
    }

    let Some(_current_identity) = sqlite_schema_btree_identity(current) else {
        return false;
    };

    match next {
        Some(next) => !sqlite_schema_versions_refer_to_btree(current, next),
        None => true,
    }
}

impl<Clock: LogicalClock, A: ConcurrentAllocator> SeqCompactDriver<Clock, A> {
    /// Drive one step of the compaction sweep. Returns `IOResult::IO` on
    /// any cursor page IO so the caller can yield up; returns
    /// `IOResult::Done(())` when every pending backing table has been
    /// compacted to its single watermark row.
    fn step(&mut self) -> IOResultOr<()> {
        loop {
            let Some(seq) = self.pending.get(self.current_idx).copied() else {
                return Ok(IOResult::Done(()));
            };
            if self.cursor.is_none() {
                self.cursor = Some(BTreeCursor::new_table(
                    self.pager.clone(),
                    seq.backing_root,
                    seq.backing_num_cols,
                ));
                self.phase = SeqCompactPhase::SeekWatermark;
                self.watermark_key = None;
                self.pending_delete_rowid = None;
            }
            let cursor = self
                .cursor
                .as_mut()
                .expect("cursor must be set in active compaction");
            match self.phase {
                SeqCompactPhase::SeekWatermark => {
                    let r = if seq.increment_positive {
                        cursor.last()?
                    } else {
                        cursor.rewind()?
                    };
                    if let IOResult::IO(io) = r {
                        return Ok(IOResult::IO(io));
                    }
                    self.phase = SeqCompactPhase::ReadWatermarkRowid;
                }
                SeqCompactPhase::ReadWatermarkRowid => {
                    let r = cursor.rowid()?;
                    let key = match r {
                        IOResult::IO(io) => return Ok(IOResult::IO(io)),
                        IOResult::Done(opt) => opt,
                    };
                    match key {
                        Some(k) => {
                            self.watermark_key = Some(k);
                            self.phase = SeqCompactPhase::ScanRewind;
                        }
                        None => {
                            // Empty backing table — nothing to compact.
                            self.advance_to_next_sequence();
                        }
                    }
                }
                SeqCompactPhase::ScanRewind => {
                    let r = cursor.rewind()?;
                    if let IOResult::IO(io) = r {
                        return Ok(IOResult::IO(io));
                    }
                    self.phase = SeqCompactPhase::ScanReadRowid;
                }
                SeqCompactPhase::ScanReadRowid => {
                    let r = cursor.rowid()?;
                    let key = match r {
                        IOResult::IO(io) => return Ok(IOResult::IO(io)),
                        IOResult::Done(opt) => opt,
                    };
                    match key {
                        Some(k) if Some(k) == self.watermark_key => {
                            self.phase = SeqCompactPhase::ScanNext;
                        }
                        Some(k) => {
                            if self.passive {
                                // Passive: don't touch the B-tree or purge the chain here (the
                                // purge contract needs the lock). Record the row; the publish
                                // window turns it into a proper end-stamped delete and a later
                                // checkpoint materializes the physical delete.
                                self.compacted.push((
                                    RowID {
                                        table_id: seq.table_id,
                                        row_id: RowKey::Int(k),
                                    },
                                    seq.backing_num_cols,
                                ));
                                self.phase = SeqCompactPhase::ScanNext;
                            } else {
                                // Stash the key for the paired MVCC purge that
                                // ScanDelete performs after `cursor.delete()`
                                // returns Done. This branch and the transition
                                // are synchronous (no yield between them), so
                                // ScanDelete is guaranteed to observe
                                // `pending_delete_rowid = Some(k)` on the
                                // current iteration.
                                self.pending_delete_rowid = Some(k);
                                self.phase = SeqCompactPhase::ScanDelete;
                            }
                        }
                        None => {
                            self.advance_to_next_sequence();
                        }
                    }
                }
                SeqCompactPhase::ScanDelete => {
                    let r = cursor.delete()?;
                    if let IOResult::IO(io) = r {
                        return Ok(IOResult::IO(io));
                    }
                    // Pair the B-tree delete with the MVCC version-chain
                    // purge so a snapshot reader can't observe the row
                    // via `RowVersion.row` after the B-tree row is gone.
                    // `pending_delete_rowid` was set in ScanReadRowid for
                    // the row the cursor was on when we entered this
                    // phase; consume it with `take()` so it doesn't leak
                    // into the next scan iteration. See the
                    // `purge_row_versions_during_checkpoint` doc comment
                    // for the caller contract this depends on
                    // (pager_commit_lock serializing nextval allocators).
                    let rowid = self
                        .pending_delete_rowid
                        .take()
                        .expect("pending_delete_rowid must be set when ScanDelete completes");
                    self.mvstore.purge_row_versions_during_checkpoint(RowID {
                        table_id: seq.table_id,
                        row_id: RowKey::Int(rowid),
                    });
                    // After Delete the cursor is positioned at the slot
                    // the deleted row used to occupy; the next Next
                    // advances to the following row.
                    self.phase = SeqCompactPhase::ScanNext;
                }
                SeqCompactPhase::ScanNext => {
                    let r = cursor.next()?;
                    if let IOResult::IO(io) = r {
                        return Ok(IOResult::IO(io));
                    }
                    // `cursor.next()` leaves `has_record()` false at EOF
                    // and the next `rowid()` returns Done(None); rely on
                    // ScanReadRowid's None branch to advance.
                    self.phase = SeqCompactPhase::ScanReadRowid;
                }
            }
        }
    }

    fn advance_to_next_sequence(&mut self) {
        self.cursor = None;
        self.watermark_key = None;
        self.pending_delete_rowid = None;
        self.current_idx += 1;
        self.phase = SeqCompactPhase::SeekWatermark;
    }
}

impl<Clock: LogicalClock, A: ConcurrentAllocator> CheckpointStateMachine<Clock, A> {
    /// Build the per-checkpoint list of non-CYCLE sequence backing tables
    /// to compact. CYCLE seqs are skipped — they keep themselves at one
    /// row via inline compaction in the nextval bytecode and using
    /// MAX/MIN-based compaction after a wrap would lose the post-wrap
    /// "current" value. Reads the live root page from the MVCC store
    /// when the schema still carries the uncheckpointed-negative
    /// sentinel; tables that have no real root yet (created and
    /// immediately dropped in this checkpoint) are filtered out.
    fn pending_sequence_compactions(&self) -> Result<Vec<SeqCompaction>> {
        let resolve_root = |schema_root: i64| -> Option<i64> {
            if schema_root > 0 {
                return Some(schema_root);
            }
            if schema_root == 0 {
                return None;
            }
            let table_id = self.mvstore.get_table_id_from_root_page(schema_root);
            self.mvstore
                .current_root_page(&table_id)
                .map(|rp| rp as i64)
        };
        let db_id = crate::MAIN_DB_ID;
        Ok(self
            .connection
            .with_schema(db_id, |schema| {
                crate::without_allocation_faults!(
                    // Checkpoint has already written table/index rows by the time sequence
                    // compaction setup runs. An injected fault here can abort before the
                    // checkpoint reaches its normal pager cleanup/retry path.
                    // TODO: make sequence compaction setup resumable before re-enabling
                    // fault injection for this collection.
                    schema
                        .sequences
                        .values()
                        .filter(|seq| !seq.cycle)
                        .filter_map(|seq| {
                            let backing_name =
                                crate::translate::sequence::sequence_backing_table_name(&seq.name);
                            let bt = schema.get_btree_table(&backing_name)?;
                            let backing_root = resolve_root(bt.root_page)?;
                            // Resolve the MVCC table_id once per sequence so the
                            // per-row purge in `ScanDelete` doesn't re-scan
                            // `table_id_to_rootpage` on every deletion. Uses the
                            // schema-side root (pre-resolve) because
                            // `get_table_id_from_root_page` already understands
                            // the negative uncheckpointed-sentinel encoding.
                            let table_id = self.mvstore.get_table_id_from_root_page(bt.root_page);
                            Some(SeqCompaction {
                                backing_root,
                                backing_num_cols: bt.columns().len(),
                                table_id,
                                increment_positive: seq.increment_by >= 0,
                            })
                        })
                        .try_collect()
                )
            })
            .expect(ALLOC_ERR_MSG))
    }

    fn refresh_checkpoint_bounds(&mut self) {
        let durable_tx_max = self.mvstore.durable_txid_max.load(Ordering::SeqCst);
        self.durable_txid_max_old = NonZeroU64::new(durable_tx_max);
        self.durable_txid_max_new = durable_tx_max;
    }

    fn refresh_schema_metadata(&mut self) {
        let schema = self.connection.clone_shared_schema(self.database_id);
        self.index_id_to_index = schema
            .indexes
            .values()
            .flatten()
            .filter(|index| index.is_btree_backed())
            .map(|index| {
                turso_assert!(index.root_page != 0, "index root_page must be non-zero");
                (
                    self.mvstore.get_table_id_from_root_page(index.root_page),
                    index.clone(),
                )
            })
            .collect();
        self.mvcc_meta_table = schema.get_btree_table(MVCC_META_TABLE_NAME).map(|table| {
            turso_assert!(
                table.root_page != 0,
                "mvcc meta table root_page must be non-zero"
            );
            (
                self.mvstore.get_table_id_from_root_page(table.root_page),
                table.columns().len(),
            )
        });
        self.durable_mvcc_metadata =
            !self.database.is_in_memory_db() && self.mvcc_meta_table.is_some();
    }

    pub fn new(
        pager: Arc<Pager>,
        mvstore: Arc<MvStore<Clock, A>>,
        connection: Arc<Connection>,
        update_transaction_state: bool,
        sync_mode: SyncMode,
        database_id: usize,
        mode: CheckpointMode,
    ) -> Self {
        assert!(
            !matches!(mode, CheckpointMode::Passive { .. }) || mvstore.uses_passive_checkpoint(),
            "passive checkpoint mode requires experimental_mvcc_passive_checkpoint"
        );
        // MVCC supports only Passive (no blocking lock, requires the experimental flag) and
        // Truncate (blocking). Full/Restart map to Truncate — the pre-feature baseline
        // always checkpointed via TRUNCATE.
        let mode = match mode {
            CheckpointMode::Passive { .. } | CheckpointMode::Truncate { .. } => mode,
            CheckpointMode::Full | CheckpointMode::Restart => CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        };
        let checkpoint_lock = mvstore.blocking_checkpoint_lock.clone();
        let database = connection.get_source_database(database_id);
        // Use the shared DB schema (not the per-connection cache, which may be
        // stale) for the database whose pager we're checkpointing. Unlike WAL
        // mode, MVCC checkpoint writes from the mv store back to the pager —
        // so the schema must match the pager being checkpointed.
        let schema = connection.clone_shared_schema(database_id);
        let index_id_to_index = if mvstore.uses_passive_checkpoint() {
            HashMap::default()
        } else {
            schema
                .indexes
                .values()
                .flatten()
                .filter(|index| index.is_btree_backed())
                .map(|index| {
                    turso_assert!(index.root_page != 0, "index root_page must be non-zero");
                    (
                        mvstore.get_table_id_from_root_page(index.root_page),
                        index.clone(),
                    )
                })
                .collect()
        };

        let mvcc_meta_table = schema.get_btree_table(MVCC_META_TABLE_NAME).map(|table| {
            turso_assert!(
                table.root_page != 0,
                "mvcc meta table root_page must be non-zero"
            );
            (
                mvstore.get_table_id_from_root_page(table.root_page),
                table.columns().len(),
            )
        });
        let durable_mvcc_metadata = !database.is_in_memory_db() && mvcc_meta_table.is_some();
        let durable_tx_max = mvstore.durable_txid_max.load(Ordering::SeqCst);
        let durable_txid_max_old = NonZeroU64::new(durable_tx_max);
        #[cfg(any(test, injected_yields))]
        let yield_instance_id = connection.next_yield_instance_id();
        Self {
            state: CheckpointState::PrepareCheckpoint,
            lock_states: LockStates {
                blocking_checkpoint_lock_held: false,
                pager_read_tx: false,
                pager_write_tx: false,
            },
            pager,
            durable_txid_max_old,
            durable_txid_max_new: durable_tx_max,
            mvstore,
            connection,
            database,
            database_id,
            #[cfg(any(test, injected_yields))]
            yield_instance_id,
            checkpoint_lock,
            write_set: crate::alloc::vec![],
            write_row_state_machine: None,
            delete_row_state_machine: None,
            cursors: HashMap::default(),
            created_btrees: HashMap::default(),
            destroyed_tables: HashSet::default(),
            destroyed_indexes: HashSet::default(),
            index_write_set: crate::alloc::vec![],
            index_id_to_index,
            checkpoint_result: None,
            update_transaction_state,
            sync_mode,
            mode,
            mvcc_meta_table,
            durable_mvcc_metadata,
            staged_checkpoint_header: None,
            header_staged_for_commit: false,
            pager_commit_done: false,
            pending_rootmap_ops: crate::alloc::vec![],
            pending_alloc_roots: std::collections::HashMap::new(),
            collect_table_cursor: None,
            collect_index_tableid_cursor: None,
            collect_index_key_cursor: None,
            seq_compact: None,
            pending_seq_deletes: crate::alloc::vec![],
            // Set in PrepareCheckpoint once the collection snapshot is taken; until
            // then `u64::MAX` disables the upper-bound filter (collect everything).
            snapshot_ts: u64::MAX,
            build_local_schema_sm: None,
            build_local_schema_began_read_tx: false,
            local_schema: None,
            owns_checkpoint_in_progress: false,
            staged_roots: crate::alloc::vec![],
            ended_created_roots: HashSet::default(),
            freed_root_pages: HashSet::default(),
            written_table_rowids: HashSet::default(),
            written_index_slots: HashSet::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn state_for_test(&self) -> CheckpointState {
        self.state
    }

    #[cfg(test)]
    pub(crate) fn checkpoint_bounds_for_test(&self) -> (Option<u64>, u64) {
        (
            self.durable_txid_max_old.map(u64::from),
            self.durable_txid_max_new,
        )
    }

    /// Drop checkpoint locks. Call only after `on_checkpoint_end`.
    fn release_checkpoint_locks_if_needed(&mut self) {
        if self.lock_states.blocking_checkpoint_lock_held {
            tracing::debug!("Releasing blocking checkpoint lock");
            self.checkpoint_lock.unlock();
            self.lock_states.blocking_checkpoint_lock_held = false;
        }
        if self.owns_checkpoint_in_progress {
            self.mvstore
                .checkpoint_in_progress
                .store(false, Ordering::Release);
            self.owns_checkpoint_in_progress = false;
        }
    }

    /// Cleanup path for I/O errors that happen while waiting on completions outside
    /// of `step()`. This mirrors `step()` error handling and also resets pager/WAL
    /// checkpoint bookkeeping.
    pub fn cleanup_after_external_io_error(&mut self, err: LimboError) -> Result<()> {
        // run storage cleanup within proper checkpoint context (e.g. pager has pending read/write txn)
        let result = self.mvstore.storage.on_checkpoint_end(Err(err));

        // Drop this checkpoint's staged (not-yet-applied) root-map mutations. They were never
        // written to the shared map (deferred to the post-commit publish window), so a failed
        // checkpoint simply discards them — no revert, no divergence. A post-commit failure
        // finds these already drained, so nothing is dropped.
        self.pending_rootmap_ops.clear();
        self.pending_alloc_roots.clear();

        if self.lock_states.pager_write_tx {
            self.pager.rollback_tx(self.connection.as_ref());
            if self.update_transaction_state {
                self.connection.set_tx_state(TransactionState::None);
            }
            self.lock_states.pager_write_tx = false;
            self.lock_states.pager_read_tx = false;
        } else if self.lock_states.pager_read_tx {
            self.pager.end_read_tx();
            if self.update_transaction_state {
                self.connection.set_tx_state(TransactionState::None);
            }
            self.lock_states.pager_read_tx = false;
        }

        // MVCC checkpointing drives WAL checkpoint directly; on errors we must
        // explicitly reset both pager and WAL checkpoint states.
        self.pager.clear_checkpoint_state();
        if let Some(wal) = self.pager.wal.as_ref() {
            wal.abort_checkpoint();
        }

        self.release_checkpoint_locks_if_needed();

        result
    }

    /// Returns all checkpointable [RowVersion]s for that `table_id`
    fn maybe_get_checkpointable_versions(
        &self,
        versions: &[RowVersion],
        table_id: MVTableId,
    ) -> smallvec::SmallVec<[RowVersion; 1]> {
        let mut versions_to_checkpoint: smallvec::SmallVec<[_; 1]> =
            smallvec::SmallVec::with_capacity(1);
        let mut exists_in_db_file = false;
        // Iterate versions from oldest-to-newest to determine if the row exists in the database file and whether the newest version should be checkpointed.
        for version in versions.iter() {
            // A row is in the database file if:
            // There is a version whose begin timestamp is <= than the last checkpoint timestamp, AND
            // There is NO version whose END timestamp is <= than the last checkpoint timestamp.
            // Resolve in-flight TxID begin/end markers to the owning tx's true state
            // (concurrent collection may not have rewritten the chain to a Timestamp yet).
            // Only a Committed tx contributes a timestamp; others resolve to None.
            let begin_ts = match version.begin() {
                Some(TxTimestampOrID::Timestamp(e)) => Some(e),
                Some(TxTimestampOrID::TxID(t)) => {
                    match lookup_tx_state(&self.mvstore.txs, &self.mvstore.finalized_tx_states, t) {
                        Some(crate::mvcc::database::TransactionState::Committed(ts)) => Some(ts),
                        _ => None,
                    }
                }
                None => None,
            };
            let mut end_ts = match version.end() {
                Some(TxTimestampOrID::Timestamp(e)) => Some(e),
                Some(TxTimestampOrID::TxID(t)) => {
                    match lookup_tx_state(&self.mvstore.txs, &self.mvstore.finalized_tx_states, t) {
                        Some(crate::mvcc::database::TransactionState::Committed(ts)) => Some(ts),
                        _ => None,
                    }
                }
                None => None,
            };
            // Insert not visible at our snapshot (committed during the collection
            // phase): defer to the next pass and don't let it affect DB-file existence now.
            if begin_ts.is_some_and(|b| b > self.snapshot_ts) {
                continue;
            }
            // Tombstone committed after our snapshot: clamp to "live" (end=None) so the
            // row is checkpointed as PRESENT, not stranded; a later pass (delete <=
            // snapshot) checkpoints the deletion. Fixes the future-tombstone orphan bug.
            let future_committed_tombstone = end_ts.is_some_and(|e| e > self.snapshot_ts);
            if future_committed_tombstone {
                end_ts = None;
            }
            if begin_ts.is_none() && end_ts.is_none() {
                // Rolled-back garbage and active TxID-only placeholders are not part of
                // the durable row history, so they must not influence DB-file existence.
                continue;
            }
            // Rows marked btree_resident existed in the DB file before MVCC tracked them.
            // This also applies to synthetic tombstones that use begin=None.
            if version.btree_resident {
                exists_in_db_file = true;
            }
            // A row exists in the DB file if it was checkpointed in a previous checkpoint.
            // For btree_resident rows we seed exists_in_db_file above, regardless of begin encoding.
            // These timestamp-derived transitions must run after the btree_resident seed so a
            // checkpointed tombstone can clear DB-file existence on a retry checkpoint.
            if self
                .durable_txid_max_old
                .is_some_and(|txid_max_old| begin_ts.is_some_and(|b| b <= u64::from(txid_max_old)))
            {
                exists_in_db_file = true;
            }
            if self
                .durable_txid_max_old
                .is_some_and(|txid_max_old| end_ts.is_some_and(|e| e <= u64::from(txid_max_old)))
            {
                exists_in_db_file = false;
            }
            // Should checkpoint the newest version if:
            // - It is not a delete and it hasn't been checkpointed yet OR (begin_ts > max_old)
            // We need the `self.durable_txid_max_old.is_none()` check because before
            // the first checkpoint there is no persisted MVCC watermark.
            let is_uncheckpointed_insert = end_ts.is_none()
                && self.durable_txid_max_old.is_none_or(|txid_max_old| {
                    begin_ts.is_some_and(|b| b > u64::from(txid_max_old))
                });
            // - It is a delete, AND some version of the row exists in the database file.
            let is_delete_and_exists_in_db_file = end_ts.is_some() && exists_in_db_file;
            // - It is a delete of an uncheckpointed sqlite_schema row for a
            //   table or index. The schema row itself is not in the DB file, but
            //   checkpoint still needs it so it can remember that the table or
            //   index was destroyed and skip that object's data/index rows.
            //   Views and triggers have rootpage=0, so their uncheckpointed
            //   deletes can be ignored here: there is no B-tree to destroy, and
            //   deleting a missing sqlite_schema row would be wrong.
            let is_schema_delete = table_id == SQLITE_SCHEMA_MVCC_TABLE_ID
                && !exists_in_db_file
                && sqlite_schema_btree_identity(version).is_some()
                && self
                    .durable_txid_max_old
                    .is_none_or(|txid_max_old| end_ts.is_some_and(|e| e > u64::from(txid_max_old)));
            let should_checkpoint =
                is_uncheckpointed_insert || is_delete_and_exists_in_db_file || is_schema_delete;
            if should_checkpoint {
                // Future tombstone: push a clamped clone so the B-tree write sees a live insert.
                let checkpoint_version = if future_committed_tombstone {
                    let mut v = version.clone();
                    v.set_end(None);
                    v
                } else {
                    let mut version = version.clone();
                    // Normalize the clone's end to the resolved end_ts so downstream
                    // is_delete is consistent (in-flight/aborted end -> live insert).
                    version.set_end(end_ts.map(TxTimestampOrID::Timestamp));
                    version
                };
                if table_id != SQLITE_SCHEMA_MVCC_TABLE_ID {
                    if versions_to_checkpoint.is_empty() {
                        versions_to_checkpoint.push(checkpoint_version)
                    } else {
                        versions_to_checkpoint[0] = checkpoint_version
                    }
                    continue;
                }

                if let Some(previous_version) = versions_to_checkpoint.last() {
                    let should_drop_previous = previous_version.end().is_some()
                        && !is_schema_metadata_only_rewrite(previous_version, Some(version));
                    if should_drop_previous {
                        versions_to_checkpoint.pop();
                    }
                }

                versions_to_checkpoint.push(checkpoint_version);
            }
        }

        versions_to_checkpoint
    }

    /// Resolve the table/index id whose B-tree binding owned `root_page` at the moment of the
    /// drop carried by `version` (a `sqlite_schema` DELETE). Selects the binding that COVERS the
    /// drop timestamp (`begin < drop_ts <= end`) rather than "any live binding at this root":
    /// under concurrent page reuse a freed root page can already belong to a newer live object, and
    /// destroying that one would corrupt a live btree. Returns `None` when no binding owned the
    /// page at the drop ts — the object was already destroyed by an earlier checkpoint and this
    /// schema-delete version merely lingered in the store and got recollected.
    fn resolve_dropped_binding(&self, root_page: u64, version: &RowVersion) -> Option<MVTableId> {
        let drop_ts = match version.end() {
            Some(TxTimestampOrID::Timestamp(t)) => t,
            _ => self.snapshot_ts,
        };
        self.mvstore
            .table_id_to_rootpage
            .iter()
            .find(|entry| {
                let e = entry.value();
                e.root_page == Some(root_page) && e.begin < drop_ts && drop_ts <= e.end
            })
            .map(|entry| *entry.key())
    }

    /// Resolve a table/index root during collection (`pending_alloc_roots` or shared map).
    fn resolve_checkpoint_root(&self, table_id: MVTableId) -> Option<u64> {
        self.pending_alloc_roots
            .get(&table_id)
            .copied()
            .or_else(|| self.mvstore.current_root_page(&table_id))
    }

    fn table_exists_for_snapshot(&self, table_id: MVTableId) -> bool {
        if self.pending_alloc_roots.contains_key(&table_id) {
            return true;
        }
        self.mvstore
            .table_id_to_rootpage
            .get(&table_id)
            .is_some_and(|entry| {
                let e = entry.value();
                e.root_page.is_some() && e.covers(self.snapshot_ts)
            })
    }

    /// Stage a newly-created root for deferred publication. The write phase resolves it via
    /// [`Self::resolve_checkpoint_root`]; it is inserted into the shared map only at commit.
    fn ckpt_rootmap_alloc(&mut self, table_id: MVTableId, root_page: u64) {
        self.pending_alloc_roots.insert(table_id, root_page);
        self.pending_rootmap_ops.push(RootMapOp::Alloc {
            id: table_id,
            root: root_page,
        });
    }

    /// Stage a root-binding retirement for deferred application at commit.
    fn ckpt_rootmap_retire(&mut self, table_id: MVTableId, end_ts: u64) {
        self.pending_rootmap_ops.push(RootMapOp::Retire {
            id: table_id,
            end_ts,
        });
    }

    /// Stage a root-binding removal for deferred application at commit.
    fn ckpt_rootmap_remove(&mut self, table_id: MVTableId) {
        self.pending_rootmap_ops
            .push(RootMapOp::Remove { id: table_id });
    }

    /// Collect all committed versions that need to be written to the B-tree.
    /// We must only write to the B-tree if:
    /// 1. The row has not already been checkpointed in a previous checkpoint.
    ///    TODO: garbage collect row versions after checkpointing.
    /// 2. Either:
    ///    * The row is not a delete (we inserted or changed an existing row), OR
    ///    * The row is a delete AND it exists in the database file already.
    ///      If the row didn't exist in the database file and was deleted, we can simply not write it.
    fn collect_table_rows(&mut self) -> Result<Option<IOCompletions>> {
        // Invariant: RowID ordering is (table_id, row_id) with table_id ascending.
        // Since MV table IDs are negative and sqlite_schema is table_id=-1, iterating
        // in reverse visits sqlite_schema first so CREATE/DROP metadata is applied
        // before user-table rows in this checkpoint pass.
        let bounds: (Bound<RowID>, Bound<RowID>) = match self.collect_table_cursor.clone() {
            None => (Bound::Unbounded, Bound::Unbounded),
            Some(last) => (Bound::Unbounded, Bound::Excluded(last)),
        };
        let mut processed = 0;
        for entry in self.mvstore.rows.range(bounds).rev() {
            let key = entry.key();
            tracing::trace!("collecting {key:?}");
            self.collect_table_cursor = Some(key.clone());
            if self.destroyed_tables.contains(&key.table_id) {
                // We won't checkpoint rows for tables that will be destroyed in this checkpoint.
                // There's two forms of destroyed table:
                // 1. A non-checkpointed table that was created in the logical log and then destroyed. We don't need to do anything about this table in the pager/btree layer.
                // 2. A checkpointed table that was destroyed in the logical log. We need to destroy the btree in the pager/btree layer.
                tracing::trace!("skipping {key:?}");
                continue;
            }

            let row_versions = entry.value().read();

            for version in self.maybe_get_checkpointable_versions(&row_versions, key.table_id) {
                let is_delete = version.end().is_some();

                let mut special_write = None;
                // Set to true for schema deletes of never-checkpointed tables/indexes.
                // These don't need to be written to the B-tree, we just need to track them.
                let mut skip_write = false;

                if let Some(schema_identity) = sqlite_schema_btree_identity(&version) {
                    let root_page = schema_identity.root_page;
                    match schema_identity.kind {
                        SqliteSchemaBtreeKind::Index => {
                            // This is an index schema change
                            if is_delete {
                                // DROP INDEX
                                if root_page < 0 {
                                    // Index was never checkpointed - derive index_id directly from root_page.
                                    // No BTreeDestroyIndex needed since there's no physical B-tree.
                                    let index_id = MVTableId(root_page);
                                    self.destroyed_indexes.insert(index_id);
                                    // Defer the removal to the publish window. Pushing to the
                                    // staged op list borrows only that field, so it is allowed
                                    // inside the `self.mvstore.rows` iteration.
                                    self.pending_rootmap_ops
                                        .push(RootMapOp::Remove { id: index_id });
                                    skip_write = true;
                                } else if let Some(index_id) =
                                    self.resolve_dropped_binding(root_page as u64, &version)
                                {
                                    // DROP INDEX - index was checkpointed. Resolve the dropped
                                    // index from the binding that owned this root at the drop ts.
                                    self.destroyed_indexes.insert(index_id);

                                    // DROP INDEX during checkpoint: schema may no longer contain the index definition.
                                    // Fixes DROP INDEX during checkpoint when the schema cache no longer
                                    // contains the index metadata; we only need a cursor to destroy pages so num_columns is not important.
                                    let num_columns = self
                                        .index_id_to_index
                                        .get(&index_id)
                                        .map(|index| index.columns.len())
                                        .unwrap_or(0);

                                    special_write = Some(SpecialWrite::BTreeDestroyIndex {
                                        index_id,
                                        root_page: root_page as u64,
                                        num_columns,
                                    });
                                } else {
                                    // No binding owned this root at the drop ts: the index was
                                    // already destroyed by an earlier checkpoint and this
                                    // schema-delete lingered in the store. Nothing to destroy.
                                    skip_write = true;
                                }
                            } else if root_page < 0 {
                                // CREATE INDEX (root page is negative so the index has not been checkpointed yet).
                                let index_id = MVTableId::from(root_page);
                                let sqlite_schema_rowid = version.row.id.row_id.to_int_or_panic();
                                special_write = Some(SpecialWrite::BTreeCreateIndex {
                                    index_id,
                                    sqlite_schema_rowid,
                                });
                            } else {
                                // Index schema row update (e.g. ALTER TABLE RENAME COLUMN propagates
                                // to index SQL). No B-tree creation needed; the row itself is written
                                // to sqlite_schema below. See: test_checkpoint_allows_index_schema_update_after_rename_column.
                            }
                        }
                        SqliteSchemaBtreeKind::Table => {
                            // This is a table schema change (existing logic)
                            tracing::trace!(
                                "table schema change with root page {root_page}, is_delete={is_delete}"
                            );
                            if is_delete {
                                if root_page < 0 {
                                    // Table was never checkpointed - derive table_id directly from root_page.
                                    // No BTreeDestroy needed since there's no physical B-tree.
                                    let table_id = MVTableId::from(root_page);
                                    self.destroyed_tables.insert(table_id);
                                    // Defer the removal to the publish window (push borrows only
                                    // the staged-op field, allowed inside the rows iteration).
                                    self.pending_rootmap_ops
                                        .push(RootMapOp::Remove { id: table_id });
                                    skip_write = true;
                                } else if let Some(table_id) =
                                    self.resolve_dropped_binding(root_page as u64, &version)
                                {
                                    // Table was checkpointed - resolve from the binding that owned
                                    // this root at the drop ts (snapshot-consistent under reuse).
                                    self.destroyed_tables.insert(table_id);

                                    // Destroy the B-tree in the pager during checkpoint
                                    special_write = Some(SpecialWrite::BTreeDestroy {
                                        table_id,
                                        root_page: root_page as u64,
                                        num_columns: version.row.column_count,
                                    });
                                }
                            } else if root_page < 0 {
                                // CREATE TABLE (root page is negative so the table has not been checkpointed yet).
                                let table_id = MVTableId::from(root_page);
                                let sqlite_schema_rowid = version.row.id.row_id.to_int_or_panic();
                                special_write = Some(SpecialWrite::BTreeCreate {
                                    table_id,
                                    sqlite_schema_rowid,
                                });
                            } else {
                                // ALTER TABLE. No "special write is needed"; we'll just update the row in sqlite_schema.
                            }
                        }
                    }
                } else if is_delete
                    && version.row.id.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID
                    && !version.btree_resident
                {
                    // Schema row without a B-tree identity (e.g. sequence, trigger, view).
                    // If it was never checkpointed to the B-tree, skip the delete — there
                    // is nothing to remove from the pager.
                    let begin_ts = match &version.begin() {
                        Some(TxTimestampOrID::Timestamp(ts)) => Some(*ts),
                        _ => None,
                    };
                    let was_checkpointed = self.durable_txid_max_old.is_some_and(|txid_max_old| {
                        begin_ts.is_some_and(|b| b <= u64::from(txid_max_old))
                    });
                    if !was_checkpointed {
                        skip_write = true;
                    }
                } else if key.table_id != SQLITE_SCHEMA_MVCC_TABLE_ID
                    && is_delete
                    && !self.table_exists_for_snapshot(key.table_id)
                {
                    // B-tree was destroyed in a prior checkpoint; late tombstones are logical-only.
                    skip_write = true;
                }
                if !skip_write {
                    tracing::trace!("adding to write_set {:?}", (&version, &special_write));
                    with_mvcc_checkpoint_allocation_site!(CheckpointWriteSet, {
                        self.write_set.try_push((version, special_write))?;
                    });
                }
            }
            processed += 1;
            if processed >= COLLECT_PREEMPTION_THRESHOLD {
                return Ok(Some(IOCompletions(Completion::new_yield())));
            }
        }
        // Writing in ascending order of rowid gives us a better chance of using balance-quick algorithm
        // in case of an insert-heavy checkpoint.
        self.write_set.sort_by_key(|version| {
            (
                // Sort by table_id descending (schema changes first)
                std::cmp::Reverse(version.0.row.id.table_id),
                // Then by row_id ascending
                version.0.row.id.row_id.clone(),
            )
        });
        Ok(None)
    }

    /// Collect all committed index row versions that need to be written to the B-tree.
    /// Index rows are stored separately from table rows and must be checkpointed independently.
    /// We must only write to the B-tree if:
    /// 1. The row has not already been checkpointed in a previous checkpoint.
    /// 2. Either:
    ///    * The row is not a delete (we inserted or changed an existing row), OR
    ///    * The row is a delete AND it exists in the database file already.
    fn collect_index_rows(&mut self) -> Result<Option<IOCompletions>> {
        let outer_bounds: (Bound<MVTableId>, Bound<MVTableId>) =
            match self.collect_index_tableid_cursor {
                None => (Bound::Unbounded, Bound::Unbounded),
                Some(last) if self.collect_index_key_cursor.is_none() => {
                    (Bound::Excluded(last), Bound::Unbounded)
                }
                Some(last) => (Bound::Included(last), Bound::Unbounded),
            };
        let mut processed = 0;
        for entry in self.mvstore.index_rows.range(outer_bounds) {
            let index_id = *entry.key();

            // Skip destroyed indexes - we won't checkpoint rows for indexes that will be destroyed
            if self.destroyed_indexes.contains(&index_id) {
                self.collect_index_tableid_cursor = Some(index_id);
                self.collect_index_key_cursor = None;
                continue;
            }

            let index_rows_map = entry.value();
            let inner_bounds: (Bound<Arc<SortableIndexKey>>, Bound<Arc<SortableIndexKey>>) =
                match self.collect_index_key_cursor.clone() {
                    None => (Bound::Unbounded, Bound::Unbounded),
                    Some(last) => (Bound::Excluded(last), Bound::Unbounded),
                };
            for entry in index_rows_map.range(inner_bounds) {
                let versions = entry.value().read();
                self.collect_index_tableid_cursor = Some(index_id);
                self.collect_index_key_cursor = Some(entry.key().clone());

                for version in self.maybe_get_checkpointable_versions(&versions, index_id) {
                    let is_delete = version.end().is_some();
                    if is_delete && !self.table_exists_for_snapshot(index_id) {
                        continue;
                    }

                    // Only write the row to the B-tree if it is not a delete, or if it is a delete and it exists in
                    // the database file.
                    with_mvcc_checkpoint_allocation_site!(CheckpointIndexWriteSet, {
                        self.index_write_set
                            .try_push((index_id, version, is_delete))?;
                    });
                }
                processed += 1;
                if processed >= COLLECT_PREEMPTION_THRESHOLD {
                    return Ok(Some(IOCompletions(Completion::new_yield())));
                }
            }
            self.collect_index_tableid_cursor = Some(index_id);
            self.collect_index_key_cursor = None;
        }
        Ok(None)
    }

    #[cfg(any(test, debug_assertions))]
    fn max_collected_version_timestamp(&self) -> u64 {
        fn max_version_timestamp(version: &RowVersion) -> u64 {
            [version.begin().as_ref(), version.end().as_ref()]
                .into_iter()
                .filter_map(|ts| match ts {
                    Some(TxTimestampOrID::Timestamp(ts)) => Some(*ts),
                    _ => None,
                })
                .max()
                .unwrap_or_default()
        }

        let table_max = self
            .write_set
            .iter()
            .map(|(version, _)| max_version_timestamp(version))
            .max()
            .unwrap_or_default();
        let index_max = self
            .index_write_set
            .iter()
            .map(|(_, version, _)| max_version_timestamp(version))
            .max()
            .unwrap_or_default();
        table_max.max(index_max)
    }

    /// Get the current row version to write to the B-tree
    fn get_current_row_version(
        &self,
        write_set_index: usize,
    ) -> Option<&(RowVersion, Option<SpecialWrite>)> {
        self.write_set.get(write_set_index)
    }

    /// Mutably get the current row version to write to the B-tree
    fn get_current_row_version_mut(
        &mut self,
        write_set_index: usize,
    ) -> Option<&mut (RowVersion, Option<SpecialWrite>)> {
        self.write_set.get_mut(write_set_index)
    }

    /// Check if we have more rows to write
    fn has_more_rows(&self, write_set_index: usize) -> bool {
        write_set_index < self.write_set.len()
    }

    fn next_requires_seek_after_insert(&self, current_idx: usize) -> bool {
        let Some(curr) = self.write_set.get(current_idx) else {
            return true;
        };
        let Some(next) = self.write_set.get(current_idx + 1) else {
            return true;
        };
        // Table not the same, then seek
        if curr.0.row.id.table_id != next.0.row.id.table_id {
            return true;
        }
        // If we have special write then seek
        if curr.1.is_some() || next.1.is_some() {
            return true;
        }
        let (RowKey::Int(prev_id), RowKey::Int(next_id)) =
            (&curr.0.row.id.row_id, &next.0.row.id.row_id)
        else {
            return true;
        };
        // if next id is strictly prev_id + 1 then we don't need to seek
        if next_id.checked_sub(*prev_id) != Some(1) {
            return true;
        }
        false
    }

    /// Fsync the logical log file
    fn fsync_logical_log(&self) -> Result<Completion> {
        self.mvstore.storage.sync(self.pager.get_sync_type())
    }

    fn truncate_logical_log(&self) -> Result<Completion> {
        use crate::mvcc::persistent_storage::LogicalLogTruncateOutcome;
        let boundary = if self.mode.should_restart_log() {
            u64::MAX
        } else {
            self.durable_txid_max_new
        };
        let (c, outcome) = self.mvstore.storage.truncate(boundary)?;
        if self.mode.should_restart_log() {
            turso_assert!(
                matches!(outcome, LogicalLogTruncateOutcome::Truncated),
                "TRUNCATE checkpoint must clear the logical log"
            );
        }
        Ok(c)
    }

    /// Perform a TRUNCATE checkpoint on the WAL
    fn checkpoint_wal(&self) -> IOResultOr<CheckpointResult> {
        let Some(wal) = &self.pager.wal else {
            panic!("No WAL to checkpoint");
        };
        match wal.checkpoint(&self.pager, self.mode, self.sync_mode)? {
            IOResult::Done(result) => Ok(IOResult::Done(result)),
            IOResult::IO(io) => Ok(IOResult::IO(io)),
        }
    }

    /// Publish `nbackfills` after Passive backfill + DB sync. Skip Truncate/Restart.
    /// Leaves the checkpoint guard held until Finalize (same as the pager).
    fn publish_wal_backfill_if_needed(&mut self) {
        if self.mode.should_restart_log() {
            return;
        }
        let Some(result) = self.checkpoint_result.as_ref() else {
            return;
        };
        if result.wal_checkpoint_backfilled == 0 {
            return;
        }
        let max_frame = result.wal_total_backfilled;
        turso_assert!(self.pager.wal.is_some(), "No WAL to publish backfill");
        let wal = self.pager.wal.as_ref().unwrap();
        tracing::debug!(
            max_frame,
            backfilled = result.wal_checkpoint_backfilled,
            "publishing WAL backfill after MVCC checkpoint"
        );
        wal.publish_backfill(max_frame);
    }

    fn has_unpublished_schema_changes(&self) -> bool {
        let schema = self.database.schema.lock();
        if !schema.dropped_root_pages.is_empty() {
            return true;
        }
        // A negative root page is "unpublished" only if THIS checkpoint materialized the
        // object (has a real root page in `table_id_to_rootpage` not yet written back to
        // the schema). Objects created after our snapshot are deferred to a later
        // checkpoint and have no mapping yet, so their placeholders aren't our work —
        // counting them was the false-positive that panicked the TruncateWal assert.
        let owned_negative = |root_page: i64| -> bool {
            root_page < 0
                && self
                    .mvstore
                    .table_id_to_rootpage
                    .get(&MVTableId::from(root_page))
                    .and_then(|entry| entry.value().root_page)
                    .is_some()
        };
        schema.tables.values().any(|table| {
            table
                .btree()
                .is_some_and(|btree| owned_negative(btree.root_page))
        }) || schema
            .indexes
            .values()
            .flatten()
            .any(|index| owned_negative(index.root_page))
    }

    fn has_pending_root_publication(&self) -> bool {
        !self.created_btrees.is_empty() || self.has_unpublished_schema_changes()
    }

    fn publish_checkpointed_schema_roots(&mut self) -> Result<(), TryReserveError> {
        if !self.has_pending_root_publication() {
            return Ok(());
        }

        // Patch sqlite_schema rows in the MV store to use positive rootpages for the btrees
        // flushed to the physical database.
        for (sqlite_schema_rowid, (_, row_version)) in self.created_btrees.drain() {
            let key = RowID {
                table_id: SQLITE_SCHEMA_MVCC_TABLE_ID,
                row_id: RowKey::Int(sqlite_schema_rowid),
            };
            let sqlite_schema_row = self
                .mvstore
                .rows
                .get(&key)
                .expect("sqlite_schema row not found");
            let mut row_versions = sqlite_schema_row.value().write();
            // Patch payload in place (same version id), don't append: a duplicate
            // (id, begin, end) would leave a phantom current version after a later
            // DELETE (which ends only the first match), causing spurious write-write
            // conflicts at commit time.
            //
            // Only replace the row payload (positive rootpage). Preserve begin/end so a
            // DROP that committed after our snapshot keeps its end stamp — wholesale
            // replace of the collected clone would resurrect the table (#7956).
            let vid = row_version.id;
            if let Some(existing) = row_versions.iter_mut().find(|rv| rv.id == vid) {
                existing.row.data = row_version.row.data;
                if existing.end().is_some() {
                    if let Some(identity) = sqlite_schema_btree_identity(existing) {
                        if identity.root_page > 0 {
                            self.ended_created_roots.insert(identity.root_page);
                        }
                    }
                }
            } else {
                self.mvstore
                    .insert_version_raw(&mut row_versions, row_version)?;
            }
        }

        if !self.has_unpublished_schema_changes()
            && self.ended_created_roots.is_empty()
            && self.freed_root_pages.is_empty()
        {
            return Ok(());
        }

        // Patch the live db.schema (not local_schema, the checkpoint's private snapshot) so
        // clone_schema() propagates real root pages; negative placeholders would orphan the
        // new btree pages.
        let mut schema_ref = self.database.schema.lock();
        let schema = Schema::try_make_mut(&mut schema_ref)?;
        for (name, table) in schema.tables.iter_mut() {
            #[cfg(not(feature = "conn_raw_api"))]
            let _ = name;
            let table = Arc::get_mut(table).expect("this should be the only reference");
            let Some(btree_table) = table.btree_mut() else {
                continue;
            };
            let btree_table = Arc::make_mut(btree_table);
            if btree_table.root_page < 0 {
                #[cfg(feature = "conn_raw_api")]
                let old_root_page = btree_table.root_page;
                let table_id = MVTableId::from(btree_table.root_page);
                // Only tables this pass materialized; ones created after our snapshot stay
                // negative for a later pass.
                if let Some(root_page) = self
                    .mvstore
                    .table_id_to_rootpage
                    .get(&table_id)
                    .and_then(|entry| entry.value().root_page)
                {
                    btree_table.root_page = root_page as i64;
                    #[cfg(feature = "conn_raw_api")]
                    {
                        schema.table_names_by_root_page.remove(&old_root_page);
                        schema
                            .table_names_by_root_page
                            .insert(btree_table.root_page, name.clone());
                    }
                }
            }
        }
        for table_index_list in schema.indexes.values_mut() {
            for index in table_index_list.iter_mut() {
                if index.root_page < 0 {
                    let table_id = MVTableId::from(index.root_page);
                    // Same as tables: skip indexes not materialized by this pass.
                    if let Some(root_page) = self
                        .mvstore
                        .table_id_to_rootpage
                        .get(&table_id)
                        .and_then(|entry| entry.value().root_page)
                    {
                        let index = Arc::make_mut(index);
                        index.root_page = root_page as i64;
                    }
                }
            }
        }

        // Only forget roots whose btrees this checkpoint actually freed. A concurrent
        // DROP after our snapshot may have added roots we did not destroy (#7957).
        for root in std::mem::take(&mut self.freed_root_pages) {
            schema.dropped_root_pages.remove(&root);
        }
        // Roots we materialized whose schema versions were already ended are still
        // allocated and must stay accounted for until a later destroy (#7956).
        schema
            .dropped_root_pages
            .extend(std::mem::take(&mut self.ended_created_roots));
        drop(schema_ref);
        let schema = self.database.clone_schema();
        if self.database_id == crate::MAIN_DB_ID {
            *self.connection.schema.write() = schema;
        } else {
            self.connection
                .database_schemas()
                .write()
                .insert(self.database_id, schema);
        }
        self.connection.bump_prepare_context_generation();
        self.mvstore.bump_schema_generation();
        Ok(())
    }

    /// Apply deferred root-map mutations, advance `durable_txid_max`, and publish the staged
    /// page-1 header + schema roots. Caller must either hold `blocking_checkpoint_lock` write
    /// (truncate / explicit passive) or run inside `MvStore::finish_passive_publish_window`.
    fn apply_checkpoint_publish_window(
        &mut self,
        materialized_at: WalPos,
        seq_delete_ts: Option<u64>,
    ) -> Result<()> {
        // PASSIVE sequence compaction (Option B): turn the rows the sweep recorded into proper
        // end-stamped deletes, using the publish-window timestamp. It is allocated under the same
        // clock as concurrent `begin_tx`, so it is > every concurrent reader's begin (they see the
        // row live and conflict, never re-synthesize a tombstone) and > durable_txid_max_new (the
        // boundary stored below), so the next checkpoint still materializes the physical delete.
        if let Some(ts) = seq_delete_ts {
            for (rowid, num_cols) in
                std::mem::replace(&mut self.pending_seq_deletes, crate::alloc::vec![])
            {
                self.mvstore.seqcompact_commit_delete(rowid, num_cols, ts);
            }
        }
        let ops = std::mem::replace(&mut self.pending_rootmap_ops, crate::alloc::vec![]);
        for op in &ops {
            match op {
                RootMapOp::Retire { id, end_ts } => self.mvstore.retire_rootpage(*id, *end_ts),
                RootMapOp::Remove { id } => self.mvstore.remove_table_id_to_rootpage(id),
                RootMapOp::Alloc { .. } => {}
            }
        }
        for op in &ops {
            if let RootMapOp::Alloc { id, root } = op {
                self.mvstore
                    .record_rootpage_alloc(*id, *root, self.snapshot_ts, WalPos::STAGED);
            }
        }
        self.pending_alloc_roots.clear();
        for table_id in std::mem::replace(&mut self.staged_roots, crate::alloc::vec![]) {
            self.mvstore
                .publish_rootpage_visible(table_id, materialized_at);
        }
        self.mvstore
            .durable_txid_max
            .store(self.durable_txid_max_new, Ordering::SeqCst);
        self.state = CheckpointState::CheckpointWal;
        self.lock_states.pager_read_tx = false;
        self.lock_states.pager_write_tx = false;
        let header = self.staged_checkpoint_header.take().ok_or_else(|| {
            LimboError::InternalError(
                "checkpoint header was not staged before pager commit".to_string(),
            )
        })?;
        self.mvstore.global_header.write().replace(header);
        crate::without_allocation_faults!(self
            .publish_checkpointed_schema_roots()
            .expect(crate::alloc::ALLOC_ERR_MSG));
        Ok(())
    }

    /// Passive auto-checkpoint publish: clock-ordered apply (no RW lock). Caller must have
    /// already acquired the publish drain bit via `try_begin_passive_publish_window`.
    fn apply_passive_publish_window_ordered(&mut self, materialized_at: WalPos) -> Result<()> {
        let mut publish_err = None;
        // SAFETY: `apply_checkpoint_publish_window` mutates only this state machine and the
        // mvstore. The clock lock excludes concurrent `begin_tx` publication; the publish
        // drain bit excludes new begins before they reach the clock lock.
        let sm = self as *mut Self;
        self.mvstore.clock.get_timestamp(|ts| {
            // SAFETY: see above. `ts` is this publish's clock timestamp; pass it as the commit ts
            // for passive sequence-compaction deletes (see apply_checkpoint_publish_window).
            if let Err(err) =
                unsafe { (*sm).apply_checkpoint_publish_window(materialized_at, Some(ts)) }
            {
                publish_err = Some(err);
            }
        });
        self.mvstore.end_passive_publish_window();
        if let Some(err) = publish_err {
            return Err(err);
        }
        Ok(())
    }

    /// The version-store GC floor mark for the passive checkpoint: the lowest of the published
    /// MVCC readers' marks AND any reader pinned at the pager/WAL level (via `begin_read_tx`) that
    /// has not yet published an MVCC transaction. The latter closes the begin-tx publish window —
    /// without it, a reader that pinned an old WAL frame but is not yet in `txs` is invisible to
    /// `compute_min_reader_mark`, so its still-needed versions get reclaimed and it then reads a
    /// stale btree (the delete/index desync). The WAL read lock is held from `begin_read_tx`, so
    /// this catches it.
    fn gc_floor_reader_mark(&self) -> WalPos {
        let mvcc = self.mvstore.compute_min_reader_mark();
        let readers = match self.pager.min_pinned_read_frame() {
            Some(frame) => {
                let (seq, _) = self.pager.wal_pos();
                mvcc.min(WalPos::from_pair((seq, frame)))
            }
            None => mvcc,
        };
        // Bound by the backfill boundary: a version still materialized in un-backfilled WAL
        // frames is unreachable by a db-file reader (present or future), so never reclaim it
        // until backfilled. This is the true floor and subsumes the live-reader marks.
        readers.min(*self.mvstore.backfill_floor.read())
    }

    fn gc_checkpointed_table_versions(&mut self) -> Option<IOCompletions> {
        // Keep empty SkipMap slots; Truncate Finalize `_and_slots` unlinks them later.
        let ckpt_max = self.durable_txid_max_new;
        // Includes pager/WAL-pinned readers not yet in `txs` (begin-tx publish window).
        let min_reader_mark = self.gc_floor_reader_mark();
        // WAL pos this checkpoint made durable; stamp just-materialized versions with it
        // (unchanged since CommitPagerTxn — single orchestrator).
        let materialized_frame = WalPos::from_pair(self.pager.wal_pos());
        let snapshot_ts = self.snapshot_ts;
        let CheckpointState::GcTableRows { next_index, lwm } = self.state else {
            unreachable!("gc_checkpointed_table_versions runs only in GcTableRows");
        };
        let mut index = next_index;
        let mut processed = 0;
        let drop_current_if_in_btree = true;
        while index < self.write_set.len() {
            let current = index;
            index += 1;
            if current > 0
                && self.write_set[current - 1].0.row.id == self.write_set[current].0.row.id
            {
                continue;
            }
            let row_id = &self.write_set[current].0.row.id;
            if let Some(entry) = self.mvstore.rows.get(row_id) {
                let mut versions = entry.value().write();
                if let RowKey::Int(n) = row_id.row_id {
                    if self.written_table_rowids.contains(&(row_id.table_id, n)) {
                        self.mvstore.stamp_chain_materialized(
                            &mut versions,
                            materialized_frame,
                            snapshot_ts,
                        );
                    }
                }
                let dropped = self.mvstore.gc_chain_now(
                    &mut versions,
                    lwm,
                    ckpt_max,
                    min_reader_mark,
                    drop_current_if_in_btree,
                );
                self.mvstore.dec_live_version_count_approx(dropped);
            } else {
                // The MVCC metadata table row (persistent_tx_ts_max) is staged
                // directly into the write set by maybe_stage_mvcc_metadata_write() and do not
                // have a backing in-memory MVCC version chain. Skip GC for these.
                assert!(
                    self.mvcc_meta_table
                        .is_some_and(|(tid, _)| tid == row_id.table_id),
                    "row {row_id:?} missing from MVCC store but is not an MVCC metadata table row"
                );
            }
            processed += 1;
            if processed >= COLLECT_PREEMPTION_THRESHOLD {
                break;
            }
        }
        if index < self.write_set.len() {
            let CheckpointState::GcTableRows { next_index, .. } = &mut self.state else {
                unreachable!("gc_checkpointed_table_versions runs only in GcTableRows");
            };
            *next_index = index;

            Some(IOCompletions(Completion::new_yield()))
        } else {
            None
        }
    }

    fn gc_checkpointed_index_versions(&mut self) -> Option<IOCompletions> {
        // Same as table GC: keep empty SkipMap slots; Truncate Finalize unlinks later.
        let ckpt_max = self.durable_txid_max_new;
        let min_reader_mark = self.gc_floor_reader_mark();
        let materialized_frame = WalPos::from_pair(self.pager.wal_pos());
        let snapshot_ts = self.snapshot_ts;
        let CheckpointState::GcIndexRows { next_index, lwm } = self.state else {
            unreachable!("gc_checkpointed_index_versions runs only in GcIndexRows");
        };
        let mut index = next_index;
        let mut processed = 0;
        let drop_current_if_in_btree = true;
        while index < self.index_write_set.len() {
            let current = index;
            index += 1;
            {
                let (index_id, row_version, _is_delete) = &self.index_write_set[current];
                let index_id = *index_id;
                let RowKey::Record(sortable_key) = &row_version.row.id.row_id else {
                    unreachable!("index row versions always have Record keys");
                };
                let outer_entry = self
                    .mvstore
                    .index_rows
                    .get(&index_id)
                    .expect("index_id from write set must exist in index_rows");
                let inner_map = outer_entry.value();
                let inner_entry = inner_map
                    .get(sortable_key)
                    .expect("index row from write set must exist in inner map");
                let mut versions = inner_entry.value().write();
                if self.written_index_slots.contains(&current) {
                    self.mvstore.stamp_chain_materialized(
                        &mut versions,
                        materialized_frame,
                        snapshot_ts,
                    );
                }
                let dropped = self.mvstore.gc_chain_now(
                    &mut versions,
                    lwm,
                    ckpt_max,
                    min_reader_mark,
                    drop_current_if_in_btree,
                );
                self.mvstore.dec_live_version_count_approx(dropped);
            }
            processed += 1;
            if processed >= COLLECT_PREEMPTION_THRESHOLD {
                break;
            }
        }
        if index < self.index_write_set.len() {
            let CheckpointState::GcIndexRows { next_index, .. } = &mut self.state else {
                unreachable!("gc_checkpointed_index_versions runs only in GcIndexRows");
            };
            *next_index = index;
            Some(IOCompletions(Completion::new_yield()))
        } else {
            None
        }
    }

    fn record_written_table_row(&mut self, write_set_index: usize) {
        let Some(row_id) = self
            .get_current_row_version(write_set_index)
            .map(|(row_version, _)| row_version.row.id.clone())
        else {
            return;
        };
        let RowKey::Int(n) = row_id.row_id else {
            return;
        };
        self.written_table_rowids.insert((row_id.table_id, n));
    }

    /// Stages synthetic `persistent_tx_ts_max` row into the checkpoint write set
    /// so it is committed atomically with all other data in the same pager transaction.
    /// This is the mechanism that advances the durable replay boundary; on recovery, only
    /// logical-log frames with `commit_ts > persistent_tx_ts_max` are replayed.
    /// No-op when metadata hasn't advanced or when running in-memory (no durable metadata).
    fn maybe_stage_mvcc_metadata_write(&mut self) -> Result<()> {
        if !self.durable_mvcc_metadata {
            return Ok(());
        }
        let old = self.durable_txid_max_old.map(u64::from).unwrap_or_default();
        let new = self.durable_txid_max_new;
        if new <= old {
            return Ok(());
        }

        let (table_id, num_columns) = self.mvcc_meta_table.ok_or_else(|| {
            LimboError::Corrupt(format!(
                "Missing required internal metadata table {MVCC_META_TABLE_NAME}"
            ))
        })?;
        let new_i64 = i64::try_from(new).map_err(|_| {
            LimboError::Corrupt(format!("MVCC checkpoint timestamp does not fit i64: {new}"))
        })?;
        let record = with_mvcc_checkpoint_allocation_site!(
            CheckpointMetadataPayload,
            ImmutableRecord::from_values(
                &[
                    Value::build_text(MVCC_META_KEY_PERSISTENT_TX_TS_MAX),
                    Value::from_i64(new_i64),
                ],
                2,
            )?
        );
        let row = with_mvcc_checkpoint_allocation_site!(
            CheckpointMetadataPayload,
            Row::new_table_row_in(
                RowID::new(table_id, RowKey::Int(1)),
                record.get_payload(),
                num_columns,
                self.mvstore.allocator(),
            )?
        );
        with_mvcc_checkpoint_allocation_site!(CheckpointWriteSet, {
            self.write_set.try_push((
                RowVersion {
                    id: 0,
                    begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::Timestamp(
                        new,
                    ))),
                    end: crate::mvcc::database::PackedTs::pack(None),
                    row,
                    btree_resident: true,
                    materialized_at: crate::mvcc::database::WalPos::ORIGIN,
                },
                None,
            ))?;
        });
        Ok(())
    }

    fn step_inner(&mut self, _context: &()) -> Result<TransitionResult<CheckpointResult>> {
        match &self.state {
            CheckpointState::PrepareCheckpoint => {
                let passive = self.mvstore.uses_passive_checkpoint();
                if passive {
                    // The passive checkpoint acquires the blocking lock only after
                    // collection, so it needs an explicit single-orchestrator gate. The
                    // blocking (flag-off) path takes the lock up front and gets that
                    // invariant — plus Busy-on-contention — from the lock itself, so it
                    // must NOT use this gate, which would turn a contended explicit
                    // TRUNCATE into a silent no-op.
                    if self
                        .mvstore
                        .checkpoint_in_progress
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        // Another checkpoint is already running: no-op (no work, no resources).
                        self.state = CheckpointState::Finalize;
                        return Ok(TransitionResult::Done(CheckpointResult::default()));
                    }
                    self.owns_checkpoint_in_progress = true;
                }

                if passive {
                    self.snapshot_ts = self.mvstore.checkpoint_snapshot_ts();
                    // Checkpoint state machines can be created before they are run.
                    // Resample after serializing so already-durable index deletes are not replayed.
                    self.refresh_checkpoint_bounds();
                    self.state = CheckpointState::BuildLocalSchemaView;
                } else {
                    self.state = CheckpointState::AcquireLock;
                }
                Ok(TransitionResult::Continue)
            }
            CheckpointState::AcquireLock => {
                inject_transition_yield!(self, CheckpointYieldPoint::BeforeAcquireLock);

                tracing::debug!("Acquiring blocking checkpoint lock");
                if !self.lock_states.blocking_checkpoint_lock_held {
                    let locked = self.checkpoint_lock.write();
                    if !locked {
                        return Err(crate::LimboError::Busy);
                    }
                    self.lock_states.blocking_checkpoint_lock_held = true;
                }

                // Sample the snapshot only after the stop-the-world lock: no concurrent
                // commits can land between snapshot_ts and collection on this path.
                self.snapshot_ts = self.mvstore.checkpoint_snapshot_ts();
                // Checkpoint state machines can be created before they are run.
                // Resample after serializing with other checkpoints so already-durable
                // index deletes are not replayed, and keep schema-derived index metadata
                // aligned with the refreshed durable boundary.
                self.refresh_checkpoint_bounds();
                self.refresh_schema_metadata();
                self.state = CheckpointState::CollectTableRows;
                Ok(TransitionResult::Continue)
            }
            CheckpointState::BuildLocalSchemaView => {
                if self.build_local_schema_sm.is_none() {
                    let began = !self
                        .pager
                        .wal
                        .as_ref()
                        .is_some_and(|wal| wal.holds_read_lock());
                    if began {
                        self.pager.begin_read_tx()?;
                    }
                    self.build_local_schema_began_read_tx = began;
                    let cursor = BTreeCursor::new_table(
                        self.pager.clone(),
                        SQLITE_SCHEMA_ROOT_PAGE,
                        SQLITE_SCHEMA_COLUMN_COUNT,
                    );
                    self.build_local_schema_sm =
                        Some(StateMachine::new(BuildLocalSchemaViewStateMachine::new(
                            cursor,
                            self.mvstore.clone(),
                            self.connection.clone(),
                            self.snapshot_ts,
                        )));
                }
                let sm = self
                    .build_local_schema_sm
                    .as_mut()
                    .expect("build_local_schema_sm just set");
                match sm.step(&())? {
                    IOResult::IO(io) => Ok(TransitionResult::Io(io)),
                    IOResult::Done(schema) => {
                        self.local_schema = Some(schema);
                        let local = self
                            .local_schema
                            .as_ref()
                            .expect("local_schema just set")
                            .clone();
                        // Key each index by the binding that owns its root page AT snapshot_ts —
                        // the same id collect_index_rows uses (mvstore.index_rows is keyed by the
                        // owning id). Resolving at the *current* owner (u64::MAX) instead would,
                        // under concurrent page reuse, key a present index under a different (reused)
                        // id, so WriteIndexRow would fail to find its Index struct and drop real
                        // entries ("row N missing from index"). filter_map: an index whose root has
                        // no binding covering the snapshot is not part of this snapshot.
                        self.index_id_to_index = local
                            .indexes
                            .values()
                            .flatten()
                            .filter(|index| index.is_btree_backed())
                            .filter_map(|index| {
                                turso_assert!(
                                    index.root_page != 0,
                                    "index root_page must be non-zero"
                                );
                                self.mvstore
                                    .try_get_table_id_from_root_page_at(
                                        index.root_page,
                                        self.snapshot_ts,
                                    )
                                    .map(|id| (id, index.clone()))
                            })
                            .collect();
                        self.build_local_schema_sm = None;
                        if self.build_local_schema_began_read_tx {
                            self.pager.end_read_tx();
                            self.build_local_schema_began_read_tx = false;
                        }
                        self.state = CheckpointState::CollectTableRows;
                        Ok(TransitionResult::Continue)
                    }
                }
            }
            CheckpointState::CollectTableRows => {
                if let Some(io) = self.collect_table_rows()? {
                    return Ok(TransitionResult::Io(io));
                }
                tracing::debug!("Collected {} committed versions", self.write_set.len());
                self.state = CheckpointState::CollectIndexRows;
                inject_transition_yield!(self, CheckpointYieldPoint::AfterCollectTableRows);
                Ok(TransitionResult::Continue)
            }
            CheckpointState::CollectIndexRows => {
                if let Some(io) = self.collect_index_rows()? {
                    return Ok(TransitionResult::Io(io));
                }
                tracing::debug!("Collected {} index row changes", self.index_write_set.len());

                let passive = self.mvstore.uses_passive_checkpoint();
                if passive {
                    inject_transition_yield!(self, CheckpointYieldPoint::BeforeAcquireLock);
                    // Passive path: collection AND the btree write phase run without the
                    // blocking lock. The only serialized point is the brief publish window in
                    // CommitPagerTxn.
                }

                let durable_old = self.durable_txid_max_old.map(u64::from).unwrap_or_default();
                #[cfg(any(test, debug_assertions))]
                {
                    let collected_max = self.max_collected_version_timestamp();
                    turso_assert!(
                        self.snapshot_ts >= collected_max,
                        "MVCC checkpoint collected version timestamp above snapshot",
                        { "collected_max": collected_max, "snapshot_ts": self.snapshot_ts }
                    );
                }
                self.durable_txid_max_new = durable_old.max(self.snapshot_ts);
                self.maybe_stage_mvcc_metadata_write()?;

                self.mvstore.storage.on_checkpoint_start()?;

                if self.write_set.is_empty() && self.index_write_set.is_empty() {
                    // Nothing to checkpoint, skip pager txn and go straight to WAL checkpoint.
                    self.state = CheckpointState::CheckpointWal;
                } else {
                    self.state = CheckpointState::BeginPagerTxn;
                }
                Ok(TransitionResult::Continue)
            }
            CheckpointState::BeginPagerTxn => {
                tracing::debug!("Beginning pager transaction");
                // Start a pager transaction to write committed versions to B-tree
                let read_tx_active = self
                    .pager
                    .wal
                    .as_ref()
                    .is_some_and(|wal| wal.holds_read_lock());
                if !read_tx_active {
                    self.pager.begin_read_tx()?;
                    self.lock_states.pager_read_tx = true;
                }

                self.pager
                    .io
                    .block(|| self.pager.begin_write_tx(WalAutoActions::all_enabled()))?;
                if self.update_transaction_state {
                    self.connection.set_tx_state(TransactionState::Write {
                        schema_did_change: false,
                    }); // TODO: schema_did_change??
                }
                self.lock_states.pager_write_tx = true;
                self.state = CheckpointState::WriteRow {
                    write_set_index: 0,
                    requires_seek: true,
                };
                Ok(TransitionResult::Continue)
            }

            CheckpointState::WriteRow {
                write_set_index,
                requires_seek,
            } => {
                let write_set_index = *write_set_index;
                let requires_seek = *requires_seek;

                if !self.has_more_rows(write_set_index) {
                    // Done writing all table rows, now process index rows
                    if self.index_write_set.is_empty() {
                        // No index rows to write, compact sequence
                        // backing tables, then commit.
                        self.state = CheckpointState::CompactSequences;
                    } else {
                        // Start writing index rows
                        self.state = CheckpointState::WriteIndexRow {
                            index_write_set_index: 0,
                            requires_seek: true,
                        };
                    }
                    return Ok(TransitionResult::Continue);
                }

                let (num_columns, table_id, special_write, drop_ts) = {
                    let (row_version, special_write) = self
                        .get_current_row_version(write_set_index)
                        .ok_or_else(|| {
                            LimboError::InternalError(
                                "row version not found in write set".to_string(),
                            )
                        })?;
                    tracing::trace!("checkpointing row {row_version:?} ");
                    // Commit ts of the tombstone driving a destroy, so a dropped checkpointed
                    // object can be retired into `retired_rootpages` for readers still at an
                    // older snapshot (see the BTreeDestroy/BTreeDestroyIndex arms below).
                    let drop_ts = match row_version.end() {
                        Some(TxTimestampOrID::Timestamp(ts)) => Some(ts),
                        _ => None,
                    };
                    (
                        row_version.row.column_count,
                        row_version.row.id.table_id,
                        *special_write,
                        drop_ts,
                    )
                };
                tracing::debug!(
                    "WriteRow: num_columns={num_columns}, table_id={table_id:?}, special_write={special_write:?}"
                );

                // Handle CREATE TABLE / DROP TABLE / CREATE INDEX / DROP INDEX ops
                if let Some(special_write) = special_write {
                    match special_write {
                        SpecialWrite::BTreeCreate { table_id, .. } => {
                            let created_root_page: u32 = self.pager.io.block(|| {
                                self.pager.btree_create(&CreateBTreeFlags::new_table())
                            })?;
                            // STAGE the binding: the checkpoint must resolve table_id -> root while
                            // writing rows below, but the pages are not durable until
                            // CommitPagerTxn, so it stays physically invisible to readers
                            // (visible_from = u64::MAX) until the post-commit publish window.
                            // Undo-logged: reverted if the checkpoint fails before commit.
                            self.ckpt_rootmap_alloc(table_id, created_root_page as u64);
                            self.staged_roots.push(table_id);
                        }
                        SpecialWrite::BTreeDestroy {
                            table_id,
                            root_page,
                            num_columns,
                        } => {
                            let known_root_page = self
                                .mvstore
                                .current_root_page(&table_id)
                                .expect("Table ID does not have a root page");
                            turso_assert_eq!(
                                known_root_page,
                                root_page,
                                "checkpoint root page mismatch for BTreeDestroy",
                                { "known_root_page": known_root_page, "schema_root_page": root_page }
                            );
                            let cursor = if let Some(cursor) = self.cursors.get(&known_root_page) {
                                cursor.clone()
                            } else {
                                let cursor = BTreeCursor::new_table(
                                    self.pager.clone(),
                                    known_root_page as i64,
                                    num_columns,
                                );
                                let cursor = Arc::new(RwLock::new(cursor));
                                self.cursors.insert(root_page, cursor.clone());
                                cursor
                            };
                            self.pager.io.block(|| cursor.write().btree_destroy())?;
                            // Evict stale cursor.
                            self.cursors.remove(&root_page);
                            self.destroyed_tables.insert(table_id);
                            self.freed_root_pages.insert(root_page as i64);
                            // Deferred destroy: retire the binding (set its `end` to the drop ts)
                            // but keep it, so a transaction still scanning this table at an older
                            // snapshot resolves the (read-mark-protected) root page. GC'd once
                            // `lwm` passes the drop. Defensively remove if the drop ts is unknown.
                            if let Some(drop_ts) = drop_ts {
                                self.ckpt_rootmap_retire(table_id, drop_ts);
                            } else {
                                self.ckpt_rootmap_remove(table_id);
                            }
                        }
                        SpecialWrite::BTreeCreateIndex { index_id, .. } => {
                            let created_root_page: u32 = self.pager.io.block(|| {
                                self.pager.btree_create(&CreateBTreeFlags::new_index())
                            })?;
                            // Staged (see BTreeCreate); published in the post-commit window.
                            // Undo-logged: reverted if the checkpoint fails before commit.
                            self.ckpt_rootmap_alloc(index_id, created_root_page as u64);
                            self.staged_roots.push(index_id);
                            // Index struct should already be stored in index_id_to_index from collect_committed_versions
                            turso_assert!(
                                self.index_id_to_index.contains_key(&index_id),
                                "checkpoint index struct missing before BTreeCreateIndex",
                                { "index_id": i64::from(index_id) }
                            );
                        }
                        SpecialWrite::BTreeDestroyIndex {
                            index_id,
                            root_page,
                            num_columns,
                        } => {
                            let known_root_page = self
                                .mvstore
                                .current_root_page(&index_id)
                                .expect("Index ID does not have a root page");
                            turso_assert_eq!(
                                known_root_page,
                                root_page,
                                "checkpoint root page mismatch for BTreeDestroyIndex",
                                { "known_root_page": known_root_page, "schema_root_page": root_page }
                            );

                            let cursor = if let Some(cursor) = self.cursors.get(&known_root_page) {
                                cursor.clone()
                            } else if let Some(index) = self.index_id_to_index.get(&index_id) {
                                let cursor = BTreeCursor::new_index(
                                    self.pager.clone(),
                                    known_root_page as i64,
                                    index.as_ref(),
                                    num_columns,
                                )?;
                                let cursor = Arc::new(RwLock::new(cursor));
                                self.cursors.insert(root_page, cursor.clone());
                                cursor
                            } else {
                                // DROP INDEX destroy path: schema may no longer contain the index definition.
                                // We only need a cursor to destroy pages so num_columns is not important.
                                Arc::new(RwLock::new(BTreeCursor::new_table(
                                    self.pager.clone(),
                                    known_root_page as i64,
                                    num_columns,
                                )))
                            };
                            self.pager.io.block(|| cursor.write().btree_destroy())?;
                            // Evict stale cursor.
                            self.cursors.remove(&root_page);
                            self.destroyed_indexes.insert(index_id);
                            self.freed_root_pages.insert(root_page as i64);
                            // Deferred destroy: retire the binding (set its `end`) but keep it so
                            // a transaction still scanning this index at an older snapshot resolves
                            // the (read-mark-protected) root page. GC'd once `lwm` passes the drop.
                            if let Some(drop_ts) = drop_ts {
                                self.ckpt_rootmap_retire(index_id, drop_ts);
                            } else {
                                self.ckpt_rootmap_remove(index_id);
                            }
                        }
                    }
                }

                if self.destroyed_tables.contains(&table_id) {
                    // Don't write rows for tables that will be destroyed in this checkpoint.
                    self.state = CheckpointState::WriteRow {
                        write_set_index: write_set_index + 1,
                        requires_seek: true,
                    };
                    return Ok(TransitionResult::Continue);
                }

                let is_delete = self
                    .get_current_row_version(write_set_index)
                    .is_some_and(|(v, _)| v.end().is_some());
                if is_delete && !self.table_exists_for_snapshot(table_id) {
                    self.state = CheckpointState::WriteRow {
                        write_set_index: write_set_index + 1,
                        requires_seek: true,
                    };
                    return Ok(TransitionResult::Continue);
                }

                let root_page = self.resolve_checkpoint_root(table_id).unwrap_or_else(|| {
                    panic!(
                        "Table ID does not have a root page: {table_id}, row_version: {:?}",
                        self.get_current_row_version(write_set_index)
                            .expect("row version should exist")
                    )
                });

                tracing::debug!("WriteRow: resolved root page: root_page={root_page}");

                // If a table was created, it now has a real root page allocated for it, but the 'root_page' field in the sqlite_schema record is still the table id.
                // So we need to rewrite the row version to use the real root page.
                if let Some(SpecialWrite::BTreeCreate {
                    table_id,
                    sqlite_schema_rowid,
                }) = special_write
                {
                    let root_page = self
                        .resolve_checkpoint_root(table_id)
                        .expect("Table ID does not have a root page");
                    let row_version = {
                        let alloc = self.mvstore.allocator();
                        let (row_version, _) = self
                            .get_current_row_version_mut(write_set_index)
                            .ok_or_else(|| {
                                LimboError::InternalError(
                                    "row version not found in write set".to_string(),
                                )
                            })?;
                        let record = ImmutableRecordRef::from_bin_record(row_version.row.payload());

                        let mut values = record.get_values_owned()?;
                        values[3] = Value::from_i64(root_page as i64);
                        let record = ImmutableRecord::from_values(&values, values.len())?;
                        // Btree creation has already happened by this point; an injected fault
                        // while publishing the sqlite_schema root page can leave retry state with
                        // a durable btree and a stale rootpage=0 schema row.
                        // TODO: make this rewrite resumable before re-enabling fault injection.
                        row_version.row.data = Some(crate::without_allocation_faults!(
                            crate::alloc::try_arc_slice_from_slice_in(record.get_payload(), alloc)?
                        ));
                        row_version.clone()
                    };
                    self.created_btrees
                        .insert(sqlite_schema_rowid, (table_id, row_version));
                } else if let Some(SpecialWrite::BTreeCreateIndex {
                    index_id,
                    sqlite_schema_rowid,
                }) = special_write
                {
                    // Same for index btrees.
                    let root_page = self
                        .resolve_checkpoint_root(index_id)
                        .expect("Index ID does not have a root page");
                    let row_version = {
                        let alloc = self.mvstore.allocator();
                        let (row_version, _) = self
                            .get_current_row_version_mut(write_set_index)
                            .ok_or_else(|| {
                                LimboError::InternalError(
                                    "row version not found in write set".to_string(),
                                )
                            })?;
                        let record = ImmutableRecordRef::from_bin_record(row_version.row.payload());
                        let mut values = record.get_values_owned()?;
                        values[3] = Value::from_i64(root_page as i64);
                        let record = ImmutableRecord::from_values(&values, values.len())?;
                        // Btree creation has already happened by this point; an injected fault
                        // while publishing the sqlite_schema root page can leave retry state with
                        // a durable btree and a stale rootpage=0 schema row.
                        // TODO: make this rewrite resumable before re-enabling fault injection.
                        row_version.row.data = Some(crate::without_allocation_faults!(
                            crate::alloc::try_arc_slice_from_slice_in(record.get_payload(), alloc)?
                        ));
                        row_version.clone()
                    };

                    self.created_btrees
                        .insert(sqlite_schema_rowid, (index_id, row_version));
                }

                // Get or create cursor for this table
                let cursor = if let Some(cursor) = self.cursors.get(&root_page) {
                    cursor.clone()
                } else {
                    let cursor =
                        BTreeCursor::new_table(self.pager.clone(), root_page as i64, num_columns);
                    let cursor = Arc::new(RwLock::new(cursor));
                    self.cursors.insert(root_page, cursor.clone());
                    cursor
                };

                let (row_version, _) =
                    self.get_current_row_version(write_set_index)
                        .ok_or_else(|| {
                            LimboError::InternalError(
                                "row version not found in write set".to_string(),
                            )
                        })?;

                // Check if this is an insert or delete
                if row_version.end().is_some() {
                    // This is a delete operation.
                    // Don't write the deletion record to the b-tree if the b-tree was just created; we can no-op in this case,
                    // since there is no existing row to delete.
                    if self
                        .created_btrees
                        .values()
                        .any(|(table_id, _)| *table_id == row_version.row.id.table_id)
                    {
                        self.state = CheckpointState::WriteRow {
                            write_set_index: write_set_index + 1,
                            requires_seek: true,
                        };
                        return Ok(TransitionResult::Continue);
                    }
                    let state_machine = self
                        .mvstore
                        .delete_row_from_pager(row_version.row.id.clone(), cursor)?;
                    self.delete_row_state_machine = Some(state_machine);
                    self.state = CheckpointState::DeleteRowStateMachine { write_set_index };
                } else {
                    // This is an insert/update operation
                    let state_machine =
                        self.mvstore
                            .write_row_to_pager(&row_version.row, cursor, requires_seek)?;
                    self.write_row_state_machine = Some(state_machine);
                    self.state = CheckpointState::WriteRowStateMachine { write_set_index };
                }

                Ok(TransitionResult::Continue)
            }

            CheckpointState::WriteRowStateMachine { write_set_index } => {
                let write_set_index = *write_set_index;
                let write_row_state_machine =
                    self.write_row_state_machine.as_mut().ok_or_else(|| {
                        LimboError::InternalError(
                            "write_row_state_machine not initialized".to_string(),
                        )
                    })?;

                match write_row_state_machine.step(&())? {
                    IOResult::IO(io) => Ok(TransitionResult::Io(io)),
                    IOResult::Done(_) => {
                        self.record_written_table_row(write_set_index);
                        let requires_seek = self.next_requires_seek_after_insert(write_set_index);
                        self.state = CheckpointState::WriteRow {
                            write_set_index: write_set_index + 1,
                            requires_seek,
                        };
                        Ok(TransitionResult::Continue)
                    }
                }
            }

            CheckpointState::DeleteRowStateMachine { write_set_index } => {
                let write_set_index = *write_set_index;
                let delete_row_state_machine =
                    self.delete_row_state_machine.as_mut().ok_or_else(|| {
                        LimboError::InternalError(
                            "delete_row_state_machine not initialized".to_string(),
                        )
                    })?;

                match delete_row_state_machine.step(&())? {
                    IOResult::IO(io) => Ok(TransitionResult::Io(io)),
                    IOResult::Done(_) => {
                        self.record_written_table_row(write_set_index);
                        self.state = CheckpointState::WriteRow {
                            write_set_index: write_set_index + 1,
                            requires_seek: true,
                        };
                        Ok(TransitionResult::Continue)
                    }
                }
            }

            CheckpointState::WriteIndexRow {
                index_write_set_index,
                requires_seek,
            } => {
                let index_write_set_index = *index_write_set_index;
                let requires_seek = *requires_seek;

                if index_write_set_index >= self.index_write_set.len() {
                    // Done writing all index rows, compact sequence
                    // backing tables, then commit.
                    self.state = CheckpointState::CompactSequences;
                    return Ok(TransitionResult::Continue);
                }

                let (index_id, row_version, is_delete) =
                    &self.index_write_set[index_write_set_index];

                // Skip destroyed indexes
                if self.destroyed_indexes.contains(index_id) {
                    self.state = CheckpointState::WriteIndexRow {
                        index_write_set_index: index_write_set_index + 1,
                        requires_seek: true,
                    };
                    return Ok(TransitionResult::Continue);
                }

                // The index is absent from the snapshot schema (index_id_to_index is built from
                // local_schema at snapshot_ts). That means the index does not exist at the
                // checkpoint snapshot — it was dropped — so its whole btree is (or will be)
                // destroyed wholesale. DROP does not tombstone each in-memory index-entry
                // version, so collect_index_rows still picks them up as live inserts/tombstones;
                // materializing them into the (possibly reused) root page would corrupt. Skip
                // every entry for a snapshot-absent index, mirroring the destroyed_indexes skip.
                let Some(index) = self.index_id_to_index.get(index_id) else {
                    self.state = CheckpointState::WriteIndexRow {
                        index_write_set_index: index_write_set_index + 1,
                        requires_seek: true,
                    };
                    return Ok(TransitionResult::Continue);
                };

                if *is_delete && !self.table_exists_for_snapshot(*index_id) {
                    self.state = CheckpointState::WriteIndexRow {
                        index_write_set_index: index_write_set_index + 1,
                        requires_seek: true,
                    };
                    return Ok(TransitionResult::Continue);
                }

                // Get root page for this index
                let root_page = self
                    .resolve_checkpoint_root(*index_id)
                    .unwrap_or_else(|| panic!("Index ID {index_id} does not have a root page"));

                // Get or create cursor for this index
                let cursor = if let Some(cursor) = self.cursors.get(&root_page) {
                    cursor.clone()
                } else {
                    let cursor = BTreeCursor::new_index(
                        self.pager.clone(),
                        root_page as i64,
                        index.as_ref(),
                        index.columns.len(),
                    )?;
                    let cursor = Arc::new(RwLock::new(cursor));
                    self.cursors.insert(root_page, cursor.clone());
                    cursor
                };

                // Check if this is an insert or delete
                if *is_delete {
                    // This is a delete operation. Don't write the deletion record to the b-tree if the b-tree was just created; we can no-op in this case,
                    // since there is no existing row to delete.
                    if self
                        .created_btrees
                        .values()
                        .any(|(table_id, _)| *table_id == row_version.row.id.table_id)
                    {
                        self.state = CheckpointState::WriteIndexRow {
                            index_write_set_index: index_write_set_index + 1,
                            requires_seek: true,
                        };
                        return Ok(TransitionResult::Continue);
                    }
                    let state_machine = self
                        .mvstore
                        .delete_row_from_pager(row_version.row.id.clone(), cursor)?;
                    self.delete_row_state_machine = Some(state_machine);
                    self.state = CheckpointState::DeleteIndexRowStateMachine {
                        index_write_set_index,
                    };
                } else {
                    // This is an insert/update operation
                    let state_machine =
                        self.mvstore
                            .write_row_to_pager(&row_version.row, cursor, requires_seek)?;
                    self.write_row_state_machine = Some(state_machine);
                    self.state = CheckpointState::WriteIndexRowStateMachine {
                        index_write_set_index,
                    };
                }

                Ok(TransitionResult::Continue)
            }

            CheckpointState::WriteIndexRowStateMachine {
                index_write_set_index,
            } => {
                let index_write_set_index = *index_write_set_index;
                let write_row_state_machine =
                    self.write_row_state_machine.as_mut().ok_or_else(|| {
                        LimboError::InternalError(
                            "write_row_state_machine not initialized".to_string(),
                        )
                    })?;

                match write_row_state_machine.step(&())? {
                    IOResult::IO(io) => Ok(TransitionResult::Io(io)),
                    IOResult::Done(_) => {
                        self.written_index_slots.insert(index_write_set_index);
                        self.state = CheckpointState::WriteIndexRow {
                            index_write_set_index: index_write_set_index + 1,
                            requires_seek: true,
                        };
                        Ok(TransitionResult::Continue)
                    }
                }
            }

            CheckpointState::DeleteIndexRowStateMachine {
                index_write_set_index,
            } => {
                let index_write_set_index = *index_write_set_index;
                let delete_row_state_machine =
                    self.delete_row_state_machine.as_mut().ok_or_else(|| {
                        LimboError::InternalError(
                            "delete_row_state_machine not initialized".to_string(),
                        )
                    })?;

                match delete_row_state_machine.step(&())? {
                    IOResult::IO(io) => Ok(TransitionResult::Io(io)),
                    IOResult::Done(_) => {
                        self.written_index_slots.insert(index_write_set_index);
                        self.state = CheckpointState::WriteIndexRow {
                            index_write_set_index: index_write_set_index + 1,
                            requires_seek: true,
                        };
                        Ok(TransitionResult::Continue)
                    }
                }
            }

            CheckpointState::CompactSequences => {
                if self.seq_compact.is_none() {
                    let pending = self.pending_sequence_compactions()?;
                    if pending.is_empty() {
                        self.state = CheckpointState::CommitPagerTxn;
                        return Ok(TransitionResult::Continue);
                    }
                    self.seq_compact = Some(SeqCompactDriver {
                        pending,
                        current_idx: 0,
                        cursor: None,
                        phase: SeqCompactPhase::SeekWatermark,
                        watermark_key: None,
                        pending_delete_rowid: None,
                        pager: self.pager.clone(),
                        mvstore: self.mvstore.clone(),
                        passive: matches!(self.mode, CheckpointMode::Passive { .. }),
                        compacted: crate::alloc::vec![],
                    });
                }
                let driver = self.seq_compact.as_mut().expect("seq_compact set above");
                match driver.step()? {
                    IOResult::IO(io) => Ok(TransitionResult::Io(io)),
                    IOResult::Done(()) => {
                        // Passive recorded its deletes instead of applying them; carry them to the
                        // clock-ordered publish window. Blocking applied them directly (empty).
                        let driver = self.seq_compact.take().expect("seq_compact set above");
                        self.pending_seq_deletes = driver.compacted;
                        self.state = CheckpointState::CommitPagerTxn;
                        Ok(TransitionResult::Continue)
                    }
                }
            }
            CheckpointState::CommitPagerTxn => {
                inject_transition_yield!(self, CheckpointYieldPoint::BeforePagerCommit);
                let passive = matches!(self.mode, CheckpointMode::Passive { .. });
                let passive_auto_publish_retry = passive && !self.update_transaction_state;
                // Passive: btree commit and publish run off the RW lock (drain bit only).
                let lock_before_commit = !passive;
                if lock_before_commit && !self.lock_states.blocking_checkpoint_lock_held {
                    if !self.checkpoint_lock.write() {
                        return Err(crate::LimboError::Busy);
                    }
                    self.lock_states.blocking_checkpoint_lock_held = true;
                }
                if !self.pager_commit_done {
                    if !self.header_staged_for_commit {
                        let mut checkpoint_header =
                            *self.mvstore.global_header.read().as_ref().ok_or_else(|| {
                                LimboError::InternalError(
                                    "global_header not initialized during checkpoint".to_string(),
                                )
                            })?;
                        checkpoint_header.schema_cookie =
                            self.database.schema.lock().schema_version.into();
                        let staged_header = self.pager.io.block(|| {
                            self.pager.with_header_mut(|header| {
                                // Keep pager-maintained fields (for example database_size/change_counter)
                                // intact, and apply only MVCC header mutations that are authored via
                                // SetCookie/PRAGMA paths.
                                header.schema_cookie = checkpoint_header.schema_cookie;
                                header.user_version = checkpoint_header.user_version;
                                header.application_id = checkpoint_header.application_id;
                                header.vacuum_mode_largest_root_page =
                                    checkpoint_header.vacuum_mode_largest_root_page;
                                header.incremental_vacuum_enabled =
                                    checkpoint_header.incremental_vacuum_enabled;
                                *header
                            })
                        })?;
                        self.staged_checkpoint_header = Some(staged_header);
                        self.header_staged_for_commit = true;
                    }
                    // On commit_tx failure the `?` rolls back the pager txn; durable_txid_max and
                    // the log offset stay put, so a retry re-stages from the previous boundary.
                    tracing::debug!("Committing pager transaction");
                    match self.pager.commit_tx(
                        &self.connection,
                        self.sync_mode,
                        self.update_transaction_state,
                    )? {
                        IOResult::Done(_) => {
                            self.pager_commit_done = true;
                        }
                        IOResult::IO(io) => return Ok(TransitionResult::Io(io)),
                    }
                }
                if passive {
                    if !self.mvstore.try_begin_passive_publish_window() {
                        if passive_auto_publish_retry {
                            tracing::debug!(
                                "passive checkpoint publish contended; yielding for retry"
                            );
                            return Ok(TransitionResult::Io(
                                IOCompletions(Completion::new_yield()),
                            ));
                        }
                        return Err(crate::LimboError::Busy);
                    }
                    let materialized_at = WalPos::from_pair(self.pager.wal_pos());
                    self.apply_passive_publish_window_ordered(materialized_at)?;
                } else if !self.lock_states.blocking_checkpoint_lock_held {
                    if !self.checkpoint_lock.write() {
                        return Err(crate::LimboError::Busy);
                    }
                    self.lock_states.blocking_checkpoint_lock_held = true;
                    let materialized_at = WalPos::from_pair(self.pager.wal_pos());
                    // Blocking holds the lock: SeqCompact applied its deletes+purge directly under
                    // the contract, so there are no recorded passive deletes to publish (None).
                    self.apply_checkpoint_publish_window(materialized_at, None)?;
                } else {
                    let materialized_at = WalPos::from_pair(self.pager.wal_pos());
                    self.apply_checkpoint_publish_window(materialized_at, None)?;
                }
                inject_transition_failure!(
                    self,
                    CheckpointYieldPoint::AfterDurableBoundaryAdvanced
                );
                inject_transition_yield!(self, CheckpointYieldPoint::AfterDurableBoundaryAdvanced);
                Ok(TransitionResult::Continue)
            }

            CheckpointState::TruncateLogicalLog => {
                tracing::debug!("Truncating logical log file");
                let c = self.truncate_logical_log()?;
                self.state = CheckpointState::FsyncLogicalLog;
                // if Completion Completed without errors we can continue
                if c.succeeded() {
                    if self.mode.should_restart_log() {
                        turso_assert!(
                            self.mvstore.storage.logical_log_offset() == 0,
                            "TRUNCATE checkpoint must reset logical log offset to 0"
                        );
                        turso_assert!(
                            self.mvstore
                                .get_logical_log_file()
                                .size()
                                .expect("logical log file size should be readable after truncate")
                                == 0,
                            "TRUNCATE checkpoint must zero the logical log file"
                        );
                    }
                    Ok(TransitionResult::Continue)
                } else {
                    Ok(TransitionResult::Io(IOCompletions(c)))
                }
            }

            CheckpointState::FsyncLogicalLog => {
                // Skip fsync when synchronous mode is off
                if self.sync_mode == SyncMode::Off {
                    tracing::debug!("Skipping fsync of logical log file (synchronous=off)");
                    self.state = CheckpointState::TruncateWal;
                    return Ok(TransitionResult::Continue);
                }
                tracing::debug!("Fsyncing logical log file");
                let c = self.fsync_logical_log()?;
                self.state = CheckpointState::TruncateWal;
                // if Completion Completed without errors we can continue
                if c.succeeded() {
                    Ok(TransitionResult::Continue)
                } else {
                    Ok(TransitionResult::Io(IOCompletions(c)))
                }
            }

            CheckpointState::CheckpointWal => {
                tracing::debug!("Performing checkpoint on WAL");
                match self.checkpoint_wal() {
                    Ok(IOResult::Done(result)) => {
                        self.checkpoint_result = Some(result);
                        self.state = CheckpointState::SyncDbFile;
                        Ok(TransitionResult::Continue)
                    }
                    Ok(IOResult::IO(io)) => Ok(TransitionResult::Io(io)),
                    // Busy under a DbFile reader: finish without publishing nbackfills.
                    Err(err)
                        if matches!(*err, crate::LimboError::Busy)
                            && matches!(self.mode, CheckpointMode::Passive { .. }) =>
                    {
                        tracing::debug!(
                            "Passive WAL checkpoint Busy under pinned DbFile reader; continuing without backfill"
                        );
                        let (max_frame, nbackfills) = self
                            .pager
                            .wal
                            .as_ref()
                            .map(|wal| (wal.get_max_frame_in_wal(), wal.backfill_frame()))
                            .unwrap_or((0, 0));
                        self.checkpoint_result =
                            Some(CheckpointResult::new(max_frame, nbackfills, 0));
                        self.state = CheckpointState::TruncateLogicalLog;
                        Ok(TransitionResult::Continue)
                    }
                    Err(e) => Err(*e),
                }
            }

            CheckpointState::SyncDbFile => {
                // Fsync DB before WAL truncate / publish_backfill (pager PublishBackfill order).
                // Crash after truncate but before fsync would lose checkpointed data.
                if self.sync_mode == SyncMode::Off {
                    tracing::debug!("Skipping fsync of database file (synchronous=off)");
                    self.publish_wal_backfill_if_needed();
                    self.state = CheckpointState::TruncateLogicalLog;
                    return Ok(TransitionResult::Continue);
                }

                let checkpoint_result = self
                    .checkpoint_result
                    .as_mut()
                    .expect("checkpoint_result should be set");

                // Only sync if we actually backfilled any frames
                if checkpoint_result.wal_checkpoint_backfilled == 0 {
                    self.state = CheckpointState::TruncateLogicalLog;
                    return Ok(TransitionResult::Continue);
                }

                // Check if we already sent the sync
                if checkpoint_result.db_sync_sent {
                    self.publish_wal_backfill_if_needed();
                    self.state = CheckpointState::TruncateLogicalLog;
                    return Ok(TransitionResult::Continue);
                }

                tracing::debug!("Fsyncing database file before WAL truncation");
                let c = self
                    .pager
                    .db_file
                    .sync(Completion::new_sync(|_| {}), self.pager.get_sync_type())?;
                checkpoint_result.db_sync_sent = true;
                Ok(TransitionResult::Io(IOCompletions(c)))
            }

            CheckpointState::TruncateWal => {
                if self.mode.should_restart_log() {
                    // Truncate/Restart renumbers WAL frames — only safe stop-the-world. Acquire
                    // the lock if the blocking path didn't already. Passive never restarts the
                    // log, so it skips this branch and stays lock-free.
                    if !self.lock_states.blocking_checkpoint_lock_held {
                        if !self.checkpoint_lock.write() {
                            return Err(crate::LimboError::Busy);
                        }
                        self.lock_states.blocking_checkpoint_lock_held = true;
                    }
                    // Zero the WAL file explicitly: MVCC calls wal.checkpoint() directly,
                    // bypassing the pager's TruncateWalFile. Resumable on IO until Done.
                    let Some(wal) = &self.pager.wal else {
                        panic!("No WAL to truncate");
                    };
                    let checkpoint_result = self
                        .checkpoint_result
                        .as_mut()
                        .expect("checkpoint_result should be set");
                    if let IOResult::IO(io) =
                        wal.truncate_wal(checkpoint_result, self.pager.get_sync_type())?
                    {
                        return Ok(TransitionResult::Io(io));
                    }
                }
                // Passive leaves the WAL non-empty; the logical log is already truncated, so
                // recovery sees NoLog + committed WAL — the normal passive steady state.
                // Scope to THIS checkpoint's own staged work: a no-op (nothing-to-write) pass
                // staged nothing, and a real publish drains both. A global schema scan would
                // false-trip on owned-negative leftovers from an earlier checkpoint that mapped
                // a root positive but couldn't patch the (not-yet-adopted) live schema — benign,
                // since cursors resolve negative->positive via table_id_to_rootpage.
                turso_assert!(
                    self.created_btrees.is_empty() && self.staged_roots.is_empty(),
                    "checkpoint finalized with un-published staged schema roots"
                );
                self.mvstore
                    .durable_txid_max
                    .store(self.durable_txid_max_new, Ordering::SeqCst);
                // Publish the WAL backfill boundary as the passive checkpoint GC floor: a version
                // materialized at or below it is durable in the DB file, hence reachable by
                // every snapshot. Un-backfilled ones stay retained for low-frame readers.
                let (seq, _) = self.pager.wal_pos();
                let backfill =
                    WalPos::from_pair((seq, self.pager.wal_backfill_frame().unwrap_or(0)));
                *self.mvstore.backfill_floor.write() = backfill;
                let lwm = self.mvstore.sample_gc_lwm();
                // Reclaim retired root-page bindings no transaction can still see (end <= lwm).
                self.mvstore.gc_rootpage_entries(lwm);
                self.state = CheckpointState::GcTableRows { next_index: 0, lwm };
                Ok(TransitionResult::Continue)
            }

            CheckpointState::GcTableRows { .. } => {
                if let Some(io) = self.gc_checkpointed_table_versions() {
                    return Ok(TransitionResult::Io(io));
                }
                let CheckpointState::GcTableRows { lwm, .. } = self.state else {
                    unreachable!("state is GcTableRows here");
                };
                self.state = CheckpointState::GcIndexRows { next_index: 0, lwm };
                Ok(TransitionResult::Continue)
            }

            CheckpointState::GcIndexRows { .. } => {
                if let Some(io) = self.gc_checkpointed_index_versions() {
                    return Ok(TransitionResult::Io(io));
                }
                self.state = CheckpointState::Finalize;
                Ok(TransitionResult::Continue)
            }

            CheckpointState::Finalize => {
                if self.lock_states.blocking_checkpoint_lock_held {
                    // Truncate: under the blocking lock, drop last SkipMap copies and empty slots.
                    // That lock waits out open MVCC txs, so no old reader can see a later rewrite.
                    self.mvstore.drop_unused_row_versions_and_slots();
                } else {
                    // Passive: drop superseded history and empty slots. Rule 3 also
                    // drops last currents when idle (`lwm == MAX`); with open
                    // snapshots those stay in the SkipMap. Use the pager reader
                    // mark so we don't GC versions a brand-new reader still needs
                    // before its MVCC tx is registered.
                    self.mvstore
                        .drop_unused_row_versions_unlink_empty_at(self.gc_floor_reader_mark());
                }
                // Locks stay held until `step()` runs `on_checkpoint_end`, then
                // `release_checkpoint_locks_if_needed`.
                self.finalize(&())?;
                Ok(TransitionResult::Done(
                    self.checkpoint_result.take().ok_or_else(|| {
                        LimboError::InternalError("checkpoint_result not set".to_string())
                    })?,
                ))
            }
        }
    }
}

impl<Clock: LogicalClock, A: ConcurrentAllocator> StateTransition
    for CheckpointStateMachine<Clock, A>
{
    type Context = ();
    type SMResult = CheckpointResult;

    fn step(&mut self, _context: &Self::Context) -> Result<TransitionResult<Self::SMResult>> {
        let res = self.step_inner(&());
        match res {
            Err(ref err) => {
                tracing::debug!("Error in checkpoint state machine: {err}");
                // cleanup already calls on_checkpoint_end + unlock
                self.cleanup_after_external_io_error(err.clone())?;
                res
            }
            Ok(TransitionResult::Done(ref result)) => {
                // End hook before unlock so storage cannot race the next writer/checkpoint.
                let end = self.mvstore.storage.on_checkpoint_end(Ok(result));
                self.release_checkpoint_locks_if_needed();
                end?;
                res
            }
            Ok(result) => Ok(result),
        }
    }

    fn finalize(&mut self, _context: &Self::Context) -> Result<()> {
        Ok(())
    }

    fn is_finalized(&self) -> bool {
        matches!(self.state, CheckpointState::Finalize)
    }
}

/// Re-entrant state machine that builds a snapshot-consistent `Schema` for the
/// checkpoint: scans the on-disk `sqlite_schema` B-tree (root page 1) and overlays the
/// MVCC delta visible at `snapshot_ts`, matching exactly the rows the checkpoint
/// collects. The live schema would include post-snapshot objects and mis-map index ids.
enum BuildLocalSchemaViewState {
    Rewind,
    ReadRowid,
    ReadRecord { rowid: i64 },
    Advance,
    MergeMvccDelta,
    Done,
}

pub struct BuildLocalSchemaViewStateMachine<
    Clock: LogicalClock,
    A: ConcurrentAllocator = TursoAllocator,
> {
    cursor: BTreeCursor,
    mvstore: Arc<MvStore<Clock, A>>,
    connection: Arc<Connection>,
    snapshot_ts: u64,
    state: BuildLocalSchemaViewState,
    rows: HashMap<i64, ImmutableRecordRef<'static>>,
    finalized: bool,
}

impl<Clock: LogicalClock, A: ConcurrentAllocator> BuildLocalSchemaViewStateMachine<Clock, A> {
    fn new(
        cursor: BTreeCursor,
        mvstore: Arc<MvStore<Clock, A>>,
        connection: Arc<Connection>,
        snapshot_ts: u64,
    ) -> Self {
        Self {
            cursor,
            mvstore,
            connection,
            snapshot_ts,
            state: BuildLocalSchemaViewState::Rewind,
            rows: HashMap::default(),
            finalized: false,
        }
    }

    /// Overlay the in-memory MVCC sqlite_schema versions onto the rows read from
    /// the B-tree, keeping only the version live at `snapshot_ts` and removing
    /// rows whose live-at-snapshot state is a delete.
    fn merge_mvcc_delta(&mut self) {
        let snapshot_ts = self.snapshot_ts;
        for entry in self.mvstore.rows.iter() {
            let key = entry.key();
            if key.table_id != SQLITE_SCHEMA_MVCC_TABLE_ID {
                continue;
            }
            let rowid = key.row_id.to_int_or_panic();
            let versions = entry.value().read();
            let present = versions.iter().find(|version| {
                let begin_committed = matches!(
                    version.begin(),
                    Some(TxTimestampOrID::Timestamp(b)) if b <= snapshot_ts
                );
                if !begin_committed {
                    return false;
                }
                match version.end() {
                    None => true,
                    Some(TxTimestampOrID::Timestamp(e)) => e > snapshot_ts,
                    Some(TxTimestampOrID::TxID(_)) => true,
                }
            });
            match present {
                Some(version) => {
                    let data = version
                        .row
                        .data
                        .as_ref()
                        .expect("present schema version must carry row data at snapshot_ts");
                    self.rows
                        .insert(rowid, ImmutableRecordRef::from_shared_record(data.clone()));
                }
                None => {
                    let existed_and_gone = versions.iter().any(|version| {
                        matches!(
                            version.begin(),
                            Some(TxTimestampOrID::Timestamp(b)) if b <= snapshot_ts
                        ) || matches!(
                            version.end(),
                            Some(TxTimestampOrID::Timestamp(e)) if e <= snapshot_ts
                        )
                    });
                    if existed_and_gone {
                        self.rows.remove(&rowid);
                    }
                }
            }
        }
    }
}

impl<Clock: LogicalClock, A: ConcurrentAllocator> StateTransition
    for BuildLocalSchemaViewStateMachine<Clock, A>
{
    type Context = ();
    type SMResult = Arc<Schema>;

    fn step(&mut self, _context: &()) -> Result<TransitionResult<Self::SMResult>> {
        match self.state {
            BuildLocalSchemaViewState::Rewind => match self.cursor.rewind()? {
                IOResult::IO(io) => Ok(TransitionResult::Io(io)),
                IOResult::Done(()) => {
                    self.state = BuildLocalSchemaViewState::ReadRowid;
                    Ok(TransitionResult::Continue)
                }
            },
            BuildLocalSchemaViewState::ReadRowid => {
                if !self.cursor.has_record() {
                    self.state = BuildLocalSchemaViewState::MergeMvccDelta;
                    return Ok(TransitionResult::Continue);
                }
                match self.cursor.rowid()? {
                    IOResult::IO(io) => Ok(TransitionResult::Io(io)),
                    IOResult::Done(Some(rowid)) => {
                        self.state = BuildLocalSchemaViewState::ReadRecord { rowid };
                        Ok(TransitionResult::Continue)
                    }
                    IOResult::Done(None) => {
                        self.state = BuildLocalSchemaViewState::Advance;
                        Ok(TransitionResult::Continue)
                    }
                }
            }
            BuildLocalSchemaViewState::ReadRecord { rowid } => {
                let record = match self.cursor.record()? {
                    IOResult::IO(io) => return Ok(TransitionResult::Io(io)),
                    IOResult::Done(Some(record)) => Some(record.clone()),
                    IOResult::Done(None) => None,
                };
                if let Some(record) = record {
                    self.rows
                        .insert(rowid, ImmutableRecordRef::from_owned_record(record));
                }
                self.state = BuildLocalSchemaViewState::Advance;
                Ok(TransitionResult::Continue)
            }
            BuildLocalSchemaViewState::Advance => match self.cursor.next()? {
                IOResult::IO(io) => Ok(TransitionResult::Io(io)),
                IOResult::Done(()) => {
                    self.state = BuildLocalSchemaViewState::ReadRowid;
                    Ok(TransitionResult::Continue)
                }
            },
            BuildLocalSchemaViewState::MergeMvccDelta => {
                self.merge_mvcc_delta();
                self.state = BuildLocalSchemaViewState::Done;
                Ok(TransitionResult::Continue)
            }
            BuildLocalSchemaViewState::Done => {
                self.finalized = true;
                let schema =
                    self.mvstore
                        .build_schema_from_rows(&self.connection, &self.rows, &[])?;
                Ok(TransitionResult::Done(schema))
            }
        }
    }

    fn finalize(&mut self, _context: &()) -> Result<()> {
        self.finalized = true;
        Ok(())
    }

    fn is_finalized(&self) -> bool {
        self.finalized
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alloc::vec;
    use crate::mvcc::database::tests::MvccTestDbNoConn;
    use crate::mvcc::database::SortableIndexKey;
    use crate::translate::collate::CollationSeq;
    use crate::types::{IndexInfo, KeyInfo};
    use turso_parser::ast::SortOrder;

    fn sqlite_schema_row_version(
        rowid: i64,
        entry_type: &'static str,
        name: &'static str,
        table_name: &'static str,
        root_page: i64,
        begin: Option<u64>,
        end: Option<u64>,
    ) -> RowVersion {
        let record = ImmutableRecord::from_values(
            &[
                Value::build_text(entry_type),
                Value::build_text(name),
                Value::build_text(table_name),
                Value::from_i64(root_page),
                Value::build_text(format!("sql:{entry_type}:{name}:{root_page}")),
            ],
            5,
        )
        .unwrap();
        RowVersion {
            id: 1,
            begin: crate::mvcc::database::PackedTs::pack(begin.map(TxTimestampOrID::Timestamp)),
            end: crate::mvcc::database::PackedTs::pack(end.map(TxTimestampOrID::Timestamp)),
            row: Row::new_table_row(
                RowID::new(SQLITE_SCHEMA_MVCC_TABLE_ID, RowKey::Int(rowid)),
                record.as_blob(),
                5,
            )
            .unwrap(),
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        }
    }

    #[test]
    fn local_schema_record_shares_mvcc_payload() {
        let version = sqlite_schema_row_version(2, "table", "t", "t", -2, Some(1), None);
        let payload = version.row.data.as_ref().unwrap();
        let payload_ptr = payload.as_ptr();

        let record = ImmutableRecordRef::from_shared_record(payload.clone());

        assert_eq!(record.get_payload().as_ptr(), payload_ptr);
        assert_eq!(record.column_count(), SQLITE_SCHEMA_COLUMN_COUNT);
    }

    #[test]
    fn sqlite_schema_identity_treats_index_sql_rewrite_as_same_object() {
        let old = sqlite_schema_row_version(3, "index", "idx_t_a", "t", 7, Some(1), Some(2));
        let new = sqlite_schema_row_version(3, "index", "idx_t_a", "t", 7, Some(2), None);

        assert_eq!(
            sqlite_schema_btree_identity(&old),
            Some(SqliteSchemaBtreeIdentity {
                kind: SqliteSchemaBtreeKind::Index,
                root_page: 7,
            })
        );
        assert!(sqlite_schema_versions_refer_to_btree(&old, &new));
        assert!(!is_schema_metadata_only_rewrite(&old, Some(&new)));
    }

    #[test]
    fn sqlite_schema_identity_treats_table_sql_rewrite_as_same_object() {
        let old = sqlite_schema_row_version(2, "table", "t", "t", 5, Some(1), Some(2));
        let new = sqlite_schema_row_version(2, "table", "t", "t", 5, Some(2), None);

        assert!(sqlite_schema_versions_refer_to_btree(&old, &new));
        assert!(!is_schema_metadata_only_rewrite(&old, Some(&new)));
    }

    #[test]
    fn sqlite_schema_identity_detects_drop_recreate_as_different_objects() {
        let dropped = sqlite_schema_row_version(3, "index", "idx_t_v", "t", -4, Some(1), Some(2));
        let recreated = sqlite_schema_row_version(3, "index", "idx_t_v", "t", -5, Some(2), None);

        assert!(!sqlite_schema_versions_refer_to_btree(&dropped, &recreated));
        assert!(is_schema_metadata_only_rewrite(&dropped, Some(&recreated)));
    }

    #[test]
    fn sqlite_schema_identity_detects_drop_without_successor() {
        let dropped = sqlite_schema_row_version(3, "index", "idx_t_v", "t", 11, Some(1), Some(2));

        assert!(is_schema_metadata_only_rewrite(&dropped, None));
    }

    #[test]
    fn sqlite_schema_identity_ignores_non_btree_schema_entries() {
        let trigger = sqlite_schema_row_version(9, "trigger", "trg_t", "t", 0, Some(1), Some(2));
        let rewritten_trigger =
            sqlite_schema_row_version(9, "trigger", "trg_t", "t", 0, Some(2), None);

        assert_eq!(sqlite_schema_btree_identity(&trigger), None);
        assert!(!sqlite_schema_versions_refer_to_btree(
            &trigger,
            &rewritten_trigger
        ));
        assert!(!is_schema_metadata_only_rewrite(
            &trigger,
            Some(&rewritten_trigger)
        ));
    }

    #[test]
    fn sqlite_schema_identity_ignores_payloadless_tombstones() {
        let tombstone = RowVersion {
            id: 1,
            begin: crate::mvcc::database::PackedTs::pack(None),
            end: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::Timestamp(2))),
            row: Row::new_table_row(
                RowID::new(SQLITE_SCHEMA_MVCC_TABLE_ID, RowKey::Int(9)),
                &[],
                0,
            )
            .unwrap(),
            btree_resident: false,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        };

        assert_eq!(sqlite_schema_btree_identity(&tombstone), None);
        assert!(!is_schema_metadata_only_rewrite(&tombstone, None));
    }

    fn index_row_version(
        index_id: MVTableId,
        key_text: &str,
        rowid: i64,
        version_id: u64,
        begin: Option<u64>,
        end: Option<u64>,
        btree_resident: bool,
    ) -> (Arc<SortableIndexKey>, RowVersion) {
        let index_info = Arc::new(
            IndexInfo::new(
                vec![
                    KeyInfo {
                        sort_order: SortOrder::Asc,
                        collation: CollationSeq::Binary,
                        nulls_order: None,
                    },
                    KeyInfo {
                        sort_order: SortOrder::Asc,
                        collation: CollationSeq::Binary,
                        nulls_order: None,
                    },
                ],
                true,
                2,
                true,
            )
            .unwrap(),
        );
        let key_record = ImmutableRecord::from_values(
            &[
                Value::Text(crate::types::Text::new(key_text.to_string())),
                Value::from_i64(rowid),
            ],
            2,
        )
        .unwrap();
        let sortable_key =
            SortableIndexKey::new_from_payload_in(&key_record, index_info, TursoAllocator).unwrap();
        let key_arc = Arc::new(sortable_key.clone());
        let row = Row::new_index_row(
            RowID::new(index_id, RowKey::Record(Arc::new(sortable_key))),
            2,
        );
        let row_version = RowVersion {
            id: version_id,
            begin: crate::mvcc::database::PackedTs::pack(begin.map(TxTimestampOrID::Timestamp)),
            end: crate::mvcc::database::PackedTs::pack(end.map(TxTimestampOrID::Timestamp)),
            row,
            btree_resident,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        };
        (key_arc, row_version)
    }

    #[test]
    fn checkpoint_retry_does_not_replay_checkpointed_btree_resident_delete() {
        let db = MvccTestDbNoConn::new();
        let conn = db.connect();
        let mvstore = db.get_mvcc_store();
        let pager = conn.pager.load().clone();
        let mut checkpoint = CheckpointStateMachine::new(
            pager,
            mvstore.clone(),
            conn.clone(),
            true,
            conn.get_sync_mode(),
            crate::MAIN_DB_ID,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );
        checkpoint.durable_txid_max_old = std::num::NonZeroU64::new(10);
        checkpoint.durable_txid_max_new = 10;

        let index_id = MVTableId::from(-42);
        let (garbage_key, garbage_version) =
            index_row_version(index_id, "blue_river_906", 75, 1, None, None, true);
        let (_, tombstone_version) =
            index_row_version(index_id, "blue_river_906", 75, 2, None, Some(10), true);

        mvstore
            .insert_index_version(index_id, garbage_key, garbage_version)
            .unwrap();
        let entry = mvstore
            .index_rows
            .get(&index_id)
            .expect("index entry should exist after first insert");
        let tombstone_key = entry
            .value()
            .front()
            .expect("key bucket should exist after first insert")
            .key()
            .clone();
        mvstore
            .insert_index_version(index_id, tombstone_key, tombstone_version)
            .unwrap();

        while checkpoint.collect_index_rows().unwrap().is_some() {}

        assert!(
            checkpoint.index_write_set.is_empty(),
            "a retry checkpoint must not replay a delete whose btree_resident tombstone was already made durable"
        );
    }

    fn committed_table_row_version(table_id: MVTableId, rowid: i64) -> RowVersion {
        table_row_version(table_id, rowid, 1, Some(5), None, false)
    }

    fn table_row_version(
        table_id: MVTableId,
        rowid: i64,
        version_id: u64,
        begin: Option<u64>,
        end: Option<u64>,
        btree_resident: bool,
    ) -> RowVersion {
        let record = ImmutableRecord::from_values(&[Value::from_i64(rowid)], 1).unwrap();
        RowVersion {
            id: version_id,
            begin: crate::mvcc::database::PackedTs::pack(begin.map(TxTimestampOrID::Timestamp)),
            end: crate::mvcc::database::PackedTs::pack(end.map(TxTimestampOrID::Timestamp)),
            row: Row::new_table_row(
                RowID::new(table_id, RowKey::Int(rowid)),
                record.as_blob(),
                1,
            )
            .unwrap(),
            btree_resident,
            materialized_at: crate::mvcc::database::WalPos::ORIGIN,
        }
    }

    fn checkpoint_for_collect_tests(
    ) -> CheckpointStateMachine<crate::mvcc::clock::MvccClock, crate::alloc::DynAllocator> {
        let db = MvccTestDbNoConn::new();
        let conn = db.connect();
        let mvstore = db.get_mvcc_store();
        let pager = conn.pager.load().clone();
        let mut checkpoint = CheckpointStateMachine::new(
            pager,
            mvstore,
            conn.clone(),
            true,
            conn.get_sync_mode(),
            crate::MAIN_DB_ID,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );
        checkpoint.durable_txid_max_old = NonZeroU64::new(2);
        checkpoint
    }

    #[test]
    fn checkpoint_collection_uses_btree_marker_for_existence_but_writes_surviving_replacement() {
        let checkpoint = checkpoint_for_collect_tests();
        let table_id = MVTableId::from(-2);
        let btree_tombstone = table_row_version(table_id, 1, 1, None, Some(5), true);
        let replacement = table_row_version(table_id, 1, 2, Some(5), None, false);

        let checkpointable =
            checkpoint.maybe_get_checkpointable_versions(&[btree_tombstone, replacement], table_id);

        assert_eq!(checkpointable.len(), 1);
        assert_eq!(checkpointable[0].id, 2);
        assert_eq!(checkpointable[0].end(), None);
    }

    #[test]
    fn checkpoint_collection_uses_btree_marker_for_later_delete_of_replacement() {
        let checkpoint = checkpoint_for_collect_tests();
        let table_id = MVTableId::from(-2);
        let btree_tombstone = table_row_version(table_id, 1, 1, None, Some(5), true);
        let deleted_replacement = table_row_version(table_id, 1, 2, Some(5), Some(6), false);

        let checkpointable = checkpoint
            .maybe_get_checkpointable_versions(&[btree_tombstone, deleted_replacement], table_id);

        assert_eq!(checkpointable.len(), 1);
        assert_eq!(checkpointable[0].id, 2);
        assert_eq!(checkpointable[0].end(), Some(TxTimestampOrID::Timestamp(6)));
    }

    #[test]
    fn checkpoint_collection_skips_delete_of_never_checkpointed_replacement_without_btree_marker() {
        let checkpoint = checkpoint_for_collect_tests();
        let table_id = MVTableId::from(-2);
        let deleted_replacement = table_row_version(table_id, 1, 2, Some(5), Some(6), false);

        let checkpointable =
            checkpoint.maybe_get_checkpointable_versions(&[deleted_replacement], table_id);

        assert!(checkpointable.is_empty());
    }

    #[test]
    fn collect_table_rows_preempts_on_large_scan() {
        let db = MvccTestDbNoConn::new();
        let conn = db.connect();
        let mvstore = db.get_mvcc_store();
        let pager = conn.pager.load().clone();
        let mut checkpoint = CheckpointStateMachine::new(
            pager,
            mvstore.clone(),
            conn.clone(),
            true,
            conn.get_sync_mode(),
            crate::MAIN_DB_ID,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );

        // More than one chunk worth of committed rows so collection must preempt.
        let table_id = MVTableId::from(-2);
        let row_count = COLLECT_PREEMPTION_THRESHOLD + 10;
        for i in 0..row_count as i64 {
            let version = committed_table_row_version(table_id, i);
            let mut versions =
                <crate::mvcc::database::RowVersionChain<crate::alloc::DynAllocator> as crate::alloc::TursoVecInExt<
                    RowVersion,
                    crate::alloc::DynAllocator,
                >>::new_in(crate::alloc::DynAllocator::default());
            versions.push(version);
            mvstore.rows.insert(
                RowID::new(table_id, RowKey::Int(i)),
                Arc::new(RwLock::new(versions)),
            );
        }

        // The first chunk fills up before the scan finishes, so it must yield.
        let first = checkpoint.collect_table_rows().unwrap();
        assert!(
            first.is_some_and(|io| io.is_explicit_yield()),
            "scanning more than COLLECT_PREEMPTION_THRESHOLD rows must preempt with an explicit yield"
        );

        // Resume from the cursor until the scan finishes; every row must still
        // be collected exactly once across the chunks.
        while checkpoint.collect_table_rows().unwrap().is_some() {}
        assert_eq!(checkpoint.write_set.len(), row_count);
    }

    #[test]
    fn collect_index_rows_preempts_on_large_scan() {
        let db = MvccTestDbNoConn::new();
        let conn = db.connect();
        let mvstore = db.get_mvcc_store();
        let pager = conn.pager.load().clone();
        let mut checkpoint = CheckpointStateMachine::new(
            pager,
            mvstore.clone(),
            conn.clone(),
            true,
            conn.get_sync_mode(),
            crate::MAIN_DB_ID,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );

        let index_id = MVTableId::from(-7);
        let row_count = COLLECT_PREEMPTION_THRESHOLD + 10;
        for i in 0..row_count as i64 {
            let (key, version) = index_row_version(index_id, "k", i, 1, Some(5), None, false);
            mvstore
                .insert_index_version(index_id, key, version)
                .unwrap();
        }

        let first = checkpoint.collect_index_rows().unwrap();
        assert!(
            first.is_some_and(|io| io.is_explicit_yield()),
            "scanning more than COLLECT_PREEMPTION_THRESHOLD index rows must preempt with an explicit yield"
        );

        while checkpoint.collect_index_rows().unwrap().is_some() {}
        assert_eq!(checkpoint.index_write_set.len(), row_count);
    }

    #[test]
    fn gc_checkpointed_table_versions_preempts_on_large_scan() {
        let db = MvccTestDbNoConn::new();
        let conn = db.connect();
        let mvstore = db.get_mvcc_store();
        let pager = conn.pager.load().clone();
        let mut checkpoint = CheckpointStateMachine::new(
            pager,
            mvstore.clone(),
            conn.clone(),
            true,
            conn.get_sync_mode(),
            crate::MAIN_DB_ID,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );
        checkpoint.lock_states.blocking_checkpoint_lock_held = true;
        checkpoint.durable_txid_max_new = 5;

        let table_id = MVTableId::from(-2);
        let row_count = COLLECT_PREEMPTION_THRESHOLD + 10;
        for i in 0..row_count as i64 {
            let version = committed_table_row_version(table_id, i);
            let row_id = RowID::new(table_id, RowKey::Int(i));
            let mut versions =
                <crate::mvcc::database::RowVersionChain<crate::alloc::DynAllocator> as crate::alloc::TursoVecInExt<
                    RowVersion,
                    crate::alloc::DynAllocator,
                >>::new_in(crate::alloc::DynAllocator::default());
            versions.push(version.clone());
            mvstore.rows.insert(row_id, Arc::new(RwLock::new(versions)));
            checkpoint.write_set.push((version, None));
            checkpoint.written_table_rowids.insert((table_id, i));
        }
        // The real run publishes `backfill_floor` from the checkpointed WAL right
        // before entering `GcTableRows`. Rule 3 needs every reader mark to have
        // reached the stamp, and `gc_floor_reader_mark` is clamped by this floor,
        // so a fixture that jumps straight into the state must publish it too.
        *mvstore.backfill_floor.write() = WalPos::from_pair(checkpoint.pager.wal_pos());
        checkpoint.state = CheckpointState::GcTableRows {
            next_index: 0,
            lwm: u64::MAX,
        };

        let first = checkpoint.gc_checkpointed_table_versions();
        assert!(
            first.is_some_and(|io| io.is_explicit_yield()),
            "GCing more than COLLECT_PREEMPTION_THRESHOLD rows must preempt with an explicit yield"
        );

        while checkpoint.gc_checkpointed_table_versions().is_some() {}
        // Write-set GC clears version chains but leaves empty SkipMap slots;
        // Truncate Finalize `_and_slots` unlinks them.
        let slots = mvstore
            .rows
            .iter()
            .filter(|entry| entry.key().table_id == table_id)
            .count();
        let versions: usize = mvstore
            .rows
            .iter()
            .filter(|entry| entry.key().table_id == table_id)
            .map(|entry| entry.value().read().len())
            .sum();
        assert_eq!(versions, 0, "write-set GC must reclaim version chains");
        assert_eq!(
            slots, row_count,
            "empty slots remain until Finalize `_and_slots`"
        );
        mvstore.drop_unused_row_versions_and_slots();
        let remaining = mvstore
            .rows
            .iter()
            .filter(|entry| entry.key().table_id == table_id)
            .count();
        assert_eq!(remaining, 0);
    }

    #[test]
    fn gc_checkpointed_index_versions_preempts_on_large_scan() {
        let db = MvccTestDbNoConn::new();
        let conn = db.connect();
        let mvstore = db.get_mvcc_store();
        let pager = conn.pager.load().clone();
        let mut checkpoint = CheckpointStateMachine::new(
            pager,
            mvstore.clone(),
            conn.clone(),
            true,
            conn.get_sync_mode(),
            crate::MAIN_DB_ID,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );
        checkpoint.lock_states.blocking_checkpoint_lock_held = true;
        checkpoint.durable_txid_max_new = 5;

        let index_id = MVTableId::from(-7);
        let row_count = COLLECT_PREEMPTION_THRESHOLD + 10;
        for i in 0..row_count as i64 {
            let (key, version) = index_row_version(index_id, "k", i, 1, Some(5), None, false);
            mvstore
                .insert_index_version(index_id, key, version.clone())
                .unwrap();
            checkpoint.index_write_set.push((index_id, version, false));
            checkpoint.written_index_slots.insert(i as usize);
        }
        // See the table variant: publish the floor the real `GcIndexRows` entry
        // would have published, or Rule 3 keeps every stamped current.
        *mvstore.backfill_floor.write() = WalPos::from_pair(checkpoint.pager.wal_pos());
        checkpoint.state = CheckpointState::GcIndexRows {
            next_index: 0,
            lwm: u64::MAX,
        };

        let first = checkpoint.gc_checkpointed_index_versions();
        assert!(
            first.is_some_and(|io| io.is_explicit_yield()),
            "GCing more than COLLECT_PREEMPTION_THRESHOLD index rows must preempt with an explicit yield"
        );

        while checkpoint.gc_checkpointed_index_versions().is_some() {}
        // Write-set GC clears version chains but leaves empty SkipMap slots;
        // Truncate Finalize `_and_slots` unlinks them.
        let inner = mvstore
            .index_rows
            .get(&index_id)
            .expect("index map must exist");
        let slots = inner.value().len();
        let versions: usize = inner
            .value()
            .iter()
            .map(|entry| entry.value().read().len())
            .sum();
        assert_eq!(
            versions, 0,
            "write-set GC must reclaim index version chains"
        );
        assert_eq!(
            slots, row_count,
            "empty index slots remain until Finalize `_and_slots`"
        );
        mvstore.drop_unused_row_versions_and_slots();
        let remaining = mvstore
            .index_rows
            .get(&index_id)
            .map_or(0, |entry| entry.value().len());
        assert_eq!(remaining, 0);
    }
}
