#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::io::FileSyncType;
use crate::sync::Mutex;
use crate::sync::OnceLock;
use crate::types::IOResultOr;
use crate::{turso_assert, turso_assert_greater_than, turso_debug_assert};
use branches::mark_unlikely;
use rustc_hash::{FxHashMap, FxHashSet};
use std::array;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use strum::EnumString;
use tracing::{instrument, Level};

use crate::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use crate::sync::RwLock;
use bitflags::bitflags;
use std::fmt::{Debug, Formatter};
use std::{fmt, sync::Arc};

use super::buffer_pool::BufferPool;
use super::pager::{PageRef, Pager};
use super::sqlite3_ondisk::{
    self, checksum_wal, DatabaseHeader, WalHeader, WAL_MAGIC_BE, WAL_MAGIC_LE,
};
use crate::fast_lock::SpinLock;
use crate::io::clock::MonotonicInstant;
use crate::io::CompletionGroup;
use crate::io::{File, IO};
use crate::storage::database::DatabaseStorage;
use crate::storage::page_transform::{
    page_codec_completion_error, PageCodecContext, PageLocation, PageTransform,
};
#[cfg(host_shared_wal)]
use crate::storage::shared_wal_coordination::SharedWalCoordinationOpenMode;
#[cfg(host_shared_wal)]
use crate::storage::shared_wal_coordination::{
    MappedSharedWalCoordination, SharedOwnerRecord, SharedReaderSlot, SharedWalCoordinationHeader,
};
use crate::storage::sqlite3_ondisk::{
    begin_read_wal_frame, begin_read_wal_frame_raw, finish_read_page, prepare_wal_frame_header,
    recompute_wal_frame_checksum, write_pages_vectored, PageSize, WAL_FRAME_HEADER_SIZE,
    WAL_HEADER_SIZE,
};
use crate::types::{IOCompletions, IOResult};
use crate::util::IOExt as _;
use crate::{
    bail_corrupt_error, io_yield_one, Buffer, Completion, CompletionError, IOContext, LimboError,
    Result, SyncMode,
};

/// this contains the frame to rollback to and its associated checksum.
#[derive(Debug, Clone)]
pub struct RollbackTo {
    pub frame: u64,
    pub checksum: (u32, u32),
    /// WAL checkpoint sequence (generation) the position was captured in;
    /// asserted against the current generation on rollback.
    pub checkpoint_seq: u32,
}

#[derive(Debug, Clone, Default)]
pub struct CheckpointResult {
    /// max frame in the WAL after checkpoint
    /// note, that as we TRUNCATE wal outside of the main checkpoint routine - this field will be set to non-zero number even for TRUNCATE mode
    pub wal_max_frame: u64,
    /// total amount of frames backfilled to the DB file after checkpoint
    pub wal_total_backfilled: u64,
    /// amount of new frames backfilled to the DB file during this checkpoint procedure
    pub wal_checkpoint_backfilled: u64,
    /// In the case of everything backfilled, we need to hold the locks until the db
    /// file is truncated.
    maybe_guard: Option<CheckpointLocks>,
    pub db_truncate_sent: bool,
    pub db_sync_sent: bool,
    /// Whether WAL truncation I/O has been submitted (for TRUNCATE checkpoint mode)
    pub wal_truncate_sent: bool,
    /// Whether WAL sync I/O has been submitted after truncation
    pub wal_sync_sent: bool,
}

impl Drop for CheckpointResult {
    fn drop(&mut self) {
        let _ = self.maybe_guard.take();
    }
}

impl CheckpointResult {
    pub fn new(
        wal_max_frame: u64,
        wal_total_backfilled: u64,
        wal_checkpoint_backfilled: u64,
    ) -> Self {
        Self {
            wal_max_frame,
            wal_total_backfilled,
            wal_checkpoint_backfilled,
            maybe_guard: None,
            db_sync_sent: false,
            db_truncate_sent: false,
            wal_truncate_sent: false,
            wal_sync_sent: false,
        }
    }

    pub const fn everything_backfilled(&self) -> bool {
        self.wal_max_frame == self.wal_total_backfilled
    }
    pub fn should_truncate(&self) -> bool {
        // TRUNCATE should also clear any stale WAL bytes when the log was restarted
        // (wal_max_frame=0) but the file still contains old frames.
        self.everything_backfilled()
    }
    pub fn release_guard(&mut self) {
        let _ = self.maybe_guard.take();
    }
}

#[cfg(host_shared_wal)]
pub(crate) fn coordination_path_for_wal_path(wal_path: &str) -> String {
    if let Some(db_path) = wal_path.strip_suffix("-wal") {
        format!("{db_path}-tshm")
    } else {
        format!("{wal_path}-tshm")
    }
}

bitflags! {
    /// Automatic WAL maintenance actions a caller permits the engine to take
    /// during routine operations (begin write tx, commit, shutdown).
    ///
    /// Callers that manage WAL state out-of-band — e.g. the sync engine,
    /// which keeps its own watermarks across the WAL header — pass an
    /// explicit subset so unrelated bookkeeping remains untouched. The
    /// previous single `wal_auto_checkpoint_disabled` boolean conflated both
    /// auto-checkpoint and WAL header restart; spelling them out separately
    /// avoids breaking sync-engine assumptions whenever one of the two is
    /// disabled.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct WalAutoActions: u8 {
        /// Run an auto-checkpoint after commit when `should_checkpoint()`
        /// is true, and the truncate-checkpoint on connection shutdown.
        const Checkpoint = 0b01;
        /// Restart the WAL header in `try_restart_log_before_write` when
        /// every frame has been backfilled, before starting a write tx.
        const Restart    = 0b10;
    }
}

impl WalAutoActions {
    /// Default policy for ordinary connections: every auto action allowed.
    pub const fn all_enabled() -> Self {
        Self::from_bits_truncate(Self::Checkpoint.bits() | Self::Restart.bits())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, EnumString)]
#[strum(ascii_case_insensitive)]
pub enum CheckpointMode {
    /// Checkpoint as many frames as possible without waiting for any database readers or writers to finish, then sync the database file if all frames in the log were checkpointed.
    /// Passive never blocks readers or writers, only ensures (like all modes do) that there are no other checkpointers.
    ///
    /// Optional upper_bound_inclusive parameter can be set in order to checkpoint frames with number no larger than the parameter
    Passive { upper_bound_inclusive: Option<u64> },
    /// This mode blocks until there is no database writer and all readers are reading from the most recent database snapshot. It then checkpoints all frames in the log file and syncs the database file. This mode blocks new database writers while it is pending, but new database readers are allowed to continue unimpeded.
    Full,
    /// This mode works the same way as `Full` with the addition that after checkpointing the log file it blocks (calls the busy-handler callback) until all readers are reading from the database file only. This ensures that the next writer will restart the log file from the beginning. Like `Full`, this mode blocks new database writer attempts while it is pending, but does not impede readers.
    Restart,
    /// This mode works the same way as `Restart` with the addition that it also truncates the log file to zero bytes just prior to a successful return.
    ///
    /// Extra parameter can be set in order to perform conditional TRUNCATE: database will be checkpointed and truncated only if max_frames equals to the parameter value
    /// this behaviour used by sync-engine which consolidate WAL before checkpoint and needs to be sure that no frames will be missed
    Truncate { upper_bound_inclusive: Option<u64> },
}

impl CheckpointMode {
    pub(crate) fn should_restart_log(&self) -> bool {
        matches!(
            self,
            CheckpointMode::Truncate { .. } | CheckpointMode::Restart
        )
    }
    /// All modes other than Passive require a complete backfilling of all available frames
    /// from `shared.metadata.nbackfills + 1 -> shared.metadata.max_frame`
    fn require_all_backfilled(&self) -> bool {
        !matches!(self, CheckpointMode::Passive { .. })
    }
}

/// Immutable view of the WAL metadata a connection snapshots from shared state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalSnapshot {
    max_frame: u64,
    nbackfills: u64,
    last_checksum: (u32, u32),
    checkpoint_seq: u32,
    transaction_count: u64,
}

impl WalSnapshot {
    /// First frame that is still visible in the WAL after checkpoint backfill.
    const fn min_frame(self) -> u64 {
        self.nbackfills + 1
    }
}

/// Which read-mark, if any, currently protects this connection's snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadGuardKind {
    None,
    DbFile,
    ReadMark(NonZeroUsize),
}

impl ReadGuardKind {
    /// Convert the lock index stored on `WalFile` into a semantic guard kind.
    const fn from_lock_index(lock_index: usize) -> Self {
        match lock_index {
            NO_LOCK_HELD => Self::None,
            0 => Self::DbFile,
            idx => Self::ReadMark(NonZeroUsize::new(idx).expect("idx checked to be non-zero")),
        }
    }

    /// Convert the semantic guard kind back into the legacy lock index representation.
    fn lock_index(self) -> usize {
        match self {
            Self::None => NO_LOCK_HELD,
            Self::DbFile => 0,
            Self::ReadMark(idx) => idx.into(),
        }
    }
}

/// Connection-local WAL state derived from a shared snapshot plus a held read guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalConnectionState {
    snapshot: WalSnapshot,
    read_guard: ReadGuardKind,
}

impl WalConnectionState {
    /// Build a new connection-local WAL state bundle.
    const fn new(snapshot: WalSnapshot, read_guard: ReadGuardKind) -> Self {
        Self {
            snapshot,
            read_guard,
        }
    }

    /// Replace just the shared snapshot while preserving the current read guard.
    const fn with_snapshot(self, snapshot: WalSnapshot) -> Self {
        Self {
            snapshot,
            read_guard: self.read_guard,
        }
    }
}

#[repr(transparent)]
#[derive(Debug, Default)]
/// A 64-bit read-write lock with embedded 32-bit value storage.
/// Using a single Atomic allows the reader count and lock state are updated
/// atomically together while sitting in a single cpu cache line.
///
/// # Memory Layout:
/// ```ignore
/// [63:32] Value bits    - 32 bits for stored value
/// [31:1]  Reader count  - 31 bits for reader count
/// [0]     Writer bit    - 1 bit indicating exclusive write lock
/// ```
///
/// # Synchronization Guarantees:
/// - Acquire semantics on lock acquisition ensure visibility of all writes
///   made by the previous lock holder
/// - Release semantics on unlock ensure all writes made while holding the
///   lock are visible to the next acquirer
/// - The embedded value can be atomically read without holding any lock
pub struct TursoRwLock(AtomicU64);

pub const READMARK_NOT_USED: u32 = 0xffffffff;
const NO_LOCK_HELD: usize = usize::MAX;

impl TursoRwLock {
    /// Bit 0: Writer flag
    const WRITER: u64 = 0b1;

    /// Reader increment value (bit 1)
    const READER_INC: u64 = 0b10;

    /// Reader count starts at bit 1
    const READER_SHIFT: u32 = 1;

    /// Mask for 31 reader bits [31:1]
    const READER_COUNT_MASK: u64 = 0x7fff_ffffu64 << Self::READER_SHIFT;

    /// Value starts at bit 32
    const VALUE_SHIFT: u32 = 32;

    /// Mask for 32 value bits [63:32]
    const VALUE_MASK: u64 = 0xffff_ffffu64 << Self::VALUE_SHIFT;

    #[inline]
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    const fn has_writer(val: u64) -> bool {
        val & Self::WRITER != 0
    }

    const fn has_readers(val: u64) -> bool {
        val & Self::READER_COUNT_MASK != 0
    }

    #[inline]
    /// Try to acquire a shared read lock.
    pub fn read(&self) -> bool {
        let mut count = 0;
        // Bounded loop to avoid infinite loops
        // Retry on Reader contention (should hopefully be spurious)
        while count < 1_000_000 {
            let cur = self.0.load(Ordering::Acquire);
            // If a writer is present we cannot proceed.
            if Self::has_writer(cur) {
                return false;
            }
            // 2 billion readers is a high enough number where we will skip the branch
            // and assume that we are not overflowing :)
            let desired = cur.wrapping_add(Self::READER_INC);
            // for success, Acquire establishes happens-before relationship with the previous Release from unlock
            // for failure we only care about reading it for the next iteration so we can use Relaxed.
            let res = self
                .0
                .compare_exchange(cur, desired, Ordering::Acquire, Ordering::Relaxed);
            if res.is_err() {
                count += 1;
                crate::thread::spin_loop();
                continue;
            }
            return true;
        }
        // Too much reader contention return Busy
        false
    }

    /// Try to take an exclusive lock. Succeeds if no readers and no writer.
    #[inline]
    pub fn write(&self) -> bool {
        let cur = self.0.load(Ordering::Acquire);
        // exclusive lock, so require no readers and no writer
        if Self::has_writer(cur) || Self::has_readers(cur) {
            return false;
        }
        let desired = cur | Self::WRITER;
        self.0 // Safety: Failure here can be Relaxed as we will read again on next iteration.
            .compare_exchange(cur, desired, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    /// upgrade read lock to the write lock
    /// only possible if there is exactly single reader at the moment
    /// return true if lock was upgraded succesfully - and false otherwise
    #[inline]
    pub fn upgrade(&self) -> bool {
        let cur = self.0.load(Ordering::Acquire);
        // Check for single reader: exactly one reader, any value
        if (cur & !Self::VALUE_MASK) != Self::READER_INC {
            return false;
        }
        // Preserve value bits, replace reader with writer
        let desired = (cur & Self::VALUE_MASK) | Self::WRITER;
        self.0
            .compare_exchange(cur, desired, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    /// downgrade write lock to the read lock
    /// MUST be called for a lock acquired by the writer
    #[inline]
    pub fn downgrade(&self) {
        let cur = self.0.load(Ordering::Acquire);
        turso_debug_assert!(Self::has_writer(cur));
        // Preserve value bits, replace writer with one reader
        let desired = (cur & Self::VALUE_MASK) | Self::READER_INC;
        #[cfg(debug_assertions)]
        {
            let prev = self
                .0
                .compare_exchange(cur, desired, Ordering::AcqRel, Ordering::Relaxed);
            turso_debug_assert!(
                prev.is_ok(),
                "downgrade CAS failed — lock was mutated concurrently"
            );
        }
        #[cfg(not(debug_assertions))]
        {
            self.0.store(desired, Ordering::Release);
        }
    }

    #[inline]
    /// Unlock whatever lock is currently held.
    /// For write lock: clear writer bit
    /// For read lock: decrement reader count
    pub fn unlock(&self) {
        let cur = self.0.load(Ordering::Acquire);
        if (cur & Self::WRITER) != 0 {
            // Clear writer bit, preserve everything else (including value)
            // Release ordering ensures all our writes are visible to next acquirer
            let cur = self.0.fetch_and(!Self::WRITER, Ordering::Release);
            turso_assert!(!Self::has_readers(cur), "write lock was held with readers");
        } else {
            turso_assert!(
                Self::has_readers(cur),
                "unlock called with no readers or writers"
            );
            self.0.fetch_sub(Self::READER_INC, Ordering::Release);
        }
    }

    #[inline]
    /// Read the embedded 32-bit value atomically regardless of slot occupancy.
    pub fn get_value(&self) -> u32 {
        (self.0.load(Ordering::Acquire) >> Self::VALUE_SHIFT) as u32
    }

    #[inline]
    /// The embedded read-mark value, but only if a reader currently holds this slot
    /// (otherwise the value is stale from a past holder). Lock-free single-load; used to
    /// find the minimum frame any active reader is pinned at without mutating the slot.
    pub fn held_value(&self) -> Option<u32> {
        let cur = self.0.load(Ordering::Acquire);
        if Self::has_readers(cur) {
            Some((cur >> Self::VALUE_SHIFT) as u32)
        } else {
            None
        }
    }

    #[inline]
    /// Set the embedded value while holding the write lock.
    pub fn set_value_exclusive(&self, v: u32) {
        // Must be called only while WRITER bit is set
        let cur = self.0.load(Ordering::Acquire);
        turso_assert!(Self::has_writer(cur), "must hold exclusive lock");
        let desired = (cur & !Self::VALUE_MASK) | ((v as u64) << Self::VALUE_SHIFT);
        self.0.store(desired, Ordering::Release);
    }
}

/// Represents a batch of WAL frames which will be appended to the log
/// with a `pwritev` call and then sync'd to disk.
pub struct PreparedFrames {
    /// File offset for the first frame
    pub offset: u64,
    /// Serialized frame buffers
    pub bufs: Vec<Arc<Buffer>>,
    /// Per-frame metadata: (page_ref, frame_id, cumulative_checksum)
    pub metadata: Vec<(PageRef, u64, (u32, u32))>,
    /// Checksum after all frames in this batch
    pub final_checksum: (u32, u32),
    /// Max frame ID after this batch
    pub final_max_frame: u64,
    /// Epoch at preparation time
    pub epoch: u32,
}

/// Metadata published by the coordination backend once a WAL commit becomes visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalCommitState {
    max_frame: u64,
    last_checksum: (u32, u32),
    transaction_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CoordinationCheckpointGuardKind {
    Read0,
    Writer,
}

/// Coordination operations that back the WAL's authoritative state.
trait WalCoordination: Debug + Send + Sync {
    /// Load the current authoritative WAL snapshot.
    fn load_snapshot(&self) -> WalSnapshot;

    /// Ensure any process-local fallback cache is complete for `snapshot`.
    fn ensure_local_frame_cache_covers(
        &self,
        _io: &Arc<dyn IO>,
        _snapshot: WalSnapshot,
    ) -> Result<()> {
        Ok(())
    }

    /// Publish a newly committed WAL state snapshot.
    fn publish_commit(&self, commit: WalCommitState);

    /// Publish the highest frame durably backfilled during checkpoint.
    fn publish_backfill(&self, max_frame: u64);

    /// Install any backend-specific durable proof before publishing backfill.
    /// Returns an optional completion that must finish before `publish_backfill`.
    fn install_durable_backfill_proof(
        &self,
        max_frame: u64,
        db_size_pages: u32,
        db_header_crc32c: u32,
        sync_type: FileSyncType,
    ) -> Result<Option<Completion>>;

    /// Find the newest frame for `page_id` within the caller's visible range.
    fn find_frame(
        &self,
        page_id: u64,
        min_frame: u64,
        max_frame: u64,
        frame_watermark: Option<u64>,
    ) -> Option<u64>;

    /// Enumerate the latest visible frame per page in the requested frame range.
    fn iter_latest_frames(&self, min_frame: u64, max_frame: u64) -> Vec<(u64, u64)>;

    /// Read the current checkpoint epoch used to tag cached WAL pages.
    fn checkpoint_epoch(&self) -> u32;

    /// Advance the checkpoint epoch after checkpoint or restart invalidates cached pages.
    fn bump_checkpoint_epoch(&self) -> u32;

    /// Try to acquire the reader protection needed for `snapshot`.
    fn try_begin_read_tx(&self, snapshot: WalSnapshot) -> Option<ReadGuardKind>;

    /// Release a read guard previously returned by `try_begin_read_tx`.
    fn end_read_tx(&self, guard: ReadGuardKind);

    /// Try to acquire the WAL writer guard.
    fn try_begin_write_tx(&self) -> bool;

    /// Release a previously acquired WAL writer guard.
    fn end_write_tx(&self);

    /// Acquire the checkpoint-related locks needed for `mode`.
    fn acquire_checkpoint_guard(
        &self,
        mode: CheckpointMode,
    ) -> Result<CoordinationCheckpointGuardKind>;

    /// Acquire the remaining checkpoint-related locks for VACUUM when the
    /// caller already owns the raw process-local checkpoint lock.
    ///
    /// On error, implementations must release that held checkpoint lock before
    /// returning.
    ///
    fn acquire_vacuum_checkpoint_guard_from_held_lock(
        &self,
    ) -> Result<CoordinationCheckpointGuardKind>;

    /// Release the checkpoint-related locks previously acquired for `guard`.
    fn release_checkpoint_guard(&self, guard: CoordinationCheckpointGuardKind);

    /// Compute the highest frame a checkpoint may safely backfill and refresh read marks.
    fn determine_max_safe_checkpoint_frame(&self, max_frame: u64) -> u64;

    /// Lowest read-mark frame any reader is currently pinned at, or `None` if none. Read-only.
    fn min_pinned_read_frame(&self) -> Option<u64>;

    /// Begin a restart while the caller holds the required external checkpoint/write guards.
    fn begin_restart(&self, io: &dyn IO) -> Result<WalSnapshot>;

    /// Release any restart-only coordination state held by `begin_restart`.
    fn end_restart(&self);

    /// Attempt the restart path used by a writer holding read-mark 0.
    fn try_restart_log_for_write(&self, io: &dyn IO) -> Result<Option<WalSnapshot>>;

    /// Mark the WAL uninitialized before truncation and return the WAL file handle.
    fn prepare_truncate(&self) -> Result<Arc<dyn File>>;

    /// Return the current WAL header snapshot.
    fn wal_header(&self) -> WalHeader;

    /// Return the WAL file used for durable reads and writes.
    fn wal_file(&self) -> Result<Arc<dyn File>>;

    /// Clone the shared WAL state backing this coordination backend.
    fn shared_wal_state(&self) -> Arc<RwLock<WalFileShared>>;

    /// Report whether the WAL header has already been written and synced.
    fn wal_is_initialized(&self) -> bool;

    /// Initialize or refresh the WAL header before the first append after restart/truncate.
    fn prepare_wal_header(&self, io: &dyn IO, page_size: PageSize) -> Option<WalHeader>;

    /// Mark the WAL header durable after the header sync completes.
    fn mark_initialized(&self);

    /// Record a newly appended frame in the backend's page-to-frame lookup state.
    fn cache_frame(&self, page_id: u64, frame_id: u64);

    /// Drop any cached frame mappings newer than `max_frame`.
    fn rollback_cache(&self, max_frame: u64);

    /// Whether a process-local "last connection" close may run shutdown checkpointing.
    fn should_checkpoint_on_close(&self) -> bool;

    #[cfg(test)]
    fn backend_name(&self) -> &'static str;

    #[cfg(test)]
    fn shared_ptr(&self) -> usize;

    #[cfg(test)]
    fn open_mode_name(&self) -> Option<&'static str> {
        None
    }
}

/// Write-ahead log (WAL).
#[aristo::intent("The WAL subsystem maintains LSN monotonicity, frame commitment ordering, recovery idempotency, checkpoint safety, and group commit atomicity.", id = "wal_protocol_correctness", verify = "neural")]
pub trait Wal: Debug + Send + Sync {
    /// Begin a read transaction.
    /// Returns whether the database state has changed since the last read transaction.
    fn begin_read_tx(&self) -> Result<bool>;
    /// MVCC helper: check if WAL state changed without starting a read tx.
    fn mvcc_refresh_if_db_changed(&self) -> bool;

    /// Begin a write transaction.
    ///
    /// `allowed_auto_actions` controls which automatic WAL maintenance
    /// actions are permitted within this call — currently only
    /// `WalAutoActions::Restart` is consulted (it gates
    /// `try_restart_log_before_write`). Callers that own WAL state
    /// externally (e.g. the sync engine) pass an empty set to opt out.
    fn begin_write_tx(&self, allowed_auto_actions: WalAutoActions) -> Result<()>;

    /// End a read transaction.
    fn end_read_tx(&self);

    /// End a write transaction.
    fn end_write_tx(&self);

    /// Returns true if this WAL instance currently holds a read lock.
    fn holds_read_lock(&self) -> bool;

    /// Returns true if this WAL instance currently holds the write lock.
    fn holds_write_lock(&self) -> bool;

    /// Whether shutdown checkpointing is valid when this process closes its last connection.
    fn should_checkpoint_on_close(&self) -> bool;

    /// Find the latest frame containing a page.
    ///
    /// optional frame_watermark parameter can be passed to force WAL to find frame not larger than watermark value
    /// caller must guarantee, that frame_watermark must be greater than last checkpointed frame, otherwise method will panic
    fn find_frame(&self, page_id: u64, frame_watermark: Option<u64>) -> Result<Option<u64>>;

    /// Read a frame from the WAL. The read is added to `group`, when
    /// given, before it is submitted.
    fn read_frame(
        &self,
        frame_id: u64,
        page: PageRef,
        buffer_pool: Arc<BufferPool>,
        group: Option<&mut CompletionGroup>,
    ) -> Result<Completion>;

    /// Read a contiguous run of WAL frames with a single `pread`.
    /// For each `i`, `pages[i]` receives the decoded page body of frame
    /// `start_frame + i`. This method is a batched version of `read_frame`.
    ///
    /// If `scratch_buf` is `Some`, it is used as the pread destination (must
    /// have length exactly `(page_size + WAL_FRAME_HEADER_SIZE) * pages.len()`).
    /// Otherwise a fresh temporary buffer is allocated. VACUUM passes a
    /// pre-allocated buffer to amortize the ~batch-size allocation across
    /// batches.
    ///
    /// The read is added to `group`, when given, before it is submitted.
    fn read_frames_batch(
        &self,
        start_frame: u64,
        pages: &[PageRef],
        buffer_pool: Arc<BufferPool>,
        scratch_buf: Option<Arc<Buffer>>,
        group: Option<&mut CompletionGroup>,
    ) -> Result<Completion>;

    /// Read a raw WAL frame with its on-disk header and page body decoded by
    /// the configured encryption context or external page codec.
    fn read_frame_raw(&self, frame_id: u64, frame: &mut [u8]) -> Result<Completion>;

    /// Write an in-memory page as a WAL frame.
    ///
    /// TursoDB uses `page_no` and `size_after` from the supplied header, applies
    /// any configured page transform to the body, and overwrites the checksum.
    fn write_frame_raw(
        &self,
        buffer_pool: Arc<BufferPool>,
        frame_id: u64,
        page_id: u64,
        db_size: u64,
        page: &[u8],
        sync_type: FileSyncType,
    ) -> Result<()>;

    /// Prepare WAL header for the future append
    /// Most of the time this method will return Ok(None)
    fn prepare_wal_start(&self, page_sz: PageSize) -> Result<Option<Completion>>;

    fn prepare_wal_finish(&self, sync_type: FileSyncType) -> Result<Completion>;

    /// Prepare a batch of WAL frames for durable commit/append to the log.
    fn prepare_frames(
        &self,
        pages: &[PageRef],
        page_sz: PageSize,
        db_size_on_commit: Option<u32>,
        prev: Option<&PreparedFrames>,
    ) -> Result<PreparedFrames>;

    /// For each prepared frame, update in-memory WAL index and rolling checksum
    /// and advance max_frame to make committed frames visible to readers.
    fn commit_prepared_frames(&self, prepared: &[PreparedFrames]);

    /// Mark in-memory pages clean and set WAL tags after durable commit.
    fn finalize_committed_pages(&self, prepared: &[PreparedFrames]);

    /// Return a handle to the underlying File.
    fn wal_file(&self) -> Result<Arc<dyn File>>;

    /// Write a bunch of frames to the WAL.
    /// db_size is the database size in pages after the transaction finishes.
    /// db_size is set  -> last frame written in transaction
    /// db_size is none -> non-last frame written in transaction
    fn append_frames_vectored(&self, pages: Vec<PageRef>, page_sz: PageSize) -> Result<Completion>;

    /// Complete append of frames by updating shared wal state. Before this
    /// all changes were stored locally.
    fn finish_append_frames_commit(&self) -> Result<()>;

    fn should_checkpoint(&self) -> bool;
    /// Checkpoint the WAL into the database file.
    /// `sync_mode` controls the WAL durability barrier: unless it is
    /// [SyncMode::Off], the WAL is fsynced before any frame is backfilled so
    /// that a crash mid-backfill can always be healed by WAL recovery.
    fn checkpoint(
        &self,
        pager: &Pager,
        mode: CheckpointMode,
        sync_mode: SyncMode,
    ) -> IOResultOr<CheckpointResult>;
    fn install_durable_backfill_proof(
        &self,
        max_frame: u64,
        db_size_pages: u32,
        db_header_crc32c: u32,
        sync_type: FileSyncType,
    ) -> Result<Option<Completion>>;
    fn publish_backfill(&self, max_frame: u64);
    fn sync(&self, sync_type: FileSyncType) -> Result<Completion>;
    fn is_syncing(&self) -> bool;
    /// Whether the WAL file is dirty: frames were appended that no successful
    /// WAL fsync has covered yet. A dirty WAL owes an fsync before a commit
    /// may be reported durable, even when the committer itself has no dirty
    /// pages to write (e.g. frames inserted through [Wal::write_frame_raw]).
    fn is_dirty(&self) -> bool;
    fn get_max_frame_in_wal(&self) -> u64;
    fn get_checkpoint_seq(&self) -> u32;
    fn get_max_frame(&self) -> u64;
    /// This connection's frozen `(checkpoint_seq, max_frame)`: for a reader it is the WAL read
    /// mark installed at `begin_read_tx`; for a writer it is the position after its last commit.
    /// Used by MVCC to gate btree reads on physical reachability (a materialization at WAL
    /// position `P` is reachable iff `P <= this`, lexicographically). See `RootEntry`.
    fn connection_wal_pos(&self) -> (u32, u64);
    /// The lowest WAL frame any active reader is currently pinned at (across the read-mark
    /// slots), or `None` if no reader holds a slot. This is the authoritative set of pinned
    /// readers — it includes a reader that has called `begin_read_tx` but not yet published an
    /// MVCC transaction — so the MVCC checkpoint uses it as the version-store GC floor (a row
    /// whose btree page was materialized past a pinned reader's frame is invisible in that
    /// reader's snapshot, so its version-store copy must be retained).
    fn min_pinned_read_frame(&self) -> Option<u64>;
    fn get_min_frame(&self) -> u64;
    /// The shared backfill boundary: WAL frames at or below this are durably copied into the DB
    /// file, so a version materialized there is reachable by EVERY snapshot (including a db-file
    /// reader pinned at the boundary). Used as the passive-checkpoint version-store GC floor.
    fn backfill_frame(&self) -> u64;
    fn rollback(&self, rollback_to: Option<RollbackTo>);
    fn abort_checkpoint(&self);
    fn get_last_checksum(&self) -> (u32, u32);

    /// Return unique set of pages changed **after** frame_watermark position and until current WAL session max_frame_no
    fn changed_pages_after(&self, frame_watermark: u64) -> Result<Vec<u32>>;

    fn set_io_context(&self, ctx: IOContext);

    /// Update the max frame to the current shared max frame.
    /// Currently this is only used for MVCC as it takes care of write conflicts on its own.
    /// This should't be used with regular WAL mode.
    fn update_max_frame(&self);

    /// Truncate WAL file to zero and sync it. This is called AFTER the DB file has been
    /// synced during TRUNCATE checkpoint mode, ensuring data durability.
    /// The result parameter is used to track I/O progress (wal_truncate_sent, wal_sync_sent).
    fn truncate_wal(
        &self,
        result: &mut CheckpointResult,
        sync_type: FileSyncType,
    ) -> IOResultOr<()>;

    /// Try to acquire the checkpoint serialization lock. Returns `Busy` if
    /// another checkpointer or VACUUM already holds it. Used by plain VACUUM
    /// to fail fast if a concurrent checkpoint would block later.
    fn try_begin_vacuum_checkpoint_lock(&self) -> Result<()>;

    /// Release the checkpoint serialization lock acquired by
    /// `try_begin_vacuum_checkpoint_lock`.
    fn release_vacuum_checkpoint_lock(&self);

    /// Acquire exclusive WAL access. This will block all new readers and writers. Also,
    /// this routine succeeds only if no other transactions are active. This is used by
    /// VACUUM routine.
    ///
    ///
    /// VACUUM: take `vacuum_lock` exclusively, take the WAL write lock, and install
    /// the source snapshot that VACUUM will copy from.
    ///
    /// This does not acquire a physical read-mark lock. The exclusive snapshot
    /// is protected by `vacuum_lock`: normal readers hold that lock shared for
    /// their read transaction, so once the exclusive lock is acquired no new
    /// normal reader or writer can enter.
    fn begin_vacuum_blocking_tx(&self) -> Result<()>;

    /// Checkpoint using a checkpoint lock already held by the caller. The
    /// method consumes that raw checkpoint-lock ownership: on success the guard
    /// is held by the checkpoint state machine, and on early failure it is
    /// released before returning.
    fn vacuum_checkpoint_with_held_lock(
        &self,
        pager: &Pager,
        sync_mode: SyncMode,
    ) -> IOResultOr<CheckpointResult>;

    /// Release the exclusive VACUUM lock acquired by `begin_vacuum_blocking_tx`.
    /// VACUUM calls this once done, after which new
    /// readers and writers may proceed again.
    fn release_vacuum_lock(&self);

    #[cfg(any(test, debug_assertions))]
    fn as_any(&self) -> &dyn std::any::Any;
}

#[derive(Debug)]
struct InProcessWalCoordination {
    shared: Arc<RwLock<WalFileShared>>,
}

impl InProcessWalCoordination {
    /// Build the in-process coordination backend over the existing shared WAL state.
    fn new(shared: Arc<RwLock<WalFileShared>>) -> Self {
        Self { shared }
    }

    #[cfg(test)]
    fn try_read_mark_shared(&self, slot: usize) -> bool {
        self.shared.read().runtime.read_locks[slot].read()
    }

    fn try_read_mark_exclusive(&self, slot: usize) -> bool {
        self.shared.read().runtime.read_locks[slot].write()
    }

    fn unlock_read_mark(&self, slot: usize) {
        self.shared.read().runtime.read_locks[slot].unlock();
    }

    fn read_mark_value(&self, slot: usize) -> u32 {
        self.shared.read().runtime.read_locks[slot].get_value()
    }

    fn snapshot_of(shared: &WalFileShared) -> WalSnapshot {
        let checkpoint_seq = shared.metadata.wal_header.lock().checkpoint_seq;
        WalSnapshot {
            max_frame: shared.metadata.max_frame.load(Ordering::Acquire),
            nbackfills: shared.metadata.nbackfills.load(Ordering::Acquire),
            last_checksum: shared.metadata.last_checksum,
            checkpoint_seq,
            transaction_count: shared.metadata.transaction_count.load(Ordering::Acquire),
        }
    }

    /// Lowest read-mark frame across slots currently held by a reader (1..5; slot 0 is the
    /// db-file read mark), or `None` if no reader holds a slot. Read-only / lock-free.
    fn min_pinned_read_frame_inner(&self) -> Option<u64> {
        let shared = self.shared.read();
        let mut min: Option<u64> = None;
        for slot in 1..5 {
            if let Some(v) = shared.runtime.read_locks[slot].held_value() {
                if v != READMARK_NOT_USED {
                    let f = v as u64;
                    min = Some(min.map_or(f, |m: u64| m.min(f)));
                }
            }
        }
        min
    }

    fn set_read_mark_value_exclusive(&self, slot: usize, value: u32) {
        self.shared.read().runtime.read_locks[slot].set_value_exclusive(value);
    }

    fn try_upgrade_read_mark(&self, slot: usize) -> bool {
        self.shared.read().runtime.read_locks[slot].upgrade()
    }

    fn downgrade_read_mark(&self, slot: usize) {
        self.shared.read().runtime.read_locks[slot].downgrade();
    }

    fn try_write_lock(&self) -> bool {
        self.shared.read().runtime.write_lock.write()
    }

    fn unlock_write_lock(&self) {
        self.shared.read().runtime.write_lock.unlock();
    }

    fn try_checkpoint_lock(&self) -> bool {
        self.shared.read().runtime.checkpoint_lock.write()
    }

    fn unlock_checkpoint_lock(&self) {
        self.shared.read().runtime.checkpoint_lock.unlock();
    }
}

impl WalCoordination for InProcessWalCoordination {
    fn load_snapshot(&self) -> WalSnapshot {
        Self::snapshot_of(&self.shared.read())
    }

    fn publish_commit(&self, commit: WalCommitState) {
        let mut shared = self.shared.write();
        shared
            .metadata
            .max_frame
            .store(commit.max_frame, Ordering::Release);
        shared.metadata.last_checksum = commit.last_checksum;
        shared
            .metadata
            .transaction_count
            .store(commit.transaction_count, Ordering::Release);
    }

    fn publish_backfill(&self, max_frame: u64) {
        self.shared
            .write()
            .metadata
            .nbackfills
            .store(max_frame, Ordering::Release);
    }

    fn install_durable_backfill_proof(
        &self,
        _max_frame: u64,
        _db_size_pages: u32,
        _db_header_crc32c: u32,
        _sync_type: FileSyncType,
    ) -> Result<Option<Completion>> {
        Ok(None)
    }

    fn find_frame(
        &self,
        page_id: u64,
        min_frame: u64,
        max_frame: u64,
        frame_watermark: Option<u64>,
    ) -> Option<u64> {
        let shared = self.shared.read();
        let frame_cache = shared.runtime.frame_cache.lock();
        let range = frame_watermark
            .map(|x| 0..=x)
            .unwrap_or(min_frame..=max_frame);
        let result = frame_cache.get(&page_id).and_then(|frames| {
            frames
                .iter()
                .rfind(|&&frame| range.contains(&frame))
                .copied()
        });
        result
    }

    fn iter_latest_frames(&self, min_frame: u64, max_frame: u64) -> Vec<(u64, u64)> {
        let shared = self.shared.read();
        let frame_cache = shared.runtime.frame_cache.lock();
        let mut list = Vec::with_capacity(frame_cache.len());
        for (&page_id, frames) in frame_cache.iter() {
            if let Some(&frame_id) = frames
                .iter()
                .rfind(|&&frame| (min_frame..=max_frame).contains(&frame))
            {
                list.push((page_id, frame_id));
            }
        }
        list.sort_unstable_by_key(|&(page_id, _)| page_id);
        list
    }

    fn checkpoint_epoch(&self) -> u32 {
        self.shared.read().runtime.epoch.load(Ordering::Acquire)
    }

    fn bump_checkpoint_epoch(&self) -> u32 {
        self.shared
            .read()
            .runtime
            .epoch
            .fetch_add(1, Ordering::Release)
    }

    fn try_begin_read_tx(&self, snapshot: WalSnapshot) -> Option<ReadGuardKind> {
        turso_assert!(
            snapshot.max_frame <= u32::MAX as u64,
            "max_frame exceeds u32 read mark range"
        );
        // One read lock on the shared state for the whole slot selection.
        // The read marks are atomics inside it, so taking the lock once
        // instead of once per access changes nothing about their ordering,
        // and it saves about eight lock round trips per read transaction.
        let shared = self.shared.read();
        let read_locks = &shared.runtime.read_locks;
        if snapshot.max_frame == snapshot.nbackfills {
            if !read_locks[0].read() {
                return None;
            }
            if Self::snapshot_of(&shared) != snapshot {
                read_locks[0].unlock();
                return None;
            }
            return Some(ReadGuardKind::DbFile);
        }

        let mut best_idx: i64 = -1;
        let mut best_mark: u32 = 0;
        for (idx, read_lock) in read_locks.iter().enumerate().skip(1) {
            let mark = read_lock.get_value();
            if mark != READMARK_NOT_USED && mark <= snapshot.max_frame as u32 && mark > best_mark {
                best_mark = mark;
                best_idx = idx as i64;
            }
        }

        if best_idx == -1 || (best_mark as u64) < snapshot.max_frame {
            for (idx, read_lock) in read_locks.iter().enumerate().skip(1) {
                if !read_lock.write() {
                    continue;
                }
                read_lock.set_value_exclusive(snapshot.max_frame as u32);
                best_idx = idx as i64;
                best_mark = snapshot.max_frame as u32;
                read_lock.unlock();
                break;
            }
        }

        if best_idx == -1 || !read_locks[best_idx as usize].read() {
            return None;
        }

        let snapshot_after_lock = Self::snapshot_of(&shared);
        let current_slot_mark = read_locks[best_idx as usize].get_value();
        if current_slot_mark != best_mark || snapshot_after_lock != snapshot {
            read_locks[best_idx as usize].unlock();
            return None;
        }

        Some(ReadGuardKind::ReadMark(
            NonZeroUsize::new(best_idx as usize)
                .expect("best_idx checked to be non-negative and non-zero"),
        ))
    }

    fn end_read_tx(&self, guard: ReadGuardKind) {
        match guard {
            ReadGuardKind::None => {}
            ReadGuardKind::DbFile => self.unlock_read_mark(0),
            ReadGuardKind::ReadMark(slot) => self.unlock_read_mark(slot.into()),
        }
    }

    fn try_begin_write_tx(&self) -> bool {
        self.try_write_lock()
    }

    fn end_write_tx(&self) {
        self.unlock_write_lock();
    }

    fn acquire_checkpoint_guard(
        &self,
        mode: CheckpointMode,
    ) -> Result<CoordinationCheckpointGuardKind> {
        if !self.try_checkpoint_lock() {
            tracing::trace!("CheckpointGuard::new: checkpoint lock failed, returning Busy");
            return Err(LimboError::Busy);
        }
        match mode {
            CheckpointMode::Passive { .. } => {
                if !self.try_read_mark_exclusive(0) {
                    self.unlock_checkpoint_lock();
                    tracing::trace!("CheckpointGuard: read0 lock failed, returning Busy");
                    return Err(LimboError::Busy);
                }
                Ok(CoordinationCheckpointGuardKind::Read0)
            }
            CheckpointMode::Full => {
                if !self.try_read_mark_exclusive(0) {
                    self.unlock_checkpoint_lock();
                    tracing::trace!("CheckpointGuard: read0 lock failed (Full), Busy");
                    return Err(LimboError::Busy);
                }
                if !self.try_write_lock() {
                    self.unlock_read_mark(0);
                    self.unlock_checkpoint_lock();
                    tracing::trace!("CheckpointGuard: write lock failed (Full), Busy");
                    return Err(LimboError::Busy);
                }
                Ok(CoordinationCheckpointGuardKind::Writer)
            }
            CheckpointMode::Restart | CheckpointMode::Truncate { .. } => {
                if !self.try_read_mark_exclusive(0) {
                    self.unlock_checkpoint_lock();
                    tracing::trace!("CheckpointGuard: read0 lock failed, returning Busy");
                    return Err(LimboError::Busy);
                }
                if !self.try_write_lock() {
                    self.unlock_checkpoint_lock();
                    self.unlock_read_mark(0);
                    tracing::trace!("CheckpointGuard: write lock failed, returning Busy");
                    return Err(LimboError::Busy);
                }
                Ok(CoordinationCheckpointGuardKind::Writer)
            }
        }
    }

    fn acquire_vacuum_checkpoint_guard_from_held_lock(
        &self,
    ) -> Result<CoordinationCheckpointGuardKind> {
        if !self.try_read_mark_exclusive(0) {
            self.unlock_checkpoint_lock();
            tracing::trace!("CheckpointGuard: held VACUUM read0 lock failed, returning Busy");
            return Err(LimboError::Busy);
        }
        if !self.try_write_lock() {
            self.unlock_read_mark(0);
            self.unlock_checkpoint_lock();
            tracing::trace!("CheckpointGuard: held VACUUM write lock failed, returning Busy");
            return Err(LimboError::Busy);
        }
        Ok(CoordinationCheckpointGuardKind::Writer)
    }

    fn release_checkpoint_guard(&self, guard: CoordinationCheckpointGuardKind) {
        match guard {
            CoordinationCheckpointGuardKind::Writer => {
                self.unlock_write_lock();
                self.unlock_read_mark(0);
                self.unlock_checkpoint_lock();
            }
            CoordinationCheckpointGuardKind::Read0 => {
                self.unlock_read_mark(0);
                self.unlock_checkpoint_lock();
            }
        }
    }

    fn determine_max_safe_checkpoint_frame(&self, max_frame: u64) -> u64 {
        turso_assert!(
            max_frame <= u32::MAX as u64,
            "max_frame exceeds u32 read mark range"
        );
        let mut max_safe_frame = max_frame;
        for read_lock_idx in 1..5 {
            let this_mark = self.read_mark_value(read_lock_idx);
            if this_mark < max_safe_frame as u32 {
                let busy = !self.try_read_mark_exclusive(read_lock_idx);
                if !busy {
                    let val = if read_lock_idx == 1 {
                        max_safe_frame as u32
                    } else {
                        READMARK_NOT_USED
                    };
                    self.set_read_mark_value_exclusive(read_lock_idx, val);
                    self.unlock_read_mark(read_lock_idx);
                } else {
                    max_safe_frame = this_mark as u64;
                }
            }
        }
        max_safe_frame
    }

    fn min_pinned_read_frame(&self) -> Option<u64> {
        self.min_pinned_read_frame_inner()
    }

    fn begin_restart(&self, io: &dyn IO) -> Result<WalSnapshot> {
        for idx in 1..5 {
            if !self.try_read_mark_exclusive(idx) {
                for j in 1..idx {
                    self.unlock_read_mark(j);
                }
                return Err(LimboError::Busy);
            }
            self.set_read_mark_value_exclusive(idx, READMARK_NOT_USED);
        }
        let mut shared = self.shared.write();
        shared.restart_wal_header(io);
        let checkpoint_seq = shared.metadata.wal_header.lock().checkpoint_seq;
        Ok(WalSnapshot {
            max_frame: shared.metadata.max_frame.load(Ordering::Acquire),
            nbackfills: shared.metadata.nbackfills.load(Ordering::Acquire),
            last_checksum: shared.metadata.last_checksum,
            checkpoint_seq,
            transaction_count: shared.metadata.transaction_count.load(Ordering::Acquire),
        })
    }

    fn end_restart(&self) {
        for idx in 1..5 {
            self.unlock_read_mark(idx);
        }
    }

    fn try_restart_log_for_write(&self, io: &dyn IO) -> Result<Option<WalSnapshot>> {
        if !self.try_upgrade_read_mark(0) {
            return Ok(None);
        }
        let result = self.begin_restart(io);
        self.downgrade_read_mark(0);
        match result {
            Ok(snapshot) => {
                self.end_restart();
                Ok(Some(snapshot))
            }
            Err(err) => Err(err),
        }
    }

    fn prepare_truncate(&self) -> Result<Arc<dyn File>> {
        let shared = self.shared.read();
        turso_assert!(
            shared.metadata.enabled.load(Ordering::Relaxed),
            "WAL must be enabled"
        );
        shared.metadata.initialized.store(false, Ordering::Release);
        shared.runtime.file.as_ref().cloned().ok_or_else(|| {
            mark_unlikely();
            LimboError::InternalError("WAL file not open".into())
        })
    }

    fn wal_header(&self) -> WalHeader {
        *self.shared.read().metadata.wal_header.lock()
    }

    fn wal_file(&self) -> Result<Arc<dyn File>> {
        let shared = self.shared.read();
        turso_assert!(
            shared.metadata.enabled.load(Ordering::Relaxed),
            "WAL must be enabled"
        );
        shared.runtime.file.as_ref().cloned().ok_or_else(|| {
            mark_unlikely();
            LimboError::InternalError("WAL file not open".into())
        })
    }

    fn wal_is_initialized(&self) -> bool {
        self.shared
            .read()
            .metadata
            .initialized
            .load(Ordering::Acquire)
    }

    fn prepare_wal_header(&self, io: &dyn IO, page_size: PageSize) -> Option<WalHeader> {
        let mut shared: crate::sync::RwLockWriteGuard<'_, WalFileShared> = self.shared.write();
        if shared.metadata.initialized.load(Ordering::Acquire) {
            return None;
        }

        let (header, checksum) = {
            let mut hdr = shared.metadata.wal_header.lock();
            hdr.magic = if cfg!(target_endian = "big") {
                WAL_MAGIC_BE
            } else {
                WAL_MAGIC_LE
            };
            if hdr.page_size == 0 {
                hdr.page_size = page_size.get();
            }
            if hdr.salt_1 == 0 && hdr.salt_2 == 0 {
                hdr.salt_1 = io.generate_random_number() as u32;
                hdr.salt_2 = io.generate_random_number() as u32;
            }

            let prefix = &hdr.as_bytes()[..WAL_HEADER_SIZE - 8];
            let use_native = (hdr.magic & 1) != 0;
            let (c1, c2) = checksum_wal(prefix, &hdr, (0, 0), use_native);
            hdr.checksum_1 = c1;
            hdr.checksum_2 = c2;
            (*hdr, (c1, c2))
        };
        shared.metadata.last_checksum = checksum;
        Some(header)
    }

    fn mark_initialized(&self) {
        self.shared
            .read()
            .metadata
            .initialized
            .store(true, Ordering::Release);
    }

    fn cache_frame(&self, page_id: u64, frame_id: u64) {
        let shared = self.shared.read();
        let mut frame_cache = shared.runtime.frame_cache.lock();
        // Frame-slot reuse / append-position rewind guard. Within a WAL
        // generation frames are appended with strictly increasing numbers, so
        // a `frame_id` that does not exceed the current high-water means the
        // slots from `frame_id` upward are being overwritten: by frames from a
        // prior uncommitted/aborted append that was never rolled back out of
        // the cache, or by another connection reusing the slots after a
        // rewind. Drop every stale `page -> frame` mapping for those slots
        // before recording the new one, otherwise `find_frame` can return a
        // frame slot that now physically holds a different page (corruption).
        // (Per-page frame lists are kept ascending, so popping the tail
        // `>= frame_id` removes exactly the overwritten suffix.)
        let high_water = shared
            .runtime
            .frame_cache_high_water
            .load(Ordering::Acquire);
        if frame_id <= high_water {
            frame_cache.retain(|_page_id, frames| {
                while frames.last().is_some_and(|&frame| frame >= frame_id) {
                    frames.pop();
                }
                !frames.is_empty()
            });
        }
        match frame_cache.get_mut(&page_id) {
            Some(frames) => {
                frames.push(frame_id);
            }
            None => {
                frame_cache.insert(page_id, vec![frame_id]);
            }
        }
        shared
            .runtime
            .frame_cache_high_water
            .store(frame_id, Ordering::Release);
    }

    fn rollback_cache(&self, max_frame: u64) {
        let shared = self.shared.read();
        let mut frame_cache = shared.runtime.frame_cache.lock();
        frame_cache.retain(|_page_id, frames| {
            while frames.last().is_some_and(|&frame| frame > max_frame) {
                frames.pop();
            }
            !frames.is_empty()
        });
        // Keep the high-water consistent with the truncation so a subsequent
        // append at `max_frame + 1` is not misread as a rewind.
        if shared
            .runtime
            .frame_cache_high_water
            .load(Ordering::Acquire)
            > max_frame
        {
            shared
                .runtime
                .frame_cache_high_water
                .store(max_frame, Ordering::Release);
        }
    }

    fn should_checkpoint_on_close(&self) -> bool {
        true
    }

    #[cfg(test)]
    fn backend_name(&self) -> &'static str {
        "in_process"
    }

    #[cfg(test)]
    fn shared_ptr(&self) -> usize {
        Arc::as_ptr(&self.shared) as usize
    }

    fn shared_wal_state(&self) -> Arc<RwLock<WalFileShared>> {
        self.shared.clone()
    }
}

/// Per-connection WAL coordination that delegates to the mmap'd tshm authority.
///
/// One instance exists per `WalFile` (i.e. per `Connection`). All instances
/// within a process share the same `Arc<MappedSharedWalCoordination>` and the
/// same `SharedOwnerRecord` (derived from the authority at construction time).
///
/// `fallback` provides the process-local read-mark / write-lock layer (the
/// same locks used in single-process mode). `authority` provides the
/// cross-process shared state (reader slots, frame index, snapshot metadata).
/// Both are consulted: the fallback serializes same-process connections, the
/// authority serializes across processes.
#[cfg(host_shared_wal)]
#[derive(Debug)]
struct ShmWalCoordination {
    shared: Arc<RwLock<WalFileShared>>,
    fallback: InProcessWalCoordination,
    authority: Arc<MappedSharedWalCoordination>,
    /// This connection's currently held reader slot, if any.
    active_reader: Mutex<Option<SharedReaderSlot>>,
    /// Copied from `authority.owner_record()` at construction — all connections
    /// in the same process share the same owner identity.
    owner: SharedOwnerRecord,
}

#[cfg(host_shared_wal)]
impl ShmWalCoordination {
    fn overflow_fallback_covers(
        &self,
        snapshot: SharedWalCoordinationHeader,
        max_frame: u64,
    ) -> bool {
        self.shared
            .read()
            .runtime
            .overflow_fallback_coverage
            .lock()
            .covers(snapshot, max_frame)
    }

    fn clear_overflow_fallback_coverage(&self) {
        self.shared
            .read()
            .runtime
            .overflow_fallback_coverage
            .lock()
            .clear();
    }

    fn local_authority_snapshot_from_shared(
        shared: &WalFileShared,
        authority_snapshot: SharedWalCoordinationHeader,
    ) -> SharedWalCoordinationHeader {
        let header = shared.metadata.wal_header.lock();
        SharedWalCoordinationHeader {
            max_frame: shared.metadata.max_frame.load(Ordering::Acquire),
            nbackfills: shared.metadata.nbackfills.load(Ordering::Acquire),
            transaction_count: shared.metadata.transaction_count.load(Ordering::Acquire),
            visibility_generation: authority_snapshot.visibility_generation,
            checkpoint_seq: header.checkpoint_seq,
            checkpoint_epoch: shared.runtime.epoch.load(Ordering::Acquire),
            page_size: header.page_size,
            salt_1: header.salt_1,
            salt_2: header.salt_2,
            checksum_1: shared.metadata.last_checksum.0,
            checksum_2: shared.metadata.last_checksum.1,
            reader_slot_count: authority_snapshot.reader_slot_count,
        }
    }

    fn new(
        shared: Arc<RwLock<WalFileShared>>,
        authority: Arc<MappedSharedWalCoordination>,
    ) -> Self {
        let fallback = InProcessWalCoordination::new(shared.clone());
        let coordination = Self {
            shared,
            fallback,
            owner: authority.owner_record(),
            authority,
            active_reader: Mutex::new(None),
        };
        coordination.seed_or_sync_authority();
        coordination
    }

    fn authority_is_uninitialized(snapshot: SharedWalCoordinationHeader) -> bool {
        snapshot.max_frame == 0
            && snapshot.nbackfills == 0
            && snapshot.transaction_count == 0
            && snapshot.visibility_generation == 0
            && snapshot.checkpoint_seq == 0
            && snapshot.checkpoint_epoch == 0
            && snapshot.page_size == 0
            && snapshot.salt_1 == 0
            && snapshot.salt_2 == 0
            && snapshot.checksum_1 == 0
            && snapshot.checksum_2 == 0
    }

    fn local_authority_snapshot(&self) -> SharedWalCoordinationHeader {
        let authority_snapshot = self.authority.snapshot();
        let shared = self.shared.read();
        Self::local_authority_snapshot_from_shared(&shared, authority_snapshot)
    }

    fn install_local_snapshot(
        shared: &mut WalFileShared,
        snapshot: SharedWalCoordinationHeader,
        install_header: bool,
    ) {
        shared
            .metadata
            .max_frame
            .store(snapshot.max_frame, Ordering::Release);
        shared
            .metadata
            .nbackfills
            .store(snapshot.nbackfills, Ordering::Release);
        shared.metadata.last_checksum = (snapshot.checksum_1, snapshot.checksum_2);
        shared
            .metadata
            .transaction_count
            .store(snapshot.transaction_count, Ordering::Release);
        shared
            .runtime
            .epoch
            .store(snapshot.checkpoint_epoch, Ordering::Release);
        if install_header {
            let mut header = shared.metadata.wal_header.lock();
            header.checkpoint_seq = snapshot.checkpoint_seq;
            header.page_size = snapshot.page_size;
            header.salt_1 = snapshot.salt_1;
            header.salt_2 = snapshot.salt_2;
            header.checksum_1 = snapshot.checksum_1;
            header.checksum_2 = snapshot.checksum_2;
        }
    }

    fn sync_local_from_authority(&self, snapshot: SharedWalCoordinationHeader) {
        let mut shared = self.shared.write();
        Self::install_local_snapshot(&mut shared, snapshot, snapshot.page_size != 0);
    }

    fn sync_authority_from_local(&self) {
        self.authority
            .install_snapshot(self.local_authority_snapshot());
    }

    fn sync_local_to_zero_frame_authority(&self, snapshot: SharedWalCoordinationHeader) {
        let mut shared = self.shared.write();
        Self::install_local_snapshot(&mut shared, snapshot, true);
        shared.metadata.initialized.store(false, Ordering::Release);
        shared.runtime.frame_cache.lock().clear();
        shared
            .runtime
            .frame_cache_high_water
            .store(0, Ordering::Release);
        shared.runtime.overflow_fallback_coverage.lock().clear();
    }

    fn sync_authority_frames_from_local(&self) {
        let entries = {
            let shared = self.shared.read();
            let frame_cache = shared.runtime.frame_cache.lock();
            let mut entries = Vec::new();
            for (&page_id, frames) in frame_cache.iter() {
                for &frame_id in frames {
                    entries.push((frame_id, page_id));
                }
            }
            entries
        };
        let mut entries = entries;
        entries.sort_unstable();
        for (frame_id, page_id) in entries {
            self.authority.record_frame(page_id, frame_id);
        }
    }

    fn repair_or_reseed_authority_from_local_disk_scan(
        &self,
        mut authority_snapshot: SharedWalCoordinationHeader,
    ) {
        self.authority.repair_transient_state_for_exclusive_open();
        if authority_snapshot.nbackfills != 0 {
            // A local WAL scan can rebuild the visible WAL tail, but it cannot
            // prove that positive checkpoint progress is durable in the main DB
            // file. Stay on the conservative reopen path until we implement a
            // SQLite-equivalent recovery protocol for trusting partial-checkpoint state.
            authority_snapshot.nbackfills = 0;
            self.authority.install_snapshot(authority_snapshot);
        }
        let local_snapshot = self.local_authority_snapshot();
        if Self::local_scan_predates_zero_frame_authority(authority_snapshot, local_snapshot) {
            self.sync_local_to_zero_frame_authority(authority_snapshot);
            return;
        }
        if Self::local_scan_cannot_disprove_zero_frame_authority(authority_snapshot, local_snapshot)
        {
            self.sync_local_from_authority(authority_snapshot);
            return;
        }
        if Self::local_scan_cannot_disprove_positive_authority(authority_snapshot, local_snapshot) {
            self.sync_local_from_authority(authority_snapshot);
            return;
        }
        if Self::authority_matches_local_wal_scan(authority_snapshot, local_snapshot) {
            self.sync_local_from_authority(authority_snapshot);
            // Matching header metadata is not enough to trust the durable
            // frame index. A restart or interrupted reopen can leave stale or
            // empty page->frame mappings behind while max_frame/checksums
            // still match the scanned WAL. When both snapshots describe the
            // same visible WAL generation, compare the latest per-page
            // mappings directly and rebuild if they diverge.
            if self.authority.frame_index_overflowed()
                || (self.authority.open_mode() == SharedWalCoordinationOpenMode::Exclusive
                    && !self.authority_frame_index_matches_local_wal_scan(local_snapshot.max_frame))
            {
                self.authority
                    .discard_durable_frame_index_for_exclusive_rebuild();
                self.sync_authority_frames_from_local();
            }
            return;
        }
        // The authority and disk scan are from the same generation (matching
        // checkpoint_seq/salts) but disagree on max_frame or checksums. This
        // happens when a concurrent write advances the authority between the
        // snapshot read and the disk scan.  Or the authority is from a strictly
        // newer generation (higher checkpoint_seq) because the WAL was
        // restarted but the on-disk header hasn't been rewritten yet.
        //
        // In both cases the authority's header fields are at least as current
        // as the disk, so adopt them.  As above, preserve the authority's
        // frame index — it is maintained by writers and must not be replaced
        // with a potentially incomplete reconstruction.
        if Self::authority_is_same_or_newer_generation(authority_snapshot, local_snapshot) {
            self.sync_local_from_authority(authority_snapshot);
            if authority_snapshot.checkpoint_seq == local_snapshot.checkpoint_seq
                && self.authority.frame_index_overflowed()
            {
                self.authority
                    .discard_durable_frame_index_for_exclusive_rebuild();
                self.sync_authority_frames_from_local();
            }
            return;
        }

        self.authority
            .discard_durable_frame_index_for_exclusive_rebuild();
        self.sync_authority_from_local();
        self.sync_authority_frames_from_local();
    }

    fn local_scan_cannot_disprove_zero_frame_authority(
        authority_snapshot: SharedWalCoordinationHeader,
        local_snapshot: SharedWalCoordinationHeader,
    ) -> bool {
        !Self::authority_is_uninitialized(authority_snapshot)
            && authority_snapshot.max_frame == 0
            && local_snapshot.max_frame == 0
    }

    fn local_scan_predates_zero_frame_authority(
        authority_snapshot: SharedWalCoordinationHeader,
        local_snapshot: SharedWalCoordinationHeader,
    ) -> bool {
        !Self::authority_is_uninitialized(authority_snapshot)
            && authority_snapshot.max_frame == 0
            && local_snapshot.max_frame > 0
            && local_snapshot.checkpoint_seq < authority_snapshot.checkpoint_seq
    }

    fn local_scan_cannot_disprove_positive_authority(
        authority_snapshot: SharedWalCoordinationHeader,
        local_snapshot: SharedWalCoordinationHeader,
    ) -> bool {
        authority_snapshot.max_frame > 0
            && local_snapshot.max_frame == 0
            && local_snapshot.checkpoint_seq == authority_snapshot.checkpoint_seq
            && local_snapshot.page_size == authority_snapshot.page_size
            && local_snapshot.salt_1 == authority_snapshot.salt_1
            && local_snapshot.salt_2 == authority_snapshot.salt_2
    }

    /// The authority is from a strictly newer WAL generation (higher
    /// checkpoint_seq), OR from the same generation with at least as many
    /// frames.  In either case the authority's header fields were updated
    /// atomically by writers and are at least as current as a point-in-time
    /// disk scan of the WAL file.
    ///
    /// When the generations match but the authority has a *lower* max_frame,
    /// the authority was likely rolled back or corrupted; the disk scan's
    /// higher max_frame is more accurate, so we must NOT match here.
    fn authority_is_same_or_newer_generation(
        authority_snapshot: SharedWalCoordinationHeader,
        local_snapshot: SharedWalCoordinationHeader,
    ) -> bool {
        if Self::authority_is_uninitialized(authority_snapshot) {
            return false;
        }
        // Strictly newer generation — always trust authority.
        if authority_snapshot.checkpoint_seq > local_snapshot.checkpoint_seq {
            return true;
        }
        // Same generation: the authority is atomically updated by writers,
        // so its max_frame is at least as current as what the disk scan
        // observed.  Only match when authority.max_frame >= local to
        // avoid masking a genuinely rolled-back authority.
        authority_snapshot.checkpoint_seq == local_snapshot.checkpoint_seq
            && authority_snapshot.salt_1 == local_snapshot.salt_1
            && authority_snapshot.salt_2 == local_snapshot.salt_2
            && authority_snapshot.max_frame >= local_snapshot.max_frame
    }

    fn authority_matches_local_wal_scan(
        authority_snapshot: SharedWalCoordinationHeader,
        local_snapshot: SharedWalCoordinationHeader,
    ) -> bool {
        authority_snapshot.max_frame == local_snapshot.max_frame
            && authority_snapshot.checkpoint_seq == local_snapshot.checkpoint_seq
            && authority_snapshot.page_size == local_snapshot.page_size
            && authority_snapshot.salt_1 == local_snapshot.salt_1
            && authority_snapshot.salt_2 == local_snapshot.salt_2
            && authority_snapshot.checksum_1 == local_snapshot.checksum_1
            && authority_snapshot.checksum_2 == local_snapshot.checksum_2
    }

    fn authority_frame_index_matches_local_wal_scan(&self, max_frame: u64) -> bool {
        self.authority.iter_latest_frames(0, max_frame)
            == self.fallback.iter_latest_frames(0, max_frame)
    }

    fn local_zero_frame_generation_is_initialized(
        &self,
        authority_snapshot: SharedWalCoordinationHeader,
    ) -> bool {
        let shared = self.shared.read();
        if !shared.metadata.initialized.load(Ordering::Acquire) {
            return false;
        }
        Self::local_zero_frame_generation_matches_authority_snapshot(authority_snapshot, &shared)
    }

    fn local_zero_frame_generation_matches_authority_snapshot(
        authority_snapshot: SharedWalCoordinationHeader,
        shared: &WalFileShared,
    ) -> bool {
        let header = shared.metadata.wal_header.lock();
        header.checkpoint_seq == authority_snapshot.checkpoint_seq
            && header.page_size == authority_snapshot.page_size
            && header.salt_1 == authority_snapshot.salt_1
            && header.salt_2 == authority_snapshot.salt_2
    }

    fn authority_needs_local_header_seed(snapshot: SharedWalCoordinationHeader) -> bool {
        Self::authority_is_uninitialized(snapshot) || snapshot.page_size == 0
    }

    /// Called once at `ShmWalCoordination` construction to reconcile the
    /// process-local WAL view (built from a WAL file scan or inherited from
    /// a previous connection) with the shared tshm authority.
    ///
    /// Three cases:
    ///
    /// 1. **Authority uninitialized** (fresh tshm): seed it from our local
    ///    WAL scan — we are the first process.
    ///
    /// 2. **Authority initialized and we opened from a local disk scan**
    ///    (no writer/checkpoint is active): repair transient owner/reader
    ///    state first. If the scan only sees an empty WAL and the durable
    ///    authority is already at frame 0, keep the durable authority because
    ///    the scan cannot prove newer header metadata. Otherwise, if the
    ///    local scan agrees with the WAL-provable subset of the durable
    ///    snapshot, keep the durable authority. If not, discard the durable
    ///    frame index and rebuild it from the local scan.
    ///
    /// 3. **Authority initialized and trustworthy**: adopt the authority's
    ///    snapshot as our local state. If our local view also came from a
    ///    disk scan and the authority's frame index is empty, backfill it
    ///    from our local frame cache.
    fn seed_or_sync_authority(&self) {
        let snapshot = self.authority.snapshot();
        let local_wal_view_loaded_from_disk = self
            .shared
            .read()
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire);
        if Self::authority_is_uninitialized(snapshot) {
            self.sync_authority_from_local();
            self.sync_authority_frames_from_local();
        } else if local_wal_view_loaded_from_disk
            && !self.authority.writer_or_checkpoint_lock_active()
        {
            self.repair_or_reseed_authority_from_local_disk_scan(snapshot);
        } else {
            let needs_zero_frame_header_rewrite = snapshot.max_frame == 0 && {
                let shared = self.shared.read();
                !shared.metadata.initialized.load(Ordering::Acquire)
                    || !Self::local_zero_frame_generation_matches_authority_snapshot(
                        snapshot, &shared,
                    )
            };
            self.sync_local_from_authority(snapshot);
            if needs_zero_frame_header_rewrite {
                self.shared
                    .read()
                    .metadata
                    .initialized
                    .store(false, Ordering::Release);
            }
            if local_wal_view_loaded_from_disk
                && !self.authority.writer_or_checkpoint_lock_active()
                && self.authority.iter_latest_frames(0, u64::MAX).is_empty()
            {
                self.sync_authority_frames_from_local();
            }
        }
    }

    fn restart_snapshot_from_authority(
        &self,
        snapshot: SharedWalCoordinationHeader,
        io: &dyn IO,
    ) -> WalSnapshot {
        let checkpoint_seq = snapshot.checkpoint_seq.wrapping_add(1);
        let salt_1 = snapshot.salt_1.wrapping_add(1);
        let salt_2 = io.generate_random_number() as u32;
        let restarted = SharedWalCoordinationHeader {
            max_frame: 0,
            nbackfills: 0,
            transaction_count: snapshot.transaction_count,
            visibility_generation: snapshot.visibility_generation,
            checkpoint_seq,
            checkpoint_epoch: snapshot.checkpoint_epoch,
            page_size: snapshot.page_size,
            salt_1,
            salt_2,
            checksum_1: snapshot.checksum_1,
            checksum_2: snapshot.checksum_2,
            reader_slot_count: snapshot.reader_slot_count,
        };

        {
            let mut shared = self.shared.write();
            Self::install_local_snapshot(&mut shared, restarted, true);
            shared.metadata.initialized.store(false, Ordering::Release);
            shared.runtime.frame_cache.lock().clear();
            shared
                .runtime
                .frame_cache_high_water
                .store(0, Ordering::Release);
            shared.runtime.overflow_fallback_coverage.lock().clear();
            shared.runtime.read_locks[0].set_value_exclusive(0);
            shared.runtime.read_locks[1].set_value_exclusive(0);
            for lock in &shared.runtime.read_locks[2..] {
                lock.set_value_exclusive(READMARK_NOT_USED);
            }
        }

        self.authority.rollback_frames(0);
        self.authority.install_snapshot(restarted);

        WalSnapshot {
            max_frame: restarted.max_frame,
            nbackfills: restarted.nbackfills,
            last_checksum: (restarted.checksum_1, restarted.checksum_2),
            checkpoint_seq: restarted.checkpoint_seq,
            transaction_count: restarted.transaction_count,
        }
    }

    fn ensure_local_frame_cache_covers_snapshot(
        &self,
        io: &Arc<dyn IO>,
        required_snapshot: WalSnapshot,
    ) -> Result<()> {
        if required_snapshot.max_frame == 0 || !self.authority.frame_index_overflowed() {
            return Ok(());
        }

        let authority_snapshot = self.authority.snapshot();
        if authority_snapshot.checkpoint_seq != required_snapshot.checkpoint_seq {
            return Err(LimboError::Busy);
        }
        if self.overflow_fallback_covers(authority_snapshot, required_snapshot.max_frame) {
            return Ok(());
        }

        let _ = io;
        tracing::debug!(
            required_max_frame = required_snapshot.max_frame,
            authority_max_frame = authority_snapshot.max_frame,
            authority_checkpoint_seq = authority_snapshot.checkpoint_seq,
            "refusing live overflow fallback refresh on a read path because it would require blocking WAL scan I/O"
        );
        Err(LimboError::Busy)
    }
}

#[cfg(host_shared_wal)]
impl WalCoordination for ShmWalCoordination {
    fn load_snapshot(&self) -> WalSnapshot {
        let snapshot = self.authority.snapshot();
        WalSnapshot {
            max_frame: snapshot.max_frame,
            nbackfills: snapshot.nbackfills,
            last_checksum: (snapshot.checksum_1, snapshot.checksum_2),
            checkpoint_seq: snapshot.checkpoint_seq,
            transaction_count: snapshot.transaction_count,
        }
    }

    fn ensure_local_frame_cache_covers(
        &self,
        io: &Arc<dyn IO>,
        snapshot: WalSnapshot,
    ) -> Result<()> {
        self.ensure_local_frame_cache_covers_snapshot(io, snapshot)
    }

    fn publish_commit(&self, commit: WalCommitState) {
        {
            let mut shared = self.shared.write();
            shared
                .metadata
                .max_frame
                .store(commit.max_frame, Ordering::Release);
            shared.metadata.last_checksum = commit.last_checksum;
            shared
                .metadata
                .transaction_count
                .store(commit.transaction_count, Ordering::Release);
            let mut header = shared.metadata.wal_header.lock();
            header.checksum_1 = commit.last_checksum.0;
            header.checksum_2 = commit.last_checksum.1;
        }
        self.authority.publish_commit(
            commit.max_frame,
            commit.last_checksum.0,
            commit.last_checksum.1,
            commit.transaction_count,
        );
        if self.authority.frame_index_overflowed() {
            let snapshot = self.authority.snapshot();
            let shared = self.shared.read();
            let mut coverage = shared.runtime.overflow_fallback_coverage.lock();
            if coverage.covers(snapshot, commit.max_frame.saturating_sub(1)) {
                coverage.record_snapshot(snapshot, commit.max_frame);
            }
        }
    }

    fn publish_backfill(&self, max_frame: u64) {
        self.shared
            .write()
            .metadata
            .nbackfills
            .store(max_frame, Ordering::Release);
        self.authority.publish_backfill(max_frame);
    }

    fn install_durable_backfill_proof(
        &self,
        nbackfills: u64,
        db_size_pages: u32,
        db_header_crc32c: u32,
        sync_type: FileSyncType,
    ) -> Result<Option<Completion>> {
        let snapshot = self.authority.snapshot();
        turso_assert!(
            (snapshot.nbackfills..=snapshot.max_frame).contains(&nbackfills),
            "durable backfill proof requires nbackfills within the authoritative WAL range",
            {
                "nbackfills": nbackfills,
                "authority_nbackfills": snapshot.nbackfills,
                "authority_max_frame": snapshot.max_frame
            }
        );
        let proof_snapshot = SharedWalCoordinationHeader {
            nbackfills,
            ..snapshot
        };
        self.authority
            .install_backfill_proof(proof_snapshot, db_size_pages, db_header_crc32c);
        Ok(Some(self.authority.begin_sync(sync_type)?))
    }

    fn find_frame(
        &self,
        page_id: u64,
        min_frame: u64,
        max_frame: u64,
        frame_watermark: Option<u64>,
    ) -> Option<u64> {
        // Exhausting the reserved shared index space leaves the authority
        // incomplete. Fall back to the local scanned cache rather than trusting
        // a truncated shared index.
        if self.authority.frame_index_overflowed() {
            return self
                .fallback
                .find_frame(page_id, min_frame, max_frame, frame_watermark);
        }
        self.authority
            .find_frame(page_id, min_frame, max_frame, frame_watermark)
    }

    fn iter_latest_frames(&self, min_frame: u64, max_frame: u64) -> Vec<(u64, u64)> {
        // Same trade-off as find_frame(): if the reserved shared index space is
        // exhausted, keep correctness by consulting the local scanned cache.
        if self.authority.frame_index_overflowed() {
            return self.fallback.iter_latest_frames(min_frame, max_frame);
        }
        self.authority.iter_latest_frames(min_frame, max_frame)
    }

    fn checkpoint_epoch(&self) -> u32 {
        self.authority.checkpoint_epoch()
    }

    fn bump_checkpoint_epoch(&self) -> u32 {
        let prev = self.authority.bump_checkpoint_epoch();
        self.shared
            .write()
            .runtime
            .epoch
            .store(prev + 1, Ordering::Release);
        prev
    }

    fn try_begin_read_tx(&self, snapshot: WalSnapshot) -> Option<ReadGuardKind> {
        turso_assert!(
            snapshot.max_frame <= u32::MAX as u64,
            "max_frame exceeds u32 read mark range"
        );
        let shared = self.shared.read();
        let read_locks = &shared.runtime.read_locks;

        if snapshot.max_frame == snapshot.nbackfills {
            if !read_locks[0].read() {
                return None;
            }
            if self.load_snapshot() != snapshot {
                read_locks[0].unlock();
                return None;
            }
            return Some(ReadGuardKind::DbFile);
        }

        let mut best_idx: i64 = -1;
        let mut best_mark: u32 = 0;
        for (idx, lock) in read_locks.iter().enumerate().take(5).skip(1) {
            let mark = lock.get_value();
            if mark != READMARK_NOT_USED && mark <= snapshot.max_frame as u32 && mark > best_mark {
                best_mark = mark;
                best_idx = idx as i64;
            }
        }

        if best_idx == -1 || (best_mark as u64) < snapshot.max_frame {
            for (idx, lock) in read_locks.iter().enumerate().take(5).skip(1) {
                if !lock.write() {
                    continue;
                }
                lock.set_value_exclusive(snapshot.max_frame as u32);
                best_idx = idx as i64;
                best_mark = snapshot.max_frame as u32;
                read_locks[idx].unlock();
                break;
            }
        }

        if best_idx == -1 || !read_locks[best_idx as usize].read() {
            return None;
        }

        let current_slot_mark = read_locks[best_idx as usize].get_value();
        if current_slot_mark != best_mark || self.load_snapshot() != snapshot {
            read_locks[best_idx as usize].unlock();
            return None;
        }

        let read_mark_index =
            NonZeroUsize::new(best_idx as usize).expect("best_idx checked to be positive");
        let reader = self
            .authority
            .register_reader_for_snapshot(self.owner, snapshot.max_frame)?;
        if self.load_snapshot() != snapshot {
            self.authority.unregister_reader_for_snapshot(reader);
            read_locks[best_idx as usize].unlock();
            return None;
        }

        let mut active_reader = self.active_reader.lock();
        turso_assert!(active_reader.is_none(), "shared reader registration leaked");
        *active_reader = Some(reader);
        Some(ReadGuardKind::ReadMark(read_mark_index))
    }

    fn end_read_tx(&self, guard: ReadGuardKind) {
        if let Some(reader) = self.active_reader.lock().take() {
            self.authority.unregister_reader_for_snapshot(reader);
        }
        self.fallback.end_read_tx(guard);
    }

    fn try_begin_write_tx(&self) -> bool {
        if !self.authority.try_acquire_writer(self.owner) {
            return false;
        }
        if !self.fallback.try_write_lock() {
            self.authority.release_writer(self.owner);
            return false;
        }
        true
    }

    fn end_write_tx(&self) {
        self.fallback.unlock_write_lock();
        self.authority.release_writer(self.owner);
    }

    fn acquire_checkpoint_guard(
        &self,
        mode: CheckpointMode,
    ) -> Result<CoordinationCheckpointGuardKind> {
        if !self.authority.try_acquire_checkpoint(self.owner) {
            return Err(LimboError::Busy);
        }
        let needs_writer = !matches!(mode, CheckpointMode::Passive { .. });
        if needs_writer && !self.authority.try_acquire_writer(self.owner) {
            self.authority.release_checkpoint(self.owner);
            return Err(LimboError::Busy);
        }
        if !self.fallback.try_checkpoint_lock() {
            if needs_writer {
                self.authority.release_writer(self.owner);
            }
            self.authority.release_checkpoint(self.owner);
            return Err(LimboError::Busy);
        }
        match mode {
            CheckpointMode::Passive { .. } => {
                if !self.fallback.try_read_mark_exclusive(0) {
                    self.fallback.unlock_checkpoint_lock();
                    if needs_writer {
                        self.authority.release_writer(self.owner);
                    }
                    self.authority.release_checkpoint(self.owner);
                    return Err(LimboError::Busy);
                }
                Ok(CoordinationCheckpointGuardKind::Read0)
            }
            CheckpointMode::Full | CheckpointMode::Restart | CheckpointMode::Truncate { .. } => {
                if !self.fallback.try_read_mark_exclusive(0) {
                    self.fallback.unlock_checkpoint_lock();
                    self.authority.release_writer(self.owner);
                    self.authority.release_checkpoint(self.owner);
                    return Err(LimboError::Busy);
                }
                if !self.fallback.try_write_lock() {
                    self.fallback.unlock_read_mark(0);
                    self.fallback.unlock_checkpoint_lock();
                    self.authority.release_writer(self.owner);
                    self.authority.release_checkpoint(self.owner);
                    return Err(LimboError::Busy);
                }
                Ok(CoordinationCheckpointGuardKind::Writer)
            }
        }
    }

    fn acquire_vacuum_checkpoint_guard_from_held_lock(
        &self,
    ) -> Result<CoordinationCheckpointGuardKind> {
        if !self.authority.try_acquire_checkpoint(self.owner) {
            self.fallback.unlock_checkpoint_lock();
            return Err(LimboError::Busy);
        }
        if !self.authority.try_acquire_writer(self.owner) {
            self.authority.release_checkpoint(self.owner);
            self.fallback.unlock_checkpoint_lock();
            return Err(LimboError::Busy);
        }
        if !self.fallback.try_read_mark_exclusive(0) {
            self.fallback.unlock_checkpoint_lock();
            self.authority.release_writer(self.owner);
            self.authority.release_checkpoint(self.owner);
            return Err(LimboError::Busy);
        }
        if !self.fallback.try_write_lock() {
            self.fallback.unlock_read_mark(0);
            self.fallback.unlock_checkpoint_lock();
            self.authority.release_writer(self.owner);
            self.authority.release_checkpoint(self.owner);
            return Err(LimboError::Busy);
        }
        Ok(CoordinationCheckpointGuardKind::Writer)
    }

    fn release_checkpoint_guard(&self, guard: CoordinationCheckpointGuardKind) {
        match guard {
            CoordinationCheckpointGuardKind::Writer => {
                self.fallback.unlock_write_lock();
                self.fallback.unlock_read_mark(0);
                self.fallback.unlock_checkpoint_lock();
                self.authority.release_writer(self.owner);
                self.authority.release_checkpoint(self.owner);
            }
            CoordinationCheckpointGuardKind::Read0 => {
                self.fallback.unlock_read_mark(0);
                self.fallback.unlock_checkpoint_lock();
                self.authority.release_checkpoint(self.owner);
            }
        }
    }

    fn determine_max_safe_checkpoint_frame(&self, max_frame: u64) -> u64 {
        turso_assert!(
            max_frame <= u32::MAX as u64,
            "max_frame exceeds u32 read mark range"
        );
        let mut max_safe_frame = max_frame;
        for read_lock_idx in 1..5 {
            let this_mark = self.fallback.read_mark_value(read_lock_idx);
            if this_mark < max_safe_frame as u32 {
                let busy = !self.fallback.try_read_mark_exclusive(read_lock_idx);
                if !busy {
                    let val = if read_lock_idx == 1 {
                        max_safe_frame as u32
                    } else {
                        READMARK_NOT_USED
                    };
                    self.fallback
                        .set_read_mark_value_exclusive(read_lock_idx, val);
                    self.fallback.unlock_read_mark(read_lock_idx);
                } else {
                    max_safe_frame = this_mark as u64;
                }
            }
        }
        match self.authority.min_active_reader_frame() {
            Some(shared_min) => max_safe_frame.min(shared_min),
            None => max_safe_frame,
        }
    }

    fn min_pinned_read_frame(&self) -> Option<u64> {
        // Combine this process's local read marks with cross-process readers tracked by the
        // shared authority.
        let local = self.fallback.min_pinned_read_frame_inner();
        match (local, self.authority.min_active_reader_frame()) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, b) => b,
        }
    }

    fn begin_restart(&self, io: &dyn IO) -> Result<WalSnapshot> {
        for idx in 1..5 {
            if !self.fallback.try_read_mark_exclusive(idx) {
                for held_idx in 1..idx {
                    self.fallback.unlock_read_mark(held_idx);
                }
                return Err(LimboError::Busy);
            }
        }
        // In multi-process mode, readers register with the authority (tshm shared
        // memory), not with fallback OFD byte-range locks. We must also check for
        // active cross-process readers before proceeding with the WAL restart,
        // otherwise we reset the shared WAL state while another process still has
        // an active read transaction, leading to data loss.
        if self.authority.min_active_reader_frame().is_some() {
            for idx in 1..5 {
                self.fallback.unlock_read_mark(idx);
            }
            return Err(LimboError::Busy);
        }
        Ok(self.restart_snapshot_from_authority(self.authority.snapshot(), io))
    }

    fn end_restart(&self) {
        self.fallback.end_restart();
    }

    fn try_restart_log_for_write(&self, io: &dyn IO) -> Result<Option<WalSnapshot>> {
        if !self.fallback.try_upgrade_read_mark(0) {
            return Ok(None);
        }
        let result = self.begin_restart(io);
        self.fallback.downgrade_read_mark(0);
        match result {
            Ok(snapshot) => {
                self.end_restart();
                Ok(Some(snapshot))
            }
            Err(err) => Err(err),
        }
    }

    fn prepare_truncate(&self) -> Result<Arc<dyn File>> {
        self.fallback.prepare_truncate()
    }

    fn wal_header(&self) -> WalHeader {
        let snapshot = self.authority.snapshot();
        let mut header = self.fallback.wal_header();
        if snapshot.page_size == 0 {
            return header;
        }
        header.page_size = snapshot.page_size;
        header.checkpoint_seq = snapshot.checkpoint_seq;
        header.salt_1 = snapshot.salt_1;
        header.salt_2 = snapshot.salt_2;
        header.checksum_1 = snapshot.checksum_1;
        header.checksum_2 = snapshot.checksum_2;
        header
    }

    fn wal_file(&self) -> Result<Arc<dyn File>> {
        self.fallback.wal_file()
    }

    fn shared_wal_state(&self) -> Arc<RwLock<WalFileShared>> {
        self.shared.clone()
    }

    fn wal_is_initialized(&self) -> bool {
        let authority_snapshot = self.authority.snapshot();
        if Self::authority_needs_local_header_seed(authority_snapshot) {
            return self.fallback.wal_is_initialized();
        }
        if authority_snapshot.max_frame > 0 {
            self.sync_local_from_authority(authority_snapshot);
            self.fallback.mark_initialized();
            return true;
        }
        if self.local_zero_frame_generation_is_initialized(authority_snapshot) {
            return true;
        }

        self.sync_local_from_authority(authority_snapshot);
        self.shared
            .read()
            .metadata
            .initialized
            .store(false, Ordering::Release);
        false
    }

    fn prepare_wal_header(&self, io: &dyn IO, page_size: PageSize) -> Option<WalHeader> {
        let authority_snapshot = self.authority.snapshot();
        // A zero-frame authority snapshot after RESTART/TRUNCATE is still
        // authoritative: it carries the latest transaction_count,
        // checkpoint_seq, salts, and checksums for readers. Sync from it
        // before preparing the header so the bytes written to disk belong to
        // the same generation as the authority snapshot.
        if Self::authority_needs_local_header_seed(authority_snapshot) {
            let header = self.fallback.prepare_wal_header(io, page_size);
            if header.is_some() {
                self.sync_authority_from_local();
            }
            return header;
        }
        self.sync_local_from_authority(authority_snapshot);
        let header = self.fallback.prepare_wal_header(io, page_size);
        if header.is_some() {
            self.sync_authority_from_local();
        }
        header
    }

    fn mark_initialized(&self) {
        self.fallback.mark_initialized();
    }

    fn cache_frame(&self, page_id: u64, frame_id: u64) {
        self.fallback.cache_frame(page_id, frame_id);
        self.authority.record_frame(page_id, frame_id);
    }

    fn rollback_cache(&self, max_frame: u64) {
        self.fallback.rollback_cache(max_frame);
        self.authority.rollback_frames(max_frame);
        self.clear_overflow_fallback_coverage();
    }

    fn should_checkpoint_on_close(&self) -> bool {
        self.authority.is_last_process_mapping()
    }

    #[cfg(test)]
    fn backend_name(&self) -> &'static str {
        "tshm"
    }

    #[cfg(test)]
    fn shared_ptr(&self) -> usize {
        Arc::as_ptr(&self.shared) as usize
    }

    #[cfg(test)]
    fn open_mode_name(&self) -> Option<&'static str> {
        Some(match self.authority.open_mode() {
            SharedWalCoordinationOpenMode::Exclusive => "exclusive",
            SharedWalCoordinationOpenMode::MultiProcess => "multiprocess",
        })
    }
}

#[derive(Debug, Clone)]
pub enum CheckpointState {
    Start,
    /// Fsync the WAL before backfilling any frame into the database file.
    /// Under `synchronous=NORMAL` commits do not fsync the WAL, so without
    /// this durability barrier a crash mid-backfill could persist some
    /// backfilled DB pages while recovery drops the unsynced WAL tail,
    /// leaving a torn database that matches no committed prefix.
    SyncWal,
    Processing,
    /// Determine the checkpoint result: update nBackfills, restart log if needed.
    DetermineResult,
    /// Final cleanup: release locks, clear internal state, return result.
    /// WAL truncation (if needed) is handled by pager.rs via truncate_wal() AFTER the DB is synced.
    Finalize {
        checkpoint_result: Option<CheckpointResult>,
    },
}

/// IOV_MAX is 1024 on most systems, lets use 512 to be safe
pub const CKPT_BATCH_PAGES: usize = 512;

/// TODO: *ALL* of these need to be tuned for perf. It is tricky
/// trying to figure out the ideal numbers here to work together concurrently
const MIN_AVG_RUN_FOR_FLUSH: f32 = 32.0;
const MIN_BATCH_LEN_FOR_FLUSH: usize = 512;
const MAX_INFLIGHT_WRITES: usize = 64;
pub const MAX_INFLIGHT_READS: usize = 512;
pub const IOV_MAX: usize = 1024;

type PageId = usize;
struct InflightRead {
    completion: Completion,
    page_id: PageId,
    /// Buffer slot to contain the page content from the WAL read.
    buf: Arc<SpinLock<Option<Arc<Buffer>>>>,
}

/// WriteBatch is a collection of pages that are being checkpointed together. It is used to
/// aggregate contiguous pages into a single write operation to the database file.
#[derive(Default)]
struct WriteBatch {
    /// BTreeMap for sorting during insertion, helps create more efficient `writev` operations.
    items: BTreeMap<PageId, Arc<Buffer>>,
    /// total number of `runs`, each representing a contiguous group of `PageId`s
    run_count: usize,
}

impl WriteBatch {
    fn new() -> Self {
        Self {
            items: BTreeMap::new(),
            run_count: 0,
        }
    }

    #[inline]
    /// Add a pageId + Buffer to the batch of Writes to be submitted.
    fn insert(&mut self, page_id: PageId, buf: Arc<Buffer>) {
        if let std::collections::btree_map::Entry::Occupied(mut e) = self.items.entry(page_id) {
            e.insert(buf);
            return;
        }
        // Single range query to check neighbors
        let start = page_id.saturating_sub(1);
        let end = page_id.saturating_add(1);
        let mut has_left = false;
        let mut has_right = false;

        for (k, _) in self.items.range(start..=end) {
            if *k == page_id.wrapping_sub(1) {
                has_left = true;
            }
            if *k == page_id.wrapping_add(1) {
                has_right = true;
            }
        }
        match (has_left, has_right) {
            (false, false) => self.run_count += 1,
            (true, true) => self.run_count = self.run_count.saturating_sub(1),
            _ => {}
        }
        self.items.insert(page_id, buf);
    }

    #[inline]
    fn len(&self) -> usize {
        self.items.len()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    #[inline]
    fn is_full(&self) -> bool {
        self.items.len() >= CKPT_BATCH_PAGES
    }

    #[inline]
    fn avg_run_len(&self) -> f32 {
        if self.run_count == 0 {
            0.0
        } else {
            self.items.len() as f32 / self.run_count as f32
        }
    }

    #[inline]
    fn take(&mut self) -> BTreeMap<PageId, Arc<Buffer>> {
        self.run_count = 0;
        std::mem::take(&mut self.items)
    }

    #[inline]
    fn clear(&mut self) {
        self.items.clear();
        self.run_count = 0;
    }
}

impl std::ops::Deref for WriteBatch {
    type Target = BTreeMap<PageId, Arc<Buffer>>;
    fn deref(&self) -> &Self::Target {
        &self.items
    }
}
impl std::ops::DerefMut for WriteBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.items
    }
}

/// Information and structures for processing a checkpoint operation.
struct OngoingCheckpoint {
    /// Used for benchmarking/debugging a checkpoint operation.
    time: MonotonicInstant,
    /// minimum frame number to be backfilled by this checkpoint operation.
    min_frame: u64,
    /// maximum safe frame number that will be backfilled by this checkpoint operation.
    max_frame: u64,
    /// cursor used to iterate through all the pages that might have a frame in the safe range
    current_page: u64,
    /// State of the checkpoint
    state: CheckpointState,
    /// Batch repreesnts a collection of pages to be backfilled to the DB file.
    pending_writes: WriteBatch,
    /// Read operations currently ongoing.
    inflight_reads: Vec<InflightRead>,
    /// Array of atomic counters representing write operations that are currently in flight.
    inflight_writes: Vec<InflightWriteBatch>,
    /// List of all page_id + frame_id combinations to be backfilled
    pages_to_checkpoint: Vec<(u64, u64)>,
}

struct InflightWriteBatch {
    done: Arc<AtomicBool>,
    err: Arc<crate::sync::OnceLock<CompletionError>>,
}

impl OngoingCheckpoint {
    fn reset(&mut self) {
        self.min_frame = 0;
        self.max_frame = 0;
        self.current_page = 0;
        self.pages_to_checkpoint.clear();
        self.pending_writes.clear();
        self.inflight_reads.clear();
        self.inflight_writes.clear();
        self.state = CheckpointState::Start;
    }

    #[inline]
    /// Whether or not new reads should be issued during checkpoint processing.
    fn should_issue_reads(&self) -> bool {
        (self.current_page as usize) < self.pages_to_checkpoint.len()
            && !self.pending_writes.is_full()
            && self.inflight_reads.len() < MAX_INFLIGHT_READS
    }

    #[inline]
    /// Whether the backfilling/IO process is entirely completed during checkpoint processing.
    fn complete(&self) -> bool {
        (self.current_page as usize) >= self.pages_to_checkpoint.len()
            && self.inflight_reads.is_empty()
            && self.pending_writes.is_empty()
            && self.inflight_writes.is_empty()
    }

    #[inline]
    /// Whether we should flush an exisitng batch of writes and begin concurrently aggregating a new one.
    fn should_flush_batch(&self) -> bool {
        self.pending_writes.is_full()
            || (self.pending_writes.len() >= MIN_BATCH_LEN_FOR_FLUSH
                && self.pending_writes.avg_run_len() >= MIN_AVG_RUN_FOR_FLUSH)
            || ((self.current_page as usize) >= self.pages_to_checkpoint.len()
                && self.inflight_reads.is_empty()
                && !self.pending_writes.is_empty())
    }

    #[inline]
    /// Remove any completed write operations from `inflight_writes`,
    /// returns whether any progress was made.
    fn process_inflight_writes(&mut self) -> bool {
        let before_len = self.inflight_writes.len();
        self.inflight_writes
            .retain(|w| !w.done.load(Ordering::Acquire));
        before_len > self.inflight_writes.len()
    }

    #[inline]
    /// Remove any completed read operations from `inflight_reads`
    /// returns whether any progress was made.
    fn process_pending_reads(&mut self) -> Result<bool> {
        let mut moved = false;
        let mut err: Option<CompletionError> = None;

        self.inflight_reads.retain(|slot| {
            if !slot.completion.finished() {
                return true;
            }
            if slot.completion.succeeded() {
                if let Some(buf) = slot.buf.lock().take() {
                    self.pending_writes.insert(slot.page_id, buf);
                    moved = true;
                } else {
                    err = Some(CompletionError::IOError(std::io::ErrorKind::Other, "read"));
                }
            } else {
                err = Some(
                    slot.completion
                        .get_error()
                        .unwrap_or(CompletionError::IOError(std::io::ErrorKind::Other, "read")),
                );
            }
            false
        });
        if let Some(e) = err {
            return Err(LimboError::CompletionError(e));
        }
        Ok(moved)
    }

    fn first_write_error(&self) -> Option<CompletionError>
    where
        CompletionError: Clone,
    {
        self.inflight_writes
            .iter()
            .find_map(|w| w.err.get().cloned())
    }
}

impl InflightWriteBatch {
    #[inline]
    fn new() -> InflightWriteBatch {
        InflightWriteBatch {
            done: Arc::new(AtomicBool::new(false)),
            err: Arc::new(OnceLock::new()),
        }
    }
}

impl fmt::Debug for OngoingCheckpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OngoingCheckpoint")
            .field("state", &self.state)
            .field("min_frame", &self.min_frame)
            .field("max_frame", &self.max_frame)
            .field("current_page", &self.current_page)
            .finish()
    }
}

pub struct WalFile {
    io: Arc<dyn IO>,
    buffer_pool: Arc<BufferPool>,
    coordination: Arc<dyn WalCoordination>,

    syncing: Arc<AtomicBool>,
    write_lock_held: AtomicBool,

    ongoing_checkpoint: RwLock<OngoingCheckpoint>,
    checkpoint_threshold: usize,
    /// This is the index to the read_lock in WalFileShared that we are holding. This lock contains
    /// the max frame for this connection.
    max_frame_read_lock_index: AtomicUsize,
    /// Max frame allowed to lookup range=(minframe..max_frame)
    max_frame: AtomicU64,
    /// Start of range to look for frames range=(minframe..max_frame)
    min_frame: AtomicU64,
    /// Check of last frame in WAL, this is a cumulative checksum over all frames in the WAL
    last_checksum: RwLock<(u32, u32)>,
    checkpoint_seq: AtomicU32,
    transaction_count: AtomicU64,

    /// Manages locks needed for checkpointing
    checkpoint_guard: RwLock<Option<CheckpointLocks>>,
    /// Manages locks needed for VACUUM. This is very much similar to `checkpoint_guard`
    /// This lock is to be held by all readers before they can begin. And VACUUM holds it
    /// exclusively. See `install_vacuum_lock_guard` for its lifecycle.
    vacuum_lock_guard: RwLock<Option<VacuumLockGuard>>,

    io_ctx: RwLock<IOContext>,

    /// The WAL file is dirty: frames were appended that no successful fsync
    /// has covered yet. Set whenever a frame is recorded via
    /// `complete_append_frame`, cleared when a WAL fsync completes
    /// successfully. A dirty WAL owes an fsync before a commit may be
    /// reported durable under synchronous=FULL.
    /// Shared with the fsync completion callback, hence the Arc.
    dirty: Arc<AtomicBool>,
}

impl fmt::Debug for WalFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalFile")
            .field("syncing", &self.syncing.load(Ordering::Relaxed))
            .field("page_size", &self.page_size())
            .field("ongoing_checkpoint", &*self.ongoing_checkpoint.read())
            .field("checkpoint_threshold", &self.checkpoint_threshold)
            .field("max_frame_read_lock_index", &self.max_frame_read_lock_index)
            .field("max_frame", &self.max_frame)
            .field("min_frame", &self.min_frame)
            // Excluding other fields
            .finish()
    }
}

/*
* sqlite3/src/wal.c
*
** nBackfill is the number of frames in the WAL that have been written
** back into the database. (We call the act of moving content from WAL to
** database "backfilling".)  The nBackfill number is never greater than
** WalIndexHdr.mxFrame.  nBackfill can only be increased by threads
** holding the WAL_CKPT_LOCK lock (which includes a recovery thread).
** However, a WAL_WRITE_LOCK thread can move the value of nBackfill from
** mxFrame back to zero when the WAL is reset.
**
** nBackfillAttempted is the largest value of nBackfill that a checkpoint
** has attempted to achieve.  Normally nBackfill==nBackfillAtempted, however
** the nBackfillAttempted is set before any backfilling is done and the
** nBackfill is only set after all backfilling completes.  So if a checkpoint
** crashes, nBackfillAttempted might be larger than nBackfill.  The
** WalIndexHdr.mxFrame must never be less than nBackfillAttempted.
**
** The aLock[] field is a set of bytes used for locking.  These bytes should
** never be read or written.
**
** There is one entry in aReadMark[] for each reader lock.  If a reader
** holds read-lock K, then the value in aReadMark[K] is no greater than
** the mxFrame for that reader.  The value READMARK_NOT_USED (0xffffffff)
** for any aReadMark[] means that entry is unused.  aReadMark[0] is
** a special case; its value is never used and it exists as a place-holder
** to avoid having to offset aReadMark[] indexes by one.  Readers holding
** WAL_READ_LOCK(0) always ignore the entire WAL and read all content
** directly from the database.
**
** The value of aReadMark[K] may only be changed by a thread that
** is holding an exclusive lock on WAL_READ_LOCK(K).  Thus, the value of
** aReadMark[K] cannot changed while there is a reader is using that mark
** since the reader will be holding a shared lock on WAL_READ_LOCK(K).
**
** The checkpointer may only transfer frames from WAL to database where
** the frame numbers are less than or equal to every aReadMark[] that is
** in use (that is, every aReadMark[j] for which there is a corresponding
** WAL_READ_LOCK(j)).  New readers (usually) pick the aReadMark[] with the
** largest value and will increase an unused aReadMark[] to mxFrame if there
** is not already an aReadMark[] equal to mxFrame.  The exception to the
** previous sentence is when nBackfill equals mxFrame (meaning that everything
** in the WAL has been backfilled into the database) then new readers
** will choose aReadMark[0] which has value 0 and hence such reader will
** get all their all content directly from the database file and ignore
** the WAL.
**
** Writers normally append new frames to the end of the WAL.  However,
** if nBackfill equals mxFrame (meaning that all WAL content has been
** written back into the database) and if no readers are using the WAL
** (in other words, if there are no WAL_READ_LOCK(i) where i>0) then
** the writer will first "reset" the WAL back to the beginning and start
** writing new content beginning at frame 1.
*/

/// Authoritative WAL metadata currently shared by all connections in a process.
pub struct WalSharedMetadata {
    pub enabled: AtomicBool,
    pub wal_header: Arc<SpinLock<WalHeader>>,
    pub min_frame: AtomicU64,
    pub max_frame: AtomicU64,
    pub nbackfills: AtomicU64,
    pub transaction_count: AtomicU64,
    pub last_checksum: (u32, u32), // Check of last frame in WAL, this is a cumulative checksum over all frames in the WAL
    pub loaded: AtomicBool,
    pub loaded_from_disk_scan: AtomicBool,
    pub initialized: AtomicBool,
}

/// Process-local coordination and caches layered around the shared WAL metadata.
pub struct WalSharedRuntime {
    // Frame cache maps a Page to all the frames it has stored in WAL in ascending order.
    // This is to easily find the frame it must checkpoint each connection if a checkpoint is
    // necessary.
    // One difference between SQLite and limbo is that we will never support multi process, meaning
    // we don't need WAL's index file. So we can do stuff like this without shared memory.
    // TODO: this will need refactoring because this is incredible memory inefficient.
    pub frame_cache: Arc<SpinLock<FxHashMap<u64, Vec<u64>>>>,
    /// Highest frame number currently recorded in `frame_cache` for the active
    /// WAL generation. Used to detect frame-slot reuse / append-position
    /// rewinds: within a generation frames are appended with strictly
    /// increasing numbers, so caching a frame that is not above this watermark
    /// means the slots from that frame upward are being overwritten and any
    /// stale `page -> frame` mappings for them must be purged (otherwise
    /// `find_frame` can return a frame slot that now holds a different page).
    /// Only read/written while holding the `frame_cache` lock.
    pub frame_cache_high_water: AtomicU64,
    pub file: Option<Arc<dyn File>>,
    /// Read locks advertise the maximum WAL frame a reader may access.
    /// Slot 0 is special, when it is held (shared) the reader bypasses the WAL and uses the main DB file.
    /// When checkpointing, we must acquire the exclusive read lock 0 to ensure that no readers read
    /// from a partially checkpointed db file.
    /// Slots 1‑4 carry a frame‑number in value and may be shared by many readers. Slot 1 is the
    /// default read lock and is to contain the max_frame in WAL.
    pub read_locks: [TursoRwLock; 5],
    /// Lock used by in-place VACUUM to keep new read/write transactions out
    /// while VACUUM is in progress.
    /// Normal WAL transactions hold this shared for the lifetime of their
    /// transaction. VACUUM holds it exclusively until its final truncate
    /// checkpoint has completed.
    pub vacuum_lock: TursoRwLock,
    /// There is only one write allowed in WAL mode. This lock takes care of ensuring there is only
    /// one used.
    pub write_lock: TursoRwLock,

    /// Serialises checkpointer threads, only one checkpoint can be in flight at any time. Blocking and exclusive only
    pub checkpoint_lock: TursoRwLock,
    /// Increments on each checkpoint, used to prevent stale cached pages being used for
    /// backfilling.
    pub epoch: AtomicU32,
    /// Tracks how far the process-local `frame_cache` is known to be complete
    /// for overflow fallback in the current WAL generation.
    pub overflow_fallback_coverage: Arc<SpinLock<OverflowFallbackCoverage>>,
}

/// Drivable result of [`WalFileShared::open_shared_if_exists_begin`]. Either an
/// immediate no-op WAL (readonly, file absent) or an in-progress recovery scan
/// to be pumped via [`OpenSharedWal::poll`] until it returns `Done`.
pub enum OpenSharedWal {
    Noop(Arc<RwLock<WalFileShared>>),
    Build(sqlite3_ondisk::BuildSharedWal),
}

impl OpenSharedWal {
    pub fn poll(&mut self) -> IOResultOr<Arc<RwLock<WalFileShared>>> {
        match self {
            OpenSharedWal::Noop(wal) => Ok(IOResult::Done(wal.clone())),
            OpenSharedWal::Build(driver) => driver.poll(),
        }
    }
}

/// WalFileShared holds process-wide WAL metadata plus process-local coordination state.
pub struct WalFileShared {
    pub metadata: WalSharedMetadata,
    pub runtime: WalSharedRuntime,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OverflowFallbackCoverage {
    checkpoint_seq: u32,
    salt_1: u32,
    salt_2: u32,
    max_frame: u64,
    valid: bool,
}

impl OverflowFallbackCoverage {
    pub(crate) fn clear(&mut self) {
        *self = Self::default();
    }

    pub(crate) fn record(&mut self, checkpoint_seq: u32, salt_1: u32, salt_2: u32, max_frame: u64) {
        if max_frame == 0 {
            self.clear();
            return;
        }
        self.checkpoint_seq = checkpoint_seq;
        self.salt_1 = salt_1;
        self.salt_2 = salt_2;
        self.max_frame = max_frame;
        self.valid = true;
    }

    #[cfg(host_shared_wal)]
    pub(crate) fn record_snapshot(
        &mut self,
        snapshot: SharedWalCoordinationHeader,
        max_frame: u64,
    ) {
        self.record(
            snapshot.checkpoint_seq,
            snapshot.salt_1,
            snapshot.salt_2,
            max_frame,
        );
    }

    #[cfg(host_shared_wal)]
    pub(crate) fn same_generation(&self, snapshot: SharedWalCoordinationHeader) -> bool {
        self.valid
            && self.checkpoint_seq == snapshot.checkpoint_seq
            && self.salt_1 == snapshot.salt_1
            && self.salt_2 == snapshot.salt_2
    }

    #[cfg(host_shared_wal)]
    pub(crate) fn covers(&self, snapshot: SharedWalCoordinationHeader, max_frame: u64) -> bool {
        self.same_generation(snapshot) && self.max_frame >= max_frame
    }
}

impl fmt::Debug for WalFileShared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalFileShared")
            .field("enabled", &self.metadata.enabled.load(Ordering::Relaxed))
            .field("wal_header", &self.metadata.wal_header)
            .field("min_frame", &self.metadata.min_frame)
            .field("max_frame", &self.metadata.max_frame)
            .field("nbackfills", &self.metadata.nbackfills)
            .field("frame_cache", &self.runtime.frame_cache)
            .field("last_checksum", &self.metadata.last_checksum)
            // Excluding `file`, `read_locks`, and `write_lock`
            .finish()
    }
}

#[derive(Debug)]
enum VacuumLockGuard {
    Read { ptr: Arc<RwLock<WalFileShared>> },
    Write { ptr: Arc<RwLock<WalFileShared>> },
}

impl VacuumLockGuard {
    fn try_read(ptr: Arc<RwLock<WalFileShared>>) -> Option<Self> {
        let acquired = {
            let shared = ptr.read();
            shared.runtime.vacuum_lock.read()
        };
        if acquired {
            Some(Self::Read { ptr })
        } else {
            None
        }
    }

    fn try_write(ptr: Arc<RwLock<WalFileShared>>) -> Option<Self> {
        let acquired = {
            let shared = ptr.read();
            shared.runtime.vacuum_lock.write()
        };
        if acquired {
            Some(Self::Write { ptr })
        } else {
            None
        }
    }

    const fn is_read(&self) -> bool {
        matches!(self, Self::Read { .. })
    }

    const fn is_write(&self) -> bool {
        matches!(self, Self::Write { .. })
    }
}

impl Drop for VacuumLockGuard {
    fn drop(&mut self) {
        match self {
            Self::Read { ptr } | Self::Write { ptr } => {
                ptr.read().runtime.vacuum_lock.unlock();
            }
        }
    }
}

#[derive(Clone, Debug)]
/// To manage and ensure that no locks are leaked during checkpointing in
/// the case of errors. It is held by the WalFile while checkpoint is ongoing
/// then transferred to the CheckpointResult if necessary.
enum CheckpointLocks {
    Writer {
        coordination: Arc<dyn WalCoordination>,
    },
    Read0 {
        coordination: Arc<dyn WalCoordination>,
    },
}

/// CheckpointLockSource says whether the checkpoint state machine should acquire checkpoint_lock
/// itself or consume checkpoint_lock already held by the caller.
/// Most of the time, the default `Acquire` is used, except for VACUUM.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum CheckpointLockSource {
    #[default]
    Acquire,
    HeldByCaller,
}

/// Database checkpointers takes the following locks, in order:
/// The exclusive CHECKPOINTER lock.
/// The exclusive WRITER lock (FULL, RESTART and TRUNCATE only).
/// Exclusive lock on read-mark slots 1-N. These are immediately released after being taken.
/// Exclusive lock on read-mark 0.
/// Exclusive lock on read-mark slots 1-N again. These are immediately released after being taken (RESTART and TRUNCATE only).
/// All of the above use blocking locks.
impl CheckpointLocks {
    fn new(coordination: Arc<dyn WalCoordination>, mode: CheckpointMode) -> Result<Self> {
        let guard = coordination.acquire_checkpoint_guard(mode)?;
        Ok(match guard {
            CoordinationCheckpointGuardKind::Read0 => Self::Read0 { coordination },
            CoordinationCheckpointGuardKind::Writer => Self::Writer { coordination },
        })
    }

    /// Build checkpoint ownership from a checkpoint_lock that VACUUM already
    /// holds. This consumes that raw lock ownership: on success the
    /// returned guard owns checkpoint/read0/write as appropriate, and on error
    /// the coordination backend releases the held checkpoint lock before
    /// returning.
    fn from_held_vacuum_checkpoint_lock(coordination: Arc<dyn WalCoordination>) -> Result<Self> {
        let guard = coordination.acquire_vacuum_checkpoint_guard_from_held_lock()?;
        Ok(match guard {
            CoordinationCheckpointGuardKind::Read0 => Self::Read0 { coordination },
            CoordinationCheckpointGuardKind::Writer => Self::Writer { coordination },
        })
    }
}

impl Drop for CheckpointLocks {
    fn drop(&mut self) {
        match self {
            CheckpointLocks::Writer { coordination } => {
                coordination.release_checkpoint_guard(CoordinationCheckpointGuardKind::Writer);
            }
            CheckpointLocks::Read0 { coordination } => {
                coordination.release_checkpoint_guard(CoordinationCheckpointGuardKind::Read0);
            }
        }
    }
}

/// Result of try_begin_read_tx - either success or a retriable condition.
enum TryBeginReadResult {
    /// Successfully started read transaction, returns whether DB changed
    Ok(bool),
    /// Transient condition, caller should retry immediately (like SQLite's WAL_RETRY)
    Retry,
    /// Non-retriable failure while preparing the local WAL view.
    Err(LimboError),
    /// We could get a lock / source snapshot for readers because WAL is exclusively held by
    /// other transaction.
    /// This usually happens during VACUUM when it holds the vacuum lock exclusively.
    /// Retrying will not help until VACUUM releases; caller should surface Busy
    /// to the client rather than spin.
    Busy,
}

impl WalFile {
    fn prepare_transformed_frame(
        buffer_pool: &Arc<BufferPool>,
        wal_header: &WalHeader,
        previous_checksums: (u32, u32),
        page_number: u32,
        db_size: u32,
        page: &[u8],
        page_transform: &PageTransform,
    ) -> Result<((u32, u32), Arc<Buffer>)> {
        turso_assert!(
            page_number > 0,
            "WAL page number must be one-based",
            { "page_number": page_number }
        );
        let page_size = wal_header.page_size as usize;
        turso_assert!(
            PageSize::new(wal_header.page_size).is_some(),
            "WAL header must contain a valid page size",
            { "page_size": wal_header.page_size }
        );
        turso_assert!(
            page.len() == page_size,
            "WAL input page size must match the WAL header",
            { "input_len": page.len(), "page_size": page_size }
        );

        let frame = prepare_wal_frame_header(buffer_pool, wal_header, page_number, db_size);
        turso_assert!(
            frame.len() == WAL_FRAME_HEADER_SIZE + page_size,
            "WAL frame buffer size must match its header and page body",
            {
                "frame_len": frame.len(),
                "expected_len": WAL_FRAME_HEADER_SIZE + page_size
            }
        );
        let frame_body = &mut frame.as_mut_slice()[WAL_FRAME_HEADER_SIZE..];
        turso_assert!(
            frame_body.len() == page.len(),
            "WAL frame body size must match the input page",
            { "frame_body_len": frame_body.len(), "input_len": page.len() }
        );

        match page_transform {
            PageTransform::Codec(codec) => codec.encode_page(
                PageCodecContext::new(page_number, PageLocation::Wal),
                page,
                frame_body,
            )?,
            PageTransform::Checksum(checksum) => {
                frame_body.copy_from_slice(page);
                checksum.add_checksum_to_page(frame_body, page_number as usize)?;
            }
            PageTransform::None => frame_body.copy_from_slice(page),
        }

        let checksum =
            recompute_wal_frame_checksum(frame.as_mut_slice(), wal_header, previous_checksums);
        Ok((checksum, frame))
    }

    /// Load the authoritative WAL snapshot through the coordination backend.
    fn load_coordination_snapshot(&self) -> WalSnapshot {
        self.coordination.load_snapshot()
    }

    /// Reconstruct the connection-local WAL state stored on this `WalFile`.
    fn connection_state(&self) -> WalConnectionState {
        WalConnectionState::new(
            WalSnapshot {
                max_frame: self.max_frame.load(Ordering::Acquire),
                nbackfills: self.min_frame.load(Ordering::Acquire).saturating_sub(1),
                last_checksum: *self.last_checksum.read(),
                checkpoint_seq: self.checkpoint_seq.load(Ordering::Acquire),
                transaction_count: self.transaction_count.load(Ordering::Acquire),
            },
            ReadGuardKind::from_lock_index(self.max_frame_read_lock_index.load(Ordering::Acquire)),
        )
    }

    /// Persist a connection-local WAL snapshot bundle back into the legacy fields on `WalFile`.
    fn install_connection_state(&self, state: WalConnectionState) {
        self.max_frame
            .store(state.snapshot.max_frame, Ordering::Release);
        self.min_frame
            .store(state.snapshot.min_frame(), Ordering::Release);
        *self.last_checksum.write() = state.snapshot.last_checksum;
        self.checkpoint_seq
            .store(state.snapshot.checkpoint_seq, Ordering::Release);
        self.transaction_count
            .store(state.snapshot.transaction_count, Ordering::Release);
        self.max_frame_read_lock_index
            .store(state.read_guard.lock_index(), Ordering::Release);
    }

    /// Compare a freshly loaded shared snapshot against the connection's current snapshot.
    fn db_changed_against(&self, snapshot: WalSnapshot, local_state: WalConnectionState) -> bool {
        snapshot != local_state.snapshot
    }

    fn has_vacuum_read_lock_guard(&self) -> bool {
        self.vacuum_lock_guard
            .read()
            .as_ref()
            .is_some_and(VacuumLockGuard::is_read)
    }

    // VACUUM lock guard lifecycle:
    // - Normal readers install a read guard in `try_begin_read_tx` after the
    //   read-mark slot is selected; `end_read_tx` releases that guard through
    //   `release_vacuum_read_lock_guard`.
    // - Normal writers do not install their own VACUUM guard. They are an
    //   upgrade of an existing read transaction, so their guard is still the
    //   read guard owned by the read transaction.
    // - In-place VACUUM installs a write guard and takes the WAL write lock in
    //   `begin_vacuum_blocking_tx`. `end_write_tx` releases the WAL write lock, and
    //   `release_vacuum_lock` releases the write guard.
    fn install_vacuum_lock_guard(&self, guard: VacuumLockGuard) {
        let mut slot = self.vacuum_lock_guard.write();
        turso_assert!(slot.is_none(), "VACUUM lock guard is already installed");
        *slot = Some(guard);
    }

    fn release_vacuum_read_lock_guard(&self) {
        let guard = {
            let mut slot = self.vacuum_lock_guard.write();
            turso_assert!(
                slot.as_ref().is_some_and(VacuumLockGuard::is_read),
                "VACUUM read lock guard is not held"
            );
            slot.take()
                .expect("VACUUM read lock guard should be present after kind check")
        };
        drop(guard);
    }

    fn release_vacuum_write_lock_guard(&self) {
        let guard = {
            let mut slot = self.vacuum_lock_guard.write();
            turso_assert!(
                slot.as_ref().is_some_and(VacuumLockGuard::is_write),
                "VACUUM write lock guard is not held"
            );
            slot.take()
                .expect("VACUUM write lock guard should be present after kind check")
        };
        drop(guard);
    }

    /// Try to begin a read transaction. Returns Retry for transient conditions
    /// that should be retried immediately, Ok for success.
    fn try_begin_read_tx(&self) -> TryBeginReadResult {
        turso_assert!(
            self.max_frame_read_lock_index
                .load(Ordering::Acquire)
                .eq(&NO_LOCK_HELD),
            "cannot start a new read tx without ending an existing one",
            { "lock_value": self.max_frame_read_lock_index.load(Ordering::Acquire), "expected": NO_LOCK_HELD }
        );
        turso_assert!(
            self.vacuum_lock_guard.read().is_none(),
            "VACUUM lock guard already held"
        );

        // Before we can start the txn, we must first take read lock on the vacuum. If we cannot,
        // then vacuum is already in progress. Once we acquire a read lock, this would prevent
        // vacuum to run till the lock is released.
        let Some(vacuum_lock_guard) =
            VacuumLockGuard::try_read(self.coordination.shared_wal_state())
        else {
            tracing::debug!("begin_read_tx: VACUUM holds the vacuum lock, returning Busy");
            return TryBeginReadResult::Busy;
        };

        // Snapshot the shared WAL state. We haven't taken a read lock yet, so we need
        // to validate these values later.
        let shared_snapshot = self.load_coordination_snapshot();
        turso_assert!(
            shared_snapshot.nbackfills <= shared_snapshot.max_frame,
            "WAL snapshot cannot have backfills beyond max frame",
            {
                "nbackfills": shared_snapshot.nbackfills,
                "max_frame": shared_snapshot.max_frame,
                "checkpoint_seq": shared_snapshot.checkpoint_seq
            }
        );
        tracing::debug!(
            "try_begin_read_tx: shared_max={}, nbackfills={}, last_checksum={:?}, checkpoint_seq={:?}, transaction_count={}",
            shared_snapshot.max_frame,
            shared_snapshot.nbackfills,
            shared_snapshot.last_checksum,
            shared_snapshot.checkpoint_seq,
            shared_snapshot.transaction_count
        );
        if let Err(err) = self
            .coordination
            .ensure_local_frame_cache_covers(&self.io, shared_snapshot)
        {
            return match err {
                LimboError::Busy => TryBeginReadResult::Retry,
                other => TryBeginReadResult::Err(other),
            };
        }

        // Check if database changed since this connection's last read transaction.
        // If it has, the connection will invalidate its page cache.
        let db_changed = self.db_changed_against(shared_snapshot, self.connection_state());

        tracing::debug!("try_begin_read_tx: db_changed={}", db_changed);

        // If WAL is fully checkpointed (shared_max == nbackfills), readers can ignore
        // the WAL and read directly from the DB file by holding read_locks[0].
        if shared_snapshot.max_frame == shared_snapshot.nbackfills {
            tracing::debug!(
                "begin_read_tx: WAL fully checkpointed, shared_max={}, nbackfills={}",
                shared_snapshot.max_frame,
                shared_snapshot.nbackfills
            );
        }

        let Some(read_guard) = self.coordination.try_begin_read_tx(shared_snapshot) else {
            return TryBeginReadResult::Retry;
        };
        self.install_vacuum_lock_guard(vacuum_lock_guard);
        self.install_connection_state(WalConnectionState::new(shared_snapshot, read_guard));
        tracing::debug!(
            "begin_read_tx(min={}, max={}, slot={}, max_frame_in_wal={})",
            self.min_frame.load(Ordering::Acquire),
            self.max_frame.load(Ordering::Acquire),
            read_guard.lock_index(),
            shared_snapshot.max_frame
        );
        TryBeginReadResult::Ok(db_changed)
    }
}

impl Wal for WalFile {
    fn begin_read_tx(&self) -> Result<bool> {
        // Implement progressive backoff because transient lock contention
        // should resolve quickly, but under heavy contention busy-spinning wastes
        // CPU. SQLite uses quadratic backoff after 5 retries, with total delay
        // up to ~10 seconds before giving up, so we just mirror SQLite's implementation
        // here.
        let mut cnt = 0u32;
        loop {
            tracing::trace!("begin_read_tx: cnt={cnt}");
            match self.try_begin_read_tx() {
                TryBeginReadResult::Ok(changed) => return Ok(changed),
                TryBeginReadResult::Err(err) => return Err(err),
                TryBeginReadResult::Busy => return Err(LimboError::Busy),
                TryBeginReadResult::Retry => {
                    cnt += 1;
                    if cnt > 100 {
                        return Err(LimboError::Busy);
                    }
                    // Progressive backoff: first 5 retries are immediate, then we
                    // start yielding/sleeping with increasing delays.
                    if cnt > 5 {
                        if cnt < 10 {
                            // Retries 6-9: yield to scheduler (minimal delay)
                            self.io.yield_now();
                        } else {
                            // Retries 10+: quadratic backoff in microseconds
                            // Formula matches SQLite: (cnt-9)^2 * 39 microseconds
                            let delay_us = ((cnt - 9) * (cnt - 9) * 39) as u64;
                            self.io.sleep(std::time::Duration::from_micros(delay_us));
                        }
                    }
                    continue;
                }
            }
        }
    }

    fn mvcc_refresh_if_db_changed(&self) -> bool {
        WalFile::mvcc_refresh_if_db_changed(self)
    }

    /// End a read transaction.
    #[inline(always)]
    #[instrument(skip_all, level = Level::DEBUG)]
    fn end_read_tx(&self) {
        let slot = self.max_frame_read_lock_index.load(Ordering::Acquire);
        if slot != NO_LOCK_HELD {
            self.coordination
                .end_read_tx(ReadGuardKind::from_lock_index(slot));
            self.max_frame_read_lock_index
                .store(NO_LOCK_HELD, Ordering::Release);
            self.release_vacuum_read_lock_guard();
            tracing::debug!("end_read_tx(slot={slot})");
        } else {
            // if NO_LOCK_HELD, then we must not have vacuum lock either.
            turso_assert!(
                !self.has_vacuum_read_lock_guard(),
                "vacuum read lock guard held without setting lock slot NO_LOCK_HELD"
            );
            tracing::debug!("end_read_tx(slot=no_lock)");
        }
    }

    /// Begin a write transaction
    #[instrument(skip_all, level = Level::DEBUG)]
    fn begin_write_tx(&self, allowed_auto_actions: WalAutoActions) -> Result<()> {
        tracing::debug!("begin_write_tx");
        let begin_write_result: Result<()> = {
            // sqlite/src/wal.c 3702
            // Cannot start a write transaction without first holding a read
            // transaction.
            // assert(pWal->readLock >= 0);
            // assert(pWal->writeLock == 0 && pWal->iReCksum == 0);
            turso_assert!(
                self.max_frame_read_lock_index.load(Ordering::Acquire) != NO_LOCK_HELD,
                "must have a read transaction to begin a write transaction"
            );
            turso_assert!(
                !self.holds_write_lock(),
                "write lock already held by this connection"
            );
            if !self.coordination.try_begin_write_tx() {
                return Err(LimboError::Busy);
            }
            let db_changed =
                self.db_changed_against(self.load_coordination_snapshot(), self.connection_state());
            if db_changed {
                // Snapshot is stale, give up and let caller retry from scratch.
                // Return BusySnapshot instead of Busy so the caller knows it must
                // restart the read transaction to get a fresh snapshot.
                // Retrying with busy_timeout will NEVER HELP.
                tracing::debug!(
                    "unable to upgrade transaction from read to write: snapshot is stale, give up and let caller retry from scratch, self.max_frame={}, shared_max={}",
                    self.max_frame.load(Ordering::Acquire),
                    self.load_coordination_snapshot().max_frame
                );
                self.coordination.end_write_tx();
                return Err(LimboError::BusySnapshot);
            }

            Ok(())
        };
        begin_write_result?;
        if self
            .write_lock_held
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            self.coordination.end_write_tx();
            turso_assert!(
                false,
                "begin_write_tx called while write lock already held according to connection state"
            );
        }

        if !allowed_auto_actions.contains(WalAutoActions::Restart) {
            return Ok(());
        }

        let result = self.try_restart_log_before_write();
        if let Err(LimboError::Busy) | Ok(()) = &result {
            // it's fine if we were unable to restart WAL file due to Busy errors
            return Ok(());
        }

        // don't forget to release the write-lock if
        self.coordination.end_write_tx();
        turso_assert!(
            self.write_lock_held
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok(),
            "end_write_tx called while write lock not held according to connection state"
        );

        Err(result.expect_err("Ok case handled above"))
    }

    /// End a write transaction
    #[instrument(skip_all, level = Level::DEBUG)]
    fn end_write_tx(&self) {
        turso_assert!(
            self.write_lock_held
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok(),
            "end_write_tx called while write lock not held according to connection state"
        );
        self.coordination.end_write_tx();
    }

    /// Returns true if this WAL instance currently holds a read lock.
    fn holds_read_lock(&self) -> bool {
        self.max_frame_read_lock_index.load(Ordering::Acquire) != NO_LOCK_HELD
    }

    /// Returns true if this WAL instance currently holds the write lock.
    fn holds_write_lock(&self) -> bool {
        self.write_lock_held.load(Ordering::Acquire)
    }

    fn should_checkpoint_on_close(&self) -> bool {
        self.coordination.should_checkpoint_on_close()
    }

    /// Find the latest frame containing a page.
    #[instrument(skip_all, level = Level::DEBUG)]
    #[aristo::intent(
        "find_frame never reads outside the live frame range [nbackfills, max_frame]\n",
        id = "aristos:wal_find_frame_range_invariant",
        verify = "full",
        parent = "wal_protocol_correctness"
    )]
    fn find_frame(&self, page_id: u64, frame_watermark: Option<u64>) -> Result<Option<u64>> {
        #[cfg(not(feature = "conn_raw_api"))]
        turso_assert!(
            frame_watermark.is_none(),
            "unexpected use of frame_watermark optional argument"
        );

        turso_assert!(
            frame_watermark.unwrap_or(0) <= self.max_frame.load(Ordering::Acquire),
            "frame_watermark must be <= than current WAL max_frame value"
        );

        // we can guarantee correctness of the method, only if frame_watermark is strictly after the current checkpointed prefix
        //
        // if it's not, than pages from WAL range [frame_watermark..nBackfill] are already in the DB file,
        // and in case if page first occurrence in WAL was after frame_watermark - we will be unable to read proper previous version of the page
        let nbackfills = self.load_coordination_snapshot().nbackfills;
        turso_assert!(
            frame_watermark.is_none() || frame_watermark.unwrap() >= nbackfills,
            "frame_watermark must be >= than current WAL backfill amount",
            { "frame_watermark": frame_watermark, "nbackfills": nbackfills }
        );

        // if we are holding read_lock 0 and didn't write anything to the WAL, skip and read right from db file.
        //
        // note, that max_frame_read_lock_index is set to 0 only when shared_max_frame == nbackfill in which case
        // min_frame is set to nbackfill + 1 and max_frame is set to shared_max_frame
        //
        // by default, SQLite tries to restart log file in this case - but for now let's keep it simple in the turso-db
        if self.max_frame_read_lock_index.load(Ordering::Acquire) == 0
            && self.max_frame.load(Ordering::Acquire) < self.min_frame.load(Ordering::Acquire)
        {
            tracing::debug!(
                "find_frame(page_id={}, frame_watermark={:?}): max_frame is 0 - read from DB file",
                page_id,
                frame_watermark,
            );
            return Ok(None);
        }
        let min_frame = self.min_frame.load(Ordering::Acquire);
        let max_frame = self.max_frame.load(Ordering::Acquire);
        self.coordination.ensure_local_frame_cache_covers(
            &self.io,
            WalSnapshot {
                max_frame,
                nbackfills: self.min_frame.load(Ordering::Acquire).saturating_sub(1),
                last_checksum: *self.last_checksum.read(),
                checkpoint_seq: self.coordination.wal_header().checkpoint_seq,
                transaction_count: self.transaction_count.load(Ordering::Acquire),
            },
        )?;
        tracing::debug!(
            "find_frame(page_id={}, frame_watermark={:?}): min_frame={}, max_frame={}",
            page_id,
            frame_watermark,
            min_frame,
            max_frame
        );
        let frame = self
            .coordination
            .find_frame(page_id, min_frame, max_frame, frame_watermark);
        if let Some(frame) = frame {
            tracing::debug!(
                "find_frame(page_id={}, frame_watermark={:?}): found frame={}",
                page_id,
                frame_watermark,
                frame
            );
        }
        Ok(frame)
    }

    /// Read a frame from the WAL.
    #[instrument(skip_all, level = Level::DEBUG)]
    fn read_frame(
        &self,
        frame_id: u64,
        page: PageRef,
        buffer_pool: Arc<BufferPool>,
        group: Option<&mut CompletionGroup>,
    ) -> Result<Completion> {
        tracing::debug!(
            "read_frame(page_idx = {}, frame_id = {})",
            page.get().id,
            frame_id
        );
        let offset = self.frame_offset(frame_id);
        page.set_locked();
        let frame = page.clone();
        let page_idx = page.get().id;
        let epoch_at_issue = self.coordination.checkpoint_epoch();
        let complete = Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
            let Ok((buf, bytes_read)) = res else {
                tracing::debug!(err = ?res.unwrap_err());
                page.clear_locked();
                page.clear_wal_tag();
                return None; // IO error already captured in completion
            };
            let buf_len = buf.len();
            if bytes_read != buf_len as i32 {
                tracing::debug!(
                    "WAL short read at offset {offset}, page {page_idx}, frame_id={frame_id}: expected {buf_len} bytes, got {bytes_read}"
                );
                page.clear_locked();
                page.clear_wal_tag();
                return Some(CompletionError::ShortReadWalFrame {
                    offset,
                    expected: buf_len,
                    actual: bytes_read as usize,
                });
            }
            let cloned = frame.clone();
            finish_read_page(page.get().id, buf, cloned);
            frame.set_wal_tag(frame_id, epoch_at_issue);
            None
        });
        // important not to hold shared state locks beyond this point to avoid deadlock with
        // completions that re-enter WAL state while a writer is waiting.
        let file = self.coordination.wal_file()?;
        begin_read_wal_frame(
            file.as_ref(),
            offset + WAL_FRAME_HEADER_SIZE as u64,
            buffer_pool,
            complete,
            page_idx,
            &self.io_ctx.read(),
            group,
        )
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn read_frames_batch(
        &self,
        start_frame: u64,
        pages: &[PageRef],
        buffer_pool: Arc<BufferPool>,
        scratch_buf: Option<Arc<Buffer>>,
        group: Option<&mut CompletionGroup>,
    ) -> Result<Completion> {
        turso_assert!(
            !pages.is_empty(),
            "read_frames_batch requires at least one page"
        );
        let page_size = self.page_size() as usize;
        turso_assert!(page_size > 0, "WAL page size must be initialized");
        let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
        let count = pages.len();
        let total = frame_size * count;
        let offset = self.frame_offset(start_frame);
        if let Some(buf) = &scratch_buf {
            turso_assert!(
                buf.len() == total,
                "read_frames_batch scratch_buf size must match expected pread length",
                { "buf_len": buf.len(), "expected": total }
            );
        }

        // Lock each target page and pre-allocate its destination buffer so the
        // completion callback only parses headers, decrypts/verifies, and copies.
        let mut slots: Vec<(PageRef, Arc<Buffer>)> = Vec::with_capacity(count);
        for page in pages.iter() {
            #[cfg(debug_assertions)]
            {
                turso_assert!(
                    !page.is_locked(), "read_frames_batch target page must not already be locked",
                    { "page_id": page.get().id }
                );
                turso_assert!(
                    !page.is_loaded(), "read_frames_batch target page must be an unloaded scratch page",
                    { "page_id": page.get().id }
                );
                turso_assert!(
                    page.get().buffer().is_none(),
                    "read_frames_batch target page must not already retain a buffer",
                    { "page_id": page.get().id }
                );
            }
            page.set_locked();
            slots.push((page.clone(), Arc::new(buffer_pool.get_page())));
        }

        let epoch = self.coordination.checkpoint_epoch();
        let page_transform = self.io_ctx.read().page_transform().clone();
        let raw_buf = scratch_buf.unwrap_or_else(|| Arc::new(Buffer::new_temporary(total)));

        let complete = Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
            let clear_slots_on_err = |slots: &[(PageRef, Arc<Buffer>)]| {
                for (page, _) in slots {
                    page.clear_locked();
                    page.clear_wal_tag();
                }
            };

            let Ok((buf, bytes_read)) = res else {
                tracing::debug!(err = ?res.unwrap_err());
                clear_slots_on_err(&slots);
                return None;
            };
            if bytes_read != total as i32 {
                tracing::debug!(
                    "short read on WAL batch at offset {offset}: expected {total} bytes, got {bytes_read}"
                );
                clear_slots_on_err(&slots);
                return Some(CompletionError::ShortReadWalFrame {
                    offset,
                    expected: total,
                    actual: bytes_read as usize,
                });
            }
            let raw = buf.as_slice();
            for (i, (page, page_buf)) in slots.iter().enumerate() {
                let frame_start = i * frame_size;
                let frame = &raw[frame_start..frame_start + frame_size];
                let (header, page_body) = sqlite3_ondisk::parse_wal_frame_header(frame);
                let expected_page_id = page.get().id;
                if header.page_number as usize != expected_page_id {
                    mark_unlikely();
                    tracing::error!(
                        frame_id = start_frame + i as u64,
                        expected = expected_page_id,
                        got = header.page_number,
                        "WAL batch frame page_no mismatch"
                    );
                    clear_slots_on_err(&slots);
                    return Some(CompletionError::WalFramePageMismatch {
                        frame_id: start_frame + i as u64,
                        expected: expected_page_id,
                        actual: header.page_number,
                    });
                }

                let body_slice = page_buf.as_mut_slice();
                turso_assert!(
                    body_slice.len() == page_size,
                    "read_frames_batch buffer size must match WAL page size",
                    { "buffer_len": body_slice.len(), "page_size": page_size }
                );
                match &page_transform {
                    PageTransform::Codec(ctx) => {
                        let codec_context =
                            PageCodecContext::from_page_idx(expected_page_id, PageLocation::Wal)
                                .expect("WAL page numbers fit in u32");
                        match ctx.decode_page(codec_context, page_body, body_slice) {
                            Ok(()) => {}
                            Err(e) => {
                                mark_unlikely();
                                tracing::error!(
                                    "Failed to decode WAL batch frame for page_idx={expected_page_id}: {e}"
                                );
                                clear_slots_on_err(&slots);
                                return Some(page_codec_completion_error(
                                    ctx.as_ref(),
                                    expected_page_id,
                                ));
                            }
                        }
                    }
                    PageTransform::Checksum(ctx) => {
                        body_slice.copy_from_slice(page_body);
                        if let Err(e) = ctx.verify_checksum(body_slice, expected_page_id) {
                            mark_unlikely();
                            tracing::error!(
                                "Failed to verify checksum for page_id={expected_page_id}: {e}"
                            );
                            clear_slots_on_err(&slots);
                            return Some(e);
                        }
                    }
                    PageTransform::None => body_slice.copy_from_slice(page_body),
                }
            }

            for (i, (page, page_buf)) in slots.iter().enumerate() {
                let page_id = page.get().id;
                finish_read_page(page_id, page_buf.clone(), page.clone());
                page.set_wal_tag(start_frame + i as u64, epoch);
            }
            None
        });

        let c = Completion::new_read(raw_buf, complete);
        if let Some(group) = group {
            group.add(&c);
        }
        let file = self.coordination.wal_file()?;
        file.pread(offset, c)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    // todo(sivukhin): change API to accept Buffer or some other owned type
    // this method involves IO and cross "async" boundary - so juggling with references is bad and dangerous
    fn read_frame_raw(&self, frame_id: u64, frame: &mut [u8]) -> Result<Completion> {
        tracing::debug!("read_frame_raw({})", frame_id);
        let offset = self.frame_offset(frame_id);
        let expected_frame_len = WAL_FRAME_HEADER_SIZE + self.page_size() as usize;
        if frame.len() != expected_frame_len {
            return Err(LimboError::InvalidArgument(format!(
                "unexpected WAL frame buffer size: got={}, expected={expected_frame_len}",
                frame.len()
            )));
        }

        // HACK: *mut u8 can't be Sent between threads safely, cast it to usize then
        // for the time of writing this comment - this is *safe* as all callers immediately call synchronous method wait_for_completion and hold necessary references
        let (frame_ptr, frame_len) = (frame.as_mut_ptr() as usize, frame.len());

        let page_transform = self.io_ctx.read().page_transform().clone();
        let complete = Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
            let Ok((buf, bytes_read)) = res else {
                return None; // IO error already captured in completion
            };
            let buf_len = buf.len();
            if bytes_read != buf_len as i32 {
                tracing::debug!(
                    "short read on WAL frame {frame_id} at offset {offset}: expected {buf_len} bytes, got {bytes_read}"
                );
                return Some(CompletionError::ShortReadWalFrame {
                    offset,
                    expected: buf_len,
                    actual: bytes_read as usize,
                });
            }
            let frame_ptr = frame_ptr as *mut u8;
            let frame_ref: &mut [u8] =
                unsafe { std::slice::from_raw_parts_mut(frame_ptr, frame_len) };

            frame_ref[..WAL_FRAME_HEADER_SIZE]
                .copy_from_slice(&buf.as_slice()[..WAL_FRAME_HEADER_SIZE]);
            let (header, raw_page) = sqlite3_ondisk::parse_wal_frame_header(buf.as_slice());

            match &page_transform {
                PageTransform::Codec(ctx) => {
                    let codec_context =
                        PageCodecContext::new(header.page_number, PageLocation::Wal);
                    let output = &mut frame_ref[WAL_FRAME_HEADER_SIZE..];
                    turso_assert!(
                        output.len() == raw_page.len(),
                        "decoded WAL page buffer size must match encoded page size",
                        { "output_len": output.len(), "input_len": raw_page.len() }
                    );
                    match ctx.decode_page(codec_context, raw_page, output) {
                        Ok(()) => {}
                        Err(e) => {
                            tracing::debug!(
                                "Failed to decode page data for frame_id={frame_id}: {e}"
                            );
                            return Some(page_codec_completion_error(
                                ctx.as_ref(),
                                header.page_number as usize,
                            ));
                        }
                    }
                }
                PageTransform::Checksum(_) | PageTransform::None => {
                    frame_ref[WAL_FRAME_HEADER_SIZE..].copy_from_slice(raw_page);
                }
            }
            None
        });
        let file = self.coordination.wal_file()?;
        let c = begin_read_wal_frame_raw(&self.buffer_pool, file.as_ref(), offset, complete)?;
        Ok(c)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    // todo(sivukhin): change API to accept Buffer or some other owned type
    // this method involves IO and cross "async" boundary - so juggling with references is bad and dangerous
    fn write_frame_raw(
        &self,
        buffer_pool: Arc<BufferPool>,
        frame_id: u64,
        page_id: u64,
        db_size: u64,
        page: &[u8],
        sync_type: FileSyncType,
    ) -> Result<()> {
        let Some(page_size) = PageSize::new(page.len() as u32) else {
            bail_corrupt_error!("invalid page size: {}", page.len());
        };
        self.ensure_header_if_needed(page_size, sync_type)?;
        tracing::debug!("write_raw_frame({})", frame_id);
        // if page_size wasn't initialized before - we will initialize it during that raw write
        if self.page_size() != 0 && page.len() != self.page_size() as usize {
            return Err(LimboError::InvalidArgument(format!(
                "unexpected page size in frame: got={}, expected={}",
                page.len(),
                self.page_size(),
            )));
        }
        if frame_id > self.max_frame.load(Ordering::Acquire) + 1 {
            // attempt to write frame out of sequential order - error out
            return Err(LimboError::InvalidArgument(format!(
                "frame_id is beyond next frame in the WAL: frame_id={}, max_frame={}",
                frame_id,
                self.max_frame.load(Ordering::Acquire)
            )));
        }
        if frame_id <= self.max_frame.load(Ordering::Acquire) {
            // just validate if page content from the frame matches frame in the WAL
            let offset = self.frame_offset(frame_id);
            let conflict = Arc::new(Mutex::new(false));

            // HACK: *mut u8 can't be shared between threads safely, cast it to usize then
            // for the time of writing this comment - this is *safe* as the function immediately call synchronous method wait_for_completion and hold necessary references
            let (page_ptr, page_len) = (page.as_ptr() as usize, page.len());

            let complete = Box::new({
                let conflict = conflict.clone();
                move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                    let Ok((buf, bytes_read)) = res else {
                        return None; // IO error already captured in completion
                    };
                    let buf_len = buf.len();
                    if bytes_read != buf_len as i32 {
                        tracing::debug!(
                            "short read on WAL frame validation at offset {offset}, page_id={page_id}: expected {buf_len} bytes, got {bytes_read}"
                        );
                        return Some(CompletionError::ShortReadWalFrame {
                            offset,
                            expected: buf_len,
                            actual: bytes_read as usize,
                        });
                    }
                    let page = unsafe { std::slice::from_raw_parts(page_ptr as *mut u8, page_len) };
                    if buf.as_slice() != page {
                        *conflict.lock() = true;
                    }
                    None
                }
            });
            let file = self.coordination.wal_file()?;
            let c = begin_read_wal_frame(
                file.as_ref(),
                offset + WAL_FRAME_HEADER_SIZE as u64,
                buffer_pool,
                complete,
                page_id as usize,
                &self.io_ctx.read(),
                None,
            )?;
            self.io.wait_for_completion(c)?;
            return if *conflict.lock() {
                Err(LimboError::Conflict(format!(
                    "frame content differs from the WAL: frame_id={frame_id}"
                )))
            } else {
                Ok(())
            };
        }

        // perform actual write
        let offset = self.frame_offset(frame_id);
        let header = self.coordination.wal_header();
        let file = self.coordination.wal_file()?;
        let previous_checksums = *self.last_checksum.read();
        let page_number = u32::try_from(page_id).map_err(|_| LimboError::IntegerOverflow)?;
        let db_size = u32::try_from(db_size).map_err(|_| LimboError::IntegerOverflow)?;
        let page_transform = self.io_ctx.read().page_transform().clone();
        let (checksums, frame_bytes) = Self::prepare_transformed_frame(
            &buffer_pool,
            &header,
            previous_checksums,
            page_number,
            db_size,
            page,
            &page_transform,
        )?;
        let c = Completion::new_write(|_| {});
        let c = file.pwrite(offset, frame_bytes, c)?;
        self.io.wait_for_completion(c)?;
        self.complete_append_frame(page_id, frame_id, checksums);
        if db_size > 0 {
            self.finish_append_frames_commit()?;
        }
        Ok(())
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn should_checkpoint(&self) -> bool {
        let snapshot = self.load_coordination_snapshot();
        snapshot.max_frame as usize > self.checkpoint_threshold + snapshot.nbackfills as usize
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn checkpoint(
        &self,
        pager: &Pager,
        mode: CheckpointMode,
        sync_mode: SyncMode,
    ) -> IOResultOr<CheckpointResult> {
        self.checkpoint_inner(pager, mode, CheckpointLockSource::Acquire, sync_mode)
            .inspect_err(|e| {
                tracing::debug!("WAL checkpoint failed: {e}");
                let _ = self.checkpoint_guard.write().take();
                self.ongoing_checkpoint.write().state = CheckpointState::Start;
            })
    }

    fn vacuum_checkpoint_with_held_lock(
        &self,
        pager: &Pager,
        sync_mode: SyncMode,
    ) -> IOResultOr<CheckpointResult> {
        self.checkpoint_inner(
            pager,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
            CheckpointLockSource::HeldByCaller,
            sync_mode,
        )
        .inspect_err(|e| {
            tracing::debug!("WAL checkpoint failed: {e}");
            let _ = self.checkpoint_guard.write().take();
            self.ongoing_checkpoint.write().state = CheckpointState::Start;
        })
    }

    fn install_durable_backfill_proof(
        &self,
        max_frame: u64,
        db_size_pages: u32,
        db_header_crc32c: u32,
        sync_type: FileSyncType,
    ) -> Result<Option<Completion>> {
        self.coordination.install_durable_backfill_proof(
            max_frame,
            db_size_pages,
            db_header_crc32c,
            sync_type,
        )
    }

    fn publish_backfill(&self, max_frame: u64) {
        let snapshot = self.load_coordination_snapshot();
        turso_assert!(
            (snapshot.nbackfills..=snapshot.max_frame).contains(&max_frame),
            "published backfill must stay within the current WAL generation",
            {
                "publish_backfill": max_frame,
                "current_nbackfills": snapshot.nbackfills,
                "current_max_frame": snapshot.max_frame
            }
        );
        self.coordination.publish_backfill(max_frame);
    }

    #[instrument(err, skip_all, level = Level::DEBUG)]
    fn sync(&self, sync_type: FileSyncType) -> Result<Completion> {
        tracing::debug!("wal_sync");
        let syncing = self.syncing.clone();
        let dirty = self.dirty.clone();
        let completion = Completion::new_sync(move |result| {
            tracing::debug!("wal_sync finish");
            if let Err(err) = result {
                tracing::debug!("wal_sync failed: {err}");
            } else {
                dirty.store(false, Ordering::Release);
            }
            syncing.store(false, Ordering::Release);
        });
        let file = self.coordination.wal_file()?;
        self.syncing.store(true, Ordering::Release);
        let c = file.sync(completion, sync_type)?;
        Ok(c)
    }

    // Currently used for assertion purposes
    fn is_syncing(&self) -> bool {
        self.syncing.load(Ordering::Acquire)
    }

    fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    fn get_max_frame_in_wal(&self) -> u64 {
        self.load_coordination_snapshot().max_frame
    }

    fn get_checkpoint_seq(&self) -> u32 {
        self.load_coordination_snapshot().checkpoint_seq
    }

    fn get_max_frame(&self) -> u64 {
        self.max_frame.load(Ordering::Acquire)
    }

    fn connection_wal_pos(&self) -> (u32, u64) {
        (
            self.checkpoint_seq.load(Ordering::Acquire),
            self.max_frame.load(Ordering::Acquire),
        )
    }

    fn min_pinned_read_frame(&self) -> Option<u64> {
        self.coordination.min_pinned_read_frame()
    }

    fn get_min_frame(&self) -> u64 {
        self.min_frame.load(Ordering::Acquire)
    }

    fn backfill_frame(&self) -> u64 {
        self.load_coordination_snapshot().nbackfills
    }

    fn get_last_checksum(&self) -> (u32, u32) {
        *self.last_checksum.read()
    }
    #[instrument(skip_all, level = Level::DEBUG)]

    fn rollback(&self, rollback_to: Option<RollbackTo>) {
        let is_savepoint = rollback_to.is_some();
        let snapshot = self.load_coordination_snapshot();
        if let Some(r) = &rollback_to {
            // Savepoint WAL positions are captured under the write lock
            // (still held here), and no restart can happen while it is
            // held: the writer-upgrade restart runs before positions
            // materialize, and checkpoint RESTART/TRUNCATE takes the writer
            // lock. A cross-generation position is therefore impossible.
            // (SQLite must clamp instead — sqlite3WalSavepointUndo resets
            // aWalData on an nCkpt mismatch — because it captures at
            // write-tx begin but restarts later, at the first frame write.)
            turso_assert!(
                r.checkpoint_seq == snapshot.checkpoint_seq,
                "savepoint WAL position must be from the current WAL generation",
                {
                    "savepoint_checkpoint_seq": r.checkpoint_seq,
                    "authority_checkpoint_seq": snapshot.checkpoint_seq,
                    "savepoint_frame": r.frame,
                    "authority_max_frame": snapshot.max_frame
                }
            );
            // The committed mark cannot advance while the write lock is
            // held, so the position can never be behind it.
            turso_assert!(
                r.frame >= snapshot.max_frame,
                "savepoint WAL position must not be behind the committed high-water mark",
                { "savepoint_frame": r.frame, "authority_max_frame": snapshot.max_frame }
            );
        }
        // Restored verbatim, like SQLite's aWalData. A checksum captured at
        // frame 0 of a freshly restarted generation predates the new WAL
        // header; that is harmless because prepare_frames seeds frame 1
        // from the header itself.
        let max_frame = rollback_to
            .as_ref()
            .map(|r| r.frame)
            .unwrap_or(snapshot.max_frame);
        let last_checksum = rollback_to
            .as_ref()
            .map(|r| r.checksum)
            .unwrap_or(snapshot.last_checksum);
        self.coordination.rollback_cache(max_frame);
        *self.last_checksum.write() = last_checksum;
        self.max_frame.store(max_frame, Ordering::Release);
        if !is_savepoint {
            self.reset_internal_states();
        }
    }

    fn abort_checkpoint(&self) {
        let _ = self.checkpoint_guard.write().take();
        self.reset_internal_states();
    }

    fn try_begin_vacuum_checkpoint_lock(&self) -> Result<()> {
        self.with_shared(|shared| {
            if !shared.runtime.checkpoint_lock.write() {
                return Err(LimboError::Busy);
            }
            Ok(())
        })
    }

    fn release_vacuum_checkpoint_lock(&self) {
        self.with_shared(|shared| {
            shared.runtime.checkpoint_lock.unlock();
        });
    }

    fn begin_vacuum_blocking_tx(&self) -> Result<()> {
        turso_assert!(
            self.max_frame_read_lock_index.load(Ordering::Acquire) == NO_LOCK_HELD,
            "begin_vacuum_blocking_tx: must not already hold a read lock"
        );
        turso_assert!(
            !self.holds_write_lock(),
            "begin_vacuum_blocking_tx: must not already hold the write lock"
        );
        turso_assert!(
            self.vacuum_lock_guard.read().is_none(),
            "VACUUM lock guard already held"
        );

        let Some(vacuum_lock_guard) =
            VacuumLockGuard::try_write(self.coordination.shared_wal_state())
        else {
            return Err(LimboError::Busy);
        };

        // This block is purely an invariant check. The exclusive VACUUM lock can be held
        // only if we don't have any other active locks.
        self.with_shared(|shared| {
            for idx in 0..shared.runtime.read_locks.len() {
                // iff there are no read locks active, only then we should be able to
                // acquire the write lock
                turso_assert!(
                    shared.runtime.read_locks[idx].write(),
                    "begin_vacuum_blocking_tx: read lock held after VACUUM lock acquired",
                    { "read_lock_idx": idx }
                );
                shared.runtime.read_locks[idx].unlock();
            }
        });

        // Install connection state with a fresh snapshot.
        let snapshot = self.load_coordination_snapshot();
        self.install_connection_state(WalConnectionState::new(snapshot, ReadGuardKind::None));
        turso_assert!(
            self.with_shared(|shared| shared.runtime.write_lock.write()),
            "begin_vacuum_blocking_tx: write lock held after VACUUM lock acquired"
        );
        if self
            .write_lock_held
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            turso_assert!(
                false,
                "begin_vacuum_blocking_tx: write_lock_held already set"
            );
        }
        self.install_vacuum_lock_guard(vacuum_lock_guard);
        Ok(())
    }

    fn release_vacuum_lock(&self) {
        // This drops the stop-the-world gate after VACUUM is one.
        // Only after this new readers can proceed.
        turso_assert!(
            !self.holds_write_lock(),
            "release_vacuum_lock called while source write lock is still held"
        );
        self.release_vacuum_write_lock_guard();
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn finish_append_frames_commit(&self) -> Result<()> {
        let max_frame = self.max_frame.load(Ordering::Acquire);
        let last_checksum = *self.last_checksum.read();
        tracing::trace!(max_frame, ?last_checksum);
        let transaction_count = self.transaction_count.fetch_add(1, Ordering::AcqRel) + 1;
        self.coordination.publish_commit(WalCommitState {
            max_frame,
            last_checksum,
            transaction_count,
        });
        Ok(())
    }

    fn changed_pages_after(&self, frame_watermark: u64) -> Result<Vec<u32>> {
        let frame_count = self.get_max_frame();
        let page_size = self.page_size();
        let mut frame = vec![0u8; page_size as usize + WAL_FRAME_HEADER_SIZE];
        let mut seen = FxHashSet::default();
        turso_assert!(
            frame_count >= frame_watermark,
            "frame_count must be not less than frame_watermark",
            { "frame_count": frame_count, "frame_watermark": frame_watermark }
        );
        let mut pages = Vec::with_capacity((frame_count - frame_watermark) as usize);
        for frame_no in frame_watermark + 1..=frame_count {
            let c = self.read_frame_raw(frame_no, &mut frame)?;
            self.io.wait_for_completion(c)?;
            let (header, _) = sqlite3_ondisk::parse_wal_frame_header(&frame);
            if seen.insert(header.page_number) {
                pages.push(header.page_number);
            }
        }
        Ok(pages)
    }

    fn prepare_wal_start(&self, page_size: PageSize) -> Result<Option<Completion>> {
        if self.coordination.wal_is_initialized() {
            return Ok(None);
        }
        tracing::debug!("ensure_header_if_needed");
        let Some(header) = self
            .coordination
            .prepare_wal_header(self.io.as_ref(), page_size)
        else {
            return Ok(None);
        };
        *self.last_checksum.write() = (header.checksum_1, header.checksum_2);

        self.max_frame.store(0, Ordering::Release);
        let file = self.coordination.wal_file()?;
        let mut group = CompletionGroup::new(|_| {});
        let _header_c =
            sqlite3_ondisk::begin_write_wal_header(file.as_ref(), &header, Some(&mut group))?;

        // After a RESTART or try_restart_log_before_write the WAL file may
        // still contain orphaned frames from the previous epoch. Truncate
        // them so that classify_authority_snapshot_against_wal does not see a
        // length mismatch and unnecessarily fall back to a full disk scan
        // (which can race with concurrent writers and corrupt the authority).
        let should_skip_truncate = match file.size() {
            Ok(size) => size <= WAL_HEADER_SIZE as u64,
            Err(_) => {
                tracing::warn!("Failed to get WAL file size");
                true
            }
        };
        if !should_skip_truncate {
            let c = Completion::new_trunc(|res| {
                if let Err(err) = res {
                    tracing::warn!("WAL truncate of orphaned frames failed: {err}");
                }
            });
            group.add(&c);
            let _trunc_c = file.truncate(WAL_HEADER_SIZE as u64, c)?;
        }
        Ok(Some(group.build()))
    }

    #[aristo::intent(
        "The WAL initialized flag is set true only after a successful sync of the wal-header\n",
        id = "aristos:wal_initialized_reflects_sync_outcome",
        verify = "full",
        parent = "wal_protocol_correctness"
    )]
    fn prepare_wal_finish(&self, sync_type: FileSyncType) -> Result<Completion> {
        let file = self.coordination.wal_file()?;
        let coordination = self.coordination.clone();
        let c = file.sync(
            Completion::new_sync(move |res| {
                // Only mark the WAL header durable once its sync has actually
                // succeeded. A failed sync must leave the WAL uninitialized so
                // the header is re-issued before the next append, keeping the
                // in-memory initialized state consistent with what is on disk.
                if res.is_ok() {
                    coordination.mark_initialized();
                }
            }),
            sync_type,
        )?;
        Ok(c)
    }

    /// Prepares a batch of dirty pages as WAL frames without modifying WAL state.
    ///
    /// This is the first phase of a three-phase commit protocol:
    /// 1. prepare (`prepare_frames`) - serialize frames, compute checksums
    /// 2. write + fsync - caller submits I/O and waits for durability
    /// 3. commit/finalize (`commit_prepared_frames`) - update WAL index and page metadata
    ///
    /// WAL frames form a checksum chain for corruption detection. When writing
    /// multiple batches in a single transaction, pass the previous batch via `prev`
    /// to continue the chain. For the first batch, pass `None` to start from
    /// the committed WAL state.
    fn prepare_frames(
        &self,
        pages: &[PageRef],
        page_sz: PageSize,
        db_size_on_commit: Option<u32>,
        prev: Option<&PreparedFrames>,
    ) -> Result<PreparedFrames> {
        turso_assert!(
            !pages.is_empty(),
            "prepare_frames requires at least one page"
        );
        turso_assert!(
            pages.len() <= IOV_MAX,
            "supported up to IOV_MAX pages at once"
        );
        turso_assert!(
            self.coordination.wal_is_initialized(),
            "WAL must be initialized"
        );

        let header = self.coordination.wal_header();
        let epoch = self.coordination.checkpoint_epoch();

        turso_assert!(
            header.page_size == page_sz.get(),
            "page size mismatch between header and requested",
            { "header_page_size": header.page_size, "requested_page_size": page_sz.get() }
        );

        // Either chain from previous batch of PreparedFrames or use committed WAL state.
        // For the first batch, also check the authority's max_frame to handle
        // cross-process WAL restarts where our local max_frame is stale.
        let (mut rolling_checksum, mut next_frame_id) = match prev {
            Some(p) => (p.final_checksum, p.final_max_frame + 1),
            None => {
                let snapshot = self.load_coordination_snapshot();
                let local_state = self.connection_state();
                if local_state.snapshot.max_frame > snapshot.max_frame {
                    // The local position is past the committed high-water
                    // mark exactly when this connection has spilled or
                    // raw-inserted frames that carry no commit marker yet.
                    // Chain from local state so we don't overwrite them.
                    (
                        local_state.snapshot.last_checksum,
                        local_state.snapshot.max_frame + 1,
                    )
                } else {
                    // Inside a write transaction the local position can
                    // never be behind the committed mark: the upgrade
                    // requires a fresh snapshot, and the mark cannot advance
                    // while the write lock is held.
                    turso_assert!(
                        local_state.snapshot.max_frame == snapshot.max_frame,
                        "connection WAL position must not be behind the committed high-water mark",
                        {
                            "local_max_frame": local_state.snapshot.max_frame,
                            "authority_max_frame": snapshot.max_frame
                        }
                    );
                    // At the mark the authority owns the seed; re-sync local
                    // state if it drifted (e.g. a savepoint rollback
                    // reinstalled a pre-header checksum at frame 0, or a
                    // concurrent checkpoint advanced nbackfills).
                    if snapshot != local_state.snapshot {
                        self.install_connection_state(local_state.with_snapshot(snapshot));
                    }
                    (snapshot.last_checksum, snapshot.max_frame + 1)
                }
            }
        };

        // The first frame of a generation always chains from the WAL header
        // checksum, like SQLite's walFrames at mxFrame == 0. Connection and
        // authority state may still carry the pre-header checksum here: a
        // restart resets the position before the next append writes the new
        // header, and a savepoint rollback can reinstall a position captured
        // in that window. The wal_is_initialized assert above guarantees
        // `header` is the current generation's synced header.
        if next_frame_id == 1 {
            rolling_checksum = (header.checksum_1, header.checksum_2);
        }

        let first_frame_id = next_frame_id;

        let mut bufs: Vec<Arc<Buffer>> = Vec::with_capacity(pages.len());
        let mut metadata = Vec::with_capacity(pages.len());
        let page_transform = self.io_ctx.read().page_transform().clone();

        for (idx, page) in pages.iter().enumerate() {
            let page_id = page.get().id;
            let plain = page.get_contents().as_ptr();

            // if DB size is included for commit frame, it will need to be included only in the last frame of the batch.
            // however it might not be present in this batch so we cannot assert its presence
            let frame_db_size = if idx + 1 == pages.len() {
                db_size_on_commit.unwrap_or(0)
            } else {
                0
            };
            let page_number = u32::try_from(page_id).map_err(|_| LimboError::IntegerOverflow)?;
            let (checksum, frame_buf) = Self::prepare_transformed_frame(
                &self.buffer_pool,
                &header,
                rolling_checksum,
                page_number,
                frame_db_size,
                plain,
                &page_transform,
            )?;
            bufs.push(frame_buf);
            metadata.push((page.clone(), next_frame_id, checksum));
            rolling_checksum = checksum;
            next_frame_id += 1;
        }
        let offset = self.frame_offset(first_frame_id);
        Ok(PreparedFrames {
            offset,
            bufs,
            metadata,
            final_checksum: rolling_checksum,
            final_max_frame: next_frame_id - 1,
            epoch,
        })
    }

    /// For each prepared frame, update in-memory WAL index and rolling checksum.
    /// and advance max_frame to make frames visible to readers.
    fn commit_prepared_frames(&self, batches: &[PreparedFrames]) {
        for batch in batches {
            for (page, frame_id, checksum) in &batch.metadata {
                // Update WAL index mapping page -> frame
                self.complete_append_frame(page.get().id as u64, *frame_id, *checksum);
            }
            // Update rolling checksum
            *self.last_checksum.write() = batch.final_checksum;
            // Advance max_frame and make frames visible to readers
            self.max_frame
                .store(batch.final_max_frame, Ordering::Release);
        }
    }

    /// Mark pages clean and set WAL tags after durable commit.
    fn finalize_committed_pages(&self, prepared: &[PreparedFrames]) {
        for batch in prepared {
            for (page, frame_id, _) in &batch.metadata {
                page.clear_dirty();
                page.set_wal_tag(*frame_id, batch.epoch);
            }
        }
    }

    /// Get WAL file for durable writes.
    fn wal_file(&self) -> Result<Arc<dyn File>> {
        self.coordination.wal_file()
    }

    /// Use pwritev to append many frames to the log at once.
    ///
    /// # Safety:
    /// this method should only be used for cacheflush/spilling,
    /// the commit path should use prepare_frames + commit_prepared_frames instead,
    /// as it prevents prematurely modifing WAL state before durability is ensured.
    fn append_frames_vectored(&self, pages: Vec<PageRef>, page_sz: PageSize) -> Result<Completion> {
        turso_assert!(
            pages.len() <= IOV_MAX,
            "we limit number of iovecs to IOV_MAX"
        );
        turso_assert!(
            self.coordination.wal_is_initialized(),
            "WAL must be prepared with prepare_wal_start/prepare_wal_finish method"
        );

        let header = self.coordination.wal_header();
        let shared_page_size = header.page_size;
        let epoch = self.coordination.checkpoint_epoch();
        turso_assert!(
            shared_page_size == page_sz.get(),
            "page size mismatch, tried to change page size after WAL header was already initialized",
            { "shared_page_size": shared_page_size, "page_size": page_sz.get() }
        );

        // Prepare write buffers and bookkeeping
        let mut iovecs: Vec<Arc<Buffer>> = Vec::with_capacity(pages.len());
        let mut page_frame_and_checksum: Vec<(PageRef, u64, (u32, u32))> =
            Vec::with_capacity(pages.len());
        let page_transform = self.io_ctx.read().page_transform().clone();

        // Rolling checksum input to each frame build
        let mut next_frame_id = self.max_frame.load(Ordering::Acquire) + 1;
        let mut rolling_checksum = if next_frame_id == 1 {
            (header.checksum_1, header.checksum_2)
        } else {
            *self.last_checksum.read()
        };
        // Build every frame in order, updating the rolling checksum
        for page in pages.iter() {
            tracing::debug!("append_frames_vectored: page_id={}", page.get().id);
            let page_id = page.get().id;
            let plain = page.get_contents().as_ptr();

            let frame_db_size = 0; // this method is not used for the commit path
            let page_number = u32::try_from(page_id).map_err(|_| LimboError::IntegerOverflow)?;
            let (new_checksum, frame_bytes) = Self::prepare_transformed_frame(
                &self.buffer_pool,
                &header,
                rolling_checksum,
                page_number,
                frame_db_size,
                plain,
                &page_transform,
            )?;
            iovecs.push(frame_bytes);

            // (page, assigned_frame_id, cumulative_checksum_at_this_frame)
            page_frame_and_checksum.push((page.clone(), next_frame_id, new_checksum));

            // Advance for the next frame
            rolling_checksum = new_checksum;
            next_frame_id += 1;
        }

        let first_frame_id = self.max_frame.load(Ordering::Acquire) + 1;
        let start_off = self.frame_offset(first_frame_id);

        // single completion for the whole batch
        let total_len: i32 = iovecs.iter().map(|b| b.len() as i32).sum();
        let page_frame_for_cb = page_frame_and_checksum.clone();
        // Make the frames readable only once the write is durable. `find_frame`
        // (reads) and `iter_latest_frames` (checkpoint) resolve a page->frame
        // only through the frame cache, so populating it here — from the write
        // completion callback — is what publishes the frames. Doing it before
        // durability would let a reader or a checkpoint pick up a frame whose
        // bytes are not on disk yet. On write failure `res` is `Err`, so we
        // publish nothing.
        let coordination = self.coordination.clone();
        let on_complete = move |res: Result<i32, CompletionError>| {
            let Ok(bytes_written) = res else {
                return;
            };
            turso_assert!(
                bytes_written == total_len,
                "pwritev wrote unexpected number of bytes",
                { "bytes_written": bytes_written, "expected": total_len }
            );

            for (page, fid, _csum) in &page_frame_for_cb {
                page.set_wal_tag(*fid, epoch);
                coordination.cache_frame(page.get().id as u64, *fid);
            }
        };

        let c = Completion::new_write(on_complete);

        let file = self.coordination.wal_file()?;
        let c = file.pwritev(start_off, iovecs, c)?;

        // Advance the connection-private write cursor (max_frame / rolling
        // checksum / dirty) synchronously so a following batch in the same
        // flush chains onto the correct frame ids and checksum.
        //
        // These are optimistic in-memory bookkeeping fields, not durable state,
        // and they do not make the frame visible (visibility is the frame
        // cache, published from the completion callback above only after the
        // write succeeds). So advancing them before the write lands is safe:
        // if the write fails the transaction unwinds and `rollback()` restores
        // max_frame / last_checksum from the committed watermark and drops
        // cached frames above it; nothing is durable until a commit frame is
        // fsynced, and crash recovery rebuilds max_frame by scanning only
        // committed, checksum-valid frames. `dirty` is conservative — it only
        // forces an fsync before the next commit is reported durable.
        //
        // Must NOT block for durability here: the returned completion is awaited
        // by the caller's state machine (spill: `SpillState::WritingToWal`;
        // cacheflush: the collected completions). A synchronous drain would
        // deadlock a caller that drives I/O from a single-threaded event loop.
        if let Some((_, last_frame_id, last_checksum)) = page_frame_and_checksum.last() {
            self.dirty.store(true, Ordering::Release);
            *self.last_checksum.write() = *last_checksum;
            self.max_frame.store(*last_frame_id, Ordering::Release);
        }

        Ok(c)
    }

    #[cfg(any(test, debug_assertions))]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn set_io_context(&self, ctx: IOContext) {
        *self.io_ctx.write() = ctx;
    }

    fn update_max_frame(&self) {
        let new_max_frame = self.load_coordination_snapshot().max_frame;
        self.max_frame.store(new_max_frame, Ordering::Release);
    }

    fn truncate_wal(
        &self,
        result: &mut CheckpointResult,
        sync_type: FileSyncType,
    ) -> IOResultOr<()> {
        self.truncate_log(result, sync_type)
    }
}

impl WalFile {
    #[cfg(host_shared_wal)]
    pub(crate) fn new_with_shared_coordination(
        io: Arc<dyn IO>,
        shared: Arc<RwLock<WalFileShared>>,
        authority: Arc<MappedSharedWalCoordination>,
        _last_checksum_and_max_frame: ((u32, u32), u64),
        buffer_pool: Arc<BufferPool>,
    ) -> Self {
        let coordination: Arc<dyn WalCoordination> =
            Arc::new(ShmWalCoordination::new(shared, authority));
        let snapshot = coordination.load_snapshot();
        Self::new_with_coordination(
            io,
            coordination,
            (snapshot.last_checksum, snapshot.max_frame),
            buffer_pool,
        )
    }

    pub fn new(
        io: Arc<dyn IO>,
        shared: Arc<RwLock<WalFileShared>>,
        (last_checksum, max_frame): ((u32, u32), u64),
        buffer_pool: Arc<BufferPool>,
    ) -> Self {
        let coordination: Arc<dyn WalCoordination> =
            Arc::new(InProcessWalCoordination::new(shared));
        Self::new_with_coordination(io, coordination, (last_checksum, max_frame), buffer_pool)
    }

    /// Construct a WAL using an explicit coordination backend.
    fn new_with_coordination(
        io: Arc<dyn IO>,
        coordination: Arc<dyn WalCoordination>,
        (last_checksum, max_frame): ((u32, u32), u64),
        buffer_pool: Arc<BufferPool>,
    ) -> Self {
        let now = io.current_time_monotonic();
        Self {
            io,
            coordination,
            // default to max frame in WAL, so that when we read schema we can read from WAL too if it's there.
            max_frame: AtomicU64::new(max_frame),
            ongoing_checkpoint: RwLock::new(OngoingCheckpoint {
                time: now,
                pending_writes: WriteBatch::new(),
                inflight_writes: Vec::new(),
                state: CheckpointState::Start,
                min_frame: 0,
                max_frame: 0,
                current_page: 0,
                pages_to_checkpoint: Vec::new(),
                inflight_reads: Vec::with_capacity(MAX_INFLIGHT_READS),
            }),
            checkpoint_threshold: 1000,
            buffer_pool,
            checkpoint_seq: AtomicU32::new(0),
            syncing: Arc::new(AtomicBool::new(false)),
            write_lock_held: AtomicBool::new(false),
            vacuum_lock_guard: RwLock::new(None),
            min_frame: AtomicU64::new(0),
            transaction_count: AtomicU64::new(0),
            max_frame_read_lock_index: AtomicUsize::new(NO_LOCK_HELD),
            last_checksum: RwLock::new(last_checksum),
            checkpoint_guard: RwLock::new(None),
            io_ctx: RwLock::new(IOContext::default()),
            dirty: Arc::new(AtomicBool::new(false)),
        }
    }

    #[cfg(test)]
    pub(crate) fn shared_ptr(&self) -> usize {
        self.coordination.shared_ptr()
    }

    #[cfg(test)]
    pub(crate) fn coordination_backend_name(&self) -> &'static str {
        self.coordination.backend_name()
    }

    #[cfg(test)]
    pub(crate) fn coordination_open_mode_name(&self) -> Option<&'static str> {
        self.coordination.open_mode_name()
    }

    fn with_shared<F, R>(&self, func: F) -> R
    where
        F: FnOnce(&WalFileShared) -> R,
    {
        let shared = self.coordination.shared_wal_state();
        let guard = shared.read();
        func(&guard)
    }

    fn page_size(&self) -> u32 {
        self.coordination.wal_header().page_size
    }

    fn frame_offset(&self, frame_id: u64) -> u64 {
        turso_assert_greater_than!(frame_id, 0, "Frame ID must be 1-based");
        let page_offset = (frame_id - 1) * (self.page_size() + WAL_FRAME_HEADER_SIZE as u32) as u64;
        WAL_HEADER_SIZE as u64 + page_offset
    }

    fn increment_checkpoint_epoch(&self) {
        let prev = self.coordination.bump_checkpoint_epoch();
        tracing::debug!("increment checkpoint epoch: prev={}", prev);
    }

    fn complete_append_frame(&self, page_id: u64, frame_id: u64, checksums: (u32, u32)) {
        self.dirty.store(true, Ordering::Release);
        *self.last_checksum.write() = checksums;
        self.max_frame.store(frame_id, Ordering::Release);
        self.coordination.cache_frame(page_id, frame_id);
    }

    /// Reset connection-private WAL state.
    fn reset_internal_states(&self) {
        self.ongoing_checkpoint.write().reset();
        self.syncing.store(false, Ordering::Release);
    }

    /// the WAL file has been truncated and we are writing the first
    /// frame since then. We need to ensure that the header is initialized.
    fn ensure_header_if_needed(&self, page_size: PageSize, sync_type: FileSyncType) -> Result<()> {
        let Some(c) = self.prepare_wal_start(page_size)? else {
            return Ok(());
        };
        self.io.wait_for_completion(c)?;
        let c = self.prepare_wal_finish(sync_type)?;
        self.io.wait_for_completion(c)?;
        Ok(())
    }

    #[aristo::intent("Checkpoint backfill copies a log frame into the main database file only after that frame is durable in the log, so a crash can never recover a database torn between persisted backfill pages and dropped log frames", id = "aristos:wal_checkpoint_backfill_crash_atomic", verify = "full", parent = "wal_protocol_correctness")]
    fn checkpoint_inner(
        &self,
        pager: &Pager,
        mode: CheckpointMode,
        lock_source: CheckpointLockSource,
        sync_mode: SyncMode,
    ) -> IOResultOr<CheckpointResult> {
        loop {
            let state = self.ongoing_checkpoint.read().state.clone();
            tracing::debug!(?state);
            match state {
                // Acquire the relevant exclusive locks and checkpoint_lock
                // so no other checkpointer can run. fsync WAL if there are unapplied frames.
                // Decide the largest frame we are allowed to back‑fill.
                CheckpointState::Start => {
                    let snapshot = self.load_coordination_snapshot();
                    let max_frame = snapshot.max_frame;
                    let nbackfills = snapshot.nbackfills;
                    tracing::debug!("shared_wal: max_frame={max_frame}, nbackfills={nbackfills}");
                    let needs_backfill = max_frame > nbackfills;
                    if matches!(lock_source, CheckpointLockSource::HeldByCaller) {
                        turso_assert!(
                            needs_backfill,
                            "held checkpoint-lock path requires WAL frames to backfill",
                            { "max_frame": max_frame, "nbackfills": nbackfills }
                        );
                    }
                    if !needs_backfill && !mode.should_restart_log() {
                        // there are no frames to copy over and we don't need to reset
                        // the log so we can return early success.
                        return Ok(IOResult::Done(CheckpointResult::new(
                            max_frame, nbackfills, 0,
                        )));
                    }
                    // acquire the appropriate exclusive locks depending on the checkpoint mode
                    self.acquire_proper_checkpoint_guard(mode, lock_source)?;
                    let mut max_frame = self.determine_max_safe_checkpoint_frame();

                    if let CheckpointMode::Truncate {
                        upper_bound_inclusive: Some(upper_bound),
                    } = mode
                    {
                        if max_frame > upper_bound {
                            tracing::debug!(
                                "abort checkpoint because latest frame in WAL is greater than upper_bound in TRUNCATE mode: {max_frame} != {upper_bound}"
                            );
                            return Err(LimboError::Busy.into());
                        }
                    }
                    if let CheckpointMode::Passive {
                        upper_bound_inclusive: Some(upper_bound),
                    } = mode
                    {
                        max_frame = max_frame.min(upper_bound);
                    }

                    {
                        let mut oc = self.ongoing_checkpoint.write();
                        oc.max_frame = max_frame;
                        oc.min_frame = nbackfills + 1;
                    }
                    let (oc_min_frame, oc_max_frame) = {
                        let oc = self.ongoing_checkpoint.read();
                        (oc.min_frame, oc.max_frame)
                    };
                    self.coordination.ensure_local_frame_cache_covers(
                        &self.io,
                        WalSnapshot {
                            max_frame: oc_max_frame,
                            ..self.load_coordination_snapshot()
                        },
                    )?;
                    tracing::debug!(
                        "checkpoint_inner::Start: min_frame={oc_min_frame}, max_frame={oc_max_frame}"
                    );
                    let mut to_checkpoint = self
                        .coordination
                        .iter_latest_frames(oc_min_frame, oc_max_frame);
                    // sort by frame_id for read locality
                    to_checkpoint.sort_unstable_by(|a, b| (a.1, a.0).cmp(&(b.1, b.0)));
                    // Every frame we are about to backfill must be durable in
                    // the WAL before it may be copied into the database file:
                    // commits under synchronous=NORMAL do not fsync the WAL,
                    // so the checkpoint owes that fsync itself. The barrier is
                    // issued after the frame range is fixed (under the
                    // checkpoint locks), so it covers exactly the frames that
                    // will be backfilled. Skipped only under synchronous=OFF,
                    // which forgoes crash durability entirely.
                    let needs_wal_sync = !to_checkpoint.is_empty() && sync_mode != SyncMode::Off;
                    {
                        let mut oc = self.ongoing_checkpoint.write();
                        oc.pages_to_checkpoint = to_checkpoint;
                        oc.current_page = 0;
                        oc.inflight_writes.clear();
                        oc.inflight_reads.clear();
                        oc.state = if needs_wal_sync {
                            CheckpointState::SyncWal
                        } else {
                            CheckpointState::Processing
                        };
                        oc.time = self.io.current_time_monotonic();
                    }
                    tracing::trace!(
                        "checkpoint_start(min_frame={}, max_frame={})",
                        oc_min_frame,
                        oc_max_frame,
                    );
                }
                // Durability barrier: fsync the WAL so every frame selected
                // for backfill is on stable storage before any of them is
                // copied into the database file. Without it, a crash during
                // the backfill could persist some DB pages while recovery
                // drops the unsynced WAL tail — a torn database that matches
                // no committed prefix.
                CheckpointState::SyncWal => {
                    let c = self.sync(pager.get_sync_type())?;
                    self.ongoing_checkpoint.write().state = CheckpointState::Processing;
                    io_yield_one!(c);
                }
                // For locality, reading is ordered by frame ID, and writing ordered by page ID.
                // the more consecutive page ID's that we submit together, the fewer overall
                // write/writev syscalls made. All I/O during checkpointing is now in a single step
                // to prevent serialization, and we try to issue reads and flush batches concurrently
                // if at all possible, at the cost of some batching potential.
                CheckpointState::Processing => {
                    // Gather I/O completions using a completion group
                    let mut nr_completions = 0;
                    let mut group = CompletionGroup::new(|_| {});
                    let mut ongoing_chkpt = self.ongoing_checkpoint.write();

                    // Check and clean any completed writes from pending flush
                    if ongoing_chkpt.process_inflight_writes() {
                        tracing::trace!("Completed a write batch");
                    }
                    // Process completed reads into current batch
                    if ongoing_chkpt.process_pending_reads()? {
                        tracing::trace!("Drained reads into batch");
                    }
                    if let Some(e) = ongoing_chkpt.first_write_error() {
                        mark_unlikely();
                        // cancel everything still in-flight to avoid leaks
                        let to_cancel: Vec<Completion> = ongoing_chkpt
                            .inflight_reads
                            .iter()
                            .map(|r| r.completion.clone())
                            .collect();
                        pager.io.cancel(&to_cancel)?;
                        pager.io.drain_completions(&to_cancel)?;
                        return Err(LimboError::CompletionError(e).into());
                    }
                    let epoch = self.coordination.checkpoint_epoch();
                    // Issue reads until we hit limits
                    'inner: while ongoing_chkpt.should_issue_reads() {
                        let (page_id, target_frame) = {
                            ongoing_chkpt.pages_to_checkpoint[ongoing_chkpt.current_page as usize]
                        };
                        if let Some(cached_page) =
                            pager.cache_get_for_checkpoint(page_id as usize, target_frame, epoch)?
                        {
                            let buffer = cached_page
                                .get_contents()
                                .buffer()
                                .expect("buffer missing")
                                .clone();
                            {
                                ongoing_chkpt
                                    .pending_writes
                                    .insert(page_id as usize, buffer);
                                // signify that a cached page was used, so it can be unpinned
                                let current = ongoing_chkpt.current_page as usize;
                                ongoing_chkpt.pages_to_checkpoint[current] =
                                    (page_id, target_frame);
                                ongoing_chkpt.current_page += 1;
                            }
                            continue 'inner;
                        }
                        // Issue read if page wasn't found in the page cache or doesnt meet
                        // the frame requirements
                        let inflight = self.issue_wal_read_into_buffer(
                            page_id as usize,
                            target_frame,
                            &mut group,
                        )?;
                        nr_completions += 1;
                        ongoing_chkpt.inflight_reads.push(inflight);
                        ongoing_chkpt.current_page += 1;
                    }

                    // Start a write if batch is ready and we're not at write limit
                    let should_flush = ongoing_chkpt.inflight_writes.len() < MAX_INFLIGHT_WRITES
                        && ongoing_chkpt.should_flush_batch();
                    if should_flush {
                        let batch_map = ongoing_chkpt.pending_writes.take();
                        if !batch_map.is_empty() {
                            let new_write = InflightWriteBatch::new();
                            nr_completions += write_pages_vectored(
                                pager,
                                batch_map,
                                new_write.done.clone(),
                                new_write.err.clone(),
                                &mut group,
                            )?;
                            ongoing_chkpt.inflight_writes.push(new_write);
                        }
                    }
                    if nr_completions > 0 {
                        io_yield_one!(group.build());
                    } else if ongoing_chkpt.complete() {
                        ongoing_chkpt.state = CheckpointState::DetermineResult;
                    } else {
                        // This should be impossible now so we treat it as logic error.
                        mark_unlikely();
                        return Err(LimboError::InternalError(
                            "checkpoint stuck: no inflight completions but not complete".into(),
                        )
                        .into());
                    }
                }
                // All eligible frames copied to the db file.
                // Compute checkpoint result, update nBackfills, restart log if needed.
                CheckpointState::DetermineResult => {
                    let mut ongoing_chkpt = self.ongoing_checkpoint.write();
                    turso_assert!(
                        ongoing_chkpt.complete(),
                        "checkpoint pending flush must have finished"
                    );
                    let wal_max_frame = self.load_coordination_snapshot().max_frame;
                    let wal_total_backfilled = ongoing_chkpt.max_frame;
                    // Record two num pages fields to return as checkpoint result to caller.
                    // Ref: pnLog, pnCkpt on https://www.sqlite.org/c3ref/wal_checkpoint_v2.html

                    // the total # of frames we actually backfilled
                    let wal_checkpoint_backfilled =
                        wal_total_backfilled.saturating_sub(ongoing_chkpt.min_frame - 1);

                    let checkpoint_result = CheckpointResult::new(
                        wal_max_frame,
                        wal_total_backfilled,
                        wal_checkpoint_backfilled,
                    );
                    tracing::debug!("checkpoint_result={:?}, mode={:?}", checkpoint_result, mode);
                    if mode.require_all_backfilled() && !checkpoint_result.everything_backfilled() {
                        return Err(LimboError::Busy.into());
                    }
                    if mode.should_restart_log() {
                        turso_assert!(
                            matches!(
                                *self.checkpoint_guard.read(),
                                Some(CheckpointLocks::Writer { .. })
                            ),
                            "We must hold writer and checkpoint locks to restart the log",
                            { "checkpoint_guard": *self.checkpoint_guard.read() }
                        );
                        self.restart_log()?;
                    }
                    ongoing_chkpt.state = CheckpointState::Finalize {
                        checkpoint_result: Some(checkpoint_result),
                    };
                }
                CheckpointState::Finalize { .. } => {
                    // NOTE: For TRUNCATE mode, WAL truncation is NOT done here.
                    // It is deferred to pager.rs after the DB file has been synced,
                    // at which point it calls truncate_wal().
                    // This ensures data durability: if a crash occurs after WAL truncation
                    // but before DB sync, the data would be lost. By truncating the WAL
                    // only after the DB is safely synced, we guarantee recoverability.
                    if mode.should_restart_log() {
                        Self::unlock_after_restart(&self.coordination, None);
                    }
                    let mut checkpoint_result = {
                        let mut oc = self.ongoing_checkpoint.write();
                        let CheckpointState::Finalize {
                            checkpoint_result, ..
                        } = &mut oc.state
                        else {
                            panic!("unexpected state");
                        };
                        checkpoint_result.take().unwrap()
                    };
                    // increment wal epoch to ensure no stale pages are used for backfilling
                    self.increment_checkpoint_epoch();

                    tracing::debug!("checkpoint_result={:?}", checkpoint_result);
                    // we cannot truncate the db file here because we are currently inside a
                    // mut borrow of pager.wal, and accessing the header will attempt a borrow
                    // during 'read_page', so the caller will use the result to determine if:
                    // a. the max frame == num wal frames (everything backfilled)
                    // b. the max frame > 0 (we have something to truncate)
                    if checkpoint_result.should_truncate()
                        || checkpoint_result.wal_checkpoint_backfilled > 0
                    {
                        // Backfilled frames are not globally durable until
                        // the pager syncs the DB file and publishes
                        // nbackfills. Keep the checkpoint guard through that
                        // tail so another writer cannot restart the WAL
                        // generation underneath a pending publish.
                        checkpoint_result.maybe_guard = self.checkpoint_guard.write().take();
                    } else {
                        let _ = self.checkpoint_guard.write().take();
                    }
                    {
                        let mut oc = self.ongoing_checkpoint.write();
                        oc.inflight_writes.clear();
                        oc.pending_writes.clear();
                        oc.pages_to_checkpoint.clear();
                        oc.current_page = 0;
                    }
                    let oc_time = self.ongoing_checkpoint.read().time;
                    tracing::debug!(
                        "total time spent checkpointing: {:?}",
                        self.io
                            .current_time_monotonic()
                            .duration_since(oc_time)
                            .as_millis()
                    );
                    self.ongoing_checkpoint.write().state = CheckpointState::Start;
                    return Ok(IOResult::Done(checkpoint_result));
                }
            }
        }
    }

    /// Coordinate what the maximum safe frame is for us to backfill when checkpointing.
    /// We can never backfill a frame with a higher number than any reader's read mark,
    /// because we might overwrite content the reader is reading from the database file.
    ///
    /// A checkpoint must never overwrite a page in the main DB file if some
    /// active reader might still need to read that page from the WAL.
    /// Concretely: the checkpoint may only copy frames `<= aReadMark[k]` for
    /// every in-use reader slot `k > 0`.
    ///
    /// `read_locks[0]` is special: readers holding slot 0 ignore the WAL entirely
    /// (they read only the DB file). Its value is a placeholder and does not
    /// constrain `mxSafeFrame`.
    ///
    /// For each slot 1..N:
    /// - If we can acquire the write lock (slot is free):
    ///   - Slot 1: Set to mxSafeFrame (allowing new readers to see up to this point)
    ///   - Slots 2+: Set to READMARK_NOT_USED (freeing the slot)
    /// - If we cannot acquire the lock (SQLITE_BUSY):
    ///   - Lower mxSafeFrame to that reader's mark
    ///   - In PASSIVE mode: Already have no busy handler, continue scanning
    ///   - In FULL/RESTART/TRUNCATE: Disable busy handler for remaining slots
    ///
    /// Locking behavior:
    /// - PASSIVE: Never waits, no busy handler (xBusy==NULL)
    /// - FULL/RESTART/TRUNCATE: May wait via busy handler, but after first BUSY,
    ///   switches to non-blocking for remaining slots
    ///
    /// We never modify slot values while a reader holds that slot's lock.
    /// TOOD: implement proper BUSY handling behavior
    fn determine_max_safe_checkpoint_frame(&self) -> u64 {
        self.coordination
            .determine_max_safe_checkpoint_frame(self.load_coordination_snapshot().max_frame)
    }

    /// attempt to restart WAL header before write in order to keep WAL file size under the control
    /// The conditions for WAL restart are following:
    /// 1. we can do that only under write transaction
    /// 2. max_frame_read_lock_index == 0 - this means that transaction was initiated to read data from DB file
    /// 3. nbackfills > 0 - otherwise nothing was backfilled and there is no reason to truncate header
    /// 4. max_frame == nbackfills - otherwise there are some non-checkpointed frames in the WAL and we can't truncate the log
    pub fn try_restart_log_before_write(&self) -> Result<()> {
        let max_frame_read_lock_index = self.max_frame_read_lock_index.load(Ordering::Acquire);
        if max_frame_read_lock_index != 0 {
            tracing::debug!(
                "try_restart_log_before_write: max_frame_read_lock_index={max_frame_read_lock_index}, writer use WAL - can't restart the log"
            );
            return Ok(());
        }
        let snapshot = self.load_coordination_snapshot();
        let max_frame = snapshot.max_frame;
        let nbackfills = snapshot.nbackfills;
        if nbackfills == 0 {
            tracing::debug!(
                "try_restart_log_before_write: nbackfills={nbackfills}, nothing were backfilled - can't restart the log"
            );
            return Ok(());
        }
        turso_assert!(
            max_frame >= nbackfills,
            "backfills can't be more than max_frame"
        );
        if max_frame != nbackfills {
            tracing::debug!(
                "try_restart_log_before_write: max_frame={max_frame}, nbackfills={nbackfills}, not everything is backfilled to the DB file - can't restart the log"
            );
            return Ok(());
        }
        let Some(snapshot) = self
            .coordination
            .try_restart_log_for_write(self.io.as_ref())?
        else {
            return Ok(());
        };
        self.apply_restart_snapshot(snapshot);
        self.increment_checkpoint_epoch();
        let result = Ok(());
        tracing::debug!("try_restart_log_before_write: result={:?}", result);
        result
    }

    fn restart_log(&self) -> Result<()> {
        tracing::debug!("restart_log");
        let snapshot = self.coordination.begin_restart(self.io.as_ref())?;
        self.apply_restart_snapshot(snapshot);
        Ok(())
    }

    /// Truncate WAL file to zero and sync it. Called by pager AFTER DB file is synced.
    #[aristo::intent("WAL truncate is atomic: no committed frame can be observed lost across the truncate operation\n", id = "aristos:wal_truncate_atomic_under_concurrent_writers", verify = "full", parent = "wal_protocol_correctness")]
    fn truncate_log(
        &self,
        result: &mut CheckpointResult,
        sync_type: FileSyncType,
    ) -> IOResultOr<()> {
        let file = self.coordination.prepare_truncate()?;

        if !result.wal_truncate_sent {
            let c = Completion::new_trunc({
                move |res| {
                    if let Err(err) = res {
                        tracing::debug!("WAL truncate failed: {err}")
                    } else {
                        tracing::trace!("WAL file truncated to 0 B");
                    }
                }
            });
            let c = file.truncate(0, c)?;
            result.wal_truncate_sent = true;
            // after truncation - there will be nothing in the WAL
            result.wal_max_frame = 0;
            result.wal_total_backfilled = 0;
            io_yield_one!(c);
        } else if !result.wal_sync_sent {
            let c = file.sync(
                Completion::new_sync(move |res| {
                    if let Err(err) = res {
                        tracing::debug!("WAL sync failed: {err}")
                    } else {
                        tracing::trace!("WAL file synced after truncation");
                    }
                }),
                sync_type,
            )?;
            result.wal_sync_sent = true;
            io_yield_one!(c);
        }
        Ok(IOResult::Done(()))
    }

    fn apply_restart_snapshot(&self, snapshot: WalSnapshot) {
        *self.last_checksum.write() = snapshot.last_checksum;
        self.max_frame.store(snapshot.max_frame, Ordering::Release);
        self.min_frame.store(0, Ordering::Release);
        self.checkpoint_seq
            .store(snapshot.checkpoint_seq, Ordering::Release);
    }

    // unlock shared read locks taken by RESTART/TRUNCATE checkpoint modes
    fn unlock_after_restart(coordination: &Arc<dyn WalCoordination>, e: Option<&LimboError>) {
        coordination.end_restart();
        if let Some(e) = e {
            mark_unlikely();
            tracing::debug!(
                "Failed to restart WAL header: {:?}, releasing read locks",
                e
            );
        }
    }

    fn acquire_proper_checkpoint_guard(
        &self,
        mode: CheckpointMode,
        lock_source: CheckpointLockSource,
    ) -> Result<()> {
        let needs_new_guard = {
            let guard = self.checkpoint_guard.read();
            !matches!(
                (&*guard, mode),
                (
                    Some(CheckpointLocks::Read0 { .. }),
                    CheckpointMode::Passive { .. },
                ) | (
                    Some(CheckpointLocks::Writer { .. }),
                    CheckpointMode::Restart | CheckpointMode::Truncate { .. },
                ),
            )
        };
        if needs_new_guard {
            // Drop any existing guard
            if self.checkpoint_guard.read().is_some() {
                let _ = self.checkpoint_guard.write().take();
            }
            let guard = match lock_source {
                CheckpointLockSource::Acquire => {
                    CheckpointLocks::new(self.coordination.clone(), mode)?
                }
                CheckpointLockSource::HeldByCaller => {
                    CheckpointLocks::from_held_vacuum_checkpoint_lock(self.coordination.clone())?
                }
            };
            *self.checkpoint_guard.write() = Some(guard);
        }
        Ok(())
    }

    /// Starts reading a frame's page body for the checkpoint. The read is
    /// added to `group` before it is submitted.
    fn issue_wal_read_into_buffer(
        &self,
        page_id: usize,
        frame_id: u64,
        group: &mut CompletionGroup,
    ) -> Result<InflightRead> {
        let offset = self.frame_offset(frame_id);
        let buf_slot = Arc::new(SpinLock::new(None));
        tracing::debug!(
            "Issuing WAL read: page_id={}, frame_id={}, offset={}",
            page_id,
            frame_id,
            offset
        );

        let complete = {
            let buf_slot = buf_slot.clone();
            Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                let Ok((buf, read)) = res else {
                    return None;
                };
                let buf_len = buf.len();
                turso_assert!(
                    read == buf_len as i32,
                    "read bytes does not match expected buffer length",
                    { "read": read, "expected": buf_len, "frame_id": frame_id }
                );
                *buf_slot.lock() = Some(buf);
                None
            })
        };
        // schedule read of the page payload
        let file = self.coordination.wal_file()?;
        let c = begin_read_wal_frame(
            file.as_ref(),
            offset + WAL_FRAME_HEADER_SIZE as u64,
            self.buffer_pool.clone(),
            complete,
            page_id,
            &self.io_ctx.read(),
            Some(group),
        )?;

        Ok(InflightRead {
            completion: c,
            page_id,
            buf: buf_slot,
        })
    }

    /// MVCC helper: check if WAL state changed and refresh local snapshot without starting a read tx.
    /// FIXME: this isn't TOCTOU safe because we're not taking WAL read locks.
    ///
    /// No-op while this connection holds a read guard: an active read tx pinned the
    /// connection's WAL view, and an MVCC transaction's `read_mark` was captured from it.
    /// Advancing `max_frame` here would let the transaction's B-tree reads see pages a
    /// passive checkpoint materialized after its snapshot, leaking later commits into it.
    /// The guard also keeps everything at-or-below the pinned view immutable, so the page
    /// cache stays valid and needs no invalidation.
    pub fn mvcc_refresh_if_db_changed(&self) -> bool {
        if self.max_frame_read_lock_index.load(Ordering::Acquire) != NO_LOCK_HELD {
            return false;
        }
        let snapshot = self.load_coordination_snapshot();
        let local_state = self.connection_state();
        let changed = self.db_changed_against(snapshot, local_state);
        if changed {
            self.install_connection_state(local_state.with_snapshot(snapshot));
        }
        changed
    }
}

#[cfg(host_shared_wal)]
fn read_exact_bytes_from_file(
    io: &Arc<dyn IO>,
    file: &Arc<dyn File>,
    offset: u64,
    len: usize,
) -> Result<Option<Vec<u8>>> {
    let read_buf = Arc::new(Buffer::new_temporary(len));
    let bytes_read = Arc::new(AtomicUsize::new(usize::MAX));
    let c = file.pread(
        offset,
        Completion::new_read(read_buf.clone(), {
            let bytes_read = bytes_read.clone();
            Box::new(move |res| {
                if let Ok((_buf, count)) = res {
                    bytes_read.store(count as usize, Ordering::Release);
                }
                None
            })
        }),
    )?;
    io.wait_for_completion(c)?;
    if bytes_read.load(Ordering::Acquire) != len {
        return Ok(None);
    }
    Ok(Some(read_buf.as_slice()[..len].to_vec()))
}

#[cfg(host_shared_wal)]
fn read_validated_wal_header_from_file(
    io: &Arc<dyn IO>,
    file: &Arc<dyn File>,
) -> Result<Option<WalHeader>> {
    let Some(bytes) = read_exact_bytes_from_file(io, file, 0, WAL_HEADER_SIZE)? else {
        return Ok(None);
    };
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
    if !matches!(header.magic, WAL_MAGIC_LE | WAL_MAGIC_BE) {
        return Ok(None);
    }
    if PageSize::new(header.page_size).is_none() {
        return Ok(None);
    }
    let use_native_endian = cfg!(target_endian = "big") == ((header.magic & 1) != 0);
    let calc = checksum_wal(
        &bytes[..WAL_HEADER_SIZE - 8],
        &header,
        (0, 0),
        use_native_endian,
    );
    if calc != (header.checksum_1, header.checksum_2) {
        return Ok(None);
    }
    Ok(Some(header))
}

#[cfg(host_shared_wal)]
fn wal_header_matches_authority_snapshot(
    wal_header: WalHeader,
    snapshot: SharedWalCoordinationHeader,
) -> bool {
    wal_header.page_size == snapshot.page_size
        && wal_header.checkpoint_seq == snapshot.checkpoint_seq
        && wal_header.salt_1 == snapshot.salt_1
        && wal_header.salt_2 == snapshot.salt_2
}

pub(crate) fn database_identity_from_header_bytes(header_bytes: &[u8]) -> Result<(u32, u32)> {
    if header_bytes.len() < DatabaseHeader::SIZE {
        return Err(LimboError::Corrupt(format!(
            "database header must be at least {} bytes, got {}",
            DatabaseHeader::SIZE,
            header_bytes.len()
        )));
    }
    if header_bytes[0..16] != *b"SQLite format 3\0" {
        return Err(LimboError::Corrupt("database header magic mismatch".into()));
    }
    let db_size_pages = u32::from_be_bytes(header_bytes[28..32].try_into().unwrap());
    let header_crc32c = crc32c::crc32c(&header_bytes[..DatabaseHeader::SIZE]);
    Ok((db_size_pages, header_crc32c))
}

fn read_database_identity_from_storage(
    io: &Arc<dyn IO>,
    db_file: &Arc<dyn DatabaseStorage>,
) -> Result<Option<(u32, u32)>> {
    let read_buf = Arc::new(Buffer::new_temporary(PageSize::MIN as usize));
    let bytes_read = Arc::new(AtomicUsize::new(usize::MAX));
    let c = db_file.read_header(Completion::new_read(read_buf.clone(), {
        let bytes_read = bytes_read.clone();
        Box::new(move |res| {
            if let Ok((_buf, count)) = res {
                bytes_read.store(count as usize, Ordering::Release);
            }
            None
        })
    }))?;
    io.wait_for_completion(c)?;
    if bytes_read.load(Ordering::Acquire) < DatabaseHeader::SIZE {
        return Ok(None);
    }
    Ok(Some(database_identity_from_header_bytes(
        &read_buf.as_slice()[..DatabaseHeader::SIZE],
    )?))
}

#[cfg(all(test, host_shared_wal))]
fn read_database_identity_from_file_path(
    io: &Arc<dyn IO>,
    wal_path: &str,
) -> Result<Option<(u32, u32)>> {
    let db_path = wal_path
        .strip_suffix("-wal")
        .unwrap_or(wal_path)
        .to_string();
    let file = match io.open_file(&db_path, crate::OpenFlags::None, false) {
        Ok(file) => file,
        Err(LimboError::CompletionError(CompletionError::IOError(
            std::io::ErrorKind::NotFound,
            _,
        ))) => return Ok(None),
        Err(err) => return Err(err),
    };
    let Some(bytes) = read_exact_bytes_from_file(io, &file, 0, DatabaseHeader::SIZE)? else {
        return Ok(None);
    };
    Ok(Some(database_identity_from_header_bytes(&bytes)?))
}

#[cfg(host_shared_wal)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthoritySnapshotValidation {
    Trusted,
    RebuildFromDisk(AuthoritySnapshotRebuildReason),
}

#[cfg(host_shared_wal)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthoritySnapshotRebuildReason {
    WalHeaderUnreadable,
    WalHeaderMismatch,
    WalTooShortForSnapshot,
    WalLengthMismatch,
    LastFrameMissing,
    LastFrameNotCommit,
    LastFrameSaltMismatch,
    LastFrameChecksumMismatch,
}

#[cfg(host_shared_wal)]
fn classify_authority_snapshot_against_wal(
    io: &Arc<dyn IO>,
    file: &Arc<dyn File>,
    snapshot: SharedWalCoordinationHeader,
) -> Result<AuthoritySnapshotValidation> {
    let wal_size = file.size()?;
    if snapshot.max_frame == 0 {
        if wal_size == 0 {
            return Ok(AuthoritySnapshotValidation::Trusted);
        }
        if wal_size == WAL_HEADER_SIZE as u64 {
            let Some(wal_header) = read_validated_wal_header_from_file(io, file)? else {
                return Ok(AuthoritySnapshotValidation::RebuildFromDisk(
                    AuthoritySnapshotRebuildReason::WalHeaderUnreadable,
                ));
            };
            return Ok(
                if wal_header_matches_authority_snapshot(wal_header, snapshot) {
                    AuthoritySnapshotValidation::Trusted
                } else {
                    AuthoritySnapshotValidation::RebuildFromDisk(
                        AuthoritySnapshotRebuildReason::WalHeaderMismatch,
                    )
                },
            );
        }
        return Ok(AuthoritySnapshotValidation::RebuildFromDisk(
            AuthoritySnapshotRebuildReason::WalLengthMismatch,
        ));
    }

    if wal_size < WAL_HEADER_SIZE as u64 {
        return Ok(AuthoritySnapshotValidation::RebuildFromDisk(
            AuthoritySnapshotRebuildReason::WalTooShortForSnapshot,
        ));
    }

    let Some(wal_header) = read_validated_wal_header_from_file(io, file)? else {
        return Ok(AuthoritySnapshotValidation::RebuildFromDisk(
            AuthoritySnapshotRebuildReason::WalHeaderUnreadable,
        ));
    };
    if !wal_header_matches_authority_snapshot(wal_header, snapshot) {
        return Ok(AuthoritySnapshotValidation::RebuildFromDisk(
            AuthoritySnapshotRebuildReason::WalHeaderMismatch,
        ));
    }

    let frame_size = WAL_FRAME_HEADER_SIZE as u64 + wal_header.page_size as u64;
    let expected_wal_len = WAL_HEADER_SIZE as u64 + snapshot.max_frame * frame_size;
    if wal_size != expected_wal_len {
        return Ok(AuthoritySnapshotValidation::RebuildFromDisk(
            AuthoritySnapshotRebuildReason::WalLengthMismatch,
        ));
    }

    let last_frame_offset = WAL_HEADER_SIZE as u64 + (snapshot.max_frame - 1) * frame_size;
    let Some(frame_bytes) =
        read_exact_bytes_from_file(io, file, last_frame_offset, frame_size as usize)?
    else {
        return Ok(AuthoritySnapshotValidation::RebuildFromDisk(
            AuthoritySnapshotRebuildReason::LastFrameMissing,
        ));
    };
    let (frame_header, _) = sqlite3_ondisk::parse_wal_frame_header(&frame_bytes);
    if !frame_header.is_commit_frame() {
        return Ok(AuthoritySnapshotValidation::RebuildFromDisk(
            AuthoritySnapshotRebuildReason::LastFrameNotCommit,
        ));
    }
    if frame_header.salt_1 != snapshot.salt_1 || frame_header.salt_2 != snapshot.salt_2 {
        return Ok(AuthoritySnapshotValidation::RebuildFromDisk(
            AuthoritySnapshotRebuildReason::LastFrameSaltMismatch,
        ));
    }
    if frame_header.checksum_1 != snapshot.checksum_1
        || frame_header.checksum_2 != snapshot.checksum_2
    {
        return Ok(AuthoritySnapshotValidation::RebuildFromDisk(
            AuthoritySnapshotRebuildReason::LastFrameChecksumMismatch,
        ));
    }
    Ok(AuthoritySnapshotValidation::Trusted)
}

impl WalFileShared {
    pub fn last_checksum_and_max_frame(&self) -> ((u32, u32), u64) {
        (
            self.metadata.last_checksum,
            self.metadata.max_frame.load(Ordering::Acquire),
        )
    }

    #[cfg(host_shared_wal)]
    pub(crate) fn open_shared_from_authority_if_exists(
        io: &Arc<dyn IO>,
        path: &str,
        flags: crate::OpenFlags,
        authority: &Arc<MappedSharedWalCoordination>,
        db_file: &Arc<dyn DatabaseStorage>,
    ) -> Result<Arc<RwLock<WalFileShared>>> {
        let snapshot = authority.snapshot();
        let file = match io.open_file(path, flags, false) {
            Ok(file) => file,
            Err(LimboError::CompletionError(CompletionError::IOError(
                std::io::ErrorKind::NotFound,
                _,
            ))) if flags.contains(crate::OpenFlags::ReadOnly) => {
                return Ok(WalFileShared::new_noop());
            }
            Err(e) => return Err(e),
        };
        let wal_size = file.size()?;

        match classify_authority_snapshot_against_wal(io, &file, snapshot)? {
            AuthoritySnapshotValidation::Trusted => {}
            AuthoritySnapshotValidation::RebuildFromDisk(reason) => {
                tracing::debug!(
                    ?reason,
                    "rebuilding WAL state from disk because persisted authority is not provably reusable"
                );
                return sqlite3_ondisk::build_shared_wal(&file, io);
            }
        }
        if authority.frame_index_overflowed() {
            tracing::debug!(
                "rebuilding WAL state from disk because the persisted tshm frame index is marked overflowed"
            );
            return sqlite3_ondisk::build_shared_wal(&file, io);
        }
        if snapshot.nbackfills != 0
            && authority.open_mode() == SharedWalCoordinationOpenMode::Exclusive
        {
            tracing::debug!(
                nbackfills = snapshot.nbackfills,
                max_frame = snapshot.max_frame,
                "rebuilding WAL state from disk because an exclusive reopen must conservatively clear published backfill progress"
            );
            return sqlite3_ondisk::build_shared_wal(&file, io);
        }
        if snapshot.max_frame > snapshot.nbackfills
            && authority
                .iter_latest_frames(0, snapshot.max_frame)
                .is_empty()
        {
            tracing::debug!(
                max_frame = snapshot.max_frame,
                nbackfills = snapshot.nbackfills,
                "rebuilding WAL state from disk because the persisted tshm frame index has no entries for a visible WAL tail"
            );
            return sqlite3_ondisk::build_shared_wal(&file, io);
        }
        if snapshot.nbackfills != 0 {
            let Some((db_size_pages, db_header_crc32c)) =
                read_database_identity_from_storage(io, db_file)?
            else {
                tracing::debug!(
                    nbackfills = snapshot.nbackfills,
                    "rebuilding WAL state from disk because the main database header is unavailable for backfill-proof validation"
                );
                return sqlite3_ondisk::build_shared_wal(&file, io);
            };
            if !authority.validate_backfill_proof(snapshot, db_size_pages, db_header_crc32c) {
                tracing::debug!(
                    nbackfills = snapshot.nbackfills,
                    "rebuilding WAL state from disk because persisted tshm backfill proof is not valid for the current database header"
                );
                return sqlite3_ondisk::build_shared_wal(&file, io);
            }
        }
        let wal_is_initialized = wal_size >= WAL_HEADER_SIZE as u64;

        let wal_header = WalHeader {
            page_size: snapshot.page_size,
            checkpoint_seq: snapshot.checkpoint_seq,
            salt_1: snapshot.salt_1,
            salt_2: snapshot.salt_2,
            checksum_1: snapshot.checksum_1,
            checksum_2: snapshot.checksum_2,
            ..WalHeader::new()
        };
        let read_locks = array::from_fn(|_| TursoRwLock::new());
        for (i, lock) in read_locks.iter().enumerate() {
            lock.write();
            lock.set_value_exclusive(if i < 2 { 0 } else { READMARK_NOT_USED });
            lock.unlock();
        }

        let shared = WalFileShared {
            metadata: WalSharedMetadata {
                enabled: AtomicBool::new(true),
                wal_header: Arc::new(SpinLock::new(wal_header)),
                min_frame: AtomicU64::new(0),
                max_frame: AtomicU64::new(snapshot.max_frame),
                nbackfills: AtomicU64::new(snapshot.nbackfills),
                transaction_count: AtomicU64::new(snapshot.transaction_count),
                last_checksum: (snapshot.checksum_1, snapshot.checksum_2),
                loaded: AtomicBool::new(true),
                loaded_from_disk_scan: AtomicBool::new(false),
                initialized: AtomicBool::new(wal_is_initialized),
            },
            runtime: WalSharedRuntime {
                frame_cache: Arc::new(SpinLock::new(FxHashMap::default())),
                frame_cache_high_water: AtomicU64::new(0),
                file: Some(file),
                read_locks,
                vacuum_lock: TursoRwLock::new(),
                write_lock: TursoRwLock::new(),
                checkpoint_lock: TursoRwLock::new(),
                epoch: AtomicU32::new(snapshot.checkpoint_epoch),
                overflow_fallback_coverage: Arc::new(SpinLock::new(
                    OverflowFallbackCoverage::default(),
                )),
            },
        };
        Ok(Arc::new(RwLock::new(shared)))
    }

    pub fn open_shared_if_exists(
        io: &Arc<dyn IO>,
        path: &str,
        flags: crate::OpenFlags,
    ) -> Result<Arc<RwLock<WalFileShared>>> {
        let mut driver = Self::open_shared_if_exists_begin(io, path, flags)?;
        io.block(|| driver.poll())
    }

    /// Non-blocking entry point for [`WalFileShared::open_shared_if_exists`].
    /// Performs only the synchronous file open (and readonly/NotFound noop
    /// handling); the WAL recovery scan is driven via [`OpenSharedWal::poll`].
    pub fn open_shared_if_exists_begin(
        io: &Arc<dyn IO>,
        path: &str,
        flags: crate::OpenFlags,
    ) -> Result<OpenSharedWal> {
        let file = match io.open_file(path, flags, false) {
            Ok(file) => file,
            Err(LimboError::CompletionError(CompletionError::IOError(
                std::io::ErrorKind::NotFound,
                _,
            ))) if flags.contains(crate::OpenFlags::ReadOnly) => {
                // In readonly mode, if the WAL file doesn't exist, we just return a noop WAL
                // since there's nothing to read from.
                return Ok(OpenSharedWal::Noop(WalFileShared::new_noop()));
            }
            Err(e) => return Err(e),
        };
        Ok(OpenSharedWal::Build(sqlite3_ondisk::BuildSharedWal::begin(
            &file,
        )?))
    }

    pub fn is_initialized(&self) -> Result<bool> {
        Ok(self.metadata.initialized.load(Ordering::Acquire))
    }

    pub fn new_noop() -> Arc<RwLock<WalFileShared>> {
        let wal_header = WalHeader::new();
        let read_locks = array::from_fn(|_| TursoRwLock::new());
        for (i, lock) in read_locks.iter().enumerate() {
            lock.write();
            lock.set_value_exclusive(if i < 2 { 0 } else { READMARK_NOT_USED });
            lock.unlock();
        }
        let shared = WalFileShared {
            metadata: WalSharedMetadata {
                enabled: AtomicBool::new(false),
                wal_header: Arc::new(SpinLock::new(wal_header)),
                min_frame: AtomicU64::new(0),
                max_frame: AtomicU64::new(0),
                nbackfills: AtomicU64::new(0),
                transaction_count: AtomicU64::new(0),
                last_checksum: (0, 0),
                loaded: AtomicBool::new(true),
                loaded_from_disk_scan: AtomicBool::new(false),
                initialized: AtomicBool::new(false),
            },
            runtime: WalSharedRuntime {
                frame_cache: Arc::new(SpinLock::new(FxHashMap::default())),
                frame_cache_high_water: AtomicU64::new(0),
                file: None,
                read_locks,
                vacuum_lock: TursoRwLock::new(),
                write_lock: TursoRwLock::new(),
                checkpoint_lock: TursoRwLock::new(),
                epoch: AtomicU32::new(0),
                overflow_fallback_coverage: Arc::new(SpinLock::new(
                    OverflowFallbackCoverage::default(),
                )),
            },
        };
        Arc::new(RwLock::new(shared))
    }

    #[cfg(test)]
    pub(super) fn new_shared(file: Arc<dyn File>) -> Result<Arc<RwLock<WalFileShared>>> {
        let wal_header = WalHeader::new();
        let read_locks = array::from_fn(|_| TursoRwLock::new());
        // slot zero is always zero as it signifies that reads can be done from the db file
        // directly, and slot 1 is the default read mark containing the max frame. in this case
        // our max frame is zero so both slots 0 and 1 begin at 0
        for (i, lock) in read_locks.iter().enumerate() {
            lock.write();
            lock.set_value_exclusive(if i < 2 { 0 } else { READMARK_NOT_USED });
            lock.unlock();
        }
        let shared = WalFileShared {
            metadata: WalSharedMetadata {
                enabled: AtomicBool::new(true),
                wal_header: Arc::new(SpinLock::new(wal_header)),
                min_frame: AtomicU64::new(0),
                max_frame: AtomicU64::new(0),
                nbackfills: AtomicU64::new(0),
                transaction_count: AtomicU64::new(0),
                last_checksum: (0, 0),
                loaded: AtomicBool::new(true),
                loaded_from_disk_scan: AtomicBool::new(false),
                initialized: AtomicBool::new(false),
            },
            runtime: WalSharedRuntime {
                frame_cache: Arc::new(SpinLock::new(FxHashMap::default())),
                frame_cache_high_water: AtomicU64::new(0),
                file: Some(file),
                read_locks,
                vacuum_lock: TursoRwLock::new(),
                write_lock: TursoRwLock::new(),
                checkpoint_lock: TursoRwLock::new(),
                epoch: AtomicU32::new(0),
                overflow_fallback_coverage: Arc::new(SpinLock::new(
                    OverflowFallbackCoverage::default(),
                )),
            },
        };
        Ok(Arc::new(RwLock::new(shared)))
    }

    pub fn page_size(&self) -> u32 {
        self.metadata.wal_header.lock().page_size
    }

    /// Called after a successful RESTART/TRUNCATE mode checkpoint
    /// when all frames are back‑filled.
    ///
    /// sqlite3/src/wal.c
    /// The following is guaranteed when this function is called:
    ///
    ///   a) the WRITER lock is held,
    ///   b) the entire log file has been checkpointed, and
    ///   c) any existing readers are reading exclusively from the database
    ///      file - there are no readers that may attempt to read a frame from
    ///      the log file.
    ///
    /// This function updates the shared-memory structures so that the next
    /// client to write to the database (which may be this one) does so by
    /// writing frames into the start of the log file.
    fn restart_wal_header(&mut self, io: &dyn IO) {
        {
            let mut hdr = self.metadata.wal_header.lock();
            hdr.checkpoint_seq = hdr.checkpoint_seq.wrapping_add(1);
            // keep hdr.magic, hdr.file_format, hdr.page_size as-is
            hdr.salt_1 = hdr.salt_1.wrapping_add(1);
            hdr.salt_2 = io.generate_random_number() as u32;

            self.metadata.max_frame.store(0, Ordering::Release);
            self.metadata.nbackfills.store(0, Ordering::Release);
            self.metadata.last_checksum = (hdr.checksum_1, hdr.checksum_2);
            // `prepare_wal_start` (used in the `commit_wal_inner`) do the work only if WAL is not initialized yet (so, self.initialized is false)
            // we change WAL state here, so on next write attempt `prepare_wal_start` will update WAL header
            self.metadata.initialized.store(false, Ordering::Release);
        }

        self.runtime.frame_cache.lock().clear();
        self.runtime
            .frame_cache_high_water
            .store(0, Ordering::Release);
        // read-marks
        self.runtime.read_locks[0].set_value_exclusive(0);
        self.runtime.read_locks[1].set_value_exclusive(0);
        for lock in &self.runtime.read_locks[2..] {
            lock.set_value_exclusive(READMARK_NOT_USED);
        }
    }

    /// Replace restored WAL state while preserving process-local locks owned by
    /// existing connections.
    ///
    /// External restore paths rebuild metadata/cache/file state from disk while
    /// other connections may still hold read guards. Those guards are tied to
    /// the process-local lock objects, not to the restored on-disk WAL view, so
    /// replacing the lock objects would make normal `end_read_tx` unlock a
    /// fresh empty lock. Keep lock identity stable and refresh only state
    /// derived from storage.
    #[cfg(feature = "conn_raw_api")]
    pub fn replace_after_external_restore(&mut self, restored: WalFileShared) {
        self.metadata = restored.metadata;
        self.runtime.frame_cache = restored.runtime.frame_cache;
        self.runtime.file = restored.runtime.file;
        self.runtime.epoch.store(
            restored.runtime.epoch.load(Ordering::Acquire),
            Ordering::Release,
        );
        self.runtime.overflow_fallback_coverage = restored.runtime.overflow_fallback_coverage;
    }
}

#[cfg(test)]
pub mod test {
    #[cfg(host_shared_wal)]
    use super::{
        classify_authority_snapshot_against_wal, AuthoritySnapshotRebuildReason,
        AuthoritySnapshotValidation, ShmWalCoordination,
    };
    use super::{
        CheckpointLocks, InProcessWalCoordination, ReadGuardKind, RollbackTo, TryBeginReadResult,
        Wal, WalAutoActions, WalCommitState, WalConnectionState, WalCoordination, WalFile,
        WalSnapshot, NO_LOCK_HELD,
    };
    #[cfg(host_shared_wal)]
    use crate::storage::shared_wal_coordination::{
        MappedSharedWalCoordination, SharedWalCoordinationHeader, SharedWalCoordinationOpenMode,
    };
    use crate::sync::{atomic::Ordering, Arc};
    use crate::sync::{Mutex, RwLock};
    use crate::SqliteDialect;
    use crate::{
        io::FileSyncType,
        storage::{
            buffer_pool::BufferPool,
            database::{DatabaseFile, DatabaseStorage},
            encryption::{CipherMode, EncryptionContext, EncryptionKey},
            page_transform::{PageCodec, PageCodecContext, PageCodecId},
            pager::{allocate_new_page, PageRef},
            sqlite3_ondisk::{self, PageSize, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE},
            wal::READMARK_NOT_USED,
        },
        types::IOResult,
        util::IOExt,
        Buffer, CheckpointMode, CheckpointResult, Completion, CompletionError, Connection,
        Database, File, IOContext, LimboError, MemoryIO, OpenFlags, PlatformIO, Result, SyncMode,
        WalFileShared, IO,
    };
    use std::num::NonZeroUsize;
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    /// Returns an IO backend that supports shared WAL coordination on the host.
    /// On Windows the default `PlatformIO` (`WindowsIO`) lacks the byte-locking
    /// and mapping primitives, so the experimental IOCP backend is used when
    /// the `experimental_win_iocp` feature is enabled.
    fn shared_wal_test_io() -> Arc<dyn IO> {
        #[cfg(all(target_os = "windows", feature = "experimental_win_iocp"))]
        {
            Arc::new(crate::WindowsIOCP::new().unwrap())
        }
        #[cfg(not(all(target_os = "windows", feature = "experimental_win_iocp")))]
        {
            Arc::new(PlatformIO::new().unwrap())
        }
    }

    /// The returned `TempDir` deletes the database directory when it drops, so
    /// callers must hold it for as long as they use the database.
    #[allow(clippy::arc_with_non_send_sync)]
    pub(crate) fn get_database() -> (Arc<Database>, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.db");
        {
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection
                .pragma_update(None, "journal_mode", "wal")
                .unwrap();
        }
        let io = shared_wal_test_io();
        let db = Database::open_file_with_flags(
            io.clone(),
            path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new().with_multiprocess_wal(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        // db + tmp directory
        (db, temp_dir)
    }

    struct DeferredReadFile {
        inner: Arc<dyn File>,
        pending_reads: Mutex<Vec<(u64, Completion)>>,
    }

    impl DeferredReadFile {
        fn new(inner: Arc<dyn File>) -> Self {
            Self {
                inner,
                pending_reads: Mutex::new(Vec::new()),
            }
        }

        fn complete_pending_reads(&self) {
            let pending_reads = std::mem::take(&mut *self.pending_reads.lock());
            for (pos, completion) in pending_reads {
                std::mem::drop(self.inner.pread(pos, completion).unwrap());
            }
        }
    }

    impl File for DeferredReadFile {
        fn lock_file(&self, exclusive: bool) -> crate::Result<()> {
            self.inner.lock_file(exclusive)
        }

        fn unlock_file(&self) -> crate::Result<()> {
            self.inner.unlock_file()
        }

        fn pread(&self, pos: u64, c: Completion) -> crate::Result<Completion> {
            self.pending_reads.lock().push((pos, c.clone()));
            Ok(c)
        }

        fn pwrite(
            &self,
            pos: u64,
            buffer: Arc<Buffer>,
            c: Completion,
        ) -> crate::Result<Completion> {
            self.inner.pwrite(pos, buffer, c)
        }

        fn sync(
            &self,
            c: Completion,
            sync_type: crate::io::FileSyncType,
        ) -> crate::Result<Completion> {
            self.inner.sync(c, sync_type)
        }

        fn size(&self) -> crate::Result<u64> {
            self.inner.size()
        }

        fn truncate(&self, len: u64, c: Completion) -> crate::Result<Completion> {
            self.inner.truncate(len, c)
        }
    }

    #[cfg(feature = "conn_raw_api")]
    #[test]
    fn replace_after_external_restore_preserves_lock_identity() {
        let shared = WalFileShared::new_noop();
        let restored = WalFileShared::new_noop();

        let read_lock_ptrs = {
            let shared = shared.read();
            shared
                .runtime
                .read_locks
                .iter()
                .map(std::ptr::from_ref)
                .collect::<Vec<_>>()
        };
        {
            let shared = shared.read();
            assert!(shared.runtime.read_locks[1].write());
            shared.runtime.read_locks[1].set_value_exclusive(7);
            shared.runtime.read_locks[1].unlock();
        }
        {
            let restored = restored.read();
            restored.metadata.max_frame.store(42, Ordering::Release);
            assert!(restored.runtime.read_locks[1].write());
            restored.runtime.read_locks[1].set_value_exclusive(99);
            restored.runtime.read_locks[1].unlock();
        }

        let restored = match Arc::try_unwrap(restored) {
            Ok(restored) => restored.into_inner(),
            Err(_) => panic!("restored WAL test state should not be shared"),
        };
        shared.write().replace_after_external_restore(restored);

        let shared = shared.read();
        assert_eq!(shared.metadata.max_frame.load(Ordering::Acquire), 42);
        assert_eq!(shared.runtime.read_locks[1].get_value(), 7);
        for (idx, lock) in shared.runtime.read_locks.iter().enumerate() {
            assert_eq!(std::ptr::from_ref(lock), read_lock_ptrs[idx]);
        }
    }

    #[test]
    fn test_truncate_file() {
        let (db, _path) = get_database();
        let conn = db.connect().unwrap();
        conn.execute("create table test (id integer primary key, value text)")
            .unwrap();
        let _ = conn.execute("insert into test (value) values ('test1'), ('test2'), ('test3')");
        let wal = db.shared_wal.write();
        let wal_file = wal.runtime.file.as_ref().unwrap().clone();
        let done = Arc::new(Mutex::new(false));
        let _done = done.clone();
        let _ = wal_file.truncate(
            WAL_HEADER_SIZE as u64,
            Completion::new_trunc(move |_| {
                *_done.lock() = true;
            }),
        );
        assert!(wal_file.size().unwrap() == WAL_HEADER_SIZE as u64);
        assert!(*done.lock());
    }

    #[test]
    fn test_wal_truncate_checkpoint() {
        let (db, path) = get_database();
        let walpath = path.path().join("test.db-wal");

        let conn = db.connect().unwrap();
        conn.execute("create table test (id integer primary key, value text)")
            .unwrap();
        for _i in 0..25 {
            let _ = conn.execute("insert into test (value) values (randomblob(1024)), (randomblob(1024)), (randomblob(1024))");
        }
        let pager = conn.pager.load();
        let _ = pager.cacheflush();

        let stat = std::fs::metadata(&walpath).unwrap();
        let meta_before = std::fs::metadata(&walpath).unwrap();
        let bytes_before = meta_before.len();
        run_checkpoint_until_done(
            &pager,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );

        assert_eq!(pager.wal_state().unwrap().max_frame, 0);

        tracing::debug!("wal filepath: {walpath:?}, size: {}", stat.len());
        let meta_after = std::fs::metadata(&walpath).unwrap();
        let bytes_after = meta_after.len();
        assert_ne!(
            bytes_before, bytes_after,
            "WAL file should not have been empty before checkpoint"
        );
        assert_eq!(
            bytes_after, 0,
            "WAL file should be truncated to 0 bytes, but is {bytes_after} bytes",
        );
    }

    #[test]
    #[cfg_attr(
        all(target_os = "windows", not(feature = "experimental_win_iocp")),
        ignore = "shared WAL coordination requires the experimental Windows IOCP backend"
    )]
    fn test_shutdown_checkpoint_truncates_after_restart() {
        let (db, path) = get_database();
        let walpath = path.path().join("test.db-wal");

        let conn = db.connect().unwrap();
        conn.execute("create table test (id integer primary key, value text)")
            .unwrap();
        conn.execute("insert into test (value) values ('v1'), ('v2')")
            .unwrap();

        let pager = conn.pager.load();
        run_checkpoint_until_done(&pager, CheckpointMode::Restart);

        let bytes_before = std::fs::metadata(&walpath).unwrap().len();
        assert!(
            bytes_before > 0,
            "WAL should still have data after RESTART checkpoint"
        );

        conn.close().unwrap();

        let bytes_after = std::fs::metadata(&walpath).unwrap().len();
        assert_eq!(
            bytes_after, 0,
            "Shutdown checkpoint should truncate WAL after RESTART, but WAL is {bytes_after} bytes",
        );
    }

    fn bulk_inserts(conn: &Arc<Connection>, n_txns: usize, rows_per_txn: usize) {
        for _ in 0..n_txns {
            conn.execute("begin transaction").unwrap();
            for i in 0..rows_per_txn {
                conn.execute(format!("insert into test(value) values ('v{i}')"))
                    .unwrap();
            }
            conn.execute("commit").unwrap();
        }
    }

    fn count_test_table(conn: &Arc<Connection>) -> i64 {
        let mut stmt = conn.prepare("select count(*) from test").unwrap();
        let mut count: i64 = 0;
        stmt.run_with_row_callback(|row| {
            count = row.get(0).unwrap();
            Ok(())
        })
        .unwrap();
        count
    }

    fn run_checkpoint_until_done(pager: &crate::Pager, mode: CheckpointMode) -> CheckpointResult {
        // Use pager.checkpoint() instead of wal.checkpoint() directly because
        // WAL truncation (for TRUNCATE mode) now happens in pager's TruncateWalFile phase.
        pager
            .io
            .block(|| pager.checkpoint(mode, crate::SyncMode::Full, true))
            .unwrap()
    }

    fn run_wal_checkpoint_until_done(
        db: &Database,
        pager: &crate::Pager,
        mode: CheckpointMode,
    ) -> CheckpointResult {
        let wal = pager.wal.as_ref().expect("wal should be present");
        loop {
            match wal.checkpoint(pager, mode, SyncMode::Full) {
                Ok(IOResult::IO(io)) => io.wait(db.io.as_ref()).unwrap(),
                Ok(IOResult::Done(result)) => return result,
                Err(err) => panic!("checkpoint should succeed: {err:?}"),
            }
        }
    }

    #[test]
    fn test_wal_checkpoint_defers_backfill_publication_until_db_sync() {
        let (db, _path) = get_database();
        let wal_shared = db.shared_wal.clone();
        let conn = db.connect().unwrap();
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 8, 2);

        let pager = conn.pager.load();
        let result = run_wal_checkpoint_until_done(&db, &pager, CheckpointMode::Full);
        assert!(
            result.wal_total_backfilled > 0,
            "checkpoint setup should backfill frames before DB sync"
        );
        assert_eq!(
            wal_shared.read().metadata.nbackfills.load(Ordering::SeqCst),
            0,
            "wal.checkpoint() must not publish positive nbackfills before DB sync completes"
        );
    }

    #[test]
    fn test_checkpoint_sync_mode_off_leaves_backfill_unpublished() {
        let (db, _path) = get_database();
        let wal_shared = db.shared_wal.clone();
        let conn = db.connect().unwrap();
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 8, 2);

        let pager = conn.pager.load();
        let result = pager
            .io
            .block(|| pager.checkpoint(CheckpointMode::Full, SyncMode::Off, true))
            .unwrap();
        assert!(
            result.wal_total_backfilled > 0,
            "sync-mode-off checkpoint setup should still backfill frames into the DB file"
        );
        assert_eq!(
            wal_shared.read().metadata.nbackfills.load(Ordering::SeqCst),
            0,
            "SyncMode::Off must not publish positive nbackfills as durable shared state"
        );
    }

    fn make_test_wal() -> (Arc<RwLock<WalFileShared>>, WalFile) {
        let io = shared_wal_test_io();
        let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
        let shared = WalFileShared::new_noop();
        let coordination: Arc<dyn WalCoordination> =
            Arc::new(InProcessWalCoordination::new(shared.clone()));
        let wal = WalFile::new_with_coordination(io, coordination, ((0, 0), 0), buffer_pool);
        (shared, wal)
    }

    fn make_test_wal_from_shared(shared: Arc<RwLock<WalFileShared>>) -> WalFile {
        let io = shared_wal_test_io();
        let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
        let snapshot = shared.read().last_checksum_and_max_frame();
        WalFile::new(io, shared, snapshot, buffer_pool)
    }

    fn make_initialized_memory_wal(page_size: u32) -> (Arc<dyn IO>, Arc<BufferPool>, WalFile) {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
        buffer_pool
            .finalize_with_page_size(page_size as usize)
            .unwrap();
        let file = io
            .open_file("direct-batch-read.db-wal", OpenFlags::Create, false)
            .unwrap();
        let shared = WalFileShared::new_shared(file).unwrap();
        let wal = WalFile::new(io.clone(), shared, ((0, 0), 0), buffer_pool.clone());
        let page_size = PageSize::new(page_size).unwrap();

        if let Some(c) = wal.prepare_wal_start(page_size).unwrap() {
            io.wait_for_completion(c).unwrap();
        }
        let c = wal.prepare_wal_finish(FileSyncType::Fsync).unwrap();
        io.wait_for_completion(c).unwrap();

        (io, buffer_pool, wal)
    }

    /// Like `make_initialized_memory_wal`, but backed by `MemoryYieldIO`, which
    /// writes bytes synchronously yet defers every I/O *completion* until the
    /// next `io.step()`. That makes the "write submitted but not yet durable"
    /// window observable in a single-threaded test.
    #[cfg(feature = "io_memory_yield")]
    fn make_initialized_memory_yield_wal(
        page_size: u32,
    ) -> (Arc<dyn IO>, Arc<BufferPool>, WalFile) {
        let io: Arc<dyn IO> = Arc::new(crate::io::MemoryYieldIO::new());
        let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
        buffer_pool
            .finalize_with_page_size(page_size as usize)
            .unwrap();
        let file = io
            .open_file("spill-visibility.db-wal", OpenFlags::Create, false)
            .unwrap();
        let shared = WalFileShared::new_shared(file).unwrap();
        let wal = WalFile::new(io.clone(), shared, ((0, 0), 0), buffer_pool.clone());
        let page_size = PageSize::new(page_size).unwrap();

        if let Some(c) = wal.prepare_wal_start(page_size).unwrap() {
            io.wait_for_completion(c).unwrap();
        }
        let c = wal.prepare_wal_finish(FileSyncType::Fsync).unwrap();
        io.wait_for_completion(c).unwrap();

        (io, buffer_pool, wal)
    }

    /// Regression test for the cache-spill WAL append path: a frame appended via
    /// `append_frames_vectored` must not become resolvable by `find_frame`
    /// (reads) or `iter_latest_frames` (checkpoint) until its write is durable.
    ///
    /// An earlier version of the async spill fix published the page->frame
    /// mapping (`cache_frame`) synchronously at submission, before the write
    /// landed on disk — so a reader or a concurrent checkpoint could resolve a
    /// frame whose bytes were not yet written. `MemoryYieldIO` defers the write
    /// completion until `io.step()`, so this test can observe the frame while
    /// the write is still in flight: the mapping must not be visible yet.
    #[cfg(feature = "io_memory_yield")]
    #[test]
    fn append_frames_vectored_frame_hidden_until_write_is_durable() {
        let page_size = 512;
        let (io, buffer_pool, wal) = make_initialized_memory_yield_wal(page_size);
        let page = page_with_pattern(7, 0x70, &buffer_pool);

        let completion = wal
            .append_frames_vectored(vec![page], PageSize::new(page_size).unwrap())
            .unwrap();

        // The write cursor advances synchronously (so a following batch chains
        // correctly), but the completion has not fired: the write is not durable.
        assert!(
            !completion.succeeded(),
            "MemoryYieldIO must defer the write completion until io.step()"
        );
        assert_eq!(
            wal.get_max_frame(),
            1,
            "write cursor advances synchronously"
        );

        // The frame must NOT be resolvable before the write is durable. The
        // buggy version cached the mapping at submission and returned Some(1)
        // here, exposing bytes that were not on disk yet.
        assert_eq!(
            wal.find_frame(7, None).unwrap(),
            None,
            "frame must not be visible to readers before its write is durable"
        );

        // Drive the deferred completion: the write is now durable and the
        // completion callback publishes the page->frame mapping.
        io.step().unwrap();
        assert!(completion.succeeded());

        assert_eq!(
            wal.find_frame(7, None).unwrap(),
            Some(1),
            "frame must be visible once its write is durable"
        );
    }

    fn page_with_pattern(page_id: i64, seed: u8, buffer_pool: &Arc<BufferPool>) -> PageRef {
        let page = allocate_new_page(page_id, buffer_pool);
        for (idx, byte) in page.get_contents().as_ptr().iter_mut().enumerate() {
            *byte = seed.wrapping_add(idx as u8).wrapping_add(page_id as u8);
        }
        page
    }

    #[derive(Debug)]
    enum TestPageCodec {
        Xor(u8),
        ErrorEncode,
        ErrorDecode,
    }

    impl PageCodec for TestPageCodec {
        fn codec_id(&self) -> PageCodecId {
            let mut id = *b"wal-test-codec--";
            id[15] = match self {
                Self::Xor(mask) => *mask,
                Self::ErrorEncode => 1,
                Self::ErrorDecode => 2,
            };
            PageCodecId::new(id)
        }

        fn required_reserved_bytes(&self) -> u8 {
            0
        }

        fn encode_page(
            &self,
            _context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> Result<()> {
            match self {
                Self::Xor(mask) => {
                    for (input, output) in input.iter().zip(output) {
                        *output = input ^ mask;
                    }
                    Ok(())
                }
                Self::ErrorEncode => Err(LimboError::InternalError("codec encode failed".into())),
                Self::ErrorDecode => {
                    output.copy_from_slice(input);
                    Ok(())
                }
            }
        }

        fn decode_page(
            &self,
            context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> Result<()> {
            match self {
                Self::Xor(_) => self.encode_page(context, input, output),
                Self::ErrorEncode => {
                    output.copy_from_slice(input);
                    Ok(())
                }
                Self::ErrorDecode => Err(LimboError::InternalError("codec decode failed".into())),
            }
        }
    }

    #[derive(Debug)]
    struct XorFailOnPageCodec {
        mask: u8,
        fail_encode_page: Option<u32>,
        fail_decode_page: Option<u32>,
    }

    impl XorFailOnPageCodec {
        fn transform(&self, input: &[u8], output: &mut [u8]) {
            for (input, output) in input.iter().zip(output) {
                *output = input ^ self.mask;
            }
        }
    }

    impl PageCodec for XorFailOnPageCodec {
        fn codec_id(&self) -> PageCodecId {
            PageCodecId::new(*b"wal-fail-page---")
        }

        fn required_reserved_bytes(&self) -> u8 {
            0
        }

        fn encode_page(
            &self,
            context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> Result<()> {
            if self.fail_encode_page == Some(context.page_no) {
                return Err(LimboError::InternalError("codec encode failed".into()));
            }
            self.transform(input, output);
            Ok(())
        }

        fn decode_page(
            &self,
            context: PageCodecContext,
            input: &[u8],
            output: &mut [u8],
        ) -> Result<()> {
            if self.fail_decode_page == Some(context.page_no) {
                return Err(LimboError::InternalError("codec decode failed".into()));
            }
            self.transform(input, output);
            Ok(())
        }
    }

    fn set_test_page_codec(wal: &WalFile, codec: Arc<dyn PageCodec>) {
        let mut io_ctx = IOContext::default();
        io_ctx.set_page_codec(codec);
        wal.set_io_context(io_ctx);
    }

    fn set_test_encryption(wal: &WalFile, page_size: usize) {
        let key = EncryptionKey::from_hex_string("000102030405060708090a0b0c0d0e0f").unwrap();
        let mut io_ctx = IOContext::default();
        io_ctx.set_encryption(
            EncryptionContext::new(CipherMode::Aes128Gcm, &key, page_size).unwrap(),
        );
        wal.set_io_context(io_ctx);
    }

    fn append_test_pages(
        io: &Arc<dyn IO>,
        wal: &WalFile,
        page_size: u32,
        pages: &[PageRef],
    ) -> Vec<Vec<u8>> {
        let prepared = wal
            .prepare_frames(pages, PageSize::new(page_size).unwrap(), Some(99), None)
            .unwrap();
        let expected = pages
            .iter()
            .map(|page| page.get_contents().as_ptr().to_vec())
            .collect::<Vec<_>>();

        let file = wal.wal_file().unwrap();
        let c = file
            .pwritev(
                prepared.offset,
                prepared.bufs.clone(),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();
        wal.commit_prepared_frames(&[prepared]);
        wal.finish_append_frames_commit().unwrap();
        expected
    }

    #[test]
    fn append_frames_vectored_spill_frames_are_not_reused_by_next_prepare() {
        let page_size = 512;
        let (_io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        let spill_page = page_with_pattern(7, 0x70, &buffer_pool);

        let completion = wal
            .append_frames_vectored(vec![spill_page], PageSize::new(page_size).unwrap())
            .unwrap();
        assert!(completion.succeeded());
        assert_eq!(wal.get_max_frame(), 1);
        assert_eq!(wal.get_max_frame_in_wal(), 0);

        let commit_page = page_with_pattern(9, 0x90, &buffer_pool);
        let prepared = wal
            .prepare_frames(
                &[commit_page],
                PageSize::new(page_size).unwrap(),
                Some(99),
                None,
            )
            .unwrap();

        assert_eq!(
            prepared.metadata[0].1, 2,
            "prepare_frames must chain after unpublished spill frames"
        );
        assert_eq!(prepared.final_max_frame, 2);
    }

    fn wait_for_completion_error(io: &Arc<dyn IO>, completion: Completion) -> CompletionError {
        match io.wait_for_completion(completion) {
            Err(LimboError::CompletionError(err)) => err,
            other => panic!("expected completion error, got {other:?}"),
        }
    }

    #[test]
    fn read_frames_batch_reads_contiguous_wal_frames_directly() {
        let page_size = 512;
        let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        let source_pages = vec![
            page_with_pattern(2, 0x10, &buffer_pool),
            page_with_pattern(3, 0x20, &buffer_pool),
            page_with_pattern(4, 0x30, &buffer_pool),
            page_with_pattern(5, 0x40, &buffer_pool),
        ];
        let expected = append_test_pages(&io, &wal, page_size, &source_pages);

        let target_pages = vec![
            Arc::new(crate::Page::new(2)),
            Arc::new(crate::Page::new(3)),
            Arc::new(crate::Page::new(4)),
            Arc::new(crate::Page::new(5)),
        ];
        let c = wal
            .read_frames_batch(1, &target_pages, buffer_pool, None, None)
            .unwrap();
        io.wait_for_completion(c).unwrap();

        for (idx, page) in target_pages.iter().enumerate() {
            assert!(page.is_loaded(), "page {} should be loaded", page.get().id);
            assert!(!page.is_locked(), "page {} lock leaked", page.get().id);
            assert_eq!(page.wal_tag_pair(), ((idx + 1) as u64, 0));
            assert_eq!(page.get_contents().as_ptr(), expected[idx].as_slice());
        }
    }

    #[test]
    fn page_codec_round_trips_wal_batch_reads() {
        let page_size = 512;
        let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        set_test_page_codec(&wal, Arc::new(TestPageCodec::Xor(0xa5)));
        let source_pages = vec![
            page_with_pattern(32, 0x10, &buffer_pool),
            page_with_pattern(33, 0x20, &buffer_pool),
        ];
        let expected = append_test_pages(&io, &wal, page_size, &source_pages);

        let target_pages = vec![
            Arc::new(crate::Page::new(32)),
            Arc::new(crate::Page::new(33)),
        ];
        let c = wal
            .read_frames_batch(1, &target_pages, buffer_pool, None, None)
            .unwrap();
        io.wait_for_completion(c).unwrap();

        for (idx, page) in target_pages.iter().enumerate() {
            assert_eq!(page.get_contents().as_ptr(), expected[idx].as_slice());
        }
    }

    #[test]
    fn page_codec_missing_wal_frame_reports_short_read() {
        let page_size = 512;
        let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        set_test_page_codec(&wal, Arc::new(TestPageCodec::Xor(0xa5)));
        let target_page = Arc::new(crate::Page::new(43));

        let completion = wal.read_frame(1, target_page, buffer_pool, None).unwrap();
        let error = wait_for_completion_error(&io, completion);

        assert!(matches!(
            error,
            CompletionError::ShortReadWalFrame {
                expected: 512,
                actual: 0,
                ..
            }
        ));
    }

    #[test]
    fn page_codec_round_trips_vectored_wal_spill_frames() {
        let page_size = 512;
        let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        set_test_page_codec(&wal, Arc::new(TestPageCodec::Xor(0xa5)));
        let source_page = page_with_pattern(32, 0x10, &buffer_pool);
        let expected = source_page.get_contents().as_ptr().to_vec();

        let completion = wal
            .append_frames_vectored(vec![source_page], PageSize::new(page_size).unwrap())
            .unwrap();
        io.wait_for_completion(completion).unwrap();

        let target_page = Arc::new(crate::Page::new(32));
        let completion = wal
            .read_frames_batch(1, &[target_page.clone()], buffer_pool, None, None)
            .unwrap();
        io.wait_for_completion(completion).unwrap();
        assert_eq!(target_page.get_contents().as_ptr(), expected.as_slice());
    }

    #[test]
    fn page_codec_vectored_spill_encode_error_does_not_advance_wal() {
        let page_size = 512;
        let (_io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        set_test_page_codec(
            &wal,
            Arc::new(XorFailOnPageCodec {
                mask: 0xa5,
                fail_encode_page: Some(33),
                fail_decode_page: None,
            }),
        );
        let pages = vec![
            page_with_pattern(32, 0x10, &buffer_pool),
            page_with_pattern(33, 0x20, &buffer_pool),
        ];

        let err = wal
            .append_frames_vectored(pages, PageSize::new(page_size).unwrap())
            .unwrap_err();

        assert!(err.to_string().contains("codec encode failed"));
        assert_eq!(wal.get_max_frame(), 0);
        assert_eq!(wal.find_frame(32, None).unwrap(), None);
        assert_eq!(wal.find_frame(33, None).unwrap(), None);
    }

    #[test]
    fn page_codec_round_trips_raw_wal_frames() {
        let page_size = 512;
        let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        set_test_page_codec(&wal, Arc::new(TestPageCodec::Xor(0xa5)));
        let expected = (0..page_size).map(|i| i as u8).collect::<Vec<_>>();

        wal.write_frame_raw(buffer_pool, 1, 44, 0, &expected, FileSyncType::Fsync)
            .unwrap();

        let mut frame = vec![0; WAL_FRAME_HEADER_SIZE + page_size as usize];
        let completion = wal.read_frame_raw(1, &mut frame).unwrap();
        io.wait_for_completion(completion).unwrap();
        assert_eq!(&frame[WAL_FRAME_HEADER_SIZE..], expected.as_slice());
    }

    #[test]
    fn page_codec_raw_wal_encode_error_does_not_advance_wal() {
        let page_size = 512;
        let (_io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        set_test_page_codec(&wal, Arc::new(TestPageCodec::ErrorEncode));
        let page = vec![0; page_size as usize];

        let err = wal
            .write_frame_raw(buffer_pool, 1, 44, 0, &page, FileSyncType::Fsync)
            .unwrap_err();

        assert!(err.to_string().contains("codec encode failed"));
        assert_eq!(wal.get_max_frame(), 0);
        assert_eq!(wal.find_frame(44, None).unwrap(), None);
    }

    #[test]
    fn page_codec_raw_wal_duplicate_is_idempotent_and_detects_conflict() {
        let page_size = 512;
        let (_io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        set_test_page_codec(&wal, Arc::new(TestPageCodec::Xor(0xa5)));
        let page = (0..page_size).map(|i| i as u8).collect::<Vec<_>>();

        wal.write_frame_raw(buffer_pool.clone(), 1, 44, 0, &page, FileSyncType::Fsync)
            .unwrap();
        wal.write_frame_raw(buffer_pool.clone(), 1, 44, 0, &page, FileSyncType::Fsync)
            .unwrap();

        let mut different_page = page;
        different_page[0] ^= 1;
        let err = wal
            .write_frame_raw(buffer_pool, 1, 44, 0, &different_page, FileSyncType::Fsync)
            .unwrap_err();
        assert!(matches!(err, LimboError::Conflict(_)));
        assert_eq!(wal.get_max_frame(), 1);
        assert_eq!(wal.find_frame(44, None).unwrap(), Some(1));
    }

    #[test]
    fn page_codec_raw_wal_read_rejects_wrong_buffer_size() {
        let page_size = 512;
        let (_io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        set_test_page_codec(&wal, Arc::new(TestPageCodec::Xor(0xa5)));
        let page = vec![0; page_size as usize];
        wal.write_frame_raw(buffer_pool, 1, 44, 0, &page, FileSyncType::Fsync)
            .unwrap();

        let expected_frame_len = WAL_FRAME_HEADER_SIZE + page_size as usize;
        for frame_len in [expected_frame_len - 1, expected_frame_len + 1] {
            let mut frame = vec![0; frame_len];
            match wal.read_frame_raw(1, &mut frame) {
                Err(LimboError::InvalidArgument(message)) => assert_eq!(
                    message,
                    format!(
                        "unexpected WAL frame buffer size: got={frame_len}, expected={expected_frame_len}"
                    )
                ),
                Err(error) => panic!("expected invalid argument, got {error:?}"),
                Ok(_) => panic!("expected frame size {frame_len} to be rejected"),
            }
        }
    }

    #[test]
    fn page_codec_encode_error_does_not_publish_wal_frames() {
        let page_size = 512;
        let (_io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        set_test_page_codec(&wal, Arc::new(TestPageCodec::ErrorEncode));
        let source_page = page_with_pattern(43, 0x43, &buffer_pool);

        assert!(wal
            .prepare_frames(
                &[source_page],
                PageSize::new(page_size).unwrap(),
                Some(1),
                None,
            )
            .is_err());
        assert_eq!(wal.get_max_frame(), 0);
        assert_eq!(wal.find_frame(43, None).unwrap(), None);
    }

    #[test]
    fn encryption_round_trips_raw_wal_frames() {
        let page_size = 4096;
        let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        set_test_encryption(&wal, page_size as usize);
        let mut expected = (0..page_size).map(|i| i as u8).collect::<Vec<_>>();
        expected[page_size as usize - 64..].fill(0);

        wal.write_frame_raw(buffer_pool, 1, 44, 0, &expected, FileSyncType::Fsync)
            .unwrap();

        let mut frame = vec![0; WAL_FRAME_HEADER_SIZE + page_size as usize];
        let completion = wal.read_frame_raw(1, &mut frame).unwrap();
        io.wait_for_completion(completion).unwrap();
        assert_eq!(&frame[WAL_FRAME_HEADER_SIZE..], expected.as_slice());
    }

    #[cfg(feature = "checksum")]
    #[test]
    fn checksum_is_applied_to_raw_wal_frames() {
        let page_size = 4096;
        let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        let mut source = (0..page_size).map(|i| i as u8).collect::<Vec<_>>();
        source[page_size as usize - 8..].fill(0);

        wal.write_frame_raw(buffer_pool.clone(), 1, 44, 0, &source, FileSyncType::Fsync)
            .unwrap();

        let target = Arc::new(crate::Page::new(44));
        let completion = wal
            .read_frame(1, target.clone(), buffer_pool, None)
            .unwrap();
        io.wait_for_completion(completion).unwrap();
        assert_eq!(
            &target.get_contents().as_ptr()[..page_size as usize - 8],
            &source[..page_size as usize - 8]
        );
    }

    #[test]
    fn page_codec_raw_wal_read_propagates_decode_error() {
        let page_size = 512;
        let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        set_test_page_codec(&wal, Arc::new(TestPageCodec::ErrorDecode));
        let source_pages = vec![page_with_pattern(43, 0x43, &buffer_pool)];
        append_test_pages(&io, &wal, page_size, &source_pages);

        let mut frame = vec![0; WAL_FRAME_HEADER_SIZE + page_size as usize];
        let c = wal.read_frame_raw(1, &mut frame).unwrap();
        let err = wait_for_completion_error(&io, c);

        assert!(matches!(
            err,
            CompletionError::PageCodecError { page_idx: 43 }
        ));
    }

    #[test]
    fn page_codec_wal_batch_read_reports_codec_error() {
        let page_size = 512;
        let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        set_test_page_codec(&wal, Arc::new(TestPageCodec::ErrorDecode));
        let source_pages = vec![page_with_pattern(43, 0x43, &buffer_pool)];
        append_test_pages(&io, &wal, page_size, &source_pages);

        let target_page = Arc::new(crate::Page::new(43));
        let c = wal
            .read_frames_batch(1, &[target_page], buffer_pool, None, None)
            .unwrap();
        let err = wait_for_completion_error(&io, c);

        assert!(matches!(
            err,
            CompletionError::PageCodecError { page_idx: 43 }
        ));
    }

    #[test]
    fn page_codec_wal_batch_decode_failure_does_not_publish_earlier_pages() {
        let page_size = 512;
        let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        set_test_page_codec(&wal, Arc::new(TestPageCodec::Xor(0xa5)));
        let source_pages = vec![
            page_with_pattern(32, 0x10, &buffer_pool),
            page_with_pattern(33, 0x20, &buffer_pool),
            page_with_pattern(34, 0x30, &buffer_pool),
        ];
        append_test_pages(&io, &wal, page_size, &source_pages);
        set_test_page_codec(
            &wal,
            Arc::new(XorFailOnPageCodec {
                mask: 0xa5,
                fail_encode_page: None,
                fail_decode_page: Some(33),
            }),
        );
        let target_pages = vec![
            Arc::new(crate::Page::new(32)),
            Arc::new(crate::Page::new(33)),
            Arc::new(crate::Page::new(34)),
        ];

        let completion = wal
            .read_frames_batch(1, &target_pages, buffer_pool, None, None)
            .unwrap();
        let err = wait_for_completion_error(&io, completion);

        assert!(matches!(
            err,
            CompletionError::PageCodecError { page_idx: 33 }
        ));
        for page in target_pages {
            assert!(!page.is_locked());
            assert!(!page.is_loaded());
            assert!(!page.has_wal_tag());
        }
    }

    #[test]
    fn read_frames_batch_can_start_from_middle_frame() {
        let page_size = 512;
        let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        let source_pages = vec![
            page_with_pattern(10, 0x01, &buffer_pool),
            page_with_pattern(11, 0x02, &buffer_pool),
            page_with_pattern(12, 0x03, &buffer_pool),
            page_with_pattern(13, 0x04, &buffer_pool),
        ];
        let expected = append_test_pages(&io, &wal, page_size, &source_pages);

        let target_pages = vec![
            Arc::new(crate::Page::new(11)),
            Arc::new(crate::Page::new(12)),
            Arc::new(crate::Page::new(13)),
        ];
        let c = wal
            .read_frames_batch(2, &target_pages, buffer_pool, None, None)
            .unwrap();
        io.wait_for_completion(c).unwrap();

        for (idx, page) in target_pages.iter().enumerate() {
            assert!(page.is_loaded(), "page {} should be loaded", page.get().id);
            assert!(!page.is_locked(), "page {} lock leaked", page.get().id);
            assert_eq!(page.wal_tag_pair(), ((idx + 2) as u64, 0));
            assert_eq!(page.get_contents().as_ptr(), expected[idx + 1].as_slice());
        }
    }

    #[test]
    fn read_frames_batch_follows_physical_frame_order_not_page_id_order() {
        let page_size = 512;
        let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        let source_pages = vec![
            page_with_pattern(7, 0x71, &buffer_pool),
            page_with_pattern(2, 0x22, &buffer_pool),
            page_with_pattern(5, 0x55, &buffer_pool),
            page_with_pattern(9, 0x99, &buffer_pool),
        ];
        let expected = append_test_pages(&io, &wal, page_size, &source_pages);

        let target_pages = vec![
            Arc::new(crate::Page::new(7)),
            Arc::new(crate::Page::new(2)),
            Arc::new(crate::Page::new(5)),
            Arc::new(crate::Page::new(9)),
        ];
        let c = wal
            .read_frames_batch(1, &target_pages, buffer_pool, None, None)
            .unwrap();
        io.wait_for_completion(c).unwrap();

        for (idx, page) in target_pages.iter().enumerate() {
            assert_eq!(
                page.get_contents().as_ptr(),
                expected[idx].as_slice(),
                "frame-order read should preserve page {} contents",
                page.get().id
            );
            assert_eq!(page.wal_tag_pair(), ((idx + 1) as u64, 0));
        }
    }

    #[test]
    fn read_frames_batch_short_read_errors_and_clears_page_locks() {
        let page_size = 512;
        let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        let source_pages = vec![
            page_with_pattern(20, 0x20, &buffer_pool),
            page_with_pattern(21, 0x21, &buffer_pool),
        ];
        append_test_pages(&io, &wal, page_size, &source_pages);

        let target_pages = vec![
            Arc::new(crate::Page::new(20)),
            Arc::new(crate::Page::new(21)),
            Arc::new(crate::Page::new(22)),
        ];
        let c = wal
            .read_frames_batch(1, &target_pages, buffer_pool, None, None)
            .unwrap();
        let err = wait_for_completion_error(&io, c);

        assert!(
            matches!(err, CompletionError::ShortReadWalFrame { .. }),
            "unexpected error: {err:?}"
        );
        for page in &target_pages {
            assert!(!page.is_locked(), "page {} lock leaked", page.get().id);
            assert!(
                !page.is_loaded(),
                "page {} should not be loaded",
                page.get().id
            );
            assert!(
                !page.has_wal_tag(),
                "page {} should not be tagged",
                page.get().id
            );
        }
    }

    #[test]
    fn read_frames_batch_page_number_mismatch_returns_error_not_panic() {
        let page_size = 512;
        let (io, buffer_pool, wal) = make_initialized_memory_wal(page_size);
        let source_pages = vec![
            page_with_pattern(30, 0x30, &buffer_pool),
            page_with_pattern(31, 0x31, &buffer_pool),
        ];
        append_test_pages(&io, &wal, page_size, &source_pages);

        let target_pages = vec![
            Arc::new(crate::Page::new(30)),
            Arc::new(crate::Page::new(99)),
        ];
        let c = wal
            .read_frames_batch(1, &target_pages, buffer_pool, None, None)
            .unwrap();
        let err = wait_for_completion_error(&io, c);

        assert!(
            matches!(
                err,
                CompletionError::WalFramePageMismatch {
                    frame_id: 2,
                    expected: 99,
                    actual: 31
                }
            ),
            "unexpected error: {err:?}"
        );
        for page in &target_pages {
            assert!(!page.is_locked(), "page {} lock leaked", page.get().id);
            assert!(
                !page.is_loaded(),
                "page {} should not be loaded",
                page.get().id
            );
            assert!(
                !page.has_wal_tag(),
                "page {} should not be tagged",
                page.get().id
            );
            assert!(
                page.get().buffer().is_none(),
                "page {} should not retain a buffer",
                page.get().id
            );
        }
    }

    fn set_shared_snapshot(shared: &Arc<RwLock<WalFileShared>>, snapshot: WalSnapshot) {
        let mut guard = shared.write();
        guard
            .metadata
            .max_frame
            .store(snapshot.max_frame, Ordering::Release);
        guard
            .metadata
            .nbackfills
            .store(snapshot.nbackfills, Ordering::Release);
        guard.metadata.last_checksum = snapshot.last_checksum;
        guard.metadata.wal_header.lock().checkpoint_seq = snapshot.checkpoint_seq;
        guard
            .metadata
            .transaction_count
            .store(snapshot.transaction_count, Ordering::Release);
    }

    fn make_test_coordination(shared: &Arc<RwLock<WalFileShared>>) -> InProcessWalCoordination {
        InProcessWalCoordination::new(shared.clone())
    }

    #[cfg(host_shared_wal)]
    fn make_test_shm_coordination(
        shared: &Arc<RwLock<WalFileShared>>,
        path: &std::path::Path,
    ) -> (Arc<MappedSharedWalCoordination>, ShmWalCoordination) {
        let io = shared_wal_test_io();
        let authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, path, 64).unwrap());
        let coordination = ShmWalCoordination::new(shared.clone(), authority.clone());
        (authority, coordination)
    }

    #[cfg(host_shared_wal)]
    fn active_shared_reader_slot_count(authority: &MappedSharedWalCoordination) -> usize {
        let reader_slot_count = authority.snapshot().reader_slot_count;
        (0..reader_slot_count)
            .filter(|&slot_index| authority.reader_owner(slot_index).is_some())
            .count()
    }

    #[cfg(host_shared_wal)]
    fn write_test_wal_with_single_commit_frame(
        io: &Arc<dyn IO>,
        wal_path: &std::path::Path,
    ) -> SharedWalCoordinationHeader {
        let file = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let mut wal_header = sqlite3_ondisk::WalHeader {
            page_size: 4096,
            checkpoint_seq: 5,
            salt_1: 17,
            salt_2: 23,
            ..sqlite3_ondisk::WalHeader::new()
        };
        let use_native_endian = cfg!(target_endian = "big") == ((wal_header.magic & 1) != 0);
        let mut header_prefix = [0u8; WAL_HEADER_SIZE - 8];
        header_prefix[0..4].copy_from_slice(&wal_header.magic.to_be_bytes());
        header_prefix[4..8].copy_from_slice(&wal_header.file_format.to_be_bytes());
        header_prefix[8..12].copy_from_slice(&wal_header.page_size.to_be_bytes());
        header_prefix[12..16].copy_from_slice(&wal_header.checkpoint_seq.to_be_bytes());
        header_prefix[16..20].copy_from_slice(&wal_header.salt_1.to_be_bytes());
        header_prefix[20..24].copy_from_slice(&wal_header.salt_2.to_be_bytes());
        let header_checksum =
            sqlite3_ondisk::checksum_wal(&header_prefix, &wal_header, (0, 0), use_native_endian);
        wal_header.checksum_1 = header_checksum.0;
        wal_header.checksum_2 = header_checksum.1;

        io.wait_for_completion(
            sqlite3_ondisk::begin_write_wal_header(file.as_ref(), &wal_header, None).unwrap(),
        )
        .unwrap();

        let buffer_pool = BufferPool::begin_init(io, BufferPool::TEST_ARENA_SIZE);
        buffer_pool
            .finalize_with_page_size(wal_header.page_size as usize)
            .unwrap();
        #[allow(unused_mut)]
        let mut page = vec![0x5a; wal_header.page_size as usize];
        #[cfg(feature = "checksum")]
        crate::storage::checksum::ChecksumContext::new()
            .add_checksum_to_page(&mut page, 7)
            .unwrap();
        let (frame_checksum, frame_buf) = sqlite3_ondisk::prepare_wal_frame(
            &buffer_pool,
            &wal_header,
            header_checksum,
            wal_header.page_size,
            7,
            1,
            &page,
        );
        let c = file
            .pwrite(
                WAL_HEADER_SIZE as u64,
                frame_buf,
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();
        let c = file
            .sync(Completion::new_sync(|_| {}), crate::io::FileSyncType::Fsync)
            .unwrap();
        io.wait_for_completion(c).unwrap();

        SharedWalCoordinationHeader {
            max_frame: 1,
            nbackfills: 0,
            transaction_count: 9,
            visibility_generation: 3,
            checkpoint_seq: wal_header.checkpoint_seq,
            checkpoint_epoch: 7,
            page_size: wal_header.page_size,
            salt_1: wal_header.salt_1,
            salt_2: wal_header.salt_2,
            checksum_1: frame_checksum.0,
            checksum_2: frame_checksum.1,
            reader_slot_count: 64,
        }
    }

    #[cfg(host_shared_wal)]
    fn open_test_db_file_for_wal(
        io: &Arc<dyn IO>,
        wal_path: &std::path::Path,
    ) -> Arc<dyn DatabaseStorage> {
        let db_path = wal_path.with_extension("db");
        Arc::new(DatabaseFile::new(
            io.open_file(db_path.to_str().unwrap(), crate::OpenFlags::Create, false)
                .unwrap(),
        ))
    }

    #[test]
    #[cfg(host_shared_wal)]
    fn test_read_frame_keeps_epoch_from_issue_time() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("epoch-race.db-wal");
        let io = shared_wal_test_io();
        let snapshot = write_test_wal_with_single_commit_frame(&io, &wal_path);

        let file = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared = WalFileShared::new_shared(file.clone()).unwrap();
        let deferred_file = Arc::new(DeferredReadFile::new(file));
        {
            let mut shared = shared.write();
            shared.runtime.file = Some(deferred_file.clone());
            shared
                .runtime
                .epoch
                .store(snapshot.checkpoint_epoch, Ordering::Release);
        }

        let coordination: Arc<dyn WalCoordination> = Arc::new(make_test_coordination(&shared));
        let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
        buffer_pool
            .finalize_with_page_size(snapshot.page_size as usize)
            .unwrap();
        let wal = WalFile::new_with_coordination(
            io.clone(),
            coordination,
            (
                (snapshot.checksum_1, snapshot.checksum_2),
                snapshot.max_frame,
            ),
            buffer_pool.clone(),
        );

        let page = Arc::new(crate::storage::pager::Page::new(7));
        let issued_epoch = wal.coordination.checkpoint_epoch();
        let completion = wal.read_frame(1, page.clone(), buffer_pool, None).unwrap();

        wal.increment_checkpoint_epoch();
        deferred_file.complete_pending_reads();
        io.wait_for_completion(completion).unwrap();

        assert_eq!(
            page.wal_tag_pair(),
            (1, issued_epoch),
            "WAL reads must retain the epoch from when the read was issued"
        );
    }

    #[cfg(test)]
    fn read_slots_with_readers(shared: &WalFileShared) -> Vec<usize> {
        shared
            .runtime
            .read_locks
            .iter()
            .enumerate()
            .filter_map(|(slot, lock)| {
                let state = lock.0.load(Ordering::Acquire);
                let has_readers = (state & super::TursoRwLock::READER_COUNT_MASK) != 0;
                has_readers.then_some(slot)
            })
            .collect()
    }

    fn wal_header_snapshot(shared: &Arc<RwLock<WalFileShared>>) -> (u32, u32, u32, u32) {
        // (checkpoint_seq, salt1, salt2, page_size)
        let shared_guard = shared.read();
        let hdr = shared_guard.metadata.wal_header.lock();
        (hdr.checkpoint_seq, hdr.salt_1, hdr.salt_2, hdr.page_size)
    }

    #[test]
    fn test_wal_connection_state_round_trip() {
        let (_shared, wal) = make_test_wal();
        let state = WalConnectionState::new(
            WalSnapshot {
                max_frame: 11,
                nbackfills: 7,
                last_checksum: (31, 47),
                checkpoint_seq: 5,
                transaction_count: 13,
            },
            ReadGuardKind::ReadMark(NonZeroUsize::new(3).unwrap()),
        );

        wal.install_connection_state(state);

        assert_eq!(wal.connection_state(), state);
        assert_eq!(wal.connection_state().snapshot.min_frame(), 8);
    }

    #[test]
    fn test_wal_explicit_backend_constructor_does_not_keep_shared_handle() {
        let io = shared_wal_test_io();
        let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
        let shared = WalFileShared::new_noop();
        let coordination: Arc<dyn WalCoordination> =
            Arc::new(InProcessWalCoordination::new(shared.clone()));

        assert_eq!(Arc::strong_count(&shared), 2);

        let _wal = WalFile::new_with_coordination(io, coordination, ((0, 0), 0), buffer_pool);

        assert_eq!(Arc::strong_count(&shared), 2);
    }

    #[test]
    fn test_mvcc_refresh_updates_snapshot_only_when_no_read_guard_is_held() {
        let (shared, wal) = make_test_wal();
        let initial = WalSnapshot {
            max_frame: 4,
            nbackfills: 2,
            last_checksum: (9, 10),
            checkpoint_seq: 1,
            transaction_count: 3,
        };
        set_shared_snapshot(&shared, initial);
        wal.install_connection_state(WalConnectionState::new(
            initial,
            ReadGuardKind::ReadMark(NonZeroUsize::new(2).unwrap()),
        ));

        assert!(!wal.mvcc_refresh_if_db_changed());

        let updated = WalSnapshot {
            max_frame: 8,
            nbackfills: 5,
            last_checksum: (21, 34),
            checkpoint_seq: 7,
            transaction_count: 4,
        };
        set_shared_snapshot(&shared, updated);

        // A held read guard pins this connection's WAL view; the refresh must not
        // advance it past the snapshot an active transaction captured.
        assert!(!wal.mvcc_refresh_if_db_changed());
        assert_eq!(
            wal.connection_state(),
            WalConnectionState::new(
                initial,
                ReadGuardKind::ReadMark(NonZeroUsize::new(2).unwrap())
            )
        );

        // With no read guard held the refresh picks up the new shared snapshot.
        wal.install_connection_state(WalConnectionState::new(initial, ReadGuardKind::None));
        assert!(wal.mvcc_refresh_if_db_changed());
        assert_eq!(
            wal.connection_state(),
            WalConnectionState::new(updated, ReadGuardKind::None)
        );
    }

    #[test]
    fn test_in_process_coordination_uses_shared_authority() {
        let (shared, _wal) = make_test_wal();
        let coordination = make_test_coordination(&shared);
        let snapshot = WalSnapshot {
            max_frame: 9,
            nbackfills: 3,
            last_checksum: (55, 89),
            checkpoint_seq: 7,
            transaction_count: 11,
        };
        set_shared_snapshot(&shared, snapshot);
        {
            let guard = shared.write();
            guard.runtime.epoch.store(5, Ordering::Release);
            guard.runtime.frame_cache.lock().extend([
                (1, vec![1, 4, 8]),
                (2, vec![2, 6]),
                (3, vec![3]),
            ]);
        }

        assert_eq!(coordination.load_snapshot(), snapshot);
        assert_eq!(coordination.checkpoint_epoch(), 5);
        assert_eq!(coordination.find_frame(1, 4, 9, None), Some(8));
        assert_eq!(coordination.find_frame(2, 4, 9, Some(5)), Some(2));
        assert_eq!(coordination.iter_latest_frames(4, 9), vec![(1, 8), (2, 6)]);

        coordination.publish_commit(WalCommitState {
            max_frame: 12,
            last_checksum: (144, 233),
            transaction_count: 12,
        });
        let published = coordination.load_snapshot();
        assert_eq!(published.max_frame, 12);
        assert_eq!(published.last_checksum, (144, 233));
        assert_eq!(published.transaction_count, 12);
        assert_eq!(published.nbackfills, snapshot.nbackfills);
        assert_eq!(published.checkpoint_seq, snapshot.checkpoint_seq);
    }

    #[test]
    fn test_in_process_coordination_publishes_checkpoint_and_restart_state() {
        let (shared, _wal) = make_test_wal();
        let coordination = make_test_coordination(&shared);
        let io = PlatformIO::new().unwrap();
        let snapshot = WalSnapshot {
            max_frame: 9,
            nbackfills: 3,
            last_checksum: (55, 89),
            checkpoint_seq: 7,
            transaction_count: 11,
        };
        set_shared_snapshot(&shared, snapshot);
        {
            let guard = shared.write();
            let mut header = guard.metadata.wal_header.lock();
            header.page_size = 4096;
            header.checksum_1 = 144;
            header.checksum_2 = 233;
            guard.metadata.initialized.store(true, Ordering::Release);
            guard.runtime.epoch.store(5, Ordering::Release);
            guard
                .runtime
                .frame_cache
                .lock()
                .extend([(1, vec![1, 4, 8]), (2, vec![2, 6])]);
        }

        coordination.publish_backfill(8);
        assert_eq!(coordination.load_snapshot().nbackfills, 8);
        assert_eq!(coordination.bump_checkpoint_epoch(), 5);
        assert_eq!(coordination.checkpoint_epoch(), 6);

        assert!(coordination.try_read_mark_exclusive(0));
        let restarted = coordination.begin_restart(&io).unwrap();
        coordination.end_restart();
        coordination.unlock_read_mark(0);

        assert_eq!(restarted.max_frame, 0);
        assert_eq!(restarted.nbackfills, 0);
        assert_eq!(restarted.last_checksum, (144, 233));
        assert_eq!(restarted.checkpoint_seq, 8);
        assert_eq!(restarted.transaction_count, 11);

        let guard = shared.read();
        assert_eq!(guard.runtime.read_locks[0].get_value(), 0);
        assert_eq!(guard.runtime.read_locks[1].get_value(), 0);
        for lock in &guard.runtime.read_locks[2..] {
            assert_eq!(lock.get_value(), READMARK_NOT_USED);
        }
        assert!(guard.runtime.frame_cache.lock().is_empty());
        assert!(!guard.metadata.initialized.load(Ordering::Acquire));
    }

    #[test]
    fn test_in_process_coordination_manages_frame_cache() {
        let (shared, _wal) = make_test_wal();
        let coordination = make_test_coordination(&shared);

        // Frames are cached in WAL append order (globally ascending): page 7 at
        // frame 2, page 9 at frame 4, page 7 again at frame 5.
        coordination.cache_frame(7, 2);
        coordination.cache_frame(9, 4);
        coordination.cache_frame(7, 5);

        assert_eq!(coordination.find_frame(7, 0, 5, None), Some(5));
        assert_eq!(coordination.iter_latest_frames(0, 5), vec![(7, 5), (9, 4)]);

        coordination.rollback_cache(4);

        assert_eq!(coordination.find_frame(7, 0, 5, None), Some(2));
        assert_eq!(coordination.iter_latest_frames(0, 5), vec![(7, 2), (9, 4)]);
        assert_eq!(
            shared.read().runtime.frame_cache.lock().get(&7),
            Some(&vec![2])
        );
    }

    /// Regression test for WAL frame-index aliasing corruption: when a WAL
    /// frame slot is reused for a different page (the append position rewinds
    /// to an already-cached frame — e.g. an aborted/uncommitted append's slots
    /// being overwritten, or a different connection reusing the slots), the
    /// stale `page -> frame` mapping for that slot must be purged. Otherwise
    /// `find_frame` can hand a page a frame number whose slot now physically
    /// holds a different page, and the reader gets the wrong page's bytes
    /// (surfacing as "non-index page" / "Invalid page type" / corruption).
    #[test]
    fn cache_frame_purges_stale_mapping_on_frame_slot_reuse() {
        let (shared, _wal) = make_test_wal();
        let coordination = make_test_coordination(&shared);

        // Ascending append: page 7 @3, page 9 @4, page 7 @5.
        coordination.cache_frame(7, 3);
        coordination.cache_frame(9, 4);
        coordination.cache_frame(7, 5);
        assert_eq!(coordination.find_frame(9, 0, 10, None), Some(4));

        // The append position rewinds and frame slots 4 and 5 are overwritten,
        // now belonging to page 11 (@4) and page 13 (@5). The earlier owners of
        // those slots (page 9 @4, page 7 @5) must no longer be reachable.
        coordination.cache_frame(11, 4);
        coordination.cache_frame(13, 5);

        assert_eq!(
            coordination.find_frame(9, 0, 10, None),
            None,
            "stale page 9 -> frame 4 mapping must be purged once slot 4 is reused"
        );
        assert_eq!(
            coordination.find_frame(11, 0, 10, None),
            Some(4),
            "page 11 now owns frame slot 4"
        );
        assert_eq!(
            coordination.find_frame(13, 0, 10, None),
            Some(5),
            "page 13 now owns frame slot 5"
        );
        // Page 7's still-valid lower frame (3) survives; its stale 5 is gone.
        assert_eq!(coordination.find_frame(7, 0, 10, None), Some(3));
    }

    #[test]
    fn test_savepoint_rollback_discards_frame_cache_past_rollback_point() {
        let (shared, wal) = make_test_wal();
        let coordination = make_test_coordination(&shared);
        set_shared_snapshot(
            &shared,
            WalSnapshot {
                max_frame: 25,
                nbackfills: 0,
                last_checksum: (55, 89),
                checkpoint_seq: 1,
                transaction_count: 3,
            },
        );

        // The connection spilled uncommitted frames past the committed
        // high-water mark (25); a savepoint opened mid-transaction recorded
        // frame 27.
        coordination.cache_frame(7, 10);
        coordination.cache_frame(9, 26);
        coordination.cache_frame(11, 28);
        wal.max_frame.store(30, Ordering::Release);

        wal.rollback(Some(RollbackTo {
            frame: 27,
            checksum: (13, 21),
            checkpoint_seq: 1,
        }));

        // Mappings at or below the rollback point survive, later ones are
        // discarded; frames 26..=27 remain as unpublished spills.
        assert_eq!(coordination.find_frame(7, 0, 30, None), Some(10));
        assert_eq!(coordination.find_frame(9, 0, 30, None), Some(26));
        assert_eq!(coordination.find_frame(11, 0, 30, None), None);
        assert_eq!(wal.get_max_frame(), 27);
        assert_eq!(*wal.last_checksum.read(), (13, 21));

        // Rolling back to the committed high-water mark discards every
        // spill.
        wal.rollback(Some(RollbackTo {
            frame: 25,
            checksum: (55, 89),
            checkpoint_seq: 1,
        }));
        assert_eq!(coordination.find_frame(9, 0, 30, None), None);
        assert_eq!(wal.get_max_frame(), 25);
    }

    #[test]
    fn test_in_process_coordination_transaction_guards() {
        let (shared, _wal) = make_test_wal();
        let coordination = make_test_coordination(&shared);

        let db_file_snapshot = WalSnapshot {
            max_frame: 0,
            nbackfills: 0,
            last_checksum: (0, 0),
            checkpoint_seq: 0,
            transaction_count: 0,
        };
        set_shared_snapshot(&shared, db_file_snapshot);
        let read_guard = coordination.try_begin_read_tx(db_file_snapshot).unwrap();
        assert_eq!(read_guard, ReadGuardKind::DbFile);
        coordination.end_read_tx(read_guard);

        let wal_snapshot = WalSnapshot {
            max_frame: 5,
            nbackfills: 2,
            last_checksum: (11, 13),
            checkpoint_seq: 1,
            transaction_count: 2,
        };
        set_shared_snapshot(&shared, wal_snapshot);
        let read_guard = coordination.try_begin_read_tx(wal_snapshot).unwrap();
        assert!(matches!(read_guard, ReadGuardKind::ReadMark(_)));
        coordination.end_read_tx(read_guard);

        assert!(coordination.try_begin_write_tx());
        assert!(!coordination.try_begin_write_tx());
        coordination.end_write_tx();
    }

    #[cfg(host_shared_wal)]
    #[test]
    #[cfg_attr(
        all(target_os = "windows", not(feature = "experimental_win_iocp")),
        ignore = "shared WAL coordination requires the experimental Windows IOCP backend"
    )]
    fn test_shm_coordination_uses_shared_authority() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.db-wal");
        let shm_path = dir.path().join("test.db-tshm");
        let io = shared_wal_test_io();
        let file_a = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let file_b = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared_a = WalFileShared::new_shared(file_a).unwrap();
        let shared_b = WalFileShared::new_shared(file_b).unwrap();
        let snapshot = WalSnapshot {
            max_frame: 14,
            nbackfills: 8,
            last_checksum: (31, 37),
            checkpoint_seq: 5,
            transaction_count: 9,
        };
        set_shared_snapshot(&shared_a, snapshot);
        {
            let shared = shared_a.write();
            let mut header = shared.metadata.wal_header.lock();
            header.page_size = 4096;
            header.salt_1 = 17;
            header.salt_2 = 23;
            header.checksum_1 = snapshot.last_checksum.0;
            header.checksum_2 = snapshot.last_checksum.1;
        }

        let (_authority_a, coordination_a) = make_test_shm_coordination(&shared_a, &shm_path);
        let (authority_b, coordination_b) = make_test_shm_coordination(&shared_b, &shm_path);
        coordination_a.cache_frame(7, 2);
        coordination_a.cache_frame(9, 4);
        coordination_a.cache_frame(7, 5);

        assert_eq!(coordination_b.load_snapshot(), snapshot);
        assert_eq!(coordination_b.wal_header().page_size, 4096);
        assert_eq!(coordination_b.wal_header().salt_1, 17);
        assert_eq!(coordination_b.wal_header().salt_2, 23);
        assert_eq!(coordination_b.find_frame(7, 0, 5, None), Some(5));
        assert_eq!(
            coordination_b.iter_latest_frames(0, 5),
            vec![(7, 5), (9, 4)]
        );
        assert_eq!(coordination_a.checkpoint_epoch(), 0);
        assert_eq!(coordination_b.bump_checkpoint_epoch(), 0);
        assert_eq!(coordination_a.checkpoint_epoch(), 1);

        assert!(coordination_a.try_begin_write_tx());
        assert!(!coordination_b.try_begin_write_tx());
        coordination_a.end_write_tx();

        let read_guard = coordination_a.try_begin_read_tx(snapshot).unwrap();
        assert_eq!(
            authority_b.min_active_reader_frame(),
            Some(snapshot.max_frame)
        );
        coordination_a.end_read_tx(read_guard);
        assert_eq!(authority_b.min_active_reader_frame(), None);

        coordination_b.publish_commit(WalCommitState {
            max_frame: 21,
            last_checksum: (55, 89),
            transaction_count: 10,
        });
        assert_eq!(
            coordination_a.load_snapshot(),
            WalSnapshot {
                max_frame: 21,
                nbackfills: 8,
                last_checksum: (55, 89),
                checkpoint_seq: 5,
                transaction_count: 10,
            }
        );

        coordination_b.rollback_cache(4);
        assert_eq!(coordination_a.find_frame(7, 0, 5, None), Some(2));
        assert_eq!(
            coordination_a.iter_latest_frames(0, 5),
            vec![(7, 2), (9, 4)]
        );
        assert!(shm_path.exists());
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_shm_coordination_many_same_snapshot_readers_share_one_published_slot() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test-many-same-snapshot-readers.db-wal");
        let shm_path = dir.path().join("test-many-same-snapshot-readers.db-tshm");
        let io = shared_wal_test_io();
        let file = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared = WalFileShared::new_shared(file).unwrap();
        let snapshot = WalSnapshot {
            max_frame: 9,
            nbackfills: 2,
            last_checksum: (31, 37),
            checkpoint_seq: 5,
            transaction_count: 9,
        };
        set_shared_snapshot(&shared, snapshot);
        {
            let shared = shared.write();
            let mut header = shared.metadata.wal_header.lock();
            header.page_size = 4096;
            header.salt_1 = 17;
            header.salt_2 = 23;
            header.checksum_1 = snapshot.last_checksum.0;
            header.checksum_2 = snapshot.last_checksum.1;
        }

        let authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        let mut readers = Vec::new();
        for _ in 0..128 {
            let coordination = ShmWalCoordination::new(shared.clone(), authority.clone());
            let read_guard = coordination
                .try_begin_read_tx(snapshot)
                .expect("same-snapshot readers should share a published reader barrier");
            readers.push((coordination, read_guard));
        }

        assert_eq!(
            authority.min_active_reader_frame(),
            Some(snapshot.max_frame)
        );
        assert_eq!(
            active_shared_reader_slot_count(&authority),
            1,
            "same-snapshot readers should collapse onto one shared reader slot"
        );

        for (coordination, read_guard) in readers {
            coordination.end_read_tx(read_guard);
        }
        assert_eq!(authority.min_active_reader_frame(), None);
        assert_eq!(active_shared_reader_slot_count(&authority), 0);
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_shm_coordination_uses_one_published_slot_per_active_snapshot_generation() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test-mixed-snapshot-readers.db-wal");
        let shm_path = dir.path().join("test-mixed-snapshot-readers.db-tshm");
        let io = shared_wal_test_io();
        let file = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared = WalFileShared::new_shared(file).unwrap();
        let snapshot_a = WalSnapshot {
            max_frame: 5,
            nbackfills: 2,
            last_checksum: (31, 37),
            checkpoint_seq: 5,
            transaction_count: 9,
        };
        set_shared_snapshot(&shared, snapshot_a);
        {
            let shared = shared.write();
            let mut header = shared.metadata.wal_header.lock();
            header.page_size = 4096;
            header.salt_1 = 17;
            header.salt_2 = 23;
            header.checksum_1 = snapshot_a.last_checksum.0;
            header.checksum_2 = snapshot_a.last_checksum.1;
        }

        let authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        let reader_a1 = ShmWalCoordination::new(shared.clone(), authority.clone());
        let guard_a1 = reader_a1.try_begin_read_tx(snapshot_a).unwrap();
        let reader_a2 = ShmWalCoordination::new(shared.clone(), authority.clone());
        let guard_a2 = reader_a2.try_begin_read_tx(snapshot_a).unwrap();
        assert_eq!(
            authority.min_active_reader_frame(),
            Some(snapshot_a.max_frame)
        );
        assert_eq!(active_shared_reader_slot_count(&authority), 1);

        let snapshot_b = WalSnapshot {
            max_frame: 9,
            nbackfills: 2,
            last_checksum: (41, 43),
            checkpoint_seq: 5,
            transaction_count: 10,
        };
        reader_a1.publish_commit(WalCommitState {
            max_frame: snapshot_b.max_frame,
            last_checksum: snapshot_b.last_checksum,
            transaction_count: snapshot_b.transaction_count,
        });

        let reader_b1 = ShmWalCoordination::new(shared.clone(), authority.clone());
        let guard_b1 = reader_b1.try_begin_read_tx(snapshot_b).unwrap();
        let reader_b2 = ShmWalCoordination::new(shared, authority.clone());
        let guard_b2 = reader_b2.try_begin_read_tx(snapshot_b).unwrap();

        assert_eq!(
            active_shared_reader_slot_count(&authority),
            2,
            "distinct live snapshots should each publish one shared reader slot"
        );
        assert_eq!(
            authority.min_active_reader_frame(),
            Some(snapshot_a.max_frame),
            "checkpoint barrier should stay pinned to the oldest active snapshot"
        );

        reader_a1.end_read_tx(guard_a1);
        reader_a2.end_read_tx(guard_a2);
        assert_eq!(
            authority.min_active_reader_frame(),
            Some(snapshot_b.max_frame)
        );
        assert_eq!(active_shared_reader_slot_count(&authority), 1);

        reader_b1.end_read_tx(guard_b1);
        reader_b2.end_read_tx(guard_b2);
        assert_eq!(authority.min_active_reader_frame(), None);
        assert_eq!(active_shared_reader_slot_count(&authority), 0);
    }

    #[cfg(host_shared_wal)]
    #[test]
    #[cfg_attr(
        all(target_os = "windows", not(feature = "experimental_win_iocp")),
        ignore = "shared WAL coordination requires the experimental Windows IOCP backend"
    )]
    fn test_shm_coordination_shared_index_grows_past_old_fixed_limit() {
        const OLD_FIXED_LIMIT: u64 = 65_536;

        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.db-wal");
        let shm_path = dir.path().join("test.db-tshm");
        let io = shared_wal_test_io();
        let file_a = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let file_b = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared_a = WalFileShared::new_shared(file_a).unwrap();
        let shared_b = WalFileShared::new_shared(file_b).unwrap();
        let snapshot = WalSnapshot {
            max_frame: OLD_FIXED_LIMIT + 2,
            nbackfills: 0,
            last_checksum: (31, 37),
            checkpoint_seq: 5,
            transaction_count: OLD_FIXED_LIMIT + 2,
        };
        set_shared_snapshot(&shared_a, snapshot);
        {
            let shared = shared_a.write();
            let mut header = shared.metadata.wal_header.lock();
            header.page_size = 4096;
            header.salt_1 = 17;
            header.salt_2 = 23;
            header.checksum_1 = snapshot.last_checksum.0;
            header.checksum_2 = snapshot.last_checksum.1;
        }

        let (_authority_a, coordination_a) = make_test_shm_coordination(&shared_a, &shm_path);
        let (_authority_b, coordination_b) = make_test_shm_coordination(&shared_b, &shm_path);

        coordination_a.cache_frame(7, 2);
        for frame_id in 3..=OLD_FIXED_LIMIT + 1 {
            coordination_a.cache_frame(100 + (frame_id % 31), frame_id);
        }
        coordination_a.cache_frame(7, OLD_FIXED_LIMIT + 2);

        assert_eq!(
            coordination_b.find_frame(7, 0, OLD_FIXED_LIMIT + 2, None),
            Some(OLD_FIXED_LIMIT + 2)
        );
        assert_eq!(
            coordination_b.find_frame(7, 0, OLD_FIXED_LIMIT + 2, Some(OLD_FIXED_LIMIT + 1)),
            Some(2)
        );
        assert!(coordination_b
            .iter_latest_frames(0, OLD_FIXED_LIMIT + 2)
            .contains(&(7, OLD_FIXED_LIMIT + 2)));
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_shm_coordination_restart_uses_authority_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.db-wal");
        let shm_path = dir.path().join("test.db-tshm");
        let io = PlatformIO::new().unwrap();
        let file_a = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let file_b = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared_a = WalFileShared::new_shared(file_a).unwrap();
        let shared_b = WalFileShared::new_shared(file_b).unwrap();
        let snapshot = WalSnapshot {
            max_frame: 12,
            nbackfills: 12,
            last_checksum: (31, 37),
            checkpoint_seq: 5,
            transaction_count: 9,
        };
        set_shared_snapshot(&shared_a, snapshot);
        {
            let shared = shared_a.write();
            let mut header = shared.metadata.wal_header.lock();
            header.page_size = 4096;
            header.salt_1 = 17;
            header.salt_2 = 23;
            header.checksum_1 = snapshot.last_checksum.0;
            header.checksum_2 = snapshot.last_checksum.1;
            shared.metadata.initialized.store(true, Ordering::Release);
            shared.runtime.epoch.store(5, Ordering::Release);
        }

        let (_authority_a, coordination_a) = make_test_shm_coordination(&shared_a, &shm_path);
        let (_authority_b, coordination_b) = make_test_shm_coordination(&shared_b, &shm_path);
        coordination_a.cache_frame(7, 2);
        coordination_a.cache_frame(9, 4);

        {
            let mut shared = shared_b.write();
            shared.metadata.max_frame.store(99, Ordering::Release);
            shared.metadata.nbackfills.store(77, Ordering::Release);
            shared.metadata.last_checksum = (1, 2);
            shared
                .metadata
                .transaction_count
                .store(42, Ordering::Release);
            shared.runtime.epoch.store(99, Ordering::Release);
            shared.metadata.initialized.store(true, Ordering::Release);
            let mut header = shared.metadata.wal_header.lock();
            header.checkpoint_seq = 88;
            header.page_size = 2048;
            header.salt_1 = 91;
            header.salt_2 = 92;
            header.checksum_1 = 93;
            header.checksum_2 = 94;
        }

        assert!(coordination_b.fallback.try_read_mark_exclusive(0));
        let restarted = coordination_b.begin_restart(&io).unwrap();
        coordination_b.end_restart();
        coordination_b.fallback.unlock_read_mark(0);

        assert_eq!(
            restarted,
            WalSnapshot {
                max_frame: 0,
                nbackfills: 0,
                last_checksum: snapshot.last_checksum,
                checkpoint_seq: snapshot.checkpoint_seq.wrapping_add(1),
                transaction_count: snapshot.transaction_count,
            }
        );
        assert_eq!(
            coordination_a.load_snapshot(),
            WalSnapshot {
                max_frame: 0,
                nbackfills: 0,
                last_checksum: snapshot.last_checksum,
                checkpoint_seq: snapshot.checkpoint_seq.wrapping_add(1),
                transaction_count: snapshot.transaction_count,
            }
        );
        let header = coordination_a.wal_header();
        assert_eq!(header.page_size, 4096);
        assert_eq!(
            header.checkpoint_seq,
            snapshot.checkpoint_seq.wrapping_add(1)
        );
        assert_eq!(header.salt_1, 18);
        assert_ne!(header.salt_2, 23);
        assert_eq!(header.checksum_1, snapshot.last_checksum.0);
        assert_eq!(header.checksum_2, snapshot.last_checksum.1);
        assert_eq!(coordination_a.iter_latest_frames(0, u64::MAX), Vec::new());
        assert_eq!(coordination_a.checkpoint_epoch(), 5);

        let shared = shared_b.read();
        assert_eq!(shared.metadata.max_frame.load(Ordering::Acquire), 0);
        assert_eq!(shared.metadata.nbackfills.load(Ordering::Acquire), 0);
        assert_eq!(shared.metadata.last_checksum, snapshot.last_checksum);
        assert_eq!(
            shared.metadata.transaction_count.load(Ordering::Acquire),
            snapshot.transaction_count
        );
        assert_eq!(shared.runtime.epoch.load(Ordering::Acquire), 5);
        assert!(!shared.metadata.initialized.load(Ordering::Acquire));
        assert!(shared.runtime.frame_cache.lock().is_empty());
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_shm_coordination_exclusive_reopen_reuses_persisted_authority() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.db-wal");
        let shm_path = dir.path().join("test.db-tshm");
        let io = shared_wal_test_io();

        {
            let file = io
                .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
                .unwrap();
            let shared = WalFileShared::new_shared(file).unwrap();
            let snapshot = WalSnapshot {
                max_frame: 12,
                nbackfills: 8,
                last_checksum: (31, 37),
                checkpoint_seq: 5,
                transaction_count: 9,
            };
            set_shared_snapshot(&shared, snapshot);
            {
                let shared = shared.write();
                let mut header = shared.metadata.wal_header.lock();
                header.page_size = 4096;
                header.salt_1 = 17;
                header.salt_2 = 23;
                header.checksum_1 = snapshot.last_checksum.0;
                header.checksum_2 = snapshot.last_checksum.1;
            }

            let (authority, coordination) = make_test_shm_coordination(&shared, &shm_path);
            coordination.cache_frame(7, 2);
            coordination.cache_frame(7, 5);
            assert_eq!(
                authority.open_mode(),
                SharedWalCoordinationOpenMode::Exclusive
            );
            assert_eq!(coordination.load_snapshot(), snapshot);
            assert_eq!(coordination.find_frame(7, 0, 5, None), Some(5));
        }

        let reopened_file = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let reopened_shared = WalFileShared::new_shared(reopened_file).unwrap();
        let (reopened_authority, reopened_coordination) =
            make_test_shm_coordination(&reopened_shared, &shm_path);

        assert_eq!(
            reopened_authority.open_mode(),
            SharedWalCoordinationOpenMode::Exclusive
        );
        assert_eq!(
            reopened_coordination.load_snapshot(),
            WalSnapshot {
                max_frame: 12,
                nbackfills: 8,
                last_checksum: (31, 37),
                checkpoint_seq: 5,
                transaction_count: 9,
            }
        );
        assert_eq!(
            reopened_coordination.iter_latest_frames(0, u64::MAX),
            vec![(7, 5)]
        );
        assert_eq!(reopened_authority.min_active_reader_frame(), None);
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_open_shared_from_authority_reuses_trusted_snapshot_after_exclusive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.db-wal");
        let shm_path = dir.path().join("test.db-tshm");
        let io = shared_wal_test_io();
        let snapshot = write_test_wal_with_single_commit_frame(&io, &wal_path);
        {
            let authority =
                Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
            authority.install_snapshot(snapshot);
            authority.record_frame(7, 1);
        }
        let reopened_authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        assert_eq!(
            reopened_authority.open_mode(),
            SharedWalCoordinationOpenMode::Exclusive
        );

        let shared = WalFileShared::open_shared_from_authority_if_exists(
            &io,
            wal_path.to_str().unwrap(),
            crate::OpenFlags::Create,
            &reopened_authority,
            &open_test_db_file_for_wal(&io, &wal_path),
        )
        .unwrap();

        let shared = shared.read();
        assert_eq!(shared.metadata.max_frame.load(Ordering::Acquire), 1);
        assert_eq!(shared.metadata.nbackfills.load(Ordering::Acquire), 0);
        assert_eq!(
            shared.metadata.transaction_count.load(Ordering::Acquire),
            snapshot.transaction_count
        );
        assert_eq!(
            shared.metadata.last_checksum,
            (snapshot.checksum_1, snapshot.checksum_2)
        );
        assert_eq!(
            shared.runtime.epoch.load(Ordering::Acquire),
            snapshot.checkpoint_epoch
        );
        assert!(shared.metadata.initialized.load(Ordering::Acquire));
        assert!(!shared
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire));
        assert!(shared.runtime.frame_cache.lock().is_empty());
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_shm_coordination_live_overflow_returns_busy_without_runtime_disk_scan() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test-live-overflow.db-wal");
        let shm_path = dir.path().join("test-live-overflow.db-tshm");
        let io = shared_wal_test_io();
        let snapshot = write_test_wal_with_single_commit_frame(&io, &wal_path);
        let authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        authority.install_snapshot(snapshot);
        authority.record_frame(7, 1);

        let reopened_authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        let shared = WalFileShared::open_shared_from_authority_if_exists(
            &io,
            wal_path.to_str().unwrap(),
            crate::OpenFlags::Create,
            &reopened_authority,
            &open_test_db_file_for_wal(&io, &wal_path),
        )
        .unwrap();
        assert!(shared.read().runtime.frame_cache.lock().is_empty());

        let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
        buffer_pool.finalize_with_page_size(4096).unwrap();
        let wal = WalFile::new_with_shared_coordination(
            io.clone(),
            shared.clone(),
            reopened_authority.clone(),
            ((0, 0), 0),
            buffer_pool,
        );

        wal.begin_read_tx().unwrap();
        reopened_authority.mark_frame_index_overflowed_for_tests();

        assert!(
            matches!(wal.find_frame(7, None), Err(LimboError::Busy)),
            "page lookup must refuse the overflowed path instead of rescanning the WAL synchronously"
        );
        assert!(
            shared.read().runtime.frame_cache.lock().is_empty(),
            "refusing the overflow refresh must leave the local fallback cache untouched"
        );

        wal.end_read_tx();
        assert!(
            matches!(wal.begin_read_tx(), Err(LimboError::Busy)),
            "new readers must also refuse an uncovered overflowed frame index without blocking"
        );
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_open_shared_from_authority_exclusive_rebuilds_positive_snapshot_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test-exclusive-positive.db-wal");
        let shm_path = dir.path().join("test-exclusive-positive.db-tshm");
        let io = shared_wal_test_io();
        let snapshot = write_test_wal_with_single_commit_frame(&io, &wal_path);
        {
            let authority =
                Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
            authority.install_snapshot(SharedWalCoordinationHeader {
                nbackfills: snapshot.max_frame,
                ..snapshot
            });
            authority.record_frame(7, 1);
            assert_eq!(
                authority.open_mode(),
                SharedWalCoordinationOpenMode::Exclusive
            );
        }

        let reopened_authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        assert_eq!(
            reopened_authority.open_mode(),
            SharedWalCoordinationOpenMode::Exclusive
        );

        let shared = WalFileShared::open_shared_from_authority_if_exists(
            &io,
            wal_path.to_str().unwrap(),
            crate::OpenFlags::Create,
            &reopened_authority,
            &open_test_db_file_for_wal(&io, &wal_path),
        )
        .unwrap();

        let shared = shared.read();
        assert_eq!(shared.metadata.max_frame.load(Ordering::Acquire), 1);
        assert_eq!(shared.metadata.nbackfills.load(Ordering::Acquire), 0);
        assert!(shared
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire));
        assert_eq!(
            shared.runtime.frame_cache.lock().get(&7).cloned(),
            Some(vec![1])
        );
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_shared_coordination_open_uses_reconciled_snapshot_for_local_wal_state() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.db-wal");
        let shm_path = dir.path().join("test.db-tshm");
        let io = shared_wal_test_io();

        let file = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared = WalFileShared::new_shared(file).unwrap();

        let authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        let snapshot = SharedWalCoordinationHeader {
            max_frame: 1,
            nbackfills: 0,
            transaction_count: 9,
            visibility_generation: 3,
            checkpoint_seq: 5,
            checkpoint_epoch: 7,
            page_size: 4096,
            salt_1: 17,
            salt_2: 23,
            checksum_1: 31,
            checksum_2: 37,
            reader_slot_count: 64,
        };
        authority.install_snapshot(snapshot);
        authority.record_frame(7, 1);

        let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
        buffer_pool.finalize_with_page_size(4096).unwrap();
        let wal =
            WalFile::new_with_shared_coordination(io, shared, authority, ((0, 0), 0), buffer_pool);

        assert_eq!(wal.get_max_frame(), 1);
        assert_eq!(wal.get_last_checksum(), (31, 37));
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_open_shared_from_authority_rebuilds_from_disk_when_snapshot_is_stale() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test-stale.db-wal");
        let shm_path = dir.path().join("test-stale.db-tshm");
        let io = shared_wal_test_io();
        let valid_snapshot = write_test_wal_with_single_commit_frame(&io, &wal_path);
        let authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        authority.install_snapshot(SharedWalCoordinationHeader {
            max_frame: 0,
            nbackfills: 0,
            transaction_count: 0,
            visibility_generation: 0,
            checkpoint_seq: valid_snapshot.checkpoint_seq,
            checkpoint_epoch: 0,
            page_size: valid_snapshot.page_size,
            salt_1: valid_snapshot.salt_1,
            salt_2: valid_snapshot.salt_2,
            checksum_1: 0,
            checksum_2: 0,
            reader_slot_count: 64,
        });

        let shared = WalFileShared::open_shared_from_authority_if_exists(
            &io,
            wal_path.to_str().unwrap(),
            crate::OpenFlags::Create,
            &authority,
            &open_test_db_file_for_wal(&io, &wal_path),
        )
        .unwrap();

        let shared = shared.read();
        assert_eq!(shared.metadata.max_frame.load(Ordering::Acquire), 1);
        assert_eq!(
            shared.metadata.last_checksum,
            (valid_snapshot.checksum_1, valid_snapshot.checksum_2)
        );
        assert!(shared
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire));
        assert_eq!(
            shared.runtime.frame_cache.lock().get(&7).cloned(),
            Some(vec![1])
        );
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_open_shared_from_authority_rebuilt_authority_persists_across_exclusive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test-republish.db-wal");
        let shm_path = dir.path().join("test-republish.db-tshm");
        let io = shared_wal_test_io();
        let snapshot = write_test_wal_with_single_commit_frame(&io, &wal_path);
        let authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        authority.install_snapshot(snapshot);

        let exclusive = WalFileShared::open_shared_from_authority_if_exists(
            &io,
            wal_path.to_str().unwrap(),
            crate::OpenFlags::Create,
            &authority,
            &open_test_db_file_for_wal(&io, &wal_path),
        )
        .unwrap();
        assert!(exclusive
            .read()
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire));
        assert!(
            authority.iter_latest_frames(0, u64::MAX).is_empty(),
            "open_shared_from_authority_if_exists should not republish authority before coordination reconciliation"
        );

        let exclusive_coordination = ShmWalCoordination::new(exclusive, authority.clone());
        assert_eq!(authority.iter_latest_frames(0, u64::MAX), vec![(7, 1)]);
        assert_eq!(exclusive_coordination.find_frame(7, 0, 1, None), Some(1));

        drop(exclusive_coordination);
        drop(authority);

        let reopened_authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        assert_eq!(
            reopened_authority.open_mode(),
            SharedWalCoordinationOpenMode::Exclusive
        );

        let reopened_shared = WalFileShared::open_shared_from_authority_if_exists(
            &io,
            wal_path.to_str().unwrap(),
            crate::OpenFlags::Create,
            &reopened_authority,
            &open_test_db_file_for_wal(&io, &wal_path),
        )
        .unwrap();
        assert!(!reopened_shared
            .read()
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire));
        let reopened_coordination = ShmWalCoordination::new(reopened_shared, reopened_authority);
        assert_eq!(reopened_coordination.find_frame(7, 0, 1, None), Some(1));
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_open_shared_from_authority_exclusive_disk_scan_does_not_downgrade_newer_zero_frame_generation(
    ) {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test-zero-frame-reopen.db-wal");
        let shm_path = dir.path().join("test-zero-frame-reopen.db-tshm");
        let io = shared_wal_test_io();
        let prior_generation = write_test_wal_with_single_commit_frame(&io, &wal_path);
        let authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        let restarted_generation = SharedWalCoordinationHeader {
            max_frame: 0,
            nbackfills: 0,
            transaction_count: prior_generation.transaction_count,
            visibility_generation: prior_generation.visibility_generation,
            checkpoint_seq: prior_generation.checkpoint_seq.wrapping_add(1),
            checkpoint_epoch: prior_generation.checkpoint_epoch,
            page_size: prior_generation.page_size,
            salt_1: prior_generation.salt_1.wrapping_add(1),
            salt_2: prior_generation.salt_2.wrapping_add(1),
            checksum_1: prior_generation.checksum_1,
            checksum_2: prior_generation.checksum_2,
            reader_slot_count: prior_generation.reader_slot_count,
        };
        authority.install_snapshot(restarted_generation);

        let shared = WalFileShared::open_shared_from_authority_if_exists(
            &io,
            wal_path.to_str().unwrap(),
            crate::OpenFlags::Create,
            &authority,
            &open_test_db_file_for_wal(&io, &wal_path),
        )
        .unwrap();
        assert!(shared
            .read()
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire));

        let coordination = ShmWalCoordination::new(shared.clone(), authority.clone());
        let reopened = coordination.load_snapshot();
        assert_eq!(reopened.max_frame, 0);
        assert_eq!(reopened.nbackfills, 0);
        assert_eq!(reopened.checkpoint_seq, restarted_generation.checkpoint_seq);
        assert_eq!(
            authority.snapshot().checkpoint_seq,
            restarted_generation.checkpoint_seq
        );
        assert!(
            !coordination.wal_is_initialized(),
            "preserving a newer zero-frame generation must require the first append to rewrite the WAL header"
        );
        assert!(
            shared.read().runtime.frame_cache.lock().is_empty(),
            "older WAL frames from a prior generation must not survive zero-frame authority recovery"
        );
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_open_shared_from_authority_ignores_unpublished_backfill_proof_after_exclusive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test-unpublished-proof.db-wal");
        let shm_path = dir.path().join("test-unpublished-proof.db-tshm");
        let io = shared_wal_test_io();
        let snapshot = write_test_wal_with_single_commit_frame(&io, &wal_path);
        {
            let authority =
                Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
            authority.install_snapshot(snapshot);
            authority.install_backfill_proof(
                SharedWalCoordinationHeader {
                    nbackfills: snapshot.max_frame,
                    ..snapshot
                },
                11,
                0xAABB_CCDD,
            );
        }
        let reopened_authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        assert_eq!(
            reopened_authority.open_mode(),
            SharedWalCoordinationOpenMode::Exclusive
        );

        let shared = WalFileShared::open_shared_from_authority_if_exists(
            &io,
            wal_path.to_str().unwrap(),
            crate::OpenFlags::Create,
            &reopened_authority,
            &open_test_db_file_for_wal(&io, &wal_path),
        )
        .unwrap();

        let shared = shared.read();
        assert_eq!(shared.metadata.max_frame.load(Ordering::Acquire), 1);
        assert_eq!(shared.metadata.nbackfills.load(Ordering::Acquire), 0);
        assert!(shared
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire));
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_restart_checkpoint_clears_backfill_proof_and_later_replaces_it() {
        let (db, path) = get_database();
        let wal_path = path.path().join("test.db-wal");
        let wal_path_str = wal_path.to_str().unwrap();
        let conn = db.connect().unwrap();
        conn.wal_auto_actions_disable();
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 8, 2);

        let pager = conn.pager.load();
        let partial = run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: Some(1),
            },
        );
        assert!(
            partial.wal_total_backfilled > 0 && !partial.everything_backfilled(),
            "setup must create a partial checkpoint with a positive durable backfill proof"
        );

        let authority = db.shared_wal_coordination().unwrap().unwrap();
        let snapshot_before_restart = authority.snapshot();
        let (db_size_before, db_crc_before) =
            super::read_database_identity_from_file_path(&db.io, wal_path_str)
                .unwrap()
                .unwrap();
        assert!(
            authority.validate_backfill_proof(
                snapshot_before_restart,
                db_size_before,
                db_crc_before
            ),
            "setup must install a valid proof before RESTART"
        );

        let restart = run_checkpoint_until_done(&pager, CheckpointMode::Restart);
        assert!(
            restart.everything_backfilled(),
            "RESTART should fully backfill before resetting the WAL generation"
        );

        let snapshot_after_restart = authority.snapshot();
        assert_eq!(snapshot_after_restart.max_frame, 0);
        assert_eq!(snapshot_after_restart.nbackfills, 0);
        assert!(
            !authority.validate_backfill_proof(
                snapshot_before_restart,
                db_size_before,
                db_crc_before
            ),
            "RESTART must clear the proof for the old WAL generation"
        );

        bulk_inserts(&conn, 6, 2);
        let replacement = run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: Some(1),
            },
        );
        assert!(
            replacement.wal_total_backfilled > 0 && !replacement.everything_backfilled(),
            "replacement setup must create a new partial checkpoint after RESTART"
        );

        let snapshot_after_replacement = authority.snapshot();
        let (db_size_after, db_crc_after) =
            super::read_database_identity_from_file_path(&db.io, wal_path_str)
                .unwrap()
                .unwrap();
        assert!(
            authority.validate_backfill_proof(
                snapshot_after_replacement,
                db_size_after,
                db_crc_after
            ),
            "partial checkpoint after RESTART must install a replacement proof for the new generation"
        );
        assert_ne!(
            snapshot_after_replacement.checkpoint_seq, snapshot_before_restart.checkpoint_seq,
            "replacement proof must belong to the restarted WAL generation"
        );
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_truncate_checkpoint_clears_backfill_proof_and_later_replaces_it() {
        let (db, path) = get_database();
        let wal_path = path.path().join("test.db-wal");
        let wal_path_str = wal_path.to_str().unwrap();
        let conn = db.connect().unwrap();
        conn.wal_auto_actions_disable();
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 8, 2);

        let pager = conn.pager.load();
        let partial = run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: Some(1),
            },
        );
        assert!(
            partial.wal_total_backfilled > 0 && !partial.everything_backfilled(),
            "setup must create a partial checkpoint with a positive durable backfill proof"
        );

        let authority = db.shared_wal_coordination().unwrap().unwrap();
        let snapshot_before_truncate = authority.snapshot();
        let (db_size_before, db_crc_before) =
            super::read_database_identity_from_file_path(&db.io, wal_path_str)
                .unwrap()
                .unwrap();
        assert!(
            authority.validate_backfill_proof(
                snapshot_before_truncate,
                db_size_before,
                db_crc_before
            ),
            "setup must install a valid proof before TRUNCATE"
        );

        let truncate = run_checkpoint_until_done(
            &pager,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );
        assert!(
            truncate.everything_backfilled(),
            "TRUNCATE should fully backfill before truncating the WAL"
        );

        let snapshot_after_truncate = authority.snapshot();
        assert_eq!(snapshot_after_truncate.max_frame, 0);
        assert_eq!(snapshot_after_truncate.nbackfills, 0);
        assert!(
            !authority.validate_backfill_proof(
                snapshot_before_truncate,
                db_size_before,
                db_crc_before
            ),
            "TRUNCATE must clear the proof for the truncated WAL generation"
        );
        assert_eq!(
            std::fs::metadata(&wal_path).unwrap().len(),
            0,
            "TRUNCATE must leave the WAL file empty before the new generation begins"
        );

        bulk_inserts(&conn, 6, 2);
        let replacement = run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: Some(1),
            },
        );
        assert!(
            replacement.wal_total_backfilled > 0 && !replacement.everything_backfilled(),
            "replacement setup must create a new partial checkpoint after TRUNCATE"
        );

        let snapshot_after_replacement = authority.snapshot();
        let (db_size_after, db_crc_after) =
            super::read_database_identity_from_file_path(&db.io, wal_path_str)
                .unwrap()
                .unwrap();
        assert!(
            authority.validate_backfill_proof(
                snapshot_after_replacement,
                db_size_after,
                db_crc_after
            ),
            "partial checkpoint after TRUNCATE must install a replacement proof for the new generation"
        );
        assert_ne!(
            snapshot_after_replacement.checkpoint_seq, snapshot_before_truncate.checkpoint_seq,
            "replacement proof must belong to the truncated WAL generation"
        );
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_classify_authority_snapshot_marks_truncated_wal_for_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test-truncated.db-wal");
        let io = shared_wal_test_io();
        let snapshot = write_test_wal_with_single_commit_frame(&io, &wal_path);

        let wal_len = std::fs::metadata(&wal_path).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .unwrap()
            .set_len(wal_len - 1)
            .unwrap();

        assert_eq!(
            classify_authority_snapshot_against_wal(
                &io,
                &io.open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
                    .unwrap(),
                snapshot,
            )
            .unwrap(),
            AuthoritySnapshotValidation::RebuildFromDisk(
                AuthoritySnapshotRebuildReason::WalLengthMismatch
            )
        );
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_classify_authority_snapshot_marks_corrupt_header_for_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test-corrupt-header.db-wal");
        let io = shared_wal_test_io();
        std::fs::write(&wal_path, [0u8; WAL_HEADER_SIZE]).unwrap();

        let snapshot = SharedWalCoordinationHeader {
            max_frame: 0,
            nbackfills: 0,
            transaction_count: 9,
            visibility_generation: 1,
            checkpoint_seq: 5,
            checkpoint_epoch: 7,
            page_size: 4096,
            salt_1: 17,
            salt_2: 23,
            checksum_1: 31,
            checksum_2: 37,
            reader_slot_count: 64,
        };

        assert_eq!(
            classify_authority_snapshot_against_wal(
                &io,
                &io.open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
                    .unwrap(),
                snapshot,
            )
            .unwrap(),
            AuthoritySnapshotValidation::RebuildFromDisk(
                AuthoritySnapshotRebuildReason::WalHeaderUnreadable
            )
        );
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_open_shared_from_authority_keeps_zero_length_wal_uninitialized_after_exclusive_reopen()
    {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test-empty.db-wal");
        let shm_path = dir.path().join("test-empty.db-tshm");
        let io = shared_wal_test_io();

        io.open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        {
            let authority =
                Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
            authority.install_snapshot(SharedWalCoordinationHeader {
                max_frame: 0,
                nbackfills: 0,
                transaction_count: 9,
                visibility_generation: 1,
                checkpoint_seq: 5,
                checkpoint_epoch: 7,
                page_size: 4096,
                salt_1: 17,
                salt_2: 23,
                checksum_1: 31,
                checksum_2: 37,
                reader_slot_count: 64,
            });
        }
        let reopened_authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        assert_eq!(
            reopened_authority.open_mode(),
            SharedWalCoordinationOpenMode::Exclusive
        );

        let shared = WalFileShared::open_shared_from_authority_if_exists(
            &io,
            wal_path.to_str().unwrap(),
            crate::OpenFlags::Create,
            &reopened_authority,
            &open_test_db_file_for_wal(&io, &wal_path),
        )
        .unwrap();

        let shared = shared.read();
        assert_eq!(shared.metadata.max_frame.load(Ordering::Acquire), 0);
        assert_eq!(shared.metadata.last_checksum, (31, 37));
        assert!(!shared.metadata.initialized.load(Ordering::Acquire));
        assert!(!shared
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire));
    }

    #[cfg(host_shared_wal)]
    #[test]
    #[cfg_attr(
        all(target_os = "windows", not(feature = "experimental_win_iocp")),
        ignore = "shared WAL coordination requires the experimental Windows IOCP backend"
    )]
    fn test_shm_coordination_secondary_disk_scan_does_not_reseed_authority_while_writer_active() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.db-wal");
        let shm_path = dir.path().join("test.db-tshm");
        let io = shared_wal_test_io();

        let file_a = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared_a = WalFileShared::new_shared(file_a).unwrap();
        let authoritative = WalSnapshot {
            max_frame: 5,
            nbackfills: 0,
            last_checksum: (31, 37),
            checkpoint_seq: 5,
            transaction_count: 9,
        };
        set_shared_snapshot(&shared_a, authoritative);
        {
            let shared = shared_a.write();
            let mut header = shared.metadata.wal_header.lock();
            header.page_size = 4096;
            header.salt_1 = 17;
            header.salt_2 = 23;
            header.checksum_1 = authoritative.last_checksum.0;
            header.checksum_2 = authoritative.last_checksum.1;
        }
        let (authority, coordination_a) = make_test_shm_coordination(&shared_a, &shm_path);
        coordination_a.cache_frame(7, 2);
        coordination_a.cache_frame(7, 5);
        assert!(authority.try_acquire_writer(authority.owner_record()));

        let file_b = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared_b = WalFileShared::new_shared(file_b).unwrap();
        let stale = WalSnapshot {
            max_frame: 2,
            nbackfills: 0,
            last_checksum: (11, 13),
            checkpoint_seq: 4,
            transaction_count: 3,
        };
        set_shared_snapshot(&shared_b, stale);
        {
            let shared = shared_b.write();
            shared
                .metadata
                .loaded_from_disk_scan
                .store(true, Ordering::Release);
            let mut header = shared.metadata.wal_header.lock();
            header.page_size = 4096;
            header.salt_1 = 17;
            header.salt_2 = 23;
            header.checksum_1 = stale.last_checksum.0;
            header.checksum_2 = stale.last_checksum.1;
            shared.runtime.frame_cache.lock().insert(7, vec![2]);
        }

        let (_authority_b, coordination_b) = make_test_shm_coordination(&shared_b, &shm_path);

        assert_eq!(coordination_b.load_snapshot(), authoritative);
        assert_eq!(authority.snapshot().max_frame, authoritative.max_frame);
        assert_eq!(
            authority.snapshot().transaction_count,
            authoritative.transaction_count
        );
        assert_eq!(coordination_b.find_frame(7, 0, 5, None), Some(5));
        authority.release_writer(authority.owner_record());
    }

    #[cfg(host_shared_wal)]
    #[test]
    #[cfg_attr(
        all(target_os = "windows", not(feature = "experimental_win_iocp")),
        ignore = "shared WAL coordination requires the experimental Windows IOCP backend"
    )]
    fn test_shm_coordination_disk_scan_matching_authority_keeps_frame_index() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.db-wal");
        let shm_path = dir.path().join("test.db-tshm");
        let io = shared_wal_test_io();

        let file_a = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared_a = WalFileShared::new_shared(file_a).unwrap();
        let authoritative = WalSnapshot {
            max_frame: 5,
            nbackfills: 2,
            last_checksum: (31, 37),
            checkpoint_seq: 5,
            transaction_count: 9,
        };
        set_shared_snapshot(&shared_a, authoritative);
        {
            let shared = shared_a.write();
            let mut header = shared.metadata.wal_header.lock();
            header.page_size = 4096;
            header.salt_1 = 17;
            header.salt_2 = 23;
            header.checksum_1 = authoritative.last_checksum.0;
            header.checksum_2 = authoritative.last_checksum.1;
        }
        let (authority, coordination_a) = make_test_shm_coordination(&shared_a, &shm_path);
        coordination_a.cache_frame(7, 2);
        coordination_a.cache_frame(9, 5);

        let file_b = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared_b = WalFileShared::new_shared(file_b).unwrap();
        set_shared_snapshot(&shared_b, authoritative);
        {
            let shared = shared_b.write();
            shared
                .metadata
                .loaded_from_disk_scan
                .store(true, Ordering::Release);
            let mut header = shared.metadata.wal_header.lock();
            header.page_size = 4096;
            header.salt_1 = 17;
            header.salt_2 = 23;
            header.checksum_1 = authoritative.last_checksum.0;
            header.checksum_2 = authoritative.last_checksum.1;
            let mut frame_cache = shared.runtime.frame_cache.lock();
            frame_cache.insert(7, vec![2]);
            frame_cache.insert(9, vec![5]);
        }

        let (_authority_b, coordination_b) = make_test_shm_coordination(&shared_b, &shm_path);

        let reopened = coordination_b.load_snapshot();
        assert_eq!(reopened.max_frame, authoritative.max_frame);
        assert_eq!(reopened.last_checksum, authoritative.last_checksum);
        assert_eq!(reopened.checkpoint_seq, authoritative.checkpoint_seq);
        assert_eq!(reopened.transaction_count, authoritative.transaction_count);
        assert_eq!(
            reopened.nbackfills, 0,
            "disk-scan reconciliation must preserve the frame index without reviving positive nbackfills"
        );
        assert_eq!(authority.find_frame(7, 0, 5, None), Some(2));
        assert_eq!(authority.find_frame(9, 0, 5, None), Some(5));
        assert_eq!(
            authority.iter_latest_frames(0, authoritative.max_frame),
            vec![(7, 2), (9, 5)]
        );
    }

    #[cfg(host_shared_wal)]
    #[test]
    #[cfg_attr(
        all(target_os = "windows", not(feature = "experimental_win_iocp")),
        ignore = "shared WAL coordination requires the experimental Windows IOCP backend"
    )]
    fn test_shm_coordination_disk_scan_matching_snapshot_rebuilds_stale_frame_index() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.db-wal");
        let shm_path = dir.path().join("test.db-tshm");
        let io = shared_wal_test_io();

        let file_a = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared_a = WalFileShared::new_shared(file_a).unwrap();
        let authoritative = WalSnapshot {
            max_frame: 5,
            nbackfills: 0,
            last_checksum: (31, 37),
            checkpoint_seq: 5,
            transaction_count: 9,
        };
        set_shared_snapshot(&shared_a, authoritative);
        {
            let shared = shared_a.write();
            let mut header = shared.metadata.wal_header.lock();
            header.page_size = 4096;
            header.salt_1 = 17;
            header.salt_2 = 23;
            header.checksum_1 = authoritative.last_checksum.0;
            header.checksum_2 = authoritative.last_checksum.1;
        }
        {
            let (_authority, coordination_a) = make_test_shm_coordination(&shared_a, &shm_path);
            coordination_a.cache_frame(7, 2);
            coordination_a.cache_frame(9, 4);
        }

        let file_b = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared_b = WalFileShared::new_shared(file_b).unwrap();
        set_shared_snapshot(&shared_b, authoritative);
        {
            let shared = shared_b.write();
            shared
                .metadata
                .loaded_from_disk_scan
                .store(true, Ordering::Release);
            let mut header = shared.metadata.wal_header.lock();
            header.page_size = 4096;
            header.salt_1 = 17;
            header.salt_2 = 23;
            header.checksum_1 = authoritative.last_checksum.0;
            header.checksum_2 = authoritative.last_checksum.1;
            shared.runtime.frame_cache.lock().insert(7, vec![2]);
            shared.runtime.frame_cache.lock().insert(9, vec![5]);
        }

        let (authority, coordination_b) = make_test_shm_coordination(&shared_b, &shm_path);
        assert_eq!(
            authority.open_mode(),
            SharedWalCoordinationOpenMode::Exclusive
        );

        let reopened = coordination_b.load_snapshot();
        assert_eq!(reopened.max_frame, authoritative.max_frame);
        assert_eq!(reopened.last_checksum, authoritative.last_checksum);
        assert_eq!(reopened.checkpoint_seq, authoritative.checkpoint_seq);
        assert_eq!(reopened.transaction_count, authoritative.transaction_count);
        assert_eq!(authority.find_frame(7, 0, 5, None), Some(2));
        assert_eq!(
            authority.find_frame(9, 0, 5, None),
            Some(5),
            "matching snapshot metadata must not preserve a stale shared frame index across restart recovery"
        );
        assert_eq!(
            authority.iter_latest_frames(0, authoritative.max_frame),
            vec![(7, 2), (9, 5)]
        );
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_shm_coordination_empty_disk_scan_keeps_zero_frame_authority_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.db-wal");
        let shm_path = dir.path().join("test.db-tshm");
        let io = shared_wal_test_io();

        let authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        let authoritative = SharedWalCoordinationHeader {
            max_frame: 0,
            nbackfills: 0,
            transaction_count: 9,
            visibility_generation: 3,
            checkpoint_seq: 5,
            checkpoint_epoch: 7,
            page_size: 4096,
            salt_1: 17,
            salt_2: 23,
            checksum_1: 31,
            checksum_2: 37,
            reader_slot_count: 64,
        };
        authority.install_snapshot(authoritative);

        let file = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared = WalFileShared::new_shared(file).unwrap();
        {
            let shared = shared.write();
            shared
                .metadata
                .loaded_from_disk_scan
                .store(true, Ordering::Release);
        }

        let coordination = ShmWalCoordination::new(shared, authority.clone());
        let snapshot = authority.snapshot();
        assert_eq!(snapshot, authoritative);
        let header = coordination.wal_header();
        assert_eq!(header.page_size, 4096);
        assert_eq!(header.checkpoint_seq, authoritative.checkpoint_seq);
        assert_eq!(header.salt_1, authoritative.salt_1);
        assert_eq!(header.salt_2, authoritative.salt_2);
    }

    #[cfg(host_shared_wal)]
    #[test]
    #[cfg_attr(
        all(target_os = "windows", not(feature = "experimental_win_iocp")),
        ignore = "shared WAL coordination requires the experimental Windows IOCP backend"
    )]
    fn test_shm_coordination_empty_disk_scan_does_not_clobber_positive_authority() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.db-wal");
        let shm_path = dir.path().join("test.db-tshm");
        let io = shared_wal_test_io();

        let file_a = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared_a = WalFileShared::new_shared(file_a).unwrap();
        let authoritative = WalSnapshot {
            max_frame: 5,
            nbackfills: 0,
            last_checksum: (31, 37),
            checkpoint_seq: 5,
            transaction_count: 9,
        };
        set_shared_snapshot(&shared_a, authoritative);
        {
            let shared = shared_a.write();
            let mut header = shared.metadata.wal_header.lock();
            header.page_size = 4096;
            header.salt_1 = 17;
            header.salt_2 = 23;
            header.checksum_1 = authoritative.last_checksum.0;
            header.checksum_2 = authoritative.last_checksum.1;
        }
        let (authority, coordination_a) = make_test_shm_coordination(&shared_a, &shm_path);
        coordination_a.cache_frame(7, 2);
        coordination_a.cache_frame(9, 5);

        let file_b = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared_b = WalFileShared::new_shared(file_b).unwrap();
        {
            let shared = shared_b.write();
            shared
                .metadata
                .loaded_from_disk_scan
                .store(true, Ordering::Release);
            let mut header = shared.metadata.wal_header.lock();
            header.page_size = 4096;
            header.checkpoint_seq = authoritative.checkpoint_seq;
            header.salt_1 = 17;
            header.salt_2 = 23;
            header.checksum_1 = 11;
            header.checksum_2 = 13;
        }

        let (_authority_b, coordination_b) = make_test_shm_coordination(&shared_b, &shm_path);
        let reopened = coordination_b.load_snapshot();
        assert_eq!(reopened.max_frame, authoritative.max_frame);
        assert_eq!(reopened.checkpoint_seq, authoritative.checkpoint_seq);
        assert_eq!(reopened.transaction_count, authoritative.transaction_count);
        assert_eq!(
            authority.find_frame(7, 0, authoritative.max_frame, None),
            Some(2)
        );
        assert_eq!(
            authority.find_frame(9, 0, authoritative.max_frame, None),
            Some(5)
        );
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_shm_zero_frame_authority_invalidates_stale_local_initialized_state() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.db-wal");
        let shm_path = dir.path().join("test.db-tshm");
        let io = shared_wal_test_io();

        let authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        let authoritative = SharedWalCoordinationHeader {
            max_frame: 0,
            nbackfills: 0,
            transaction_count: 9,
            visibility_generation: 3,
            checkpoint_seq: 5,
            checkpoint_epoch: 7,
            page_size: 4096,
            salt_1: 17,
            salt_2: 23,
            checksum_1: 31,
            checksum_2: 37,
            reader_slot_count: 64,
        };
        authority.install_snapshot(authoritative);

        let file = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared = WalFileShared::new_shared(file).unwrap();
        {
            let mut shared = shared.write();
            shared.metadata.max_frame.store(11, Ordering::Release);
            shared.metadata.nbackfills.store(11, Ordering::Release);
            shared.metadata.last_checksum = (11, 13);
            shared
                .metadata
                .transaction_count
                .store(3, Ordering::Release);
            shared.metadata.initialized.store(true, Ordering::Release);
            let mut header = shared.metadata.wal_header.lock();
            header.checkpoint_seq = 2;
            header.page_size = 4096;
            header.salt_1 = 7;
            header.salt_2 = 13;
            header.checksum_1 = 11;
            header.checksum_2 = 13;
            shared.runtime.epoch.store(1, Ordering::Release);
        }

        let coordination = ShmWalCoordination::new(shared.clone(), authority);
        assert!(
            !coordination.wal_is_initialized(),
            "a stale local initialized bit must not suppress the first header rewrite after RESTART/TRUNCATE"
        );
        {
            let shared = shared.read();
            assert!(
                !shared.metadata.initialized.load(Ordering::Acquire),
                "stale local initialized state must be cleared"
            );
            let header = shared.metadata.wal_header.lock();
            assert_eq!(header.checkpoint_seq, authoritative.checkpoint_seq);
            assert_eq!(header.page_size, authoritative.page_size);
            assert_eq!(header.salt_1, authoritative.salt_1);
            assert_eq!(header.salt_2, authoritative.salt_2);
            assert_eq!(header.checksum_1, authoritative.checksum_1);
            assert_eq!(header.checksum_2, authoritative.checksum_2);
        }

        let prepared = coordination
            .prepare_wal_header(io.as_ref(), PageSize::new(4096).unwrap())
            .expect("zero-frame authority should force a header rewrite");
        assert_eq!(prepared.checkpoint_seq, authoritative.checkpoint_seq);
        coordination.mark_initialized();
        assert!(
            coordination.wal_is_initialized(),
            "once the current-generation header is durably rewritten, wal_is_initialized should succeed"
        );
    }

    #[cfg(host_shared_wal)]
    #[test]
    fn test_shm_prepare_wal_header_seeds_uninitialized_authority_from_prepared_header() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.db-wal");
        let shm_path = dir.path().join("test.db-tshm");
        let io = shared_wal_test_io();

        let authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        let file = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared = WalFileShared::new_shared(file).unwrap();
        let coordination = ShmWalCoordination::new(shared, authority.clone());

        let prepared = coordination
            .prepare_wal_header(io.as_ref(), PageSize::new(4096).unwrap())
            .expect("fresh authority should accept the first prepared header");

        let snapshot = authority.snapshot();
        assert_eq!(
            snapshot.page_size, prepared.page_size,
            "authority must publish the prepared page size for later writers and checkpointers"
        );
        assert_eq!(
            snapshot.checkpoint_seq, prepared.checkpoint_seq,
            "authority must publish the prepared checkpoint generation"
        );
        assert_eq!(snapshot.salt_1, prepared.salt_1);
        assert_eq!(snapshot.salt_2, prepared.salt_2);
    }

    #[cfg(host_shared_wal)]
    #[test]
    #[cfg_attr(
        all(target_os = "windows", not(feature = "experimental_win_iocp")),
        ignore = "shared WAL coordination requires the experimental Windows IOCP backend"
    )]
    fn test_shm_prepare_wal_header_does_not_clobber_zero_frame_authority_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.db-wal");
        let shm_path = dir.path().join("test.db-tshm");
        let io = shared_wal_test_io();

        let file_a = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared_a = WalFileShared::new_shared(file_a).unwrap();
        let authoritative = SharedWalCoordinationHeader {
            max_frame: 0,
            nbackfills: 0,
            transaction_count: 9,
            visibility_generation: 3,
            checkpoint_seq: 5,
            checkpoint_epoch: 7,
            page_size: 4096,
            salt_1: 17,
            salt_2: 23,
            checksum_1: 31,
            checksum_2: 37,
            reader_slot_count: 64,
        };
        {
            let mut shared = shared_a.write();
            shared.metadata.max_frame.store(0, Ordering::Release);
            shared.metadata.nbackfills.store(0, Ordering::Release);
            shared
                .metadata
                .transaction_count
                .store(authoritative.transaction_count, Ordering::Release);
            shared.metadata.last_checksum = (31, 37);
            let mut header = shared.metadata.wal_header.lock();
            header.checkpoint_seq = authoritative.checkpoint_seq;
            header.page_size = authoritative.page_size;
            header.salt_1 = authoritative.salt_1;
            header.salt_2 = authoritative.salt_2;
            header.checksum_1 = authoritative.checksum_1;
            header.checksum_2 = authoritative.checksum_2;
            shared
                .runtime
                .epoch
                .store(authoritative.checkpoint_epoch, Ordering::Release);
            shared.metadata.initialized.store(false, Ordering::Release);
        }
        let authority =
            Arc::new(MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).unwrap());
        authority.install_snapshot(authoritative);

        let file_b = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared_b = WalFileShared::new_shared(file_b).unwrap();
        let coordination_b = ShmWalCoordination::new(shared_b.clone(), authority.clone());
        // Simulate a long-lived process whose process-wide shared WAL metadata
        // fell behind the authority after another process checkpointed and
        // restarted the WAL back to frame 0.
        {
            let mut shared = shared_b.write();
            shared.metadata.max_frame.store(0, Ordering::Release);
            shared.metadata.nbackfills.store(0, Ordering::Release);
            shared.metadata.last_checksum = (11, 13);
            shared
                .metadata
                .transaction_count
                .store(3, Ordering::Release);
            let mut header = shared.metadata.wal_header.lock();
            header.checkpoint_seq = 2;
            header.page_size = 4096;
            header.salt_1 = 17;
            header.salt_2 = 23;
            header.checksum_1 = 11;
            header.checksum_2 = 13;
            shared.runtime.epoch.store(1, Ordering::Release);
            shared.metadata.initialized.store(false, Ordering::Release);
        }

        let page_size = PageSize::new(4096).unwrap();
        let prepared = coordination_b
            .prepare_wal_header(io.as_ref(), page_size)
            .expect("prepare_wal_header should produce a header");

        let snapshot = authority.snapshot();
        assert_eq!(
            snapshot.transaction_count, authoritative.transaction_count,
            "first writer after restart must not downgrade authority transaction_count"
        );
        assert_eq!(
            snapshot.checkpoint_seq, authoritative.checkpoint_seq,
            "first writer after restart must not downgrade checkpoint metadata"
        );
        assert_eq!(
            prepared.checkpoint_seq, authoritative.checkpoint_seq,
            "header written after restart must use authority checkpoint metadata"
        );
        assert_eq!(prepared.page_size, authoritative.page_size);
        assert_eq!(prepared.salt_1, authoritative.salt_1);
        assert_eq!(prepared.salt_2, authoritative.salt_2);
        let refreshed = authority.snapshot();
        assert_eq!(
            refreshed.checksum_1, prepared.checksum_1,
            "preparing the first zero-frame header must refresh the authoritative checksum seed"
        );
        assert_eq!(
            refreshed.checksum_2, prepared.checksum_2,
            "preparing the first zero-frame header must refresh the authoritative checksum seed"
        );
    }

    #[test]
    fn test_in_process_coordination_lock_primitives() {
        let (shared, _wal) = make_test_wal();
        let coordination = make_test_coordination(&shared);

        assert!(coordination.try_checkpoint_lock());
        coordination.unlock_checkpoint_lock();

        assert!(coordination.try_write_lock());
        assert!(!coordination.try_write_lock());
        coordination.unlock_write_lock();

        assert!(coordination.try_read_mark_exclusive(1));
        coordination.set_read_mark_value_exclusive(1, 42);
        assert_eq!(coordination.read_mark_value(1), 42);
        coordination.unlock_read_mark(1);

        assert!(coordination.try_read_mark_shared(1));
        assert!(coordination.try_upgrade_read_mark(1));
        coordination.downgrade_read_mark(1);
        coordination.unlock_read_mark(1);

        // The coordination backend should still observe the shared state underneath.
        assert_eq!(shared.read().runtime.read_locks[1].get_value(), 42);
    }

    #[test]
    fn test_in_process_coordination_prepare_truncate_marks_wal_uninitialized() {
        let io = shared_wal_test_io();
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let file = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared = WalFileShared::new_shared(file).unwrap();
        let coordination = make_test_coordination(&shared);

        shared
            .read()
            .metadata
            .initialized
            .store(true, Ordering::Release);
        let file = coordination.prepare_truncate().unwrap();

        assert!(file.size().is_ok());
        assert!(!shared.read().metadata.initialized.load(Ordering::Acquire));
    }

    #[test]
    fn test_in_process_coordination_exposes_wal_io_state() {
        let io = shared_wal_test_io();
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let file = io
            .open_file(wal_path.to_str().unwrap(), crate::OpenFlags::Create, false)
            .unwrap();
        let shared = WalFileShared::new_shared(file).unwrap();
        let coordination = make_test_coordination(&shared);

        assert!(!coordination.wal_is_initialized());
        assert_eq!(coordination.wal_header().page_size, 0);
        assert!(coordination.wal_file().unwrap().size().is_ok());

        let header = coordination
            .prepare_wal_header(io.as_ref(), PageSize::new(4096).unwrap())
            .unwrap();
        assert_eq!(header.page_size, 4096);
        assert_eq!(
            coordination.load_snapshot().last_checksum,
            (header.checksum_1, header.checksum_2)
        );
        assert!(!coordination.wal_is_initialized());

        coordination.mark_initialized();
        assert!(coordination.wal_is_initialized());
        assert!(coordination
            .prepare_wal_header(io.as_ref(), PageSize::new(4096).unwrap())
            .is_none());
    }

    #[test]
    fn test_vacuum_lock_blocks_new_read_transactions_until_release() {
        let (shared, vacuum_wal) = make_test_wal();
        let reader_wal = make_test_wal_from_shared(shared);

        vacuum_wal.try_begin_vacuum_checkpoint_lock().unwrap();
        vacuum_wal.begin_vacuum_blocking_tx().unwrap();

        assert!(
            matches!(reader_wal.try_begin_read_tx(), TryBeginReadResult::Busy),
            "VACUUM lock should block new WAL readers before they take a read-mark slot"
        );
        assert!(
            !vacuum_wal.holds_read_lock(),
            "exclusive VACUUM snapshot must not masquerade as a read-mark lock"
        );
        assert!(
            vacuum_wal.holds_write_lock(),
            "begin_vacuum_blocking_tx should acquire the source write lock"
        );

        vacuum_wal.end_write_tx();
        vacuum_wal.release_vacuum_lock();
        vacuum_wal.release_vacuum_checkpoint_lock();

        assert!(
            matches!(reader_wal.try_begin_read_tx(), TryBeginReadResult::Ok(_)),
            "reader should start after VACUUM releases the lock"
        );
        reader_wal.end_read_tx();
    }

    #[test]
    fn test_active_reader_blocks_vacuum_exclusive_tx() {
        let (shared, reader_wal) = make_test_wal();
        let vacuum_wal = make_test_wal_from_shared(shared);

        assert!(matches!(
            reader_wal.try_begin_read_tx(),
            TryBeginReadResult::Ok(_)
        ));
        vacuum_wal.try_begin_vacuum_checkpoint_lock().unwrap();

        assert!(
            matches!(vacuum_wal.begin_vacuum_blocking_tx(), Err(LimboError::Busy)),
            "active reader should prevent VACUUM from acquiring its exclusive lock"
        );

        reader_wal.end_read_tx();
        vacuum_wal.begin_vacuum_blocking_tx().unwrap();
        vacuum_wal.end_write_tx();
        vacuum_wal.release_vacuum_lock();
        vacuum_wal.release_vacuum_checkpoint_lock();
    }

    #[test]
    fn test_read_retry_does_not_leak_vacuum_guard_or_block_vacuum() {
        let (shared, _) = make_test_wal();
        let retry_reader = make_test_wal_from_shared(shared.clone());
        let vacuum_wal = make_test_wal_from_shared(shared.clone());

        set_shared_snapshot(
            &shared,
            WalSnapshot {
                max_frame: 5,
                nbackfills: 0,
                last_checksum: (0, 0),
                checkpoint_seq: 0,
                transaction_count: 1,
            },
        );

        for idx in 1..5 {
            assert!(
                shared.read().runtime.read_locks[idx].write(),
                "expected setup to occupy read-mark slot {idx}"
            );
        }

        assert!(
            matches!(retry_reader.try_begin_read_tx(), TryBeginReadResult::Retry),
            "reader should retry when all read-mark slots are transiently unavailable"
        );
        assert!(
            !retry_reader.has_vacuum_read_lock_guard(),
            "retry path must not retain a shared VACUUM lock guard"
        );
        assert_eq!(
            retry_reader
                .max_frame_read_lock_index
                .load(Ordering::Acquire),
            NO_LOCK_HELD,
            "retry path must not retain a read-mark slot"
        );

        for idx in 1..5 {
            shared.read().runtime.read_locks[idx].unlock();
        }

        vacuum_wal.try_begin_vacuum_checkpoint_lock().unwrap();
        vacuum_wal.begin_vacuum_blocking_tx().unwrap();
        vacuum_wal.end_write_tx();
        vacuum_wal.release_vacuum_lock();
        vacuum_wal.release_vacuum_checkpoint_lock();
    }

    #[test]
    fn test_held_vacuum_checkpoint_locks_do_not_release_vacuum_lock() {
        let (shared, vacuum_wal) = make_test_wal();
        let contender_wal = make_test_wal_from_shared(shared);

        vacuum_wal.try_begin_vacuum_checkpoint_lock().unwrap();
        vacuum_wal.begin_vacuum_blocking_tx().unwrap();

        assert!(vacuum_wal.holds_write_lock());
        assert!(!vacuum_wal.holds_read_lock());
        assert!(
            matches!(
                contender_wal.try_begin_vacuum_checkpoint_lock(),
                Err(LimboError::Busy)
            ),
            "held checkpoint lock should block other checkpointers"
        );

        vacuum_wal.end_write_tx();
        assert!(!vacuum_wal.holds_write_lock());

        let guard =
            CheckpointLocks::from_held_vacuum_checkpoint_lock(vacuum_wal.coordination.clone())
                .unwrap();
        assert!(
            matches!(contender_wal.try_begin_read_tx(), TryBeginReadResult::Busy),
            "VACUUM lock should continue blocking readers during final checkpoint"
        );

        drop(guard);
        assert!(
            contender_wal.try_begin_vacuum_checkpoint_lock().is_ok(),
            "checkpoint cleanup should release the checkpoint lock"
        );
        contender_wal.release_vacuum_checkpoint_lock();
        assert!(
            matches!(contender_wal.try_begin_read_tx(), TryBeginReadResult::Busy),
            "checkpoint cleanup must not release the VACUUM lock"
        );

        vacuum_wal.release_vacuum_lock();
        assert!(matches!(
            contender_wal.try_begin_read_tx(),
            TryBeginReadResult::Ok(_)
        ));
        contender_wal.end_read_tx();
    }

    #[test]
    fn restart_checkpoint_reset_wal_state_handling() {
        let (db, path) = get_database();

        let walpath = path.path().join("test.db-wal");

        let conn = db.connect().unwrap();
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 20, 3);
        let IOResult::Done(completions) = conn.pager.load().cacheflush().unwrap() else {
            panic!()
        };
        for c in completions {
            db.io.wait_for_completion(c).unwrap();
        }

        // Snapshot header & counters before the RESTART checkpoint.
        let wal_shared = db.shared_wal.clone();
        let (seq_before, salt1_before, salt2_before, _ps_before) = wal_header_snapshot(&wal_shared);
        let (mx_before, backfill_before) = {
            let s = wal_shared.read();
            (
                s.metadata.max_frame.load(Ordering::SeqCst),
                s.metadata.nbackfills.load(Ordering::SeqCst),
            )
        };
        assert!(mx_before > 0);
        assert_eq!(backfill_before, 0);

        let meta_before = std::fs::metadata(&walpath).unwrap();
        #[cfg(unix)]
        let size_before = meta_before.blocks();
        #[cfg(not(unix))]
        let size_before = meta_before.len();
        // Run a RESTART checkpoint, should backfill everything and reset WAL counters,
        // but NOT truncate the file.
        {
            let pager = conn.pager.load();
            let res = run_checkpoint_until_done(&pager, CheckpointMode::Restart);
            assert_eq!(res.wal_max_frame, mx_before);
            assert_eq!(res.wal_total_backfilled, mx_before);
            assert_eq!(res.wal_checkpoint_backfilled, mx_before);
        }

        // Validate post‑RESTART header & counters.
        let (seq_after, salt1_after, salt2_after, _ps_after) = wal_header_snapshot(&wal_shared);
        assert_eq!(
            seq_after,
            seq_before.wrapping_add(1),
            "checkpoint_seq must increment on RESTART"
        );
        assert_eq!(
            salt1_after,
            salt1_before.wrapping_add(1),
            "salt_1 is incremented"
        );
        assert_ne!(salt2_after, salt2_before, "salt_2 is randomized");

        let (mx_after, backfill_after) = {
            let s = wal_shared.read();
            (
                s.metadata.max_frame.load(Ordering::SeqCst),
                s.metadata.nbackfills.load(Ordering::SeqCst),
            )
        };
        assert_eq!(mx_after, 0, "mxFrame reset to 0 after RESTART");
        assert_eq!(backfill_after, 0, "nBackfill reset to 0 after RESTART");

        // File size should be unchanged for RESTART (no truncate).
        let meta_after = std::fs::metadata(&walpath).unwrap();
        #[cfg(unix)]
        let size_after = meta_after.blocks();
        #[cfg(not(unix))]
        let size_after = meta_after.len();
        assert_eq!(
            size_before, size_after,
            "RESTART must not change WAL file size"
        );

        // Next write should start a new sequence at frame 1.
        conn.execute("insert into test(value) values ('post_restart')")
            .unwrap();
        conn.pager
            .load()
            .wal
            .as_ref()
            .unwrap()
            .finish_append_frames_commit()
            .unwrap();
        let new_max = wal_shared.read().metadata.max_frame.load(Ordering::SeqCst);
        assert_eq!(new_max, 1, "first append after RESTART starts at frame 1");
    }

    #[test]
    fn test_wal_passive_partial_then_complete() {
        let (db, _tmp) = get_database();
        let conn1 = db.connect().unwrap();
        let conn2 = db.connect().unwrap();

        conn1
            .execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn1, 15, 2);
        let IOResult::Done(completions) = conn1.pager.load().cacheflush().unwrap() else {
            panic!()
        };
        for c in completions {
            db.io.wait_for_completion(c).unwrap();
        }

        // Force a read transaction that will freeze a lower read mark
        let readmark = {
            let pager = conn2.pager.load();
            let wal2 = pager.wal.as_ref().unwrap();
            wal2.begin_read_tx().unwrap();
            wal2.get_max_frame()
        };

        // generate more frames that the reader will not see.
        bulk_inserts(&conn1, 15, 2);
        let IOResult::Done(completions) = conn1.pager.load().cacheflush().unwrap() else {
            panic!()
        };
        for c in completions {
            db.io.wait_for_completion(c).unwrap();
        }

        // Run passive checkpoint, expect partial
        let (res1, max_before) = {
            let pager = conn1.pager.load();
            let res = run_checkpoint_until_done(
                &pager,
                CheckpointMode::Passive {
                    upper_bound_inclusive: None,
                },
            );
            let maxf = db
                .shared_wal
                .read()
                .metadata
                .max_frame
                .load(Ordering::SeqCst);
            (res, maxf)
        };
        assert_eq!(res1.wal_max_frame, max_before);
        assert!(
            res1.wal_total_backfilled < res1.wal_max_frame,
            "Partial backfill expected, {} : {}",
            res1.wal_total_backfilled,
            res1.wal_max_frame
        );
        assert_eq!(
            res1.wal_total_backfilled, readmark,
            "Checkpointed frames should match read mark"
        );
        // Release reader
        {
            let pager = conn2.pager.load();
            let wal2 = pager.wal.as_ref().unwrap();
            wal2.end_read_tx();
        }

        // Second passive checkpoint should finish
        let pager = conn1.pager.load();
        let res2 = run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
        assert_eq!(
            res2.wal_total_backfilled, res2.wal_max_frame,
            "Second checkpoint completes remaining frames"
        );
    }

    #[test]
    fn test_wal_restart_blocks_readers() {
        let (db, _temp_dir) = get_database();
        let conn1 = db.connect().unwrap();
        let conn2 = db.connect().unwrap();

        // Start a read transaction
        conn2
            .pager
            .load()
            .wal
            .as_ref()
            .unwrap()
            .begin_read_tx()
            .unwrap();

        // checkpoint should succeed here because the wal is fully checkpointed (empty)
        // so the reader is using readmark0 to read directly from the db file.
        let p = conn1.pager.load();
        let w = p.wal.as_ref().unwrap();
        loop {
            match w.checkpoint(&p, CheckpointMode::Restart, SyncMode::Full) {
                Ok(IOResult::IO(io)) => {
                    io.wait(db.io.as_ref()).unwrap();
                }
                e => {
                    assert!(
                        matches!(&e, Err(err) if matches!(**err, LimboError::Busy)),
                        "reader is holding readmark0 we should return Busy"
                    );
                    break;
                }
            }
        }
        conn2.pager.load().end_read_tx();

        conn1
            .execute("create table test(id integer primary key, value text)")
            .unwrap();
        for i in 0..10 {
            conn1
                .execute(format!("insert into test(value) values ('value{i}')"))
                .unwrap();
        }
        // now that we have some frames to checkpoint, try again
        conn2.pager.load().begin_read_tx().unwrap();
        let p = conn1.pager.load();
        let w = p.wal.as_ref().unwrap();
        loop {
            match w.checkpoint(&p, CheckpointMode::Restart, SyncMode::Full) {
                Ok(IOResult::IO(io)) => {
                    io.wait(db.io.as_ref()).unwrap();
                }
                Ok(IOResult::Done(_)) => {
                    panic!("Checkpoint should not have succeeded");
                }
                Err(e) => {
                    assert!(
                        matches!(*e, LimboError::Busy),
                        "should return busy if we have readers"
                    );
                    break;
                }
            }
        }
    }

    #[test]
    fn test_wal_read_marks_after_restart() {
        let (db, _path) = get_database();
        let wal_shared = db.shared_wal.clone();

        let conn = db.connect().unwrap();
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 10, 5);
        // Checkpoint with restart
        {
            let pager = conn.pager.load();
            let result = run_checkpoint_until_done(&pager, CheckpointMode::Restart);
            assert!(result.everything_backfilled());
        }

        // Verify read marks after restart
        let read_marks_after: Vec<_> = {
            let s = wal_shared.read();
            (0..5)
                .map(|i| s.runtime.read_locks[i].get_value())
                .collect()
        };

        assert_eq!(read_marks_after[0], 0, "Slot 0 should remain 0");
        assert_eq!(
            read_marks_after[1], 0,
            "Slot 1 (default reader) should be reset to 0"
        );
        for (i, item) in read_marks_after.iter().take(5).skip(2).enumerate() {
            assert_eq!(
                *item, READMARK_NOT_USED,
                "Slot {i} should be READMARK_NOT_USED after restart",
            );
        }
    }

    #[test]
    fn test_wal_concurrent_readers_during_checkpoint() {
        let (db, _path) = get_database();
        let conn_writer = db.connect().unwrap();

        conn_writer
            .execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn_writer, 5, 10);

        // Start multiple readers at different points
        let conn_r1 = db.connect().unwrap();
        let conn_r2 = db.connect().unwrap();

        // R1 starts reading
        let r1_max_frame = {
            let pager = conn_r1.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            wal.begin_read_tx().unwrap();
            wal.get_max_frame()
        };
        bulk_inserts(&conn_writer, 5, 10);

        // R2 starts reading, sees more frames than R1
        let r2_max_frame = {
            let pager = conn_r2.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            wal.begin_read_tx().unwrap();
            wal.get_max_frame()
        };

        // try passive checkpoint, should only checkpoint up to R1's position
        let checkpoint_result = {
            let pager = conn_writer.pager.load();
            run_checkpoint_until_done(
                &pager,
                CheckpointMode::Passive {
                    upper_bound_inclusive: None,
                },
            )
        };

        assert!(
            checkpoint_result.wal_total_backfilled < checkpoint_result.wal_max_frame,
            "Should not checkpoint all frames when readers are active"
        );
        assert_eq!(
            checkpoint_result.wal_total_backfilled, r1_max_frame,
            "Should have checkpointed up to R1's max frame"
        );

        // Verify R2 still sees its frames
        assert_eq!(
            conn_r2.pager.load().wal.as_ref().unwrap().get_max_frame(),
            r2_max_frame,
            "Reader should maintain its snapshot"
        );
    }

    #[test]
    fn test_wal_checkpoint_updates_read_marks() {
        let (db, _path) = get_database();
        let wal_shared = db.shared_wal.clone();

        let conn = db.connect().unwrap();
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 10, 5);

        // get max frame before checkpoint
        let max_frame_before = wal_shared.read().metadata.max_frame.load(Ordering::SeqCst);

        {
            let pager = conn.pager.load();
            let _result = run_checkpoint_until_done(
                &pager,
                CheckpointMode::Passive {
                    upper_bound_inclusive: None,
                },
            );
        }

        // check that read mark 1 (default reader) was updated to max_frame
        let read_mark_1 = wal_shared.read().runtime.read_locks[1].get_value();

        assert_eq!(
            read_mark_1 as u64, max_frame_before,
            "Read mark 1 should be updated to max frame during checkpoint"
        );
    }

    #[test]
    fn test_wal_writer_blocks_restart_checkpoint() {
        let (db, _path) = get_database();
        let conn1 = db.connect().unwrap();
        let conn2 = db.connect().unwrap();

        conn1
            .execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn1, 5, 5);

        // start a write transaction
        {
            let pager = conn2.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            let _ = wal.begin_read_tx().unwrap();
            wal.begin_write_tx(WalAutoActions::all_enabled()).unwrap();
        }

        // should fail because writer lock is held
        let result = {
            let pager = conn1.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            wal.checkpoint(&pager, CheckpointMode::Restart, SyncMode::Full)
        };

        assert!(
            matches!(&result, Err(err) if matches!(**err, LimboError::Busy)),
            "Restart checkpoint should fail when write lock is held"
        );

        conn2.pager.load().wal.as_ref().unwrap().end_read_tx();
        // release write lock
        conn2.pager.load().wal.as_ref().unwrap().end_write_tx();

        // now restart should succeed
        let result = {
            let pager = conn1.pager.load();
            run_checkpoint_until_done(&pager, CheckpointMode::Restart)
        };

        assert!(result.everything_backfilled());
    }

    #[test]
    #[should_panic(expected = "must have a read transaction to begin a write transaction")]
    fn test_wal_read_transaction_required_before_write() {
        let (db, _path) = get_database();
        let conn = db.connect().unwrap();

        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();

        // Attempt to start a write transaction without a read transaction
        let pager = conn.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        let _ = wal.begin_write_tx(WalAutoActions::all_enabled());
    }

    fn check_read_lock_slot(conn: &Arc<Connection>, _expected_slot: usize) -> bool {
        let pager = conn.pager.load();
        let _wal = pager.wal.as_ref().unwrap();
        #[cfg(debug_assertions)]
        {
            let wal_any = _wal.as_any();
            if let Some(wal_file) = wal_any.downcast_ref::<crate::WalFile>() {
                return wal_file.max_frame_read_lock_index.load(Ordering::Acquire)
                    == _expected_slot;
            }
        }

        false
    }

    #[test]
    fn test_wal_multiple_readers_at_different_frames() {
        let (db, _path) = get_database();
        let conn_writer = db.connect().unwrap();

        conn_writer
            .execute("CREATE TABLE test(id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();

        fn start_reader(conn: &Arc<Connection>) -> (u64, crate::Statement) {
            conn.execute("BEGIN").unwrap();
            let mut stmt = conn.prepare("SELECT * FROM test").unwrap();
            stmt.step().unwrap();
            let frame = conn.pager.load().wal.as_ref().unwrap().get_max_frame();
            (frame, stmt)
        }

        bulk_inserts(&conn_writer, 3, 5);

        let conn1 = &db.connect().unwrap();
        let (r1_frame, _stmt) = start_reader(conn1); // reader 1

        bulk_inserts(&conn_writer, 3, 5);

        let conn_r2 = db.connect().unwrap();
        let (r2_frame, _stmt2) = start_reader(&conn_r2); // reader 2

        bulk_inserts(&conn_writer, 3, 5);

        let conn_r3 = db.connect().unwrap();
        let (r3_frame, _stmt3) = start_reader(&conn_r3); // reader 3

        assert!(r1_frame < r2_frame && r2_frame < r3_frame);

        // passive checkpoint #1
        let result1 = {
            let pager = conn_writer.pager.load();
            run_checkpoint_until_done(
                &pager,
                CheckpointMode::Passive {
                    upper_bound_inclusive: None,
                },
            )
        };
        assert_eq!(result1.wal_total_backfilled, r1_frame);

        // finish reader‑1
        conn1.execute("COMMIT").unwrap();

        // passive checkpoint #2
        let result2 = {
            let pager = conn_writer.pager.load();
            run_checkpoint_until_done(
                &pager,
                CheckpointMode::Passive {
                    upper_bound_inclusive: None,
                },
            )
        };
        assert_eq!(
            result1.wal_checkpoint_backfilled + result2.wal_checkpoint_backfilled,
            r2_frame
        );

        // verify visible rows
        let r2_cnt = count_test_table(&conn_r2);
        let r3_cnt = count_test_table(&conn_r3);

        assert_eq!(r2_cnt, 30);
        assert_eq!(r3_cnt, 45);
    }

    #[test]
    fn test_checkpoint_truncate_reset_handling() {
        let (db, path) = get_database();
        let conn = db.connect().unwrap();

        let walpath = path.path().join("test.db-wal");

        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 10, 10);

        // Get size before checkpoint
        let size_before = std::fs::metadata(&walpath).unwrap().len();
        assert!(size_before > 0, "WAL file should have content");

        // Do a TRUNCATE checkpoint
        {
            let pager = conn.pager.load();
            run_checkpoint_until_done(
                &pager,
                CheckpointMode::Truncate {
                    upper_bound_inclusive: None,
                },
            );
        }

        // Check file size after truncate
        let size_after = std::fs::metadata(&walpath).unwrap().len();
        assert_eq!(size_after, 0, "WAL file should be truncated to 0 bytes");

        // Verify we can still write to the database
        conn.execute("INSERT INTO test VALUES (1001, 'after-truncate')")
            .unwrap();

        // Check WAL has new content
        let new_size = std::fs::metadata(&walpath).unwrap().len();
        assert!(new_size >= 32, "WAL file too small");
        let hdr = read_wal_header(&walpath);
        let expected_magic = if cfg!(target_endian = "big") {
            sqlite3_ondisk::WAL_MAGIC_BE
        } else {
            sqlite3_ondisk::WAL_MAGIC_LE
        };
        assert!(
            hdr.magic == expected_magic,
            "bad WAL magic: {:#X}, expected: {:#X}",
            hdr.magic,
            sqlite3_ondisk::WAL_MAGIC_BE
        );
        assert_eq!(hdr.file_format, 3007000);
        assert_eq!(hdr.page_size, 4096, "invalid page size");
        assert_eq!(hdr.checkpoint_seq, 1, "invalid checkpoint_seq");
    }

    #[test]
    fn test_wal_checkpoint_truncate_db_file_contains_data() {
        let (db, path) = get_database();
        let conn = db.connect().unwrap();

        let walpath = path.path().join("test.db-wal");

        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 10, 100);

        // Get size before checkpoint
        let size_before = std::fs::metadata(&walpath).unwrap().len();
        assert!(size_before > 0, "WAL file should have content");

        // Do a TRUNCATE checkpoint
        {
            let pager = conn.pager.load();
            run_checkpoint_until_done(
                &pager,
                CheckpointMode::Truncate {
                    upper_bound_inclusive: None,
                },
            );
        }

        // Check file size after truncate
        let size_after = std::fs::metadata(&walpath).unwrap().len();
        assert_eq!(size_after, 0, "WAL file should be truncated to 0 bytes");

        // Verify we can still write to the database
        conn.execute("INSERT INTO test VALUES (1001, 'after-truncate')")
            .unwrap();

        // Check WAL has new content
        let new_size = std::fs::metadata(&walpath).unwrap().len();
        assert!(new_size >= 32, "WAL file too small");
        let hdr = read_wal_header(&walpath);
        let expected_magic = if cfg!(target_endian = "big") {
            sqlite3_ondisk::WAL_MAGIC_BE
        } else {
            sqlite3_ondisk::WAL_MAGIC_LE
        };
        assert!(
            hdr.magic == expected_magic,
            "bad WAL magic: {:#X}, expected: {:#X}",
            hdr.magic,
            sqlite3_ondisk::WAL_MAGIC_BE
        );
        assert_eq!(hdr.file_format, 3007000);
        assert_eq!(hdr.page_size, 4096, "invalid page size");
        assert_eq!(hdr.checkpoint_seq, 1, "invalid checkpoint_seq");
        {
            let pager = conn.pager.load();
            run_checkpoint_until_done(
                &pager,
                CheckpointMode::Passive {
                    upper_bound_inclusive: None,
                },
            );
        }
        // delete the WAL file so we can read right from db and assert
        // that everything was backfilled properly
        std::fs::remove_file(&walpath).unwrap();

        let count = count_test_table(&conn);
        assert_eq!(
            count, 1001,
            "we should have 1001 rows in the table all together"
        );
    }

    fn read_wal_header(path: &std::path::Path) -> sqlite3_ondisk::WalHeader {
        use std::{fs::File, io::Read};
        let mut hdr = [0u8; 32];
        File::open(path).unwrap().read_exact(&mut hdr).unwrap();
        let be = |i| u32::from_be_bytes(hdr[i..i + 4].try_into().unwrap());
        sqlite3_ondisk::WalHeader {
            magic: be(0x00),
            file_format: be(0x04),
            page_size: be(0x08),
            checkpoint_seq: be(0x0C),
            salt_1: be(0x10),
            salt_2: be(0x14),
            checksum_1: be(0x18),
            checksum_2: be(0x1C),
        }
    }

    #[test]
    fn test_wal_stale_snapshot_in_write_transaction() {
        let (db, _path) = get_database();
        let conn1 = db.connect().unwrap();
        let conn2 = db.connect().unwrap();

        conn1
            .execute("create table test(id integer primary key, value text)")
            .unwrap();
        // Start a read transaction on conn2
        {
            let pager = conn2.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            wal.begin_read_tx().unwrap();
        }
        // Make changes using conn1
        bulk_inserts(&conn1, 5, 5);
        // Try to start a write transaction on conn2 with a stale snapshot
        let result = {
            let pager = conn2.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            wal.begin_write_tx(WalAutoActions::all_enabled())
        };
        // Should get BusySnapShot due to stale snapshot
        assert!(matches!(result, Err(LimboError::BusySnapshot)));

        // End read transaction and start a fresh one
        {
            let pager = conn2.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            wal.end_read_tx();
            wal.begin_read_tx().unwrap();
        }
        // Now write transaction should work
        let result = {
            let pager = conn2.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            wal.begin_write_tx(WalAutoActions::all_enabled())
        };
        assert!(matches!(result, Ok(())));
    }

    #[test]
    fn test_wal_readlock0_optimization_behavior() {
        let (db, _path) = get_database();
        let conn1 = db.connect().unwrap();
        let conn2 = db.connect().unwrap();

        conn1
            .execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn1, 5, 5);
        // Do a full checkpoint to move all data to DB file
        {
            let pager = conn1.pager.load();
            run_checkpoint_until_done(
                &pager,
                CheckpointMode::Passive {
                    upper_bound_inclusive: None,
                },
            );
        }

        // Start a read transaction on conn2
        {
            let pager = conn2.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            wal.begin_read_tx().unwrap();
        }
        // should use slot 0, as everything is backfilled
        assert!(check_read_lock_slot(&conn2, 0));
        {
            let pager = conn1.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            let frame = wal.find_frame(5, None);
            // since we hold readlock0, we should ignore the db file and find_frame should return none
            assert!(frame.is_ok_and(|f| f.is_none()));
        }
        // Try checkpoint, should fail because reader has slot 0
        {
            let pager = conn1.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            let result = wal.checkpoint(&pager, CheckpointMode::Restart, SyncMode::Full);

            assert!(
                matches!(&result, Err(err) if matches!(**err, LimboError::Busy)),
                "RESTART checkpoint should fail when a reader is using slot 0"
            );
        }
        // End the read transaction
        {
            let pager = conn2.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            wal.end_read_tx();
        }
        {
            let pager = conn1.pager.load();
            let result = run_checkpoint_until_done(&pager, CheckpointMode::Restart);
            assert!(
                result.everything_backfilled(),
                "RESTART checkpoint should succeed after reader releases slot 0"
            );
        }
    }

    #[test]
    fn test_wal_full_backfills_all() {
        let (db, _tmp) = get_database();
        let conn = db.connect().unwrap();

        // Write some data to put frames in the WAL
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 8, 4);

        // Ensure frames are flushed to the WAL
        let IOResult::Done(completions) = conn.pager.load().cacheflush().unwrap() else {
            panic!()
        };
        for c in completions {
            db.io.wait_for_completion(c).unwrap();
        }

        // Snapshot the current mxFrame before running FULL
        let wal_shared = db.shared_wal.clone();
        let mx_before = wal_shared.read().metadata.max_frame.load(Ordering::SeqCst);
        assert!(mx_before > 0, "expected frames in WAL before FULL");

        // Run FULL checkpoint - must backfill *all* frames up to mx_before
        let result = {
            let pager = conn.pager.load();
            run_checkpoint_until_done(&pager, CheckpointMode::Full)
        };

        assert_eq!(result.wal_checkpoint_backfilled, mx_before);
        assert_eq!(result.wal_total_backfilled, mx_before);
    }

    #[test]
    fn test_wal_full_waits_for_old_reader_then_succeeds() {
        let (db, _tmp) = get_database();
        let writer = db.connect().unwrap();
        let reader = db.connect().unwrap();

        writer
            .execute("create table test(id integer primary key, value text)")
            .unwrap();

        // First commit some data and flush (reader will snapshot here)
        bulk_inserts(&writer, 2, 3);
        let IOResult::Done(completions) = writer.pager.load().cacheflush().unwrap() else {
            panic!()
        };
        for c in completions {
            db.io.wait_for_completion(c).unwrap();
        }

        // Start a read transaction pinned at the current snapshot
        {
            let pager = reader.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            wal.begin_read_tx().unwrap();
        }
        let r_snapshot = {
            let pager = reader.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            wal.get_max_frame()
        };

        // Advance WAL beyond the reader's snapshot
        bulk_inserts(&writer, 3, 4);
        let IOResult::Done(completions) = writer.pager.load().cacheflush().unwrap() else {
            panic!()
        };
        for c in completions {
            db.io.wait_for_completion(c).unwrap();
        }
        let mx_now = db
            .shared_wal
            .read()
            .metadata
            .max_frame
            .load(Ordering::SeqCst);
        assert!(mx_now > r_snapshot);

        // FULL must return Busy while a reader is stuck behind
        {
            let pager = writer.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            loop {
                match wal.checkpoint(&pager, CheckpointMode::Full, SyncMode::Full) {
                    Ok(IOResult::IO(io)) => {
                        // Drive any pending IO (should quickly become Busy or Done)
                        io.wait(db.io.as_ref()).unwrap();
                    }
                    Err(err) if matches!(*err, LimboError::Busy) => {
                        break;
                    }
                    other => panic!("expected Busy from FULL with old reader, got {other:?}"),
                }
            }
        }
        assert_eq!(
            db.shared_wal
                .read()
                .metadata
                .nbackfills
                .load(Ordering::SeqCst),
            0,
            "a FULL checkpoint that returns Busy must not publish positive nbackfills before DB sync"
        );

        // Release the reader, now full mode should succeed and backfill everything
        {
            let pager = reader.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            wal.end_read_tx();
        }

        let result = {
            let pager = writer.pager.load();
            run_checkpoint_until_done(&pager, CheckpointMode::Full)
        };

        assert_eq!(
            result.wal_checkpoint_backfilled, mx_now,
            "the successful FULL reruns from the last durable backfill point because the Busy attempt did not publish progress"
        );
        assert!(result.everything_backfilled());
    }

    #[test]
    fn test_rollback_releases_read_lock() {
        let (db, _path) = get_database();
        let conn = db.connect().unwrap();

        conn.execute("CREATE TABLE t(x)").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t VALUES(1)").unwrap();

        {
            let pager = conn.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            assert!(
                wal.holds_read_lock(),
                "read lock must be held during write tx"
            );
        }

        conn.execute("ROLLBACK").unwrap();

        {
            let pager = conn.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            assert!(
                !wal.holds_read_lock(),
                "read lock must be released after ROLLBACK"
            );
        }
    }

    #[test]
    fn test_rollback_releases_shared_read_lock_slot() {
        let (db, _path) = get_database();
        let conn = db.connect().unwrap();

        conn.execute("CREATE TABLE t(x)").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t VALUES(1)").unwrap();

        let locked_slots_before = {
            let shared = db.shared_wal.read();
            read_slots_with_readers(&shared)
        };
        assert_eq!(
            locked_slots_before.len(),
            1,
            "expected exactly one shared read-lock slot while transaction is active"
        );

        conn.execute("ROLLBACK").unwrap();

        let locked_slots_after = {
            let shared = db.shared_wal.read();
            read_slots_with_readers(&shared)
        };
        assert!(
            locked_slots_after.is_empty(),
            "ROLLBACK must release the shared read-lock slot"
        );
    }

    #[test]
    fn test_rollback_releases_slot_zero_read_lock() {
        let (db, _path) = get_database();
        let conn = db.connect().unwrap();

        conn.execute("CREATE TABLE test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 3, 3);
        {
            let pager = conn.pager.load();
            let result = run_checkpoint_until_done(&pager, CheckpointMode::Restart);
            assert!(
                result.everything_backfilled(),
                "restart checkpoint setup must fully backfill WAL"
            );
        }

        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO test(value) VALUES('slot0')")
            .unwrap();

        let locked_slots_before = {
            let shared = db.shared_wal.read();
            read_slots_with_readers(&shared)
        };
        assert_eq!(
            locked_slots_before,
            vec![0],
            "writer should use slot 0 when WAL is fully checkpointed"
        );

        conn.execute("ROLLBACK").unwrap();

        let locked_slots_after = {
            let shared = db.shared_wal.read();
            read_slots_with_readers(&shared)
        };
        assert!(
            locked_slots_after.is_empty(),
            "ROLLBACK must release slot 0 shared read-lock as well"
        );
    }

    #[test]
    fn test_savepoint_rollback_preserves_read_lock() {
        let (db, _path) = get_database();
        let conn = db.connect().unwrap();

        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t VALUES(1)").unwrap();

        // Trigger a statement failure that causes savepoint rollback.
        // A duplicate primary key on the second INSERT will fail the
        // statement, rolling back to the anonymous savepoint while
        // keeping the write transaction open.
        let res = conn.execute("INSERT INTO t VALUES(1)");
        assert!(res.is_err(), "duplicate PK insert must fail");

        {
            let pager = conn.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            assert!(
                wal.holds_read_lock(),
                "read lock must still be held after savepoint rollback"
            );
            assert!(
                wal.holds_write_lock(),
                "write lock must still be held after savepoint rollback"
            );
        }

        // The transaction should still be usable: commit succeeds and
        // the first insert is preserved.
        conn.execute("COMMIT").unwrap();

        let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
        let mut count: i64 = 0;
        stmt.run_with_row_callback(|row| {
            count = row.get(0).unwrap();
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 1, "first insert should survive savepoint rollback");
    }

    #[test]
    fn test_savepoint_then_tx_rollback_allows_restart_checkpoint_from_other_connection() {
        let (db, _path) = get_database();
        let conn1 = db.connect().unwrap();
        let conn2 = db.connect().unwrap();

        conn1
            .execute("CREATE TABLE test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn1, 2, 2);
        let count_before = count_test_table(&conn1);

        conn1.execute("BEGIN").unwrap();
        conn1
            .execute("INSERT INTO test(id, value) VALUES(1000, 'first')")
            .unwrap();
        let duplicate = conn1.execute("INSERT INTO test(id, value) VALUES(1000, 'dup')");
        assert!(duplicate.is_err(), "duplicate PK insert must fail");

        {
            let pager = conn1.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            assert!(
                wal.holds_read_lock(),
                "read lock must still be held after savepoint rollback"
            );
            assert!(
                wal.holds_write_lock(),
                "write lock must still be held after savepoint rollback"
            );
        }

        conn1.execute("ROLLBACK").unwrap();

        {
            let pager = conn1.pager.load();
            let wal = pager.wal.as_ref().unwrap();
            assert!(
                !wal.holds_read_lock(),
                "read lock must be released after transaction rollback"
            );
            assert!(
                !wal.holds_write_lock(),
                "write lock must be released after transaction rollback"
            );
        }

        let locked_slots_after_rollback = {
            let shared = db.shared_wal.read();
            read_slots_with_readers(&shared)
        };
        assert!(
            locked_slots_after_rollback.is_empty(),
            "transaction rollback after savepoint failure must not leak shared read locks"
        );
        assert_eq!(
            count_test_table(&conn1),
            count_before,
            "transaction rollback should remove writes made before savepoint failure"
        );

        let result = {
            let pager = conn2.pager.load();
            run_checkpoint_until_done(&pager, CheckpointMode::Restart)
        };
        assert!(
            result.everything_backfilled(),
            "restart checkpoint from another connection must succeed after full rollback"
        );
    }

    #[test]
    fn test_checkpoint_succeeds_after_rollback() {
        let (db, _path) = get_database();
        let conn = db.connect().unwrap();

        conn.execute("CREATE TABLE test(id integer primary key, value text)")
            .unwrap();
        bulk_inserts(&conn, 5, 3);

        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO test(value) VALUES('rollback_me')")
            .unwrap();
        conn.execute("ROLLBACK").unwrap();

        let pager = conn.pager.load();
        let result = run_checkpoint_until_done(&pager, CheckpointMode::Restart);
        assert!(
            result.everything_backfilled(),
            "checkpoint must succeed after rollback, not return Busy"
        );
    }
}
