use crate::types::IOResultOr;
use branches::{mark_unlikely, unlikely};
use rustc_hash::FxHashMap as HashMap;
#[cfg(debug_assertions)]
use rustc_hash::FxHashSet as HashSet;
use smallvec::SmallVec;
use tracing::{instrument, Level};

use super::{
    pager::PageRef,
    sqlite3_ondisk::{IndexInteriorCell, OverflowCell, MINIMUM_CELL_SIZE},
};
use crate::alloc::{TursoFromIterator, TursoSliceExt, TursoVecExt};
#[cfg(any(test, injected_yields))]
use crate::mvcc::yield_hooks::{ProvidesYieldContext, YieldContext, YieldPointMarker};
use crate::mvcc::yield_points::inject_io_yield;
use crate::{
    io::CompletionGroup,
    io_yield_one,
    schema::{BTreeTable, Index},
    storage::{
        pager::{BtreePageAllocMode, Pager},
        sqlite3_ondisk::{
            payload_overflows, read_u32, read_varint, write_varint, BTreeCell, DatabaseHeader,
            PageContent, PageSize, PageType, TableInteriorCell, CELL_PTR_SIZE_BYTES,
            FREELIST_LEAF_PTR_SIZE, FREELIST_TRUNK_HEADER_SIZE,
            FREELIST_TRUNK_OFFSET_FIRST_LEAF_PTR, FREELIST_TRUNK_OFFSET_LEAF_COUNT,
            FREELIST_TRUNK_OFFSET_NEXT_TRUNK_PTR, INTERIOR_PAGE_HEADER_SIZE_BYTES,
            LEAF_PAGE_HEADER_SIZE_BYTES, LEFT_CHILD_PTR_SIZE_BYTES,
        },
        state_machines::{
            AdvanceState, CountState, EmptyTableState, MoveToRightState, MoveToState, RewindState,
            SeekEndState, SeekToLastState,
        },
    },
    translate::plan::IterationDirection,
    turso_assert,
    types::{
        find_compare, get_tie_breaker_from_seek_op, IOCompletions, IndexInfo, RecordCompare,
        SeekResult,
    },
    util::IOExt,
    vdbe::Register,
    Completion, MvStore,
};
use crate::{
    numeric::Numeric,
    return_corrupt, return_if_io,
    types::{
        compare_immutable_iter, AsValueRef, IOResult, ImmutableRecord, ImmutableRecordRef, SeekKey,
        SeekOp, Value, ValueRef,
    },
    LimboError, Result,
};
use crate::{
    turso_assert_eq, turso_assert_greater_than, turso_assert_greater_than_or_equal,
    turso_assert_less_than, turso_assert_less_than_or_equal, turso_debug_assert,
};
use std::{
    any::Any,
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    fmt::Debug,
    ops::ControlFlow,
    pin::Pin,
    sync::Arc,
};

/// Maximum number of key values to store on the stack when converting registers to ValueRefs
/// during seeking. Since we use a SmallVec it'll gracefully fall back to heap allocating beyond
/// this threshold.
const STACK_ALLOC_KEY_VALS_MAX: usize = 16;

fn write_varint_to_vec(value: u64, payload: &mut crate::alloc::Vec<u8>) -> Result<()> {
    let mut varint = [0u8; 9];
    let len = write_varint(&mut varint, value);
    crate::with_btree_allocation_site!(
        CellPayload,
        payload.try_extend(varint[..len].iter().copied())
    )?;
    Ok(())
}

fn take_vec<T>(values: &mut crate::alloc::Vec<T>) -> crate::alloc::Vec<T> {
    std::mem::replace(values, crate::alloc::vec![])
}

/// The B-Tree page header is 12 bytes for interior pages and 8 bytes for leaf pages.
///
/// +--------+-----------------+-----------------+-----------------+--------+----- ..... ----+
/// | Page   | First Freeblock | Cell Count      | Cell Content    | Frag.  | Right-most     |
/// | Type   | Offset          |                 | Area Start      | Bytes  | pointer        |
/// +--------+-----------------+-----------------+-----------------+--------+----- ..... ----+
///     0        1        2        3        4        5        6        7        8       11
///
pub mod offset {
    /// Type of the B-Tree page (u8).
    pub const BTREE_PAGE_TYPE: usize = 0;

    /// A pointer to the first freeblock (u16).
    ///
    /// This field of the B-Tree page header is an offset to the first freeblock, or zero if
    /// there are no freeblocks on the page.  A freeblock is a structure used to identify
    /// unallocated space within a B-Tree page, organized as a chain.
    ///
    /// Please note that freeblocks do not mean the regular unallocated free space to the left
    /// of the cell content area pointer, but instead blocks of at least 4
    /// bytes WITHIN the cell content area that are not in use due to e.g.
    /// deletions.
    pub const BTREE_FIRST_FREEBLOCK: usize = 1;

    /// The number of cells in the page (u16).
    pub const BTREE_CELL_COUNT: usize = 3;

    /// A pointer to the first byte of cell allocated content from top (u16).
    ///
    /// A zero value for this integer is interpreted as 65,536.
    /// If a page contains no cells (which is only possible for a root page of a table that
    /// contains no rows) then the offset to the cell content area will equal the page size minus
    /// the bytes of reserved space. If the database uses a 65536-byte page size and the
    /// reserved space is zero (the usual value for reserved space) then the cell content offset of
    /// an empty page wants to be 6,5536
    ///
    /// SQLite strives to place cells as far toward the end of the b-tree page as it can, in
    /// order to leave space for future growth of the cell pointer array. This means that the
    /// cell content area pointer moves leftward as cells are added to the page.
    pub const BTREE_CELL_CONTENT_AREA: usize = 5;

    /// The number of fragmented bytes (u8).
    ///
    /// Fragments are isolated groups of 1, 2, or 3 unused bytes within the cell content area.
    pub const BTREE_FRAGMENTED_BYTES_COUNT: usize = 7;

    /// The right-most pointer (saved separately from cells) (u32)
    pub const BTREE_RIGHTMOST_PTR: usize = 8;
}

/// Maximum depth of an SQLite B-Tree structure. Any B-Tree deeper than
/// this will be declared corrupt. This value is calculated based on a
/// maximum database size of 2^31 pages a minimum fanout of 2 for a
/// root-node and 3 for all other internal nodes.
///
/// If a tree that appears to be taller than this is encountered, it is
/// assumed that the database is corrupt.
pub const BTCURSOR_MAX_DEPTH: usize = 20;

/// Maximum number of sibling pages that balancing is performed on.
pub const MAX_SIBLING_PAGES_TO_BALANCE: usize = 3;

/// We only need maximum 5 pages to balance 3 pages, because we can guarantee that cells from 3 pages will fit in 5 pages.
pub const MAX_NEW_SIBLING_PAGES_AFTER_BALANCE: usize = 5;

/// Validate cells in a page are in a valid state. Only in debug mode.
macro_rules! debug_validate_cells {
    ($page_contents:expr, $usable_space:expr) => {
        #[cfg(debug_assertions)]
        {
            debug_validate_cells_core($page_contents, $usable_space);
        }
    };
}

/// State machine of destroy operations
/// Keep track of traversal so that it can be resumed when IO is encountered
#[derive(Debug, Clone)]
enum DestroyState {
    Start,
    LoadPage,
    ProcessPage,
    ClearOverflowPages {
        cell: BTreeCell,
    },
    /// Transitional state used after a spill yield from one of the descent
    /// reads inside `ProcessPage` or after `ClearOverflowPages` returned
    /// `Done`. We've committed to descending into `target` (its parent's
    /// cell_idx has already been advanced or `clear_overflow_pages` has
    /// already cleared the overflow chain for the divider cell), so on
    /// re-entry we just retry the read + push + transition to `LoadPage`
    /// without re-running those prior steps.
    PendingDescent {
        target: i64,
    },
    FreePage,
}

struct DestroyInfo {
    state: DestroyState,
}

#[derive(Debug)]
enum DeleteState {
    Start,
    DeterminePostBalancingSeekKey,
    LoadPage {
        post_balancing_seek_key: Option<CursorContext>,
    },
    FindCell {
        post_balancing_seek_key: Option<CursorContext>,
    },
    ClearOverflowPages {
        cell_idx: usize,
        cell: BTreeCell,
        original_child_pointer: Option<u32>,
        post_balancing_seek_key: Option<CursorContext>,
    },
    InteriorNodeReplacement {
        page: PageRef,
        /// the btree level of the page where the cell replacement happened.
        /// if the replacement causes the page to overflow/underflow, we need to remember it and balance it
        /// after the deletion process is otherwise complete.
        btree_depth: usize,
        cell_idx: usize,
        original_child_pointer: Option<u32>,
        post_balancing_seek_key: Option<CursorContext>,
    },
    CheckNeedsBalancing {
        /// same as `InteriorNodeReplacement::btree_depth`
        btree_depth: usize,
        post_balancing_seek_key: Option<CursorContext>,
        interior_node_was_replaced: bool,
    },
    /// If an interior node was replaced, we need to move back up from the subtree to the interior cell
    /// that now has the replaced content, so that the next invocation of BTreeCursor::next() does not
    /// stop at that cell.
    /// The reason it is important to land here is that the replaced cell was smaller (LT) than the deleted cell,
    /// so we must ensure we skip over it. I.e., when BTreeCursor::next() is called, it will move past the cell
    /// that holds the replaced content.
    /// See: https://github.com/tursodatabase/turso/issues/3045
    PostInteriorNodeReplacement,
    Balancing {
        /// If provided, will also balance an ancestor page at depth `balance_ancestor_at_depth`.
        /// If not provided, balancing will stop as soon as a level is encountered where no balancing is required.
        balance_ancestor_at_depth: Option<usize>,
    },
    RestoreContextAfterBalancing,
}

#[derive(Debug)]
pub enum OverwriteCellState {
    /// Allocate a new payload for the cell.
    AllocatePayload,
    /// Fill the cell payload with the new payload.
    FillPayload {
        new_payload: crate::alloc::Vec<u8>,
        rowid: Option<i64>,
        fill_cell_payload_state: FillCellPayloadState,
    },
    /// Clear the old cell's overflow pages and add them to the freelist.
    /// Overwrite the cell with the new payload.
    ClearOverflowPagesAndOverwrite {
        new_payload: crate::alloc::Vec<u8>,
        old_offset: usize,
        old_local_size: usize,
    },
}

struct BalanceContext {
    pages_to_balance_new: [Option<PinGuard>; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE],
    sibling_count_new: usize,
    cell_array: CellArray,
    old_cell_count_per_page_cumulative: [u16; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE],
    #[cfg(debug_assertions)]
    cells_debug: crate::alloc::Vec<crate::alloc::Vec<u8>>,
}

impl std::fmt::Debug for BalanceContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BalanceContext")
            .field("pages_to_balance_new", &self.pages_to_balance_new)
            .field("sibling_count_new", &self.sibling_count_new)
            .field("cell_array", &self.cell_array)
            .field(
                "old_cell_count_per_page_cumulative",
                &self.old_cell_count_per_page_cumulative,
            )
            .finish()
    }
}

#[derive(Debug, Default)]
/// State machine of a btree rebalancing operation.
enum BalanceSubState {
    #[default]
    Start,
    BalanceRoot,
    Decide,
    Quick,
    /// Choose which sibling pages to balance (max 3).
    /// Generally, the siblings involved will be the page that triggered the balancing and its left and right siblings.
    /// The exceptions are:
    /// 1. If the leftmost page triggered balancing, up to 3 leftmost pages will be balanced.
    /// 2. If the rightmost page triggered balancing, up to 3 rightmost pages will be balanced.
    NonRootPickSiblings,
    /// Perform the actual balancing. This will result in 1-5 pages depending on the number of total cells to be distributed
    /// from the source pages.
    NonRootDoBalancing,
    NonRootDoBalancingAllocate {
        i: usize,
        context: Option<BalanceContext>,
    },
    NonRootDoBalancingFinish {
        context: BalanceContext,
    },
    /// Free pages that are not used anymore after balancing.
    FreePages {
        curr_page: usize,
        sibling_count_new: usize,
    },
}

#[derive(Debug)]
struct BalanceState {
    sub_state: BalanceSubState,
    balance_info: Option<BalanceInfo>,
    /// Reusable buffers for divider cell payloads.
    /// These persist across balance operations to avoid repeated allocations.
    /// We use Vec<u8> with clear/resize instead of allocating new each time.
    reusable_divider_buffers: [crate::alloc::Vec<u8>; MAX_SIBLING_PAGES_TO_BALANCE - 1],
    /// Reusable Vec for CellArray cell_payloads to avoid per-balance allocation.
    /// Cleared before each use; grows as needed and retains capacity across operations.
    reusable_cell_payloads: crate::alloc::Vec<&'static mut [u8]>,
    /// Group for the sibling page reads issued by `NonRootPickSiblings`.
    /// It lives in `BalanceState` rather than on the stack so that when
    /// the loop yields for spill IO and is re-entered, reads from earlier
    /// iterations are still waited on before `NonRootDoBalancing` looks at
    /// page contents. Taken and built when the loop completes.
    sibling_load_group: Option<CompletionGroup>,
}

impl Default for BalanceState {
    fn default() -> Self {
        Self {
            sub_state: BalanceSubState::default(),
            balance_info: None,
            reusable_divider_buffers: std::array::from_fn(|_| crate::alloc::vec![]),
            reusable_cell_payloads: crate::alloc::vec![],
            sibling_load_group: None,
        }
    }
}

/// State machine of a write operation.
/// May involve balancing due to overflow.
#[derive(Debug)]
enum WriteState {
    Start,
    /// Overwrite an existing cell.
    /// In addition to deleting the old cell and writing a new one,
    /// we may also need to clear the old cell's overflow pages
    /// and add them to the freelist.
    Overwrite {
        page: PageRef,
        cell_idx: usize,
        // This is an Option although it's not optional; we `take` it as owned for [BTreeCursor::overwrite_cell]
        // to work around the borrow checker, and then insert it back if overwriting returns IO.
        state: Option<OverwriteCellState>,
    },
    /// Insert a new cell. This path is taken when inserting a new row.
    Insert {
        page: PageRef,
        cell_idx: usize,
        new_payload: crate::alloc::Vec<u8>,
        fill_cell_payload_state: FillCellPayloadState,
    },
    Balancing,
    Finish,
}

#[cfg(any(test, injected_yields))]
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub(crate) enum BTreeWriteYieldPoint {
    AfterInsertOverflowCellBeforeBalance,
}

#[cfg(any(test, injected_yields))]
pub(crate) const BTREE_WRITE_YIELD_FAMILY: u64 = 0x4254_5245_5752_4954;

#[cfg(any(test, injected_yields))]
impl YieldPointMarker for BTreeWriteYieldPoint {
    const POINT_COUNT: u8 = 1;

    fn ordinal(self) -> u8 {
        self as u8
    }
}

struct ReadPayloadOverflow {
    payload: crate::alloc::Vec<u8>,
    next_page: u32,
    remaining_to_read: usize,
    page: PageRef,
}

#[derive(Debug)]
pub struct PinGuard(PageRef);
impl PinGuard {
    pub fn new(p: PageRef) -> Self {
        p.pin();
        Self(p)
    }
}

// Since every Drop will unpin, every clone
// needs to add to the pin count
impl Clone for PinGuard {
    fn clone(&self) -> Self {
        self.0.pin();
        Self(self.0.clone())
    }
}

impl PinGuard {
    pub fn to_page(&self) -> PageRef {
        self.0.clone()
    }
}

impl Drop for PinGuard {
    fn drop(&mut self) {
        self.0.try_unpin();
    }
}

impl std::ops::Deref for PinGuard {
    type Target = PageRef;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub enum BTreeKey<'a> {
    TableRowId((i64, Option<&'a ImmutableRecord>)),
    IndexKey(ImmutableRecordRef<'a>),
}

impl<'a> BTreeKey<'a> {
    /// Create a new table rowid key from a rowid and an optional immutable record.
    /// The record is optional because it may not be available when the key is created.
    pub fn new_table_rowid(rowid: i64, record: Option<&'a ImmutableRecord>) -> Self {
        BTreeKey::TableRowId((rowid, record))
    }

    /// Create a new index key from an immutable record.
    pub fn new_index_key(record: ImmutableRecordRef<'a>) -> Self {
        BTreeKey::IndexKey(record)
    }

    /// Get the record, if present. Index will always be present,
    pub fn get_record(&self) -> Option<ImmutableRecordRef<'_>> {
        match self {
            BTreeKey::TableRowId((_, record)) => record.map(|record| record.as_record_ref()),
            BTreeKey::IndexKey(record) => Some(record.reborrow()),
        }
    }

    /// Get the rowid, if present. Index will never be present.
    pub fn maybe_rowid(&self) -> Option<i64> {
        match self {
            BTreeKey::TableRowId((rowid, _)) => Some(*rowid),
            BTreeKey::IndexKey(_) => None,
        }
    }

    /// Assert that the key is an integer rowid and return it.
    fn to_rowid(&self) -> i64 {
        match self {
            BTreeKey::TableRowId((rowid, _)) => *rowid,
            BTreeKey::IndexKey(_) => {
                panic!("BTreeKey::to_rowid called on IndexKey")
            }
        }
    }
}

#[derive(Debug, Clone)]
struct BalanceInfo {
    /// Old pages being balanced. We can have maximum 3 pages being balanced at the same time.
    pages_to_balance: [Option<PinGuard>; MAX_SIBLING_PAGES_TO_BALANCE],
    /// Bookkeeping of the rightmost pointer so the offset::BTREE_RIGHTMOST_PTR can be updated.
    rightmost_pointer: *mut u8,
    /// Number of siblings being used to balance
    sibling_count: usize,
    /// First divider cell to remove that marks the first sibling
    first_divider_cell: usize,
    /// Reusable buffer for constructing new divider cells during balance.
    /// Avoids allocating a new Vec for each sibling during balance_non_root.
    reusable_divider_cell: crate::alloc::Vec<u8>,
}

// SAFETY: Need to guarantee during balancing that we do not modify the rightmost pointer on the pointee `PageContent`
// safe as long as the Balance Algorithm does not modify the pointer
unsafe impl Send for BalanceInfo {}
unsafe impl Sync for BalanceInfo {}

/// Holds the state machine for the operation that was in flight when the cursor
/// was suspended due to IO.
enum CursorState {
    None,
    /// The cursor is in a write operation.
    Write(WriteState),
    Destroy(DestroyInfo),
    Delete(DeleteState),
}

impl CursorState {
    fn destroy_info(&self) -> Option<&DestroyInfo> {
        match self {
            CursorState::Destroy(x) => Some(x),
            _ => None,
        }
    }
    fn mut_destroy_info(&mut self) -> Option<&mut DestroyInfo> {
        match self {
            CursorState::Destroy(x) => Some(x),
            _ => None,
        }
    }
}

impl Debug for CursorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Delete(..) => write!(f, "Delete"),
            Self::Destroy(..) => write!(f, "Destroy"),
            Self::None => write!(f, "None"),
            Self::Write(..) => write!(f, "Write"),
        }
    }
}

#[derive(Debug, Clone)]
enum OverflowState {
    Start,
    ProcessPage {
        next_page: PageRef,
    },
    /// Transitional state used to make `OverflowState::ProcessPage`
    /// re-entry-safe across yields. Once `free_page` has returned `Done` for
    /// the current page, we move to this state before validating or reading
    /// the next page so `free_page` cannot be invoked a second time on a page
    /// that is already in the freelist.
    ReadNext {
        next: u32,
    },
    Done,
}

/// Holds a Record or RowId, so that these can be transformed into a SeekKey to restore
/// cursor position to its previous location.
#[derive(Debug)]
pub enum CursorContextKey {
    TableRowId(i64),

    /// If we are in an index tree we can then reuse this field to save
    /// our cursor information
    IndexKeyRowId(ImmutableRecordRef<'static>),
}

#[derive(Debug)]
pub struct CursorContext {
    pub key: CursorContextKey,
    pub seek_op: SeekOp,
}

impl CursorContext {
    fn seek_eq_only(key: &BTreeKey<'_>) -> Self {
        let key = match key {
            BTreeKey::TableRowId((rowid, _)) => CursorContextKey::TableRowId(*rowid),
            BTreeKey::IndexKey(record) => {
                let payload = crate::types::value_blob_from_slice(record.get_payload())
                    .expect(crate::alloc::ALLOC_ERR_MSG);
                let owned = ImmutableRecord::from_bin_record(payload);
                CursorContextKey::IndexKeyRowId(ImmutableRecordRef::from_owned_record(owned))
            }
        };
        Self {
            key,
            seek_op: SeekOp::GE { eq_only: true },
        }
    }
}

/// In the future, we may expand these general validity states
#[derive(Debug, PartialEq, Eq)]
pub enum CursorValidState {
    /// Cursor does not point to a valid entry, and Btree will never yield a record.
    Invalid,
    /// Cursor is pointing a to an existing location/cell in the Btree
    Valid,
    /// Cursor may be pointing to a non-existent location/cell. This can happen after balancing operations
    RequireSeek,
    /// Cursor requires an advance after a seek
    RequireAdvance(IterationDirection),
}

#[derive(Debug, Clone, Copy)]
pub struct InteriorPageBinarySearchState {
    min_cell_idx: isize,
    max_cell_idx: isize,
    nearest_matching_cell: Option<usize>,
    eq_seen: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct LeafPageBinarySearchState {
    min_cell_idx: isize,
    max_cell_idx: isize,
    nearest_matching_cell: Option<usize>,
    /// Indicates if we have seen an exact match during the downwards traversal of the btree.
    /// This is only needed in index seeks, in cases where we need to determine whether we call
    /// an additional next()/prev() to fetch a matching record from an interior node. We will not
    /// do that if both are true:
    /// 1. We have not seen an EQ during the traversal
    /// 2. We are looking for an exact match ([SeekOp::GE] or [SeekOp::LE] with eq_only: true)
    eq_seen: bool,
    /// In multiple places, we do a seek that checks for an exact match (SeekOp::EQ) in the tree.
    /// In those cases, we need to know where to land if we don't find an exact match in the leaf page.
    /// For non-eq-only conditions (GT, LT, GE, LE), this is pretty simple:
    /// - If we are looking for GT/GE and don't find a match, we should end up beyond the end of the page (idx=cell count).
    /// - If we are looking for LT/LE and don't find a match, we should end up before the beginning of the page (idx=-1).
    ///
    /// For eq-only conditions (GE { eq_only: true } or LE { eq_only: true }), we need to know where to land if we don't find an exact match.
    /// For GE, we want to land at the first cell that is greater than the seek key.
    /// For LE, we want to land at the last cell that is less than the seek key.
    /// This is because e.g. when we attempt to insert rowid 666, we first check if it exists.
    /// If it doesn't, we want to land in the place where rowid 666 WOULD be inserted.
    target_cell_when_not_found: i32,
}

#[derive(Debug)]
/// State used for seeking
pub enum CursorSeekState {
    Start,
    MovingBetweenPages {
        eq_seen: bool,
    },
    InteriorPageBinarySearch {
        state: InteriorPageBinarySearchState,
    },
    FoundLeaf {
        eq_seen: bool,
    },
    LeafPageBinarySearch {
        state: LeafPageBinarySearchState,
    },
}

/// Outcome of [`CursorTrait::try_save_position_for_external_balance`]. Mirrors
/// the two paths SQLite's saveCursorPosition takes (btree.c:756) — succeed and
/// have the caller skip invalidation, or fall through to clearing the cached
/// page stack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SavePositionResult {
    /// Position captured via [`CursorTrait::save_context`]; cursor will re-seek
    /// on next use. Caller does not need to invalidate.
    Saved,
    /// Position cannot be represented (MVCC cursor, mid-operation page stack,
    /// stale record state). Caller must fall back to `invalidate_btree_cache`.
    MustInvalidate,
}

pub trait CursorTrait: Any + Send + Sync {
    /// Move cursor to last entry.
    fn last(&mut self) -> IOResultOr<()>;
    /// Move cursor to next entry.
    fn next(&mut self) -> IOResultOr<()>;
    /// Move cursor to previous entry.
    fn prev(&mut self) -> IOResultOr<()>;
    /// Get the rowid of the entry the cursor is poiting to if any
    fn rowid(&mut self) -> IOResultOr<Option<i64>>;

    /// Incremental blob I/O — read `len` bytes at `off` within column `column`'s value
    /// into `out`. Only table (rowid) b-tree cursors support this; the default errors.
    fn blob_read_column(
        &mut self,
        _column: usize,
        _off: usize,
        _len: usize,
        _out: &mut crate::ValueBlob,
    ) -> IOResultOr<()> {
        Err(LimboError::InternalError(
            "incremental blob I/O is only supported on table rows".to_string(),
        )
        .into())
    }
    /// Incremental blob I/O — write `data` at `off` within column `column`'s value.
    fn blob_write_column(&mut self, _column: usize, _off: usize, _data: &[u8]) -> IOResultOr<()> {
        Err(LimboError::InternalError(
            "incremental blob I/O is only supported on table rows".to_string(),
        )
        .into())
    }
    /// Byte length of column `column`'s value on the current row, validating that
    /// the value is byte-addressable (TEXT or BLOB).
    fn blob_column_len(&mut self, _column: usize) -> IOResultOr<usize> {
        Err(LimboError::InternalError(
            "incremental blob I/O is only supported on table rows".to_string(),
        )
        .into())
    }
    /// Notification, delivered during a peer's saveAllCursors pass, that the peer is
    /// about to write the row `rowid` in this cursor's b-tree (`None` when the rowid
    /// is unknown, which must be treated as "could be any row"). Lets cursors backing
    /// incremental blob handles expire when their own row is written — SQLite's
    /// invalidateIncrblobCursors — while surviving writes to other rows. Default no-op.
    fn note_external_row_write(&mut self, _rowid: Option<i64>) {}
    /// Get the record of the entry the cursor is poiting to if any
    fn record(&mut self) -> IOResultOr<Option<&ImmutableRecord>>;
    /// Move the cursor based on the key and the type of operation (op).
    fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> IOResultOr<SeekResult>;
    /// Seek using registers directly without serializing them into an ImmutableRecord first.
    /// This avoids heap allocation and serialization overhead in hot paths like index lookups.
    fn seek_unpacked(&mut self, registers: &[Register], op: SeekOp) -> IOResultOr<SeekResult>;
    /// Insert a record in the position the cursor is at.
    fn insert(&mut self, key: &BTreeKey) -> IOResultOr<()>;
    /// Delete a record in the position the cursor is at.
    fn delete(&mut self) -> IOResultOr<()>;
    fn set_null_flag(&mut self, flag: bool);
    fn get_null_flag(&self) -> bool;
    /// Check if a key exists.
    fn exists(&mut self, key: &Value) -> IOResultOr<bool>;
    fn clear_btree(&mut self) -> IOResultOr<Option<usize>>;
    fn btree_destroy(&mut self) -> IOResultOr<Option<usize>>;
    /// Count the number of entries in the b-tree
    ///
    /// Only supposed to be used in the context of a simple Count Select Statement
    fn count(&mut self) -> IOResultOr<usize>;
    fn is_empty(&self) -> bool;
    fn root_page(&self) -> i64;
    /// Move cursor at the start.
    fn rewind(&mut self) -> IOResultOr<()>;
    /// Check if cursor is poiting at a valid entry with a record.
    fn has_record(&self) -> bool;
    fn set_has_record(&mut self, has_record: bool);
    fn get_index_info(&self) -> &Arc<IndexInfo>;

    fn seek_end(&mut self) -> IOResultOr<()>;
    fn seek_to_last(&mut self) -> IOResultOr<()>;

    /// Returns true if this cursor operates in MVCC mode.
    fn is_mvcc(&self) -> bool {
        false
    }

    // --- start: BTreeCursor specific functions ----
    fn invalidate_record(&mut self);
    fn has_rowid(&self) -> bool;
    fn get_pager(&self) -> Arc<Pager>;
    fn get_skip_advance(&self) -> bool;
    /// Invalidate cached navigation state. Must be called on cursors that
    /// share a btree (e.g. OpenDup cursors) when the btree structure is
    /// modified by another cursor (e.g. clear_btree via ResetSorter).
    fn invalidate_btree_cache(&mut self) {}
    /// Opt into the pager's cursor_registry. Default no-op so non-BTreeCursor
    /// impls (MvccLazyCursor) stay out. Opt-in impls must unregister in Drop.
    fn register_with_pager(&self) {}
    /// Mirror of SQLite's BTCF_Multiple flag; toggled by Pager when a bucket
    /// crosses the 1↔2 threshold.
    fn set_has_peers_for_external_writes(&self, _has_peers: bool) {}
    /// Save position so the cursor can re-seek after a peer write. Returns
    /// [`SavePositionResult::MustInvalidate`] when the position can't be
    /// represented (MVCC cursors, stale page stack); the caller falls back to
    /// invalidate_btree_cache.
    fn try_save_position_for_external_balance(&mut self) -> IOResultOr<SavePositionResult> {
        Ok(IOResult::Done(SavePositionResult::MustInvalidate))
    }
    // --- end: BTreeCursor specific functions ----
}

pub struct BTreeCursor {
    /// The pager that is used to read and write to the database file.
    pub pager: Arc<Pager>,
    /// Cached value of the usable space of a BTree page, since it is very expensive to call in a hot loop via pager.usable_space().
    /// This is OK to cache because both 'PRAGMA page_size' and '.filectrl reserve_bytes' only have an effect on:
    /// 1. an uninitialized database,
    /// 2. an initialized database when the command is immediately followed by VACUUM.
    usable_space_cached: usize,
    /// Page id of the root page used to go back up fast.
    root_page: i64,
    /// Rowid and record are stored before being consumed.
    pub has_record: bool,
    null_flag: bool,
    /// Index internal pages are consumed on the way up, so we store going upwards flag in case
    /// we just moved to a parent page and the parent page is an internal index page which requires
    /// to be consumed.
    going_upwards: bool,
    /// Information maintained across execution attempts when an operation yields due to I/O.
    state: CursorState,
    /// State machine for balancing.
    balance_state: BalanceState,
    /// Information maintained while freeing overflow pages. Maintained separately from cursor state since
    /// any method could require freeing overflow pages
    overflow_state: OverflowState,
    /// Page stack used to traverse the btree.
    /// Each cursor has a stack because each cursor traverses the btree independently.
    stack: PageStack,
    /// Reusable immutable record, used to allow better allocation strategy.
    reusable_immutable_record: Option<ImmutableRecord>,
    /// Information about the index key structure (sort order, collation, etc)
    pub index_info: Option<Arc<IndexInfo>>,
    /// Maintain count of the number of records in the btree. Used for the `Count` opcode
    count: usize,
    /// Stores the cursor context before rebalancing so that a seek can be done later
    context: Option<CursorContext>,
    /// Store whether the Cursor is in a valid state. Meaning if it is pointing to a valid cell index or not
    pub valid_state: CursorValidState,
    seek_state: CursorSeekState,
    /// Separate state to read a record with overflow pages. This separation from `state` is necessary as
    /// we can be in a function that relies on `state`, but also needs to process overflow pages
    read_overflow_state: Option<ReadPayloadOverflow>,
    /// State machine for [BTreeCursor::is_empty_table]
    is_empty_table_state: EmptyTableState,
    /// State machine for [BTreeCursor::move_to_rightmost] and, optionally, the id of the rightmost page in the btree.
    /// If we know the rightmost page id and are already on that page, we can skip a seek.
    move_to_right_state: (MoveToRightState, Option<usize>),
    /// State machine for [BTreeCursor::seek_to_last]
    seek_to_last_state: SeekToLastState,
    /// State machine for [BTreeCursor::rewind]
    rewind_state: RewindState,
    /// State machine for [BTreeCursor::next] and [BTreeCursor::prev]
    advance_state: AdvanceState,
    /// State machine for [BTreeCursor::count]
    count_state: CountState,
    /// State machine for [BTreeCursor::seek_end]
    seek_end_state: SeekEndState,
    /// State machine for [BTreeCursor::move_to]
    move_to_state: MoveToState,
    /// Whether the next call to [BTreeCursor::next()] should be a no-op.
    /// This is currently only used after a delete operation causes a rebalancing.
    /// Advancing is only skipped if the cursor is currently pointing to a valid record
    /// when next() is called.
    pub skip_advance: bool,
    /// Reusable buffer for cell payloads during insert/update operations.
    /// This avoids allocating a new Vec for each write operation.
    reusable_cell_payload: crate::alloc::Vec<u8>,
    /// Per-cell access cache for incremental blob I/O. Caches the leaf cell's payload
    /// layout, the overflow-page-number array (Turso's runtime reconstruction of
    /// SQLite's `aOverflow`), and the byte range of the most recently accessed column,
    /// so repeated byte accesses to the same row avoid re-parsing the cell and record
    /// header and re-walking the overflow chain — turning each access into an O(1)
    /// page lookup. Invalidated when the cursor moves to a different (page, cell). The
    /// on-disk format is unchanged; this index lives only in RAM.
    blob_cache: BlobCellCache,
    /// Rowid the incremental-blob machinery last addressed. Unlike `blob_cache` it
    /// survives position saves: it is what `note_external_row_write` compares against
    /// to decide whether a peer's write hit *this* handle's row (expire, like SQLite's
    /// invalidateIncrblobCursors) or a different row (survivable via re-seek).
    blob_pinned_rowid: Option<i64>,
    /// Latched when an external write hits the pinned row or the position becomes
    /// unrecoverable. Every subsequent blob operation fails with
    /// [`LimboError::BlobHandleExpired`]; nothing ever clears it — SQLite's expired
    /// blob handles behave the same way until closed.
    blob_expired: bool,
    /// If `Some(page_idx)`, a previous call to [`BTreeCursor::get_next_record`]
    /// or [`BTreeCursor::get_prev_record`] yielded mid-descent into `page_idx`
    /// for spill IO, AFTER the loop-top `stack.advance()` / `stack.retreat()`
    /// mutations had already been applied. On re-entry, the traversal loop
    /// short-circuits to retry the read+descend rather than re-running those
    /// mutations and corrupting the cursor's cell-index state.
    iteration_pending_descent: Option<IterationPendingDescent>,
    /// (peers, idx) snapshot for the saveAllCursors pass driven from
    /// insert/delete. Carries iteration progress across IO re-entry
    /// (index records can yield via the overflow chain walk).
    pending_peer_save: Option<(
        smallvec::SmallVec<[crate::storage::pager::RegisteredCursor; 4]>,
        usize,
    )>,
    /// Mirrors SQLite's BTCF_Multiple. Toggled by Pager::register_cursor /
    /// unregister_cursor when the bucket crosses the 1↔2 threshold; lets
    /// drive_pending_peer_save skip the registry mutex in the common case.
    has_peers: crate::sync::atomic::AtomicBool,
    /// True if this cursor went through Cursor::new_btree (and thus pushed
    /// itself into the registry). Direct BTreeCursor::new callers (tests,
    /// internal utilities) bypass that path; their Drop skips unregister.
    did_register: crate::sync::atomic::AtomicBool,
    #[cfg(any(test, injected_yields))]
    yield_injector: Option<Arc<dyn crate::mvcc::yield_points::YieldInjector>>,
    #[cfg(any(test, injected_yields))]
    yield_instance_id: u64,
}

/// Records the in-flight descent for `iteration_pending_descent`. The direction
/// determines which `descend*` helper to apply once the page is read.
#[derive(Clone, Copy)]
enum IterationPendingDescent {
    Forwards(i64),
    Backwards(i64),
}

/// Cache backing the incremental-blob-I/O fast path (see [`BTreeCursor::blob_cache`]).
/// `valid`/`col_valid` gate the two tiers: the cell payload layout and the parsed byte
/// range of the last-accessed column. `overflow_pages[i]` is the i-th overflow page
/// number (`overflow_pages[0]` == the value's first overflow page); it grows lazily as
/// deeper offsets are touched. All offsets are payload-relative (0 == first payload byte)
/// except `local_off`, which is the leaf page byte offset of the local payload.
struct BlobCellCache {
    valid: bool,
    leaf_id: usize,
    cell_idx: usize,
    /// Byte offset of the local payload within the leaf page.
    local_off: usize,
    /// Number of payload bytes held locally on the leaf (the rest are in overflow).
    local_len: usize,
    /// Total payload size of the cell.
    payload_size: usize,
    /// Data bytes per overflow page (`usable - 4`).
    per: usize,
    first_overflow: Option<u32>,
    overflow_pages: crate::alloc::Vec<u32>,
    /// Most recently accessed overflow page and its index in `overflow_pages`, held
    /// for spatial locality: consecutive accesses landing on the same page reuse it
    /// instead of going back through the pager's page cache. The [`PinGuard`] type is
    /// load-bearing, not incidental: any `PageRef` kept live across a blob operation
    /// MUST be pinned, or the pager can evict it and take its buffer out from under
    /// the still-held reference (eviction does `buffer.take()` regardless of live
    /// `Arc<Page>` refs). Storing a `PinGuard` — which pins on construction and unpins
    /// on drop — makes an unpinned held page unrepresentable rather than a discipline
    /// to remember. Storing a raw page NUMBER (like `overflow_pages`) is the other
    /// safe option, because it is re-fetched through the pager on each use.
    last_ov_idx: usize,
    last_ov_page: Option<PinGuard>,
    col_valid: bool,
    col: usize,
    /// Payload-relative byte offset of column `col`'s value.
    col_body_off: usize,
    col_len: usize,
    /// Serial type of column `col`, kept so every access can re-assert the value is
    /// TEXT or BLOB (byte-addressable) without re-parsing the record header.
    col_serial: u64,
}

impl Default for BlobCellCache {
    fn default() -> Self {
        Self {
            valid: false,
            leaf_id: 0,
            cell_idx: 0,
            local_off: 0,
            local_len: 0,
            payload_size: 0,
            per: 0,
            first_overflow: None,
            overflow_pages: crate::alloc::vec![],
            last_ov_idx: 0,
            last_ov_page: None,
            col_valid: false,
            col: 0,
            col_body_off: 0,
            col_len: 0,
            col_serial: 0,
        }
    }
}

impl BlobCellCache {
    /// Drop every cached tier, including the pinned overflow page. Called whenever the
    /// cursor's position stops being trustworthy (external writes, stack invalidation)
    /// so no stale offset or page number can ever back a byte-level read or write.
    fn reset(&mut self) {
        self.valid = false;
        self.col_valid = false;
        self.first_overflow = None;
        self.overflow_pages.clear();
        self.release_pinned_overflow();
    }

    /// Pin `page` and remember it as the spatial-locality overflow page for `idx`,
    /// releasing whatever was pinned before. The [`PinGuard`] keeps the pager from
    /// evicting the page (and taking its buffer) while a blob handle still holds this
    /// reference — the same contract the cursor's page stack relies on, applied to
    /// the one overflow page kept live between blob operations.
    fn pin_overflow_page(&mut self, idx: usize, page: PageRef) {
        self.last_ov_idx = idx;
        // Assigning a fresh guard drops the previous one, unpinning the old page.
        self.last_ov_page = Some(PinGuard::new(page));
    }

    /// Forget the cached overflow page; dropping its [`PinGuard`] unpins the page.
    /// Idempotent.
    fn release_pinned_overflow(&mut self) {
        self.last_ov_page = None;
        self.last_ov_idx = 0;
    }
}

/// Upper bound on a well-formed record header: the header-size varint plus one
/// maximum-width (9 byte) serial-type varint per column, at SQLite's hard column
/// limit of 32767. Anything larger is corruption, and rejecting it also bounds the
/// allocation made when a spilled header is materialized from the overflow chain.
const MAX_RECORD_HEADER_SIZE: usize = 9 + 32767 * 9;

/// Where a run of payload bytes physically lives. A payload byte range maps to a
/// sequence of these; reading and writing are the same traversal with opposite copy
/// directions, so the (bug-prone) offset arithmetic lives here once, in
/// [`BTreeCursor::blob_span`], rather than being transcribed per operation.
enum BlobSpan {
    /// Bytes resident on the leaf page at byte offset `page_off` within it.
    Local { page_off: usize, take: usize },
    /// Bytes on overflow page number `idx` in the chain, `within` bytes past that
    /// page's 4-byte next-pointer.
    Overflow {
        idx: usize,
        within: usize,
        take: usize,
    },
}

/// Walk the serial types of a record header and locate `column`'s value. `header`
/// must be exactly the header bytes (so a varint can never stray into the body) and
/// `hpos0` the offset of the first serial type, i.e. the width of the header-size
/// varint. Returns the value's payload-relative byte offset, its byte length, and
/// its serial type, after checking the range lies inside `payload_size`.
fn blob_locate_column_in_header(
    header: &[u8],
    hpos0: usize,
    payload_size: usize,
    column: usize,
) -> Result<(usize, usize, u64)> {
    let header_size = header.len();
    let mut hpos = hpos0;
    let mut body = header_size;
    let mut col = 0usize;
    loop {
        if hpos >= header_size {
            return Err(LimboError::InternalError(format!(
                "blob column {column} out of range for row with {col} columns"
            )));
        }
        let (serial, n) = crate::storage::sqlite3_ondisk::read_varint(&header[hpos..])?;
        hpos += n;
        let size = crate::types::get_serial_type_size(serial)?;
        let end = body
            .checked_add(size)
            .ok_or_else(|| LimboError::Corrupt("record body offsets overflow usize".to_string()))?;
        if end > payload_size {
            return Err(LimboError::Corrupt(format!(
                "column {col} claims bytes {body}..{end} beyond payload size {payload_size}"
            )));
        }
        if col == column {
            return Ok((body, size, serial));
        }
        body = end;
        col += 1;
    }
}

crate::assert::assert_send!(BTreeCursor);
crate::assert::assert_sync!(BTreeCursor);

/// We store the cell index and cell count for each page in the stack.
/// The reason we store the cell count is because we need to know when we are at the end of the page,
/// without having to perform IO to get the ancestor pages.
#[derive(Debug, Clone, Copy, Default)]
struct BTreeNodeState {
    cell_idx: i32,
    cell_count: Option<i32>,
}

impl BTreeNodeState {
    /// Check if the current cell index is at the end of the page.
    /// This information is used to determine whether a child page should move up to its parent.
    /// If the child page is the rightmost leaf page and it has reached the end, this means all of its ancestors have
    /// already reached the end, so it should not go up because there are no more records to traverse.
    fn is_at_end(&self) -> bool {
        let cell_count = self.cell_count.expect("cell_count is not set");
        // cell_idx == cell_count means: we will traverse to the rightmost pointer next.
        // cell_idx == cell_count + 1 means: we have already gone down to the rightmost pointer.
        self.cell_idx == cell_count + 1
    }
}

impl BTreeCursor {
    pub fn new(pager: Arc<Pager>, root_page: i64, _num_columns: usize) -> Self {
        let valid_state = if root_page == 1 && !pager.db_initialized() {
            CursorValidState::Invalid
        } else {
            CursorValidState::Valid
        };
        let usable_space = pager.usable_space();
        Self {
            pager,
            root_page,
            usable_space_cached: usable_space,
            has_record: false,
            null_flag: false,
            going_upwards: false,
            state: CursorState::None,
            balance_state: BalanceState::default(),
            overflow_state: OverflowState::Start,
            stack: PageStack {
                current_page: -1,
                node_states: [BTreeNodeState::default(); BTCURSOR_MAX_DEPTH + 1],
                stack: [const { None }; BTCURSOR_MAX_DEPTH + 1],
            },
            reusable_immutable_record: None,
            index_info: None,
            count: 0,
            context: None,
            valid_state,
            seek_state: CursorSeekState::Start,
            read_overflow_state: None,
            is_empty_table_state: EmptyTableState::Start,
            move_to_right_state: (MoveToRightState::Start, None),
            seek_to_last_state: SeekToLastState::Start,
            rewind_state: RewindState::Start,
            advance_state: AdvanceState::Start,
            count_state: CountState::Start,
            seek_end_state: SeekEndState::Start,
            move_to_state: MoveToState::Start,
            skip_advance: false,
            reusable_cell_payload: crate::alloc::vec![],
            blob_cache: BlobCellCache::default(),
            blob_pinned_rowid: None,
            blob_expired: false,
            iteration_pending_descent: None,
            pending_peer_save: None,
            has_peers: crate::sync::atomic::AtomicBool::new(false),
            did_register: crate::sync::atomic::AtomicBool::new(false),
            #[cfg(any(test, injected_yields))]
            yield_injector: None,
            #[cfg(any(test, injected_yields))]
            yield_instance_id: 0,
        }
    }

    #[cfg(any(test, injected_yields))]
    pub(crate) fn install_yield_context(&mut self, connection: &crate::Connection) {
        self.yield_injector = connection.yield_injector();
        self.yield_instance_id = connection.next_yield_instance_id();
    }

    pub fn new_table(pager: Arc<Pager>, root_page: i64, num_columns: usize) -> Self {
        Self::new(pager, root_page, num_columns)
    }

    pub fn new_without_rowid_table(
        pager: Arc<Pager>,
        root_page: i64,
        table: &BTreeTable,
        num_columns: usize,
    ) -> Self {
        let mut cursor = Self::new(pager, root_page, num_columns);
        let key_info = table.primary_key_columns.iter().map(|(col_name, order)| {
            let (_, column) = table
                .get_column(col_name)
                .expect("WITHOUT ROWID primary key column should exist");
            crate::types::KeyInfo {
                sort_order: *order,
                collation: column.collation_opt().unwrap_or_default(),
                nulls_order: None,
            }
        });
        cursor.index_info = Some(Arc::new(
            IndexInfo::new(key_info, false, table.primary_key_columns.len(), true)
                .expect(crate::alloc::ALLOC_ERR_MSG),
        ));
        cursor
    }

    pub fn new_index(
        pager: Arc<Pager>,
        root_page: i64,
        index: &Index,
        num_columns: usize,
    ) -> Result<Self> {
        let mut cursor = Self::new(pager, root_page, num_columns);
        cursor.index_info = Some(Arc::new(IndexInfo::new_from_index(index)?));
        Ok(cursor)
    }

    /// Resets the cached count state so the next `count()` call re-traverses the
    /// btree. Must be called after any mutation (insert, delete, clear) that may
    /// change the number of rows in the tree.
    fn invalidate_count_cache(&mut self) {
        self.count_state = CountState::Start;
        self.count = 0;
    }

    pub fn get_index_rowid_from_record(&self) -> Option<i64> {
        if !self.has_rowid() {
            return None;
        }
        let rowid = match self.get_immutable_record().as_ref().unwrap().last_value() {
            Some(Ok(ValueRef::Numeric(Numeric::Integer(rowid)))) => rowid,
            _ => unreachable!(
                "index where has_rowid() is true should have an integer rowid as the last value"
            ),
        };
        Some(rowid)
    }

    /// Check if the table is empty.
    /// This is done by checking if the root page has no cells.
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn is_empty_table(&mut self) -> IOResultOr<bool> {
        loop {
            let state = self.is_empty_table_state.clone();
            match state {
                EmptyTableState::Start => {
                    // On spill `return_if_io!` propagates `IO` up unchanged —
                    // we have not produced a page yet, the state stays at
                    // `Start`, and re-entry resumes here with the pager's
                    // pending-read tracking returning the same PageRef.
                    let (page, c) = return_if_io!(self.pager.read_page(self.root_page));
                    self.is_empty_table_state = EmptyTableState::ReadPage { page };
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                EmptyTableState::ReadPage { page } => {
                    turso_assert!(page.is_loaded(), "page should be loaded");
                    let cell_count = page.get_contents().cell_count();
                    break Ok(IOResult::Done(cell_count == 0));
                }
            }
        }
    }

    /// Move the cursor to the previous record and return it.
    /// Used in backwards iteration.
    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG, name = "prev"))]
    pub fn get_prev_record(&mut self) -> IOResultOr<()> {
        let mut inner = || {
            loop {
                // Resume hook: if a previous backwards-iteration call yielded
                // for spill IO mid-descent, the loop-top mutations
                // (cell_idx set, `stack.retreat()` for IndexInterior) have
                // already been applied. Retry the read+descend without
                // re-running them.
                if let Some(IterationPendingDescent::Backwards(target)) =
                    self.iteration_pending_descent
                {
                    let (mem_page, c) = return_if_io!(self.pager.read_page(target));
                    self.iteration_pending_descent = None;
                    self.descend_backwards(mem_page);
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                    continue;
                }
                let (old_top_idx, page_type, is_index, is_leaf, cell_count) = {
                    let page = self.stack.top_ref();
                    let contents = page.get_contents();
                    (
                        self.stack.current(),
                        contents.page_type()?,
                        page.is_index()?,
                        contents.is_leaf(),
                        contents.cell_count(),
                    )
                };

                let cell_idx = self.stack.current_cell_index();

                // If we are at the end of the page and we haven't just come back from the right child,
                // we now need to move to the rightmost child.
                if cell_idx == i32::MAX && !self.going_upwards {
                    let rightmost_pointer =
                        self.stack.top_ref().get_contents().rightmost_pointer()?;
                    if let Some(rightmost_pointer) = rightmost_pointer {
                        let past_rightmost_pointer = cell_count as i32 + 1;
                        // On `IO(spill_c)` we must NOT mutate `cell_idx` or
                        // descend; the loop's outer match on `cell_idx ==
                        // i32::MAX` would not re-fire if `set_cell_index`
                        // had moved us past it.
                        let (page, c) = return_if_io!(self.read_page(rightmost_pointer as i64));
                        self.stack.set_cell_index(past_rightmost_pointer);
                        self.descend_backwards(page);
                        if let Some(c) = c {
                            io_yield_one!(c);
                        }
                        continue;
                    }
                }

                if cell_idx >= cell_count as i32 {
                    self.stack.set_cell_index(cell_count as i32 - 1);
                } else if !self.stack.current_cell_index_less_than_min() {
                    // skip retreat in case we still haven't visited this cell in index
                    let should_visit_internal_node = is_index && self.going_upwards; // we are going upwards, this means we still need to visit divider cell in an index
                    if should_visit_internal_node {
                        self.going_upwards = false;
                        return Ok::<_, Box<crate::LimboError>>(IOResult::Done(true));
                    } else if matches!(
                        page_type,
                        PageType::IndexLeaf | PageType::TableLeaf | PageType::TableInterior
                    ) {
                        self.stack.retreat();
                    }
                }
                // moved to beginning of current page
                // todo: find a better way to flag moved to end or begin of page
                if self.stack.current_cell_index_less_than_min() {
                    loop {
                        if self.stack.current_cell_index() >= 0 {
                            break;
                        }
                        if self.stack.has_parent() {
                            self.pop_upwards();
                        } else {
                            // moved to begin of btree
                            return Ok(IOResult::Done(false));
                        }
                    }
                    // continue to next loop to get record from the new page
                    continue;
                }
                if is_leaf {
                    return Ok(IOResult::Done(true));
                }

                if is_index && self.going_upwards {
                    // If we are going upwards, we need to visit the divider cell before going back to another child page.
                    // This is because index interior cells have payloads, so unless we do this we will be skipping an entry when traversing the tree.
                    self.going_upwards = false;
                    return Ok(IOResult::Done(true));
                }

                let cell_idx = self.stack.current_cell_index() as usize;
                let left_child_page = self
                    .stack
                    .get_page_contents_at_level(old_top_idx)
                    .unwrap()
                    .cell_interior_read_left_child_page(cell_idx)?;

                if page_type == PageType::IndexInterior {
                    // In backwards iteration, if we haven't just moved to this interior node from the
                    // right child, but instead are about to move to the left child, we need to retreat
                    // so that we don't come back to this node again.
                    // For example:
                    // this parent: key 666
                    // left child has: key 663, key 664, key 665
                    // we need to move to the previous parent (with e.g. key 662) when iterating backwards.
                    self.stack.retreat();
                }

                // The loop-top mutations (cell_idx set above, optional
                // `stack.retreat()` for IndexInterior) have already been
                // applied for this step. Route a spill yield through
                // `iteration_pending_descent` so the resume hook at the top
                // of the loop replays only the read+descend on re-entry.
                match self.pager.read_page(left_child_page as i64)? {
                    IOResult::Done((mem_page, c)) => {
                        self.descend_backwards(mem_page);
                        if let Some(c) = c {
                            io_yield_one!(c);
                        }
                    }
                    IOResult::IO(IOCompletions(spill_c)) => {
                        self.iteration_pending_descent =
                            Some(IterationPendingDescent::Backwards(left_child_page as i64));
                        io_yield_one!(spill_c);
                    }
                }
            }
        };

        let has_record = return_if_io!(inner());
        self.invalidate_record();
        self.set_has_record(has_record);
        Ok(IOResult::Done(()))
    }

    /// Reads the record of a cell that has overflow pages. This is a state machine that requires to be called until completion so everything
    /// that calls this function should be reentrant.
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn process_overflow_read(
        &mut self,
        payload: &'static [u8],
        start_next_page: u32,
        payload_size: u64,
    ) -> IOResultOr<()> {
        loop {
            if self.read_overflow_state.is_none() {
                let remaining_to_read =
                    payload_size
                        .checked_sub(payload.len() as u64)
                        .ok_or_else(|| {
                            LimboError::Corrupt(
                                "payload size is smaller than local payload bytes".to_string(),
                            )
                        })? as usize;
                // We must not populate `read_overflow_state` before the page
                // is actually produced — otherwise an `IO(spill_c)` yield
                // would leave `read_overflow_state` half-initialized on a
                // page that doesn't exist yet, and re-entry would skip the
                // `is_none()` branch entirely.
                let (page, c) = return_if_io!(self.read_page(start_next_page as i64));
                let payload =
                    crate::with_btree_allocation_site!(OverflowRead, payload.try_to_vec())?;
                self.read_overflow_state.replace(ReadPayloadOverflow {
                    payload,
                    next_page: start_next_page,
                    remaining_to_read,
                    page,
                });
                if let Some(c) = c {
                    io_yield_one!(c);
                }
                continue;
            }
            // Compute `next` / `to_read` and fetch the next chain page (if
            // any) BEFORE applying the loop body's non-idempotent mutations,
            // so that a spill yield from `read_page(next)` leaves
            // `read_overflow_state` untouched and is safe to re-enter.
            let (next, to_read, need_next_page) = {
                let state = self.read_overflow_state.as_ref().unwrap();
                turso_assert!(state.page.is_loaded(), "page should be loaded");
                tracing::debug!(
                    next_page = state.next_page,
                    remaining_to_read = state.remaining_to_read,
                    "reading overflow page"
                );
                // The first four bytes of each overflow page are a big-endian integer which is the page number of the next page in the chain, or zero for the final page in the chain.
                let next = state.page.get_contents().read_u32_no_offset(0);
                let to_read = state.remaining_to_read.min(self.pager.usable_space() - 4);
                (
                    next,
                    to_read,
                    state.remaining_to_read > to_read && next != 0,
                )
            };
            let new_page_and_c = if need_next_page {
                Some(return_if_io!(self.read_page(next as i64)))
            } else {
                None
            };

            let ReadPayloadOverflow {
                payload,
                remaining_to_read,
                next_page,
                page,
            } = self.read_overflow_state.as_mut().unwrap();
            let buf = page.get_contents().as_ptr();
            crate::with_btree_allocation_site!(
                OverflowRead,
                payload.try_extend(buf[4..4 + to_read].iter().copied())
            )?;
            *remaining_to_read -= to_read;

            if let Some((new_page, c)) = new_page_and_c {
                *page = new_page;
                *next_page = next;
                // Re-entrancy: the four mutations above (payload extend,
                // remaining decrement, page swap, next_page swap) together
                // advance the state to "current page consumed, positioned on
                // new_page". Yielding on `c` here is safe because re-entry
                // resumes one iteration forward — the loop top reads from
                // the new page, not the old one — so none of these mutations
                // re-fire against the page they were applied to.
                if let Some(c) = c {
                    io_yield_one!(c);
                }
                continue;
            }
            if *remaining_to_read != 0 || next != 0 {
                let chain_page = *next_page;
                let remaining = *remaining_to_read;
                self.read_overflow_state.take();
                tracing::warn!(
                    chain_page,
                    next,
                    remaining,
                    "inconsistent overflow chain observed during payload read"
                );
                return Err(LimboError::Corrupt(
                    "inconsistent overflow chain observed during payload read".to_string(),
                )
                .into());
            }
            // Take the whole state before the fallible record allocations below,
            // like the inconsistent-chain branch above: an error must not leave
            // behind resumable state whose payload was already moved out, or a
            // retry would silently complete with an empty record.
            let payload_swap = self
                .read_overflow_state
                .take()
                .expect("read_overflow_state was checked above")
                .payload;

            let mut reuse_immutable = self.get_immutable_record_or_create()?;
            reuse_immutable.as_mut().unwrap().invalidate();

            crate::with_btree_allocation_site!(
                RecordPayload,
                reuse_immutable
                    .as_mut()
                    .unwrap()
                    .start_serialization(&payload_swap)
            )?;

            break Ok(IOResult::Done(()));
        }
    }

    /// Check if any ancestor pages still have cells to iterate.
    /// If not, traversing back up to parent is of no use because we are at the end of the tree.
    fn ancestor_pages_have_more_children(&self) -> bool {
        let node_states = self.stack.node_states;
        (0..self.stack.current())
            .rev()
            .any(|idx| !node_states[idx].is_at_end())
    }

    /// Move the cursor to the next record and return it.
    /// Used in forwards iteration, which is the default.
    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG, name = "next"))]
    pub fn get_next_record(&mut self) -> IOResultOr<()> {
        let mut inner = || {
            if self.stack.current_page == -1 {
                // This can happen in nested left joins. See:
                // https://github.com/tursodatabase/turso/issues/2924
                return Ok::<_, Box<crate::LimboError>>(IOResult::Done(false));
            }
            loop {
                // Resume hook: if a previous call yielded for spill IO mid-
                // descent, the loop-top mutations (stack.advance) have
                // already been applied. Retry the read+descend without
                // re-running them. If the spill is still pending the pager's
                // `pending_reads` memoization will return IO again; otherwise
                // it returns Done immediately and we descend.
                if let Some(IterationPendingDescent::Forwards(target)) =
                    self.iteration_pending_descent
                {
                    let (mem_page, c) = return_if_io!(self.pager.read_page(target));
                    self.iteration_pending_descent = None;
                    self.descend(mem_page);
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                    continue;
                }
                let mem_page = self.stack.top_ref();
                let contents = mem_page.get_contents();
                let cell_idx = self.stack.current_cell_index();
                let cell_count = contents.cell_count();
                let is_leaf = contents.is_leaf();
                if cell_idx != -1 && is_leaf && cell_idx as usize + 1 < cell_count {
                    self.stack.advance();
                    return Ok(IOResult::Done(true));
                }

                let mem_page = mem_page.clone();
                let contents = mem_page.get_contents();
                tracing::debug!(
                    id = mem_page.get().id,
                    cell = self.stack.current_cell_index(),
                    cell_count,
                    "current_before_advance",
                );

                let is_index = mem_page.is_index()?;
                let should_skip_advance = is_index
                && self.going_upwards // we are going upwards, this means we still need to visit divider cell in an index
                && self.stack.current_cell_index() >= 0 && self.stack.current_cell_index() < cell_count as i32; // if we weren't on a
                                                                                                                // valid cell then it means we will have to move upwards again or move to right page,
                                                                                                                // anyways, we won't visit this invalid cell index
                if should_skip_advance {
                    tracing::debug!(
                        going_upwards = self.going_upwards,
                        page = mem_page.get().id,
                        cell_idx = self.stack.current_cell_index(),
                        "skipping advance",
                    );
                    self.going_upwards = false;
                    return Ok(IOResult::Done(true));
                }

                // Important to advance only after loading the page in order to not advance > 1 times
                self.stack.advance();
                let cell_idx = self.stack.current_cell_index() as usize;
                tracing::debug!(id = mem_page.get().id, cell = cell_idx, "current");

                if cell_idx >= cell_count {
                    let rightmost_already_traversed = cell_idx > cell_count;
                    match (contents.rightmost_pointer()?, rightmost_already_traversed) {
                        (Some(right_most_pointer), false) => {
                            // do rightmost
                            self.stack.advance();
                            // Spill yield from here would re-enter the loop
                            // top with cell_idx already advanced twice; we
                            // record the descent target in
                            // `iteration_pending_descent` so the resume hook
                            // above skips the loop-top advances on re-entry.
                            match self.pager.read_page(right_most_pointer as i64)? {
                                IOResult::Done((mem_page, c)) => {
                                    self.descend(mem_page);
                                    if let Some(c) = c {
                                        io_yield_one!(c);
                                    }
                                    continue;
                                }
                                IOResult::IO(IOCompletions(spill_c)) => {
                                    self.iteration_pending_descent =
                                        Some(IterationPendingDescent::Forwards(
                                            right_most_pointer as i64,
                                        ));
                                    io_yield_one!(spill_c);
                                }
                            }
                        }
                        _ => {
                            if self.ancestor_pages_have_more_children() {
                                tracing::trace!("moving simple upwards");
                                self.pop_upwards();
                                continue;
                            } else {
                                // If none of the ancestor pages have more children to iterate, that means we are at the end of the btree and should stop iterating.
                                return Ok(IOResult::Done(false));
                            }
                        }
                    }
                }

                turso_assert!(
                    cell_idx < cell_count,
                    "cell index out of bounds",
                    { "cell_idx": cell_idx, "cell_count": cell_count, "page_type": contents.page_type().ok(), "page_id": mem_page.get().id }
                );

                if is_leaf {
                    return Ok(IOResult::Done(true));
                }
                if is_index && self.going_upwards {
                    // This means we just came up from a child, so now we need to visit the divider cell before going back to another child page.
                    // This is because index interior cells have payloads, so unless we do this we will be skipping an entry when traversing the tree.
                    self.going_upwards = false;
                    return Ok(IOResult::Done(true));
                }

                let left_child_page = contents.cell_interior_read_left_child_page(cell_idx)?;
                // Same re-entry handling as the rightmost branch above —
                // the loop-top `stack.advance()` has already fired for this
                // step, so we route a spill yield through
                // `iteration_pending_descent`.
                match self.pager.read_page(left_child_page as i64)? {
                    IOResult::Done((mem_page, c)) => {
                        self.descend(mem_page);
                        if let Some(c) = c {
                            io_yield_one!(c);
                        }
                    }
                    IOResult::IO(IOCompletions(spill_c)) => {
                        self.iteration_pending_descent =
                            Some(IterationPendingDescent::Forwards(left_child_page as i64));
                        io_yield_one!(spill_c);
                    }
                }
            }
        };
        let has_record = return_if_io!(inner());
        self.invalidate_record();
        self.set_has_record(has_record);
        Ok(IOResult::Done(()))
    }

    /// Move the cursor to the record that matches the seek key and seek operation.
    /// This may be used to seek to a specific record in a point query (e.g. SELECT * FROM table WHERE col = 10)
    /// or e.g. find the first record greater than the seek key in a range query (e.g. SELECT * FROM table WHERE col > 10).
    /// We don't include the rowid in the comparison and that's why the last value from the record is not included.
    fn do_seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> IOResultOr<SeekResult> {
        let ret = return_if_io!(match &key {
            SeekKey::TableRowId(rowid) => self.tablebtree_seek(*rowid, op),
            SeekKey::IndexKey(index_key) => {
                self.indexbtree_seek(index_key, op)
            }
        });
        self.valid_state = CursorValidState::Valid;
        Ok(IOResult::Done(ret))
    }

    fn do_seek_unpacked(&mut self, registers: &[Register], op: SeekOp) -> IOResultOr<SeekResult> {
        let ret = return_if_io!(self.indexbtree_seek_unpacked(registers, op));
        self.valid_state = CursorValidState::Valid;
        Ok(IOResult::Done(ret))
    }

    /// Pop the stack and mark that we are going upwards in the B-tree.
    /// This is the only place where `going_upwards` should be set to `true`.
    fn pop_upwards(&mut self) {
        self.going_upwards = true;
        self.stack.pop();
    }

    /// Descend into a child page during forward iteration.
    /// Clears the `going_upwards` flag — once we descend, we are no longer going upwards.
    fn descend(&mut self, page: PageRef) {
        self.going_upwards = false;
        self.stack.push(page);
    }

    /// Descend into a child page during backward iteration.
    /// Clears the `going_upwards` flag — once we descend, we are no longer going upwards.
    fn descend_backwards(&mut self, page: PageRef) {
        self.going_upwards = false;
        self.stack.push_backwards(page);
    }

    /// Move the cursor to the root page of the btree.
    ///
    /// Blocking shim retained for tests and any caller that doesn't have an
    /// outer state machine yet. Production state machines should prefer
    /// [`BTreeCursor::move_to_root_nonblock`] so they can yield through spill
    /// IO instead of blocking inside the pager.
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn move_to_root(&mut self) -> Result<Option<Completion>> {
        let io = self.pager.io.clone();
        io.block(|| self.move_to_root_nonblock())
    }

    /// Non-blocking variant of [`BTreeCursor::move_to_root`].
    ///
    /// Re-entrancy: `seek_state`, `going_upwards`, `stack.clear()` and
    /// `stack.push(root)` are all idempotent across re-entry — clearing an
    /// already-cleared stack and pushing the same root yields the same state.
    /// The disk-read completion (if any) is returned as the `Done` payload for
    /// the caller to yield itself, matching the contract of the legacy
    /// `move_to_root`.
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn move_to_root_nonblock(&mut self) -> IOResultOr<Option<Completion>> {
        self.seek_state = CursorSeekState::Start;
        self.going_upwards = false;
        tracing::trace!(root_page = self.root_page);
        let (mem_page, c) = return_if_io!(self.read_page(self.root_page));
        self.stack.clear();
        self.stack.push(mem_page);
        Ok(IOResult::Done(c))
    }

    /// Move the cursor to the rightmost record in the btree.
    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    fn move_to_rightmost(&mut self) -> IOResultOr<bool> {
        loop {
            let (move_to_right_state, rightmost_page_id) = &self.move_to_right_state;
            match *move_to_right_state {
                MoveToRightState::Start => {
                    if let Some(rightmost_page_id) = rightmost_page_id {
                        // If we know the rightmost page and are already on it, we can skip a seek.
                        // The cache is safe to trust: any modification of this btree — our own
                        // balancing, or a peer cursor's write (e.g. a trigger subprogram's, via
                        // the saveAllCursors pass) — invalidates it.
                        let current_page = self.stack.top_ref();
                        if current_page.get().id == *rightmost_page_id {
                            let contents = current_page.get_contents();
                            let cell_count = contents.cell_count();
                            self.stack.set_cell_index(cell_count as i32 - 1);
                            return Ok(IOResult::Done(cell_count > 0));
                        }
                    }
                    let rightmost_page_id = *rightmost_page_id;
                    let c = return_if_io!(self.move_to_root_nonblock());
                    self.move_to_right_state = (MoveToRightState::ProcessPage, rightmost_page_id);
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                MoveToRightState::ProcessPage => {
                    let mem_page = self.stack.top_ref();
                    let page_idx = mem_page.get().id;
                    let contents = mem_page.get_contents();
                    if contents.is_leaf() {
                        self.move_to_right_state = (MoveToRightState::Start, Some(page_idx));
                        if contents.cell_count() > 0 {
                            self.stack.set_cell_index(contents.cell_count() as i32 - 1);
                            return Ok(IOResult::Done(true));
                        }
                        return Ok(IOResult::Done(false));
                    }

                    match contents.rightmost_pointer()? {
                        Some(right_most_pointer) => {
                            // On `IO(spill_c)` the stack is unchanged, so re-entry
                            // re-reads the same parent contents and retries the
                            // descent — the disk-read for this child is memoized
                            // in `pending_reads`, so no duplicate IO is issued.
                            let (mem_page, c) =
                                return_if_io!(self.read_page(right_most_pointer as i64));
                            self.stack.set_cell_index(contents.cell_count() as i32 + 1);
                            self.stack.push(mem_page);
                            if let Some(c) = c {
                                io_yield_one!(c);
                            }
                        }
                        None => {
                            unreachable!("interior page should have a rightmost pointer");
                        }
                    }
                }
            }
        }
    }

    /// Specialized version of move_to() for table btrees.
    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    fn tablebtree_move_to(&mut self, rowid: i64, seek_op: SeekOp) -> IOResultOr<()> {
        loop {
            let (old_top_idx, is_leaf, cell_count) = {
                let page = self.stack.top_ref();
                let contents = page.get_contents();
                (
                    self.stack.current(),
                    contents.is_leaf(),
                    contents.cell_count(),
                )
            };

            if is_leaf {
                self.seek_state = CursorSeekState::FoundLeaf { eq_seen: false };
                return Ok(IOResult::Done(()));
            }

            if matches!(
                self.seek_state,
                CursorSeekState::Start | CursorSeekState::MovingBetweenPages { .. }
            ) {
                let eq_seen = match &self.seek_state {
                    CursorSeekState::MovingBetweenPages { eq_seen } => *eq_seen,
                    _ => false,
                };
                let min_cell_idx = 0;
                let max_cell_idx = cell_count as isize - 1;
                let nearest_matching_cell = None;

                self.seek_state = CursorSeekState::InteriorPageBinarySearch {
                    state: InteriorPageBinarySearchState {
                        min_cell_idx,
                        max_cell_idx,
                        nearest_matching_cell,
                        eq_seen,
                    },
                };
            }

            let CursorSeekState::InteriorPageBinarySearch { state } = &self.seek_state else {
                unreachable!("we must be in an interior binary search state");
            };

            let mut state = *state;

            let control =
                self.tablebtree_move_inner(rowid, seek_op, old_top_idx, cell_count, &mut state)?;
            // Persist state if inner function didn't change seek_state to something else (e.g., MovingBetweenPages)
            if matches!(
                self.seek_state,
                CursorSeekState::InteriorPageBinarySearch { .. }
            ) {
                self.seek_state = CursorSeekState::InteriorPageBinarySearch { state };
            }
            match control {
                ControlFlow::Continue(_) => {}
                ControlFlow::Break(result) => {
                    return Ok(result);
                }
            }
        }
    }

    fn tablebtree_move_inner(
        &mut self,
        rowid: i64,
        seek_op: SeekOp,
        old_top_idx: usize,
        cell_count: usize,
        state: &mut InteriorPageBinarySearchState,
    ) -> Result<ControlFlow<IOResult<()>>> {
        let min = state.min_cell_idx;
        let max = state.max_cell_idx;
        if min > max {
            if let Some(nearest_matching_cell) = state.nearest_matching_cell {
                let left_child_page = self
                    .stack
                    .get_page_contents_at_level(old_top_idx)
                    .unwrap()
                    .cell_interior_read_left_child_page(nearest_matching_cell)?;
                // On `IO(spill_c)` we keep `seek_state` at
                // `InteriorPageBinarySearch` (the caller persists `state` to
                // it after we return), so re-entry retries this same step
                // with no double-push and no cell-index drift.
                match self.read_page(left_child_page as i64)? {
                    IOResult::Done((mem_page, c)) => {
                        self.stack.set_cell_index(nearest_matching_cell as i32);
                        self.stack.push(mem_page);
                        self.seek_state = CursorSeekState::MovingBetweenPages {
                            eq_seen: state.eq_seen,
                        };
                        if let Some(c) = c {
                            return Ok(ControlFlow::Break(IOResult::IO(IOCompletions(c))));
                        }
                        return Ok(ControlFlow::Continue(()));
                    }
                    IOResult::IO(IOCompletions(spill_c)) => {
                        return Ok(ControlFlow::Break(IOResult::IO(IOCompletions(spill_c))));
                    }
                }
            }
            match self
                .stack
                .get_page_contents_at_level(old_top_idx)
                .unwrap()
                .rightmost_pointer()?
            {
                Some(right_most_pointer) => match self.read_page(right_most_pointer as i64)? {
                    IOResult::Done((mem_page, c)) => {
                        self.stack.set_cell_index(cell_count as i32 + 1);
                        self.stack.push(mem_page);
                        self.seek_state = CursorSeekState::MovingBetweenPages {
                            eq_seen: state.eq_seen,
                        };
                        if let Some(c) = c {
                            return Ok(ControlFlow::Break(IOResult::IO(IOCompletions(c))));
                        }
                        return Ok(ControlFlow::Continue(()));
                    }
                    IOResult::IO(IOCompletions(spill_c)) => {
                        return Ok(ControlFlow::Break(IOResult::IO(IOCompletions(spill_c))));
                    }
                },
                None => {
                    unreachable!("we shall not go back up! The only way is down the slope");
                }
            }
        }
        let cur_cell_idx = (min + max) >> 1; // rustc generates extra insns for (min+max)/2 due to them being isize. we know min&max are >=0 here.
        let cell_rowid = self
            .stack
            .get_page_contents_at_level(old_top_idx)
            .unwrap()
            .cell_table_interior_read_rowid(cur_cell_idx as usize)?;
        // in sqlite btrees left child pages have <= keys.
        // table btrees can have a duplicate rowid in the interior cell, so for example if we are looking for rowid=10,
        // and we find an interior cell with rowid=10, we need to move to the left page since (due to the <= rule of sqlite btrees)
        // the left page may have a rowid=10.
        // Logic table for determining if target leaf page is in left subtree
        //
        // Forwards iteration (looking for first match in tree):
        // OP  | Current Cell vs Seek Key   | Action?  | Explanation
        // GT  | >                          | go left  | First > key is in left subtree
        // GT  | = or <                     | go right | First > key is in right subtree
        // GE  | > or =                     | go left  | First >= key is in left subtree
        // GE  | <                          | go right | First >= key is in right subtree
        //
        // Backwards iteration (looking for last match in tree):
        // OP  | Current Cell vs Seek Key   | Action?  | Explanation
        // LE  | > or =                     | go left  | Last <= key is in left subtree
        // LE  | <                          | go right | Last <= key is in right subtree
        // LT  | > or =                     | go left  | Last < key is in left subtree
        // LT  | <                          | go right?| Last < key is in right subtree, except if cell rowid is exactly 1 less
        //
        // No iteration (point query):
        // EQ  | > or =                     | go left  | Last = key is in left subtree
        // EQ  | <                          | go right | Last = key is in right subtree
        let is_on_left = match seek_op {
            SeekOp::GT => cell_rowid > rowid,
            SeekOp::GE { .. } => cell_rowid >= rowid,
            SeekOp::LE { .. } => cell_rowid >= rowid,
            SeekOp::LT => cell_rowid + 1 >= rowid,
        };
        if is_on_left {
            state.nearest_matching_cell.replace(cur_cell_idx as usize);
            state.max_cell_idx = cur_cell_idx - 1;
        } else {
            state.min_cell_idx = cur_cell_idx + 1;
        }
        Ok(ControlFlow::Continue(()))
    }

    /// Specialized version of move_to() for index btrees.
    #[cfg_attr(debug_assertions, instrument(skip(self, index_key), level = Level::DEBUG))]
    fn indexbtree_move_to(
        &mut self,
        index_key: &ImmutableRecordRef<'_>,
        cmp: SeekOp,
    ) -> IOResultOr<()> {
        let key_values = index_key.get_values()?;
        let record_comparer = {
            let index_info = self
                .index_info
                .as_ref()
                .expect("indexbtree_move_to: index_info required");
            find_compare(key_values.iter().peekable(), index_info)
        };
        self.indexbtree_move_to_internal(cmp, record_comparer, &key_values)
    }

    /// Move cursor to position using registers directly, avoiding record serialization.
    /// See `seek_unpacked` for rationale.
    #[instrument(skip(self, registers), level = Level::DEBUG)]
    fn indexbtree_move_to_unpacked(
        &mut self,
        registers: &[Register],
        cmp: SeekOp,
    ) -> IOResultOr<()> {
        if matches!(
            self.seek_state,
            CursorSeekState::LeafPageBinarySearch { .. } | CursorSeekState::FoundLeaf { .. }
        ) {
            self.seek_state = CursorSeekState::Start;
        }

        if matches!(self.seek_state, CursorSeekState::Start) {
            if let Some(c) = return_if_io!(self.move_to_root_nonblock()) {
                return Ok(IOResult::IO(IOCompletions(c)));
            }
        }

        let index_info = self
            .index_info
            .as_ref()
            .expect("indexbtree_move_to_unpacked: index_info required");

        let key_values: SmallVec<[ValueRef<'_>; STACK_ALLOC_KEY_VALS_MAX]> = registers
            .iter()
            .map(|r| r.get_value().as_value_ref())
            .collect();
        let record_comparer = find_compare(key_values.iter().peekable(), index_info);
        self.indexbtree_move_to_internal(cmp, record_comparer, &key_values)
    }

    fn indexbtree_move_to_internal(
        &mut self,
        cmp: SeekOp,
        record_comparer: RecordCompare,
        key_values: &[ValueRef<'_>],
    ) -> IOResultOr<()> {
        tracing::debug!("Using record comparison strategy: {:?}", record_comparer);
        let tie_breaker = get_tie_breaker_from_seek_op(cmp);

        loop {
            let (old_top_idx, is_leaf, cell_count) = {
                let page = self.stack.top_ref();
                let contents = page.get_contents();
                (
                    self.stack.current(),
                    contents.is_leaf(),
                    contents.cell_count(),
                )
            };

            if is_leaf {
                let eq_seen = match &self.seek_state {
                    CursorSeekState::MovingBetweenPages { eq_seen } => *eq_seen,
                    _ => false,
                };
                self.seek_state = CursorSeekState::FoundLeaf { eq_seen };
                return Ok(IOResult::Done(()));
            }

            if matches!(
                self.seek_state,
                CursorSeekState::Start | CursorSeekState::MovingBetweenPages { .. }
            ) {
                let eq_seen = match &self.seek_state {
                    CursorSeekState::MovingBetweenPages { eq_seen } => *eq_seen,
                    _ => false,
                };
                let min_cell_idx = 0;
                let max_cell_idx = cell_count as isize - 1;
                let nearest_matching_cell = None;

                self.seek_state = CursorSeekState::InteriorPageBinarySearch {
                    state: InteriorPageBinarySearchState {
                        min_cell_idx,
                        max_cell_idx,
                        nearest_matching_cell,
                        eq_seen,
                    },
                };
            }

            let CursorSeekState::InteriorPageBinarySearch { state } = &self.seek_state else {
                unreachable!(
                    "we must be in an interior binary search state, got {:?}",
                    self.seek_state
                );
            };

            let mut state = *state;

            let control = self.indexbtree_move_to_inner(
                cmp,
                old_top_idx,
                cell_count,
                record_comparer,
                key_values,
                tie_breaker,
                &mut state,
            )?;
            // Persist state if inner function didn't change seek_state to something else (e.g., MovingBetweenPages)
            if matches!(
                self.seek_state,
                CursorSeekState::InteriorPageBinarySearch { .. }
            ) {
                self.seek_state = CursorSeekState::InteriorPageBinarySearch { state };
            }
            match control {
                ControlFlow::Continue(_) => {}
                ControlFlow::Break(result) => {
                    return Ok(result);
                }
            }
        }
    }

    #[expect(clippy::too_many_arguments)]
    fn indexbtree_move_to_inner(
        &mut self,
        cmp: SeekOp,
        old_top_idx: usize,
        cell_count: usize,
        record_comparer: RecordCompare,
        key_values: &[ValueRef<'_>],
        tie_breaker: Ordering,
        state: &mut InteriorPageBinarySearchState,
    ) -> Result<ControlFlow<IOResult<()>>> {
        let iter_dir = cmp.iteration_direction();
        let min = state.min_cell_idx;
        let max = state.max_cell_idx;
        if min > max {
            let Some(leftmost_matching_cell) = state.nearest_matching_cell else {
                match self
                    .stack
                    .get_page_contents_at_level(old_top_idx)
                    .unwrap()
                    .rightmost_pointer()?
                {
                    Some(right_most_pointer) => {
                        // On `IO(spill_c)` keep seek_state at the binary
                        // search so re-entry retries this same step. None
                        // of the cursor mutations have happened yet.
                        match self.read_page(right_most_pointer as i64)? {
                            IOResult::Done((mem_page, c)) => {
                                self.stack.set_cell_index(cell_count as i32 + 1);
                                self.stack.push(mem_page);
                                self.seek_state = CursorSeekState::MovingBetweenPages {
                                    eq_seen: state.eq_seen,
                                };
                                if let Some(c) = c {
                                    return Ok(ControlFlow::Break(IOResult::IO(IOCompletions(c))));
                                }
                                return Ok(ControlFlow::Continue(()));
                            }
                            IOResult::IO(IOCompletions(spill_c)) => {
                                return Ok(ControlFlow::Break(IOResult::IO(IOCompletions(
                                    spill_c,
                                ))));
                            }
                        }
                    }
                    None => {
                        unreachable!("we shall not go back up! The only way is down the slope");
                    }
                }
            };
            let matching_cell = self
                .stack
                .get_page_contents_at_level(old_top_idx)
                .unwrap()
                .cell_get(leftmost_matching_cell, self.usable_space())?;
            // We don't advance in case of forward iteration and index tree
            // internal nodes because we will visit this node going up.
            // In backwards iteration, we must retreat because otherwise we
            // would unnecessarily visit this node again. Example:
            //   parent:     key 666 (target found in left child)
            //   left child: key 663, key 664, key 665
            // we need to move to the previous parent (e.g. key 662) when
            // iterating backwards so that we don't end up back here again.
            //
            // On `IO(spill_c)` we MUST NOT mutate `cell_idx` (set or
            // retreat) — see the Done branch.
            let BTreeCell::IndexInteriorCell(IndexInteriorCell {
                left_child_page, ..
            }) = &matching_cell
            else {
                unreachable!("unexpected cell type: {:?}", matching_cell);
            };

            {
                let page = self.stack.get_page_at_level(old_top_idx).unwrap();
                turso_assert!(
                    page.get().id != *left_child_page as usize,
                    "corrupt: current page and left child page are the same",
                    { "cell": leftmost_matching_cell, "page_id": page.get().id }
                );
            }

            match self.read_page(*left_child_page as i64)? {
                IOResult::Done((mem_page, c)) => {
                    self.stack.set_cell_index(leftmost_matching_cell as i32);
                    if iter_dir == IterationDirection::Backwards {
                        self.stack.retreat();
                    }
                    self.stack.push(mem_page);
                    self.seek_state = CursorSeekState::MovingBetweenPages {
                        eq_seen: state.eq_seen,
                    };
                    if let Some(c) = c {
                        return Ok(ControlFlow::Break(IOResult::IO(IOCompletions(c))));
                    }
                }
                IOResult::IO(IOCompletions(spill_c)) => {
                    return Ok(ControlFlow::Break(IOResult::IO(IOCompletions(spill_c))));
                }
            }
            return Ok(ControlFlow::Continue(()));
        }

        let cur_cell_idx = (min + max) >> 1; // rustc generates extra insns for (min+max)/2 due to them being isize. we know min&max are >=0 here.
        self.stack.set_cell_index(cur_cell_idx as i32);

        let (payload, payload_size, first_overflow_page) = self
            .stack
            .get_page_contents_at_level(old_top_idx)
            .unwrap()
            .cell_read_payload_ptr(cur_cell_idx as usize, self.usable_space())?;

        if let Some(next_page) = first_overflow_page {
            let res = self.process_overflow_read(payload, next_page, payload_size)?;
            if res.is_io() {
                return Ok(ControlFlow::Break(res));
            }
        } else {
            self.get_immutable_record_or_create()?
                .as_mut()
                .unwrap()
                .invalidate();
            crate::with_btree_allocation_site!(
                RecordPayload,
                self.get_immutable_record_or_create()?
                    .as_mut()
                    .unwrap()
                    .start_serialization(payload)
            )?;
        };

        let (target_leaf_page_is_in_left_subtree, is_eq) = {
            let record = self.get_immutable_record();
            let record = record.as_ref().unwrap();

            let interior_cell_vs_index_key = record_comparer.compare(
                record,
                key_values,
                self.index_info
                    .as_ref()
                    .expect("indexbtree_move_to: index_info required"),
                0,
                tie_breaker,
            )?;

            // in sqlite btrees left child pages have <= keys.
            // in general, in forwards iteration we want to find the first key that matches the seek condition.
            // in backwards iteration we want to find the last key that matches the seek condition.
            //
            // Logic table for determining if target leaf page is in left subtree.
            // For index b-trees this is a bit more complicated since the interior cells contain payloads (the key is the payload).
            // and for non-unique indexes there might be several cells with the same key.
            //
            // Forwards iteration (looking for first match in tree):
            // OP  | Current Cell vs Seek Key  | Action?  | Explanation
            // GT  | >                         | go left  | First > key could be exactly this one, or in left subtree
            // GT  | = or <                    | go right | First > key must be in right subtree
            // GE  | >                         | go left  | First >= key could be exactly this one, or in left subtree
            // GE  | =                         | go left  | First >= key could be exactly this one, or in left subtree
            // GE  | <                         | go right | First >= key must be in right subtree
            //
            // Backwards iteration (looking for last match in tree):
            // OP  | Current Cell vs Seek Key  | Action?  | Explanation
            // LE  | >                         | go left  | Last <= key must be in left subtree
            // LE  | =                         | go right | Last <= key is either this one, or somewhere to the right of this one. So we need to go right to make sure
            // LE  | <                         | go right | Last <= key must be in right subtree
            // LT  | >                         | go left  | Last < key must be in left subtree
            // LT  | =                         | go left  | Last < key must be in left subtree since we want strictly less than
            // LT  | <                         | go right | Last < key could be exactly this one, or in right subtree
            //
            // No iteration (point query):
            // EQ  | >                         | go left  | First = key must be in left subtree
            // EQ  | =                         | go left  | First = key could be exactly this one, or in left subtree
            // EQ  | <                         | go right | First = key must be in right subtree

            (
                match cmp {
                    SeekOp::GT => interior_cell_vs_index_key.is_gt(),
                    SeekOp::GE { .. } => interior_cell_vs_index_key.is_ge(),
                    SeekOp::LE { .. } => interior_cell_vs_index_key.is_gt(),
                    SeekOp::LT => interior_cell_vs_index_key.is_ge(),
                },
                interior_cell_vs_index_key.is_eq(),
            )
        };

        if is_eq {
            state.eq_seen = true;
        }

        if target_leaf_page_is_in_left_subtree {
            state.nearest_matching_cell = Some(cur_cell_idx as usize);
            state.max_cell_idx = cur_cell_idx - 1;
        } else {
            state.min_cell_idx = cur_cell_idx + 1;
        }
        Ok(ControlFlow::Continue(()))
    }

    /// Specialized version of do_seek() for table btrees that uses binary search instead
    /// of iterating cells in order.
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn tablebtree_seek(&mut self, rowid: i64, seek_op: SeekOp) -> IOResultOr<SeekResult> {
        if matches!(
            self.seek_state,
            CursorSeekState::Start
                | CursorSeekState::MovingBetweenPages { .. }
                | CursorSeekState::InteriorPageBinarySearch { .. }
        ) {
            // No need for another move_to_root. Move_to already moves to root
            return_if_io!(self.move_to(SeekKey::TableRowId(rowid), seek_op));
            let page = self.stack.top_ref();
            let contents = page.get_contents();
            turso_assert!(
                contents.is_leaf(),
                "tablebtree_seek() called on non-leaf page"
            );

            let cell_count = contents.cell_count();
            if cell_count == 0 {
                self.stack.set_cell_index(0);
                return Ok(IOResult::Done(SeekResult::NotFound));
            }
            let min_cell_idx = 0;
            let max_cell_idx = cell_count as isize - 1;

            // If iter dir is forwards, we want the first cell that matches;
            // If iter dir is backwards, we want the last cell that matches.
            let nearest_matching_cell = None;

            self.seek_state = CursorSeekState::LeafPageBinarySearch {
                state: LeafPageBinarySearchState {
                    min_cell_idx,
                    max_cell_idx,
                    nearest_matching_cell,
                    eq_seen: false, // not relevant for table btrees
                    target_cell_when_not_found: match seek_op.iteration_direction() {
                        IterationDirection::Forwards => cell_count as i32,
                        IterationDirection::Backwards => -1,
                    },
                },
            };
        }

        let CursorSeekState::LeafPageBinarySearch { state } = &self.seek_state else {
            unreachable!("we must be in a leaf binary search state");
        };

        let page = self.stack.top_ref().clone();
        let contents = page.get_contents();
        let mut state = *state;

        loop {
            let control = self.tablebtree_seek_inner(rowid, seek_op, contents, &mut state)?;
            // Persist state after each iteration since inner function modifies it
            if matches!(
                self.seek_state,
                CursorSeekState::LeafPageBinarySearch { .. }
            ) {
                self.seek_state = CursorSeekState::LeafPageBinarySearch { state };
            }
            match control {
                ControlFlow::Continue(_) => {}
                ControlFlow::Break(res) => {
                    return Ok(res);
                }
            }
        }
    }

    fn tablebtree_seek_inner(
        &mut self,
        rowid: i64,
        seek_op: SeekOp,
        contents: &mut PageContent,
        state: &mut LeafPageBinarySearchState,
    ) -> Result<ControlFlow<IOResult<SeekResult>>> {
        let iter_dir = seek_op.iteration_direction();
        let min = state.min_cell_idx;
        let max = state.max_cell_idx;
        let target_cell_when_not_found = state.target_cell_when_not_found;
        if min > max {
            if let Some(nearest_matching_cell) = state.nearest_matching_cell {
                self.stack.set_cell_index(nearest_matching_cell as i32);
                self.set_has_record(true);
                return Ok(ControlFlow::Break(IOResult::Done(SeekResult::Found)));
            } else {
                // if !eq_only - matching entry can exist in neighbour leaf page
                // this can happen if key in the interiour page was deleted - but divider kept untouched
                // in such case BTree can navigate to the leaf which no longer has matching key for seek_op
                // in this case, caller must advance cursor if necessary
                return Ok(ControlFlow::Break(IOResult::Done(if seek_op.eq_only() {
                    let has_record = target_cell_when_not_found >= 0
                        && target_cell_when_not_found < contents.cell_count() as i32;
                    self.has_record = has_record;
                    self.stack.set_cell_index(target_cell_when_not_found);
                    SeekResult::NotFound
                } else {
                    // set cursor to the position where which would hold the op-boundary if it were present
                    self.stack.set_cell_index(target_cell_when_not_found);
                    SeekResult::TryAdvance
                })));
            };
        }

        let cur_cell_idx = (min + max) >> 1; // rustc generates extra insns for (min+max)/2 due to them being isize. we know min&max are >=0 here.
        let cell_rowid = contents.cell_table_leaf_read_rowid(cur_cell_idx as usize)?;

        let cmp = cell_rowid.cmp(&rowid);

        let found = match seek_op {
            SeekOp::GT => cmp.is_gt(),
            SeekOp::GE { eq_only: true } => cmp.is_eq(),
            SeekOp::GE { eq_only: false } => cmp.is_ge(),
            SeekOp::LE { eq_only: true } => cmp.is_eq(),
            SeekOp::LE { eq_only: false } => cmp.is_le(),
            SeekOp::LT => cmp.is_lt(),
        };

        // rowids are unique, so we can return the rowid immediately
        if found && seek_op.eq_only() {
            self.stack.set_cell_index(cur_cell_idx as i32);
            self.set_has_record(true);
            return Ok(ControlFlow::Break(IOResult::Done(SeekResult::Found)));
        }

        if found {
            state.nearest_matching_cell = Some(cur_cell_idx as usize);
            match iter_dir {
                IterationDirection::Forwards => {
                    state.max_cell_idx = cur_cell_idx - 1;
                }
                IterationDirection::Backwards => {
                    state.min_cell_idx = cur_cell_idx + 1;
                }
            }
        } else if cmp.is_gt() {
            if matches!(seek_op, SeekOp::GE { eq_only: true }) {
                state.target_cell_when_not_found =
                    target_cell_when_not_found.min(cur_cell_idx as i32);
            }
            state.max_cell_idx = cur_cell_idx - 1;
        } else if cmp.is_lt() {
            if matches!(seek_op, SeekOp::LE { eq_only: true }) {
                state.target_cell_when_not_found =
                    target_cell_when_not_found.max(cur_cell_idx as i32);
            }
            state.min_cell_idx = cur_cell_idx + 1;
        } else {
            match iter_dir {
                IterationDirection::Forwards => {
                    state.min_cell_idx = cur_cell_idx + 1;
                }
                IterationDirection::Backwards => {
                    state.max_cell_idx = cur_cell_idx - 1;
                }
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn indexbtree_seek(
        &mut self,
        key: &ImmutableRecordRef<'_>,
        seek_op: SeekOp,
    ) -> IOResultOr<SeekResult> {
        let key_values = key.get_values()?;
        let record_comparer = {
            let index_info = self
                .index_info
                .as_ref()
                .expect("indexbtree_seek: index_info required");
            find_compare(key_values.iter().peekable(), index_info)
        };

        tracing::debug!(
            "Using record comparison strategy for seek: {:?}",
            record_comparer
        );

        self.indexbtree_seek_internal(seek_op, record_comparer, &key_values)
    }

    /// Seek using registers directly, avoiding record serialization overhead.
    /// See `seek_unpacked` trait method for rationale.
    #[instrument(skip_all, level = Level::DEBUG)]
    fn indexbtree_seek_unpacked(
        &mut self,
        registers: &[Register],
        seek_op: SeekOp,
    ) -> IOResultOr<SeekResult> {
        let index_info = self
            .index_info
            .as_ref()
            .expect("indexbtree_seek_unpacked: index_info required");

        // SmallVec stores up to MAX_STACK_KEY_VALUES on the stack, spilling to heap only if exceeded
        let key_values: SmallVec<[ValueRef<'_>; STACK_ALLOC_KEY_VALS_MAX]> = registers
            .iter()
            .map(|r| r.get_value().as_value_ref())
            .collect();
        let record_comparer = find_compare(key_values.iter().peekable(), index_info);
        tracing::debug!(
            "Using record comparison strategy for seek: {:?}",
            record_comparer
        );
        self.indexbtree_seek_internal(seek_op, record_comparer, &key_values)
    }

    fn indexbtree_seek_internal(
        &mut self,
        seek_op: SeekOp,
        record_comparer: RecordCompare,
        key_values: &[ValueRef<'_>],
    ) -> IOResultOr<SeekResult> {
        if matches!(
            self.seek_state,
            CursorSeekState::Start
                | CursorSeekState::MovingBetweenPages { .. }
                | CursorSeekState::InteriorPageBinarySearch { .. }
        ) {
            if matches!(self.seek_state, CursorSeekState::Start) {
                if let Some(c) = return_if_io!(self.move_to_root_nonblock()) {
                    return Ok(IOResult::IO(IOCompletions(c)));
                }
            }
            return_if_io!(self.indexbtree_move_to_internal(seek_op, record_comparer, key_values));
            let CursorSeekState::FoundLeaf { eq_seen } = &self.seek_state else {
                unreachable!(
                    "We must still be in FoundLeaf state after indexbtree_move_to_internal, got: {:?}",
                    self.seek_state
                );
            };
            let eq_seen = *eq_seen;
            let page = self.stack.top_ref();

            let contents = page.get_contents();
            let cell_count = contents.cell_count();
            if cell_count == 0 {
                return Ok(IOResult::Done(SeekResult::NotFound));
            }

            let min = 0;
            let max = cell_count as isize - 1;

            // If iter dir is forwards, we want the first cell that matches;
            // If iter dir is backwards, we want the last cell that matches.
            let nearest_matching_cell = None;

            self.seek_state = CursorSeekState::LeafPageBinarySearch {
                state: LeafPageBinarySearchState {
                    min_cell_idx: min,
                    max_cell_idx: max,
                    nearest_matching_cell,
                    eq_seen,
                    target_cell_when_not_found: match seek_op.iteration_direction() {
                        IterationDirection::Forwards => cell_count as i32,
                        IterationDirection::Backwards => -1,
                    },
                },
            };
        }

        let CursorSeekState::LeafPageBinarySearch { state } = &self.seek_state else {
            unreachable!(
                "we must be in a leaf binary search state, got: {:?}",
                self.seek_state
            );
        };

        let old_top_idx = self.stack.current();

        let mut state = *state;

        loop {
            let control = self.indexbtree_seek_inner(
                seek_op,
                old_top_idx,
                key_values,
                record_comparer,
                &mut state,
            )?;
            // Persist state after each iteration since inner function modifies it
            if matches!(
                self.seek_state,
                CursorSeekState::LeafPageBinarySearch { .. }
            ) {
                self.seek_state = CursorSeekState::LeafPageBinarySearch { state };
            }
            match control {
                ControlFlow::Continue(_) => {}
                ControlFlow::Break(res) => {
                    return Ok(res);
                }
            }
        }
    }

    fn indexbtree_seek_inner(
        &mut self,
        seek_op: SeekOp,
        old_top_idx: usize,
        key_values: &[ValueRef<'_>],
        record_comparer: RecordCompare,
        state: &mut LeafPageBinarySearchState,
    ) -> Result<ControlFlow<IOResult<SeekResult>>> {
        let iter_dir = seek_op.iteration_direction();
        let min = state.min_cell_idx;
        let max = state.max_cell_idx;
        let eq_seen = state.eq_seen;
        if min > max {
            if let Some(nearest_matching_cell) = state.nearest_matching_cell {
                self.stack.set_cell_index(nearest_matching_cell as i32);
                self.set_has_record(true);

                return Ok(ControlFlow::Break(IOResult::Done(SeekResult::Found)));
            } else {
                // set cursor to the position where which would hold the op-boundary if it were present
                let target_cell = state.target_cell_when_not_found;
                self.stack.set_cell_index(target_cell);
                let has_record = target_cell >= 0
                    && target_cell
                        < self
                            .stack
                            .get_page_contents_at_level(old_top_idx)
                            .unwrap()
                            .cell_count() as i32;
                self.has_record = has_record;

                // Similar logic as in tablebtree_seek(), but for indexes.
                // The difference is that since index keys are not necessarily unique, we need to TryAdvance
                // even when eq_only=true and we have seen an EQ match up in the tree in an interior node.
                if seek_op.eq_only() && !eq_seen {
                    return Ok(ControlFlow::Break(IOResult::Done(SeekResult::NotFound)));
                }
                return Ok(ControlFlow::Break(IOResult::Done(SeekResult::TryAdvance)));
            };
        }

        let cur_cell_idx = (min + max) >> 1; // rustc generates extra insns for (min+max)/2 due to them being isize. we know min&max are >=0 here.
        self.stack.set_cell_index(cur_cell_idx as i32);

        let (payload, payload_size, first_overflow_page) = self
            .stack
            .get_page_contents_at_level(old_top_idx)
            .unwrap()
            .cell_read_payload_ptr(cur_cell_idx as usize, self.usable_space())?;

        if let Some(next_page) = first_overflow_page {
            let res = self.process_overflow_read(payload, next_page, payload_size)?;
            if let IOResult::IO(io) = res {
                return Ok(ControlFlow::Break(IOResult::IO(io)));
            }
        } else {
            self.get_immutable_record_or_create()?
                .as_mut()
                .unwrap()
                .invalidate();
            crate::with_btree_allocation_site!(
                RecordPayload,
                self.get_immutable_record_or_create()?
                    .as_mut()
                    .unwrap()
                    .start_serialization(payload)
            )?;
        };

        let (cmp, found) = self.compare_with_current_record(
            key_values,
            seek_op,
            &record_comparer,
            self.index_info
                .as_ref()
                .expect("indexbtree_seek: index_info required"),
        )?;
        if found {
            state.nearest_matching_cell.replace(cur_cell_idx as usize);
            match iter_dir {
                IterationDirection::Forwards => {
                    state.max_cell_idx = cur_cell_idx - 1;
                }
                IterationDirection::Backwards => {
                    state.min_cell_idx = cur_cell_idx + 1;
                }
            }
        } else if cmp.is_gt() {
            if matches!(seek_op, SeekOp::GE { eq_only: true }) {
                state.target_cell_when_not_found =
                    state.target_cell_when_not_found.min(cur_cell_idx as i32);
            }
            state.max_cell_idx = cur_cell_idx - 1;
        } else if cmp.is_lt() {
            if matches!(seek_op, SeekOp::LE { eq_only: true }) {
                state.target_cell_when_not_found =
                    state.target_cell_when_not_found.max(cur_cell_idx as i32);
            }
            state.min_cell_idx = cur_cell_idx + 1;
        } else {
            match iter_dir {
                IterationDirection::Forwards => {
                    state.min_cell_idx = cur_cell_idx + 1;
                }
                IterationDirection::Backwards => {
                    state.max_cell_idx = cur_cell_idx - 1;
                }
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    fn compare_with_current_record(
        &self,
        key_values: &[ValueRef],
        seek_op: SeekOp,
        record_comparer: &RecordCompare,
        index_info: &IndexInfo,
    ) -> Result<(Ordering, bool)> {
        let record = self.get_immutable_record();
        let record = record.as_ref().unwrap();

        let tie_breaker = get_tie_breaker_from_seek_op(seek_op);
        let cmp = record_comparer.compare(record, key_values, index_info, 0, tie_breaker)?;

        let found = match seek_op {
            SeekOp::GT => cmp.is_gt(),
            SeekOp::GE { eq_only: true } => cmp.is_eq(),
            SeekOp::GE { eq_only: false } => cmp.is_ge(),
            SeekOp::LE { eq_only: true } => cmp.is_eq(),
            SeekOp::LE { eq_only: false } => cmp.is_le(),
            SeekOp::LT => cmp.is_lt(),
        };
        Ok((cmp, found))
    }

    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    pub fn move_to(&mut self, key: SeekKey<'_>, cmp: SeekOp) -> IOResultOr<()> {
        tracing::trace!(?key, ?cmp);
        // For a table with N rows, we can find any row by row id in O(log(N)) time by starting at the root page and following the B-tree pointers.
        // B-trees consist of interior pages and leaf pages. Interior pages contain pointers to other pages, while leaf pages contain the actual row data.
        //
        // Conceptually, each Interior Cell in a interior page has a rowid and a left child node, and the page itself has a right-most child node.
        // Example: consider an interior page that contains cells C1(rowid=10), C2(rowid=20), C3(rowid=30).
        // - All rows with rowids <= 10 are in the left child node of C1.
        // - All rows with rowids > 10 and <= 20 are in the left child node of C2.
        // - All rows with rowids > 20 and <= 30 are in the left child node of C3.
        // - All rows with rowids > 30 are in the right-most child node of the page.
        //
        // There will generally be multiple levels of interior pages before we reach a leaf page,
        // so we need to follow the interior page pointers until we reach the leaf page that contains the row we are looking for (if it exists).
        //
        // Here's a high-level overview of the algorithm:
        // 1. Since we start at the root page, its cells are all interior cells.
        // 2. We scan the interior cells until we find a cell whose rowid is greater than or equal to the rowid we are looking for.
        // 3. Follow the left child pointer of the cell we found in step 2.
        //    a. In case none of the cells in the page have a rowid greater than or equal to the rowid we are looking for,
        //       we follow the right-most child pointer of the page instead (since all rows with rowids greater than the rowid we are looking for are in the right-most child node).
        // 4. We are now at a new page. If it's another interior page, we repeat the process from step 2. If it's a leaf page, we continue to step 5.
        // 5. We scan the leaf cells in the leaf page until we find the cell whose rowid is equal to the rowid we are looking for.
        //    This cell contains the actual data we are looking for.
        // 6. If we find the cell, we return the record. Otherwise, we return an empty result.

        // If we are at the beginning/end of seek state, start a new move from the root.
        if matches!(
            self.seek_state,
            // these are stages that happen at the leaf page, so we can consider that the previous seek finished and we can start a new one.
            CursorSeekState::LeafPageBinarySearch { .. } | CursorSeekState::FoundLeaf { .. }
        ) {
            self.seek_state = CursorSeekState::Start;
        }
        loop {
            match self.move_to_state {
                MoveToState::Start => {
                    if matches!(self.seek_state, CursorSeekState::Start) {
                        let c = return_if_io!(self.move_to_root_nonblock());
                        self.move_to_state = MoveToState::MoveToPage;
                        if let Some(c) = c {
                            io_yield_one!(c);
                        }
                    } else {
                        self.move_to_state = MoveToState::MoveToPage;
                    }
                }
                MoveToState::MoveToPage => {
                    let ret = match &key {
                        SeekKey::TableRowId(rowid_key) => self.tablebtree_move_to(*rowid_key, cmp),
                        SeekKey::IndexKey(index_key) => self.indexbtree_move_to(index_key, cmp),
                    };
                    return_if_io!(ret);
                    self.move_to_state = MoveToState::Start;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    /// Insert a record into the btree.
    /// If the insert operation overflows the page, it will be split and the btree will be balanced.
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn insert_into_page(&mut self, bkey: &BTreeKey) -> IOResultOr<()> {
        let record = bkey
            .get_record()
            .expect("expected record present on insert");
        if let CursorState::None = &self.state {
            self.state = CursorState::Write(WriteState::Start);
        }
        let usable_space = self.usable_space();
        let ret = loop {
            let CursorState::Write(write_state) = &mut self.state else {
                panic!("expected write state");
            };
            match write_state {
                WriteState::Start => {
                    let page = self.stack.top();

                    // get page and find cell
                    let cell_idx = {
                        self.pager.add_dirty(&page)?;
                        self.stack.current_cell_index()
                    };
                    if cell_idx == -1 {
                        // This might be a brand new table and the cursor hasn't moved yet. Let's advance it to the first slot.
                        self.stack.set_cell_index(0);
                    }
                    let cell_idx = self.stack.current_cell_index() as usize;
                    tracing::debug!(cell_idx);

                    // if the cell index is less than the total cells, check: if its an existing
                    // rowid, we are going to update / overwrite the cell
                    if cell_idx < page.get_contents().cell_count() {
                        let cell = page.get_contents().cell_get(cell_idx, usable_space)?;
                        match cell {
                            BTreeCell::TableLeafCell(tbl_leaf) => {
                                if tbl_leaf.rowid == bkey.to_rowid() {
                                    tracing::debug!("TableLeafCell: found exact match with cell_idx={cell_idx}, overwriting");
                                    self.has_record = true;
                                    *write_state = WriteState::Overwrite {
                                        page,
                                        cell_idx,
                                        state: Some(OverwriteCellState::AllocatePayload),
                                    };
                                    continue;
                                }
                            }
                            BTreeCell::IndexLeafCell(..) | BTreeCell::IndexInteriorCell(..) => {
                                return_if_io!(self.record());
                                let cmp = compare_immutable_iter(
                                    record.iter()?,
                                    self.get_immutable_record()
                                        .as_ref()
                                        .unwrap()
                                        .iter()?,
                                        &self.index_info.as_ref().unwrap().key_info,
                                )?;
                                if cmp == Ordering::Equal {
                                    tracing::debug!("IndexLeafCell: found exact match with cell_idx={cell_idx}, overwriting");
                                    self.set_has_record(true);
                                    let CursorState::Write(write_state) = &mut self.state else {
                                        panic!("expected write state");
                                    };
                                    *write_state = WriteState::Overwrite {
                                        page,
                                        cell_idx,
                                        state: Some(OverwriteCellState::AllocatePayload),
                                    };
                                    continue;
                                } else {
                                    turso_assert!(
                                        !matches!(cell, BTreeCell::IndexInteriorCell(..)),
                                         "we should not be inserting a new index interior cell. the only valid operation on an index interior cell is an overwrite!"
                                    );
                                }
                            }
                            other => panic!("unexpected cell type, expected TableLeaf or IndexLeaf, found: {other:?}"),
                        }
                    }

                    let CursorState::Write(write_state) = &mut self.state else {
                        panic!("expected write state");
                    };
                    // Reuse the cell payload buffer to avoid allocations
                    let mut payload = take_vec(&mut self.reusable_cell_payload);
                    payload.clear();
                    // Reserve capacity if needed (typical cell is small)
                    // child pointer (4) + payload size varint (up to 9) + rowid varint (up to 9)
                    const MAX_CELL_HEADER: usize = 22;
                    let needed_capacity = record.get_payload().len() + MAX_CELL_HEADER;
                    if payload.capacity() < needed_capacity {
                        crate::with_btree_allocation_site!(
                            CellPayload,
                            payload.try_reserve(needed_capacity - payload.capacity())
                        )?;
                    }
                    *write_state = WriteState::Insert {
                        page,
                        cell_idx,
                        new_payload: payload,
                        fill_cell_payload_state: FillCellPayloadState::Start,
                    };
                    continue;
                }
                WriteState::Insert {
                    page,
                    cell_idx,
                    new_payload,
                    ref mut fill_cell_payload_state,
                } => {
                    return_if_io!(fill_cell_payload(
                        &PinGuard::new(page.clone()),
                        bkey.maybe_rowid(),
                        new_payload,
                        *cell_idx,
                        &record,
                        usable_space,
                        self.pager.clone(),
                        fill_cell_payload_state,
                    ));

                    {
                        let contents = page.get_contents();
                        tracing::debug!(name: "overflow", cell_count = contents.cell_count());

                        insert_into_cell(
                            contents,
                            new_payload.as_slice(),
                            *cell_idx,
                            usable_space,
                        )?;
                    };
                    self.stack.set_cell_index(*cell_idx as i32);
                    let overflows = !page.get_contents().overflow_cells.is_empty();

                    // Recover the reusable buffer before transitioning state
                    let recovered_payload = take_vec(new_payload);
                    self.reusable_cell_payload = recovered_payload;

                    if overflows {
                        *write_state = WriteState::Balancing;
                        turso_assert!(matches!(self.balance_state.sub_state, BalanceSubState::Start), "no balancing operation should be in progress during insert", { "state": self.state, "sub_state": self.balance_state.sub_state });
                        // If we balance, we must save the cursor position and seek to it later.
                        self.save_context(CursorContext::seek_eq_only(bkey));
                        inject_io_yield!(
                            self,
                            BTreeWriteYieldPoint::AfterInsertOverflowCellBeforeBalance
                        );
                    } else {
                        *write_state = WriteState::Finish;
                    }
                    continue;
                }
                WriteState::Overwrite {
                    page,
                    cell_idx,
                    ref mut state,
                } => {
                    turso_assert!(page.is_loaded(), "page is not loaded", { "page_id": page.get().id });
                    let page = page.clone();

                    // Currently it's necessary to .take() here to prevent double-borrow of `self` in `overwrite_cell`.
                    // We insert the state back if overwriting returns IO.
                    let mut state = state.take().expect("state should be present");
                    let cell_idx = *cell_idx;
                    if let IOResult::IO(io) =
                        self.overwrite_cell(&page, cell_idx, &record, &mut state)?
                    {
                        let CursorState::Write(write_state) = &mut self.state else {
                            panic!("expected write state");
                        };
                        *write_state = WriteState::Overwrite {
                            page,
                            cell_idx,
                            state: Some(state),
                        };
                        return Ok(IOResult::IO(io));
                    }
                    let overflows = !page.get_contents().overflow_cells.is_empty();
                    let underflows = !overflows && {
                        let free_space = compute_free_space(page.get_contents(), usable_space)?;
                        free_space * 3 > usable_space * 2
                    };
                    let CursorState::Write(write_state) = &mut self.state else {
                        panic!("expected write state");
                    };
                    if overflows || underflows {
                        *write_state = WriteState::Balancing;
                        turso_assert!(matches!(self.balance_state.sub_state, BalanceSubState::Start), "no balancing operation should be in progress during overwrite", { "state": self.state, "sub_state": self.balance_state.sub_state });
                        // If we balance, we must save the cursor position and seek to it later.
                        self.save_context(CursorContext::seek_eq_only(bkey));
                    } else {
                        *write_state = WriteState::Finish;
                    }
                    continue;
                }
                WriteState::Balancing => {
                    return_if_io!(self.balance(None));
                    let CursorState::Write(write_state) = &mut self.state else {
                        panic!("expected write state");
                    };
                    *write_state = WriteState::Finish;
                }
                WriteState::Finish => {
                    break Ok(IOResult::Done(()));
                }
            };
        };
        if matches!(self.state, CursorState::Write(WriteState::Finish)) {
            // if there was a balance triggered, the cursor position is invalid.
            // it's probably not the greatest idea in the world to do this eagerly here,
            // but at least it works.
            return_if_io!(self.restore_context());
        }
        self.state = CursorState::None;
        ret
    }

    /// Balance a leaf page.
    /// Balancing is done when a page overflows.
    /// see e.g. https://en.wikipedia.org/wiki/B-tree
    ///
    /// This is a naive algorithm that doesn't try to distribute cells evenly by content.
    /// It will try to split the page in half by keys not by content.
    /// Sqlite tries to have a page at least 40% full.
    ///
    /// `balance_ancestor_at_depth` specifies whether to balance an ancestor page at a specific depth.
    /// If `None`, balancing stops when a level is encountered that doesn't need balancing.
    /// If `Some(depth)`, the page on the stack at depth `depth` will be rebalanced after balancing the current page.
    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    fn balance(&mut self, balance_ancestor_at_depth: Option<usize>) -> IOResultOr<()> {
        loop {
            let usable_space = self.usable_space();
            let BalanceState {
                sub_state,
                balance_info,
                ..
            } = &mut self.balance_state;
            match sub_state {
                BalanceSubState::Start => {
                    turso_assert!(
                        balance_info.is_none(),
                        "BalanceInfo should be empty on start"
                    );
                    let current_page = self.stack.top_ref();
                    let next_balance_depth =
                        balance_ancestor_at_depth.unwrap_or_else(|| self.stack.current());
                    {
                        // check if we don't need to balance
                        // don't continue if:
                        // - current page is not overfull root
                        // OR
                        // - current page is not overfull and the amount of free space on the page
                        // is less than 2/3rds of the total usable space on the page
                        //
                        // https://github.com/sqlite/sqlite/blob/0aa95099f5003dc99f599ab77ac0004950b281ef/src/btree.c#L9064-L9071
                        let page = current_page.get_contents();
                        let free_space = compute_free_space(page, usable_space)?;
                        let this_level_is_already_balanced = page.overflow_cells.is_empty()
                            && (!self.stack.has_parent() || free_space * 3 <= usable_space * 2);
                        if this_level_is_already_balanced {
                            if self.stack.current() > next_balance_depth {
                                while self.stack.current() > next_balance_depth {
                                    // Even though this level is already balanced, we know there's an upper level that needs balancing.
                                    // So we pop the stack and continue.
                                    self.stack.pop();
                                }
                                continue;
                            }
                            // Otherwise, we're done.
                            *sub_state = BalanceSubState::Start;
                            return Ok(IOResult::Done(()));
                        }
                    }
                    if !self.stack.has_parent() {
                        *sub_state = BalanceSubState::BalanceRoot;
                    } else {
                        *sub_state = BalanceSubState::Decide;
                    }
                }
                BalanceSubState::BalanceRoot => {
                    return_if_io!(self.balance_root());

                    let BalanceState { sub_state, .. } = &mut self.balance_state;
                    *sub_state = BalanceSubState::Decide;
                }
                BalanceSubState::Decide => {
                    let cur_page = self.stack.top_ref();
                    let cur_page_contents = cur_page.get_contents();

                    // Check if we can use the balance_quick() fast path.
                    let mut do_quick = false;
                    if cur_page_contents.page_type()? == PageType::TableLeaf
                        && cur_page_contents.overflow_cells.len() == 1
                    {
                        let overflow_cell_is_last =
                            cur_page_contents.overflow_cells.first().unwrap().index
                                == cur_page_contents.cell_count();
                        if overflow_cell_is_last {
                            let parent = self
                                .stack
                                .get_page_at_level(self.stack.current() - 1)
                                .expect("parent page should be on the stack");
                            let parent_contents = parent.get_contents();
                            let parent_rightmost =
                                parent_contents.rightmost_pointer()?.ok_or_else(|| {
                                    mark_unlikely();
                                    LimboError::Corrupt(format!(
                                        "parent page {} is a leaf page, expected interior page",
                                        parent.get().id
                                    ))
                                })?;
                            if parent.get().id != 1 && parent_rightmost == cur_page.get().id as u32
                            {
                                // If all of the following are true, we can use the balance_quick() fast path:
                                // - The page is a table leaf page
                                // - The overflow cell would be the last cell on the leaf page
                                // - The parent page is not page 1
                                // - The leaf page is the rightmost page in the subtree
                                do_quick = true;
                            }
                        }
                    }

                    let BalanceState { sub_state, .. } = &mut self.balance_state;
                    if do_quick {
                        *sub_state = BalanceSubState::Quick;
                    } else {
                        *sub_state = BalanceSubState::NonRootPickSiblings;
                        self.stack.pop();
                    }
                }
                BalanceSubState::Quick => {
                    return_if_io!(self.balance_quick());
                }
                BalanceSubState::NonRootPickSiblings
                | BalanceSubState::NonRootDoBalancing
                | BalanceSubState::NonRootDoBalancingAllocate { .. }
                | BalanceSubState::NonRootDoBalancingFinish { .. }
                | BalanceSubState::FreePages { .. } => {
                    return_if_io!(self.balance_non_root());
                }
            }
        }
    }

    /// Fast balancing routine for the common special case where the rightmost leaf page of a given subtree overflows (= an append).
    /// In this case we just add a new leaf page as the right sibling of that page, and insert a new divider cell into the parent.
    /// The high level steps are:
    /// 1. Allocate a new leaf page and insert the overflow cell payload in it.
    /// 2. Create a new divider cell in the parent - it contains the page number of the old rightmost leaf, plus the largest rowid on that page.
    /// 3. Update the rightmost pointer of the parent to point to the new leaf page.
    /// 4. Continue balance from the parent page (inserting the new divider cell may have overflowed the parent)
    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    fn balance_quick(&mut self) -> IOResultOr<()> {
        // Since we are going to change the btree structure, let's forget our cached knowledge of the rightmost page.
        let _ = self.move_to_right_state.1.take();

        // Allocate a new leaf page and insert the overflow cell payload in it.
        let new_rightmost_leaf = return_if_io!(self.pager.do_allocate_page(
            PageType::TableLeaf,
            0,
            BtreePageAllocMode::Any
        ));
        self.pager.add_dirty(&new_rightmost_leaf)?;

        let usable_space = self.usable_space();
        let old_rightmost_leaf = self.stack.top_ref();
        let old_rightmost_leaf_contents = old_rightmost_leaf.get_contents();
        turso_assert!(
            old_rightmost_leaf_contents.overflow_cells.len() == 1,
            "expected 1 overflow cell",
            { "overflow_cell_count": old_rightmost_leaf_contents.overflow_cells.len() }
        );

        let parent = self
            .stack
            .get_page_at_level(self.stack.current() - 1)
            .expect("parent page should be on the stack");
        self.pager.add_dirty(parent)?;
        let parent_contents = parent.get_contents();
        let rightmost_pointer = parent_contents
            .rightmost_pointer()?
            .expect("parent should have a rightmost pointer");
        turso_assert!(
            rightmost_pointer == old_rightmost_leaf.get().id as u32,
            "leaf should be the rightmost page in the subtree"
        );

        let overflow_cell = old_rightmost_leaf_contents
            .overflow_cells
            .pop()
            .expect("overflow cell should be present");
        turso_assert!(
            overflow_cell.index == old_rightmost_leaf_contents.cell_count(),
            "overflow cell must be the last cell in the leaf"
        );

        let new_rightmost_leaf_contents = new_rightmost_leaf.get_contents();
        insert_into_cell(
            new_rightmost_leaf_contents,
            &overflow_cell.payload.as_ref(),
            0,
            usable_space,
        )?;

        // Create a new divider cell in the parent - it contains the page number of the old rightmost leaf, plus the largest rowid on that page.
        let mut new_divider: [u8; 13] = [0; 13]; // 4 bytes for page number, max 9 bytes for rowid (varint)
        new_divider[0..4].copy_from_slice(&(old_rightmost_leaf.get().id as u32).to_be_bytes());
        let largest_rowid = old_rightmost_leaf_contents
            .cell_table_leaf_read_rowid(old_rightmost_leaf_contents.cell_count() - 1)?;
        let n = write_varint(&mut new_divider[4..], largest_rowid as u64);
        let divider_length = 4 + n;

        // Insert the new divider cell into the parent.
        insert_into_cell(
            parent_contents,
            &new_divider[..divider_length],
            parent_contents.cell_count(),
            usable_space,
        )?;
        parent_contents.write_rightmost_ptr(new_rightmost_leaf.get().id as u32);
        // Continue balance from the parent page (inserting the new divider cell may have overflowed the parent)
        self.stack.pop();

        let BalanceState { sub_state, .. } = &mut self.balance_state;
        *sub_state = BalanceSubState::Start;
        Ok(IOResult::Done(()))
    }

    /// Balance a non root page by trying to balance cells between a maximum of 3 siblings that should be neighboring the page that overflowed/underflowed.
    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    fn balance_non_root(&mut self) -> IOResultOr<()> {
        loop {
            let usable_space = self.usable_space();
            let BalanceState {
                sub_state,
                balance_info,
                reusable_divider_buffers,
                reusable_cell_payloads,
                sibling_load_group,
            } = &mut self.balance_state;
            tracing::debug!(?sub_state);

            match sub_state {
                BalanceSubState::Start
                | BalanceSubState::BalanceRoot
                | BalanceSubState::Decide
                | BalanceSubState::Quick => {
                    panic!("balance_non_root: unexpected state {sub_state:?}")
                }
                BalanceSubState::NonRootPickSiblings => {
                    // Since we are going to change the btree structure, let's forget our cached knowledge of the rightmost page.
                    let _ = self.move_to_right_state.1.take();

                    let (parent_page_idx, page_type, cell_count, over_cell_count) = {
                        let parent_page = self.stack.top_ref();
                        let parent_contents = parent_page.get_contents();
                        (
                            self.stack.current(),
                            parent_contents.page_type()?,
                            parent_contents.cell_count(),
                            parent_contents.overflow_cells.len(),
                        )
                    };

                    turso_assert!(
                        matches!(page_type, PageType::IndexInterior | PageType::TableInterior),
                        "expected index or table interior page"
                    );
                    let number_of_cells_in_parent = cell_count + over_cell_count;

                    // If `seek` moved to rightmost page, cell index will be out of bounds. Meaning cell_count+1.
                    // In any other case, `seek` will stay in the correct index.
                    let past_rightmost_pointer =
                        self.stack.current_cell_index() as usize == number_of_cells_in_parent + 1;
                    if past_rightmost_pointer {
                        self.stack.retreat();
                    }

                    let parent_page = self.stack.get_page_at_level(parent_page_idx).unwrap();
                    let parent_contents = parent_page.get_contents();
                    if !past_rightmost_pointer && over_cell_count > 0 {
                        // The ONLY way we can have an overflow cell in the parent is if we replaced an interior cell from a cell in the child, and that replacement did not fit.
                        // This can only happen on index btrees.
                        if matches!(page_type, PageType::IndexInterior) {
                            turso_assert!(parent_contents.overflow_cells.len() == 1, "index interior page must have no more than 1 overflow cell, as a result of InteriorNodeReplacement");
                        } else {
                            turso_assert!(false, "page type must have no overflow cells", {
                                "page_type": page_type
                            });
                        }
                        let overflow_cell = parent_contents.overflow_cells.first().unwrap();
                        let parent_page_cell_idx = self.stack.current_cell_index() as usize;
                        // Parent page must be positioned at the divider cell that overflowed due to the replacement.
                        turso_assert!(
                            overflow_cell.index == parent_page_cell_idx,
                            "overflow cell index must be the result of InteriorNodeReplacement that leaves both child and parent unbalanced, and hence parent page's position must equal overflow_cell.index",
                            { "parent_page_id": parent_page.get().id, "parent_page_cell_idx": parent_page_cell_idx, "overflow_cell_index": overflow_cell.index }
                        );
                    }
                    self.pager.add_dirty(parent_page)?;
                    let parent_contents = parent_page.get_contents();
                    let page_to_balance_idx = self.stack.current_cell_index() as usize;

                    tracing::debug!(
                        "balance_non_root(parent_id={} page_to_balance_idx={})",
                        parent_page.get().id,
                        page_to_balance_idx
                    );
                    // Part 1: Find the sibling pages to balance
                    let mut pages_to_balance: [Option<PinGuard>; MAX_SIBLING_PAGES_TO_BALANCE] =
                        [const { None }; MAX_SIBLING_PAGES_TO_BALANCE];
                    turso_assert!(
                        page_to_balance_idx <= parent_contents.cell_count(),
                        "page_to_balance_idx={page_to_balance_idx} is out of bounds for parent cell count {number_of_cells_in_parent}"
                    );
                    // As there will be at maximum 3 pages used to balance:
                    // sibling_pointer is the index represeneting one of those 3 pages, and we initialize it to the last possible page.
                    // next_divider is the first divider that contains the first page of the 3 pages.
                    let (sibling_pointer, first_cell_divider) = match number_of_cells_in_parent {
                        n if n < 2 => (number_of_cells_in_parent, 0),
                        2 => (2, 0),
                        // Here we will have at lest 2 cells and one right pointer, therefore we can get 3 siblings.
                        // In case of 2 we will have all pages to balance.
                        _ => {
                            // In case of > 3 we have to check which ones to get
                            let next_divider = if page_to_balance_idx == 0 {
                                // first cell, take first 3
                                0
                            } else if page_to_balance_idx == number_of_cells_in_parent {
                                // Page corresponds to right pointer, so take last 3
                                number_of_cells_in_parent - 2
                            } else {
                                // Some cell in the middle, so we want to take sibling on left and right.
                                page_to_balance_idx - 1
                            };
                            (2, next_divider)
                        }
                    };
                    let sibling_count = sibling_pointer + 1;

                    let last_sibling_is_right_pointer = sibling_pointer + first_cell_divider
                        - parent_contents.overflow_cells.len()
                        == parent_contents.cell_count();
                    // Get the right page pointer that we will need to update later
                    let right_pointer = if last_sibling_is_right_pointer {
                        parent_contents.rightmost_pointer_raw()?.unwrap()
                    } else {
                        let max_overflow_cells = if matches!(page_type, PageType::IndexInterior) {
                            1
                        } else {
                            0
                        };
                        turso_assert!(
                            parent_contents.overflow_cells.len() <= max_overflow_cells,
                            "must have at most {max_overflow_cells} overflow cell in the parent"
                        );
                        // OVERFLOW CELL ADJUSTMENT:
                        // Let there be parent with cells [0,1,2,3,4].
                        // Let's imagine the cell at idx 2 gets replaced with a new payload that causes it to overflow.
                        // See handling of InteriorNodeReplacement in btree.rs.
                        //
                        // In this case the rightmost divider is going to be 3 (2 is the middle one and we pick neighbors 1-3).
                        // drop_cell(): [0,1,2,3,4] -> [0,1,3,4]   <-- cells on right side get shifted left!
                        // insert_into_cell(): [0,1,3,4] -> [0,1,3,4] + overflow cell (2)  <-- crucially, no physical shifting happens, overflow cell is stored separately
                        //
                        // This means '3' is actually physically located at index '2'.
                        // So IF the parent has an overflow cell, we need to subtract 1 to get the actual rightmost divider cell idx to physically read from.
                        // The formula for the actual cell idx is:
                        // first_cell_divider + sibling_pointer - parent_contents.overflow_cells.len()
                        // so in the above case:
                        // actual_cell_idx = 1 + 2 - 1 = 2
                        //
                        // In the case where the last divider cell is the overflow cell, there would be no left-shifting of cells in drop_cell(),
                        // because they are still positioned correctly (imagine .pop() from a vector).
                        // However, note that we are always looking for the _rightmost_ child page pointer between the (max 2) dividers, and for any case where the last divider cell is the overflow cell,
                        // the 'last_sibling_is_right_pointer' condition will also be true (since the overflow cell's left child will be the middle page), so we won't enter this code branch.
                        //
                        // Hence: when we enter this branch with overflow_cells.len() == 1, we know that left-shifting has happened and we need to subtract 1.
                        let actual_cell_idx = first_cell_divider + sibling_pointer
                            - parent_contents.overflow_cells.len();
                        let start_of_cell =
                            parent_contents.cell_get_raw_start_offset(actual_cell_idx);
                        let buf = parent_contents.as_ptr().as_mut_ptr();
                        unsafe { buf.add(start_of_cell) }
                    };

                    // load sibling pages
                    // start loading right page first
                    let mut pgno: u32 =
                        unsafe { right_pointer.cast::<u32>().read_unaligned().swap_bytes() };
                    let current_sibling = sibling_pointer;
                    let group =
                        sibling_load_group.get_or_insert_with(|| CompletionGroup::new(|_| {}));
                    for i in (0..=current_sibling).rev() {
                        match self.pager.read_page_into(pgno as i64, Some(group)) {
                            Err(e) => {
                                mark_unlikely();
                                tracing::error!("error reading page {}: {}", pgno, e);
                                // Drain any in-flight reads we accumulated
                                // across previous iterations / yields so the
                                // IO scheduler can finalize them before we
                                // bail out.
                                self.pager.io.drain_completions(group.completions())?;
                                *sibling_load_group = None;
                                return Err(e);
                            }
                            Ok(IOResult::Done((page, _))) => {
                                pages_to_balance[i].replace(PinGuard::new(page));
                            }
                            Ok(IOResult::IO(IOCompletions(spill_c))) => {
                                // Spill yield. The loop is fully re-entrant:
                                // on re-entry we re-execute from the top of
                                // `NonRootPickSiblings`, the pager's
                                // `pending_reads` returns the same PageRef
                                // for the in-flight sibling (its read is
                                // already in the group), and cache hits for
                                // previously-loaded siblings need no read.
                                io_yield_one!(spill_c);
                            }
                        }
                        if i == 0 {
                            break;
                        }
                        let next_cell_divider = i + first_cell_divider - 1;
                        let divider_is_overflow_cell = parent_contents
                            .overflow_cells
                            .first()
                            .is_some_and(|overflow_cell| overflow_cell.index == next_cell_divider);
                        if divider_is_overflow_cell {
                            turso_assert!(
                                matches!(
                                    parent_contents.page_type().ok(),
                                    Some(PageType::IndexInterior)
                                ),
                                "expected index interior page",
                                { "page_type": parent_contents.page_type().ok() }
                            );
                            turso_assert!(
                                parent_contents.overflow_cells.len() == 1,
                                "must have a single overflow cell in the parent, as a result of InteriorNodeReplacement"
                            );
                            let overflow_cell = parent_contents.overflow_cells.first().unwrap();
                            pgno =
                                u32::from_be_bytes(overflow_cell.payload[0..4].try_into().unwrap());
                        } else {
                            // grep for 'OVERFLOW CELL ADJUSTMENT' for explanation.
                            // here we only subtract 1 if the divider cell has been shifted left, i.e. the overflow cell was placed to the left
                            // this cell.
                            let actual_cell_idx = if let Some(overflow_cell) =
                                parent_contents.overflow_cells.first()
                            {
                                if next_cell_divider < overflow_cell.index {
                                    next_cell_divider
                                } else {
                                    next_cell_divider - 1
                                }
                            } else {
                                next_cell_divider
                            };
                            pgno = match parent_contents.cell_get(actual_cell_idx, usable_space)? {
                                BTreeCell::TableInteriorCell(TableInteriorCell {
                                    left_child_page,
                                    ..
                                })
                                | BTreeCell::IndexInteriorCell(IndexInteriorCell {
                                    left_child_page,
                                    ..
                                }) => left_child_page,
                                other => {
                                    mark_unlikely();
                                    crate::bail_corrupt_error!(
                                        "expected interior cell, got {:?}",
                                        other
                                    )
                                }
                            };
                        }
                    }

                    balance_info.replace(BalanceInfo {
                        pages_to_balance,
                        rightmost_pointer: right_pointer,
                        sibling_count,
                        first_divider_cell: first_cell_divider,
                        reusable_divider_cell: crate::alloc::vec![],
                    });
                    *sub_state = BalanceSubState::NonRootDoBalancing;
                    // Wait for the sibling reads issued across (possibly
                    // multiple) entries into this state. Take the group so
                    // the next balance starts fresh.
                    let completion = sibling_load_group
                        .take()
                        .expect("the sibling loop above created the group")
                        .build();
                    if !completion.finished() {
                        io_yield_one!(completion);
                    }
                }
                BalanceSubState::NonRootDoBalancing => {
                    // Ensure all involved pages are in memory.
                    let balance_info = balance_info.as_mut().unwrap();
                    for page in balance_info
                        .pages_to_balance
                        .iter()
                        .take(balance_info.sibling_count)
                    {
                        let page = page.as_ref().unwrap();
                        self.pager.add_dirty(page)?;

                        #[cfg(debug_assertions)]
                        let page_type_of_siblings = balance_info.pages_to_balance[0]
                            .as_ref()
                            .unwrap()
                            .get_contents()
                            .page_type()
                            .ok();

                        #[cfg(debug_assertions)]
                        {
                            let contents = page.get_contents();
                            debug_validate_cells!(&contents, usable_space);
                            turso_assert_eq!(contents.page_type().ok(), page_type_of_siblings);
                        }
                    }
                    // Start balancing.
                    let parent_page = PinGuard::new(self.stack.top_ref().clone());
                    let parent_contents = parent_page.get_contents();

                    // Pre-compute parent page parameters for faster cell region lookups.
                    // Note: cell_count cannot be pre-computed as it changes during the loop via drop_cell.
                    let parent_page_type = parent_contents.page_type()?;
                    let parent_max_local =
                        payload_overflow_threshold_max(parent_page_type, usable_space);
                    let parent_min_local =
                        payload_overflow_threshold_min(parent_page_type, usable_space);

                    // 1. Collect cell data from divider cells, and count the total number of cells to be distributed.
                    // The count includes: all cells and overflow cells from the sibling pages, and divider cells from the parent page,
                    // excluding the rightmost divider, which will not be dropped from the parent; instead it will be updated at the end.
                    let mut total_cells_to_redistribute = 0;
                    let pages_to_balance_new: [Option<PinGuard>;
                        MAX_NEW_SIBLING_PAGES_AFTER_BALANCE] =
                        [const { None }; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE];
                    for i in (0..balance_info.sibling_count).rev() {
                        let sibling_page = balance_info.pages_to_balance[i].as_ref().unwrap();
                        turso_assert!(sibling_page.is_loaded(), "sibling page is not loaded");
                        let sibling_contents = sibling_page.get_contents();
                        total_cells_to_redistribute += sibling_contents.cell_count();
                        total_cells_to_redistribute += sibling_contents.overflow_cells.len();

                        // Right pointer is not dropped, we simply update it at the end. This could be a divider cell that points
                        // to the last page in the list of pages to balance or this could be the rightmost pointer that points to a page.
                        let is_last_sibling = i == balance_info.sibling_count - 1;
                        if is_last_sibling {
                            continue;
                        }
                        // Since we know we have a left sibling, take the divider that points to left sibling of this page
                        let cell_idx = balance_info.first_divider_cell + i;
                        let divider_is_overflow_cell = parent_contents
                            .overflow_cells
                            .first()
                            .is_some_and(|overflow_cell| overflow_cell.index == cell_idx);
                        let cell_buf = if divider_is_overflow_cell {
                            turso_assert!(
                                matches!(
                                    parent_contents.page_type().ok(),
                                    Some(PageType::IndexInterior)
                                ),
                                "expected index interior page",
                                { "page_type": parent_contents.page_type().ok() }
                            );
                            turso_assert!(
                                parent_contents.overflow_cells.len() == 1,
                                "must have a single overflow cell in the parent, as a result of InteriorNodeReplacement"
                            );
                            let overflow_cell = parent_contents.overflow_cells.first().unwrap();
                            &overflow_cell.payload
                        } else {
                            // grep for 'OVERFLOW CELL ADJUSTMENT' for explanation.
                            // here we can subtract overflow_cells.len() every time, because we are iterating right-to-left,
                            // so if we are to the left of the overflow cell, it has already been cleared from the parent and overflow_cells.len() is 0.
                            let actual_cell_idx = cell_idx - parent_contents.overflow_cells.len();
                            // Use pre-computed page parameters for faster lookup.
                            // Note: cell_count must be fresh as it changes during the loop.
                            let (cell_start, cell_len) = parent_contents
                                ._cell_get_raw_region_faster(
                                    actual_cell_idx,
                                    usable_space,
                                    parent_contents.cell_count(),
                                    parent_max_local,
                                    parent_min_local,
                                    parent_page_type,
                                )?;
                            let buf = parent_contents.as_ptr();
                            &buf[cell_start..cell_start + cell_len]
                        };

                        // Count the divider cell itself (which will be dropped from the parent)
                        total_cells_to_redistribute += 1;

                        tracing::debug!(
                            "balance_non_root(drop_divider_cell, first_divider_cell={}, divider_cell={}, left_pointer={})",
                            balance_info.first_divider_cell,
                            i,
                            read_u32(cell_buf, 0)
                        );

                        // Reuse the divider buffer to avoid allocation per balance operation.
                        // The buffer is cleared and filled with the new cell data.
                        reusable_divider_buffers[i].clear();
                        reusable_divider_buffers[i].extend_from_slice(cell_buf);
                        if divider_is_overflow_cell {
                            tracing::debug!(
                                "clearing overflow cells from parent cell_idx={}",
                                cell_idx
                            );
                            parent_contents.overflow_cells.clear();
                        } else {
                            // grep for 'OVERFLOW CELL ADJUSTMENT' for explanation.
                            // here we can subtract overflow_cells.len() every time, because we are iterating right-to-left,
                            // so if we are to the left of the overflow cell, it has already been cleared from the parent and overflow_cells.len() is 0.
                            let actual_cell_idx = cell_idx - parent_contents.overflow_cells.len();
                            tracing::trace!(
                                "dropping divider cell from parent cell_idx={} count={}",
                                actual_cell_idx,
                                parent_contents.cell_count()
                            );
                            drop_cell(parent_contents, actual_cell_idx, usable_space)?;
                        }
                    }

                    /* 2. Initialize CellArray with all the cells used for distribution, this includes divider cells if !leaf. */
                    // Reuse the cell_payloads Vec from previous balance operations to avoid allocation.
                    let mut cell_payloads_vec = take_vec(reusable_cell_payloads);
                    cell_payloads_vec.clear();
                    // Ensure we have at least total_cells_to_redistribute capacity.
                    // Since len=0 after clear, reserve(n) ensures capacity >= n.
                    cell_payloads_vec.reserve(total_cells_to_redistribute);
                    let mut cell_array = CellArray {
                        cell_payloads: cell_payloads_vec,
                        cell_count_per_page_cumulative: [0; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE],
                    };
                    let cells_capacity_start = cell_array.cell_payloads.capacity();

                    let mut total_cells_inserted = 0;
                    // This is otherwise identical to CellArray.cell_count_per_page_cumulative,
                    // but we exclusively track what the prefix sums were _before_ we started redistributing cells.
                    let mut old_cell_count_per_page_cumulative: [u16;
                        MAX_NEW_SIBLING_PAGES_AFTER_BALANCE] =
                        [0; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE];

                    let page_type = balance_info.pages_to_balance[0]
                        .as_ref()
                        .unwrap()
                        .get_contents()
                        .page_type()?;
                    tracing::debug!("balance_non_root(page_type={:?})", page_type);
                    let is_table_leaf = matches!(page_type, PageType::TableLeaf);
                    let is_leaf = matches!(page_type, PageType::TableLeaf | PageType::IndexLeaf);
                    for (i, old_page) in balance_info
                        .pages_to_balance
                        .iter()
                        .take(balance_info.sibling_count)
                        .enumerate()
                    {
                        let old_page = old_page.as_ref().unwrap();
                        let old_page_contents = old_page.get_contents();
                        let page_type = old_page_contents.page_type()?;
                        let max_local = payload_overflow_threshold_max(page_type, usable_space);
                        let min_local = payload_overflow_threshold_min(page_type, usable_space);
                        let cell_count = old_page_contents.cell_count();
                        debug_validate_cells!(&old_page_contents, usable_space);
                        for cell_idx in 0..cell_count {
                            let (cell_start, cell_len) = old_page_contents
                                ._cell_get_raw_region_faster(
                                    cell_idx,
                                    usable_space,
                                    cell_count,
                                    max_local,
                                    min_local,
                                    page_type,
                                )?;
                            let buf = old_page_contents.as_ptr();
                            let cell_buf = &mut buf[cell_start..cell_start + cell_len];
                            // TODO(pere): make this reference and not copy
                            cell_array.cell_payloads.push(to_static_buf(cell_buf));
                        }
                        // Insert overflow cells into correct place
                        let offset = total_cells_inserted;
                        for overflow_cell in old_page_contents.overflow_cells.iter_mut() {
                            cell_array.cell_payloads.insert(
                                offset + overflow_cell.index,
                                to_static_buf(&mut Pin::as_mut(&mut overflow_cell.payload)),
                            );
                        }

                        old_cell_count_per_page_cumulative[i] =
                            cell_array.cell_payloads.len() as u16;

                        let mut cells_inserted =
                            old_page_contents.cell_count() + old_page_contents.overflow_cells.len();

                        let is_last_sibling = i == balance_info.sibling_count - 1;
                        if !is_last_sibling && !is_table_leaf {
                            // If we are a index page or a interior table page we need to take the divider cell too.
                            // But we don't need the last divider as it will remain the same.
                            if is_leaf {
                                // The divider holds the leaf cell's real size after its child pointer;
                                // back on a leaf the cell takes the minimum size again.
                                ensure_min_cell_size(
                                    &mut reusable_divider_buffers[i],
                                    LEFT_CHILD_PTR_SIZE_BYTES,
                                );
                            }
                            let mut divider_cell = reusable_divider_buffers[i].as_mut_slice();
                            // TODO(pere): in case of old pages are leaf pages, so index leaf page, we need to strip page pointers
                            // from divider cells in index interior pages (parent) because those should not be included.
                            cells_inserted += 1;
                            if !is_leaf {
                                // This divider cell needs to be updated with new left pointer,
                                let right_pointer = old_page_contents.rightmost_pointer()?.unwrap();
                                divider_cell[..LEFT_CHILD_PTR_SIZE_BYTES]
                                    .copy_from_slice(&right_pointer.to_be_bytes());
                            } else {
                                // index leaf
                                turso_assert!(
                                    divider_cell.len() >= LEFT_CHILD_PTR_SIZE_BYTES,
                                    "divider cell is too short"
                                );
                                // let's strip the page pointer
                                divider_cell = &mut divider_cell[LEFT_CHILD_PTR_SIZE_BYTES..];
                            }
                            cell_array.cell_payloads.push(to_static_buf(divider_cell));
                        }
                        total_cells_inserted += cells_inserted;
                    }
                    turso_assert!(
                        cell_array.cell_payloads.capacity() == cells_capacity_start,
                        "calculation of max cells was wrong"
                    );

                    // Verify that all cells were collected correctly.
                    // Note: For table leaf pages, dividers are counted in total_cells_to_redistribute
                    // but are NOT included in cell_array (they stay in parent as bookkeeping).
                    // For index/interior pages, dividers ARE included in cell_array.
                    let dividers_in_parent_only = if is_table_leaf {
                        // Table leaf: dividers are NOT added to cell_array
                        balance_info.sibling_count.saturating_sub(1)
                    } else {
                        // Index/interior: dividers ARE added to cell_array
                        0
                    };
                    let expected_cells_in_array =
                        total_cells_to_redistribute - dividers_in_parent_only;
                    turso_assert!(
                        cell_array.cell_payloads.len() == expected_cells_in_array,
                        "cell count mismatch after collection",
                        { "collected": cell_array.cell_payloads.len(), "expected": expected_cells_in_array, "total_cells_to_redistribute": total_cells_to_redistribute, "dividers_in_parent_only": dividers_in_parent_only, "is_table_leaf": is_table_leaf }
                    );
                    turso_assert!(
                        total_cells_inserted == expected_cells_in_array,
                        "cell count mismatch between total cells inserted and expected",
                        { "total_cells_inserted": total_cells_inserted, "expected_cells_in_array": expected_cells_in_array, "total_cells_to_redistribute": total_cells_to_redistribute, "dividers_in_parent_only": dividers_in_parent_only }
                    );

                    // Let's copy all cells for later checks
                    #[cfg(debug_assertions)]
                    let mut cells_debug: crate::alloc::Vec<
                        crate::alloc::Vec<u8>,
                    > = crate::alloc::vec![];
                    #[cfg(debug_assertions)]
                    {
                        for cell in &cell_array.cell_payloads {
                            crate::with_btree_allocation_site!(Balance, {
                                let cell = cell.try_to_vec()?;
                                cells_debug.try_push(cell)
                            })?;
                            if is_leaf {
                                crate::turso_assert_ne!(cell[0], 0);
                            }
                        }
                    }

                    #[cfg(debug_assertions)]
                    validate_cells_after_insertion(&cell_array, is_table_leaf);

                    /* 3. Initiliaze current size of every page including overflow cells and divider cells that might be included. */
                    let mut new_page_sizes: [i64; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE] =
                        [0; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE];
                    let header_size = if is_leaf {
                        LEAF_PAGE_HEADER_SIZE_BYTES
                    } else {
                        INTERIOR_PAGE_HEADER_SIZE_BYTES
                    };
                    // number of bytes beyond header, different from global usableSapce which includes
                    // header
                    let usable_space_without_header = usable_space - header_size;
                    for i in 0..balance_info.sibling_count {
                        cell_array.cell_count_per_page_cumulative[i] =
                            old_cell_count_per_page_cumulative[i];
                        let page = &balance_info.pages_to_balance[i].as_ref().unwrap();
                        let page_contents = page.get_contents();
                        let free_space = compute_free_space(page_contents, usable_space)?;

                        new_page_sizes[i] = usable_space_without_header as i64 - free_space as i64;
                        for overflow in &page_contents.overflow_cells {
                            // 2 to account of pointer
                            new_page_sizes[i] += 2 + overflow.payload.len() as i64;
                        }
                        let is_last_sibling = i == balance_info.sibling_count - 1;
                        if !is_leaf && !is_last_sibling {
                            // Account for divider cell which is included in this page.
                            new_page_sizes[i] += cell_array.cell_payloads
                                [cell_array.cell_count_up_to_page(i)]
                            .len() as i64;
                        }
                    }

                    /* 4. Now let's try to move cells to the left trying to stack them without exceeding the maximum size of a page.
                         There are two cases:
                           * If current page has too many cells, it will move them to the next page.
                           * If it still has space, and it can take a cell from the right it will take them.
                             Here there is a caveat. Taking a cell from the right might take cells from page i+1, i+2, i+3, so not necessarily
                             adjacent. But we decrease the size of the adjacent page if we move from the right. This might cause a intermitent state
                             where page can have size <0.
                        This will also calculate how many pages are required to balance the cells and store in sibling_count_new.
                    */
                    // Try to pack as many cells to the left
                    let mut sibling_count_new = balance_info.sibling_count;
                    let mut i = 0;
                    while i < sibling_count_new {
                        // First try to move cells to the right if they do not fit
                        while new_page_sizes[i] > usable_space_without_header as i64 {
                            let needs_new_page = i + 1 >= sibling_count_new;
                            if needs_new_page {
                                sibling_count_new = i + 2;
                                turso_assert!(
                                    sibling_count_new <= 5,
                                    "it is corrupt to require more than 5 pages to balance 3 siblings"
                                );

                                new_page_sizes[sibling_count_new - 1] = 0;
                                cell_array.cell_count_per_page_cumulative[sibling_count_new - 1] =
                                    cell_array.cell_payloads.len() as u16;
                            }
                            let size_of_cell_to_remove_from_left = 2 + cell_array.cell_payloads
                                [cell_array.cell_count_up_to_page(i) - 1]
                                .len()
                                as i64;
                            new_page_sizes[i] -= size_of_cell_to_remove_from_left;
                            let size_of_cell_to_move_right = if !is_table_leaf {
                                if cell_array.cell_count_per_page_cumulative[i]
                                    < cell_array.cell_payloads.len() as u16
                                {
                                    // This means we move to the right page the divider cell and we
                                    // promote left cell to divider
                                    CELL_PTR_SIZE_BYTES as i64
                                        + cell_array.cell_payloads
                                            [cell_array.cell_count_up_to_page(i)]
                                        .len() as i64
                                } else {
                                    0
                                }
                            } else {
                                size_of_cell_to_remove_from_left
                            };
                            new_page_sizes[i + 1] += size_of_cell_to_move_right;
                            cell_array.cell_count_per_page_cumulative[i] -= 1;
                        }

                        // Now try to take from the right if we didn't have enough
                        while cell_array.cell_count_per_page_cumulative[i]
                            < cell_array.cell_payloads.len() as u16
                        {
                            let size_of_cell_to_remove_from_right = CELL_PTR_SIZE_BYTES as i64
                                + cell_array.cell_payloads[cell_array.cell_count_up_to_page(i)]
                                    .len() as i64;
                            let can_take = new_page_sizes[i] + size_of_cell_to_remove_from_right
                                > usable_space_without_header as i64;
                            if can_take {
                                break;
                            }
                            new_page_sizes[i] += size_of_cell_to_remove_from_right;
                            cell_array.cell_count_per_page_cumulative[i] += 1;

                            let size_of_cell_to_remove_from_right = if !is_table_leaf {
                                if cell_array.cell_count_per_page_cumulative[i]
                                    < cell_array.cell_payloads.len() as u16
                                {
                                    CELL_PTR_SIZE_BYTES as i64
                                        + cell_array.cell_payloads
                                            [cell_array.cell_count_up_to_page(i)]
                                        .len() as i64
                                } else {
                                    0
                                }
                            } else {
                                size_of_cell_to_remove_from_right
                            };

                            new_page_sizes[i + 1] -= size_of_cell_to_remove_from_right;
                        }

                        // Check if this page contains up to the last cell. If this happens it means we really just need up to this page.
                        // Let's update the number of new pages to be up to this page (i+1)
                        let page_completes_all_cells = cell_array.cell_count_per_page_cumulative[i]
                            >= cell_array.cell_payloads.len() as u16;
                        if page_completes_all_cells {
                            sibling_count_new = i + 1;
                            break;
                        }
                        i += 1;
                        if i >= sibling_count_new {
                            break;
                        }
                    }

                    tracing::debug!(
                        "balance_non_root(sibling_count={}, sibling_count_new={}, cells={})",
                        balance_info.sibling_count,
                        sibling_count_new,
                        cell_array.cell_payloads.len()
                    );

                    /* 5. Balance pages starting from a left stacked cell state and move them to right trying to maintain a balanced state
                    where we only move from left to right if it will not unbalance both pages, meaning moving left to right won't make
                    right page bigger than left page.
                    */
                    // Comment borrowed from SQLite src/btree.c
                    // The packing computed by the previous block is biased toward the siblings
                    // on the left side (siblings with smaller keys). The left siblings are
                    // always nearly full, while the right-most sibling might be nearly empty.
                    // The next block of code attempts to adjust the packing of siblings to
                    // get a better balance.
                    //
                    // This adjustment is more than an optimization.  The packing above might
                    // be so out of balance as to be illegal.  For example, the right-most
                    // sibling might be completely empty.  This adjustment is not optional.
                    for i in (1..sibling_count_new).rev() {
                        let mut size_right_page = new_page_sizes[i];
                        let mut size_left_page = new_page_sizes[i - 1];
                        let mut cell_left = cell_array.cell_count_per_page_cumulative[i - 1] - 1;
                        // When table leaves are being balanced, divider cells are not part of the balancing,
                        // because table dividers don't have payloads unlike index dividers.
                        // Hence:
                        // - For table leaves: the same cell that is removed from left is added to right.
                        // - For all other page types: the divider cell is added to right, and the last non-divider cell is removed from left;
                        //   the cell removed from the left will later become a new divider cell in the parent page.
                        // TABLE LEAVES BALANCING:
                        // =======================
                        // Before balancing:
                        // LEFT                          RIGHT
                        // +-----+-----+-----+-----+    +-----+-----+
                        // | C1  | C2  | C3  | C4  |    | C5  | C6  |
                        // +-----+-----+-----+-----+    +-----+-----+
                        //         ^                           ^
                        //    (too full)                  (has space)
                        // After balancing:
                        // LEFT                     RIGHT
                        // +-----+-----+-----+      +-----+-----+-----+
                        // | C1  | C2  | C3  |      | C4  | C5  | C6  |
                        // +-----+-----+-----+      +-----+-----+-----+
                        //                               ^
                        //                          (C4 moved directly)
                        //
                        // (C3's rowid also becomes the divider cell's rowid in the parent page
                        //
                        // OTHER PAGE TYPES BALANCING:
                        // ===========================
                        // Before balancing:
                        // PARENT: [...|D1|...]
                        //            |
                        // LEFT                          RIGHT
                        // +-----+-----+-----+-----+    +-----+-----+
                        // | K1  | K2  | K3  | K4  |    | K5  | K6  |
                        // +-----+-----+-----+-----+    +-----+-----+
                        //         ^                           ^
                        //    (too full)                  (has space)
                        // After balancing:
                        // PARENT: [...|K4|...]  <-- K4 becomes new divider
                        //            |
                        // LEFT                     RIGHT
                        // +-----+-----+-----+      +-----+-----+-----+
                        // | K1  | K2  | K3  |      | D1  | K5  | K6  |
                        // +-----+-----+-----+      +-----+-----+-----+
                        //                               ^
                        //                     (old divider D1 added to right)
                        // Legend:
                        // - C# = Cell (table leaf)
                        // - K# = Key cell (index/internal node)
                        // - D# = Divider cell
                        let mut cell_right = if is_table_leaf {
                            cell_left
                        } else {
                            cell_left + 1
                        };
                        loop {
                            let cell_left_size =
                                cell_array.cell_size_bytes(cell_left as usize) as i64;
                            let cell_right_size =
                                cell_array.cell_size_bytes(cell_right as usize) as i64;
                            // TODO: add assert nMaxCells

                            let is_last_sibling = i == sibling_count_new - 1;
                            let pointer_size = if is_last_sibling {
                                0
                            } else {
                                CELL_PTR_SIZE_BYTES as i64
                            };
                            // As mentioned, this step rebalances the siblings so that cells are moved from left to right, since the previous step just
                            // packed as much as possible to the left. However, if the right-hand-side page would become larger than the left-hand-side page,
                            // we stop.
                            let would_not_improve_balance =
                                size_right_page + cell_right_size + (CELL_PTR_SIZE_BYTES as i64)
                                    > size_left_page - (cell_left_size + pointer_size);
                            if size_right_page != 0 && would_not_improve_balance {
                                break;
                            }

                            size_left_page -= cell_left_size + (CELL_PTR_SIZE_BYTES as i64);
                            size_right_page += cell_right_size + (CELL_PTR_SIZE_BYTES as i64);
                            cell_array.cell_count_per_page_cumulative[i - 1] = cell_left;

                            if cell_left == 0 {
                                break;
                            }
                            cell_left -= 1;
                            cell_right -= 1;
                        }

                        new_page_sizes[i] = size_right_page;
                        new_page_sizes[i - 1] = size_left_page;
                        turso_assert_greater_than!(
                            cell_array.cell_count_per_page_cumulative[i - 1],
                            if i > 1 {
                                cell_array.cell_count_per_page_cumulative[i - 2]
                            } else {
                                0
                            }
                        );
                    }

                    *sub_state = BalanceSubState::NonRootDoBalancingAllocate {
                        i: 0,
                        context: Some(BalanceContext {
                            pages_to_balance_new,
                            sibling_count_new,
                            cell_array,
                            old_cell_count_per_page_cumulative,
                            #[cfg(debug_assertions)]
                            cells_debug,
                        }),
                    };
                }
                BalanceSubState::NonRootDoBalancingAllocate { i, context } => {
                    let BalanceContext {
                        pages_to_balance_new,
                        old_cell_count_per_page_cumulative,
                        cell_array,
                        sibling_count_new,
                        ..
                    } = context.as_mut().unwrap();
                    let pager = self.pager.clone();
                    let balance_info = balance_info.as_mut().unwrap();
                    let page_type = balance_info.pages_to_balance[0]
                        .as_ref()
                        .unwrap()
                        .get_contents()
                        .page_type()?;
                    // Allocate pages or set dirty if not needed
                    if *i < balance_info.sibling_count {
                        let page = balance_info.pages_to_balance[*i].as_ref().unwrap();
                        turso_assert!(page.is_dirty(), "sibling page must be already marked dirty");
                        pages_to_balance_new[*i].replace(page.clone());
                    } else {
                        let page = return_if_io!(pager.do_allocate_page(
                            page_type,
                            0,
                            BtreePageAllocMode::Any
                        ));
                        pages_to_balance_new[*i].replace(PinGuard::new(page));
                        // Since this page didn't exist before, we can set it to cells length as it
                        // marks them as empty since it is a prefix sum of cells.
                        old_cell_count_per_page_cumulative[*i] =
                            cell_array.cell_payloads.len() as u16;
                    }
                    if *i + 1 < *sibling_count_new {
                        *i += 1;
                        continue;
                    } else {
                        *sub_state = BalanceSubState::NonRootDoBalancingFinish {
                            context: context.take().unwrap(),
                        };
                    }
                }
                BalanceSubState::NonRootDoBalancingFinish {
                    context:
                        BalanceContext {
                            pages_to_balance_new,
                            sibling_count_new,
                            cell_array,
                            old_cell_count_per_page_cumulative,
                            #[cfg(debug_assertions)]
                            cells_debug,
                        },
                } => {
                    let balance_info = balance_info.as_mut().unwrap();
                    let page_type = balance_info.pages_to_balance[0]
                        .as_ref()
                        .unwrap()
                        .get_contents()
                        .page_type()?;
                    let parent_is_root = !self.stack.has_parent();
                    let parent_page = PinGuard::new(self.stack.top_ref().clone());
                    let parent_contents = parent_page.get_contents();
                    let mut sibling_count_new = *sibling_count_new;
                    let is_table_leaf = matches!(page_type, PageType::TableLeaf);
                    // Reassign page numbers in increasing order
                    {
                        let mut page_numbers: [usize; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE] =
                            [0; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE];
                        for (i, page) in pages_to_balance_new
                            .iter()
                            .take(sibling_count_new)
                            .enumerate()
                        {
                            page_numbers[i] = page.as_ref().unwrap().get().id;
                        }
                        page_numbers.sort_unstable();
                        for (page, new_id) in pages_to_balance_new
                            .iter()
                            .take(sibling_count_new)
                            .rev()
                            .zip(page_numbers.iter().rev().take(sibling_count_new))
                        {
                            let page = page.as_ref().unwrap();
                            if *new_id != page.get().id {
                                page.get().id = *new_id;
                                self.pager
                                    .upsert_page_in_cache(*new_id, page.0.clone(), true)?;
                            }
                        }

                        #[cfg(debug_assertions)]
                        {
                            tracing::debug!(
                                "balance_non_root(parent page_id={})",
                                parent_page.get().id
                            );
                            for page in pages_to_balance_new.iter().take(sibling_count_new) {
                                tracing::debug!(
                                    "balance_non_root(new_sibling page_id={})",
                                    page.as_ref().unwrap().get().id
                                );
                            }
                        }
                    }

                    // pages_pointed_to helps us debug we did in fact create divider cells to all the new pages and the rightmost pointer,
                    // also points to the last page.
                    #[cfg(debug_assertions)]
                    let mut pages_pointed_to = HashSet::default();

                    // Write right pointer in parent page to point to new rightmost page. keep in mind
                    // we update rightmost pointer first because inserting cells could defragment parent page,
                    // therfore invalidating the pointer.
                    let right_page_id = pages_to_balance_new[sibling_count_new - 1]
                        .as_ref()
                        .unwrap()
                        .get()
                        .id as u32;
                    let rightmost_pointer = balance_info.rightmost_pointer;
                    let rightmost_pointer =
                        unsafe { std::slice::from_raw_parts_mut(rightmost_pointer, 4) };
                    rightmost_pointer[0..4].copy_from_slice(&right_page_id.to_be_bytes());

                    #[cfg(debug_assertions)]
                    pages_pointed_to.insert(right_page_id);
                    tracing::debug!(
                        "balance_non_root(rightmost_pointer_update, rightmost_pointer={})",
                        right_page_id
                    );

                    /* 6. Update parent pointers. Update right pointer and insert divider cells with newly created distribution of cells */
                    // Ensure right-child pointer of the right-most new sibling pge points to the page
                    // that was originally on that place.
                    let is_leaf_page =
                        matches!(page_type, PageType::TableLeaf | PageType::IndexLeaf);
                    if !is_leaf_page {
                        let last_sibling_idx = balance_info.sibling_count - 1;
                        let last_page = balance_info.pages_to_balance[last_sibling_idx]
                            .as_ref()
                            .unwrap();
                        let right_pointer = last_page.get_contents().rightmost_pointer()?.unwrap();
                        let new_last_page = pages_to_balance_new[sibling_count_new - 1]
                            .as_ref()
                            .unwrap();
                        new_last_page
                            .get_contents()
                            .write_rightmost_ptr(right_pointer);
                    }
                    turso_assert!(
                        parent_contents.overflow_cells.is_empty(),
                        "parent page overflow cells should be empty before divider cell reinsertion"
                    );
                    // TODO: pointer map update (vacuum support)
                    // Update divider cells in parent
                    // Cache first_divider_cell to allow mutable access to reusable_divider_cell
                    let first_divider_cell_cached = balance_info.first_divider_cell;
                    for (sibling_page_idx, page) in pages_to_balance_new
                        .iter()
                        .enumerate()
                        .take(sibling_count_new - 1)
                    /* do not take last page */
                    {
                        let page = page.as_ref().unwrap();
                        // e.g. if we have 3 pages and the leftmost child page has 3 cells,
                        // then the divider cell idx is 3 in the flat cell array.
                        let divider_cell_idx = cell_array.cell_count_up_to_page(sibling_page_idx);
                        let mut divider_cell = &mut cell_array.cell_payloads[divider_cell_idx];
                        // Reuse the buffer for constructing new divider cell to avoid allocation per iteration
                        balance_info.reusable_divider_cell.clear();
                        if !is_leaf_page {
                            // Interior
                            // Make this page's rightmost pointer point to pointer of divider cell before modification
                            let previous_pointer_divider = read_u32(divider_cell, 0);
                            page.get_contents()
                                .write_rightmost_ptr(previous_pointer_divider);
                            // divider cell now points to this page
                            balance_info
                                .reusable_divider_cell
                                .extend_from_slice(&(page.get().id as u32).to_be_bytes());
                            // now copy the rest of the divider cell:
                            // Table Interior page:
                            //   * varint rowid
                            // Index Interior page:
                            //   * varint payload size
                            //   * payload
                            //   * first overflow page (u32 optional)
                            balance_info
                                .reusable_divider_cell
                                .extend_from_slice(&divider_cell[4..]);
                        } else if is_table_leaf {
                            // For table leaves, divider_cell_idx effectively points to the last cell of the old left page.
                            // The new divider cell's rowid becomes the second-to-last cell's rowid.
                            // i.e. in the diagram above, the new divider cell's rowid becomes the rowid of C3.
                            // FIXME: not needed conversion
                            // FIXME: need to update cell size in order to free correctly?
                            // insert into cell with correct range should be enough
                            divider_cell = &mut cell_array.cell_payloads[divider_cell_idx - 1];
                            let (_, n_bytes_payload) = read_varint(divider_cell)?;
                            let (rowid, _) = read_varint(&divider_cell[n_bytes_payload..])?;
                            balance_info
                                .reusable_divider_cell
                                .extend_from_slice(&(page.get().id as u32).to_be_bytes());
                            write_varint_to_vec(rowid, &mut balance_info.reusable_divider_cell)?;
                        } else {
                            // Leaf index
                            // A leaf cell is read as at least MINIMUM_CELL_SIZE bytes, so a 2-byte
                            // record carries one byte of padding here. The parent stores the cell's
                            // real size after the child pointer, so drop the padding when promoting.
                            let (payload_len, n_payload) = read_varint(divider_cell)?;
                            let real_len = n_payload + payload_len as usize;
                            let divider_cell = if real_len < divider_cell.len() {
                                turso_assert!(
                                    real_len < MINIMUM_CELL_SIZE,
                                    "only cells below the minimum cell size carry padding",
                                    { "real_len": real_len, "cell_len": divider_cell.len() }
                                );
                                &divider_cell[..real_len]
                            } else {
                                &divider_cell[..]
                            };
                            balance_info
                                .reusable_divider_cell
                                .extend_from_slice(&(page.get().id as u32).to_be_bytes());
                            balance_info
                                .reusable_divider_cell
                                .extend_from_slice(divider_cell);
                        }

                        let left_pointer = read_u32(
                            &balance_info.reusable_divider_cell[..LEFT_CHILD_PTR_SIZE_BYTES],
                            0,
                        );
                        turso_assert!(
                            left_pointer != parent_page.get().id as u32,
                            "left pointer is the same as parent page id"
                        );
                        #[cfg(debug_assertions)]
                        {
                            pages_pointed_to.insert(left_pointer);
                            tracing::debug!(
                                "balance_non_root(insert_divider_cell, first_divider_cell={}, divider_cell={}, left_pointer={})",
                                first_divider_cell_cached,
                                sibling_page_idx,
                                left_pointer
                            );
                        }
                        turso_assert!(
                            left_pointer == page.get().id as u32,
                            "left pointer is not the same as page id"
                        );
                        // FIXME: remove this lock
                        let database_size = self
                            .pager
                            .io
                            .block(|| self.pager.with_header(|header| header.database_size))?
                            .get();
                        turso_assert!(
                            left_pointer <= database_size,
                            "invalid page number divider left pointer exceeds database number of pages",
                            { "left_pointer": left_pointer, "database_size": database_size }
                        );
                        let divider_cell_insert_idx_in_parent =
                            first_divider_cell_cached + sibling_page_idx;
                        #[cfg(debug_assertions)]
                        let overflow_cell_count_before = parent_contents.overflow_cells.len();
                        insert_into_cell(
                            parent_contents,
                            &balance_info.reusable_divider_cell,
                            divider_cell_insert_idx_in_parent,
                            usable_space,
                        )?;
                        #[cfg(debug_assertions)]
                        {
                            let overflow_cell_count_after = parent_contents.overflow_cells.len();
                            let divider_cell_is_overflow_cell =
                                overflow_cell_count_after > overflow_cell_count_before;

                            BTreeCursor::validate_balance_non_root_divider_cell_insertion(
                                balance_info,
                                parent_contents,
                                divider_cell_insert_idx_in_parent,
                                divider_cell_is_overflow_cell,
                                page,
                                usable_space,
                            );
                        }
                    }
                    tracing::debug!(
                        "balance_non_root(parent_overflow={})",
                        parent_contents.overflow_cells.len()
                    );

                    #[cfg(debug_assertions)]
                    {
                        // Let's ensure every page is pointed to by the divider cell or the rightmost pointer.
                        for page in pages_to_balance_new.iter().take(sibling_count_new) {
                            let page = page.as_ref().unwrap();
                            turso_assert!(
                                pages_pointed_to.contains(&(page.get().id as u32)),
                                "page not pointed to by divider cell or rightmost pointer",
                                { "page_id": page.get().id }
                            );
                        }
                    }
                    /* 7. Start real movement of cells. Next comment is borrowed from SQLite: */
                    /* Now update the actual sibling pages. The order in which they are updated
                     ** is important, as this code needs to avoid disrupting any page from which
                     ** cells may still to be read. In practice, this means:
                     **
                     **  (1) If cells are moving left (from apNew[iPg] to apNew[iPg-1])
                     **      then it is not safe to update page apNew[iPg] until after
                     **      the left-hand sibling apNew[iPg-1] has been updated.
                     **
                     **  (2) If cells are moving right (from apNew[iPg] to apNew[iPg+1])
                     **      then it is not safe to update page apNew[iPg] until after
                     **      the right-hand sibling apNew[iPg+1] has been updated.
                     **
                     ** If neither of the above apply, the page is safe to update.
                     **
                     ** The iPg value in the following loop starts at nNew-1 goes down
                     ** to 0, then back up to nNew-1 again, thus making two passes over
                     ** the pages.  On the initial downward pass, only condition (1) above
                     ** needs to be tested because (2) will always be true from the previous
                     ** step.  On the upward pass, both conditions are always true, so the
                     ** upwards pass simply processes pages that were missed on the downward
                     ** pass.
                     */
                    let mut done = [false; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE];
                    let rightmost_page_negative_idx = 1 - sibling_count_new as i64;
                    let rightmost_page_positive_idx = sibling_count_new as i64 - 1;
                    for i in rightmost_page_negative_idx..=rightmost_page_positive_idx {
                        // As mentioned above, we do two passes over the pages:
                        // 1. Downward pass: Process pages in decreasing order
                        // 2. Upward pass: Process pages in increasing order
                        // Hence if we have 3 siblings:
                        // the order of 'i' will be: -2, -1, 0, 1, 2.
                        // and the page processing order is: 2, 1, 0, 1, 2.
                        let page_idx = i.unsigned_abs() as usize;
                        if done[page_idx] {
                            continue;
                        }
                        // As outlined above, this condition ensures we process pages in the correct order to avoid disrupting cells that still need to be read.
                        // 1. i >= 0 handles the upward pass where we process any pages not processed in the downward pass.
                        //    - condition (1) is not violated: if cells are moving right-to-left, righthand sibling has not been updated yet.
                        //    - condition (2) is not violated: if cells are moving left-to-right, righthand sibling has already been updated in the downward pass.
                        // 2. The second condition checks if it's safe to process a page during the downward pass.
                        //    - condition (1) is not violated: if cells are moving right-to-left, we do nothing.
                        //    - condition (2) is not violated: if cells are moving left-to-right, we are allowed to update.
                        if i >= 0
                            || old_cell_count_per_page_cumulative[page_idx - 1]
                                >= cell_array.cell_count_per_page_cumulative[page_idx - 1]
                        {
                            let (start_old_cells, start_new_cells, number_new_cells) = if page_idx
                                == 0
                            {
                                (0, 0, cell_array.cell_count_up_to_page(0))
                            } else {
                                let this_was_old_page = page_idx < balance_info.sibling_count;
                                // We add !is_table_leaf because we want to skip 1 in case of divider cell which is encountared between pages assigned
                                let start_old_cells = if this_was_old_page {
                                    old_cell_count_per_page_cumulative[page_idx - 1] as usize
                                        + (!is_table_leaf) as usize
                                } else {
                                    cell_array.cell_payloads.len()
                                };
                                let start_new_cells = cell_array
                                    .cell_count_up_to_page(page_idx - 1)
                                    + (!is_table_leaf) as usize;
                                (
                                    start_old_cells,
                                    start_new_cells,
                                    cell_array.cell_count_up_to_page(page_idx) - start_new_cells,
                                )
                            };
                            let page = pages_to_balance_new[page_idx].as_ref().unwrap();
                            tracing::debug!("pre_edit_page(page={})", page.get().id);
                            let page_contents = page.get_contents();
                            edit_page(
                                page_contents,
                                start_old_cells,
                                start_new_cells,
                                number_new_cells,
                                cell_array,
                                usable_space,
                            )?;
                            debug_validate_cells!(page_contents, usable_space);
                            tracing::trace!(
                                "edit_page page={} cells={}",
                                page.get().id,
                                page_contents.cell_count()
                            );
                            page_contents.overflow_cells.clear();

                            done[page_idx] = true;
                        }
                    }

                    // TODO: vacuum support
                    let first_child_page = pages_to_balance_new[0].as_ref().unwrap();
                    let first_child_contents = first_child_page.get_contents();
                    if parent_is_root
                        && parent_contents.cell_count() == 0
                        // this check to make sure we are not having negative free space
                        && parent_contents.offset()
                            <= compute_free_space(first_child_contents, usable_space)?
                    {
                        // From SQLite:
                        // The root page of the b-tree now contains no cells. The only sibling
                        // page is the right-child of the parent. Copy the contents of the
                        // child page into the parent, decreasing the overall height of the
                        // b-tree structure by one. This is described as the "balance-shallower"
                        // sub-algorithm in some documentation.
                        turso_assert_eq!(sibling_count_new, 1);
                        let parent_offset = if parent_page.get().id == 1 {
                            DatabaseHeader::SIZE
                        } else {
                            0
                        };
                        #[cfg(debug_assertions)]
                        turso_assert_eq!(parent_offset, parent_contents.offset());

                        // From SQLite:
                        // It is critical that the child page be defragmented before being
                        // copied into the parent, because if the parent is page 1 then it will
                        // by smaller than the child due to the database header, and so
                        // all the free space needs to be up front.
                        defragment_page_full(first_child_contents, usable_space)?;

                        let child_top = first_child_contents.cell_content_area() as usize;
                        let parent_buf = parent_contents.as_ptr();
                        let child_buf = first_child_contents.as_ptr();
                        let content_size = usable_space - child_top;

                        // Copy cell contents
                        parent_buf[child_top..child_top + content_size]
                            .copy_from_slice(&child_buf[child_top..child_top + content_size]);

                        // Copy header and pointer
                        // NOTE: don't use .cell_pointer_array_offset_and_size() because of different
                        // header size
                        let header_and_pointer_size = first_child_contents.header_size()
                            + first_child_contents.cell_pointer_array_size();
                        let first_child_offset = first_child_contents.offset();
                        parent_buf[parent_offset..parent_offset + header_and_pointer_size]
                            .copy_from_slice(
                                &child_buf[first_child_offset
                                    ..first_child_offset + header_and_pointer_size],
                            );

                        sibling_count_new -= 1; // decrease sibling count for debugging and free at the end
                        turso_assert_less_than!(sibling_count_new, balance_info.sibling_count);
                    }

                    #[cfg(debug_assertions)]
                    BTreeCursor::post_balance_non_root_validation(
                        &parent_page,
                        balance_info,
                        parent_contents,
                        pages_to_balance_new,
                        page_type,
                        is_table_leaf,
                        cells_debug,
                        sibling_count_new,
                        right_page_id,
                        usable_space,
                    );

                    // Balance-shallower case
                    if sibling_count_new == 0 {
                        self.stack.set_cell_index(0); // reset cell index, top is already parent
                    }

                    // Restore the cell_payloads Vec to BalanceState for reuse in future operations.
                    // This avoids allocation on subsequent balance operations.
                    let mut recovered_vec = take_vec(&mut cell_array.cell_payloads);
                    recovered_vec.clear();
                    *reusable_cell_payloads = recovered_vec;

                    *sub_state = BalanceSubState::FreePages {
                        curr_page: sibling_count_new,
                        sibling_count_new,
                    };
                }
                BalanceSubState::FreePages {
                    curr_page,
                    sibling_count_new,
                } => {
                    let sibling_count = {
                        balance_info
                            .as_ref()
                            .expect("must be balancing")
                            .sibling_count
                    };
                    // We have to free pages that are not used anymore
                    if !((*sibling_count_new..sibling_count).contains(curr_page)) {
                        *sub_state = BalanceSubState::Start;
                        let _ = balance_info.take();
                        return Ok(IOResult::Done(()));
                    } else {
                        let balance_info = balance_info.as_ref().expect("must be balancing");
                        let page = balance_info.pages_to_balance[*curr_page].as_ref().unwrap();
                        return_if_io!(self.pager.free_page(Some(page.0.clone()), page.get().id));
                        *sub_state = BalanceSubState::FreePages {
                            curr_page: *curr_page + 1,
                            sibling_count_new: *sibling_count_new,
                        };
                    }
                }
            }
        }
    }

    /// Validates that a divider cell was correctly inserted into the parent page
    /// during B-tree balancing and that it points to the correct child page.
    #[cfg(debug_assertions)]
    fn validate_balance_non_root_divider_cell_insertion(
        balance_info: &BalanceInfo,
        parent_contents: &mut PageContent,
        divider_cell_insert_idx_in_parent: usize,
        divider_cell_is_overflow_cell: bool,
        child_page: &PageRef,
        usable_space: usize,
    ) {
        let left_pointer = if divider_cell_is_overflow_cell {
            parent_contents.overflow_cells
                .iter()
                .find(|cell| cell.index == divider_cell_insert_idx_in_parent)
                .map(|cell| read_u32(&cell.payload, 0))
                .unwrap_or_else(|| {
                    panic!(
                        "overflow cell with divider cell was not found (divider_cell_idx={}, balance_info.first_divider_cell={}, overflow_cells.len={})",
                        divider_cell_insert_idx_in_parent,
                        balance_info.first_divider_cell,
                        parent_contents.overflow_cells.len(),
                    )
                })
        } else if divider_cell_insert_idx_in_parent < parent_contents.cell_count() {
            let (cell_start, cell_len) = parent_contents
                .cell_get_raw_region(divider_cell_insert_idx_in_parent, usable_space)
                .unwrap();
            read_u32(
                &parent_contents.as_ptr()[cell_start..cell_start + cell_len],
                0,
            )
        } else {
            panic!(
                "divider cell is not in the parent page (divider_cell_idx={}, balance_info.first_divider_cell={}, overflow_cells.len={})",
                divider_cell_insert_idx_in_parent,
                balance_info.first_divider_cell,
                parent_contents.overflow_cells.len(),
            )
        };

        // Verify the left pointer points to the correct page
        turso_assert_eq!(
            left_pointer,
            child_page.get().id as u32,
            "inserted cell doesn't point to correct page",
            { "left_pointer": left_pointer, "child_page_id": child_page.get().id }
        );
    }

    #[cfg(debug_assertions)]
    #[allow(clippy::too_many_arguments)]
    fn post_balance_non_root_validation(
        parent_page: &PageRef,
        balance_info: &BalanceInfo,
        parent_contents: &mut PageContent,
        pages_to_balance_new: &[Option<PinGuard>; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE],
        page_type: PageType,
        is_table_leaf: bool,
        cells_debug: &mut [crate::alloc::Vec<u8>],
        sibling_count_new: usize,
        right_page_id: u32,
        usable_space: usize,
    ) {
        let mut valid = true;
        let mut current_index_cell = 0;
        for cell_idx in 0..parent_contents.cell_count() {
            let cell = parent_contents.cell_get(cell_idx, usable_space).unwrap();
            match cell {
                BTreeCell::TableInteriorCell(table_interior_cell) => {
                    let left_child_page = table_interior_cell.left_child_page;
                    if left_child_page == parent_page.get().id as u32 {
                        tracing::error!("balance_non_root(parent_divider_points_to_same_page, page_id={}, cell_left_child_page={})",
                                parent_page.get().id,
                                left_child_page,
                            );
                        valid = false;
                    }
                }
                BTreeCell::IndexInteriorCell(index_interior_cell) => {
                    let left_child_page = index_interior_cell.left_child_page;
                    if left_child_page == parent_page.get().id as u32 {
                        tracing::error!("balance_non_root(parent_divider_points_to_same_page, page_id={}, cell_left_child_page={})",
                                parent_page.get().id,
                                left_child_page,
                            );
                        valid = false;
                    }
                }
                _ => {}
            }
        }
        // Let's now make a in depth check that we in fact added all possible cells somewhere and they are not lost
        for (page_idx, page) in pages_to_balance_new
            .iter()
            .take(sibling_count_new)
            .enumerate()
        {
            let page = page.as_ref().unwrap();
            let contents = page.get_contents();
            debug_validate_cells!(contents, usable_space);
            // Cells are distributed in order
            for cell_idx in 0..contents.cell_count() {
                let (cell_start, cell_len) = contents
                    .cell_get_raw_region(cell_idx, usable_space)
                    .unwrap();
                let buf = contents.as_ptr();
                let cell_buf = to_static_buf(&mut buf[cell_start..cell_start + cell_len]);
                let cell_buf_in_array = &cells_debug[current_index_cell];
                if cell_buf != cell_buf_in_array {
                    tracing::error!("balance_non_root(cell_not_found_debug, page_id={}, cell_in_cell_array_idx={})",
                        page.get().id,
                        current_index_cell,
                    );
                    valid = false;
                }

                let cell = crate::storage::sqlite3_ondisk::read_btree_cell(
                    cell_buf,
                    contents,
                    0,
                    usable_space,
                )
                .unwrap();
                match &cell {
                    BTreeCell::TableInteriorCell(table_interior_cell) => {
                        let left_child_page = table_interior_cell.left_child_page;
                        if left_child_page == page.get().id as u32 {
                            tracing::error!("balance_non_root(child_page_points_same_page, page_id={}, cell_left_child_page={}, page_idx={})",
                                page.get().id,
                                left_child_page,
                                page_idx
                            );
                            valid = false;
                        }
                        if left_child_page == parent_page.get().id as u32 {
                            tracing::error!("balance_non_root(child_page_points_parent_of_child, page_id={}, cell_left_child_page={}, page_idx={})",
                                page.get().id,
                                left_child_page,
                                page_idx
                            );
                            valid = false;
                        }
                    }
                    BTreeCell::IndexInteriorCell(index_interior_cell) => {
                        let left_child_page = index_interior_cell.left_child_page;
                        if left_child_page == page.get().id as u32 {
                            tracing::error!("balance_non_root(child_page_points_same_page, page_id={}, cell_left_child_page={}, page_idx={})",
                                page.get().id,
                                left_child_page,
                                page_idx
                            );
                            valid = false;
                        }
                        if left_child_page == parent_page.get().id as u32 {
                            tracing::error!("balance_non_root(child_page_points_parent_of_child, page_id={}, cell_left_child_page={}, page_idx={})",
                                page.get().id,
                                left_child_page,
                                page_idx
                            );
                            valid = false;
                        }
                    }
                    _ => {}
                }
                current_index_cell += 1;
            }
            // Now check divider cells and their pointers.
            let parent_buf = parent_contents.as_ptr();
            let cell_divider_idx = balance_info.first_divider_cell + page_idx;
            if sibling_count_new == 0 {
                // Balance-shallower case
                // We need to check data in parent page
                debug_validate_cells!(parent_contents, usable_space);

                if pages_to_balance_new[0].is_none() {
                    tracing::error!(
                        "balance_non_root(balance_shallower_incorrect_page, page_idx={})",
                        0
                    );
                    valid = false;
                }

                for (i, value) in pages_to_balance_new
                    .iter()
                    .enumerate()
                    .take(sibling_count_new)
                    .skip(1)
                {
                    if value.is_some() {
                        tracing::error!(
                            "balance_non_root(balance_shallower_incorrect_page, page_idx={})",
                            i
                        );
                        valid = false;
                    }
                }

                if current_index_cell != cells_debug.len()
                    || cells_debug.len() != contents.cell_count()
                    || contents.cell_count() != parent_contents.cell_count()
                {
                    tracing::error!("balance_non_root(balance_shallower_incorrect_cell_count, current_index_cell={}, cells_debug={}, cell_count={}, parent_cell_count={})",
                        current_index_cell,
                        cells_debug.len(),
                        contents.cell_count(),
                        parent_contents.cell_count()
                    );
                    valid = false;
                }

                if right_page_id == page.get().id as u32
                    || right_page_id == parent_page.get().id as u32
                {
                    tracing::error!("balance_non_root(balance_shallower_rightmost_pointer, page_id={}, parent_page_id={}, rightmost={})",
                        page.get().id,
                        parent_page.get().id,
                        right_page_id,
                    );
                    valid = false;
                }

                if let Some(rm) = contents.rightmost_pointer().ok().flatten() {
                    if rm != right_page_id {
                        tracing::error!("balance_non_root(balance_shallower_rightmost_pointer, page_rightmost={}, rightmost={})",
                            rm,
                            right_page_id,
                        );
                        valid = false;
                    }
                }

                if let Some(rm) = parent_contents.rightmost_pointer().ok().flatten() {
                    if rm != right_page_id {
                        tracing::error!("balance_non_root(balance_shallower_rightmost_pointer, parent_rightmost={}, rightmost={})",
                            rm,
                            right_page_id,
                        );
                        valid = false;
                    }
                }

                if parent_contents.page_type().ok() != Some(page_type) {
                    tracing::error!("balance_non_root(balance_shallower_parent_page_type, page_type={:?}, parent_page_type={:?})",
                        page_type,
                        parent_contents.page_type().ok()
                    );
                    valid = false
                }

                for (parent_cell_idx, cell_buf_in_array) in
                    cells_debug.iter().enumerate().take(contents.cell_count())
                {
                    let (parent_cell_start, parent_cell_len) = parent_contents
                        .cell_get_raw_region(parent_cell_idx, usable_space)
                        .unwrap();

                    let (cell_start, cell_len) = contents
                        .cell_get_raw_region(parent_cell_idx, usable_space)
                        .unwrap();

                    let buf = contents.as_ptr();
                    let cell_buf = to_static_buf(&mut buf[cell_start..cell_start + cell_len]);
                    let parent_cell_buf = to_static_buf(
                        &mut parent_buf[parent_cell_start..parent_cell_start + parent_cell_len],
                    );

                    if cell_buf != cell_buf_in_array || cell_buf != parent_cell_buf {
                        tracing::error!("balance_non_root(balance_shallower_cell_not_found_debug, page_id={}, cell_in_cell_array_idx={})",
                            page.get().id,
                            parent_cell_idx,
                        );
                        valid = false;
                    }
                }
            } else if page_idx == sibling_count_new - 1 {
                // We will only validate rightmost pointer of parent page, we will not validate rightmost if it's a cell and not the last pointer because,
                // insert cell could've defragmented the page and invalidated the pointer.
                // right pointer, we just check right pointer points to this page.
                if cell_divider_idx == parent_contents.cell_count()
                    && right_page_id != page.get().id as u32
                {
                    tracing::error!("balance_non_root(cell_divider_right_pointer, should point to {}, but points to {})",
                        page.get().id,
                        right_page_id
                    );
                    valid = false;
                }
            } else {
                // divider cell might be an overflow cell
                let mut was_overflow = false;
                for overflow_cell in &parent_contents.overflow_cells {
                    if overflow_cell.index == cell_divider_idx {
                        let left_pointer = read_u32(&overflow_cell.payload, 0);
                        if left_pointer != page.get().id as u32 {
                            tracing::error!("balance_non_root(cell_divider_left_pointer_overflow, should point to page_id={}, but points to {}, divider_cell={}, overflow_cells_parent={})",
                        page.get().id,
                        left_pointer,
                        page_idx,
                        parent_contents.overflow_cells.len()
                    );
                            valid = false;
                        }
                        was_overflow = true;
                        break;
                    }
                }
                if was_overflow {
                    if !is_table_leaf {
                        // remember to increase cell if this cell was moved to parent
                        current_index_cell += 1;
                    }
                    continue;
                }
                // check if overflow
                // check if right pointer, this is the last page. Do we update rightmost pointer and defragment moves it?
                let (cell_start, cell_len) = parent_contents
                    .cell_get_raw_region(cell_divider_idx, usable_space)
                    .unwrap();
                let cell_left_pointer = read_u32(&parent_buf[cell_start..cell_start + cell_len], 0);
                if cell_left_pointer != page.get().id as u32 {
                    tracing::error!("balance_non_root(cell_divider_left_pointer, should point to page_id={}, but points to {}, divider_cell={}, overflow_cells_parent={})",
                        page.get().id,
                        cell_left_pointer,
                        page_idx,
                        parent_contents.overflow_cells.len()
                    );
                    valid = false;
                }
                if is_table_leaf {
                    // If we are in a table leaf page, we just need to check that this cell that should be a divider cell is in the parent
                    // This means we already check cell in leaf pages but not on parent so we don't advance current_index_cell
                    let last_sibling_idx = balance_info.sibling_count - 1;
                    if page_idx >= last_sibling_idx {
                        // This means we are in the last page and we don't need to check anything
                        continue;
                    }
                    let cell_buf: &'static mut [u8] =
                        to_static_buf(&mut cells_debug[current_index_cell - 1]);
                    let cell = crate::storage::sqlite3_ondisk::read_btree_cell(
                        cell_buf,
                        contents,
                        0,
                        usable_space,
                    )
                    .unwrap();
                    let parent_cell = parent_contents
                        .cell_get(cell_divider_idx, usable_space)
                        .unwrap();
                    let rowid = match cell {
                        BTreeCell::TableLeafCell(table_leaf_cell) => table_leaf_cell.rowid,
                        _ => unreachable!(),
                    };
                    let rowid_parent = match parent_cell {
                        BTreeCell::TableInteriorCell(table_interior_cell) => {
                            table_interior_cell.rowid
                        }
                        _ => unreachable!(),
                    };
                    if rowid_parent != rowid {
                        tracing::error!("balance_non_root(cell_divider_rowid, page_id={}, cell_divider_idx={}, rowid_parent={}, rowid={})",
                            page.get().id,
                            cell_divider_idx,
                            rowid_parent,
                            rowid
                        );
                        valid = false;
                    }
                } else {
                    // In any other case, we need to check that this cell was moved to parent as divider cell
                    let mut was_overflow = false;
                    for overflow_cell in &parent_contents.overflow_cells {
                        if overflow_cell.index == cell_divider_idx {
                            let left_pointer = read_u32(&overflow_cell.payload, 0);
                            if left_pointer != page.get().id as u32 {
                                tracing::error!("balance_non_root(cell_divider_divider_cell_overflow should point to page_id={}, but points to {}, divider_cell={}, overflow_cells_parent={})",
                                    page.get().id,
                                    left_pointer,
                                    page_idx,
                                    parent_contents.overflow_cells.len()
                                );
                                valid = false;
                            }
                            was_overflow = true;
                            break;
                        }
                    }
                    if was_overflow {
                        if !is_table_leaf {
                            // remember to increase cell if this cell was moved to parent
                            current_index_cell += 1;
                        }
                        continue;
                    }
                    let (parent_cell_start, parent_cell_len) = parent_contents
                        .cell_get_raw_region(cell_divider_idx, usable_space)
                        .unwrap();
                    let cell_buf_in_array = &cells_debug[current_index_cell];
                    let left_pointer = read_u32(
                        &parent_buf[parent_cell_start..parent_cell_start + parent_cell_len],
                        0,
                    );
                    if left_pointer != page.get().id as u32 {
                        tracing::error!("balance_non_root(divider_cell_left_pointer_interior should point to page_id={}, but points to {}, divider_cell={}, overflow_cells_parent={})",
                                    page.get().id,
                                    left_pointer,
                                    page_idx,
                                    parent_contents.overflow_cells.len()
                                );
                        valid = false;
                    }
                    match page_type {
                        PageType::TableInterior | PageType::IndexInterior => {
                            let parent_cell_buf =
                                &parent_buf[parent_cell_start..parent_cell_start + parent_cell_len];
                            if parent_cell_buf[4..] != cell_buf_in_array[4..] {
                                tracing::error!("balance_non_root(cell_divider_cell, page_id={}, cell_divider_idx={})",
                                    page.get().id,
                                    cell_divider_idx,
                                );
                                valid = false;
                            }
                        }
                        PageType::IndexLeaf => {
                            let parent_cell_buf =
                                &parent_buf[parent_cell_start..parent_cell_start + parent_cell_len];
                            // The parent stores the cell's real size; the cell array pads leaf
                            // cells up to MINIMUM_CELL_SIZE.
                            let parent_payload = &parent_cell_buf[4..];
                            let padded = parent_payload.len() < MINIMUM_CELL_SIZE
                                && cell_buf_in_array.len() == MINIMUM_CELL_SIZE;
                            let matches = cell_buf_in_array.len() >= parent_payload.len()
                                && cell_buf_in_array[..parent_payload.len()] == *parent_payload
                                && (cell_buf_in_array.len() == parent_payload.len() || padded);
                            if !matches {
                                tracing::error!("balance_non_root(cell_divider_cell_index_leaf, page_id={}, cell_divider_idx={})",
                                    page.get().id,
                                    cell_divider_idx,
                                );
                                valid = false;
                            }
                        }
                        _ => {
                            unreachable!()
                        }
                    }
                    current_index_cell += 1;
                }
            }
        }

        // Verify all cells were accounted for (non-shallower case)
        if sibling_count_new > 0 && current_index_cell != cells_debug.len() {
            tracing::error!(
                "balance_non_root(cell_count_mismatch, current_index_cell={}, cells_debug_len={}, sibling_count_new={})",
                current_index_cell,
                cells_debug.len(),
                sibling_count_new
            );
            valid = false;
        }

        turso_assert!(
            valid,
            "corrupted database, cells were not balanced properly"
        );
    }

    /// Balance the root page.
    /// This is done when the root page overflows, and we need to create a new root page.
    /// See e.g. https://en.wikipedia.org/wiki/B-tree
    fn balance_root(&mut self) -> IOResultOr<()> {
        /* todo: balance deeper, create child and copy contents of root there. Then split root */
        /* if we are in root page then we just need to create a new root and push key there */

        // Since we are going to change the btree structure, let's forget our cached knowledge of the rightmost page.
        let _ = self.move_to_right_state.1.take();

        let root = self.stack.top();
        let root_contents = root.get_contents();
        let child = return_if_io!(self.pager.do_allocate_page(
            root_contents.page_type()?,
            0,
            BtreePageAllocMode::Any
        ));

        let is_page_1 = root.get().id == 1;
        let offset = if is_page_1 { DatabaseHeader::SIZE } else { 0 };
        #[cfg(debug_assertions)]
        turso_assert_eq!(offset, root_contents.offset());

        tracing::debug!(
            "balance_root(root={}, rightmost={}, page_type={:?})",
            root.get().id,
            child.get().id,
            root_contents.page_type().ok()
        );

        turso_assert!(root.is_dirty(), "root must be marked dirty");
        turso_assert!(
            child.is_dirty(),
            "child must be marked dirty as freshly allocated page"
        );

        let root_buf = root_contents.as_ptr();
        let child_contents = child.get_contents();
        let child_buf = child_contents.as_ptr();
        let (root_pointer_start, root_pointer_len) =
            root_contents.cell_pointer_array_offset_and_size();
        let (child_pointer_start, _) = child.get_contents().cell_pointer_array_offset_and_size();

        let top = root_contents.cell_content_area() as usize;

        // 1. Modify child
        // Copy pointers
        child_buf[child_pointer_start..child_pointer_start + root_pointer_len]
            .copy_from_slice(&root_buf[root_pointer_start..root_pointer_start + root_pointer_len]);
        // Copy cell contents
        child_buf[top..].copy_from_slice(&root_buf[top..]);
        // Copy header
        child_buf[0..root_contents.header_size()]
            .copy_from_slice(&root_buf[offset..offset + root_contents.header_size()]);
        // Copy overflow cells
        std::mem::swap(
            &mut child_contents.overflow_cells,
            &mut root_contents.overflow_cells,
        );
        root_contents.overflow_cells.clear();

        // 2. Modify root
        let new_root_page_type = match root_contents.page_type()? {
            PageType::IndexLeaf => PageType::IndexInterior,
            PageType::TableLeaf => PageType::TableInterior,
            other => other,
        } as u8;
        // set new page type
        root_contents.write_page_type(new_root_page_type);
        root_contents.write_rightmost_ptr(child.get().id as u32);
        root_contents.write_cell_content_area(self.usable_space());
        root_contents.write_cell_count(0);
        root_contents.write_first_freeblock(0);

        root_contents.write_fragmented_bytes_count(0);
        root_contents.overflow_cells.clear();
        self.root_page = root.get().id as i64;
        self.stack.clear();
        self.stack.push(root);
        self.stack.set_cell_index(0); // leave parent pointing at the rightmost pointer (in this case 0, as there are no cells), since we will be balancing the rightmost child page.
        self.stack.push(child);
        Ok(IOResult::Done(()))
    }

    #[inline(always)]
    /// Returns the usable space of the current page (which is computed as: page_size - reserved_bytes).
    /// This is cached to avoid calling `pager.usable_space()` in a hot loop.
    fn usable_space(&self) -> usize {
        self.usable_space_cached
    }

    /// Clear the overflow pages linked to a specific page provided by the leaf cell
    /// Uses a state machine to keep track of it's operations so that traversal can be
    /// resumed from last point after IO interruption
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn clear_overflow_pages(&mut self, cell: &BTreeCell) -> IOResultOr<()> {
        // `database_size` is invariant for the duration of this invocation, so
        // read the page-1 header at most once and reuse it for every overflow
        // page validation below instead of re-reading it per `ReadNext`.
        let mut database_size: Option<u32> = None;
        loop {
            match self.overflow_state.clone() {
                OverflowState::Start => {
                    let first_overflow_page = match cell {
                        BTreeCell::TableLeafCell(leaf_cell) => leaf_cell.first_overflow_page,
                        BTreeCell::IndexLeafCell(leaf_cell) => leaf_cell.first_overflow_page,
                        BTreeCell::IndexInteriorCell(interior_cell) => {
                            interior_cell.first_overflow_page
                        }
                        BTreeCell::TableInteriorCell(_) => return Ok(IOResult::Done(())), // No overflow pages
                    };

                    if let Some(next_page) = first_overflow_page {
                        let database_size =
                            return_if_io!(self.overflow_database_size(&mut database_size));
                        if unlikely(!Self::valid_overflow_page_id(next_page, database_size)) {
                            self.overflow_state = OverflowState::Start;
                            return Err(
                                LimboError::Corrupt("Invalid overflow page number".into()).into()
                            );
                        }
                        // No mutations precede this read in the Start branch,
                        // so a spill yield safely re-enters here.
                        let (page, c) = return_if_io!(self.read_page(next_page as i64));
                        self.overflow_state = OverflowState::ProcessPage { next_page: page };
                        if let Some(c) = c {
                            io_yield_one!(c);
                        }
                    } else {
                        self.overflow_state = OverflowState::Done;
                    }
                }
                OverflowState::ProcessPage { next_page: page } => {
                    turso_assert!(page.is_loaded(), "page should be loaded");

                    let contents = page.get_contents();
                    let next = contents.read_u32_no_offset(0);
                    let next_page_id = page.get().id;

                    return_if_io!(self.pager.free_page(Some(page), next_page_id));

                    // free_page returned `Done` — commit `next` to state
                    // BEFORE any fallible IO so re-entry cannot invoke
                    // `free_page` again on the now-freed page.
                    if next != 0 {
                        self.overflow_state = OverflowState::ReadNext { next };
                    } else {
                        self.overflow_state = OverflowState::Done;
                    }
                }
                OverflowState::ReadNext { next } => {
                    let database_size =
                        return_if_io!(self.overflow_database_size(&mut database_size));
                    if unlikely(!Self::valid_overflow_page_id(next, database_size)) {
                        self.overflow_state = OverflowState::Start;
                        return Err(
                            LimboError::Corrupt("Invalid overflow page number".into()).into()
                        );
                    }
                    let (page, c) = return_if_io!(self.pager.read_page(next as i64));
                    self.overflow_state = OverflowState::ProcessPage { next_page: page };
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                OverflowState::Done => {
                    self.overflow_state = OverflowState::Start;
                    return Ok(IOResult::Done(()));
                }
            };
        }
    }

    /// Read `database_size` from the page-1 header at most once per
    /// `clear_overflow_pages` invocation, memoizing it in `cache`.
    fn overflow_database_size(&self, cache: &mut Option<u32>) -> IOResultOr<u32> {
        if let Some(database_size) = cache {
            return Ok(IOResult::Done(*database_size));
        }
        let database_size =
            return_if_io!(self.pager.with_header(|header| header.database_size)).get();
        *cache = Some(database_size);
        Ok(IOResult::Done(database_size))
    }

    fn valid_overflow_page_id(page_id: u32, database_size: u32) -> bool {
        page_id >= 2 && page_id <= database_size
    }

    /// Deletes all contents of the B-tree by freeing all its pages in an iterative depth-first order.
    /// This ensures child pages are freed before their parents
    /// Uses a state machine to keep track of the operation to ensure IO doesn't cause repeated traversals
    ///
    /// Depending on the caller, the root page may either be freed as well or left allocated but emptied.
    ///
    /// # Example
    /// For a B-tree with this structure (where 4' is an overflow page):
    /// ```text
    ///            1 (root)
    ///           /        \
    ///          2          3
    ///        /   \      /   \
    /// 4' <- 4     5    6     7
    /// ```
    ///
    /// The destruction order would be: [4',4,5,2,6,7,3,1]
    fn destroy_btree_contents(&mut self, keep_root: bool) -> IOResultOr<Option<usize>> {
        if let CursorState::None = &self.state {
            let c = return_if_io!(self.move_to_root_nonblock());
            self.state = CursorState::Destroy(DestroyInfo {
                state: DestroyState::Start,
            });
            if let Some(c) = c {
                io_yield_one!(c);
            }
        }

        loop {
            let destroy_state = {
                let destroy_info = self
                    .state
                    .destroy_info()
                    .expect("unable to get a mut reference to destroy state in cursor");
                destroy_info.state.clone()
            };

            match destroy_state {
                DestroyState::Start => {
                    let destroy_info = self
                        .state
                        .mut_destroy_info()
                        .expect("unable to get a mut reference to destroy state in cursor");
                    destroy_info.state = DestroyState::LoadPage;
                }
                DestroyState::LoadPage => {
                    let _page = self.stack.top_ref();

                    let destroy_info = self
                        .state
                        .mut_destroy_info()
                        .expect("unable to get a mut reference to destroy state in cursor");
                    destroy_info.state = DestroyState::ProcessPage;
                }
                DestroyState::ProcessPage => {
                    self.stack.advance();
                    let page = self.stack.top_ref();
                    let contents = page.get_contents();
                    let cell_idx = self.stack.current_cell_index();

                    //  If we've processed all cells in this page, figure out what to do with this page
                    if cell_idx >= contents.cell_count() as i32 {
                        match (contents.is_leaf(), cell_idx) {
                            //  Leaf pages with all cells processed
                            (true, n) if n >= contents.cell_count() as i32 => {
                                let destroy_info = self.state.mut_destroy_info().expect(
                                    "unable to get a mut reference to destroy state in cursor",
                                );
                                destroy_info.state = DestroyState::FreePage;
                                continue;
                            }
                            //  Non-leaf page which has processed all children but not it's potential right child
                            (false, n) if n == contents.cell_count() as i32 => {
                                if let Some(rightmost) = contents.rightmost_pointer()? {
                                    // Spill yield here would re-enter
                                    // `ProcessPage` and re-fire the loop-top
                                    // `stack.advance()`. Route through
                                    // `PendingDescent` so re-entry resumes
                                    // there.
                                    match self.pager.read_page(rightmost as i64)? {
                                        IOResult::Done((rightmost_page, c)) => {
                                            self.stack.push(rightmost_page);
                                            let destroy_info =
                                                self.state.mut_destroy_info().expect(
                                                    "unable to get a mut reference to destroy state in cursor",
                                                );
                                            destroy_info.state = DestroyState::LoadPage;
                                            if let Some(c) = c {
                                                io_yield_one!(c);
                                            }
                                        }
                                        IOResult::IO(IOCompletions(spill_c)) => {
                                            let destroy_info =
                                                self.state.mut_destroy_info().expect(
                                                    "unable to get a mut reference to destroy state in cursor",
                                                );
                                            destroy_info.state = DestroyState::PendingDescent {
                                                target: rightmost as i64,
                                            };
                                            io_yield_one!(spill_c);
                                        }
                                    }
                                } else {
                                    let destroy_info = self.state.mut_destroy_info().expect(
                                        "unable to get a mut reference to destroy state in cursor",
                                    );
                                    destroy_info.state = DestroyState::FreePage;
                                }
                                continue;
                            }
                            //  Non-leaf page which has processed all children and it's right child
                            (false, n) if n > contents.cell_count() as i32 => {
                                let destroy_info = self.state.mut_destroy_info().expect(
                                    "unable to get a mut reference to destroy state in cursor",
                                );
                                destroy_info.state = DestroyState::FreePage;
                                continue;
                            }
                            _ => unreachable!("Invalid cell idx state"),
                        }
                    }

                    //  We have not yet processed all cells in this page
                    //  Get the current cell
                    let cell = contents.cell_get(cell_idx as usize, self.usable_space())?;

                    match contents.is_leaf() {
                        //  For a leaf cell, clear the overflow pages associated with this cell
                        true => {
                            let destroy_info = self
                                .state
                                .mut_destroy_info()
                                .expect("unable to get a mut reference to destroy state in cursor");
                            destroy_info.state = DestroyState::ClearOverflowPages { cell };
                            continue;
                        }
                        //  For interior cells, check the type of cell to determine what to do
                        false => match &cell {
                            //  For index interior cells, remove the overflow pages
                            BTreeCell::IndexInteriorCell(_) => {
                                let destroy_info = self.state.mut_destroy_info().expect(
                                    "unable to get a mut reference to destroy state in cursor",
                                );
                                destroy_info.state = DestroyState::ClearOverflowPages { cell };
                                continue;
                            }
                            //  For all other interior cells, load the left child page
                            _ => {
                                let child_page_id = match &cell {
                                    BTreeCell::TableInteriorCell(cell) => cell.left_child_page,
                                    BTreeCell::IndexInteriorCell(cell) => cell.left_child_page,
                                    _ => panic!("expected interior cell"),
                                };
                                // Spill yield routed through `PendingDescent`
                                // — see the rightmost branch comment above.
                                match self.pager.read_page(child_page_id as i64)? {
                                    IOResult::Done((child_page, c)) => {
                                        self.stack.push(child_page);
                                        let destroy_info =
                                            self.state.mut_destroy_info().expect(
                                                "unable to get a mut reference to destroy state in cursor",
                                            );
                                        destroy_info.state = DestroyState::LoadPage;
                                        if let Some(c) = c {
                                            io_yield_one!(c);
                                        }
                                    }
                                    IOResult::IO(IOCompletions(spill_c)) => {
                                        let destroy_info =
                                            self.state.mut_destroy_info().expect(
                                                "unable to get a mut reference to destroy state in cursor",
                                            );
                                        destroy_info.state = DestroyState::PendingDescent {
                                            target: child_page_id as i64,
                                        };
                                        io_yield_one!(spill_c);
                                    }
                                }
                            }
                        },
                    }
                }
                DestroyState::ClearOverflowPages { cell } => {
                    return_if_io!(self.clear_overflow_pages(&cell));
                    match cell {
                        //  For an index interior cell, clear the left child page now that overflow pages have been cleared
                        BTreeCell::IndexInteriorCell(index_int_cell) => {
                            // `clear_overflow_pages` has returned `Done` and
                            // reset its internal state to `Start`. Re-entry
                            // into `ClearOverflowPages` would re-run it
                            // against the same (already-cleared) cell, so
                            // route a spill yield from the read through
                            // `PendingDescent` to skip back into the descent
                            // path.
                            let target = index_int_cell.left_child_page as i64;
                            match self.pager.read_page(target)? {
                                IOResult::Done((child_page, c)) => {
                                    self.stack.push(child_page);
                                    let destroy_info = self.state.mut_destroy_info().expect(
                                        "unable to get a mut reference to destroy state in cursor",
                                    );
                                    destroy_info.state = DestroyState::LoadPage;
                                    if let Some(c) = c {
                                        io_yield_one!(c);
                                    }
                                }
                                IOResult::IO(IOCompletions(spill_c)) => {
                                    let destroy_info = self.state.mut_destroy_info().expect(
                                        "unable to get a mut reference to destroy state in cursor",
                                    );
                                    destroy_info.state = DestroyState::PendingDescent { target };
                                    io_yield_one!(spill_c);
                                }
                            }
                        }
                        //  For any leaf cell, advance the index now that overflow pages have been cleared
                        BTreeCell::TableLeafCell(_) | BTreeCell::IndexLeafCell(_) => {
                            let destroy_info = self
                                .state
                                .mut_destroy_info()
                                .expect("unable to get a mut reference to destroy state in cursor");
                            destroy_info.state = DestroyState::LoadPage;
                        }
                        _ => panic!("unexpected cell type"),
                    }
                }
                DestroyState::PendingDescent { target } => {
                    let (child_page, c) = return_if_io!(self.pager.read_page(target));
                    self.stack.push(child_page);
                    let destroy_info = self
                        .state
                        .mut_destroy_info()
                        .expect("unable to get a mut reference to destroy state in cursor");
                    destroy_info.state = DestroyState::LoadPage;
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                DestroyState::FreePage => {
                    let page = self.stack.top();
                    let page_id = page.get().id;

                    if self.stack.has_parent() {
                        return_if_io!(self.pager.free_page(Some(page), page_id));

                        self.stack.pop();
                        let destroy_info = self
                            .state
                            .mut_destroy_info()
                            .expect("unable to get a mut reference to destroy state in cursor");
                        destroy_info.state = DestroyState::ProcessPage;
                    } else {
                        if keep_root {
                            self.clear_root(&page)?;
                        } else {
                            return_if_io!(self.pager.free_page(Some(page), page_id));
                        }

                        self.state = CursorState::None;
                        //  TODO: For now, no-op the result return None always. This will change once [AUTO_VACUUM](https://www.sqlite.org/lang_vacuum.html) is introduced
                        //  At that point, the last root page(call this x) will be moved into the position of the root page of this table and the value returned will be x
                        return Ok(IOResult::Done(None));
                    }
                }
            }
        }
    }

    fn clear_root(&mut self, root_page: &PageRef) -> Result<()> {
        let contents = root_page.get_contents();

        let page_type = match contents.page_type()? {
            PageType::TableLeaf | PageType::TableInterior => PageType::TableLeaf,
            PageType::IndexLeaf | PageType::IndexInterior => PageType::IndexLeaf,
        };

        self.pager.add_dirty(root_page)?;
        btree_init_page(root_page, page_type, 0, self.pager.usable_space());
        Ok(())
    }

    /// True iff the cursor sits cleanly on a row: valid, positioned, and not mid-way
    /// through some other operation's state machine.
    fn blob_position_is_live(&self) -> bool {
        self.valid_state == CursorValidState::Valid
            && self.has_record
            && matches!(self.state, CursorState::None)
            && self.stack.current_page >= 0
    }

    /// Gate for every incremental-blob entry point: ensure the cursor owns a live
    /// position on its row before any byte-level access.
    ///
    /// A peer write to the same table saves this cursor's position first
    /// (drive_pending_peer_save), and the same pass expires the handle if the write
    /// hit the pinned row (note_external_row_write) — SQLite's
    /// invalidateIncrblobCursors. An expired handle fails here with
    /// [`LimboError::BlobHandleExpired`] (SQLITE_ABORT at the C API), permanently. A
    /// position merely disturbed by a write to a *different* row is restored by
    /// re-seeking the pinned rowid, exactly like SQLite's restoreCursorPosition for
    /// incrblob cursors; if the row cannot be found again the handle expires too.
    fn blob_ensure_position(&mut self) -> IOResultOr<()> {
        if self.blob_expired {
            return Err(LimboError::BlobHandleExpired.into());
        }
        if self.blob_position_is_live() {
            return Ok(IOResult::Done(()));
        }
        if self.needs_restore() {
            return_if_io!(self.restore_context());
        }
        if !self.blob_position_is_live() {
            self.blob_expired = true;
            return Err(LimboError::BlobHandleExpired.into());
        }
        Ok(IOResult::Done(()))
    }

    /// Populate the cell-layout tier of `blob_cache` for the cursor's current
    /// table-leaf cell, unless it is already cached for this (page, cell), and pin
    /// the cell's rowid for expiry tracking. Restoring a disturbed position may
    /// yield IO; the layout parse itself reads only the resident leaf page.
    fn blob_ensure_layout(&mut self) -> IOResultOr<()> {
        return_if_io!(self.blob_ensure_position());
        let usable = self.pager.usable_space();
        turso_assert!(usable > 4, "usable space must exceed overflow header");
        let cell_idx = self.stack.current_cell_index();
        if cell_idx < 0 {
            return Err(LimboError::BlobHandleExpired.into());
        }
        let cell_idx = cell_idx as usize;
        let leaf_id = self.stack.top_ref().get().id;
        if self.blob_cache.valid
            && self.blob_cache.leaf_id == leaf_id
            && self.blob_cache.cell_idx == cell_idx
        {
            return Ok(IOResult::Done(()));
        }
        // Extract owned scalars from the cell before mutating the cache (the cell borrows
        // the page, which borrows `self`).
        let (rowid, local_off, local_len, payload_size, first_overflow) = {
            let page = self.stack.top_ref();
            let contents = page.get_contents();
            let base = contents.as_ptr().as_ptr() as usize;
            let BTreeCell::TableLeafCell(cell) = contents.cell_get(cell_idx, usable)? else {
                return Err(LimboError::InternalError(
                    "incremental blob I/O requires a table (rowid) row".to_string(),
                )
                .into());
            };
            (
                cell.rowid,
                cell.payload.as_ptr() as usize - base,
                cell.payload.len(),
                cell.payload_size as usize,
                cell.first_overflow_page,
            )
        };
        if local_len > payload_size {
            return Err(LimboError::Corrupt(format!(
                "cell claims {payload_size} payload bytes but holds {local_len} locally"
            ))
            .into());
        }
        let c = &mut self.blob_cache;
        c.valid = false;
        c.leaf_id = leaf_id;
        c.cell_idx = cell_idx;
        c.local_off = local_off;
        c.local_len = local_len;
        c.payload_size = payload_size;
        c.per = usable - 4;
        c.first_overflow = first_overflow;
        c.overflow_pages.clear();
        if let Some(fo) = first_overflow {
            crate::with_btree_allocation_site!(OverflowRead, c.overflow_pages.try_push(fo))?;
        }
        self.blob_pinned_rowid = Some(rowid);
        c.valid = true;
        // Moving to a different cell: unpin the previous cell's cached overflow page.
        c.release_pinned_overflow();
        c.col_valid = false;
        Ok(IOResult::Done(()))
    }

    /// Fetch the `idx`-th overflow page, reusing the pinned last page on a spatial-
    /// locality hit and otherwise going through the O(1) overflow cache + pager, then
    /// pinning the result for the next access.
    fn blob_overflow_page(&mut self, idx: usize) -> IOResultOr<PageRef> {
        if self.blob_cache.last_ov_idx == idx {
            if let Some(p) = &self.blob_cache.last_ov_page {
                // Pinned while cached, so the pager can't evict it and take its
                // buffer; the asserts guard that (as_ptr would panic otherwise —
                // crash over corrupt).
                turso_debug_assert!(p.is_pinned(), "cached blob overflow page must be pinned");
                turso_debug_assert!(p.is_loaded(), "pinned blob overflow page must stay loaded");
                return Ok(IOResult::Done(p.to_page()));
            }
        }
        return_if_io!(self.ensure_overflow_cached(idx));
        let pageno = *self.blob_cache.overflow_pages.get(idx).ok_or_else(|| {
            LimboError::Corrupt(format!(
                "overflow page {idx} requested but chain holds only {}",
                self.blob_cache.overflow_pages.len()
            ))
        })?;
        let (page, c) = return_if_io!(self.read_page(pageno as i64));
        if let Some(c) = c {
            io_yield_one!(c);
        }
        self.blob_cache.pin_overflow_page(idx, page.clone());
        Ok(IOResult::Done(page))
    }

    /// Populate the column tier of `blob_cache` for `column`, unless already cached.
    /// Parses the record header to find the column value's payload-relative byte
    /// offset, length, and serial type. The header usually sits in the local payload
    /// (no IO), but a wide row on a small page can spill it into the overflow chain,
    /// in which case just the header bytes are streamed in (with IO yields).
    fn blob_ensure_column(&mut self, column: usize) -> IOResultOr<()> {
        return_if_io!(self.blob_ensure_layout());
        if self.blob_cache.col_valid && self.blob_cache.col == column {
            return Ok(IOResult::Done(()));
        }
        let local_off = self.blob_cache.local_off;
        let local_len = self.blob_cache.local_len;
        let payload_size = self.blob_cache.payload_size;
        // The header-size varint itself is always local: either the record fits
        // entirely in the local payload, or SQLite's minimum-local formula keeps
        // well over 9 bytes (one max-size varint) on the leaf. A truncated varint
        // here therefore means a corrupt cell, and read_varint reports it as such.
        let (header_size, hpos0) = {
            let page = self.stack.top_ref();
            let contents = page.get_contents();
            let local = &contents.as_ptr()[local_off..local_off + local_len];
            crate::storage::sqlite3_ondisk::read_varint(local)?
        };
        let header_size = usize::try_from(header_size)
            .map_err(|_| LimboError::Corrupt("record header size does not fit".to_string()))?;
        if header_size < hpos0 || header_size > payload_size || header_size > MAX_RECORD_HEADER_SIZE
        {
            return Err(LimboError::Corrupt(format!(
                "record header size {header_size} out of bounds (payload {payload_size})"
            ))
            .into());
        }
        let (body_off, len, serial) = if header_size <= local_len {
            let page = self.stack.top_ref();
            let contents = page.get_contents();
            let local = &contents.as_ptr()[local_off..local_off + local_len];
            blob_locate_column_in_header(&local[..header_size], hpos0, payload_size, column)?
        } else {
            // Header spills into the overflow chain: materialize exactly the header
            // bytes (bounded above by MAX_RECORD_HEADER_SIZE) and parse those. On an
            // IO yield the whole function restarts; every step up to here is a pure
            // cached read, so the restart is cheap and idempotent.
            let mut header = crate::with_btree_allocation_site!(
                BlobRecordHeader,
                crate::alloc::try_vec![0u8; header_size]
            )?;
            return_if_io!(self.blob_read_range(0, &mut header));
            blob_locate_column_in_header(&header, hpos0, payload_size, column)?
        };
        let c = &mut self.blob_cache;
        c.col = column;
        c.col_body_off = body_off;
        c.col_len = len;
        c.col_serial = serial;
        c.col_valid = true;
        Ok(IOResult::Done(()))
    }

    /// Error unless the cached column value is byte-addressable, i.e. stored as TEXT
    /// or BLOB. SQLite refuses incremental I/O on NULL/integer/real values because
    /// their on-disk bytes are an encoding, not the value: "writing" into them would
    /// silently manufacture a different number. Callers must run
    /// [`Self::blob_ensure_column`] first.
    fn blob_require_text_or_blob(&self) -> Result<()> {
        turso_assert!(
            self.blob_cache.col_valid,
            "column tier must be cached before the type check"
        );
        let serial = self.blob_cache.col_serial;
        if serial >= 12 {
            return Ok(());
        }
        let type_name = match serial {
            0 => "null",
            7 => "real",
            _ => "integer",
        };
        Err(LimboError::InternalError(format!(
            "cannot open value of type {type_name}"
        )))
    }

    /// Ensure `blob_cache.overflow_pages` holds at least `upto + 1` entries, extending
    /// it by walking the chain from the last cached page. Turso's runtime `aOverflow`:
    /// the first access pays the O(n) walk, after which any offset is located in O(1).
    ///
    /// Idempotent and IO-reentrant: the array lives on the cursor and grows append-only,
    /// so on a yield the operation restarts and this resumes where it left off.
    fn ensure_overflow_cached(&mut self, upto: usize) -> IOResultOr<()> {
        loop {
            let last = {
                let pages = &self.blob_cache.overflow_pages;
                if pages.len() > upto {
                    return Ok(IOResult::Done(()));
                }
                match pages.last() {
                    Some(p) => *p,
                    None => {
                        return Err(LimboError::Corrupt(
                            "blob value needs overflow pages but has none".to_string(),
                        )
                        .into())
                    }
                }
            };
            let (page, c) = return_if_io!(self.read_page(last as i64));
            if let Some(c) = c {
                io_yield_one!(c);
            }
            let next = page.get_contents().read_u32_no_offset(0);
            if next == 0 {
                // The loop only reaches here while the chain is still shorter than the
                // requested index, so a terminated chain means the record header
                // promises more payload than the pages can hold.
                return Err(LimboError::Corrupt(format!(
                    "overflow chain ends after {} pages but the record needs at least {}",
                    self.blob_cache.overflow_pages.len(),
                    upto + 1
                ))
                .into());
            }
            crate::with_btree_allocation_site!(
                OverflowRead,
                self.blob_cache.overflow_pages.try_push(next)
            )?;
        }
    }

    /// Map payload offset `pos` (0 = first payload byte) to the physical span that
    /// begins there, yielding at most `remaining` bytes. Pure arithmetic over the
    /// cached cell layout; the caller fetches the page and copies. Requires
    /// `blob_ensure_layout` to have populated the layout tier.
    fn blob_span(&self, pos: usize, remaining: usize) -> BlobSpan {
        // Overflow spans divide by `per` (bytes per overflow page); it is `usable - 4`,
        // established > 0 by blob_ensure_layout. Assert it so the invariant is explicit.
        turso_debug_assert!(
            self.blob_cache.per > 0,
            "overflow bytes-per-page must be > 0"
        );
        let local_len = self.blob_cache.local_len;
        if pos < local_len {
            let take = (local_len - pos).min(remaining);
            BlobSpan::Local {
                page_off: self.blob_cache.local_off + pos,
                take,
            }
        } else {
            let per = self.blob_cache.per;
            let ov = pos - local_len;
            let within = ov % per;
            let take = (per - within).min(remaining);
            BlobSpan::Overflow {
                idx: ov / per,
                within,
                take,
            }
        }
    }

    /// Copy `buf.len()` bytes starting at payload offset `off` (0 = first byte of the
    /// value) into `buf`, spanning the leaf-local payload and the overflow chain. Uses
    /// the O(1) overflow cache to locate each page. Requires `blob_ensure_layout` first.
    fn blob_read_range(&mut self, off: usize, buf: &mut [u8]) -> IOResultOr<()> {
        let len = buf.len();
        let mut done = 0usize;
        while done < len {
            match self.blob_span(off + done, len - done) {
                // Local bytes are on the resident leaf page — no IO.
                BlobSpan::Local { page_off, take } => {
                    let src = self.stack.top_ref().get_contents().as_ptr();
                    buf[done..done + take].copy_from_slice(&src[page_off..page_off + take]);
                    done += take;
                }
                BlobSpan::Overflow { idx, within, take } => {
                    let page = return_if_io!(self.blob_overflow_page(idx));
                    let src = page.get_contents().as_ptr();
                    buf[done..done + take].copy_from_slice(&src[4 + within..4 + within + take]);
                    done += take;
                }
            }
        }
        Ok(IOResult::Done(()))
    }

    /// Write `data` starting at payload offset `off` in place, spanning the leaf-local
    /// payload and the overflow chain. Each mutated page is registered dirty so the
    /// write is journaled. Requires `blob_ensure_layout` first.
    fn blob_write_range(&mut self, off: usize, data: &[u8]) -> IOResultOr<()> {
        let len = data.len();
        let mut done = 0usize;
        while done < len {
            match self.blob_span(off + done, len - done) {
                BlobSpan::Local { page_off, take } => {
                    let page = self.stack.top_ref().clone();
                    self.pager.add_dirty(&page)?;
                    let dst = page.get_contents().as_ptr();
                    dst[page_off..page_off + take].copy_from_slice(&data[done..done + take]);
                    done += take;
                }
                BlobSpan::Overflow { idx, within, take } => {
                    let page = return_if_io!(self.blob_overflow_page(idx));
                    self.pager.add_dirty(&page)?;
                    let dst = page.get_contents().as_ptr();
                    dst[4 + within..4 + within + take].copy_from_slice(&data[done..done + take]);
                    done += take;
                }
            }
        }
        Ok(IOResult::Done(()))
    }

    /// Shared entry validation for every incremental-blob column access: bring the
    /// column tier of the cache up to date and confirm the value is byte-addressable
    /// (TEXT or BLOB). Reads/writes/length all funnel through here so the type rule
    /// is enforced in exactly one place.
    fn blob_typed_column(&mut self, column: usize) -> IOResultOr<()> {
        return_if_io!(self.blob_ensure_column(column));
        self.blob_require_text_or_blob()?;
        Ok(IOResult::Done(()))
    }

    /// Validate the byte range `[off, off+len)` against column `column`'s value and
    /// return its payload-relative start offset (for `blob_read_range` /
    /// `blob_write_range`). One bounds check backs both read and write.
    fn blob_resolve_range(&mut self, column: usize, off: usize, len: usize) -> IOResultOr<usize> {
        return_if_io!(self.blob_typed_column(column));
        if off.saturating_add(len) > self.blob_cache.col_len {
            return Err(LimboError::InternalError(
                "blob access past end of column value".to_string(),
            )
            .into());
        }
        Ok(IOResult::Done(self.blob_cache.col_body_off + off))
    }

    /// Total byte length of column `column`'s value in the current row, validating
    /// that the value is byte-addressable (TEXT or BLOB). This backs
    /// `sqlite3_blob_open`, so the type error surfaces at open time.
    pub fn blob_column_len_inherent(&mut self, column: usize) -> IOResultOr<usize> {
        return_if_io!(self.blob_typed_column(column));
        Ok(IOResult::Done(self.blob_cache.col_len))
    }

    /// Read `len` bytes at `off` within column `column`'s value into `out`.
    ///
    /// Reentrant: on an IO yield it restarts from the beginning. Every step is a pure
    /// read and therefore idempotent, so no per-call state is saved — a restart simply
    /// re-reads (now-cached) pages and reproduces the same output.
    pub fn blob_read_column_inherent(
        &mut self,
        column: usize,
        off: usize,
        len: usize,
        out: &mut crate::ValueBlob,
    ) -> IOResultOr<()> {
        let payload_off = return_if_io!(self.blob_resolve_range(column, off, len));
        crate::with_btree_allocation_site!(
            OverflowRead,
            out.try_reserve(len.saturating_sub(out.len()))
        )?;
        out.clear();
        if len == 0 {
            return Ok(IOResult::Done(()));
        }
        out.resize(len, 0);
        return_if_io!(self.blob_read_range(payload_off, out));
        Ok(IOResult::Done(()))
    }

    /// Write `data` at `off` within column `column`'s value, in place across the
    /// local page and overflow chain; the value's size cannot change. Each mutated
    /// page is registered dirty so the write is journaled and persisted.
    ///
    /// Reentrant: on an IO yield it restarts from the beginning. Every write is
    /// idempotent (the same bytes to the same offsets), so a restart re-applies
    /// already-written bytes harmlessly.
    pub fn blob_write_column_inherent(
        &mut self,
        column: usize,
        off: usize,
        data: &[u8],
    ) -> IOResultOr<()> {
        let payload_off = return_if_io!(self.blob_resolve_range(column, off, data.len()));
        if data.is_empty() {
            return Ok(IOResult::Done(()));
        }
        return_if_io!(self.blob_write_range(payload_off, data));
        Ok(IOResult::Done(()))
    }

    pub fn overwrite_cell(
        &mut self,
        page: &PageRef,
        cell_idx: usize,
        record: &ImmutableRecordRef<'_>,
        state: &mut OverwriteCellState,
    ) -> IOResultOr<()> {
        loop {
            turso_assert!(page.is_loaded(), "page is not loaded", { "page_id": page.get().id });
            match state {
                OverwriteCellState::AllocatePayload => {
                    let serial_types_len = record.column_count();
                    // Reuse the cell payload buffer to avoid allocations
                    let mut new_payload = take_vec(&mut self.reusable_cell_payload);
                    new_payload.clear();
                    if new_payload.capacity() < serial_types_len {
                        crate::with_btree_allocation_site!(
                            CellPayload,
                            new_payload.try_reserve(serial_types_len - new_payload.capacity())
                        )?;
                    }
                    let rowid = return_if_io!(self.rowid());
                    *state = OverwriteCellState::FillPayload {
                        new_payload,
                        rowid,
                        fill_cell_payload_state: FillCellPayloadState::Start,
                    };
                    continue;
                }
                OverwriteCellState::FillPayload {
                    new_payload,
                    rowid,
                    fill_cell_payload_state,
                } => {
                    {
                        return_if_io!(fill_cell_payload(
                            &PinGuard::new(page.clone()),
                            *rowid,
                            new_payload,
                            cell_idx,
                            record,
                            self.usable_space(),
                            self.pager.clone(),
                            fill_cell_payload_state,
                        ));
                    }
                    // figure out old cell offset & size
                    let (old_offset, old_local_size) = {
                        let contents = page.get_contents();
                        contents.cell_get_raw_region(cell_idx, self.usable_space())?
                    };

                    *state = OverwriteCellState::ClearOverflowPagesAndOverwrite {
                        new_payload: take_vec(new_payload),
                        old_offset,
                        old_local_size,
                    };
                    continue;
                }
                OverwriteCellState::ClearOverflowPagesAndOverwrite {
                    new_payload,
                    old_offset,
                    old_local_size,
                } => {
                    let contents = page.get_contents();
                    let cell = contents.cell_get(cell_idx, self.usable_space())?;
                    return_if_io!(self.clear_overflow_pages(&cell));

                    // if it all fits in local space and old_local_size is enough, do an in-place overwrite
                    if new_payload.len() == *old_local_size {
                        Self::overwrite_content(page, *old_offset, new_payload)?;
                        // Recover the reusable buffer
                        self.reusable_cell_payload = take_vec(new_payload);
                        return Ok(IOResult::Done(()));
                    }

                    drop_cell(contents, cell_idx, self.usable_space())?;
                    insert_into_cell(contents, new_payload, cell_idx, self.usable_space())?;
                    // Recover the reusable buffer
                    self.reusable_cell_payload = take_vec(new_payload);
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    pub fn overwrite_content(page: &PageRef, dest_offset: usize, new_payload: &[u8]) -> Result<()> {
        turso_assert!(page.is_loaded(), "page should be loaded");
        let buf = page.get_contents().as_ptr();
        buf[dest_offset..dest_offset + new_payload.len()].copy_from_slice(new_payload);
        Ok(())
    }

    fn get_immutable_record_or_create(&mut self) -> Result<Option<&mut ImmutableRecord>> {
        let reusable_immutable_record = &mut self.reusable_immutable_record;
        if reusable_immutable_record.is_none() {
            let page_size = self.pager.get_page_size_unchecked().get();
            let record = crate::with_btree_allocation_site!(
                RecordPayload,
                ImmutableRecord::new(page_size as usize)
            )?;
            reusable_immutable_record.replace(record);
        }
        Ok(reusable_immutable_record.as_mut())
    }

    fn get_immutable_record(&self) -> Option<&ImmutableRecord> {
        self.reusable_immutable_record.as_ref()
    }

    pub fn is_write_in_progress(&self) -> bool {
        matches!(self.state, CursorState::Write(_))
    }

    /// True iff the cursor sits on a valid record that is NOT the first cell of
    /// its page. Used by the MVCC checkpoint's sequential-write optimization:
    /// only at such a position is "insert the next adjacent rowid here, without
    /// re-seeking" provably within the page's divider bounds (the previous,
    /// strictly smaller rowid is in this same page, and so is a strictly larger
    /// one). At cell 0 the cursor has just crossed a leaf boundary, and the
    /// adjacent rowid may belong on the LEFT side of the parent divider — a
    /// divider whose row was deleted keeps its key, so the divider can
    /// be >= the rowid being inserted — in which case the caller must re-seek
    /// from the root.
    pub fn is_positioned_past_page_start(&self) -> bool {
        self.has_record() && self.stack.current_cell_index() > 0
    }

    /// True iff the cursor has run off the last cell of the table's rightmost
    /// leaf: `next()` left it on that leaf with the cell index one past the
    /// last cell and no record, and no ancestor has a child to the right.
    ///
    /// That is the append slot. A key larger than every key in the table
    /// belongs there, and there is no right-hand divider that could put it
    /// anywhere else, so the MVCC checkpoint's sequential-write optimization
    /// can insert here without re-seeking from the root. Without this check
    /// every append pays a full root-to-leaf seek: `next()` after inserting
    /// the last cell always runs off the end.
    pub fn is_at_end_of_rightmost_leaf(&self) -> bool {
        if self.has_record()
            || self.valid_state != CursorValidState::Valid
            || self.stack.current_page < 0
        {
            return false;
        }
        let contents = self.stack.top_ref().get_contents();
        contents.is_leaf()
            && self.stack.current_cell_index() == contents.cell_count() as i32
            && !self.ancestor_pages_have_more_children()
    }

    /// Rowid of the table-leaf cell the cursor currently sits on, or `None` when the
    /// cursor is not cleanly positioned on a table leaf (index cursors, sentinel
    /// stacks, mid-operation states). Callers that use `None` must treat it as
    /// "unknown row" and act conservatively.
    fn current_table_leaf_rowid(&self) -> Option<i64> {
        if self.valid_state != CursorValidState::Valid || !self.has_record {
            return None;
        }
        if self.stack.current_page < 0
            || (self.stack.current_page as usize) >= self.stack.stack.len()
            || self.stack.stack[self.stack.current_page as usize].is_none()
        {
            return None;
        }
        let cell_idx = self.stack.current_cell_index();
        if cell_idx < 0 {
            return None;
        }
        let page = self.stack.top_ref();
        let contents = page.get_contents();
        if !matches!(contents.page_type(), Ok(PageType::TableLeaf)) {
            return None;
        }
        if cell_idx as usize >= contents.cell_count() {
            return None;
        }
        contents.cell_table_leaf_read_rowid(cell_idx as usize).ok()
    }

    /// saveAllCursors pass for this cursor's insert/delete entry. Iteration
    /// state lives in `pending_peer_save` so we can resume across IO yields
    /// from per-peer overflow-chain reads. `written_rowid` is the row about to be
    /// written (`None` when unknown); peers backing incremental blob handles use it
    /// to expire exactly when their own row is hit (sqlite3's
    /// invalidateIncrblobCursors, btree.c:672).
    fn drive_pending_peer_save(&mut self, written_rowid: Option<i64>) -> IOResultOr<()> {
        if self.pending_peer_save.is_none() && matches!(self.state, CursorState::None) {
            // BTCF_Multiple fast path (sqlite3 btree.c:9348).
            if !self.has_peers.load(crate::sync::atomic::Ordering::Relaxed) {
                return Ok(IOResult::Done(()));
            }
            let dyn_ref: &dyn CursorTrait = self;
            let peers = self.pager.snapshot_peers_for_root(dyn_ref);
            if peers.is_empty() {
                return Ok(IOResult::Done(()));
            }
            self.pending_peer_save = Some((peers, 0));
        }
        let Some((peers, idx)) = self.pending_peer_save.as_mut() else {
            return Ok(IOResult::Done(()));
        };
        while *idx < peers.len() {
            let peer = peers[*idx];
            // Idempotent, so safe to re-run for the same peer after an IO yield
            // below. SAFETY: see RegisteredCursor's invariant.
            unsafe { peer.as_mut().note_external_row_write(written_rowid) };
            // SAFETY: see RegisteredCursor's invariant.
            let outcome =
                unsafe { return_if_io!(peer.as_mut().try_save_position_for_external_balance()) };
            if outcome == SavePositionResult::MustInvalidate {
                // SAFETY: see RegisteredCursor's invariant.
                unsafe { peer.as_mut().invalidate_btree_cache() };
            }
            *idx += 1;
        }
        self.pending_peer_save = None;
        Ok(IOResult::Done(()))
    }

    // Save cursor context, to be restored later
    pub fn save_context(&mut self, cursor_context: CursorContext) {
        self.valid_state = CursorValidState::RequireSeek;
        self.context = Some(cursor_context);
        // The tree is about to change under this cursor (that is the only reason a
        // position ever gets saved), so cached payload offsets and overflow page
        // numbers must not survive: blob I/O through them would touch relocated or
        // freed pages. The next blob access re-seeks via restore_context and
        // re-parses the layout from scratch (see blob_ensure_position).
        self.blob_cache.reset();
    }

    /// Drop any pending saved seek-context; used by callers that re-navigate
    /// from the root and don't want restore_context to clobber them.
    #[inline]
    fn clear_saved_seek(&mut self) {
        self.context = None;
        self.valid_state = CursorValidState::Valid;
    }

    #[inline]
    fn needs_restore(&self) -> bool {
        self.context.is_some() && !matches!(self.valid_state, CursorValidState::Valid)
    }

    /// If context is defined, restore it and set it None on success. Parallels
    /// SQLite's btreeRestoreCursorPosition (btree.c:896). NotFound stays at
    /// Valid with has_record=false rather than transitioning to Invalid: a
    /// peer-deleted cursor is still recoverable via Rewind/Seek, whereas our
    /// Invalid is reserved for cursors that can never be repositioned.
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn restore_context(&mut self) -> IOResultOr<()> {
        if !self.needs_restore() {
            return Ok(IOResult::Done(()));
        }
        if let CursorValidState::RequireAdvance(direction) = self.valid_state {
            return_if_io!(match direction {
                // Avoid calling next()/prev() directly because they immediately call restore_context()
                IterationDirection::Forwards => self.get_next_record(),
                IterationDirection::Backwards => self.get_prev_record(),
            });
            self.context = None;
            self.valid_state = CursorValidState::Valid;
            return Ok(IOResult::Done(()));
        }
        let ctx = self.context.take().unwrap();
        let seek_key = match ctx.key {
            CursorContextKey::TableRowId(rowid) => SeekKey::TableRowId(rowid),
            CursorContextKey::IndexKeyRowId(ref record) => SeekKey::IndexKey(record.reborrow()),
        };
        let res = self.seek(seek_key, ctx.seek_op)?;
        match res {
            IOResult::Done(res) => {
                match res {
                    SeekResult::Found => {
                        self.valid_state = CursorValidState::Valid;
                        Ok(IOResult::Done(()))
                    }
                    SeekResult::TryAdvance => {
                        self.valid_state =
                            CursorValidState::RequireAdvance(ctx.seek_op.iteration_direction());
                        self.context = Some(ctx);
                        io_yield_one!(Completion::new_yield());
                    }
                    SeekResult::NotFound => {
                        // Saved row is gone (deleted by us, or by a peer).
                        // The seek positioned the stack at the next-greater
                        // cell, which is the correct iteration target for
                        // forward iteration after the deletion. Signal that
                        // via skip_advance so the next next() returns the
                        // landed cell instead of advancing past it — mirrors
                        // SQLite's CURSOR_SKIPNEXT (btree.c:915).
                        self.skip_advance = true;
                        self.valid_state = CursorValidState::Valid;
                        Ok(IOResult::Done(()))
                    }
                }
            }
            IOResult::IO(io) => {
                self.context = Some(ctx);
                Ok(IOResult::IO(io))
            }
        }
    }

    pub fn read_page_blocking(&self, page_idx: i64) -> Result<(PageRef, Option<Completion>)> {
        self.pager.io.block(|| self.pager.read_page(page_idx))
    }

    pub fn read_page(&self, page_idx: i64) -> IOResultOr<(PageRef, Option<Completion>)> {
        self.pager.read_page(page_idx)
    }

    pub fn allocate_page(&self, page_type: PageType, offset: usize) -> IOResultOr<PageRef> {
        self.pager
            .do_allocate_page(page_type, offset, BtreePageAllocMode::Any)
    }
}

#[cfg(any(test, injected_yields))]
impl ProvidesYieldContext for BTreeCursor {
    fn yield_context(&self) -> YieldContext {
        YieldContext::new(
            self.yield_injector.clone(),
            None,
            self.yield_instance_id,
            BTREE_WRITE_YIELD_FAMILY ^ self.root_page as u64,
        )
    }
}

impl BTreeCursor {
    fn clear_transient_overflow_cells(&mut self) {
        // Overflow cells are page-local scratch for the cursor's in-flight balance.
        // If the cursor is abandoned after queueing them, cached pages may outlive
        // the cursor and must not carry that scratch into later writes.
        if matches!(self.state, CursorState::None)
            && matches!(self.balance_state.sub_state, BalanceSubState::Start)
        {
            turso_assert!(
                self.balance_state.balance_info.is_none(),
                "idle cursor has balance info"
            );
            // No write or balance operation is in progress, so this cursor has no
            // transient overflow cells to clean up.
            return;
        }

        for page in self.stack.stack.iter().flatten() {
            page.get().overflow_cells.clear();
        }

        // Insert/overwrite can stage overflow cells before balance_info is populated.
        // If the cursor is dropped in that window, this page handle is the only owner
        // of that transient state.
        match &self.state {
            CursorState::Write(WriteState::Insert { page, .. })
            | CursorState::Write(WriteState::Overwrite { page, .. }) => {
                page.get().overflow_cells.clear();
            }
            CursorState::Write(WriteState::Start)
            | CursorState::Write(WriteState::Balancing)
            | CursorState::Write(WriteState::Finish)
            | CursorState::Destroy(_)
            | CursorState::Delete(_)
            | CursorState::None => {}
        }

        if let Some(balance_info) = &self.balance_state.balance_info {
            for page in balance_info.pages_to_balance.iter().flatten() {
                page.get().overflow_cells.clear();
            }
        }

        // Newly allocated/reused sibling pages are tracked only by BalanceContext until
        // non-root balancing finishes. If the cursor is dropped before then, clear any
        // overflow scratch from those pages explicitly.
        match &self.balance_state.sub_state {
            BalanceSubState::NonRootDoBalancingAllocate {
                context: Some(context),
                ..
            }
            | BalanceSubState::NonRootDoBalancingFinish { context } => {
                for page in context.pages_to_balance_new.iter().flatten() {
                    page.get().overflow_cells.clear();
                }
            }
            BalanceSubState::Start
            | BalanceSubState::BalanceRoot
            | BalanceSubState::Decide
            | BalanceSubState::Quick
            | BalanceSubState::NonRootPickSiblings
            | BalanceSubState::NonRootDoBalancing
            | BalanceSubState::NonRootDoBalancingAllocate { context: None, .. }
            | BalanceSubState::FreePages { .. } => {}
        }
    }
}

impl Drop for BTreeCursor {
    fn drop(&mut self) {
        self.clear_transient_overflow_cells();
        if !self
            .did_register
            .load(crate::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
        let dyn_ref: &dyn CursorTrait = self;
        self.pager.unregister_cursor(dyn_ref);
    }
}

impl CursorTrait for BTreeCursor {
    fn blob_read_column(
        &mut self,
        column: usize,
        off: usize,
        len: usize,
        out: &mut crate::ValueBlob,
    ) -> IOResultOr<()> {
        self.blob_read_column_inherent(column, off, len, out)
    }
    fn blob_write_column(&mut self, column: usize, off: usize, data: &[u8]) -> IOResultOr<()> {
        self.blob_write_column_inherent(column, off, data)
    }
    fn blob_column_len(&mut self, column: usize) -> IOResultOr<usize> {
        self.blob_column_len_inherent(column)
    }
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn next(&mut self) -> IOResultOr<()> {
        if self.can_advance_within_leaf() {
            self.stack.advance();
            self.invalidate_record();
            return Ok(IOResult::Done(()));
        }
        if self.valid_state == CursorValidState::Invalid {
            return Ok(IOResult::Done(()));
        }
        loop {
            match self.advance_state {
                AdvanceState::Start => {
                    return_if_io!(self.restore_context());
                    // Set by DeleteState::RestoreContextAfterBalancing and by
                    // restore_context on NotFound: the cursor is already at
                    // the right iteration target, so return it without
                    // advancing. If the landed cell has no record (past
                    // EOF), fall through to Advance.
                    if self.skip_advance {
                        self.skip_advance = false;
                        if self.stack.current_page >= 0 {
                            let mem_page = self.stack.top_ref();
                            let contents = mem_page.get_contents();
                            let cell_idx = self.stack.current_cell_index();
                            let cell_count = contents.cell_count();
                            let has_record = cell_idx >= 0 && cell_idx < cell_count as i32;
                            if has_record {
                                self.set_has_record(true);
                                self.read_overflow_state = None;
                                return Ok(IOResult::Done(()));
                            }
                        }
                    }
                    self.advance_state = AdvanceState::Advance;
                }
                AdvanceState::Advance => {
                    return_if_io!(self.get_next_record());
                    self.advance_state = AdvanceState::Start;
                    self.read_overflow_state = None;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn last(&mut self) -> IOResultOr<()> {
        self.set_null_flag(false);
        if self.valid_state == CursorValidState::Invalid {
            return Ok(IOResult::Done(()));
        }
        self.clear_saved_seek();
        let cursor_has_record = return_if_io!(self.move_to_rightmost());
        self.set_has_record(cursor_has_record);
        self.invalidate_record();
        self.read_overflow_state = None;
        Ok(IOResult::Done(()))
    }

    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn prev(&mut self) -> IOResultOr<()> {
        loop {
            match self.advance_state {
                AdvanceState::Start => {
                    return_if_io!(self.restore_context());
                    self.advance_state = AdvanceState::Advance;
                }
                AdvanceState::Advance => {
                    return_if_io!(self.get_prev_record());
                    self.advance_state = AdvanceState::Start;
                    self.read_overflow_state = None;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    fn rowid(&mut self) -> IOResultOr<Option<i64>> {
        if self.needs_restore() {
            return_if_io!(self.restore_context());
        }
        if self.get_null_flag() {
            return Ok(IOResult::Done(None));
        }
        if self.has_record() {
            let page = self.stack.top_ref();
            let contents = page.get_contents();
            let page_type = contents.page_type()?;
            if page_type.is_table() {
                let cell_idx = self.stack.current_cell_index();
                let rowid = contents.cell_table_leaf_read_rowid(cell_idx as usize)?;
                Ok(IOResult::Done(Some(rowid)))
            } else {
                let _ = return_if_io!(self.record());
                Ok(IOResult::Done(self.get_index_rowid_from_record()))
            }
        } else {
            Ok(IOResult::Done(None))
        }
    }

    #[cfg_attr(debug_assertions, instrument(skip(self, key), level = Level::DEBUG))]
    fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> IOResultOr<SeekResult> {
        self.skip_advance = false;
        // Empty trace to capture the span information
        tracing::trace!("");
        // We need to clear the null flag for the table cursor before seeking,
        // because it might have been set to false by an unmatched left-join row during the previous iteration
        // on the outer loop.
        self.set_null_flag(false);
        let seek_result = return_if_io!(self.do_seek(key, op));
        self.invalidate_record();
        // Reset seek state
        self.seek_state = CursorSeekState::Start;
        self.valid_state = CursorValidState::Valid;
        self.read_overflow_state = None;
        Ok(IOResult::Done(seek_result))
    }

    #[cfg_attr(debug_assertions, instrument(skip(self, registers), level = Level::DEBUG))]
    fn seek_unpacked(&mut self, registers: &[Register], op: SeekOp) -> IOResultOr<SeekResult> {
        self.skip_advance = false;
        // Empty trace to capture the span information
        tracing::trace!("");
        // We need to clear the null flag for the table cursor before seeking,
        // because it might have been set to false by an unmatched left-join row during the previous iteration
        // on the outer loop.
        self.set_null_flag(false);
        let seek_result = return_if_io!(self.do_seek_unpacked(registers, op));
        self.invalidate_record();
        // Reset seek state
        self.seek_state = CursorSeekState::Start;
        self.valid_state = CursorValidState::Valid;
        self.read_overflow_state = None;
        Ok(IOResult::Done(seek_result))
    }

    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    fn record(&mut self) -> IOResultOr<Option<&ImmutableRecord>> {
        // Mirrors sqlite3BtreeRestoreCursorPosition called at btree read
        // entry points (btree.c:5315, etc).
        if self.needs_restore() {
            return_if_io!(self.restore_context());
        }
        if !self.has_record() {
            return Ok(IOResult::Done(None));
        }
        let invalidated = self
            .reusable_immutable_record
            .as_ref()
            .is_none_or(|record| record.is_invalidated());
        if !invalidated {
            return Ok(IOResult::Done(self.reusable_immutable_record.as_ref()));
        }

        let page = self.stack.top_ref();
        let contents = page.get_contents();
        let cell_idx = self.stack.current_cell_index();
        let (payload, payload_size, first_overflow_page) =
            contents.cell_read_payload_ptr(cell_idx as usize, self.usable_space())?;
        if let Some(next_page) = first_overflow_page {
            return_if_io!(self.process_overflow_read(payload, next_page, payload_size))
        } else {
            self.get_immutable_record_or_create()?
                .as_mut()
                .unwrap()
                .invalidate();
            crate::with_btree_allocation_site!(
                RecordPayload,
                self.get_immutable_record_or_create()?
                    .as_mut()
                    .unwrap()
                    .start_serialization(payload)
            )?;
        };

        Ok(IOResult::Done(self.reusable_immutable_record.as_ref()))
    }

    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn insert(&mut self, key: &BTreeKey) -> IOResultOr<()> {
        tracing::debug!(valid_state = ?self.valid_state, cursor_state = ?self.state, is_write_in_progress = self.is_write_in_progress());
        // saveAllCursors at the head of sqlite3BtreeInsert (btree.c:9348).
        return_if_io!(self.drive_pending_peer_save(key.maybe_rowid()));
        return_if_io!(self.insert_into_page(key));
        self.invalidate_count_cache();
        if key.maybe_rowid().is_some() {
            self.set_has_record(true);
        }
        Ok(IOResult::Done(()))
    }

    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    /// Delete state machine flow:
    /// 1. Start -> check if the rowid to be delete is present in the page or not. If not we early return
    /// 2. DeterminePostBalancingSeekKey -> determine the key to seek to after balancing.
    /// 3. LoadPage -> load the page.
    /// 4. FindCell -> find the cell to be deleted in the page.
    /// 5. ClearOverflowPages -> Clear the overflow pages if there are any before dropping the cell, then if we are in a leaf page we just drop the cell in place.
    /// if we are in interior page, we need to rotate keys in order to replace current cell (InteriorNodeReplacement).
    /// 6. InteriorNodeReplacement -> we copy the left subtree leaf node into the deleted interior node's place.
    /// 7. Balancing -> perform balancing
    /// 8. PostInteriorNodeReplacement -> if an interior node was replaced, we need to advance the cursor once.
    /// 9. SeekAfterBalancing -> adjust the cursor to a node that is closer to the deleted value. go to Finish
    /// 10. Finish -> Delete operation is done. Return CursorResult(Ok())
    fn delete(&mut self) -> IOResultOr<()> {
        if let CursorState::None = &self.state {
            // saveAllCursors at the head of sqlite3BtreeDelete (btree.c:9841). The
            // cursor is positioned on the row being deleted, so its rowid tells peer
            // blob cursors whether the deletion hits their pinned row.
            let deleted_rowid = self.current_table_leaf_rowid();
            return_if_io!(self.drive_pending_peer_save(deleted_rowid));
            self.invalidate_count_cache();
            self.state = CursorState::Delete(DeleteState::Start);
        }

        loop {
            let usable_space = self.usable_space();
            let delete_state = match &mut self.state {
                CursorState::Delete(x) => x,
                _ => unreachable!("expected delete state"),
            };
            tracing::debug!(?delete_state);

            match delete_state {
                DeleteState::Start => {
                    let page = self.stack.top_ref();
                    self.pager.add_dirty(page)?;
                    if matches!(
                        page.get_contents().page_type()?,
                        PageType::TableLeaf | PageType::TableInterior
                    ) {
                        if return_if_io!(self.rowid()).is_none() {
                            self.state = CursorState::None;
                            return Ok(IOResult::Done(()));
                        }
                    } else if !self.has_record() {
                        self.state = CursorState::None;
                        return Ok(IOResult::Done(()));
                    }

                    self.state = CursorState::Delete(DeleteState::DeterminePostBalancingSeekKey);
                }

                DeleteState::DeterminePostBalancingSeekKey => {
                    // FIXME: skip this work if we determine deletion wont result in balancing
                    // Right now we calculate the key every time for simplicity/debugging
                    // since it won't affect correctness which is more important
                    let page = self.stack.top_ref();
                    let target_key = if page.is_index()? {
                        let record = match return_if_io!(self.record()) {
                            Some(record) => record.clone(),
                            None => unreachable!("there should've been a record"),
                        };
                        CursorContext {
                            key: CursorContextKey::IndexKeyRowId(
                                ImmutableRecordRef::from_owned_record(record),
                            ),
                            seek_op: SeekOp::GE { eq_only: true },
                        }
                    } else {
                        let Some(rowid) = return_if_io!(self.rowid()) else {
                            panic!("cursor should be pointing to a record with a rowid");
                        };
                        CursorContext {
                            key: CursorContextKey::TableRowId(rowid),
                            seek_op: SeekOp::GE { eq_only: true },
                        }
                    };

                    self.state = CursorState::Delete(DeleteState::LoadPage {
                        post_balancing_seek_key: Some(target_key),
                    });
                }

                DeleteState::LoadPage {
                    post_balancing_seek_key,
                } => {
                    self.state = CursorState::Delete(DeleteState::FindCell {
                        post_balancing_seek_key: post_balancing_seek_key.take(),
                    });
                }

                DeleteState::FindCell {
                    post_balancing_seek_key,
                } => {
                    let page = self.stack.top_ref();
                    let cell_idx = self.stack.current_cell_index() as usize;
                    let contents = page.get_contents();
                    if unlikely(cell_idx >= contents.cell_count()) {
                        return_corrupt!(
                            "Corrupted page: cell index {} is out of bounds for page with {} cells",
                            cell_idx,
                            contents.cell_count()
                        );
                    }

                    tracing::debug!(
                        "DeleteState::FindCell: page_id: {}, cell_idx: {}",
                        page.get().id,
                        cell_idx
                    );

                    let cell = contents.cell_get(cell_idx, usable_space)?;

                    let original_child_pointer = match &cell {
                        BTreeCell::TableInteriorCell(interior) => Some(interior.left_child_page),
                        BTreeCell::IndexInteriorCell(interior) => Some(interior.left_child_page),
                        _ => None,
                    };

                    self.state = CursorState::Delete(DeleteState::ClearOverflowPages {
                        cell_idx,
                        cell,
                        original_child_pointer,
                        post_balancing_seek_key: post_balancing_seek_key.take(),
                    });
                }

                DeleteState::ClearOverflowPages { cell, .. } => {
                    let cell = cell.clone();
                    return_if_io!(self.clear_overflow_pages(&cell));

                    let CursorState::Delete(DeleteState::ClearOverflowPages {
                        cell_idx,
                        original_child_pointer,
                        ref mut post_balancing_seek_key,
                        ..
                    }) = self.state
                    else {
                        unreachable!("expected clear overflow pages state");
                    };

                    let page = self.stack.top_ref();
                    let contents = page.get_contents();

                    if !contents.is_leaf() {
                        self.state = CursorState::Delete(DeleteState::InteriorNodeReplacement {
                            page: page.clone(),
                            btree_depth: self.stack.current(),
                            cell_idx,
                            original_child_pointer,
                            post_balancing_seek_key: post_balancing_seek_key.take(),
                        });
                    } else {
                        drop_cell(contents, cell_idx, usable_space)?;

                        self.state = CursorState::Delete(DeleteState::CheckNeedsBalancing {
                            btree_depth: self.stack.current(),
                            post_balancing_seek_key: post_balancing_seek_key.take(),
                            interior_node_was_replaced: false,
                        });
                    }
                }

                DeleteState::InteriorNodeReplacement { .. } => {
                    // This is an interior node, we need to handle deletion differently.
                    // 1. Move cursor to the largest key in the left subtree.
                    // 2. Replace the cell in the interior (parent) node with that key.
                    // 3. Delete that key from the child page.

                    // Step 1: Move cursor to the largest key in the left subtree.
                    // The largest key is always in a leaf, and so this traversal may involvegoing multiple pages downwards,
                    // so we store the page we are currently on.

                    // avoid calling prev() because it internally calls restore_context() which may cause unintended behavior.
                    return_if_io!(self.get_prev_record());

                    let CursorState::Delete(DeleteState::InteriorNodeReplacement {
                        ref page,
                        btree_depth,
                        cell_idx,
                        original_child_pointer,
                        ref mut post_balancing_seek_key,
                        ..
                    }) = self.state
                    else {
                        unreachable!("expected interior node replacement state");
                    };

                    // Ensure we keep the parent page at the same position as before the replacement.
                    self.stack
                        .node_states
                        .get_mut(btree_depth)
                        .expect("parent page should be on the stack")
                        .cell_idx = cell_idx as i32;
                    let (cell_payload, leaf_cell_idx) = {
                        let leaf_page = self.stack.top_ref();
                        let leaf_contents = leaf_page.get_contents();
                        turso_assert!(leaf_contents.is_leaf());
                        turso_assert_greater_than!(leaf_contents.cell_count(), 0);
                        let leaf_cell_idx = leaf_contents.cell_count() - 1;
                        let last_cell_on_child_page =
                            leaf_contents.cell_get(leaf_cell_idx, usable_space)?;

                        let mut cell_payload: crate::alloc::Vec<u8> = crate::alloc::vec![];
                        let child_pointer =
                            original_child_pointer.expect("there should be a pointer");
                        // Rewrite the old leaf cell as an interior cell depending on type.
                        match last_cell_on_child_page {
                            BTreeCell::TableLeafCell(leaf_cell) => {
                                // Table interior cells contain the left child pointer and the rowid as varint.
                                crate::with_btree_allocation_site!(
                                    CellPayload,
                                    cell_payload.try_extend(child_pointer.to_be_bytes())
                                )?;
                                write_varint_to_vec(leaf_cell.rowid as u64, &mut cell_payload)?;
                            }
                            BTreeCell::IndexLeafCell(leaf_cell) => {
                                // Index interior cells contain:
                                // 1. The left child pointer
                                // 2. The payload size as varint
                                // 3. The payload
                                // 4. The first overflow page as varint, omitted if no overflow.
                                crate::with_btree_allocation_site!(
                                    CellPayload,
                                    cell_payload.try_extend(child_pointer.to_be_bytes())
                                )?;
                                write_varint_to_vec(leaf_cell.payload_size, &mut cell_payload)?;
                                crate::with_btree_allocation_site!(
                                    CellPayload,
                                    cell_payload.try_extend(leaf_cell.payload.iter().copied())
                                )?;
                                if let Some(first_overflow_page) = leaf_cell.first_overflow_page {
                                    crate::with_btree_allocation_site!(
                                        CellPayload,
                                        cell_payload.try_extend(first_overflow_page.to_be_bytes())
                                    )?;
                                }
                            }
                            _ => unreachable!("Expected table leaf cell"),
                        }
                        (cell_payload, leaf_cell_idx)
                    };

                    let leaf_page = self.stack.top_ref();

                    self.pager.add_dirty(page)?;
                    self.pager.add_dirty(leaf_page)?;

                    // Step 2: Replace the cell in the parent (interior) page.
                    {
                        let parent_contents = page.get_contents();
                        let parent_page_id = page.get().id;
                        let left_child_page = u32::from_be_bytes(
                            cell_payload[..4].try_into().expect("invalid cell payload"),
                        );
                        turso_assert!(
                            left_child_page as usize != parent_page_id,
                            "corrupt: current page and left child page are the same",
                            { "left_child_page": left_child_page, "parent_page_id": parent_page_id }
                        );

                        // First, drop the old cell that is being replaced.
                        drop_cell(parent_contents, cell_idx, usable_space)?;
                        // Then, insert the new cell (the predecessor) in its place.
                        insert_into_cell(parent_contents, &cell_payload, cell_idx, usable_space)?;
                    }

                    // Step 3: Delete the predecessor cell from the leaf page.
                    {
                        let leaf_contents = leaf_page.get_contents();
                        drop_cell(leaf_contents, leaf_cell_idx, usable_space)?;
                    }

                    self.state = CursorState::Delete(DeleteState::CheckNeedsBalancing {
                        btree_depth,
                        post_balancing_seek_key: post_balancing_seek_key.take(),
                        interior_node_was_replaced: true,
                    });
                }

                DeleteState::CheckNeedsBalancing { btree_depth, .. } => {
                    let page = self.stack.top_ref();
                    // Check if either the leaf page we took the replacement cell from underflows, or if the interior page we inserted it into overflows OR underflows.
                    // If the latter is true, we must always balance that level regardless of whether the leaf page (or any ancestor pages in between) need balancing.

                    let leaf_underflows = {
                        let leaf_contents = page.get_contents();
                        let free_space = compute_free_space(leaf_contents, usable_space)?;
                        free_space * 3 > usable_space * 2
                    };

                    let interior_overflows_or_underflows = {
                        // Invariant: ancestor pages on the stack are pinned to the page cache,
                        // so we don't need return_if_locked_maybe_load! any ancestor,
                        // and we already loaded the current page above.
                        let interior_page = self
                            .stack
                            .get_page_at_level(*btree_depth)
                            .expect("ancestor page should be on the stack");
                        let interior_contents = interior_page.get_contents();
                        let overflows = !interior_contents.overflow_cells.is_empty();
                        if overflows {
                            true
                        } else {
                            let free_space = compute_free_space(interior_contents, usable_space)?;
                            free_space * 3 > usable_space * 2
                        }
                    };

                    let needs_balancing = leaf_underflows || interior_overflows_or_underflows;

                    let CursorState::Delete(DeleteState::CheckNeedsBalancing {
                        btree_depth,
                        ref mut post_balancing_seek_key,
                        interior_node_was_replaced,
                        ..
                    }) = self.state
                    else {
                        unreachable!("expected check needs balancing state");
                    };

                    if needs_balancing {
                        let balance_only_ancestor =
                            !leaf_underflows && interior_overflows_or_underflows;
                        if balance_only_ancestor {
                            // Only need to balance the ancestor page; move there immediately.
                            while self.stack.current() > btree_depth {
                                self.stack.pop();
                            }
                        }
                        let balance_both = leaf_underflows && interior_overflows_or_underflows;
                        turso_assert!(matches!(self.balance_state.sub_state, BalanceSubState::Start), "no balancing operation should be in progress during delete", { "sub_state": self.balance_state.sub_state });
                        let post_balancing_seek_key = post_balancing_seek_key
                            .take()
                            .expect("post_balancing_seek_key should be Some");
                        self.save_context(post_balancing_seek_key);
                        self.state = CursorState::Delete(DeleteState::Balancing {
                            balance_ancestor_at_depth: if balance_both {
                                Some(btree_depth)
                            } else {
                                None
                            },
                        });
                    } else {
                        // No balancing needed.
                        if interior_node_was_replaced {
                            // If we did replace an interior node, we need to advance the cursor once to
                            // get back at the interior node that now has the replaced content.
                            // The reason it is important to land here is that the replaced cell was smaller (LT) than the deleted cell,
                            // so we must ensure we skip over it. I.e., when BTreeCursor::next() is called, it will move past the cell
                            // that holds the replaced content.
                            self.state =
                                CursorState::Delete(DeleteState::PostInteriorNodeReplacement);
                        } else {
                            // If we didn't replace an interior node, we are done,
                            // except we need to retreat, so that the next call to BTreeCursor::next() lands at the next record (because we deleted the current one)
                            self.stack.retreat();
                            self.state = CursorState::None;
                            return Ok(IOResult::Done(()));
                        }
                    }
                }
                DeleteState::PostInteriorNodeReplacement => {
                    return_if_io!(self.get_next_record());
                    self.state = CursorState::None;
                    return Ok(IOResult::Done(()));
                }

                DeleteState::Balancing {
                    balance_ancestor_at_depth,
                } => {
                    let balance_ancestor_at_depth = *balance_ancestor_at_depth;
                    return_if_io!(self.balance(balance_ancestor_at_depth));
                    self.state = CursorState::Delete(DeleteState::RestoreContextAfterBalancing);
                }
                DeleteState::RestoreContextAfterBalancing => {
                    return_if_io!(self.restore_context());

                    // We deleted key K, and performed a seek to: GE { eq_only: true } K.
                    // This means that the cursor is now pointing to the next key after K.
                    // We need to make the next call to BTreeCursor::next() a no-op so that we don't skip over
                    // a row when deleting rows in a loop.
                    self.skip_advance = true;
                    self.state = CursorState::None;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    #[inline(always)]
    /// In outer joins, whenever the right-side table has no matching row, the query must still return a row
    /// for each left-side row. In order to achieve this, we set the null flag on the right-side table cursor
    /// so that it returns NULL for all columns until cleared.
    fn set_null_flag(&mut self, flag: bool) {
        self.null_flag = flag;
    }

    #[inline(always)]
    fn get_null_flag(&self) -> bool {
        self.null_flag
    }

    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn exists(&mut self, key: &Value) -> IOResultOr<bool> {
        let int_key = match key {
            Value::Numeric(Numeric::Integer(i)) => i,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        let seek_result =
            return_if_io!(self.seek(SeekKey::TableRowId(*int_key), SeekOp::GE { eq_only: true }));
        let exists = matches!(seek_result, SeekResult::Found);
        self.invalidate_record();
        Ok(IOResult::Done(exists))
    }

    /// Deletes all content from the B-Tree but preserves the root page.
    ///
    /// Unlike [`btree_destroy`], which frees all pages including the root,
    /// this method only clears the tree’s contents. The root page remains
    /// allocated and is reset to an empty leaf page.
    fn clear_btree(&mut self) -> IOResultOr<Option<usize>> {
        // First entry only — destroy_btree_contents yields IO and resumes
        // through this method, so guard with the same state==None gate it
        // uses for its own state machine. Every page in this btree is about
        // to be freed; peers must drop their page stacks rather than save
        // positions that wouldn't outlive the clear (cf. sqlite3BtreeClearTable,
        // btree.c:10194).
        if matches!(self.state, CursorState::None) {
            self.pager.invalidate_peer_cursors(self);
            self.invalidate_count_cache();
            // Every page in this btree is about to be freed, so our own cached
            // rightmost page id is meaningless too (the id may even be
            // reallocated to an unrelated page after a refill).
            self.move_to_right_state.1 = None;
        }
        self.destroy_btree_contents(true)
    }

    /// Destroys the entire B-Tree, including the root page.
    ///
    /// All pages belonging to the tree are freed, leaving no trace of the B-Tree.
    /// Use this when the structure itself is no longer needed.
    ///
    /// For cases where the B-Tree should remain allocated but emptied, see [`btree_clear`].
    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    fn btree_destroy(&mut self) -> IOResultOr<Option<usize>> {
        // See clear_btree for the state==None gate rationale.
        if matches!(self.state, CursorState::None) {
            self.pager.invalidate_peer_cursors(self);
        }
        self.destroy_btree_contents(false)
    }

    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    /// Count the number of entries in the b-tree
    ///
    /// Only supposed to be used in the context of a simple Count Select Statement
    fn count(&mut self) -> IOResultOr<usize> {
        let mut mem_page;
        let mut contents;

        if self.valid_state == CursorValidState::Invalid {
            return Ok(IOResult::Done(0));
        }

        'outer: loop {
            let state = self.count_state;
            match state {
                CountState::Start => {
                    self.clear_saved_seek();
                    let c = return_if_io!(self.move_to_root_nonblock());
                    self.count_state = CountState::Loop;
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                CountState::Loop => {
                    self.stack.advance();
                    mem_page = self.stack.top_ref();
                    contents = mem_page.get_contents();

                    /* If this is a leaf page or the tree is not an int-key tree, then
                     ** this page contains countable entries. Increment the entry counter
                     ** accordingly.
                     */
                    if !matches!(contents.page_type()?, PageType::TableInterior) {
                        self.count += contents.cell_count();
                    }

                    let cell_idx = self.stack.current_cell_index() as usize;

                    // Second condition is necessary in case we return if the page is locked in the loop below
                    if contents.is_leaf() || cell_idx > contents.cell_count() {
                        loop {
                            if !self.stack.has_parent() {
                                // All pages of the b-tree have been visited. Return successfully.
                                // Move the `move_to_root_nonblock` call into `Finish` so a spill
                                // yield from it can't re-enter `Loop`'s `count += cell_count()`.
                                self.count_state = CountState::Finish;
                                continue 'outer;
                            }

                            // Move to parent
                            self.stack.pop();

                            mem_page = self.stack.top_ref();
                            turso_assert!(mem_page.is_loaded(), "page should be loaded");
                            contents = mem_page.get_contents();

                            let cell_idx = self.stack.current_cell_index() as usize;

                            if cell_idx <= contents.cell_count() {
                                break;
                            }
                        }
                    }

                    let cell_idx = self.stack.current_cell_index() as usize;

                    turso_assert_less_than_or_equal!(cell_idx, contents.cell_count());
                    turso_assert!(!contents.is_leaf());

                    if cell_idx == contents.cell_count() {
                        // Move to right child
                        // should be safe as contents is not a leaf page
                        let right_most_pointer = contents.rightmost_pointer()?.unwrap();
                        // Spill yield here would re-enter `CountState::Loop`,
                        // which re-runs `stack.advance()` and the leaf-count
                        // increment. Transition to `CountState::Descend` so
                        // re-entry skips those mutations and only retries the
                        // read + (second) advance + push.
                        match self.pager.read_page(right_most_pointer as i64)? {
                            IOResult::Done((child, c)) => {
                                self.stack.advance();
                                self.stack.push(child);
                                if let Some(c) = c {
                                    io_yield_one!(c);
                                }
                            }
                            IOResult::IO(IOCompletions(spill_c)) => {
                                self.count_state = CountState::Descend {
                                    target: right_most_pointer as i64,
                                };
                                io_yield_one!(spill_c);
                            }
                        }
                    } else {
                        // Move to child left page
                        let cell = contents.cell_get(cell_idx, self.usable_space())?;

                        match cell {
                            BTreeCell::TableInteriorCell(TableInteriorCell {
                                left_child_page,
                                ..
                            })
                            | BTreeCell::IndexInteriorCell(IndexInteriorCell {
                                left_child_page,
                                ..
                            }) => {
                                // Same re-entry handling as the rightmost
                                // branch above.
                                match self.pager.read_page(left_child_page as i64)? {
                                    IOResult::Done((child, c)) => {
                                        self.stack.advance();
                                        self.stack.push(child);
                                        if let Some(c) = c {
                                            io_yield_one!(c);
                                        }
                                    }
                                    IOResult::IO(IOCompletions(spill_c)) => {
                                        self.count_state = CountState::Descend {
                                            target: left_child_page as i64,
                                        };
                                        io_yield_one!(spill_c);
                                    }
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                CountState::Descend { target } => {
                    // Resume after a spill yield from `CountState::Loop` mid-
                    // descent. The loop-top mutations are already applied for
                    // this step; finish the descent and return to `Loop`.
                    let (child, c) = return_if_io!(self.pager.read_page(target));
                    self.stack.advance();
                    self.stack.push(child);
                    self.count_state = CountState::Loop;
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                CountState::Finish => {
                    // Idempotent: a spill yield re-enters this same arm.
                    let c = return_if_io!(self.move_to_root_nonblock());
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                    return Ok(IOResult::Done(self.count));
                }
            }
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        !self.has_record
    }

    #[inline]
    fn root_page(&self) -> i64 {
        self.root_page
    }

    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn rewind(&mut self) -> IOResultOr<()> {
        self.set_null_flag(false);
        if self.valid_state == CursorValidState::Invalid {
            return Ok(IOResult::Done(()));
        }
        self.clear_saved_seek();
        self.skip_advance = false;
        loop {
            match self.rewind_state {
                RewindState::Start => {
                    let c = return_if_io!(self.move_to_root_nonblock());
                    self.rewind_state = RewindState::NextRecord;
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                RewindState::NextRecord => {
                    return_if_io!(self.get_next_record());
                    self.rewind_state = RewindState::Start;
                    self.read_overflow_state = None;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    #[inline]
    fn has_rowid(&self) -> bool {
        match &self.index_info {
            Some(index_key_info) => index_key_info.has_rowid,
            None => true,
        }
    }

    #[inline]
    fn invalidate_record(&mut self) {
        if let Some(record) = self.reusable_immutable_record.as_mut() {
            record.invalidate();
        }
    }

    #[inline]
    fn get_pager(&self) -> Arc<Pager> {
        self.pager.clone()
    }

    #[inline]
    fn get_skip_advance(&self) -> bool {
        self.skip_advance
    }

    /// Drop the page stack and auxiliary caches so the cursor will re-navigate
    /// from the root on its next access. Used when saving the position via
    /// try_save_position_for_external_balance isn't applicable (the peer
    /// btree was cleared/destroyed, or the position can't be expressed).
    /// valid_state stays Valid — only rewind/seek-style entry points are safe
    /// next; next/prev land on `current_page == -1` and return Done(false).
    fn invalidate_btree_cache(&mut self) {
        self.stack.clear();
        self.has_record = false;
        self.move_to_right_state.1 = None;
        self.invalidate_count_cache();
        self.blob_cache.reset();
    }

    fn note_external_row_write(&mut self, rowid: Option<i64>) {
        // Only cursors that have served incremental blob I/O carry a pin. An unknown
        // rowid could be the pinned row, so it must expire the handle too — better a
        // spurious SQLITE_ABORT than byte access to a rewritten value.
        if let Some(pinned) = self.blob_pinned_rowid {
            if rowid.is_none_or(|r| r == pinned) {
                self.blob_expired = true;
            }
        }
    }

    fn register_with_pager(&self) {
        // Store before the push so a panic inside register_cursor still
        // triggers an (idempotent) unregister via Drop.
        self.did_register
            .store(true, crate::sync::atomic::Ordering::Relaxed);
        self.pager.register_cursor(self);
    }

    fn set_has_peers_for_external_writes(&self, has_peers: bool) {
        self.has_peers
            .store(has_peers, crate::sync::atomic::Ordering::Relaxed);
    }

    /// Mirrors SQLite's saveCursorPosition (btree.c:756). Saves rowid for
    /// table btrees, the cell record for index btrees; index records can
    /// yield IO via the overflow chain walk (`record()`). Returns
    /// [`SavePositionResult::MustInvalidate`] when the page stack is in a
    /// sentinel/dirty state we can't save from — has_record can lag the stack
    /// across an in-flight Insert/Delete — in which case the caller falls back
    /// to invalidate_btree_cache.
    fn try_save_position_for_external_balance(&mut self) -> IOResultOr<SavePositionResult> {
        // The peer is about to modify this btree's structure, so any cached
        // knowledge of the tree shape goes stale: the rightmost page id
        // (move_to_rightmost's skip-a-seek optimization) and the memoized
        // count. Positional state is preserved separately via save_context.
        // Idempotent, so safe across IO re-entry into this function.
        self.move_to_right_state.1 = None;
        self.invalidate_count_cache();
        if self.valid_state != CursorValidState::Valid || !self.has_record() {
            // Nothing to save: cursor has no live position. No invalidation
            // needed either — the stack is already in a state where the next
            // entry point will re-navigate from the root.
            return Ok(IOResult::Done(SavePositionResult::Saved));
        }
        // A peer mid-Insert/Delete may not yet have reached its own
        // save_context-at-balance point, so we can't claim it's saved. Fall
        // back to invalidation; the peer will re-navigate on next use.
        if !matches!(self.state, CursorState::None) {
            return Ok(IOResult::Done(SavePositionResult::MustInvalidate));
        }
        if self.stack.current_page < 0
            || (self.stack.current_page as usize) >= self.stack.stack.len()
            || self.stack.stack[self.stack.current_page as usize].is_none()
        {
            return Ok(IOResult::Done(SavePositionResult::MustInvalidate));
        }
        let cell_idx = self.stack.current_cell_index();
        if cell_idx < 0 {
            return Ok(IOResult::Done(SavePositionResult::MustInvalidate));
        }
        let (is_table, cell_count) = {
            let page = self.stack.top_ref();
            let contents = page.get_contents();
            (contents.page_type()?.is_table(), contents.cell_count())
        };
        if (cell_idx as usize) >= cell_count {
            return Ok(IOResult::Done(SavePositionResult::MustInvalidate));
        }
        if is_table {
            let page = self.stack.top_ref();
            let contents = page.get_contents();
            debug_assert!(
                matches!(contents.page_type(), Ok(PageType::TableLeaf)),
                "save_position: table cursor with has_record=true must be on a leaf"
            );
            let rowid = contents.cell_table_leaf_read_rowid(cell_idx as usize)?;
            self.save_context(CursorContext {
                key: CursorContextKey::TableRowId(rowid),
                seek_op: SeekOp::GE { eq_only: true },
            });
            return Ok(IOResult::Done(SavePositionResult::Saved));
        }
        // Index btree: yield IO for overflow chains. Allocate to the actual
        // payload size so wide-key indexes don't keep a page-sized buffer
        // per saved cursor.
        let cloned = {
            let record = return_if_io!(self.record());
            let record = record.expect("has_record=true but record() returned None");
            let payload = record.get_payload();
            let mut owned = crate::with_btree_allocation_site!(
                SavedCursorRecord,
                ImmutableRecord::new(payload.len())
            )?;
            crate::with_btree_allocation_site!(
                SavedCursorRecord,
                owned.start_serialization(payload)
            )?;
            owned
        };
        self.save_context(CursorContext {
            key: CursorContextKey::IndexKeyRowId(ImmutableRecordRef::from_owned_record(cloned)),
            seek_op: SeekOp::GE { eq_only: true },
        });
        Ok(IOResult::Done(SavePositionResult::Saved))
    }

    #[inline]
    fn has_record(&self) -> bool {
        self.has_record
    }

    #[inline]
    fn set_has_record(&mut self, has_record: bool) {
        self.has_record = has_record
    }

    #[inline]
    fn get_index_info(&self) -> &Arc<IndexInfo> {
        self.index_info.as_ref().unwrap()
    }

    fn seek_end(&mut self) -> IOResultOr<()> {
        if self.valid_state == CursorValidState::Invalid {
            return Ok(IOResult::Done(()));
        }
        loop {
            match self.seek_end_state {
                SeekEndState::Start => {
                    self.clear_saved_seek();
                    let c = return_if_io!(self.move_to_root_nonblock());
                    self.seek_end_state = SeekEndState::ProcessPage;
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                SeekEndState::ProcessPage => {
                    let mem_page = self.stack.top_ref();
                    let contents = mem_page.get_contents();
                    if contents.is_leaf() {
                        // set cursor just past the last cell to append
                        self.stack.set_cell_index(contents.cell_count() as i32);
                        self.seek_end_state = SeekEndState::Start;
                        return Ok(IOResult::Done(()));
                    }

                    match contents.rightmost_pointer()? {
                        Some(right_most_pointer) => {
                            let (child, c) =
                                return_if_io!(self.read_page(right_most_pointer as i64));
                            self.stack.set_cell_index(contents.cell_count() as i32 + 1); // invalid on interior
                            self.stack.push(child);
                            if let Some(c) = c {
                                io_yield_one!(c);
                            }
                        }
                        None => unreachable!("interior page must have rightmost pointer"),
                    }
                }
            }
        }
    }

    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn seek_to_last(&mut self) -> IOResultOr<()> {
        loop {
            match self.seek_to_last_state {
                SeekToLastState::Start => {
                    // A write through another cursor may save this cursor's old
                    // position. We need the current largest rowid, not that old
                    // position. Otherwise rowid() restores the old position.
                    self.clear_saved_seek();
                    let has_record = return_if_io!(self.move_to_rightmost());
                    self.invalidate_record();
                    self.set_has_record(has_record);
                    self.read_overflow_state = None;
                    if !has_record {
                        self.seek_to_last_state = SeekToLastState::IsEmpty;
                        continue;
                    }
                    return Ok(IOResult::Done(()));
                }
                SeekToLastState::IsEmpty => {
                    let is_empty = return_if_io!(self.is_empty_table());
                    turso_assert!(is_empty);
                    self.seek_to_last_state = SeekToLastState::Start;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }
}

impl BTreeCursor {
    /// True when the next cell is on the same leaf page and no resumable
    /// state is pending, so advancing cannot yield and `next()` can skip
    /// its state machine. Every pending flag routes to the full path,
    /// which owns its handling: `skip_advance` (restore landed on the
    /// iteration target; advancing would skip a row), an abandoned
    /// overflow read, and an in-flight spill descent.
    fn can_advance_within_leaf(&self) -> bool {
        if !matches!(self.advance_state, AdvanceState::Start)
            || !matches!(self.valid_state, CursorValidState::Valid)
            || self.needs_restore()
            || self.skip_advance
            || !self.has_record
            || self.read_overflow_state.is_some()
            || self.iteration_pending_descent.is_some()
        {
            return false;
        }
        let contents = self.stack.top_ref().get_contents();
        let cell_idx = self.stack.current_cell_index();
        cell_idx >= 0 && contents.is_leaf() && cell_idx as usize + 1 < contents.cell_count()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IntegrityCheckError {
    #[error("Cell {cell_idx} in page {page_id} is out of range. cell_range={cell_start}..{cell_end}, content_area={content_area}, usable_space={usable_space}")]
    CellOutOfRange {
        cell_idx: usize,
        page_id: i64,
        cell_start: usize,
        cell_end: usize,
        content_area: usize,
        usable_space: usize,
    },
    #[error("Cell {cell_idx} in page {page_id} extends out of page. cell_range={cell_start}..{cell_end}, content_area={content_area}, usable_space={usable_space}")]
    CellOverflowsPage {
        cell_idx: usize,
        page_id: i64,
        cell_start: usize,
        cell_end: usize,
        content_area: usize,
        usable_space: usize,
    },
    #[error("Page {page_id} ({page_category:?}) cell {cell_idx} has rowid={rowid} in wrong order. Parent cell has parent_rowid={max_intkey} and next_rowid={next_rowid}")]
    CellRowidOutOfRange {
        page_id: i64,
        page_category: PageCategory,
        cell_idx: usize,
        rowid: i64,
        max_intkey: i64,
        next_rowid: i64,
    },
    #[error("Page {page_id} is at different depth from another leaf page this_page_depth={this_page_depth}, other_page_depth={other_page_depth} ")]
    LeafDepthMismatch {
        page_id: i64,
        this_page_depth: usize,
        other_page_depth: usize,
    },
    #[error("Page {page_id} detected freeblock that extends page start={start} end={end}")]
    FreeBlockOutOfRange {
        page_id: i64,
        start: usize,
        end: usize,
    },
    #[error("Page {page_id} cell overlap detected at position={start} with previous_end={prev_end}. content_area={content_area}, is_free_block={is_free_block}")]
    CellOverlap {
        page_id: i64,
        start: usize,
        prev_end: usize,
        content_area: usize,
        is_free_block: bool,
    },
    #[error("Page {page_id} unexpected fragmentation got={got}, expected={expected}")]
    UnexpectedFragmentation {
        page_id: i64,
        got: usize,
        expected: usize,
    },
    #[error("Page {page_id} referenced multiple times (references={references:?}, page_category={page_category:?})")]
    PageReferencedMultipleTimes {
        page_id: i64,
        references: crate::alloc::Vec<i64>,
        page_category: PageCategory,
    },
    #[error("Freelist: size is {actual_count} but should be {expected_count}")]
    FreelistCountMismatch {
        actual_count: usize,
        expected_count: usize,
    },
    #[error("Page {page_id}: never used")]
    PageNeverUsed { page_id: i64 },
    #[error("Pending byte page {page_id} is being used")]
    PendingBytePageUsed { page_id: i64 },
    #[error("Freelist: freelist leaf count too big on page {page_id}")]
    FreelistTrunkCorrupt {
        page_id: i64,
        page_pointers: u32,
        max_pointers: usize,
    },
    #[error("Freelist: invalid page number {pointer}")]
    FreelistPointerOutOfRange { page_id: i64, pointer: i64 },
    #[error("overflow list length is {got} but should be {expected}")]
    OverflowListLengthMismatch { got: usize, expected: usize },
}

fn push_integrity_error(
    errors: &mut crate::alloc::Vec<IntegrityCheckError>,
    error: IntegrityCheckError,
) -> Result<()> {
    crate::with_btree_allocation_site!(IntegrityCheck, errors.try_push(error))?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageCategory {
    Normal,
    Overflow,
    FreeListTrunk,
    FreePage,
}

#[derive(Clone)]
pub struct CheckFreelist {
    pub expected_count: usize,
    pub actual_count: usize,
}

#[derive(Clone)]
struct IntegrityCheckPageEntry {
    page_idx: i64,
    level: usize,
    max_intkey: i64,
    page_category: PageCategory,
    overflow_pages_expected: Option<usize>,
    overflow_pages_seen: usize,
}
pub struct IntegrityCheckState {
    page_stack: crate::alloc::Vec<IntegrityCheckPageEntry>,
    pub db_size: usize,
    first_leaf_level: Option<usize>,
    pub page_reference: HashMap<i64, i64>,
    page: Option<PageRef>,
    pub freelist_count: CheckFreelist,
}

impl IntegrityCheckState {
    pub fn new(db_size: usize) -> Self {
        Self {
            page_stack: crate::alloc::vec![],
            db_size,
            page_reference: HashMap::default(),
            first_leaf_level: None,
            page: None,
            freelist_count: CheckFreelist {
                expected_count: 0,
                actual_count: 0,
            },
        }
    }

    pub fn set_expected_freelist_count(&mut self, count: usize) {
        self.freelist_count.expected_count = count;
    }

    pub fn start(
        &mut self,
        page_idx: i64,
        page_category: PageCategory,
        errors: &mut crate::alloc::Vec<IntegrityCheckError>,
    ) -> Result<()> {
        turso_assert!(
            self.page_stack.is_empty(),
            "stack should be empty before integrity check for new root"
        );
        // root can't be referenced from anywhere - so we insert "zero entry" for it
        self.push_page(
            IntegrityCheckPageEntry {
                page_idx,
                level: 0,
                max_intkey: i64::MAX,
                page_category,
                overflow_pages_expected: None,
                overflow_pages_seen: 0,
            },
            0,
            errors,
        )?;
        self.first_leaf_level = None;
        let _ = self.page.take();
        Ok(())
    }

    fn push_page(
        &mut self,
        entry: IntegrityCheckPageEntry,
        referenced_by: i64,
        errors: &mut crate::alloc::Vec<IntegrityCheckError>,
    ) -> Result<()> {
        let page_id = entry.page_idx;
        let Some(previous) = self.page_reference.get(&page_id).copied() else {
            crate::with_btree_allocation_site!(IntegrityCheck, self.page_stack.try_reserve(1))?;
            let previous = self.page_reference.insert(page_id, referenced_by);
            turso_assert!(
                previous.is_none(),
                "page reference changed during insertion"
            );
            self.page_stack
                .push_within_capacity(entry)
                .unwrap_or_else(|_| unreachable!("reserved page stack slot was unavailable"));
            return Ok(());
        };
        let references = crate::with_btree_allocation_site!(
            IntegrityCheck,
            crate::alloc::try_vec![previous, referenced_by]
        )?;
        push_integrity_error(
            errors,
            IntegrityCheckError::PageReferencedMultipleTimes {
                page_id,
                page_category: entry.page_category,
                references,
            },
        )?;
        Ok(())
    }
}
impl std::fmt::Debug for IntegrityCheckState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IntegrityCheckState")
            .field("first_leaf_level", &self.first_leaf_level)
            .finish()
    }
}

fn overflow_pages_expected_for_cell(
    payload_size: u64,
    local_payload_size: usize,
    usable_space: usize,
) -> usize {
    let payload_size = usize::try_from(payload_size).unwrap_or(usize::MAX);
    let remaining_payload = payload_size.saturating_sub(local_payload_size);
    if remaining_payload == 0 {
        return 0;
    }
    let overflow_page_payload = usable_space.saturating_sub(4).max(1);
    remaining_payload.div_ceil(overflow_page_payload)
}

/// Perform integrity check on a whole table/index. We check for:
/// 1. Correct order of keys in case of rowids.
/// 2. There are no overlap between cells.
/// 3. Cells do not scape outside expected range.
/// 4. Depth of leaf pages are equal.
/// 5. Overflow pages are correct (TODO)
///
/// In order to keep this reentrant, we keep a stack of pages we need to check. Ideally, like in
/// SQLlite, we would have implemented a recursive solution which would make it easier to check the
/// depth.
pub fn integrity_check(
    state: &mut IntegrityCheckState,
    errors: &mut crate::alloc::Vec<IntegrityCheckError>,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> IOResultOr<()> {
    if let Some(mv_store) = mv_store {
        let Some(IntegrityCheckPageEntry {
            page_idx: root_page,
            ..
        }) = state.page_stack.last().cloned()
        else {
            return Ok(IOResult::Done(()));
        };
        if root_page < 0 {
            let table_id = mv_store.get_table_id_from_root_page(root_page);
            turso_assert!(
                !mv_store.is_btree_allocated(&table_id),
                "we got a negative page index that is reported as allocated"
            );
            state.page_stack.pop();
            return Ok(IOResult::Done(()));
        }
    }
    if state.db_size == 0 {
        state.page_stack.pop();
        return Ok(IOResult::Done(()));
    }
    loop {
        let Some(IntegrityCheckPageEntry {
            page_idx,
            page_category,
            level,
            max_intkey,
            overflow_pages_expected,
            overflow_pages_seen,
        }) = state.page_stack.last().cloned()
        else {
            return Ok(IOResult::Done(()));
        };
        turso_assert!(
            page_idx >= 0,
            "pages should be positive during integrity check"
        );
        let page = match state.page.take() {
            Some(page) => page,
            None => {
                // On `IO(spill_c)` we leave `state.page = None` so re-entry
                // re-takes this None branch and resumes via the pager's
                // `pending_reads` memoization.
                let (page, c) = return_if_io!(pager.read_page(page_idx));
                state.page = Some(page);
                if let Some(c) = c {
                    io_yield_one!(c);
                }
                state.page.take().expect("page should be present")
            }
        };
        turso_assert!(page.is_loaded(), "page should be loaded");
        state.page_stack.pop();

        let contents = page.get_contents();
        if page_category == PageCategory::FreeListTrunk {
            state.freelist_count.actual_count += 1;
            let next_freelist_trunk_page =
                contents.read_u32_no_offset(FREELIST_TRUNK_OFFSET_NEXT_TRUNK_PTR);
            if next_freelist_trunk_page != 0 {
                if next_freelist_trunk_page as usize > state.db_size {
                    tracing::error!(
                        "integrity_check: freelist trunk page {} has invalid next pointer {}. header_bytes={:02x?}",
                        page.get().id,
                        next_freelist_trunk_page,
                        &contents.as_ptr()[0..16]
                    );
                    push_integrity_error(
                        errors,
                        IntegrityCheckError::FreelistPointerOutOfRange {
                            page_id: page.get().id as i64,
                            pointer: next_freelist_trunk_page as i64,
                        },
                    )?;
                    continue;
                }
                state.push_page(
                    IntegrityCheckPageEntry {
                        page_idx: next_freelist_trunk_page as i64,
                        level,
                        max_intkey,
                        page_category: PageCategory::FreeListTrunk,
                        overflow_pages_expected: None,
                        overflow_pages_seen: 0,
                    },
                    page.get().id as i64,
                    errors,
                )?;
            }
            let page_pointers = contents.read_u32_no_offset(FREELIST_TRUNK_OFFSET_LEAF_COUNT);
            let page_size = contents.as_ptr().len();
            let max_pointers =
                page_size.saturating_sub(FREELIST_TRUNK_HEADER_SIZE) / FREELIST_LEAF_PTR_SIZE;
            if unlikely(page_pointers as usize > max_pointers) {
                tracing::error!(
                    "integrity_check: freelist trunk page {} has invalid leaf count {} (max {}). header_bytes={:02x?}",
                    page.get().id,
                    page_pointers,
                    max_pointers,
                    &contents.as_ptr()[0..16]
                );
                push_integrity_error(
                    errors,
                    IntegrityCheckError::FreelistTrunkCorrupt {
                        page_id: page.get().id as i64,
                        page_pointers,
                        max_pointers,
                    },
                )?;
                continue;
            }
            for i in 0..page_pointers {
                let offset =
                    FREELIST_TRUNK_OFFSET_FIRST_LEAF_PTR + FREELIST_LEAF_PTR_SIZE * i as usize;
                if unlikely(offset + FREELIST_LEAF_PTR_SIZE > page_size) {
                    tracing::error!(
                        "integrity_check: freelist trunk page {} has invalid leaf offset {}. header_bytes={:02x?}",
                        page.get().id,
                        offset,
                        &contents.as_ptr()[0..16]
                    );
                    push_integrity_error(
                        errors,
                        IntegrityCheckError::FreelistTrunkCorrupt {
                            page_id: page.get().id as i64,
                            page_pointers,
                            max_pointers,
                        },
                    )?;
                    break;
                }
                let page_pointer = contents.read_u32_no_offset(offset);
                if page_pointer as usize > state.db_size {
                    tracing::error!(
                        "integrity_check: freelist trunk page {} has invalid leaf pointer {}. header_bytes={:02x?}",
                        page.get().id,
                        page_pointer,
                        &contents.as_ptr()[0..16]
                    );
                    push_integrity_error(
                        errors,
                        IntegrityCheckError::FreelistPointerOutOfRange {
                            page_id: page.get().id as i64,
                            pointer: page_pointer as i64,
                        },
                    )?;
                    continue;
                }
                state.push_page(
                    IntegrityCheckPageEntry {
                        page_idx: page_pointer as i64,
                        level,
                        max_intkey,
                        page_category: PageCategory::FreePage,
                        overflow_pages_expected: None,
                        overflow_pages_seen: 0,
                    },
                    page.get().id as i64,
                    errors,
                )?;
            }
            continue;
        }
        if page_category == PageCategory::FreePage {
            state.freelist_count.actual_count += 1;
            continue;
        }
        if page_category == PageCategory::Overflow {
            let overflow_pages_seen = overflow_pages_seen.saturating_add(1);
            let next_overflow_page = contents.read_u32_no_offset(0);
            if next_overflow_page != 0 {
                state.push_page(
                    IntegrityCheckPageEntry {
                        page_idx: next_overflow_page as i64,
                        level,
                        max_intkey,
                        page_category: PageCategory::Overflow,
                        overflow_pages_expected,
                        overflow_pages_seen,
                    },
                    page.get().id as i64,
                    errors,
                )?;
            } else if let Some(expected) = overflow_pages_expected {
                if overflow_pages_seen != expected {
                    push_integrity_error(
                        errors,
                        IntegrityCheckError::OverflowListLengthMismatch {
                            got: overflow_pages_seen,
                            expected,
                        },
                    )?;
                }
            }
            continue;
        }

        let usable_space = pager.usable_space();
        let mut coverage_checker = CoverageChecker::new(page.get().id as i64);

        // Now we check every cell for few things:
        // 1. Check cell is in correct range. Not exceeds page and not starts before we have marked
        //    (cell content area).
        // 2. We add the cell to coverage checker in order to check if cells do not overlap.
        // 3. We check order of rowids in case of table pages. We iterate backwards in order to check
        //    if current cell's rowid is less than the next cell. We also check rowid is less than the
        //    parent's divider cell. In case of this page being root page max rowid will be i64::MAX.
        // 4. We append pages to the stack to check later.
        // 5. In case of leaf page, check if the current level(depth) is equal to other leaf pages we
        //    have seen.
        let mut next_rowid = max_intkey;
        for cell_idx in (0..contents.cell_count()).rev() {
            let (cell_start, cell_length) = contents.cell_get_raw_region(cell_idx, usable_space)?;
            if cell_start < contents.cell_content_area() as usize || cell_start > usable_space - 4 {
                push_integrity_error(
                    errors,
                    IntegrityCheckError::CellOutOfRange {
                        cell_idx,
                        page_id: page.get().id as i64,
                        cell_start,
                        cell_end: cell_start + cell_length,
                        content_area: contents.cell_content_area() as usize,
                        usable_space,
                    },
                )?;
            }
            if cell_start + cell_length > usable_space {
                push_integrity_error(
                    errors,
                    IntegrityCheckError::CellOverflowsPage {
                        cell_idx,
                        page_id: page.get().id as i64,
                        cell_start,
                        cell_end: cell_start + cell_length,
                        content_area: contents.cell_content_area() as usize,
                        usable_space,
                    },
                )?;
            }
            coverage_checker.add_cell(cell_start, cell_start + cell_length);
            let cell = contents.cell_get(cell_idx, usable_space)?;
            match cell {
                BTreeCell::TableInteriorCell(table_interior_cell) => {
                    state.push_page(
                        IntegrityCheckPageEntry {
                            page_idx: table_interior_cell.left_child_page as i64,
                            level: level + 1,
                            max_intkey: table_interior_cell.rowid,
                            page_category: PageCategory::Normal,
                            overflow_pages_expected: None,
                            overflow_pages_seen: 0,
                        },
                        page.get().id as i64,
                        errors,
                    )?;
                    let rowid = table_interior_cell.rowid;
                    if rowid > max_intkey || rowid > next_rowid {
                        push_integrity_error(
                            errors,
                            IntegrityCheckError::CellRowidOutOfRange {
                                page_id: page.get().id as i64,
                                page_category,
                                cell_idx,
                                rowid,
                                max_intkey,
                                next_rowid,
                            },
                        )?;
                    }
                    next_rowid = rowid;
                }
                BTreeCell::TableLeafCell(table_leaf_cell) => {
                    // check depth of leaf pages are equal
                    if let Some(expected_leaf_level) = state.first_leaf_level {
                        if expected_leaf_level != level {
                            push_integrity_error(
                                errors,
                                IntegrityCheckError::LeafDepthMismatch {
                                    page_id: page.get().id as i64,
                                    this_page_depth: level,
                                    other_page_depth: expected_leaf_level,
                                },
                            )?;
                        }
                    } else {
                        state.first_leaf_level = Some(level);
                    }
                    let rowid = table_leaf_cell.rowid;
                    if rowid > max_intkey || rowid > next_rowid {
                        push_integrity_error(
                            errors,
                            IntegrityCheckError::CellRowidOutOfRange {
                                page_id: page.get().id as i64,
                                page_category,
                                cell_idx,
                                rowid,
                                max_intkey,
                                next_rowid,
                            },
                        )?;
                    }
                    next_rowid = rowid;
                    if let Some(first_overflow_page) = table_leaf_cell.first_overflow_page {
                        let expected_pages = overflow_pages_expected_for_cell(
                            table_leaf_cell.payload_size,
                            table_leaf_cell.payload.len(),
                            usable_space,
                        );
                        state.push_page(
                            IntegrityCheckPageEntry {
                                page_idx: first_overflow_page as i64,
                                level,
                                max_intkey,
                                page_category: PageCategory::Overflow,
                                overflow_pages_expected: Some(expected_pages),
                                overflow_pages_seen: 0,
                            },
                            page.get().id as i64,
                            errors,
                        )?;
                    }
                }
                BTreeCell::IndexInteriorCell(index_interior_cell) => {
                    state.push_page(
                        IntegrityCheckPageEntry {
                            page_idx: index_interior_cell.left_child_page as i64,
                            level: level + 1,
                            max_intkey, // we don't care about intkey in non-table pages
                            page_category: PageCategory::Normal,
                            overflow_pages_expected: None,
                            overflow_pages_seen: 0,
                        },
                        page.get().id as i64,
                        errors,
                    )?;
                    if let Some(first_overflow_page) = index_interior_cell.first_overflow_page {
                        let expected_pages = overflow_pages_expected_for_cell(
                            index_interior_cell.payload_size,
                            index_interior_cell.payload.len(),
                            usable_space,
                        );
                        state.push_page(
                            IntegrityCheckPageEntry {
                                page_idx: first_overflow_page as i64,
                                level,
                                max_intkey,
                                page_category: PageCategory::Overflow,
                                overflow_pages_expected: Some(expected_pages),
                                overflow_pages_seen: 0,
                            },
                            page.get().id as i64,
                            errors,
                        )?;
                    }
                }
                BTreeCell::IndexLeafCell(index_leaf_cell) => {
                    // check depth of leaf pages are equal
                    if let Some(expected_leaf_level) = state.first_leaf_level {
                        if expected_leaf_level != level {
                            push_integrity_error(
                                errors,
                                IntegrityCheckError::LeafDepthMismatch {
                                    page_id: page.get().id as i64,
                                    this_page_depth: level,
                                    other_page_depth: expected_leaf_level,
                                },
                            )?;
                        }
                    } else {
                        state.first_leaf_level = Some(level);
                    }
                    if let Some(first_overflow_page) = index_leaf_cell.first_overflow_page {
                        let expected_pages = overflow_pages_expected_for_cell(
                            index_leaf_cell.payload_size,
                            index_leaf_cell.payload.len(),
                            usable_space,
                        );
                        state.push_page(
                            IntegrityCheckPageEntry {
                                page_idx: first_overflow_page as i64,
                                level,
                                max_intkey,
                                page_category: PageCategory::Overflow,
                                overflow_pages_expected: Some(expected_pages),
                                overflow_pages_seen: 0,
                            },
                            page.get().id as i64,
                            errors,
                        )?;
                    }
                }
            }
        }

        if let Some(rightmost) = contents.rightmost_pointer()? {
            state.push_page(
                IntegrityCheckPageEntry {
                    page_idx: rightmost as i64,
                    level: level + 1,
                    max_intkey,
                    page_category: PageCategory::Normal,
                    overflow_pages_expected: None,
                    overflow_pages_seen: 0,
                },
                page.get().id as i64,
                errors,
            )?;
        }

        // Now we add free blocks to the coverage checker
        let first_freeblock = contents.first_freeblock() as usize;
        if first_freeblock > 0 {
            let mut pc = first_freeblock;
            while pc > 0 {
                let next = contents.read_u16_no_offset(pc) as usize;
                let size = contents.read_u16_no_offset(pc + 2) as usize;
                // check it doesn't go out of range
                if pc > usable_space - 4 {
                    push_integrity_error(
                        errors,
                        IntegrityCheckError::FreeBlockOutOfRange {
                            page_id: page.get().id as i64,
                            start: pc,
                            end: pc + size,
                        },
                    )?;
                    break;
                }
                coverage_checker.add_free_block(pc, pc + size);
                pc = next;
            }
        }

        // Let's check the overlap of freeblocks and cells now that we have collected them all.
        coverage_checker.analyze(
            usable_space,
            contents.cell_content_area() as usize,
            errors,
            contents.num_frag_free_bytes() as usize,
        )?;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IntegrityCheckCellRange {
    start: usize,
    end: usize,
    is_free_block: bool,
}

// Implement ordering for min-heap (smallest start address first)
impl Ord for IntegrityCheckCellRange {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.start.cmp(&other.start)
    }
}

impl PartialOrd for IntegrityCheckCellRange {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Pads the cell that starts at `cell_start` in `buf` with zero bytes until it takes
/// MINIMUM_CELL_SIZE bytes, the least space any cell takes on a page (a freed cell must be
/// able to hold a 4-byte freeblock header).
///
/// Only index cells can be smaller: a one-column record of 0, 1, '' or X'' is 2 bytes, so
/// its cell is 3. A leaf page stores such a cell padded and cell_get_raw_region reports the
/// padded size, while a divider in the parent stores the cell's real size after its child
/// pointer. Padding the cell whenever it is headed for a leaf keeps balancing working with
/// one size; the record's own length prefix keeps readers from ever looking at the padding.
fn ensure_min_cell_size(buf: &mut crate::alloc::Vec<u8>, cell_start: usize) {
    const ZEROS: [u8; MINIMUM_CELL_SIZE] = [0; MINIMUM_CELL_SIZE];
    let padding = (cell_start + MINIMUM_CELL_SIZE).saturating_sub(buf.len());
    buf.extend_from_slice(&ZEROS[..padding]);
}

#[cfg(debug_assertions)]
fn validate_cells_after_insertion(cell_array: &CellArray, leaf_data: bool) {
    for cell in &cell_array.cell_payloads {
        turso_assert_greater_than_or_equal!(cell.len(), 4);

        if leaf_data {
            turso_assert!(cell[0] != 0);
        }
    }
}

pub struct CoverageChecker {
    /// Min-heap ordered by cell start
    heap: BinaryHeap<Reverse<IntegrityCheckCellRange>>,
    page_idx: i64,
}

impl CoverageChecker {
    pub fn new(page_idx: i64) -> Self {
        Self {
            heap: BinaryHeap::new(),
            page_idx,
        }
    }

    fn add_range(&mut self, cell_start: usize, cell_end: usize, is_free_block: bool) {
        self.heap.push(Reverse(IntegrityCheckCellRange {
            start: cell_start,
            end: cell_end,
            is_free_block,
        }));
    }

    pub fn add_cell(&mut self, cell_start: usize, cell_end: usize) {
        self.add_range(cell_start, cell_end, false);
    }

    pub fn add_free_block(&mut self, cell_start: usize, cell_end: usize) {
        self.add_range(cell_start, cell_end, true);
    }

    pub fn analyze(
        &mut self,
        usable_space: usize,
        content_area: usize,
        errors: &mut crate::alloc::Vec<IntegrityCheckError>,
        expected_fragmentation: usize,
    ) -> Result<()> {
        let mut fragmentation = 0;
        let mut prev_end = content_area;
        while let Some(cell) = self.heap.pop() {
            let start = cell.0.start;
            if prev_end > start {
                push_integrity_error(
                    errors,
                    IntegrityCheckError::CellOverlap {
                        page_id: self.page_idx,
                        start,
                        prev_end,
                        content_area,
                        is_free_block: cell.0.is_free_block,
                    },
                )?;
                break;
            } else {
                fragmentation += start - prev_end;
                prev_end = cell.0.end;
            }
        }
        fragmentation += usable_space - prev_end;
        if fragmentation != expected_fragmentation {
            push_integrity_error(
                errors,
                IntegrityCheckError::UnexpectedFragmentation {
                    page_id: self.page_idx,
                    got: fragmentation,
                    expected: expected_fragmentation,
                },
            )?;
        }
        Ok(())
    }
}

/// Stack of pages representing the tree traversal order.
/// current_page represents the current page being used in the tree and current_page - 1 would be
/// the parent. Using current_page + 1 or higher is undefined behaviour.
struct PageStack {
    /// Pointer to the current page being consumed
    current_page: i32,
    /// List of pages in the stack. Root page will be in index 0
    pub stack: [Option<PageRef>; BTCURSOR_MAX_DEPTH + 1],
    /// List of cell indices in the stack.
    /// node_states[current_page] is the current cell index being consumed. Similarly
    /// node_states[current_page-1] is the cell index of the parent of the current page
    /// that we save in case of going back up.
    /// There are two points that need special attention:
    ///  If node_states[current_page] = -1, it indicates that the current iteration has reached the start of the current_page
    ///  If node_states[current_page] = `cell_count`, it means that the current iteration has reached the end of the current_page
    node_states: [BTreeNodeState; BTCURSOR_MAX_DEPTH + 1],
}

impl PageStack {
    /// Push a new page onto the stack.
    /// This effectively means traversing to a child page.
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG, name = "pagestack::push"))]
    fn _push(&mut self, page: PageRef, starting_cell_idx: i32) {
        tracing::trace!(current = self.current_page, new_page_id = page.get().id,);
        'validate: {
            let current = self.current_page;
            if current == -1 {
                break 'validate;
            }
            let current_top = self.stack[current as usize].as_ref();
            if let Some(current_top) = current_top {
                turso_assert!(
                    current_top.get().id != page.get().id,
                    "about to push page twice",
                    { "page_id": page.get().id }
                );
            }
        }
        self.populate_parent_cell_count();
        self.current_page += 1;
        turso_assert_greater_than_or_equal!(self.current_page, 0);
        let current = self.current_page as usize;
        turso_assert_less_than!(
            current,
            BTCURSOR_MAX_DEPTH,
            "corrupted database, stack is bigger than expected"
        );

        // Pin the page to prevent it from being evicted while on the stack
        page.pin();

        self.stack[current] = Some(page);
        self.node_states[current] = BTreeNodeState {
            cell_idx: starting_cell_idx,
            cell_count: None, // we don't know the cell count yet, so we set it to None. any code pushing a child page onto the stack MUST set the parent page's cell_count.
        };
    }

    /// Populate the parent page's cell count.
    /// This is needed so that we can, from a child page, check of ancestor pages' position relative to its cell index
    /// without having to perform IO to get the ancestor page contents.
    ///
    /// This rests on the assumption that the parent page is already in memory whenever a child is pushed onto the stack.
    /// We currently ensure this by pinning all the pages on [PageStack] to the page cache so that they cannot be evicted.
    fn populate_parent_cell_count(&mut self) {
        let stack_empty = self.current_page == -1;
        if stack_empty {
            return;
        }
        let current = self.current();
        let page = self.stack[current].as_ref().unwrap();
        turso_assert!(
            page.is_pinned(),
            "parent page is not pinned",
            { "page_id": page.get().id }
        );
        turso_assert!(
            page.is_loaded(),
            "parent page is not loaded",
            { "page_id": page.get().id }
        );
        let contents = page.get_contents();
        let cell_count = contents.cell_count() as i32;
        self.node_states[current].cell_count = Some(cell_count);
    }

    fn push(&mut self, page: PageRef) {
        self._push(page, -1);
    }

    fn push_backwards(&mut self, page: PageRef) {
        self._push(page, i32::MAX);
    }

    /// Pop a page off the stack.
    /// This effectively means traversing back up to a parent page.
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG, name = "pagestack::pop"))]
    fn pop(&mut self) {
        let current = self.current_page;
        turso_assert_greater_than_or_equal!(current, 0);
        tracing::trace!(current);
        let current = current as usize;

        // Unpin the page before removing it from the stack
        if let Some(page) = &self.stack[current] {
            page.unpin();
        }

        turso_assert_greater_than!(current, 0);
        self.node_states[current] = BTreeNodeState::default();
        self.stack[current] = None;
        self.current_page -= 1;
    }

    /// Get the top page on the stack.
    /// This is the page that is currently being traversed.
    fn top(&self) -> PageRef {
        let current = self.current();
        let page = self.stack[current].clone().unwrap();
        turso_assert!(page.is_loaded(), "page should be loaded");
        page
    }

    fn top_ref(&self) -> &PageRef {
        let current = self.current();
        let page = self.stack[current].as_ref().unwrap();
        turso_assert!(page.is_loaded(), "page should be loaded");
        page
    }

    /// Current page pointer being used
    #[inline(always)]
    fn current(&self) -> usize {
        turso_assert_greater_than_or_equal!(self.current_page, 0);
        self.current_page as usize
    }

    /// Cell index of the current page
    fn current_cell_index(&self) -> i32 {
        let current = self.current();
        self.node_states[current].cell_idx
    }

    /// Check if the current cell index is less than 0.
    /// This means we have been iterating backwards and have reached the start of the page.
    fn current_cell_index_less_than_min(&self) -> bool {
        let cell_idx = self.current_cell_index();
        cell_idx < 0
    }

    /// Advance the current cell index of the current page to the next cell.
    /// We usually advance after going traversing a new page
    #[inline(always)]
    fn advance(&mut self) {
        let current = self.current();
        self.node_states[current].cell_idx += 1;
    }

    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG, name = "pagestack::retreat"))]
    fn retreat(&mut self) {
        let current = self.current();
        #[cfg(debug_assertions)]
        {
            let node_states: [i32; BTCURSOR_MAX_DEPTH + 1] =
                std::array::from_fn(|index| self.node_states[index].cell_idx);
            tracing::trace!(
                curr_cell_index = self.node_states[current].cell_idx,
                ?node_states,
            );
        }
        self.node_states[current].cell_idx -= 1;
    }

    fn set_cell_index(&mut self, idx: i32) {
        let current = self.current();
        self.node_states[current].cell_idx = idx;
    }

    fn has_parent(&self) -> bool {
        self.current_page > 0
    }

    /// Get a page at a specific level in the stack (0 = root, 1 = first child, etc.)
    fn get_page_at_level(&self, level: usize) -> Option<&PageRef> {
        if level < self.stack.len() {
            self.stack[level].as_ref()
        } else {
            None
        }
    }

    fn get_page_contents_at_level(&self, level: usize) -> Option<&mut PageContent> {
        self.get_page_at_level(level)
            .map(|page| page.get_contents())
    }

    /// Unpin and remove every page currently held in the stack.
    /// Slots are taken so that stale page references cannot survive past a
    /// reset — a leftover `Some(page)` after a clear could otherwise be
    /// unpinned again on the next reset, decrementing the pin count of a
    /// page another cursor's stack still relies on.
    fn unpin_all_and_clear_slots(&mut self) {
        for slot in self.stack.iter_mut() {
            if let Some(page) = slot.take() {
                let _ = page.try_unpin();
            }
        }
        for state in self.node_states.iter_mut() {
            *state = BTreeNodeState::default();
        }
    }

    fn clear(&mut self) {
        self.unpin_all_and_clear_slots();
        self.current_page = -1;
    }
}

impl Drop for PageStack {
    fn drop(&mut self) {
        self.unpin_all_and_clear_slots();
    }
}

/// Used for redistributing cells during a balance operation.
struct CellArray {
    /// The actual cell data.
    /// For all other page types except table leaves, this will also contain the associated divider cell from the parent page.
    cell_payloads: crate::alloc::Vec<&'static mut [u8]>,

    /// Prefix sum of cells in each page.
    /// For example, if three pages have 1, 2, and 3 cells, respectively,
    /// then cell_count_per_page_cumulative will be [1, 3, 6].
    cell_count_per_page_cumulative: [u16; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE],
}

impl std::fmt::Debug for CellArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellArray").finish()
    }
}

impl CellArray {
    pub fn cell_size_bytes(&self, cell_idx: usize) -> u16 {
        self.cell_payloads[cell_idx].len() as u16
    }

    /// Returns the number of cells up to and including the given page.
    pub fn cell_count_up_to_page(&self, page_idx: usize) -> usize {
        self.cell_count_per_page_cumulative[page_idx] as usize
    }
}

/// Try to find a freeblock inside the cell content area that is large enough to fit the given amount of bytes.
/// Used to check if a cell can be inserted into a freeblock to reduce fragmentation.
/// Returns the absolute byte offset of the freeblock if found.
fn find_free_slot(
    page_ref: &PageContent,
    usable_space: usize,
    amount: usize,
) -> Result<Option<usize>> {
    const CELL_SIZE_MIN: usize = 4;
    // NOTE: freelist is in ascending order of keys and pc
    // unuse_space is reserved bytes at the end of page, therefore we must substract from maxpc
    let mut prev_block = None;
    let mut cur_block = match page_ref.first_freeblock() {
        0 => None,
        first_block => Some(first_block as usize),
    };

    let max_start_offset = usable_space - amount;

    while let Some(cur) = cur_block {
        if unlikely(cur + CELL_SIZE_MIN > usable_space) {
            return_corrupt!("Free block header extends beyond page");
        }

        let (next, size) = {
            let cur_u16: u16 = cur
                .try_into()
                .unwrap_or_else(|_| panic!("cur={cur} is too large to fit in a u16"));
            let (next, size) = page_ref.read_freeblock(cur_u16);
            (next as usize, size as usize)
        };

        // Doesn't fit in this freeblock, try the next one.
        if amount > size {
            if next == 0 {
                // No next -> can't fit.
                return Ok(None);
            }

            prev_block = cur_block;
            if unlikely(next <= cur) {
                return_corrupt!("Free list not in ascending order");
            }
            cur_block = Some(next);
            continue;
        }

        let new_size = size - amount;
        // If the freeblock's new size is < CELL_SIZE_MIN, the freeblock is deleted and the remaining bytes
        // become fragmented free bytes.
        if new_size < CELL_SIZE_MIN {
            if page_ref.num_frag_free_bytes() > 57 {
                // SQLite has a fragmentation limit of 60 bytes.
                // check sqlite docs https://www.sqlite.org/fileformat.html#:~:text=A%20freeblock%20requires,not%20exceed%2060
                return Ok(None);
            }
            // Delete the slot from freelist and update the page's fragment count.
            match prev_block {
                Some(prev) => {
                    let prev_u16: u16 = prev
                        .try_into()
                        .unwrap_or_else(|_| panic!("prev={prev} is too large to fit in a u16"));
                    let next_u16: u16 = next
                        .try_into()
                        .unwrap_or_else(|_| panic!("next={next} is too large to fit in a u16"));
                    page_ref.write_freeblock_next_ptr(prev_u16, next_u16);
                }
                None => {
                    let next_u16: u16 = next
                        .try_into()
                        .unwrap_or_else(|_| panic!("next={next} is too large to fit in a u16"));
                    page_ref.write_first_freeblock(next_u16);
                }
            }
            let new_size_u8: u8 = new_size
                .try_into()
                .unwrap_or_else(|_| panic!("new_size={new_size} is too large to fit in a u8"));
            let frag = page_ref.num_frag_free_bytes() + new_size_u8;
            page_ref.write_fragmented_bytes_count(frag);
            return Ok(cur_block);
        } else if unlikely(new_size + cur > max_start_offset) {
            return_corrupt!("Free block extends beyond page end");
        } else {
            // Requested amount fits inside the current free slot so we reduce its size
            // to account for newly allocated space.
            let cur_u16: u16 = cur
                .try_into()
                .unwrap_or_else(|_| panic!("cur={cur} is too large to fit in a u16"));
            let new_size_u16: u16 = new_size
                .try_into()
                .unwrap_or_else(|_| panic!("new_size={new_size} is too large to fit in a u16"));
            page_ref.write_freeblock_size(cur_u16, new_size_u16);
            // Return the offset immediately after the shrunk freeblock.
            return Ok(Some(cur + new_size));
        }
    }

    Ok(None)
}

pub fn btree_init_page(page: &PageRef, page_type: PageType, offset: usize, usable_space: usize) {
    // setup btree page
    let contents = page.get_contents();
    contents.overflow_cells.clear();
    tracing::debug!(
        "btree_init_page(id={}, offset={}, usable_space={})",
        page.get().id,
        offset,
        usable_space
    );
    #[cfg(debug_assertions)]
    //TODO restore format args (as the "details" last arg)
    turso_assert_eq!(
        offset,
        contents.offset(),
        "offset doesn't match computed offset for page"
    );
    let id = page_type as u8;
    contents.write_page_type(id);
    contents.write_first_freeblock(0);
    contents.write_cell_count(0);

    contents.write_cell_content_area(usable_space);

    contents.write_fragmented_bytes_count(0);
    contents.write_rightmost_ptr(0);

    #[cfg(debug_assertions)]
    {
        // we might get already used page from the pool. generally this is not a problem because
        // b tree access is very controlled. However, for encrypted pages (and also checksums) we want
        // to ensure that there are no reserved bytes that contain old data.
        let buf = contents.as_ptr();
        let buffer_len = buf.len();
        turso_assert!(
            usable_space <= buffer_len,
            "usable_space must be <= buffer_len"
        );
        // this is no op if usable_space == buffer_len
        buf[usable_space..buffer_len].fill(0);
    }
}

fn to_static_buf(buf: &mut [u8]) -> &'static mut [u8] {
    unsafe { std::mem::transmute::<&mut [u8], &'static mut [u8]>(buf) }
}

fn edit_page(
    page: &mut PageContent,
    start_old_cells: usize,
    start_new_cells: usize,
    number_new_cells: usize,
    cell_array: &CellArray,
    usable_space: usize,
) -> Result<()> {
    tracing::debug!(
        "edit_page start_old_cells={} start_new_cells={} number_new_cells={} cell_array={}",
        start_old_cells,
        start_new_cells,
        number_new_cells,
        cell_array.cell_payloads.len()
    );
    let end_old_cells = start_old_cells + page.cell_count() + page.overflow_cells.len();
    let end_new_cells = start_new_cells + number_new_cells;
    let mut count_cells = page.cell_count();
    if start_old_cells < start_new_cells {
        debug_validate_cells!(page, usable_space);
        let number_to_shift = page_free_array(
            page,
            start_old_cells,
            start_new_cells - start_old_cells,
            cell_array,
            usable_space,
        )?;
        // shift pointers left
        shift_cells_left(page, count_cells, number_to_shift);
        count_cells -= number_to_shift;
        debug_validate_cells!(page, usable_space);
    }
    if end_new_cells < end_old_cells {
        debug_validate_cells!(page, usable_space);
        let number_tail_removed = page_free_array(
            page,
            end_new_cells,
            end_old_cells - end_new_cells,
            cell_array,
            usable_space,
        )?;
        turso_assert_greater_than_or_equal!(count_cells, number_tail_removed);
        count_cells -= number_tail_removed;
        debug_validate_cells!(page, usable_space);
    }
    // TODO: make page_free_array defragment, for now I'm lazy so this will work for now.
    let mut defragmented_page = defragment_page_for_insert(page, usable_space, 0)?;
    // TODO: add to start
    if start_new_cells < start_old_cells {
        let count = number_new_cells.min(start_old_cells - start_new_cells);
        page_insert_array(
            &mut defragmented_page,
            start_new_cells,
            count,
            cell_array,
            0,
            usable_space,
        )?;
        count_cells += count;
    }
    // TODO: overflow cells
    debug_validate_cells!(defragmented_page.0, usable_space);
    for i in 0..defragmented_page.0.overflow_cells.len() {
        let overflow_cell = &defragmented_page.0.overflow_cells[i];
        // cell index in context of new list of cells that should be in the page
        if start_old_cells + overflow_cell.index >= start_new_cells {
            let cell_idx = start_old_cells + overflow_cell.index - start_new_cells;
            if cell_idx < number_new_cells {
                count_cells += 1;
                page_insert_array(
                    &mut defragmented_page,
                    start_new_cells + cell_idx,
                    1,
                    cell_array,
                    cell_idx,
                    usable_space,
                )?;
            }
        }
    }
    debug_validate_cells!(defragmented_page.0, usable_space);
    // TODO: append cells to end
    page_insert_array(
        &mut defragmented_page,
        start_new_cells + count_cells,
        number_new_cells - count_cells,
        cell_array,
        count_cells,
        usable_space,
    )?;
    debug_validate_cells!(defragmented_page.0, usable_space);
    // TODO: noverflow
    page.write_cell_count(number_new_cells as u16);
    Ok(())
}

/// Shifts the cell pointers in the B-tree page to the left by a specified number of positions.
///
/// # Parameters
/// - `page`: A mutable reference to the `PageContent` representing the B-tree page.
/// - `count_cells`: The total number of cells currently in the page.
/// - `number_to_shift`: The number of cell pointers to shift to the left.
///
/// # Behavior
/// This function modifies the cell pointer array within the page by copying memory regions.
/// It shifts the pointers starting from `number_to_shift` to the beginning of the array,
/// effectively removing the first `number_to_shift` pointers.
fn shift_cells_left(page: &mut PageContent, count_cells: usize, number_to_shift: usize) {
    let buf = page.as_ptr();
    let (start, _) = page.cell_pointer_array_offset_and_size();
    buf.copy_within(
        start + (number_to_shift * 2)..start + (count_cells * 2),
        start,
    );
}

fn page_free_array(
    page: &mut PageContent,
    first: usize,
    count: usize,
    cell_array: &CellArray,
    usable_space: usize,
) -> Result<usize> {
    tracing::debug!("page_free_array {}..{}", first, first + count);
    let buf = &mut page.as_ptr()[page.offset()..usable_space];
    let buf_range = buf.as_ptr_range();
    let mut number_of_cells_removed = 0;
    let mut number_of_cells_buffered = 0;
    let mut buffered_cells_offsets: [usize; 10] = [0; 10];
    let mut buffered_cells_ends: [usize; 10] = [0; 10];
    for i in first..first + count {
        let cell = &cell_array.cell_payloads[i];
        let cell_pointer = cell.as_ptr_range();
        // check if not overflow cell
        if cell_pointer.start >= buf_range.start && cell_pointer.start < buf_range.end {
            turso_assert!(
                cell_pointer.end >= buf_range.start && cell_pointer.end <= buf_range.end,
                "whole cell should be inside the page"
            );
            // TODO: remove pointer too
            let offset = cell_pointer.start as usize - buf_range.start as usize;
            let len = cell_pointer.end as usize - cell_pointer.start as usize;
            turso_assert_greater_than!(len, 0, "cell size should be greater than 0");
            let end = offset + len;

            /* Try to merge the current cell with a contiguous buffered cell to reduce the number of
             * `free_cell_range()` operations. Break on the first merge to avoid consuming too much time,
             * `free_cell_range()` will try to merge contiguous cells anyway. */
            let mut j = 0;
            while j < number_of_cells_buffered {
                // If the buffered cell is immediately after the current cell
                if buffered_cells_offsets[j] == end {
                    // Merge them by updating the buffered cell's offset to the current cell's offset
                    buffered_cells_offsets[j] = offset;
                    break;
                // If the buffered cell is immediately before the current cell
                } else if buffered_cells_ends[j] == offset {
                    // Merge them by updating the buffered cell's end offset to the current cell's end offset
                    buffered_cells_ends[j] = end;
                    break;
                }
                j += 1;
            }
            // If no cells were merged
            if j >= number_of_cells_buffered {
                // If the buffered cells array is full, flush the buffered cells using `free_cell_range()` to empty the array
                if number_of_cells_buffered >= buffered_cells_offsets.len() {
                    for j in 0..number_of_cells_buffered {
                        free_cell_range(
                            page,
                            buffered_cells_offsets[j],
                            buffered_cells_ends[j] - buffered_cells_offsets[j],
                            usable_space,
                        )?;
                    }
                    number_of_cells_buffered = 0; // Reset array counter
                }
                // Buffer the current cell
                buffered_cells_offsets[number_of_cells_buffered] = offset;
                buffered_cells_ends[number_of_cells_buffered] = end;
                number_of_cells_buffered += 1;
            }
            number_of_cells_removed += 1;
        }
    }
    for j in 0..number_of_cells_buffered {
        free_cell_range(
            page,
            buffered_cells_offsets[j],
            buffered_cells_ends[j] - buffered_cells_offsets[j],
            usable_space,
        )?;
    }
    page.write_cell_count(page.cell_count() as u16 - number_of_cells_removed as u16);
    Ok(number_of_cells_removed)
}

/// A proof type that guarantees a page has been defragmented.
///
/// This type can only be constructed by calling [`defragment_page_for_insert`],
/// which ensures the page has been defragmented before any insert operations.
/// Functions like [`page_insert_array`] require this type to enforce at compile-time
/// that defragmentation has occurred.
pub struct DefragmentedPage<'a>(&'a mut PageContent);

impl std::ops::Deref for DefragmentedPage<'_> {
    type Target = PageContent;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl std::ops::DerefMut for DefragmentedPage<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

/// Insert multiple cells into a page in a single batch operation.
///
/// This is an optimized version that avoids O(N²) complexity by:
/// 1. Computing total space needed upfront
/// 2. Allocating all space at once
/// 3. Copying all cell payloads sequentially
/// 4. Shifting existing cell pointers once
/// 5. Writing all new cell pointers in one pass
/// 6. Updating cell count once
fn page_insert_array(
    page: &mut DefragmentedPage,
    first: usize,
    count: usize,
    cell_array: &CellArray,
    start_insert: usize,
    _usable_space: usize,
) -> Result<()> {
    if count == 0 {
        return Ok(());
    }

    tracing::debug!(
        "page_insert_array(first={}, count={}, start_insert={}, cell_count={}, page_type={:?})",
        first,
        count,
        start_insert,
        page.cell_count(),
        page.page_type().ok()
    );

    turso_assert!(first <= cell_array.cell_payloads.len(), "first OOB");
    turso_assert!(
        count <= cell_array.cell_payloads.len().saturating_sub(first),
        "first+count OOB"
    );
    // Calculate total space needed for all cell payloads
    // We read from cell_array at indices [first, first+count)
    let mut total_payload_size: usize = 0;
    for i in 0..count {
        let payload = &cell_array.cell_payloads[first + i];
        let cell_size = payload.len().max(MINIMUM_CELL_SIZE);
        total_payload_size += cell_size;
    }

    // Total space needed includes cell pointers
    let total_ptr_space = count.checked_mul(CELL_PTR_SIZE_BYTES).ok_or_else(|| {
        mark_unlikely();
        LimboError::Corrupt("page_insert_array: ptr space overflow".into())
    })?;

    // After defragmentation, all free space is in the unallocated region
    // between the cell pointer array and the cell content area.
    let current_cell_count = page.cell_count();
    let mut cell_content_area = page.cell_content_area() as usize;
    let unallocated_start = page.unallocated_region_start();
    turso_assert!(
        start_insert <= current_cell_count,
        "start_insert beyond cell_count"
    );
    turso_assert!(
        // we cast to u16 later so assert no overflow
        current_cell_count + count <= u16::MAX as usize,
        "cell_count overflow"
    );
    // Verify we have enough space
    // The new cell pointers will extend the cell pointer array by `total_ptr_space`
    // The new cell content will reduce cell_content_area by `total_payload_size`
    let new_unallocated_start =
        unallocated_start
            .checked_add(total_ptr_space)
            .ok_or_else(|| {
                mark_unlikely();
                LimboError::Corrupt("page_insert_array: unalloc start overflow".into())
            })?;
    let new_cell_content_area = cell_content_area
        .checked_sub(total_payload_size)
        .ok_or_else(|| {
            mark_unlikely();
            LimboError::Corrupt("page_insert_array: payload underflow".to_string())
        })?;

    turso_assert!(
        new_unallocated_start <= new_cell_content_area,
        "page_insert_array: not enough space for pointers and payloads in unallocated region",
        { "total_ptr_space": total_ptr_space, "total_payload_size": total_payload_size, "unallocated_start": unallocated_start, "cell_content_area": cell_content_area, "unallocated_region_size": cell_content_area - unallocated_start }
    );

    let buf = page.as_ptr();
    let (cell_pointer_array_start, _) = page.cell_pointer_array_offset_and_size();

    // Shift existing cell pointers to make room for new ones
    // We're inserting `count` cells at position `start_insert`, so we need to shift
    // all cell pointers from position `start_insert` onwards to the right by `count * 2` bytes
    if start_insert < current_cell_count {
        let cells_to_shift = current_cell_count - start_insert;
        let shift_src_start = cell_pointer_array_start + (start_insert * CELL_PTR_SIZE_BYTES);
        let shift_dst_start = shift_src_start + total_ptr_space;
        let shift_size = cells_to_shift * CELL_PTR_SIZE_BYTES;
        buf.copy_within(
            shift_src_start..shift_src_start + shift_size,
            shift_dst_start,
        );
    }

    // Allocate space for all cells and write payloads + pointers
    // We allocate space from the content area (which grows downward)
    // Read from cell_array[first..first+count], insert at page positions [start_insert..start_insert+count]
    for i in 0..count {
        let payload = &cell_array.cell_payloads[first + i];
        let cell_size = payload.len().max(MINIMUM_CELL_SIZE);

        // Allocate space for this cell (grow content area downward)
        cell_content_area = cell_content_area.checked_sub(cell_size).ok_or_else(|| {
            mark_unlikely();
            LimboError::Corrupt("page_insert_array: cell allocation underflow".to_string())
        })?;

        // Copy cell payload
        buf[cell_content_area..cell_content_area + payload.len()].copy_from_slice(payload);

        // Write cell pointer at position (start_insert + i)
        let ptr_offset = cell_pointer_array_start + ((start_insert + i) * CELL_PTR_SIZE_BYTES);
        page.write_u16_no_offset(ptr_offset, cell_content_area as u16);
    }

    // Update page header
    page.write_cell_content_area(cell_content_area);
    page.write_cell_count((current_cell_count + count) as u16);

    debug_validate_cells!(page, _usable_space);
    Ok(())
}

/// Free the range of bytes that a cell occupies.
/// This function also updates the freeblock list in the page.
/// Freeblocks are used to keep track of free space in the page,
/// and are organized as a linked list.
///
/// This function may merge the freed cell range into either the next freeblock,
/// previous freeblock, or both.
fn free_cell_range(
    page: &mut PageContent,
    mut offset: usize,
    len: usize,
    usable_space: usize,
) -> Result<()> {
    const CELL_SIZE_MIN: usize = 4;
    if unlikely(len < CELL_SIZE_MIN) {
        return_corrupt!("free_cell_range: minimum cell size is {CELL_SIZE_MIN}");
    }
    if unlikely(offset > usable_space.saturating_sub(CELL_SIZE_MIN)) {
        return_corrupt!("free_cell_range: start offset beyond usable space: offset={offset} usable_space={usable_space}");
    }

    let mut size = len;
    let mut end = offset + len;
    if unlikely(end > usable_space) {
        return_corrupt!("free_cell_range: freed range extends beyond usable space: offset={offset} len={len} end={end} usable_space={usable_space}");
    }
    let cur_content_area = page.cell_content_area() as usize;
    let first_block = page.first_freeblock() as usize;
    if first_block == 0 {
        if unlikely(offset < cur_content_area) {
            return_corrupt!("free_cell_range: free block before content area: offset={offset} cell_content_area={cur_content_area}");
        }
        if offset == cur_content_area {
            // if the freeblock list is empty and the freed range is exactly at the beginning of the content area,
            // we are not creating a freeblock; instead we are just extending the unallocated region.
            page.write_cell_content_area(end);
        } else {
            // otherwise we set it as the first freeblock in the page header.
            let offset_u16: u16 = offset
                .try_into()
                .unwrap_or_else(|_| panic!("offset={offset} is too large to fit in a u16"));
            page.write_first_freeblock(offset_u16);
            let size_u16: u16 = size
                .try_into()
                .unwrap_or_else(|_| panic!("size={size} is too large to fit in a u16"));
            page.write_freeblock(offset_u16, size_u16, None);
        }
        return Ok(());
    }

    // if the freeblock list is not empty, we need to find the correct position to insert the new freeblock
    // resulting from the freeing of this cell range; we may be also able to merge the freed range into existing freeblocks.
    let mut prev_block = None;
    let mut next_block = Some(first_block);

    while let Some(next) = next_block {
        if unlikely(prev_block.is_some_and(|prev| next <= prev)) {
            return_corrupt!("free_cell_range: freeblocks not in ascending order: next_block={next} prev_block={prev_block:?}");
        }
        if next >= offset {
            break;
        }
        prev_block = Some(next);
        next_block = match page.read_u16_no_offset(next) {
            // Freed range extends beyond the last freeblock, so we are creating a new freeblock.
            0 => None,
            next => Some(next as usize),
        };
    }

    if let Some(next) = next_block {
        if unlikely(next + CELL_SIZE_MIN > usable_space) {
            return_corrupt!("free_cell_range: free block beyond usable space: next_block={next} usable_space={usable_space}");
        }
    }
    let mut removed_fragmentation = 0;
    const SINGLE_FRAGMENT_SIZE_MAX: usize = CELL_SIZE_MIN - 1;

    // If the freed range extends into the next freeblock, we will merge the freed range into it.
    // If there is a 1-3 byte gap between the freed range and the next freeblock, we are effectively
    // clearing that amount of fragmented bytes, since a 1-3 byte range cannot be a valid cell.
    if let Some(next) = next_block {
        if end + SINGLE_FRAGMENT_SIZE_MAX >= next {
            if unlikely(end > next) {
                return_corrupt!("free_cell_range: freed range overlaps next freeblock: end={end} next_block={next}");
            }
            removed_fragmentation = (next - end) as u8;
            let next_size = page.read_u16_no_offset(next + 2) as usize;
            end = next + next_size;
            if unlikely(end > usable_space) {
                return_corrupt!("free_cell_range: coalesced block extends beyond page: offset={offset} len={len} end={end} usable_space={usable_space}");
            }
            size = end - offset;
            // Since we merged the two freeblocks, we need to update the next_block to the next freeblock in the list.
            next_block = match page.read_u16_no_offset(next) {
                0 => None,
                next => Some(next as usize),
            };
        }
    }

    // If the freed range extends into the previous freeblock, we will merge them similarly as above.
    if let Some(prev) = prev_block {
        let prev_size = page.read_u16_no_offset(prev + 2) as usize;
        let prev_end = prev + prev_size;
        if unlikely(prev_end > offset) {
            return_corrupt!(
                "free_cell_range: previous block overlap: prev_end={prev_end} offset={offset}"
            );
        }
        // If the previous freeblock extends into the freed range, we will merge the freed range into the
        // previous freeblock and clear any 1-3 byte fragmentation in between, similarly as above
        if prev_end + SINGLE_FRAGMENT_SIZE_MAX >= offset {
            removed_fragmentation += (offset - prev_end) as u8;
            size = end - prev;
            offset = prev;
        }
    }

    let cur_frag_free_bytes = page.num_frag_free_bytes();
    if unlikely(removed_fragmentation > cur_frag_free_bytes) {
        return_corrupt!("free_cell_range: invalid fragmentation count: removed_fragmentation={removed_fragmentation} num_frag_free_bytes={cur_frag_free_bytes}");
    }
    let frag = cur_frag_free_bytes - removed_fragmentation;
    page.write_fragmented_bytes_count(frag);

    if unlikely(offset < cur_content_area) {
        return_corrupt!("free_cell_range: free block before content area: offset={offset} cell_content_area={cur_content_area}");
    }

    // As above, if the freed range is exactly at the beginning of the content area, we are not creating a freeblock;
    // instead we are just extending the unallocated region.
    if offset == cur_content_area {
        if unlikely(prev_block.is_some_and(|prev| prev != first_block)) {
            return_corrupt!("free_cell_range: invalid content area merge - freed range should have been merged with previous freeblock: prev={prev_block:?} first_block={first_block}");
        }
        // If we get here, we are freeing data from the left end of the content area,
        // so we are extending the unallocated region instead of creating a freeblock.
        // We update the first freeblock to be the next one, and shrink the content area to start from the end
        // of the freed range.
        match next_block {
            Some(next) => {
                if unlikely(next <= end) {
                    return_corrupt!("free_cell_range: invalid content area merge - first freeblock should either be 0 or greater than the content area start: next_block={next} end={end}");
                }
                let next_u16: u16 = next
                    .try_into()
                    .unwrap_or_else(|_| panic!("next={next} is too large to fit in a u16"));
                page.write_first_freeblock(next_u16);
            }
            None => {
                page.write_first_freeblock(0);
            }
        }
        page.write_cell_content_area(end);
    } else {
        // If we are creating a new freeblock:
        // a) if it's the first one, we update the header to indicate so,
        // b) if it's not the first one, we update the previous freeblock to point to the new one,
        //    and the new one to point to the next one.
        let offset_u16: u16 = offset
            .try_into()
            .unwrap_or_else(|_| panic!("offset={offset} is too large to fit in a u16"));
        if let Some(prev) = prev_block {
            page.write_u16_no_offset(prev, offset_u16);
        } else {
            page.write_first_freeblock(offset_u16);
        }
        let size_u16: u16 = size
            .try_into()
            .unwrap_or_else(|_| panic!("size={size} is too large to fit in a u16"));
        let next_block_u16 = next_block.map(|b| {
            b.try_into()
                .unwrap_or_else(|_| panic!("next_block={b} is too large to fit in a u16"))
        });
        page.write_freeblock(offset_u16, size_u16, next_block_u16);
    }

    Ok(())
}

/// This function handles pages with two or fewer freeblocks and max_frag_bytes (parameter to defragment_page())
/// or fewer fragmented bytes. In this case it is faster to move the two (or one)
/// blocks of cells using memmove() and add the required offsets to each pointer
/// in the cell-pointer array than it is to reconstruct the entire page.
/// Note that this function will leave max_frag_bytes as is, it will not try to reduce it.
fn defragment_page_fast(
    page: &PageContent,
    usable_space: usize,
    freeblock_1st: usize,
    freeblock_2nd: usize,
) -> Result<()> {
    if unlikely(freeblock_1st == 0) {
        return_corrupt!("defragment_page_fast: expected at least one freeblock");
    }
    if unlikely(freeblock_2nd > 0 && freeblock_1st >= freeblock_2nd) {
        return_corrupt!(
            "defragment_page_fast: first freeblock must be before second freeblock: freeblock_1st={freeblock_1st} freeblock_2nd={freeblock_2nd}"
        );
    }
    const FREEBLOCK_SIZE_MIN: usize = 4;
    if unlikely(freeblock_1st > usable_space - FREEBLOCK_SIZE_MIN) {
        return_corrupt!(
            "defragment_page_fast: first freeblock beyond usable space: freeblock_1st={freeblock_1st} usable_space={usable_space}"
        );
    }
    if unlikely(freeblock_2nd > usable_space - FREEBLOCK_SIZE_MIN) {
        return_corrupt!(
            "defragment_page_fast: second freeblock beyond usable space: freeblock_2nd={freeblock_2nd} usable_space={usable_space}"
        );
    }

    let freeblock_1st_size = page.read_u16_no_offset(freeblock_1st + 2) as usize;
    let freeblock_2nd_size = if freeblock_2nd > 0 {
        page.read_u16_no_offset(freeblock_2nd + 2) as usize
    } else {
        0
    };
    let freeblocks_total_size = freeblock_1st_size + freeblock_2nd_size;

    let cell_content_area = page.cell_content_area() as usize;

    if freeblock_2nd > 0 {
        // If there's 2 freeblocks, merge them into one first.
        if unlikely(freeblock_1st + freeblock_1st_size > freeblock_2nd) {
            return_corrupt!(
                "defragment_page_fast: overlapping freeblocks: freeblock_1st={freeblock_1st} freeblock_1st_size={freeblock_1st_size} freeblock_2nd={freeblock_2nd}"
            );
        }
        if unlikely(freeblock_2nd + freeblock_2nd_size > usable_space) {
            return_corrupt!(
                "defragment_page_fast: second freeblock extends beyond usable space: freeblock_2nd={freeblock_2nd} freeblock_2nd_size={freeblock_2nd_size} usable_space={usable_space}"
            );
        }
        let buf = page.as_ptr();
        // Effectively moves everything in between the two freeblocks rightwards by the length of the 2nd freeblock,
        // so that the first freeblock size becomes `freeblocks_total_size` (merging the two freeblocks)
        // and the second freeblock gets overwritten by non-free cell data.
        // Illustrative doodle:
        // | content area start |--cell content A--| 1st free |--cell content B--| 2nd free |--cell content C--|
        // ->
        // | content area start |--cell content A--|      merged free    |--cell content B--|--cell content C--|
        let after_first_freeblock = freeblock_1st + freeblock_1st_size;
        let copy_amount = freeblock_2nd - after_first_freeblock;
        buf.copy_within(
            after_first_freeblock..after_first_freeblock + copy_amount,
            freeblock_1st + freeblocks_total_size,
        );
    } else if unlikely(freeblock_1st + freeblock_1st_size > usable_space) {
        return_corrupt!(
            "defragment_page_fast: first freeblock extends beyond usable space: freeblock_1st={freeblock_1st} freeblock_1st_size={freeblock_1st_size} usable_space={usable_space}"
        );
    }

    // Now we have one freeblock somewhere in the middle of the content area, e.g.:
    // content area start |-----------| merged freeblock |-----------|
    // By moving the cells from the left of the merged free block to where the merged freeblock was, we effectively move the freeblock to the very left end of the content area,
    // meaning, it's no longer a freeblock, it's just plain old free space.
    // content area start | free space | ----------- cells ----------|
    let new_cell_content_area = cell_content_area + freeblocks_total_size;
    if unlikely(new_cell_content_area + (freeblock_1st - cell_content_area) > usable_space) {
        return_corrupt!(
            "defragment_page_fast: new cell content area extends beyond usable space: new_cell_content_area={new_cell_content_area} freeblock_1st={freeblock_1st} cell_content_area={cell_content_area} usable_space={usable_space}"
        );
    }

    let copy_amount = freeblock_1st - cell_content_area; // cells to the left of the first freeblock
    let buf = page.as_ptr();
    buf.copy_within(
        cell_content_area..cell_content_area + copy_amount,
        new_cell_content_area,
    );

    // Freeblocks are now erased since the free space is at the beginning, but we must update the cell pointer array to point to the right locations.
    let cell_count = page.cell_count();
    let cell_pointer_array_offset = page.cell_pointer_array_offset_and_size().0;
    for i in 0..cell_count {
        let ptr_offset = cell_pointer_array_offset + (i * CELL_PTR_SIZE_BYTES);
        let cell_ptr = page.read_u16_no_offset(ptr_offset) as usize;
        if cell_ptr < freeblock_1st {
            // If the cell pointer was located before the first freeblock, we need to shift it right by the size of the merged freeblock
            // since the space occupied by both the 1st and 2nd freeblocks was now moved to its left.
            let new_offset = cell_ptr + freeblocks_total_size;
            if unlikely(new_offset > usable_space) {
                return_corrupt!(
                    "defragment_page_fast: shifted cell pointer beyond usable space: new_offset={new_offset} usable_space={usable_space}"
                );
            }
            page.write_u16_no_offset(ptr_offset, (cell_ptr + freeblocks_total_size) as u16);
        } else if freeblock_2nd > 0 && cell_ptr < freeblock_2nd {
            // If the cell pointer was located between the first and second freeblock, we need to shift it right by the size of only the second freeblock,
            // since the first one was already on its left.
            let new_offset = cell_ptr + freeblock_2nd_size;
            if unlikely(new_offset > usable_space) {
                return_corrupt!(
                    "defragment_page_fast: shifted cell pointer beyond usable space: new_offset={new_offset} usable_space={usable_space}"
                );
            }
            page.write_u16_no_offset(ptr_offset, (cell_ptr + freeblock_2nd_size) as u16);
        }
    }

    // Update page header
    page.write_cell_content_area(new_cell_content_area);
    page.write_first_freeblock(0);

    debug_validate_cells!(page, usable_space);

    Ok(())
}

/// Defragment a page, and never use the fast-path algorithm.
fn defragment_page_full(page: &PageContent, usable_space: usize) -> Result<()> {
    defragment_page(page, usable_space, -1)
}

/// Defragment a page and return a proof that can be used with [`page_insert_array`].
///
/// This is the entry point for defragmentation when you need to perform insert
/// operations afterward. The returned [`DefragmentedPage`] proves at compile-time
/// that defragmentation has occurred.
///
/// For defragmentation without the type-state proof (e.g., in `allocate_cell_space`),
/// use [`defragment_page`] directly.
#[inline]
fn defragment_page_for_insert(
    page: &mut PageContent,
    usable_space: usize,
    max_frag_bytes: isize,
) -> Result<DefragmentedPage<'_>> {
    defragment_page(page, usable_space, max_frag_bytes)?;
    Ok(DefragmentedPage(page))
}

/// Defragment a page. This means packing all the cells to the end of the page.
fn defragment_page(page: &PageContent, usable_space: usize, max_frag_bytes: isize) -> Result<()> {
    debug_validate_cells!(page, usable_space);
    tracing::debug!("defragment_page (optimized in-place)");

    let cell_count = page.cell_count();
    if cell_count == 0 {
        page.write_cell_content_area(usable_space);
        page.write_first_freeblock(0);
        page.write_fragmented_bytes_count(0);
        debug_validate_cells!(page, usable_space);
        return Ok(());
    }

    // Use fast algorithm if there are at most 2 freeblocks and the total fragmented free space is less than max_frag_bytes.
    if page.num_frag_free_bytes() as isize <= max_frag_bytes {
        let freeblock_1st = page.first_freeblock() as usize;
        if freeblock_1st == 0 {
            // No freeblocks and very little if any fragmented free bytes -> no need to defragment.
            return Ok(());
        }
        let freeblock_2nd = page.read_u16_no_offset(freeblock_1st) as usize;
        if freeblock_2nd == 0 {
            return defragment_page_fast(page, usable_space, freeblock_1st, 0);
        }
        let freeblock_3rd = page.read_u16_no_offset(freeblock_2nd) as usize;
        if freeblock_3rd == 0 {
            return defragment_page_fast(page, usable_space, freeblock_1st, freeblock_2nd);
        }
    }

    // A small struct to hold cell metadata for sorting.
    // Size: 2 + 2 + 8 = 12 bytes, with alignment likely 16 bytes.
    #[derive(Clone, Copy)]
    struct CellInfo {
        old_offset: u16,
        size: u16,
        pointer_index: usize,
    }

    // Use stack allocation for the common case (most pages have <256 cells).
    // This avoids heap allocation in the hot path.
    // MAX_STACK_CELLS * 16 bytes = 4KB of stack space.
    const MAX_STACK_CELLS: usize = 256;

    // Helper function to process cells and defragment the page.
    // This is generic over the slice type to work with both stack and heap storage.
    // Cells must already be sorted by old physical offset in descending order.
    #[inline]
    fn process_cells(page: &PageContent, usable_space: usize, cells: &[CellInfo]) -> Result<()> {
        // Get direct mutable access to the page buffer.
        let buffer = page.as_ptr();
        let cell_pointer_area_offset = page.cell_pointer_array_offset();
        let first_cell_content_byte = page.unallocated_region_start();

        // Move data and update pointers.
        let mut cbrk = usable_space;
        for cell in cells.iter() {
            cbrk -= cell.size as usize;
            let new_offset = cbrk;
            let old_offset = cell.old_offset as usize;

            // Basic corruption check
            turso_assert!(
                new_offset >= first_cell_content_byte && old_offset + cell.size as usize <= usable_space,
                "corrupt page detected during defragmentation",
                { "new_offset": new_offset, "first_cell_content_byte": first_cell_content_byte, "old_offset": old_offset, "cell_size": cell.size, "usable_space": usable_space }
            );

            // Move the cell data. `copy_within` is the idiomatic and safe
            // way to perform a `memmove` operation on a slice.
            if new_offset != old_offset {
                let src_range = old_offset..(old_offset + cell.size as usize);
                buffer.copy_within(src_range, new_offset);
            }

            // Update the pointer in the cell pointer array to the new offset.
            let pointer_location = cell_pointer_area_offset + (cell.pointer_index * 2);
            turso_assert!(
                new_offset < PageSize::MAX as usize,
                "new_offset exceeds PageSize::MAX",
                { "new_offset": new_offset, "page_size_max": PageSize::MAX }
            );
            page.write_u16_no_offset(pointer_location, new_offset as u16);
        }

        page.write_cell_content_area(cbrk);
        page.write_first_freeblock(0);
        page.write_fragmented_bytes_count(0);
        Ok(())
    }

    // Gather cell metadata.
    let cell_offset = page.cell_pointer_array_offset();
    let mut is_physically_sorted = true;
    let mut last_offset = u16::MAX;

    // Pre-compute page-level constants for cell_get_raw_region_faster.
    // These are the same for all cells on the page, so computing them once
    // avoids redundant work in the loop.
    let page_type = page.page_type()?;
    let max_local = payload_overflow_threshold_max(page_type, usable_space);
    let min_local = payload_overflow_threshold_min(page_type, usable_space);

    let mut cells = SmallVec::<[CellInfo; MAX_STACK_CELLS]>::with_capacity(cell_count);
    for i in 0..cell_count {
        let pc = page.read_u16_no_offset(cell_offset + (i * 2));
        let (_, size) = page._cell_get_raw_region_faster(
            i,
            usable_space,
            cell_count,
            max_local,
            min_local,
            page_type,
        )?;

        if pc > last_offset {
            is_physically_sorted = false;
        }
        last_offset = pc;

        cells.push(CellInfo {
            old_offset: pc,
            size: size as u16,
            pointer_index: i,
        });
    }

    // Sort once, descending by old physical offset — the order the copy
    // loop in `process_cells` needs. Using unstable sort is fine as the
    // original order doesn't matter.
    if !is_physically_sorted {
        cells.sort_unstable_by(|a, b| b.old_offset.cmp(&a.old_offset));
    }

    // Cells below the pointer array or overlapping each other mean the
    // page is corrupt (each region was individually bounds-checked above).
    // Moving them would compound the damage: defragmentation interleaves
    // pointer-array writes with cell copies, so a cell sourced from inside
    // the pointer array — or from another cell's region — gets its bytes
    // rewritten before or while it is copied. Walking the descending sort
    // in reverse checks the regions in ascending offset order.
    let first_allowed_cell_offset = cell_offset + 2 * cell_count;
    let mut previous_end = first_allowed_cell_offset;
    for cell in cells.iter().rev() {
        if (cell.old_offset as usize) < previous_end {
            return Err(LimboError::Corrupt(format!(
                "page has overlapping or misplaced cells at offset {}",
                cell.old_offset
            )));
        }
        previous_end = cell.old_offset as usize + cell.size as usize;
    }

    process_cells(page, usable_space, &cells)?;
    debug_validate_cells!(page, usable_space);
    Ok(())
}

#[cfg(debug_assertions)]
/// Only enabled in debug mode, where we ensure that all cells are valid.
fn debug_validate_cells_core(page: &PageContent, usable_space: usize) {
    let pointer_array_end = page.offset() + page.header_size() + 2 * page.cell_count();
    turso_assert_greater_than_or_equal!(
        page.cell_content_area() as usize,
        pointer_array_end,
        "cell content area overlaps cell pointer array"
    );
    for i in 0..page.cell_count() {
        let (offset, size) = page.cell_get_raw_region(i, usable_space).unwrap();
        turso_assert_greater_than_or_equal!(
            offset,
            pointer_array_end,
            "cell overlaps cell pointer array",
            { "idx": i }
        );
        let _buf = &page.as_ptr()[offset..offset + size];
        // E.g. the following table btree cell may just have two bytes:
        // Payload size 0 (stored as SerialTypeKind::ConstInt0)
        // Rowid 1 (stored as SerialTypeKind::ConstInt1)
        turso_assert_greater_than_or_equal!(
            size, 2,
            "cell size should be at least 2 bytes",
            { "idx": i, "offset": offset, "buf": _buf }
        );
        if page.is_leaf() {
            turso_assert!(page.as_ptr()[offset] != 0);
        }
        turso_assert_less_than_or_equal!(
            offset + size,
            usable_space,
            "cell spans out of usable space"
        );
    }
}

/// Insert a record into a cell.
/// If the cell overflows, an overflow cell is created.
/// insert_into_cell() is called from insert_into_page(),
/// and the overflow cell count is used to determine if the page overflows,
/// i.e. whether we need to balance the btree after the insert.
fn _insert_into_cell(
    page: &mut PageContent,
    payload: &[u8],
    cell_idx: usize,
    usable_space: usize,
    allow_regular_insert_despite_overflow: bool, // used during balancing to allow regular insert despite overflow cells
) -> Result<()> {
    turso_assert_less_than_or_equal!(
        cell_idx, page.cell_count() + page.overflow_cells.len(),
        "attempting to add cell to incorrect place",
        { "cell_idx": cell_idx, "cell_count": page.cell_count(), "overflow_count": page.overflow_cells.len(), "page_type": format!("{:?}", page.page_type()) }
    );
    let already_has_overflow = !page.overflow_cells.is_empty();
    let free = compute_free_space(page, usable_space)?;
    let enough_space = if already_has_overflow && !allow_regular_insert_despite_overflow {
        false
    } else {
        // otherwise, we need to check if we have enough space.
        // allocate_cell_space() never allocates less than MINIMUM_CELL_SIZE
        // bytes, so a smaller payload must reserve that much or the cell
        // content area slides into the cell pointer array.
        payload.len().max(MINIMUM_CELL_SIZE) + CELL_PTR_SIZE_BYTES <= free
    };
    if !enough_space {
        #[cfg(debug_assertions)]
        {
            if let Some(overflow_cell) = page.overflow_cells.last() {
                turso_assert!(overflow_cell.index + 1 == cell_idx, "multiple overflow cells can only occur when a parent overflows during balancing as divider cells are inserted into it. those cells should always be in-order and sequential", { "page_id": page.id, "last_overflow_index": overflow_cell.index, "cell_idx": cell_idx, "cell_count": page.cell_count(), "overflow_count": page.overflow_cells.len() });
            }
        }
        let mut payload = crate::with_btree_allocation_site!(OverflowCell, payload.try_to_vec())?;
        if page.page_type()? == PageType::IndexLeaf {
            // Balancing must see one size for every leaf cell, whether it sits on the page
            // (where cell_get_raw_region reports at least the minimum size) or overflowed.
            ensure_min_cell_size(&mut payload, 0);
        }
        let overflow_cell = OverflowCell {
            index: cell_idx,
            payload: Pin::new(payload),
        };
        crate::with_btree_allocation_site!(
            OverflowCell,
            page.overflow_cells.try_push(overflow_cell)
        )?;
        return Ok(());
    }
    turso_assert_less_than_or_equal!(
        cell_idx,
        page.cell_count(),
        "cell_idx > cell_count without overflow cells"
    );

    let new_cell_data_pointer = allocate_cell_space(page, payload.len(), usable_space, free)?;
    tracing::debug!(
        "insert_into_cell(idx={}, pc={}, size={})",
        cell_idx,
        new_cell_data_pointer,
        payload.len()
    );
    turso_assert_less_than_or_equal!(new_cell_data_pointer as usize + payload.len(), usable_space);
    let buf = page.as_ptr();

    // copy data
    buf[new_cell_data_pointer as usize..new_cell_data_pointer as usize + payload.len()]
        .copy_from_slice(payload);
    //  memmove(pIns+2, pIns, 2*(pPage->nCell - i));
    let (cell_pointer_array_start, _) = page.cell_pointer_array_offset_and_size();
    let cell_pointer_cur_idx = cell_pointer_array_start + (CELL_PTR_SIZE_BYTES * cell_idx);

    // move existing pointers forward by CELL_PTR_SIZE_BYTES...
    let n_cells_forward = page.cell_count() - cell_idx;
    let n_bytes_forward = CELL_PTR_SIZE_BYTES * n_cells_forward;
    if n_bytes_forward > 0 {
        buf.copy_within(
            cell_pointer_cur_idx..cell_pointer_cur_idx + n_bytes_forward,
            cell_pointer_cur_idx + CELL_PTR_SIZE_BYTES,
        );
    }
    // ...and insert new cell pointer at the current index
    page.write_u16_no_offset(cell_pointer_cur_idx, new_cell_data_pointer);

    // update cell count
    let new_n_cells = (page.cell_count() + 1) as u16;
    page.write_cell_count(new_n_cells);
    debug_validate_cells!(page, usable_space);
    Ok(())
}

fn insert_into_cell(
    page: &mut PageContent,
    payload: &[u8],
    cell_idx: usize,
    usable_space: usize,
) -> Result<()> {
    _insert_into_cell(page, payload, cell_idx, usable_space, false)
}

/// Normally in [insert_into_cell()], if a page already has overflow cells, all
/// new insertions are also added to the overflow cells vector.
/// The amount of free space is the sum of:
///  #1. The size of the unallocated region
///  #2. Fragments (isolated 1-3 byte chunks of free space within the cell content area)
///  #3. freeblocks (linked list of blocks of at least 4 bytes within the cell content area that
///      are not in use due to e.g. deletions)
/// Free blocks can be zero, meaning the "real free space" that can be used to allocate is expected
/// to be between first cell byte and end of cell pointer area.
#[allow(unused_assignments)]
#[inline]
fn compute_free_space(page: &PageContent, usable_space: usize) -> Result<usize> {
    // TODO(pere): maybe free space is not calculated correctly with offset

    // Usable space, not the same as free space, simply means:
    // space that is not reserved for extensions by sqlite. Usually reserved_space is 0.

    let first_cell = page.offset() + page.header_size() + (2 * page.cell_count());
    if unlikely(first_cell > usable_space) {
        return_corrupt!(
            "compute_free_space: first_cell beyond usable space: first_cell={first_cell} usable_space={usable_space}"
        );
    }

    let cell_content_area_start = page.cell_content_area() as usize;
    if unlikely(cell_content_area_start > usable_space) {
        return_corrupt!(
            "compute_free_space: cell content area beyond usable space: cell_content_area_start={cell_content_area_start} usable_space={usable_space}"
        );
    }

    let mut free_space_bytes = cell_content_area_start + page.num_frag_free_bytes() as usize;

    // #3 is computed by iterating over the freeblocks linked list
    let mut cur_freeblock_ptr = page.first_freeblock() as usize;
    if cur_freeblock_ptr > 0 {
        if unlikely(cur_freeblock_ptr < cell_content_area_start) {
            return_corrupt!(
                "compute_free_space: first freeblock before content area: first_freeblock={cur_freeblock_ptr} cell_content_area_start={cell_content_area_start}"
            );
        }

        let mut next = 0usize;
        let mut size = 0usize;
        loop {
            if unlikely(cur_freeblock_ptr + 4 > usable_space) {
                return_corrupt!(
                    "compute_free_space: freeblock header out of bounds: cur_freeblock_ptr={cur_freeblock_ptr} usable_space={usable_space}"
                );
            }
            next = page.read_u16_no_offset(cur_freeblock_ptr) as usize; // first 2 bytes in freeblock = next freeblock pointer
            size = page.read_u16_no_offset(cur_freeblock_ptr + 2) as usize; // next 2 bytes in freeblock = size of current freeblock
            if unlikely(size < 4) {
                return_corrupt!(
                    "compute_free_space: freeblock too small: cur_freeblock_ptr={cur_freeblock_ptr} size={size}"
                );
            }
            if unlikely(cur_freeblock_ptr + size > usable_space) {
                return_corrupt!(
                    "compute_free_space: freeblock extends beyond page: cur_freeblock_ptr={cur_freeblock_ptr} size={size} usable_space={usable_space}"
                );
            }
            free_space_bytes += size;

            if next == 0 {
                break;
            }
            // Freeblocks are in order from left to right on the page.
            if unlikely(next <= cur_freeblock_ptr + size + 3) {
                return_corrupt!(
                    "compute_free_space: freeblocks list not in ascending order: cur_freeblock_ptr={cur_freeblock_ptr} size={size} next={next}"
                );
            }
            cur_freeblock_ptr = next;
        }
    }

    if unlikely(free_space_bytes > usable_space) {
        return_corrupt!(
            "compute_free_space: free space greater than usable space: free_space_bytes={free_space_bytes} usable_space={usable_space}"
        );
    }
    if unlikely(free_space_bytes < first_cell) {
        return_corrupt!(
            "compute_free_space: free space underflow: free_space_bytes={free_space_bytes} first_cell={first_cell} usable_space={usable_space}"
        );
    }

    Ok(free_space_bytes - first_cell)
}

/// Allocate space for a cell on a page.
#[inline]
fn allocate_cell_space(
    page_ref: &PageContent,
    mut amount: usize,
    usable_space: usize,
    free_space: usize,
) -> Result<u16> {
    if amount < MINIMUM_CELL_SIZE {
        amount = MINIMUM_CELL_SIZE;
    }

    let unallocated_region_start = page_ref.unallocated_region_start();
    let mut cell_content_area_start = page_ref.cell_content_area() as usize;

    // there are free blocks and enough space to fit a new 2-byte cell pointer
    if page_ref.first_freeblock() != 0
        && unallocated_region_start + CELL_PTR_SIZE_BYTES <= cell_content_area_start
    {
        // find slot
        if let Some(pc) = find_free_slot(page_ref, usable_space, amount)? {
            // we can fit the cell in a freeblock.
            return Ok(pc as u16);
        }
        /* fall through, we might need to defragment */
    }

    // We know at this point that we have no freeblocks in the middle of the cell content area
    // that can fit the cell, but we do know we have enough space to _somehow_ fit it.
    // The check below sees whether we can just put the cell in the unallocated region.
    if unallocated_region_start + CELL_PTR_SIZE_BYTES + amount > cell_content_area_start {
        // There's no room in the unallocated region, so we need to defragment.
        // max_frag_bytes is a parameter to defragment_page() that controls whether we are able to use
        // the fast-path defragmentation. The calculation here is done to see whether we can merge 1-2 freeblocks
        // and move them to the unallocated region and fit the cell that way.
        // Basically: if we have exactly enough space for the cell and the cell pointer on the page,
        // we cannot have any fragmented space because then the freeblocks would not fit the cell.
        let max_frag_bytes = 4.min(free_space as isize - (CELL_PTR_SIZE_BYTES + amount) as isize);
        defragment_page(page_ref, usable_space, max_frag_bytes)?;
        cell_content_area_start = page_ref.cell_content_area() as usize;
    }

    // insert the cell -> content area start moves left by that amount.
    cell_content_area_start -= amount;
    page_ref.write_cell_content_area(cell_content_area_start);

    turso_assert_less_than_or_equal!(cell_content_area_start + amount, usable_space);
    // we can just return the start of the cell content area, since the cell is inserted to the very left of the cell content area.
    Ok(cell_content_area_start as u16)
}

#[derive(Debug, Clone)]
pub enum FillCellPayloadState {
    /// Determine whether we can fit the record on the current page.
    /// If yes, return immediately after copying the data.
    /// Otherwise move to [CopyData] state.
    Start,
    /// Copy the next chunk of data from the record buffer to the cell payload.
    /// If we can't fit all of the remaining data on the current page,
    /// move the internal state to [CopyDataState::AllocateOverflowPage]
    CopyData {
        /// Internal state of the copy data operation.
        /// We can either be copying data or allocating an overflow page.
        state: CopyDataState,
        /// Track how much space we have left on the current page we are copying data into.
        /// This is reset whenever a new overflow page is allocated.
        space_left_on_cur_page: usize,
        /// Offset into the record buffer to copy from.
        src_data_offset: usize,
        /// Offset into the destination buffer we are copying data into.
        /// This is either:
        /// - an offset in the btree page where the cell is, or
        /// - an offset in an overflow page
        dst_data_offset: usize,
        /// If this is Some, we will copy data into this overflow page.
        /// If this is None, we will copy data into the cell payload on the btree page.
        /// Also: to safely form a chain of overflow pages, the current page must be pinned to the page cache
        /// so that e.g. a spilling operation does not evict it to disk.
        current_overflow_page: Option<PinGuard>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum CopyDataState {
    /// Copy the next chunk of data from the record buffer to the cell payload.
    Copy,
    /// Allocate a new overflow page if we couldn't fit all data to the current page.
    AllocateOverflowPage,
}

/// Fill in the cell payload with the record.
/// If the record is too large to fit in the cell, it will spill onto overflow pages.
/// This function needs a separate [FillCellPayloadState] because allocating overflow pages
/// may require I/O.
#[allow(clippy::too_many_arguments)]
fn fill_cell_payload(
    page: &PinGuard,
    int_key: Option<i64>,
    cell_payload: &mut crate::alloc::Vec<u8>,
    cell_idx: usize,
    record: &impl AsRef<[u8]>,
    usable_space: usize,
    pager: Arc<Pager>,
    fill_cell_payload_state: &mut FillCellPayloadState,
) -> IOResultOr<()> {
    let overflow_page_pointer_size = 4;
    let overflow_page_data_size = usable_space - overflow_page_pointer_size;
    let result = loop {
        let record_buf = record.as_ref();
        match fill_cell_payload_state {
            FillCellPayloadState::Start => {
                let page_contents = page.get_contents();

                let page_type = page_contents.page_type()?;
                // fill in header
                if matches!(page_type, PageType::IndexInterior) {
                    // if a write happened on an index interior page, it is always an overwrite.
                    // we must copy the left child pointer of the replaced cell to the new cell.
                    let left_child_page =
                        page_contents.cell_interior_read_left_child_page(cell_idx)?;
                    crate::with_btree_allocation_site!(
                        CellPayload,
                        cell_payload.try_extend(left_child_page.to_be_bytes())
                    )?;
                }
                if matches!(page_type, PageType::TableLeaf) {
                    let int_key = int_key.unwrap();
                    write_varint_to_vec(record_buf.len() as u64, cell_payload)?;
                    write_varint_to_vec(int_key as u64, cell_payload)?;
                } else {
                    write_varint_to_vec(record_buf.len() as u64, cell_payload)?;
                }

                let max_local = payload_overflow_threshold_max(page_type, usable_space);
                let min_local = payload_overflow_threshold_min(page_type, usable_space);

                let (overflows, local_size_if_overflow) =
                    payload_overflows(record_buf.len(), max_local, min_local, usable_space);
                if !overflows {
                    // enough allowed space to fit inside a btree page
                    crate::with_btree_allocation_site!(
                        CellPayload,
                        cell_payload.try_extend(record_buf.iter().copied())
                    )?;
                    break Ok(IOResult::Done(()));
                }

                // so far we've written any of: left child page, rowid, payload size (depending on page type)
                let cell_non_payload_elems_size = cell_payload.len();
                let new_total_local_size = cell_non_payload_elems_size + local_size_if_overflow;
                crate::with_btree_allocation_site!(
                    CellPayload,
                    cell_payload.try_reserve(new_total_local_size - cell_payload.len())
                )?;
                cell_payload.resize(new_total_local_size, 0);

                *fill_cell_payload_state = FillCellPayloadState::CopyData {
                    state: CopyDataState::Copy,
                    space_left_on_cur_page: local_size_if_overflow - overflow_page_pointer_size, // local_size_if_overflow includes the overflow page pointer, but we don't want to write payload data there.
                    src_data_offset: 0,
                    dst_data_offset: cell_non_payload_elems_size,
                    current_overflow_page: None,
                };
                continue;
            }
            FillCellPayloadState::CopyData {
                state,
                src_data_offset,
                space_left_on_cur_page,
                dst_data_offset,
                current_overflow_page,
            } => {
                match state {
                    CopyDataState::Copy => {
                        turso_assert!(*src_data_offset < record_buf.len(), "trying to read past end of record buffer", { "src_data_offset": src_data_offset, "record_buf_len": record_buf.len() });
                        let record_offset_slice = &record_buf[*src_data_offset..];
                        let amount_to_copy =
                            (*space_left_on_cur_page).min(record_offset_slice.len());
                        let record_offset_slice_to_copy = &record_offset_slice[..amount_to_copy];
                        if let Some(cur_page) = current_overflow_page {
                            // Copy data into the current overflow page.
                            turso_assert!(
                                cur_page.is_loaded(),
                                "current overflow page is not loaded"
                            );
                            turso_assert!(*dst_data_offset == overflow_page_pointer_size, "data must be copied to overflow page pointer offset on overflow pages", { "dst_data_offset": dst_data_offset, "overflow_page_pointer_size": overflow_page_pointer_size });
                            let contents = cur_page.get_contents();
                            let buf = &mut contents.as_ptr()
                                [*dst_data_offset..*dst_data_offset + amount_to_copy];
                            buf.copy_from_slice(record_offset_slice_to_copy);
                        } else {
                            // Copy data into the cell payload on the btree page.
                            let buf = &mut cell_payload
                                [*dst_data_offset..*dst_data_offset + amount_to_copy];
                            buf.copy_from_slice(record_offset_slice_to_copy);
                        }

                        if record_offset_slice.len() - amount_to_copy == 0 {
                            break Ok(IOResult::Done(()));
                        }
                        *state = CopyDataState::AllocateOverflowPage;
                        *src_data_offset += amount_to_copy;
                    }
                    CopyDataState::AllocateOverflowPage => {
                        let new_overflow_page = match pager.allocate_overflow_page() {
                            Ok(IOResult::Done(new_overflow_page)) => {
                                PinGuard::new(new_overflow_page)
                            }
                            Ok(IOResult::IO(io_result)) => return Ok(IOResult::IO(io_result)),
                            Err(e) => {
                                mark_unlikely();
                                break Err(e);
                            }
                        };
                        turso_assert!(
                            new_overflow_page.is_loaded(),
                            "new overflow page is not loaded"
                        );
                        let new_overflow_page_id = new_overflow_page.get().id as u32;

                        if let Some(prev_page) = current_overflow_page {
                            // Update the previous overflow page's "next overflow page" pointer to point to the new overflow page.
                            turso_assert!(
                                prev_page.is_loaded(),
                                "previous overflow page is not loaded"
                            );
                            let contents = prev_page.get_contents();
                            let buf = &mut contents.as_ptr()[..overflow_page_pointer_size];
                            buf.copy_from_slice(&new_overflow_page_id.to_be_bytes());
                        } else {
                            // Update the cell payload's "next overflow page" pointer to point to the new overflow page.
                            let first_overflow_page_ptr_offset =
                                cell_payload.len() - overflow_page_pointer_size;
                            let buf = &mut cell_payload[first_overflow_page_ptr_offset
                                ..first_overflow_page_ptr_offset + overflow_page_pointer_size];
                            buf.copy_from_slice(&new_overflow_page_id.to_be_bytes());
                        }

                        *dst_data_offset = overflow_page_pointer_size;
                        *space_left_on_cur_page = overflow_page_data_size;
                        *current_overflow_page = Some(new_overflow_page.clone());
                        *state = CopyDataState::Copy;
                    }
                }
            }
        }
    };
    result
}
/// Returns the maximum payload size (X) that can be stored directly on a b-tree page without spilling to overflow pages.
///
/// For table leaf pages: X = usable_size - 35
/// For index pages: X = ((usable_size - 12) * 64/255) - 23
///
/// The usable size is the total page size less the reserved space at the end of each page.
/// These thresholds are designed to:
/// - Give a minimum fanout of 4 for index b-trees
/// - Ensure enough payload is on the b-tree page that the record header can usually be accessed
///   without consulting an overflow page
#[inline]
pub fn payload_overflow_threshold_max(page_type: PageType, usable_space: usize) -> usize {
    match page_type {
        PageType::IndexInterior | PageType::IndexLeaf => {
            ((usable_space - 12) * 64 / 255) - 23 // Index page formula
        }
        PageType::TableInterior | PageType::TableLeaf => {
            usable_space - 35 // Table leaf page formula
        }
    }
}

/// Returns the minimum payload size (M) that must be stored on the b-tree page before spilling to overflow pages is allowed.
///
/// For all page types: M = ((usable_size - 12) * 32/255) - 23
///
/// When payload size P exceeds max_local():
/// - If K = M + ((P-M) % (usable_size-4)) <= max_local(): store K bytes on page
/// - Otherwise: store M bytes on page
///
/// The remaining bytes are stored on overflow pages in both cases.
#[inline]
pub fn payload_overflow_threshold_min(_page_type: PageType, usable_space: usize) -> usize {
    // Same formula for all page types
    ((usable_space - 12) * 32 / 255) - 23
}

/// Drop a cell from a page.
/// This is done by freeing the range of bytes that the cell occupies.
#[inline]
fn drop_cell(page: &mut PageContent, cell_idx: usize, usable_space: usize) -> Result<()> {
    let (cell_start, cell_len) = page.cell_get_raw_region(cell_idx, usable_space)?;
    free_cell_range(page, cell_start, cell_len, usable_space)?;
    if page.cell_count() > 1 {
        shift_pointers_left(page, cell_idx);
    } else {
        page.write_cell_content_area(usable_space);
        page.write_first_freeblock(0);
        page.write_fragmented_bytes_count(0);
    }
    page.write_cell_count(page.cell_count() as u16 - 1);

    for overflow_cell in page.overflow_cells.iter() {
        turso_debug_assert!(
            overflow_cell.index <= cell_idx,
            "drop_cell: pending overflow cell positioned after dropped cell",
            { "overflow_index": overflow_cell.index, "cell_idx": cell_idx }
        );
    }

    debug_validate_cells!(page, usable_space);
    Ok(())
}

/// Shift pointers to the left once starting from a cell position
/// This is useful when we remove a cell and we want to move left the cells from the right to fill
/// the empty space that's not needed
#[inline]
fn shift_pointers_left(page: &mut PageContent, cell_idx: usize) {
    turso_assert_greater_than!(page.cell_count(), 0);
    let buf = page.as_ptr();
    let (start, _) = page.cell_pointer_array_offset_and_size();
    let start = start + (cell_idx * 2) + 2;
    let right_cells = page.cell_count() - cell_idx - 1;
    let amount_to_shift = right_cells * 2;
    buf.copy_within(start..start + amount_to_shift, start - 2);
}

#[cfg(test)]
mod tests {
    use crate::SqliteDialect;
    use rand::{rng, Rng};
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };
    use sorted_vec::SortedVec;
    use test_log::test;

    use super::*;
    use crate::{
        io::{Buffer, MemoryIO, OpenFlags, IO},
        mvcc::{
            yield_hooks::YieldPointMarker,
            yield_points::{YieldInjector, YieldPoint},
        },
        schema::IndexColumn,
        storage::{
            database::DatabaseFile, page_cache::PageCache, pager::default_page1,
            sqlite3_ondisk::PageSize,
        },
        types::Text,
        vdbe::Register,
        BufferPool, Completion, Connection, IOContext, StepResult, Wal, WalAutoActions, WalFile,
        WalFileShared,
    };
    use arc_swap::ArcSwapOption;
    use std::{
        mem::transmute,
        ops::Deref,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
    };

    use tempfile::TempDir;

    use crate::{
        storage::{
            btree::{compute_free_space, fill_cell_payload, payload_overflow_threshold_max},
            sqlite3_ondisk::{BTreeCell, PageContent, PageType, TableLeafCell},
        },
        types::Value,
        Database, Page, Pager, PlatformIO,
    };

    use super::{btree_init_page, defragment_page, drop_cell, insert_into_cell};

    #[derive(Debug)]
    struct TargetedYieldInjector {
        point: YieldPoint,
        selection_key: u64,
        fired: AtomicBool,
    }

    impl TargetedYieldInjector {
        fn new(point: YieldPoint, selection_key: u64) -> Arc<Self> {
            Arc::new(Self {
                point,
                selection_key,
                fired: AtomicBool::new(false),
            })
        }

        fn fired(&self) -> bool {
            self.fired.load(Ordering::Acquire)
        }
    }

    impl YieldInjector for TargetedYieldInjector {
        fn should_yield(&self, _instance_id: u64, selection_key: u64, point: YieldPoint) -> bool {
            point == self.point
                && selection_key == self.selection_key
                && !self.fired.swap(true, Ordering::AcqRel)
        }
    }

    fn btree_write_selection_key(root_page: i64) -> u64 {
        BTREE_WRITE_YIELD_FAMILY ^ root_page as u64
    }

    fn query_single_i64(conn: &Arc<Connection>, sql: &str) -> i64 {
        let mut stmt = conn.prepare(sql).unwrap();
        match stmt.step().unwrap() {
            StepResult::Row => stmt.row().unwrap().get::<i64>(0).unwrap(),
            other => panic!("expected a row, got {other:?}"),
        }
    }

    fn query_single_text(conn: &Arc<Connection>, sql: &str) -> String {
        let mut stmt = conn.prepare(sql).unwrap();
        match stmt.step().unwrap() {
            StepResult::Row => stmt.row().unwrap().get::<String>(0).unwrap(),
            other => panic!("expected a row, got {other:?}"),
        }
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn get_page(id: usize) -> PageRef {
        let page = Arc::new(Page::new(id as i64));

        {
            let inner = page.get();
            inner.buffer = Some(Arc::new(Buffer::new_temporary(4096)));
        }
        page.set_loaded();

        btree_init_page(&page, PageType::TableLeaf, 0, 4096);
        page
    }

    /// The returned `TempDir` deletes the database directory when it drops, so
    /// callers must hold it for as long as they use the database.
    #[allow(clippy::arc_with_non_send_sync)]
    fn get_database() -> (Arc<Database>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");
        {
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection
                .pragma_update(None, "journal_mode", "wal")
                .unwrap();
        }
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io.clone(), path.to_str().unwrap(), Arc::new(SqliteDialect))
            .unwrap();

        (db, temp_dir)
    }

    /// Deterministic coverage for the allocation-failure window created by routing
    /// overflow-cell payloads through `TursoAllocator`: in
    /// `OverwriteCellState::ClearOverflowPagesAndOverwrite`, `drop_cell` mutates the
    /// page before `insert_into_cell`, whose overflow-cell branch can fail on
    /// allocation. A fault there must not lose the row or corrupt the tree.
    ///
    /// Nightly-only because on stable `crate::alloc::Vec` degrades to the global
    /// allocator, so the injected fault can never fire.
    #[cfg(all(nightly, feature = "allocation_metric"))]
    mod allocation_fault_repro {
        use super::*;
        use crate::alloc::{
            current_allocation_site, AllocError, AllocationSite, ApiAllocator, BTreeAllocationSite,
            Global, Layout, TursoAllocBackend,
        };
        use std::cell::Cell as StdCell;
        use std::ptr::NonNull;
        use test_log::test;

        thread_local! {
            static FAIL_SITE: StdCell<Option<AllocationSite>> = const { StdCell::new(None) };
        }

        struct SiteFaultBackend;

        unsafe impl TursoAllocBackend for SiteFaultBackend {
            fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
                let fail = FAIL_SITE.with(|slot| {
                    let target = slot.get();
                    target.is_some() && target == current_allocation_site()
                });
                if fail {
                    return Err(AllocError);
                }
                <Global as ApiAllocator>::allocate(&Global, layout)
            }

            unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
                unsafe { <Global as ApiAllocator>::deallocate(&Global, ptr, layout) }
            }
        }

        static BACKEND: SiteFaultBackend = SiteFaultBackend;

        fn arm_fault(site: BTreeAllocationSite) {
            // The backend passes through to Global unless a fault is armed on this
            // thread, so sharing it process-wide with other tests is safe.
            let _ = unsafe { crate::alloc::set_allocator(&BACKEND) };
            FAIL_SITE.with(|slot| slot.set(Some(AllocationSite::BTree(site))));
        }

        fn disarm_fault() {
            FAIL_SITE.with(|slot| slot.set(None));
        }

        #[test]
        fn overwrite_survives_overflow_cell_allocation_failure() {
            #[allow(clippy::arc_with_non_send_sync)]
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db =
                Database::open_file(io, "overflow-cell-fault.db", Arc::new(SqliteDialect)).unwrap();
            let conn = db.connect().unwrap();

            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
                .unwrap();
            // Row 1 is the overwrite target; the filler rows pack the leaf page so
            // that growing row 1 cannot fit locally and must spill to an overflow
            // cell inside insert_into_cell.
            conn.execute(format!("INSERT INTO t VALUES (1, '{}')", "a".repeat(600)))
                .unwrap();
            for id in 2..=6 {
                conn.execute(format!(
                    "INSERT INTO t VALUES ({id}, '{}')",
                    "b".repeat(600)
                ))
                .unwrap();
            }
            let rows_before = query_single_i64(&conn, "SELECT count(*) FROM t");
            assert_eq!(rows_before, 6);

            arm_fault(BTreeAllocationSite::OverflowCell);
            let update = conn.execute(format!(
                "UPDATE t SET v = '{}' WHERE id = 1",
                "c".repeat(1400)
            ));
            disarm_fault();
            assert!(
                update.is_err(),
                "update should fail once the overflow-cell allocation is denied"
            );

            // The failed statement must not lose the row or corrupt the tree.
            assert_eq!(query_single_i64(&conn, "SELECT count(*) FROM t"), 6);
            assert_eq!(
                query_single_i64(&conn, "SELECT length(v) FROM t WHERE id = 1"),
                600
            );
            assert_eq!(query_single_text(&conn, "PRAGMA integrity_check"), "ok");
        }

        /// Regression test: the overflow-read epilogue empties the payload held
        /// inside `read_overflow_state` before the fallible record allocations
        /// run. On failure it must clear the state — same invariant the adjacent
        /// corrupt-chain branch upholds — otherwise a later call resumes against
        /// the emptied buffer and silently completes with an empty record.
        #[test]
        fn overflow_read_epilogue_clears_state_on_allocation_failure() {
            let pager = setup_test_env(5);
            let mut cursor = BTreeCursor::new_table(pager.clone(), 1, 5);
            let usable = cursor.usable_space();
            let data_per_page = usable - 4;

            // Re-purpose pages 4 and 5 as a valid 2-page overflow chain: 4 -> 5 -> end.
            let load_and_fill = |id: i64, next: u32, fill: u8| {
                let (page, c) = cursor.read_page_blocking(id).unwrap();
                if let Some(c) = c {
                    pager.io.wait_for_completion(c).unwrap();
                }
                while page.is_locked() {
                    pager.io.step().unwrap();
                }
                let buf = page.get_contents().as_ptr();
                buf[0..4].copy_from_slice(&next.to_be_bytes());
                buf[4..usable].fill(fill);
            };
            load_and_fill(4, 5, b'A');
            load_and_fill(5, 0, b'B');

            let payload_size = (2 * data_per_page) as u64;
            let cursor_pager = cursor.pager.clone();

            // Fail the epilogue's record allocation, which runs after the chain
            // has been fully read and the accumulated payload was already moved
            // out of `read_overflow_state`.
            arm_fault(BTreeAllocationSite::RecordPayload);
            run_until_done(
                || cursor.process_overflow_read(b"", 4, payload_size),
                &cursor_pager,
            )
            .expect_err("record allocation failure should surface");
            disarm_fault();

            assert!(
                cursor.read_overflow_state.is_none(),
                "failed overflow read left resumable state behind"
            );

            // A retry must rebuild the record from scratch and see the full chain.
            run_until_done(
                || cursor.process_overflow_read(b"", 4, payload_size),
                &cursor_pager,
            )
            .unwrap();
            let rec_slot = cursor.get_immutable_record_or_create().unwrap();
            let bytes = rec_slot.as_ref().unwrap().get_payload();
            assert_eq!(bytes.len(), 2 * data_per_page);
            assert!(bytes[..data_per_page].iter().all(|&b| b == b'A'));
            assert!(bytes[data_per_page..].iter().all(|&b| b == b'B'));
        }
    }

    #[cfg(nightly)]
    #[test]
    fn btree_state_buffers_use_turso_allocator() {
        fn assert_alloc_vec<T>(_: &crate::alloc::Vec<T>) {}
        fn assert_cursor_buffers(cursor: &BTreeCursor) {
            assert_alloc_vec(&cursor.reusable_cell_payload);
            assert_alloc_vec(&cursor.blob_cache.overflow_pages);
        }
        fn assert_overflow_cell_buffers(
            page: &crate::storage::pager::PageInner,
            cell: &OverflowCell,
        ) {
            fn assert_alloc_payload(_: &Pin<crate::alloc::Vec<u8>>) {}

            assert_alloc_vec(&page.overflow_cells);
            assert_alloc_payload(&cell.payload);
        }

        let balance = BalanceState::default();
        assert_alloc_vec(&balance.reusable_divider_buffers[0]);
        assert_alloc_vec(&balance.reusable_cell_payloads);
        let integrity_check = IntegrityCheckState::new(0);
        assert_alloc_vec(&integrity_check.page_stack);
        let op_integrity_check =
            crate::vdbe::execute::OpIntegrityCheckState::CheckingBTreeStructure {
                errors: crate::alloc::vec![],
                current_root_idx: 0,
                current_dropped_idx: 0,
                state: IntegrityCheckState::new(0),
            };
        let crate::vdbe::execute::OpIntegrityCheckState::CheckingBTreeStructure { errors, .. } =
            op_integrity_check
        else {
            unreachable!()
        };
        assert_alloc_vec(&errors);
        let _ = assert_cursor_buffers;
        let _ = assert_overflow_cell_buffers;
    }

    #[test]
    fn wal_reuses_freelist_leaf_after_abandoned_overflowing_insert() {
        #[allow(clippy::arc_with_non_send_sync)]
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db =
            Database::open_file(io, "freelist-stale-overflow.db", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();

        conn.execute("PRAGMA journal_mode = WAL").unwrap();
        conn.execute(
            "CREATE TABLE red_rain_308 (
                slow_desk_238 INTEGER NOT NULL,
                quiet_tree_398 TEXT NOT NULL,
                smart_dog_422 REAL UNIQUE,
                new_hill_140 TEXT,
                light_door_957 REAL,
                wild_dog_803 TEXT NOT NULL,
                new_flower_28 BLOB,
                old_fish_931 TEXT PRIMARY KEY
            )",
        )
        .unwrap();
        conn.execute(
            "CREATE TABLE full_fish_194 (
                brave_star_722 TEXT PRIMARY KEY,
                hot_star_957 NUMERIC,
                empty_chair_834 REAL
            )",
        )
        .unwrap();

        conn.execute(
            "INSERT INTO red_rain_308 (
                slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                light_door_957, wild_dog_803, new_flower_28, old_fish_931
            ) VALUES (
                1, 'seed', 0.24, 'seed', 1.0, 'seed', zeroblob(3600), 'existing_unique'
            )",
        )
        .unwrap();
        for id in 2..260 {
            conn.execute(format!(
                "INSERT INTO red_rain_308 (
                    slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                    light_door_957, wild_dog_803, new_flower_28, old_fish_931
                ) VALUES (
                    {id}, 'quiet_tree_{id}', {id}.0, 'new_hill_{id}',
                    1.5, 'wild_dog_{id}', zeroblob(3600), 'seed_{id}'
                )"
            ))
            .unwrap();
        }

        conn.execute("BEGIN").unwrap();
        conn.execute("DELETE FROM red_rain_308 WHERE old_fish_931 = 'seed_2'")
            .unwrap();
        conn.execute("SAVEPOINT sp_91").unwrap();
        conn.execute(
            "INSERT INTO full_fish_194 (brave_star_722, hot_star_957, empty_chair_834)
             VALUES ('sweet_fish_597', 328, 7.71)",
        )
        .unwrap();

        let red_rain_root_page = query_single_i64(
            &conn,
            "SELECT rootpage FROM sqlite_schema
             WHERE type = 'table' AND name = 'red_rain_308'",
        );
        let injector = TargetedYieldInjector::new(
            BTreeWriteYieldPoint::AfterInsertOverflowCellBeforeBalance.point(),
            btree_write_selection_key(red_rain_root_page),
        );
        conn.set_yield_injector(Some(injector.clone()));
        let mut abandoned = conn
            .prepare(
                "INSERT INTO red_rain_308 (
                    slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                    light_door_957, wild_dog_803, new_flower_28, old_fish_931
                ) VALUES (
                    926, 'dry_hill_411', 9.24, 'blue_hill_65',
                    7.50, 'happy_moon_474', zeroblob(7200), 'smart_rock_113'
                )",
            )
            .unwrap();
        assert!(matches!(abandoned.step().unwrap(), StepResult::Yield));
        assert!(injector.fired());
        abandoned.reset().unwrap();
        drop(abandoned);
        conn.set_yield_injector(None);

        conn.execute("DELETE FROM red_rain_308 WHERE slow_desk_238 BETWEEN 2 AND 240")
            .unwrap();
        conn.execute("RELEASE sp_91").unwrap();

        assert!(conn
            .execute(
                "INSERT INTO red_rain_308 (
                    slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                    light_door_957, wild_dog_803, new_flower_28, old_fish_931
                ) VALUES (
                    927, 'dry_hill_412', 0.24, 'blue_hill_66',
                    7.50, 'happy_moon_475', x'736f66745f73746f6e655f313732', 'smart_rock_114'
                )"
            )
            .is_err());

        for id in 300..420 {
            conn.execute(format!(
                "INSERT INTO red_rain_308 (
                    slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                    light_door_957, wild_dog_803, new_flower_28, old_fish_931
                ) VALUES (
                    {id}, 'quiet_tree_{id}', {id}.0, 'new_hill_{id}',
                    1.5, 'wild_dog_{id}', zeroblob(3600), 'reuse_{id}'
                )"
            ))
            .unwrap();
        }

        conn.execute("ROLLBACK").unwrap();
        assert_eq!(query_single_text(&conn, "PRAGMA integrity_check"), "ok");
    }

    #[test]
    fn wal_overflowing_insert_resumes_after_yield_before_balance() {
        #[allow(clippy::arc_with_non_send_sync)]
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db =
            Database::open_file(io, "resume-overflow-yield.db", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();

        conn.execute("PRAGMA journal_mode = WAL").unwrap();
        conn.execute(
            "CREATE TABLE red_rain_308 (
                slow_desk_238 INTEGER NOT NULL,
                quiet_tree_398 TEXT NOT NULL,
                smart_dog_422 REAL UNIQUE,
                new_hill_140 TEXT,
                light_door_957 REAL,
                wild_dog_803 TEXT NOT NULL,
                new_flower_28 BLOB,
                old_fish_931 TEXT PRIMARY KEY
            )",
        )
        .unwrap();

        for id in 1..260 {
            conn.execute(format!(
                "INSERT INTO red_rain_308 (
                    slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                    light_door_957, wild_dog_803, new_flower_28, old_fish_931
                ) VALUES (
                    {id}, 'quiet_tree_{id}', {id}.0, 'new_hill_{id}',
                    1.5, 'wild_dog_{id}', zeroblob(3600), 'seed_{id}'
                )"
            ))
            .unwrap();
        }

        let red_rain_root_page = query_single_i64(
            &conn,
            "SELECT rootpage FROM sqlite_schema
             WHERE type = 'table' AND name = 'red_rain_308'",
        );
        let injector = TargetedYieldInjector::new(
            BTreeWriteYieldPoint::AfterInsertOverflowCellBeforeBalance.point(),
            btree_write_selection_key(red_rain_root_page),
        );
        conn.set_yield_injector(Some(injector.clone()));
        let mut stmt = conn
            .prepare(
                "INSERT INTO red_rain_308 (
                    slow_desk_238, quiet_tree_398, smart_dog_422, new_hill_140,
                    light_door_957, wild_dog_803, new_flower_28, old_fish_931
                ) VALUES (
                    926, 'dry_hill_411', 9.24, 'blue_hill_65',
                    7.50, 'happy_moon_474', zeroblob(7200), 'smart_rock_113'
                )",
            )
            .unwrap();

        assert!(matches!(stmt.step().unwrap(), StepResult::Yield));
        assert!(injector.fired());
        assert!(matches!(stmt.step().unwrap(), StepResult::Done));
        drop(stmt);
        conn.set_yield_injector(None);

        assert_eq!(
            query_single_i64(
                &conn,
                "SELECT COUNT(*) FROM red_rain_308 WHERE old_fish_931 = 'smart_rock_113'",
            ),
            1
        );
        assert_eq!(query_single_text(&conn, "PRAGMA integrity_check"), "ok");
    }

    fn ensure_cell(page: &mut PageContent, cell_idx: usize, payload: &[u8]) {
        let cell = page.cell_get_raw_region(cell_idx, 4096).unwrap();
        tracing::trace!("cell idx={} start={} len={}", cell_idx, cell.0, cell.1);
        let buf = &page.as_ptr()[cell.0..cell.0 + cell.1];
        assert_eq!(buf.len(), payload.len());
        assert_eq!(buf, payload);
    }

    fn add_record(
        id: usize,
        pos: usize,
        page: PageRef,
        record: ImmutableRecord,
        conn: &Arc<Connection>,
    ) -> crate::alloc::Vec<u8> {
        let mut payload: crate::alloc::Vec<u8> = crate::alloc::vec![];
        let mut fill_cell_payload_state = FillCellPayloadState::Start;
        run_until_done(
            || {
                fill_cell_payload(
                    &PinGuard::new(page.clone()),
                    Some(id as i64),
                    &mut payload,
                    pos,
                    &record,
                    4096,
                    conn.pager.load().clone(),
                    &mut fill_cell_payload_state,
                )
            },
            &conn.pager.load().clone(),
        )
        .unwrap();
        insert_into_cell(page.get_contents(), &payload, pos, 4096).unwrap();
        payload
    }

    fn insert_record(
        cursor: &mut BTreeCursor,
        pager: &Arc<Pager>,
        rowid: i64,
        val: Value,
    ) -> Result<(), LimboError> {
        let regs = &[Register::Value(val)];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();

        run_until_done(
            || {
                let key = SeekKey::TableRowId(rowid);
                cursor.seek(key, SeekOp::GE { eq_only: true })
            },
            pager.deref(),
        )?;
        run_until_done(
            || cursor.insert(&BTreeKey::new_table_rowid(rowid, Some(&record))),
            pager.deref(),
        )?;
        Ok(())
    }

    fn assert_btree_empty(cursor: &mut BTreeCursor, pager: &Pager) -> Result<()> {
        let _c = cursor.move_to_root()?;
        run_until_done(|| cursor.next(), pager)?;
        let empty = !cursor.has_record;
        assert!(empty, "expected B-tree to be empty");
        Ok(())
    }

    #[test]
    fn test_insert_cell() {
        let (db, _temp_dir) = get_database();
        let conn = db.connect().unwrap();
        let page = get_page(2);

        let header_size = 8;
        let regs = &[Register::Value(Value::from_i64(1))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let payload = add_record(1, 0, page.clone(), record, &conn);
        let page_contents = page.get_contents();
        assert_eq!(page_contents.cell_count(), 1);
        let free = compute_free_space(page_contents, 4096).unwrap();
        assert_eq!(free, 4096 - payload.len() - 2 - header_size);

        let cell_idx = 0;
        ensure_cell(page_contents, cell_idx, &payload);
    }

    struct Cell {
        pos: usize,
        payload: crate::alloc::Vec<u8>,
    }

    #[test]
    fn test_drop_1() {
        let (db, _temp_dir) = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);

        let page_contents = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        for i in 0..3 {
            let regs = &[Register::Value(Value::from_i64(i as i64))];
            let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
            let payload = add_record(i, i, page.clone(), record, &conn);
            assert_eq!(page_contents.cell_count(), i + 1);
            let free = compute_free_space(page_contents, usable_space).unwrap();
            total_size += payload.len() + 2;
            assert_eq!(free, 4096 - total_size - header_size);
            cells.push(Cell { pos: i, payload });
        }

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page_contents, i, &cell.payload);
        }
        cells.remove(1);
        drop_cell(page_contents, 1, usable_space).unwrap();

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page_contents, i, &cell.payload);
        }
    }

    fn validate_btree(pager: Arc<Pager>, page_idx: i64) -> (usize, bool) {
        let num_columns = 5;
        let cursor = BTreeCursor::new_table(pager.clone(), page_idx, num_columns);
        let (page, _c) = cursor.read_page_blocking(page_idx).unwrap();
        while page.is_locked() {
            pager.io.step().unwrap();
        }

        // Pin page in order to not drop it in between
        page.set_dirty();
        let contents = page.get_contents();
        let mut previous_key = None;
        let mut valid = true;
        let mut depth = None;
        debug_validate_cells!(contents, pager.usable_space());
        let mut child_pages = Vec::new();
        for cell_idx in 0..contents.cell_count() {
            let cell = contents.cell_get(cell_idx, cursor.usable_space()).unwrap();
            let current_depth = match cell {
                BTreeCell::TableLeafCell(..) => 1,
                BTreeCell::TableInteriorCell(TableInteriorCell {
                    left_child_page, ..
                }) => {
                    let (child_page, _c) =
                        cursor.read_page_blocking(left_child_page as i64).unwrap();
                    while child_page.is_locked() {
                        pager.io.step().unwrap();
                    }
                    child_pages.push(child_page);
                    if left_child_page == page.get().id as u32 {
                        valid = false;
                        tracing::error!(
                            "left child page is the same as parent {}",
                            left_child_page
                        );
                        continue;
                    }
                    let (child_depth, child_valid) =
                        validate_btree(pager.clone(), left_child_page as i64);
                    valid &= child_valid;
                    child_depth
                }
                _ => panic!("unsupported btree cell: {cell:?}"),
            };
            if current_depth >= 100 {
                tracing::error!("depth is too big");
                page.clear_dirty();
                return (100, false);
            }
            depth = Some(depth.unwrap_or(current_depth + 1));
            if depth != Some(current_depth + 1) {
                tracing::error!("depth is different for child of page {}", page_idx);
                valid = false;
            }
            match cell {
                BTreeCell::TableInteriorCell(TableInteriorCell { rowid, .. })
                | BTreeCell::TableLeafCell(TableLeafCell { rowid, .. }) => {
                    if previous_key.is_some() && previous_key.unwrap() >= rowid {
                        tracing::error!(
                            "keys are in bad order: prev={:?}, current={}",
                            previous_key,
                            rowid
                        );
                        valid = false;
                    }
                    previous_key = Some(rowid);
                }
                _ => panic!("unsupported btree cell: {cell:?}"),
            }
        }
        if let Some(right) = contents.rightmost_pointer().ok().flatten() {
            let (right_depth, right_valid) = validate_btree(pager.clone(), right as i64);
            valid &= right_valid;
            depth = Some(depth.unwrap_or(right_depth + 1));
            if depth != Some(right_depth + 1) {
                tracing::error!("depth is different for child of page {}", page_idx);
                valid = false;
            }
        }
        let first_page_type = child_pages.first_mut().map(|p| {
            if !p.is_loaded() {
                let (new_page, _c) = pager
                    .io
                    .block(|| pager.read_page(p.get().id as i64))
                    .unwrap();
                *p = new_page;
            }
            while p.is_locked() {
                pager.io.step().unwrap();
            }
            p.get_contents().page_type().ok()
        });
        if let Some(child_type) = first_page_type {
            for page in child_pages.iter_mut().skip(1) {
                if !page.is_loaded() {
                    let (new_page, _c) = pager
                        .io
                        .block(|| pager.read_page(page.get().id as i64))
                        .unwrap();
                    *page = new_page;
                }
                while page.is_locked() {
                    pager.io.step().unwrap();
                }
                if page.get_contents().page_type().ok() != child_type {
                    tracing::error!("child pages have different types");
                    valid = false;
                }
            }
        }
        if contents.rightmost_pointer().ok().flatten().is_none() && contents.cell_count() == 0 {
            valid = false;
        }
        page.clear_dirty();
        (depth.unwrap(), valid)
    }

    fn format_btree(pager: Arc<Pager>, page_idx: i64, depth: usize) -> String {
        let num_columns = 5;

        let cursor = BTreeCursor::new_table(pager.clone(), page_idx, num_columns);
        let (page, _c) = cursor.read_page_blocking(page_idx).unwrap();
        while page.is_locked() {
            pager.io.step().unwrap();
        }

        // Pin page in order to not drop it in between loading of different pages. If not contents will be a dangling reference.
        page.set_dirty();
        let contents = page.get_contents();
        let mut current = Vec::new();
        let mut child = Vec::new();
        for cell_idx in 0..contents.cell_count() {
            let cell = contents.cell_get(cell_idx, cursor.usable_space()).unwrap();
            match cell {
                BTreeCell::TableInteriorCell(cell) => {
                    current.push(format!(
                        "node[rowid:{}, ptr(<=):{}]",
                        cell.rowid, cell.left_child_page
                    ));
                    child.push(format_btree(
                        pager.clone(),
                        cell.left_child_page as i64,
                        depth + 2,
                    ));
                }
                BTreeCell::TableLeafCell(cell) => {
                    current.push(format!(
                        "leaf[rowid:{}, len(payload):{}, overflow:{}]",
                        cell.rowid,
                        cell.payload.len(),
                        cell.first_overflow_page.is_some()
                    ));
                }
                _ => panic!("unsupported btree cell: {cell:?}"),
            }
        }
        if let Some(rightmost) = contents.rightmost_pointer().ok().flatten() {
            child.push(format_btree(pager, rightmost as i64, depth + 2));
        }
        let current = format!(
            "{}-page:{}, ptr(right):{:?}\n{}+cells:{}",
            " ".repeat(depth),
            page_idx,
            contents.rightmost_pointer().ok().flatten(),
            " ".repeat(depth),
            current.join(", ")
        );
        page.clear_dirty();
        if child.is_empty() {
            current
        } else {
            current + "\n" + &child.join("\n")
        }
    }

    fn empty_btree() -> (Arc<Pager>, i64, Arc<Database>, Arc<Connection>) {
        #[allow(clippy::arc_with_non_send_sync)]
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io.clone(), ":memory:", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        let pager = conn.pager.load().clone();

        // FIXME: handle page cache is full

        // force allocate page1 with a transaction
        pager.begin_read_tx().unwrap();
        run_until_done(
            || pager.begin_write_tx(WalAutoActions::all_enabled()),
            &pager,
        )
        .unwrap();
        run_until_done(
            || pager.commit_tx(&conn, conn.get_sync_mode(), true),
            &pager,
        )
        .unwrap();

        let page2 = run_until_done(|| pager.allocate_page(), &pager).unwrap();
        btree_init_page(&page2, PageType::TableLeaf, 0, pager.usable_space());
        (pager, page2.get().id as i64, db, conn)
    }

    #[test]
    fn btree_with_virtual_page_1() -> Result<()> {
        #[allow(clippy::arc_with_non_send_sync)]
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io.clone(), ":memory:", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        let pager = conn.pager.load().clone();

        let mut cursor = BTreeCursor::new(pager, 1, 5);
        let result = cursor.rewind()?;
        assert!(matches!(result, IOResult::Done(_)));
        let result = cursor.next()?;
        assert!(matches!(result, IOResult::Done(_)));
        assert!(!cursor.has_record);
        let result = cursor.record()?;
        assert!(matches!(result, IOResult::Done(record) if record.is_none()));
        Ok(())
    }

    #[test]
    pub fn btree_test_overflow_pages_are_cleared_on_overwrite() {
        // Create a database with a table
        let (pager, root_page, _, _) = empty_btree();
        let num_columns = 5;

        // Get the maximum local payload size for table leaf pages
        let max_local = payload_overflow_threshold_max(PageType::TableLeaf, 4096);
        let usable_size = 4096;

        // Create a payload that is definitely larger than the maximum local size
        // This will force the creation of overflow pages
        let large_payload_size = max_local + usable_size * 2;
        let large_payload = crate::alloc::vec![b'X'; large_payload_size];

        // Create a record with the large payload
        let regs = &[Register::Value(Value::Blob(large_payload))];
        let large_record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();

        // Create cursor for the table
        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
        let cursor = &mut cursor;

        let initial_pagecount = pager
            .io
            .block(|| pager.with_header(|header| header.database_size.get()))
            .unwrap();
        assert_eq!(
            initial_pagecount, 2,
            "Page count should be 2 after initial insert, was {initial_pagecount}"
        );

        // Insert the large record with rowid 1
        run_until_done(
            || {
                let key = SeekKey::TableRowId(1);
                cursor.seek(key, SeekOp::GE { eq_only: true })
            },
            pager.deref(),
        )
        .unwrap();
        let key = BTreeKey::new_table_rowid(1, Some(&large_record));
        run_until_done(|| cursor.insert(&key), pager.deref()).unwrap();

        // Verify that overflow pages were created by checking freelist count
        // The freelist count should be 0 initially, and after inserting a large record,
        // some pages should be allocated for overflow, but they won't be in freelist yet
        let freelist_after_insert = pager
            .io
            .block(|| pager.with_header(|header| header.freelist_pages.get()))
            .unwrap();
        assert_eq!(
            freelist_after_insert, 0,
            "Freelist count should be 0 after insert, was {freelist_after_insert}"
        );
        let pagecount_after_insert = pager
            .io
            .block(|| pager.with_header(|header| header.database_size.get()))
            .unwrap();
        const EXPECTED_OVERFLOW_PAGES: u32 = 3;
        assert_eq!(
            pagecount_after_insert,
            initial_pagecount + EXPECTED_OVERFLOW_PAGES,
            "Page count should be {} after insert, was {pagecount_after_insert}",
            initial_pagecount + EXPECTED_OVERFLOW_PAGES
        );

        // Create a smaller record to overwrite with
        let small_payload = crate::alloc::vec![b'Y'; 100]; // Much smaller payload
        let regs = &[Register::Value(Value::Blob(small_payload.clone()))];
        let small_record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();

        // Seek to the existing record
        run_until_done(
            || {
                let key = SeekKey::TableRowId(1);
                cursor.seek(key, SeekOp::GE { eq_only: true })
            },
            pager.deref(),
        )
        .unwrap();

        // Overwrite the record with the same rowid
        let key = BTreeKey::new_table_rowid(1, Some(&small_record));
        run_until_done(|| cursor.insert(&key), pager.deref()).unwrap();

        // Check that the freelist count has increased, indicating overflow pages were cleared
        let freelist_after_overwrite = pager
            .io
            .block(|| pager.with_header(|header| header.freelist_pages.get()))
            .unwrap();
        assert_eq!(freelist_after_overwrite, EXPECTED_OVERFLOW_PAGES, "Freelist count should be {EXPECTED_OVERFLOW_PAGES} after overwrite, was {freelist_after_overwrite}");

        // Verify the record was actually overwritten by reading it back
        run_until_done(
            || {
                let key = SeekKey::TableRowId(1);
                cursor.seek(key, SeekOp::GE { eq_only: true })
            },
            pager.deref(),
        )
        .unwrap();

        let record = loop {
            match cursor.record().unwrap() {
                IOResult::Done(r) => break r,
                IOResult::IO(io) => io.wait(&*pager.io).unwrap(),
            }
        };
        let record = record.unwrap();

        // The record should now contain the smaller payload
        let record_payload = record.get_payload();
        const RECORD_HEADER_SIZE: usize = 1;
        const ROWID_VARINT_SIZE: usize = 1;
        const ROWID_PAYLOAD_SIZE: usize = 0; // const int 1 doesn't take any space
        const BLOB_PAYLOAD_SIZE: usize = 1; // the size '100 bytes' can be expressed as 1 byte
        assert_eq!(
            record_payload.len(),
            RECORD_HEADER_SIZE
                + ROWID_VARINT_SIZE
                + ROWID_PAYLOAD_SIZE
                + BLOB_PAYLOAD_SIZE
                + small_payload.len(),
            "Record should now contain smaller payload after overwrite"
        );
    }

    #[test]
    #[ignore]
    pub fn btree_insert_fuzz_ex() {
        for sequence in [
            &[
                (777548915, 3364),
                (639157228, 3796),
                (709175417, 1214),
                (390824637, 210),
                (906124785, 1481),
                (197677875, 1305),
                (457946262, 3734),
                (956825466, 592),
                (835875722, 1334),
                (649214013, 1250),
                (531143011, 1788),
                (765057993, 2351),
                (510007766, 1349),
                (884516059, 822),
                (81604840, 2545),
            ]
            .as_slice(),
            &[
                (293471650, 2452),
                (163608869, 627),
                (544576229, 464),
                (705823748, 3441),
            ]
            .as_slice(),
            &[
                (987283511, 2924),
                (261851260, 1766),
                (343847101, 1657),
                (315844794, 572),
            ]
            .as_slice(),
            &[
                (987283511, 2924),
                (261851260, 1766),
                (343847101, 1657),
                (315844794, 572),
                (649272840, 1632),
                (723398505, 3140),
                (334416967, 3874),
            ]
            .as_slice(),
        ] {
            let (pager, root_page, _, _) = empty_btree();
            let num_columns = 5;

            let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
            for (key, size) in sequence.iter() {
                run_until_done(
                    || {
                        let key = SeekKey::TableRowId(*key);
                        cursor.seek(key, SeekOp::GE { eq_only: true })
                    },
                    pager.deref(),
                )
                .unwrap();
                let regs = &[Register::Value(Value::Blob(crate::alloc::vec![0; *size]))];
                let value = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
                tracing::info!("insert key:{}", key);
                run_until_done(
                    || cursor.insert(&BTreeKey::new_table_rowid(*key, Some(&value))),
                    pager.deref(),
                )
                .unwrap();
                tracing::info!(
                    "=========== btree ===========\n{}\n\n",
                    format_btree(pager.clone(), root_page, 0)
                );
            }
            for (key, _) in sequence.iter() {
                let seek_key = SeekKey::TableRowId(*key);
                assert!(
                    matches!(
                        cursor.seek(seek_key, SeekOp::GE { eq_only: true }).unwrap(),
                        IOResult::Done(SeekResult::Found)
                    ),
                    "key {key} is not found"
                );
            }
        }
    }

    fn rng_from_time_or_env() -> (ChaCha8Rng, u64) {
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

    fn btree_insert_fuzz_run(
        attempts: usize,
        inserts: usize,
        size: impl Fn(&mut ChaCha8Rng) -> usize,
    ) {
        const VALIDATE_INTERVAL: usize = 1000;
        let do_validate_btree = std::env::var("VALIDATE_BTREE")
            .is_ok_and(|v| v.parse().expect("validate should be bool"));
        let (mut rng, seed) = rng_from_time_or_env();
        let mut seen = crate::HashSet::default();
        tracing::info!("super seed: {}", seed);
        let num_columns = 5;

        for _ in 0..attempts {
            let (pager, root_page, _db, conn) = empty_btree();
            let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
            let mut keys = SortedVec::new();
            tracing::info!("seed: {seed}");
            for insert_id in 0..inserts {
                let do_validate = do_validate_btree || (insert_id % VALIDATE_INTERVAL == 0);
                pager.begin_read_tx().unwrap();
                run_until_done(
                    || pager.begin_write_tx(WalAutoActions::all_enabled()),
                    &pager,
                )
                .unwrap();
                let size = size(&mut rng);
                let key = {
                    let result;
                    loop {
                        let key = (rng.next_u64() % (1 << 30)) as i64;
                        if seen.contains(&key) {
                            continue;
                        } else {
                            seen.insert(key);
                        }
                        result = key;
                        break;
                    }
                    result
                };
                keys.push(key);
                tracing::info!(
                    "INSERT INTO t VALUES ({}, randomblob({})); -- {}",
                    key,
                    size,
                    insert_id
                );
                run_until_done(
                    || {
                        let key = SeekKey::TableRowId(key);
                        cursor.seek(key, SeekOp::GE { eq_only: true })
                    },
                    pager.deref(),
                )
                .unwrap();
                let regs = &[Register::Value(Value::Blob(crate::alloc::vec![0; size]))];
                let value = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
                let btree_before = if do_validate {
                    format_btree(pager.clone(), root_page, 0)
                } else {
                    "".to_string()
                };
                run_until_done(
                    || cursor.insert(&BTreeKey::new_table_rowid(key, Some(&value))),
                    pager.deref(),
                )
                .unwrap();
                pager
                    .io
                    .block(|| pager.commit_tx(&conn, conn.get_sync_mode(), true))
                    .unwrap();
                pager.begin_read_tx().unwrap();
                // FIXME: add sorted vector instead, should be okay for small amounts of keys for now :P, too lazy to fix right now
                let _c = cursor.move_to_root().unwrap();
                let mut valid = true;
                if do_validate {
                    let _c = cursor.move_to_root().unwrap();
                    for key in keys.iter() {
                        tracing::trace!("seeking key: {}", key);
                        run_until_done(|| cursor.next(), pager.deref()).unwrap();
                        let cursor_rowid = run_until_done(|| cursor.rowid(), pager.deref())
                            .unwrap()
                            .unwrap();
                        if *key != cursor_rowid {
                            valid = false;
                            println!("key {key} is not found, got {cursor_rowid}");
                            break;
                        }
                    }
                }
                // let's validate btree too so that we undertsand where the btree failed
                if do_validate
                    && (!valid || matches!(validate_btree(pager.clone(), root_page), (_, false)))
                {
                    let btree_after = format_btree(pager, root_page, 0);
                    println!("btree before:\n{btree_before}");
                    println!("btree after:\n{btree_after}");
                    panic!("invalid btree");
                }
                pager.end_read_tx();
            }
            pager.begin_read_tx().unwrap();
            tracing::info!(
                "=========== btree ===========\n{}\n\n",
                format_btree(pager.clone(), root_page, 0)
            );
            if matches!(validate_btree(pager.clone(), root_page), (_, false)) {
                panic!("invalid btree");
            }
            let _c = cursor.move_to_root().unwrap();
            for key in keys.iter() {
                tracing::trace!("seeking key: {}", key);
                run_until_done(|| cursor.next(), pager.deref()).unwrap();
                let cursor_rowid = run_until_done(|| cursor.rowid(), pager.deref())
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    *key, cursor_rowid,
                    "key {key} is not found, got {cursor_rowid}"
                );
            }
            pager.end_read_tx();
        }
    }

    fn btree_index_insert_fuzz_run(attempts: usize, inserts: usize) {
        use crate::storage::pager::CreateBTreeFlags;

        let (mut rng, seed) = if std::env::var("SEED").is_ok() {
            let seed = std::env::var("SEED").unwrap();
            let seed = seed.parse::<u64>().unwrap();
            let rng = ChaCha8Rng::seed_from_u64(seed);
            (rng, seed)
        } else {
            rng_from_time_or_env()
        };
        let mut seen = crate::HashSet::default();
        tracing::info!("super seed: {}", seed);
        for _ in 0..attempts {
            let (pager, _, _db, conn) = empty_btree();
            let index_root_page = pager
                .io
                .block(|| pager.btree_create(&CreateBTreeFlags::new_index()))
                .unwrap() as i64;
            let index_def = Index {
                name: "testindex".to_string(),
                where_clause: None,
                columns: IndexColumn::new_many((0..10).map(|i| format!("test{i}"))),
                table_name: "test".to_string(),
                root_page: index_root_page,
                unique: false,
                ephemeral: false,
                has_rowid: false,
                index_method: None,
                on_conflict: None,
            };
            let num_columns = index_def.columns.len();
            let mut cursor =
                BTreeCursor::new_index(pager.clone(), index_root_page, &index_def, num_columns)
                    .unwrap();
            let mut keys = SortedVec::new();
            tracing::info!("seed: {seed}");
            for i in 0..inserts {
                pager.begin_read_tx().unwrap();
                pager
                    .io
                    .block(|| pager.begin_write_tx(WalAutoActions::all_enabled()))
                    .unwrap();
                let key = {
                    let result;
                    loop {
                        let cols = (0..num_columns)
                            .map(|_| (rng.next_u64() % (1 << 30)) as i64)
                            .collect::<Vec<_>>();
                        if seen.contains(&cols) {
                            continue;
                        } else {
                            seen.insert(cols.clone());
                        }
                        result = cols;
                        break;
                    }
                    result
                };
                tracing::info!("insert {}/{}: {:?}", i + 1, inserts, key);
                keys.push(key.clone());
                let regs = key
                    .iter()
                    .map(|col| Register::Value(Value::from_i64(*col)))
                    .collect::<Vec<_>>();
                let value = ImmutableRecord::from_registers(&regs, regs.len()).unwrap();
                run_until_done(
                    || {
                        let record = ImmutableRecord::from_registers(&regs, regs.len()).unwrap();
                        let key = SeekKey::IndexKey(record.as_record_ref());
                        cursor.seek(key, SeekOp::GE { eq_only: true })
                    },
                    pager.deref(),
                )
                .unwrap();
                run_until_done(
                    || cursor.insert(&BTreeKey::new_index_key(value.as_record_ref())),
                    pager.deref(),
                )
                .unwrap();
                let c = cursor.move_to_root().unwrap();
                if let Some(c) = c {
                    pager.io.wait_for_completion(c).unwrap();
                }
                pager
                    .io
                    .block(|| pager.commit_tx(&conn, conn.get_sync_mode(), true))
                    .unwrap();
            }

            // Check that all keys can be found by seeking
            pager.begin_read_tx().unwrap();
            let _c = cursor.move_to_root().unwrap();
            for (i, key) in keys.iter().enumerate() {
                tracing::info!("seeking key {}/{}: {:?}", i + 1, keys.len(), key);
                let exists = run_until_done(
                    || {
                        let regs = key
                            .iter()
                            .map(|col| Register::Value(Value::from_i64(*col)))
                            .collect::<Vec<_>>();
                        cursor.seek(
                            SeekKey::IndexKey(
                                ImmutableRecord::from_registers(&regs, regs.len())
                                    .unwrap()
                                    .as_record_ref(),
                            ),
                            SeekOp::GE { eq_only: true },
                        )
                    },
                    pager.deref(),
                )
                .unwrap();
                let mut found = matches!(exists, SeekResult::Found);
                if matches!(exists, SeekResult::TryAdvance) {
                    run_until_done(|| cursor.next(), pager.deref()).unwrap();
                    found = cursor.has_record();
                }
                assert!(found, "key {key:?} is not found");
            }
            // Check that key count is right
            let _c = cursor.move_to_root().unwrap();
            let mut count = 0;
            while {
                run_until_done(|| cursor.next(), pager.deref()).unwrap();
                cursor.has_record
            } {
                count += 1;
            }
            assert_eq!(
                count,
                keys.len(),
                "key count is not right, got {}, expected {}",
                count,
                keys.len()
            );
            // Check that all keys can be found in-order, by iterating the btree
            let _c = cursor.move_to_root().unwrap();
            let mut prev = None;
            for (i, key) in keys.iter().enumerate() {
                tracing::info!("iterating key {}/{}: {:?}", i + 1, keys.len(), key);
                run_until_done(|| cursor.next(), pager.deref()).unwrap();
                let record = loop {
                    match cursor.record().unwrap() {
                        IOResult::Done(r) => break r,
                        IOResult::IO(io) => io.wait(&*pager.io).unwrap(),
                    }
                };
                let record = record.as_ref().unwrap();
                let cur = record
                    .get_values()
                    .unwrap()
                    .iter()
                    .map(|value| value.to_owned().expect(crate::alloc::ALLOC_ERR_MSG))
                    .collect::<Vec<_>>();
                if let Some(prev) = prev {
                    if prev >= cur {
                        println!("Seed: {seed}");
                    }
                    assert!(
                        prev < cur,
                        "keys are not in ascending order: {prev:?} < {cur:?}",
                    );
                }
                prev = Some(cur);
            }
            pager.end_read_tx();
        }
    }

    fn btree_index_insert_delete_fuzz_run(
        attempts: usize,
        operations: usize,
        size: impl Fn(&mut ChaCha8Rng) -> usize,
        insert_chance: f64,
    ) {
        use crate::storage::pager::CreateBTreeFlags;

        let (mut rng, seed) = if std::env::var("SEED").is_ok() {
            let seed = std::env::var("SEED").unwrap();
            let seed = seed.parse::<u64>().unwrap();
            let rng = ChaCha8Rng::seed_from_u64(seed);
            (rng, seed)
        } else {
            rng_from_time_or_env()
        };
        let mut seen = crate::HashSet::default();
        tracing::info!("super seed: {}", seed);

        for _ in 0..attempts {
            let (pager, _, _db, conn) = empty_btree();
            let index_root_page = pager
                .io
                .block(|| pager.btree_create(&CreateBTreeFlags::new_index()))
                .unwrap() as i64;
            let index_def = Index {
                name: "testindex".to_string(),
                where_clause: None,
                columns: IndexColumn::new_many(vec!["testcol"]),
                table_name: "test".to_string(),
                root_page: index_root_page,
                unique: false,
                ephemeral: false,
                has_rowid: false,
                index_method: None,
                on_conflict: None,
            };
            let mut cursor =
                BTreeCursor::new_index(pager.clone(), index_root_page, &index_def, 1).unwrap();

            // Track expected keys that should be present in the tree
            let mut expected_keys = Vec::new();

            tracing::info!("seed: {seed}");
            for i in 0..operations {
                let print_progress = i % 100 == 0;
                pager.begin_read_tx().unwrap();

                pager
                    .io
                    .block(|| pager.begin_write_tx(WalAutoActions::all_enabled()))
                    .unwrap();

                // Decide whether to insert or delete (80% chance of insert)
                let is_insert = rng.next_u64() % 100 < (insert_chance * 100.0) as u64;

                if is_insert {
                    // Generate a unique key for insertion
                    let key = {
                        let result;
                        loop {
                            let sizeof_blob = size(&mut rng);
                            let blob = (0..sizeof_blob)
                                .map(|_| (rng.next_u64() % 256) as u8)
                                .collect::<Vec<_>>();
                            if seen.contains(&blob) {
                                continue;
                            } else {
                                seen.insert(blob.clone());
                            }
                            result = blob;
                            break;
                        }
                        result
                    };

                    if print_progress {
                        tracing::info!("insert {}/{}, seed: {seed}", i + 1, operations);
                    }
                    expected_keys.push(key.clone());

                    let regs = vec![Register::Value(
                        Value::from_slice(&key).expect(crate::alloc::ALLOC_ERR_MSG),
                    )];
                    let value = ImmutableRecord::from_registers(&regs, regs.len()).unwrap();

                    let seek_result = run_until_done(
                        || {
                            let record =
                                ImmutableRecord::from_registers(&regs, regs.len()).unwrap();
                            let key = SeekKey::IndexKey(record.as_record_ref());
                            cursor.seek(key, SeekOp::GE { eq_only: true })
                        },
                        pager.deref(),
                    )
                    .unwrap();
                    if let SeekResult::TryAdvance = seek_result {
                        run_until_done(|| cursor.next(), pager.deref()).unwrap();
                    }
                    run_until_done(
                        || cursor.insert(&BTreeKey::new_index_key(value.as_record_ref())),
                        pager.deref(),
                    )
                    .unwrap();
                } else {
                    // Delete a random existing key
                    if !expected_keys.is_empty() {
                        let delete_idx = rng.next_u64() as usize % expected_keys.len();
                        let key_to_delete = expected_keys[delete_idx].clone();

                        if print_progress {
                            tracing::info!("delete {}/{}, seed: {seed}", i + 1, operations);
                        }

                        let regs = vec![Register::Value(
                            Value::from_slice(&key_to_delete).expect(crate::alloc::ALLOC_ERR_MSG),
                        )];
                        let record = ImmutableRecord::from_registers(&regs, regs.len()).unwrap();

                        // Seek to the key to delete
                        let seek_result = run_until_done(
                            || {
                                cursor.seek(
                                    SeekKey::IndexKey(record.as_record_ref()),
                                    SeekOp::GE { eq_only: true },
                                )
                            },
                            pager.deref(),
                        )
                        .unwrap();
                        let mut found = matches!(seek_result, SeekResult::Found);
                        if matches!(seek_result, SeekResult::TryAdvance) {
                            run_until_done(|| cursor.next(), pager.deref()).unwrap();
                            found = cursor.has_record()
                        }
                        assert!(found, "expected key {key_to_delete:?} is not found");

                        // Delete the key
                        run_until_done(|| cursor.delete(), pager.deref()).unwrap();

                        // Remove from expected keys
                        expected_keys.remove(delete_idx);
                    }
                }

                let c = cursor.move_to_root().unwrap();
                if let Some(c) = c {
                    pager.io.wait_for_completion(c).unwrap();
                }
                pager
                    .io
                    .block(|| pager.commit_tx(&conn, conn.get_sync_mode(), true))
                    .unwrap();
            }

            // Final validation
            let mut sorted_keys = expected_keys.clone();
            sorted_keys.sort();
            validate_expected_keys(&pager, &mut cursor, &sorted_keys, seed);

            pager.end_read_tx();
        }
    }

    fn validate_expected_keys(
        pager: &Arc<Pager>,
        cursor: &mut BTreeCursor,
        expected_keys: &[Vec<u8>],
        seed: u64,
    ) {
        // Check that all expected keys can be found by seeking
        pager.begin_read_tx().unwrap();
        let _c = cursor.move_to_root().unwrap();
        for (i, key) in expected_keys.iter().enumerate() {
            tracing::info!(
                "validating key {}/{}, seed: {seed}",
                i + 1,
                expected_keys.len()
            );
            let exists = run_until_done(
                || {
                    let regs = vec![Register::Value(
                        Value::from_slice(key).expect(crate::alloc::ALLOC_ERR_MSG),
                    )];
                    cursor.seek(
                        SeekKey::IndexKey(
                            ImmutableRecord::from_registers(&regs, regs.len())
                                .unwrap()
                                .as_record_ref(),
                        ),
                        SeekOp::GE { eq_only: true },
                    )
                },
                pager.deref(),
            )
            .unwrap();
            let mut found = matches!(exists, SeekResult::Found);
            if matches!(exists, SeekResult::TryAdvance) {
                run_until_done(|| cursor.next(), pager.deref()).unwrap();
                found = cursor.has_record();
            }
            assert!(found, "expected key {key:?} is not found");
        }

        // Check key count
        let _c = cursor.move_to_root().unwrap();
        run_until_done(|| cursor.rewind(), pager.deref()).unwrap();
        if !cursor.has_record() {
            panic!("no keys in tree");
        }
        let mut count = 1;
        loop {
            run_until_done(|| cursor.next(), pager.deref()).unwrap();
            if !cursor.has_record() {
                break;
            }
            count += 1;
        }
        assert_eq!(
            count,
            expected_keys.len(),
            "key count is not right, got {}, expected {}, seed: {seed}",
            count,
            expected_keys.len()
        );

        // Check that all keys can be found in-order, by iterating the btree
        let _c = cursor.move_to_root().unwrap();
        for (i, key) in expected_keys.iter().enumerate() {
            run_until_done(|| cursor.next(), pager.deref()).unwrap();
            tracing::info!(
                "iterating key {}/{}, cursor stack cur idx: {:?}, cursor stack depth: {:?}, seed: {seed}",
                i + 1,
                expected_keys.len(),
                cursor.stack.current_cell_index(),
                cursor.stack.current()
            );
            let record = loop {
                match cursor.record().unwrap() {
                    IOResult::Done(r) => break r,
                    IOResult::IO(io) => io.wait(&*pager.io).unwrap(),
                }
            };
            let record = record.as_ref().unwrap();
            let cur = record.get_value(0).expect("expected at least one column");
            let ValueRef::Blob(ref cur) = cur else {
                panic!("expected blob, got {cur:?}");
            };
            assert_eq!(cur, key, "key {key:?} is not found, seed: {seed}");
        }
        pager.end_read_tx();
    }

    #[test]
    pub fn test_drop_odd() {
        let (db, _temp_dir) = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);

        let page_contents = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        let total_cells = 10;
        for i in 0..total_cells {
            let regs = &[Register::Value(Value::from_i64(i as i64))];
            let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
            let payload = add_record(i, i, page.clone(), record, &conn);
            assert_eq!(page_contents.cell_count(), i + 1);
            let free = compute_free_space(page_contents, usable_space).unwrap();
            total_size += payload.len() + 2;
            assert_eq!(free, 4096 - total_size - header_size);
            cells.push(Cell { pos: i, payload });
        }

        let mut removed = 0;
        let mut new_cells = Vec::new();
        for cell in cells {
            if cell.pos % 2 == 1 {
                drop_cell(page_contents, cell.pos - removed, usable_space).unwrap();
                removed += 1;
            } else {
                new_cells.push(cell);
            }
        }
        let cells = new_cells;
        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page_contents, i, &cell.payload);
        }

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page_contents, i, &cell.payload);
        }
    }

    #[test]
    pub fn btree_insert_fuzz_run_equal_size() {
        for size in 1..8 {
            tracing::info!("======= size:{} =======", size);
            btree_insert_fuzz_run(2, 1024, |_| size);
        }
    }

    #[test]
    pub fn btree_index_insert_fuzz_run_equal_size() {
        btree_index_insert_fuzz_run(2, 1024);
    }

    #[test]
    pub fn btree_index_insert_delete_fuzz_run_test() {
        btree_index_insert_delete_fuzz_run(
            2,
            2000,
            |rng| {
                let min: u32 = 4;
                let size = min + rng.next_u32() % (1024 - min);
                size as usize
            },
            0.65,
        );
    }

    #[test]
    pub fn btree_insert_fuzz_run_random() {
        btree_insert_fuzz_run(128, 16, |rng| (rng.next_u32() % 4096) as usize);
    }

    #[test]
    pub fn btree_insert_fuzz_run_small() {
        btree_insert_fuzz_run(1, 100, |rng| (rng.next_u32() % 128) as usize);
    }

    #[test]
    pub fn btree_insert_fuzz_run_big() {
        btree_insert_fuzz_run(64, 32, |rng| 3 * 1024 + (rng.next_u32() % 1024) as usize);
    }

    #[test]
    pub fn btree_insert_fuzz_run_overflow() {
        btree_insert_fuzz_run(64, 32, |rng| (rng.next_u32() % 32 * 1024) as usize);
    }

    #[test]
    #[ignore]
    pub fn fuzz_long_btree_insert_fuzz_run_equal_size() {
        for size in 1..8 {
            tracing::info!("======= size:{} =======", size);
            btree_insert_fuzz_run(2, 10_000, |_| size);
        }
    }

    #[test]
    #[ignore]
    pub fn fuzz_long_btree_index_insert_fuzz_run_equal_size() {
        btree_index_insert_fuzz_run(2, 10_000);
    }

    /// Re-insert an already-present index key the way FTS's RowInserter
    /// does — seek `GE { eq_only: true }`, advance once on `TryAdvance`,
    /// insert at the cursor — after delete churn that leaves stale interior
    /// dividers. `insert` only checks the cursor's current cell for an
    /// exact-key overwrite, so a caller that skips the advance lands one
    /// leaf early and writes a second physical copy of the key (the whopper
    /// duplicate-registry-row corruption). The full scan must see every key
    /// exactly once.
    #[test]
    pub fn btree_index_reinsert_after_tryadvance_never_duplicates() {
        use crate::storage::pager::CreateBTreeFlags;
        let (mut rng, seed) = rng_from_time_or_env();
        tracing::info!("seed: {seed}");
        for attempt in 0..8 {
            let (pager, _, _db, conn) = empty_btree();
            let index_root_page = pager
                .io
                .block(|| pager.btree_create(&CreateBTreeFlags::new_index()))
                .unwrap() as i64;
            let index_def = Index {
                name: "testindex".to_string(),
                where_clause: None,
                columns: IndexColumn::new_many(vec!["testcol"]),
                table_name: "test".to_string(),
                root_page: index_root_page,
                unique: false,
                ephemeral: false,
                has_rowid: false,
                index_method: None,
                on_conflict: None,
            };
            let mut cursor =
                BTreeCursor::new_index(pager.clone(), index_root_page, &index_def, 1).unwrap();

            let key_bytes = |i: u64| -> Vec<u8> {
                let mut k = vec![0u8; 128];
                k[..8].copy_from_slice(&i.to_be_bytes());
                k
            };
            let record_for = |k: &[u8]| {
                let regs = vec![Register::Value(Value::from_slice(k).unwrap())];
                ImmutableRecord::from_registers(&regs, regs.len()).unwrap()
            };

            let mut live: Vec<u64> = Vec::new();
            pager.begin_read_tx().unwrap();
            pager
                .io
                .block(|| pager.begin_write_tx(WalAutoActions::all_enabled()))
                .unwrap();
            for i in 0..600u64 {
                let record = record_for(&key_bytes(i));
                run_until_done(
                    || {
                        cursor.seek(
                            SeekKey::IndexKey(record.as_record_ref()),
                            SeekOp::GE { eq_only: false },
                        )
                    },
                    pager.deref(),
                )
                .unwrap();
                run_until_done(
                    || cursor.insert(&BTreeKey::new_index_key(record.as_record_ref())),
                    pager.deref(),
                )
                .unwrap();
                live.push(i);
            }
            // Delete a random half to churn interior dividers.
            for _ in 0..300 {
                let idx = rng.next_u64() as usize % live.len();
                let victim = live.swap_remove(idx);
                let record = record_for(&key_bytes(victim));
                let seek_result = run_until_done(
                    || {
                        cursor.seek(
                            SeekKey::IndexKey(record.as_record_ref()),
                            SeekOp::GE { eq_only: true },
                        )
                    },
                    pager.deref(),
                )
                .unwrap();
                if matches!(seek_result, SeekResult::TryAdvance) {
                    run_until_done(|| cursor.next(), pager.deref()).unwrap();
                }
                run_until_done(|| cursor.delete(), pager.deref()).unwrap();
            }
            // Re-insert EVERY surviving key with RowInserter's protocol:
            // seek GE eq_only, advance once on TryAdvance, insert.
            let mut tryadvance_reinserts = 0usize;
            for &survivor in &live {
                let record = record_for(&key_bytes(survivor));
                let seek_result = run_until_done(
                    || {
                        cursor.seek(
                            SeekKey::IndexKey(record.as_record_ref()),
                            SeekOp::GE { eq_only: true },
                        )
                    },
                    pager.deref(),
                )
                .unwrap();
                if matches!(seek_result, SeekResult::TryAdvance) {
                    tryadvance_reinserts += 1;
                    run_until_done(|| cursor.next(), pager.deref()).unwrap();
                }
                run_until_done(
                    || cursor.insert(&BTreeKey::new_index_key(record.as_record_ref())),
                    pager.deref(),
                )
                .unwrap();
            }
            let c = cursor.move_to_root().unwrap();
            if let Some(c) = c {
                pager.io.wait_for_completion(c).unwrap();
            }
            pager
                .io
                .block(|| pager.commit_tx(&conn, conn.get_sync_mode(), true))
                .unwrap();

            // Full scan: every key must appear exactly once.
            pager.begin_read_tx().unwrap();
            let _c = cursor.move_to_root().unwrap();
            run_until_done(|| cursor.rewind(), pager.deref()).unwrap();
            let mut seen: Vec<Vec<u8>> = Vec::new();
            while cursor.has_record() {
                let bytes = loop {
                    match cursor.record().unwrap() {
                        IOResult::Done(record) => {
                            let record = record.unwrap();
                            break record
                                .get_value_opt(0)
                                .and_then(|v| match v {
                                    crate::types::ValueRef::Blob(b) => Some(b.to_vec()),
                                    _ => None,
                                })
                                .unwrap();
                        }
                        IOResult::IO(io) => {
                            while !io.finished() {
                                pager.io.step().unwrap();
                            }
                        }
                    }
                };
                seen.push(bytes);
                run_until_done(|| cursor.next(), pager.deref()).unwrap();
            }
            pager.end_read_tx();
            let total = seen.len();
            seen.dedup();
            assert_eq!(
                total,
                seen.len(),
                "attempt {attempt}: duplicate index keys after RowInserter-style \
                 re-inserts (seed {seed}, {tryadvance_reinserts} TryAdvance re-inserts)"
            );
            assert_eq!(total, live.len(), "attempt {attempt}: scan count mismatch");
            tracing::info!(
                "attempt {attempt}: {} keys, {} TryAdvance re-inserts, no duplicates",
                total,
                tryadvance_reinserts
            );
        }
    }

    #[test]
    #[ignore]
    pub fn fuzz_long_btree_index_insert_delete_fuzz_run() {
        btree_index_insert_delete_fuzz_run(
            2,
            10000,
            |rng| {
                let min: u32 = 4;
                let size = min + rng.next_u32() % (1024 - min);
                size as usize
            },
            0.65,
        );
    }

    #[test]
    #[ignore]
    pub fn fuzz_long_btree_insert_fuzz_run_random() {
        btree_insert_fuzz_run(2, 10_000, |rng| (rng.next_u32() % 4096) as usize);
    }

    #[test]
    #[ignore]
    pub fn fuzz_long_btree_insert_fuzz_run_small() {
        btree_insert_fuzz_run(2, 10_000, |rng| (rng.next_u32() % 128) as usize);
    }

    #[test]
    #[ignore]
    pub fn fuzz_long_btree_insert_fuzz_run_big() {
        btree_insert_fuzz_run(2, 10_000, |rng| 3 * 1024 + (rng.next_u32() % 1024) as usize);
    }

    #[test]
    #[ignore]
    pub fn fuzz_long_btree_insert_fuzz_run_overflow() {
        btree_insert_fuzz_run(2, 5_000, |rng| (rng.next_u32() % 32 * 1024) as usize);
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn setup_test_env(database_size: u32) -> Arc<Pager> {
        let page_size = 512;

        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let buffer_pool = BufferPool::begin_init(&io, page_size * 128);

        let db_file = Arc::new(DatabaseFile::new(
            io.open_file(":memory:", OpenFlags::Create, false).unwrap(),
        ));

        let wal_file = io.open_file("test.wal", OpenFlags::Create, false).unwrap();
        let wal_shared = WalFileShared::new_shared(wal_file).unwrap();
        let last_checksum_and_max_frame = wal_shared.read().last_checksum_and_max_frame();
        let wal: Arc<dyn Wal> = Arc::new(WalFile::new(
            io.clone(),
            wal_shared,
            last_checksum_and_max_frame,
            buffer_pool.clone(),
        ));

        // For new empty databases, init_page_1 must be Some(page) so allocate_page1() can be called
        let init_page_1 = Arc::new(ArcSwapOption::new(Some(default_page1(None))));
        let pager = Arc::new(
            Pager::new(
                db_file,
                Some(wal),
                io,
                PageCache::new(10),
                buffer_pool,
                Arc::new(crate::sync::Mutex::new(())),
                init_page_1,
            )
            .unwrap(),
        );

        pager.io.step().unwrap();

        let _ = run_until_done(|| pager.allocate_page1(), &pager);
        for _ in 0..(database_size - 1) {
            let _res = pager.allocate_page().unwrap();
        }

        pager
            .io
            .block(|| {
                pager.with_header_mut(|header| {
                    header.page_size = PageSize::new(page_size as u32).unwrap()
                })
            })
            .unwrap();

        pager
    }

    #[test]
    pub fn test_clear_overflow_pages() -> Result<()> {
        let pager = setup_test_env(5);
        let num_columns = 5;

        let mut cursor = BTreeCursor::new_table(pager.clone(), 1, num_columns);

        let max_local = payload_overflow_threshold_max(PageType::TableLeaf, 4096);
        let usable_size = cursor.usable_space();

        // Create a large payload that will definitely trigger overflow
        let large_payload = vec![b'A'; max_local + usable_size];

        // Setup overflow pages (2, 3, 4) with linking
        let mut current_page = 2_usize;
        while current_page <= 4 {
            #[allow(clippy::arc_with_non_send_sync)]
            let buf = Arc::new(Buffer::new_temporary(
                pager
                    .io
                    .block(|| pager.with_header(|header| header.page_size))?
                    .get() as usize,
            ));
            let _buf = buf.clone();
            let c = Completion::new_write(move |_| {
                let _ = _buf.clone();
            });
            let _c =
                pager
                    .db_file
                    .write_page(current_page, buf.clone(), &IOContext::default(), c)?;
            pager.io.step()?;

            let (page, _c) = cursor.read_page_blocking(current_page as i64)?;
            while page.is_locked() {
                cursor.pager.io.step()?;
            }

            {
                let contents = page.get_contents();

                let next_page = if current_page < 4 {
                    current_page + 1
                } else {
                    0
                };
                contents.write_u32_no_offset(0, next_page as u32); // Write pointer to next overflow page

                let buf = contents.as_ptr();
                buf[4..].fill(b'A');
            }

            current_page += 1;
        }
        pager.io.step()?;

        // Create leaf cell pointing to start of overflow chain
        let leaf_cell = BTreeCell::TableLeafCell(TableLeafCell {
            rowid: 1,
            payload: unsafe { transmute::<&[u8], &'static [u8]>(large_payload.as_slice()) },
            first_overflow_page: Some(2), // Point to first overflow page
            payload_size: large_payload.len() as u64,
        });

        let initial_freelist_pages = pager
            .io
            .block(|| pager.with_header(|header| header.freelist_pages))?
            .get();
        // Clear overflow pages
        pager.io.block(|| cursor.clear_overflow_pages(&leaf_cell))?;
        let (freelist_pages, freelist_trunk_page) = pager
            .io
            .block(|| {
                pager.with_header(|header| {
                    (
                        header.freelist_pages.get(),
                        header.freelist_trunk_page.get(),
                    )
                })
            })
            .unwrap();

        // Verify proper number of pages were added to freelist
        assert_eq!(
            freelist_pages,
            initial_freelist_pages + 3,
            "Expected 3 pages to be added to freelist"
        );

        // If this is first trunk page
        let trunk_page_id = freelist_trunk_page;
        if trunk_page_id > 0 {
            // Verify trunk page structure
            let (trunk_page, _c) = cursor.read_page_blocking(trunk_page_id as i64)?;
            let contents = trunk_page.get_contents();
            // Read number of leaf pages in trunk
            let n_leaf = contents.read_u32_no_offset(4);
            assert!(n_leaf > 0, "Trunk page should have leaf entries");

            for i in 0..n_leaf {
                let leaf_page_id = contents.read_u32_no_offset(8 + (i as usize * 4));
                assert!(
                    (2..=4).contains(&leaf_page_id),
                    "Leaf page ID {leaf_page_id} should be in range 2-4"
                );
            }
        }

        Ok(())
    }

    #[test]
    fn test_process_overflow_read_inconsistent_chain_returns_corrupt() -> Result<()> {
        let pager = setup_test_env(3);
        let mut cursor = BTreeCursor::new_table(pager.clone(), 1, 5);

        let (overflow_page, c) = cursor.read_page_blocking(2)?;
        if let Some(c) = c {
            pager.io.wait_for_completion(c)?;
        }
        while overflow_page.is_locked() {
            pager.io.step()?;
        }

        let overflow_contents = overflow_page.get_contents();
        overflow_contents.write_u32_no_offset(0, 0);
        overflow_contents.as_ptr()[4..].fill(b'Z');

        let local_payload: &'static [u8] = Box::leak(vec![b'Y'; 32].into_boxed_slice());
        let payload_size = local_payload.len() as u64 + ((cursor.usable_space() - 4) as u64 * 2);
        let cursor_pager = cursor.pager.clone();

        let err = run_until_done(
            || cursor.process_overflow_read(local_payload, 2, payload_size),
            &cursor_pager,
        )
        .expect_err("inconsistent overflow chain should fail with Corrupt");
        assert!(matches!(err, LimboError::Corrupt(_)));
        assert!(cursor.read_overflow_state.is_none());
        Ok(())
    }

    /// Forces a spill yield from `read_page(next-chain-page)` during
    /// `process_overflow_read`'s loop and verifies the resulting record
    /// holds the chain's bytes in order with no duplicates and no truncation.
    #[test]
    fn process_overflow_read_survives_spill_yield_from_next_chain_read() {
        let pager = setup_test_env(5);
        let mut cursor = BTreeCursor::new_table(pager.clone(), 1, 5);
        let usable = cursor.usable_space();
        let data_per_page = usable - 4;

        // Re-purpose pages 4 and 5 as a 2-page overflow chain: 4 -> 5 -> end.
        let load_and_fill = |id: i64, next: u32, fill: u8| {
            let (page, c) = cursor.read_page_blocking(id).unwrap();
            if let Some(c) = c {
                pager.io.wait_for_completion(c).unwrap();
            }
            while page.is_locked() {
                pager.io.step().unwrap();
            }
            let buf = page.get_contents().as_ptr();
            buf[0..4].copy_from_slice(&next.to_be_bytes());
            buf[4..usable].fill(fill);
        };
        load_and_fill(4, 5, b'A');
        load_and_fill(5, 0, b'B');

        // Arm `read_page(5)` to yield IO once
        pager.arm_spill_yield_on_read(5, 0);

        let payload_size = (2 * data_per_page) as u64;
        let cursor_pager = cursor.pager.clone();
        run_until_done(
            || cursor.process_overflow_read(b"", 4, payload_size),
            &cursor_pager,
        )
        .unwrap();

        let rec_slot = cursor.get_immutable_record_or_create().unwrap();
        let bytes = rec_slot.as_ref().unwrap().get_payload();
        assert_eq!(bytes.len(), 2 * data_per_page);
        assert!(bytes[..data_per_page].iter().all(|&b| b == b'A'));
        assert!(bytes[data_per_page..].iter().all(|&b| b == b'B'));
    }

    /// Forces a real spill yield from the finalization `move_to_root_nonblock`
    /// at the end of `count`'s traversal and verifies the returned tally
    /// equals the true cell count.
    #[test]
    fn count_survives_spill_yield_at_finalization() {
        let (pager, root_page, _db, _conn) = empty_btree();
        let n: u16 = 7;

        // `count()` only reads cell_count from the header; no real cells needed.
        let (root, _c) = pager.io.block(|| pager.read_page(root_page)).unwrap();
        while root.is_locked() {
            pager.io.step().unwrap();
        }
        root.get_contents().write_cell_count(n);

        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, 5);

        // `count` calls `move_to_root_nonblock` -> `read_page(root)` twice:
        // once in `Start` to push the root, and once at finalization. Skip
        // the first; yield on the second.
        pager.arm_spill_yield_on_read(root_page, 1);

        let final_count = run_until_done(|| cursor.count(), &pager).unwrap();
        assert_eq!(final_count, n as usize);
    }

    #[test]
    pub fn test_clear_overflow_pages_no_overflow() -> Result<()> {
        let pager = setup_test_env(5);
        let num_columns = 5;

        let mut cursor = BTreeCursor::new_table(pager.clone(), 1, num_columns);

        let small_payload = vec![b'A'; 10];

        // Create leaf cell with no overflow pages
        let leaf_cell = BTreeCell::TableLeafCell(TableLeafCell {
            rowid: 1,
            payload: unsafe { transmute::<&[u8], &'static [u8]>(small_payload.as_slice()) },
            first_overflow_page: None,
            payload_size: small_payload.len() as u64,
        });

        let initial_freelist_pages = pager
            .io
            .block(|| pager.with_header(|header| header.freelist_pages))?
            .get() as usize;

        // Try to clear non-existent overflow pages
        pager.io.block(|| cursor.clear_overflow_pages(&leaf_cell))?;
        let (freelist_pages, freelist_trunk_page) = pager.io.block(|| {
            pager.with_header(|header| {
                (
                    header.freelist_pages.get(),
                    header.freelist_trunk_page.get(),
                )
            })
        })?;

        // Verify freelist was not modified
        assert_eq!(
            freelist_pages as usize, initial_freelist_pages,
            "Freelist should not change when no overflow pages exist"
        );

        // Verify trunk page wasn't created
        assert_eq!(
            freelist_trunk_page, 0,
            "No trunk page should be created when no overflow pages exist"
        );

        Ok(())
    }

    #[test]
    fn test_btree_destroy() -> Result<()> {
        let initial_size = 1;
        let pager = setup_test_env(initial_size);
        let num_columns = 5;

        let mut cursor = BTreeCursor::new_table(pager.clone(), 2, num_columns);

        // Initialize page 2 as a root page (interior)
        let root_page = run_until_done(
            || cursor.allocate_page(PageType::TableInterior, 0),
            &cursor.pager,
        )?;

        // Allocate two leaf pages
        let page3 = run_until_done(
            || cursor.allocate_page(PageType::TableLeaf, 0),
            &cursor.pager,
        )?;
        let page4 = run_until_done(
            || cursor.allocate_page(PageType::TableLeaf, 0),
            &cursor.pager,
        )?;

        // Configure the root page to point to the two leaf pages
        {
            let contents = root_page.get_contents();

            // Set rightmost pointer to page4
            contents.write_rightmost_ptr(page4.get().id as u32);

            // Create a cell with pointer to page3
            let cell_content = vec![
                // First 4 bytes: left child pointer (page3)
                (page3.get().id >> 24) as u8,
                (page3.get().id >> 16) as u8,
                (page3.get().id >> 8) as u8,
                page3.get().id as u8,
                // Next byte: rowid as varint (simple value 100)
                100,
            ];

            // Insert the cell
            insert_into_cell(contents, &cell_content, 0, 512)?;
        }

        // Add a simple record to each leaf page
        for page in [&page3, &page4] {
            let contents = page.get_contents();

            // Simple record with just a rowid and payload
            let record_bytes = vec![
                5,                   // Payload length (varint)
                page.get().id as u8, // Rowid (varint)
                b'h',
                b'e',
                b'l',
                b'l',
                b'o', // Payload
            ];

            insert_into_cell(contents, &record_bytes, 0, 512)?;
        }

        // Verify structure before destruction
        assert_eq!(
            pager
                .io
                .block(|| pager.with_header(|header| header.database_size))?
                .get(),
            4, // We should have pages 1-4
            "Database should have 4 pages total"
        );

        // Track freelist state before destruction
        let initial_free_pages = pager
            .io
            .block(|| pager.with_header(|header| header.freelist_pages))?
            .get();
        assert_eq!(initial_free_pages, 0, "should start with no free pages");

        run_until_done(|| cursor.btree_destroy(), pager.deref())?;

        let pages_freed = pager
            .io
            .block(|| pager.with_header(|header| header.freelist_pages))?
            .get()
            - initial_free_pages;
        assert_eq!(pages_freed, 3, "should free 3 pages (root + 2 leaves)");

        Ok(())
    }

    #[test]
    pub fn test_clear_btree_with_single_page() -> Result<()> {
        let (pager, root_page, _, _) = empty_btree();
        let num_columns = 5;
        let record_count = 10;

        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);

        for rowid in 1..=record_count {
            insert_record(&mut cursor, &pager, rowid, Value::from_i64(rowid))?;
        }

        let page_count = pager
            .io
            .block(|| pager.with_header(|header| header.database_size.get()))?;
        assert_eq!(
            page_count, 2,
            "expected two pages (header + root), got {page_count}"
        );

        run_until_done(|| cursor.clear_btree(), &pager)?;

        assert_btree_empty(&mut cursor, pager.deref())
    }

    #[test]
    pub fn test_clear_btree_with_multiple_pages() -> Result<()> {
        let (pager, root_page, _, _) = empty_btree();
        let num_columns = 5;
        let record_count = 1000;

        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);

        for rowid in 1..=record_count {
            insert_record(&mut cursor, &pager, rowid, Value::from_i64(rowid))?;
        }

        // Ensure enough records were created so the tree spans multiple pages.
        let page_count = pager
            .io
            .block(|| pager.with_header(|header| header.database_size.get()))?;
        assert!(
            page_count > 2,
            "expected more pages than just header + root, got {page_count}"
        );

        run_until_done(|| cursor.clear_btree(), &pager)?;

        assert_btree_empty(&mut cursor, pager.deref())
    }

    #[test]
    pub fn test_clear_btree_reinsertion() -> Result<()> {
        let (pager, root_page, _, _) = empty_btree();
        let num_columns = 5;
        let record_count = 1000;

        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);

        for rowid in 1..=record_count {
            insert_record(&mut cursor, &pager, rowid, Value::from_i64(rowid))?;
        }

        run_until_done(|| cursor.clear_btree(), &pager)?;

        // Reinsert into cleared B-tree to ensure it’s still functional
        for rowid in 1..=record_count {
            insert_record(&mut cursor, &pager, rowid, Value::from_i64(rowid))?;
        }

        if let (_, false) = validate_btree(pager.clone(), root_page) {
            panic!("Invalid B-tree after reinsertion");
        }

        let _c = cursor.move_to_root()?;
        for i in 1..=record_count {
            run_until_done(|| cursor.next(), &pager)?;
            let exists = cursor.has_record();
            assert!(exists, "Record {i} not found");

            let record = loop {
                match cursor.record()? {
                    IOResult::Done(r) => break r,
                    IOResult::IO(io) => io.wait(&*pager.io)?,
                }
            }
            .unwrap();
            let value = record.get_value(0)?;
            assert_eq!(
                value,
                ValueRef::Numeric(Numeric::Integer(i)),
                "Unexpected value for record {i}",
            );
        }

        Ok(())
    }

    #[test]
    pub fn test_clear_btree_multiple_cursors() -> Result<()> {
        let (pager, root_page, _, _) = empty_btree();
        let num_columns = 5;
        let record_count = 1000;

        let mut cursor1 = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
        let mut cursor2 = BTreeCursor::new_table(pager.clone(), root_page, num_columns);

        // Use cursor1 to insert records
        for rowid in 1..=record_count {
            insert_record(&mut cursor1, &pager, rowid, Value::from_i64(rowid))?;
        }

        // Use cursor1 to clear the btree
        run_until_done(|| cursor1.clear_btree(), &pager)?;

        // Verify that cursor2 works correctly
        assert_btree_empty(&mut cursor2, pager.deref())?;

        // Insert using cursor2
        insert_record(&mut cursor1, &pager, 1, Value::from_i64(123))?;

        if let (_, false) = validate_btree(pager.clone(), root_page) {
            panic!("Invalid B-tree after insertion");
        }

        let key = Value::from_i64(1);
        let exists = run_until_done(|| cursor2.exists(&key), pager.deref())?;
        assert!(exists, "key not found {key}");

        Ok(())
    }

    /// Regression test: after clear_btree() on one cursor and invalidate_btree_cache()
    /// on a sibling cursor sharing the same btree (e.g. OpenDup), the count cache must
    /// be reset. Otherwise count() returns the stale value from before the clear.
    ///
    /// This is the mechanism behind stale partition counts in window functions:
    /// ResetSorter calls clear_btree on the main cursor and invalidate_btree_cache on
    /// OpenDup cursors. If count_state/count are not reset, the Count instruction on the
    /// dup cursor returns the previous partition's row count.
    #[test]
    pub fn test_clear_btree_resets_count_cache() -> Result<()> {
        let (pager, root_page, _, _) = empty_btree();
        let num_columns = 1;

        let mut cursor_main = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
        let mut cursor_dup = BTreeCursor::new_table(pager.clone(), root_page, num_columns);

        // Insert 5 records (simulating partition 'a' with 5 rows)
        for rowid in 1..=5 {
            insert_record(&mut cursor_main, &pager, rowid, Value::from_i64(rowid))?;
        }

        // Count via the dup cursor -- should be 5 and caches the result
        let count1 = run_until_done(|| cursor_dup.count(), pager.deref())?;
        assert_eq!(count1, 5, "first count should be 5");

        // Simulate ResetSorter: clear the btree via the main cursor
        run_until_done(|| cursor_main.clear_btree(), &pager)?;
        // Invalidate sibling cursor's cache (as op_reset_sorter does)
        cursor_dup.invalidate_btree_cache();

        // Insert only 2 records (simulating partition 'b' with 2 rows)
        for rowid in 1..=2 {
            insert_record(&mut cursor_main, &pager, rowid, Value::from_i64(rowid + 10))?;
        }

        // Count via the dup cursor again -- must be 2, not the stale 5
        let count2 = run_until_done(|| cursor_dup.count(), pager.deref())?;
        assert_eq!(
            count2, 2,
            "count after clear + re-insert should be 2, got stale count if cache was not reset"
        );

        Ok(())
    }

    /// Verify that clear_btree() resets its own count cache, not just sibling cursors.
    #[test]
    pub fn test_clear_btree_resets_own_count_cache() -> Result<()> {
        let (pager, root_page, _, _) = empty_btree();
        let num_columns = 1;

        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);

        // Insert 5 records and count
        for rowid in 1..=5 {
            insert_record(&mut cursor, &pager, rowid, Value::from_i64(rowid))?;
        }
        let count1 = run_until_done(|| cursor.count(), pager.deref())?;
        assert_eq!(count1, 5);

        // Clear and re-insert 3 records
        run_until_done(|| cursor.clear_btree(), &pager)?;
        for rowid in 1..=3 {
            insert_record(&mut cursor, &pager, rowid, Value::from_i64(rowid + 10))?;
        }

        // Count should reflect the new 3 records, not the stale 5
        let count2 = run_until_done(|| cursor.count(), pager.deref())?;
        assert_eq!(
            count2, 3,
            "count after clear_btree + re-insert should be 3, not stale 5"
        );

        Ok(())
    }

    /// Verify that insert() invalidates the count cache so a subsequent count()
    /// re-traverses the btree instead of returning the stale cached value.
    #[test]
    pub fn test_insert_invalidates_count_cache() -> Result<()> {
        let (pager, root_page, _, _) = empty_btree();
        let num_columns = 1;

        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);

        // Insert 3 records and count
        for rowid in 1..=3 {
            insert_record(&mut cursor, &pager, rowid, Value::from_i64(rowid))?;
        }
        let count1 = run_until_done(|| cursor.count(), pager.deref())?;
        assert_eq!(count1, 3, "initial count should be 3");

        // Insert 2 more records
        for rowid in 4..=5 {
            insert_record(&mut cursor, &pager, rowid, Value::from_i64(rowid))?;
        }

        // Count should reflect all 5 records, not the stale 3
        let count2 = run_until_done(|| cursor.count(), pager.deref())?;
        assert_eq!(
            count2, 5,
            "count after additional inserts should be 5, not stale 3"
        );

        Ok(())
    }

    #[test]
    pub fn test_clear_btree_with_overflow_pages() -> Result<()> {
        let (pager, root_page, _, _) = empty_btree();
        let num_columns = 5;
        let record_count = 100;

        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);

        let initial_page_count = pager
            .io
            .block(|| pager.with_header(|header| header.database_size.get()))?;

        for rowid in 1..=record_count {
            let large_blob = crate::alloc::vec![b'A'; 8192];
            insert_record(&mut cursor, &pager, rowid, Value::Blob(large_blob))?;
        }

        let page_count_after_inserts = pager
            .io
            .block(|| pager.with_header(|header| header.database_size.get()))?;
        let created_pages = page_count_after_inserts - initial_page_count;
        assert!(
            created_pages > record_count as u32,
            "expected more pages to be created than records, got {created_pages}"
        );

        run_until_done(|| cursor.clear_btree(), &pager)?;

        assert_btree_empty(&mut cursor, pager.deref())
    }

    #[test]
    pub fn test_defragment() {
        let (db, _temp_dir) = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);

        let page_contents = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        for i in 0..3 {
            let regs = &[Register::Value(Value::from_i64(i as i64))];
            let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
            let payload = add_record(i, i, page.clone(), record, &conn);
            assert_eq!(page_contents.cell_count(), i + 1);
            let free = compute_free_space(page_contents, usable_space).unwrap();
            total_size += payload.len() + 2;
            assert_eq!(free, 4096 - total_size - header_size);
            cells.push(Cell { pos: i, payload });
        }

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page_contents, i, &cell.payload);
        }
        cells.remove(1);
        drop_cell(page_contents, 1, usable_space).unwrap();

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page_contents, i, &cell.payload);
        }

        defragment_page(page_contents, usable_space, 4).unwrap();

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page_contents, i, &cell.payload);
        }
    }

    #[test]
    pub fn test_drop_odd_with_defragment() {
        let (db, _temp_dir) = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);

        let page_contents = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        let total_cells = 10;
        for i in 0..total_cells {
            let regs = &[Register::Value(Value::from_i64(i as i64))];
            let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
            let payload = add_record(i, i, page.clone(), record, &conn);
            assert_eq!(page_contents.cell_count(), i + 1);
            let free = compute_free_space(page_contents, usable_space).unwrap();
            total_size += payload.len() + 2;
            assert_eq!(free, 4096 - total_size - header_size);
            cells.push(Cell { pos: i, payload });
        }

        let mut removed = 0;
        let mut new_cells = Vec::new();
        for cell in cells {
            if cell.pos % 2 == 1 {
                drop_cell(page_contents, cell.pos - removed, usable_space).unwrap();
                removed += 1;
            } else {
                new_cells.push(cell);
            }
        }
        let cells = new_cells;
        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page_contents, i, &cell.payload);
        }

        defragment_page(page_contents, usable_space, 4).unwrap();

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page_contents, i, &cell.payload);
        }
    }

    #[test]
    pub fn test_fuzz_drop_defragment_insert() {
        let (db, _temp_dir) = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);

        let page_contents = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        let mut i = 100000;
        let seed = rng().random();
        tracing::info!("seed {}", seed);
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        while i > 0 {
            i -= 1;
            match rng.next_u64() % 4 {
                0 => {
                    // allow appends with extra place to insert
                    let cell_idx = rng.next_u64() as usize % (page_contents.cell_count() + 1);
                    let free = compute_free_space(page_contents, usable_space).unwrap();
                    let regs = &[Register::Value(Value::from_i64(i as i64))];
                    let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
                    let mut payload: crate::alloc::Vec<u8> = crate::alloc::vec![];
                    let mut fill_cell_payload_state = FillCellPayloadState::Start;
                    run_until_done(
                        || {
                            fill_cell_payload(
                                &PinGuard::new(page.clone()),
                                Some(i as i64),
                                &mut payload,
                                cell_idx,
                                &record,
                                4096,
                                conn.pager.load().clone(),
                                &mut fill_cell_payload_state,
                            )
                        },
                        &conn.pager.load().clone(),
                    )
                    .unwrap();
                    if (free as usize) < payload.len() + 2 {
                        // do not try to insert overflow pages because they require balancing
                        continue;
                    }
                    insert_into_cell(page_contents, &payload, cell_idx, 4096).unwrap();
                    assert!(page_contents.overflow_cells.is_empty());
                    total_size += payload.len() + 2;
                    cells.insert(cell_idx, Cell { pos: i, payload });
                }
                1 => {
                    if page_contents.cell_count() == 0 {
                        continue;
                    }
                    let cell_idx = rng.next_u64() as usize % page_contents.cell_count();
                    let (_, len) = page_contents
                        .cell_get_raw_region(cell_idx, usable_space)
                        .unwrap();
                    drop_cell(page_contents, cell_idx, usable_space).unwrap();
                    total_size -= len + 2;
                    cells.remove(cell_idx);
                }
                2 => {
                    defragment_page(page_contents, usable_space, 4).unwrap();
                }
                3 => {
                    // check cells
                    for (i, cell) in cells.iter().enumerate() {
                        ensure_cell(page_contents, i, &cell.payload);
                    }
                    assert_eq!(page_contents.cell_count(), cells.len());
                }
                _ => unreachable!(),
            }
            let free = compute_free_space(page_contents, usable_space).unwrap();
            assert_eq!(free, 4096 - total_size - header_size);
        }
    }

    #[test]
    pub fn test_fuzz_drop_defragment_insert_issue_1085() {
        // This test is used to demonstrate that issue at https://github.com/tursodatabase/turso/issues/1085
        // is FIXED.
        let (db, _temp_dir) = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);

        let page_contents = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let usable_space = 4096;
        let mut i = 1000;
        for seed in [15292777653676891381, 9261043168681395159] {
            tracing::info!("seed {}", seed);
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            while i > 0 {
                i -= 1;
                match rng.next_u64() % 3 {
                    0 => {
                        // allow appends with extra place to insert
                        let cell_idx = rng.next_u64() as usize % (page_contents.cell_count() + 1);
                        let free = compute_free_space(page_contents, usable_space).unwrap();
                        let regs = &[Register::Value(Value::from_i64(i))];
                        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
                        let mut payload: crate::alloc::Vec<u8> = crate::alloc::vec![];
                        let mut fill_cell_payload_state = FillCellPayloadState::Start;
                        run_until_done(
                            || {
                                fill_cell_payload(
                                    &PinGuard::new(page.clone()),
                                    Some(i),
                                    &mut payload,
                                    cell_idx,
                                    &record,
                                    4096,
                                    conn.pager.load().clone(),
                                    &mut fill_cell_payload_state,
                                )
                            },
                            &conn.pager.load().clone(),
                        )
                        .unwrap();
                        if (free as usize) < payload.len() - 2 {
                            // do not try to insert overflow pages because they require balancing
                            continue;
                        }
                        insert_into_cell(page_contents, &payload, cell_idx, 4096).unwrap();
                        assert!(page_contents.overflow_cells.is_empty());
                        total_size += payload.len() + 2;
                    }
                    1 => {
                        if page_contents.cell_count() == 0 {
                            continue;
                        }
                        let cell_idx = rng.next_u64() as usize % page_contents.cell_count();
                        let (_, len) = page_contents
                            .cell_get_raw_region(cell_idx, usable_space)
                            .unwrap();
                        drop_cell(page_contents, cell_idx, usable_space).unwrap();
                        total_size -= len + 2;
                    }
                    2 => {
                        defragment_page(page_contents, usable_space, 4).unwrap();
                    }
                    _ => unreachable!(),
                }
                let free = compute_free_space(page_contents, usable_space).unwrap();
                assert_eq!(free, 4096 - total_size - header_size);
            }
        }
    }

    // this test will create a tree like this:
    // -page:2, ptr(right):4
    // +cells:node[rowid:14, ptr(<=):3]
    //   -page:3, ptr(right):0
    //   +cells:leaf[rowid:11, len(payload):137, overflow:false]
    //   -page:4, ptr(right):0
    //   +cells:
    #[test]
    pub fn test_drop_page_in_balancing_issue_1203() {
        let (db, _temp_dir) = get_database();
        let conn = db.connect().unwrap();

        let queries = vec![
"CREATE TABLE lustrous_petit (awesome_nomous TEXT,ambitious_amargi TEXT,fantastic_daniels BLOB,stupendous_highleyman TEXT,relaxed_crane TEXT,elegant_bromma INTEGER,proficient_castro BLOB,ambitious_liman TEXT,responsible_lusbert BLOB);",
"INSERT INTO lustrous_petit VALUES ('funny_sarambi', 'hardworking_naoumov', X'666561726C6573735F68696C6C', 'elegant_iafd', 'rousing_flag', 681399778772406122, X'706572736F6E61626C655F676F6477696E6772696D6D', 'insightful_anonymous', X'706F77657266756C5F726F636861'), ('personable_holmes', 'diligent_pera', X'686F6E6573745F64696D656E73696F6E', 'energetic_raskin', 'gleaming_federasyon', -2778469859573362611, X'656666696369656E745F6769617A', 'sensible_skirda', X'66616E7461737469635F6B656174696E67'), ('inquisitive_baedan', 'brave_sphinx', X'67656E65726F75735F6D6F6E7473656E79', 'inquisitive_syndicate', 'amiable_room', 6954857961525890638, X'7374756E6E696E675F6E6965747A73636865', 'glowing_coordinator', X'64617A7A6C696E675F7365766572696E65'), ('upbeat_foxtale', 'engaging_aktimon', X'63726561746976655F6875746368696E6773', 'ample_locura', 'creative_barrett', 6413352509911171593, X'6772697070696E675F6D696E7969', 'competitive_parissi', X'72656D61726B61626C655F77696E7374616E6C6579');",
"INSERT INTO lustrous_petit VALUES ('ambitious_berry', 'devoted_marshall', X'696E7175697369746976655F6C6172657661', 'flexible_pramen', 'outstanding_stauch', 6936508362673228293, X'6C6F76696E675F6261756572', 'charming_anonymous', X'68617264776F726B696E675F616E6E6973'), ('enchanting_cohen', 'engaging_rubel', X'686F6E6573745F70726F766F63617A696F6E65', 'humorous_robin', 'imaginative_shuzo', 4762266264295288131, X'726F7573696E675F6261796572', 'vivid_bolling', X'6F7267616E697A65645F7275696E73'), ('affectionate_resistance', 'gripping_rustamova', X'6B696E645F6C61726B696E', 'bright_boulanger', 'upbeat_ashirov', -1726815435854320541, X'61646570745F66646361', 'dazzling_tashjian', X'68617264776F726B696E675F6D6F72656C'), ('zestful_ewald', 'favorable_lewis', X'73747570656E646F75735F7368616C6966', 'bright_combustion', 'blithesome_harding', 8408539013935554176, X'62726176655F737079726F706F756C6F75', 'hilarious_finnegan', X'676976696E675F6F7267616E697A696E67'), ('blithesome_picqueray', 'sincere_william', X'636F75726167656F75735F6D69746368656C6C', 'rousing_atan', 'mirthful_katie', -429232313453215091, X'6C6F76656C795F776174616E616265', 'stupendous_mcmillan', X'666F63757365645F6B61666568'), ('incredible_kid', 'friendly_yvetot', X'706572666563745F617A697A', 'helpful_manhattan', 'shining_horrox', -4318061095860308846, X'616D626974696F75735F726F7765', 'twinkling_anarkiya', X'696D6167696E61746976655F73756D6E6572');",
"INSERT INTO lustrous_petit VALUES ('sleek_graeber', 'approachable_ghazzawi', X'62726176655F6865776974747768697465', 'adaptable_zimmer', 'polite_cohn', -5464225138957223865, X'68756D6F726F75735F736E72', 'adaptable_igualada', X'6C6F76656C795F7A686F75'), ('imaginative_rautiainen', 'magnificent_ellul', X'73706C656E6469645F726F6361', 'responsible_brown', 'upbeat_uruguaya', -1185340834321792223, X'616D706C655F6D6470', 'philosophical_kelly', X'676976696E675F6461676865726D6172676F7369616E'), ('blithesome_darkness', 'creative_newell', X'6C757374726F75735F61706174726973', 'engaging_kids', 'charming_wark', -1752453819873942466, X'76697669645F6162657273', 'independent_barricadas', X'676C697374656E696E675F64686F6E6474'), ('productive_chardronnet', 'optimistic_karnage', X'64696C6967656E745F666F72657374', 'engaging_beggar', 'sensible_wolke', 784341549042407442, X'656E676167696E675F6265726B6F7769637A', 'blithesome_zuzenko', X'6E6963655F70726F766F63617A696F6E65');",
"INSERT INTO lustrous_petit VALUES ('shining_sagris', 'considerate_mother', X'6F70656E5F6D696E6465645F72696F74', 'polite_laufer', 'patient_mink', 2240393952789100851, X'636F75726167656F75735F6D636D696C6C616E', 'glowing_robertson', X'68656C7066756C5F73796D6F6E6473'), ('dazzling_glug', 'stupendous_poznan', X'706572736F6E61626C655F6672616E6B73', 'open_minded_ruins', 'qualified_manes', 2937238916206423261, X'696E736967687466756C5F68616B69656C', 'passionate_borl', X'616D6961626C655F6B7570656E647561'), ('wondrous_parry', 'knowledgeable_giovanni', X'6D6F76696E675F77696E6E', 'shimmering_aberlin', 'affectionate_calhoun', 702116954493913499, X'7265736F7572636566756C5F62726F6D6D61', 'propitious_mezzagarcia', X'746563686E6F6C6F676963616C5F6E6973686974616E69');",
"INSERT INTO lustrous_petit VALUES ('kind_room', 'hilarious_crow', X'6F70656E5F6D696E6465645F6B6F74616E7969', 'hardworking_petit', 'adaptable_zarrow', 2491343172109894986, X'70726F647563746976655F646563616C6F677565', 'willing_sindikalis', X'62726561746874616B696E675F6A6F7264616E');",
"INSERT INTO lustrous_petit VALUES ('confident_etrebilal', 'agreeable_shifu', X'726F6D616E7469635F7363687765697A6572', 'loving_debs', 'gripping_spooner', -3136910055229112693, X'677265676172696F75735F736B726F7A6974736B79', 'ample_ontiveros', X'7175616C69666965645F726F6D616E69656E6B6F'), ('competitive_call', 'technological_egoumenides', X'6469706C6F6D617469635F6D6F6E616768616E', 'willing_stew', 'frank_neal', -5973720171570031332, X'6C6F76696E675F6465737461', 'dazzling_gambone', X'70726F647563746976655F6D656E64656C676C6565736F6E'), ('favorable_delesalle', 'sensible_atterbury', X'666169746866756C5F64617861', 'bountiful_aldred', 'marvelous_malgraith', 5330463874397264493, X'706572666563745F7765726265', 'lustrous_anti', X'6C6F79616C5F626F6F6B6368696E'), ('stellar_corlu', 'loyal_espana', X'6D6F76696E675F7A6167', 'efficient_nelson', 'qualified_shepard', 1015518116803600464, X'737061726B6C696E675F76616E6469766572', 'loving_scoffer', X'686F6E6573745F756C72696368'), ('adaptable_taylor', 'shining_yasushi', X'696D6167696E61746976655F776974746967', 'alluring_blackmore', 'zestful_coeurderoy', -7094136731216188999, X'696D6167696E61746976655F757A63617465677569', 'gleaming_hernandez', X'6672616E6B5F646F6D696E69636B'), ('competitive_luis', 'stellar_fredericks', X'616772656561626C655F6D696368656C', 'optimistic_navarro', 'funny_hamilton', 4003895682491323194, X'6F70656E5F6D696E6465645F62656C6D6173', 'incredible_thorndycraft', X'656C6567616E745F746F6C6B69656E'), ('remarkable_parsons', 'sparkling_ulrich', X'737061726B6C696E675F6D6172696E636561', 'technological_leighlais', 'warmhearted_konok', -5789111414354869563, X'676976696E675F68657272696E67', 'adept_dabtara', X'667269656E646C795F72617070');",
"INSERT INTO lustrous_petit VALUES ('hardworking_norberg', 'approachable_winter', X'62726176655F68617474696E6768', 'imaginative_james', 'open_minded_capital', -5950508516718821688, X'6C757374726F75735F72616E7473', 'warmhearted_limanov', X'696E736967687466756C5F646F637472696E65'), ('generous_shatz', 'generous_finley', X'726176697368696E675F6B757A6E6574736F76', 'stunning_arrigoni', 'favorable_volcano', -8442328990977069526, X'6D6972746866756C5F616C7467656C64', 'thoughtful_zurbrugg', X'6D6972746866756C5F6D6F6E726F65'), ('frank_kerr', 'splendid_swain', X'70617373696F6E6174655F6D6470', 'flexible_dubey', 'sensible_tj', 6352949260574274181, X'656666696369656E745F6B656D736B79', 'vibrant_ege', X'736C65656B5F6272696768746F6E'), ('organized_neal', 'glistening_sugar', X'656E676167696E675F6A6F72616D', 'romantic_krieger', 'qualified_corr', -4774868512022958085, X'706572666563745F6B6F7A6172656B', 'bountiful_zaikowska', X'74686F7567687466756C5F6C6F6767616E73'), ('excellent_lydiettcarrion', 'diligent_denslow', X'666162756C6F75735F6D616E68617474616E', 'confident_tomar', 'glistening_ligt', -1134906665439009896, X'7175616C69666965645F6F6E6B656E', 'remarkable_anarkiya', X'6C6F79616C5F696E64616261'), ('passionate_melis', 'loyal_xsilent', X'68617264776F726B696E675F73637564', 'lustrous_barnes', 'nice_sugako', -4097897163377829983, X'726F6D616E7469635F6461686572', 'bright_imrie', X'73656E7369626C655F6D61726B'), ('giving_mlb', 'breathtaking_fourier', X'736C65656B5F616E61726368697374', 'glittering_malet', 'brilliant_crew', 8791228049111405793, X'626F756E746966756C5F626576656E736565', 'lovely_swords', X'70726F706974696F75735F696E656469746173'), ('honest_wright', 'qualified_rabble', X'736C65656B5F6D6172656368616C', 'shimmering_marius', 'blithesome_mckelvie', -1330737263592370654, X'6F70656E5F6D696E6465645F736D616C6C', 'energetic_gorman', X'70726F706974696F75735F6B6F74616E7969');",
"DELETE FROM lustrous_petit WHERE (ambitious_liman > 'adept_dabtaqu');",
"INSERT INTO lustrous_petit VALUES ('technological_dewey', 'fabulous_st', X'6F7074696D69737469635F73687562', 'considerate_levy', 'adaptable_kernis', 4195134012457716562, X'61646570745F736F6C6964617269646164', 'vibrant_crump', X'6C6F79616C5F72796E6572'), ('super_marjan', 'awesome_gethin', X'736C65656B5F6F737465727765696C', 'diplomatic_loidl', 'qualified_bokani', -2822676417968234733, X'6272696768745F64756E6C6170', 'creative_en', X'6D6972746866756C5F656C6F6666'), ('philosophical_malet', 'unique_garcia', X'76697669645F6E6F7262657267', 'spellbinding_fire', 'faithful_barringtonbush', -7293711848773657758, X'6272696C6C69616E745F6F6B65656665', 'gripping_guillon', X'706572736F6E61626C655F6D61726C696E7370696B65'), ('thoughtful_morefus', 'lustrous_rodriguez', X'636F6E666964656E745F67726F73736D616E726F73686368696E', 'devoted_jackson', 'propitious_karnage', -7802999054396485709, X'63617061626C655F64', 'enchanting_orwell', X'7477696E6B6C696E675F64616C616B6F676C6F75'), ('alluring_guillon', 'brilliant_pinotnoir', X'706572736F6E61626C655F6A6165636B6C65', 'open_minded_azeez', 'courageous_romania', 2126962403055072268, X'746563686E6F6C6F676963616C5F6962616E657A', 'open_minded_rosa', X'6C757374726F75735F6575726F7065'), ('courageous_kolokotronis', 'inquisitive_gahman', X'677265676172696F75735F626172726574', 'ambitious_shakur', 'fantastic_apatris', -1232732971861520864, X'737061726B6C696E675F7761746368', 'captivating_clover', X'636F6E666964656E745F736574686E65737363617374726F'), ('charming_sullivan', 'focused_congress', X'7368696D6D6572696E675F636C7562', 'wondrous_skrbina', 'giving_mendanlioglu', -6837337053772308333, X'636861726D696E675F73616C696E6173', 'rousing_hedva', X'6469706C6F6D617469635F7061796E');",
        ];

        for query in queries {
            let mut stmt = conn.query(query).unwrap().unwrap();
            loop {
                let row = stmt.step().expect("step");
                match row {
                    StepResult::Done => {
                        break;
                    }
                    _ => {
                        tracing::debug!("row {:?}", row);
                    }
                }
            }
        }
    }

    // this test will create a tree like this:
    // -page:2, ptr(right):3
    // +cells:
    //   -page:3, ptr(right):0
    //   +cells:
    #[test]
    pub fn test_drop_page_in_balancing_issue_1203_2() {
        let (db, _temp_dir) = get_database();
        let conn = db.connect().unwrap();

        let queries = vec![
"CREATE TABLE super_becky (engrossing_berger BLOB,plucky_chai BLOB,mirthful_asbo REAL,bountiful_jon REAL,competitive_petit REAL,engrossing_rexroth REAL);",
"INSERT INTO super_becky VALUES (X'636861726D696E675F6261796572', X'70726F647563746976655F70617269737369', 6847793643.408741, 7330361375.924953, -6586051582.891455, -6921021872.711397), (X'657863656C6C656E745F6F7267616E697A696E67', X'6C757374726F75735F73696E64696B616C6973', 9905774996.48619, 570325205.2246342, 5852346465.53047, 728566012.1968269), (X'7570626561745F73656174746C65', X'62726176655F6661756E', -2202725836.424899, 5424554426.388281, 2625872085.917082, -6657362503.808359), (X'676C6F77696E675F6D617877656C6C', X'7761726D686561727465645F726F77616E', -9610936969.793116, 4886606277.093559, -3414536174.7928505, 6898267795.317778), (X'64796E616D69635F616D616E', X'7374656C6C61725F7374657073', 3918935692.153696, 151068445.947237, 4582065669.356403, -3312668220.4789667), (X'64696C6967656E745F64757272757469', X'7175616C69666965645F6D726163686E696B', 5527271629.262201, 6068855126.044355, 289904657.13490677, 2975774820.0877323), (X'6469706C6F6D617469635F726F76657363696F', X'616C6C7572696E675F626F7474696369', 9844748192.66119, -6180276383.305578, -4137330511.025565, -478754566.79494476), (X'776F6E64726F75735F6173686572', X'6465766F7465645F6176657273696F6E', 2310211470.114773, -6129166761.628184, -2865371645.3145514, 7542428654.8645935), (X'617070726F61636861626C655F6B686F6C61', X'6C757374726F75735F6C696E6E656C6C', -4993113161.458349, 7356727284.362968, -3228937035.568404, -1779334005.5067253);",
"INSERT INTO super_becky VALUES (X'74686F7567687466756C5F726576696577', X'617765736F6D655F63726F73736579', 9401977997.012783, 8428201961.643898, 2822821303.052643, 4555601220.718847), (X'73706563746163756C61725F6B686179617469', X'616772656561626C655F61646F6E696465', 7414547022.041355, 365016845.73330307, 50682963.055828094, -9258802584.962656), (X'6C6F79616C5F656D6572736F6E', X'676C6F77696E675F626174616C6F', -5522070106.765736, 2712536599.6384163, 6631385631.869345, 1242757880.7583427), (X'68617264776F726B696E675F6F6B656C6C79', X'666162756C6F75735F66696C697373', 6682622809.9778805, 4233900041.917185, 9017477903.795563, -756846353.6034946), (X'68617264776F726B696E675F626C61756D616368656E', X'616666656374696F6E6174655F6B6F736D616E', -1146438175.3174362, -7545123696.438596, -6799494012.403366, 5646913977.971333), (X'66616E7461737469635F726F77616E', X'74686F7567687466756C5F7465727269746F72696573', -4414529784.916277, -6209371635.279242, 4491104121.288605, 2590223842.117277);",
"INSERT INTO super_becky VALUES (X'676C697374656E696E675F706F72746572', X'696E7175697369746976655F656D', 2986144164.3676434, 3495899172.5935287, -849280584.9386635, 6869709150.2699375), (X'696D6167696E61746976655F6D65726C696E6F', X'676C6F77696E675F616B74696D6F6E', 8733490615.829357, 6782649864.719433, 6926744218.74107, 1532081022.4379768), (X'6E6963655F726F73736574', X'626C69746865736F6D655F66696C697373', -839304300.0706863, 6155504968.705227, -2951592321.950267, -6254186334.572437), (X'636F6E666964656E745F6C69626574', X'676C696D6D6572696E675F6B6F74616E7969', -5344675223.37533, -8703794729.211002, 3987472096.020382, -7678989974.961197), (X'696D6167696E61746976655F6B61726162756C7574', X'64796E616D69635F6D6367697272', 2028227065.6995697, -7435689525.030833, 7011220815.569796, 5526665697.213846), (X'696E7175697369746976655F636C61726B', X'616666656374696F6E6174655F636C6561766572', 3016598350.546356, -3686782925.383732, 9671422351.958004, 9099319829.078941), (X'63617061626C655F746174616E6B61', X'696E6372656469626C655F6F746F6E6F6D61', 6339989259.432795, -8888997534.102034, 6855868409.475763, -2565348887.290493), (X'676F7267656F75735F6265726E657269', X'65647563617465645F6F6D6F77616C69', 6992467657.527826, -3538089391.748543, -7103111660.146708, 4019283237.3740463), (X'616772656561626C655F63756C74757265', X'73706563746163756C61725F657370616E61', 189387871.06959534, 6211851191.361202, 1786455196.9768047, 7966404387.318119);",
"INSERT INTO super_becky VALUES (X'7068696C6F736F70686963616C5F6C656967686C616973', X'666162756C6F75735F73656D696E61746F7265', 8688321500.141502, -7855144036.024546, -5234949709.573349, -9937638367.366447), (X'617070726F61636861626C655F726F677565', X'676C65616D696E675F6D7574696E79', -5351540099.744092, -3614025150.9013805, -2327775310.276925, 2223379997.077526), (X'676C696D6D6572696E675F63617263686961', X'696D6167696E61746976655F61737379616E6E', 4104832554.8371887, -5531434716.627781, 1652773397.4099865, 3884980522.1830273);",
"DELETE FROM super_becky WHERE (plucky_chai != X'7761726D686561727465645F6877616E67' AND mirthful_asbo != 9537234687.183533 AND bountiful_jon = -3538089391.748543);",
"INSERT INTO super_becky VALUES (X'706C75636B795F6D617263616E74656C', X'696D6167696E61746976655F73696D73', 9535651632.375484, 92270815.0720501, 1299048084.6248207, 6460855331.572151), (X'726F6D616E7469635F706F746C61746368', X'68756D6F726F75735F63686165686F', 9345375719.265533, 7825332230.247925, -7133157299.39028, -6939677879.6597), (X'656666696369656E745F6261676E696E69', X'63726561746976655F67726168616D', -2615470560.1954746, 6790849074.977201, -8081732985.448849, -8133707792.312794), (X'677265676172696F75735F73637564', X'7368696E696E675F67726F7570', -7996394978.2610035, -9734939565.228964, 1108439333.8481388, -5420483517.169478), (X'6C696B61626C655F6B616E6176616C6368796B', X'636F75726167656F75735F7761726669656C64', -1959869609.656724, 4176668769.239971, -8423220404.063669, 9987687878.685959), (X'657863656C6C656E745F68696C6473646F74746572', X'676C6974746572696E675F7472616D7564616E61', -5220160777.908238, 3892402687.8826714, 9803857762.617172, -1065043714.0265541), (X'6D61676E69666963656E745F717565657273', X'73757065725F717565657273', -700932053.2006226, -4706306995.253335, -5286045811.046467, 1954345265.5250092), (X'676976696E675F6275636B65726D616E6E', X'667269656E646C795F70697A7A6F6C61746F', -2186859620.9089565, -6098492099.446075, -7456845586.405931, 8796967674.444252);",
"DELETE FROM super_becky WHERE TRUE;",
"INSERT INTO super_becky VALUES (X'6F7074696D69737469635F6368616E69616C', X'656E657267657469635F6E65677261', 1683345860.4208698, 4163199322.9289455, -4192968616.7868404, -7253371206.571701), (X'616C6C7572696E675F686176656C', X'7477696E6B6C696E675F626965627579636B', -9947019174.287437, 5975899640.893995, 3844707723.8570194, -9699970750.513876), (X'6F7074696D69737469635F7A686F75', X'616D626974696F75735F636F6E6772657373', 4143738484.1081524, -2138255286.170598, 9960750454.03466, 5840575852.80299), (X'73706563746163756C61725F6A6F6E67', X'73656E7369626C655F616269646F72', -1767611042.9716015, -7684260477.580351, 4570634429.188147, -9222640121.140202), (X'706F6C6974655F6B657272', X'696E736967687466756C5F63686F646F726B6F6666', -635016769.5123329, -4359901288.494518, -7531565119.905825, -1180410948.6572971), (X'666C657869626C655F636F6D756E69656C6C6F', X'6E6963655F6172636F73', 8708423014.802425, -6276712625.559328, -771680766.2485523, 8639486874.113342);",
"DELETE FROM super_becky WHERE (mirthful_asbo < 9730384310.536528 AND plucky_chai < X'6E6963655F61726370B2');",
"DELETE FROM super_becky WHERE (mirthful_asbo > 6248699554.426553 AND bountiful_jon > 4124481472.333034);",
"INSERT INTO super_becky VALUES (X'676C696D6D6572696E675F77656C7368', X'64696C6967656E745F636F7262696E', 8217054003.369003, 8745594518.77864, 1928172803.2261295, -8375115534.050233), (X'616772656561626C655F6463', X'6C6F76696E675F666F72656D616E', -5483889804.871533, -8264576639.127487, 4770567289.404846, -3409172927.2573576), (X'6D617276656C6F75735F6173696D616B6F706F756C6F73', X'746563686E6F6C6F676963616C5F6A61637175696572', 2694858779.206814, -1703227425.3442516, -4504989231.263319, -3097265869.5230227), (X'73747570656E646F75735F64757075697364657269', X'68696C6172696F75735F6D75697268656164', 568174708.66469, -4878260547.265669, -9579691520.956625, 73507727.8100338), (X'626C69746865736F6D655F626C6F6B', X'61646570745F6C65696572', 7772117077.916897, 4590608571.321514, -881713470.657032, -9158405774.647465);",
"INSERT INTO super_becky VALUES (X'6772697070696E675F6573736578', X'67656E65726F75735F636875726368696C6C', -4180431825.598956, 7277443000.677654, 2499796052.7878246, -2858339306.235305), (X'756E697175655F6D6172656368616C', X'62726561746874616B696E675F636875726368696C6C', 1401354536.7625294, -611427440.2796707, -4621650430.463729, 1531473111.7482872), (X'657863656C6C656E745F66696E6C6579', X'666169746866756C5F62726F636B', -4020697828.0073624, -2833530733.19637, -7766170050.654022, 8661820959.434689);",
"INSERT INTO super_becky VALUES (X'756E697175655F6C617061797265', X'6C6F76696E675F7374617465', 7063237787.258968, -5425712581.365798, -7750509440.0141945, -7570954710.892544), (X'62726561746874616B696E675F6E65616C', X'636F75726167656F75735F61727269676F6E69', 289862394.2028198, 9690362375.014446, -4712463267.033899, 2474917855.0973473), (X'7477696E6B6C696E675F7368616B7572', X'636F75726167656F75735F636F6D6D6974746565', 5449035403.229155, -2159678989.597906, 3625606019.1150894, -3752010405.4475393);",
"INSERT INTO super_becky VALUES (X'70617373696F6E6174655F73686970776179', X'686F6E6573745F7363687765697A6572', 4193384746.165228, -2232151704.896323, 8615245520.962444, -9789090953.995636);",
"INSERT INTO super_becky VALUES (X'6C696B61626C655F69', X'6661766F7261626C655F6D626168', 6581403690.769894, 3260059398.9544716, -407118859.046051, -3155853965.2700634), (X'73696E636572655F6F72', X'616772656561626C655F617070656C6261756D', 9402938544.308651, -7595112171.758331, -7005316716.211025, -8368210960.419411);",
"INSERT INTO super_becky VALUES (X'6D617276656C6F75735F6B61736864616E', X'6E6963655F636F7272', -5976459640.85817, -3177550476.2092276, 2073318650.736992, -1363247319.9978447);",
"INSERT INTO super_becky VALUES (X'73706C656E6469645F6C616D656E646F6C61', X'677265676172696F75735F766F6E6E65677574', 6898259773.050102, 8973519699.707073, -25070632.280548096, -1845922497.9676847), (X'617765736F6D655F7365766572', X'656E657267657469635F706F746C61746368', -8750678407.717808, 5130907533.668898, -6778425327.111566, 3718982135.202587);",
"INSERT INTO super_becky VALUES (X'70726F706974696F75735F6D616C617465737461', X'657863656C6C656E745F65766572657474', -8846855772.62094, -6168969732.697067, -8796372709.125793, 9983557891.544613), (X'73696E636572655F6C6177', X'696E7175697369746976655F73616E647374726F6D', -6366985697.975358, 3838628702.6652164, 3680621713.3371124, -786796486.8049564), (X'706F6C6974655F676C6561736F6E', X'706C75636B795F677579616E61', -3987946379.104308, -2119148244.413993, -1448660343.6888638, -1264195510.1611118), (X'676C6974746572696E675F6C6975', X'70657273697374656E745F6F6C6976696572', 6741779968.943846, -3239809989.227495, -1026074003.5506897, 4654600514.871752);",
"DELETE FROM super_becky WHERE (engrossing_berger < X'6566651A3C70278D4E200657551D8071A1' AND competitive_petit > 1236742147.9451914);",
"INSERT INTO super_becky VALUES (X'6661766F7261626C655F726569746D616E', X'64657465726D696E65645F726974746572', -7412553243.829927, -7572665195.290464, 7879603411.222157, 3706943306.5691853), (X'70657273697374656E745F6E6F6C616E', X'676C6974746572696E675F73686570617264', 7028261282.277422, -2064164782.3494844, -5244048504.507779, -2399526243.005843), (X'6B6E6F776C6564676561626C655F70617474656E', X'70726F66696369656E745F726F7365627261756768', 3713056763.583538, 3919834206.566164, -6306779387.430006, -9939464323.995546), (X'616461707461626C655F7172757A', X'696E7175697369746976655F68617261776179', 6519349690.299835, -9977624623.820414, 7500579325.440605, -8118341251.362242);",
"INSERT INTO super_becky VALUES (X'636F6E73696465726174655F756E696F6E', X'6E6963655F6573736578', -1497385534.8720198, 9957688503.242973, 9191804202.566128, -179015615.7117195), (X'666169746866756C5F626F776C656773', X'6361707469766174696E675F6D6367697272', 893707300.1576138, 3381656294.246702, 6884723724.381908, 6248331214.701559), (X'6B6E6F776C6564676561626C655F70656E6E61', X'6B696E645F616A697468', -3335162603.6574974, 1812878172.8505402, 5115606679.658335, -5690100280.808182), (X'617765736F6D655F77696E7374616E6C6579', X'70726F706974696F75735F6361726173736F', -7395576292.503981, 4956546102.029215, -1468521769.7486448, -2968223925.60355), (X'636F75726167656F75735F77617266617265', X'74686F7567687466756C5F7361707068697265', 7052982930.566017, -9806098174.104418, -6910398936.377775, -4041963031.766964), (X'657863656C6C656E745F6B62', X'626C69746865736F6D655F666F75747A6F706F756C6F73', 6142173202.994768, 5193126957.544125, -7522202722.983735, -1659088056.594862), (X'7374756E6E696E675F6E6576616461', X'626F756E746966756C5F627572746F6E', -3822097036.7628613, -3458840259.240303, 2544472236.86788, 6928890176.466003);",
"INSERT INTO super_becky VALUES (X'706572736F6E61626C655F646D69747269', X'776F6E64726F75735F6133796F', 2651932559.0077076, 811299402.3174248, -8271909238.671928, 6761098864.189909);",
"INSERT INTO super_becky VALUES (X'726F7573696E675F6B6C6166657461', X'64617A7A6C696E675F6B6E617070', 9370628891.439335, -5923332007.253168, -2763161830.5880013, -9156194881.875952), (X'656666696369656E745F6C6576656C6C6572', X'616C6C7572696E675F706561636F7474', 3102641409.8314342, 2838360181.628153, 2466271662.169607, 1015942181.844162), (X'6469706C6F6D617469635F7065726B696E73', X'726F7573696E675F6172616269', -1551071129.022499, -8079487600.186886, 7832984580.070087, -6785993247.895652), (X'626F756E746966756C5F6D656D62657273', X'706F77657266756C5F70617269737369', 9226031830.72445, 7012021503.536997, -2297349030.108919, -2738320055.4710903), (X'676F7267656F75735F616E6172636F7469636F', X'68656C7066756C5F7765696C616E64', -8394163480.676959, -2978605095.699134, -6439355448.021704, 9137308022.281273), (X'616666656374696F6E6174655F70726F6C65696E666F', X'706C75636B795F73616E7A', 3546758708.3524914, -1870964264.9353771, 338752565.3643894, -3908023657.299715), (X'66756E6E795F706F70756C61697265', X'6F75747374616E64696E675F626576696E67746F6E', -1533858145.408224, 6164225076.710373, 8419445987.622173, 584555253.6852646), (X'76697669645F6D7474', X'7368696D6D6572696E675F70616F6E65737361', 5512251366.193035, -8680583180.123213, -4445968638.153208, -3274009935.4229546);",
"INSERT INTO super_becky VALUES (X'7068696C6F736F70686963616C5F686F7264', X'657863656C6C656E745F67757373656C7370726F757473', -816909447.0240917, -3614686681.8786583, 7701617524.26067, -4541962047.183721), (X'616D6961626C655F69676E6174696576', X'6D61676E69666963656E745F70726F76696E6369616C69', -1318532883.847702, -4918966075.976474, -7601723171.33518, -3515747704.3847466), (X'70726F66696369656E745F32303137', X'66756E6E795F6E77', -1264540201.518032, 8227396547.578808, 6245093925.183641, -8368355328.110817);",
"INSERT INTO super_becky VALUES (X'77696C6C696E675F6E6F6B6B65', X'726F6D616E7469635F677579616E61', 6618610796.3707695, -3814565359.1524105, 1663106272.4565296, -4175107840.768817), (X'72656C617865645F7061766C6F76', X'64657465726D696E65645F63686F646F726B6F6666', -3350029338.034504, -3520837855.4619064, 3375167499.631817, -8866806483.714607), (X'616D706C655F67696464696E6773', X'667269656E646C795F6A6F686E', 1458864959.9942684, 1344208968.0486107, 9335156635.91314, -6180643697.918882), (X'72656C617865645F6C65726F79', X'636F75726167656F75735F6E6F72646772656E', -5164986537.499656, 8820065797.720875, 6146530425.891005, 6949241471.958189), (X'666F63757365645F656D6D61', X'696D6167696E61746976655F6C6F6E67', -9587619060.80035, 6128068142.184402, 6765196076.956905, 800226302.7983418);",
"INSERT INTO super_becky VALUES (X'616D626974696F75735F736F6E67', X'706572666563745F6761686D616E', 4989979180.706432, -9374266591.537058, 314459621.2820797, -3200029490.9553604), (X'666561726C6573735F626C6174', X'676C697374656E696E675F616374696F6E', -8512203612.903147, -7625581186.013805, -9711122307.234787, -301590929.32751083), (X'617765736F6D655F6669646573', X'666169746866756C5F63756E6E696E6768616D', -1428228887.9205084, 7669883854.400173, 5604446195.905277, -1509311057.9653416), (X'68756D6F726F75735F77697468647261776E', X'62726561746874616B696E675F7472617562656C', -7292778713.676636, -6728132503.529593, 2805341768.7252483, 330416975.2300949);",
"INSERT INTO super_becky VALUES (X'677265676172696F75735F696873616E', X'7374656C6C61725F686172746D616E', 8819210651.1988, 5298459883.813452, 7293544377.958424, 460475869.72971725), (X'696E736967687466756C5F62657765726E69747A', X'676C65616D696E675F64656E736C6F77', -6911957282.193239, 1754196756.2193146, -6316860403.693853, -3094020672.236368), (X'6D6972746866756C5F616D6265727261656B656C6C79', X'68756D6F726F75735F6772617665', 1785574023.0269203, -372056983.82761574, 4133719439.9538956, 9374053482.066044), (X'76697669645F736169747461', X'7761726D686561727465645F696E656469746173', 2787071361.6099434, 9663839418.553448, -5934098589.901047, -9774745509.608858), (X'61646570745F6F6375727279', X'6C696B61626C655F726569746D616E', -3098540915.1310825, 5460848322.672174, -6012867197.519758, 6769770087.661135), (X'696E646570656E64656E745F6F', X'656C6567616E745F726F6F726461', 1462542860.3143978, 3360904654.2464733, 5458876201.665213, -5522844849.529962), (X'72656D61726B61626C655F626F6B616E69', X'6F70656E5F6D696E6465645F686F72726F78', 7589481760.867031, 7970075121.546291, 7513467575.5213585, 9663061478.289227), (X'636F6E666964656E745F6C616479', X'70617373696F6E6174655F736B726F7A6974736B79', 8266917234.53915, -7172933478.625412, 309854059.94031143, -8309837814.497616);",
"DELETE FROM super_becky WHERE (competitive_petit != 8725256604.165474 OR engrossing_rexroth > -3607424615.7839313 OR plucky_chai < X'726F7573696E675F6216E20375');",
"INSERT INTO super_becky VALUES (X'7368696E696E675F736F6C69646169726573', X'666561726C6573735F63617264616E', -170727879.20838165, 2744601113.384678, 5676912434.941502, 6757573601.657997), (X'636F75726167656F75735F706C616E636865', X'696E646570656E64656E745F636172736F6E', -6271723086.761938, -180566679.7470188, -1285774632.134449, 1359665735.7842407), (X'677265676172696F75735F7374616D61746F76', X'7374756E6E696E675F77696C64726F6F7473', -6210238866.953484, 2492683045.8287067, -9688894361.68205, 5420275482.048567), (X'696E646570656E64656E745F6F7267616E697A6572', X'676C6974746572696E675F736F72656C', 9291163783.3073, -6843003475.769236, -1320245894.772686, -5023483808.044955), (X'676C6F77696E675F6E65736963', X'676C65616D696E675F746F726D6579', 829526382.8027191, 9365690945.1316, 4761505764.826195, -4149154965.0024815), (X'616C6C7572696E675F646F637472696E65', X'6E6963655F636C6561766572', 3896644979.981762, -288600448.8016701, 9462856570.130062, -909633752.5993862);",
        ];

        for query in queries {
            let mut stmt = conn.query(query).unwrap().unwrap();
            loop {
                let row = stmt.step().expect("step");
                match row {
                    StepResult::Done => {
                        break;
                    }
                    _ => {
                        tracing::debug!("row {:?}", row);
                    }
                }
            }
        }
    }

    #[test]
    pub fn test_free_space() {
        let (db, _temp_dir) = get_database();
        let conn = db.connect().unwrap();
        let page = get_page(2);

        let page_contents = page.get_contents();
        let header_size = 8;
        let usable_space = 4096;

        let regs = &[Register::Value(Value::from_i64(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let payload = add_record(0, 0, page.clone(), record, &conn);
        let free = compute_free_space(page_contents, usable_space).unwrap();
        assert_eq!(free, 4096 - payload.len() - 2 - header_size);
    }

    /// A cell smaller than MINIMUM_CELL_SIZE still takes MINIMUM_CELL_SIZE
    /// bytes on the page. If the free-space check in insert_into_cell()
    /// forgets that, a 3-byte cell inserted into a page with exactly 5 free
    /// bytes passes the check (3 + 2 == 5) but the allocation takes 6, and
    /// the cell content area slides one byte into the cell pointer array.
    /// The cell must go to the overflow path instead.
    #[test]
    pub fn test_tiny_cell_insert_must_not_overlap_cell_pointer_array() {
        let page = get_page(2);
        btree_init_page(&page, PageType::IndexLeaf, 0, 4096);
        let contents = page.get_contents();
        let usable_space = 4096;

        // Fill the page so exactly 5 free bytes remain: the empty index leaf
        // has 4096 - 8 = 4088 free bytes, and 679 four-byte cells plus one
        // seven-byte cell consume 679 * (4 + 2) + (7 + 2) = 4083 of them.
        let four_byte_cell = [0x03, b'a', b'b', b'c'];
        for i in 0..679 {
            insert_into_cell(contents, &four_byte_cell, i, usable_space).unwrap();
        }
        let seven_byte_cell = [0x06, b'a', b'b', b'c', b'd', b'e', b'f'];
        insert_into_cell(contents, &seven_byte_cell, 679, usable_space).unwrap();
        assert_eq!(compute_free_space(contents, usable_space).unwrap(), 5);

        let three_byte_cell = [0x02, b'x', b'y'];
        insert_into_cell(contents, &three_byte_cell, 680, usable_space).unwrap();

        assert_eq!(
            contents.overflow_cells.len(),
            1,
            "a 3-byte cell needs MINIMUM_CELL_SIZE + 2 = 6 bytes, so it must overflow when only 5 are free"
        );
        let pointer_array_end =
            contents.offset() + contents.header_size() + 2 * contents.cell_count();
        assert!(
            contents.cell_content_area() as usize >= pointer_array_end,
            "cell content area ({}) overlaps the cell pointer array (ends at {})",
            contents.cell_content_area(),
            pointer_array_end
        );
    }

    #[test]
    pub fn test_defragment_1() {
        let (db, _temp_dir) = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);

        let page_contents = page.get_contents();
        let usable_space = 4096;

        let regs = &[Register::Value(Value::from_i64(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let payload = add_record(0, 0, page.clone(), record, &conn);

        assert_eq!(page_contents.cell_count(), 1);
        defragment_page(page_contents, usable_space, 4).unwrap();
        assert_eq!(page_contents.cell_count(), 1);
        let (start, len) = page_contents.cell_get_raw_region(0, usable_space).unwrap();
        let buf = page_contents.as_ptr();
        assert_eq!(&payload, &buf[start..start + len]);
    }

    #[test]
    pub fn test_insert_drop_insert() {
        let (db, _temp_dir) = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);

        let page_contents = page.get_contents();
        let usable_space = 4096;

        let regs = &[
            Register::Value(Value::from_i64(0)),
            Register::Value(Value::Text(Text::new("aaaaaaaa"))),
        ];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let _ = add_record(0, 0, page.clone(), record, &conn);

        assert_eq!(page_contents.cell_count(), 1);
        drop_cell(page_contents, 0, usable_space).unwrap();
        assert_eq!(page_contents.cell_count(), 0);

        let regs = &[Register::Value(Value::from_i64(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let payload = add_record(0, 0, page.clone(), record, &conn);
        assert_eq!(page_contents.cell_count(), 1);

        let (start, len) = page_contents.cell_get_raw_region(0, usable_space).unwrap();
        let buf = page_contents.as_ptr();
        assert_eq!(&payload, &buf[start..start + len]);
    }

    #[test]
    pub fn test_insert_drop_insert_multiple() {
        let (db, _temp_dir) = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);

        let page_contents = page.get_contents();
        let usable_space = 4096;

        let regs = &[
            Register::Value(Value::from_i64(0)),
            Register::Value(Value::Text(Text::new("aaaaaaaa"))),
        ];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let _ = add_record(0, 0, page.clone(), record, &conn);

        for _ in 0..100 {
            assert_eq!(page_contents.cell_count(), 1);
            drop_cell(page_contents, 0, usable_space).unwrap();
            assert_eq!(page_contents.cell_count(), 0);

            let regs = &[Register::Value(Value::from_i64(0))];
            let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
            let payload = add_record(0, 0, page.clone(), record, &conn);
            assert_eq!(page_contents.cell_count(), 1);

            let (start, len) = page_contents.cell_get_raw_region(0, usable_space).unwrap();
            let buf = page_contents.as_ptr();
            assert_eq!(&payload, &buf[start..start + len]);
        }
    }

    #[test]
    pub fn test_drop_a_few_insert() {
        let (db, _temp_dir) = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);

        let page_contents = page.get_contents();
        let usable_space = 4096;

        let regs = &[Register::Value(Value::from_i64(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let payload = add_record(0, 0, page.clone(), record, &conn);
        let regs = &[Register::Value(Value::from_i64(1))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let _ = add_record(1, 1, page.clone(), record, &conn);
        let regs = &[Register::Value(Value::from_i64(2))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let _ = add_record(2, 2, page.clone(), record, &conn);

        drop_cell(page_contents, 1, usable_space).unwrap();
        drop_cell(page_contents, 1, usable_space).unwrap();

        ensure_cell(page_contents, 0, &payload);
    }

    #[test]
    pub fn test_fuzz_victim_1() {
        let (db, _temp_dir) = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);

        let page_contents = page.get_contents();
        let usable_space = 4096;

        let regs = &[Register::Value(Value::from_i64(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let _ = add_record(0, 0, page.clone(), record, &conn);

        let regs = &[Register::Value(Value::from_i64(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let _ = add_record(0, 0, page.clone(), record, &conn);
        drop_cell(page_contents, 0, usable_space).unwrap();

        defragment_page(page_contents, usable_space, 4).unwrap();

        let regs = &[Register::Value(Value::from_i64(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let _ = add_record(0, 1, page.clone(), record, &conn);

        drop_cell(page_contents, 0, usable_space).unwrap();

        let regs = &[Register::Value(Value::from_i64(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let _ = add_record(0, 1, page.clone(), record, &conn);
    }

    #[test]
    pub fn test_fuzz_victim_2() {
        let (db, _temp_dir) = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let usable_space = 4096;
        let insert = |pos, page| {
            let regs = &[Register::Value(Value::from_i64(0))];
            let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
            let _ = add_record(0, pos, page, record, &conn);
        };
        let drop = |pos, page| {
            drop_cell(page, pos, usable_space).unwrap();
        };
        let defragment = |page| {
            defragment_page(page, usable_space, 4).unwrap();
        };

        defragment(page.get_contents());
        defragment(page.get_contents());
        insert(0, page.clone());
        drop(0, page.get_contents());
        insert(0, page.clone());
        drop(0, page.get_contents());
        insert(0, page.clone());
        defragment(page.get_contents());
        defragment(page.get_contents());
        drop(0, page.get_contents());
        defragment(page.get_contents());
        insert(0, page.clone());
        drop(0, page.get_contents());
        insert(0, page.clone());
        insert(1, page.clone());
        insert(1, page.clone());
        insert(0, page.clone());
        drop(3, page.get_contents());
        drop(2, page.get_contents());
        compute_free_space(page.get_contents(), usable_space).unwrap();
    }

    #[test]
    pub fn test_fuzz_victim_3() {
        let (db, _temp_dir) = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let usable_space = 4096;
        let insert = |pos, page| {
            let regs = &[Register::Value(Value::from_i64(0))];
            let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
            let _ = add_record(0, pos, page, record, &conn);
        };
        let drop = |pos, page| {
            drop_cell(page, pos, usable_space).unwrap();
        };
        let defragment = |page| {
            defragment_page(page, usable_space, 4).unwrap();
        };
        let regs = &[Register::Value(Value::from_i64(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let mut payload: crate::alloc::Vec<u8> = crate::alloc::vec![];
        let mut fill_cell_payload_state = FillCellPayloadState::Start;
        run_until_done(
            || {
                fill_cell_payload(
                    &PinGuard::new(page.clone()),
                    Some(0),
                    &mut payload,
                    0,
                    &record,
                    4096,
                    conn.pager.load().clone(),
                    &mut fill_cell_payload_state,
                )
            },
            &conn.pager.load().clone(),
        )
        .unwrap();

        insert(0, page.clone());
        defragment(page.get_contents());
        insert(0, page.clone());
        defragment(page.get_contents());
        insert(0, page.clone());
        drop(2, page.get_contents());
        drop(0, page.get_contents());
        let free = compute_free_space(page.get_contents(), usable_space).unwrap();
        let total_size = payload.len() + 2;
        assert_eq!(
            free,
            usable_space - page.get_contents().header_size() - total_size
        );
        dbg!(free);
    }

    #[test]
    pub fn btree_insert_sequential() {
        let (pager, root_page, _, _) = empty_btree();
        let mut keys = Vec::new();
        let num_columns = 5;

        for i in 0..10000 {
            let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
            tracing::info!("INSERT INTO t VALUES ({});", i,);
            let regs = &[Register::Value(Value::from_i64(i))];
            let value = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
            tracing::trace!("before insert {}", i);
            run_until_done(
                || {
                    let key = SeekKey::TableRowId(i);
                    cursor.seek(key, SeekOp::GE { eq_only: true })
                },
                pager.deref(),
            )
            .unwrap();
            run_until_done(
                || cursor.insert(&BTreeKey::new_table_rowid(i, Some(&value))),
                pager.deref(),
            )
            .unwrap();
            keys.push(i);
        }
        if matches!(validate_btree(pager.clone(), root_page), (_, false)) {
            panic!("invalid btree");
        }
        tracing::trace!(
            "=========== btree ===========\n{}\n\n",
            format_btree(pager.clone(), root_page, 0)
        );
        for key in keys.iter() {
            let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
            let key = Value::from_i64(*key);
            let exists = run_until_done(|| cursor.exists(&key), pager.deref()).unwrap();
            assert!(exists, "key not found {key}");
        }
    }

    #[test]
    pub fn test_big_payload_compute_free() {
        let (db, _temp_dir) = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let usable_space = 4096;
        let regs = &[Register::Value(Value::Blob(crate::alloc::vec![0; 3600]))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let mut payload: crate::alloc::Vec<u8> = crate::alloc::vec![];
        let mut fill_cell_payload_state = FillCellPayloadState::Start;
        run_until_done(
            || {
                fill_cell_payload(
                    &PinGuard::new(page.clone()),
                    Some(0),
                    &mut payload,
                    0,
                    &record,
                    4096,
                    conn.pager.load().clone(),
                    &mut fill_cell_payload_state,
                )
            },
            &conn.pager.load().clone(),
        )
        .unwrap();
        insert_into_cell(page.get_contents(), &payload, 0, 4096).unwrap();
        let free = compute_free_space(page.get_contents(), usable_space).unwrap();
        let total_size = payload.len() + 2;
        assert_eq!(
            free,
            usable_space - page.get_contents().header_size() - total_size
        );
        dbg!(free);
    }

    #[test]
    pub fn test_delete_balancing() {
        // What does this test do:
        // 1. Insert 10,000 rows of ~15 byte payload each. This creates
        //    nearly 40 pages (10,000 * 15 / 4096) and 240 rows per page.
        // 2. Delete enough rows to create empty/ nearly empty pages to trigger balancing
        //    (verified this in SQLite).
        // 3. Verify validity/integrity of btree after deleting and also verify that these
        //    values are actually deleted.

        let (pager, root_page, _, _) = empty_btree();
        let num_columns = 5;

        // Insert 10,000 records in to the BTree.
        for i in 1..=10000 {
            let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
            let regs = &[Register::Value(Value::Text(Text::new("hello world")))];
            let value = ImmutableRecord::from_registers(regs, regs.len()).unwrap();

            run_until_done(
                || {
                    let key = SeekKey::TableRowId(i);
                    cursor.seek(key, SeekOp::GE { eq_only: true })
                },
                pager.deref(),
            )
            .unwrap();

            run_until_done(
                || cursor.insert(&BTreeKey::new_table_rowid(i, Some(&value))),
                pager.deref(),
            )
            .unwrap();
        }

        if let (_, false) = validate_btree(pager.clone(), root_page) {
            panic!("Invalid B-tree after insertion");
        }
        let num_columns = 5;

        // Delete records with 500 <= key <= 3500
        for i in 500..=3500 {
            let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
            let seek_key = SeekKey::TableRowId(i);

            let seek_result = run_until_done(
                || cursor.seek(seek_key.clone(), SeekOp::GE { eq_only: true }),
                pager.deref(),
            )
            .unwrap();

            if matches!(seek_result, SeekResult::Found) {
                run_until_done(|| cursor.delete(), pager.deref()).unwrap();
            }
        }

        // Verify that records with key < 500 and key > 3500 still exist in the BTree.
        for i in 1..=10000 {
            if (500..=3500).contains(&i) {
                continue;
            }

            let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
            let key = Value::from_i64(i);
            let exists = run_until_done(|| cursor.exists(&key), pager.deref()).unwrap();
            assert!(exists, "Key {i} should exist but doesn't");
        }

        // Verify the deleted records don't exist.
        for i in 500..=3500 {
            let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
            let key = Value::from_i64(i);
            let exists = run_until_done(|| cursor.exists(&key), pager.deref()).unwrap();
            assert!(!exists, "Deleted key {i} still exists");
        }
    }

    #[test]
    pub fn test_overflow_cells() {
        let iterations = 10_usize;
        let mut huge_texts = Vec::new();
        for i in 0..iterations {
            let mut huge_text = String::new();
            for _j in 0..8192 {
                huge_text.push((b'A' + i as u8) as char);
            }
            huge_texts.push(huge_text);
        }

        let (pager, root_page, _, _) = empty_btree();
        let num_columns = 5;

        for (i, huge_text) in huge_texts.iter().enumerate().take(iterations) {
            let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
            tracing::info!("INSERT INTO t VALUES ({});", i,);
            let regs = &[Register::Value(Value::Text(Text::new(huge_text.clone())))];
            let value = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
            tracing::trace!("before insert {}", i);
            tracing::debug!(
                "=========== btree before ===========\n{}\n\n",
                format_btree(pager.clone(), root_page, 0)
            );
            run_until_done(
                || {
                    let key = SeekKey::TableRowId(i as i64);
                    cursor.seek(key, SeekOp::GE { eq_only: true })
                },
                pager.deref(),
            )
            .unwrap();
            run_until_done(
                || cursor.insert(&BTreeKey::new_table_rowid(i as i64, Some(&value))),
                pager.deref(),
            )
            .unwrap();
            tracing::debug!(
                "=========== btree after ===========\n{}\n\n",
                format_btree(pager.clone(), root_page, 0)
            );
        }
        let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, num_columns);
        let _c = cursor.move_to_root().unwrap();
        for i in 0..iterations {
            run_until_done(|| cursor.next(), pager.deref()).unwrap();
            let has_next = cursor.has_record();
            if !has_next {
                panic!("expected Some(rowid) but got {:?}", cursor.has_record());
            };
            let rowid = run_until_done(|| cursor.rowid(), pager.deref())
                .unwrap()
                .unwrap();
            assert_eq!(rowid, i as i64, "got!=expected");
        }
    }

    fn run_until_done<T>(action: impl FnMut() -> IOResultOr<T>, pager: &Pager) -> Result<T> {
        pager.io.block(action)
    }

    #[test]
    fn test_free_array() {
        let (mut rng, seed) = rng_from_time_or_env();
        tracing::info!("seed={}", seed);

        const ITERATIONS: usize = 10000;
        for _ in 0..ITERATIONS {
            let mut cell_array = CellArray {
                cell_payloads: crate::alloc::vec![],
                cell_count_per_page_cumulative: [0; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE],
            };
            let mut cells_cloned = Vec::new();
            let (pager, _, _, _) = empty_btree();
            let page_type = PageType::TableLeaf;
            let page = run_until_done(|| pager.allocate_page(), &pager).unwrap();
            btree_init_page(&page, page_type, 0, pager.usable_space());

            let mut size = (rng.next_u64() % 100) as u16;
            let mut i = 0;
            // add a bunch of cells
            while compute_free_space(page.get_contents(), pager.usable_space()).unwrap()
                >= size as usize + 10
            {
                insert_cell(i, size, page.clone(), pager.clone());
                i += 1;
                size = (rng.next_u64() % 1024) as u16;
            }

            // Create cell array with references to cells inserted
            let contents = page.get_contents();
            for cell_idx in 0..contents.cell_count() {
                let buf = contents.as_ptr();
                let (start, len) = contents
                    .cell_get_raw_region(cell_idx, pager.usable_space())
                    .unwrap();
                cell_array
                    .cell_payloads
                    .push(to_static_buf(&mut buf[start..start + len]));
                cells_cloned.push(buf[start..start + len].to_vec());
            }

            debug_validate_cells!(contents, pager.usable_space());

            // now free a prefix or suffix of cells added
            let cells_before_free = contents.cell_count();
            let size = rng.next_u64() as usize % cells_before_free;
            let prefix = rng.next_u64() % 2 == 0;
            let start = if prefix {
                0
            } else {
                contents.cell_count() - size
            };
            let removed =
                page_free_array(contents, start, size, &cell_array, pager.usable_space()).unwrap();
            // shift if needed
            if prefix {
                shift_cells_left(contents, cells_before_free, removed);
            }

            assert_eq!(removed, size);
            assert_eq!(contents.cell_count(), cells_before_free - size);
            #[cfg(debug_assertions)]
            debug_validate_cells_core(contents, pager.usable_space());
            // check cells are correct
            let mut cell_idx_cloned = if prefix { size } else { 0 };
            for cell_idx in 0..contents.cell_count() {
                let buf = contents.as_ptr();
                let (start, len) = contents
                    .cell_get_raw_region(cell_idx, pager.usable_space())
                    .unwrap();
                let cell_in_page = &buf[start..start + len];
                let cell_in_array = &cells_cloned[cell_idx_cloned];
                assert_eq!(cell_in_page, cell_in_array);
                cell_idx_cloned += 1;
            }
        }
    }

    fn insert_cell(cell_idx: u64, size: u16, page: PageRef, pager: Arc<Pager>) {
        let mut payload: crate::alloc::Vec<u8> = crate::alloc::vec![];
        let regs = &[Register::Value(Value::Blob(
            crate::alloc::vec![0; size as usize],
        ))];
        let record = ImmutableRecord::from_registers(regs, regs.len()).unwrap();
        let mut fill_cell_payload_state = FillCellPayloadState::Start;
        let contents = page.get_contents();
        run_until_done(
            || {
                fill_cell_payload(
                    &PinGuard::new(page.clone()),
                    Some(cell_idx as i64),
                    &mut payload,
                    cell_idx as usize,
                    &record,
                    pager.usable_space(),
                    pager.clone(),
                    &mut fill_cell_payload_state,
                )
            },
            &pager,
        )
        .unwrap();
        insert_into_cell(contents, &payload, cell_idx as usize, pager.usable_space()).unwrap();
    }

    /// Direct red-first coverage for the pager-level cursor registry and
    /// saveAllCursors port introduced in #7341 (register_with_pager,
    /// has_peers toggling, drive_pending_peer_save inside insert/delete,
    /// clear_btree's auto-invalidation of peers).
    ///
    /// Pairs with the SQL-surface red-first cases in
    /// sqlite/conformance/sqlite-sqltests/save-all-cursors.sqltest (window function
    /// over triple self-joined source) and the pre-existing
    /// sqlite/conformance/sqlite-sqltests/window-selfjoin-reset-sorter.sqltest.
    /// Those exercise the same machinery end-to-end; the cases here
    /// pin individual primitives (peer registration, save vs.
    /// invalidate, restore semantics) so a regression bisects faster.
    mod save_all_cursors {
        use super::*;
        use crate::storage::btree::SavePositionResult;
        use test_log::test;

        /// Boxed cursor pinned to its heap location for the duration of the
        /// test — register_cursor stores raw pointers, so the cursor must
        /// not be moved after registration.
        fn make_registered_cursor(
            pager: &Arc<Pager>,
            root_page: i64,
            num_columns: usize,
        ) -> Box<BTreeCursor> {
            let cursor = Box::new(BTreeCursor::new_table(
                pager.clone(),
                root_page,
                num_columns,
            ));
            (*cursor).register_with_pager();
            cursor
        }

        /// A peer's insert must invalidate this cursor's cached rightmost
        /// page id (move_to_rightmost's skip-a-seek optimization). Once the
        /// peer's appends split the rightmost leaf, a long-lived cursor's
        /// last() would otherwise trust the stale cache and return the old
        /// page's last cell instead of the true maximum.
        #[test]
        fn peer_write_invalidates_rightmost_page_cache() {
            let (pager, root_page, _db, _conn) = empty_btree();
            let mut writer = make_registered_cursor(&pager, root_page, 1);
            let mut reader = make_registered_cursor(&pager, root_page, 1);

            // Blob payloads so a handful of appends split the rightmost leaf.
            for rowid in 1..=8 {
                insert_record(
                    &mut writer,
                    &pager,
                    rowid,
                    Value::Blob(crate::alloc::vec![0u8; 1000]),
                )
                .unwrap();
            }

            // Positions the reader on the last row and caches the rightmost page id.
            run_until_done(|| reader.last(), pager.deref()).unwrap();
            let max1 = run_until_done(|| reader.rowid(), pager.deref()).unwrap();
            assert_eq!(max1, Some(8));

            // Peer appends: balance_quick allocates a new rightmost leaf.
            for rowid in 9..=16 {
                insert_record(
                    &mut writer,
                    &pager,
                    rowid,
                    Value::Blob(crate::alloc::vec![0u8; 1000]),
                )
                .unwrap();
            }

            run_until_done(|| reader.last(), pager.deref()).unwrap();
            let max2 = run_until_done(|| reader.rowid(), pager.deref()).unwrap();
            assert_eq!(
                max2,
                Some(16),
                "last() must see the true maximum after a peer split the rightmost leaf"
            );
        }

        /// A peer's insert must reset this cursor's memoized count():
        /// count_state stays at CountState::Finish after a full count, so
        /// without invalidation every later count() returns the stale value.
        #[test]
        fn peer_write_invalidates_count_cache() {
            let (pager, root_page, _db, _conn) = empty_btree();
            let mut writer = make_registered_cursor(&pager, root_page, 1);
            let mut reader = make_registered_cursor(&pager, root_page, 1);

            for rowid in 1..=5 {
                insert_record(&mut writer, &pager, rowid, Value::from_i64(rowid)).unwrap();
            }
            let count1 = run_until_done(|| reader.count(), pager.deref()).unwrap();
            assert_eq!(count1, 5);

            insert_record(&mut writer, &pager, 6, Value::from_i64(6)).unwrap();
            let count2 = run_until_done(|| reader.count(), pager.deref()).unwrap();
            assert_eq!(
                count2, 6,
                "count() must not return the memoized pre-write value"
            );
        }

        #[test]
        fn registry_toggles_has_peers_flag() {
            let (pager, root_page, _db, _conn) = empty_btree();
            let cursor_a = make_registered_cursor(&pager, root_page, 1);
            assert!(
                !cursor_a
                    .has_peers
                    .load(crate::sync::atomic::Ordering::Relaxed),
                "single registered cursor must have has_peers=false"
            );

            let cursor_b = make_registered_cursor(&pager, root_page, 1);
            assert!(
                cursor_a
                    .has_peers
                    .load(crate::sync::atomic::Ordering::Relaxed),
                "registering a peer must set has_peers on the original"
            );
            assert!(
                cursor_b
                    .has_peers
                    .load(crate::sync::atomic::Ordering::Relaxed),
                "the newly registered cursor must also see has_peers=true"
            );

            drop(cursor_b);
            assert!(
                !cursor_a
                    .has_peers
                    .load(crate::sync::atomic::Ordering::Relaxed),
                "dropping the peer must clear has_peers on the survivor"
            );
        }

        #[test]
        fn registry_buckets_per_root_page() {
            // Cursors on different root pages must not see each other as
            // peers; saveAllCursors is per-root (SQLite btree.c:806).
            let (pager, root_a, _db, _conn) = empty_btree();
            let page_b = run_until_done(|| pager.allocate_page(), &pager).unwrap();
            btree_init_page(&page_b, PageType::TableLeaf, 0, pager.usable_space());
            let root_b = page_b.get().id as i64;

            let cursor_a = make_registered_cursor(&pager, root_a, 1);
            let cursor_b = make_registered_cursor(&pager, root_b, 1);
            assert!(
                !cursor_a
                    .has_peers
                    .load(crate::sync::atomic::Ordering::Relaxed)
                    && !cursor_b
                        .has_peers
                        .load(crate::sync::atomic::Ordering::Relaxed),
                "cursors on different roots must not be peers"
            );
        }

        #[test]
        fn try_save_position_table_returns_saved() {
            let (pager, root_page, _db, _conn) = empty_btree();
            let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, 1);
            for rowid in 1..=5 {
                insert_record(&mut cursor, &pager, rowid, Value::from_i64(rowid)).unwrap();
            }
            run_until_done(
                || cursor.seek(SeekKey::TableRowId(3), SeekOp::GE { eq_only: true }),
                pager.deref(),
            )
            .unwrap();
            assert!(cursor.has_record());

            let outcome = run_until_done(
                || cursor.try_save_position_for_external_balance(),
                pager.deref(),
            )
            .unwrap();
            assert_eq!(outcome, SavePositionResult::Saved);
            assert_eq!(cursor.valid_state, CursorValidState::RequireSeek);
            assert!(cursor.context.is_some());
        }

        #[test]
        fn try_save_position_unpositioned_returns_saved_noop() {
            // valid_state=Valid + has_record=false ⇒ nothing to save and
            // nothing to invalidate (stack already in a re-navigable state).
            let (pager, root_page, _db, _conn) = empty_btree();
            let mut cursor = BTreeCursor::new_table(pager.clone(), root_page, 1);
            let outcome = run_until_done(
                || cursor.try_save_position_for_external_balance(),
                pager.deref(),
            )
            .unwrap();
            assert_eq!(outcome, SavePositionResult::Saved);
            assert!(cursor.context.is_none());
        }

        #[test]
        fn clear_btree_auto_invalidates_peer() {
            // Without saveAllCursors, ResetSorter used to invalidate dup
            // cursors via pointer-equality from op_reset_sorter. Now
            // clear_btree itself does it via the pager registry.
            let (pager, root_page, _db, _conn) = empty_btree();
            let mut writer = make_registered_cursor(&pager, root_page, 1);
            let mut peer = make_registered_cursor(&pager, root_page, 1);

            for rowid in 1..=10 {
                insert_record(&mut writer, &pager, rowid, Value::from_i64(rowid)).unwrap();
            }
            run_until_done(
                || peer.seek(SeekKey::TableRowId(5), SeekOp::GE { eq_only: true }),
                pager.deref(),
            )
            .unwrap();
            assert!(peer.has_record());
            assert!(peer.stack.current_page >= 0);

            run_until_done(|| writer.clear_btree(), &pager).unwrap();

            assert!(
                !peer.has_record(),
                "peer must observe has_record=false after a peer's clear_btree"
            );
            assert_eq!(
                peer.stack.current_page, -1,
                "peer's page stack must be reset to the sentinel"
            );
        }

        #[test]
        fn delete_preserves_peer_logical_position() {
            // Cursor1 deletes rowid=3 while cursor2 sits on rowid=5. Without
            // the saveAllCursors port, cursor2's cached cell_idx points to
            // the cell that shifted left into rowid=5's slot (now rowid=6),
            // and rowid() silently returns 6. With the port, cursor2's
            // position is saved on entry to delete and restored on next
            // access — rowid() returns 5.
            let (pager, root_page, _db, _conn) = empty_btree();
            let mut writer = make_registered_cursor(&pager, root_page, 1);
            let mut peer = make_registered_cursor(&pager, root_page, 1);

            for rowid in 1..=10 {
                insert_record(&mut writer, &pager, rowid, Value::from_i64(rowid)).unwrap();
            }
            run_until_done(
                || peer.seek(SeekKey::TableRowId(5), SeekOp::GE { eq_only: true }),
                pager.deref(),
            )
            .unwrap();
            assert_eq!(
                run_until_done(|| peer.rowid(), pager.deref()).unwrap(),
                Some(5)
            );

            run_until_done(
                || writer.seek(SeekKey::TableRowId(3), SeekOp::GE { eq_only: true }),
                pager.deref(),
            )
            .unwrap();
            run_until_done(|| writer.delete(), pager.deref()).unwrap();

            assert_eq!(
                run_until_done(|| peer.rowid(), pager.deref()).unwrap(),
                Some(5),
                "peer must still observe its logical rowid after a peer delete"
            );
        }

        #[test]
        fn delete_of_peers_own_row_lands_on_next_greater() {
            // SQLite's CURSOR_SKIPNEXT (btree.c:915): when restore_context's
            // re-seek lands on NotFound, the cursor sets skip_advance so the
            // next() returns the cell the seek landed on instead of stepping
            // past it. Forward iteration continues correctly after a peer
            // deletes the row we were sitting on.
            let (pager, root_page, _db, _conn) = empty_btree();
            let mut writer = make_registered_cursor(&pager, root_page, 1);
            let mut peer = make_registered_cursor(&pager, root_page, 1);

            for rowid in 1..=10 {
                insert_record(&mut writer, &pager, rowid, Value::from_i64(rowid)).unwrap();
            }
            run_until_done(
                || peer.seek(SeekKey::TableRowId(5), SeekOp::GE { eq_only: true }),
                pager.deref(),
            )
            .unwrap();

            run_until_done(
                || writer.seek(SeekKey::TableRowId(5), SeekOp::GE { eq_only: true }),
                pager.deref(),
            )
            .unwrap();
            run_until_done(|| writer.delete(), pager.deref()).unwrap();

            // First access after a peer wipe-out triggers restore_context's
            // NotFound branch and sets skip_advance.
            run_until_done(|| peer.next(), pager.deref()).unwrap();
            assert_eq!(
                run_until_done(|| peer.rowid(), pager.deref()).unwrap(),
                Some(6),
                "next() after peer deletion of our row must land on the next-greater rowid"
            );
        }

        #[test]
        fn insert_preserves_peer_logical_position() {
            // Cursor2 sits on rowid=5; cursor1 inserts rowid=2 (causes
            // cells to shift right on the shared page). Without saveAllCursors,
            // cursor2's cell_idx now points to rowid=4. With the port, the
            // saved rowid=5 is restored on next access.
            let (pager, root_page, _db, _conn) = empty_btree();
            let mut writer = make_registered_cursor(&pager, root_page, 1);
            let mut peer = make_registered_cursor(&pager, root_page, 1);

            // Insert odd rowids so even slots are open for the peer-disrupting
            // insert below.
            for rowid in [1i64, 3, 5, 7, 9] {
                insert_record(&mut writer, &pager, rowid, Value::from_i64(rowid)).unwrap();
            }
            run_until_done(
                || peer.seek(SeekKey::TableRowId(5), SeekOp::GE { eq_only: true }),
                pager.deref(),
            )
            .unwrap();
            assert_eq!(
                run_until_done(|| peer.rowid(), pager.deref()).unwrap(),
                Some(5)
            );

            insert_record(&mut writer, &pager, 2, Value::from_i64(2)).unwrap();

            assert_eq!(
                run_until_done(|| peer.rowid(), pager.deref()).unwrap(),
                Some(5),
                "peer must observe its saved rowid after a left-of-it peer insert"
            );
        }
    }

    /// Strict property tests for page-level btree mutations.
    ///
    /// These tests model expected cell bytes and check that every mutation
    /// preserves both byte-level payload contents and page-layout invariants.
    mod property_tests {
        use std::collections::HashSet;

        use quickcheck::{quickcheck, TestResult};

        use crate::storage::btree::{
            compute_free_space, defragment_page, drop_cell, insert_into_cell,
        };
        use crate::storage::sqlite3_ondisk::{write_varint, PageContent, CELL_PTR_SIZE_BYTES};
        use crate::PageRef;

        use super::get_page;

        const PAGE_SIZE: usize = 4096;
        const MIN_INSERTED_CELLS: usize = 6;

        struct FillOutcome {
            expected: Vec<Vec<u8>>,
            had_middle_insert: bool,
        }

        /// Convert arbitrary fuzz bytes into bounded payload sizes that are small enough
        /// to produce many cells and varied freeblock behavior in a single page.
        fn normalize_sizes(raw: &[u8]) -> Vec<usize> {
            raw.iter()
                .take(300)
                .map(|v| ((*v as usize) % 220) + 1)
                .collect()
        }

        /// Validate strict page invariants after each mutation.
        ///
        /// Checks:
        /// - pointer array/header consistency:
        ///   `unallocated_region_start` must equal
        ///   `cell_pointer_array_offset + (cell_count * 2)`, so the header and pointer-array
        ///   metadata agree on where unallocated space begins.
        /// - structural bounds:
        ///   every cell and freeblock must lie fully within `[cell_content_area, usable_space)`.
        /// - pointer uniqueness:
        ///   no two cell-pointer entries may reference the same cell start offset.
        /// - interval non-overlap:
        ///   cell byte ranges and freeblock ranges must be disjoint; overlap means corruption.
        /// - freeblock chain validity:
        ///   the linked list must be strictly ascending by offset and must not contain cycles.
        /// - accounting equality:
        ///   independently computed free space from layout pieces must exactly equal
        ///   `compute_free_space(page, usable_space)`.
        /// - logical data preservation (optional):
        ///   when an expected model is provided, each on-page cell must match expected bytes
        ///   at the same logical index.
        fn strict_validate_page(
            page: &PageContent,
            usable_space: usize,
            expected_cells: Option<&[Vec<u8>]>,
        ) {
            let cell_count = page.cell_count();
            let cell_content_area = page.cell_content_area() as usize;
            let unallocated_start = page.unallocated_region_start();
            let ptr_start = page.cell_pointer_array_offset();
            let expected_unallocated_start = ptr_start + (cell_count * CELL_PTR_SIZE_BYTES);

            assert_eq!(
                unallocated_start, expected_unallocated_start,
                "unallocated region start inconsistent with cell pointer array"
            );
            assert!(
                unallocated_start <= cell_content_area,
                "cell pointer array overlaps cell content area"
            );
            assert!(
                cell_content_area <= usable_space,
                "cell content area beyond usable space"
            );
            assert!(
                page.num_frag_free_bytes() <= 60,
                "fragmented free bytes exceed SQLite limit"
            );

            let mut intervals = Vec::<(usize, usize, &'static str)>::new();
            let mut ptrs = HashSet::new();
            for i in 0..cell_count {
                let ptr_offset = ptr_start + (i * CELL_PTR_SIZE_BYTES);
                let raw_ptr = page.read_u16_no_offset(ptr_offset) as usize;
                let (start, len) = page.cell_get_raw_region(i, usable_space).unwrap();
                assert_eq!(
                    raw_ptr, start,
                    "cell pointer does not match parsed cell start"
                );
                assert!(len >= 2, "cell too small");
                assert!(
                    start >= cell_content_area,
                    "cell starts before cell content area"
                );
                assert!(
                    start + len <= usable_space,
                    "cell extends beyond usable space"
                );
                assert!(ptrs.insert(raw_ptr), "duplicate cell pointer");
                intervals.push((start, start + len, "cell"));
            }

            let mut freeblock_total = 0usize;
            let mut seen_freeblocks = HashSet::new();
            let mut cur = page.first_freeblock() as usize;
            let mut prev = 0usize;
            while cur != 0 {
                assert!(
                    seen_freeblocks.insert(cur),
                    "freeblock cycle detected at offset {cur}"
                );
                assert!(
                    cur >= cell_content_area,
                    "freeblock before cell content area"
                );
                assert!(cur + 4 <= usable_space, "freeblock header out of bounds");
                let (next, size_u16) = page.read_freeblock(cur as u16);
                let size = size_u16 as usize;
                assert!(size >= 4, "freeblock size too small");
                assert!(
                    cur + size <= usable_space,
                    "freeblock extends beyond usable space"
                );
                if prev != 0 {
                    assert!(cur > prev, "freeblocks must be strictly ascending");
                }
                let next_usize = next as usize;
                if next_usize != 0 {
                    assert!(next_usize > cur, "freeblock next pointer not ascending");
                }
                intervals.push((cur, cur + size, "freeblock"));
                freeblock_total += size;
                prev = cur;
                cur = next_usize;
            }

            intervals.sort_by_key(|(start, _, _)| *start);
            for pair in intervals.windows(2) {
                let (a_start, a_end, a_kind) = pair[0];
                let (b_start, _b_end, b_kind) = pair[1];
                assert!(
                    a_end <= b_start,
                    "interval overlap: {a_kind}@{a_start}..{a_end} overlaps {b_kind}@{b_start}"
                );
            }

            let computed = compute_free_space(page, usable_space).unwrap();
            let expected_free = (cell_content_area - unallocated_start)
                + page.num_frag_free_bytes() as usize
                + freeblock_total;
            assert_eq!(
                computed, expected_free,
                "compute_free_space mismatch: computed={computed}, expected={expected_free}"
            );

            if let Some(expected_cells) = expected_cells {
                assert_eq!(
                    cell_count,
                    expected_cells.len(),
                    "cell count mismatch against expected model"
                );
                for (i, expected) in expected_cells.iter().enumerate() {
                    let (start, len) = page.cell_get_raw_region(i, usable_space).unwrap();
                    let actual = &page.as_ptr()[start..start + len];
                    assert_eq!(
                        actual,
                        expected.as_slice(),
                        "cell bytes mismatch at idx {i}"
                    );
                }
            }
        }

        /// Build a valid table-leaf cell:
        /// [payload_size varint][rowid varint][record(header + blob data)].
        ///
        /// The body is synthetic but stable, so byte-level equality checks are deterministic.
        fn make_table_leaf_cell(rowid: u64, data_size: usize) -> Vec<u8> {
            let mut cell = Vec::new();
            let serial_type = (data_size as u64) * 2 + 12;

            let mut header_buf = [0u8; 9];
            let mut serial_buf = [0u8; 9];
            let serial_len = write_varint(&mut serial_buf, serial_type);
            let header_size = 1 + serial_len;
            let header_size_len = write_varint(&mut header_buf, header_size as u64);

            let mut record = Vec::new();
            record.extend_from_slice(&header_buf[..header_size_len]);
            record.extend_from_slice(&serial_buf[..serial_len]);
            record.extend(vec![0xAB; data_size]);

            let payload_size = record.len() as u64;
            let mut payload_size_buf = [0u8; 9];
            let payload_size_len = write_varint(&mut payload_size_buf, payload_size);
            cell.extend_from_slice(&payload_size_buf[..payload_size_len]);

            let mut rowid_buf = [0u8; 9];
            let rowid_len = write_varint(&mut rowid_buf, rowid);
            cell.extend_from_slice(&rowid_buf[..rowid_len]);
            cell.extend_from_slice(&record);
            cell
        }

        /// Execute a modeled insertion workload against one page.
        ///
        /// For each insert that fits, mutate both:
        /// - the real page (via `insert_into_cell`), and
        /// - the expected model vector at the same index.
        ///
        /// `had_middle_insert` ensures we exercised pointer-shift paths, not only appends.
        fn fill_page_with_model(
            page: &PageRef,
            cell_sizes: &[usize],
            insert_hints: &[u8],
        ) -> FillOutcome {
            let mut expected = Vec::new();
            let mut had_middle_insert = false;
            let contents = page.get_contents();

            for (i, size) in cell_sizes.iter().copied().enumerate() {
                let cell = make_table_leaf_cell(i as u64, size);
                let free = compute_free_space(contents, PAGE_SIZE).unwrap();
                if cell.len() + CELL_PTR_SIZE_BYTES > free {
                    continue;
                }

                let idx = if expected.is_empty() {
                    0
                } else {
                    insert_hints.get(i).copied().unwrap_or(i as u8) as usize % (expected.len() + 1)
                };
                if idx < expected.len() {
                    had_middle_insert = true;
                }

                insert_into_cell(contents, &cell, idx, PAGE_SIZE).unwrap();
                expected.insert(idx, cell);
                strict_validate_page(contents, PAGE_SIZE, Some(&expected));
            }

            FillOutcome {
                expected,
                had_middle_insert,
            }
        }

        quickcheck! {
            // Invariant: arbitrary insert sequences (including middle inserts) preserve exact cell bytes
            // and keep page layout/accounting valid after every insertion.
            fn prop_insertions_preserve_exact_cell_bytes(
                raw_sizes: Vec<u8>,
                insert_hints: Vec<u8>
            ) -> TestResult {
                // Build many small payload sizes from random bytes so one page gets many edits.
                let cell_sizes = normalize_sizes(&raw_sizes);
                if cell_sizes.len() < MIN_INSERTED_CELLS {
                    return TestResult::discard();
                }

                let page = get_page(2);
                // Mutate both the real page and the expected-model vector in lock-step.
                let outcome = fill_page_with_model(&page, &cell_sizes, &insert_hints);
                // Require enough inserts and at least one middle insert (not append-only).
                if outcome.expected.len() < MIN_INSERTED_CELLS || !outcome.had_middle_insert {
                    return TestResult::discard();
                }

                // Final strict check: metadata + free-space accounting + exact cell bytes.
                strict_validate_page(page.get_contents(), PAGE_SIZE, Some(&outcome.expected));
                TestResult::passed()
            }
        }

        quickcheck! {
            // Invariant: every drop operation removes exactly one modeled cell, never mutates surviving
            // cell bytes, and always preserves freeblock/pointer/free-space structural validity.
            fn prop_drop_sequence_preserves_model_and_layout(
                raw_sizes: Vec<u8>,
                insert_hints: Vec<u8>,
                drop_ops: Vec<u8>
            ) -> TestResult {
                if drop_ops.is_empty() {
                    return TestResult::discard();
                }

                // Start from a non-trivial page state built with randomized inserts.
                let page = get_page(2);
                let cell_sizes = normalize_sizes(&raw_sizes);
                let mut outcome = fill_page_with_model(&page, &cell_sizes, &insert_hints);
                if outcome.expected.len() < MIN_INSERTED_CELLS || !outcome.had_middle_insert {
                    return TestResult::discard();
                }

                let contents = page.get_contents();
                let mut drops_executed = 0usize;
                for op in drop_ops.iter().take(200) {
                    // Stop once the model is empty; there is nothing left to drop.
                    if outcome.expected.is_empty() {
                        break;
                    }
                    // Drop same logical index in model and real page.
                    let idx = (*op as usize) % outcome.expected.len();
                    outcome.expected.remove(idx);
                    drop_cell(contents, idx, PAGE_SIZE).unwrap();
                    // After each mutation, validate structure and surviving bytes immediately.
                    strict_validate_page(contents, PAGE_SIZE, Some(&outcome.expected));
                    drops_executed += 1;
                }

                if drops_executed == 0 {
                    // Require at least one real mutation, otherwise this run is not informative.
                    return TestResult::discard();
                }
                TestResult::passed()
            }
        }

        quickcheck! {
            // Invariant: after creating holes via drops, inserting new cells back into the page
            // preserves all existing bytes and keeps freeblock reuse/allocation safe.
            fn prop_insert_drop_insert_reuses_space_safely(
                raw_sizes: Vec<u8>,
                insert_hints: Vec<u8>,
                drop_ops: Vec<u8>,
                new_sizes: Vec<u8>,
                new_insert_hints: Vec<u8>
            ) -> TestResult {
                if drop_ops.is_empty() || new_sizes.is_empty() {
                    return TestResult::discard();
                }

                let page = get_page(2);
                let cell_sizes = normalize_sizes(&raw_sizes);
                let mut outcome = fill_page_with_model(&page, &cell_sizes, &insert_hints);
                if outcome.expected.len() < MIN_INSERTED_CELLS {
                    return TestResult::discard();
                }

                let contents = page.get_contents();
                let mut drops_executed = 0usize;
                // Phase 1: create holes and freeblocks by dropping cells in random positions.
                for op in drop_ops.iter().take(16) {
                    if outcome.expected.len() <= 2 {
                        break;
                    }
                    let idx = (*op as usize) % outcome.expected.len();
                    outcome.expected.remove(idx);
                    drop_cell(contents, idx, PAGE_SIZE).unwrap();
                    strict_validate_page(contents, PAGE_SIZE, Some(&outcome.expected));
                    drops_executed += 1;
                }
                if drops_executed == 0 {
                    return TestResult::discard();
                }

                let base_rowid = 1_000_000u64 + outcome.expected.len() as u64;
                let mut inserted = 0usize;
                // Phase 2: insert new cells back, forcing allocator/freeblock reuse paths.
                for (i, raw) in new_sizes.iter().take(32).enumerate() {
                    let size = ((*raw as usize) % 220) + 1;
                    let cell = make_table_leaf_cell(base_rowid + i as u64, size);
                    let free = compute_free_space(contents, PAGE_SIZE).unwrap();
                    if cell.len() + CELL_PTR_SIZE_BYTES > free {
                        continue;
                    }
                    let idx = if outcome.expected.is_empty() {
                        0
                    } else {
                        new_insert_hints
                            .get(i)
                            .copied()
                            .unwrap_or(i as u8) as usize
                            % (outcome.expected.len() + 1)
                    };
                    insert_into_cell(contents, &cell, idx, PAGE_SIZE).unwrap();
                    outcome.expected.insert(idx, cell);
                    strict_validate_page(contents, PAGE_SIZE, Some(&outcome.expected));
                    inserted += 1;
                }

                if inserted == 0 {
                    // Require at least one successful re-insert to exercise the target path.
                    return TestResult::discard();
                }
                TestResult::passed()
            }
        }

        quickcheck! {
            // Invariant: full defragmentation is lossless (all live cell bytes unchanged), reaches canonical
            // no-freeblock/no-fragment state, and is idempotent when applied repeatedly.
            fn prop_defragment_is_lossless_and_idempotent(
                raw_sizes: Vec<u8>,
                insert_hints: Vec<u8>,
                drop_ops: Vec<u8>
            ) -> TestResult {
                if drop_ops.is_empty() {
                    return TestResult::discard();
                }

                let page = get_page(2);
                let cell_sizes = normalize_sizes(&raw_sizes);
                let mut outcome = fill_page_with_model(&page, &cell_sizes, &insert_hints);
                if outcome.expected.len() < MIN_INSERTED_CELLS {
                    // Need enough cells so one drop still leaves a meaningful page state.
                    return TestResult::discard();
                }

                let contents = page.get_contents();
                let mut drops_executed = 0usize;
                for op in drop_ops.iter().take(40) {
                    if outcome.expected.len() <= 1 {
                        break;
                    }
                    // Create realistic holes/freeblocks before defragmenting.
                    let idx = (*op as usize) % outcome.expected.len();
                    outcome.expected.remove(idx);
                    drop_cell(contents, idx, PAGE_SIZE).unwrap();
                    strict_validate_page(contents, PAGE_SIZE, Some(&outcome.expected));
                    drops_executed += 1;
                }

                if drops_executed == 0 || outcome.expected.is_empty() {
                    return TestResult::discard();
                }

                // First defrag: must preserve live cells and clean freeblock/fragment metadata.
                defragment_page(contents, PAGE_SIZE, -1).unwrap();
                strict_validate_page(contents, PAGE_SIZE, Some(&outcome.expected));
                assert_eq!(contents.first_freeblock(), 0, "freeblocks remain after defrag");
                assert_eq!(contents.num_frag_free_bytes(), 0, "fragments remain after defrag");

                // Second defrag should be a no-op on bytes (idempotence).
                let snapshot_after_first = contents.as_ptr().to_vec();
                defragment_page(contents, PAGE_SIZE, -1).unwrap();
                strict_validate_page(contents, PAGE_SIZE, Some(&outcome.expected));
                assert_eq!(
                    contents.as_ptr().to_vec(),
                    snapshot_after_first,
                    "defragmentation is not idempotent"
                );
                TestResult::passed()
            }
        }

        quickcheck! {
            // Invariant: for simple freeblock layouts where fast-path is applicable, fast defrag and
            // full defrag produce the same logical page state and identical serialized cell bytes.
            fn prop_defragment_fast_matches_full(
                raw_sizes: Vec<u8>,
                insert_hints: Vec<u8>,
                drop_op: u8
            ) -> TestResult {
                let cell_sizes = normalize_sizes(&raw_sizes);
                if cell_sizes.len() < MIN_INSERTED_CELLS {
                    return TestResult::discard();
                }

                let page_fast = get_page(2);
                let mut outcome = fill_page_with_model(&page_fast, &cell_sizes, &insert_hints);
                if outcome.expected.len() < MIN_INSERTED_CELLS {
                    return TestResult::discard();
                }

                // Clone logical state to second page so both start identical.
                let page_full = get_page(3);
                let full_contents = page_full.get_contents();
                for (i, cell) in outcome.expected.iter().enumerate() {
                    insert_into_cell(full_contents, cell, i, PAGE_SIZE).unwrap();
                }
                strict_validate_page(full_contents, PAGE_SIZE, Some(&outcome.expected));

                // Create a single hole => one freeblock, making fast-path eligibility likely.
                let idx = drop_op as usize % outcome.expected.len();
                let fast_contents = page_fast.get_contents();
                outcome.expected.remove(idx);
                drop_cell(fast_contents, idx, PAGE_SIZE).unwrap();
                drop_cell(full_contents, idx, PAGE_SIZE).unwrap();
                strict_validate_page(fast_contents, PAGE_SIZE, Some(&outcome.expected));
                strict_validate_page(full_contents, PAGE_SIZE, Some(&outcome.expected));

                // Try fast-path on one page and force full-path on the other.
                defragment_page(fast_contents, PAGE_SIZE, 4).unwrap();
                defragment_page(full_contents, PAGE_SIZE, -1).unwrap();

                // Both algorithms must preserve the exact same logical model.
                strict_validate_page(fast_contents, PAGE_SIZE, Some(&outcome.expected));
                strict_validate_page(full_contents, PAGE_SIZE, Some(&outcome.expected));
                assert_eq!(fast_contents.cell_count(), full_contents.cell_count());
                assert_eq!(
                    compute_free_space(fast_contents, PAGE_SIZE).unwrap(),
                    compute_free_space(full_contents, PAGE_SIZE).unwrap()
                );
                assert_eq!(fast_contents.first_freeblock(), full_contents.first_freeblock());
                assert_eq!(
                    fast_contents.num_frag_free_bytes(),
                    full_contents.num_frag_free_bytes()
                );

                for i in 0..fast_contents.cell_count() {
                    let (s1, l1) = fast_contents.cell_get_raw_region(i, PAGE_SIZE).unwrap();
                    let (s2, l2) = full_contents.cell_get_raw_region(i, PAGE_SIZE).unwrap();
                    assert_eq!(l1, l2, "cell {i} length mismatch after defragmentation");
                    assert_eq!(
                        &fast_contents.as_ptr()[s1..s1 + l1],
                        &full_contents.as_ptr()[s2..s2 + l2],
                        "cell {i} bytes mismatch between fast and full defrag"
                    );
                }
                TestResult::passed()
            }
        }

        quickcheck! {
            // Invariant: dropping all cells and defragmenting returns the page to a canonical empty state
            // (zero cells, no fragments/freeblocks, content area at end, exact free-space accounting).
            fn prop_drop_all_then_defrag_returns_canonical_empty_page(
                raw_sizes: Vec<u8>,
                insert_hints: Vec<u8>
            ) -> TestResult {
                let page = get_page(2);
                let cell_sizes = normalize_sizes(&raw_sizes);
                let mut outcome = fill_page_with_model(&page, &cell_sizes, &insert_hints);
                if outcome.expected.len() < MIN_INSERTED_CELLS {
                    return TestResult::discard();
                }

                let contents = page.get_contents();
                while !outcome.expected.is_empty() {
                    // Repeatedly drop from the logical front so model and page stay aligned.
                    outcome.expected.remove(0);
                    drop_cell(contents, 0, PAGE_SIZE).unwrap();
                    strict_validate_page(contents, PAGE_SIZE, Some(&outcome.expected));
                }

                // After all cells are gone, defrag should normalize page to canonical empty form.
                defragment_page(contents, PAGE_SIZE, -1).unwrap();
                strict_validate_page(contents, PAGE_SIZE, Some(&[]));
                assert_eq!(contents.cell_count(), 0);
                assert_eq!(contents.first_freeblock(), 0);
                assert_eq!(contents.num_frag_free_bytes(), 0);
                assert_eq!(contents.cell_content_area() as usize, PAGE_SIZE);
                assert_eq!(
                    compute_free_space(contents, PAGE_SIZE).unwrap(),
                    PAGE_SIZE - contents.header_size(),
                    "empty page must expose full free space minus header"
                );
                TestResult::passed()
            }
        }
    }

    /// Corruption-handling properties.
    ///
    /// These tests verify that malformed on-page metadata is rejected with
    /// corruption errors, instead of silently succeeding or panicking.
    mod corruption_properties {
        use quickcheck::quickcheck;

        use crate::storage::btree::{compute_free_space, defragment_page, insert_into_cell};
        use crate::storage::sqlite3_ondisk::write_varint;

        use super::get_page;

        const PAGE_SIZE: usize = 4096;

        fn make_table_leaf_cell(rowid: u64, data_size: usize) -> Vec<u8> {
            let mut cell = Vec::new();
            let serial_type = (data_size as u64) * 2 + 12;

            let mut header_buf = [0u8; 9];
            let mut serial_buf = [0u8; 9];
            let serial_len = write_varint(&mut serial_buf, serial_type);
            let header_size = 1 + serial_len;
            let header_size_len = write_varint(&mut header_buf, header_size as u64);

            let mut record = Vec::new();
            record.extend_from_slice(&header_buf[..header_size_len]);
            record.extend_from_slice(&serial_buf[..serial_len]);
            record.extend(vec![0xCC; data_size]);

            let payload_size = record.len() as u64;
            let mut payload_size_buf = [0u8; 9];
            let payload_size_len = write_varint(&mut payload_size_buf, payload_size);
            cell.extend_from_slice(&payload_size_buf[..payload_size_len]);

            let mut rowid_buf = [0u8; 9];
            let rowid_len = write_varint(&mut rowid_buf, rowid);
            cell.extend_from_slice(&rowid_buf[..rowid_len]);
            cell.extend_from_slice(&record);
            cell
        }

        quickcheck! {
            // Desired invariant: malformed freeblock pointer values should return Corrupt errors.
            fn prop_compute_free_space_returns_err_when_first_freeblock_is_invalid(seed: u16) -> bool {
                let page = get_page(2);
                let contents = page.get_contents();
                let bad_ptr = ((seed as usize % (PAGE_SIZE - 1)) + 1) as u16; // 1..=4095, always < initial cell_content_area (4096)
                contents.write_first_freeblock(bad_ptr);
                compute_free_space(contents, PAGE_SIZE).is_err()
            }
        }

        quickcheck! {
            // Desired invariant: malformed freeblock chain ordering should return Corrupt errors.
            fn prop_compute_free_space_returns_err_on_malformed_freeblock_chain(seed: u16) -> bool {
                let page = get_page(2);
                let contents = page.get_contents();

                // Move content area left so freeblocks can exist "inside content area".
                contents.write_cell_content_area(64);

                // Create one freeblock whose "next" pointer violates ordering assumptions.
                let base = 128 + (seed as usize % (PAGE_SIZE - 256));
                let cur = base as u16;
                let next = (base + 1) as u16; // intentionally invalid relative to size constraints
                contents.write_first_freeblock(cur);
                contents.write_freeblock(cur, 8, Some(next));
                compute_free_space(contents, PAGE_SIZE).is_err()
            }
        }

        quickcheck! {
            // Desired invariant: malformed freeblock metadata should return Corrupt errors.
            fn prop_defragment_returns_err_on_malformed_freeblock_chain(seed: u8) -> bool {
                let page = get_page(2);
                let contents = page.get_contents();

                // Ensure page is non-empty so defragmentation doesn't early-return.
                let cell = make_table_leaf_cell(1, (seed as usize % 24) + 1);
                if insert_into_cell(contents, &cell, 0, PAGE_SIZE).is_err() {
                    return true;
                }

                // Construct malformed chain: first freeblock points "backwards".
                contents.write_first_freeblock(100);
                contents.write_freeblock(100, 8, Some(90));
                defragment_page(contents, PAGE_SIZE, 4).is_err()
            }
        }
    }
}
