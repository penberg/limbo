use log::debug;

use crate::storage::pager::Pager;
use crate::storage::sqlite3_ondisk::{
    read_btree_cell, read_varint, BTreeCell, DatabaseHeader, PageContent, PageType,
    TableInteriorCell, TableLeafCell,
};

use crate::types::{CursorResult, OwnedValue, Record, SeekKey, SeekOp};
use crate::{LimboError, Result};

use std::cell::{Ref, RefCell};
use std::pin::Pin;
use std::rc::Rc;

use super::pager::PageRef;
use super::sqlite3_ondisk::{
    payload_overflows, write_varint_to_vec, IndexInteriorCell, IndexLeafCell, OverflowCell,
    DATABASE_HEADER_SIZE,
};

/*
    These are offsets of fields in the header of a b-tree page.
*/

/// type of btree page -> u8
const PAGE_HEADER_OFFSET_PAGE_TYPE: usize = 0;
/// pointer to first freeblock -> u16
/// The second field of the b-tree page header is the offset of the first freeblock, or zero if there are no freeblocks on the page.
/// A freeblock is a structure used to identify unallocated space within a b-tree page.
/// Freeblocks are organized as a chain.
///
/// To be clear, freeblocks do not mean the regular unallocated free space to the left of the cell content area pointer, but instead
/// blocks of at least 4 bytes WITHIN the cell content area that are not in use due to e.g. deletions.
const PAGE_HEADER_OFFSET_FIRST_FREEBLOCK: usize = 1;
/// number of cells in the page -> u16
const PAGE_HEADER_OFFSET_CELL_COUNT: usize = 3;
/// pointer to first byte of cell allocated content from top -> u16
/// SQLite strives to place cells as far toward the end of the b-tree page as it can,
/// in order to leave space for future growth of the cell pointer array.
/// = the cell content area pointer moves leftward as cells are added to the page
const PAGE_HEADER_OFFSET_CELL_CONTENT_AREA: usize = 5;
/// number of fragmented bytes -> u8
/// Fragments are isolated groups of 1, 2, or 3 unused bytes within the cell content area.
const PAGE_HEADER_OFFSET_FRAGMENTED_BYTES_COUNT: usize = 7;
/// if internalnode, pointer right most pointer (saved separately from cells) -> u32
const PAGE_HEADER_OFFSET_RIGHTMOST_PTR: usize = 8;

/// Maximum depth of an SQLite B-Tree structure. Any B-Tree deeper than
/// this will be declared corrupt. This value is calculated based on a
/// maximum database size of 2^31 pages a minimum fanout of 2 for a
/// root-node and 3 for all other internal nodes.
///
/// If a tree that appears to be taller than this is encountered, it is
/// assumed that the database is corrupt.
pub const BTCURSOR_MAX_DEPTH: usize = 20;

/// Evaluate a Result<CursorResult<T>>, if IO return IO.
macro_rules! return_if_io {
    ($expr:expr) => {
        match $expr? {
            CursorResult::Ok(v) => v,
            CursorResult::IO => return Ok(CursorResult::IO),
        }
    };
}

/// Check if the page is unlocked, if not return IO.
macro_rules! return_if_locked {
    ($expr:expr) => {{
        if $expr.is_locked() {
            return Ok(CursorResult::IO);
        }
    }};
}

/// State machine of a write operation.
/// May involve balancing due to overflow.
#[derive(Debug, Clone, Copy)]
enum WriteState {
    Start,
    BalanceStart,
    BalanceNonRoot,
    BalanceGetParentPage,
    BalanceMoveUp,
    Finish,
}

struct WriteInfo {
    /// State of the write operation state machine.
    state: WriteState,
    /// Pages involved in the split of the page due to balancing (splits_pages[0] is the balancing page, while other - fresh allocated pages)
    split_pages: RefCell<Vec<PageRef>>,
    /// Amount of cells from balancing page for every split page
    split_pages_cells_count: RefCell<Vec<usize>>,
    /// Scratch space used during balancing.
    scratch_cells: RefCell<Vec<&'static [u8]>>,
    /// Bookkeeping of the rightmost pointer so the PAGE_HEADER_OFFSET_RIGHTMOST_PTR can be updated.
    rightmost_pointer: RefCell<Option<u32>>,
    /// Copy of the current page needed for buffer references.
    page_copy: RefCell<Option<PageContent>>,
}

impl WriteInfo {
    fn new() -> WriteInfo {
        WriteInfo {
            state: WriteState::Start,
            split_pages: RefCell::new(Vec::with_capacity(4)),
            split_pages_cells_count: RefCell::new(Vec::with_capacity(4)),
            scratch_cells: RefCell::new(Vec::new()),
            rightmost_pointer: RefCell::new(None),
            page_copy: RefCell::new(None),
        }
    }
}

/// Holds the state machine for the operation that was in flight when the cursor
/// was suspended due to IO.
enum CursorState {
    None,
    Write(WriteInfo),
}

impl CursorState {
    fn write_info(&self) -> Option<&WriteInfo> {
        match self {
            CursorState::Write(x) => Some(x),
            _ => None,
        }
    }
    fn mut_write_info(&mut self) -> Option<&mut WriteInfo> {
        match self {
            CursorState::Write(x) => Some(x),
            _ => None,
        }
    }
}

pub struct BTreeCursor {
    pager: Rc<Pager>,
    /// Page id of the root page used to go back up fast.
    root_page: usize,
    /// Rowid and record are stored before being consumed.
    rowid: RefCell<Option<u64>>,
    record: RefCell<Option<Record>>,
    null_flag: bool,
    /// Index internal pages are consumed on the way up, so we store going upwards flag in case
    /// we just moved to a parent page and the parent page is an internal index page which requires
    /// to be consumed.
    going_upwards: bool,
    /// Information maintained across execution attempts when an operation yields due to I/O.
    state: CursorState,
    /// Page stack used to traverse the btree.
    /// Each cursor has a stack because each cursor traverses the btree independently.
    stack: PageStack,
}

/// Stack of pages representing the tree traversal order.
/// current_page represents the current page being used in the tree and current_page - 1 would be
/// the parent. Using current_page + 1 or higher is undefined behaviour.
struct PageStack {
    /// Pointer to the current page being consumed
    current_page: RefCell<i32>,
    /// List of pages in the stack. Root page will be in index 0
    stack: RefCell<[Option<PageRef>; BTCURSOR_MAX_DEPTH + 1]>,
    /// List of cell indices in the stack.
    /// cell_indices[current_page] is the current cell index being consumed. Similarly
    /// cell_indices[current_page-1] is the cell index of the parent of the current page
    /// that we save in case of going back up.
    /// There are two points that need special attention:
    ///  If cell_indices[current_page] = -1, it indicates that the current iteration has reached the start of the current_page
    ///  If cell_indices[current_page] = `cell_count`, it means that the current iteration has reached the end of the current_page
    cell_indices: RefCell<[i32; BTCURSOR_MAX_DEPTH + 1]>,
}

impl BTreeCursor {
    pub fn new(pager: Rc<Pager>, root_page: usize) -> Self {
        Self {
            pager,
            root_page,
            rowid: RefCell::new(None),
            record: RefCell::new(None),
            null_flag: false,
            going_upwards: false,
            state: CursorState::None,
            stack: PageStack {
                current_page: RefCell::new(-1),
                cell_indices: RefCell::new([0; BTCURSOR_MAX_DEPTH + 1]),
                stack: RefCell::new([const { None }; BTCURSOR_MAX_DEPTH + 1]),
            },
        }
    }

    /// Check if the table is empty.
    /// This is done by checking if the root page has no cells.
    fn is_empty_table(&self) -> Result<CursorResult<bool>> {
        let page = self.pager.read_page(self.root_page)?;
        return_if_locked!(page);

        let cell_count = page.get().contents.as_ref().unwrap().cell_count();
        Ok(CursorResult::Ok(cell_count == 0))
    }

    /// Move the cursor to the previous record and return it.
    /// Used in backwards iteration.
    fn get_prev_record(&mut self) -> Result<CursorResult<(Option<u64>, Option<Record>)>> {
        loop {
            let page = self.stack.top();
            let cell_idx = self.stack.current_cell_index();

            // moved to beginning of current page
            // todo: find a better way to flag moved to end or begin of page
            if self.stack.current_cell_index_less_than_min() {
                loop {
                    if self.stack.current_cell_index() > 0 {
                        self.stack.retreat();
                        break;
                    }
                    if self.stack.has_parent() {
                        self.stack.pop();
                    } else {
                        // moved to begin of btree
                        return Ok(CursorResult::Ok((None, None)));
                    }
                }
                // continue to next loop to get record from the new page
                continue;
            }

            let cell_idx = cell_idx as usize;
            debug!(
                "get_prev_record current id={} cell={}",
                page.get().id,
                cell_idx
            );
            return_if_locked!(page);
            if !page.is_loaded() {
                self.pager.load_page(page.clone())?;
                return Ok(CursorResult::IO);
            }
            let contents = page.get().contents.as_ref().unwrap();

            let cell_count = contents.cell_count();
            let cell_idx = if cell_idx >= cell_count {
                self.stack.set_cell_index(cell_count as i32 - 1);
                cell_count - 1
            } else {
                cell_idx
            };

            let cell = contents.cell_get(
                cell_idx,
                self.pager.clone(),
                self.payload_overflow_threshold_max(contents.page_type()),
                self.payload_overflow_threshold_min(contents.page_type()),
                self.usable_space(),
            )?;

            match cell {
                BTreeCell::TableInteriorCell(TableInteriorCell {
                    _left_child_page,
                    _rowid,
                }) => {
                    let mem_page = self.pager.read_page(_left_child_page as usize)?;
                    self.stack.push(mem_page);
                    // use cell_index = i32::MAX to tell next loop to go to the end of the current page
                    self.stack.set_cell_index(i32::MAX);
                    continue;
                }
                BTreeCell::TableLeafCell(TableLeafCell {
                    _rowid, _payload, ..
                }) => {
                    self.stack.retreat();
                    let record: Record = crate::storage::sqlite3_ondisk::read_record(&_payload)?;
                    return Ok(CursorResult::Ok((Some(_rowid), Some(record))));
                }
                BTreeCell::IndexInteriorCell(_) => todo!(),
                BTreeCell::IndexLeafCell(_) => todo!(),
            }
        }
    }

    /// Move the cursor to the next record and return it.
    /// Used in forwards iteration, which is the default.
    fn get_next_record(
        &mut self,
        predicate: Option<(SeekKey<'_>, SeekOp)>,
    ) -> Result<CursorResult<(Option<u64>, Option<Record>)>> {
        loop {
            let mem_page_rc = self.stack.top();
            let cell_idx = self.stack.current_cell_index() as usize;

            debug!("current id={} cell={}", mem_page_rc.get().id, cell_idx);
            return_if_locked!(mem_page_rc);
            if !mem_page_rc.is_loaded() {
                self.pager.load_page(mem_page_rc.clone())?;
                return Ok(CursorResult::IO);
            }
            let mem_page = mem_page_rc.get();

            let contents = mem_page.contents.as_ref().unwrap();

            if cell_idx == contents.cell_count() {
                // do rightmost
                let has_parent = self.stack.has_parent();
                match contents.rightmost_pointer() {
                    Some(right_most_pointer) => {
                        self.stack.advance();
                        let mem_page = self.pager.read_page(right_most_pointer as usize)?;
                        self.stack.push(mem_page);
                        continue;
                    }
                    None => {
                        if has_parent {
                            debug!("moving simple upwards");
                            self.going_upwards = true;
                            self.stack.pop();
                            continue;
                        } else {
                            return Ok(CursorResult::Ok((None, None)));
                        }
                    }
                }
            }

            if cell_idx > contents.cell_count() {
                // end
                let has_parent = self.stack.current() > 0;
                if has_parent {
                    debug!("moving upwards");
                    self.going_upwards = true;
                    self.stack.pop();
                    continue;
                } else {
                    return Ok(CursorResult::Ok((None, None)));
                }
            }
            assert!(cell_idx < contents.cell_count());

            let cell = contents.cell_get(
                cell_idx,
                self.pager.clone(),
                self.payload_overflow_threshold_max(contents.page_type()),
                self.payload_overflow_threshold_min(contents.page_type()),
                self.usable_space(),
            )?;
            match &cell {
                BTreeCell::TableInteriorCell(TableInteriorCell {
                    _left_child_page,
                    _rowid,
                }) => {
                    assert!(predicate.is_none());
                    self.stack.advance();
                    let mem_page = self.pager.read_page(*_left_child_page as usize)?;
                    self.stack.push(mem_page);
                    continue;
                }
                BTreeCell::TableLeafCell(TableLeafCell {
                    _rowid,
                    _payload,
                    first_overflow_page: _,
                }) => {
                    assert!(predicate.is_none());
                    self.stack.advance();
                    let record = crate::storage::sqlite3_ondisk::read_record(_payload)?;
                    return Ok(CursorResult::Ok((Some(*_rowid), Some(record))));
                }
                BTreeCell::IndexInteriorCell(IndexInteriorCell {
                    payload,
                    left_child_page,
                    ..
                }) => {
                    if !self.going_upwards {
                        let mem_page = self.pager.read_page(*left_child_page as usize)?;
                        self.stack.push(mem_page);
                        continue;
                    }

                    self.going_upwards = false;
                    self.stack.advance();

                    let record = crate::storage::sqlite3_ondisk::read_record(payload)?;
                    if predicate.is_none() {
                        let rowid = match record.last_value() {
                            Some(OwnedValue::Integer(rowid)) => *rowid as u64,
                            _ => unreachable!("index cells should have an integer rowid"),
                        };
                        return Ok(CursorResult::Ok((Some(rowid), Some(record))));
                    }

                    let (key, op) = predicate.as_ref().unwrap();
                    let SeekKey::IndexKey(index_key) = key else {
                        unreachable!("index seek key should be a record");
                    };
                    let found = match op {
                        SeekOp::GT => &record > *index_key,
                        SeekOp::GE => &record >= *index_key,
                        SeekOp::EQ => &record == *index_key,
                    };
                    if found {
                        let rowid = match record.last_value() {
                            Some(OwnedValue::Integer(rowid)) => *rowid as u64,
                            _ => unreachable!("index cells should have an integer rowid"),
                        };
                        return Ok(CursorResult::Ok((Some(rowid), Some(record))));
                    } else {
                        continue;
                    }
                }
                BTreeCell::IndexLeafCell(IndexLeafCell { payload, .. }) => {
                    self.stack.advance();
                    let record = crate::storage::sqlite3_ondisk::read_record(payload)?;
                    if predicate.is_none() {
                        let rowid = match record.last_value() {
                            Some(OwnedValue::Integer(rowid)) => *rowid as u64,
                            _ => unreachable!("index cells should have an integer rowid"),
                        };
                        return Ok(CursorResult::Ok((Some(rowid), Some(record))));
                    }
                    let (key, op) = predicate.as_ref().unwrap();
                    let SeekKey::IndexKey(index_key) = key else {
                        unreachable!("index seek key should be a record");
                    };
                    let found = match op {
                        SeekOp::GT => &record > *index_key,
                        SeekOp::GE => &record >= *index_key,
                        SeekOp::EQ => &record == *index_key,
                    };
                    if found {
                        let rowid = match record.last_value() {
                            Some(OwnedValue::Integer(rowid)) => *rowid as u64,
                            _ => unreachable!("index cells should have an integer rowid"),
                        };
                        return Ok(CursorResult::Ok((Some(rowid), Some(record))));
                    } else {
                        continue;
                    }
                }
            }
        }
    }

    /// Move the cursor to the record that matches the seek key and seek operation.
    /// This may be used to seek to a specific record in a point query (e.g. SELECT * FROM table WHERE col = 10)
    /// or e.g. find the first record greater than the seek key in a range query (e.g. SELECT * FROM table WHERE col > 10).
    /// We don't include the rowid in the comparison and that's why the last value from the record is not included.
    fn do_seek(
        &mut self,
        key: SeekKey<'_>,
        op: SeekOp,
    ) -> Result<CursorResult<(Option<u64>, Option<Record>)>> {
        return_if_io!(self.move_to(key.clone(), op.clone()));

        {
            let page = self.stack.top();
            return_if_locked!(page);

            let contents = page.get().contents.as_ref().unwrap();

            for cell_idx in 0..contents.cell_count() {
                let cell = contents.cell_get(
                    cell_idx,
                    self.pager.clone(),
                    self.payload_overflow_threshold_max(contents.page_type()),
                    self.payload_overflow_threshold_min(contents.page_type()),
                    self.usable_space(),
                )?;
                match &cell {
                    BTreeCell::TableLeafCell(TableLeafCell {
                        _rowid: cell_rowid,
                        _payload: payload,
                        first_overflow_page: _,
                    }) => {
                        let SeekKey::TableRowId(rowid_key) = key else {
                            unreachable!("table seek key should be a rowid");
                        };
                        let found = match op {
                            SeekOp::GT => *cell_rowid > rowid_key,
                            SeekOp::GE => *cell_rowid >= rowid_key,
                            SeekOp::EQ => *cell_rowid == rowid_key,
                        };
                        self.stack.advance();
                        if found {
                            let record = crate::storage::sqlite3_ondisk::read_record(payload)?;
                            return Ok(CursorResult::Ok((Some(*cell_rowid), Some(record))));
                        }
                    }
                    BTreeCell::IndexLeafCell(IndexLeafCell { payload, .. }) => {
                        let SeekKey::IndexKey(index_key) = key else {
                            unreachable!("index seek key should be a record");
                        };
                        let record = crate::storage::sqlite3_ondisk::read_record(payload)?;
                        let found = match op {
                            SeekOp::GT => {
                                record.get_values()[..record.len() - 1] > index_key.get_values()[..]
                            }
                            SeekOp::GE => {
                                record.get_values()[..record.len() - 1]
                                    >= index_key.get_values()[..]
                            }
                            SeekOp::EQ => {
                                record.get_values()[..record.len() - 1]
                                    == index_key.get_values()[..]
                            }
                        };
                        self.stack.advance();
                        if found {
                            let rowid = match record.last_value() {
                                Some(OwnedValue::Integer(rowid)) => *rowid as u64,
                                _ => unreachable!("index cells should have an integer rowid"),
                            };
                            return Ok(CursorResult::Ok((Some(rowid), Some(record))));
                        }
                    }
                    cell_type => {
                        unreachable!("unexpected cell type: {:?}", cell_type);
                    }
                }
            }
        }

        // We have now iterated over all cells in the leaf page and found no match.
        let is_index = matches!(key, SeekKey::IndexKey(_));
        if is_index {
            // Unlike tables, indexes store payloads in interior cells as well. self.move_to() always moves to a leaf page, so there are cases where we need to
            // move back up to the parent interior cell and get the next record from there to perform a correct seek.
            // an example of how this can occur:
            //
            // we do an index seek for key K with cmp = SeekOp::GT, meaning we want to seek to the first key that is greater than K.
            // in self.move_to(), we encounter an interior cell with key K' = K+2, and move the left child page, which is a leaf page.
            // the reason we move to the left child page is that we know that in an index, all keys in the left child page are less than K' i.e. less than K+2,
            // meaning that the left subtree may contain a key greater than K, e.g. K+1. however, it is possible that it doesn't, in which case the correct
            // next key is K+2, which is in the parent interior cell.
            //
            // In the seek() method, once we have landed in the leaf page and find that there is no cell with a key greater than K,
            // if we were to return Ok(CursorResult::Ok((None, None))), self.record would be None, which is incorrect, because we already know
            // that there is a record with a key greater than K (K' = K+2) in the parent interior cell. Hence, we need to move back up the tree
            // and get the next matching record from there.
            return self.get_next_record(Some((key, op)));
        }

        Ok(CursorResult::Ok((None, None)))
    }

    /// Move the cursor to the root page of the btree.
    fn move_to_root(&mut self) {
        let mem_page = self.pager.read_page(self.root_page).unwrap();
        self.stack.clear();
        self.stack.push(mem_page);
    }

    /// Move the cursor to the rightmost record in the btree.
    fn move_to_rightmost(&mut self) -> Result<CursorResult<()>> {
        self.move_to_root();

        loop {
            let mem_page = self.stack.top();
            let page_idx = mem_page.get().id;
            let page = self.pager.read_page(page_idx)?;
            return_if_locked!(page);
            let contents = page.get().contents.as_ref().unwrap();
            if contents.is_leaf() {
                if contents.cell_count() > 0 {
                    self.stack.set_cell_index(contents.cell_count() as i32 - 1);
                }
                return Ok(CursorResult::Ok(()));
            }

            match contents.rightmost_pointer() {
                Some(right_most_pointer) => {
                    self.stack.set_cell_index(contents.cell_count() as i32 + 1);
                    let mem_page = self.pager.read_page(right_most_pointer as usize)?;
                    self.stack.push(mem_page);
                    continue;
                }

                None => {
                    unreachable!("interior page should have a rightmost pointer");
                }
            }
        }
    }

    pub fn move_to(&mut self, key: SeekKey<'_>, cmp: SeekOp) -> Result<CursorResult<()>> {
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
        self.move_to_root();

        loop {
            let page = self.stack.top();
            return_if_locked!(page);

            let contents = page.get().contents.as_ref().unwrap();
            if contents.is_leaf() {
                return Ok(CursorResult::Ok(()));
            }

            let mut found_cell = false;
            for cell_idx in 0..contents.cell_count() {
                match &contents.cell_get(
                    cell_idx,
                    self.pager.clone(),
                    self.payload_overflow_threshold_max(contents.page_type()),
                    self.payload_overflow_threshold_min(contents.page_type()),
                    self.usable_space(),
                )? {
                    BTreeCell::TableInteriorCell(TableInteriorCell {
                        _left_child_page,
                        _rowid,
                    }) => {
                        let SeekKey::TableRowId(rowid_key) = key else {
                            unreachable!("table seek key should be a rowid");
                        };
                        let target_leaf_page_is_in_left_subtree = match cmp {
                            SeekOp::GT => rowid_key < *_rowid,
                            SeekOp::GE => rowid_key <= *_rowid,
                            SeekOp::EQ => rowid_key <= *_rowid,
                        };
                        self.stack.advance();
                        if target_leaf_page_is_in_left_subtree {
                            let mem_page = self.pager.read_page(*_left_child_page as usize)?;
                            self.stack.push(mem_page);
                            found_cell = true;
                            break;
                        }
                    }
                    BTreeCell::TableLeafCell(TableLeafCell {
                        _rowid: _,
                        _payload: _,
                        first_overflow_page: _,
                    }) => {
                        unreachable!(
                            "we don't iterate leaf cells while trying to move to a leaf cell"
                        );
                    }
                    BTreeCell::IndexInteriorCell(IndexInteriorCell {
                        left_child_page,
                        payload,
                        ..
                    }) => {
                        let SeekKey::IndexKey(index_key) = key else {
                            unreachable!("index seek key should be a record");
                        };
                        let record = crate::storage::sqlite3_ondisk::read_record(payload)?;
                        let target_leaf_page_is_in_the_left_subtree = match cmp {
                            SeekOp::GT => index_key < &record,
                            SeekOp::GE => index_key <= &record,
                            SeekOp::EQ => index_key <= &record,
                        };
                        if target_leaf_page_is_in_the_left_subtree {
                            // we don't advance in case of index tree internal nodes because we will visit this node going up
                            let mem_page = self.pager.read_page(*left_child_page as usize)?;
                            self.stack.push(mem_page);
                            found_cell = true;
                            break;
                        } else {
                            self.stack.advance();
                        }
                    }
                    BTreeCell::IndexLeafCell(_) => {
                        unreachable!(
                            "we don't iterate leaf cells while trying to move to a leaf cell"
                        );
                    }
                }
            }

            if !found_cell {
                match contents.rightmost_pointer() {
                    Some(right_most_pointer) => {
                        self.stack.advance();
                        let mem_page = self.pager.read_page(right_most_pointer as usize)?;
                        self.stack.push(mem_page);
                        continue;
                    }
                    None => {
                        unreachable!("we shall not go back up! The only way is down the slope");
                    }
                }
            }
        }
    }

    /// Insert a record into the btree.
    /// If the insert operation overflows the page, it will be split and the btree will be balanced.
    fn insert_into_page(&mut self, key: &OwnedValue, record: &Record) -> Result<CursorResult<()>> {
        if let CursorState::None = &self.state {
            self.state = CursorState::Write(WriteInfo::new());
        }
        let ret = loop {
            let write_state = {
                let write_info = self
                    .state
                    .mut_write_info()
                    .expect("can't insert while counting");
                write_info.state.clone()
            };
            match write_state {
                WriteState::Start => {
                    let page = self.stack.top();
                    let int_key = match key {
                        OwnedValue::Integer(i) => *i as u64,
                        _ => unreachable!("btree tables are indexed by integers!"),
                    };

                    // get page and find cell
                    let (cell_idx, page_type) = {
                        return_if_locked!(page);

                        page.set_dirty();
                        self.pager.add_dirty(page.get().id);

                        let page = page.get().contents.as_mut().unwrap();
                        assert!(matches!(page.page_type(), PageType::TableLeaf));

                        // find cell
                        (self.find_cell(page, int_key), page.page_type())
                    };

                    // TODO: if overwrite drop cell

                    // insert cell
                    let mut cell_payload: Vec<u8> = Vec::new();
                    self.fill_cell_payload(page_type, Some(int_key), &mut cell_payload, record);

                    // insert
                    let overflow = {
                        let contents = page.get().contents.as_mut().unwrap();
                        self.insert_into_cell(contents, cell_payload.as_slice(), cell_idx);
                        let overflow_cells = contents.overflow_cells.len();
                        debug!(
                            "insert_into_page(overflow, cell_count={}, overflow_cells={})",
                            contents.cell_count(),
                            overflow_cells
                        );
                        overflow_cells
                    };
                    let write_info = self
                        .state
                        .mut_write_info()
                        .expect("can't count while inserting");
                    if overflow > 0 {
                        write_info.state = WriteState::BalanceStart;
                    } else {
                        write_info.state = WriteState::Finish;
                    }
                }
                WriteState::BalanceStart
                | WriteState::BalanceNonRoot
                | WriteState::BalanceMoveUp
                | WriteState::BalanceGetParentPage => {
                    return_if_io!(self.balance());
                }
                WriteState::Finish => {
                    break Ok(CursorResult::Ok(()));
                }
            };
        };
        self.state = CursorState::None;
        return ret;
    }

    /// Insert a record into a cell.
    /// If the cell overflows, an overflow cell is created.
    /// insert_into_cell() is called from insert_into_page(),
    /// and the overflow cell count is used to determine if the page overflows,
    /// i.e. whether we need to balance the btree after the insert.
    fn insert_into_cell(&self, page: &mut PageContent, payload: &[u8], cell_idx: usize) {
        let free = self.compute_free_space(page, RefCell::borrow(&self.pager.db_header));
        const CELL_POINTER_SIZE_BYTES: usize = 2;
        let enough_space = payload.len() + CELL_POINTER_SIZE_BYTES <= free as usize;
        if !enough_space {
            // add to overflow cells
            page.overflow_cells.push(OverflowCell {
                index: cell_idx,
                payload: Pin::new(Vec::from(payload)),
            });
            return;
        }

        // TODO: insert into cell payload in internal page
        let new_cell_data_pointer = self
            .allocate_cell_space(page, payload.len() as u16)
            .unwrap();
        let buf = page.as_ptr();

        // Copy cell data
        buf[new_cell_data_pointer as usize..new_cell_data_pointer as usize + payload.len()]
            .copy_from_slice(payload);
        //  memmove(pIns+2, pIns, 2*(pPage->nCell - i));
        let (cell_pointer_array_start, _) = page.cell_pointer_array_offset_and_size();
        let cell_pointer_cur_idx = cell_pointer_array_start + (CELL_POINTER_SIZE_BYTES * cell_idx);

        let cell_count = page.cell_count();

        // Move existing pointers if needed
        let n_bytes_forward = CELL_POINTER_SIZE_BYTES * (cell_count - cell_idx);
        if n_bytes_forward > 0 {
            buf.copy_within(
                cell_pointer_cur_idx..cell_pointer_cur_idx + n_bytes_forward,
                cell_pointer_cur_idx + CELL_POINTER_SIZE_BYTES,
            );
        }

        // Insert new cell pointer at the current cell index
        page.write_u16(cell_pointer_cur_idx - page.offset, new_cell_data_pointer);

        // Update cell count
        let new_n_cells = (page.cell_count() + 1) as u16;
        page.write_u16(PAGE_HEADER_OFFSET_CELL_COUNT, new_n_cells);
    }

    /// Free the range of bytes that a cell occupies.
    /// This function also updates the freeblock list in the page.
    /// Freeblocks are used to keep track of free space in the page,
    /// and are organized as a linked list.
    fn free_cell_range(&self, page: &mut PageContent, offset: u16, len: u16) -> Result<()> {
        let mut cell_block_end = offset as u32 + len as u32;
        let mut fragments_reduced = 0;
        let mut cell_length = len;
        let mut cell_block_start = offset;
        let mut next_free_block_ptr = PAGE_HEADER_OFFSET_FIRST_FREEBLOCK as u16;

        let usable_size = {
            let db_header = self.pager.db_header.borrow();
            (db_header.page_size - db_header.reserved_space as u16) as u32
        };

        assert!(
            cell_block_end <= usable_size,
            "cell block end is out of bounds"
        );
        assert!(cell_block_start >= 4, "minimum cell size is 4");
        assert!(
            cell_block_start <= self.usable_space() as u16 - 4,
            "offset is out of page bounds"
        );

        // Check for empty freelist fast path
        let mut next_free_block = if page.read_u8(next_free_block_ptr as usize + 1) == 0
            && page.read_u8(next_free_block_ptr as usize) == 0
        {
            0 // Fast path for empty freelist
        } else {
            // Find position in free list
            let mut block = page.read_u16(next_free_block_ptr as usize);
            while block != 0 && block < cell_block_start {
                if block <= next_free_block_ptr {
                    if block == 0 {
                        break; // Handle corruption test case
                    }
                    return Err(LimboError::Corrupt("Free block list not ascending".into()));
                }
                next_free_block_ptr = block;
                block = page.read_u16(block as usize);
            }
            block
        };

        if next_free_block as u32 > usable_size - 4 {
            return Err(LimboError::Corrupt("Free block beyond usable space".into()));
        }

        // Coalesce with next block if adjacent
        if next_free_block != 0 && cell_block_end + 3 >= next_free_block as u32 {
            fragments_reduced = (next_free_block as u32 - cell_block_end) as u8;
            if cell_block_end > next_free_block as u32 {
                return Err(LimboError::Corrupt("Invalid block overlap".into()));
            }

            let next_block_size = page.read_u16(next_free_block as usize + 2) as u32;
            cell_block_end = next_free_block as u32 + next_block_size;
            if cell_block_end > usable_size {
                return Err(LimboError::Corrupt(
                    "Coalesced block extends beyond page".into(),
                ));
            }

            cell_length = cell_block_end as u16 - cell_block_start;
            next_free_block = page.read_u16(next_free_block as usize);
        }

        // Coalesce with previous block if adjacent
        if next_free_block_ptr > PAGE_HEADER_OFFSET_FIRST_FREEBLOCK as u16 {
            let prev_block_end =
                next_free_block_ptr as u32 + page.read_u16(next_free_block_ptr as usize + 2) as u32;

            if prev_block_end + 3 >= cell_block_start as u32 {
                if prev_block_end > cell_block_start as u32 {
                    return Err(LimboError::Corrupt("Invalid previous block overlap".into()));
                }
                fragments_reduced += (cell_block_start as u32 - prev_block_end) as u8;
                cell_length = (cell_block_end - next_free_block_ptr as u32) as u16;
                cell_block_start = next_free_block_ptr;
            }
        }

        // Update frag count
        let current_frags = page.read_u8(PAGE_HEADER_OFFSET_FRAGMENTED_BYTES_COUNT);
        if fragments_reduced > current_frags {
            return Err(LimboError::Corrupt("Invalid fragmentation count".into()));
        }
        page.write_u8(
            PAGE_HEADER_OFFSET_FRAGMENTED_BYTES_COUNT,
            current_frags - fragments_reduced,
        );

        let content_area_start = page.cell_content_area();
        if cell_block_start <= content_area_start {
            if cell_block_start < content_area_start {
                return Err(LimboError::Corrupt("Free block before content area".into()));
            }
            if next_free_block_ptr != PAGE_HEADER_OFFSET_FIRST_FREEBLOCK as u16 {
                return Err(LimboError::Corrupt("Invalid content area merge".into()));
            }
            // Extend content area
            page.write_u16(PAGE_HEADER_OFFSET_FIRST_FREEBLOCK, next_free_block);
            page.write_u16(PAGE_HEADER_OFFSET_CELL_CONTENT_AREA, cell_block_end as u16);
        } else {
            // Insert in free list
            page.write_u16(next_free_block_ptr as usize, cell_block_start);
            page.write_u16(cell_block_start as usize, next_free_block);
            page.write_u16(cell_block_start as usize + 2, cell_length);
        }

        Ok(())
    }

    /// Drop a cell from a page.
    /// This is done by freeing the range of bytes that the cell occupies.
    fn drop_cell(&self, page: &mut PageContent, cell_idx: usize) {
        let cell_count = page.cell_count();
        let (cell_start, cell_len) = page.cell_get_raw_region(
            cell_idx,
            self.payload_overflow_threshold_max(page.page_type()),
            self.payload_overflow_threshold_min(page.page_type()),
            self.usable_space(),
        );

        self.free_cell_range(page, cell_start as u16, cell_len as u16)
            .expect("Failed to free cell range");

        let new_cell_count = cell_count - 1;
        page.write_u16(PAGE_HEADER_OFFSET_CELL_COUNT, new_cell_count as u16);

        if new_cell_count == 0 {
            page.write_u16(PAGE_HEADER_OFFSET_FIRST_FREEBLOCK, 0);
            page.write_u8(PAGE_HEADER_OFFSET_FRAGMENTED_BYTES_COUNT, 0);
            page.write_u16(
                PAGE_HEADER_OFFSET_CELL_CONTENT_AREA,
                self.usable_space() as u16,
            );
        } else {
            let (pointer_array_start, _) = page.cell_pointer_array_offset_and_size();
            let buf = page.as_ptr();
            buf.copy_within(
                pointer_array_start + (2 * (cell_idx + 1))  // src
                    ..pointer_array_start + (2 * cell_count),
                pointer_array_start + (2 * cell_idx), // dst
            );
        }
    }

    fn find_free_cell(
        &self,
        page_ref: &PageContent,
        amount: usize,
        db_header: Ref<DatabaseHeader>,
    ) -> Result<usize> {
        // NOTE: freelist is in ascending order of keys and pc
        // unused_space is reserved bytes at the end of page, therefore we must subtract from maxpc
        let mut free_list_pointer_addr = 1;
        let mut pc = page_ref.first_freeblock() as usize;

        let usable_space = (db_header.page_size - db_header.reserved_space as u16) as usize;
        let maxpc = usable_space - amount;

        if pc == 0 {
            return Ok(0);
        }

        while pc <= maxpc {
            let size = page_ref.read_u16(pc + 2) as usize;

            if let Some(x) = size.checked_sub(amount) {
                if x < 4 {
                    if page_ref.read_u8(PAGE_HEADER_OFFSET_FRAGMENTED_BYTES_COUNT) > 57 {
                        return Ok(0);
                    }

                    let next_ptr = page_ref.read_u16(pc);
                    page_ref.write_u16(free_list_pointer_addr, next_ptr);

                    let frag_count = page_ref.read_u8(PAGE_HEADER_OFFSET_FRAGMENTED_BYTES_COUNT);
                    page_ref.write_u8(
                        PAGE_HEADER_OFFSET_FRAGMENTED_BYTES_COUNT,
                        frag_count + x as u8,
                    );
                    return Ok(pc);
                } else if x + pc > maxpc {
                    return Err(LimboError::Corrupt("Free block extends beyond page".into()));
                } else {
                    page_ref.write_u16(pc + 2, x as u16);
                    return Ok(pc + x);
                }
            }

            free_list_pointer_addr = pc;
            pc = page_ref.read_u16(pc) as usize;
            if pc <= free_list_pointer_addr && pc != 0 {
                return Err(LimboError::Corrupt(
                    "Free list not in ascending order".into(),
                ));
            }
        }

        if pc > maxpc + amount - 4 {
            return Err(LimboError::Corrupt(
                "Free block chain extends beyond page end".into(),
            ));
        }
        Ok(0)
    }

    /// Balance a leaf page.
    /// Balancing is done when a page overflows.
    /// see e.g. https://en.wikipedia.org/wiki/B-tree
    ///
    /// This is a naive algorithm that doesn't try to distribute cells evenly by content.
    /// It will try to split the page in half by keys not by content.
    /// Sqlite tries to have a page at least 40% full.
    fn balance(&mut self) -> Result<CursorResult<()>> {
        assert!(
            matches!(self.state, CursorState::Write(_)),
            "Cursor must be in balancing state"
        );
        loop {
            let state = self.state.write_info().expect("must be balancing").state;
            match state {
                WriteState::BalanceStart => {
                    let current_page = self.stack.top();
                    {
                        // check if we don't need to balance
                        // don't continue if there are no overflow cells
                        let page = current_page.get().contents.as_mut().unwrap();
                        if page.overflow_cells.is_empty() {
                            let write_info = self.state.mut_write_info().unwrap();
                            write_info.state = WriteState::Finish;
                            return Ok(CursorResult::Ok(()));
                        }
                    }

                    if !self.stack.has_parent() {
                        self.balance_root();
                        return Ok(CursorResult::Ok(()));
                    }

                    let write_info = self.state.mut_write_info().unwrap();
                    write_info.state = WriteState::BalanceNonRoot;
                }
                WriteState::BalanceNonRoot
                | WriteState::BalanceGetParentPage
                | WriteState::BalanceMoveUp => {
                    return_if_io!(self.balance_non_root());
                }

                _ => unreachable!("invalid balance leaf state {:?}", state),
            }
        }
    }

    fn balance_non_root(&mut self) -> Result<CursorResult<()>> {
        assert!(
            matches!(self.state, CursorState::Write(_)),
            "Cursor must be in balancing state"
        );
        let state = self.state.write_info().expect("must be balancing").state;
        let (next_write_state, result) = match state {
            WriteState::Start => todo!(),
            WriteState::BalanceStart => todo!(),
            WriteState::BalanceNonRoot => {
                // drop divider cells and find right pointer
                // NOTE: since we are doing a simple split we only finding the pointer we want to update (right pointer).
                // Right pointer means cell that points to the last page, as we don't really want to drop this one. This one
                // can be a "rightmost pointer" or a "cell".
                // we always asumme there is a parent
                let current_page = self.stack.top();
                debug!("balance_non_root(page={})", current_page.get().id);

                let current_page_inner = current_page.get();
                let current_page_contents = &mut current_page_inner.contents;
                let current_page_contents = current_page_contents.as_mut().unwrap();
                // Copy of page used to reference cell bytes.
                // This needs to be saved somewhere safe so that references still point to here,
                // this will be store in write_info below
                let page_copy = current_page_contents.clone();
                current_page_contents.overflow_cells.clear();

                // In memory in order copy of all cells in pages we want to balance. For now let's do a 2 page split.
                // Right pointer in interior cells should be converted to regular cells if more than 2 pages are used for balancing.
                let write_info = self.state.write_info().unwrap();
                let mut scratch_cells = write_info.scratch_cells.borrow_mut();
                scratch_cells.clear();

                let usable_space = self.usable_space();
                for cell_idx in 0..page_copy.cell_count() {
                    let (start, len) = page_copy.cell_get_raw_region(
                        cell_idx,
                        self.payload_overflow_threshold_max(page_copy.page_type()),
                        self.payload_overflow_threshold_min(page_copy.page_type()),
                        usable_space,
                    );
                    let cell_buffer = to_static_buf(&page_copy.as_ptr()[start..start + len]);
                    scratch_cells.push(cell_buffer);
                }
                // overflow_cells are stored in order - so we need to insert them in reverse order
                for cell in page_copy.overflow_cells.iter().rev() {
                    scratch_cells.insert(cell.index, to_static_buf(&cell.payload));
                }

                // amount of cells for pages involved in split
                // the algorithm accumulate cells in greedy manner with 2 conditions for split:
                // 1. new cell will overflow single cell (accumulated + new > usable_space - header_size)
                // 2. accumulated size already reach >50% of content_usable_size
                // second condition is necessary, otherwise in case of small cells we will create a lot of almost empty pages
                //
                // if we have single overflow cell in a table leaf node - we still can have 3 split pages
                //
                // for example, if current page has 4 entries with size ~1/4 page size, and new cell has size ~page size
                // then we will need 3 pages to distribute cells between them
                let split_pages_cells_count = &mut write_info.split_pages_cells_count.borrow_mut();
                split_pages_cells_count.clear();
                let mut last_page_cells_count = 0;
                let mut last_page_cells_size = 0;
                let content_usable_space = usable_space - page_copy.header_size();
                for scratch_cell in scratch_cells.iter() {
                    let cell_size = scratch_cell.len() + 2; // + cell pointer size (u16)
                    if last_page_cells_size + cell_size > content_usable_space
                        || 2 * last_page_cells_size > content_usable_space
                    {
                        split_pages_cells_count.push(last_page_cells_count);
                        last_page_cells_count = 0;
                        last_page_cells_size = 0;
                    }
                    last_page_cells_count += 1;
                    last_page_cells_size += cell_size;
                    assert!(last_page_cells_size <= content_usable_space);
                }
                split_pages_cells_count.push(last_page_cells_count);
                let new_pages_count = split_pages_cells_count.len();

                debug!(
                    "splitting left={} new_pages={}, cells_count={:?}",
                    current_page.get().id,
                    new_pages_count - 1,
                    split_pages_cells_count
                );

                *write_info.rightmost_pointer.borrow_mut() = page_copy.rightmost_pointer();
                write_info.page_copy.replace(Some(page_copy));

                let page = current_page.get().contents.as_mut().unwrap();
                let page_type = page.page_type();
                assert!(
                    matches!(page_type, PageType::TableLeaf | PageType::TableInterior),
                    "indexes still not supported"
                );

                write_info.split_pages.borrow_mut().clear();
                write_info.split_pages.borrow_mut().push(current_page);
                // allocate new pages
                for _ in 1..new_pages_count {
                    let new_page = self.allocate_page(page_type, 0);
                    write_info.split_pages.borrow_mut().push(new_page);
                }

                (WriteState::BalanceGetParentPage, Ok(CursorResult::Ok(())))
            }
            WriteState::BalanceGetParentPage => {
                let parent = self.stack.parent();
                let loaded = parent.is_loaded();
                return_if_locked!(parent);

                if !loaded {
                    debug!("balance_leaf(loading page)");
                    self.pager.load_page(parent.clone())?;
                    return Ok(CursorResult::IO);
                }
                parent.set_dirty();
                (WriteState::BalanceMoveUp, Ok(CursorResult::Ok(())))
            }
            WriteState::BalanceMoveUp => {
                let parent = self.stack.parent();

                let (page_type, current_idx) = {
                    let current_page = self.stack.top();
                    let contents = current_page.get().contents.as_ref().unwrap();
                    (contents.page_type().clone(), current_page.get().id)
                };

                parent.set_dirty();
                self.pager.add_dirty(parent.get().id);
                let parent_contents = parent.get().contents.as_mut().unwrap();
                // if this isn't empty next loop won't work
                assert_eq!(parent_contents.overflow_cells.len(), 0);

                // Right page pointer is u32 in right most pointer, and in cell is u32 too, so we can use a *u32 to hold where we want to change this value
                let mut right_pointer = PAGE_HEADER_OFFSET_RIGHTMOST_PTR;
                for cell_idx in 0..parent_contents.cell_count() {
                    let cell = parent_contents.cell_get(
                        cell_idx,
                        self.pager.clone(),
                        self.payload_overflow_threshold_max(page_type.clone()),
                        self.payload_overflow_threshold_min(page_type.clone()),
                        self.usable_space(),
                    )?;
                    let found = match cell {
                        BTreeCell::TableInteriorCell(interior) => {
                            interior._left_child_page as usize == current_idx
                        }
                        _ => unreachable!("Parent should always be an interior page"),
                    };
                    if found {
                        let (start, _len) = parent_contents.cell_get_raw_region(
                            cell_idx,
                            self.payload_overflow_threshold_max(page_type.clone()),
                            self.payload_overflow_threshold_min(page_type.clone()),
                            self.usable_space(),
                        );
                        right_pointer = start;
                        break;
                    }
                }

                let write_info = self.state.write_info().unwrap();
                let mut split_pages = write_info.split_pages.borrow_mut();
                let split_pages_len = split_pages.len();
                let scratch_cells = write_info.scratch_cells.borrow();

                // reset pages
                for page in split_pages.iter() {
                    assert!(page.is_dirty());
                    let contents = page.get().contents.as_mut().unwrap();

                    contents.write_u16(PAGE_HEADER_OFFSET_FIRST_FREEBLOCK, 0);
                    contents.write_u16(PAGE_HEADER_OFFSET_CELL_COUNT, 0);

                    contents.write_u16(
                        PAGE_HEADER_OFFSET_CELL_CONTENT_AREA,
                        self.usable_space() as u16,
                    );

                    contents.write_u8(PAGE_HEADER_OFFSET_FRAGMENTED_BYTES_COUNT, 0);
                    if !contents.is_leaf() {
                        contents.write_u32(PAGE_HEADER_OFFSET_RIGHTMOST_PTR, 0);
                    }
                }

                let mut current_cell_index = 0_usize;
                /* index to scratch cells that will be used as dividers in order */
                let mut divider_cells_index = Vec::with_capacity(split_pages.len());

                debug!("balance_leaf::distribute(cells={})", scratch_cells.len());

                for (i, page) in split_pages.iter_mut().enumerate() {
                    let page_id = page.get().id;
                    let contents = page.get().contents.as_mut().unwrap();

                    let cells_to_copy = write_info.split_pages_cells_count.borrow()[i];
                    debug!(
                        "balance_leaf::distribute(page={}, cells_to_copy={})",
                        page_id, cells_to_copy
                    );

                    let cell_index_range = current_cell_index..current_cell_index + cells_to_copy;
                    for (j, cell_idx) in cell_index_range.enumerate() {
                        debug!("balance_leaf::distribute_in_page(page={}, cells_to_copy={}, j={}, cell_idx={})", page_id, cells_to_copy, j, cell_idx);

                        let cell = scratch_cells[cell_idx];
                        self.insert_into_cell(contents, cell, j);
                    }
                    divider_cells_index.push(current_cell_index + cells_to_copy - 1);
                    current_cell_index += cells_to_copy;
                }

                let is_leaf = {
                    let page = self.stack.top();
                    let page = page.get().contents.as_ref().unwrap();
                    page.is_leaf()
                };

                // update rightmost pointer for each page if we are in interior page
                if !is_leaf {
                    for page in split_pages.iter_mut().take(split_pages_len - 1) {
                        let contents = page.get().contents.as_mut().unwrap();

                        assert!(contents.cell_count() >= 1);
                        let last_cell = contents.cell_get(
                            contents.cell_count() - 1,
                            self.pager.clone(),
                            self.payload_overflow_threshold_max(contents.page_type()),
                            self.payload_overflow_threshold_min(contents.page_type()),
                            self.usable_space(),
                        )?;
                        let last_cell_pointer = match last_cell {
                            BTreeCell::TableInteriorCell(interior) => interior._left_child_page,
                            _ => unreachable!(),
                        };
                        self.drop_cell(contents, contents.cell_count() - 1);
                        contents.write_u32(PAGE_HEADER_OFFSET_RIGHTMOST_PTR, last_cell_pointer);
                    }
                    // last page right most pointer points to previous right most pointer before splitting
                    let last_page = split_pages.last().unwrap();
                    let last_page_contents = last_page.get().contents.as_mut().unwrap();
                    last_page_contents.write_u32(
                        PAGE_HEADER_OFFSET_RIGHTMOST_PTR,
                        write_info.rightmost_pointer.borrow().unwrap(),
                    );
                }

                // insert dividers in parent
                // we can consider dividers the first cell of each page starting from the second page
                for (page_id_index, page) in
                    split_pages.iter_mut().take(split_pages_len - 1).enumerate()
                {
                    let contents = page.get().contents.as_mut().unwrap();
                    let divider_cell_index = divider_cells_index[page_id_index];
                    let cell_payload = scratch_cells[divider_cell_index];
                    let cell = read_btree_cell(
                        cell_payload,
                        &contents.page_type(),
                        0,
                        self.pager.clone(),
                        self.payload_overflow_threshold_max(contents.page_type()),
                        self.payload_overflow_threshold_min(contents.page_type()),
                        self.usable_space(),
                    )?;

                    let key = match cell {
                        BTreeCell::TableLeafCell(TableLeafCell { _rowid, .. })
                        | BTreeCell::TableInteriorCell(TableInteriorCell { _rowid, .. }) => _rowid,
                        _ => unreachable!(),
                    };

                    let mut divider_cell = Vec::with_capacity(4 + 9); // 4 - page id, 9 - max length of varint
                    divider_cell.extend_from_slice(&(page.get().id as u32).to_be_bytes());
                    write_varint_to_vec(key, &mut divider_cell);

                    let parent_cell_idx = self.find_cell(parent_contents, key);
                    self.insert_into_cell(parent_contents, &divider_cell, parent_cell_idx);
                }

                {
                    // copy last page id to right pointer
                    let last_pointer = split_pages.last().unwrap().get().id as u32;
                    parent_contents.write_u32(right_pointer, last_pointer);
                }
                self.stack.pop();
                let _ = write_info.page_copy.take();
                (WriteState::BalanceStart, Ok(CursorResult::Ok(())))
            }
            WriteState::Finish => todo!(),
        };
        let write_info = self.state.mut_write_info().unwrap();
        write_info.state = next_write_state;
        result
    }

    /// Balance the root page.
    /// This is done when the root page overflows, and we need to create a new root page.
    /// See e.g. https://en.wikipedia.org/wiki/B-tree
    fn balance_root(&mut self) {
        /* todo: balance deeper, create child and copy contents of root there. Then split root */
        /* if we are in root page then we just need to create a new root and push key there */

        let is_page_1 = {
            let current_root = self.stack.top();
            current_root.get().id == 1
        };

        let offset = if is_page_1 { DATABASE_HEADER_SIZE } else { 0 };
        let new_root_page = self.allocate_page(PageType::TableInterior, offset);
        {
            let current_root = self.stack.top();
            let current_root_contents = current_root.get().contents.as_ref().unwrap();

            let new_root_page_contents = new_root_page.get().contents.as_mut().unwrap();
            if is_page_1 {
                // Copy header
                let current_root_buf = current_root_contents.as_ptr();
                let new_root_buf = new_root_page_contents.as_ptr();
                new_root_buf[0..DATABASE_HEADER_SIZE]
                    .copy_from_slice(&current_root_buf[0..DATABASE_HEADER_SIZE]);
            }
            // point new root right child to previous root
            new_root_page_contents.write_u32(
                PAGE_HEADER_OFFSET_RIGHTMOST_PTR,
                current_root.get().id as u32,
            );
            new_root_page_contents.write_u16(PAGE_HEADER_OFFSET_CELL_COUNT, 0);
        }

        /* swap split page buffer with new root buffer so we don't have to update page idx */
        {
            let (root_id, child_id, child) = {
                let page_ref = self.stack.top();
                let child = page_ref.clone();

                // Swap the entire Page structs
                std::mem::swap(&mut child.get().id, &mut new_root_page.get().id);
                // TODO:: shift bytes by offset to left on child because now child has offset 100
                // and header bytes
                // Also change the offset of page
                //
                if is_page_1 {
                    // Remove header from child and set offset to 0
                    let contents = child.get().contents.as_mut().unwrap();
                    let (cell_pointer_offset, _) = contents.cell_pointer_array_offset_and_size();
                    // change cell pointers
                    for cell_idx in 0..contents.cell_count() {
                        let cell_pointer_offset = cell_pointer_offset + (2 * cell_idx) - offset;
                        let pc = contents.read_u16(cell_pointer_offset);
                        contents.write_u16(cell_pointer_offset, pc - offset as u16);
                    }

                    contents.offset = 0;
                    let buf = contents.as_ptr();
                    buf.copy_within(DATABASE_HEADER_SIZE.., 0);
                }

                self.pager.add_dirty(new_root_page.get().id);
                self.pager.add_dirty(child.get().id);
                (new_root_page.get().id, child.get().id, child)
            };

            debug!("Balancing root. root={}, rightmost={}", root_id, child_id);
            let root = new_root_page.clone();

            self.root_page = root_id;
            self.stack.clear();
            self.stack.push(root.clone());
            self.stack.push(child.clone());

            self.pager.put_loaded_page(root_id, root);
            self.pager.put_loaded_page(child_id, child);
        }
    }

    /// Allocate a new page to the btree via the pager.
    /// This marks the page as dirty and writes the page header.
    fn allocate_page(&self, page_type: PageType, offset: usize) -> PageRef {
        let page = self.pager.allocate_page().unwrap();
        btree_init_page(&page, page_type, &self.pager.db_header.borrow(), offset);
        page
    }

    /// Allocate a new overflow page.
    /// This is done when a cell overflows and new space is needed.
    fn allocate_overflow_page(&self) -> PageRef {
        let page = self.pager.allocate_page().unwrap();

        // setup overflow page
        let contents = page.get().contents.as_mut().unwrap();
        let buf = contents.as_ptr();
        buf.fill(0);

        page
    }

    /// Allocate space for a cell on a page.
    fn allocate_cell_space(&self, page_ref: &mut PageContent, amount: u16) -> Result<u16> {
        let amount = amount as usize;
        let (cell_offset, _) = page_ref.cell_pointer_array_offset_and_size();
        let gap = cell_offset + 2 * page_ref.cell_count();
        let mut top = page_ref.cell_content_area() as usize;

        if page_ref.first_freeblock() != 0 && gap + 2 <= top {
            let db_header = RefCell::borrow(&self.pager.db_header);
            let pc = self.find_free_cell(page_ref, amount, db_header)?;
            if pc != 0 {
                // Corruption check
                if pc <= gap {
                    return Err(LimboError::Corrupt(
                        "Corrupted page: free block overlaps cell pointer array".into(),
                    ));
                }
                return Ok(pc as u16);
            }
        }

        if gap + 2 + amount > top {
            // defragment
            self.defragment_page(page_ref, RefCell::borrow(&self.pager.db_header));
            top = page_ref.read_u16(PAGE_HEADER_OFFSET_CELL_CONTENT_AREA) as usize;
            assert!(gap + 2 + amount <= top);
        }

        top -= amount;
        page_ref.write_u16(PAGE_HEADER_OFFSET_CELL_CONTENT_AREA, top as u16);

        let db_header = RefCell::borrow(&self.pager.db_header);
        let usable_space = (db_header.page_size - db_header.reserved_space as u16) as usize;
        assert!(top + amount <= usable_space);

        Ok(top as u16)
    }

    /// Defragment a page. This means packing all the cells to the end of the page.
    fn defragment_page(&self, page: &PageContent, db_header: Ref<DatabaseHeader>) {
        debug!("defragment_page");
        let cloned_page = page.clone();
        // TODO(pere): usable space should include offset probably
        let usable_space = (db_header.page_size - db_header.reserved_space as u16) as u64;
        let mut cbrk = usable_space;

        // TODO: implement fast algorithm

        let last_cell = usable_space - 4;
        let first_cell = cloned_page.unallocated_region_start() as u64;

        if cloned_page.cell_count() > 0 {
            let page_type = page.page_type();
            let read_buf = cloned_page.as_ptr();
            let write_buf = page.as_ptr();

            for i in 0..cloned_page.cell_count() {
                let cell_offset = page.offset + 8;
                let cell_idx = cell_offset + i * 2;

                let pc = u16::from_be_bytes([read_buf[cell_idx], read_buf[cell_idx + 1]]) as u64;
                if pc > last_cell {
                    unimplemented!("corrupted page");
                }

                assert!(pc <= last_cell);

                let size = match page_type {
                    PageType::TableInterior => {
                        let (_, nr_key) = match read_varint(&read_buf[pc as usize ..]) {
                            Ok(v) => v,
                            Err(_) => todo!(
                                "error while parsing varint from cell, probably treat this as corruption?"
                            ),
                        };
                        4 + nr_key as u64
                    }
                    PageType::TableLeaf => {
                        let (payload_size, nr_payload) = match read_varint(&read_buf[pc as usize..]) {
                            Ok(v) => v,
                            Err(_) => todo!(
                                "error while parsing varint from cell, probably treat this as corruption?"
                            ),
                        };
                        let (_, nr_key) = match read_varint(&read_buf[pc as usize + nr_payload..]) {
                            Ok(v) => v,
                            Err(_) => todo!(
                                "error while parsing varint from cell, probably treat this as corruption?"
                            ),
                        };
                        // TODO: add overflow page calculation
                        payload_size + nr_payload as u64 + nr_key as u64
                    }
                    PageType::IndexInterior => todo!(),
                    PageType::IndexLeaf => todo!(),
                };
                cbrk -= size;
                if cbrk < first_cell || pc + size > usable_space {
                    todo!("corrupt");
                }
                assert!(cbrk + size <= usable_space && cbrk >= first_cell);
                // set new pointer
                write_buf[cell_idx..cell_idx + 2].copy_from_slice(&(cbrk as u16).to_be_bytes());
                // copy payload
                write_buf[cbrk as usize..cbrk as usize + size as usize]
                    .copy_from_slice(&read_buf[pc as usize..pc as usize + size as usize]);
            }
        }

        // assert!( nfree >= 0 );
        // if( data[hdr+7]+cbrk-iCellFirst!=pPage->nFree ){
        //   return SQLITE_CORRUPT_PAGE(pPage);
        // }
        assert!(cbrk >= first_cell);
        let write_buf = page.as_ptr();

        // set new first byte of cell content
        page.write_u16(PAGE_HEADER_OFFSET_CELL_CONTENT_AREA, cbrk as u16);
        // set free block to 0, unused spaced can be retrieved from gap between cell pointer end and content start
        page.write_u16(PAGE_HEADER_OFFSET_FIRST_FREEBLOCK, 0);
        // set fragmented bytes counter to zero
        page.write_u8(PAGE_HEADER_OFFSET_FRAGMENTED_BYTES_COUNT, 0);
        // set unused space to 0
        let first_cell = cloned_page.cell_content_area() as u64;
        assert!(first_cell <= cbrk);
        write_buf[first_cell as usize..cbrk as usize].fill(0);
    }

    /// Free blocks can be zero, meaning the "real free space" that can be used to allocate is expected to be between first cell byte
    /// and end of cell pointer area.
    #[allow(unused_assignments)]
    fn compute_free_space(&self, page: &PageContent, db_header: Ref<DatabaseHeader>) -> u16 {
        // TODO(pere): maybe free space is not calculated correctly with offset

        // Usable space, not the same as free space, simply means:
        // space that is not reserved for extensions by sqlite. Usually reserved_space is 0.
        let usable_space = (db_header.page_size - db_header.reserved_space as u16) as usize;

        let mut cell_content_area_start = page.cell_content_area();
        // A zero value for the cell content area pointer is interpreted as 65536.
        // See https://www.sqlite.org/fileformat.html
        // The max page size for a sqlite database is 64kiB i.e. 65536 bytes.
        // 65536 is u16::MAX + 1, and since cell content grows from right to left, this means
        // the cell content area pointer is at the end of the page,
        // i.e.
        // 1. the page size is 64kiB
        // 2. there are no cells on the page
        // 3. there is no reserved space at the end of the page
        if cell_content_area_start == 0 {
            cell_content_area_start = u16::MAX;
        }

        // The amount of free space is the sum of:
        // #1. the size of the unallocated region
        // #2. fragments (isolated 1-3 byte chunks of free space within the cell content area)
        // #3. freeblocks (linked list of blocks of at least 4 bytes within the cell content area that are not in use due to e.g. deletions)

        let mut free_space_bytes =
            page.unallocated_region_size() + page.num_frag_free_bytes() as usize;

        // #3 is computed by iterating over the freeblocks linked list
        let mut cur_freeblock_ptr = page.first_freeblock() as usize;
        let page_buf = page.as_ptr();
        if cur_freeblock_ptr > 0 {
            if cur_freeblock_ptr < cell_content_area_start as usize {
                // Freeblocks exist in the cell content area e.g. after deletions
                // They should never exist in the unused area of the page.
                todo!("corrupted page");
            }

            let mut next = 0;
            let mut size = 0;
            loop {
                // TODO: check corruption icellast
                next = u16::from_be_bytes(
                    page_buf[cur_freeblock_ptr..cur_freeblock_ptr + 2]
                        .try_into()
                        .unwrap(),
                ) as usize; // first 2 bytes in freeblock = next freeblock pointer
                size = u16::from_be_bytes(
                    page_buf[cur_freeblock_ptr + 2..cur_freeblock_ptr + 4]
                        .try_into()
                        .unwrap(),
                ) as usize; // next 2 bytes in freeblock = size of current freeblock
                free_space_bytes += size;
                // Freeblocks are in order from left to right on the page,
                // so next pointer should > current pointer + its size, or 0 if no next block exists.
                if next <= cur_freeblock_ptr + size + 3 {
                    break;
                }
                cur_freeblock_ptr = next;
            }

            // Next should always be 0 (NULL) at this point since we have reached the end of the freeblocks linked list
            assert_eq!(
                next, 0,
                "corrupted page: freeblocks list not in ascending order"
            );

            assert!(
                cur_freeblock_ptr + size <= usable_space,
                "corrupted page: last freeblock extends last page end"
            );
        }

        assert!(
            free_space_bytes <= usable_space,
            "corrupted page: free space is greater than usable space"
        );

        // if( nFree>usableSize || nFree<iCellFirst ){
        //   return SQLITE_CORRUPT_PAGE(pPage);
        // }

        free_space_bytes as u16
    }

    /// Fill in the cell payload with the record.
    /// If the record is too large to fit in the cell, it will spill onto overflow pages.
    fn fill_cell_payload(
        &self,
        page_type: PageType,
        int_key: Option<u64>,
        cell_payload: &mut Vec<u8>,
        record: &Record,
    ) {
        assert!(matches!(
            page_type,
            PageType::TableLeaf | PageType::IndexLeaf
        ));
        // TODO: make record raw from start, having to serialize is not good
        let mut record_buf = Vec::new();
        record.serialize(&mut record_buf);

        // fill in header
        if matches!(page_type, PageType::TableLeaf) {
            let int_key = int_key.unwrap();
            write_varint_to_vec(record_buf.len() as u64, cell_payload);
            write_varint_to_vec(int_key, cell_payload);
        } else {
            write_varint_to_vec(record_buf.len() as u64, cell_payload);
        }

        let payload_overflow_threshold_max = self.payload_overflow_threshold_max(page_type.clone());
        debug!(
            "fill_cell_payload(record_size={}, payload_overflow_threshold_max={})",
            record_buf.len(),
            payload_overflow_threshold_max
        );
        if record_buf.len() <= payload_overflow_threshold_max {
            // enough allowed space to fit inside a btree page
            cell_payload.extend_from_slice(record_buf.as_slice());
            return;
        }
        debug!("fill_cell_payload(overflow)");

        let payload_overflow_threshold_min = self.payload_overflow_threshold_min(page_type);
        // see e.g. https://github.com/sqlite/sqlite/blob/9591d3fe93936533c8c3b0dc4d025ac999539e11/src/dbstat.c#L371
        let mut space_left = payload_overflow_threshold_min
            + (record_buf.len() - payload_overflow_threshold_min) % (self.usable_space() - 4);

        if space_left > payload_overflow_threshold_max {
            space_left = payload_overflow_threshold_min;
        }

        // cell_size must be equal to first value of space_left as this will be the bytes copied to non-overflow page.
        let cell_size = space_left + cell_payload.len() + 4; // 4 is the number of bytes of pointer to first overflow page
        let mut to_copy_buffer = record_buf.as_slice();

        let prev_size = cell_payload.len();
        cell_payload.resize(prev_size + space_left + 4, 0);
        let mut pointer = unsafe { cell_payload.as_mut_ptr().add(prev_size) };
        let mut pointer_to_next = unsafe { cell_payload.as_mut_ptr().add(prev_size + space_left) };
        let mut overflow_pages = Vec::new();

        loop {
            let to_copy = space_left.min(to_copy_buffer.len());
            unsafe { std::ptr::copy(to_copy_buffer.as_ptr(), pointer, to_copy) };

            let left = to_copy_buffer.len() - to_copy;
            if left == 0 {
                break;
            }

            // we still have bytes to add, we will need to allocate new overflow page
            let overflow_page = self.allocate_overflow_page();
            overflow_pages.push(overflow_page.clone());
            {
                let id = overflow_page.get().id as u32;
                let contents = overflow_page.get().contents.as_mut().unwrap();

                // TODO: take into account offset here?
                let buf = contents.as_ptr();
                let as_bytes = id.to_be_bytes();
                // update pointer to new overflow page
                unsafe { std::ptr::copy(as_bytes.as_ptr(), pointer_to_next, 4) };

                pointer = unsafe { buf.as_mut_ptr().add(4) };
                pointer_to_next = buf.as_mut_ptr();
                space_left = self.usable_space() - 4;
            }

            to_copy_buffer = &to_copy_buffer[to_copy..];
        }

        assert_eq!(cell_size, cell_payload.len());
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
    fn payload_overflow_threshold_max(&self, page_type: PageType) -> usize {
        let usable_size = self.usable_space();
        match page_type {
            PageType::IndexInterior | PageType::IndexLeaf => {
                ((usable_size - 12) * 64 / 255) - 23 // Index page formula
            }
            PageType::TableInterior | PageType::TableLeaf => {
                usable_size - 35 // Table leaf page formula
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
    fn payload_overflow_threshold_min(&self, _page_type: PageType) -> usize {
        let usable_size = self.usable_space();
        // Same formula for all page types
        ((usable_size - 12) * 32 / 255) - 23
    }

    /// The "usable size" of a database page is the page size specified by the 2-byte integer at offset 16
    /// in the header, minus the "reserved" space size recorded in the 1-byte integer at offset 20 in the header.
    /// The usable size of a page might be an odd number. However, the usable size is not allowed to be less than 480.
    /// In other words, if the page size is 512, then the reserved space size cannot exceed 32.
    fn usable_space(&self) -> usize {
        let db_header = self.pager.db_header.borrow();
        (db_header.page_size - db_header.reserved_space as u16) as usize
    }

    /// Find the index of the cell in the page that contains the given rowid.
    /// BTree tables only.
    fn find_cell(&self, page: &PageContent, int_key: u64) -> usize {
        let mut cell_idx = 0;
        let cell_count = page.cell_count();
        while cell_idx < cell_count {
            match page
                .cell_get(
                    cell_idx,
                    self.pager.clone(),
                    self.payload_overflow_threshold_max(page.page_type()),
                    self.payload_overflow_threshold_min(page.page_type()),
                    self.usable_space(),
                )
                .unwrap()
            {
                BTreeCell::TableLeafCell(cell) => {
                    if int_key <= cell._rowid {
                        break;
                    }
                }
                BTreeCell::TableInteriorCell(cell) => {
                    if int_key <= cell._rowid {
                        break;
                    }
                }
                _ => todo!(),
            }
            cell_idx += 1;
        }
        cell_idx
    }

    pub fn seek_to_last(&mut self) -> Result<CursorResult<()>> {
        return_if_io!(self.move_to_rightmost());
        let (rowid, record) = return_if_io!(self.get_next_record(None));
        if rowid.is_none() {
            let is_empty = return_if_io!(self.is_empty_table());
            assert!(is_empty);
            return Ok(CursorResult::Ok(()));
        }
        self.rowid.replace(rowid);
        self.record.replace(record);
        Ok(CursorResult::Ok(()))
    }

    pub fn is_empty(&self) -> bool {
        self.record.borrow().is_none()
    }

    pub fn root_page(&self) -> usize {
        self.root_page
    }

    pub fn rewind(&mut self) -> Result<CursorResult<()>> {
        self.move_to_root();

        let (rowid, record) = return_if_io!(self.get_next_record(None));
        self.rowid.replace(rowid);
        self.record.replace(record);
        Ok(CursorResult::Ok(()))
    }

    pub fn last(&mut self) -> Result<CursorResult<()>> {
        match self.move_to_rightmost()? {
            CursorResult::Ok(_) => self.prev(),
            CursorResult::IO => Ok(CursorResult::IO),
        }
    }

    pub fn next(&mut self) -> Result<CursorResult<()>> {
        let (rowid, record) = return_if_io!(self.get_next_record(None));
        self.rowid.replace(rowid);
        self.record.replace(record);
        Ok(CursorResult::Ok(()))
    }

    pub fn prev(&mut self) -> Result<CursorResult<()>> {
        match self.get_prev_record()? {
            CursorResult::Ok((rowid, record)) => {
                self.rowid.replace(rowid);
                self.record.replace(record);
                Ok(CursorResult::Ok(()))
            }
            CursorResult::IO => Ok(CursorResult::IO),
        }
    }

    pub fn wait_for_completion(&mut self) -> Result<()> {
        // TODO: Wait for pager I/O to complete
        Ok(())
    }

    pub fn rowid(&self) -> Result<Option<u64>> {
        Ok(*self.rowid.borrow())
    }

    pub fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> Result<CursorResult<bool>> {
        let (rowid, record) = return_if_io!(self.do_seek(key, op));
        self.rowid.replace(rowid);
        self.record.replace(record);
        Ok(CursorResult::Ok(rowid.is_some()))
    }

    pub fn record(&self) -> Result<Ref<Option<Record>>> {
        Ok(self.record.borrow())
    }

    pub fn insert(
        &mut self,
        key: &OwnedValue,
        _record: &Record,
        moved_before: bool, /* Indicate whether it's necessary to traverse to find the leaf page */
    ) -> Result<CursorResult<()>> {
        let int_key = match key {
            OwnedValue::Integer(i) => i,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        if !moved_before {
            return_if_io!(self.move_to(SeekKey::TableRowId(*int_key as u64), SeekOp::EQ));
        }

        return_if_io!(self.insert_into_page(key, _record));
        self.rowid.replace(Some(*int_key as u64));
        Ok(CursorResult::Ok(()))
    }

    pub fn delete(&mut self) -> Result<CursorResult<()>> {
        let page = self.stack.top();
        return_if_locked!(page);

        if !page.is_loaded() {
            self.pager.load_page(page.clone())?;
            return Ok(CursorResult::IO);
        }

        let target_rowid = match self.rowid.borrow().as_ref() {
            Some(rowid) => *rowid,
            None => return Ok(CursorResult::Ok(())),
        };

        let contents = page.get().contents.as_ref().unwrap();

        // TODO(Krishna): We are doing this linear search here because seek() is returning the index of previous cell.
        // And the fix is currently not very clear to me.
        // This finds the cell with matching rowid with in a page.
        let mut cell_idx = None;
        for idx in 0..contents.cell_count() {
            let cell = contents.cell_get(
                idx,
                self.pager.clone(),
                self.payload_overflow_threshold_max(contents.page_type()),
                self.payload_overflow_threshold_min(contents.page_type()),
                self.usable_space(),
            )?;

            if let BTreeCell::TableLeafCell(leaf_cell) = cell {
                if leaf_cell._rowid == target_rowid {
                    cell_idx = Some(idx);
                    break;
                }
            }
        }

        let cell_idx = match cell_idx {
            Some(idx) => idx,
            None => return Ok(CursorResult::Ok(())),
        };

        let contents = page.get().contents.as_ref().unwrap();
        let cell = contents.cell_get(
            cell_idx,
            self.pager.clone(),
            self.payload_overflow_threshold_max(contents.page_type()),
            self.payload_overflow_threshold_min(contents.page_type()),
            self.usable_space(),
        )?;

        if cell_idx >= contents.cell_count() {
            return Err(LimboError::Corrupt(format!(
                "Corrupted page: cell index {} is out of bounds for page with {} cells",
                cell_idx,
                contents.cell_count()
            )));
        }

        let original_child_pointer = match &cell {
            BTreeCell::TableInteriorCell(interior) => Some(interior._left_child_page),
            _ => None,
        };

        return_if_io!(self.clear_overflow_pages(&cell));

        let page = self.stack.top();
        return_if_locked!(page);
        if !page.is_loaded() {
            self.pager.load_page(page.clone())?;
            return Ok(CursorResult::IO);
        }

        page.set_dirty();
        self.pager.add_dirty(page.get().id);

        let contents = page.get().contents.as_mut().unwrap();

        // If this is an interior node, we need to handle deletion differently
        // For interior nodes:
        // 1. Move cursor to largest entry in left subtree
        // 2. Copy that entry to replace the one being deleted
        // 3. Delete the leaf entry
        if !contents.is_leaf() {
            // 1. Move cursor to largest entry in left subtree
            return_if_io!(self.prev());

            let leaf_page = self.stack.top();

            // 2. Copy that entry to replace the one being deleted
            let leaf_contents = leaf_page.get().contents.as_ref().unwrap();
            let leaf_cell_idx = self.stack.current_cell_index() as usize - 1;
            let predecessor_cell = leaf_contents.cell_get(
                leaf_cell_idx,
                self.pager.clone(),
                self.payload_overflow_threshold_max(leaf_contents.page_type()),
                self.payload_overflow_threshold_min(leaf_contents.page_type()),
                self.usable_space(),
            )?;

            // 3. Create an interior cell from the leaf cell
            let mut cell_payload: Vec<u8> = Vec::new();
            match predecessor_cell {
                BTreeCell::TableLeafCell(leaf_cell) => {
                    // Format: [left child page (4 bytes)][rowid varint]
                    if let Some(child_pointer) = original_child_pointer {
                        cell_payload.extend_from_slice(&child_pointer.to_be_bytes());
                        write_varint_to_vec(leaf_cell._rowid, &mut cell_payload);
                    }
                }
                _ => unreachable!("Expected table leaf cell"),
            }
            self.insert_into_cell(contents, &cell_payload, cell_idx);
            self.drop_cell(contents, cell_idx);
        } else {
            // For leaf nodes, simply remove the cell
            self.drop_cell(contents, cell_idx);
        }

        // TODO(Krishna): Implement balance after delete. I will implement after balance_nonroot is extended.
        Ok(CursorResult::Ok(()))
    }

    pub fn set_null_flag(&mut self, flag: bool) {
        self.null_flag = flag;
    }

    pub fn get_null_flag(&self) -> bool {
        self.null_flag
    }

    pub fn exists(&mut self, key: &OwnedValue) -> Result<CursorResult<bool>> {
        let int_key = match key {
            OwnedValue::Integer(i) => i,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        return_if_io!(self.move_to(SeekKey::TableRowId(*int_key as u64), SeekOp::EQ));
        let page = self.stack.top();
        // TODO(pere): request load
        return_if_locked!(page);

        let contents = page.get().contents.as_ref().unwrap();

        // find cell
        let int_key = match key {
            OwnedValue::Integer(i) => *i as u64,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        let cell_idx = self.find_cell(contents, int_key);
        if cell_idx >= contents.cell_count() {
            Ok(CursorResult::Ok(false))
        } else {
            let equals = match &contents.cell_get(
                cell_idx,
                self.pager.clone(),
                self.payload_overflow_threshold_max(contents.page_type()),
                self.payload_overflow_threshold_min(contents.page_type()),
                self.usable_space(),
            )? {
                BTreeCell::TableLeafCell(l) => l._rowid == int_key,
                _ => unreachable!(),
            };
            Ok(CursorResult::Ok(equals))
        }
    }

    pub fn btree_create(&mut self, flags: usize) -> u32 {
        let page_type = match flags {
            1 => PageType::TableLeaf,
            2 => PageType::IndexLeaf,
            _ => unreachable!(
                "wrong create table flags, should be 1 for table and 2 for index, got {}",
                flags,
            ),
        };
        let page = self.allocate_page(page_type, 0);
        let id = page.get().id;
        id as u32
    }

    fn clear_overflow_pages(&self, cell: &BTreeCell) -> Result<CursorResult<()>> {
        // Get overflow info based on cell type
        let (first_overflow_page, n_overflow) = match cell {
            BTreeCell::TableLeafCell(leaf_cell) => {
                match self.calculate_overflow_info(leaf_cell._payload.len(), PageType::TableLeaf)? {
                    Some(n_overflow) => (leaf_cell.first_overflow_page, n_overflow),
                    None => return Ok(CursorResult::Ok(())),
                }
            }
            BTreeCell::IndexLeafCell(leaf_cell) => {
                match self.calculate_overflow_info(leaf_cell.payload.len(), PageType::IndexLeaf)? {
                    Some(n_overflow) => (leaf_cell.first_overflow_page, n_overflow),
                    None => return Ok(CursorResult::Ok(())),
                }
            }
            BTreeCell::IndexInteriorCell(interior_cell) => {
                match self
                    .calculate_overflow_info(interior_cell.payload.len(), PageType::IndexInterior)?
                {
                    Some(n_overflow) => (interior_cell.first_overflow_page, n_overflow),
                    None => return Ok(CursorResult::Ok(())),
                }
            }
            BTreeCell::TableInteriorCell(_) => return Ok(CursorResult::Ok(())), // No overflow pages
        };

        let Some(first_page) = first_overflow_page else {
            return Ok(CursorResult::Ok(()));
        };
        let page_count = self.pager.db_header.borrow().database_size as usize;
        let mut pages_left = n_overflow;
        let mut current_page = first_page;
        // Clear overflow pages
        while pages_left > 0 {
            pages_left -= 1;

            // Validate overflow page number
            if current_page < 2 || current_page as usize > page_count {
                return Err(LimboError::Corrupt("Invalid overflow page number".into()));
            }

            let page = self.pager.read_page(current_page as usize)?;
            return_if_locked!(page);
            let contents = page.get().contents.as_ref().unwrap();

            let next_page = if pages_left > 0 {
                contents.read_u32(0)
            } else {
                0
            };

            // Free the current page
            self.pager.free_page(Some(page), current_page as usize)?;

            current_page = next_page;
        }
        Ok(CursorResult::Ok(()))
    }

    fn calculate_overflow_info(
        &self,
        payload_len: usize,
        page_type: PageType,
    ) -> Result<Option<usize>> {
        let max_local = self.payload_overflow_threshold_max(page_type.clone());
        let min_local = self.payload_overflow_threshold_min(page_type.clone());
        let usable_size = self.usable_space();

        let (_, local_size) = payload_overflows(payload_len, max_local, min_local, usable_size);

        assert!(
            local_size != payload_len,
            "Trying to clear overflow pages when there are no overflow pages"
        );

        // Calculate expected overflow pages
        let overflow_page_size = self.usable_space() - 4;
        let n_overflow =
            (payload_len - local_size + overflow_page_size).div_ceil(overflow_page_size);
        if n_overflow == 0 {
            return Err(LimboError::Corrupt("Invalid overflow calculation".into()));
        }

        Ok(Some(n_overflow))
    }
}

impl PageStack {
    /// Push a new page onto the stack.
    /// This effectively means traversing to a child page.
    fn push(&self, page: PageRef) {
        debug!(
            "pagestack::push(current={}, new_page_id={})",
            self.current_page.borrow(),
            page.get().id
        );
        *self.current_page.borrow_mut() += 1;
        let current = *self.current_page.borrow();
        assert!(
            current < BTCURSOR_MAX_DEPTH as i32,
            "corrupted database, stack is bigger than expected"
        );
        self.stack.borrow_mut()[current as usize] = Some(page);
        self.cell_indices.borrow_mut()[current as usize] = 0;
    }

    /// Pop a page off the stack.
    /// This effectively means traversing back up to a parent page.
    fn pop(&self) {
        let current = *self.current_page.borrow();
        debug!("pagestack::pop(current={})", current);
        self.cell_indices.borrow_mut()[current as usize] = 0;
        self.stack.borrow_mut()[current as usize] = None;
        *self.current_page.borrow_mut() -= 1;
    }

    /// Get the top page on the stack.
    /// This is the page that is currently being traversed.
    fn top(&self) -> PageRef {
        let current = *self.current_page.borrow();
        let page = self.stack.borrow()[current as usize]
            .as_ref()
            .unwrap()
            .clone();
        debug!(
            "pagestack::top(current={}, page_id={})",
            current,
            page.get().id
        );
        page
    }

    /// Get the parent page of the current page.
    fn parent(&self) -> PageRef {
        let current = *self.current_page.borrow();
        self.stack.borrow()[current as usize - 1]
            .as_ref()
            .unwrap()
            .clone()
    }

    /// Current page pointer being used
    fn current(&self) -> usize {
        *self.current_page.borrow() as usize
    }

    /// Cell index of the current page
    fn current_cell_index(&self) -> i32 {
        let current = self.current();
        self.cell_indices.borrow()[current]
    }

    /// Check if the current cell index is less than 0.
    /// This means we have been iterating backwards and have reached the start of the page.
    fn current_cell_index_less_than_min(&self) -> bool {
        let cell_idx = self.current_cell_index();
        cell_idx < 0
    }

    /// Advance the current cell index of the current page to the next cell.
    fn advance(&self) {
        let current = self.current();
        self.cell_indices.borrow_mut()[current] += 1;
    }

    fn retreat(&self) {
        let current = self.current();
        self.cell_indices.borrow_mut()[current] -= 1;
    }

    fn set_cell_index(&self, idx: i32) {
        let current = self.current();
        self.cell_indices.borrow_mut()[current] = idx
    }

    fn has_parent(&self) -> bool {
        *self.current_page.borrow() > 0
    }

    fn clear(&self) {
        *self.current_page.borrow_mut() = -1;
    }
}

pub fn btree_init_page(
    page: &PageRef,
    page_type: PageType,
    db_header: &DatabaseHeader,
    offset: usize,
) {
    // setup btree page
    let contents = page.get();
    debug!("btree_init_page(id={}, offset={})", contents.id, offset);
    let contents = contents.contents.as_mut().unwrap();
    contents.offset = offset;
    let id = page_type as u8;
    contents.write_u8(PAGE_HEADER_OFFSET_PAGE_TYPE, id);
    contents.write_u16(PAGE_HEADER_OFFSET_FIRST_FREEBLOCK, 0);
    contents.write_u16(PAGE_HEADER_OFFSET_CELL_COUNT, 0);

    let cell_content_area_start = db_header.page_size - db_header.reserved_space as u16;
    contents.write_u16(
        PAGE_HEADER_OFFSET_CELL_CONTENT_AREA,
        cell_content_area_start,
    );

    contents.write_u8(PAGE_HEADER_OFFSET_FRAGMENTED_BYTES_COUNT, 0);
    contents.write_u32(PAGE_HEADER_OFFSET_RIGHTMOST_PTR, 0);
}

fn to_static_buf(buf: &[u8]) -> &'static [u8] {
    unsafe { std::mem::transmute::<&[u8], &'static [u8]>(buf) }
}

#[cfg(test)]
mod tests {
    use rand_chacha::rand_core::RngCore;
    use rand_chacha::rand_core::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    use super::*;
    use crate::io::{Buffer, Completion, MemoryIO, OpenFlags, IO};
    use crate::storage::database::FileStorage;
    use crate::storage::page_cache::DumbLruPageCache;
    use crate::storage::sqlite3_ondisk;
    use crate::{BufferPool, DatabaseStorage, WalFile, WalFileShared, WriteCompletion};
    use std::cell::RefCell;
    use std::sync::Arc;

    fn validate_btree(pager: Rc<Pager>, page_idx: usize) -> (usize, bool) {
        let cursor = BTreeCursor::new(pager.clone(), page_idx);
        let page = pager.read_page(page_idx).unwrap();
        let page = page.get();
        let contents = page.contents.as_ref().unwrap();
        let page_type = contents.page_type();
        let mut previous_key = None;
        let mut valid = true;
        let mut depth = None;
        for cell_idx in 0..contents.cell_count() {
            let cell = contents
                .cell_get(
                    cell_idx,
                    pager.clone(),
                    cursor.payload_overflow_threshold_max(page_type),
                    cursor.payload_overflow_threshold_min(page_type),
                    cursor.usable_space(),
                )
                .unwrap();
            let current_depth = match cell {
                BTreeCell::TableLeafCell(..) => 1,
                BTreeCell::TableInteriorCell(TableInteriorCell {
                    _left_child_page, ..
                }) => {
                    let (child_depth, child_valid) =
                        validate_btree(pager.clone(), _left_child_page as usize);
                    valid &= child_valid;
                    child_depth
                }
                _ => panic!("unsupported btree cell: {:?}", cell),
            };
            depth = Some(depth.unwrap_or(current_depth + 1));
            if depth != Some(current_depth + 1) {
                log::error!("depth is different for child of page {}", page_idx);
                valid = false;
            }
            match cell {
                BTreeCell::TableInteriorCell(TableInteriorCell { _rowid, .. })
                | BTreeCell::TableLeafCell(TableLeafCell { _rowid, .. }) => {
                    if previous_key.is_some() && previous_key.unwrap() >= _rowid {
                        log::error!(
                            "keys are in bad order: prev={:?}, current={}",
                            previous_key,
                            _rowid
                        );
                        valid = false;
                    }
                    previous_key = Some(_rowid);
                }
                _ => panic!("unsupported btree cell: {:?}", cell),
            }
        }
        if let Some(right) = contents.rightmost_pointer() {
            let (right_depth, right_valid) = validate_btree(pager.clone(), right as usize);
            valid &= right_valid;
            depth = Some(depth.unwrap_or(right_depth + 1));
            if depth != Some(right_depth + 1) {
                log::error!("depth is different for child of page {}", page_idx);
                valid = false;
            }
        }
        (depth.unwrap(), valid)
    }

    fn format_btree(pager: Rc<Pager>, page_idx: usize, depth: usize) -> String {
        let cursor = BTreeCursor::new(pager.clone(), page_idx);
        let page = pager.read_page(page_idx).unwrap();
        let page = page.get();
        let contents = page.contents.as_ref().unwrap();
        let page_type = contents.page_type();
        let mut current = Vec::new();
        let mut child = Vec::new();
        for cell_idx in 0..contents.cell_count() {
            let cell = contents
                .cell_get(
                    cell_idx,
                    pager.clone(),
                    cursor.payload_overflow_threshold_max(page_type),
                    cursor.payload_overflow_threshold_min(page_type),
                    cursor.usable_space(),
                )
                .unwrap();
            match cell {
                BTreeCell::TableInteriorCell(cell) => {
                    current.push(format!(
                        "node[rowid:{}, ptr(<=):{}]",
                        cell._rowid, cell._left_child_page
                    ));
                    child.push(format_btree(
                        pager.clone(),
                        cell._left_child_page as usize,
                        depth + 2,
                    ));
                }
                BTreeCell::TableLeafCell(cell) => {
                    current.push(format!(
                        "leaf[rowid:{}, len(payload):{}, overflow:{}]",
                        cell._rowid,
                        cell._payload.len(),
                        cell.first_overflow_page.is_some()
                    ));
                }
                _ => panic!("unsupported btree cell: {:?}", cell),
            }
        }
        if let Some(rightmost) = contents.rightmost_pointer() {
            child.push(format_btree(pager.clone(), rightmost as usize, depth + 2));
        }
        let current = format!(
            "{}-page:{}, ptr(right):{}\n{}+cells:{}",
            " ".repeat(depth),
            page_idx,
            contents.rightmost_pointer().unwrap_or(0),
            " ".repeat(depth),
            current.join(", ")
        );
        if child.is_empty() {
            current
        } else {
            current + "\n" + &child.join("\n")
        }
    }

    fn empty_btree() -> (Rc<Pager>, usize) {
        let db_header = DatabaseHeader::default();
        let page_size = db_header.page_size as usize;

        #[allow(clippy::arc_with_non_send_sync)]
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new().unwrap());
        let io_file = io.open_file("test.db", OpenFlags::Create, false).unwrap();
        let page_io = Rc::new(FileStorage::new(io_file));

        let buffer_pool = Rc::new(BufferPool::new(db_header.page_size as usize));
        let wal_shared = WalFileShared::open_shared(&io, "test.wal", db_header.page_size).unwrap();
        let wal_file = WalFile::new(io.clone(), page_size, wal_shared, buffer_pool.clone());
        let wal = Rc::new(RefCell::new(wal_file));

        let page_cache = Arc::new(parking_lot::RwLock::new(DumbLruPageCache::new(10)));
        let pager = {
            let db_header = Rc::new(RefCell::new(db_header.clone()));
            Pager::finish_open(db_header, page_io, wal, io, page_cache, buffer_pool).unwrap()
        };
        let pager = Rc::new(pager);
        let page1 = pager.allocate_page().unwrap();
        btree_init_page(&page1, PageType::TableLeaf, &db_header, 0);
        (pager, page1.get().id)
    }

    #[test]
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
            let (pager, root_page) = empty_btree();
            let mut cursor = BTreeCursor::new(pager.clone(), root_page);
            for (key, size) in sequence.iter() {
                let key = OwnedValue::Integer(*key);
                let value = Record::new(vec![OwnedValue::Blob(Rc::new(vec![0; *size]))]);
                log::info!("insert key:{}", key);
                cursor.insert(&key, &value, false).unwrap();
                log::info!(
                    "=========== btree ===========\n{}\n\n",
                    format_btree(pager.clone(), root_page, 0)
                );
            }
            for (key, _) in sequence.iter() {
                let seek_key = SeekKey::TableRowId(*key as u64);
                assert!(
                    matches!(
                        cursor.seek(seek_key, SeekOp::EQ).unwrap(),
                        CursorResult::Ok(true)
                    ),
                    "key {} is not found",
                    key
                );
            }
        }
    }

    fn rng_from_time() -> (ChaCha8Rng, u64) {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let rng = ChaCha8Rng::seed_from_u64(seed);
        (rng, seed)
    }

    fn btree_insert_fuzz_run(
        attempts: usize,
        inserts: usize,
        size: impl Fn(&mut ChaCha8Rng) -> usize,
    ) {
        let (mut rng, seed) = rng_from_time();
        log::info!("super seed: {}", seed);
        for _ in 0..attempts {
            let (pager, root_page) = empty_btree();
            let mut cursor = BTreeCursor::new(pager.clone(), root_page);
            let mut keys = Vec::new();
            let seed = rng.next_u64();
            log::info!("seed: {}", seed);
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            for insert_id in 0..inserts {
                let size = size(&mut rng);
                let key = (rng.next_u64() % (1 << 30)) as i64;
                keys.push(key);
                log::info!(
                    "INSERT INTO t VALUES ({}, randomblob({})); -- {}",
                    key,
                    size,
                    insert_id
                );
                let key = OwnedValue::Integer(key);
                let value = Record::new(vec![OwnedValue::Blob(Rc::new(vec![0; size]))]);
                cursor.insert(&key, &value, false).unwrap();
            }
            log::info!(
                "=========== btree ===========\n{}\n\n",
                format_btree(pager.clone(), root_page, 0)
            );
            if matches!(validate_btree(pager.clone(), root_page), (_, false)) {
                panic!("invalid btree");
            }
            for key in keys.iter() {
                let seek_key = SeekKey::TableRowId(*key as u64);
                assert!(
                    matches!(
                        cursor.seek(seek_key, SeekOp::EQ).unwrap(),
                        CursorResult::Ok(true)
                    ),
                    "key {} is not found",
                    key
                );
            }
        }
    }

    #[test]
    pub fn btree_insert_fuzz_run_equal_size() {
        for size in 1..8 {
            log::info!("======= size:{} =======", size);
            btree_insert_fuzz_run(2, 1024, |_| size);
        }
    }

    #[test]
    pub fn btree_insert_fuzz_run_random() {
        btree_insert_fuzz_run(128, 16, |rng| (rng.next_u32() % 4096) as usize);
    }

    #[test]
    pub fn btree_insert_fuzz_run_small() {
        btree_insert_fuzz_run(1, 1024, |rng| (rng.next_u32() % 128) as usize);
    }

    #[test]
    pub fn btree_insert_fuzz_run_big() {
        btree_insert_fuzz_run(64, 32, |rng| 3 * 1024 + (rng.next_u32() % 1024) as usize);
    }

    #[test]
    pub fn btree_insert_fuzz_run_overflow() {
        btree_insert_fuzz_run(64, 32, |rng| (rng.next_u32() % 32 * 1024) as usize);
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn setup_test_env(database_size: u32) -> (Rc<Pager>, Rc<RefCell<DatabaseHeader>>) {
        let page_size = 512;
        let mut db_header = DatabaseHeader::default();
        db_header.page_size = page_size;
        db_header.database_size = database_size;
        let db_header = Rc::new(RefCell::new(db_header));

        let buffer_pool = Rc::new(BufferPool::new(10));

        // Initialize buffer pool with correctly sized buffers
        for _ in 0..10 {
            let vec = vec![0; page_size as usize]; // Initialize with correct length, not just capacity
            buffer_pool.put(Pin::new(vec));
        }

        let io: Arc<dyn IO> = Arc::new(MemoryIO::new().unwrap());
        let page_io = Rc::new(FileStorage::new(
            io.open_file("test.db", OpenFlags::Create, false).unwrap(),
        ));

        let drop_fn = Rc::new(|_buf| {});
        let buf = Rc::new(RefCell::new(Buffer::allocate(page_size as usize, drop_fn)));
        {
            let mut buf_mut = buf.borrow_mut();
            let buf_slice = buf_mut.as_mut_slice();
            sqlite3_ondisk::write_header_to_buf(buf_slice, &db_header.borrow());
        }

        let write_complete = Box::new(|_| {});
        let c = Completion::Write(WriteCompletion::new(write_complete));
        page_io.write_page(1, buf.clone(), c).unwrap();

        let wal_shared = WalFileShared::open_shared(&io, "test.wal", page_size).unwrap();
        let wal = Rc::new(RefCell::new(WalFile::new(
            io.clone(),
            page_size as usize,
            wal_shared,
            buffer_pool.clone(),
        )));

        let pager = Rc::new(
            Pager::finish_open(
                db_header.clone(),
                page_io,
                wal,
                io,
                Arc::new(parking_lot::RwLock::new(DumbLruPageCache::new(10))),
                buffer_pool,
            )
            .unwrap(),
        );

        pager.io.run_once().unwrap();

        (pager, db_header)
    }

    #[test]
    fn test_clear_overflow_pages() -> Result<()> {
        let (pager, db_header) = setup_test_env(5);
        let cursor = BTreeCursor::new(pager.clone(), 1);

        let max_local = cursor.payload_overflow_threshold_max(PageType::TableLeaf);
        let usable_size = cursor.usable_space();

        // Create a large payload that will definitely trigger overflow
        let large_payload = vec![b'A'; max_local + usable_size];

        // Setup overflow pages (2, 3, 4) with linking
        let mut current_page = 2u32;
        while current_page <= 4 {
            let drop_fn = Rc::new(|_buf| {});
            let buf = Rc::new(RefCell::new(Buffer::allocate(
                db_header.borrow().page_size as usize,
                drop_fn,
            )));
            let write_complete = Box::new(|_| {});
            let c = Completion::Write(WriteCompletion::new(write_complete));
            pager
                .page_io
                .write_page(current_page as usize, buf.clone(), c)?;
            pager.io.run_once()?;

            let page = cursor.pager.read_page(current_page as usize)?;
            while page.is_locked() {
                cursor.pager.io.run_once()?;
            }

            {
                let contents = page.get().contents.as_mut().unwrap();

                let next_page = if current_page < 4 {
                    current_page + 1
                } else {
                    0
                };
                contents.write_u32(0, next_page); // Write pointer to next overflow page

                let buf = contents.as_ptr();
                buf[4..].fill(b'A');
            }

            current_page += 1;
        }
        pager.io.run_once()?;

        // Create leaf cell pointing to start of overflow chain
        let leaf_cell = BTreeCell::TableLeafCell(TableLeafCell {
            _rowid: 1,
            _payload: large_payload,
            first_overflow_page: Some(2), // Point to first overflow page
        });

        let initial_freelist_pages = db_header.borrow().freelist_pages;
        // Clear overflow pages
        let clear_result = cursor.clear_overflow_pages(&leaf_cell)?;
        match clear_result {
            CursorResult::Ok(_) => {
                // Verify proper number of pages were added to freelist
                assert_eq!(
                    db_header.borrow().freelist_pages,
                    initial_freelist_pages + 3,
                    "Expected 3 pages to be added to freelist"
                );

                // If this is first trunk page
                let trunk_page_id = db_header.borrow().freelist_trunk_page;
                if trunk_page_id > 0 {
                    // Verify trunk page structure
                    let trunk_page = cursor.pager.read_page(trunk_page_id as usize)?;
                    if let Some(contents) = trunk_page.get().contents.as_ref() {
                        // Read number of leaf pages in trunk
                        let n_leaf = contents.read_u32(4);
                        assert!(n_leaf > 0, "Trunk page should have leaf entries");

                        for i in 0..n_leaf {
                            let leaf_page_id = contents.read_u32(8 + (i as usize * 4));
                            assert!(
                                (2..=4).contains(&leaf_page_id),
                                "Leaf page ID {} should be in range 2-4",
                                leaf_page_id
                            );
                        }
                    }
                }
            }
            CursorResult::IO => {
                cursor.pager.io.run_once()?;
            }
        }

        Ok(())
    }

    #[test]
    fn test_clear_overflow_pages_no_overflow() -> Result<()> {
        let (pager, db_header) = setup_test_env(5);
        let cursor = BTreeCursor::new(pager.clone(), 1);

        let small_payload = vec![b'A'; 10];

        // Create leaf cell with no overflow pages
        let leaf_cell = BTreeCell::TableLeafCell(TableLeafCell {
            _rowid: 1,
            _payload: small_payload,
            first_overflow_page: None,
        });

        let initial_freelist_pages = db_header.borrow().freelist_pages;

        // Try to clear non-existent overflow pages
        let clear_result = cursor.clear_overflow_pages(&leaf_cell)?;
        match clear_result {
            CursorResult::Ok(_) => {
                // Verify freelist was not modified
                assert_eq!(
                    db_header.borrow().freelist_pages,
                    initial_freelist_pages,
                    "Freelist should not change when no overflow pages exist"
                );

                // Verify trunk page wasn't created
                assert_eq!(
                    db_header.borrow().freelist_trunk_page,
                    0,
                    "No trunk page should be created when no overflow pages exist"
                );
            }
            CursorResult::IO => {
                cursor.pager.io.run_once()?;
            }
        }

        Ok(())
    }
}
