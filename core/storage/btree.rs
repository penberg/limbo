use log::trace;

use crate::storage::pager::{Page, Pager};
use crate::storage::sqlite3_ondisk::{
    read_btree_cell, read_varint, write_varint, BTreeCell, DatabaseHeader, PageContent, PageType,
    TableInteriorCell, TableLeafCell,
};
use crate::types::{Cursor, CursorResult, OwnedRecord, OwnedValue};
use crate::Result;

use std::cell::{Ref, RefCell};
use std::rc::Rc;

use super::sqlite3_ondisk::{write_varint_to_vec, OverflowCell};

/*
    These are offsets of fields in the header of a b-tree page.
*/
const BTREE_HEADER_OFFSET_TYPE: usize = 0; /* type of btree page -> u8 */
const BTREE_HEADER_OFFSET_FREEBLOCK: usize = 1; /* pointer to first freeblock -> u16 */
const BTREE_HEADER_OFFSET_CELL_COUNT: usize = 3; /* number of cells in the page -> u16 */
const BTREE_HEADER_OFFSET_CELL_CONTENT: usize = 5; /* pointer to first byte of cell allocated content from top -> u16 */
const BTREE_HEADER_OFFSET_FRAGMENTED: usize = 7; /* number of fragmented bytes -> u8 */
const BTREE_HEADER_OFFSET_RIGHTMOST: usize = 8; /* if internalnode, pointer right most pointer (saved separately from cells) -> u32 */

pub struct MemPage {
    parent: Option<Rc<MemPage>>,
    page_idx: usize,
    cell_idx: RefCell<usize>,
}

impl MemPage {
    pub fn new(parent: Option<Rc<MemPage>>, page_idx: usize, cell_idx: usize) -> Self {
        Self {
            parent,
            page_idx,
            cell_idx: RefCell::new(cell_idx),
        }
    }

    pub fn cell_idx(&self) -> usize {
        *self.cell_idx.borrow()
    }

    pub fn advance(&self) {
        let mut cell_idx = self.cell_idx.borrow_mut();
        *cell_idx += 1;
    }
}

pub struct BTreeCursor {
    pager: Rc<Pager>,
    root_page: usize,
    page: RefCell<Option<Rc<MemPage>>>,
    rowid: RefCell<Option<u64>>,
    record: RefCell<Option<OwnedRecord>>,
    null_flag: bool,
    database_header: Rc<RefCell<DatabaseHeader>>,
}

impl BTreeCursor {
    pub fn new(
        pager: Rc<Pager>,
        root_page: usize,
        database_header: Rc<RefCell<DatabaseHeader>>,
    ) -> Self {
        Self {
            pager,
            root_page,
            page: RefCell::new(None),
            rowid: RefCell::new(None),
            record: RefCell::new(None),
            null_flag: false,
            database_header,
        }
    }

    fn is_empty_table(&mut self) -> Result<CursorResult<bool>> {
        let page = self.pager.read_page(self.root_page)?;
        let page = RefCell::borrow(&page);
        if page.is_locked() {
            return Ok(CursorResult::IO);
        }

        let page = page.contents.read().unwrap();
        let page = page.as_ref().unwrap();
        Ok(CursorResult::Ok(page.cell_count() == 0))
    }

    fn get_next_record(&mut self) -> Result<CursorResult<(Option<u64>, Option<OwnedRecord>)>> {
        loop {
            let mem_page = self.get_mem_page();
            let page_idx = mem_page.page_idx;
            let page = self.pager.read_page(page_idx)?;
            let page = RefCell::borrow(&page);
            if page.is_locked() {
                return Ok(CursorResult::IO);
            }
            let page = page.contents.read().unwrap();
            let page = page.as_ref().unwrap();

            if mem_page.cell_idx() >= page.cell_count() {
                let parent = mem_page.parent.clone();
                match page.rightmost_pointer() {
                    Some(right_most_pointer) => {
                        let mem_page = MemPage::new(parent.clone(), right_most_pointer as usize, 0);
                        self.page.replace(Some(Rc::new(mem_page)));
                        continue;
                    }
                    None => match parent {
                        Some(ref parent) => {
                            self.page.replace(Some(parent.clone()));
                            continue;
                        }
                        None => {
                            return Ok(CursorResult::Ok((None, None)));
                        }
                    },
                }
            }
            let cell = page.cell_get(mem_page.cell_idx())?;
            match &cell {
                BTreeCell::TableInteriorCell(TableInteriorCell {
                    _left_child_page,
                    _rowid,
                }) => {
                    mem_page.advance();
                    let mem_page =
                        MemPage::new(Some(mem_page.clone()), *_left_child_page as usize, 0);
                    self.page.replace(Some(Rc::new(mem_page)));
                    continue;
                }
                BTreeCell::TableLeafCell(TableLeafCell {
                    _rowid,
                    _payload,
                    first_overflow_page: _,
                }) => {
                    mem_page.advance();
                    let record = crate::storage::sqlite3_ondisk::read_record(_payload)?;
                    return Ok(CursorResult::Ok((Some(*_rowid), Some(record))));
                }
                BTreeCell::IndexInteriorCell(_) => {
                    unimplemented!();
                }
                BTreeCell::IndexLeafCell(_) => {
                    unimplemented!();
                }
            }
        }
    }

    fn btree_seek_rowid(
        &mut self,
        rowid: u64,
    ) -> Result<CursorResult<(Option<u64>, Option<OwnedRecord>)>> {
        self.move_to(rowid)?;

        let mem_page = self.get_mem_page();

        let page_idx = mem_page.page_idx;
        let page = self.pager.read_page(page_idx)?;
        let page = RefCell::borrow(&page);
        if page.is_locked() {
            return Ok(CursorResult::IO);
        }
        let page = page.contents.read().unwrap();
        let page = page.as_ref().unwrap();

        for cell_idx in 0..page.cell_count() {
            match &page.cell_get(cell_idx)? {
                BTreeCell::TableLeafCell(TableLeafCell {
                    _rowid: cell_rowid,
                    _payload: p,
                    first_overflow_page: _,
                }) => {
                    if *cell_rowid == rowid {
                        let record = crate::storage::sqlite3_ondisk::read_record(p)?;
                        return Ok(CursorResult::Ok((Some(*cell_rowid), Some(record))));
                    }
                }
                cell_type => {
                    unreachable!("unexpected cell type: {:?}", cell_type);
                }
            }
        }

        Ok(CursorResult::Ok((None, None)))
    }

    fn move_to_root(&mut self) {
        self.page
            .replace(Some(Rc::new(MemPage::new(None, self.root_page, 0))));
    }

    fn move_to_rightmost(&mut self) -> Result<CursorResult<()>> {
        self.move_to_root();

        loop {
            let mem_page = self.page.borrow().as_ref().unwrap().clone();
            let page_idx = mem_page.page_idx;
            let page = self.pager.read_page(page_idx)?;
            let page = RefCell::borrow(&page);
            if page.is_locked() {
                return Ok(CursorResult::IO);
            }
            let page = page.contents.read().unwrap();
            let page = page.as_ref().unwrap();
            if page.is_leaf() {
                if page.cell_count() > 0 {
                    mem_page.cell_idx.replace(page.cell_count() - 1);
                }
                return Ok(CursorResult::Ok(()));
            }

            match page.rightmost_pointer() {
                Some(right_most_pointer) => {
                    mem_page.cell_idx.replace(page.cell_count());
                    let mem_page =
                        MemPage::new(Some(mem_page.clone()), right_most_pointer as usize, 0);
                    self.page.replace(Some(Rc::new(mem_page)));
                    continue;
                }

                None => {
                    unreachable!("interior page should have a rightmost pointer");
                }
            }
        }
    }

    pub fn move_to(&mut self, key: u64) -> Result<CursorResult<()>> {
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
            let mem_page = self.get_mem_page();
            let page_idx = mem_page.page_idx;
            let page = self.pager.read_page(page_idx)?;
            let page = RefCell::borrow(&page);
            if page.is_locked() {
                return Ok(CursorResult::IO);
            }

            let page = page.contents.read().unwrap();
            let page = page.as_ref().unwrap();
            if page.is_leaf() {
                return Ok(CursorResult::Ok(()));
            }

            let mut found_cell = false;
            for cell_idx in 0..page.cell_count() {
                match &page.cell_get(cell_idx)? {
                    BTreeCell::TableInteriorCell(TableInteriorCell {
                        _left_child_page,
                        _rowid,
                    }) => {
                        if key < *_rowid {
                            mem_page.advance();
                            let mem_page =
                                MemPage::new(Some(mem_page.clone()), *_left_child_page as usize, 0);
                            self.page.replace(Some(Rc::new(mem_page)));
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
                    BTreeCell::IndexInteriorCell(_) => {
                        unimplemented!();
                    }
                    BTreeCell::IndexLeafCell(_) => {
                        unimplemented!();
                    }
                }
            }

            if !found_cell {
                let parent = mem_page.clone();
                match page.rightmost_pointer() {
                    Some(right_most_pointer) => {
                        let mem_page = MemPage::new(Some(parent), right_most_pointer as usize, 0);
                        self.page.replace(Some(Rc::new(mem_page)));
                        continue;
                    }
                    None => {
                        unreachable!("we shall not go back up! The only way is down the slope");
                    }
                }
            }
        }
    }

    fn insert_to_page(
        &mut self,
        key: &OwnedValue,
        record: &OwnedRecord,
    ) -> Result<CursorResult<()>> {
        let page_ref = self.get_page()?;
        let int_key = match key {
            OwnedValue::Integer(i) => *i as u64,
            _ => unreachable!("btree tables are indexed by integers!"),
        };

        let (cell_idx, page_type) = {
            let page = RefCell::borrow(&page_ref);
            if page.is_locked() {
                return Ok(CursorResult::IO);
            }

            page.set_dirty();
            self.pager.add_dirty(page.id);

            let mut page = page.contents.write().unwrap();
            let page = page.as_mut().unwrap();
            assert!(matches!(page.page_type(), PageType::TableLeaf));

            // find cell
            (find_cell(page, int_key), page.page_type())
        };

        // TODO: if overwrite drop cell

        // insert cell

        let mut cell_payload: Vec<u8> = Vec::new();
        self.fill_cell_payload(page_type, Some(int_key), &mut cell_payload, record);

        // insert
        let overflow = {
            let page = RefCell::borrow(&page_ref);

            let mut page = page.contents.write().unwrap();
            let page = page.as_mut().unwrap();
            self.insert_into_cell(page, &cell_payload.as_slice(), cell_idx);
            page.overflow_cells.len()
        };

        if overflow > 0 {
            self.balance_leaf();
        }

        Ok(CursorResult::Ok(()))
    }

    /* insert to postion and shift other pointers */
    fn insert_into_cell(&mut self, page: &mut PageContent, payload: &[u8], cell_idx: usize) {
        let free = self.compute_free_space(page, RefCell::borrow(&self.database_header));
        let enough_space = payload.len() + 2 <= free as usize;
        if !enough_space {
            // add to overflow cell
            page.overflow_cells.push(OverflowCell {
                index: cell_idx,
                payload: Vec::from(payload),
            });
            return;
        }

        // TODO: insert into cell payload in internal page
        let pc = self.allocate_cell_space(page, payload.len() as u16);
        let buf = page.as_ptr();

        // copy data
        buf[pc as usize..pc as usize + payload.len()].copy_from_slice(payload);
        //  memmove(pIns+2, pIns, 2*(pPage->nCell - i));
        let (pointer_area_pc_by_idx, _) = page.cell_get_raw_pointer_region();
        let pointer_area_pc_by_idx = pointer_area_pc_by_idx + (2 * cell_idx);

        // move previous pointers forward and insert new pointer there
        let n_cells_forward = 2 * (page.cell_count() - cell_idx);
        if n_cells_forward > 0 {
            buf.copy_within(
                pointer_area_pc_by_idx..pointer_area_pc_by_idx + n_cells_forward,
                pointer_area_pc_by_idx + 2,
            );
        }
        page.write_u16(pointer_area_pc_by_idx, pc);

        // update first byte of content area
        page.write_u16(BTREE_HEADER_OFFSET_CELL_CONTENT, pc);

        // update cell count
        let new_n_cells = (page.cell_count() + 1) as u16;
        page.write_u16(BTREE_HEADER_OFFSET_CELL_COUNT, new_n_cells);
    }

    fn free_cell_range(&mut self, page: &mut PageContent, offset: u16, len: u16) {
        if page.first_freeblock() == 0 {
            // insert into empty list
            page.write_u16(offset as usize, 0);
            page.write_u16(offset as usize + 2, len as u16);
            page.write_u16(BTREE_HEADER_OFFSET_FREEBLOCK, offset as u16);
            return;
        }
        let first_block = page.first_freeblock();

        if offset < first_block {
            // insert into head of list
            page.write_u16(offset as usize, first_block);
            page.write_u16(offset as usize + 2, len as u16);
            page.write_u16(BTREE_HEADER_OFFSET_FREEBLOCK, offset as u16);
            return;
        }

        if offset <= page.cell_content_area() {
            // extend boundary of content area
            page.write_u16(BTREE_HEADER_OFFSET_FREEBLOCK, page.first_freeblock());
            page.write_u16(BTREE_HEADER_OFFSET_CELL_CONTENT, offset + len);
            return;
        }

        let maxpc = {
            let db_header = self.database_header.borrow();
            let usable_space = (db_header.page_size - db_header.unused_space as u16) as usize;
            usable_space as u16
        };

        let mut pc = first_block;
        let mut prev = first_block;

        while pc <= maxpc && pc < offset as u16 {
            let next = page.read_u16(pc as usize);
            prev = pc;
            pc = next;
        }

        if pc >= maxpc {
            // insert into tail
            let offset = offset as usize;
            let prev = prev as usize;
            page.write_u16(prev, offset as u16);
            page.write_u16(offset, 0);
            page.write_u16(offset + 2, len);
        } else {
            // insert in between
            let next = page.read_u16(pc as usize);
            let offset = offset as usize;
            let prev = prev as usize;
            page.write_u16(prev, offset as u16);
            page.write_u16(offset, next);
            page.write_u16(offset + 2, len);
        }
    }

    fn drop_cell(&mut self, page: &mut PageContent, cell_idx: usize) {
        let (cell_start, cell_len) = page.cell_get_raw_region(cell_idx);
        self.free_cell_range(page, cell_start as u16, cell_len as u16);
        page.write_u16(BTREE_HEADER_OFFSET_CELL_COUNT, page.cell_count() as u16 - 1);
    }

    fn get_page(&mut self) -> crate::Result<Rc<RefCell<Page>>> {
        let mem_page = {
            let mem_page = self.page.borrow();
            let mem_page = mem_page.as_ref().unwrap();
            mem_page.clone()
        };
        let page_idx = mem_page.page_idx;
        let page_ref = self.pager.read_page(page_idx)?;
        Ok(page_ref)
    }

    fn balance_leaf(&mut self) {
        // This is a naive algorithm that doesn't try to distribute cells evenly by content.
        // It will try to split the page in half by keys not by content.
        // Sqlite tries to have a page at least 40% full.
        loop {
            let mem_page = {
                let mem_page = self.page.borrow();
                let mem_page = mem_page.as_ref().unwrap();
                mem_page.clone()
            };

            {
                // check if we don't need to balance
                let page_ref = self.read_page_sync(mem_page.page_idx);
                let page_rc = RefCell::borrow(&page_ref);

                {
                    // don't continue if there are no overflow cells
                    let mut page = page_rc.contents.write().unwrap();
                    let page = page.as_mut().unwrap();
                    if page.overflow_cells.is_empty() {
                        break;
                    }
                }
            }

            trace!("Balancing leaf. leaf={}", mem_page.page_idx);
            if mem_page.parent.is_none() {
                self.balance_root();
                continue;
            }

            let page_ref = self.read_page_sync(mem_page.page_idx);
            let page_rc = RefCell::borrow(&page_ref);

            // Copy of page used to reference cell bytes.
            let page_copy = {
                let mut page = page_rc.contents.write().unwrap();
                let page = page.as_mut().unwrap();
                page.clone()
            };

            // In memory in order copy of all cells in pages we want to balance. For now let's do a 2 page split.
            // Right pointer in interior cells should be converted to regular cells if more than 2 pages are used for balancing.
            let (scratch_cells, right_most_pointer) = {
                let mut scratch_cells: Vec<&[u8]> = Vec::new();

                for cell_idx in 0..page_copy.cell_count() {
                    let (start, len) = page_copy.cell_get_raw_region(cell_idx);
                    let buf = page_copy.as_ptr();
                    scratch_cells.push(&buf[start..start + len]);
                }
                for overflow_cell in &page_copy.overflow_cells {
                    scratch_cells.insert(overflow_cell.index, &overflow_cell.payload);
                }
                (scratch_cells, page_copy.rightmost_pointer())
            };

            // allocate new pages and move cells to those new pages
            {
                // split procedure
                let mut page = page_rc.contents.write().unwrap();
                let page = page.as_mut().unwrap();
                assert!(
                    matches!(
                        page.page_type(),
                        PageType::TableLeaf | PageType::TableInterior
                    ),
                    "indexes still not supported "
                );

                let right_page_ref = self.allocate_page(page.page_type());
                let right_page = RefCell::borrow_mut(&right_page_ref);
                let right_page_id = right_page.id;
                let mut right_page = right_page.contents.write().unwrap();
                let right_page = right_page.as_mut().unwrap();
                {
                    let is_leaf = page.is_leaf();
                    let mut new_pages = vec![page, right_page];
                    let new_pages_ids = vec![mem_page.page_idx, right_page_id];
                    trace!(
                        "splitting left={} right={}",
                        new_pages_ids[0],
                        new_pages_ids[1]
                    );

                    // drop divider cells and find right pointer
                    // NOTE: since we are doing a simple split we only finding the pointer we want to update (right pointer).
                    // Right pointer means cell that points to the last page, as we don't really want to drop this one. This one
                    // can be a "rightmost pointer" or a "cell".
                    // TODO(pere): simplify locking...
                    // we always asumme there is a parent
                    let parent_rc = mem_page.parent.as_ref().unwrap();

                    let parent_ref = self.read_page_sync(parent_rc.page_idx);
                    let parent = RefCell::borrow_mut(&parent_ref);
                    parent.set_dirty();
                    self.pager.add_dirty(parent.id);
                    let mut parent = parent.contents.write().unwrap();
                    let parent = parent.as_mut().unwrap();
                    // if this isn't empty next loop won't work
                    assert!(parent.overflow_cells.is_empty());

                    // Right page pointer is u32 in right most pointer, and in cell is u32 too, so we can use a *u32 to hold where we want to change this value
                    let mut right_pointer = BTREE_HEADER_OFFSET_RIGHTMOST;
                    for cell_idx in 0..parent.cell_count() {
                        let cell = parent.cell_get(cell_idx).unwrap();
                        let found = match cell {
                            BTreeCell::TableInteriorCell(interior) => {
                                interior._left_child_page as usize == mem_page.page_idx
                            }
                            _ => unreachable!("Parent should always be a "),
                        };
                        if found {
                            let (start, len) = parent.cell_get_raw_region(cell_idx);
                            right_pointer = start;
                            break;
                        }
                    }

                    // reset pages
                    for page in &new_pages {
                        page.write_u16(BTREE_HEADER_OFFSET_FREEBLOCK, 0);
                        page.write_u16(BTREE_HEADER_OFFSET_CELL_COUNT, 0);

                        let db_header = RefCell::borrow(&self.database_header);
                        let cell_content_area_start =
                            db_header.page_size - db_header.unused_space as u16;
                        page.write_u16(BTREE_HEADER_OFFSET_CELL_CONTENT, cell_content_area_start);

                        page.write_u8(BTREE_HEADER_OFFSET_FRAGMENTED, 0);
                        page.write_u32(BTREE_HEADER_OFFSET_RIGHTMOST, 0);
                    }

                    // distribute cells
                    let new_pages_len = new_pages.len();
                    let cells_per_page = scratch_cells.len() / new_pages.len();
                    let mut current_cell_index = 0_usize;
                    let mut divider_cells_index = Vec::new(); /* index to scratch cells that will be used as dividers in order */

                    for (i, page) in new_pages.iter_mut().enumerate() {
                        let last_page = i == new_pages_len - 1;
                        let cells_to_copy = if last_page {
                            // last cells is remaining pages if division was odd
                            scratch_cells.len() - current_cell_index
                        } else {
                            cells_per_page
                        };

                        let mut i = 0;
                        for cell_idx in current_cell_index..current_cell_index + cells_to_copy {
                            let cell = scratch_cells[cell_idx];
                            self.insert_into_cell(*page, cell, i);
                            i += 1;
                        }
                        divider_cells_index.push(current_cell_index + cells_to_copy - 1);
                        current_cell_index += cells_to_copy;
                    }

                    // update rightmost pointer for each page if we are in interior page
                    if !is_leaf {
                        for page in new_pages.iter_mut().take(new_pages_len - 1) {
                            assert!(page.cell_count() == 1);
                            let last_cell = page.cell_get(page.cell_count() - 1).unwrap();
                            let last_cell_pointer = match last_cell {
                                BTreeCell::TableInteriorCell(interior) => interior._left_child_page,
                                _ => unreachable!(),
                            };
                            self.drop_cell(*page, page.cell_count() - 1);
                            page.write_u32(BTREE_HEADER_OFFSET_RIGHTMOST, last_cell_pointer);
                        }
                        // last page right most pointer points to previous right most pointer before splitting
                        let last_page = new_pages.last().unwrap();
                        last_page
                            .write_u32(BTREE_HEADER_OFFSET_RIGHTMOST, right_most_pointer.unwrap());
                    }

                    // insert dividers in parent
                    // we can consider dividers the first cell of each page starting from the second page
                    for (page_id_index, page) in
                        new_pages.iter_mut().take(new_pages_len - 1).enumerate()
                    {
                        assert!(page.cell_count() > 1);
                        let divider_cell_index = divider_cells_index[page_id_index];
                        let cell_payload = scratch_cells[divider_cell_index];
                        let cell = read_btree_cell(cell_payload, &page.page_type(), 0).unwrap();
                        if is_leaf {
                            // create a new divider cell and push
                            let key = match cell {
                                BTreeCell::TableLeafCell(leaf) => leaf._rowid,
                                _ => unreachable!(),
                            };
                            let mut divider_cell = Vec::new();
                            divider_cell.extend_from_slice(
                                &(new_pages_ids[page_id_index] as u32).to_be_bytes(),
                            );
                            divider_cell.extend(std::iter::repeat(0).take(9));
                            let n = write_varint(&mut divider_cell.as_mut_slice()[4..], key);
                            divider_cell.truncate(4 + n);
                            let parent_cell_idx = find_cell(parent, key);
                            self.insert_into_cell(parent, divider_cell.as_slice(), parent_cell_idx);
                        } else {
                            // move cell
                            let key = match cell {
                                BTreeCell::TableInteriorCell(interior) => interior._rowid,
                                _ => unreachable!(),
                            };
                            let parent_cell_idx = find_cell(page, key);
                            self.insert_into_cell(parent, cell_payload, parent_cell_idx);
                            // self.drop_cell(*page, 0);
                        }
                    }

                    {
                        // copy last page id to right pointer
                        let last_pointer = *new_pages_ids.last().unwrap() as u32;
                        parent.write_u32(right_pointer, last_pointer);
                    }
                }
            }

            self.page = RefCell::new(Some(mem_page.parent.as_ref().unwrap().clone()));
        }
    }

    fn balance_root(&mut self) {
        /* todo: balance deeper, create child and copy contents of root there. Then split root */
        /* if we are in root page then we just need to create a new root and push key there */
        let mem_page = {
            let mem_page = self.page.borrow();
            let mem_page = mem_page.as_ref().unwrap();
            mem_page.clone()
        };

        let new_root_page_ref = self.allocate_page(PageType::TableInterior);
        {
            let new_root_page = RefCell::borrow(&new_root_page_ref);
            let new_root_page_id = new_root_page.id;
            let mut new_root_page_contents = new_root_page.contents.write().unwrap();
            let new_root_page_contents = new_root_page_contents.as_mut().unwrap();
            // point new root right child to previous root
            new_root_page_contents
                .write_u32(BTREE_HEADER_OFFSET_RIGHTMOST, new_root_page_id as u32);
            new_root_page_contents.write_u16(BTREE_HEADER_OFFSET_CELL_COUNT, 0);
        }

        /* swap splitted page buffer with new root buffer so we don't have to update page idx */
        {
            let page_ref = self.read_page_sync(mem_page.page_idx);
            let (root_id, child_id) = {
                let mut page_rc = RefCell::borrow_mut(&page_ref);
                let mut new_root_page = RefCell::borrow_mut(&new_root_page_ref);

                // Swap the entire Page structs
                std::mem::swap(&mut page_rc.id, &mut new_root_page.id);

                self.pager.add_dirty(new_root_page.id);
                self.pager.add_dirty(page_rc.id);
                (new_root_page.id, page_rc.id)
            };

            let root = new_root_page_ref.clone();
            let child = page_ref.clone();

            let parent = Some(Rc::new(MemPage::new(None, root_id, 0)));
            self.page = RefCell::new(Some(Rc::new(MemPage::new(parent, child_id, 0))));
            trace!("Balancing root. root={}, rightmost={}", root_id, child_id);
            self.pager.put_page(root_id, root);
            self.pager.put_page(child_id, child);
        }
    }

    fn read_page_sync(&mut self, page_idx: usize) -> Rc<RefCell<Page>> {
        loop {
            let page_ref = self.pager.read_page(page_idx);
            if let Ok(p) = page_ref {
                return p;
            }
        }
    }

    fn allocate_page(&mut self, page_type: PageType) -> Rc<RefCell<Page>> {
        let page = self.pager.allocate_page().unwrap();

        {
            // setup btree page
            let contents = RefCell::borrow(&page);
            let mut contents = contents.contents.write().unwrap();
            let contents = contents.as_mut().unwrap();
            let id = page_type as u8;
            contents.write_u8(BTREE_HEADER_OFFSET_TYPE, id);
            contents.write_u16(BTREE_HEADER_OFFSET_FREEBLOCK, 0);
            contents.write_u16(BTREE_HEADER_OFFSET_CELL_COUNT, 0);

            let db_header = RefCell::borrow(&self.database_header);
            let cell_content_area_start = db_header.page_size - db_header.unused_space as u16;
            contents.write_u16(BTREE_HEADER_OFFSET_CELL_CONTENT, cell_content_area_start);

            contents.write_u8(BTREE_HEADER_OFFSET_FRAGMENTED, 0);
            contents.write_u32(BTREE_HEADER_OFFSET_RIGHTMOST, 0);
        }

        page
    }

    /*
        Allocate space for a cell on a page.
    */
    fn allocate_cell_space(&mut self, page_ref: &PageContent, amount: u16) -> u16 {
        let amount = amount as usize;

        let (cell_offset, _) = page_ref.cell_get_raw_pointer_region();
        let gap = cell_offset + 2 * page_ref.cell_count();
        let mut top = page_ref.cell_content_area() as usize;

        // there are free blocks and enough space
        if page_ref.first_freeblock() != 0 && gap + 2 <= top {
            // find slot
            let db_header = RefCell::borrow(&self.database_header);
            let pc = find_free_cell(page_ref, db_header, amount);
            if pc != 0 {
                return pc as u16;
            }
            /* fall through, we might need to defragment */
        }

        if gap + 2 + amount > top {
            // defragment
            self.defragment_page(page_ref, RefCell::borrow(&self.database_header));
            let buf = page_ref.as_ptr();
            top = u16::from_be_bytes([buf[5], buf[6]]) as usize;
        }

        let db_header = RefCell::borrow(&self.database_header);
        top -= amount;

        {
            let buf = page_ref.as_ptr();
            buf[5..7].copy_from_slice(&(top as u16).to_be_bytes());
        }

        let usable_space = (db_header.page_size - db_header.unused_space as u16) as usize;
        assert!(top + amount <= usable_space);
        top as u16
    }

    fn defragment_page(&self, page: &PageContent, db_header: Ref<DatabaseHeader>) {
        let cloned_page = page.clone();
        let usable_space = (db_header.page_size - db_header.unused_space as u16) as u64;
        let mut cbrk = usable_space;

        // TODO: implement fast algorithm

        let last_cell = usable_space - 4;
        let first_cell = {
            let (start, end) = cloned_page.cell_get_raw_pointer_region();
            start + end
        };

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
                if cbrk < first_cell as u64 || pc + size > usable_space {
                    todo!("corrupt");
                }
                assert!(cbrk + size <= usable_space && cbrk >= first_cell as u64);
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
        assert!(cbrk >= first_cell as u64);
        let write_buf = page.as_ptr();

        // set new first byte of cell content
        write_buf[5..7].copy_from_slice(&(cbrk as u16).to_be_bytes());
        // set free block to 0, unused spaced can be retrieved from gap between cell pointer end and content start
        write_buf[1] = 0;
        write_buf[2] = 0;
        // set unused space to 0
        let first_cell = cloned_page.cell_content_area() as u64;
        assert!(first_cell <= cbrk);
        write_buf[first_cell as usize..cbrk as usize].fill(0);
    }

    // Free blocks can be zero, meaning the "real free space" that can be used to allocate is expected to be between first cell byte
    // and end of cell pointer area.
    fn compute_free_space(&self, page: &PageContent, db_header: Ref<DatabaseHeader>) -> u16 {
        let buf = page.as_ptr();

        let usable_space = (db_header.page_size - db_header.unused_space as u16) as usize;
        let mut first_byte_in_cell_content = page.cell_content_area();
        if first_byte_in_cell_content == 0 {
            first_byte_in_cell_content = u16::MAX;
        }

        let fragmented_free_bytes = page.num_frag_free_bytes();
        let free_block_pointer = page.first_freeblock();
        let ncell = page.cell_count();

        // 8 + 4 == header end
        let first_cell = (page.offset + 8 + 4 + (2 * ncell)) as u16;

        let mut nfree = fragmented_free_bytes as usize + first_byte_in_cell_content as usize;

        let mut pc = free_block_pointer as usize;
        if pc > 0 {
            let mut next = 0;
            let mut size = 0;
            if pc < first_byte_in_cell_content as usize {
                // corrupt
                todo!("corrupted page");
            }

            loop {
                // TODO: check corruption icellast
                next = u16::from_be_bytes(buf[pc..pc + 2].try_into().unwrap()) as usize;
                size = u16::from_be_bytes(buf[pc + 2..pc + 4].try_into().unwrap()) as usize;
                nfree += size;
                if next <= pc + size + 3 {
                    break;
                }
                pc = next;
            }

            if next > 0 {
                todo!("corrupted page ascending order");
            }

            if pc + size > usable_space {
                todo!("corrupted page last freeblock extends last page end");
            }
        }

        // if( nFree>usableSize || nFree<iCellFirst ){
        //   return SQLITE_CORRUPT_PAGE(pPage);
        // }
        // don't count header and cell pointers?
        nfree -= first_cell as usize;
        nfree as u16
    }

    fn get_mem_page(&self) -> Rc<MemPage> {
        let mem_page = self.page.borrow();
        let mem_page = mem_page.as_ref().unwrap();
        mem_page.clone()
    }

    fn fill_cell_payload(
        &self,
        page_type: PageType,
        int_key: Option<u64>,
        cell_payload: &mut Vec<u8>,
        record: &OwnedRecord,
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

        if record_buf.len() <= self.max_local(page_type) {
            // enough allowed space to fit inside a btree page
            cell_payload.extend_from_slice(record_buf.as_slice());
            return;
        }
        todo!("implement overflow page");
    }

    fn max_local(&self, page_type: PageType) -> usize {
        let usable_space = {
            let db_header = RefCell::borrow(&self.database_header);
            (db_header.page_size - db_header.unused_space as u16) as usize
        };
        match page_type {
            PageType::IndexInterior | PageType::TableInterior => {
                (usable_space - 12) * 64 / 255 - 23
            }
            PageType::IndexLeaf | PageType::TableLeaf => usable_space - 35,
        }
    }
}

fn find_free_cell(page_ref: &PageContent, db_header: Ref<DatabaseHeader>, amount: usize) -> usize {
    // NOTE: freelist is in ascending order of keys and pc
    // unuse_space is reserved bytes at the end of page, therefore we must substract from maxpc
    let mut pc = page_ref.first_freeblock() as usize;

    let buf = page_ref.as_ptr();

    let usable_space = (db_header.page_size - db_header.unused_space as u16) as usize;
    let maxpc = usable_space - amount;
    let mut found = false;
    while pc <= maxpc {
        let next = u16::from_be_bytes(buf[pc..pc + 2].try_into().unwrap());
        let size = u16::from_be_bytes(buf[pc + 2..pc + 4].try_into().unwrap());
        if amount <= size as usize {
            found = true;
            break;
        }
        pc = next as usize;
    }
    if !found {
        0
    } else {
        pc
    }
}

impl Cursor for BTreeCursor {
    fn seek_to_last(&mut self) -> Result<CursorResult<()>> {
        self.move_to_rightmost()?;
        match self.get_next_record()? {
            CursorResult::Ok((rowid, next)) => {
                if rowid.is_none() {
                    match self.is_empty_table()? {
                        CursorResult::Ok(is_empty) => {
                            assert!(is_empty)
                        }
                        CursorResult::IO => (),
                    }
                }
                self.rowid.replace(rowid);
                self.record.replace(next);
                Ok(CursorResult::Ok(()))
            }
            CursorResult::IO => Ok(CursorResult::IO),
        }
    }

    fn is_empty(&self) -> bool {
        self.record.borrow().is_none()
    }

    fn rewind(&mut self) -> Result<CursorResult<()>> {
        let mem_page = MemPage::new(None, self.root_page, 0);
        self.page.replace(Some(Rc::new(mem_page)));
        match self.get_next_record()? {
            CursorResult::Ok((rowid, next)) => {
                self.rowid.replace(rowid);
                self.record.replace(next);
                Ok(CursorResult::Ok(()))
            }
            CursorResult::IO => Ok(CursorResult::IO),
        }
    }

    fn next(&mut self) -> Result<CursorResult<()>> {
        match self.get_next_record()? {
            CursorResult::Ok((rowid, next)) => {
                self.rowid.replace(rowid);
                self.record.replace(next);
                Ok(CursorResult::Ok(()))
            }
            CursorResult::IO => Ok(CursorResult::IO),
        }
    }

    fn wait_for_completion(&mut self) -> Result<()> {
        // TODO: Wait for pager I/O to complete
        Ok(())
    }

    fn rowid(&self) -> Result<Option<u64>> {
        Ok(*self.rowid.borrow())
    }

    fn seek_rowid(&mut self, rowid: u64) -> Result<CursorResult<bool>> {
        match self.btree_seek_rowid(rowid)? {
            CursorResult::Ok((rowid, record)) => {
                self.rowid.replace(rowid);
                self.record.replace(record);
                Ok(CursorResult::Ok(rowid.is_some()))
            }
            CursorResult::IO => Ok(CursorResult::IO),
        }
    }

    fn record(&self) -> Result<Ref<Option<OwnedRecord>>> {
        Ok(self.record.borrow())
    }

    fn insert(
        &mut self,
        key: &OwnedValue,
        _record: &OwnedRecord,
        moved_before: bool, /* Indicate whether it's necessary to traverse to find the leaf page */
    ) -> Result<CursorResult<()>> {
        let int_key = match key {
            OwnedValue::Integer(i) => i,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        if !moved_before {
            match self.move_to(*int_key as u64)? {
                CursorResult::Ok(_) => {}
                CursorResult::IO => return Ok(CursorResult::IO),
            };
        }

        match self.insert_to_page(key, _record)? {
            CursorResult::Ok(_) => Ok(CursorResult::Ok(())),
            CursorResult::IO => Ok(CursorResult::IO),
        }
    }

    fn set_null_flag(&mut self, flag: bool) {
        self.null_flag = flag;
    }

    fn get_null_flag(&self) -> bool {
        self.null_flag
    }

    fn exists(&mut self, key: &OwnedValue) -> Result<CursorResult<bool>> {
        let int_key = match key {
            OwnedValue::Integer(i) => i,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        match self.move_to(*int_key as u64)? {
            CursorResult::Ok(_) => {}
            CursorResult::IO => return Ok(CursorResult::IO),
        };
        let page_ref = self.get_page()?;
        let page = RefCell::borrow(&page_ref);
        if page.is_locked() {
            return Ok(CursorResult::IO);
        }

        let page = page.contents.read().unwrap();
        let page = page.as_ref().unwrap();

        // find cell
        let int_key = match key {
            OwnedValue::Integer(i) => *i as u64,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        let cell_idx = find_cell(page, int_key);
        if cell_idx >= page.cell_count() {
            Ok(CursorResult::Ok(false))
        } else {
            let equals = match &page.cell_get(cell_idx)? {
                BTreeCell::TableLeafCell(l) => l._rowid == int_key,
                _ => unreachable!(),
            };
            Ok(CursorResult::Ok(equals))
        }
    }
}

fn find_cell(page: &PageContent, int_key: u64) -> usize {
    let mut cell_idx = 0;
    let cell_count = page.cell_count();
    while cell_idx < cell_count {
        match page.cell_get(cell_idx).unwrap() {
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
