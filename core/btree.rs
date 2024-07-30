use crate::pager::{Page, Pager};
use crate::sqlite3_ondisk::{
    read_varint, write_varint, BTreeCell, DatabaseHeader, PageContent, PageType, TableInteriorCell,
    TableLeafCell,
};
use crate::types::{Cursor, CursorResult, OwnedRecord, OwnedValue};
use crate::Result;

use std::cell::{Ref, RefCell};
use std::mem;
use std::rc::Rc;

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

    fn get_next_record(&mut self) -> Result<CursorResult<(Option<u64>, Option<OwnedRecord>)>> {
        loop {
            let mem_page = {
                let mem_page = self.page.borrow();
                let mem_page = mem_page.as_ref().unwrap();
                mem_page.clone()
            };
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
                    let record = crate::sqlite3_ondisk::read_record(_payload)?;
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

    fn move_to_root(&mut self) {
        self.page
            .replace(Some(Rc::new(MemPage::new(None, self.root_page, 0))));
    }

    pub fn move_to(&mut self, key: u64) -> Result<CursorResult<()>> {
        self.move_to_root();

        loop {
            let mem_page = {
                let mem_page = self.page.borrow();
                let mem_page = mem_page.as_ref().unwrap();
                mem_page.clone()
            };
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
                let parent = mem_page.parent.clone();
                match page.rightmost_pointer() {
                    Some(right_most_pointer) => {
                        let mem_page = MemPage::new(parent, right_most_pointer as usize, 0);
                        self.page.replace(Some(Rc::new(mem_page)));
                        continue;
                    }
                    None => {
                        unreachable!("we shall not go back up! The only way is down the slope")
                    }
                }
            }
        }
    }

    fn insert_to_page(
        &mut self,
        key: &OwnedValue,
        _record: &OwnedRecord,
    ) -> Result<CursorResult<()>> {
        let page_ref = self.get_page()?;
        let page = RefCell::borrow(&page_ref);
        if page.is_locked() {
            return Ok(CursorResult::IO);
        }

        page.set_dirty();
        self.pager.add_dirty(page_ref.clone());

        let mut page = page.contents.write().unwrap();
        let page = page.as_mut().unwrap();
        assert!(matches!(page.page_type(), PageType::TableLeaf));

        // find cell
        let int_key = match key {
            OwnedValue::Integer(i) => *i as u64,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        let cell_idx = find_cell(page, int_key);

        // TODO: if overwrite drop cell

        // insert cell
        let mut payload: Vec<u8> = Vec::new();

        {
            // Data len will be prepended later
            // Key
            let mut key_varint: Vec<u8> = Vec::new();
            key_varint.extend(std::iter::repeat(0).take(9));
            let n = write_varint(&mut key_varint.as_mut_slice()[0..9], int_key);
            write_varint(&mut key_varint, int_key);
            key_varint.truncate(n);
            payload.extend_from_slice(&key_varint);
        }

        // Data payload
        let payload_size_before_record = payload.len();
        _record.serialize(&mut payload);
        let header_size = payload.len() - payload_size_before_record;

        {
            // Data len
            let mut data_len_varint: Vec<u8> = Vec::new();
            data_len_varint.extend(std::iter::repeat(0).take(9));
            let n = write_varint(
                &mut data_len_varint.as_mut_slice()[0..9],
                header_size as u64,
            );
            data_len_varint.truncate(n);
            payload.splice(0..0, data_len_varint.iter().cloned());
        }

        let usable_space = {
            let db_header = RefCell::borrow(&self.database_header);
            (db_header.page_size - db_header.unused_space as u16) as usize
        };
        let free = self.compute_free_space(page, RefCell::borrow(&self.database_header));
        assert!(
            payload.len() <= usable_space - 100, /* 100 bytes minus for precaution to remember */
            "need to implemented overflow pages, too big to even add to a an empty page"
        );
        if payload.len() + 2 > free as usize {
            // overflow or balance
            self.balance_leaf(int_key, payload);
        } else {
            // insert
            self.insert_into_cell(page, &payload, cell_idx);
        }

        Ok(CursorResult::Ok(()))
    }

    /* insert to postion and shift other pointers */
    fn insert_into_cell(&mut self, page: &mut PageContent, payload: &Vec<u8>, cell_idx: usize) {
        dbg!(page.is_leaf(), cell_idx);
        assert!(
            page.is_leaf() || (!page.is_leaf() && cell_idx < page.cell_count()),
            "if it's greater it might mean we need to insert in a rightmost pointer?"
        );
        // TODO: insert into cell payload in internal page
        let pc = self.allocate_cell_space(page, payload.len() as u16);
        let mut buf_ref = RefCell::borrow_mut(&page.buffer);
        let buf: &mut [u8] = buf_ref.as_mut_slice();

        // copy data
        buf[pc as usize..pc as usize + payload.len()].copy_from_slice(&payload);
        //  memmove(pIns+2, pIns, 2*(pPage->nCell - i));
        let pointer_area_pc_by_idx = page.offset + 8 + 2 * cell_idx;

        // move previous pointers forward and insert new pointer there
        let n_cells_forward = 2 * (page.cell_count() - cell_idx);
        buf.copy_within(
            pointer_area_pc_by_idx..pointer_area_pc_by_idx + n_cells_forward,
            pointer_area_pc_by_idx + 2,
        );
        buf[pointer_area_pc_by_idx..pointer_area_pc_by_idx + 2].copy_from_slice(&pc.to_be_bytes());

        // update first byte of content area
        buf[5..7].copy_from_slice(&pc.to_be_bytes());

        // update cell count
        let new_n_cells = (page.cell_count() + 1) as u16;
        buf[3..5].copy_from_slice(&new_n_cells.to_be_bytes());
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

    fn balance_leaf(&mut self, key: u64, payload: Vec<u8>) {
        // This is a naive algorithm that doesn't try to distribute cells evenly by content.
        // It will try to split the page in half by keys not by content.
        // Sqlite tries to have a page at least 40% full.
        let mut key = key;
        let mut payload = payload;
        loop {
            let mem_page = {
                let mem_page = self.page.borrow();
                let mem_page = mem_page.as_ref().unwrap();
                mem_page.clone()
            };
            let page_ref = self.read_page_sync(mem_page.page_idx);
            let mut page_rc = RefCell::borrow_mut(&page_ref);

            let right_page_id = {
                // split procedure
                let mut page = page_rc.contents.write().unwrap();
                let page = page.as_mut().unwrap();
                let free = self.compute_free_space(page, RefCell::borrow(&self.database_header));
                assert!(
                    matches!(
                        page.page_type(),
                        PageType::TableLeaf | PageType::TableInterior
                    ),
                    "indexes still not supported "
                );
                if payload.len() + 2 <= free as usize {
                    let cell_idx = find_cell(page, key);
                    self.insert_into_cell(page, &payload, cell_idx);
                    break;
                }

                let right_page_ref = self.allocate_page(page.page_type());
                let right_page = RefCell::borrow_mut(&right_page_ref);
                let right_page_id = right_page.id;
                let mut right_page = right_page.contents.write().unwrap();
                let right_page = right_page.as_mut().unwrap();
                {
                    // move data from one buffer to another
                    // done in a separate block to satisfy borrow checker
                    let mut left_buf = RefCell::borrow_mut(&page.buffer);
                    let left_buf: &mut [u8] = left_buf.as_mut_slice();
                    let mut right_buf = RefCell::borrow_mut(&right_page.buffer);
                    let right_buf: &mut [u8] = right_buf.as_mut_slice();

                    let mut rbrk = right_page.cell_content_area() as usize;

                    // move half of cells to right page
                    for cell_idx in 0..page.cell_count() {
                        let (start, len) = page.cell_get_raw_region(cell_idx);
                        rbrk -= len;
                        right_buf[rbrk..rbrk + len].copy_from_slice(&left_buf[start..start + len]);
                    }
                    // move half of keys to right page
                    let (src_pointers_start, src_pointers_len) = page.cell_get_raw_pointer_region();
                    assert!(page.cell_count() >= 2);
                    let keys_to_move_start = page.cell_count() / 2;
                    let (dst_pointers_start, _) = right_page.cell_get_raw_pointer_region();
                    /*
                        Copy half
                        count = 8
                        k-v = 2 bytes
                                            keys_to_move_start
                                                     V
                        -------------------------------------------------
                        | 0k-v | 1k-v | 2k-v | 3k-v | 4k-v | 5k-v | 7k-v |
                        -------------------------------------------------

                    */
                    let pointer_data_to_move = (page.cell_count() - keys_to_move_start - 1) * 2;
                    right_buf[dst_pointers_start + pointer_data_to_move
                        ..dst_pointers_start + pointer_data_to_move]
                        .copy_from_slice(
                            &left_buf[src_pointers_start + pointer_data_to_move
                                ..src_pointers_start + (pointer_data_to_move * 2)],
                        );
                    // update cell count in both pages
                    let keys_moved = page.cell_count() - keys_to_move_start + 1;
                    page.write_u16(
                        BTREE_HEADER_OFFSET_CELL_COUNT,
                        (page.cell_count() - keys_moved) as u16,
                    );
                    right_page.write_u16(BTREE_HEADER_OFFSET_CELL_COUNT, keys_moved as u16);
                    // update cell content are start
                    right_page.write_u16(BTREE_HEADER_OFFSET_CELL_CONTENT, rbrk as u16);
                }
                let last_cell = page.cell_get(page.cell_count() - 1).unwrap();
                let last_cell_key = match &last_cell {
                    BTreeCell::TableLeafCell(cell) => cell._rowid,
                    BTreeCell::TableInteriorCell(cell) => cell._rowid,
                    _ => unreachable!(), /* not yet supported index tables */
                };
                // if not leaf page update rightmost pointer
                if let PageType::TableInterior = page.page_type() {
                    right_page.write_u32(
                        BTREE_HEADER_OFFSET_RIGHTMOST,
                        page.rightmost_pointer().unwrap(),
                    );
                    // convert last cell to rightmost pointer
                    let BTreeCell::TableInteriorCell(last_cell) = &last_cell else {
                        unreachable!();
                    };
                    page.write_u32(BTREE_HEADER_OFFSET_RIGHTMOST, last_cell._left_child_page);
                    // page count now has one less cell because we've added the last one to rightmost pointer
                    page.write_u16(
                        BTREE_HEADER_OFFSET_CELL_COUNT,
                        (page.cell_count() - 1) as u16,
                    );
                }

                // update free list block by defragmenting page
                self.defragment_page(page, RefCell::borrow(&self.database_header));
                // insert into one of the pages
                if key < last_cell_key {
                    let cell_idx = find_cell(page, key);
                    self.insert_into_cell(page, &payload, cell_idx);
                } else {
                    let cell_idx = find_cell(right_page, key);
                    self.insert_into_cell(right_page, &payload, cell_idx);
                }
                // propagate parent split
                key = last_cell_key;
                right_page_id
            };

            payload = Vec::new();
            if mem_page.page_idx == self.root_page {
                /* if we are in root page then we just need to create a new root and push key there */
                let new_root_page_ref = self.allocate_page(PageType::TableInterior);
                let mut new_root_page = RefCell::borrow_mut(&new_root_page_ref);
                let new_root_page_id = new_root_page.id;
                new_root_page.set_dirty();
                self.pager.add_dirty(new_root_page_ref.clone());
                {
                    let mut new_root_page_contents = new_root_page.contents.write().unwrap();
                    let new_root_page_contents = new_root_page_contents.as_mut().unwrap();
                    /*
                        Note that we set cell pointer to point to itself, because we will later swap this page's
                        content with splitted page in order to not update root page idx.
                    */
                    payload.extend_from_slice(&(new_root_page_id as u32).to_be_bytes());
                    payload.extend(std::iter::repeat(0).take(9));
                    let n = write_varint(&mut payload.as_mut_slice()[0..9], key as u64);
                    payload.truncate(n);

                    // write left child cell
                    self.insert_into_cell(new_root_page_contents, &payload, 0);

                    // write right child cell
                    new_root_page_contents
                        .write_u32(BTREE_HEADER_OFFSET_RIGHTMOST, right_page_id as u32);
                }

                /* swap splitted page buffer with new root buffer so we don't have to update page idx */
                {
                    let mut new_root_page_contents = new_root_page.contents.write().unwrap();
                    let new_root_page_contents = new_root_page_contents.as_mut().unwrap();
                    let root_buf = new_root_page_contents.buffer.as_ptr();
                    let root_buf = unsafe { (*root_buf).as_mut_slice() };
                    let mut page = page_rc.contents.write().unwrap();
                    let page = page.as_mut().unwrap();
                    let mut left_buf = RefCell::borrow_mut(&page.buffer);
                    let left_buf: &mut [u8] = left_buf.as_mut_slice();

                    left_buf.swap_with_slice(root_buf);
                }
                // swap in memory state of pages
                mem::swap(&mut page_rc.id, &mut new_root_page.id);
                self.page = RefCell::new(Some(Rc::new(MemPage::new(None, new_root_page.id, 0))));

                break;
            }

            payload.extend_from_slice(&(mem_page.page_idx as u32).to_be_bytes());
            payload.extend(std::iter::repeat(0).take(9));
            let n = write_varint(&mut payload.as_mut_slice()[0..9], key as u64);
            payload.truncate(n);

            self.page = RefCell::new(Some(mem_page.parent.as_ref().unwrap().clone()));
        }
    }

    fn read_page_sync(&mut self, page_idx: usize) -> Rc<RefCell<Page>> {
        loop {
            let page_ref = self.pager.read_page(page_idx);
            match page_ref {
                Ok(p) => return p,
                Err(_) => {}
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
        let mut buf_ref = RefCell::borrow_mut(&page_ref.buffer);
        let buf = buf_ref.as_mut_slice();

        let cell_offset = 8;
        let gap = cell_offset + 2 * page_ref.cell_count();
        let mut top = page_ref.cell_content_area() as usize;

        // there are free blocks and enough space
        if page_ref.first_freeblock() != 0 && gap + 2 <= top {
            // find slot
            let db_header = RefCell::borrow(&self.database_header);
            let pc = find_free_cell(page_ref, db_header, amount, buf);
            return pc as u16;
        }

        if gap + 2 + amount as usize > top {
            // defragment
            self.defragment_page(page_ref, RefCell::borrow(&self.database_header));
            top = u16::from_be_bytes([buf[5], buf[6]]) as usize;
            return 0;
        }

        let db_header = RefCell::borrow(&self.database_header);
        top -= amount;
        buf[5..7].copy_from_slice(&(top as u16).to_be_bytes());
        let usable_space = (db_header.page_size - db_header.unused_space as u16) as usize;
        assert!(top + amount <= usable_space);
        return top as u16;
    }

    fn defragment_page(&self, page: &PageContent, db_header: Ref<DatabaseHeader>) {
        let cloned_page = page.clone();
        let usable_space = (db_header.page_size - db_header.unused_space as u16) as u64;
        let mut cbrk = usable_space as u64;

        // TODO: implement fast algorithm

        let last_cell = (usable_space - 4) as u64;
        let first_cell = cloned_page.cell_content_area() as u64;
        if cloned_page.cell_count() > 0 {
            let buf = RefCell::borrow(&cloned_page.buffer);
            let buf = buf.as_slice();
            let mut write_buf = RefCell::borrow_mut(&page.buffer);
            let write_buf = write_buf.as_mut_slice();

            for i in 0..cloned_page.cell_count() {
                let cell_offset = 8;
                let cell_idx = cell_offset + i * 2;

                let pc = u16::from_be_bytes([buf[cell_idx], buf[cell_idx + 1]]) as u64;
                if pc > last_cell {
                    unimplemented!("corrupted page");
                }

                assert!(pc <= last_cell);

                let size = match read_varint(&buf[pc as usize..pc as usize + 9]) {
                    Ok(v) => v.0,
                    Err(_) => todo!(
                        "error while parsing varint from cell, probably treat this as corruption?"
                    ),
                };
                cbrk -= size;
                if cbrk < first_cell as u64 || pc as u64 + size > usable_space as u64 {
                    todo!("corrupt");
                }
                assert!(cbrk + size <= usable_space && cbrk >= first_cell);
                // set new pointer
                write_buf[cell_idx..cell_idx + 2].copy_from_slice(&cbrk.to_be_bytes());
                // copy payload
                write_buf[cbrk as usize..cbrk as usize + size as usize]
                    .copy_from_slice(&buf[pc as usize..pc as usize + size as usize]);
            }
        }

        // assert!( nfree >= 0 );
        // if( data[hdr+7]+cbrk-iCellFirst!=pPage->nFree ){
        //   return SQLITE_CORRUPT_PAGE(pPage);
        // }
        assert!(cbrk >= first_cell);
        let mut write_buf = RefCell::borrow_mut(&page.buffer);
        let write_buf = write_buf.as_mut_slice();

        // set new first byte of cell content
        write_buf[5..7].copy_from_slice(&cbrk.to_be_bytes());
        // set free block to 0, unused spaced can be retrieved from gap between cell pointer end and content start
        write_buf[1] = 0;
        write_buf[2] = 0;
        // set unused space to 0
        write_buf[first_cell as usize..first_cell as usize + cbrk as usize - first_cell as usize]
            .fill(0);
    }

    // Free blocks can be zero, meaning the "real free space" that can be used to allocate is expected to be between first cell byte
    // and end of cell pointer area.
    fn compute_free_space(&self, page: &PageContent, db_header: Ref<DatabaseHeader>) -> u16 {
        let buffer = RefCell::borrow(&page.buffer);
        let buf = buffer.as_slice();

        let usable_space = (db_header.page_size - db_header.unused_space as u16) as usize;
        let mut first_byte_in_cell_content = page.cell_content_area();
        if first_byte_in_cell_content == 0 {
            first_byte_in_cell_content = u16::MAX;
        }

        let fragmented_free_bytes = page.num_frag_free_bytes();
        let free_block_pointer = page.first_freeblock();
        let ncell = page.cell_count();

        // 8 + 4 == header end
        let first_cell = 8 + 4 + (2 * ncell) as u16;

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
                nfree += size as usize;
                if next <= pc + size + 3 {
                    break;
                }
                pc = next as usize;
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
        nfree = nfree - first_cell as usize;
        return nfree as u16;
    }
}

fn find_free_cell(
    page_ref: &PageContent,
    db_header: Ref<DatabaseHeader>,
    amount: usize,
    buf: &[u8],
) -> usize {
    // NOTE: freelist is in ascending order of keys and pc
    // unuse_space is reserved bytes at the end of page, therefore we must substract from maxpc
    let mut pc = page_ref.first_freeblock() as usize;
    let usable_space = (db_header.page_size - db_header.unused_space as u16) as usize;
    let maxpc = (usable_space - amount as usize) as usize;
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
        unimplemented!("recover for fragmented space");
    }
    pc
}

impl Cursor for BTreeCursor {
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
    while cell_idx < page.cell_count() {
        match page.cell_get(cell_idx).unwrap() {
            BTreeCell::TableLeafCell(cell) => {
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
