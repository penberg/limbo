use crate::pager::Pager;
use crate::sqlite3_ondisk::{
    read_varint, write_varint, BTreeCell, BTreePage, DatabaseHeader, PageType, TableInteriorCell,
    TableLeafCell,
};
use crate::types::{Cursor, CursorResult, OwnedRecord, OwnedValue};
use crate::Result;

use std::cell::{Ref, RefCell};
use std::rc::Rc;

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
            let page = page.borrow();
            if page.is_locked() {
                return Ok(CursorResult::IO);
            }
            let page = page.contents.read().unwrap();
            let page = page.as_ref().unwrap();
            if mem_page.cell_idx() >= page.cells.len() {
                let parent = mem_page.parent.clone();
                match page.header.right_most_pointer {
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
            let cell = &page.cells[mem_page.cell_idx()];
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
            let page = page.borrow();
            if page.is_locked() {
                return Ok(CursorResult::IO);
            }
            let page = page.contents.read().unwrap();
            let page = page.as_ref().unwrap();
            if page.is_leaf() {
                return Ok(CursorResult::Ok(()));
            }

            let mut found_cell = false;
            for cell in &page.cells {
                match &cell {
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
                match page.header.right_most_pointer {
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
        let mem_page = {
            let mem_page = self.page.borrow();
            let mem_page = mem_page.as_ref().unwrap();
            mem_page.clone()
        };
        let page_idx = mem_page.page_idx;
        let page = self.pager.read_page(page_idx)?;
        let page = page.borrow_mut();
        if page.is_locked() {
            return Ok(CursorResult::IO);
        }

        page.set_dirty();

        let mut page = page.contents.write().unwrap();
        let page = page.as_mut().unwrap();

        let free = self.compute_free_space(page, self.database_header.borrow());

        // find cell
        let int_key = match key {
            OwnedValue::Integer(i) => *i as u64,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        let mut cell_idx = 0;
        for cell in &page.cells {
            match cell {
                BTreeCell::TableLeafCell(cell) => {
                    if int_key <= cell._rowid {
                        break;
                    }
                }
                _ => todo!(),
            }
            cell_idx += 1;
        }

        // if overwrite drop cell

        // insert cell
        assert!(page.header.page_type == PageType::TableLeaf);
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

        if payload.len() + 2 > free as usize {
            // overflow or balance
            todo!("overflow/balance");
        } else {
            // insert
            let pc = self.allocate_cell_space(page, payload.len() as u16);
            let mut buf = page.buffer.borrow_mut();
            let mut buf = buf.as_mut_slice();
            buf[pc as usize..pc as usize + payload.len()].copy_from_slice(&payload);
            //  memmove(pIns+2, pIns, 2*(pPage->nCell - i));
            let pointer_area_pc_by_idx = 8 + 2 * cell_idx;
            // move previous pointers forward and insert new pointer there
            let n_cells_forward = 2 * (page.cells.len() - cell_idx);
            buf.copy_within(
                pointer_area_pc_by_idx..pointer_area_pc_by_idx + n_cells_forward,
                pointer_area_pc_by_idx + 2,
            );
            // TODo: refactor cells to be lazy loadable because this will be crazy slow
            let mut payload_for_cell_in_memory: Vec<u8> = Vec::new();
            _record.serialize(&mut payload_for_cell_in_memory);
            page.cells.insert(
                cell_idx,
                BTreeCell::TableLeafCell(TableLeafCell {
                    _rowid: int_key,
                    _payload: payload_for_cell_in_memory,
                    first_overflow_page: None,
                }),
            );
        }

        Ok(CursorResult::Ok(()))
    }

    fn allocate_cell_space(&mut self, page_ref: &BTreePage, amount: u16) -> u16 {
        let amount = amount as usize;
        let mut page = page_ref.buffer.borrow_mut();
        let buf = page.as_mut_slice();

        let cell_offset = 8;
        let mut gap = cell_offset + 2 * page_ref.cells.len();
        let mut top = page_ref.header._cell_content_area as usize;

        // there are free blocks and enough space
        if page_ref.header._first_freeblock_offset != 0 && gap + 2 <= top {
            // find slot
            let db_header = self.database_header.borrow();
            let pc = find_free_cell(page_ref, db_header, amount, buf);
            return pc as u16;
        }

        if gap + 2 + amount as usize > top {
            // defragment
            self.defragment_page(page_ref, self.database_header.borrow());
            top = u16::from_be_bytes([buf[5], buf[6]]) as usize;
            return 0;
        }

        let db_header = self.database_header.borrow();
        top -= amount;
        buf[5..7].copy_from_slice(&(top as u16).to_be_bytes());
        let usable_space = (db_header.page_size - db_header.unused_space as u16) as usize;
        assert!(top + amount <= usable_space);
        return top as u16;
    }

    fn defragment_page(&self, page: &BTreePage, db_header: Ref<DatabaseHeader>) {
        let cloned_page = page.clone();
        let usable_space = (db_header.page_size - db_header.unused_space as u16) as u64;
        let mut cbrk = usable_space as u64;

        // TODO: implement fast algorithm

        let last_cell = (usable_space - 4) as u64;
        let first_cell = cloned_page.header._cell_content_area as u64;
        if cloned_page.cells.len() > 0 {
            let buf = cloned_page.buffer.borrow();
            let buf = buf.as_slice();
            let mut write_buf = page.buffer.borrow_mut();
            let write_buf = write_buf.as_mut_slice();

            for i in 0..cloned_page.cells.len() {
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
        let mut write_buf = page.buffer.borrow_mut();
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
    fn compute_free_space(&self, page: &BTreePage, db_header: Ref<DatabaseHeader>) -> u16 {
        let buffer = page.buffer.borrow();
        let buf = buffer.as_slice();

        let usable_space = (db_header.page_size - db_header.unused_space as u16) as usize;
        let mut first_byte_in_cell_content = page.header._cell_content_area;
        if first_byte_in_cell_content == 0 {
            first_byte_in_cell_content = u16::MAX;
        }

        let fragmented_free_bytes = page.header._num_frag_free_bytes;
        let free_block_pointer = page.header._first_freeblock_offset;
        let ncell = page.cells.len();

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
    page_ref: &BTreePage,
    db_header: Ref<DatabaseHeader>,
    amount: usize,
    buf: &[u8],
) -> usize {
    // NOTE: freelist is in ascending order of keys and pc
    // unuse_space is reserved bytes at the end of page, therefore we must substract from maxpc
    let mut pc = page_ref.header._first_freeblock_offset as usize;
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

    fn insert(&mut self, key: &OwnedValue, _record: &OwnedRecord) -> Result<CursorResult<()>> {
        let int_key = match key {
            OwnedValue::Integer(i) => i,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        match self.move_to(*int_key as u64)? {
            CursorResult::Ok(_) => {}
            CursorResult::IO => return Ok(CursorResult::IO),
        };

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

    fn exists(&mut self, key: &OwnedValue) -> Result<bool> {
        Ok(false)
    }
}
