use crate::pager::Pager;
use crate::sqlite3_ondisk::{BTreeCell, BTreePage, TableInteriorCell, TableLeafCell};
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
}

impl BTreeCursor {
    pub fn new(pager: Rc<Pager>, root_page: usize) -> Self {
        Self {
            pager,
            root_page,
            page: RefCell::new(None),
            rowid: RefCell::new(None),
            record: RefCell::new(None),
            null_flag: false,
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
        let page = page.borrow();
        if page.is_locked() {
            return Ok(CursorResult::IO);
        }

        page.set_dirty();

        let page = page.contents.read().unwrap();
        let page = page.as_ref().unwrap();

        let free = self.compute_free_space(page);
        dbg!(free);

        Ok(CursorResult::Ok(()))
    }

    fn compute_free_space(&self, page: &BTreePage) -> u16 {
        let buffer = page.buffer.borrow();
        let buf = buffer.as_slice();

        let mut first_byte_in_cell_content = page.header._cell_content_area;
        if first_byte_in_cell_content == 0 {
            first_byte_in_cell_content = u16::MAX;
        }

        let fragmented_free_bytes = page.header._num_frag_free_bytes;
        let free_block_pointer = page.header._first_freeblock_offset;
        let ncell = page.cells.len();

        // 8 + 4 == header end
        let first_cell = 8 + 4 + (2 * ncell) as u16;

        dbg!(first_byte_in_cell_content);
        dbg!(fragmented_free_bytes);
        let mut nfree = fragmented_free_bytes as usize + first_byte_in_cell_content as usize;

        dbg!(nfree);
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
                /* Freeblock not in ascending order */
                todo!("corrupted page ascending order");
            }
            // if( pc+size>(unsigned int)usableSize ){
            //   /* Last freeblock extends past page end */
            //   todo!("corrupted page last freeblock extends last page end");
            // }
        }

        // if( nFree>usableSize || nFree<iCellFirst ){
        //   return SQLITE_CORRUPT_PAGE(pPage);
        // }
        // pPage->nFree = (u16)(nFree - iCellFirst);

        // don't count header and cell pointers?
        nfree = nfree - first_cell as usize;
        return nfree as u16;
    }
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
