use crate::pager::Pager;
use crate::sqlite3_ondisk::{BTreeCell, TableInteriorCell, TableLeafCell};
use crate::types::{Cursor, CursorResult, OwnedRecord};

use anyhow::Result;

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
}

impl BTreeCursor {
    pub fn new(pager: Rc<Pager>, root_page: usize) -> Self {
        Self {
            pager,
            root_page,
            page: RefCell::new(None),
            rowid: RefCell::new(None),
            record: RefCell::new(None),
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
                BTreeCell::TableLeafCell(TableLeafCell { _rowid, _payload }) => {
                    mem_page.advance();
                    let record = crate::sqlite3_ondisk::read_record(_payload)?;
                    return Ok(CursorResult::Ok((Some(*_rowid), Some(record))));
                }
            }
        }
    }
}

impl Cursor for BTreeCursor {
    fn is_empty(&self) -> bool {
        self.page.borrow().is_none()
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

    fn rowid(&self) -> Result<Ref<Option<u64>>> {
        Ok(self.rowid.borrow())
    }

    fn record(&self) -> Result<Ref<Option<OwnedRecord>>> {
        Ok(self.record.borrow())
    }

    fn has_record(&self) -> bool {
        self.record.borrow().is_some()
    }
}
