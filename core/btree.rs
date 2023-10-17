use crate::pager::Pager;
use crate::sqlite3_ondisk::{BTreeCell, TableInteriorCell, TableLeafCell};
use crate::types::Record;

use anyhow::Result;

use std::cell::{Ref, RefCell};
use std::sync::Arc;

pub struct MemPage {
    parent: Option<Arc<MemPage>>,
    page_idx: usize,
    cell_idx: RefCell<usize>,
}

impl MemPage {
    pub fn new(parent: Option<Arc<MemPage>>, page_idx: usize, cell_idx: usize) -> Self {
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

pub struct Cursor {
    pager: Arc<Pager>,
    root_page: usize,
    page: RefCell<Option<Arc<MemPage>>>,
    rowid: RefCell<Option<u64>>,
    record: RefCell<Option<Record>>,
}

impl Cursor {
    pub fn new(pager: Arc<Pager>, root_page: usize) -> Self {
        Self {
            pager,
            root_page,
            page: RefCell::new(None),
            rowid: RefCell::new(None),
            record: RefCell::new(None),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.page.borrow().is_none()
    }

    pub fn rewind(&mut self) -> Result<()> {
        let mem_page = MemPage::new(None, self.root_page, 0);
        self.page.replace(Some(Arc::new(mem_page)));
        let (rowid, next) = self.get_next_record()?;
        self.rowid.replace(rowid);
        self.record.replace(next);
        Ok(())
    }

    pub fn next(&mut self) -> Result<Option<Record>> {
        let result = self.record.take();
        let (rowid, next) = self.get_next_record()?;
        self.rowid.replace(rowid);
        self.record.replace(next);
        Ok(result)
    }

    pub fn wait_for_completion(&mut self) -> Result<()> {
        // TODO: Wait for pager I/O to complete
        Ok(())
    }

    pub fn rowid(&self) -> Result<Ref<Option<u64>>> {
        Ok(self.rowid.borrow())
    }

    pub fn record(&self) -> Result<Ref<Option<Record>>> {
        Ok(self.record.borrow())
    }

    pub fn has_record(&self) -> bool {
        self.record.borrow().is_some()
    }

    fn get_next_record(&mut self) -> Result<(Option<u64>, Option<Record>)> {
        loop {
            let mem_page = {
                let mem_page = self.page.borrow();
                let mem_page = mem_page.as_ref().unwrap();
                mem_page.clone()
            };
            let page_idx = mem_page.page_idx;
            let page = self.pager.read_page(page_idx)?;
            if mem_page.cell_idx() >= page.cells.len() {
                match mem_page.parent {
                    Some(ref parent) => {
                        self.page.replace(Some(parent.clone()));
                        continue;
                    }
                    None => match page.header.right_most_pointer {
                        Some(right_most_pointer) => {
                            let mem_page = MemPage::new(None, right_most_pointer as usize, 0);
                            self.page.replace(Some(Arc::new(mem_page)));
                            continue;
                        }
                        None => {
                            return Ok((None, None));
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
                    self.page.replace(Some(Arc::new(mem_page)));
                    continue;
                }
                BTreeCell::TableLeafCell(TableLeafCell { _rowid, _payload }) => {
                    mem_page.advance();
                    let record = crate::sqlite3_ondisk::read_record(_payload)?;
                    return Ok((Some(*_rowid), Some(record)));
                }
            }
        }
    }
}
