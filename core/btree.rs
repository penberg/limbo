use crate::pager::Pager;
use crate::sqlite3_ondisk::{BTreeCell, BTreePage, TableLeafCell};
use crate::types::Record;

use anyhow::Result;

use std::cell::{Ref, RefCell};
use std::sync::Arc;

pub struct Cursor {
    pager: Arc<Pager>,
    root_page: usize,
    page: RefCell<Option<Arc<BTreePage>>>,
    record: RefCell<Option<Record>>,
    cell_idx: usize,
}

impl Cursor {
    pub fn new(pager: Arc<Pager>, root_page: usize) -> Self {
        Self {
            pager,
            root_page,
            page: RefCell::new(None),
            record: RefCell::new(None),
            cell_idx: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.page.borrow().is_none()
    }

    pub fn rewind(&mut self) -> Result<()> {
        self.page
            .replace(Some(self.pager.read_page(self.root_page)?));
        let record = self.get_next_record()?;
        self.record.replace(record);
        Ok(())
    }

    pub fn next(&mut self) -> Result<Option<Record>> {
        let result = self.record.take();
        let next = self.get_next_record()?;
        self.record.replace(next);
        Ok(result)
    }

    pub fn wait_for_completion(&mut self) -> Result<()> {
        // TODO: Wait for pager I/O to complete
        Ok(())
    }

    pub fn record(&self) -> Result<Ref<Option<Record>>> {
        Ok(self.record.borrow())
    }

    pub fn has_record(&self) -> bool {
        self.record.borrow().is_some()
    }

    fn get_next_record(&mut self) -> Result<Option<Record>> {
        match self.page.borrow_mut().as_mut() {
            Some(page) => {
                if self.cell_idx < page.cells.len() {
                    let cell = &page.cells[self.cell_idx];
                    self.cell_idx += 1;
                    match &cell {
                        BTreeCell::TableInteriorCell(_) => {
                            todo!();
                        }
                        BTreeCell::TableLeafCell(TableLeafCell { _rowid, _payload }) => {
                            let record = crate::sqlite3_ondisk::read_record(_payload)?;
                            Ok(Some(record))
                        }
                    }
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }
}
