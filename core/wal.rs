use std::{cell::RefCell, rc::Rc};

use crate::{pager::Page, Result};

/// Write-ahead log (WAL).
pub struct Wal {}

impl Wal {
    pub fn new() -> Self {
        Self {}
    }

    /// Begin a write transaction.
    pub fn begin_read_tx(&self) -> Result<()> {
        Ok(())
    }

    /// End a write transaction.
    pub fn end_read_tx(&self) -> Result<()> {
        Ok(())
    }

    /// Find the latest frame containing a page.
    pub fn find_frame(&self, _page_id: u64) -> Result<Option<u64>> {
        Ok(None)
    }

    /// Read a frame from the WAL.
    pub fn read_frame(&self, _frame_id: u64, _page: Rc<RefCell<Page>>) -> Result<()> {
        todo!();
    }
}
