use std::{cell::RefCell, rc::Rc};

use crate::{storage::pager::Page, Result};

/// Write-ahead log (WAL).
pub trait Wal {
    /// Begin a write transaction.
    fn begin_read_tx(&self) -> Result<()>;

    /// End a write transaction.
    fn end_read_tx(&self) -> Result<()>;

    /// Find the latest frame containing a page.
    fn find_frame(&self, page_id: u64) -> Result<Option<u64>>;

    /// Read a frame from the WAL.
    fn read_frame(&self, frame_id: u64, page: Rc<RefCell<Page>>) -> Result<()>;
}

#[cfg(feature = "fs")]
pub struct WalFile {}

#[cfg(feature = "fs")]
impl Wal for WalFile {
    /// Begin a write transaction.
    fn begin_read_tx(&self) -> Result<()> {
        Ok(())
    }

    /// End a write transaction.
    fn end_read_tx(&self) -> Result<()> {
        Ok(())
    }

    /// Find the latest frame containing a page.
    fn find_frame(&self, _page_id: u64) -> Result<Option<u64>> {
        Ok(None)
    }

    /// Read a frame from the WAL.
    fn read_frame(&self, _frame_id: u64, _page: Rc<RefCell<Page>>) -> Result<()> {
        todo!();
    }
}

#[cfg(feature = "fs")]
impl WalFile {
    pub fn new() -> Self {
        Self {}
    }
}
