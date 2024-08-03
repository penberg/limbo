use std::{cell::RefCell, rc::Rc, sync::Arc};

use crate::io::{File, IO};
use crate::{storage::pager::Page, Result};

use super::sqlite3_ondisk;

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
pub struct WalFile {
    io: Arc<dyn crate::io::IO>,
    wal_path: String,
    file: RefCell<Option<Rc<dyn File>>>,
    wal_header: RefCell<Option<Rc<RefCell<sqlite3_ondisk::WalHeader>>>>,
}

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
        self.ensure_init()?;
        Ok(None)
    }

    /// Read a frame from the WAL.
    fn read_frame(&self, _frame_id: u64, _page: Rc<RefCell<Page>>) -> Result<()> {
        todo!();
    }
}

#[cfg(feature = "fs")]
impl WalFile {
    pub fn new(io: Arc<dyn IO>, wal_path: String) -> Self {
        Self {
            io,
            wal_path,
            file: RefCell::new(None),
            wal_header: RefCell::new(None),
        }
    }

    fn ensure_init(&self) -> Result<()> {
        if self.file.borrow().is_none() {
            if let Ok(file) = self.io.open_file(&self.wal_path) {
                *self.file.borrow_mut() = Some(file.clone());
                let wal_header = sqlite3_ondisk::begin_read_wal_header(file)?;
                // TODO: Return a completion instead.
                self.io.run_once()?;
                self.wal_header.replace(Some(wal_header));
            }
        }
        Ok(())
    }
}
