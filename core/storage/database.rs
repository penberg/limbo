use crate::{error::LimboError, io::Completion, Buffer, Result};
use std::{cell::RefCell, rc::Rc};

/// DatabaseStorage is an interface a database file that consists of pages.
///
/// The purpose of this trait is to abstract the upper layers of Limbo from
/// the storage medium. A database can either be a file on disk, like in SQLite,
/// or something like a remote page server service.
pub trait DatabaseStorage {
    fn read_page(&self, page_idx: usize, c: Rc<Completion>) -> Result<()>;
    fn write_page(
        &self,
        page_idx: usize,
        buffer: Rc<RefCell<Buffer>>,
        c: Rc<Completion>,
    ) -> Result<()>;
    fn sync(&self, c: Rc<Completion>) -> Result<()>;
}

#[cfg(feature = "fs")]
pub struct FileStorage {
    file: Rc<dyn crate::io::File>,
}

#[cfg(feature = "fs")]
impl DatabaseStorage for FileStorage {
    fn read_page(&self, page_idx: usize, c: Rc<Completion>) -> Result<()> {
        let r = match &(*c) {
            Completion::Read(r) => r,
            _ => unreachable!(),
        };
        let size = r.buf().len();
        assert!(page_idx > 0);
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            return Err(LimboError::NotADB);
        }
        let pos = (page_idx - 1) * size;
        self.file.pread(pos, c)?;
        Ok(())
    }

    fn write_page(
        &self,
        page_idx: usize,
        buffer: Rc<RefCell<Buffer>>,
        c: Rc<Completion>,
    ) -> Result<()> {
        let buffer_size = buffer.borrow().len();
        assert!(buffer_size >= 512);
        assert!(buffer_size <= 65536);
        assert!((buffer_size & (buffer_size - 1)) == 0);
        let pos = (page_idx - 1) * buffer_size;
        self.file.pwrite(pos, buffer, c)?;
        Ok(())
    }

    fn sync(&self, c: Rc<Completion>) -> Result<()> {
        self.file.sync(c)
    }
}

#[cfg(feature = "fs")]
impl FileStorage {
    pub fn new(file: Rc<dyn crate::io::File>) -> Self {
        Self { file }
    }
}
