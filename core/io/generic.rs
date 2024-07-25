use crate::{Completion, File, Result, WriteCompletion, IO};
use log::trace;
use std::cell::RefCell;
use std::io::{Read, Seek, Write};
use std::rc::Rc;

pub struct GenericIO {}

impl GenericIO {
    pub fn new() -> Result<Self> {
        Ok(Self {})
    }
}

impl IO for GenericIO {
    fn open_file(&self, path: &str) -> Result<Rc<dyn File>> {
        trace!("open_file(path = {})", path);
        let file = std::fs::File::open(path)?;
        Ok(Rc::new(GenericFile {
            file: RefCell::new(file),
        }))
    }

    fn run_once(&self) -> Result<()> {
        Ok(())
    }
}

pub struct GenericFile {
    file: RefCell<std::fs::File>,
}

impl File for GenericFile {
    // Since we let the OS handle the locking, file locking is not supported on the generic IO implementation
    // No-op implementation allows compilation but provides no actual file locking.
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(&self, pos: usize, c: Rc<Completion>) -> Result<()> {
        let mut file = self.file.borrow_mut();
        file.seek(std::io::SeekFrom::Start(pos as u64))?;
        {
            let mut buf = c.buf_mut();
            let buf = buf.as_mut_slice();
            file.read_exact(buf)?;
        }
        c.complete();
        Ok(())
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Rc<RefCell<crate::Buffer>>,
        c: Rc<WriteCompletion>,
    ) -> Result<()> {
        let mut file = self.file.borrow_mut();
        file.seek(std::io::SeekFrom::Start(pos as u64))?;
        let buf = buffer.borrow();
        let buf = buf.as_slice();
        file.write_all(buf)?;
        Ok(())
    }
}

impl Drop for GenericFile {
    fn drop(&mut self) {
        self.unlock_file().expect("Failed to unlock file");
    }
}