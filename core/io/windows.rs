use crate::{Completion, File, Result, WriteCompletion, IO};
use log::trace;
use std::cell::RefCell;
use std::io::{Read, Seek, Write};
use std::rc::Rc;

pub struct WindowsIO {}

impl WindowsIO {
    pub fn new() -> Result<Self> {
        Ok(Self {})
    }
}

impl IO for WindowsIO {
    fn open_file(&self, path: &str) -> Result<Rc<dyn File>> {
        trace!("open_file(path = {})", path);
        let file = std::fs::File::open(path)?;
        Ok(Rc::new(WindowsFile {
            file: RefCell::new(file),
        }))
    }

    fn run_once(&self) -> Result<()> {
        Ok(())
    }
}

pub struct WindowsFile {
    file: RefCell<std::fs::File>,
}

impl File for WindowsFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        unimplemented!()
    }

    fn unlock_file(&self) -> Result<()> {
        unimplemented!()
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
        _c: Rc<WriteCompletion>,
    ) -> Result<()> {
        let mut file = self.file.borrow_mut();
        file.seek(std::io::SeekFrom::Start(pos as u64))?;
        let buf = buffer.borrow();
        let buf = buf.as_slice();
        file.write_all(buf)?;
        Ok(())
    }
}
