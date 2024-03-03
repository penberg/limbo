#[cfg(feature = "fs")]
use crate::io::File;
use crate::{
    io::{Completion, WriteCompletion},
    Buffer,
};
use anyhow::Result;
use std::{cell::RefCell, rc::Rc};

pub struct PageSource {
    io: Rc<dyn PageIO>,
}

impl Clone for PageSource {
    fn clone(&self) -> Self {
        Self {
            io: self.io.clone(),
        }
    }
}

impl PageSource {
    pub fn from_io(io: Rc<dyn PageIO>) -> Self {
        Self { io }
    }

    #[cfg(feature = "fs")]
    pub fn from_file(file: Rc<dyn File>) -> Self {
        Self {
            io: Rc::new(FileStorage::new(file)),
        }
    }

    pub fn get(&self, page_idx: usize, c: Rc<Completion>) -> Result<()> {
        self.io.get(page_idx, c)
    }

    pub fn write(
        &self,
        page_idx: usize,
        buffer: Rc<RefCell<Buffer>>,
        c: Rc<WriteCompletion>,
    ) -> Result<()> {
        self.io.write(page_idx, buffer, c)
    }
}

pub trait PageIO {
    fn get(&self, page_idx: usize, c: Rc<Completion>) -> Result<()>;
    fn write(
        &self,
        page_idx: usize,
        buffer: Rc<RefCell<Buffer>>,
        c: Rc<WriteCompletion>,
    ) -> Result<()>;
}

#[cfg(feature = "fs")]
struct FileStorage {
    file: Rc<dyn crate::io::File>,
}

#[cfg(feature = "fs")]
impl PageIO for FileStorage {
    fn get(&self, page_idx: usize, c: Rc<Completion>) -> Result<()> {
        let page_size = c.buf().len();
        assert!(page_idx > 0);
        assert!(page_size >= 512);
        assert!(page_size <= 65536);
        assert!((page_size & (page_size - 1)) == 0);
        let pos = (page_idx - 1) * page_size;
        self.file.pread(pos, c)?;
        Ok(())
    }

    fn write(
        &self,
        page_idx: usize,
        buffer: Rc<RefCell<Buffer>>,
        c: Rc<WriteCompletion>,
    ) -> Result<()> {
        let buffer_size = buffer.borrow().len();
        assert!(buffer_size >= 512);
        assert!(buffer_size <= 65536);
        assert!((buffer_size & (buffer_size - 1)) == 0);
        self.file.pwrite(page_idx, buffer, c)?;
        Ok(())
    }
}

#[cfg(feature = "fs")]
impl FileStorage {
    pub fn new(file: Rc<dyn crate::io::File>) -> Self {
        Self { file }
    }
}
