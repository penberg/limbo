use crate::io::Completion;
#[cfg(feature = "fs")]
use crate::io::File;
use anyhow::Result;
use std::rc::Rc;

pub struct PageSource {
    io: Rc<dyn PageIO>,
}

impl PageSource {
    pub fn from_io(io: Rc<dyn PageIO>) -> Self {
        Self { io }
    }

    #[cfg(feature = "fs")]
    pub fn from_file(file: Box<dyn File>) -> Self {
        Self {
            io: Rc::new(FileStorage::new(file)),
        }
    }

    pub fn get(&self, page_idx: usize, c: Rc<Completion>) -> Result<()> {
        self.io.get(page_idx, c)
    }
}

pub trait PageIO {
    fn get(&self, page_idx: usize, c: Rc<Completion>) -> Result<()>;
}

#[cfg(feature = "fs")]
struct FileStorage {
    file: Box<dyn crate::io::File>,
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
}

#[cfg(feature = "fs")]
impl FileStorage {
    pub fn new(file: Box<dyn crate::io::File>) -> Self {
        Self { file }
    }
}
