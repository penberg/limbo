use anyhow::{Ok, Result};
use std::cell::RefCell;
use std::io::{Read, Seek};
use std::sync::Arc;

pub(crate) struct DarwinIO {}

impl super::IO for DarwinIO {
    fn open(&self, path: &str) -> Result<super::File> {
        let file = self.open_file(path)?;
        Ok(super::File { io: Arc::new(file) })
    }
}

impl DarwinIO {
    pub(crate) fn new() -> Result<Self> {
        Ok(DarwinIO {})
    }

    pub(crate) fn open_file(&self, path: &str) -> Result<File> {
        let file = std::fs::File::open(path)?;
        Ok(File {
            file: RefCell::new(file),
        })
    }

    pub(crate) fn run_once(&self) -> Result<()> {
        Ok(())
    }
}

pub(crate) struct File {
    file: RefCell<std::fs::File>,
}

impl super::PageIO for File {
    fn get(&self, page_idx: usize, buf: &mut [u8]) -> Result<()> {
        let page_size = buf.len();
        assert!(page_idx > 0);
        assert!(page_size >= 512);
        assert!(page_size <= 65536);
        assert!((page_size & (page_size - 1)) == 0);
        let pos = (page_idx - 1) * page_size;
        let mut file = self.file.borrow_mut();
        file.seek(std::io::SeekFrom::Start(pos as u64))?;
        file.read_exact(buf)?;
        Ok(())
    }
}
