use super::{Completion, File, IO};
use anyhow::{Ok, Result};
use std::sync::Arc;
use std::cell::RefCell;
use std::io::{Read, Seek};
use log::trace;

pub struct DarwinIO {}

impl DarwinIO {
    pub fn new() -> Result<Self> {
        Ok(Self {})
    }
}

impl IO for DarwinIO {
    fn open_file(&self, path: &str) -> Result<Box<dyn File>> {
        trace!("open_file(path = {})", path);
        let file = std::fs::File::open(path)?;
        Ok(Box::new(DarwinFile {
            file: RefCell::new(file),
        }))
    }

    fn run_once(&self) -> Result<()> {
        Ok(())
    }
}

pub struct DarwinFile {
    file: RefCell<std::fs::File>,
}

impl File for DarwinFile {
    fn pread(&self, pos: usize, c: Arc<Completion>) -> Result<()> {
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
}
