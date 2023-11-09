use super::Completion;
use anyhow::{Ok, Result};
use std::cell::RefCell;
use std::io::{Read, Seek};

pub struct IO {}

impl IO {
    pub fn new() -> Result<Self> {
        Ok(Self {})
    }

    pub fn open_file(&self, path: &str) -> Result<File> {
        let file = std::fs::File::open(path)?;
        Ok(File {
            file: RefCell::new(file),
        })
    }

    pub(crate) fn run_once(&self) -> Result<()> {
        Ok(())
    }
}

pub struct File {
    file: RefCell<std::fs::File>,
}

impl File {
    pub fn pread(&self, pos: usize, c: &mut Completion) -> Result<()> {
        let mut file = self.file.borrow_mut();
        file.seek(std::io::SeekFrom::Start(pos as u64))?;
        file.read_exact(c.buf.as_mut_slice())?;
        Ok(())
    }
}