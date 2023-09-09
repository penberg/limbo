use anyhow::{Ok, Result};
use std::cell::RefCell;
use std::fs::File;
use std::io::{Read, Seek};

pub(crate) struct SyscallIO {
    file: RefCell<File>,
}

impl SyscallIO {
    pub(crate) fn open(path: &str) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        Ok(SyscallIO {
            file: RefCell::new(file),
        })
    }
}

impl super::PageIO for SyscallIO {
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
