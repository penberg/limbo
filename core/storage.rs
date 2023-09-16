use crate::io::File;
use anyhow::Result;
use std::sync::Arc;

pub struct Storage {
    io: Arc<dyn StorageIO>,
}

impl Storage {
    pub fn from_io(io: Arc<dyn StorageIO>) -> Self {
        Self { io }
    }

    pub fn from_file(file: File) -> Self {
        Self {
            io: Arc::new(FileStorage::new(file)),
        }
    }

    pub fn get(&self, page_idx: usize, buf: &mut [u8]) -> Result<()> {
        self.io.get(page_idx, buf)
    }
}

pub trait StorageIO {
    fn get(&self, page_idx: usize, buf: &mut [u8]) -> Result<()>;
}

struct FileStorage {
    file: File,
}

impl StorageIO for FileStorage {
    fn get(&self, page_idx: usize, buf: &mut [u8]) -> Result<()> {
        let page_size = buf.len();
        assert!(page_idx > 0);
        assert!(page_size >= 512);
        assert!(page_size <= 65536);
        assert!((page_size & (page_size - 1)) == 0);
        let pos = (page_idx - 1) * page_size;
        self.file.pread(pos, buf)?;
        Ok(())
    }
}

impl FileStorage {
    pub fn new(file: File) -> Self {
        Self { file }
    }
}
