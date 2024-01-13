use crate::io::Completion;
use anyhow::Result;
use std::sync::Arc;

pub struct Storage {
    io: Arc<dyn StorageIO>,
}

impl Storage {
    pub fn from_io(io: Arc<dyn StorageIO>) -> Self {
        Self { io }
    }

    #[cfg(feature = "fs")]
    pub fn from_file(file: crate::io::File) -> Self {
        Self {
            io: Arc::new(FileStorage::new(file)),
        }
    }

    pub fn get(&self, page_idx: usize, c: Arc<Completion>) -> Result<()> {
        self.io.get(page_idx, c)
    }
}

pub trait StorageIO {
    fn get(&self, page_idx: usize, c: Arc<Completion>) -> Result<()>;
}

#[cfg(feature = "fs")]
struct FileStorage {
    file: crate::io::File,
}

#[cfg(feature = "fs")]
impl StorageIO for FileStorage {
    fn get(&self, page_idx: usize, c: Arc<Completion>) -> Result<()> {
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
    pub fn new(file: crate::io::File) -> Self {
        Self { file }
    }
}
