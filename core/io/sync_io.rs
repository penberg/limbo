use crate::{DatabaseRef, IO};

use std::io::{Read, Seek};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use anyhow::Result;

/// Synchronous I/O using traditional read() and write() system calls.
pub struct SyncIO {
    inner: RefCell<SyncIOInner>,
}

struct SyncIOInner {
    db_refs: usize,
    db_files: HashMap<DatabaseRef, File>,
}

impl IO for SyncIO {
    fn open(&self, path: &str) -> Result<DatabaseRef> {
        let file = std::fs::File::open(path)?;
        let mut inner = self.inner.borrow_mut();
        let db_ref = inner.db_refs;
        inner.db_refs += 1;
        inner.db_files.insert(db_ref, file);
        Ok(db_ref)
    }

    fn get(&self, database_ref: DatabaseRef, page_idx: usize, buf: &mut [u8]) -> Result<()> {
        let page_size = buf.len();
        assert!(page_idx > 0);
        assert!(page_size >= 512);
        assert!(page_size <= 65536);
        assert!((page_size & (page_size - 1)) == 0);
        let mut inner = self.inner.borrow_mut();
        let file = inner.db_files.get_mut(&database_ref).unwrap();
        let pos = (page_idx - 1) * page_size;
        file.seek(std::io::SeekFrom::Start(pos as u64))?;
        file.read_exact(buf)?;
        Ok(())
    }
}

impl SyncIO {
    pub fn new() -> Self {
        Self {
            inner: RefCell::new(SyncIOInner {
                db_refs: 0,
                db_files: HashMap::new(),
            }),
        }
    }
}
