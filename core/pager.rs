use crate::buffer_pool;
use crate::buffer_pool::BufferPool;
use crate::sqlite3_ondisk;
use crate::sqlite3_ondisk::BTreePage;
use crate::DatabaseRef;
use crate::IO;
use anyhow::Result;
use std::sync::{Arc, Mutex};

pub struct Pager {
    io: Arc<dyn IO>,
    database_ref: DatabaseRef,
    buffer_pool: Arc<Mutex<BufferPool>>,
}

impl Pager {
    pub fn open(io: Arc<dyn IO>, path: &str) -> Result<Self> {
        let database_ref = io.open(path)?;
        let db_header = sqlite3_ondisk::read_database_header(io.clone(), database_ref)?;
        let page_size = db_header.page_size as usize;
        let buffer_pool = Arc::new(Mutex::new(buffer_pool::BufferPool::new(page_size)));
        Ok(Self {
            io,
            database_ref,
            buffer_pool,
        })
    }

    pub fn read_page(&self, page_idx: usize) -> Result<BTreePage> {
        let mut buffer_pool = self.buffer_pool.lock().unwrap();
        sqlite3_ondisk::read_btree_page(
            self.io.clone(),
            self.database_ref,
            &mut buffer_pool,
            page_idx,
        )
    }
}
