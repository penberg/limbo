use crate::buffer_pool;
use crate::buffer_pool::BufferPool;
use crate::sqlite3_ondisk;
use crate::sqlite3_ondisk::BTreePage;
use crate::DatabaseRef;
use crate::IO;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct Pager {
    io: Arc<dyn IO>,
    database_ref: DatabaseRef,
    page_cache: Arc<Mutex<HashMap<usize, Arc<BTreePage>>>>,
    buffer_pool: Arc<Mutex<BufferPool>>,
}

impl Pager {
    pub fn open(io: Arc<dyn IO>, path: &str) -> Result<Self> {
        let database_ref = io.open(path)?;
        let db_header = sqlite3_ondisk::read_database_header(io.clone(), database_ref)?;
        let page_size = db_header.page_size as usize;
        let buffer_pool = Arc::new(Mutex::new(buffer_pool::BufferPool::new(page_size)));
        let page_cache = Arc::new(Mutex::new(HashMap::default()));
        Ok(Self {
            io,
            database_ref,
            buffer_pool,
            page_cache,
        })
    }

    pub fn read_page(&self, page_idx: usize) -> Result<Arc<BTreePage>> {
        {
            let page_cache = self.page_cache.lock().unwrap();
            if let Some(page) = page_cache.get(&page_idx) {
                return Ok(page.clone());
            }
        }
        let mut buffer_pool = self.buffer_pool.lock().unwrap();
        let page = sqlite3_ondisk::read_btree_page(
            self.io.clone(),
            self.database_ref,
            &mut buffer_pool,
            page_idx,
        )?;
        // TODO: Evict pages from the page cache when it's full.
        let mut page_cache = self.page_cache.lock().unwrap();
        page_cache.insert(page_idx, Arc::new(page));
        Ok(page_cache.get(&page_idx).unwrap().clone())
    }
}
