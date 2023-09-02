use crate::buffer_pool;
use crate::buffer_pool::BufferPool;
use crate::sqlite3_ondisk;
use crate::sqlite3_ondisk::BTreePage;
use crate::DatabaseRef;
use crate::IO;
use concurrent_lru::unsharded::LruCache;
use std::sync::{Arc, Mutex};

pub struct Pager {
    io: Arc<dyn IO>,
    database_ref: DatabaseRef,
    page_cache: LruCache<usize, Arc<BTreePage>>,
    buffer_pool: Arc<Mutex<BufferPool>>,
}

impl Pager {
    pub fn open(io: Arc<dyn IO>, path: &str) -> anyhow::Result<Self> {
        let database_ref = io.open(path)?;
        let db_header = sqlite3_ondisk::read_database_header(io.clone(), database_ref)?;
        let page_size = db_header.page_size as usize;
        let buffer_pool = Arc::new(Mutex::new(buffer_pool::BufferPool::new(page_size)));
        let page_cache = LruCache::new(10);
        Ok(Self {
            io,
            database_ref,
            buffer_pool,
            page_cache,
        })
    }

    pub fn read_page(&self, page_idx: usize) -> anyhow::Result<Arc<BTreePage>> {
        let handle = self.page_cache.get_or_try_init(page_idx, 1, |idx| {
            let mut buffer_pool = self.buffer_pool.lock().unwrap();
            let page = sqlite3_ondisk::read_btree_page(
                self.io.clone(),
                self.database_ref,
                &mut buffer_pool,
                page_idx,
            )
            .unwrap();
            Ok::<Arc<BTreePage>, anyhow::Error>(Arc::new(page))
        })?;
        Ok(handle.value().clone())
    }
}
