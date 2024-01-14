use crate::buffer_pool::BufferPool;
use crate::sqlite3_ondisk;
use crate::sqlite3_ondisk::BTreePage;
use crate::PageSource;
use concurrent_lru::unsharded::LruCache;
use log::trace;
use std::sync::RwLock;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

pub struct Page {
    flags: AtomicUsize,
    pub contents: RwLock<Option<BTreePage>>,
}

/// Page is up-to-date.
const PAGE_UPTODATE: usize = 0b001;
/// Page is locked for I/O to prevent concurrent access.
const PAGE_LOCKED: usize = 0b010;
/// Page had an I/O error.
const PAGE_ERROR: usize = 0b100;

impl Page {
    pub fn new() -> Page {
        Page {
            flags: AtomicUsize::new(0),
            contents: RwLock::new(None),
        }
    }

    pub fn is_uptodate(&self) -> bool {
        self.flags.load(Ordering::SeqCst) & PAGE_UPTODATE != 0
    }

    pub fn set_uptodate(&self) {
        self.flags.fetch_or(PAGE_UPTODATE, Ordering::SeqCst);
    }

    pub fn clear_uptodate(&self) {
        self.flags.fetch_and(!PAGE_UPTODATE, Ordering::SeqCst);
    }

    pub fn is_locked(&self) -> bool {
        self.flags.load(Ordering::SeqCst) & PAGE_LOCKED != 0
    }

    pub fn set_locked(&self) {
        self.flags.fetch_or(PAGE_LOCKED, Ordering::SeqCst);
    }

    pub fn clear_locked(&self) {
        self.flags.fetch_and(!PAGE_LOCKED, Ordering::SeqCst);
    }

    pub fn is_error(&self) -> bool {
        self.flags.load(Ordering::SeqCst) & PAGE_ERROR != 0
    }

    pub fn set_error(&self) {
        self.flags.fetch_or(PAGE_ERROR, Ordering::SeqCst);
    }

    pub fn clear_error(&self) {
        self.flags.fetch_and(!PAGE_ERROR, Ordering::SeqCst);
    }
}

pub struct Pager {
    page_source: PageSource,
    page_cache: LruCache<usize, Arc<Page>>,
    buffer_pool: Arc<BufferPool>,
}

impl Pager {
    pub fn open(page_source: PageSource) -> anyhow::Result<Self> {
        let db_header = sqlite3_ondisk::read_database_header(&page_source)?;
        let page_size = db_header.page_size as usize;
        let buffer_pool = Arc::new(BufferPool::new(page_size));
        let page_cache = LruCache::new(10);
        Ok(Self {
            page_source,
            buffer_pool,
            page_cache,
        })
    }

    pub fn read_page(&self, page_idx: usize) -> anyhow::Result<Arc<Page>> {
        trace!("read_page(page_idx = {})", page_idx);
        let handle = self.page_cache.get_or_try_init(page_idx, 1, |_idx| {
            let page = Arc::new(Page::new());
            page.set_locked();
            sqlite3_ondisk::begin_read_btree_page(
                &self.page_source,
                self.buffer_pool.clone(),
                page.clone(),
                page_idx,
            )
            .unwrap();
            Ok::<Arc<Page>, anyhow::Error>(page)
        })?;
        Ok(handle.value().clone())
    }
}
