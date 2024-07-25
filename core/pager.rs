use crate::buffer_pool::BufferPool;
use crate::sqlite3_ondisk::BTreePage;
use crate::sqlite3_ondisk::{self, DatabaseHeader};
use crate::{PageSource, Result};
use log::trace;
use sieve_cache::SieveCache;
use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

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

impl Default for Page {
    fn default() -> Self {
        Self::new()
    }
}

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

pub struct PageCache<K: Eq + Hash + Clone, V> {
    cache: SieveCache<K, V>,
}

impl<K: Eq + Hash + Clone, V> PageCache<K, V> {
    pub fn new(cache: SieveCache<K, V>) -> Self {
        Self { cache }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.cache.insert(key, value);
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.cache.get(key)
    }

    pub fn resize(&mut self, capacity: usize) {
        self.cache = SieveCache::new(capacity).unwrap();
    }
}

/// The pager interface implements the persistence layer by providing access
/// to pages of the database file, including caching, concurrency control, and
/// transaction management.
pub struct Pager {
    pub page_source: PageSource,
    page_cache: RefCell<PageCache<usize, Rc<Page>>>,
    buffer_pool: Rc<BufferPool>,
    pub io: Arc<dyn crate::io::IO>,
}

impl Pager {
    pub fn begin_open(page_source: &PageSource) -> Result<Rc<RefCell<DatabaseHeader>>> {
        sqlite3_ondisk::begin_read_database_header(page_source)
    }

    pub fn finish_open(
        db_header: Rc<RefCell<DatabaseHeader>>,
        page_source: PageSource,
        io: Arc<dyn crate::io::IO>,
    ) -> Result<Self> {
        let db_header = db_header.borrow();
        let page_size = db_header.page_size as usize;
        let buffer_pool = Rc::new(BufferPool::new(page_size));
        let page_cache = RefCell::new(PageCache::new(SieveCache::new(10).unwrap()));
        Ok(Self {
            page_source,
            buffer_pool,
            page_cache,
            io,
        })
    }

    pub fn read_page(&self, page_idx: usize) -> Result<Rc<Page>> {
        trace!("read_page(page_idx = {})", page_idx);
        let mut page_cache = self.page_cache.borrow_mut();
        if let Some(page) = page_cache.get(&page_idx) {
            return Ok(page.clone());
        }
        let page = Rc::new(Page::new());
        page.set_locked();
        sqlite3_ondisk::begin_read_btree_page(
            &self.page_source,
            self.buffer_pool.clone(),
            page.clone(),
            page_idx,
        )?;
        page_cache.insert(page_idx, page.clone());
        Ok(page)
    }

    pub fn write_database_header(&self, header: &DatabaseHeader) {
        sqlite3_ondisk::begin_write_database_header(header, self).expect("failed to write header");
    }

    pub fn change_page_cache_size(&self, capacity: usize) {
        self.page_cache.borrow_mut().resize(capacity);
    }
}
