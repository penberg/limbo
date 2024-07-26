use crate::buffer_pool::BufferPool;
use crate::sqlite3_ondisk::PageContent;
use crate::sqlite3_ondisk::{self, DatabaseHeader};
use crate::{PageSource, Result};
use log::trace;
use sieve_cache::SieveCache;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::ptr::{drop_in_place, NonNull};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

pub struct Page {
    flags: AtomicUsize,
    pub contents: RwLock<Option<PageContent>>,
    pub id: usize,
}

/// Page is up-to-date.
const PAGE_UPTODATE: usize = 0b001;
/// Page is locked for I/O to prevent concurrent access.
const PAGE_LOCKED: usize = 0b010;
/// Page had an I/O error.
const PAGE_ERROR: usize = 0b100;
/// Page is dirty. Flush needed.
const PAGE_DIRTY: usize = 0b1000;

impl Default for Page {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Page {
    pub fn new(id: usize) -> Page {
        Page {
            flags: AtomicUsize::new(0),
            contents: RwLock::new(None),
            id,
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

    pub fn is_dirty(&self) -> bool {
        self.flags.load(Ordering::SeqCst) & PAGE_DIRTY != 0
    }

    pub fn set_dirty(&self) {
        self.flags.fetch_or(PAGE_DIRTY, Ordering::SeqCst);
    }

    pub fn clear_dirty(&self) {
        self.flags.fetch_and(!PAGE_DIRTY, Ordering::SeqCst);
    }
}

struct PageCacheEntry {
    key: usize,
    page: Rc<RefCell<Page>>,
    prev: Option<NonNull<PageCacheEntry>>,
    next: Option<NonNull<PageCacheEntry>>,
}

impl PageCacheEntry {
    fn into_non_null(&mut self) -> NonNull<PageCacheEntry> {
        NonNull::new(&mut *self).unwrap()
    }
}

struct DumbLruPageCache {
    capacity: usize,
    map: RefCell<HashMap<usize, NonNull<PageCacheEntry>>>,
    head: RefCell<Option<NonNull<PageCacheEntry>>>,
    tail: RefCell<Option<NonNull<PageCacheEntry>>>,
}

impl DumbLruPageCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity,
            map: RefCell::new(HashMap::new()),
            head: RefCell::new(None),
            tail: RefCell::new(None),
        }
    }

    pub fn insert(&mut self, key: usize, value: Rc<RefCell<Page>>) {
        self.delete(key);
        let mut entry = Box::new(PageCacheEntry {
            key: key,
            next: None,
            prev: None,
            page: value,
        });
        self.touch(&mut entry);

        if self.map.borrow().len() >= self.capacity {
            self.pop_if_not_dirty();
        }
        let b = Box::into_raw(entry);
        let as_non_null = NonNull::new(b).unwrap();
        self.map.borrow_mut().insert(key, as_non_null);
    }

    pub fn delete(&mut self, key: usize) {
        let ptr = self.map.borrow_mut().remove(&key);
        if ptr.is_none() {
            return;
        }
        let mut ptr = ptr.unwrap();
        {
            let ptr = unsafe { ptr.as_mut() };
            self.detach(ptr);
        }
        unsafe { drop_in_place(ptr.as_ptr()) };
    }

    fn get_ptr(&mut self, key: usize) -> Option<NonNull<PageCacheEntry>> {
        let m = self.map.borrow_mut();
        let ptr = m.get(&key);
        match ptr {
            Some(v) => Some(*v),
            None => None,
        }
    }

    pub fn get(&mut self, key: &usize) -> Option<Rc<RefCell<Page>>> {
        let ptr = self.get_ptr(*key);
        if ptr.is_none() {
            return None;
        }
        let ptr = unsafe { ptr.unwrap().as_mut() };
        let page = ptr.page.clone();
        self.detach(ptr);
        self.touch(ptr);
        return Some(page);
    }

    pub fn resize(&mut self, capacity: usize) {
        let _ = capacity;
        todo!();
    }

    fn detach(&mut self, entry: &mut PageCacheEntry) {
        let mut current = entry.into_non_null();

        let (next, prev) = unsafe {
            let c = current.as_mut();
            let next = c.next;
            let prev = c.prev;
            c.prev = None;
            c.next = None;
            (next, prev)
        };

        // detach
        match (prev, next) {
            (None, None) => {}
            (None, Some(_)) => todo!(),
            (Some(p), None) => {
                self.tail = RefCell::new(Some(p));
            }
            (Some(mut p), Some(mut n)) => unsafe {
                let p_mut = p.as_mut();
                p_mut.next = Some(n);
                let n_mut = n.as_mut();
                n_mut.prev = Some(p);
            },
        };
    }

    fn touch(&mut self, entry: &mut PageCacheEntry) {
        let mut current = entry.into_non_null();
        unsafe {
            let c = current.as_mut();
            c.next = *self.head.borrow();
        }

        if let Some(mut head) = *self.head.borrow_mut() {
            unsafe {
                let head = head.as_mut();
                head.prev = Some(current);
            }
        }
    }

    fn pop_if_not_dirty(&mut self) {
        let tail = *self.tail.borrow();
        if tail.is_none() {
            return;
        }
        let tail = unsafe { tail.unwrap().as_mut() };
        if tail.page.borrow().is_dirty() {
            // TODO: drop from another clean entry?
            return;
        }
        self.detach(tail);
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
    /// Source of the database pages.
    pub page_source: PageSource,
    page_cache: RefCell<DumbLruPageCache>,
    /// Buffer pool for temporary data storage.
    buffer_pool: Rc<BufferPool>,
    /// I/O interface for input/output operations.
    pub io: Arc<dyn crate::io::IO>,
    dirty_pages: Rc<RefCell<Vec<Rc<RefCell<Page>>>>>,
}

impl Pager {
    /// Begins opening a database by reading the database header.
    pub fn begin_open(page_source: &PageSource) -> Result<Rc<RefCell<DatabaseHeader>>> {
        sqlite3_ondisk::begin_read_database_header(page_source)
    }

    /// Completes opening a database by initializing the Pager with the database header.
    pub fn finish_open(
        db_header: Rc<RefCell<DatabaseHeader>>,
        page_source: PageSource,
        io: Arc<dyn crate::io::IO>,
    ) -> Result<Self> {
        let db_header = db_header.borrow();
        let page_size = db_header.page_size as usize;
        let buffer_pool = Rc::new(BufferPool::new(page_size));
        let page_cache = RefCell::new(DumbLruPageCache::new(10));
        Ok(Self {
            page_source,
            buffer_pool,
            page_cache,
            io,
            dirty_pages: Rc::new(RefCell::new(Vec::new())),
        })
    }

    /// Reads a page from the database.
    pub fn read_page(&self, page_idx: usize) -> crate::Result<Rc<RefCell<Page>>> {
        trace!("read_page(page_idx = {})", page_idx);
        let mut page_cache = self.page_cache.borrow_mut();
        if let Some(page) = page_cache.get(&page_idx) {
            return Ok(page.clone());
        }
        let page = Rc::new(RefCell::new(Page::new(page_idx)));
        page.borrow().set_locked();
        sqlite3_ondisk::begin_read_page(
            &self.page_source,
            self.buffer_pool.clone(),
            page.clone(),
            page_idx,
        )?;
        page_cache.insert(page_idx, page.clone());
        Ok(page)
    }

    /// Writes the database header.
    pub fn write_database_header(&self, header: &DatabaseHeader) {
        sqlite3_ondisk::begin_write_database_header(header, self).expect("failed to write header");
    }

    /// Changes the size of the page cache.
    pub fn change_page_cache_size(&self, capacity: usize) {
        self.page_cache.borrow_mut().resize(capacity);
    }

    pub fn add_dirty(&self, page: Rc<RefCell<Page>>) {
        // TODO: cehck duplicates?
        let mut dirty_pages = RefCell::borrow_mut(&self.dirty_pages);
        dirty_pages.push(page);
    }

    pub fn cacheflush(&self) -> Result<()> {
        let mut dirty_pages = RefCell::borrow_mut(&self.dirty_pages);
        if dirty_pages.len() == 0 {
            return Ok(());
        }
        loop {
            if dirty_pages.len() == 0 {
                break;
            }
            let page = dirty_pages.pop().unwrap();
            sqlite3_ondisk::begin_write_btree_page(self, &page)?;
        }
        self.io.run_once()?;
        Ok(())
    }
}
