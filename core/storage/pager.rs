use crate::storage::buffer_pool::BufferPool;
use crate::storage::database::DatabaseStorage;
use crate::storage::sqlite3_ondisk::{self, DatabaseHeader, PageContent};
use crate::storage::wal::Wal;
use crate::{Buffer, Result};
use log::{debug, trace};
use sieve_cache::SieveCache;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::ptr::{drop_in_place, NonNull};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::wal::CheckpointStatus;

pub struct Page {
    pub flags: AtomicUsize,
    pub contents: Option<PageContent>,
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
/// Page's contents are loaded in memory.
const PAGE_LOADED: usize = 0b10000;

impl Page {
    pub fn new(id: usize) -> Page {
        Page {
            flags: AtomicUsize::new(0),
            contents: None,
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

    pub fn is_loaded(&self) -> bool {
        self.flags.load(Ordering::SeqCst) & PAGE_LOADED != 0
    }

    pub fn set_loaded(&self) {
        self.flags.fetch_or(PAGE_LOADED, Ordering::SeqCst);
    }

    pub fn clear_loaded(&self) {
        log::debug!("clear loaded {}", self.id);
        self.flags.fetch_and(!PAGE_LOADED, Ordering::SeqCst);
    }
}

#[allow(dead_code)]
struct PageCacheEntry {
    key: usize,
    page: Rc<RefCell<Page>>,
    prev: Option<NonNull<PageCacheEntry>>,
    next: Option<NonNull<PageCacheEntry>>,
}

impl PageCacheEntry {
    fn as_non_null(&mut self) -> NonNull<PageCacheEntry> {
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
            capacity,
            map: RefCell::new(HashMap::new()),
            head: RefCell::new(None),
            tail: RefCell::new(None),
        }
    }

    pub fn contains_key(&mut self, key: usize) -> bool {
        self.map.borrow().contains_key(&key)
    }

    pub fn insert(&mut self, key: usize, value: Rc<RefCell<Page>>) {
        self._delete(key, false);
        debug!("cache_insert(key={})", key);
        let mut entry = Box::new(PageCacheEntry {
            key,
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
        self._delete(key, true)
    }

    pub fn _delete(&mut self, key: usize, clean_page: bool) {
        debug!("cache_delete(key={}, clean={})", key, clean_page);
        let ptr = self.map.borrow_mut().remove(&key);
        if ptr.is_none() {
            return;
        }
        let mut ptr = ptr.unwrap();
        {
            let ptr = unsafe { ptr.as_mut() };
            self.detach(ptr, clean_page);
        }
        unsafe { drop_in_place(ptr.as_ptr()) };
    }

    fn get_ptr(&mut self, key: usize) -> Option<NonNull<PageCacheEntry>> {
        let m = self.map.borrow_mut();
        let ptr = m.get(&key);
        ptr.copied()
    }

    pub fn get(&mut self, key: &usize) -> Option<Rc<RefCell<Page>>> {
        debug!("cache_get(key={})", key);
        let ptr = self.get_ptr(*key);
        ptr?;
        let ptr = unsafe { ptr.unwrap().as_mut() };
        let page = ptr.page.clone();
        //self.detach(ptr);
        self.touch(ptr);
        Some(page)
    }

    pub fn resize(&mut self, capacity: usize) {
        let _ = capacity;
        todo!();
    }

    fn detach(&mut self, entry: &mut PageCacheEntry, clean_page: bool) {
        let mut current = entry.as_non_null();

        if clean_page {
            // evict buffer
            let mut page = entry.page.borrow_mut();
            page.clear_loaded();
            debug!("cleaning up page {}", page.id);
            let _ = page.contents.take();
        }

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
        let mut current = entry.as_non_null();
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
        if RefCell::borrow(&tail.page).is_dirty() {
            // TODO: drop from another clean entry?
            return;
        }
        self.detach(tail, true);
    }

    fn clear(&mut self) {
        let to_remove: Vec<usize> = self.map.borrow().iter().map(|v| *v.0).collect();
        for key in to_remove {
            self.delete(key);
        }
    }
}

#[allow(dead_code)]
pub struct PageCache<K: Eq + Hash + Clone, V> {
    cache: SieveCache<K, V>,
}

#[allow(dead_code)]
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

#[derive(Clone)]
enum FlushState {
    Start,
    WaitAppendFrames,
    SyncWal,
    Checkpoint,
    SyncDbFile,
    WaitSyncDbFile,
}

#[derive(Clone, Debug)]
enum CheckpointState {
    Checkpoint,
    CheckpointDone,
}

/// This will keep track of the state of current cache flush in order to not repeat work
struct FlushInfo {
    state: FlushState,
    /// Number of writes taking place. When in_flight gets to 0 we can schedule a fsync.
    in_flight_writes: Rc<RefCell<usize>>,
}

/// The pager interface implements the persistence layer by providing access
/// to pages of the database file, including caching, concurrency control, and
/// transaction management.
pub struct Pager {
    /// Source of the database pages.
    pub page_io: Rc<dyn DatabaseStorage>,
    /// The write-ahead log (WAL) for the database.
    wal: Rc<RefCell<dyn Wal>>,
    /// A page cache for the database.
    page_cache: RefCell<DumbLruPageCache>,
    /// Buffer pool for temporary data storage.
    buffer_pool: Rc<BufferPool>,
    /// I/O interface for input/output operations.
    pub io: Arc<dyn crate::io::IO>,
    dirty_pages: Rc<RefCell<HashSet<usize>>>,
    db_header: Rc<RefCell<DatabaseHeader>>,

    flush_info: RefCell<FlushInfo>,
    checkpoint_state: RefCell<CheckpointState>,
    checkpoint_inflight: Rc<RefCell<usize>>,
    syncing: Rc<RefCell<bool>>,
}

impl Pager {
    /// Begins opening a database by reading the database header.
    pub fn begin_open(page_io: Rc<dyn DatabaseStorage>) -> Result<Rc<RefCell<DatabaseHeader>>> {
        sqlite3_ondisk::begin_read_database_header(page_io)
    }

    /// Completes opening a database by initializing the Pager with the database header.
    pub fn finish_open(
        db_header_ref: Rc<RefCell<DatabaseHeader>>,
        page_io: Rc<dyn DatabaseStorage>,
        wal: Rc<RefCell<dyn Wal>>,
        io: Arc<dyn crate::io::IO>,
    ) -> Result<Self> {
        let db_header = RefCell::borrow(&db_header_ref);
        let page_size = db_header.page_size as usize;
        let buffer_pool = Rc::new(BufferPool::new(page_size));
        let page_cache = RefCell::new(DumbLruPageCache::new(10));
        Ok(Self {
            page_io,
            wal,
            buffer_pool,
            page_cache,
            io,
            dirty_pages: Rc::new(RefCell::new(HashSet::new())),
            db_header: db_header_ref.clone(),
            flush_info: RefCell::new(FlushInfo {
                state: FlushState::Start,
                in_flight_writes: Rc::new(RefCell::new(0)),
            }),
            syncing: Rc::new(RefCell::new(false)),
            checkpoint_state: RefCell::new(CheckpointState::Checkpoint),
            checkpoint_inflight: Rc::new(RefCell::new(0)),
        })
    }

    pub fn begin_read_tx(&self) -> Result<()> {
        self.wal.borrow().begin_read_tx()?;
        Ok(())
    }

    pub fn begin_write_tx(&self) -> Result<()> {
        self.wal.borrow().begin_read_tx()?;
        Ok(())
    }

    pub fn end_tx(&self) -> Result<CheckpointStatus> {
        match self.cacheflush()? {
            CheckpointStatus::Done => {}
            CheckpointStatus::IO => return Ok(CheckpointStatus::IO),
        };
        self.wal.borrow().end_read_tx()?;
        Ok(CheckpointStatus::Done)
    }

    /// Reads a page from the database.
    pub fn read_page(&self, page_idx: usize) -> crate::Result<Rc<RefCell<Page>>> {
        trace!("read_page(page_idx = {})", page_idx);
        let mut page_cache = self.page_cache.borrow_mut();
        if let Some(page) = page_cache.get(&page_idx) {
            trace!("read_page(page_idx = {}) = cached", page_idx);
            return Ok(page.clone());
        }
        let page = Rc::new(RefCell::new(Page::new(page_idx)));
        RefCell::borrow(&page).set_locked();
        if let Some(frame_id) = self.wal.borrow().find_frame(page_idx as u64)? {
            self.wal
                .borrow()
                .read_frame(frame_id, page.clone(), self.buffer_pool.clone())?;
            {
                let page = page.borrow_mut();
                page.set_uptodate();
            }
            // TODO(pere) ensure page is inserted, we should probably first insert to page cache
            // and if successful, read frame or page
            page_cache.insert(page_idx, page.clone());
            return Ok(page);
        }
        sqlite3_ondisk::begin_read_page(
            self.page_io.clone(),
            self.buffer_pool.clone(),
            page.clone(),
            page_idx,
        )?;
        // TODO(pere) ensure page is inserted
        page_cache.insert(page_idx, page.clone());
        Ok(page)
    }

    /// Loads pages if not loaded
    pub fn load_page(&self, page: Rc<RefCell<Page>>) -> Result<()> {
        let id = page.borrow().id;
        trace!("load_page(page_idx = {})", id);
        let mut page_cache = self.page_cache.borrow_mut();
        page.borrow_mut().set_locked();
        if let Some(frame_id) = self.wal.borrow().find_frame(id as u64)? {
            self.wal
                .borrow()
                .read_frame(frame_id, page.clone(), self.buffer_pool.clone())?;
            {
                let page = page.borrow_mut();
                page.set_uptodate();
            }
            // TODO(pere) ensure page is inserted
            if !page_cache.contains_key(id) {
                page_cache.insert(id, page.clone());
            }
            return Ok(());
        }
        sqlite3_ondisk::begin_read_page(
            self.page_io.clone(),
            self.buffer_pool.clone(),
            page.clone(),
            id,
        )?;
        // TODO(pere) ensure page is inserted
        if !page_cache.contains_key(id) {
            page_cache.insert(id, page.clone());
        }
        Ok(())
    }

    /// Writes the database header.
    pub fn write_database_header(&self, header: &DatabaseHeader) {
        sqlite3_ondisk::begin_write_database_header(header, self).expect("failed to write header");
    }

    /// Changes the size of the page cache.
    pub fn change_page_cache_size(&self, capacity: usize) {
        self.page_cache.borrow_mut().resize(capacity);
    }

    pub fn add_dirty(&self, page_id: usize) {
        // TODO: cehck duplicates?
        let mut dirty_pages = RefCell::borrow_mut(&self.dirty_pages);
        dirty_pages.insert(page_id);
    }

    pub fn cacheflush(&self) -> Result<CheckpointStatus> {
        loop {
            let state = self.flush_info.borrow().state.clone();
            match state {
                FlushState::Start => {
                    let db_size = self.db_header.borrow().database_size;
                    for page_id in self.dirty_pages.borrow().iter() {
                        let mut cache = self.page_cache.borrow_mut();
                        let page = cache.get(page_id).expect("we somehow added a page to dirty list but we didn't mark it as dirty, causing cache to drop it.");
                        let page_type = page.borrow().contents.as_ref().unwrap().maybe_page_type();
                        debug!("appending frame {} {:?}", page_id, page_type);
                        self.wal.borrow_mut().append_frame(
                            page.clone(),
                            db_size,
                            self,
                            self.flush_info.borrow().in_flight_writes.clone(),
                        )?;
                    }
                    self.dirty_pages.borrow_mut().clear();
                    self.flush_info.borrow_mut().state = FlushState::WaitAppendFrames;
                    return Ok(CheckpointStatus::IO);
                }
                FlushState::WaitAppendFrames => {
                    let in_flight = *self.flush_info.borrow().in_flight_writes.borrow();
                    if in_flight == 0 {
                        self.flush_info.borrow_mut().state = FlushState::SyncWal;
                    } else {
                        return Ok(CheckpointStatus::IO);
                    }
                }
                FlushState::SyncWal => {
                    match self.wal.borrow_mut().sync() {
                        Ok(CheckpointStatus::IO) => return Ok(CheckpointStatus::IO),
                        Ok(CheckpointStatus::Done) => {}
                        Err(e) => return Err(e),
                    }

                    let should_checkpoint = self.wal.borrow().should_checkpoint();
                    if should_checkpoint {
                        self.flush_info.borrow_mut().state = FlushState::Checkpoint;
                    } else {
                        self.flush_info.borrow_mut().state = FlushState::Start;
                        break;
                    }
                }
                FlushState::Checkpoint => {
                    match self.checkpoint()? {
                        CheckpointStatus::Done => {
                            self.flush_info.borrow_mut().state = FlushState::SyncDbFile;
                        }
                        CheckpointStatus::IO => return Ok(CheckpointStatus::IO),
                    };
                }
                FlushState::SyncDbFile => {
                    sqlite3_ondisk::begin_sync(self.page_io.clone(), self.syncing.clone())?;
                    self.flush_info.borrow_mut().state = FlushState::WaitSyncDbFile;
                }
                FlushState::WaitSyncDbFile => {
                    if *self.syncing.borrow() {
                        return Ok(CheckpointStatus::IO);
                    } else {
                        self.flush_info.borrow_mut().state = FlushState::Start;
                        break;
                    }
                }
            }
        }
        Ok(CheckpointStatus::Done)
    }

    pub fn checkpoint(&self) -> Result<CheckpointStatus> {
        loop {
            let state = self.checkpoint_state.borrow().clone();
            log::trace!("checkpoint(state={:?})", state);
            match state {
                CheckpointState::Checkpoint => {
                    let in_flight = self.checkpoint_inflight.clone();
                    match self.wal.borrow_mut().checkpoint(self, in_flight)? {
                        CheckpointStatus::IO => return Ok(CheckpointStatus::IO),
                        CheckpointStatus::Done => {
                            self.checkpoint_state
                                .replace(CheckpointState::CheckpointDone);
                        }
                    };
                }
                CheckpointState::CheckpointDone => {
                    let in_flight = self.checkpoint_inflight.clone();
                    if *in_flight.borrow() > 0 {
                        return Ok(CheckpointStatus::IO);
                    } else {
                        self.checkpoint_state.replace(CheckpointState::Checkpoint);
                        return Ok(CheckpointStatus::Done);
                    }
                }
            }
        }
    }

    // WARN: used for testing purposes
    pub fn clear_page_cache(&self) {
        loop {
            match self
                .wal
                .borrow_mut()
                .checkpoint(self, Rc::new(RefCell::new(0)))
            {
                Ok(CheckpointStatus::IO) => {}
                Ok(CheckpointStatus::Done) => {
                    break;
                }
                Err(err) => panic!("error while clearing cache {}", err),
            }
        }
        self.page_cache.borrow_mut().clear();
    }

    /*
        Get's a new page that increasing the size of the page or uses a free page.
        Currently free list pages are not yet supported.
    */
    #[allow(clippy::readonly_write_lock)]
    pub fn allocate_page(&self) -> Result<Rc<RefCell<Page>>> {
        let header = &self.db_header;
        let mut header = RefCell::borrow_mut(header);
        header.database_size += 1;
        {
            // update database size
            // read sync for now
            loop {
                let first_page_ref = self.read_page(1)?;
                let first_page = RefCell::borrow_mut(&first_page_ref);
                if first_page.is_locked() {
                    drop(first_page);
                    self.io.run_once()?;
                    continue;
                }
                first_page.set_dirty();
                self.add_dirty(1);

                let contents = first_page.contents.as_ref().unwrap();
                contents.write_database_header(&header);
                break;
            }
        }

        let page_ref = allocate_page(header.database_size as usize, &self.buffer_pool, 0);
        {
            // setup page and add to cache
            let page = page_ref.borrow_mut();
            page.set_dirty();
            self.add_dirty(page.id);
            let mut cache = self.page_cache.borrow_mut();
            cache.insert(page.id, page_ref.clone());
        }
        Ok(page_ref)
    }

    pub fn put_loaded_page(&self, id: usize, page: Rc<RefCell<Page>>) {
        let mut cache = RefCell::borrow_mut(&self.page_cache);
        // cache insert invalidates previous page
        cache.insert(id, page.clone());
        page.borrow_mut().set_loaded();
    }

    pub fn usable_size(&self) -> usize {
        let db_header = self.db_header.borrow();
        (db_header.page_size - db_header.unused_space as u16) as usize
    }
}

pub fn allocate_page(
    page_id: usize,
    buffer_pool: &Rc<BufferPool>,
    offset: usize,
) -> Rc<RefCell<Page>> {
    let page_ref = Rc::new(RefCell::new(Page::new(page_id)));
    {
        let mut page = RefCell::borrow_mut(&page_ref);
        let buffer = buffer_pool.get();
        let bp = buffer_pool.clone();
        let drop_fn = Rc::new(move |buf| {
            bp.put(buf);
        });
        let buffer = Rc::new(RefCell::new(Buffer::new(buffer, drop_fn)));
        page.set_loaded();
        page.contents = Some(PageContent {
            offset,
            buffer,
            overflow_cells: Vec::new(),
        });
    }
    page_ref
}
