use crate::result::LimboResult;
use crate::storage::buffer_pool::BufferPool;
use crate::storage::database::DatabaseStorage;
use crate::storage::sqlite3_ondisk::{self, DatabaseHeader, PageContent};
use crate::storage::wal::Wal;
use crate::{Buffer, Result};
use log::trace;
use std::cell::{RefCell, UnsafeCell};
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use super::page_cache::{DumbLruPageCache, PageCacheKey};
use super::wal::{CheckpointMode, CheckpointStatus};

pub struct PageInner {
    pub flags: AtomicUsize,
    pub contents: Option<PageContent>,
    pub id: usize,
}

pub struct Page {
    pub inner: UnsafeCell<PageInner>,
}

// Concurrency control of pages will be handled by the pager, we won't wrap Page with RwLock
// because that is bad bad.
pub type PageRef = Arc<Page>;

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
    pub fn new(id: usize) -> Self {
        Self {
            inner: UnsafeCell::new(PageInner {
                flags: AtomicUsize::new(0),
                contents: None,
                id,
            }),
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get(&self) -> &mut PageInner {
        unsafe { &mut *self.inner.get() }
    }

    pub fn is_uptodate(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_UPTODATE != 0
    }

    pub fn set_uptodate(&self) {
        self.get().flags.fetch_or(PAGE_UPTODATE, Ordering::SeqCst);
    }

    pub fn clear_uptodate(&self) {
        self.get().flags.fetch_and(!PAGE_UPTODATE, Ordering::SeqCst);
    }

    pub fn is_locked(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_LOCKED != 0
    }

    pub fn set_locked(&self) {
        self.get().flags.fetch_or(PAGE_LOCKED, Ordering::SeqCst);
    }

    pub fn clear_locked(&self) {
        self.get().flags.fetch_and(!PAGE_LOCKED, Ordering::SeqCst);
    }

    pub fn is_error(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_ERROR != 0
    }

    pub fn set_error(&self) {
        self.get().flags.fetch_or(PAGE_ERROR, Ordering::SeqCst);
    }

    pub fn clear_error(&self) {
        self.get().flags.fetch_and(!PAGE_ERROR, Ordering::SeqCst);
    }

    pub fn is_dirty(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_DIRTY != 0
    }

    pub fn set_dirty(&self) {
        self.get().flags.fetch_or(PAGE_DIRTY, Ordering::SeqCst);
    }

    pub fn clear_dirty(&self) {
        self.get().flags.fetch_and(!PAGE_DIRTY, Ordering::SeqCst);
    }

    pub fn is_loaded(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_LOADED != 0
    }

    pub fn set_loaded(&self) {
        self.get().flags.fetch_or(PAGE_LOADED, Ordering::SeqCst);
    }

    pub fn clear_loaded(&self) {
        log::debug!("clear loaded {}", self.get().id);
        self.get().flags.fetch_and(!PAGE_LOADED, Ordering::SeqCst);
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
    SyncDbFile,
    WaitSyncDbFile,
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
    page_cache: Arc<RwLock<DumbLruPageCache>>,
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
        page_cache: Arc<RwLock<DumbLruPageCache>>,
        buffer_pool: Rc<BufferPool>,
    ) -> Result<Self> {
        Ok(Self {
            page_io,
            wal,
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
            buffer_pool,
        })
    }

    pub fn begin_read_tx(&self) -> Result<LimboResult> {
        self.wal.borrow_mut().begin_read_tx()
    }

    pub fn begin_write_tx(&self) -> Result<LimboResult> {
        self.wal.borrow_mut().begin_write_tx()
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
    pub fn read_page(&self, page_idx: usize) -> crate::Result<PageRef> {
        trace!("read_page(page_idx = {})", page_idx);
        let mut page_cache = self.page_cache.write().unwrap();
        let page_key = PageCacheKey::new(page_idx, Some(self.wal.borrow().get_max_frame()));
        if let Some(page) = page_cache.get(&page_key) {
            trace!("read_page(page_idx = {}) = cached", page_idx);
            return Ok(page.clone());
        }
        let page = Arc::new(Page::new(page_idx));
        page.set_locked();

        if let Some(frame_id) = self.wal.borrow().find_frame(page_idx as u64)? {
            self.wal
                .borrow()
                .read_frame(frame_id, page.clone(), self.buffer_pool.clone())?;
            {
                page.set_uptodate();
            }
            // TODO(pere) ensure page is inserted, we should probably first insert to page cache
            // and if successful, read frame or page
            page_cache.insert(page_key, page.clone());
            return Ok(page);
        }
        sqlite3_ondisk::begin_read_page(
            self.page_io.clone(),
            self.buffer_pool.clone(),
            page.clone(),
            page_idx,
        )?;
        // TODO(pere) ensure page is inserted
        page_cache.insert(page_key, page.clone());
        Ok(page)
    }

    /// Loads pages if not loaded
    pub fn load_page(&self, page: PageRef) -> Result<()> {
        let id = page.get().id;
        trace!("load_page(page_idx = {})", id);
        let mut page_cache = self.page_cache.write().unwrap();
        page.set_locked();
        let page_key = PageCacheKey::new(id, Some(self.wal.borrow().get_max_frame()));
        if let Some(frame_id) = self.wal.borrow().find_frame(id as u64)? {
            self.wal
                .borrow()
                .read_frame(frame_id, page.clone(), self.buffer_pool.clone())?;
            {
                page.set_uptodate();
            }
            // TODO(pere) ensure page is inserted
            if !page_cache.contains_key(&page_key) {
                page_cache.insert(page_key, page.clone());
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
        if !page_cache.contains_key(&page_key) {
            page_cache.insert(page_key, page.clone());
        }
        Ok(())
    }

    /// Writes the database header.
    pub fn write_database_header(&self, header: &DatabaseHeader) {
        sqlite3_ondisk::begin_write_database_header(header, self).expect("failed to write header");
    }

    /// Changes the size of the page cache.
    pub fn change_page_cache_size(&self, capacity: usize) {
        let mut page_cache = self.page_cache.write().unwrap();
        page_cache.resize(capacity);
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
                        let mut cache = self.page_cache.write().unwrap();
                        let page_key =
                            PageCacheKey::new(*page_id, Some(self.wal.borrow().get_max_frame()));
                        let page = cache.get(&page_key).expect("we somehow added a page to dirty list but we didn't mark it as dirty, causing cache to drop it.");
                        let page_type = page.get().contents.as_ref().unwrap().maybe_page_type();
                        log::trace!("cacheflush(page={}, page_type={:?}", page_id, page_type);
                        self.wal.borrow_mut().append_frame(
                            page.clone(),
                            db_size,
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
            log::trace!("pager_checkpoint(state={:?})", state);
            match state {
                CheckpointState::Checkpoint => {
                    let in_flight = self.checkpoint_inflight.clone();
                    match self.wal.borrow_mut().checkpoint(
                        self,
                        in_flight,
                        CheckpointMode::Passive,
                    )? {
                        CheckpointStatus::IO => return Ok(CheckpointStatus::IO),
                        CheckpointStatus::Done => {
                            self.checkpoint_state.replace(CheckpointState::SyncDbFile);
                        }
                    };
                }
                CheckpointState::SyncDbFile => {
                    sqlite3_ondisk::begin_sync(self.page_io.clone(), self.syncing.clone())?;
                    self.checkpoint_state
                        .replace(CheckpointState::WaitSyncDbFile);
                }
                CheckpointState::WaitSyncDbFile => {
                    if *self.syncing.borrow() {
                        return Ok(CheckpointStatus::IO);
                    } else {
                        self.checkpoint_state
                            .replace(CheckpointState::CheckpointDone);
                    }
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
            match self.wal.borrow_mut().checkpoint(
                self,
                Rc::new(RefCell::new(0)),
                CheckpointMode::Passive,
            ) {
                Ok(CheckpointStatus::IO) => {
                    let _ = self.io.run_once();
                }
                Ok(CheckpointStatus::Done) => {
                    break;
                }
                Err(err) => panic!("error while clearing cache {}", err),
            }
        }
        // TODO: only clear cache of things that are really invalidated
        self.page_cache.write().unwrap().clear();
    }

    /*
        Get's a new page that increasing the size of the page or uses a free page.
        Currently free list pages are not yet supported.
    */
    #[allow(clippy::readonly_write_lock)]
    pub fn allocate_page(&self) -> Result<PageRef> {
        let header = &self.db_header;
        let mut header = RefCell::borrow_mut(header);
        header.database_size += 1;
        {
            // update database size
            // read sync for now
            loop {
                let first_page_ref = self.read_page(1)?;
                if first_page_ref.is_locked() {
                    self.io.run_once()?;
                    continue;
                }
                first_page_ref.set_dirty();
                self.add_dirty(1);

                let contents = first_page_ref.get().contents.as_ref().unwrap();
                contents.write_database_header(&header);
                break;
            }
        }

        let page = allocate_page(header.database_size as usize, &self.buffer_pool, 0);
        {
            // setup page and add to cache
            page.set_dirty();
            self.add_dirty(page.get().id);
            let mut cache = self.page_cache.write().unwrap();
            let page_key =
                PageCacheKey::new(page.get().id, Some(self.wal.borrow().get_max_frame()));
            cache.insert(page_key, page.clone());
        }
        Ok(page)
    }

    pub fn put_loaded_page(&self, id: usize, page: PageRef) {
        let mut cache = self.page_cache.write().unwrap();
        // cache insert invalidates previous page
        let page_key = PageCacheKey::new(id, Some(self.wal.borrow().get_max_frame()));
        cache.insert(page_key, page.clone());
        page.set_loaded();
    }

    pub fn usable_size(&self) -> usize {
        let db_header = self.db_header.borrow();
        (db_header.page_size - db_header.reserved_space as u16) as usize
    }
}

pub fn allocate_page(page_id: usize, buffer_pool: &Rc<BufferPool>, offset: usize) -> PageRef {
    let page = Arc::new(Page::new(page_id));
    {
        let buffer = buffer_pool.get();
        let bp = buffer_pool.clone();
        let drop_fn = Rc::new(move |buf| {
            bp.put(buf);
        });
        let buffer = Rc::new(RefCell::new(Buffer::new(buffer, drop_fn)));
        page.set_loaded();
        page.get().contents = Some(PageContent {
            offset,
            buffer,
            overflow_cells: Vec::new(),
        });
    }
    page
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use crate::storage::page_cache::{DumbLruPageCache, PageCacheKey};

    use super::Page;

    #[test]
    fn test_shared_cache() {
        // ensure cache can be shared between threads
        let cache = Arc::new(RwLock::new(DumbLruPageCache::new(10)));

        let thread = {
            let cache = cache.clone();
            std::thread::spawn(move || {
                let mut cache = cache.write().unwrap();
                let page_key = PageCacheKey::new(1, None);
                cache.insert(page_key, Arc::new(Page::new(1)));
            })
        };
        let _ = thread.join();
        let mut cache = cache.write().unwrap();
        let page_key = PageCacheKey::new(1, None);
        let page = cache.get(&page_key);
        assert_eq!(page.unwrap().get().id, 1);
    }
}
