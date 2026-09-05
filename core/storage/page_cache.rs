use crate::sync::Arc;
use intrusive_collections::{intrusive_adapter, LinkedList, LinkedListLink};
use rustc_hash::FxHashMap as HashMap;
use tracing::trace;

use crate::{
    storage::{btree::PinGuard, sqlite3_ondisk::DatabaseHeader},
    turso_assert,
};

use super::pager::PageRef;

#[cfg(not(target_family = "wasm"))]
const DEFAULT_PAGE_CACHE_SIZE_IN_PAGES: usize = 2000;
#[cfg(target_family = "wasm")]
const DEFAULT_PAGE_CACHE_SIZE_IN_PAGES: usize = 100000;

/// Minimum safe cache size in pages.
/// This accounts for:
/// - Btree cursor stack (up to BTCURSOR_MAX_DEPTH = 20 pages)
/// - Balance operations (MAX_SIBLING_PAGES_TO_BALANCE = 5 new pages)
/// - State machine pages (freelist operations, header refs, etc.)
/// - Some buffer for concurrent operations
pub const MINIMUM_PAGE_CACHE_SIZE_IN_PAGES: usize = 200;

/// The spill threshold as a fraction of capacity.
const DEFAULT_SPILL_THRESHOLD_PERCENT: usize = 90;

#[derive(Debug, Copy, Eq, Hash, PartialEq, Clone)]
#[repr(transparent)]
pub struct PageCacheKey(usize);

const CLEAR: u8 = 0;
const REF_MAX: u8 = 3;

/// An entry in the page cache.
///
/// The entry is stored in the intrusive linked list in PageCache::list`.
struct PageCacheEntry {
    /// Key identifying this page
    key: PageCacheKey,
    /// The cached page
    page: PageRef,
    /// Reference counter (SIEVE/GClock): starts at zero, bumped on access,
    /// decremented during eviction, only pages at 0 are evicted.
    ref_bit: u8,
    /// Intrusive link for SIEVE queue
    link: LinkedListLink,
}

intrusive_adapter!(EntryAdapter = Box<PageCacheEntry>: PageCacheEntry { link: LinkedListLink });

impl PageCacheEntry {
    fn new(key: PageCacheKey, page: PageRef) -> Box<Self> {
        Box::new(Self {
            key,
            page,
            ref_bit: CLEAR,
            link: LinkedListLink::new(),
        })
    }

    #[inline]
    fn bump_ref(&mut self) {
        self.ref_bit = std::cmp::min(self.ref_bit + 1, REF_MAX);
    }

    #[inline]
    /// Returns the old value
    fn decrement_ref(&mut self) -> u8 {
        let old = self.ref_bit;
        self.ref_bit = old.saturating_sub(1);
        old
    }
}

/// Result returned when attempting to spill dirty pages from the cache.
#[derive(Debug)]
pub enum SpillResult {
    /// No spilling was needed (cache is below threshold)
    NotNeeded,
    /// Spilling is needed but disabled
    Disabled,
    /// Successfully collected dirty pages to spill
    PagesToSpill(Vec<PinGuard>),
    /// Cache is at capacity with only unevictable pages
    CacheFull,
}

/// PageCache implements a variation of the SIEVE algorithm that maintains an intrusive linked list queue of
/// pages which keep a 'reference_bit' to determine how recently/frequently the page has been accessed.
/// The bit is set to `Clear` on initial insertion and then bumped on each access and decremented
/// during eviction scans.
///
/// The ring is circular. `clock_hand` points at the tail (LRU).
/// Sweep order follows next: tail (LRU) -> head (MRU) -> .. -> tail
/// New pages are inserted after the clock hand in the `next` direction,
/// which places them at head (MRU) (i.e. `tail.next` is the head).
pub struct PageCache {
    /// Capacity in pages
    capacity: usize,
    /// Map of Key -> pointer to entry in the queue
    map: HashMap<PageCacheKey, *mut PageCacheEntry>,
    /// The eviction queue (intrusive doubly-linked list)
    queue: LinkedList<EntryAdapter>,
    /// Clock hand cursor for SIEVE eviction (pointer to an entry in the queue, or null)
    clock_hand: *mut PageCacheEntry,
    /// Threshold number of pages at which we start spilling dirty pages.
    spill_threshold: usize,
    spill_enabled: bool,
    /// Conservative estimation of pages that are evictable based on dirty/spilled state.
    evictable_count: usize,
}

unsafe impl Send for PageCache {}
unsafe impl Sync for PageCache {}
crate::assert::assert_send_sync!(PageCache);

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum CacheError {
    #[error("{0}")]
    InternalError(String),
    #[error("page {pgno} is locked")]
    Locked { pgno: usize },
    #[error("page {pgno} is dirty")]
    Dirty { pgno: usize },
    #[error("page {pgno} is pinned")]
    Pinned { pgno: usize },
    #[error("Page cache is full")]
    Full,
    #[error("key already exists")]
    KeyExists,
}

#[derive(Debug, PartialEq)]
pub enum CacheResizeResult {
    Done,
    PendingEvictions,
}

impl PageCacheKey {
    pub fn new(pgno: usize) -> Self {
        Self(pgno)
    }
}

impl PageCache {
    #[cfg(not(target_family = "wasm"))]
    pub fn new(capacity: usize) -> Self {
        Self::new_with_spill(capacity, true)
    }
    #[cfg(target_family = "wasm")]
    pub fn new(capacity: usize) -> Self {
        Self::new_with_spill(capacity, false)
    }

    /// Create a new PageCache with explicit spill control.
    fn new_with_spill(capacity: usize, spill_enabled: bool) -> Self {
        let spill_threshold = (capacity * DEFAULT_SPILL_THRESHOLD_PERCENT) / 100;
        Self {
            capacity,
            map: HashMap::default(),
            queue: LinkedList::new(EntryAdapter::new()),
            clock_hand: std::ptr::null_mut(),
            spill_threshold: spill_threshold.max(1),
            spill_enabled,
            evictable_count: 0,
        }
    }

    /// Advances the clock hand to the next entry in the circular queue.
    /// Follows the "next" direction: from tail/LRU through the list back to tail.
    /// With our insertion-after-hand strategy, this moves through entries in age order.
    fn advance_clock_hand(&mut self) {
        if self.clock_hand.is_null() {
            return;
        }

        unsafe {
            let mut cursor = self.queue.cursor_mut_from_ptr(self.clock_hand);
            cursor.move_next();

            if cursor.get().is_some() {
                self.clock_hand =
                    cursor.as_cursor().get().unwrap() as *const _ as *mut PageCacheEntry;
            } else {
                // Reached end, wrap to front
                let front_cursor = self.queue.front_mut();
                if front_cursor.get().is_some() {
                    self.clock_hand =
                        front_cursor.as_cursor().get().unwrap() as *const _ as *mut PageCacheEntry;
                } else {
                    self.clock_hand = std::ptr::null_mut();
                }
            }
        }
    }

    pub fn contains_key(&self, key: &PageCacheKey) -> bool {
        self.map.contains_key(key)
    }

    #[inline]
    pub fn insert(&mut self, key: PageCacheKey, value: PageRef) -> Result<(), CacheError> {
        self._insert(key, value, false, false)
    }

    #[inline]
    fn upsert_page(&mut self, key: PageCacheKey, value: PageRef) -> Result<(), CacheError> {
        self._insert(key, value, true, false)
    }

    /// Insert or replace a page, exceeding the cache capacity if eviction
    /// cannot make room.
    ///
    /// Savepoint rollback uses this while restoring subjournaled before-images:
    /// rollback must restore every page it journaled even when the cache is full
    /// of dirty pages that cannot be evicted yet. Normal cache insertions still
    /// enforce capacity and will drain the temporary excess as pages become
    /// spillable or clean.
    pub fn force_upsert_page(
        &mut self,
        key: PageCacheKey,
        value: PageRef,
    ) -> Result<(), CacheError> {
        match self.upsert_page(key, value.clone()) {
            Err(CacheError::Full) => self._insert(key, value, true, true),
            result => result,
        }
    }

    /// Insert a page, exceeding the cache capacity if eviction cannot make
    /// room.
    ///
    /// The capacity is a soft limit, as in SQLite: when a normal insert
    /// fails with [`CacheError::Full`] every resident page is unevictable
    /// (pinned, held by a cursor, or dirty and unspillable), so refusing the
    /// insert cannot reclaim any memory — the page and its buffer are
    /// already allocated — it can only fail the statement. Admit the page
    /// over capacity instead; subsequent inserts drain the excess back under
    /// capacity as pages become evictable again.
    pub fn force_insert_page(
        &mut self,
        key: PageCacheKey,
        value: PageRef,
    ) -> Result<(), CacheError> {
        match self.insert(key, value.clone()) {
            Err(CacheError::Full) => self._insert(key, value, false, true),
            result => result,
        }
    }

    fn _insert(
        &mut self,
        key: PageCacheKey,
        value: PageRef,
        update_in_place: bool,
        bypass_capacity: bool,
    ) -> Result<(), CacheError> {
        trace!("insert(key={:?})", key);

        if let Some(&entry_ptr) = self.map.get(&key) {
            let entry = unsafe { &mut *entry_ptr };
            let p = &entry.page;

            if !p.is_loaded() && !p.is_locked() {
                // evict, then continue with fresh insert
                self._delete(key, true)?;
                // Proceed to insert new entry
            } else {
                entry.bump_ref();
                if update_in_place {
                    // Track evictable count change if page state differs
                    let old_evictable = Self::counted_as_evictable(&entry.page);
                    let new_evictable = Self::counted_as_evictable(&value);
                    if old_evictable && !new_evictable {
                        self.evictable_count = self.evictable_count.saturating_sub(1);
                    } else if !old_evictable && new_evictable {
                        self.evictable_count += 1;
                    }
                    entry.page = value;
                    return Ok(());
                } else {
                    turso_assert!(
                        Arc::ptr_eq(&entry.page, &value),
                        "Attempted to insert different page with same key: {key:?}"
                    );
                    return Err(CacheError::KeyExists);
                }
            }
        }

        // Key doesn't exist, proceed with new entry
        self.make_room_for(1, bypass_capacity)?;

        // Track evictable count for the new page
        let is_evictable = Self::counted_as_evictable(&value);

        let entry = PageCacheEntry::new(key, value);

        if self.clock_hand.is_null() {
            // First entry - just push it
            self.queue.push_back(entry);
            let entry_ptr = self.queue.back().get().unwrap() as *const _ as *mut PageCacheEntry;
            self.map.insert(key, entry_ptr);
            self.clock_hand = entry_ptr;
        } else {
            // Insert after clock hand (in circular list semantics, this makes it the new head/MRU)
            unsafe {
                let mut cursor = self.queue.cursor_mut_from_ptr(self.clock_hand);
                cursor.insert_after(entry);
                // The inserted entry is now at the next position after clock hand
                cursor.move_next();
                let entry_ptr = cursor.get().ok_or_else(|| {
                    CacheError::InternalError("Failed to get inserted entry pointer".into())
                })? as *const PageCacheEntry as *mut PageCacheEntry;
                self.map.insert(key, entry_ptr);
            }
        }

        // Update evictable count after successful insertion
        if is_evictable {
            self.evictable_count += 1;
        }

        Ok(())
    }

    fn _delete(&mut self, key: PageCacheKey, clean_page: bool) -> Result<(), CacheError> {
        let Some(&entry_ptr) = self.map.get(&key) else {
            return Ok(());
        };

        let entry = unsafe { &mut *entry_ptr };
        let page = &entry.page;

        if page.is_locked() {
            return Err(CacheError::Locked {
                pgno: page.get().id,
            });
        }
        if page.is_dirty() {
            return Err(CacheError::Dirty {
                pgno: page.get().id,
            });
        }
        if page.is_pinned() {
            return Err(CacheError::Pinned {
                pgno: page.get().id,
            });
        }

        // Track evictable count before removing
        let was_evictable = Self::counted_as_evictable(page);

        if clean_page {
            page.clear_loaded();
            let _ = page.get().take_buffer();
        }

        // Remove from map first
        self.map.remove(&key);

        // If clock hand points to this entry, advance it before removing
        if self.clock_hand == entry_ptr {
            self.advance_clock_hand();
            // If hand is still pointing to the same entry after advance, we're removing the last entry
            if self.clock_hand == entry_ptr {
                self.clock_hand = std::ptr::null_mut();
            }
        }

        // Remove the entry from the queue
        unsafe {
            let mut cursor = self.queue.cursor_mut_from_ptr(entry_ptr);
            cursor.remove();
        }

        // Update evictable count after successful removal
        if was_evictable {
            self.evictable_count = self.evictable_count.saturating_sub(1);
        }

        Ok(())
    }

    #[inline]
    /// Deletes a page from the cache
    pub fn delete(&mut self, key: PageCacheKey) -> Result<(), CacheError> {
        trace!("cache_delete(key={:?})", key);
        self._delete(key, true)
    }

    /// Test-only: evict every clean, unpinned page, exactly as sustained cache
    /// pressure through `evict_one` eventually would (buffer taken, page unloaded),
    /// but deterministically and without the clock's second-chance heuristics.
    #[cfg(test)]
    pub fn test_evict_all_unpinned_clean(&mut self) {
        let keys: Vec<PageCacheKey> = self.map.keys().copied().collect();
        for key in keys {
            let _ = self._delete(key, true);
        }
    }

    #[inline]
    pub fn get(&mut self, key: &PageCacheKey) -> crate::Result<Option<PageRef>> {
        let Some(&entry_ptr) = self.map.get(key) else {
            return Ok(None);
        };

        let entry = unsafe { &mut *entry_ptr };
        let page = entry.page.clone();

        // Because we can abort a read_page completion, this means a page can be in the cache but be unloaded and unlocked.
        // However, if we do not evict that page from the page cache, we will return an unloaded page later which will trigger
        // assertions later on. This is worsened by the fact that page cache is not per `Statement`, so you can abort a completion
        // in one Statement, and trigger some error in the next one if we don't evict the page here.
        if !page.is_loaded() && !page.is_locked() {
            self.delete(*key)?;
            return Ok(None);
        }

        entry.bump_ref();
        Ok(Some(page))
    }

    #[inline]
    pub fn peek(&mut self, key: &PageCacheKey, touch: bool) -> Option<PageRef> {
        let &entry_ptr = self.map.get(key)?;
        let entry = unsafe { &mut *entry_ptr };
        let page = entry.page.clone();
        if touch {
            entry.bump_ref();
        }
        Some(page)
    }

    /// Resizes the cache to a new capacity.
    /// If shrinking, attempts to evict pages. if growing, increases capacity.
    pub fn resize(&mut self, new_cap: usize) -> CacheResizeResult {
        if new_cap == self.capacity {
            return CacheResizeResult::Done;
        }
        // Evict entries one by one until we're at new capacity
        while new_cap < self.len() {
            if self.evict_one().is_err() {
                return CacheResizeResult::PendingEvictions;
            }
        }
        self.capacity = new_cap;
        self.spill_threshold = ((new_cap * DEFAULT_SPILL_THRESHOLD_PERCENT) / 100).max(1);
        CacheResizeResult::Done
    }

    /// Returns true if the cache is at or above the spill threshold and spilling is enabled.
    /// This indicates that dirty pages should be flushed to make room for new pages.
    #[inline]
    pub fn needs_spill(&self) -> bool {
        let len = self.len();

        if len < self.spill_threshold || !self.spill_enabled {
            return false;
        }

        let needed_evictable = len.saturating_sub(self.spill_threshold);

        // Fast path: use tracked evictable_count to avoid O(n) scan:
        // evictable_count is a conservative upper bound on evictable pages,
        // Empty slots also count as available room since make_room_for uses them first.
        // Calculate if we have enough room (evictable + empty slots) and won't need to spill.
        let empty_slots = self.capacity.saturating_sub(len);
        let available_room = self.evictable_count.saturating_add(empty_slots);
        if available_room >= needed_evictable {
            return false;
        }

        // Slow path: do the full count since our estimate suggests we might need to spill.
        // The actual count may be lower than evictable_count due to locked/pinned pages.
        self.count_evictable_pages() < needed_evictable
    }

    #[inline]
    /// Count pages that can be evicted without spilling.
    fn count_evictable_pages(&self) -> usize {
        self.map
            .values()
            .filter(|&&entry_ptr| {
                let entry = unsafe { &*entry_ptr };
                Self::evictable(&entry.page)
            })
            .count()
    }

    /// Check if spilling is enabled for this cache.
    #[inline]
    pub fn is_spill_enabled(&self) -> bool {
        self.spill_enabled
    }

    /// Enable or disable spilling for this cache.
    pub fn set_spill_enabled(&mut self, enabled: bool) {
        self.spill_enabled = enabled;
    }

    #[inline]
    fn spillable(page: &PageRef) -> bool {
        page.is_dirty()
            && !page.is_spilled()
            && !page.is_locked()
            && !page.is_pinned()
            && Arc::strong_count(page) == 1
            && page.get().id.ne(&DatabaseHeader::PAGE_ID)
            && page.get().overflow_cells.is_empty()
    }

    #[inline]
    /// Check if a page should be counted as evictable for tracking purposes.
    /// This is a conservative check that ignores locked/pinned/strong_count state
    /// since those are typically short-lived. We track based on dirty/spilled state.
    fn counted_as_evictable(page: &PageRef) -> bool {
        // Page 1 is never evictable
        if page.get().id == DatabaseHeader::PAGE_ID {
            return false;
        }
        // A page is evictable if it's clean OR spilled
        !page.is_dirty() || page.is_spilled()
    }

    /// Notify the cache that a page has become dirty.
    /// This should be called when a page transitions from clean/spilled to dirty.
    /// The page must already be in the cache.
    pub fn notify_page_dirty(&mut self, key: PageCacheKey) {
        if let Some(&entry_ptr) = self.map.get(&key) {
            let entry = unsafe { &*entry_ptr };
            let page = &entry.page;
            // Page was evictable (clean or spilled) before becoming dirty,
            // now it's dirty && !spilled, so not evictable
            if page.get().id != DatabaseHeader::PAGE_ID {
                // Only decrement if we were counting it as evictable
                // (it was clean or spilled before this call)
                self.evictable_count = self.evictable_count.saturating_sub(1);
            }
        }
    }

    /// Notify the cache that a page has been spilled.
    /// This should be called when a page transitions from dirty to spilled.
    /// The page must already be in the cache.
    pub fn notify_page_spilled(&mut self, key: PageCacheKey) {
        if let Some(&entry_ptr) = self.map.get(&key) {
            let entry = unsafe { &*entry_ptr };
            let page = &entry.page;
            // Page was dirty && !spilled (not evictable), now it's spilled (evictable)
            if page.get().id != DatabaseHeader::PAGE_ID {
                self.evictable_count += 1;
            }
        }
    }

    /// Get the current evictable page count (for diagnostics/testing).
    #[cfg(test)]
    fn evictable_count(&self) -> usize {
        self.evictable_count
    }

    /// Collect dirty pages that can be spilled to make room in the cache.
    /// Pages that are locked or pinned are skipped.
    fn collect_spillable_pages(&self, max_pages: usize) -> Vec<PinGuard> {
        if !self.spill_enabled || max_pages == 0 {
            return Vec::new();
        }
        const EST_SPILL: usize = 128;
        let mut spillable: Vec<PinGuard> = Vec::with_capacity(EST_SPILL);

        for (_, &entry_ptr) in self.map.iter() {
            let entry = unsafe { &*entry_ptr };
            let page = &entry.page;
            if Self::spillable(page) {
                spillable.push(PinGuard::new(page.clone()));
            }
            if spillable.len() >= max_pages {
                break;
            }
        }
        spillable.sort_by_key(|pg| pg.get().id);
        spillable
    }

    /// Check if the cache needs spilling and return appropriate result.
    /// This is the main entry point for the spilling check during insertion.
    pub fn check_spill(&self, max_pages: usize) -> SpillResult {
        if !self.needs_spill() {
            return SpillResult::NotNeeded;
        }

        let pages = self.collect_spillable_pages(max_pages);
        if pages.is_empty() {
            SpillResult::CacheFull
        } else {
            SpillResult::PagesToSpill(pages)
        }
    }

    /// Ensures at least `n` free slots are available
    ///
    /// Uses the SIEVE algorithm to evict pages if necessary:
    /// Start at clock hand position
    /// If page ref_bit > 0, decrement and continue
    /// If page ref_bit == 0 and evictable, evict it
    /// If page is unevictable (dirty/locked/pinned), continue sweep
    /// On sweep, pages with ref_bit > 0 are given a second chance by decrementing
    /// their ref_bit and leaving them in place; only pages with ref_bit == 0 are evicted.
    ///
    /// Returns `CacheError::Full` if not enough pages can be evicted
    fn make_room_for(&mut self, n: usize, bypass_capacity: bool) -> Result<(), CacheError> {
        if bypass_capacity {
            return Ok(());
        }
        if n > self.capacity {
            return Err(CacheError::Full);
        }

        let target_len = self.capacity - n;
        while self.len() > target_len {
            self.evict_one()?;
        }
        Ok(())
    }

    #[inline]
    fn evictable(page: &PageRef) -> bool {
        (!page.is_dirty() || page.is_spilled())
            && !page.is_locked()
            && !page.is_pinned()
            && page.get().id.ne(&DatabaseHeader::PAGE_ID)
            && Arc::strong_count(page) == 1
    }

    /// Evicts a single page using the SIEVE algorithm
    fn evict_one(&mut self) -> Result<(), CacheError> {
        if self.len() == 0 {
            return Err(CacheError::InternalError(
                "Cannot evict from empty cache".into(),
            ));
        }

        let mut examined = 0usize;
        let max_examinations = self.len().saturating_mul(REF_MAX as usize + 1);

        while examined < max_examinations {
            // Clock hand should never be null here since we checked len() > 0
            turso_assert!(
                !self.clock_hand.is_null(),
                "page_cache: clock hand is null during eviction",
                { "entries": self.len() }
            );

            let entry_ptr = self.clock_hand;
            let entry = unsafe { &mut *entry_ptr };
            let key = entry.key;
            let page = &entry.page;

            let evictable = Self::evictable(page);

            if evictable && entry.ref_bit == CLEAR {
                turso_assert!(
                    Self::counted_as_evictable(page),
                    "mismatched evictable count state"
                );

                // Evict this entry
                self.advance_clock_hand();
                // Check if clock hand wrapped back to the same entry (meaning this is the only/last entry)
                if self.clock_hand == entry_ptr {
                    self.clock_hand = std::ptr::null_mut();
                }

                self.map.remove(&key);
                // Clean the page
                page.clear_loaded();
                let _ = page.get().take_buffer();

                // Remove from queue
                unsafe {
                    let mut cursor = self.queue.cursor_mut_from_ptr(entry_ptr);
                    cursor.remove();
                }

                // Update evictable count after successful eviction
                self.evictable_count = self.evictable_count.saturating_sub(1);

                return Ok(());
            } else if evictable {
                // Decrement ref bit and continue
                entry.decrement_ref();
                self.advance_clock_hand();
                examined += 1;
            } else {
                // Skip unevictable page
                self.advance_clock_hand();
                examined += 1;
            }
        }

        Err(CacheError::Full)
    }

    pub fn clear(&mut self, clear_dirty: bool) -> Result<(), CacheError> {
        // Check all pages are clean
        for &entry_ptr in self.map.values() {
            let entry = unsafe { &*entry_ptr };
            if entry.page.is_dirty() && !clear_dirty {
                return Err(CacheError::Dirty {
                    pgno: entry.page.get().id,
                });
            }
        }

        // Clean all pages
        for &entry_ptr in self.map.values() {
            let entry = unsafe { &*entry_ptr };
            entry.page.clear_loaded();
            let _ = entry.page.get().take_buffer();
        }

        self.map.clear();
        self.queue.clear();
        self.clock_hand = std::ptr::null_mut();
        self.evictable_count = 0;
        Ok(())
    }

    /// Removes all pages from the cache with pgno greater than max_page_num
    pub fn truncate(&mut self, max_page_num: usize) -> Result<(), CacheError> {
        for key in self
            .map
            .keys()
            .filter(|k| k.0 > max_page_num)
            .copied()
            .collect::<Vec<_>>()
        {
            self.delete(key)?;
        }
        Ok(())
    }

    /// Removes clean cached pages that were read from WAL frames newer than a
    /// rollback target. Dirty pages are preserved because they may contain
    /// restored transaction-local state that still needs to be committed or
    /// rolled back by an outer savepoint.
    pub fn delete_clean_pages_after_wal_frame(&mut self, max_frame: u64) -> Result<(), CacheError> {
        for key in self
            .map
            .iter()
            .filter_map(|(key, &entry_ptr)| {
                let entry = unsafe { &*entry_ptr };
                let page = &entry.page;
                if !page.is_dirty() && page.has_wal_tag() {
                    let (frame, _) = page.wal_tag_pair();
                    if frame > max_frame {
                        return Some(*key);
                    }
                }
                None
            })
            .collect::<Vec<_>>()
        {
            self.delete(key)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn print(&self) {
        use crate::sync::atomic::Ordering;

        tracing::debug!("page_cache_len={}", self.map.len());

        let mut cursor = self.queue.front();
        let mut i = 0;
        while let Some(entry) = cursor.get() {
            let page = &entry.page;
            tracing::debug!(
                "slot={}, page={:?}, flags={}, pin_count={}, ref_bit={:?}",
                i,
                entry.key,
                page.get().flags.load(Ordering::SeqCst),
                page.get().pin_count.load(Ordering::SeqCst),
                entry.ref_bit,
            );
            cursor.move_next();
            i += 1;
        }
    }

    #[cfg(test)]
    pub fn keys(&mut self) -> Vec<PageCacheKey> {
        self.map.keys().copied().collect()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[cfg(test)]
    fn verify_cache_integrity(&self) {
        use rustc_hash::FxHashSet as HashSet;

        let map_len = self.map.len();

        // Count entries in queue
        let mut queue_len = 0;
        let mut cursor = self.queue.front();
        let mut seen_keys = HashSet::default();

        while let Some(entry) = cursor.get() {
            queue_len += 1;
            seen_keys.insert(entry.key);
            cursor.move_next();
        }

        assert_eq!(map_len, queue_len, "map and queue length mismatch");
        assert_eq!(map_len, seen_keys.len(), "duplicate keys in queue");

        // Verify all map entries are in queue
        for &key in self.map.keys() {
            assert!(seen_keys.contains(&key), "map key not in queue");
        }

        // Verify clock hand
        if !self.clock_hand.is_null() {
            assert!(map_len > 0, "clock hand set but map is empty");
            let hand_key = unsafe { (*self.clock_hand).key };
            assert!(
                self.map.contains_key(&hand_key),
                "clock hand points to non-existent entry"
            );
        } else {
            assert_eq!(map_len, 0, "clock hand null but map not empty");
        }
    }

    #[cfg(test)]
    fn ref_of(&self, key: &PageCacheKey) -> Option<u8> {
        self.map.get(key).map(|&ptr| unsafe { (*ptr).ref_bit })
    }
}

impl Default for PageCache {
    fn default() -> Self {
        PageCache::new(DEFAULT_PAGE_CACHE_SIZE_IN_PAGES)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page_cache::CacheError;
    use crate::storage::pager::{Page, PageRef};
    use crate::sync::Arc;
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };

    fn create_key(id: usize) -> PageCacheKey {
        PageCacheKey::new(id)
    }

    pub fn page_with_content(page_id: usize) -> PageRef {
        let page = Arc::new(Page::new(page_id as i64));
        {
            let inner = page.get();
            inner.set_buffer(Arc::new(crate::Buffer::new_temporary(4096)));
        }
        page.set_loaded();
        page
    }

    fn insert_page(cache: &mut PageCache, id: usize) -> PageCacheKey {
        let key = create_key(id);
        let page = page_with_content(id);
        cache
            .insert(key, page)
            .unwrap_or_else(|e| panic!("Failed to insert page {id}: {e:?}"));
        key
    }

    #[test]
    fn test_delete_only_element() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        cache.verify_cache_integrity();
        assert_eq!(cache.len(), 1);

        assert!(cache.delete(key1).is_ok());

        assert_eq!(
            cache.len(),
            0,
            "Length should be 0 after deleting only element"
        );
        assert!(
            !cache.contains_key(&key1),
            "Cache should not contain key after delete"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_detach_tail() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1); // tail
        let _key2 = insert_page(&mut cache, 2); // middle
        let _key3 = insert_page(&mut cache, 3); // head
        cache.verify_cache_integrity();
        assert_eq!(cache.len(), 3);

        // Delete tail
        assert!(cache.delete(key1).is_ok());
        assert_eq!(cache.len(), 2, "Length should be 2 after deleting tail");
        assert!(
            !cache.contains_key(&key1),
            "Cache should not contain deleted tail key"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_insert_existing_key_updates_in_place() {
        let mut cache = PageCache::default();
        let key1 = create_key(1);
        let page1_v1 = page_with_content(1);
        let page1_v2 = page1_v1.clone(); // Same Arc instance

        assert!(cache.insert(key1, page1_v1).is_ok());
        assert_eq!(cache.len(), 1);

        // Inserting same page instance should return KeyExists error
        let result = cache.insert(key1, page1_v2);
        assert_eq!(result, Err(CacheError::KeyExists));
        assert_eq!(cache.len(), 1);

        // Verify the page is still accessible
        assert!(cache.get(&key1).unwrap().is_some());
        cache.verify_cache_integrity();
    }

    #[test]
    #[should_panic(expected = "Attempted to insert different page with same key")]
    fn test_insert_different_page_same_key_panics() {
        let mut cache = PageCache::default();
        let key1 = create_key(1);
        let page1_v1 = page_with_content(1);
        let page1_v2 = page_with_content(1); // Different Arc instance

        assert!(cache.insert(key1, page1_v1).is_ok());
        assert_eq!(cache.len(), 1);
        cache.verify_cache_integrity();

        // This should panic because it's a different page instance
        let _ = cache.insert(key1, page1_v2);
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let mut cache = PageCache::default();
        let key_nonexist = create_key(99);

        // Deleting non-existent key should be a no-op (returns Ok)
        assert!(cache.delete(key_nonexist).is_ok());
        assert_eq!(cache.len(), 0);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_evict() {
        // Note: page 1 is DatabaseHeader and is never evictable, so use page ids >= 2
        let mut cache = PageCache::new_with_spill(1, true);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        // With capacity=1, inserting key3 should evict key2
        assert_eq!(cache.get(&key3).unwrap().unwrap().get().id, 3);
        assert!(
            cache.get(&key2).unwrap().is_none(),
            "key2 should be evicted"
        );

        // key3 should still be accessible
        assert_eq!(cache.get(&key3).unwrap().unwrap().get().id, 3);
        assert!(
            cache.get(&key2).unwrap().is_none(),
            "capacity=1 should have evicted the older page"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_sieve_touch_non_tail_does_not_affect_immediate_eviction() {
        // SIEVE algorithm: touching a non-tail page marks it but doesn't move it.
        // The tail (if unmarked) will still be the first eviction candidate.
        // Note: page 1 is DatabaseHeader and is never evictable, so use page ids >= 2

        // Insert 2,3,4 -> order [4,3,2] with tail=2
        let mut cache = PageCache::new_with_spill(3, true);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);
        let key4 = insert_page(&mut cache, 4);

        // Touch key3 (middle) to mark it with reference bit
        assert!(cache.get(&key3).unwrap().is_some());

        // Insert 5: SIEVE examines tail (key2, unmarked) -> evict key2
        let key5 = insert_page(&mut cache, 5);

        assert!(
            cache.get(&key3).unwrap().is_some(),
            "marked non-tail (key3) should remain"
        );
        assert!(cache.get(&key4).unwrap().is_some(), "key4 should remain");
        assert!(
            cache.get(&key5).unwrap().is_some(),
            "key5 was just inserted"
        );
        assert!(
            cache.get(&key2).unwrap().is_none(),
            "unmarked tail (key2) should be evicted first"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn clock_second_chance_decrements_tail_then_evicts_next() {
        let mut cache = PageCache::new_with_spill(3, true);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);
        assert_eq!(cache.len(), 3);
        assert!(cache.get(&key1).unwrap().is_some());
        let key4 = insert_page(&mut cache, 4);
        assert!(cache.get(&key1).unwrap().is_some(), "key1 should survive");
        assert!(cache.get(&key2).unwrap().is_some(), "key2 remains");
        assert!(cache.get(&key4).unwrap().is_some(), "key4 inserted");
        assert!(
            cache.get(&key3).unwrap().is_none(),
            "key3 (next after tail) evicted"
        );
        assert_eq!(cache.len(), 3);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_delete_locked_page() {
        let mut cache = PageCache::default();
        let key = insert_page(&mut cache, 1);
        let page = cache.get(&key).unwrap().unwrap();
        page.set_locked();

        assert_eq!(cache.delete(key), Err(CacheError::Locked { pgno: 1 }));
        assert_eq!(cache.len(), 1, "Locked page should not be deleted");
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_delete_dirty_page() {
        let mut cache = PageCache::default();
        let key = insert_page(&mut cache, 1);
        let page = cache.get(&key).unwrap().unwrap();
        page.set_dirty();

        assert_eq!(cache.delete(key), Err(CacheError::Dirty { pgno: 1 }));
        assert_eq!(cache.len(), 1, "Dirty page should not be deleted");
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_delete_pinned_page() {
        let mut cache = PageCache::default();
        let key = insert_page(&mut cache, 1);
        let page = cache.get(&key).unwrap().unwrap();
        page.pin();

        assert_eq!(cache.delete(key), Err(CacheError::Pinned { pgno: 1 }));
        assert_eq!(cache.len(), 1, "Pinned page should not be deleted");
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_make_room_for_with_dirty_pages() {
        let mut cache = PageCache::new_with_spill(2, true);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // Make both pages dirty (unevictable)
        cache.get(&key1).unwrap().unwrap().set_dirty();
        cache.get(&key2).unwrap().unwrap().set_dirty();

        // Try to insert a third page, should fail because can't evict dirty pages
        let key3 = create_key(3);
        let page3 = page_with_content(3);
        let result = cache.insert(key3, page3);

        assert_eq!(result, Err(CacheError::Full));
        assert_eq!(cache.len(), 2);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_force_insert_allows_temporary_over_capacity_cache() {
        let mut cache = PageCache::new_with_spill(2, true);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // Make both pages dirty (unevictable): a normal insert must fail.
        for key in [key1, key2] {
            cache.notify_page_dirty(key);
            cache.peek(&key, false).unwrap().set_dirty();
        }
        let key3 = create_key(3);
        assert_eq!(
            cache.insert(key3, page_with_content(3)),
            Err(CacheError::Full)
        );

        // The capacity is a soft limit: a forced insert must succeed.
        cache.force_insert_page(key3, page_with_content(3)).unwrap();
        assert_eq!(cache.len(), 3);
        assert!(cache.contains_key(&key3));
        cache.verify_cache_integrity();

        // Once pages become evictable again, the next normal insert drains
        // the excess back under capacity.
        for key in [key1, key2] {
            cache.notify_page_spilled(key);
            cache.peek(&key, false).unwrap().set_spilled();
        }
        let key4 = create_key(4);
        cache.insert(key4, page_with_content(4)).unwrap();

        assert_eq!(cache.len(), 2);
        assert!(cache.contains_key(&key4));
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_force_upsert_allows_temporary_over_capacity_cache() {
        let mut cache = PageCache::new_with_spill(2, true);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        for key in [key2, key3] {
            cache.notify_page_dirty(key);
            cache.peek(&key, false).unwrap().set_dirty();
        }

        let key4 = create_key(4);
        let page4 = page_with_content(4);
        page4.set_dirty();
        cache.force_upsert_page(key4, page4).unwrap();

        assert_eq!(cache.len(), 3);
        assert!(cache.contains_key(&key4));
        cache.verify_cache_integrity();

        for key in [key2, key3] {
            cache.notify_page_spilled(key);
            cache.peek(&key, false).unwrap().set_spilled();
        }

        let key5 = create_key(5);
        cache.insert(key5, page_with_content(5)).unwrap();

        assert_eq!(cache.len(), 2);
        assert!(cache.contains_key(&key4));
        assert!(cache.contains_key(&key5));
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_insert_and_get() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        assert_eq!(cache.get(&key1).unwrap().unwrap().get().id, 1);
        assert_eq!(cache.get(&key2).unwrap().unwrap().get().id, 2);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_over_capacity() {
        // Test SIEVE eviction when exceeding capacity
        // Note: page 1 is DatabaseHeader and is never evictable, so use page ids >= 2
        let mut cache = PageCache::new_with_spill(2, true);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        // Insert 4: tail (key2, unmarked) should be evicted
        let key4 = insert_page(&mut cache, 4);

        assert_eq!(cache.len(), 2);
        assert!(cache.get(&key3).unwrap().is_some(), "key3 should remain");
        assert!(cache.get(&key4).unwrap().is_some(), "key4 just inserted");
        assert!(
            cache.get(&key2).unwrap().is_none(),
            "key2 (oldest, unmarked) should be evicted"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_delete() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);

        assert!(cache.delete(key1).is_ok());
        assert!(cache.get(&key1).unwrap().is_none());
        assert_eq!(cache.len(), 0);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_clear() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        assert!(cache.clear(false).is_ok());
        assert!(cache.get(&key1).unwrap().is_none());
        assert!(cache.get(&key2).unwrap().is_none());
        assert_eq!(cache.len(), 0);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_resize_smaller_success() {
        let mut cache = PageCache::default();
        for i in 1..=5 {
            let _ = insert_page(&mut cache, i);
        }
        assert_eq!(cache.len(), 5);

        let result = cache.resize(3);
        assert_eq!(result, CacheResizeResult::Done);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.capacity(), 3);

        // Should still be able to insert after resize
        assert!(cache.insert(create_key(6), page_with_content(6)).is_ok());
        assert_eq!(cache.len(), 3); // One was evicted to make room
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_detach_with_multiple_pages() {
        let mut cache = PageCache::default();
        let _key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let _key3 = insert_page(&mut cache, 3);

        // Delete middle element (key2)
        assert!(cache.delete(key2).is_ok());

        // Verify structure after deletion
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains_key(&key2));

        cache.verify_cache_integrity();
    }

    #[test]
    fn test_delete_multiple_elements() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);
        cache.verify_cache_integrity();
        assert_eq!(cache.len(), 3);

        // Delete head (key3)
        assert!(cache.delete(key3).is_ok());
        assert_eq!(cache.len(), 2, "Length should be 2 after deleting head");
        assert!(
            !cache.contains_key(&key3),
            "Cache should not contain deleted head key"
        );
        cache.verify_cache_integrity();

        // Delete tail (key1)
        assert!(cache.delete(key1).is_ok());
        assert_eq!(cache.len(), 1, "Length should be 1 after deleting two");
        cache.verify_cache_integrity();

        // Delete last element (key2)
        assert!(cache.delete(key2).is_ok());
        assert_eq!(cache.len(), 0, "Length should be 0 after deleting all");
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_resize_larger() {
        let mut cache = PageCache::new_with_spill(2, true);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert_eq!(cache.len(), 2);

        let result = cache.resize(5);
        assert_eq!(result, CacheResizeResult::Done);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.capacity(), 5);

        // Existing pages should still be accessible
        assert!(cache.get(&key1).is_ok_and(|p| p.is_some()));
        assert!(cache.get(&key2).is_ok_and(|p| p.is_some()));

        // Now we should be able to add 3 more without eviction
        for i in 3..=5 {
            let _ = insert_page(&mut cache, i);
        }
        assert_eq!(cache.len(), 5);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_resize_same_capacity() {
        let mut cache = PageCache::new_with_spill(3, true);
        for i in 1..=3 {
            let _ = insert_page(&mut cache, i);
        }

        let result = cache.resize(3);
        assert_eq!(result, CacheResizeResult::Done);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.capacity(), 3);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_truncate_page_cache() {
        let mut cache = PageCache::new_with_spill(10, true);
        let _ = insert_page(&mut cache, 1);
        let _ = insert_page(&mut cache, 4);
        let _ = insert_page(&mut cache, 8);
        let _ = insert_page(&mut cache, 10);

        // Truncate to keep only pages <= 4
        cache.truncate(4).unwrap();

        assert!(cache.contains_key(&PageCacheKey(1)));
        assert!(cache.contains_key(&PageCacheKey(4)));
        assert!(!cache.contains_key(&PageCacheKey(8)));
        assert!(!cache.contains_key(&PageCacheKey(10)));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.capacity(), 10);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_truncate_page_cache_remove_all() {
        let mut cache = PageCache::new_with_spill(10, true);
        let _ = insert_page(&mut cache, 8);
        let _ = insert_page(&mut cache, 10);

        // Truncate to 4 (removes all pages since they're > 4)
        cache.truncate(4).unwrap();

        assert!(!cache.contains_key(&PageCacheKey(8)));
        assert!(!cache.contains_key(&PageCacheKey(10)));
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.capacity(), 10);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_fuzz() {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        tracing::info!("fuzz test seed: {}", seed);

        let max_pages = 10;
        let mut cache = PageCache::new_with_spill(10, true);
        let mut reference_map = HashMap::default();

        for _ in 0..10000 {
            cache.print();

            match rng.next_u64() % 2 {
                0 => {
                    // Insert operation
                    let id_page = rng.next_u64() % max_pages;
                    let key = PageCacheKey::new(id_page as usize);
                    #[allow(clippy::arc_with_non_send_sync)]
                    let page = Arc::new(Page::new(id_page as i64));

                    if cache.peek(&key, false).is_some() {
                        continue; // Skip duplicate page ids
                    }

                    tracing::debug!("inserting page {:?}", key);
                    match cache.insert(key, page.clone()) {
                        Err(CacheError::Full) => {} // Expected, ignore
                        Err(err) => {
                            panic!("Cache insertion failed unexpectedly: {err:?}");
                        }
                        Ok(_) => {
                            reference_map.insert(key, page);
                            // Clean up reference_map if cache evicted something
                            if cache.len() < reference_map.len() {
                                reference_map.retain(|k, _| cache.contains_key(k));
                            }
                        }
                    }
                    assert!(cache.len() <= 10, "Cache size exceeded capacity");
                }
                1 => {
                    // Delete operation
                    let random = rng.next_u64() % 2 == 0;
                    let key = if random || reference_map.is_empty() {
                        let id_page: u64 = rng.next_u64() % max_pages;
                        PageCacheKey::new(id_page as usize)
                    } else {
                        let i = rng.next_u64() as usize % reference_map.len();
                        *reference_map.keys().nth(i).unwrap()
                    };

                    tracing::debug!("removing page {:?}", key);
                    reference_map.remove(&key);
                    assert!(cache.delete(key).is_ok());
                }
                _ => unreachable!(),
            }

            cache.verify_cache_integrity();

            // Verify all pages in reference_map are in cache
            for (key, page) in &reference_map {
                let cached_page = cache.peek(key, false).expect("Page should be in cache");
                assert_eq!(cached_page.get().id, key.0);
                assert_eq!(page.get().id, key.0);
            }
        }
    }

    #[test]
    fn test_peek_without_touch() {
        // Test that peek with touch=false doesn't mark pages
        // Note: page 1 is DatabaseHeader and is never evictable, so use page ids >= 2
        let mut cache = PageCache::new_with_spill(2, true);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        // Peek key2 without touching (no ref bit set)
        assert!(cache.peek(&key2, false).is_some());

        // Insert 4: should evict unmarked tail (key2)
        let key4 = insert_page(&mut cache, 4);

        assert!(cache.get(&key3).unwrap().is_some(), "key3 should remain");
        assert!(
            cache.get(&key4).unwrap().is_some(),
            "key4 was just inserted"
        );
        assert!(
            cache.get(&key2).unwrap().is_none(),
            "key2 should be evicted since peek(false) didn't mark it"
        );
        assert_eq!(cache.len(), 2);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_peek_with_touch() {
        // Test that peek with touch=true marks pages for SIEVE
        let mut cache = PageCache::new_with_spill(2, true);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // Peek key1 WITH touching (sets ref bit)
        assert!(cache.peek(&key1, true).is_some());

        // Insert 3: key1 is marked, so it gets second chance
        // key2 becomes new tail and gets evicted
        let key3 = insert_page(&mut cache, 3);

        assert!(
            cache.get(&key1).unwrap().is_some(),
            "key1 should survive (was marked)"
        );
        assert!(
            cache.get(&key3).unwrap().is_some(),
            "key3 was just inserted"
        );
        assert!(
            cache.get(&key2).unwrap().is_none(),
            "key2 should be evicted after key1's second chance"
        );
        assert_eq!(cache.len(), 2);
        cache.verify_cache_integrity();
    }

    #[test]
    #[ignore = "long running test, remove ignore to verify memory stability"]
    fn test_clear_memory_stability() {
        let initial_memory = memory_stats::memory_stats().unwrap().physical_mem;

        for _ in 0..100000 {
            let mut cache = PageCache::new(1000);

            for i in 0..1000 {
                let key = create_key(i);
                let page = page_with_content(i);
                cache.insert(key, page).unwrap();
            }

            cache.clear(false).unwrap();
            drop(cache);
        }

        let final_memory = memory_stats::memory_stats().unwrap().physical_mem;
        let growth = final_memory.saturating_sub(initial_memory);

        println!("Memory growth: {growth} bytes");
        assert!(
            growth < 10_000_000,
            "Memory grew by {growth} bytes over test cycles (limit: 10MB)",
        );
    }

    #[test]
    fn clock_drains_hot_page_within_single_sweep_when_others_are_unevictable() {
        // Note: page 1 is DatabaseHeader and is never evictable, so use page ids >= 2
        // capacity 3: [4(head), 3, 2(tail)]
        let mut c = PageCache::new_with_spill(3, true);
        let k2 = insert_page(&mut c, 2);
        let k3 = insert_page(&mut c, 3);
        let k4 = insert_page(&mut c, 4);

        // Make k2 hot: bump to Max
        for _ in 0..3 {
            assert!(c.get(&k2).unwrap().is_some());
        }
        assert!(matches!(c.ref_of(&k2), Some(REF_MAX)));

        // Make other pages unevictable; clock must keep revisiting k2.
        c.get(&k3).unwrap().unwrap().set_dirty();
        c.get(&k4).unwrap().unwrap().set_dirty();

        // Insert 5 -> sweep rotates as needed, draining k2 and evicting it.
        let k5 = insert_page(&mut c, 5);

        assert!(
            c.get(&k2).unwrap().is_none(),
            "k2 should be evicted after its credit drains"
        );
        assert!(c.get(&k3).unwrap().is_some(), "k3 is dirty (unevictable)");
        assert!(c.get(&k4).unwrap().is_some(), "k4 is dirty (unevictable)");
        assert!(c.get(&k5).unwrap().is_some(), "k5 just inserted");
        c.verify_cache_integrity();
    }

    #[test]
    fn gclock_hot_survives_scan_pages() {
        let mut c = PageCache::new_with_spill(4, true);
        let _k1 = insert_page(&mut c, 1);
        let k2 = insert_page(&mut c, 2);
        let _k3 = insert_page(&mut c, 3);
        let _k4 = insert_page(&mut c, 4);

        // Make k2 truly hot: three real touches
        for _ in 0..3 {
            assert!(c.get(&k2).unwrap().is_some());
        }
        assert!(matches!(c.ref_of(&k2), Some(REF_MAX)));

        // Now simulate a scan inserting new pages 5..10 (one-hit wonders).
        for id in 5..=10 {
            let _ = insert_page(&mut c, id);
        }

        // Hot k2 should still be present; most single-hit scan pages should churn.
        assert!(
            c.get(&k2).unwrap().is_some(),
            "hot page should survive scan"
        );
        // The earliest single-hit page should be gone.
        assert!(c.get(&create_key(5)).unwrap().is_none());
        c.verify_cache_integrity();
    }

    #[test]
    fn hand_stays_valid_after_deleting_only_element() {
        let mut c = PageCache::new_with_spill(2, true);
        let k = insert_page(&mut c, 1);
        assert!(c.delete(k).is_ok());
        // Inserting again should not panic and should succeed
        let _ = insert_page(&mut c, 2);
        c.verify_cache_integrity();
    }

    #[test]
    fn hand_is_reset_after_clear_and_resize() {
        let mut c = PageCache::new_with_spill(3, true);
        for i in 1..=3 {
            let _ = insert_page(&mut c, i);
        }
        c.clear(false).unwrap();
        // No elements; insert should not rely on stale hand
        let _ = insert_page(&mut c, 10);

        // Resize from 1 -> 4 and back should not OOB the hand
        assert_eq!(c.resize(4), CacheResizeResult::Done);
        assert_eq!(c.resize(1), CacheResizeResult::Done);
        let _ = insert_page(&mut c, 11);
        c.verify_cache_integrity();
    }

    #[test]
    fn resize_preserves_ref_and_recency() {
        let mut c = PageCache::new_with_spill(4, true);
        let _k1 = insert_page(&mut c, 1);
        let k2 = insert_page(&mut c, 2);
        let _k3 = insert_page(&mut c, 3);
        let _k4 = insert_page(&mut c, 4);
        // Make k2 hot.
        for _ in 0..3 {
            assert!(c.get(&k2).unwrap().is_some());
        }
        let _r_before = c.ref_of(&k2);

        // Shrink to 3 (one page will be evicted during repack/next insert)
        assert_eq!(c.resize(3), CacheResizeResult::Done);
        assert!(matches!(c.ref_of(&k2), _r_before));

        // Force an eviction; hot k2 should survive more passes.
        let _ = insert_page(&mut c, 5);
        assert!(c.get(&k2).unwrap().is_some());
        c.verify_cache_integrity();
    }

    #[test]
    fn test_sieve_second_chance_preserves_marked_page() {
        let mut cache = PageCache::new_with_spill(3, true);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        // Mark key1 for second chance
        assert!(cache.get(&key1).unwrap().is_some());

        let key4 = insert_page(&mut cache, 4);
        // CLOCK sweep from hand:
        // - key1 marked -> decrement, continue
        // - key3 (MRU) unmarked -> evict
        assert!(
            cache.get(&key1).unwrap().is_some(),
            "key1 had ref bit set, got second chance"
        );
        assert!(
            cache.get(&key3).unwrap().is_none(),
            "key3 (MRU) should be evicted"
        );
        assert!(cache.get(&key4).unwrap().is_some(), "key4 just inserted");
        assert!(
            cache.get(&key2).unwrap().is_some(),
            "key2 (middle) should remain"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_clock_sweep_wraps_around() {
        // Test that clock hand properly wraps around the circular list
        let mut cache = PageCache::new_with_spill(3, true);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        // Mark all pages
        assert!(cache.get(&key1).unwrap().is_some());
        assert!(cache.get(&key2).unwrap().is_some());
        assert!(cache.get(&key3).unwrap().is_some());

        // Insert 4: hand will sweep full circle, decrementing all refs
        // then sweep again and evict first unmarked page
        let key4 = insert_page(&mut cache, 4);

        // One page was evicted after full sweep
        assert_eq!(cache.len(), 3);
        assert!(cache.get(&key4).unwrap().is_some());

        // Verify exactly one of the original pages was evicted
        let survivors = [key1, key2, key3]
            .iter()
            .filter(|k| cache.get(k).unwrap().is_some())
            .count();
        assert_eq!(survivors, 2, "Should have 2 survivors from original 3");
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_circular_list_single_element() {
        let mut cache = PageCache::new_with_spill(3, true);
        let key1 = insert_page(&mut cache, 1);

        // Single element exists
        assert_eq!(cache.len(), 1);
        assert!(cache.contains_key(&key1));

        // Delete single element
        assert!(cache.delete(key1).is_ok());
        assert!(cache.clock_hand.is_null());

        // Insert after empty should work
        let key2 = insert_page(&mut cache, 2);
        assert_eq!(cache.len(), 1);
        assert!(cache.contains_key(&key2));
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_hand_advances_on_eviction() {
        // Note: page 1 is DatabaseHeader and is never evictable, so use page ids >= 2
        let mut cache = PageCache::new_with_spill(2, true);
        let _key2 = insert_page(&mut cache, 2);
        let _key3 = insert_page(&mut cache, 3);

        // Note initial hand position
        let initial_hand = cache.clock_hand;

        // Force eviction
        let _key4 = insert_page(&mut cache, 4);

        // Hand should exist (not null)
        let new_hand = cache.clock_hand;
        assert!(!new_hand.is_null());
        // Hand moved during sweep (exact position depends on eviction)
        assert!(initial_hand.is_null() || new_hand != initial_hand || cache.len() < 2);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_multi_level_ref_counting() {
        let mut cache = PageCache::new_with_spill(2, true);
        let key1 = insert_page(&mut cache, 1);
        let _key2 = insert_page(&mut cache, 2);

        // Bump key1 to MAX (3 accesses)
        for _ in 0..3 {
            assert!(cache.get(&key1).unwrap().is_some());
        }
        assert_eq!(cache.ref_of(&key1), Some(REF_MAX));

        // Insert multiple new pages - key1 should survive longer
        for i in 3..6 {
            let _ = insert_page(&mut cache, i);
        }

        // key1 might still be there due to high ref count
        // (depends on exact sweep pattern, but it got multiple chances)
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_resize_maintains_circular_structure() {
        let mut cache = PageCache::new_with_spill(5, true);
        for i in 1..=4 {
            let _ = insert_page(&mut cache, i);
        }

        // Resize smaller
        assert_eq!(cache.resize(2), CacheResizeResult::Done);
        assert_eq!(cache.len(), 2);

        // Verify structure via integrity check
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_link_after_correctness() {
        let mut cache = PageCache::new_with_spill(4, true);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        // Verify all keys are in cache
        assert!(cache.contains_key(&key1));
        assert!(cache.contains_key(&key2));
        assert!(cache.contains_key(&key3));
        assert_eq!(cache.len(), 3);

        cache.verify_cache_integrity();
    }

    #[test]
    fn test_evictable_count_tracking() {
        // Test that evictable_count is tracked correctly for fast-path spill check
        // Note: page 1 is DatabaseHeader and is never evictable
        let mut cache = PageCache::new_with_spill(10, true);

        // Insert clean pages (all evictable except page 1)
        let key1 = insert_page(&mut cache, 1); // page 1 is never evictable
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        // Page 1 is not counted, pages 2 and 3 are evictable
        assert_eq!(cache.evictable_count(), 2);

        // Make page 2 dirty - it becomes non-evictable
        cache.notify_page_dirty(key2);
        assert_eq!(cache.evictable_count(), 1);

        // Make page 2 spilled - it becomes evictable again
        cache.notify_page_spilled(key2);
        assert_eq!(cache.evictable_count(), 2);

        // Delete page 3 - evictable count decreases
        assert!(cache.delete(key3).is_ok());
        assert_eq!(cache.evictable_count(), 1);

        // Delete page 1 (which wasn't counted) - no change
        assert!(cache.delete(key1).is_ok());
        assert_eq!(cache.evictable_count(), 1);

        // Clear cache
        assert!(cache.clear(true).is_ok());
        assert_eq!(cache.evictable_count(), 0);

        cache.verify_cache_integrity();
    }

    #[test]
    fn test_needs_spill_fast_path() {
        // Test that needs_spill uses the fast path when we have enough evictable pages
        // Capacity 10, threshold 90% = 9, so when len > 9 we need some evictable pages
        let mut cache = PageCache::new_with_spill(10, true);

        // Insert 10 clean pages (all evictable except page 1)
        for i in 1..=10 {
            let _ = insert_page(&mut cache, i);
        }

        // len=10, threshold=9, needed_evictable = 10-9 = 1
        // evictable_count = 9 (pages 2-10, page 1 not counted)
        // Fast path: 9 >= 1, so no spill needed
        assert!(!cache.needs_spill());
        assert_eq!(cache.evictable_count(), 9);

        // Make all pages dirty
        for i in 2..=10 {
            let key = create_key(i);
            cache.notify_page_dirty(key);
            cache.peek(&key, false).unwrap().set_dirty();
        }

        // Now evictable_count = 0 and pages are actually dirty
        // needed_evictable = 1, but we have 0 evictable pages
        assert_eq!(cache.evictable_count(), 0);
        // needs_spill should return true because we need 1 evictable page but have 0
        assert!(cache.needs_spill());

        // Mark pages as spilled
        for i in 2..=10 {
            let key = create_key(i);
            cache.notify_page_spilled(key);
            cache.peek(&key, false).unwrap().set_spilled();
        }

        // Now evictable_count = 9 and pages are spilled (evictable)
        assert_eq!(cache.evictable_count(), 9);
        // Fast path: 9 >= 1, so no spill needed
        assert!(!cache.needs_spill());

        cache.verify_cache_integrity();
    }
}
