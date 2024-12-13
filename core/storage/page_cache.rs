use std::{cell::RefCell, collections::HashMap, ptr::NonNull};

use log::debug;

use super::pager::PageRef;

// In limbo, page cache is shared by default, meaning that multiple frames from WAL can reside in
// the cache, meaning, we need a way to differentiate between pages cached in different
// connections. For this we include the max_frame that will read a connection from so that if two
// connections have different max_frames, they might or not have different frame read from WAL.
//
// WAL was introduced after Shared cache in SQLite, so this is why these two features don't work
// well together because pages with different snapshots may collide.
#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub struct PageCacheKey {
    pgno: usize,
    max_frame: Option<u64>,
}

#[allow(dead_code)]
struct PageCacheEntry {
    key: PageCacheKey,
    page: PageRef,
    prev: Option<NonNull<PageCacheEntry>>,
    next: Option<NonNull<PageCacheEntry>>,
}

impl PageCacheEntry {
    fn as_non_null(&mut self) -> NonNull<PageCacheEntry> {
        NonNull::new(&mut *self).unwrap()
    }
}

pub struct DumbLruPageCache {
    capacity: usize,
    map: RefCell<HashMap<PageCacheKey, NonNull<PageCacheEntry>>>,
    head: RefCell<Option<NonNull<PageCacheEntry>>>,
    tail: RefCell<Option<NonNull<PageCacheEntry>>>,
}
unsafe impl Send for DumbLruPageCache {}
unsafe impl Sync for DumbLruPageCache {}

impl PageCacheKey {
    pub fn new(pgno: usize, max_frame: Option<u64>) -> Self {
        Self { pgno, max_frame }
    }
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

    pub fn contains_key(&mut self, key: &PageCacheKey) -> bool {
        self.map.borrow().contains_key(key)
    }

    pub fn insert(&mut self, key: PageCacheKey, value: PageRef) {
        self._delete(key.clone(), false);
        debug!("cache_insert(key={:?})", key);
        let mut entry = Box::new(PageCacheEntry {
            key: key.clone(),
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

    pub fn delete(&mut self, key: PageCacheKey) {
        self._delete(key, true)
    }

    pub fn _delete(&mut self, key: PageCacheKey, clean_page: bool) {
        debug!("cache_delete(key={:?}, clean={})", key, clean_page);
        let ptr = self.map.borrow_mut().remove(&key);
        if ptr.is_none() {
            return;
        }
        let mut ptr = ptr.unwrap();
        {
            let ptr = unsafe { ptr.as_mut() };
            self.detach(ptr, clean_page);
        }
        unsafe { std::ptr::drop_in_place(ptr.as_ptr()) };
    }

    fn get_ptr(&mut self, key: &PageCacheKey) -> Option<NonNull<PageCacheEntry>> {
        let m = self.map.borrow_mut();
        let ptr = m.get(key);
        ptr.copied()
    }

    pub fn get(&mut self, key: &PageCacheKey) -> Option<PageRef> {
        debug!("cache_get(key={:?})", key);
        let ptr = self.get_ptr(key);
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
            let page = &entry.page;
            page.clear_loaded();
            debug!("cleaning up page {}", page.get().id);
            let _ = page.get().contents.take();
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
        if tail.page.is_dirty() {
            // TODO: drop from another clean entry?
            return;
        }
        self.detach(tail, true);
    }

    pub fn clear(&mut self) {
        let to_remove: Vec<PageCacheKey> = self.map.borrow().iter().map(|v| v.0.clone()).collect();
        for key in to_remove {
            self.delete(key);
        }
    }
}
