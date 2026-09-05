use crate::assert::assert_send_sync;
#[cfg(target_vendor = "apple")]
use crate::io::AtomicFileSyncType;
use crate::io::FileSyncType;
use crate::io::WriteBatch;
use crate::storage::btree::PinGuard;
use crate::storage::subjournal::Subjournal;
use crate::storage::wal::{CheckpointLockSource, PreparedFrames};
use crate::storage::{
    buffer_pool::BufferPool,
    database::DatabaseStorage,
    sqlite3_ondisk::{
        self, parse_wal_frame_header, DatabaseHeader, OverflowCell, PageSize, PageType,
        CELL_PTR_SIZE_BYTES, INTERIOR_PAGE_HEADER_SIZE_BYTES, LEAF_PAGE_HEADER_SIZE_BYTES,
        MINIMUM_CELL_SIZE,
    },
    wal::{CheckpointResult, RollbackTo, Wal, IOV_MAX},
};
use crate::sync::atomic::{
    AtomicBool, AtomicIsize, AtomicU16, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering,
};
use crate::sync::Arc;
use crate::sync::{Mutex, RwLock};
use crate::types::IOResultOr;
use crate::types::{IOCompletions, WalState};
use crate::util::IOExt as _;
use crate::{
    io::CompletionGroup, return_if_io, types::WalFrameInfo, Completion, Connection, IOResult,
    LimboError, Result, TransactionState,
};
use crate::{io_yield_one, Buffer, CompletionError, IOContext, OpenFlags, PageCodec, SyncMode, IO};
#[allow(unused_imports)]
use crate::{
    turso_assert, turso_assert_eq, turso_assert_greater_than, turso_assert_greater_than_or_equal,
    turso_assert_less_than, turso_assert_ne, turso_debug_assert, turso_soft_unreachable,
};
use arc_swap::ArcSwapOption;
use roaring::RoaringBitmap;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use tracing::{instrument, trace, Level};

use super::btree::offset::{
    BTREE_CELL_CONTENT_AREA, BTREE_CELL_COUNT, BTREE_FIRST_FREEBLOCK, BTREE_FRAGMENTED_BYTES_COUNT,
    BTREE_PAGE_TYPE, BTREE_RIGHTMOST_PTR,
};
use super::btree::{
    btree_init_page, payload_overflow_threshold_max, payload_overflow_threshold_min,
};
use super::page_cache::{CacheError, CacheResizeResult, PageCache, PageCacheKey, SpillResult};
use super::sqlite3_ondisk::read_varint;
use super::sqlite3_ondisk::{
    begin_write_btree_page, read_btree_cell, read_u32, BTreeCell, FREELIST_LEAF_PTR_SIZE,
    FREELIST_TRUNK_OFFSET_FIRST_LEAF_PTR, FREELIST_TRUNK_OFFSET_LEAF_COUNT,
    FREELIST_TRUNK_OFFSET_NEXT_TRUNK_PTR,
};
use super::wal::{CheckpointMode, WalAutoActions};
use crate::storage::encryption::{CipherMode, EncryptionContext, EncryptionKey};

/// SQLite's default maximum page count
const DEFAULT_MAX_PAGE_COUNT: u32 = 0xfffffffe;
const RESERVED_SPACE_NOT_SET: u16 = u16::MAX;

#[cfg(feature = "test_helper")]
/// Used for testing purposes to change the position of the PENDING BYTE
static PENDING_BYTE: AtomicU32 = AtomicU32::new(0x40000000);

#[cfg(not(feature = "test_helper"))]
/// Byte offset that signifies the start of the ignored page - 1 GB mark
const PENDING_BYTE: u32 = 0x40000000;

#[cfg(feature = "autovacuum")]
use ptrmap::*;

#[derive(Debug, Clone)]
pub struct HeaderRef(PageRef);

impl HeaderRef {
    pub fn from_pager(pager: &Pager) -> IOResultOr<Self> {
        let page = return_if_io!(pager.read_header_page());
        Ok(IOResult::Done(Self(page)))
    }

    pub fn borrow(&self) -> &DatabaseHeader {
        // TODO: Instead of erasing mutability, implement `get_mut_contents` and return a shared reference.
        let content = self.0.get_contents();
        bytemuck::from_bytes::<DatabaseHeader>(&content.as_ptr()[0..DatabaseHeader::SIZE])
    }
}

#[derive(Debug, Clone)]
pub struct HeaderRefMut(PageRef);

impl HeaderRefMut {
    pub fn from_pager(pager: &Pager) -> IOResultOr<Self> {
        let page = return_if_io!(pager.read_header_page());
        pager.add_dirty(&page)?;
        Ok(IOResult::Done(Self(page)))
    }

    pub fn borrow_mut(&self) -> &mut DatabaseHeader {
        let content = self.0.get_contents();
        bytemuck::from_bytes_mut::<DatabaseHeader>(&mut content.as_ptr()[0..DatabaseHeader::SIZE])
    }

    /// Get a reference to the underlying page
    pub fn page(&self) -> &PageRef {
        &self.0
    }
}

pub struct PageInner {
    pub flags: AtomicUsize,
    pub id: usize,
    /// If >0, the page is pinned and not eligible for eviction from the page cache.
    /// The reason this is a counter is that multiple nested code paths may signal that
    /// a page must not be evicted from the page cache, so even if an inner code path
    /// requests unpinning via [Page::unpin], the pin count will still be >0 if the outer
    /// code path has not yet requested to unpin the page as well.
    ///
    /// Note that [PageCache::clear] evicts the pages even if pinned, so as long as
    /// we clear the page cache on errors, pins will not 'leak'.
    pub pin_count: AtomicUsize,
    /// The WAL frame number this page was loaded from (0 if loaded from main DB file)
    /// This tracks which version of the page we have in memory
    pub wal_tag: AtomicU64,
    /// The actual page data buffer. None if not loaded.
    buffer: Option<Arc<Buffer>>,
    /// Start and length of the bytes of `buffer`, kept next to it so a page
    /// read does not go through the `Option`, the `Arc` and the `Buffer`
    /// variant on every access. Null and 0 while `buffer` is `None`.
    data_ptr: *mut u8,
    data_len: usize,
    /// Overflow cells during btree operations
    pub overflow_cells: crate::alloc::Vec<OverflowCell>,
}

// SAFETY: data_ptr and data_len only cache the address and length of the
// bytes owned by `buffer`, an `Arc<Buffer>` that is itself Send and Sync, so
// PageInner can cross threads exactly as it could before the cache existed.
unsafe impl Send for PageInner {}
unsafe impl Sync for PageInner {}

// Methods moved from PageContent - these provide btree page access
impl PageInner {
    /// Creates a new PageInner from an Arc<Buffer>.
    pub fn new(buffer: Arc<Buffer>) -> Self {
        let mut inner = Self::unloaded(0);
        inner.set_buffer(buffer);
        inner
    }

    /// Creates a new PageInner with an owned buffer.
    pub fn from_buffer(buffer: Buffer) -> Self {
        Self::new(Arc::new(buffer))
    }

    /// Creates a PageInner with no buffer loaded.
    pub fn unloaded(id: usize) -> Self {
        Self {
            flags: AtomicUsize::new(0),
            id,
            pin_count: AtomicUsize::new(0),
            wal_tag: AtomicU64::new(TAG_UNSET),
            buffer: None,
            data_ptr: std::ptr::null_mut(),
            data_len: 0,
            overflow_cells: crate::alloc::vec![],
        }
    }

    /// The page data buffer, if loaded.
    #[inline]
    pub fn buffer(&self) -> Option<&Arc<Buffer>> {
        self.buffer.as_ref()
    }

    /// Installs the page data buffer.
    pub fn set_buffer(&mut self, buffer: Arc<Buffer>) {
        self.data_ptr = buffer.as_mut_ptr();
        self.data_len = buffer.len();
        self.buffer = Some(buffer);
    }

    /// Removes the page data buffer, leaving the page unloaded.
    pub fn take_buffer(&mut self) -> Option<Arc<Buffer>> {
        self.data_ptr = std::ptr::null_mut();
        self.data_len = 0;
        self.buffer.take()
    }

    /// Get the page buffer as a mutable slice. Panics if buffer not loaded.
    #[inline(always)]
    #[allow(clippy::mut_from_ref)]
    pub fn as_ptr(&self) -> &mut [u8] {
        turso_assert!(!self.data_ptr.is_null(), "buffer not loaded");
        // SAFETY: `data_ptr`/`data_len` describe the bytes of the `Arc<Buffer>`
        // held in `self.buffer`, which stays alive and does not move while it is
        // installed. Handing out `&mut [u8]` from `&self` mirrors
        // `Buffer::as_mut_slice`; the page byte range is mutated only under the
        // pager's own exclusion rules, as before.
        unsafe { std::slice::from_raw_parts_mut(self.data_ptr, self.data_len) }
    }

    /// The position where page content starts. It's 100 for page 1 (database file header is 100 bytes),
    /// 0 for all other pages.
    #[inline(always)]
    pub fn offset(&self) -> usize {
        if self.id == 1 {
            DatabaseHeader::SIZE
        } else {
            0
        }
    }

    /// Read a u8 from the page content at the given offset, taking account the possible db header on page 1.
    #[inline(always)]
    fn read_u8(&self, pos: usize) -> u8 {
        let buf = self.as_ptr();
        buf[self.offset() + pos]
    }

    /// Read a u16 from the page content at the given offset, taking account the possible db header on page 1.
    #[inline(always)]
    fn read_u16(&self, pos: usize) -> u16 {
        let pos = self.offset() + pos;
        let bytes = &self.as_ptr()[pos..pos + 2];
        u16::from_be_bytes([bytes[0], bytes[1]])
    }

    /// Read a u32 from the page content at the given offset, taking account the possible db header on page 1.
    #[inline]
    fn read_u32(&self, pos: usize) -> u32 {
        let buf = self.as_ptr();
        read_u32(buf, self.offset() + pos)
    }

    /// Write a u8 to the page content at the given offset, taking account the possible db header on page 1.
    #[inline]
    fn write_u8(&self, pos: usize, value: u8) {
        tracing::trace!("write_u8(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        buf[self.offset() + pos] = value;
    }

    /// Write a u16 to the page content at the given offset, taking account the possible db header on page 1.
    #[inline]
    fn write_u16(&self, pos: usize, value: u16) {
        tracing::trace!("write_u16(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        let offset = self.offset();
        buf[offset + pos..offset + pos + 2].copy_from_slice(&value.to_be_bytes());
    }

    /// Write a u32 to the page content at the given offset, taking account the possible db header on page 1.
    #[inline]
    fn write_u32(&self, pos: usize, value: u32) {
        tracing::trace!("write_u32(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        let offset = self.offset();
        buf[offset + pos..offset + pos + 4].copy_from_slice(&value.to_be_bytes());
    }

    #[inline]
    pub fn page_type(&self) -> crate::Result<PageType> {
        self.read_u8(BTREE_PAGE_TYPE).try_into()
    }

    /// Read a u16 from the page content at the given absolute offset (no db header offset).
    #[inline]
    pub fn read_u16_no_offset(&self, pos: usize) -> u16 {
        let buf = self.as_ptr();
        u16::from_be_bytes([buf[pos], buf[pos + 1]])
    }

    /// Read a u32 from the page content at the given absolute offset (no db header offset).
    #[inline]
    pub fn read_u32_no_offset(&self, pos: usize) -> u32 {
        let buf = self.as_ptr();
        u32::from_be_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]])
    }

    /// Write a u16 at the given absolute offset (no db header offset).
    pub fn write_u16_no_offset(&self, pos: usize, value: u16) {
        tracing::trace!("write_u16_no_offset(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        buf[pos..pos + 2].copy_from_slice(&value.to_be_bytes());
    }

    /// Write a u32 at the given absolute offset (no db header offset).
    pub fn write_u32_no_offset(&self, pos: usize, value: u32) {
        tracing::trace!("write_u32_no_offset(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        buf[pos..pos + 4].copy_from_slice(&value.to_be_bytes());
    }

    pub fn write_page_type(&self, value: u8) {
        self.write_u8(BTREE_PAGE_TYPE, value);
    }

    pub fn write_rightmost_ptr(&self, value: u32) {
        self.write_u32(BTREE_RIGHTMOST_PTR, value);
    }

    pub fn write_first_freeblock(&self, value: u16) {
        self.write_u16(BTREE_FIRST_FREEBLOCK, value);
    }

    pub fn write_freeblock(&self, offset: u16, size: u16, next_block: Option<u16>) {
        self.write_freeblock_next_ptr(offset, next_block.unwrap_or(0));
        self.write_freeblock_size(offset, size);
    }

    pub fn write_freeblock_size(&self, offset: u16, size: u16) {
        self.write_u16_no_offset(offset as usize + 2, size);
    }

    pub fn write_freeblock_next_ptr(&self, offset: u16, next_block: u16) {
        self.write_u16_no_offset(offset as usize, next_block);
    }

    pub fn read_freeblock(&self, offset: u16) -> (u16, u16) {
        (
            self.read_u16_no_offset(offset as usize),
            self.read_u16_no_offset(offset as usize + 2),
        )
    }

    pub fn write_cell_count(&self, value: u16) {
        self.write_u16(BTREE_CELL_COUNT, value);
    }

    pub fn write_cell_content_area(&self, value: usize) {
        turso_debug_assert!(value <= PageSize::MAX as usize);
        let value = value as u16;
        self.write_u16(BTREE_CELL_CONTENT_AREA, value);
    }

    pub fn write_fragmented_bytes_count(&self, value: u8) {
        self.write_u8(BTREE_FRAGMENTED_BYTES_COUNT, value);
    }

    #[inline]
    pub fn first_freeblock(&self) -> u16 {
        self.read_u16(BTREE_FIRST_FREEBLOCK)
    }

    #[inline(always)]
    pub fn cell_count(&self) -> usize {
        self.read_u16(BTREE_CELL_COUNT) as usize
    }

    #[inline]
    pub fn cell_pointer_array_size(&self) -> usize {
        self.cell_count() * CELL_PTR_SIZE_BYTES
    }

    #[inline]
    pub fn unallocated_region_start(&self) -> usize {
        let (cell_ptr_array_start, cell_ptr_array_size) = self.cell_pointer_array_offset_and_size();
        cell_ptr_array_start + cell_ptr_array_size
    }

    #[inline]
    pub fn unallocated_region_size(&self) -> usize {
        self.cell_content_area() as usize - self.unallocated_region_start()
    }

    #[inline]
    pub fn cell_content_area(&self) -> u32 {
        let offset = self.read_u16(BTREE_CELL_CONTENT_AREA);
        if offset == 0 {
            PageSize::MAX
        } else {
            offset as u32
        }
    }

    #[inline]
    pub fn header_size(&self) -> usize {
        let is_interior = self.read_u8(BTREE_PAGE_TYPE) <= PageType::TableInterior as u8;
        (!is_interior as usize) * LEAF_PAGE_HEADER_SIZE_BYTES
            + (is_interior as usize) * INTERIOR_PAGE_HEADER_SIZE_BYTES
    }

    #[inline]
    pub fn num_frag_free_bytes(&self) -> u8 {
        self.read_u8(BTREE_FRAGMENTED_BYTES_COUNT)
    }

    #[inline]
    pub fn rightmost_pointer(&self) -> crate::Result<Option<u32>> {
        match self.page_type()? {
            PageType::IndexInterior | PageType::TableInterior => {
                Ok(Some(self.read_u32(BTREE_RIGHTMOST_PTR)))
            }
            PageType::IndexLeaf | PageType::TableLeaf => Ok(None),
        }
    }

    #[inline]
    pub fn rightmost_pointer_raw(&self) -> crate::Result<Option<*mut u8>> {
        match self.page_type()? {
            PageType::IndexInterior | PageType::TableInterior => Ok(Some(unsafe {
                self.as_ptr()
                    .as_mut_ptr()
                    .add(self.offset() + BTREE_RIGHTMOST_PTR)
            })),
            PageType::IndexLeaf | PageType::TableLeaf => Ok(None),
        }
    }

    #[inline]
    pub fn cell_get(&self, idx: usize, usable_size: usize) -> crate::Result<BTreeCell> {
        tracing::trace!("cell_get(idx={})", idx);
        let buf = self.as_ptr();

        let ncells = self.cell_count();
        turso_assert_less_than!(idx, ncells,
            "cell_get: idx out of bounds",
            {"idx": idx, "ncells": ncells}
        );
        let cell_pointer_array_start = self.header_size();
        let cell_pointer = cell_pointer_array_start + (idx * CELL_PTR_SIZE_BYTES);
        let cell_pointer = self.read_u16(cell_pointer) as usize;

        let static_buf: &'static [u8] = unsafe { std::mem::transmute::<&[u8], &'static [u8]>(buf) };
        read_btree_cell(static_buf, self, cell_pointer, usable_size)
    }

    #[inline(always)]
    pub fn cell_table_interior_read_rowid(&self, idx: usize) -> crate::Result<i64> {
        turso_debug_assert!(matches!(self.page_type(), Ok(PageType::TableInterior)));
        let buf = self.as_ptr();
        let cell_pointer_array_start = self.header_size();
        let cell_pointer = cell_pointer_array_start + (idx * CELL_PTR_SIZE_BYTES);
        let cell_pointer = self.read_u16(cell_pointer) as usize;
        const LEFT_CHILD_PAGE_SIZE_BYTES: usize = 4;
        let (rowid, _) = read_varint(crate::slice_in_bounds_or_corrupt!(
            buf,
            cell_pointer + LEFT_CHILD_PAGE_SIZE_BYTES..
        ))?;
        Ok(rowid as i64)
    }

    #[inline(always)]
    pub fn cell_interior_read_left_child_page(&self, idx: usize) -> crate::Result<u32> {
        turso_debug_assert!(matches!(
            self.page_type(),
            Ok(PageType::TableInterior) | Ok(PageType::IndexInterior)
        ));
        let buf = self.as_ptr();
        let cell_pointer_array_start = self.header_size();
        let cell_pointer = cell_pointer_array_start + (idx * CELL_PTR_SIZE_BYTES);
        let cell_pointer = self.read_u16(cell_pointer) as usize;
        crate::assert_or_bail_corrupt!(
            cell_pointer + 4 <= buf.len(),
            "cell pointer {} out of bounds for page size {}",
            cell_pointer,
            buf.len()
        );
        Ok(u32::from_be_bytes([
            buf[cell_pointer],
            buf[cell_pointer + 1],
            buf[cell_pointer + 2],
            buf[cell_pointer + 3],
        ]))
    }

    #[inline(always)]
    pub fn cell_table_leaf_read_rowid(&self, idx: usize) -> crate::Result<i64> {
        turso_debug_assert!(matches!(self.page_type(), Ok(PageType::TableLeaf)));
        let buf = self.as_ptr();
        let cell_pointer_array_start = self.header_size();
        let cell_pointer = cell_pointer_array_start + (idx * CELL_PTR_SIZE_BYTES);
        // Bound-check the array entry: `idx` is the untrusted on-disk cell count.
        crate::assert_or_bail_corrupt!(
            self.offset() + cell_pointer + CELL_PTR_SIZE_BYTES <= buf.len(),
            "cell pointer array index {} out of bounds for page size {}",
            idx,
            buf.len()
        );
        let cell_pointer = self.read_u16(cell_pointer) as usize;
        let mut pos = cell_pointer;
        let (_, nr) = read_varint(crate::slice_in_bounds_or_corrupt!(buf, pos..))?;
        pos += nr;
        let (rowid, _) = read_varint(crate::slice_in_bounds_or_corrupt!(buf, pos..))?;
        Ok(rowid as i64)
    }

    /// Returns a cell's record payload and overflow info without constructing
    /// a `BTreeCell`.
    ///
    /// This bypasses the full `cell_get()` to `read_btree_cell()` path for
    /// record reads and index binary-search hot loops.
    /// The returned slice is valid as long as the page is alive.
    ///
    /// Returns: (payload_slice, payload_size, first_overflow_page)
    #[inline(always)]
    pub fn cell_read_payload_ptr(
        &self,
        idx: usize,
        usable_size: usize,
    ) -> crate::Result<(&'static [u8], u64, Option<u32>)> {
        let buf = self.as_ptr();
        let cell_pointer_array_start = self.header_size();
        let cell_pointer = cell_pointer_array_start + (idx * CELL_PTR_SIZE_BYTES);
        let cell_offset = self.read_u16(cell_pointer) as usize;

        let page_type = self.page_type()?;
        let (payload_size, payload_start) = match page_type {
            PageType::IndexInterior => {
                let (size, len) =
                    read_varint(crate::slice_in_bounds_or_corrupt!(buf, cell_offset + 4..))?;
                (size, cell_offset + 4 + len)
            }
            PageType::IndexLeaf => {
                let (size, len) =
                    read_varint(crate::slice_in_bounds_or_corrupt!(buf, cell_offset..))?;
                (size, cell_offset + len)
            }
            PageType::TableLeaf => {
                let (size, payload_size_len) =
                    read_varint(crate::slice_in_bounds_or_corrupt!(buf, cell_offset..))?;
                let rowid_start = cell_offset + payload_size_len;
                let (_, rowid_len) =
                    read_varint(crate::slice_in_bounds_or_corrupt!(buf, rowid_start..))?;
                (size, rowid_start + rowid_len)
            }
            PageType::TableInterior => {
                unreachable!("table interior cells do not contain record payloads")
            }
        };

        let max_local = payload_overflow_threshold_max(page_type, usable_size);
        let min_local = payload_overflow_threshold_min(page_type, usable_size);
        let (overflows, local_size) = sqlite3_ondisk::payload_overflows(
            payload_size as usize,
            max_local,
            min_local,
            usable_size,
        );

        let (payload_slice, first_overflow) = if overflows {
            let overflow_ptr_offset = payload_start + local_size - 4;
            crate::assert_or_bail_corrupt!(
                overflow_ptr_offset + 4 <= buf.len(),
                "overflow pointer offset {} out of bounds for page size {}",
                overflow_ptr_offset,
                buf.len()
            );
            let first_overflow_page = u32::from_be_bytes([
                buf[overflow_ptr_offset],
                buf[overflow_ptr_offset + 1],
                buf[overflow_ptr_offset + 2],
                buf[overflow_ptr_offset + 3],
            ]);
            let payload_end = payload_start + local_size - 4;
            crate::assert_or_bail_corrupt!(
                payload_start < payload_end && payload_end <= buf.len(),
                "payload range {}..{} out of bounds for page size {}",
                payload_start,
                payload_end,
                buf.len()
            );
            // SAFETY: valid as long as page is alive
            let slice = unsafe {
                std::mem::transmute::<&[u8], &'static [u8]>(&buf[payload_start..payload_end])
            };
            (slice, Some(first_overflow_page))
        } else {
            let payload_end = payload_start + payload_size as usize;
            crate::assert_or_bail_corrupt!(
                payload_end <= buf.len(),
                "payload range {}..{} out of bounds for page size {}",
                payload_start,
                payload_end,
                buf.len()
            );
            // SAFETY: valid as long as page is alive
            let slice = unsafe {
                std::mem::transmute::<&[u8], &'static [u8]>(&buf[payload_start..payload_end])
            };
            (slice, None)
        };

        Ok((payload_slice, payload_size, first_overflow))
    }

    #[inline]
    pub fn cell_pointer_array_offset_and_size(&self) -> (usize, usize) {
        (
            self.cell_pointer_array_offset(),
            self.cell_pointer_array_size(),
        )
    }

    #[inline]
    pub fn cell_pointer_array_offset(&self) -> usize {
        self.offset() + self.header_size()
    }

    #[inline]
    pub fn cell_get_raw_start_offset(&self, idx: usize) -> usize {
        let cell_pointer_array_start = self.cell_pointer_array_offset();
        let cell_pointer = cell_pointer_array_start + (idx * CELL_PTR_SIZE_BYTES);
        self.read_u16_no_offset(cell_pointer) as usize
    }

    #[inline]
    pub fn cell_get_raw_region(
        &self,
        idx: usize,
        usable_size: usize,
    ) -> crate::Result<(usize, usize)> {
        let page_type = self.page_type()?;
        let max_local = payload_overflow_threshold_max(page_type, usable_size);
        let min_local = payload_overflow_threshold_min(page_type, usable_size);
        let cell_count = self.cell_count();
        self._cell_get_raw_region_faster(
            idx,
            usable_size,
            cell_count,
            max_local,
            min_local,
            page_type,
        )
    }

    #[inline]
    pub fn _cell_get_raw_region_faster(
        &self,
        idx: usize,
        usable_size: usize,
        cell_count: usize,
        max_local: usize,
        min_local: usize,
        page_type: PageType,
    ) -> crate::Result<(usize, usize)> {
        let buf = self.as_ptr();
        turso_assert_less_than!(idx, cell_count);
        let start = self.cell_get_raw_start_offset(idx);
        let len = match page_type {
            PageType::IndexInterior => {
                let (len_payload, n_payload) =
                    read_varint(crate::slice_in_bounds_or_corrupt!(buf, start + 4..))?;
                let (overflows, to_read) = sqlite3_ondisk::payload_overflows(
                    len_payload as usize,
                    max_local,
                    min_local,
                    usable_size,
                );
                if overflows {
                    4 + to_read + n_payload
                } else {
                    4 + len_payload as usize + n_payload
                }
            }
            PageType::TableInterior => {
                let (_, n_rowid) =
                    read_varint(crate::slice_in_bounds_or_corrupt!(buf, start + 4..))?;
                4 + n_rowid
            }
            PageType::IndexLeaf => {
                let (len_payload, n_payload) =
                    read_varint(crate::slice_in_bounds_or_corrupt!(buf, start..))?;
                let (overflows, to_read) = sqlite3_ondisk::payload_overflows(
                    len_payload as usize,
                    max_local,
                    min_local,
                    usable_size,
                );
                if overflows {
                    to_read + n_payload
                } else {
                    let mut size = len_payload as usize + n_payload;
                    if size < MINIMUM_CELL_SIZE {
                        size = MINIMUM_CELL_SIZE;
                    }
                    size
                }
            }
            PageType::TableLeaf => {
                let (len_payload, n_payload) =
                    read_varint(crate::slice_in_bounds_or_corrupt!(buf, start..))?;
                let (_, n_rowid) =
                    read_varint(crate::slice_in_bounds_or_corrupt!(buf, start + n_payload..))?;
                let (overflows, to_read) = sqlite3_ondisk::payload_overflows(
                    len_payload as usize,
                    max_local,
                    min_local,
                    usable_size,
                );
                if overflows {
                    to_read + n_payload + n_rowid
                } else {
                    let mut size = len_payload as usize + n_payload + n_rowid;
                    if size < MINIMUM_CELL_SIZE {
                        size = MINIMUM_CELL_SIZE;
                    }
                    size
                }
            }
        };
        crate::assert_or_bail_corrupt!(
            start + len <= buf.len(),
            "cell region {}..{} out of bounds for page size {}",
            start,
            start + len,
            buf.len()
        );
        Ok((start, len))
    }

    #[inline(always)]
    pub fn is_leaf(&self) -> bool {
        self.read_u8(BTREE_PAGE_TYPE) > PageType::TableInterior as u8
    }

    /// True for table pages (interior or leaf). A corrupt page type byte
    /// answers false; the record reader reports it when it parses the cell.
    #[inline(always)]
    pub fn is_table(&self) -> bool {
        let page_type = self.read_u8(BTREE_PAGE_TYPE);
        page_type == PageType::TableLeaf as u8 || page_type == PageType::TableInterior as u8
    }

    pub fn write_database_header(&self, header: &DatabaseHeader) {
        let buf = self.as_ptr();
        buf[0..DatabaseHeader::SIZE].copy_from_slice(bytemuck::bytes_of(header));
    }

    pub fn debug_print_freelist(&self, usable_space: usize) {
        let mut pc = self.first_freeblock() as usize;
        let mut block_num = 0;
        println!("---- Free List Blocks ----");
        println!("first freeblock pointer: {pc}");
        println!("cell content area: {}", self.cell_content_area());
        println!("fragmented bytes: {}", self.num_frag_free_bytes());

        while pc != 0 && pc <= usable_space {
            let next = self.read_u16_no_offset(pc);
            let size = self.read_u16_no_offset(pc + 2);

            println!("block {block_num}: position={pc}, size={size}, next={next}");
            pc = next as usize;
            block_num += 1;
        }
        println!("--------------");
    }
}

/// Type alias for backward compatibility - PageContent is now PageInner
pub type PageContent = PageInner;

/// WAL tag not set
pub const TAG_UNSET: u64 = u64::MAX;
/// WAL write in progress, sentinel value set before starting a WAL write
/// so we can detect if page was modified during the write
pub const TAG_WRITE_PENDING: u64 = u64::MAX - 1;

/// Bit layout:
/// epoch: 20
/// frame: 44
const EPOCH_BITS: u32 = 20;
const FRAME_BITS: u32 = 64 - EPOCH_BITS;
const EPOCH_SHIFT: u32 = FRAME_BITS;
const EPOCH_MAX: u32 = (1u32 << EPOCH_BITS) - 1;
const FRAME_MAX: u64 = (1u64 << FRAME_BITS) - 1;

#[inline]
pub fn pack_tag_pair(frame: u64, seq: u32) -> u64 {
    ((seq as u64) << EPOCH_SHIFT) | (frame & FRAME_MAX)
}

#[inline]
pub fn unpack_tag_pair(tag: u64) -> (u64, u32) {
    let epoch = ((tag >> EPOCH_SHIFT) & (EPOCH_MAX as u64)) as u32;
    let frame = tag & FRAME_MAX;
    (frame, epoch)
}

#[derive(Debug)]
pub struct Page {
    pub inner: UnsafeCell<PageInner>,
}

// SAFETY: Page is thread-safe because we use atomic page flags to serialize
// concurrent modifications.
unsafe impl Send for Page {}
unsafe impl Sync for Page {}
crate::assert::assert_send_sync!(Page);

// Concurrency control of pages will be handled by the pager, we won't wrap Page with RwLock
// because that is bad bad.
pub type PageRef = Arc<Page>;

/// Page is locked for I/O to prevent concurrent access.
const PAGE_LOCKED: usize = 0b010;
/// Page is dirty. Flush needed.
const PAGE_DIRTY: usize = 0b1000;
/// Page's contents are loaded in memory.
const PAGE_LOADED: usize = 0b10000;
/// Page has been spilled to WAL (can be evicted even though dirty).
const PAGE_SPILLED: usize = 0b100000;

impl Page {
    pub fn new(id: i64) -> Self {
        turso_assert_greater_than_or_equal!(id, 0);
        Self {
            inner: UnsafeCell::new(PageInner::unloaded(id as usize)),
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get(&self) -> &mut PageInner {
        unsafe { &mut *self.inner.get() }
    }

    /// Returns a mutable reference to PageInner for accessing page contents.
    /// Panics if the page buffer is not loaded.
    pub fn get_contents(&self) -> &mut PageInner {
        let inner = self.get();
        turso_debug_assert!(
            inner.buffer().is_some(),
            "page buffer not loaded",
            { "page_id": inner.id }
        );
        inner
    }

    #[inline]
    pub fn is_locked(&self) -> bool {
        self.get().flags.load(Ordering::Acquire) & PAGE_LOCKED != 0
    }

    #[inline]
    pub fn set_locked(&self) {
        self.get().flags.fetch_or(PAGE_LOCKED, Ordering::Acquire);
    }

    #[inline]
    pub fn clear_locked(&self) {
        self.get().flags.fetch_and(!PAGE_LOCKED, Ordering::Release);
    }

    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.get().flags.load(Ordering::Acquire) & PAGE_DIRTY != 0
    }

    #[inline]
    /// almost never should be called explicitly - instead [Pager::add_dirty] method must be used
    pub fn set_dirty(&self) {
        tracing::debug!("set_dirty(page={})", self.get().id);
        self.clear_wal_tag();
        // Clear spilled flag since page is being modified again
        self.get().flags.fetch_and(!PAGE_SPILLED, Ordering::Release);
        self.get().flags.fetch_or(PAGE_DIRTY, Ordering::Release);
    }

    #[inline]
    /// caller must ensure that [Pager::dirty_pages] will be updated accordingly
    pub fn clear_dirty(&self) {
        tracing::debug!("clear_dirty(page={})", self.get().id);
        self.get().flags.fetch_and(!PAGE_DIRTY, Ordering::Release);
        self.clear_wal_tag();
    }

    /// Clear the dirty flag without touching wal_tag.
    /// Used when a WAL frame has been durably written and the tag already encodes it.
    #[inline]
    pub fn clear_dirty_keep_wal_tag(&self) {
        tracing::debug!("clear_dirty_keep_wal_tag(page={})", self.get().id);
        self.get().flags.fetch_and(!PAGE_DIRTY, Ordering::Release);
    }

    /// Returns true if the page has been spilled to WAL and is safe to evict even while dirty.
    #[inline]
    pub fn is_spilled(&self) -> bool {
        self.get().flags.load(Ordering::Acquire) & PAGE_SPILLED != 0
    }

    /// Mark the page as spilled to WAL. Spilled pages remain dirty but may be evicted from cache.
    #[inline]
    pub fn set_spilled(&self) {
        tracing::debug!("set_spilled(page={})", self.get().id);
        self.get().flags.fetch_or(PAGE_SPILLED, Ordering::Release);
    }

    /// Clear the spilled flag. This is also done implicitly on set_dirty().
    #[inline]
    pub fn clear_spilled(&self) {
        self.get().flags.fetch_and(!PAGE_SPILLED, Ordering::Release);
    }

    #[inline]
    pub fn is_loaded(&self) -> bool {
        self.get().flags.load(Ordering::Acquire) & PAGE_LOADED != 0
    }

    #[inline]
    pub fn set_loaded(&self) {
        self.get().flags.fetch_or(PAGE_LOADED, Ordering::Release);
    }

    #[inline]
    pub fn clear_loaded(&self) {
        tracing::debug!("clear loaded {}", self.get().id);
        self.get().flags.fetch_and(!PAGE_LOADED, Ordering::Release);
    }

    #[inline]
    pub fn is_index(&self) -> crate::Result<bool> {
        Ok(match self.get_contents().page_type()? {
            PageType::IndexLeaf | PageType::IndexInterior => true,
            PageType::TableLeaf | PageType::TableInterior => false,
        })
    }

    /// Increment the pin count by 1. A pin count >0 means the page is pinned and not eligible for eviction from the page cache.
    #[inline]
    pub fn pin(&self) {
        self.get().pin_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Decrement the pin count by 1. If the count reaches 0, the page is no longer
    /// pinned and is eligible for eviction from the page cache.
    #[inline]
    pub fn unpin(&self) {
        let was_pinned = self.try_unpin();

        turso_assert!(
            was_pinned,
            "Attempted to unpin page that was not pinned",
            { "page_id": self.get().id }
        );
    }

    /// Try to decrement the pin count by 1, but do nothing if it was already 0.
    /// Returns true if the pin count was decremented.
    #[inline]
    pub fn try_unpin(&self) -> bool {
        self.get()
            .pin_count
            .fetch_update(Ordering::Release, Ordering::SeqCst, |current| {
                if current == 0 {
                    None
                } else {
                    Some(current - 1)
                }
            })
            .is_ok()
    }

    /// Returns true if the page is pinned and thus not eligible for eviction from the page cache.
    #[inline]
    pub fn is_pinned(&self) -> bool {
        self.get().pin_count.load(Ordering::Acquire) > 0
    }

    #[inline]
    /// Set the WAL tag from a (frame, epoch) pair.
    /// If inputs are invalid, stores TAG_UNSET, which will prevent
    /// the cached page from being used during checkpoint.
    pub fn set_wal_tag(&self, frame: u64, epoch: u32) {
        // use only first 20 bits for seq (max: 1048576)
        let e = epoch & EPOCH_MAX;
        self.get()
            .wal_tag
            .store(pack_tag_pair(frame, e), Ordering::Release);
    }

    #[inline]
    /// Load the (frame, seq) pair from the packed tag.
    pub fn wal_tag_pair(&self) -> (u64, u32) {
        unpack_tag_pair(self.get().wal_tag.load(Ordering::Acquire))
    }

    #[inline]
    pub fn clear_wal_tag(&self) {
        self.get().wal_tag.store(TAG_UNSET, Ordering::Release)
    }

    #[inline]
    /// Returns true if the page has a valid WAL tag (i.e., was written to WAL and not modified since).
    /// Returns false if the wal_tag is TAG_UNSET (page was modified since last WAL write).
    pub fn has_wal_tag(&self) -> bool {
        let tag = self.get().wal_tag.load(Ordering::Acquire);
        let result = tag != TAG_UNSET && tag != TAG_WRITE_PENDING;
        tracing::debug!(
            "has_wal_tag(page={}) = {} (tag={:x})",
            self.get().id,
            result,
            tag
        );
        result
    }

    #[inline]
    /// Mark page as having a WAL write in progress.
    /// This is set before starting a spill/cacheflush so we can detect
    /// if the page was modified during the write.
    pub fn set_write_pending(&self) {
        tracing::debug!("set_write_pending(page={})", self.get().id);
        self.get()
            .wal_tag
            .store(TAG_WRITE_PENDING, Ordering::Release);
    }

    #[inline]
    /// Try to set the WAL tag, but only if the page wasn't modified during the write.
    /// Returns true if the tag was set, false if the page was modified (wal_tag became TAG_UNSET).
    pub fn try_set_wal_tag(&self, frame: u64, epoch: u32) -> bool {
        let new_tag = pack_tag_pair(frame, epoch);
        let page_id = self.get().id;
        let current = self.get().wal_tag.load(Ordering::Acquire);
        // Only set if current tag is not TAG_UNSET (meaning page wasn't modified during write)
        // TAG_WRITE_PENDING is fine, it means the write was in progress and page wasn't modified
        if current == TAG_UNSET {
            tracing::debug!(
                "try_set_wal_tag(page={}, frame={}) SKIPPED: wal_tag is TAG_UNSET (page was modified)",
                page_id, frame
            );
            return false;
        }
        tracing::debug!(
            "try_set_wal_tag(page={}, frame={}) SUCCESS: current={:x}",
            page_id,
            frame,
            current
        );
        self.get().wal_tag.store(new_tag, Ordering::Release);
        true
    }

    #[inline]
    pub fn is_valid_for_checkpoint(&self, target_frame: u64, epoch: u32) -> bool {
        let (f, s) = self.wal_tag_pair();
        f == target_frame && s == epoch && !self.is_dirty() && self.is_loaded() && !self.is_locked()
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
/// The state of the current pager cache commit.
enum CommitState {
    /// Prepare WAL header for commit if needed
    PrepareWal,
    /// Sync WAL header after prepare
    PrepareWalSync,
    /// Get DB size (mostly from page cache - but in rare cases we can read it from disk)
    GetDbSize,
    /// Scan all dirty pages and issue concurrent reads for evicted (spilled) pages.
    ScanAndIssueReads { db_size: u32 },
    /// Wait for all batched reads of evicted pages to complete.
    WaitBatchedReads { db_size: u32 },
    /// Collect pages (now all available) and prepare WAL frames.
    PrepareFrames { db_size: u32 },
    /// All frames prepared, writes are in flight
    WaitWrites,
    /// Wait for the WAL fsync that makes the commit durable. Every commit
    /// converges here once its writes (if any) have completed. The fsync is
    /// submitted here, and skipped when the WAL is not dirty (no frames
    /// appended since the last successful fsync) or sync_mode is not FULL.
    /// Commits that prepared frames continue to WalCommitDone to publish
    /// them; otherwise the commit finishes here, since frames written through
    /// `write_frame_raw` published themselves when they were appended.
    WaitSync,
    /// Finalize the WAL commit by publishing the prepared frames.
    /// After this state, the write transaction is durable.
    /// If autocheckpoint is enabled and the autocheckpoint threshold is reached, checkpoint will be attempted.
    WalCommitDone,
    /// Checkpoint the WAL to the database file (if needed).
    /// This is decoupled from commit - checkpoint failure does not affect commit durability.
    AutoCheckpoint,
}

#[derive(Debug, Default)]
struct CheckpointState {
    phase: CheckpointPhase,
    /// The checkpoint result, set after WAL checkpoint completes
    result: Option<CheckpointResult>,
    /// The checkpoint mode, used to determine if WAL truncation is needed
    mode: Option<CheckpointMode>,
    /// The checkpoint state machine should acquire the lock or use the one by caller
    lock_source: CheckpointLockSource,
}

#[derive(Clone, Debug)]
struct PendingCheckpointDbIdentityRead {
    max_frame: u64,
    header_buf: Arc<Buffer>,
    bytes_read: Arc<AtomicUsize>,
    read_page: bool,
    completion: Option<Completion>,
}

#[derive(Clone, Debug, Default)]
enum CheckpointPhase {
    #[default]
    NotCheckpointing,
    Checkpoint {
        mode: CheckpointMode,
        sync_mode: crate::SyncMode,
        clear_page_cache: bool,
    },
    /// Truncate the database file if everything was backfilled and file is larger than expected.
    TruncateDbFile {
        sync_mode: crate::SyncMode,
        clear_page_cache: bool,
        /// Whether we've invalidated page 1 from cache (needed because checkpoint may write
        /// pages directly from WALto DB file, so cached page 1 of the checkpointer connection may have stale database_size)
        page1_invalidated: bool,
    },
    /// Sync the database file after checkpoint (if sync_mode != Off and we backfilled any frames from the WAL).
    SyncDbFile { clear_page_cache: bool },
    /// Read the synced database header before installing the durable backfill proof.
    ReadDbIdentity {
        clear_page_cache: bool,
        read: PendingCheckpointDbIdentityRead,
    },
    /// Wait for backend-specific durable proof sync to finish before publishing nbackfills.
    SyncBackfillProof {
        clear_page_cache: bool,
        max_frame: u64,
    },
    /// Publish the durable backfill progress after the proof is installed and synced.
    PublishBackfill {
        clear_page_cache: bool,
        max_frame: u64,
    },
    /// Truncate the WAL file after DB file is safely synced (only for TRUNCATE checkpoint mode).
    /// This must happen AFTER SyncDbFile to ensure data durability.
    TruncateWalFile { clear_page_cache: bool },
    /// Finalize: release guard and optionally clear page cache.
    Finalize { clear_page_cache: bool },
}

/// The mode of allocating a btree page.
/// SQLite defines the following:
/// #define BTALLOC_ANY   0           /* Allocate any page */
/// #define BTALLOC_EXACT 1           /* Allocate exact page if possible */
/// #define BTALLOC_LE    2           /* Allocate any page <= the parameter */
pub enum BtreePageAllocMode {
    /// Allocate any btree page
    Any,
    /// Allocate a specific page number, typically used for root page allocation
    Exact(u32),
    /// Allocate a page number less than or equal to the parameter
    Le(u32),
}

/// This will keep track of the state of current cache commit in order to not repeat work
struct CommitInfo {
    /// Group the reads or writes of the current step are added to. Taken
    /// and built by `commit_completion` when the step waits on it.
    group: Option<CompletionGroup>,
    /// The built `group`, cached so re-entries wait on the same completion.
    completion_group: Option<Completion>,
    /// The fsync in flight, if `WaitSync` has submitted one.
    pending_sync: Option<Completion>,
    state: CommitState,
    collected_pages: Vec<PageRef>,
    page_sources: Vec<PageSource>,
    page_source_cursor: usize,
    prepared_frames: Vec<PreparedFrames>,
}

/// Represents a dirty page that will be committed to the log.
enum PageSource {
    /// Cache resident page
    Cached(usize),
    /// A page read from disk because it was spilled/evicted from cache
    Evicted(PageRef),
}

impl CommitInfo {
    fn reset(&mut self) {
        self.group = None;
        self.completion_group = None;
        self.pending_sync = None;
        self.state = CommitState::PrepareWal;
        self.collected_pages.clear();
        self.page_sources.clear();
        self.prepared_frames.clear();
        self.page_source_cursor = 0;
    }

    /// Clear and reserve space for n pages in each vector.
    fn initialize(&mut self, n: usize) {
        self.page_sources.clear();
        self.page_sources.reserve(n.min(IOV_MAX));
        self.group = Some(CompletionGroup::new(|_| {}));
        self.completion_group = None;
        self.collected_pages.reserve(n.min(IOV_MAX));
    }
}

/// Track the state of the auto-vacuum mode.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AutoVacuumMode {
    None,
    Full,
    Incremental,
}

impl From<AutoVacuumMode> for u8 {
    fn from(mode: AutoVacuumMode) -> u8 {
        match mode {
            AutoVacuumMode::None => 0,
            AutoVacuumMode::Full => 1,
            AutoVacuumMode::Incremental => 2,
        }
    }
}

impl From<u8> for AutoVacuumMode {
    fn from(value: u8) -> AutoVacuumMode {
        match value {
            0 => AutoVacuumMode::None,
            1 => AutoVacuumMode::Full,
            2 => AutoVacuumMode::Incremental,
            _ => unreachable!("Invalid AutoVacuumMode value: {}", value),
        }
    }
}

const fn auto_vacuum_header_fields(mode: AutoVacuumMode) -> (u32, u32) {
    match mode {
        AutoVacuumMode::None => (0, 0),
        AutoVacuumMode::Full => (1, 0),
        AutoVacuumMode::Incremental => (1, 1),
    }
}

#[derive(Debug, Clone)]
#[cfg(feature = "autovacuum")]
enum PtrMapGetState {
    Start,
    Deserialize {
        ptrmap_page: PageRef,
        offset_in_ptrmap_page: usize,
    },
}

#[derive(Debug, Clone)]
#[cfg(feature = "autovacuum")]
enum PtrMapPutState {
    Start,
    Deserialize {
        ptrmap_page: PageRef,
        offset_in_ptrmap_page: usize,
    },
}

#[derive(Debug, Clone)]
enum HeaderRefState {
    Start,
    CreateHeader {
        page: PageRef,
        completion: Option<Completion>,
    },
}

#[cfg(feature = "autovacuum")]
#[derive(Debug, Clone, Copy)]
enum BtreeCreateVacuumFullState {
    Start,
    AllocatePage { root_page_num: u32 },
    PtrMapPut { allocated_page_id: u32 },
}

#[derive(Debug, Clone)]
enum SavepointKind {
    Statement,
    Named {
        name: String,
        starts_transaction: bool,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum SavepointResult {
    /// Releasing the named savepoint should commit the surrounding transaction.
    Commit,
    /// The named savepoint was released without committing the transaction.
    Release,
    /// No matching named savepoint exists.
    NotFound,
}

/// A connection's WAL position (max frame, running frame checksum, and the
/// WAL generation they belong to), captured as one unit for savepoint
/// rollback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SavepointWalPos {
    max_frame: u64,
    checksum: (u32, u32),
    checkpoint_seq: u32,
}

#[derive(Debug, Clone)]
struct SavepointSnapshot {
    kind: SavepointKind,
    start_offset: u64,
    db_size: u32,
    wal_pos: Option<SavepointWalPos>,
    deferred_fk_violations: isize,
}

struct Savepoint {
    kind: SavepointKind,
    /// Start offset of this savepoint in the subjournal.
    start_offset: AtomicU64,
    /// Current write offset in the subjournal.
    write_offset: AtomicU64,
    /// Bitmap of page numbers that are dirty in the savepoint.
    page_bitmap: RwLock<RoaringBitmap>,
    /// Database size at the start of the savepoint.
    /// If the database grows during the savepoint and a rollback to the savepoint is performed,
    /// the pages exceeding the database size at the start of the savepoint will be ignored.
    db_size: AtomicU32,
    /// WAL position to rewind to on `ROLLBACK TO`. Captured only under the
    /// write lock: eagerly if the savepoint is opened inside a write
    /// transaction, otherwise at write upgrade. `None` while the
    /// transaction has never held the write lock (no frames to rewind), or
    /// when the pager has no WAL.
    wal_pos: RwLock<Option<SavepointWalPos>>,
    /// Deferred FK counter value at the start of this savepoint.
    deferred_fk_violations: AtomicIsize,
}

impl Savepoint {
    fn new(
        kind: SavepointKind,
        subjournal_offset: u64,
        db_size: u32,
        wal_pos: Option<SavepointWalPos>,
        deferred_fk_violations: isize,
    ) -> Self {
        Self {
            kind,
            start_offset: AtomicU64::new(subjournal_offset),
            write_offset: AtomicU64::new(subjournal_offset),
            page_bitmap: RwLock::new(RoaringBitmap::new()),
            db_size: AtomicU32::new(db_size),
            wal_pos: RwLock::new(wal_pos),
            deferred_fk_violations: AtomicIsize::new(deferred_fk_violations),
        }
    }

    pub fn add_dirty_page(&self, page_num: u32) {
        self.page_bitmap.write().insert(page_num);
    }

    pub fn has_dirty_page(&self, page_num: u32) -> bool {
        self.page_bitmap.read().contains(page_num)
    }

    fn start_offset(&self) -> u64 {
        self.start_offset.load(Ordering::Acquire)
    }

    fn write_offset(&self) -> u64 {
        self.write_offset.load(Ordering::Acquire)
    }

    fn set_write_offset(&self, offset: u64) {
        self.write_offset.store(offset, Ordering::Release);
    }

    fn snapshot(&self) -> SavepointSnapshot {
        SavepointSnapshot {
            kind: self.kind.clone(),
            start_offset: self.start_offset(),
            db_size: self.db_size.load(Ordering::Acquire),
            wal_pos: *self.wal_pos.read(),
            deferred_fk_violations: self.deferred_fk_violations.load(Ordering::Acquire),
        }
    }

    fn from_snapshot(snapshot: SavepointSnapshot) -> Self {
        Self {
            kind: snapshot.kind,
            start_offset: AtomicU64::new(snapshot.start_offset),
            write_offset: AtomicU64::new(snapshot.start_offset),
            page_bitmap: RwLock::new(RoaringBitmap::new()),
            db_size: AtomicU32::new(snapshot.db_size),
            wal_pos: RwLock::new(snapshot.wal_pos),
            deferred_fk_violations: AtomicIsize::new(snapshot.deferred_fk_violations),
        }
    }
}

/// The pager interface implements the persistence layer by providing access
/// to pages of the database file, including caching, concurrency control, and
/// transaction management.
pub struct Pager {
    /// Source of the database pages.
    pub db_file: Arc<dyn DatabaseStorage>,
    /// The write-ahead log (WAL) for the database.
    /// in-memory databases, ephemeral tables and ephemeral indexes do not have a WAL.
    pub(crate) wal: Option<Arc<dyn Wal>>,
    /// A page cache for the database.
    page_cache: Arc<RwLock<PageCache>>,
    /// Buffer pool for temporary data storage.
    pub buffer_pool: Arc<BufferPool>,
    /// I/O interface for input/output operations.
    pub io: Arc<dyn crate::io::IO>,
    /// Reads that have begun (disk IO issued, page allocated) but whose
    /// `cache_insert` has not yet succeeded because the cache was full and we
    /// yielded waiting for a spill to complete. The next call to
    /// `read_page_nonblock(idx)` reuses the stored `(page, disk_read)` pair
    /// instead of issuing a duplicate disk read.
    pending_reads: RwLock<HashMap<i64, PendingRead>>,
    #[cfg(test)]
    spill_yield: SpillYieldHook,
    /// Dirty pages as a bitmap, naturally sorted by page number.
    dirty_pages: Arc<RwLock<RoaringBitmap>>,
    subjournal: RwLock<Option<Subjournal>>,
    savepoints: Arc<RwLock<Vec<Savepoint>>>,
    commit_info: RwLock<CommitInfo>,
    checkpoint_state: RwLock<CheckpointState>,
    syncing: Arc<AtomicBool>,
    auto_vacuum_mode: AtomicU8,
    /// Mutex for synchronizing database initialization to prevent race conditions
    init_lock: Arc<Mutex<()>>,
    /// The state of the current allocate page operation.
    allocate_page_state: RwLock<AllocatePageState>,
    /// The state of the current allocate page1 operation.
    allocate_page1_state: RwLock<AllocatePage1State>,
    /// Cache page_size and reserved_space at Pager init and reuse for subsequent
    /// `usable_space` calls. TODO: Invalidate reserved_space when we add the functionality
    /// to change it.
    pub(crate) page_size: AtomicU32,
    reserved_space: AtomicU16,
    /// Schema cookie cache.
    ///
    /// Note that schema cookie is 32-bits, but we use 64-bit field so we can
    /// represent case where value is not set.
    schema_cookie: AtomicU64,
    free_page_state: RwLock<FreePageState>,
    /// State machine for async cache spilling.
    spill_state: RwLock<SpillState>,
    /// State machine for async cacheflush operation.
    cacheflush_state: RwLock<CacheFlushState>,
    /// Maximum number of pages allowed in the database. Default is 1073741823 (SQLite default).
    max_page_count: AtomicU32,
    header_ref_state: RwLock<HeaderRefState>,
    #[cfg(feature = "autovacuum")]
    vacuum_state: RwLock<VacuumState>,
    pub(crate) io_ctx: RwLock<IOContext>,
    /// encryption is an opt-in feature. we will enable it only if the flag is passed
    enable_encryption: AtomicBool,
    /// In Memory Page 1 for Empty Dbs
    init_page_1: Arc<ArcSwapOption<Page>>,
    /// Sync type for durability. FullFsync uses F_FULLFSYNC on macOS (PRAGMA fullfsync).
    /// Only stored on Apple platforms; on others, always returns Fsync.
    #[cfg(target_vendor = "apple")]
    sync_type: AtomicFileSyncType,
    /// Live BTreeCursors on this pager, bucketed by btree root page.
    /// Counterpart of SQLite's BtShared.pCursor list; bucketing per root
    /// supplies the BTCF_Multiple fast path (btree.c:9348).
    pub(crate) cursor_registry: Mutex<rustc_hash::FxHashMap<i64, Vec<RegisteredCursor>>>,
}

/// Raw fat pointer to a registered cursor.
///
/// # Safety
/// Dereferencing requires that no other reference to the referent cursor
/// is live at the same time. The registry Mutex serializes registry
/// mutation but not access to the cursors themselves; that exclusion comes
/// from the execution model — a Connection runs one statement at a time,
/// and a statement's cursors are touched only by its executor thread.
///
/// Identity compares the data half of the pointer only: codegen may emit
/// multiple vtable pointers for the same trait/type pair across
/// translation units, so vtable comparison can spuriously fail.
#[derive(Clone, Copy)]
pub(crate) struct RegisteredCursor(std::ptr::NonNull<dyn crate::storage::btree::CursorTrait>);

impl RegisteredCursor {
    pub(crate) fn for_cursor(cursor: &dyn crate::storage::btree::CursorTrait) -> Self {
        Self(std::ptr::NonNull::from(cursor))
    }

    /// # Safety
    /// See the type-level doc.
    #[allow(clippy::mut_from_ref)]
    pub(crate) unsafe fn as_mut(&self) -> &mut dyn crate::storage::btree::CursorTrait {
        unsafe { self.0.as_ptr().as_mut().unwrap() }
    }
}

impl PartialEq for RegisteredCursor {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ptr() as *const () == other.0.as_ptr() as *const ()
    }
}

impl Eq for RegisteredCursor {}

unsafe impl Send for RegisteredCursor {}
unsafe impl Sync for RegisteredCursor {}

assert_send_sync!(Pager);

/// State for a `read_page_nonblock` call that has issued its disk read but has
/// not yet been able to insert the page into the cache (cache full, spill in
/// flight). Stored in `Pager::pending_reads` so the next re-entry can resume
/// without issuing a duplicate disk read.
#[derive(Clone)]
struct PendingRead {
    page: PageRef,
    /// `None` if the page was satisfied from WAL/cache shortcut and no
    /// disk read completion needs to be surfaced to the caller.
    disk_read: Option<Completion>,
}

/// Test-only deterministic spill-yield injector for `Pager::read_page`. When
/// armed, the next matching call (after `skip` ignored matches) returns
/// `IO(yield)` once, then disarms itself.
#[cfg(test)]
struct SpillYieldHook {
    /// `-1` = disarmed; otherwise the `page_idx` to fire on.
    target: std::sync::atomic::AtomicI64,
    /// Number of matching calls to ignore before firing.
    skip: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl SpillYieldHook {
    const fn new() -> Self {
        Self {
            target: std::sync::atomic::AtomicI64::new(-1),
            skip: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn arm(&self, page_id: i64, skip: usize) {
        use std::sync::atomic::Ordering::Relaxed;
        self.skip.store(skip, Relaxed);
        self.target.store(page_id, Relaxed);
    }

    /// Returns true exactly once per arming, when the (`skip + 1`)th call for
    /// the armed page id arrives. Internal load-then-store is fine because
    /// tests using this hook are single-threaded.
    fn should_yield_for(&self, page_idx: i64) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        if self.target.load(Relaxed) != page_idx {
            return false;
        }
        if self.skip.load(Relaxed) == 0 {
            self.target.store(-1, Relaxed);
            true
        } else {
            self.skip.fetch_sub(1, Relaxed);
            false
        }
    }
}

#[cfg(feature = "autovacuum")]
pub struct VacuumState {
    /// State machine for [Pager::ptrmap_get]
    ptrmap_get_state: PtrMapGetState,
    /// State machine for [Pager::ptrmap_put]
    ptrmap_put_state: PtrMapPutState,
    btree_create_vacuum_full_state: BtreeCreateVacuumFullState,
}

#[derive(Debug, Clone)]
enum AllocatePageState {
    Start,
    /// Search the trunk page for an available free list leaf.
    /// If none are found, there are two options:
    /// - If there are no more trunk pages, the freelist is empty, so allocate a new page.
    /// - If there are more trunk pages, use the current first trunk page as the new allocation,
    ///   and set the next trunk page as the database's "first freelist trunk page".
    SearchAvailableFreeListLeaf {
        trunk_page: PageRef,
    },
    /// If a freelist leaf is found, reuse it for the page allocation and remove it from the trunk page.
    ReuseFreelistLeaf {
        trunk_page: PageRef,
        leaf_page: PageRef,
        number_of_freelist_leaves: u32,
    },
    /// If a suitable freelist leaf is not found, allocate an entirely new page.
    AllocateNewPage {
        current_db_size: u32,
    },
}

#[derive(Clone)]
enum AllocatePage1State {
    Start,
    Writing {
        page: PageRef,
    },
    /// Fsyncing the freshly written page 1 to the main database file, so a
    /// WAL can never exist next to an empty (0-byte on disk) database file.
    Syncing {
        page: PageRef,
    },
    Done,
}

#[derive(Debug, Clone)]
enum FreePageState {
    Start,
    AddToTrunk { page: Arc<Page> },
    NewTrunk { page: Arc<Page> },
}

/// State machine for async cache spilling.
/// Tracks progress of writing dirty pages to WAL or disk.
#[derive(Debug, Default, Clone)]
enum SpillState {
    #[default]
    /// No spill operation in progress
    Idle,
    /// Lazily initializing the WAL header before the first spill write.
    /// Waiting for the header write (and possible truncate) to complete.
    /// The pinned pages destined for spilling are carried across the yield.
    PreparingWalStart {
        pages: Vec<PinGuard>,
        completion: Completion,
    },
    /// WAL header written; waiting for the fsync that marks the WAL
    /// initialized before we append spill frames.
    PreparingWalFinish {
        pages: Vec<PinGuard>,
        completion: Completion,
    },
    /// WAL spill in progress, waiting for write completions
    WritingToWal {
        /// Pinned pages being spilled
        pages: Vec<PinGuard>,
        /// Completions to wait for
        completions: Vec<Completion>,
    },
    /// Writing ephemeral tables pages directly to disk
    WritingToDisk {
        /// Pages being spilled
        pages: Vec<PinGuard>,
        /// Completions to wait for
        completions: Vec<Completion>,
    },
}
enum CacheFlushStep {
    /// Yield to caller with pending I/O, resume with given phase
    Yield(CacheFlushState, IOCompletions),
    /// Continue immediately to next phase (no I/O wait)
    Continue(CacheFlushState),
    /// Flush complete, return accumulated completions
    Done(Vec<Completion>),
}

#[derive(Default)]
pub enum CacheFlushState {
    #[default]
    Init,
    WalPrepareStart {
        dirty_ids: Vec<usize>,
        completion: Completion,
    },
    WalPrepareFinish {
        dirty_ids: Vec<usize>,
        completion: Completion,
    },
    Collecting(CollectingState),
    WaitingForRead {
        state: CollectingState,
        page_id: usize,
        page: PageRef,
        completion: Completion,
    },
}

#[derive(Default)]
pub struct CollectingState {
    pub dirty_ids: Vec<usize>,
    pub current_idx: usize,
    pub collected_pages: Vec<PageRef>,
    pub completions: Vec<Completion>,
}

impl Pager {
    pub fn new(
        db_file: Arc<dyn DatabaseStorage>,
        wal: Option<Arc<dyn Wal>>,
        io: Arc<dyn crate::io::IO>,
        page_cache: PageCache,
        buffer_pool: Arc<BufferPool>,
        init_lock: Arc<Mutex<()>>,
        init_page_1: Arc<ArcSwapOption<Page>>,
    ) -> Result<Self> {
        let allocate_page1_state = if init_page_1.load().is_some() {
            RwLock::new(AllocatePage1State::Start)
        } else {
            RwLock::new(AllocatePage1State::Done)
        };
        Ok(Self {
            db_file,
            wal,
            page_cache: Arc::new(RwLock::new(page_cache)),
            io,
            pending_reads: RwLock::new(HashMap::new()),
            #[cfg(test)]
            spill_yield: SpillYieldHook::new(),
            dirty_pages: Arc::new(RwLock::new(RoaringBitmap::new())),
            subjournal: RwLock::new(None),
            savepoints: Arc::new(RwLock::new(Vec::new())),
            commit_info: RwLock::new(CommitInfo {
                group: None,
                completion_group: None,
                pending_sync: None,
                state: CommitState::PrepareWal,
                collected_pages: Vec::new(),
                prepared_frames: Vec::new(),
                page_sources: Vec::new(),
                page_source_cursor: 0,
            }),
            syncing: Arc::new(AtomicBool::new(false)),
            checkpoint_state: RwLock::new(CheckpointState::default()),
            buffer_pool,
            auto_vacuum_mode: AtomicU8::new(AutoVacuumMode::None.into()),
            init_lock,
            allocate_page1_state,
            page_size: AtomicU32::new(0), // 0 means not set
            reserved_space: AtomicU16::new(RESERVED_SPACE_NOT_SET),
            schema_cookie: AtomicU64::new(Self::SCHEMA_COOKIE_NOT_SET),
            free_page_state: RwLock::new(FreePageState::Start),
            spill_state: RwLock::new(SpillState::Idle),
            cacheflush_state: RwLock::new(CacheFlushState::default()),
            allocate_page_state: RwLock::new(AllocatePageState::Start),
            max_page_count: AtomicU32::new(DEFAULT_MAX_PAGE_COUNT),
            header_ref_state: RwLock::new(HeaderRefState::Start),
            #[cfg(feature = "autovacuum")]
            vacuum_state: RwLock::new(VacuumState {
                ptrmap_get_state: PtrMapGetState::Start,
                ptrmap_put_state: PtrMapPutState::Start,
                btree_create_vacuum_full_state: BtreeCreateVacuumFullState::Start,
            }),
            io_ctx: RwLock::new(IOContext::default()),
            enable_encryption: AtomicBool::new(false),
            init_page_1,
            #[cfg(target_vendor = "apple")]
            sync_type: AtomicFileSyncType::new(FileSyncType::Fsync),
            cursor_registry: Mutex::new(rustc_hash::FxHashMap::default()),
        })
    }

    /// Add a cursor to the registry. Called from Cursor::new_btree once the
    /// cursor lives in its final heap location; BTreeCursor::drop unregisters.
    pub(crate) fn register_cursor(&self, cursor: &dyn crate::storage::btree::CursorTrait) {
        let root = cursor.root_page();
        let mut registry = self.cursor_registry.lock();
        let bucket = registry.entry(root).or_default();
        bucket.push(RegisteredCursor::for_cursor(cursor));
        // Set BTCF_Multiple on every cursor in the bucket. Idempotent for
        // existing peers; we don't bother branching on the 1→2 transition.
        if bucket.len() >= 2 {
            for &peer in bucket.iter() {
                // SAFETY: see RegisteredCursor's invariant.
                unsafe { peer.as_mut().set_has_peers_for_external_writes(true) };
            }
        }
    }

    pub(crate) fn unregister_cursor(&self, cursor: &dyn crate::storage::btree::CursorTrait) {
        let target = RegisteredCursor::for_cursor(cursor);
        let root = cursor.root_page();
        let mut registry = self.cursor_registry.lock();
        if let Some(bucket) = registry.get_mut(&root) {
            if let Some(idx) = bucket.iter().position(|c| *c == target) {
                bucket.swap_remove(idx);
            }
            // 2→1: clear BTCF_Multiple on the survivor.
            if bucket.len() == 1 {
                let surviving = bucket[0];
                // SAFETY: see RegisteredCursor's invariant.
                unsafe { surviving.as_mut().set_has_peers_for_external_writes(false) };
            }
            if bucket.is_empty() {
                registry.remove(&root);
            }
        }
    }

    /// Snapshot all peers of `except` on the same btree root. Snapshotting
    /// under the lock lets the caller iterate (and yield IO) without
    /// blocking concurrent cursor open/close on the registry.
    pub(crate) fn snapshot_peers_for_root(
        &self,
        except: &dyn crate::storage::btree::CursorTrait,
    ) -> smallvec::SmallVec<[RegisteredCursor; 4]> {
        let except_handle = RegisteredCursor::for_cursor(except);
        let except_root = except.root_page();
        let registry = self.cursor_registry.lock();
        let Some(bucket) = registry.get(&except_root) else {
            return smallvec::SmallVec::new();
        };
        if bucket.len() <= 1 {
            return smallvec::SmallVec::new();
        }
        bucket
            .iter()
            .copied()
            .filter(|c| *c != except_handle)
            .collect()
    }

    /// Invalidate every cursor's page stack. Called from rollback — pinned
    /// pages may now hold pre-rollback bytes (cf. saveAllCursors in
    /// sqlite3BtreeRollback, btree.c:4485).
    pub(crate) fn invalidate_all_cursors(&self) {
        let snapshot: smallvec::SmallVec<[RegisteredCursor; 8]> = {
            let registry = self.cursor_registry.lock();
            registry.values().flat_map(|b| b.iter().copied()).collect()
        };
        for peer in snapshot {
            // SAFETY: see RegisteredCursor's invariant.
            unsafe { peer.as_mut().invalidate_btree_cache() };
        }
    }

    /// Invalidate the page stacks of every peer on `except`'s btree. Used
    /// by clear_btree / btree_destroy where every page is freed; saving
    /// positions would just stash keys that no longer exist.
    pub(crate) fn invalidate_peer_cursors(&self, except: &dyn crate::storage::btree::CursorTrait) {
        let peers = self.snapshot_peers_for_root(except);
        for peer in peers {
            // SAFETY: see RegisteredCursor's invariant.
            unsafe { peer.as_mut().invalidate_btree_cache() };
        }
    }

    /// Get the sync type setting.
    /// On non-Apple platforms, always returns Fsync (compile-time constant).
    #[cfg(target_vendor = "apple")]
    #[inline]
    pub fn get_sync_type(&self) -> FileSyncType {
        self.sync_type.get()
    }

    /// Get the sync type setting.
    /// On non-Apple platforms, always returns Fsync (compile-time constant).
    #[cfg(not(target_vendor = "apple"))]
    #[inline]
    pub fn get_sync_type(&self) -> FileSyncType {
        FileSyncType::Fsync
    }

    /// Set the sync type (for PRAGMA fullfsync). Only effective on Apple platforms.
    #[cfg(target_vendor = "apple")]
    pub fn set_sync_type(&self, value: FileSyncType) {
        self.sync_type.set(value);
    }

    /// Set the sync type. No-op on non-Apple platforms.
    #[cfg(not(target_vendor = "apple"))]
    pub fn set_sync_type(&self, _value: FileSyncType) {
        // No-op: FullFsync only has effect on Apple platforms
    }

    pub fn init_page_1(&self) -> Arc<ArcSwapOption<Page>> {
        self.init_page_1.clone()
    }

    /// Read page 1 (the database header page) using the header_ref_state state machine.
    /// Used by HeaderRef and HeaderRefMut to avoid duplicating the page-loading logic.
    fn read_header_page(&self) -> IOResultOr<PageRef> {
        loop {
            let state = self.header_ref_state.read().clone();
            tracing::trace!("read_header_page - {:?}", state);
            match state {
                HeaderRefState::Start => {
                    // If db is not initialized, return the in-memory page
                    if let Some(page1) = self.init_page_1.load_full() {
                        return Ok(IOResult::Done(page1));
                    }

                    // On spill `return_if_io!` propagates IO up unchanged so
                    // re-entry resumes here via the pager's `pending_reads`
                    // memoization (no duplicate disk read).
                    let (page, c) = return_if_io!(self.read_page(DatabaseHeader::PAGE_ID as i64));
                    *self.header_ref_state.write() = HeaderRefState::CreateHeader {
                        page,
                        completion: c.clone(),
                    };
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                HeaderRefState::CreateHeader { page, completion } => {
                    // Check if the read failed (e.g., due to checksum/decryption error)
                    if let Some(ref c) = completion {
                        if let Some(err) = c.get_error() {
                            *self.header_ref_state.write() = HeaderRefState::Start;
                            return Err(err.into());
                        }
                    }
                    turso_assert!(page.is_loaded(), "page should be loaded");
                    turso_assert!(
                        page.get().id == DatabaseHeader::PAGE_ID,
                        "incorrect header page id"
                    );
                    *self.header_ref_state.write() = HeaderRefState::Start;
                    return Ok(IOResult::Done(page));
                }
            }
        }
    }

    /// Set whether cache spilling is enabled.
    pub fn set_spill_enabled(&self, enabled: bool) {
        self.page_cache.write().set_spill_enabled(enabled);
    }
    /// Get whether cache spilling is enabled.
    pub fn get_spill_enabled(&self) -> bool {
        self.page_cache.read().is_spill_enabled()
    }

    /// Open the subjournal if not yet open.
    /// The subjournal is a file that is used to store the "before images" of pages for the
    /// current savepoint. If the savepoint is rolled back, the pages can be restored from the subjournal.
    ///
    /// Currently uses MemoryIO, but should eventually be backed by temporary on-disk files.
    pub fn open_subjournal(&self) -> Result<()> {
        if self.subjournal.read().is_some() {
            return Ok(());
        }
        use crate::MemoryIO;

        let db_file_io = Arc::new(MemoryIO::new());
        let file = db_file_io.open_file("subjournal", OpenFlags::Create, false)?;
        let db_file = Subjournal::new(file);
        *self.subjournal.write() = Some(db_file);
        Ok(())
    }

    /// Write page to subjournal if the current savepoint does not currently
    /// contain an an entry for it. In case of a statement-level rollback,
    /// the page image can be restored from the subjournal.
    ///
    /// A buffer of length page_size + 4 bytes is allocated and the page id
    /// is written to the beginning of the buffer. The rest of the buffer is filled with the page contents.
    pub fn subjournal_page_if_required(&self, page: &Page) -> Result<()> {
        if self.subjournal.read().is_none() {
            return Ok(());
        }
        let write_offset = {
            let savepoints = self.savepoints.read();
            let Some(cur_savepoint) = savepoints.last() else {
                return Ok(());
            };
            // Skip subjournaling for pages that didn't exist when the savepoint was opened.
            // New pages (allocated during this statement) can be "rolled back" by simply
            // truncating back to the original db_size. This matches SQLite's subjRequiresPage()
            // which checks: p->nOrig >= pgno.
            let page_id_u32 = page.get().id as u32;
            if page_id_u32 > cur_savepoint.db_size.load(Ordering::Acquire) {
                return Ok(());
            }
            if cur_savepoint.has_dirty_page(page_id_u32) {
                return Ok(());
            }
            cur_savepoint.write_offset.load(Ordering::SeqCst)
        };
        let page_id = page.get().id;
        let page_size = self.page_size.load(Ordering::SeqCst) as usize;
        let buffer = {
            let page_id = page.get().id as u32;
            let contents = page.get_contents();
            let buffer = self.buffer_pool.allocate(page_size + 4);
            let contents_buffer = contents.as_ptr();
            turso_assert!(
                contents_buffer.len() == page_size,
                "contents buffer length should be equal to page size"
            );

            buffer.as_mut_slice()[0..4].copy_from_slice(&page_id.to_be_bytes());
            buffer.as_mut_slice()[4..4 + page_size].copy_from_slice(contents_buffer);

            Arc::new(buffer)
        };

        let savepoints = self.savepoints.clone();

        let write_complete = {
            let buf_copy = buffer.clone();
            Box::new(move |res: Result<i32, CompletionError>| {
                let Ok(bytes_written) = res else {
                    return;
                };
                let buf_copy = buf_copy.clone();
                let buf_len = buf_copy.len();

                turso_assert!(
                    bytes_written == buf_len as i32,
                    "wrote({bytes_written}) != expected({buf_len})"
                );

                let savepoints = savepoints.read();
                let cur_savepoint = savepoints.last().unwrap();
                cur_savepoint.add_dirty_page(page_id as u32);
                cur_savepoint
                    .write_offset
                    .fetch_add(page_size as u64 + 4, Ordering::SeqCst);
            })
        };
        let c = Completion::new_write(write_complete);

        let subjournal = self.subjournal.read();
        let subjournal = subjournal.as_ref().unwrap();

        let c = subjournal.write_page(write_offset, page_size, buffer, c)?;
        turso_assert!(c.succeeded(), "memory IO should complete immediately");
        Ok(())
    }

    /// try to "acquire" ownership on the subjournal of the connection-scoped pager
    /// if another statement owns the subjournal - return Busy error and let the caller retry attempt later
    pub fn try_use_subjournal(&self) -> Result<()> {
        let subjournal = self.subjournal.read();
        let subjournal = subjournal.as_ref().expect("subjournal must be opened");
        subjournal.try_use()
    }

    /// release ownership of the subjournal
    /// caller must guarantee that [Self::stop_use_subjournal] is called only after successful call to the [Self::try_use_subjournal]
    pub fn stop_use_subjournal(&self) {
        let subjournal = self.subjournal.read();
        let subjournal = subjournal.as_ref().expect("subjournal must be opened");
        subjournal.stop_use()
    }

    /// check if subjournal is in use for some statement
    pub fn subjournal_in_use(&self) -> bool {
        let subjournal = self.subjournal.read();
        let Some(subjournal) = subjournal.as_ref() else {
            return false;
        };
        subjournal.in_use()
    }

    pub fn open_savepoint(&self, db_size: u32) -> Result<()> {
        self.open_savepoint_with_kind(SavepointKind::Statement, db_size, 0)
    }

    /// Release i.e. commit the current savepoint. This basically just means removing it.
    pub fn release_savepoint(&self) -> Result<()> {
        let mut savepoints = self.savepoints.write();
        if !matches!(
            savepoints.last().map(|savepoint| &savepoint.kind),
            Some(SavepointKind::Statement)
        ) {
            return Ok(());
        }
        let savepoint = savepoints.pop().expect("savepoint must exist");
        if let Some(parent) = savepoints.last() {
            parent.set_write_offset(savepoint.write_offset());
        } else {
            let subjournal = self.subjournal.read();
            let Some(subjournal) = subjournal.as_ref() else {
                return Ok(());
            };
            let c = subjournal.truncate(0)?;
            turso_assert!(c.succeeded(), "memory IO should complete immediately");
        }
        Ok(())
    }

    /// Opens a named savepoint and captures rollback metadata for the current transaction state.
    ///
    /// If `starts_transaction` is true, releasing this savepoint at the root depth commits the
    /// transaction.
    pub fn open_named_savepoint(
        &self,
        name: String,
        db_size: u32,
        starts_transaction: bool,
        deferred_fk_violations: isize,
    ) -> Result<()> {
        self.open_savepoint_with_kind(
            SavepointKind::Named {
                name,
                starts_transaction,
            },
            db_size,
            deferred_fk_violations,
        )
    }

    /// Releases the newest matching named savepoint and all nested savepoints opened after it.
    pub fn release_named_savepoint(&self, name: &str) -> Result<SavepointResult> {
        let mut savepoints = self.savepoints.write();
        let Some(target_idx) = savepoints.iter().rposition(|savepoint| {
            matches!(
                savepoint.kind,
                SavepointKind::Named {
                    name: ref savepoint_name,
                    ..
                } if savepoint_name == name
            )
        }) else {
            return Ok(SavepointResult::NotFound);
        };

        let result = if matches!(
            savepoints[target_idx].kind,
            SavepointKind::Named {
                starts_transaction: true,
                ..
            }
        ) && target_idx == 0
        {
            SavepointResult::Commit
        } else {
            SavepointResult::Release
        };
        if matches!(result, SavepointResult::Commit) {
            // Defer mutation until transaction commit succeeds. If commit fails
            // (e.g. deferred FK violation), savepoints must remain intact.
            return Ok(result);
        }
        let journal_end_offset = savepoints
            .last()
            .map(|savepoint| savepoint.write_offset())
            .unwrap_or(0);

        savepoints.truncate(target_idx);

        if let Some(parent) = savepoints.last() {
            parent.set_write_offset(journal_end_offset);
        } else {
            let subjournal = self.subjournal.read();
            let Some(subjournal) = subjournal.as_ref() else {
                return Ok(result);
            };
            let c = subjournal.truncate(0)?;
            assert!(c.succeeded(), "memory IO should complete immediately");
        }

        Ok(result)
    }

    pub fn clear_savepoints(&self) -> Result<()> {
        *self.savepoints.write() = Vec::new();
        let subjournal = self.subjournal.read();
        let Some(subjournal) = subjournal.as_ref() else {
            return Ok(());
        };
        let c = subjournal.truncate(0)?;
        turso_assert!(c.succeeded(), "memory IO should complete immediately");
        Ok(())
    }

    /// Rollback to the newest savepoint. This basically just means reading the subjournal from the start offset
    /// of the savepoint to the end of the subjournal and restoring the page images to the page cache.
    pub fn rollback_to_newest_savepoint(&self) -> Result<bool> {
        let mut savepoints = self.savepoints.write();
        if !matches!(
            savepoints.last().map(|savepoint| &savepoint.kind),
            Some(SavepointKind::Statement)
        ) {
            return Ok(false);
        }
        let savepoint = savepoints.pop().expect("savepoint must exist");
        let journal_end_offset = savepoint.write_offset();
        let savepoint = savepoint.snapshot();

        self.rollback_to_snapshot(&savepoint, journal_end_offset)?;

        if let Some(parent) = savepoints.last() {
            parent.set_write_offset(savepoint.start_offset);
        }

        Ok(true)
    }

    /// Rollback to the newest matching named savepoint while keeping the named savepoint active.
    ///
    /// Returns deferred FK counter snapshot for the rolled-back savepoint.
    pub fn rollback_to_named_savepoint(&self, name: &str) -> Result<Option<isize>> {
        let target = {
            let savepoints = self.savepoints.read();
            let Some(target_idx) = savepoints.iter().rposition(|savepoint| {
                matches!(
                    savepoint.kind,
                    SavepointKind::Named {
                        name: ref savepoint_name,
                        ..
                    } if savepoint_name == name
                )
            }) else {
                return Ok(None);
            };
            let journal_end_offset = savepoints
                .last()
                .map(|savepoint| savepoint.write_offset())
                .unwrap_or_else(|| savepoints[target_idx].write_offset());
            (
                target_idx,
                savepoints[target_idx].snapshot(),
                journal_end_offset,
            )
        };

        self.rollback_to_snapshot(&target.1, target.2)?;

        let mut savepoints = self.savepoints.write();
        let deferred_fk_violations = target.1.deferred_fk_violations;
        savepoints.truncate(target.0);
        if let Some(parent) = savepoints.last() {
            parent.set_write_offset(target.1.start_offset);
        }
        savepoints.push(Savepoint::from_snapshot(target.1));

        Ok(Some(deferred_fk_violations))
    }

    fn open_savepoint_with_kind(
        &self,
        kind: SavepointKind,
        db_size: u32,
        deferred_fk_violations: isize,
    ) -> Result<()> {
        let subjournal_offset = self
            .savepoints
            .read()
            .last()
            .map(|savepoint| savepoint.write_offset())
            .unwrap_or(0);
        let wal_pos = self
            .wal
            .as_ref()
            .filter(|wal| wal.holds_write_lock())
            .map(|wal| SavepointWalPos {
                max_frame: wal.get_max_frame(),
                checksum: wal.get_last_checksum(),
                checkpoint_seq: wal.get_checkpoint_seq(),
            });
        let savepoint = Savepoint::new(
            kind,
            subjournal_offset,
            db_size,
            wal_pos,
            deferred_fk_violations,
        );
        self.savepoints.write().push(savepoint);
        Ok(())
    }

    #[aristo::intent(
        "Rolling back to a savepoint rewinds the database shape and the page bytes to \
         one consistent snapshot, split at the savepoint's database size. Each page at \
         or below that size that was modified during the savepoint is restored from its \
         pre-savepoint image, kept dirty, and re-inserted into the cache. Pages left \
         untouched during the savepoint keep their existing content. Every page beyond \
         that size is removed from both the dirty set and the cache. No page reachable \
         by the restored header page count or by a restored btree pointer is left as an \
         unwritten zero slot. Restoring the pre-images and discarding the beyond-boundary \
         pages must happen together; dropping either half leaves a live page pointing at \
         zeroed bytes, which the next read rejects as an invalid page type.",
        verify = "neural",
        id = "savepoint_rollback_shape_and_bytes_consistent"
    )]
    fn rollback_to_snapshot(
        &self,
        savepoint: &SavepointSnapshot,
        journal_end_offset: u64,
    ) -> Result<()> {
        self.reset_internal_states();

        let subjournal = self.subjournal.read();
        let Some(subjournal) = subjournal.as_ref() else {
            return Ok(());
        };

        let journal_start_offset = savepoint.start_offset;
        let db_size = savepoint.db_size;

        let mut rollback_bitset = RoaringBitmap::new();
        let mut current_offset = journal_start_offset;
        let page_size = self.page_size.load(Ordering::SeqCst) as u64;
        let mut dirty_pages = self.dirty_pages.write();

        while current_offset < journal_end_offset {
            let page_id_buffer = Arc::new(self.buffer_pool.allocate(4));
            let c = subjournal.read_page_number(current_offset, page_id_buffer.clone())?;
            turso_assert!(c.succeeded(), "memory IO should complete immediately");
            let page_id = u32::from_be_bytes(page_id_buffer.as_slice()[0..4].try_into().unwrap());
            current_offset += 4;

            if rollback_bitset.contains(page_id) {
                current_offset += page_size;
                continue;
            }
            if page_id > db_size {
                current_offset += page_size;
                continue;
            }

            let page_buffer = Arc::new(self.buffer_pool.allocate(page_size as usize));
            let page = Arc::new(Page::new(page_id as i64));
            let c = subjournal.read_page(
                current_offset,
                page_buffer,
                page.clone(),
                page_size as usize,
            )?;
            turso_assert!(c.succeeded(), "memory IO should complete immediately");
            current_offset += page_size;
            rollback_bitset.insert(page_id);
            // The restored image is the transaction-visible state at the
            // savepoint, not necessarily durable state. Keep it dirty so cache
            // eviction cannot drop uncommitted changes that predate the
            // rolled-back savepoint/statement.
            page.set_dirty();
            dirty_pages.insert(page_id);
            self.force_upsert_page_in_cache(page_id as usize, page)?;
        }

        let truncate_completion = subjournal.truncate(journal_start_offset)?;
        turso_assert!(
            truncate_completion.succeeded(),
            "memory IO should complete immediately"
        );

        // Discard all dirty pages allocated after the savepoint. These pages
        // are never subjournaled (see subjournal_page_if_required), so the loop
        // above won't encounter them. We must clean them from dirty_pages before
        // truncating the cache, or phantom dirty entries survive into commit.
        {
            let mut cache = self.page_cache.write();
            for page_id in dirty_pages.iter().filter(|&id| id > db_size) {
                if let Some(page) = cache.get(&PageCacheKey::new(page_id as usize))? {
                    page.clear_dirty();
                    page.try_unpin();
                }
            }
            dirty_pages.remove_range((db_size + 1)..);
            cache.truncate(db_size as usize)?;
        }

        // No WAL position: the transaction never upgraded to a write
        // transaction, so there are no frames to rewind.
        if let (Some(wal), Some(wal_pos)) = (&self.wal, savepoint.wal_pos) {
            wal.rollback(Some(RollbackTo {
                frame: wal_pos.max_frame,
                checksum: wal_pos.checksum,
                checkpoint_seq: wal_pos.checkpoint_seq,
            }));
            self.page_cache
                .write()
                .delete_clean_pages_after_wal_frame(wal_pos.max_frame)
                .map_err(|e| {
                    LimboError::InternalError(format!(
                        "failed to invalidate rolled-back WAL pages: {e:?}"
                    ))
                })?;
        }

        // saveAllCursors at sqlite3BtreeSavepoint (btree.c:4580).
        self.invalidate_all_cursors();

        Ok(())
    }

    #[cfg(feature = "test_helper")]
    pub fn get_pending_byte() -> u32 {
        PENDING_BYTE.load(Ordering::Relaxed)
    }

    #[cfg(feature = "test_helper")]
    /// Used in testing to allow for pending byte pages in smaller dbs
    pub fn set_pending_byte(val: u32) {
        PENDING_BYTE.store(val, Ordering::Relaxed);
    }

    #[cfg(not(feature = "test_helper"))]
    pub const fn get_pending_byte() -> u32 {
        PENDING_BYTE
    }

    /// From SQLITE: https://github.com/sqlite/sqlite/blob/7e38287da43ea3b661da3d8c1f431aa907d648c9/src/btreeInt.h#L608 \
    /// The database page the [PENDING_BYTE] occupies. This page is never used.
    pub fn pending_byte_page_id(&self) -> Option<u32> {
        // PENDING_BYTE_PAGE(pBt)  ((Pgno)((PENDING_BYTE/((pBt)->pageSize))+1))
        let page_size = self.page_size.load(Ordering::SeqCst);
        Self::get_pending_byte()
            .checked_div(page_size)
            .map(|val| val + 1)
    }

    /// Get the maximum page count for this database
    pub fn get_max_page_count(&self) -> u32 {
        self.max_page_count.load(Ordering::SeqCst)
    }

    /// Set the maximum page count for this database
    /// Returns the new maximum page count (may be clamped to current database size)
    pub fn set_max_page_count(&self, new_max: u32) -> IOResultOr<u32> {
        // Get current database size
        let current_page_count =
            return_if_io!(self.with_header(|header| header.database_size.get()));

        // Clamp new_max to be at least the current database size
        let clamped_max = std::cmp::max(new_max, current_page_count);
        self.max_page_count.store(clamped_max, Ordering::SeqCst);
        Ok(IOResult::Done(clamped_max))
    }

    pub fn set_wal(&mut self, wal: Arc<dyn Wal>) {
        wal.set_io_context(self.io_ctx.read().clone());
        self.wal = Some(wal);
    }

    pub fn get_auto_vacuum_mode(&self) -> AutoVacuumMode {
        self.auto_vacuum_mode.load(Ordering::SeqCst).into()
    }

    pub fn set_auto_vacuum_mode(&self, mode: AutoVacuumMode) {
        self.auto_vacuum_mode.store(mode.into(), Ordering::SeqCst);
    }

    /// Persist the auto-vacuum mode to page 1 and keep the pager cache in sync.
    pub fn persist_auto_vacuum_mode(&self, mode: AutoVacuumMode) -> Result<()> {
        let (largest_root_page, incremental_vacuum_enabled) = auto_vacuum_header_fields(mode);

        if self.db_initialized() {
            self.io.block(|| {
                self.with_header_mut(|header| {
                    header.vacuum_mode_largest_root_page = largest_root_page.into();
                    header.incremental_vacuum_enabled = incremental_vacuum_enabled.into();
                })
            })?;
        } else {
            let IOResult::Done(_) = self.with_header_mut(|header| {
                header.vacuum_mode_largest_root_page = largest_root_page.into();
                header.incremental_vacuum_enabled = incremental_vacuum_enabled.into();
            })?
            else {
                panic!("fresh database auto-vacuum setup should not do any IO");
            };
            // Clear dirty pages since this is pre-initialization setup, not a real write transaction.
            // with_header_mut marks page 1 dirty as a side effect, but no transaction is active.
            self.dirty_pages.write().clear();
        }

        self.set_auto_vacuum_mode(mode);
        Ok(())
    }

    /// Retrieves the pointer map entry for a given database page.
    /// `target_page_num` (1-indexed) is the page whose entry is sought.
    /// Returns `Ok(None)` if the page is not supposed to have a ptrmap entry (e.g. header, or a ptrmap page itself).
    #[cfg(feature = "autovacuum")]
    pub fn ptrmap_get(&self, target_page_num: u32) -> IOResultOr<Option<PtrmapEntry>> {
        loop {
            let ptrmap_get_state = {
                let vacuum_state = self.vacuum_state.read();
                vacuum_state.ptrmap_get_state.clone()
            };
            match ptrmap_get_state {
                PtrMapGetState::Start => {
                    tracing::trace!("ptrmap_get(page_idx = {})", target_page_num);
                    let configured_page_size =
                        return_if_io!(self.with_header(|header| header.page_size)).get() as usize;

                    if target_page_num < FIRST_PTRMAP_PAGE_NO
                        || is_ptrmap_page(target_page_num, configured_page_size)
                    {
                        return Ok(IOResult::Done(None));
                    }

                    let ptrmap_pg_no =
                        get_ptrmap_page_no_for_db_page(target_page_num, configured_page_size);
                    let offset_in_ptrmap_page = get_ptrmap_offset_in_page(
                        target_page_num,
                        ptrmap_pg_no,
                        configured_page_size,
                    )?;
                    tracing::trace!(
                        "ptrmap_get(page_idx = {}) = ptrmap_pg_no = {}",
                        target_page_num,
                        ptrmap_pg_no
                    );

                    // `return_if_io!` keeps `ptrmap_get_state` at `Start` on
                    // spill so re-entry resumes via pending-read tracking.
                    let (ptrmap_page, c) = return_if_io!(self.read_page(ptrmap_pg_no as i64));
                    self.vacuum_state.write().ptrmap_get_state = PtrMapGetState::Deserialize {
                        ptrmap_page,
                        offset_in_ptrmap_page,
                    };
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                PtrMapGetState::Deserialize {
                    ptrmap_page,
                    offset_in_ptrmap_page,
                } => {
                    turso_assert!(ptrmap_page.is_loaded(), "ptrmap_page should be loaded");
                    let page_content = ptrmap_page.get_contents();
                    let ptrmap_pg_no = page_content.id;

                    let full_buffer_slice: &[u8] = page_content.as_ptr();

                    // Ptrmap pages are not page 1, so their internal offset within their buffer should be 0.
                    // The actual page data starts at page_content.offset() within the full_buffer_slice.
                    if ptrmap_pg_no != 1 && page_content.offset() != 0 {
                        return Err(LimboError::Corrupt(format!(
                            "Ptrmap page {} has unexpected internal offset {}",
                            ptrmap_pg_no,
                            page_content.offset()
                        ))
                        .into());
                    }
                    let ptrmap_page_data_slice: &[u8] = &full_buffer_slice[page_content.offset()..];
                    let actual_data_length = ptrmap_page_data_slice.len();

                    // Check if the calculated offset for the entry is within the bounds of the actual page data length.
                    if offset_in_ptrmap_page + PTRMAP_ENTRY_SIZE > actual_data_length {
                        return Err(LimboError::InternalError(format!(
                        "Ptrmap offset {offset_in_ptrmap_page} + entry size {PTRMAP_ENTRY_SIZE} out of bounds for page {ptrmap_pg_no} (actual data len {actual_data_length})"
                    )).into());
                    }

                    let entry_slice = &ptrmap_page_data_slice
                        [offset_in_ptrmap_page..offset_in_ptrmap_page + PTRMAP_ENTRY_SIZE];
                    self.vacuum_state.write().ptrmap_get_state = PtrMapGetState::Start;
                    break match PtrmapEntry::deserialize(entry_slice) {
                        Some(entry) => Ok(IOResult::Done(Some(entry))),
                        None => Err(LimboError::Corrupt(format!(
                            "Failed to deserialize ptrmap entry for page {target_page_num} from ptrmap page {ptrmap_pg_no}"
                        )).into()),
                    };
                }
            }
        }
    }

    /// Writes or updates the pointer map entry for a given database page.
    /// `db_page_no_to_update` (1-indexed) is the page whose entry is to be set.
    /// `entry_type` and `parent_page_no` define the new entry.
    #[cfg(feature = "autovacuum")]
    pub fn ptrmap_put(
        &self,
        db_page_no_to_update: u32,
        entry_type: PtrmapType,
        parent_page_no: u32,
    ) -> IOResultOr<()> {
        tracing::trace!(
            "ptrmap_put(page_idx = {}, entry_type = {:?}, parent_page_no = {})",
            db_page_no_to_update,
            entry_type,
            parent_page_no
        );
        loop {
            let ptrmap_put_state = {
                let vacuum_state = self.vacuum_state.read();
                vacuum_state.ptrmap_put_state.clone()
            };
            match ptrmap_put_state {
                PtrMapPutState::Start => {
                    let page_size =
                        return_if_io!(self.with_header(|header| header.page_size)).get() as usize;

                    if db_page_no_to_update < FIRST_PTRMAP_PAGE_NO
                        || is_ptrmap_page(db_page_no_to_update, page_size)
                    {
                        turso_soft_unreachable!("Cannot set ptrmap entry for header/ptrmap page or invalid page", { "page": db_page_no_to_update });
                        return Err(LimboError::InternalError(format!(
                        "Cannot set ptrmap entry for page {db_page_no_to_update}: it's a header/ptrmap page or invalid."
                    )).into());
                    }

                    let ptrmap_pg_no =
                        get_ptrmap_page_no_for_db_page(db_page_no_to_update, page_size);
                    let offset_in_ptrmap_page =
                        get_ptrmap_offset_in_page(db_page_no_to_update, ptrmap_pg_no, page_size)?;
                    tracing::trace!(
                        "ptrmap_put(page_idx = {}, entry_type = {:?}, parent_page_no = {}) = ptrmap_pg_no = {}, offset_in_ptrmap_page = {}",
                        db_page_no_to_update,
                        entry_type,
                        parent_page_no,
                        ptrmap_pg_no,
                        offset_in_ptrmap_page
                    );

                    // `return_if_io!` keeps `ptrmap_put_state` at `Start` on
                    // spill so re-entry resumes via pending-read tracking.
                    let (ptrmap_page, c) = return_if_io!(self.read_page(ptrmap_pg_no as i64));
                    self.vacuum_state.write().ptrmap_put_state = PtrMapPutState::Deserialize {
                        ptrmap_page,
                        offset_in_ptrmap_page,
                    };
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                PtrMapPutState::Deserialize {
                    ptrmap_page,
                    offset_in_ptrmap_page,
                } => {
                    turso_assert!(ptrmap_page.is_loaded(), "page should be loaded");
                    self.add_dirty(&ptrmap_page)?;
                    let page_content = ptrmap_page.get_contents();
                    let ptrmap_pg_no = page_content.id;

                    let full_buffer_slice = page_content.as_ptr();

                    if offset_in_ptrmap_page + PTRMAP_ENTRY_SIZE > full_buffer_slice.len() {
                        return Err(LimboError::InternalError(format!(
                        "Ptrmap offset {} + entry size {} out of bounds for page {} (actual data len {})",
                        offset_in_ptrmap_page,
                        PTRMAP_ENTRY_SIZE,
                        ptrmap_pg_no,
                        full_buffer_slice.len()
                    )).into());
                    }

                    let entry = PtrmapEntry {
                        entry_type,
                        parent_page_no,
                    };
                    entry.serialize(
                        &mut full_buffer_slice
                            [offset_in_ptrmap_page..offset_in_ptrmap_page + PTRMAP_ENTRY_SIZE],
                    )?;

                    turso_assert!(
                        ptrmap_page.get().id == ptrmap_pg_no,
                        "ptrmap page has unexpected number"
                    );
                    self.vacuum_state.write().ptrmap_put_state = PtrMapPutState::Start;
                    break Ok(IOResult::Done(()));
                }
            }
        }
    }

    /// This method is used to allocate a new root page for a btree, both for tables and indexes
    /// FIXME: handle no room in page cache
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn btree_create(&self, flags: &CreateBTreeFlags) -> IOResultOr<u32> {
        let page_type = match flags {
            _ if flags.is_table() => PageType::TableLeaf,
            _ if flags.is_index() => PageType::IndexLeaf,
            _ => unreachable!("Invalid flags state"),
        };
        #[cfg(not(feature = "autovacuum"))]
        {
            let page = return_if_io!(self.do_allocate_page(page_type, 0, BtreePageAllocMode::Any));
            Ok(IOResult::Done(page.get().id as u32))
        }

        //  If autovacuum is enabled, we need to allocate a new page number that is greater than the largest root page number
        #[cfg(feature = "autovacuum")]
        {
            let auto_vacuum_mode =
                AutoVacuumMode::from(self.auto_vacuum_mode.load(Ordering::SeqCst));
            match auto_vacuum_mode {
                AutoVacuumMode::None => {
                    let page =
                        return_if_io!(self.do_allocate_page(page_type, 0, BtreePageAllocMode::Any));
                    Ok(IOResult::Done(page.get().id as u32))
                }
                AutoVacuumMode::Full => {
                    loop {
                        let btree_create_vacuum_full_state = {
                            let vacuum_state = self.vacuum_state.read();
                            vacuum_state.btree_create_vacuum_full_state
                        };
                        match btree_create_vacuum_full_state {
                            BtreeCreateVacuumFullState::Start => {
                                let (mut root_page_num, page_size) = return_if_io!(self
                                    .with_header(|header| {
                                        (
                                            header.vacuum_mode_largest_root_page.get(),
                                            header.page_size.get(),
                                        )
                                    }));

                                turso_assert_greater_than!(root_page_num, 0, "Largest root page number cannot be 0 because that is set to 1 when creating the database with autovacuum enabled");
                                root_page_num += 1;
                                turso_assert_greater_than_or_equal!(
                                    root_page_num,
                                    FIRST_PTRMAP_PAGE_NO,
                                    "can never be less than 2 because we have already incremented"
                                );

                                while is_ptrmap_page(root_page_num, page_size as usize) {
                                    root_page_num += 1;
                                }
                                turso_assert_greater_than_or_equal!(
                                    root_page_num,
                                    3,
                                    "root page must be >= 3 (number of the first root page)"
                                );
                                self.vacuum_state.write().btree_create_vacuum_full_state =
                                    BtreeCreateVacuumFullState::AllocatePage { root_page_num };
                            }
                            BtreeCreateVacuumFullState::AllocatePage { root_page_num } => {
                                //  root_page_num here is the desired root page
                                let page = return_if_io!(self.do_allocate_page(
                                    page_type,
                                    0,
                                    BtreePageAllocMode::Exact(root_page_num),
                                ));
                                let allocated_page_id = page.get().id as u32;

                                return_if_io!(self.with_header_mut(|header| {
                                    if allocated_page_id
                                        > header.vacuum_mode_largest_root_page.get()
                                    {
                                        tracing::debug!(
                                            "Updating largest root page in header from {} to {}",
                                            header.vacuum_mode_largest_root_page.get(),
                                            allocated_page_id
                                        );
                                        header.vacuum_mode_largest_root_page =
                                            allocated_page_id.into();
                                    }
                                }));

                                if allocated_page_id != root_page_num {
                                    //  TODO(Zaid): Handle swapping the allocated page with the desired root page
                                }

                                //  TODO(Zaid): Update the header metadata to reflect the new root page number
                                self.vacuum_state.write().btree_create_vacuum_full_state =
                                    BtreeCreateVacuumFullState::PtrMapPut { allocated_page_id };
                            }
                            BtreeCreateVacuumFullState::PtrMapPut { allocated_page_id } => {
                                //  For now map allocated_page_id since we are not swapping it with root_page_num
                                return_if_io!(self.ptrmap_put(
                                    allocated_page_id,
                                    PtrmapType::RootPage,
                                    0,
                                ));
                                self.vacuum_state.write().btree_create_vacuum_full_state =
                                    BtreeCreateVacuumFullState::Start;
                                return Ok(IOResult::Done(allocated_page_id));
                            }
                        }
                    }
                }
                AutoVacuumMode::Incremental => {
                    return Err(LimboError::InternalError(
                        "Incremental auto-vacuum is not supported".to_string(),
                    )
                    .into());
                }
            }
        }
    }

    /// Allocate a new overflow page.
    /// This is done when a cell overflows and new space is needed.
    // FIXME: handle no room in page cache
    pub fn allocate_overflow_page(&self) -> IOResultOr<PageRef> {
        let page = return_if_io!(self.allocate_page());
        tracing::debug!("Pager::allocate_overflow_page(id={})", page.get().id);

        // setup overflow page
        let contents = page.get_contents();
        let buf = contents.as_ptr();
        buf.fill(0);

        Ok(IOResult::Done(page))
    }

    /// Allocate a new page to the btree via the pager.
    /// This marks the page as dirty and writes the page header.
    // FIXME: handle no room in page cache
    pub fn do_allocate_page(
        &self,
        page_type: PageType,
        offset: usize,
        _alloc_mode: BtreePageAllocMode,
    ) -> IOResultOr<PageRef> {
        let page = return_if_io!(self.allocate_page());
        #[cfg(debug_assertions)]
        turso_assert_eq!(
            offset,
            page.get_contents().offset(),
            "offset doesn't match computed offset for page"
        );
        btree_init_page(&page, page_type, offset, self.usable_space());
        tracing::debug!(
            "do_allocate_page(id={}, page_type={:?})",
            page.get().id,
            page.get_contents().page_type().ok()
        );
        Ok(IOResult::Done(page))
    }

    /// The "usable size" of a database page is the page size specified by the 2-byte integer at offset 16
    /// in the header, minus the "reserved" space size recorded in the 1-byte integer at offset 20 in the header.
    /// The usable size of a page might be an odd number. However, the usable size is not allowed to be less than 480.
    /// In other words, if the page size is 512, then the reserved space size cannot exceed 32.
    pub fn usable_space(&self) -> usize {
        let page_size = self.get_page_size().unwrap_or_else(|| {
            let size = self
                .io
                .block(|| self.with_header(|header| header.page_size))
                .unwrap_or_default();
            self.page_size.store(size.get(), Ordering::SeqCst);
            size
        });

        let reserved_space = self.get_reserved_space().unwrap_or_else(|| {
            let space = if self.db_initialized() {
                self.io
                    .block(|| self.with_header(|header| header.reserved_space))
                    .unwrap_or_default()
            } else {
                // Before page 1 is allocated, the in-memory bootstrap header may still carry
                // reserved_space=0. Use IOContext so checksum/encryption-required tail bytes are
                // respected when computing usable space for first writes.
                self.io_ctx.read().get_reserved_space_bytes()
            };
            self.set_reserved_space(space);
            space
        });

        (page_size.get() as usize) - (reserved_space as usize)
    }

    pub fn db_initialized(&self) -> bool {
        self.init_page_1.load().is_none()
    }

    /// Set the initial page size for the database. Should only be called before the database is initialized
    pub fn set_initial_page_size(&self, size: PageSize) -> Result<()> {
        turso_assert!(!self.db_initialized());
        if let Some(codec) = self.page_codec_external() {
            let reserved_space = codec.required_reserved_bytes();
            if !size.has_valid_reserved_space(reserved_space) {
                return Err(LimboError::InvalidArgument(format!(
                    "page size {} with reserved space {} leaves less than {} usable bytes",
                    size.get(),
                    reserved_space,
                    PageSize::MIN_USABLE_SPACE
                )));
            }
        }
        let IOResult::Done(mut header) = self.with_header(|header| *header)? else {
            panic!("DB should not be initialized and should not do any IO");
        };
        header.page_size = size;

        let page = Arc::new(Page::new(DatabaseHeader::PAGE_ID as i64));
        {
            let inner = page.get();
            inner.set_buffer(Arc::new(Buffer::new_temporary(size.get() as usize)));
        }

        page.get_contents().write_database_header(&header);
        page.set_loaded();
        page.clear_wal_tag();

        btree_init_page(
            &page,
            PageType::TableLeaf,
            DatabaseHeader::SIZE,
            (size.get() - header.reserved_space as u32) as usize,
        );

        self.init_page_1.store(Some(page));
        self.page_size.store(size.get(), Ordering::SeqCst);
        // Clear dirty pages since this is pre-initialization setup, not a real write transaction.
        // Rebuilding init_page_1 must not leak any stale 4 KiB page-1 image into the first write.
        self.dirty_pages.write().clear();

        // Encryption can be configured before a fresh database chooses its page
        // size, so keep the IO context aligned with the pager before the first
        // page write.
        self.reset_page_size_in_encryption_ctx(size);
        Ok(())
    }

    /// Update the encryption page size in the pager IO context and its WAL copy.
    ///
    /// This is a no-op when encryption is not configured.
    fn reset_page_size_in_encryption_ctx(&self, size: PageSize) {
        self.io_ctx.write().reset_page_size_in_encryption_ctx(size);
        if !self.is_encryption_ctx_set() {
            return;
        }
        if let Some(wal) = self.wal.as_ref() {
            wal.set_io_context(self.io_ctx.read().clone());
        }
    }

    /// Set the initial journal version in page 1 before the database is initialized.
    pub fn set_initial_journal_version(&self, version: sqlite3_ondisk::Version) -> Result<()> {
        turso_assert!(!self.db_initialized());
        let raw_version = sqlite3_ondisk::RawVersion::from(version);
        let IOResult::Done(_) = self.with_header_mut(|header| {
            header.read_version = raw_version;
            header.write_version = raw_version;
        })?
        else {
            panic!("DB should not be initialized and should not do any IO");
        };
        // Clear dirty pages since this is pre-initialization setup, not a real write transaction.
        // with_header_mut marks page 1 dirty as a side effect, but no transaction is active.
        self.dirty_pages.write().clear();
        Ok(())
    }

    /// Get the current page size. Returns None if not set yet.
    pub fn get_page_size(&self) -> Option<PageSize> {
        let value = self.page_size.load(Ordering::SeqCst);
        if value == 0 {
            None
        } else {
            PageSize::new(value)
        }
    }

    /// Get the current page size, panicking if not set.
    pub fn get_page_size_unchecked(&self) -> PageSize {
        let value = self.page_size.load(Ordering::SeqCst);
        turso_assert_ne!(value, 0);
        PageSize::new(value).expect("invalid page size stored")
    }

    pub(crate) fn has_wal(&self) -> bool {
        self.wal.is_some()
    }

    #[cfg(test)]
    pub(crate) fn wal_shared_ptr(&self) -> Option<usize> {
        self.wal
            .as_ref()
            .and_then(|wal| wal.as_any().downcast_ref::<crate::storage::wal::WalFile>())
            .map(crate::storage::wal::WalFile::shared_ptr)
    }

    /// Set the page size. Used internally when page size is determined.
    pub fn set_page_size(&self, size: PageSize) {
        self.page_size.store(size.get(), Ordering::SeqCst);
    }

    /// Get the current reserved space. Returns None if not set yet.
    pub fn get_reserved_space(&self) -> Option<u8> {
        let value = self.reserved_space.load(Ordering::SeqCst);
        if value == RESERVED_SPACE_NOT_SET {
            None
        } else {
            Some(value as u8)
        }
    }

    /// Set the reserved space. Must fit in u8.
    pub fn set_reserved_space(&self, space: u8) {
        self.reserved_space.store(space as u16, Ordering::SeqCst);
    }

    /// Schema cookie sentinel value that represents value not set.
    const SCHEMA_COOKIE_NOT_SET: u64 = u64::MAX;

    /// Get the cached schema cookie. Returns None if not set yet.
    pub fn get_schema_cookie_cached(&self) -> Option<u32> {
        let value = self.schema_cookie.load(Ordering::SeqCst);
        if value == Self::SCHEMA_COOKIE_NOT_SET {
            None
        } else {
            Some(value as u32)
        }
    }

    /// Set the schema cookie cache.
    pub fn set_schema_cookie(&self, cookie: Option<u32>) {
        let value = cookie.map_or(Self::SCHEMA_COOKIE_NOT_SET, |v| v as u64);
        self.schema_cookie.store(value, Ordering::SeqCst);
    }

    /// Get the schema cookie, using the cached value if available to avoid reading page 1.
    pub fn get_schema_cookie(&self) -> IOResultOr<u32> {
        // Try to use cached value first
        if let Some(cookie) = self.get_schema_cookie_cached() {
            return Ok(IOResult::Done(cookie));
        }
        // If not cached, read from header and cache it
        self.with_header(|header| header.schema_cookie.get())
    }

    /// This connection's frozen WAL position `(checkpoint_seq, max_frame)` — the read mark for a
    /// reader, or the post-commit position for a writer. `(u32::MAX, u64::MAX)` when there is no
    /// WAL (no WAL materialization hazard). See `Wal::connection_wal_pos`.
    pub fn wal_pos(&self) -> (u32, u64) {
        self.wal
            .as_ref()
            .map_or((u32::MAX, u64::MAX), |wal| wal.connection_wal_pos())
    }

    /// Lowest WAL frame any active reader is pinned at, or `None` if none / no WAL. Used as the
    /// Passive-checkpoint version-store GC floor (includes readers pinned via `begin_read_tx`
    /// before they publish an MVCC transaction). See `MvStore::rootpage_gc_protected`.
    pub fn min_pinned_read_frame(&self) -> Option<u64> {
        self.wal
            .as_ref()
            .and_then(|wal| wal.min_pinned_read_frame())
    }

    /// The WAL backfill boundary (frames at or below this are durable in the DB file). The MVCC
    /// Version-store GC floor for passive checkpoints: a materialized version may be reclaimed only
    /// once its materialization frame is backfilled here, so every snapshot can read it from the btree.
    pub fn wal_backfill_frame(&self) -> Option<u64> {
        self.wal.as_ref().map(|wal| wal.backfill_frame())
    }

    #[inline(always)]
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn begin_read_tx(&self) -> Result<()> {
        let Some(wal) = self.wal.as_ref() else {
            return Ok(());
        };
        let changed = wal.begin_read_tx()?;
        if changed {
            // Someone else changed the database -> assume our page cache is invalid (this is default SQLite behavior, we can probably do better with more granular invalidation)
            self.clear_page_cache(false);
            // Invalidate cached schema cookie to force re-read on next access
            self.set_schema_cookie(None);
        }
        Ok(())
    }

    /// MVCC-only: refresh connection-private WAL change counters without starting a read tx and invalidate cache if needed.
    pub fn mvcc_refresh_if_db_changed(&self) {
        let Some(wal) = self.wal.as_ref() else {
            return;
        };
        if wal.mvcc_refresh_if_db_changed() {
            // Prevents stale page cache reads after MVCC checkpoints update the DB file.
            self.clear_page_cache(false);
            self.set_schema_cookie(None);
        }
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn maybe_allocate_page1(&self) -> IOResultOr<()> {
        if !self.db_initialized() {
            if let Some(_lock) = self.init_lock.try_lock() {
                return Ok(self.allocate_page1()?.map(|_| ()));
            }
            // Give a chance for the allocation to happen elsewhere
            io_yield_one!(Completion::new_yield());
        }
        Ok(IOResult::Done(()))
    }

    #[inline(always)]
    #[instrument(skip_all, level = Level::DEBUG)]
    /// `allowed_auto_actions` controls which automatic WAL maintenance the
    /// caller permits during this begin. The only action consulted here is
    /// `WalAutoActions::Restart`, which gates the WAL-header restart inside
    /// `try_restart_log_before_write`. Callers managing WAL state externally
    /// (sync engine) must not pass `Restart` because rotating the WAL header
    /// behind their back invalidates watermarks they have already published.
    pub fn begin_write_tx(&self, allowed_auto_actions: WalAutoActions) -> IOResultOr<()> {
        // TODO(Diego): The only possibly allocate page1 here is because OpenEphemeral needs a write transaction
        // we should have a unique API to begin transactions, something like sqlite3BtreeBeginTrans
        return_if_io!(self.maybe_allocate_page1());
        let Some(wal) = self.wal.as_ref() else {
            return Ok(IOResult::Done(()));
        };
        wal.begin_write_tx(allowed_auto_actions)?;
        // Must run after the upgrade (and any log restart it performed) so
        // the positions belong to the current WAL generation.
        self.materialize_savepoint_wal_positions();
        Ok(IOResult::Done(()))
    }

    /// Fill in the WAL position of savepoints opened before this write
    /// transaction, mirroring SQLite's `sqlite3PagerOpenSavepoint` at
    /// write-transaction begin. Idempotent: only fills unmaterialized
    /// positions, so upgrade retry loops (Busy/BusySnapshot) are safe.
    fn materialize_savepoint_wal_positions(&self) {
        let Some(wal) = self.wal.as_ref() else {
            return;
        };
        let pos = SavepointWalPos {
            max_frame: wal.get_max_frame(),
            checksum: wal.get_last_checksum(),
            checkpoint_seq: wal.get_checkpoint_seq(),
        };
        for savepoint in self.savepoints.read().iter() {
            let mut wal_pos = savepoint.wal_pos.write();
            if wal_pos.is_none() {
                *wal_pos = Some(pos);
            }
        }
    }

    /// Acquire exclusive WAL access + block new transactions (used by VACUUM).
    ///
    /// This is a blocking alternative to normal `begin_read_tx`.
    ///
    /// VACUUM runs on an existing database, so page 1 must already be allocated
    /// and a WAL must be present.
    pub fn begin_vacuum_blocking_tx(&self) -> IOResultOr<()> {
        if !self.db_initialized() {
            return Err(LimboError::InternalError(
                "begin_vacuum_blocking_tx can be done on an initialized database (page 1 must already be allocated)".into(),
            ).into());
        }
        let wal = self.wal.as_ref().ok_or_else(|| {
            LimboError::InternalError("begin_vacuum_blocking_tx requires WAL mode".into())
        })?;
        wal.begin_vacuum_blocking_tx()?;
        // let's be conservative and clear all cache for vacuum
        // todo: clear cache only if we detect that new writes have occurred like `begin_read_tx`
        self.clear_page_cache(false);
        self.set_schema_cookie(None);
        Ok(IOResult::Done(()))
    }

    /// commit dirty pages from current transaction in WAL mode if this is not nested statement (for nested statements, parent will do the commit)
    /// if update_transaction_state set to false, then [Connection::transaction_state] left unchanged
    /// if update_transaction_state set to true, then [Connection::transaction_state] reset to [TransactionState::None] in case when method completes without error
    /// `sync_mode` belongs to this pager's database because attached databases
    /// can use a different synchronous mode from the connection's main database.
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn commit_tx(
        &self,
        connection: &Connection,
        sync_mode: SyncMode,
        update_transaction_state: bool,
    ) -> IOResultOr<()> {
        if connection.is_nested_stmt() {
            // Parent statement will handle the transaction commit.
            return Ok(IOResult::Done(()));
        }
        let Some(wal) = self.wal.as_ref() else {
            // TODO: Unsure what the semantics of "end_tx" is for in-memory databases, ephemeral tables and ephemeral indexes.
            self.clear_savepoints()?;
            return Ok(IOResult::Done(()));
        };

        let complete_commit = || {
            if update_transaction_state {
                connection.set_tx_state(TransactionState::None);
            }
            self.commit_wal_end();
        };

        loop {
            let commit_state = self.commit_info.read().state;
            tracing::debug!("commit_state: {:?}", commit_state);
            // we separate auto-checkpoint from the commit in order for checkpoint to be able to backfill WAL till the end
            // (including new frames from current transaction)
            // otherwise, we will be unable to do WAL restart
            match commit_state {
                CommitState::AutoCheckpoint => {
                    let checkpoint_result = self.checkpoint(
                        CheckpointMode::Passive {
                            upper_bound_inclusive: None,
                        },
                        sync_mode,
                        false,
                    );
                    match checkpoint_result {
                        Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
                        Ok(IOResult::Done(_)) => complete_commit(),
                        Err(err) => {
                            tracing::debug!("auto-checkpoint failed: {err}");
                            complete_commit();
                            self.cleanup_after_auto_checkpoint_failure();
                        }
                    }
                    self.clear_savepoints()?;
                    return Ok(IOResult::Done(()));
                }
                _ => {
                    return_if_io!(self.commit_wal(
                        connection.wal_auto_actions(),
                        sync_mode,
                        connection.get_data_sync_retry(),
                    ));

                    let schema_did_change = match connection.get_tx_state() {
                        TransactionState::Write { schema_did_change } => schema_did_change,
                        _ => false,
                    };

                    wal.end_write_tx();
                    wal.end_read_tx();

                    tracing::debug!("commit_tx: schema_did_change={schema_did_change}");
                    if schema_did_change {
                        let schema = connection.schema.read().clone();
                        connection.db.update_schema_if_newer(schema);
                    }

                    if self.commit_info.read().state != CommitState::AutoCheckpoint {
                        complete_commit();
                        self.clear_savepoints()?;
                        return Ok(IOResult::Done(()));
                    }

                    // The commit is durable and the WAL locks are released; only
                    // the auto-checkpoint remains. Clear the transaction state now
                    // so an abort during the checkpoint does not try to roll back
                    // the committed transaction. Savepoints stay until the
                    // checkpoint finishes: a re-entered RELEASE must still find
                    // them (see release_named_savepoint).
                    if update_transaction_state {
                        connection.set_tx_state(TransactionState::None);
                    }
                }
            }
        }
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn rollback_tx(&self, connection: &Connection) {
        if connection.is_nested_stmt() {
            // Parent statement will handle the transaction rollback.
            return;
        }
        let Some(wal) = self.wal.as_ref() else {
            // TODO: Unsure what the semantics of "end_tx" is for in-memory databases, ephemeral tables and ephemeral indexes.
            return;
        };
        let (is_write, schema_did_change) = match connection.get_tx_state() {
            TransactionState::Write { schema_did_change } => (true, schema_did_change),
            _ => (false, false),
        };
        tracing::trace!("rollback_tx(schema_did_change={})", schema_did_change);
        if is_write {
            self.clear_savepoints()
                .expect("in practice, clear_savepoints() should never fail as it uses memory IO");
            // IMPORTANT: rollback() must be called BEFORE end_write_tx() releases the write_lock.
            // Otherwise, another thread could commit new frames to frame_cache between
            // end_write_tx() and rollback(), and rollback() would incorrectly remove them.
            self.rollback(schema_did_change, connection, is_write);
            wal.end_write_tx();
        } else {
            self.rollback(schema_did_change, connection, is_write);
        }
        wal.end_read_tx();
    }

    pub(crate) fn cleanup_read_tx(&self) {
        let Some(wal) = self.wal.as_ref() else {
            return;
        };
        self.reset_internal_states();
        if wal.holds_read_lock() {
            wal.end_read_tx();
        }
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn end_read_tx(&self) {
        let Some(wal) = self.wal.as_ref() else {
            return;
        };
        wal.end_read_tx();
    }

    /// End just the write transaction on the WAL, without affecting the read lock.
    pub fn end_write_tx(&self) {
        let Some(wal) = self.wal.as_ref() else {
            return;
        };
        wal.end_write_tx();
    }

    /// Returns true if this pager's WAL currently holds a read lock.
    pub fn holds_read_lock(&self) -> bool {
        let Some(wal) = self.wal.as_ref() else {
            return false;
        };
        wal.holds_read_lock()
    }

    pub fn holds_write_lock(&self) -> bool {
        let Some(wal) = self.wal.as_ref() else {
            return false;
        };
        wal.holds_write_lock()
    }

    /// Rollback and clean up an attached database pager's transaction.
    /// Unlike rollback_tx, this doesn't modify connection-level state.
    pub fn rollback_attached(&self) {
        let Some(wal) = self.wal.as_ref() else {
            return;
        };
        let is_write = wal.holds_write_lock();
        if is_write {
            self.clear_savepoints()
                .expect("clear_savepoints should not fail for attached DB");
            // Clear dirty pages and page cache before releasing the write lock
            self.clear_page_cache(true);
            self.dirty_pages.write().clear();
            self.reset_internal_states();
            self.set_schema_cookie(None);
            wal.rollback(None);
            wal.end_write_tx();
        } else {
            self.cleanup_read_tx();
        }
        if wal.holds_read_lock() {
            wal.end_read_tx();
        }
    }

    /// Reads a page from disk (either WAL or DB file) bypassing page-cache
    #[tracing::instrument(skip_all, level = Level::DEBUG)]
    /// Reads a page without going through the page cache. The read is
    /// added to `group`, when given, before it is submitted.
    pub fn read_page_no_cache(
        &self,
        page_idx: i64,
        frame_watermark: Option<u64>,
        allow_empty_read: bool,
        group: Option<&mut CompletionGroup>,
    ) -> Result<(PageRef, Completion)> {
        turso_assert_greater_than_or_equal!(page_idx, 0);
        tracing::debug!("read_page_no_cache(page_idx = {})", page_idx);
        let page = Arc::new(Page::new(page_idx));
        let io_ctx = self.io_ctx.read();
        let Some(wal) = self.wal.as_ref() else {
            turso_assert!(
                matches!(frame_watermark, Some(0) | None),
                "frame_watermark must be either None or Some(0) because DB has no WAL and read with other watermark is invalid"
            );

            page.set_locked();
            let c = self.begin_read_disk_page(
                page_idx as usize,
                page.clone(),
                allow_empty_read,
                &io_ctx,
                group,
            )?;
            return Ok((page, c));
        };

        if let Some(frame_id) = wal.find_frame(page_idx as u64, frame_watermark)? {
            let c = wal.read_frame(frame_id, page.clone(), self.buffer_pool.clone(), group)?;
            // TODO(pere) should probably first insert to page cache, and if successful,
            // read frame or page
            return Ok((page, c));
        }

        page.set_locked();
        let c = self.begin_read_disk_page(
            page_idx as usize,
            page.clone(),
            allow_empty_read,
            &io_ctx,
            group,
        )?;
        Ok((page, c))
    }

    /// Issue a non-blocking page read, inserting into the page cache, may spill to disk.
    ///
    /// * `Done((page, None))`: page was already in cache, no IO needed.
    /// * `Done((page, Some(c_disk)))`: page was not in cache; it has been
    ///   inserted into the cache and a disk-read is in flight against it.
    ///   The caller must yield on `c_disk` before reading `page` contents.
    /// * `IO(c_spill)`: the page cache was full and a spill is in flight.
    ///   Caller must yield on `c_spill` and then call `read_page_nonblock(idx)`
    ///   again. The disk read for this page has already been issued and will
    ///   be reused on re-entry via `pending_reads` (no duplicate IO).
    ///
    /// Re-entrancy contract: the caller may invoke this with the same
    /// `page_idx` arbitrarily many times. Each `Some(page_idx)` mapping in
    /// `pending_reads` corresponds to a single outstanding disk read; the
    /// entry is removed exactly when this method returns `Done`.
    #[tracing::instrument(skip_all, level = Level::TRACE)]
    pub fn read_page(&self, page_idx: i64) -> IOResultOr<(PageRef, Option<Completion>)> {
        self.read_page_into(page_idx, None)
    }

    /// Like `read_page`, but the disk read, if one is needed, is added to
    /// `group` before it is submitted.
    pub fn read_page_into(
        &self,
        page_idx: i64,
        group: Option<&mut CompletionGroup>,
    ) -> IOResultOr<(PageRef, Option<Completion>)> {
        turso_assert_greater_than_or_equal!(page_idx, 0, "pages in pager should be positive, negative might indicate unallocated pages from mvcc or any other nasty bug");
        tracing::debug!("read_page_nonblock(page_idx = {})", page_idx);
        #[cfg(test)]
        if self.spill_yield.should_yield_for(page_idx) {
            io_yield_one!(crate::Completion::new_yield());
        }
        let pending = self.pending_reads.read().get(&page_idx).cloned();
        let (page, c_disk) = if let Some(pending) = pending {
            // Re-entry: previous call yielded on spill before completing
            // `cache_insert`. Reuse the same PageRef and in-flight disk read
            // rather than issuing duplicate IO.
            (pending.page, pending.disk_read)
        } else {
            // Fast path: cache hit.
            {
                let mut page_cache = self.page_cache.write();
                let page_key = PageCacheKey::new(page_idx as usize);
                if let Some(page) = page_cache.get(&page_key)? {
                    turso_assert!(
                        page_idx as usize == page.get().id,
                        "attempted to read page but got different page",
                        { "expected_page": page_idx, "actual_page": page.get().id }
                    );
                    if !page.is_loaded() {
                        // The page is cache-resident but its read is still in
                        // flight: `read_page` publishes a page into the shared
                        // cache (via `cache_insert` below) *before* its disk
                        // read completes, and `PageCache::get` deliberately
                        // hands out locked-but-unloaded in-flight pages. We have
                        // no completion to surface on this path (the disk-read
                        // completion was consumed by the original caller and the
                        // `pending_reads` entry has already been removed), so
                        // returning `Done((page, None))` would hand the caller a
                        // locked, unloaded page with nothing to wait on: a torn
                        // / uninitialized read, or a concurrent writer filling
                        // the buffer underneath the reader.
                        io_yield_one!(crate::Completion::new_yield());
                    }
                    return Ok(IOResult::Done((page, None)));
                }
            }

            tracing::debug!("read_page(page_idx = {page_idx}) = reading page from disk");
            let (page, c) = self.read_page_no_cache(page_idx, None, false, group)?;
            self.pending_reads.write().insert(
                page_idx,
                PendingRead {
                    page: page.clone(),
                    disk_read: Some(c.clone()),
                },
            );
            (page, Some(c))
        };

        match self.cache_insert(page_idx as usize, page.clone())? {
            IOResult::Done(()) => {
                self.pending_reads.write().remove(&page_idx);
                Ok(IOResult::Done((page, c_disk)))
            }
            IOResult::IO(IOCompletions(spill_c)) => {
                // Leave the pending entry in place; the next call to
                // `read_page_nonblock(page_idx)` will recover it and retry
                // `cache_insert` without re-issuing the disk read.
                io_yield_one!(spill_c);
            }
        }
    }

    fn begin_read_disk_page(
        &self,
        page_idx: usize,
        page: PageRef,
        allow_empty_read: bool,
        io_ctx: &IOContext,
        group: Option<&mut CompletionGroup>,
    ) -> Result<Completion> {
        sqlite3_ondisk::begin_read_page(
            self.db_file.as_ref(),
            self.buffer_pool.clone(),
            page,
            page_idx,
            allow_empty_read,
            io_ctx,
            group,
        )
    }

    /// Insert a page into the cache, with spilling support.
    /// This handles cache full conditions by spilling dirty pages and retrying.
    /// The cache capacity is a soft limit: if nothing can be spilled or
    /// evicted, the page is admitted over capacity rather than failing the
    /// read (mirroring SQLite, where `cache_size` may be exceeded while all
    /// pages are in use); later inserts drain the excess.
    fn cache_insert(&self, page_idx: usize, page: PageRef) -> IOResultOr<()> {
        {
            let mut page_cache = self.page_cache.write();
            let page_key = PageCacheKey::new(page_idx);
            match page_cache.insert(page_key, page.clone()) {
                Ok(_) => return Ok(IOResult::Done(())),
                Err(CacheError::KeyExists) => {
                    unreachable!("Page should not exist in cache after get() miss");
                }
                Err(CacheError::Full) => {
                    // Fall through to spilling
                }
                Err(e) => return Err(e.into()),
            }
        }

        match self.try_spill_dirty_pages()? {
            IOResult::Done(()) => {
                let mut page_cache = self.page_cache.write();
                let page_key = PageCacheKey::new(page_idx);
                match page_cache.force_insert_page(page_key, page) {
                    Ok(_) => Ok(IOResult::Done(())),
                    Err(CacheError::KeyExists) => Ok(IOResult::Done(())),
                    Err(e) => Err(e.into()),
                }
            }
            IOResult::IO(c) => Ok(IOResult::IO(c)),
        }
    }

    /// Test-only: arm `read_page` to return `IO(yield)` once for `page_id`
    /// after `skip` matching calls have passed through.
    #[cfg(test)]
    pub(crate) fn arm_spill_yield_on_read(&self, page_id: i64, skip: usize) {
        self.spill_yield.arm(page_id, skip);
    }

    // Get a page from the cache, if it exists.
    pub fn cache_get(&self, page_idx: usize) -> Result<Option<PageRef>> {
        tracing::trace!("read_page(page_idx = {})", page_idx);
        let mut page_cache = self.page_cache.write();
        let page_key = PageCacheKey::new(page_idx);
        page_cache.get(&page_key)
    }

    /// Get a page from cache only if it matches the target frame
    pub fn cache_get_for_checkpoint(
        &self,
        page_idx: usize,
        target_frame: u64,
        seq: u32,
    ) -> Result<Option<PageRef>> {
        let mut page_cache = self.page_cache.write();
        let page_key = PageCacheKey::new(page_idx);
        let page = page_cache.get(&page_key)?.and_then(|page| {
            if page.is_valid_for_checkpoint(target_frame, seq) {
                tracing::debug!(
                    "cache_get_for_checkpoint: page {page_idx} frame {target_frame} is valid",
                );
                Some(page)
            } else {
                tracing::trace!(
                    "cache_get_for_checkpoint: page {} has frame/tag {:?}: (dirty={}), need frame {} and seq {seq}",
                    page_idx,
                    page.wal_tag_pair(),
                    page.is_dirty(),
                    target_frame
                );
                None
            }
        });
        Ok(page)
    }

    /// Changes the size of the page cache.
    pub fn change_page_cache_size(&self, capacity: usize) -> Result<CacheResizeResult> {
        let mut page_cache = self.page_cache.write();
        Ok(page_cache.resize(capacity))
    }

    pub fn add_dirty(&self, page: &Page) -> Result<()> {
        turso_assert!(
            page.is_loaded(),
            "page must be loaded in add_dirty() so its contents can be subjournaled",
            { "page_id": page.get().id }
        );
        self.subjournal_page_if_required(page)?;
        let mut dirty_pages = self.dirty_pages.write();
        dirty_pages.insert(page.get().id as u32);
        // Notify cache before marking dirty (page was evictable, now it won't be)
        // Only notify if page wasn't already dirty, or if it was spilled
        // State before set_dirty():
        // - clean page: evictable -> set_dirty() makes it dirty and unevictable
        // - dirty + spilled page: evictable -> set_dirty() clears spilled and makes it unevictable
        // - dirty + not spilled page: already unevictable -> no cache accounting change
        if !page.is_dirty() || page.is_spilled() {
            let key = PageCacheKey::new(page.get().id);
            self.page_cache.write().notify_page_dirty(key);
        }
        page.set_dirty();
        Ok(())
    }

    pub fn wal_state(&self) -> Result<WalState> {
        let Some(wal) = self.wal.as_ref() else {
            turso_soft_unreachable!("wal_state() called on database without WAL");
            return Err(LimboError::InternalError(
                "wal_state() called on database without WAL".to_string(),
            ));
        };
        Ok(WalState {
            checkpoint_seq_no: wal.get_checkpoint_seq(),
            max_frame: wal.get_max_frame(),
        })
    }

    /// Flush all dirty pages to disk (async/re-entrant).
    /// Unlike commit_wal, this function does not commit, checkpoint nor sync the WAL/Database.
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn cacheflush(&self) -> IOResultOr<Vec<Completion>> {
        let wal = self
            .wal
            .as_ref()
            .ok_or_else(|| LimboError::InternalError("cacheflush() called without WAL".into()))?;
        let page_sz = self.get_page_size().unwrap_or_default();

        loop {
            let phase = std::mem::take(&mut *self.cacheflush_state.write());

            match self.cacheflush_step(wal, page_sz, phase)? {
                CacheFlushStep::Yield(next_phase, io) => {
                    *self.cacheflush_state.write() = next_phase;
                    return Ok(IOResult::IO(io));
                }
                CacheFlushStep::Continue(next_phase) => {
                    *self.cacheflush_state.write() = next_phase;
                }
                CacheFlushStep::Done(completions) => {
                    *self.cacheflush_state.write() = CacheFlushState::Init;
                    return Ok(IOResult::Done(completions));
                }
            }
        }
    }

    /// Executes one step of the cache flush state machine.
    #[inline]
    fn cacheflush_step(
        &self,
        wal: &Arc<dyn Wal>,
        page_sz: PageSize,
        phase: CacheFlushState,
    ) -> Result<CacheFlushStep> {
        match phase {
            CacheFlushState::Init => self.cacheflush_init(wal, page_sz),
            CacheFlushState::WalPrepareStart {
                dirty_ids,
                completion,
            } => self.cacheflush_wal_prepare_start(wal, dirty_ids, completion),
            CacheFlushState::WalPrepareFinish {
                dirty_ids,
                completion,
            } => self.cacheflush_wal_prepare_finish(dirty_ids, completion),
            CacheFlushState::Collecting(state) => self.cacheflush_collect(wal, page_sz, state),
            CacheFlushState::WaitingForRead {
                state,
                page_id,
                page,
                completion,
            } => self.cacheflush_handle_read(wal, page_sz, state, page_id, page, completion),
        }
    }

    /// Init phase: gather dirty page IDs and begin WAL preparation.
    fn cacheflush_init(&self, wal: &Arc<dyn Wal>, page_sz: PageSize) -> Result<CacheFlushStep> {
        let dirty_ids: Vec<usize> = self.dirty_pages.read().iter().map(|x| x as usize).collect();

        if dirty_ids.is_empty() {
            return Ok(CacheFlushStep::Done(Vec::new()));
        }

        // Start WAL preparation
        match wal.prepare_wal_start(page_sz)? {
            Some(completion) => Ok(CacheFlushStep::Yield(
                CacheFlushState::WalPrepareStart {
                    dirty_ids,
                    completion: completion.clone(),
                },
                IOCompletions(completion),
            )),
            None => {
                // No async prep needed, go straight to finish
                let completion = wal.prepare_wal_finish(self.get_sync_type())?;
                Ok(CacheFlushStep::Yield(
                    CacheFlushState::WalPrepareFinish {
                        dirty_ids,
                        completion: completion.clone(),
                    },
                    IOCompletions(completion),
                ))
            }
        }
    }

    #[inline]
    /// Wait for WAL prepare_start, then call prepare_finish.
    fn cacheflush_wal_prepare_start(
        &self,
        wal: &Arc<dyn Wal>,
        dirty_ids: Vec<usize>,
        completion: Completion,
    ) -> Result<CacheFlushStep> {
        if !completion.succeeded() {
            return Ok(CacheFlushStep::Yield(
                CacheFlushState::WalPrepareStart {
                    dirty_ids,
                    completion: completion.clone(),
                },
                IOCompletions(completion),
            ));
        }

        let finish_completion = wal.prepare_wal_finish(self.get_sync_type())?;
        Ok(CacheFlushStep::Yield(
            CacheFlushState::WalPrepareFinish {
                dirty_ids,
                completion: finish_completion.clone(),
            },
            IOCompletions(finish_completion),
        ))
    }

    #[inline]
    /// Wait for WAL prepare_finish, then start collecting pages.
    fn cacheflush_wal_prepare_finish(
        &self,
        dirty_ids: Vec<usize>,
        completion: Completion,
    ) -> Result<CacheFlushStep> {
        if !completion.succeeded() {
            return Ok(CacheFlushStep::Yield(
                CacheFlushState::WalPrepareFinish {
                    dirty_ids,
                    completion: completion.clone(),
                },
                IOCompletions(completion),
            ));
        }

        Ok(CacheFlushStep::Continue(CacheFlushState::Collecting(
            CollectingState {
                dirty_ids,
                current_idx: 0,
                collected_pages: Vec::new(),
                completions: Vec::new(),
            },
        )))
    }

    #[inline]
    /// Main collection loop: fetch pages from cache, handle evictions, write batches.
    fn cacheflush_collect(
        &self,
        wal: &Arc<dyn Wal>,
        page_sz: PageSize,
        mut state: CollectingState,
    ) -> Result<CacheFlushStep> {
        while state.current_idx < state.dirty_ids.len() {
            let page_id = state.dirty_ids[state.current_idx];
            let cache_result = self.page_cache.write().get(&PageCacheKey::new(page_id))?;

            match cache_result {
                Some(page) => {
                    trace!(
                        "cacheflush(page={}, page_type={:?})",
                        page_id,
                        page.get_contents().page_type().ok()
                    );
                    state.collected_pages.push(page);
                    state.current_idx += 1;
                }
                None => {
                    // Page evicted, need async read from WAL
                    trace!("cacheflush: page {} evicted, reading from WAL", page_id);
                    let (page, completion) =
                        self.read_page_no_cache(page_id as i64, None, false, None)?;

                    if !completion.succeeded() {
                        return Ok(CacheFlushStep::Yield(
                            CacheFlushState::WaitingForRead {
                                state,
                                page_id,
                                page,
                                completion: completion.clone(),
                            },
                            IOCompletions(completion),
                        ));
                    }

                    // Sync read completed immediately
                    trace!(
                        "cacheflush(page={}, page_type={:?}) [re-read sync]",
                        page_id,
                        page.get_contents().page_type().ok()
                    );
                    state.collected_pages.push(page);
                    state.current_idx += 1;
                }
            }
            if Self::should_flush_batch(&state) {
                self.flush_page_batch(wal, page_sz, &mut state)?;
            }
        }
        // All pages collected and written
        Ok(CacheFlushStep::Done(state.completions))
    }

    /// Handle completion of async page read for evicted page.
    fn cacheflush_handle_read(
        &self,
        wal: &Arc<dyn Wal>,
        page_sz: PageSize,
        mut state: CollectingState,
        page_id: usize,
        page: PageRef,
        completion: Completion,
    ) -> Result<CacheFlushStep> {
        if !completion.succeeded() {
            if completion.finished() {
                let err = completion
                    .get_error()
                    .expect("finished unsuccessful cacheflush read must have an error");
                return Err(err.into());
            }
            return Ok(CacheFlushStep::Yield(
                CacheFlushState::WaitingForRead {
                    state,
                    page_id,
                    page,
                    completion: completion.clone(),
                },
                IOCompletions(completion),
            ));
        }
        trace!(
            "cacheflush(page={}, page_type={:?}) [re-read complete]",
            page_id,
            page.get_contents().page_type().ok()
        );
        state.collected_pages.push(page);
        state.current_idx += 1;
        if Self::should_flush_batch(&state) {
            self.flush_page_batch(wal, page_sz, &mut state)?;
        }

        Ok(CacheFlushStep::Continue(CacheFlushState::Collecting(state)))
    }

    #[inline]
    fn should_flush_batch(state: &CollectingState) -> bool {
        let at_capacity = state.collected_pages.len() == IOV_MAX;
        let at_end = state.current_idx >= state.dirty_ids.len();
        !state.collected_pages.is_empty() && (at_capacity || at_end)
    }

    /// Writes accumulated pages to WAL as a single vectored append.
    #[inline]
    fn flush_page_batch(
        &self,
        wal: &Arc<dyn Wal>,
        page_sz: PageSize,
        state: &mut CollectingState,
    ) -> Result<()> {
        let pages = std::mem::take(&mut state.collected_pages);
        // Mark pages as write-pending to detect concurrent modifications
        for page in &pages {
            page.set_write_pending();
        }
        match wal.append_frames_vectored(pages, page_sz) {
            Ok(completion) => {
                state.completions.push(completion);
                Ok(())
            }
            Err(e) => {
                self.io.cancel(&state.completions)?;
                self.io.drain_completions(&state.completions)?;
                Err(e)
            }
        }
    }

    /// Attempt to spill dirty pages from the cache to make room for new pages.
    /// This is called when the cache reaches its spill threshold.
    ///
    /// For databases with a WAL: write only spillable dirty pages to WAL,
    /// then mark them as spilled so they can be evicted even while dirty.
    /// For ephemeral tables: writes pages directly to the temp database file.
    #[instrument(skip_all, level = Level::DEBUG)]
    fn try_spill_dirty_pages(&self) -> IOResultOr<()> {
        loop {
            let state = self.spill_state.read().clone();
            match state {
                SpillState::Idle => {
                    // Check if spilling is needed
                    let spill_result = {
                        let cache = self.page_cache.read();
                        cache.check_spill(IOV_MAX)
                    };
                    match spill_result {
                        SpillResult::NotNeeded | SpillResult::Disabled => {
                            return Ok(IOResult::Done(()));
                        }
                        SpillResult::CacheFull => {
                            tracing::debug!(
                                "try_spill_dirty_pages: cache full, no spillable pages"
                            );
                            return Ok(IOResult::Done(()));
                        }
                        SpillResult::PagesToSpill(pages) => {
                            if pages.is_empty() {
                                return Ok(IOResult::Done(()));
                            }
                            let page_count = pages.len();
                            tracing::debug!("try_spill_dirty_pages: spilling {} pages", page_count);
                            if let Some(wal) = self.wal.as_ref() {
                                let page_sz = self.get_page_size().unwrap_or_default();

                                // Ensure WAL is initialized. Most of the time this
                                // is a no-op (returns None). When it does require
                                // IO we transition through `PreparingWalStart` /
                                // `PreparingWalFinish` and yield rather than block,
                                // carrying the pinned `pages` across each yield.
                                match wal.prepare_wal_start(page_sz)? {
                                    Some(c) => {
                                        *self.spill_state.write() = SpillState::PreparingWalStart {
                                            pages,
                                            completion: c,
                                        };
                                        // Loop to handle the new state (which will
                                        // yield if the completion isn't finished).
                                        continue;
                                    }
                                    None => {
                                        // WAL already initialized — append directly.
                                        return self.spill_append_frames_to_wal(pages);
                                    }
                                }
                            } else {
                                let mut group = CompletionGroup::new(|_| {});
                                // Ephemeral table case: write directly to temp file
                                for page in &pages {
                                    page.set_write_pending();
                                }
                                let completions = self.spill_pages_to_disk(&pages, &mut group)?;
                                if completions.is_empty() {
                                    self.finish_ephemeral_spill(&pages);
                                    return Ok(IOResult::Done(()));
                                }
                                *self.spill_state.write() =
                                    SpillState::WritingToDisk { pages, completions };
                                io_yield_one!(group.build());
                            }
                        }
                    }
                }
                SpillState::PreparingWalStart { pages, completion } => {
                    if !completion.succeeded() {
                        io_yield_one!(completion);
                    }
                    // Header (and any truncate) durable — issue the fsync that
                    // marks the WAL initialized.
                    let wal = self.wal.as_ref().expect("PreparingWalStart requires a WAL");
                    let finish_c = wal.prepare_wal_finish(self.get_sync_type())?;
                    *self.spill_state.write() = SpillState::PreparingWalFinish {
                        pages,
                        completion: finish_c,
                    };
                    continue;
                }
                SpillState::PreparingWalFinish { pages, completion } => {
                    if !completion.succeeded() {
                        io_yield_one!(completion);
                    }
                    // WAL is now initialized; append the spill frames.
                    return self.spill_append_frames_to_wal(pages);
                }
                SpillState::WritingToWal { pages, completions } => {
                    for c in &completions {
                        if !c.succeeded() {
                            io_yield_one!(c.clone());
                        }
                    }
                    // All I/O complete, pages are now in WAL.
                    // Mark spilled pages so they can be evicted while dirty.
                    // Only do so if page wasn't modified since write started (each page has valid wal_tag).
                    let mut spilled_count = 0;
                    {
                        let mut cache = self.page_cache.write();
                        for page in &pages {
                            if page.has_wal_tag() {
                                let key = PageCacheKey::new(page.get().id);
                                cache.notify_page_spilled(key);
                                page.set_spilled();
                                spilled_count += 1;
                            } else {
                                // Page was modified during write, it will need to be re-spilled
                                tracing::debug!(
                                "try_spill_dirty_pages: page {} modified during write, not marking as spilled",
                                page.get().id
                            );
                            }
                        }
                    }
                    if spilled_count == 0 && !pages.is_empty() {
                        tracing::warn!(
                        "try_spill_dirty_pages: no pages marked as spilled out of {}, all were modified during write",
                        pages.len()
                    );
                    }
                    *self.spill_state.write() = SpillState::Idle;
                    trace!(
                        "try_spill_dirty_pages: successfully spilled {} / {} pages to WAL",
                        spilled_count,
                        pages.len(),
                    );
                    return Ok(IOResult::Done(()));
                }
                SpillState::WritingToDisk { pages, completions } => {
                    let all_done = completions.iter().all(|c| c.succeeded());
                    if !all_done {
                        for c in &completions {
                            if !c.succeeded() {
                                io_yield_one!(c.clone());
                            }
                        }
                    }
                    // All I/O complete, finish ephemeral spill
                    self.finish_ephemeral_spill(&pages);
                    *self.spill_state.write() = SpillState::Idle;
                    trace!(
                        "try_spill_dirty_pages: successfully spilled {} pages to disk",
                        pages.len()
                    );
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    /// Append the prepared spill `pages` as WAL frames. Returns `Done` if
    /// the write completed synchronously, otherwise transitions to
    /// `SpillState::WritingToWal` and yields the write completion. The WAL
    /// must already be initialized (callers route through `PreparingWal*`
    /// first).
    fn spill_append_frames_to_wal(&self, pages: Vec<PinGuard>) -> IOResultOr<()> {
        let wal = self
            .wal
            .as_ref()
            .expect("spill_append_frames_to_wal requires a WAL");
        let page_sz = self.get_page_size().unwrap_or_default();
        let wal_pages: Vec<PageRef> = pages
            .iter()
            .map(|p| -> Result<PageRef> {
                self.subjournal_page_if_required(p)?;
                // Set write_pending on all pages before WAL write so callback can
                // detect mid-write modifications.
                p.set_write_pending();
                Ok(p.to_page())
            })
            .collect::<Result<Vec<_>>>()?;
        let c = wal.append_frames_vectored(wal_pages, page_sz)?;

        if c.succeeded() {
            // Synchronous completion, WAL tags already set by callback.
            {
                let mut cache = self.page_cache.write();
                for page in &pages {
                    if page.has_wal_tag() {
                        let key = PageCacheKey::new(page.get().id);
                        cache.notify_page_spilled(key);
                        page.set_spilled();
                    }
                }
            }
            *self.spill_state.write() = SpillState::Idle;
            return Ok(IOResult::Done(()));
        }
        *self.spill_state.write() = SpillState::WritingToWal {
            pages,
            completions: vec![c.clone()],
        };
        io_yield_one!(c);
    }

    /// Wait for any in-flight spill writes to finish.
    /// This prevents publishing WAL metadata that references frames that are not yet durable.
    fn wait_for_spill_completions(&self) -> IOResultOr<()> {
        loop {
            let state = self.spill_state.read().clone();
            if matches!(state, SpillState::Idle) {
                return Ok(IOResult::Done(()));
            }
            match self.try_spill_dirty_pages()? {
                IOResult::Done(()) => continue,
                IOResult::IO(c) => return Ok(IOResult::IO(c)),
            }
        }
    }

    /// Finish a spill operation for ephemeral tables
    fn finish_ephemeral_spill(&self, pages: &[PinGuard]) {
        for page in pages {
            let tag = page.get().wal_tag.load(Ordering::Acquire);
            // wal tag is set to TAG_UNSET when adding to dirty_pages, meaning that this
            // page was dirtied after the spill started, so we don't clear the dirty flag in that case
            if tag != TAG_UNSET {
                page.clear_dirty();
            }
        }
    }
    /// Write a set of pages directly to the database file (for ephemeral tables without WAL).
    /// This is used by try_spill_dirty_pages for ephemeral tables/indexes.
    /// Writes `pages` to the database file. Each write is added to `group`
    /// before it is submitted.
    fn spill_pages_to_disk(
        &self,
        pages: &[PinGuard],
        group: &mut CompletionGroup,
    ) -> Result<Vec<Completion>> {
        let mut completions: Vec<Completion> = Vec::with_capacity(pages.len());
        for page in pages {
            match begin_write_btree_page(self, &page.to_page(), Some(group)) {
                Ok(c) => completions.push(c),
                Err(e) => {
                    self.io.cancel(&completions)?;
                    self.io.drain_completions(&completions)?;
                    return Err(e);
                }
            }
        }

        Ok(completions)
    }

    /// Check if the cache needs spilling and attempt to spill if necessary.
    /// This should be called before inserting new pages into the cache.
    fn ensure_cache_space(&self) -> IOResultOr<()> {
        let needs_spill = {
            let cache = self.page_cache.read();
            cache.needs_spill()
        };

        if needs_spill {
            match self.try_spill_dirty_pages()? {
                IOResult::Done(()) => {
                    // Whether or not anything could be spilled, proceed: the
                    // capacity is a soft limit, and the upcoming insert
                    // evicts what it can and admits the page over capacity
                    // otherwise.
                }
                IOResult::IO(completion) => {
                    return Ok(IOResult::IO(completion));
                }
            }
        }
        Ok(IOResult::Done(()))
    }

    /// Commit the write transaction to the WAL: write any dirty pages as WAL
    /// frames, fsync the WAL if it is dirty, and publish the commit. The WAL
    /// can be dirty without any dirty pages (frames inserted through
    /// `write_frame_raw` bypass dirty-page tracking), so under
    /// synchronous=FULL this fsyncs even when there is nothing to write.
    /// If the WAL size is over the checkpoint threshold, it will checkpoint the WAL to
    /// the database file and then fsync the database file.
    ///
    /// `allowed_auto_actions` controls automatic WAL maintenance permitted at
    /// commit time. Only `WalAutoActions::Checkpoint` is consulted here — it
    /// gates the post-commit auto-checkpoint when `should_checkpoint()` is
    /// true.
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn commit_wal(
        &self,
        allowed_auto_actions: WalAutoActions,
        sync_mode: SyncMode,
        data_sync_retry: bool,
    ) -> IOResultOr<()> {
        {
            let mut commit_info = self.commit_info.write();
            if commit_info.state == CommitState::PrepareWal {
                commit_info.reset();
            }
        }

        // Wait for spill writes before publishing frames
        if let IOResult::IO(c) = self.wait_for_spill_completions()? {
            return Ok(IOResult::IO(c));
        }

        let result = self.commit_wal_inner(allowed_auto_actions, sync_mode, data_sync_retry);
        if result.is_err() {
            self.commit_info.write().reset();
        }
        result
    }

    pub fn commit_wal_end(&self) {
        self.commit_info.write().reset();
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    #[aristo::intent("A commit frame must reach stable storage via fsync before the transaction is reported as durable\n", id = "aristos:wal_commit_requires_fsync", verify = "full", parent = "wal_protocol_correctness")]
    fn commit_wal_inner(
        &self,
        allowed_auto_actions: WalAutoActions,
        sync_mode: SyncMode,
        data_sync_retry: bool,
    ) -> IOResultOr<()> {
        let Some(wal) = self.wal.as_ref() else {
            turso_soft_unreachable!("commit_wal() called without WAL");
            return Err(LimboError::InternalError("commit_wal() called without WAL".into()).into());
        };

        loop {
            let state = self.commit_info.read().state;
            trace!(?state);

            match state {
                CommitState::PrepareWal => {
                    let page_sz = self.get_page_size_unchecked();
                    let c = wal.prepare_wal_start(page_sz)?;
                    let Some(c) = c else {
                        self.commit_info.write().state = CommitState::GetDbSize;
                        continue;
                    };
                    self.commit_info.write().state = CommitState::PrepareWalSync;
                    if !c.succeeded() {
                        io_yield_one!(c);
                    }
                }
                CommitState::PrepareWalSync => {
                    let c = wal.prepare_wal_finish(self.get_sync_type())?;
                    self.commit_info.write().state = CommitState::GetDbSize;
                    if !c.succeeded() {
                        io_yield_one!(c);
                    }
                }
                CommitState::GetDbSize => {
                    let db_size = return_if_io!(self.with_header(|h| h.database_size));
                    self.commit_info.write().state = CommitState::ScanAndIssueReads {
                        db_size: db_size.get(),
                    };
                }
                CommitState::ScanAndIssueReads { db_size } => {
                    let mut commit_info = self.commit_info.write();
                    let dirty_pages = self.dirty_pages.read();

                    if dirty_pages.is_empty() {
                        // No dirty pages to flush, but that does not mean the
                        // WAL is clean: frames written through
                        // write_frame_raw() bypass dirty-page tracking, and
                        // callers (e.g. the sync engine ending a raw-insert
                        // session) treat this commit as their durability
                        // barrier. WaitSync fsyncs if the WAL is dirty.
                        commit_info.state = CommitState::WaitSync;
                        continue;
                    }
                    commit_info.initialize(dirty_pages.len() as usize);
                    let mut cache = self.page_cache.write();

                    for page_id in dirty_pages.iter() {
                        let page_id = page_id as usize;
                        let page_key = PageCacheKey::new(page_id);
                        if cache.peek(&page_key, false).is_some() {
                            commit_info.page_sources.push(PageSource::Cached(page_id));
                        } else {
                            let group = commit_info
                                .group
                                .as_mut()
                                .expect("initialize() created the group");
                            let (page, _completion) =
                                self.read_page_no_cache(page_id as i64, None, false, Some(group))?;
                            commit_info.page_sources.push(PageSource::Evicted(page));
                        }
                    }
                    drop(cache);
                    drop(dirty_pages);
                    let issued_reads = !commit_info
                        .group
                        .as_ref()
                        .expect("initialize() created the group")
                        .is_empty();
                    if issued_reads {
                        // WaitBatchedReads also catches a read that failed
                        // before we got here: the group keeps its error.
                        commit_info.state = CommitState::WaitBatchedReads { db_size };
                        continue;
                    }
                    commit_info.state = CommitState::PrepareFrames { db_size };
                }
                CommitState::WaitBatchedReads { db_size } => {
                    let reads = self.commit_completion();
                    if !reads.finished() {
                        io_yield_one!(reads);
                    }
                    let mut commit_info = self.commit_info.write();
                    if !reads.succeeded() {
                        return Err(LimboError::CompletionError(reads.get_error().unwrap_or(
                            CompletionError::IOError(std::io::ErrorKind::Other, "read"),
                        ))
                        .into());
                    }
                    // All reads complete and successful, proceed to frame preparation
                    commit_info.completion_group = None;
                    commit_info.state = CommitState::PrepareFrames { db_size };
                }
                CommitState::PrepareFrames { db_size } => {
                    let page_sz = self.get_page_size_unchecked();
                    let mut commit_info = self.commit_info.write();
                    let mut cache = self.page_cache.write();

                    'inner: loop {
                        let cursor = commit_info.page_source_cursor;
                        if cursor >= commit_info.page_sources.len() {
                            break 'inner;
                        }

                        let total = commit_info.page_sources.len();
                        let is_last = cursor + 1 >= total;
                        // Linear consumption, no lookup required
                        let page = match &commit_info.page_sources[cursor] {
                            PageSource::Cached(page_id) => {
                                let page_key = PageCacheKey::new(*page_id);
                                cache
                                    .get(&page_key)?
                                    .expect("page evicted between scan and prepare")
                            }
                            PageSource::Evicted(page) => page.clone(),
                        };
                        // Defensive check: prepare_frames will read page contents,
                        // which panics if the buffer is not loaded. If we got here
                        // with an unloaded page (e.g. an evicted dirty page whose
                        // backing WAL frame was truncated by a savepoint rollback),
                        // surface an internal error instead of panicking.
                        if !page.is_loaded() {
                            return Err(LimboError::InternalError(format!(
                                "dirty page {} has no buffer loaded at commit time",
                                page.get().id
                            ))
                            .into());
                        }
                        turso_assert!(
                            page.get().overflow_cells.is_empty(),
                            "dirty page still has overflow cells at commit time",
                            { "page_id": page.get().id }
                        );
                        commit_info.page_source_cursor += 1;
                        commit_info.collected_pages.push(page);

                        if commit_info.collected_pages.len() == IOV_MAX || is_last {
                            self.prepare_collected_frames(
                                &mut commit_info,
                                wal,
                                page_sz,
                                db_size,
                                is_last,
                            )?;
                        }
                    }
                    drop(cache);
                    if commit_info.prepared_frames.is_empty() {
                        turso_assert!(
                            self.dirty_pages.read().is_empty(),
                            "dirty pages must be empty if no frames prepared"
                        );
                        return Ok(IOResult::Done(()));
                    }
                    // Submit all WAL writes
                    let wal_file = wal.wal_file()?;
                    let mut batch = WriteBatch::new(wal_file);
                    for prepared in &commit_info.prepared_frames {
                        batch.writev(prepared.offset, &prepared.bufs);
                    }
                    let mut group = CompletionGroup::new(|_| {});
                    batch.submit(Some(&mut group))?;
                    commit_info.group = Some(group);
                    commit_info.completion_group = None;
                    commit_info.state = CommitState::WaitWrites;
                }
                CommitState::WaitWrites => {
                    let writes = self.commit_completion();
                    if !writes.finished() {
                        io_yield_one!(writes);
                    }
                    let mut commit_info = self.commit_info.write();
                    if !writes.succeeded() {
                        commit_info.completion_group = None;
                        commit_info.prepared_frames.clear();
                        return Err(LimboError::CompletionError(CompletionError::IOError(
                            std::io::ErrorKind::Other,
                            "write",
                        ))
                        .into());
                    }
                    commit_info.completion_group = None;
                    // All writes complete; WaitSync submits the WAL fsync if
                    // one is owed.
                    commit_info.state = CommitState::WaitSync;
                }
                // To protect against partial writes, we MUST ensure that all write Completions
                // finish before submitting the fsync. It is possible that a partial write will
                // cause an IO backend to resubmit the write (particularly with io_uring) and we
                // cannot have the fsync submitted before all writes are fully done, even if
                // they are IO_LINK'd together or we submit the fsync with IO_DRAIN, the only way
                // to ensure durability in the case of partial writes is to ensure the pwritev
                // completes before the fsync is submitted.
                CommitState::WaitSync => {
                    // A pending fsync means a previous entry into this state
                    // already submitted it; wait on it instead of submitting
                    // a second one.
                    let pending = self.commit_info.read().pending_sync.clone();
                    let need_fsync =
                        !self.commit_info.read().prepared_frames.is_empty() || wal.is_dirty();
                    let sync_c = match pending {
                        Some(c) => Some(c),
                        None if sync_mode == SyncMode::Full && need_fsync => {
                            let sync_c = wal.sync(self.get_sync_type())?;
                            self.commit_info.write().pending_sync = Some(sync_c.clone());
                            Some(sync_c)
                        }
                        None => None,
                    };
                    if let Some(sync_c) = sync_c {
                        // Wait for fsync to complete
                        if !sync_c.finished() {
                            io_yield_one!(sync_c);
                        }
                        // Check for fsync error as we might need to panic on data_sync_retry=off
                        let mut commit_info = self.commit_info.write();
                        if !sync_c.succeeded() {
                            commit_info.pending_sync = None;
                            commit_info.prepared_frames.clear();

                            if !data_sync_retry {
                                panic!(
                                    "fsync error (data_sync_retry=off): {:?}",
                                    sync_c.get_error()
                                );
                            }
                            return Err(LimboError::CompletionError(CompletionError::IOError(
                                std::io::ErrorKind::Other,
                                "sync",
                            ))
                            .into());
                        }
                        commit_info.pending_sync = None;
                    }
                    let mut commit_info = self.commit_info.write();
                    if commit_info.prepared_frames.is_empty() {
                        // Nothing to publish: the frames this fsync covered
                        // published themselves via finish_append_frames_commit()
                        // when they were appended.
                        return Ok(IOResult::Done(()));
                    }
                    commit_info.state = CommitState::WalCommitDone;
                }
                CommitState::WalCommitDone => {
                    // all I/O complete, NOW it's safe to advance WAL state
                    let mut commit_info = self.commit_info.write();
                    wal.commit_prepared_frames(&commit_info.prepared_frames);
                    wal.finalize_committed_pages(&commit_info.prepared_frames);
                    wal.finish_append_frames_commit()?;
                    self.dirty_pages.write().clear();
                    commit_info.prepared_frames.clear();

                    let need_checkpoint = allowed_auto_actions.contains(WalAutoActions::Checkpoint)
                        && wal.should_checkpoint();
                    if need_checkpoint {
                        commit_info.state = CommitState::AutoCheckpoint;
                    }
                    return Ok(IOResult::Done(()));
                }
                CommitState::AutoCheckpoint => panic!("checkpoint must be handled externally"),
            }
        }
    }

    /// Prepare collected pages as WAL frames without submitting I/O.
    fn prepare_collected_frames(
        &self,
        commit_info: &mut CommitInfo,
        wal: &Arc<dyn Wal>,
        page_sz: PageSize,
        db_size: u32,
        is_commit_frame: bool,
    ) -> Result<()> {
        let pages = std::mem::take(&mut commit_info.collected_pages);
        if pages.is_empty() {
            return Ok(());
        }
        let commit_flag = if is_commit_frame { Some(db_size) } else { None };
        for page in &pages {
            page.set_write_pending();
        }
        // Chain from previous batch if any
        let prev = commit_info.prepared_frames.last();
        let prepared = wal.prepare_frames(&pages, page_sz, commit_flag, prev)?;
        tracing::debug!("prepare_collected_frames: offset={}", prepared.offset);
        commit_info.prepared_frames.push(prepared);
        Ok(())
    }

    /// The completion for the reads or writes of the current commit step.
    /// Builds the step's group on first use and hands out the same
    /// completion after that.
    fn commit_completion(&self) -> Completion {
        let mut commit_info = self.commit_info.write();
        if let Some(c) = &commit_info.completion_group {
            return c.clone();
        }
        let group = commit_info
            .group
            .take()
            .expect("the commit step issued its IO before waiting on it");
        let c = group.build();
        commit_info.completion_group = Some(c.clone());
        c
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn wal_changed_pages_after(&self, frame_watermark: u64) -> Result<Vec<u32>> {
        let wal = self.wal.as_ref().unwrap();
        wal.changed_pages_after(frame_watermark)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn wal_get_frame(&self, frame_no: u64, frame: &mut [u8]) -> Result<Completion> {
        let Some(wal) = self.wal.as_ref() else {
            turso_soft_unreachable!("wal_get_frame() called on database without WAL");
            return Err(LimboError::InternalError(
                "wal_get_frame() called on database without WAL".to_string(),
            ));
        };
        wal.read_frame_raw(frame_no, frame)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn wal_insert_frame(&self, frame_no: u64, frame: &[u8]) -> Result<WalFrameInfo> {
        let Some(wal) = self.wal.as_ref() else {
            turso_soft_unreachable!("wal_insert_frame() called on database without WAL");
            return Err(LimboError::InternalError(
                "wal_insert_frame() called on database without WAL".to_string(),
            ));
        };
        let (header, raw_page) = parse_wal_frame_header(frame);

        wal.write_frame_raw(
            self.buffer_pool.clone(),
            frame_no,
            header.page_number as u64,
            header.db_size as u64,
            raw_page,
            self.get_sync_type(),
        )?;
        if let Some(page) = self.cache_get(header.page_number as usize)? {
            let content = page.get_contents();
            content.as_ptr().copy_from_slice(raw_page);
            turso_assert!(
                page.get().id == header.page_number as usize,
                "page has unexpected id"
            );
        }
        if header.page_number == 1 {
            let db_size = self
                .io
                .block(|| self.with_header(|header| header.database_size))?;
            tracing::debug!("truncate page_cache as first page was written: {}", db_size);
            let mut page_cache = self.page_cache.write();
            page_cache.truncate(db_size.get() as usize).map_err(|e| {
                LimboError::InternalError(format!("Failed to truncate page cache: {e:?}"))
            })?;
        }
        if header.is_commit_frame() {
            let mut dirty_pages = self.dirty_pages.write();
            tracing::debug!(
                "wal_callback: commit frame, clearing {} dirty pages",
                dirty_pages.len()
            );
            let mut cache = self.page_cache.write();
            for page_id in dirty_pages.iter() {
                let page_key = PageCacheKey::new(page_id as usize);
                // Page may have been evicted from cache after spilling to WAL
                if let Some(page) = cache.get(&page_key)? {
                    page.clear_dirty();
                }
            }
            dirty_pages.clear();
        }
        Ok(WalFrameInfo {
            page_no: header.page_number,
            db_size: header.db_size,
        })
    }

    pub fn is_checkpointing(&self) -> bool {
        !matches!(
            self.checkpoint_state.read().phase.clone(),
            CheckpointPhase::NotCheckpointing
        )
    }

    fn reset_checkpoint_state(&self) {
        self.clear_checkpoint_state();
        self.commit_info.write().state = CommitState::PrepareWal;
    }

    /// Reset checkpoint state machine to initial state.
    /// Use this to clean up after a failed explicit checkpoint (PRAGMA wal_checkpoint).
    pub fn clear_checkpoint_state(&self) {
        let mut state = self.checkpoint_state.write();
        state.phase = CheckpointPhase::NotCheckpointing;
        state.result = None;
        state.mode = None;
        state.lock_source = CheckpointLockSource::Acquire;
    }

    /// Clean up after a auto-checkpoint failure.
    /// Auto-checkpoint executed outside of the main transaction - so WAL transaction was already finalized
    pub fn cleanup_after_auto_checkpoint_failure(&self) {
        self.cleanup_after_checkpoint_failure();
    }

    pub fn cleanup_after_checkpoint_failure(&self) {
        self.reset_checkpoint_state();
        if let Some(wal) = self.wal.as_ref() {
            wal.abort_checkpoint();
        }
    }

    fn next_post_sync_checkpoint_phase(&self, clear_page_cache: bool) -> CheckpointPhase {
        let state = self.checkpoint_state.read();
        let result = state.result.as_ref().expect("result should be set");
        let mode = state.mode.expect("mode should be set");
        if result.wal_checkpoint_backfilled > 0
            && !matches!(
                mode,
                CheckpointMode::Restart | CheckpointMode::Truncate { .. }
            )
        {
            // if we are using a custom codec, then we might have to read the whole page 1 so that
            // it can be decoded. Otherwise reading the header is enough.
            let read_page = self.io_ctx.read().has_codec_transform();
            let read_size = if read_page {
                self.get_page_size_unchecked().get() as usize
            } else {
                PageSize::MIN as usize
            };
            return CheckpointPhase::ReadDbIdentity {
                clear_page_cache,
                read: PendingCheckpointDbIdentityRead {
                    max_frame: result.wal_total_backfilled,
                    header_buf: Arc::new(Buffer::new_temporary(read_size)),
                    bytes_read: Arc::new(AtomicUsize::new(usize::MAX)),
                    read_page,
                    completion: None,
                },
            };
        }
        if matches!(mode, CheckpointMode::Truncate { .. }) {
            CheckpointPhase::TruncateWalFile { clear_page_cache }
        } else {
            CheckpointPhase::Finalize { clear_page_cache }
        }
    }

    #[instrument(skip_all, level = Level::DEBUG, name = "pager_checkpoint",)]
    /// Checkpoint the WAL to the database file (if needed).
    /// Args:
    /// - mode: The checkpoint mode to use (PASSIVE, FULL, RESTART, TRUNCATE)
    /// - sync_mode: The fsync mode to use (OFF, NORMAL, FULL)
    /// - clear_page_cache: Whether to clear the page cache after checkpointing
    pub fn checkpoint(
        &self,
        mode: CheckpointMode,
        sync_mode: crate::SyncMode,
        clear_page_cache: bool,
    ) -> IOResultOr<CheckpointResult> {
        self.checkpoint_inner(
            mode,
            sync_mode,
            clear_page_cache,
            CheckpointLockSource::Acquire,
        )
    }

    pub fn vacuum_checkpoint_with_held_lock(
        &self,
        sync_mode: crate::SyncMode,
        clear_page_cache: bool,
    ) -> IOResultOr<CheckpointResult> {
        self.checkpoint_inner(
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
            sync_mode,
            clear_page_cache,
            CheckpointLockSource::HeldByCaller,
        )
    }

    #[aristo::intent("The nbackfills counter advances after frames are durable, so recovery never replays already-checkpointed frames\n", id = "aristos:wal_nbackfills_orders_with_recovery", verify = "full", parent = "wal_protocol_correctness")]
    fn checkpoint_inner(
        &self,
        mode: CheckpointMode,
        sync_mode: crate::SyncMode,
        clear_page_cache: bool,
        lock_source: CheckpointLockSource,
    ) -> IOResultOr<CheckpointResult> {
        let Some(wal) = self.wal.as_ref() else {
            turso_soft_unreachable!("checkpoint() called on database without WAL");
            return Err(LimboError::InternalError(
                "checkpoint() called on database without WAL".to_string(),
            )
            .into());
        };
        loop {
            // Clone the phase to check what state we're in, but keep result in place
            // This is important because we need to be careful not to e.g. clone and drop the checkpoint result which
            // causes a drop of CheckpointLocks prematurely and results in a panic.
            let phase = self.checkpoint_state.read().phase.clone();
            match phase {
                CheckpointPhase::NotCheckpointing => {
                    let mut state = self.checkpoint_state.write();
                    state.phase = CheckpointPhase::Checkpoint {
                        mode,
                        sync_mode,
                        clear_page_cache,
                    };
                    state.mode = Some(mode);
                    state.lock_source = lock_source;
                }
                CheckpointPhase::Checkpoint {
                    mode,
                    sync_mode,
                    clear_page_cache,
                } => {
                    let checkpoint_lock_source = self.checkpoint_state.read().lock_source;
                    let res = return_if_io!(match checkpoint_lock_source {
                        CheckpointLockSource::Acquire => wal.checkpoint(self, mode, sync_mode),
                        CheckpointLockSource::HeldByCaller => {
                            wal.vacuum_checkpoint_with_held_lock(self, sync_mode)
                        }
                    });
                    let mut state = self.checkpoint_state.write();
                    if matches!(mode, CheckpointMode::Truncate { .. })
                        // `should_truncate` will be true for successful truncate checkpoint
                        && res.should_truncate()
                    {
                        state.phase = CheckpointPhase::TruncateDbFile {
                            sync_mode,
                            clear_page_cache,
                            page1_invalidated: false,
                        };
                    } else if res.wal_checkpoint_backfilled == 0
                        || sync_mode == crate::SyncMode::Off
                    {
                        state.phase = CheckpointPhase::Finalize { clear_page_cache };
                    } else {
                        state.phase = CheckpointPhase::SyncDbFile { clear_page_cache };
                    }
                    state.result = Some(res);
                }
                CheckpointPhase::TruncateDbFile {
                    sync_mode,
                    clear_page_cache,
                    page1_invalidated,
                } => {
                    let should_skip_truncate_db_file = {
                        let state = self.checkpoint_state.read();
                        turso_assert!(
                            matches!(state.mode, Some(CheckpointMode::Truncate { .. })),
                            "mode should be truncate in CheckpointPhase::TruncateDbFile"
                        );
                        let result = state.result.as_ref().expect("result should be set");
                        // Skip if we already sent truncate
                        result.db_truncate_sent
                    };

                    if should_skip_truncate_db_file {
                        let mut state = self.checkpoint_state.write();
                        if sync_mode == crate::SyncMode::Off {
                            // Skip DB sync, proceed to WAL truncation
                            state.phase = CheckpointPhase::TruncateWalFile { clear_page_cache };
                        } else {
                            // Sync DB first, then SyncDbFile will transition to TruncateWalFile
                            state.phase = CheckpointPhase::SyncDbFile { clear_page_cache };
                        }
                        continue;
                    }
                    // Invalidate page 1 (header) in cache before reading - checkpoint potentially wrote pages
                    // directly to DB file from the WAL, so the checkpointer connections' page 1 may have stale database_size.
                    if !page1_invalidated {
                        let page1_key = PageCacheKey::new(DatabaseHeader::PAGE_ID);
                        self.page_cache.write().delete(page1_key)?;
                        let mut state = self.checkpoint_state.write();
                        state.phase = CheckpointPhase::TruncateDbFile {
                            sync_mode,
                            clear_page_cache,
                            page1_invalidated: true,
                        };
                    }

                    // Truncate the database file unless already at correct size
                    let db_size =
                        return_if_io!(self.with_header(|header| header.database_size)).get();
                    let page_size = self.get_page_size().unwrap_or_default();
                    let expected = db_size as u64 * page_size.get() as u64;
                    let should_skip_db_truncate = match self.db_file.size() {
                        Ok(current_size) => expected >= current_size,
                        Err(err) => {
                            // e.g. file.size() is not supported in web worker environment, so we should
                            // skip the truncate if we can't check the size.
                            tracing::debug!(
                                "checkpoint(TRUNCATE): db_file.size unavailable, skipping db truncate pre-check: {err}"
                            );
                            true
                        }
                    };
                    if should_skip_db_truncate {
                        // No DB truncation needed (or unsupported size pre-check), move to next phase.
                        let mut state = self.checkpoint_state.write();
                        if sync_mode == crate::SyncMode::Off {
                            // Skip DB sync, proceed to WAL truncation
                            state.phase = CheckpointPhase::TruncateWalFile { clear_page_cache };
                        } else {
                            // Sync DB first, then SyncDbFile will transition to TruncateWalFile
                            state.phase = CheckpointPhase::SyncDbFile { clear_page_cache };
                        }
                        continue;
                    }
                    let c = self.db_file.truncate(
                        expected as usize,
                        Completion::new_trunc(move |_| {
                            tracing::trace!(
                                "Database file truncated to expected size: {} bytes",
                                expected
                            );
                        }),
                    )?;
                    self.checkpoint_state
                        .write()
                        .result
                        .as_mut()
                        .expect("result should be set")
                        .db_truncate_sent = true;
                    io_yield_one!(c);
                }
                CheckpointPhase::SyncDbFile { clear_page_cache } => {
                    let need_sync_db_file = {
                        let state = self.checkpoint_state.read();
                        let result = state.result.as_ref().expect("result should be set");
                        !result.db_sync_sent
                    };

                    if !need_sync_db_file {
                        turso_assert!(
                            !self.syncing.load(Ordering::SeqCst),
                            "syncing should be done"
                        );
                        self.checkpoint_state.write().phase =
                            self.next_post_sync_checkpoint_phase(clear_page_cache);
                        continue;
                    }

                    let c = sqlite3_ondisk::begin_sync(
                        self.db_file.as_ref(),
                        self.syncing.clone(),
                        self.get_sync_type(),
                    )?;
                    self.checkpoint_state
                        .write()
                        .result
                        .as_mut()
                        .expect("result should be set")
                        .db_sync_sent = true;
                    io_yield_one!(c);
                }
                CheckpointPhase::ReadDbIdentity {
                    clear_page_cache,
                    mut read,
                } => {
                    if read.completion.is_none() {
                        let header_buf = read.header_buf.clone();
                        let bytes_read = read.bytes_read.clone();
                        let completion = Completion::new_read(header_buf, {
                            Box::new(move |res| {
                                if let Ok((_buf, count)) = res {
                                    bytes_read.store(count as usize, Ordering::Release);
                                }
                                None
                            })
                        });
                        let c = if read.read_page {
                            self.db_file.read_page(
                                DatabaseHeader::PAGE_ID,
                                &self.io_ctx.read(),
                                completion,
                            )?
                        } else {
                            self.db_file.read_header(completion)?
                        };
                        read.completion = Some(c.clone());
                        self.checkpoint_state.write().phase = CheckpointPhase::ReadDbIdentity {
                            clear_page_cache,
                            read,
                        };
                        io_yield_one!(c);
                    }

                    let completion = read
                        .completion
                        .as_ref()
                        .expect("database identity read completion should be set");
                    if !completion.finished() {
                        io_yield_one!(completion.clone());
                    }
                    if !completion.succeeded() {
                        return Err(completion
                            .get_error()
                            .expect("finished database identity read should have an error")
                            .into());
                    }
                    let bytes_read = read.bytes_read.load(Ordering::Acquire);
                    turso_assert!(
                        bytes_read != usize::MAX,
                        "successful database identity read must record the byte count"
                    );
                    if read.read_page && bytes_read != read.header_buf.len() {
                        return Err(CompletionError::ShortRead {
                            page_idx: DatabaseHeader::PAGE_ID,
                            expected: read.header_buf.len(),
                            actual: bytes_read,
                        }
                        .into());
                    }
                    if bytes_read < DatabaseHeader::SIZE {
                        return Err(LimboError::Corrupt(
                            "database header unreadable after checkpoint sync".into(),
                        )
                        .into());
                    }
                    let (db_size_pages, db_header_crc32c) =
                        super::wal::database_identity_from_header_bytes(
                            &read.header_buf.as_slice()[..DatabaseHeader::SIZE],
                        )?;
                    if let Some(c) = wal.install_durable_backfill_proof(
                        read.max_frame,
                        db_size_pages,
                        db_header_crc32c,
                        self.get_sync_type(),
                    )? {
                        self.checkpoint_state.write().phase = CheckpointPhase::SyncBackfillProof {
                            clear_page_cache,
                            max_frame: read.max_frame,
                        };
                        io_yield_one!(c);
                    }
                    self.checkpoint_state.write().phase = CheckpointPhase::PublishBackfill {
                        clear_page_cache,
                        max_frame: read.max_frame,
                    };
                    continue;
                }
                CheckpointPhase::SyncBackfillProof {
                    clear_page_cache,
                    max_frame,
                } => {
                    self.checkpoint_state.write().phase = CheckpointPhase::PublishBackfill {
                        clear_page_cache,
                        max_frame,
                    };
                    continue;
                }
                CheckpointPhase::PublishBackfill {
                    clear_page_cache,
                    max_frame,
                } => {
                    {
                        let state = self.checkpoint_state.read();
                        let result = state.result.as_ref().expect("result should be set");
                        turso_assert!(
                            result.wal_checkpoint_backfilled > 0,
                            "PublishBackfill phase requires frames backfilled during checkpoint",
                            {
                                "publish_backfill": max_frame,
                                "wal_max_frame": result.wal_max_frame,
                                "wal_total_backfilled": result.wal_total_backfilled,
                                "wal_checkpoint_backfilled": result.wal_checkpoint_backfilled
                            }
                        );
                        turso_assert!(
                            max_frame == result.wal_total_backfilled,
                            "PublishBackfill target must match checkpoint result",
                            {
                                "publish_backfill": max_frame,
                                "wal_total_backfilled": result.wal_total_backfilled
                            }
                        );
                        turso_assert!(
                            result.wal_total_backfilled <= result.wal_max_frame,
                            "checkpoint result cannot backfill beyond WAL max frame",
                            {
                                "wal_total_backfilled": result.wal_total_backfilled,
                                "wal_max_frame": result.wal_max_frame
                            }
                        );
                    }
                    wal.publish_backfill(max_frame);
                    let next_phase = {
                        let state = self.checkpoint_state.read();
                        if matches!(state.mode, Some(CheckpointMode::Truncate { .. })) {
                            CheckpointPhase::TruncateWalFile { clear_page_cache }
                        } else {
                            CheckpointPhase::Finalize { clear_page_cache }
                        }
                    };
                    self.checkpoint_state.write().phase = next_phase;
                    continue;
                }
                CheckpointPhase::TruncateWalFile { clear_page_cache } => {
                    // Truncate WAL file after DB is safely synced - this ensures data durability.
                    // If crash occurred after WAL truncate but before DB sync, data would be lost.
                    let need_wal_truncate = {
                        let state = self.checkpoint_state.read();
                        turso_assert!(
                            matches!(state.mode, Some(CheckpointMode::Truncate { .. })),
                            "mode should be truncate in CheckpointPhase::TruncateWalFile"
                        );
                        let result = state.result.as_ref().expect("result should be set");
                        !result.wal_truncate_sent || !result.wal_sync_sent
                    };

                    if !need_wal_truncate {
                        self.checkpoint_state.write().phase =
                            CheckpointPhase::Finalize { clear_page_cache };
                        continue;
                    }

                    // Call WAL truncate
                    return_if_io!(wal.truncate_wal(
                        self.checkpoint_state
                            .write()
                            .result
                            .as_mut()
                            .expect("result should be set"),
                        self.get_sync_type(),
                    ));
                }
                CheckpointPhase::Finalize { clear_page_cache } => {
                    let mut state = self.checkpoint_state.write();
                    let mut res = state.result.take().expect("result should be set");
                    state.phase = CheckpointPhase::NotCheckpointing;
                    state.mode = None;
                    state.lock_source = CheckpointLockSource::Acquire;

                    // Clear page cache only if requested (explicit checkpoints do this, auto-checkpoint does not)
                    if clear_page_cache {
                        self.invalidate_all_cursors();
                        self.page_cache.write().clear(false).map_err(|e| {
                            res.release_guard();
                            LimboError::InternalError(format!("Failed to clear page cache: {e:?}"))
                        })?;
                    }

                    // Release checkpoint guard
                    res.release_guard();

                    return Ok(IOResult::Done(res));
                }
            }
        }
    }

    #[cfg(feature = "simulator")]
    pub fn run_checkpoint_until_post_sync_gap_for_testing(
        &self,
        mode: CheckpointMode,
    ) -> Result<u64> {
        loop {
            match self.checkpoint(mode, crate::SyncMode::Full, true)? {
                IOResult::Done(_) => {
                    return Err(LimboError::InternalError(
                        "checkpoint completed before reaching the post-sync pre-publish gap"
                            .to_string(),
                    ));
                }
                IOResult::IO(io) => io.wait(self.io.as_ref())?,
            }

            let state = self.checkpoint_state.read();
            let Some(result) = state.result.as_ref() else {
                continue;
            };
            if matches!(state.phase, CheckpointPhase::ReadDbIdentity { .. })
                && result.db_sync_sent
                && !self.syncing.load(Ordering::SeqCst)
            {
                return Ok(result.wal_total_backfilled);
            }
        }
    }

    /// Invalidates entire page cache by removing all dirty and clean pages. Usually used in case
    /// of a rollback or in case we want to invalidate page cache after starting a read transaction
    /// right after new writes happened which would invalidate current page cache.
    /// Test-only: evict clean, unpinned pages WITHOUT invalidating cursors, so we
    /// can exercise what happens to a cursor that still holds a `PageRef` to an
    /// evicted (buffer-taken) page — the exact hazard normal LRU eviction creates.
    #[cfg(test)]
    pub fn test_evict_all_unpinned_clean(&self) {
        self.page_cache.write().test_evict_all_unpinned_clean();
    }

    pub fn clear_page_cache(&self, clear_dirty: bool) {
        self.invalidate_all_cursors();
        let dirty_pages = self.dirty_pages.write();
        let mut cache = self.page_cache.write();
        for page_id in dirty_pages.iter() {
            let page_key = PageCacheKey::new(page_id as usize);
            if let Some(page) = cache.get(&page_key).unwrap_or(None) {
                page.clear_dirty();
            }
        }
        cache
            .clear(clear_dirty)
            .expect("Failed to clear page cache");
        if clear_dirty {
            drop(dirty_pages);
            self.dirty_pages.write().clear();
        }
    }

    /// Checkpoint in Truncate mode and delete the WAL file. This method is _only_ to be called
    /// for shutting down the last remaining connection to a database.
    ///
    /// sqlite3.h
    /// Usually, when a database in [WAL mode] is closed or detached from a
    /// database handle, SQLite checks if if there are other connections to the
    /// same database, and if there are no other database connection (if the
    /// connection being closed is the last open connection to the database),
    /// then SQLite performs a [checkpoint] before closing the connection and
    /// deletes the WAL file.
    pub fn checkpoint_shutdown(
        &self,
        allowed_auto_actions: WalAutoActions,
        sync_mode: crate::SyncMode,
    ) -> Result<()> {
        let mut attempts = 0;
        {
            let Some(wal) = self.wal.as_ref() else {
                turso_soft_unreachable!("checkpoint_shutdown() called on database without WAL");
                return Err(LimboError::InternalError(
                    "checkpoint_shutdown() called on database without WAL".to_string(),
                ));
            };
            // fsync the wal syncronously before beginning checkpoint
            let c = wal.sync(self.get_sync_type())?;
            self.io.wait_for_completion(c)?;
        }
        if allowed_auto_actions.contains(WalAutoActions::Checkpoint) {
            while let Err(LimboError::Busy) = self.blocking_checkpoint(
                CheckpointMode::Truncate {
                    upper_bound_inclusive: None,
                },
                sync_mode,
            ) {
                if attempts == 3 {
                    // don't return error on `close` if we are unable to checkpoint, we can silently fail
                    tracing::warn!(
                        "Failed to checkpoint WAL on shutdown after 3 attempts, giving up"
                    );
                    return Ok(());
                }
                attempts += 1;
            }
        }
        // TODO: delete the WAL file here after truncate checkpoint, but *only* if we are sure that
        // no other connections have opened since.
        Ok(())
    }

    /// Perform a blocking checkpoint with the specified mode.
    /// This is a convenience wrapper around `checkpoint()` that blocks until completion.
    /// Explicit checkpoints clear the page cache after completion.
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn blocking_checkpoint(
        &self,
        mode: CheckpointMode,
        sync_mode: crate::SyncMode,
    ) -> Result<CheckpointResult> {
        let result = self.io.block(|| self.checkpoint(mode, sync_mode, true));
        if result.is_err() {
            self.cleanup_after_checkpoint_failure();
        }
        result
    }

    pub fn freepage_list(&self) -> u32 {
        self.io
            .block(|| HeaderRef::from_pager(self))
            .map(|header_ref| header_ref.borrow().freelist_pages.get())
            .unwrap_or(0)
    }
    // Providing a page is optional, if provided it will be used to avoid reading the page from disk.
    // This is implemented in accordance with sqlite freepage2() function.
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn free_page(&self, mut page: Option<PageRef>, page_id: usize) -> IOResultOr<()> {
        tracing::trace!("free_page(page_id={})", page_id);
        // Number of reserved slots in trunk header (next pointer + leaf count)
        const RESERVED_SLOTS: usize = 2;

        let header_ref = return_if_io!(HeaderRefMut::from_pager(self));
        let header = header_ref.borrow_mut();

        let mut state = self.free_page_state.write();
        tracing::debug!(?state);
        loop {
            match &mut *state {
                FreePageState::Start => {
                    if page_id < 2 || page_id > header.database_size.get() as usize {
                        return Err(LimboError::Corrupt(format!(
                            "Invalid page number {page_id} for free operation"
                        ))
                        .into());
                    }

                    // The first yield point is the `HeaderRefMut::from_pager`
                    // acquisition above the loop, not this read fork: if it
                    // yields for the page-1 read, re-entry re-runs that prefix
                    // (it is idempotent — the pager cache returns the same
                    // header page) before reaching `Start` again, where `state`
                    // is still `Start`. The read fork below is likewise safe:
                    // if the caller passes `Some(page)`, no IO occurs and the
                    // mutations below run synchronously. If the caller passes
                    // `None` and `read_page` yields for spill, we leave `state`
                    // at `Start` so re-entry re-takes either branch (the
                    // pager's `pending_reads` memoization returns the same
                    // `PageRef` the next time). Crucially, the non-idempotent
                    // mutations (`freelist_pages` increment, `page.pin()`,
                    // state advance) all happen AFTER both branches converge.
                    let (page, c) = match page.take() {
                        Some(page) => {
                            turso_assert_eq!(
                                page.get().id,
                                page_id,
                                "free_page page id mismatch",
                                { "expected": page_id, "actual": page.get().id }
                            );
                            (page, None)
                        }
                        None => return_if_io!(self.read_page(page_id as i64)),
                    };
                    page.get().overflow_cells.clear();
                    header.freelist_pages = (header.freelist_pages.get() + 1).into();

                    let trunk_page_id = header.freelist_trunk_page.get();

                    // Pin page to prevent eviction while stored in state machine
                    page.pin();

                    if trunk_page_id != 0 {
                        *state = FreePageState::AddToTrunk { page };
                    } else {
                        *state = FreePageState::NewTrunk { page };
                    }
                    if let Some(c) = c {
                        if !c.succeeded() {
                            io_yield_one!(c);
                        }
                    }
                }
                FreePageState::AddToTrunk { page } => {
                    let trunk_page_id = header.freelist_trunk_page.get();
                    // Spill yield here keeps `state` at `AddToTrunk`. The
                    // subsequent writes / `unpin()` only run after we have
                    // a loaded `trunk_page`; on re-entry the pager's
                    // `pending_reads` returns the same `trunk_page`, and the
                    // writes are byte-identical (we haven't written yet so
                    // `number_of_leaf_pages` is unchanged).
                    let (trunk_page, c) = return_if_io!(self.read_page(trunk_page_id as i64));
                    if let Some(c) = c {
                        if !c.succeeded() {
                            io_yield_one!(c);
                        }
                    }
                    turso_assert!(trunk_page.is_loaded(), "trunk_page should be loaded");

                    let trunk_page_contents = trunk_page.get_contents();
                    let number_of_leaf_pages =
                        trunk_page_contents.read_u32_no_offset(FREELIST_TRUNK_OFFSET_LEAF_COUNT);

                    let max_free_list_entries =
                        (header.usable_space() / FREELIST_LEAF_PTR_SIZE) - RESERVED_SLOTS;

                    if number_of_leaf_pages < max_free_list_entries as u32 {
                        turso_assert!(
                            trunk_page.get().id == trunk_page_id as usize,
                            "trunk page has unexpected id"
                        );
                        self.add_dirty(&trunk_page)?;

                        trunk_page_contents.write_u32_no_offset(
                            FREELIST_TRUNK_OFFSET_LEAF_COUNT,
                            number_of_leaf_pages + 1,
                        );
                        trunk_page_contents.write_u32_no_offset(
                            FREELIST_TRUNK_OFFSET_FIRST_LEAF_PTR
                                + (number_of_leaf_pages as usize * FREELIST_LEAF_PTR_SIZE),
                            page_id as u32,
                        );

                        // Unpin page before finishing - it's added to freelist
                        page.unpin();
                        break;
                    }
                    // page remains pinned as it transitions to NewTrunk state
                    *state = FreePageState::NewTrunk { page: page.clone() };
                }
                FreePageState::NewTrunk { page } => {
                    turso_assert!(page.is_loaded(), "page should be loaded");
                    // If we get here, need to make this page a new trunk
                    turso_assert!(page.get().id == page_id, "page has unexpected id");
                    self.add_dirty(page)?;

                    let trunk_page_id = header.freelist_trunk_page.get();

                    let contents = page.get_contents();
                    // Point to previous trunk
                    contents
                        .write_u32_no_offset(FREELIST_TRUNK_OFFSET_NEXT_TRUNK_PTR, trunk_page_id);
                    // Zero leaf count
                    contents.write_u32_no_offset(FREELIST_TRUNK_OFFSET_LEAF_COUNT, 0);
                    // Update page 1 to point to new trunk
                    header.freelist_trunk_page = (page_id as u32).into();
                    // Unpin page before finishing - it's now a trunk page
                    page.unpin();
                    break;
                }
            }
        }
        *state = FreePageState::Start;
        Ok(IOResult::Done(()))
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn allocate_page1(&self) -> IOResultOr<PageRef> {
        let state = self.allocate_page1_state.read().clone();
        match state {
            AllocatePage1State::Start => {
                turso_assert!(!self.db_initialized());
                tracing::trace!("allocate_page1(Start)");

                let IOResult::Done(mut default_header) = self.with_header(|header| *header)? else {
                    panic!("DB should not be initialized and should not do any IO");
                };

                turso_assert_eq!(default_header.database_size.get(), 0);
                default_header.database_size = 1.into();

                // Use cached reserved_space if set (e.g., by sync engine before page allocation),
                // otherwise fall back to IOContext's encryption/checksum requirements.
                let reserved_space_bytes = self.get_reserved_space().unwrap_or_else(|| {
                    let io_ctx = self.io_ctx.read();
                    io_ctx.get_reserved_space_bytes()
                });
                default_header.reserved_space = reserved_space_bytes;
                self.set_reserved_space(reserved_space_bytes);

                if let Some(size) = self.get_page_size() {
                    default_header.page_size = size;
                }

                tracing::debug!(
                    "allocate_page1(Start) page_size = {:?}, reserved_space = {}",
                    default_header.page_size,
                    default_header.reserved_space
                );

                self.buffer_pool
                    .finalize_with_page_size(default_header.page_size.get() as usize)?;
                let page = allocate_new_page(1, &self.buffer_pool);

                let contents = page.get_contents();
                contents.write_database_header(&default_header);

                let page1 = page;
                // Create the sqlite_schema table, for this we just need to create the btree page
                // for the first page of the database which is basically like any other btree page
                // but with a 100 byte offset, so we just init the page so that sqlite understands
                // this is a correct page.
                btree_init_page(
                    &page1,
                    PageType::TableLeaf,
                    DatabaseHeader::SIZE,
                    (default_header.page_size.get() - default_header.reserved_space as u32)
                        as usize,
                );
                let c = begin_write_btree_page(self, &page1, None)?;

                // Pin page1 to prevent eviction while stored in state machine
                page1.pin();
                *self.allocate_page1_state.write() = AllocatePage1State::Writing { page: page1 };
                io_yield_one!(c);
            }
            AllocatePage1State::Writing { page } => {
                turso_assert!(page.is_loaded(), "page should be loaded");
                tracing::trace!("allocate_page1(Writing done)");
                if self.wal.is_some() {
                    // Fsync page 1 to the main database file before any commit
                    // can fsync frames into the WAL. This keeps the invariant
                    // "a WAL exists ⇒ the database file has at least one page"
                    // that SQLite guarantees (`PRAGMA journal_mode=WAL` on a
                    // fresh database commits page 1 through a rollback journal
                    // first). SQLite relies on it: `pagerOpenWalIfPresent()`
                    // deletes any WAL found next to a zero-page database, so a
                    // pre-first-checkpoint crash image with a 0-byte main file
                    // would lose all its committed data if SQLite opened it.
                    let c = sqlite3_ondisk::begin_sync(
                        self.db_file.as_ref(),
                        self.syncing.clone(),
                        self.get_sync_type(),
                    )?;
                    *self.allocate_page1_state.write() = AllocatePage1State::Syncing { page };
                    io_yield_one!(c);
                }
                self.finish_allocate_page1(page)
            }
            AllocatePage1State::Syncing { page } => {
                tracing::trace!("allocate_page1(Syncing done)");
                self.finish_allocate_page1(page)
            }
            AllocatePage1State::Done => unreachable!("cannot try to allocate page 1 again"),
        }
    }

    /// Final step of [Pager::allocate_page1]: page 1 is written (and, for
    /// WAL-backed databases, fsync'd) to the main database file; publish it
    /// in the page cache and mark the database initialized.
    fn finish_allocate_page1(&self, page: PageRef) -> IOResultOr<PageRef> {
        let page_key = PageCacheKey::new(page.get().id);
        let mut cache = self.page_cache.write();
        cache.insert(page_key, page.clone()).map_err(|e| {
            LimboError::InternalError(format!("Failed to insert page 1 into cache: {e:?}"))
        })?;
        // After we wrote the header page, we may now set this None, to signify we initialized
        self.init_page_1.store(None);
        page.unpin();
        *self.allocate_page1_state.write() = AllocatePage1State::Done;
        Ok(IOResult::Done(page))
    }

    pub fn allocating_page1(&self) -> bool {
        matches!(
            *self.allocate_page1_state.read(),
            AllocatePage1State::Writing { .. } | AllocatePage1State::Syncing { .. }
        )
    }

    /// Tries to reuse a page from the freelist if available.
    /// If not, allocates a new page which increases the database size.
    ///
    /// FIXME: implement sqlite's 'nearby' parameter and use AllocMode.
    ///        SQLite's allocate_page() equivalent has a parameter 'nearby' which is a hint about the page number we want to have for the allocated page.
    ///        We should use this parameter to allocate the page in the same way as SQLite does; instead now we just either take the first available freelist page
    ///        or allocate a new page.
    #[allow(clippy::readonly_write_lock)]
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn allocate_page(&self) -> IOResultOr<PageRef> {
        // Ensure cache has room before allocating (we may spill dirty pages first)
        return_if_io!(self.ensure_cache_space());

        let header_ref = return_if_io!(HeaderRefMut::from_pager(self));
        let header = header_ref.borrow_mut();

        loop {
            let mut state = self.allocate_page_state.write();
            tracing::debug!("allocate_page(state={:?})", state);
            match &mut *state {
                AllocatePageState::Start => {
                    let old_db_size = header.database_size.get();
                    #[cfg(feature = "autovacuum")]
                    let mut new_db_size = old_db_size;
                    #[cfg(not(feature = "autovacuum"))]
                    let new_db_size = old_db_size;

                    tracing::debug!("allocate_page(database_size={})", new_db_size);
                    #[cfg(feature = "autovacuum")]
                    {
                        //  If the following conditions are met, allocate a pointer map page, add to cache and increment the database size
                        //  - autovacuum is enabled
                        //  - the last page is a pointer map page
                        if matches!(
                            AutoVacuumMode::from(self.auto_vacuum_mode.load(Ordering::SeqCst)),
                            AutoVacuumMode::Full
                        ) && is_ptrmap_page(new_db_size + 1, header.page_size.get() as usize)
                        {
                            // we will allocate a ptrmap page, so increment size
                            new_db_size += 1;
                            // Make the ptrmap allocation idempotent across
                            // spill-yield re-entries: only allocate + insert
                            // if the cache doesn't already contain it. The
                            // read-then-write pattern is safe because
                            // `allocate_page` holds the only writer for
                            // `database_size`/`freelist_trunk_page`; no
                            // concurrent caller can race in between.
                            let page_key = PageCacheKey::new(new_db_size as usize);
                            let already_present = {
                                let cache = self.page_cache.read();
                                cache.contains_key(&page_key)
                            };
                            if !already_present {
                                let page = allocate_new_page(new_db_size as i64, &self.buffer_pool);
                                self.add_dirty(&page)?;
                                self.page_cache.write().force_insert_page(page_key, page)?;
                            }
                        }
                    }

                    let first_freelist_trunk_page_id = header.freelist_trunk_page.get();
                    if first_freelist_trunk_page_id == 0 {
                        *state = AllocatePageState::AllocateNewPage {
                            current_db_size: new_db_size,
                        };
                        continue;
                    }
                    // Spill yield routes back through `Start`; the ptrmap
                    // allocation above is idempotent and `trunk_page.pin()`
                    // happens only after `Done`, so no double-pin.
                    let (trunk_page, c) =
                        return_if_io!(self.read_page(first_freelist_trunk_page_id as i64));
                    trunk_page.pin();
                    *state = AllocatePageState::SearchAvailableFreeListLeaf { trunk_page };
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                AllocatePageState::SearchAvailableFreeListLeaf { trunk_page } => {
                    turso_assert!(
                        trunk_page.is_loaded(),
                        "Freelist trunk page is not loaded",
                        { "page_id": trunk_page.get().id }
                    );
                    let page_contents = trunk_page.get_contents();
                    let next_trunk_page_id =
                        page_contents.read_u32_no_offset(FREELIST_TRUNK_OFFSET_NEXT_TRUNK_PTR);
                    let number_of_freelist_leaves =
                        page_contents.read_u32_no_offset(FREELIST_TRUNK_OFFSET_LEAF_COUNT);

                    // There are leaf pointers on this trunk page, so we can reuse one of the pages
                    // for the allocation.
                    if number_of_freelist_leaves != 0 {
                        let page_contents = trunk_page.get_contents();
                        let next_leaf_page_id =
                            page_contents.read_u32_no_offset(FREELIST_TRUNK_OFFSET_FIRST_LEAF_PTR);
                        // Pin + state-advance happen only on `Done` so a
                        // spill yield doesn't double-pin the leaf page.
                        let (leaf_page, c) =
                            return_if_io!(self.read_page(next_leaf_page_id as i64));
                        turso_assert!(
                            number_of_freelist_leaves > 0,
                            "Freelist trunk page has no leaves",
                            { "page_id": trunk_page.get().id }
                        );

                        // Pin leaf_page to prevent eviction while stored in state machine
                        // trunk_page is already pinned from previous state
                        leaf_page.pin();

                        *state = AllocatePageState::ReuseFreelistLeaf {
                            trunk_page: trunk_page.clone(),
                            leaf_page,
                            number_of_freelist_leaves,
                        };
                        if let Some(c) = c {
                            io_yield_one!(c);
                        }
                        continue;
                    }

                    // No freelist leaves on this trunk page.
                    // Reuse the trunk page itself (even if this is the last trunk).
                    // Update the database's first freelist trunk page to the next trunk page (may be 0 if there are no more trunk pages).
                    header.freelist_trunk_page = next_trunk_page_id.into();
                    header.freelist_pages = (header.freelist_pages.get() - 1).into();
                    self.add_dirty(trunk_page)?;
                    // zero out the page
                    turso_assert!(
                        trunk_page.get_contents().overflow_cells.is_empty(),
                        "Freelist trunk page has overflow cells",
                        { "page_id": trunk_page.get().id }
                    );
                    trunk_page.get_contents().as_ptr().fill(0);
                    let page_key = PageCacheKey::new(trunk_page.get().id);
                    {
                        let page_cache = self.page_cache.read();
                        turso_assert!(
                            page_cache.contains_key(&page_key),
                            "page is not in cache",
                            { "page_id": trunk_page.get().id }
                        );
                    }
                    // Unpin trunk_page before returning - caller takes ownership
                    trunk_page.unpin();
                    let trunk_page = trunk_page.clone();
                    *state = AllocatePageState::Start;
                    return Ok(IOResult::Done(trunk_page));
                }
                AllocatePageState::ReuseFreelistLeaf {
                    trunk_page,
                    leaf_page,
                    number_of_freelist_leaves,
                } => {
                    turso_assert!(
                        leaf_page.is_loaded(),
                        "Leaf page is not loaded",
                        { "page_id": leaf_page.get().id }
                    );
                    let page_contents = trunk_page.get_contents();
                    self.add_dirty(leaf_page)?;
                    // zero out the page
                    turso_assert!(
                        leaf_page.get_contents().overflow_cells.is_empty(),
                        "Freelist leaf page has overflow cells",
                        { "page_id": leaf_page.get().id }
                    );
                    leaf_page.get_contents().as_ptr().fill(0);
                    let page_key = PageCacheKey::new(leaf_page.get().id);
                    {
                        let page_cache = self.page_cache.read();
                        turso_assert!(
                            page_cache.contains_key(&page_key),
                            "page is not in cache",
                            { "page_id": leaf_page.get().id }
                        );
                    }

                    // Mark trunk page dirty BEFORE modifying it so subjournal captures original content
                    self.add_dirty(trunk_page)?;

                    // Shift left all the other leaf pages in the trunk page and subtract 1 from the leaf count
                    let remaining_leaves_count = (*number_of_freelist_leaves - 1) as usize;
                    {
                        let buf = page_contents.as_ptr();
                        // use copy within the same page
                        let offset_remaining_leaves_start =
                            FREELIST_TRUNK_OFFSET_FIRST_LEAF_PTR + FREELIST_LEAF_PTR_SIZE;
                        let offset_remaining_leaves_end = offset_remaining_leaves_start
                            + remaining_leaves_count * FREELIST_LEAF_PTR_SIZE;
                        buf.copy_within(
                            offset_remaining_leaves_start..offset_remaining_leaves_end,
                            FREELIST_TRUNK_OFFSET_FIRST_LEAF_PTR,
                        );
                    }
                    // write the new leaf count
                    page_contents.write_u32_no_offset(
                        FREELIST_TRUNK_OFFSET_LEAF_COUNT,
                        remaining_leaves_count as u32,
                    );

                    header.freelist_pages = (header.freelist_pages.get() - 1).into();
                    // Unpin both pages before returning - caller takes ownership of leaf_page
                    trunk_page.unpin();
                    leaf_page.unpin();
                    let leaf_page = leaf_page.clone();
                    *state = AllocatePageState::Start;
                    return Ok(IOResult::Done(leaf_page));
                }
                AllocatePageState::AllocateNewPage { current_db_size } => {
                    let mut new_db_size = *current_db_size + 1;

                    // if new_db_size reaches the pending page, we need to allocate a new one
                    if Some(new_db_size) == self.pending_byte_page_id() {
                        let richard_hipp_special_page =
                            allocate_new_page(new_db_size as i64, &self.buffer_pool);
                        self.add_dirty(&richard_hipp_special_page)?;
                        let page_key = PageCacheKey::new(richard_hipp_special_page.get().id);
                        self.page_cache
                            .write()
                            .force_insert_page(page_key, richard_hipp_special_page)?;
                        // HIPP special page is assumed to zeroed and should never be read or written to by the BTREE
                        new_db_size += 1;
                    }

                    // Check if allocating a new page would exceed the maximum page count
                    let max_page_count = self.get_max_page_count();
                    if new_db_size > max_page_count {
                        return Err(LimboError::DatabaseFull(
                            "database or disk is full".to_string(),
                        )
                        .into());
                    }

                    // FIXME: should reserve page cache entry before modifying the database
                    let page = allocate_new_page(new_db_size as i64, &self.buffer_pool);
                    {
                        // setup page and add to cache
                        self.add_dirty(&page)?;

                        let page_key = PageCacheKey::new(page.get().id as usize);
                        self.page_cache
                            .write()
                            .force_insert_page(page_key, page.clone())?;
                        header.database_size = new_db_size.into();
                        *state = AllocatePageState::Start;
                        return Ok(IOResult::Done(page));
                    }
                }
            }
        }
    }

    pub fn upsert_page_in_cache(
        &self,
        id: usize,
        page: PageRef,
        dirty_page_must_exist: bool,
    ) -> Result<(), LimboError> {
        let mut cache = self.page_cache.write();
        let page_key = PageCacheKey::new(id);

        // FIXME: use specific page key for writer instead of max frame, this will make readers not conflict
        if dirty_page_must_exist {
            turso_assert!(page.is_dirty(), "page must be dirty for upsert", { "page_id": id });
        }
        // The page carries writes that must stay cache-resident, so admit it
        // over capacity when nothing is evictable.
        cache
            .force_upsert_page(page_key, page.clone())
            .map_err(|e| {
                LimboError::InternalError(format!(
                    "Failed to insert loaded page {id} into cache: {e:?}"
                ))
            })?;
        page.set_loaded();
        page.clear_wal_tag();
        Ok(())
    }

    fn force_upsert_page_in_cache(&self, id: usize, page: PageRef) -> Result<(), LimboError> {
        let mut cache = self.page_cache.write();
        let page_key = PageCacheKey::new(id);

        turso_assert!(
            page.is_dirty(),
            "restored savepoint page must be dirty",
            { "page_id": id }
        );
        cache
            .force_upsert_page(page_key, page.clone())
            .map_err(|e| {
                LimboError::InternalError(format!(
                    "Failed to restore savepoint page {id} into cache: {e:?}"
                ))
            })?;
        page.set_loaded();
        page.clear_wal_tag();
        Ok(())
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn rollback(&self, schema_did_change: bool, connection: &Connection, is_write: bool) {
        tracing::debug!(schema_did_change);
        if is_write {
            let clear_dirty = true;
            // The page cache only needs to be cleared if we are rolling back a write transaction.
            // If a read transaction rolls back, and the next read transaction detects that the
            // database has changed in between (see db_changed() in wal.rs), then the page cache
            // will be cleared. Since the read transaction itself has not modified anything, it can proceed
            // with its cached pages in case the database has NOT changed in between.
            //
            // Even in the case of a write transaction, clearing the entire page cache is overkill,
            // since we only need to clear the dirty pages that were modified by the write transaction.
            self.clear_page_cache(clear_dirty);
            self.dirty_pages.write().clear();
        } else {
            turso_assert!(
                self.dirty_pages.read().is_empty(),
                "dirty pages should be empty for read txn"
            );
        }
        self.reset_internal_states();
        // Invalidate cached schema cookie since rollback may have restored the database schema cookie
        self.set_schema_cookie(None);
        if schema_did_change {
            *connection.schema.write() = connection.db.clone_schema();
        }
        if is_write {
            if let Some(wal) = self.wal.as_ref() {
                wal.rollback(None);
            }
        }
    }

    fn reset_internal_states(&self) {
        self.pending_reads.write().clear();
        *self.checkpoint_state.write() = CheckpointState::default();
        self.syncing.store(false, Ordering::SeqCst);
        self.commit_info.write().reset();
        *self.allocate_page_state.write() = AllocatePageState::Start;
        *self.free_page_state.write() = FreePageState::Start;
        *self.spill_state.write() = SpillState::Idle;
        #[cfg(feature = "autovacuum")]
        {
            let mut vacuum_state = self.vacuum_state.write();
            vacuum_state.ptrmap_get_state = PtrMapGetState::Start;
            vacuum_state.ptrmap_put_state = PtrMapPutState::Start;
            vacuum_state.btree_create_vacuum_full_state = BtreeCreateVacuumFullState::Start;
        }

        *self.header_ref_state.write() = HeaderRefState::Start;
    }

    pub fn with_header<T>(&self, f: impl Fn(&DatabaseHeader) -> T) -> IOResultOr<T> {
        let header_ref = return_if_io!(HeaderRef::from_pager(self));
        let header = header_ref.borrow();
        // Update cached schema cookie when reading header
        self.set_schema_cookie(Some(header.schema_cookie.get()));
        Ok(IOResult::Done(f(header)))
    }

    pub fn with_header_mut<T>(&self, f: impl Fn(&mut DatabaseHeader) -> T) -> IOResultOr<T> {
        let header_ref = return_if_io!(HeaderRefMut::from_pager(self));
        let header = header_ref.borrow_mut();
        let result = f(header);
        // Update cached schema cookie after modification
        self.set_schema_cookie(Some(header.schema_cookie.get()));
        Ok(IOResult::Done(result))
    }

    pub fn is_encryption_ctx_set(&self) -> bool {
        self.io_ctx.read().encryption_context().is_some()
    }

    pub(crate) fn has_external_page_codec(&self) -> bool {
        self.io_ctx.read().has_external_page_codec()
    }

    pub(crate) fn page_codec_external(&self) -> Option<Arc<dyn PageCodec>> {
        self.io_ctx.read().page_codec_external()
    }

    pub fn is_encryption_enabled(&self) -> bool {
        self.enable_encryption.load(Ordering::SeqCst)
    }

    pub fn set_encryption_context(
        &self,
        cipher_mode: CipherMode,
        key: &EncryptionKey,
    ) -> Result<()> {
        // we will set the encryption context only if the encryption is opted-in.
        if !self.enable_encryption.load(Ordering::SeqCst) {
            return Err(LimboError::InvalidArgument(
                "encryption is an opt in feature. enable it via passing `--experimental-encryption`"
                    .into(),
            ));
        }
        if self.has_external_page_codec() {
            return Err(LimboError::InvalidArgument(
                "cannot configure built-in encryption while an external page codec is installed"
                    .into(),
            ));
        }

        let page_size = self.get_page_size_unchecked().get() as usize;
        let encryption_ctx = EncryptionContext::new(cipher_mode, key, page_size)?;
        {
            let mut io_ctx = self.io_ctx.write();
            io_ctx.set_encryption(encryption_ctx);
        }
        let Some(wal) = self.wal.as_ref() else {
            return Ok(());
        };
        wal.set_io_context(self.io_ctx.read().clone());
        // whenever we set the encryption context, lets reset the page cache. The page cache
        // might have been loaded with page 1 to initialise the connection. During initialisation,
        // we only read the header which is unencrypted, but the rest of the page is. If so, lets
        // clear the cache.
        self.clear_page_cache(false);
        // Also invalidate cached schema cookie to force re-read of page 1 with encryption
        self.set_schema_cookie(None);
        Ok(())
    }

    pub(crate) fn set_page_codec(&self, codec: Arc<dyn PageCodec>) -> Result<()> {
        if self.is_encryption_ctx_set() {
            return Err(LimboError::InvalidArgument(
                "cannot install an external page codec while built-in encryption is configured"
                    .into(),
            ));
        }
        let required_reserved_space = codec.required_reserved_bytes();
        if let Some(reserved_space) = self.get_reserved_space() {
            if reserved_space != required_reserved_space {
                return Err(LimboError::InvalidArgument(format!(
                    "page codec requires exactly {required_reserved_space} reserved bytes, but database provides {reserved_space}"
                )));
            }
        } else {
            if let Some(page_size) = self.get_page_size() {
                if !page_size.has_valid_reserved_space(required_reserved_space) {
                    return Err(LimboError::InvalidArgument(format!(
                        "page codec requires {} reserved bytes, which leaves less than {} usable bytes for page size {}",
                        required_reserved_space,
                        PageSize::MIN_USABLE_SPACE,
                        page_size.get()
                    )));
                }
            }
            self.set_reserved_space(required_reserved_space);
        }
        {
            let mut io_ctx = self.io_ctx.write();
            io_ctx.set_page_codec(codec);
        }
        if let Some(wal) = self.wal.as_ref() {
            let io_ctx = self.io_ctx.read().clone();
            wal.set_io_context(io_ctx);
        }
        self.clear_page_cache(false);
        self.set_schema_cookie(None);
        Ok(())
    }

    pub fn reset_checksum_context(&self) {
        {
            let mut io_ctx = self.io_ctx.write();
            io_ctx.reset_checksum();
        }
        let Some(wal) = self.wal.as_ref() else { return };
        wal.set_io_context(self.io_ctx.read().clone())
    }

    pub fn set_reserved_space_bytes(&self, value: u8) {
        self.set_reserved_space(value);
    }

    /// Encryption is an opt-in feature. If the flag is passed, then enable the encryption on
    /// pager, which is then used to set it on the IOContext.
    pub fn enable_encryption(&self, enable: bool) {
        self.enable_encryption.store(enable, Ordering::SeqCst);
    }
}

pub fn allocate_new_page(page_id: i64, buffer_pool: &Arc<BufferPool>) -> PageRef {
    let page = Arc::new(Page::new(page_id));
    {
        let buffer = buffer_pool.get_page();
        let inner = page.get();
        inner.set_buffer(Arc::new(buffer));
        page.set_loaded();
        page.clear_wal_tag();
    }
    page
}

pub fn default_page1(cipher: Option<&CipherMode>) -> PageRef {
    // New Database header for empty Database
    let mut default_header = DatabaseHeader::default();

    if let Some(cipher) = cipher {
        // we will set the reserved space bytes as required by either the encryption
        let reserved_space_bytes = cipher.metadata_size() as u8;
        default_header.reserved_space = reserved_space_bytes;
    }

    let page = Arc::new(Page::new(DatabaseHeader::PAGE_ID as i64));

    {
        let inner = page.get();
        inner.set_buffer(Arc::new(Buffer::new_temporary(
            default_header.page_size.get() as usize,
        )));
    }

    page.get_contents().write_database_header(&default_header);
    page.set_loaded();
    page.clear_wal_tag();

    btree_init_page(
        &page,
        PageType::TableLeaf,
        DatabaseHeader::SIZE, // offset of 100 bytes
        (default_header.page_size.get() - default_header.reserved_space as u32) as usize,
    );

    page
}

#[derive(Debug, Clone, Copy)]
pub struct CreateBTreeFlags(pub u8);
impl CreateBTreeFlags {
    pub const TABLE: u8 = 0b0001;
    pub const INDEX: u8 = 0b0010;

    pub fn new_table() -> Self {
        Self(CreateBTreeFlags::TABLE)
    }

    pub fn new_index() -> Self {
        Self(CreateBTreeFlags::INDEX)
    }

    pub fn is_table(&self) -> bool {
        (self.0 & CreateBTreeFlags::TABLE) != 0
    }

    pub fn is_index(&self) -> bool {
        (self.0 & CreateBTreeFlags::INDEX) != 0
    }

    pub fn get_flags(&self) -> u8 {
        self.0
    }
}

/*
** The pointer map is a lookup table that identifies the parent page for
** each child page in the database file.  The parent page is the page that
** contains a pointer to the child.  Every page in the database contains
** 0 or 1 parent pages. Each pointer map entry consists of a single byte 'type'
** and a 4 byte parent page number.
**
** The PTRMAP_XXX identifiers below are the valid types.
**
** The purpose of the pointer map is to facilitate moving pages from one
** position in the file to another as part of autovacuum.  When a page
** is moved, the pointer in its parent must be updated to point to the
** new location.  The pointer map is used to locate the parent page quickly.
**
** PTRMAP_ROOTPAGE: The database page is a root-page. The page-number is not
**                  used in this case.
**
** PTRMAP_FREEPAGE: The database page is an unused (free) page. The page-number
**                  is not used in this case.
**
** PTRMAP_OVERFLOW1: The database page is the first page in a list of
**                   overflow pages. The page number identifies the page that
**                   contains the cell with a pointer to this overflow page.
**
** PTRMAP_OVERFLOW2: The database page is the second or later page in a list of
**                   overflow pages. The page-number identifies the previous
**                   page in the overflow page list.
**
** PTRMAP_BTREE: The database page is a non-root btree page. The page number
**               identifies the parent page in the btree.
*/
#[cfg(feature = "autovacuum")]
pub(crate) mod ptrmap {
    #[allow(unused_imports)]
    use crate::{storage::sqlite3_ondisk::PageSize, LimboError, Result};
    use crate::{turso_assert_greater_than_or_equal, turso_soft_unreachable};

    // Constants
    pub const PTRMAP_ENTRY_SIZE: usize = 5;
    /// Page 1 is the schema page which contains the database header.
    /// Page 2 is the first pointer map page if the database has any pointer map pages.
    pub const FIRST_PTRMAP_PAGE_NO: u32 = 2;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(u8)]
    pub enum PtrmapType {
        RootPage = 1,
        FreePage = 2,
        Overflow1 = 3,
        Overflow2 = 4,
        BTreeNode = 5,
    }

    impl PtrmapType {
        pub fn from_u8(value: u8) -> Option<Self> {
            match value {
                1 => Some(PtrmapType::RootPage),
                2 => Some(PtrmapType::FreePage),
                3 => Some(PtrmapType::Overflow1),
                4 => Some(PtrmapType::Overflow2),
                5 => Some(PtrmapType::BTreeNode),
                _ => None,
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    pub struct PtrmapEntry {
        pub entry_type: PtrmapType,
        pub parent_page_no: u32,
    }

    impl PtrmapEntry {
        pub fn serialize(&self, buffer: &mut [u8]) -> Result<()> {
            if buffer.len() < PTRMAP_ENTRY_SIZE {
                return Err(LimboError::InternalError(format!(
                    "Buffer too small to serialize ptrmap entry. Expected at least {} bytes, got {}",
                    PTRMAP_ENTRY_SIZE,
                    buffer.len()
                )));
            }
            buffer[0] = self.entry_type as u8;
            buffer[1..5].copy_from_slice(&self.parent_page_no.to_be_bytes());
            Ok(())
        }

        pub fn deserialize(buffer: &[u8]) -> Option<Self> {
            if buffer.len() < PTRMAP_ENTRY_SIZE {
                return None;
            }
            let entry_type_u8 = buffer[0];
            let parent_bytes_slice = buffer.get(1..5)?;
            let parent_page_no = u32::from_be_bytes(parent_bytes_slice.try_into().ok()?);
            PtrmapType::from_u8(entry_type_u8).map(|entry_type| PtrmapEntry {
                entry_type,
                parent_page_no,
            })
        }
    }

    /// Calculates how many database pages are mapped by a single pointer map page.
    /// This is based on the total page size, as ptrmap pages are filled with entries.
    pub fn entries_per_ptrmap_page(page_size: usize) -> usize {
        turso_assert_greater_than_or_equal!(page_size, PageSize::MIN as usize);
        page_size / PTRMAP_ENTRY_SIZE
    }

    /// Calculates the cycle length of pointer map pages
    /// The cycle length is the number of database pages that are mapped by a single pointer map page.
    pub fn ptrmap_page_cycle_length(page_size: usize) -> usize {
        turso_assert_greater_than_or_equal!(page_size, PageSize::MIN as usize);
        (page_size / PTRMAP_ENTRY_SIZE) + 1
    }

    /// Determines if a given page number `db_page_no` (1-indexed) is a pointer map page in a database with autovacuum enabled
    pub fn is_ptrmap_page(db_page_no: u32, page_size: usize) -> bool {
        //  The first page cannot be a ptrmap page because its for the schema
        if db_page_no == 1 {
            return false;
        }
        if db_page_no == FIRST_PTRMAP_PAGE_NO {
            return true;
        }
        get_ptrmap_page_no_for_db_page(db_page_no, page_size) == db_page_no
    }

    /// Calculates which pointer map page (1-indexed) contains the entry for `db_page_no_to_query` (1-indexed).
    /// `db_page_no_to_query` is the page whose ptrmap entry we are interested in.
    pub fn get_ptrmap_page_no_for_db_page(db_page_no_to_query: u32, page_size: usize) -> u32 {
        let group_size = ptrmap_page_cycle_length(page_size) as u32;
        if group_size == 0 {
            panic!("Page size too small, a ptrmap page cannot map any db pages.");
        }

        let effective_page_index = db_page_no_to_query - FIRST_PTRMAP_PAGE_NO;
        let group_idx = effective_page_index / group_size;

        (group_idx * group_size) + FIRST_PTRMAP_PAGE_NO
    }

    /// Calculates the byte offset of the entry for `db_page_no_to_query` (1-indexed)
    /// within its pointer map page (`ptrmap_page_no`, 1-indexed).
    pub fn get_ptrmap_offset_in_page(
        db_page_no_to_query: u32,
        ptrmap_page_no: u32,
        page_size: usize,
    ) -> Result<usize> {
        // The data pages mapped by `ptrmap_page_no` are:
        // `ptrmap_page_no + 1`, `ptrmap_page_no + 2`, ..., up to `ptrmap_page_no + n_data_pages_per_group`.
        // `db_page_no_to_query` must be one of these.
        // The 0-indexed position of `db_page_no_to_query` within this sequence of data pages is:
        // `db_page_no_to_query - (ptrmap_page_no + 1)`.

        let n_data_pages_per_group = entries_per_ptrmap_page(page_size);
        let first_data_page_mapped = ptrmap_page_no + 1;
        let last_data_page_mapped = ptrmap_page_no + n_data_pages_per_group as u32;

        if db_page_no_to_query < first_data_page_mapped
            || db_page_no_to_query > last_data_page_mapped
        {
            turso_soft_unreachable!("Page is not mapped by ptrmap data range", { "page": db_page_no_to_query, "range_start": first_data_page_mapped, "range_end": last_data_page_mapped, "ptrmap_page": ptrmap_page_no });
            return Err(LimboError::InternalError(format!(
                "Page {db_page_no_to_query} is not mapped by the data page range [{first_data_page_mapped}, {last_data_page_mapped}] of ptrmap page {ptrmap_page_no}"
            )));
        }
        if is_ptrmap_page(db_page_no_to_query, page_size) {
            turso_soft_unreachable!("Page is a pointer map page and should not have an entry calculated this way", { "page": db_page_no_to_query });
            return Err(LimboError::InternalError(format!(
                "Page {db_page_no_to_query} is a pointer map page and should not have an entry calculated this way."
            )));
        }

        let entry_index_on_page = (db_page_no_to_query - first_data_page_mapped) as usize;
        Ok(entry_index_on_page * PTRMAP_ENTRY_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use crate::sync::Arc;

    use crate::sync::RwLock;

    use crate::io::{MemoryIO, OpenFlags, IO};
    use crate::storage::buffer_pool::BufferPool;
    use crate::storage::database::DatabaseFile;
    use crate::storage::page_cache::{PageCache, PageCacheKey};
    use crate::storage::wal::{Wal, WalFile, WalFileShared};
    use crate::util::IOExt;
    use arc_swap::ArcSwapOption;

    use super::{default_page1, CacheFlushState, CollectingState, Page, PageRef, Pager};
    use crate::{Buffer, Completion, CompletionError, LimboError};

    fn pager_with_cache_capacity(cache_capacity: usize, database_pages: u32) -> Arc<Pager> {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let buffer_pool = BufferPool::begin_init(&io, 4096 * 128);

        let db_file = Arc::new(DatabaseFile::new(
            io.open_file(":memory:", OpenFlags::Create, false).unwrap(),
        ));

        let wal_file = io.open_file("test.wal", OpenFlags::Create, false).unwrap();
        let wal_shared = WalFileShared::new_shared(wal_file).unwrap();
        let last_checksum_and_max_frame = wal_shared.read().last_checksum_and_max_frame();
        let wal: Arc<dyn Wal> = Arc::new(WalFile::new(
            io.clone(),
            wal_shared,
            last_checksum_and_max_frame,
            buffer_pool.clone(),
        ));

        let init_page_1 = Arc::new(ArcSwapOption::new(Some(default_page1(None))));
        let pager = Arc::new(
            Pager::new(
                db_file,
                Some(wal),
                io,
                PageCache::new(cache_capacity),
                buffer_pool,
                Arc::new(crate::sync::Mutex::new(())),
                init_page_1,
            )
            .unwrap(),
        );

        pager.io.step().unwrap();
        pager.io.block(|| pager.allocate_page1()).unwrap();
        for _ in 0..(database_pages - 1) {
            pager.io.block(|| pager.allocate_page()).unwrap();
        }
        pager
    }

    /// The page cache capacity is a soft limit, as in SQLite: when every
    /// resident page is unevictable (held by cursors, dirty and unspillable),
    /// a read must still succeed by admitting the page over capacity instead
    /// of failing with Busy. The excess drains once pages become evictable.
    #[test]
    fn read_page_exceeds_capacity_when_cache_unevictable() {
        const CAP: usize = 5;
        let pager = pager_with_cache_capacity(CAP, 6);

        // Allocating 6 pages against a 5-page cache forces a spill and evicts
        // at least one spilled page; find one that is no longer resident.
        let missing = (2..=6)
            .find(|&id| !pager.page_cache.read().contains_key(&PageCacheKey::new(id)))
            .expect("allocating 6 pages with a 5-page cache must evict at least one page")
            as i64;

        // Hold strong references to every resident page so none can be
        // evicted or spilled.
        let held: Vec<PageRef> = (1..=6)
            .filter_map(|id| pager.cache_get(id).unwrap())
            .collect();
        assert_eq!(held.len(), CAP, "cache should be at capacity");

        let (page, c) = pager.io.block(|| pager.read_page(missing)).unwrap();
        if let Some(c) = c {
            pager.io.wait_for_completion(c).unwrap();
        }
        while page.is_locked() {
            pager.io.step().unwrap();
        }
        assert_eq!(page.get().id as i64, missing);
        assert!(
            pager.page_cache.read().len() > CAP,
            "page must have been admitted over capacity"
        );

        // Once the strong references are gone, the next insert drains the
        // excess back under capacity.
        drop(held);
        drop(page);
        pager.io.block(|| pager.allocate_page()).unwrap();
        assert!(
            pager.page_cache.read().len() <= CAP,
            "excess over capacity must drain once pages become evictable"
        );
    }

    /// Same soft-limit guarantee for the write path: allocating a new page
    /// while the cache is full of unevictable pages must not fail.
    #[test]
    fn allocate_page_exceeds_capacity_when_cache_unevictable() {
        const CAP: usize = 5;
        let pager = pager_with_cache_capacity(CAP, 5);

        // Hold strong references to all resident pages: dirty pages with
        // outstanding references can neither be spilled nor evicted.
        let held: Vec<PageRef> = (1..=5)
            .filter_map(|id| pager.cache_get(id).unwrap())
            .collect();
        assert_eq!(held.len(), CAP, "cache should be at capacity");

        let page = pager.io.block(|| pager.allocate_page()).unwrap();
        assert_eq!(page.get().id, 6);
        assert!(
            pager.page_cache.read().len() > CAP,
            "page must have been admitted over capacity"
        );
    }

    /// Verifies that cacheflush returns a codec error when rereading an evicted page fails,
    /// and resets its state so a later flush can retry.
    #[test]
    fn cacheflush_propagates_failed_page_codec_reread() {
        let pager = pager_with_cache_capacity(5, 2);
        let page = Arc::new(Page::new(2));
        page.set_loaded();
        let completion =
            Completion::new_read(Arc::new(Buffer::new_temporary(4096)), Box::new(|_| None));
        completion.error(CompletionError::PageCodecError { page_idx: 2 });
        *pager.cacheflush_state.write() = CacheFlushState::WaitingForRead {
            state: CollectingState::default(),
            page_id: 2,
            page,
            completion,
        };

        let err = pager.cacheflush().unwrap_err();
        assert!(matches!(
            *err,
            LimboError::CompletionError(CompletionError::PageCodecError { page_idx: 2 })
        ));
        assert!(matches!(
            *pager.cacheflush_state.read(),
            CacheFlushState::Init
        ));
    }

    #[test]
    fn test_shared_cache() {
        // ensure cache can be shared between threads
        let cache = Arc::new(RwLock::new(PageCache::new(10)));

        let thread = {
            let cache = cache.clone();
            std::thread::spawn(move || {
                let mut cache = cache.write();
                let page_key = PageCacheKey::new(1);
                let page = Page::new(1);
                // Set loaded so that we avoid eviction, as we evict the page from cache if it is not locked and not loaded
                page.set_loaded();
                cache.insert(page_key, Arc::new(page)).unwrap();
            })
        };
        let _ = thread.join();
        let mut cache = cache.write();
        let page_key = PageCacheKey::new(1);
        let page = cache.get(&page_key).unwrap();
        assert_eq!(page.unwrap().get().id, 1);
    }
}

#[cfg(test)]
#[cfg(feature = "autovacuum")]
mod ptrmap_tests {
    use crate::sync::Arc;

    use super::ptrmap::*;
    use super::*;
    use crate::io::{MemoryIO, OpenFlags, IO};
    use crate::storage::buffer_pool::BufferPool;
    use crate::storage::database::DatabaseFile;
    use crate::storage::page_cache::PageCache;
    use crate::storage::pager::{default_page1, Pager};
    use crate::storage::sqlite3_ondisk::PageSize;
    use crate::storage::wal::{WalFile, WalFileShared};
    use arc_swap::ArcSwapOption;

    pub fn run_until_done<T>(
        mut action: impl FnMut() -> IOResultOr<T>,
        pager: &Pager,
    ) -> Result<T> {
        loop {
            match action()? {
                IOResult::Done(res) => {
                    return Ok(res);
                }
                IOResult::IO(io) => io.wait(pager.io.as_ref())?,
            }
        }
    }
    // Helper to create a Pager for testing
    fn test_pager_setup(page_size: u32, initial_db_pages: u32) -> Pager {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db_file: Arc<dyn DatabaseStorage> = Arc::new(DatabaseFile::new(
            io.open_file("test.db", OpenFlags::Create, true).unwrap(),
        ));

        //  Construct interfaces for the pager
        let pages = initial_db_pages + 10;
        let sz = std::cmp::max(std::cmp::min(pages, 64), pages);
        let buffer_pool = BufferPool::begin_init(&io, (sz * page_size) as usize);

        let wal_shared = WalFileShared::new_shared(
            io.open_file("test.db-wal", OpenFlags::Create, false)
                .unwrap(),
        )
        .unwrap();
        let last_checksum_and_max_frame = wal_shared.read().last_checksum_and_max_frame();
        let wal: Arc<dyn Wal> = Arc::new(WalFile::new(
            io.clone(),
            wal_shared,
            last_checksum_and_max_frame,
            buffer_pool.clone(),
        ));

        // For new empty databases, init_page_1 must be Some(page) so allocate_page1() can be called
        let init_page_1 = Arc::new(ArcSwapOption::new(Some(default_page1(None))));
        let pager = Pager::new(
            db_file,
            Some(wal),
            io,
            PageCache::new(sz as usize),
            buffer_pool,
            Arc::new(Mutex::new(())),
            init_page_1,
        )
        .unwrap();
        run_until_done(|| pager.allocate_page1(), &pager).unwrap();
        {
            let page_cache = pager.page_cache.read();
            println!(
                "Cache Len: {} Cap: {}",
                page_cache.len(),
                page_cache.capacity()
            );
        }
        pager
            .persist_auto_vacuum_mode(AutoVacuumMode::Full)
            .unwrap();

        //  Allocate all the pages as btree root pages
        const EXPECTED_FIRST_ROOT_PAGE_ID: u32 = 3; // page1 = 1,  first ptrmap page = 2, root page = 3
        for i in 0..initial_db_pages {
            let res = run_until_done(
                || pager.btree_create(&CreateBTreeFlags::new_table()),
                &pager,
            );
            {
                let page_cache = pager.page_cache.read();
                println!(
                    "i: {} Cache Len: {} Cap: {}",
                    i,
                    page_cache.len(),
                    page_cache.capacity()
                );
            }
            match res {
                Ok(root_page_id) => {
                    assert_eq!(root_page_id, EXPECTED_FIRST_ROOT_PAGE_ID + i);
                }
                Err(e) => {
                    panic!("test_pager_setup: btree_create failed: {e:?}");
                }
            }
        }

        pager
    }

    #[test]
    fn persist_auto_vacuum_mode_updates_fresh_header_without_dirty_pages() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db_file: Arc<dyn DatabaseStorage> = Arc::new(DatabaseFile::new(
            io.open_file("fresh-auto-vacuum.db", OpenFlags::Create, true)
                .unwrap(),
        ));
        let buffer_pool = BufferPool::begin_init(&io, 65536);
        let pager = Pager::new(
            db_file,
            None,
            io,
            PageCache::new(4),
            buffer_pool,
            Arc::new(Mutex::new(())),
            Arc::new(ArcSwapOption::new(Some(default_page1(None)))),
        )
        .unwrap();

        pager
            .persist_auto_vacuum_mode(AutoVacuumMode::Incremental)
            .unwrap();

        let IOResult::Done((largest_root_page, incremental_vacuum_enabled)) = pager
            .with_header(|header| {
                (
                    header.vacuum_mode_largest_root_page.get(),
                    header.incremental_vacuum_enabled.get(),
                )
            })
            .unwrap()
        else {
            panic!("fresh database header reads should not do any IO");
        };

        assert_eq!(largest_root_page, 1);
        assert_eq!(incremental_vacuum_enabled, 1);
        assert_eq!(pager.get_auto_vacuum_mode(), AutoVacuumMode::Incremental);
        assert!(
            pager.dirty_pages.read().is_empty(),
            "fresh-db auto-vacuum setup must not leave dirty pages behind"
        );
    }

    #[test]
    fn test_ptrmap_page_allocation() {
        let page_size = 4096;
        let initial_db_pages = 10;
        let pager = test_pager_setup(page_size, initial_db_pages);

        // Page 5 should be mapped by ptrmap page 2.
        let db_page_to_update: u32 = 5;
        let expected_ptrmap_pg_no =
            get_ptrmap_page_no_for_db_page(db_page_to_update, page_size as usize);
        assert_eq!(expected_ptrmap_pg_no, FIRST_PTRMAP_PAGE_NO);

        //  Ensure the pointer map page ref is created and loadable via the pager
        let ptrmap_page_ref = pager
            .io
            .block(|| pager.read_page(expected_ptrmap_pg_no as i64));
        assert!(ptrmap_page_ref.is_ok());

        //  Ensure that the database header size is correctly reflected
        assert_eq!(
            pager
                .io
                .block(|| pager.with_header(|header| header.database_size))
                .unwrap()
                .get(),
            initial_db_pages + 2
        ); // (1+1) -> (header + ptrmap)

        //  Read the entry from the ptrmap page and verify it
        let entry = pager
            .io
            .block(|| pager.ptrmap_get(db_page_to_update))
            .unwrap()
            .unwrap();
        assert_eq!(entry.entry_type, PtrmapType::RootPage);
        assert_eq!(entry.parent_page_no, 0);
    }

    #[test]
    fn test_is_ptrmap_page_logic() {
        let page_size = PageSize::MIN as usize;
        let n_data_pages = entries_per_ptrmap_page(page_size);
        assert_eq!(n_data_pages, 102); //   512/5 = 102

        assert!(!is_ptrmap_page(1, page_size)); // Header
        assert!(is_ptrmap_page(2, page_size)); // P0
        assert!(!is_ptrmap_page(3, page_size)); // D0_1
        assert!(!is_ptrmap_page(4, page_size)); // D0_2
        assert!(!is_ptrmap_page(5, page_size)); // D0_3
        assert!(is_ptrmap_page(105, page_size)); // P1
        assert!(!is_ptrmap_page(106, page_size)); // D1_1
        assert!(!is_ptrmap_page(107, page_size)); // D1_2
        assert!(!is_ptrmap_page(108, page_size)); // D1_3
        assert!(is_ptrmap_page(208, page_size)); // P2
    }

    #[test]
    fn test_get_ptrmap_page_no() {
        let page_size = PageSize::MIN as usize; // Maps 103 data pages

        // Test pages mapped by P0 (page 2)
        assert_eq!(get_ptrmap_page_no_for_db_page(3, page_size), 2); // D(3) -> P0(2)
        assert_eq!(get_ptrmap_page_no_for_db_page(4, page_size), 2); // D(4) -> P0(2)
        assert_eq!(get_ptrmap_page_no_for_db_page(5, page_size), 2); // D(5) -> P0(2)
        assert_eq!(get_ptrmap_page_no_for_db_page(104, page_size), 2); // D(104) -> P0(2)

        assert_eq!(get_ptrmap_page_no_for_db_page(105, page_size), 105); // Page 105 is a pointer map page.

        // Test pages mapped by P1 (page 6)
        assert_eq!(get_ptrmap_page_no_for_db_page(106, page_size), 105); // D(106) -> P1(105)
        assert_eq!(get_ptrmap_page_no_for_db_page(107, page_size), 105); // D(107) -> P1(105)
        assert_eq!(get_ptrmap_page_no_for_db_page(108, page_size), 105); // D(108) -> P1(105)

        assert_eq!(get_ptrmap_page_no_for_db_page(208, page_size), 208); // Page 208 is a pointer map page.
    }

    #[test]
    fn test_get_ptrmap_offset() {
        let page_size = PageSize::MIN as usize; //  Maps 103 data pages

        assert_eq!(get_ptrmap_offset_in_page(3, 2, page_size).unwrap(), 0);
        assert_eq!(
            get_ptrmap_offset_in_page(4, 2, page_size).unwrap(),
            PTRMAP_ENTRY_SIZE
        );
        assert_eq!(
            get_ptrmap_offset_in_page(5, 2, page_size).unwrap(),
            2 * PTRMAP_ENTRY_SIZE
        );

        //  P1 (page 105) maps D(106)...D(207)
        // D(106) is index 0 on P1. Offset 0.
        // D(107) is index 1 on P1. Offset 5.
        // D(108) is index 2 on P1. Offset 10.
        assert_eq!(get_ptrmap_offset_in_page(106, 105, page_size).unwrap(), 0);
        assert_eq!(
            get_ptrmap_offset_in_page(107, 105, page_size).unwrap(),
            PTRMAP_ENTRY_SIZE
        );
        assert_eq!(
            get_ptrmap_offset_in_page(108, 105, page_size).unwrap(),
            2 * PTRMAP_ENTRY_SIZE
        );
    }

    /// Cache-hit fast path: `read_page_nonblock` must return `Done` with no
    /// disk-read completion and must not touch `pending_reads`.
    #[test]
    fn read_page_nonblock_cache_hit_returns_done() {
        let pager = test_pager_setup(4096, 10);

        // Page 1 is unconditionally loaded into cache by `allocate_page1`.
        let res = pager.read_page(1).unwrap();
        match res {
            IOResult::Done((page, c)) => {
                assert_eq!(page.get().id, 1);
                assert!(
                    c.is_none(),
                    "cache hit must not return a disk-read completion"
                );
            }
            IOResult::IO(_) => panic!("cache hit should not yield"),
        }
        assert!(
            pager.pending_reads.read().is_empty(),
            "pending_reads must stay empty on cache-hit path"
        );
    }

    /// Re-entry contract: if `pending_reads` already has a `PendingRead` for
    /// this page (as happens after a previous call yielded for spill), the
    /// next call must reuse that `(page, disk_read)` instead of allocating a
    /// new page and issuing a duplicate disk read.
    ///
    /// This test does NOT force a real spill yield — that requires an IO
    /// backend that returns non-finished completions, which we don't have at
    /// the core unit-test layer. We instead synthesize the post-yield state
    /// directly and assert the function honors it.
    #[test]
    fn read_page_nonblock_reentry_reuses_pending_entry() {
        let pager = test_pager_setup(4096, 10);

        // Pick a page id well beyond the initialized DB so it is *not* in
        // the cache. We never actually issue IO against it (we short-circuit
        // via the pre-populated `pending_reads` entry), so the page id only
        // needs to be unique within the cache.
        let target_idx: i64 = 9999;
        assert!(
            pager.cache_get(target_idx as usize).unwrap().is_none(),
            "test precondition: target page must not be in cache"
        );

        // Synthesize the state that would exist after a previous call had to
        // yield on spill: a `PendingRead` entry whose `page` is the
        // PageRef we already handed back to the caller, and whose
        // `disk_read` is the in-flight disk-read completion.
        let synthetic_page: PageRef = Arc::new(Page::new(target_idx));
        // Mark loaded so cache eviction logic treats it as a normal page; the
        // contents don't matter for this test.
        synthetic_page.set_loaded();
        let stub_disk_read = Completion::new_yield();
        pager.pending_reads.write().insert(
            target_idx,
            PendingRead {
                page: synthetic_page.clone(),
                disk_read: Some(stub_disk_read),
            },
        );

        let res = pager.read_page(target_idx).unwrap();
        let (page, c) = match res {
            IOResult::Done(v) => v,
            IOResult::IO(_) => panic!(
                "with pending entry present and cache space available, \
                 read_page_nonblock should complete without yielding"
            ),
        };

        assert!(
            Arc::ptr_eq(&page, &synthetic_page),
            "read_page_nonblock must reuse the PageRef from pending_reads, \
             not allocate a new page (this is the no-duplicate-IO invariant)"
        );
        assert!(
            c.is_some(),
            "the disk-read completion from pending_reads should be returned"
        );
        assert!(
            pager.pending_reads.read().get(&target_idx).is_none(),
            "pending_reads entry must be cleared once read_page_nonblock returns Done"
        );
    }

    /// Concurrency contract: a page can be cache-resident while its disk read
    /// is still in flight (locked, not loaded) — `read_page` inserts into the
    /// shared cache before the read completes, and `PageCache::get` hands out
    /// such in-flight pages. A second reader hitting the cache-hit fast path
    /// must NOT receive that unloaded page with `None` (no completion to wait
    /// on); it must yield and re-enter until the read completes. Otherwise the
    /// caller reads a torn / uninitialized buffer, or races a writer filling
    /// the buffer underneath it.
    #[test]
    fn read_page_nonblock_inflight_cache_hit_yields_not_done() {
        let pager = test_pager_setup(4096, 10);

        let target_idx: i64 = 9999;
        assert!(
            pager.cache_get(target_idx as usize).unwrap().is_none(),
            "test precondition: target page must not be in cache"
        );

        // Synthesize an in-flight read that has already been published to the
        // shared cache: locked (a read is outstanding) but not loaded (the
        // buffer hasn't been filled yet). This is exactly the state a page is
        // in between `cache_insert` and the disk-read completion firing.
        let inflight: PageRef = Arc::new(Page::new(target_idx));
        inflight.set_locked();
        assert!(!inflight.is_loaded());
        pager
            .page_cache
            .write()
            .insert(PageCacheKey::new(target_idx as usize), inflight.clone())
            .unwrap();

        // The fast path finds the page in cache but must refuse to return it
        // without a completion, because it is not yet loaded.
        match pager.read_page(target_idx).unwrap() {
            IOResult::IO(_) => {}
            IOResult::Done((page, c)) => panic!(
                "read_page handed out an in-flight (locked, unloaded) page on the \
                 cache-hit fast path: loaded={}, completion={}",
                page.is_loaded(),
                c.is_some()
            ),
        }

        // Once the read completes (page becomes loaded), the same cache-hit
        // fast path returns Done with no completion, as before.
        inflight.set_loaded();
        match pager.read_page(target_idx).unwrap() {
            IOResult::Done((page, c)) => {
                assert!(Arc::ptr_eq(&page, &inflight));
                assert!(c.is_none(), "loaded cache hit must not return a completion");
            }
            IOResult::IO(_) => panic!("loaded cache hit must not yield"),
        }
    }
}

#[cfg(all(test, feature = "fs", host_shared_wal))]
mod checkpoint_phase_tests {
    use super::*;
    use crate::io::{PlatformIO, IO};
    use crate::storage::sqlite3_ondisk::DatabaseHeader;
    use crate::storage::wal::CheckpointMode;
    use crate::sync::atomic::Ordering;
    use crate::types::IOResult;
    use crate::Database;
    use crate::SqliteDialect;

    /// Returns an IO backend that supports shared WAL coordination on the host.
    /// On Windows the default `PlatformIO` (`WindowsIO`) lacks the byte-locking
    /// and mapping primitives, so the experimental IOCP backend is used when
    /// the `experimental_win_iocp` feature is enabled.
    fn shared_wal_test_io() -> Arc<dyn IO> {
        #[cfg(all(target_os = "windows", feature = "experimental_win_iocp"))]
        {
            Arc::new(crate::WindowsIOCP::new().unwrap())
        }
        #[cfg(not(all(target_os = "windows", feature = "experimental_win_iocp")))]
        {
            Arc::new(PlatformIO::new().unwrap())
        }
    }

    /// The returned `TempDir` deletes the database directory when it drops, so
    /// callers must hold it for as long as they use the database.
    fn open_checkpoint_test_database() -> (Arc<Database>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        {
            let connection = rusqlite::Connection::open(&db_path).unwrap();
            connection
                .pragma_update(None, "journal_mode", "wal")
                .unwrap();
        }
        let io = shared_wal_test_io();
        let db = Database::open_file_with_flags(
            io,
            db_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new().with_multiprocess_wal(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        (db, dir)
    }

    fn db_identity(db_path: &std::path::Path) -> (u32, u32) {
        let bytes = std::fs::read(db_path).unwrap();
        assert!(bytes.len() >= DatabaseHeader::SIZE);
        let db_size_pages = u32::from_be_bytes(bytes[28..32].try_into().unwrap());
        let crc = crc32c::crc32c(&bytes[..DatabaseHeader::SIZE]);
        (db_size_pages, crc)
    }

    #[test]
    fn checkpoint_db_sync_completion_still_leaves_backfill_unpublished_until_proof_install() {
        let (db, dir) = open_checkpoint_test_database();
        let db_path = dir.path().join("test.db");
        let conn = db.connect().unwrap();
        conn.wal_auto_actions_disable();
        conn.execute("create table test(id integer primary key, value blob)")
            .unwrap();
        conn.execute("begin immediate").unwrap();
        for _ in 0..32 {
            conn.execute("insert into test(value) values (randomblob(2048))")
                .unwrap();
        }
        conn.execute("commit").unwrap();
        assert!(
            db.shared_wal
                .read()
                .metadata
                .max_frame
                .load(Ordering::SeqCst)
                > 1,
            "checkpoint setup requires more than one WAL frame"
        );

        let pager = conn.pager.load();
        let mode = CheckpointMode::Passive {
            upper_bound_inclusive: Some(1),
        };

        loop {
            match pager.checkpoint(mode, crate::SyncMode::Full, true).unwrap() {
                IOResult::Done(_) => {
                    panic!("checkpoint should not finish before we observe the post-sync gap")
                }
                IOResult::IO(io) => io.wait(pager.io.as_ref()).unwrap(),
            }

            let state = pager.checkpoint_state.read();
            let Some(result) = state.result.as_ref() else {
                continue;
            };
            if matches!(state.phase, CheckpointPhase::ReadDbIdentity { .. })
                && result.db_sync_sent
                && !pager.syncing.load(Ordering::SeqCst)
            {
                break;
            }
        }

        let authority = db.shared_wal_coordination().unwrap().unwrap();
        let snapshot_before_publish = authority.snapshot();
        let (db_size_pages, db_header_crc32c) = db_identity(&db_path);
        assert_eq!(
            snapshot_before_publish.nbackfills, 0,
            "DB sync completion alone must not publish positive nbackfills"
        );
        assert!(
            !authority.validate_backfill_proof(
                snapshot_before_publish,
                db_size_pages,
                db_header_crc32c
            ),
            "DB sync completion must still leave the durable backfill proof absent"
        );

        let result = pager
            .io
            .block(|| pager.checkpoint(mode, crate::SyncMode::Full, true))
            .unwrap();
        assert!(
            result.wal_total_backfilled > 0 && !result.everything_backfilled(),
            "resumed checkpoint should complete the partial checkpoint after proof installation"
        );

        let snapshot_after_publish = authority.snapshot();
        let (db_size_pages_after, db_header_crc32c_after) = db_identity(&db_path);
        assert!(
            snapshot_after_publish.nbackfills > 0,
            "proof installation step must publish positive nbackfills"
        );
        assert!(
            authority.validate_backfill_proof(
                snapshot_after_publish,
                db_size_pages_after,
                db_header_crc32c_after
            ),
            "resuming after the post-sync gap must install a valid durable backfill proof"
        );
    }
}
