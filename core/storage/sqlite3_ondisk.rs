//! SQLite on-disk file format.
//!
//! SQLite stores data in a single database file, which is divided into fixed-size
//! pages:
//!
//! ```text
//! +----------+----------+----------+-----------------------------+----------+
//! |          |          |          |                             |          |
//! |  Page 1  |  Page 2  |  Page 3  |           ...               |  Page N  |
//! |          |          |          |                             |          |
//! +----------+----------+----------+-----------------------------+----------+
//! ```
//!
//! The first page is special because it contains a 100 byte header at the beginning.
//!
//! Each page consists of a page header and N cells, which contain the records.
//!
//! ```text
//! +-----------------+----------------+---------------------+----------------+
//! |                 |                |                     |                |
//! |   Page header   |  Cell pointer  |     Unallocated     |  Cell content  |
//! | (8 or 12 bytes) |     array      |        space        |      area      |
//! |                 |                |                     |                |
//! +-----------------+----------------+---------------------+----------------+
//! ```
//!
//! The write-ahead log (WAL) is a separate file that contains the physical
//! log of changes to a database file. The file starts with a WAL header and
//! is followed by a sequence of WAL frames, which are database pages with
//! additional metadata.
//!
//! ```text
//! +-----------------+-----------------+-----------------+-----------------+
//! |                 |                 |                 |                 |
//! |    WAL header   |    WAL frame 1  |    WAL frame 2  |    WAL frame N  |
//! |                 |                 |                 |                 |
//! +-----------------+-----------------+-----------------+-----------------+
//! ```
//!
//! For more information, see the SQLite file format specification:
//!
//! https://www.sqlite.org/fileformat.html

#![allow(clippy::arc_with_non_send_sync)]

use crate::types::IOResultOr;
use crate::{
    io_yield_one, turso_assert, turso_assert_eq, turso_assert_greater_than,
    types::{IOCompletions, IOResult},
    util::IOExt as _,
};
use branches::{mark_unlikely, unlikely};
use bytemuck::{Pod, Zeroable};
use pack1::{I32BE, U16BE, U32BE};
use tracing::{instrument, Level};

use super::pager::PageRef;
pub use super::pager::{PageContent, PageInner};
use super::wal::{OverflowFallbackCoverage, TursoRwLock, WalSharedMetadata, WalSharedRuntime};
use crate::error::LimboError;
use crate::fast_lock::SpinLock;
use crate::io::{Buffer, Completion, CompletionGroup, FileSyncType, ReadComplete};
use crate::numeric::Numeric;
use crate::storage::btree::{payload_overflow_threshold_max, payload_overflow_threshold_min};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::database::DatabaseStorage;
use crate::storage::page_transform::{
    page_codec_completion_error, PageCodecContext, PageLocation, PageTransform,
};
use crate::storage::pager::Pager;
use crate::storage::wal::READMARK_NOT_USED;
use crate::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use crate::sync::Arc;
use crate::sync::RwLock;
use crate::types::{SerialType, SerialTypeKind, TextRef, TextSubtype, ValueRef};
use crate::{bail_corrupt_error, CompletionError, File, IOContext, Result, WalFileShared};
use rustc_hash::FxHashMap;
use std::collections::BTreeMap;
use std::pin::Pin;

/// The minimum size of a cell in bytes.
pub const MINIMUM_CELL_SIZE: usize = 4;

pub const CELL_PTR_SIZE_BYTES: usize = 2;
pub const INTERIOR_PAGE_HEADER_SIZE_BYTES: usize = 12;
pub const LEAF_PAGE_HEADER_SIZE_BYTES: usize = 8;
pub const LEFT_CHILD_PTR_SIZE_BYTES: usize = 4;

// Freelist trunk page layout:
// - Bytes 0-3: Page number of next freelist trunk page (0 if none)
// - Bytes 4-7: Number of leaf page pointers on this trunk page
// - Bytes 8+: Array of 4-byte leaf page pointers
pub const FREELIST_TRUNK_OFFSET_NEXT_TRUNK_PTR: usize = 0;
pub const FREELIST_TRUNK_OFFSET_LEAF_COUNT: usize = 4;
pub const FREELIST_TRUNK_OFFSET_FIRST_LEAF_PTR: usize = 8;
pub const FREELIST_TRUNK_HEADER_SIZE: usize = 8;
pub const FREELIST_LEAF_PTR_SIZE: usize = 4;

#[derive(PartialEq, Eq, Zeroable, Pod, Clone, Copy, Debug)]
#[repr(transparent)]
/// Read/Write file format version.
pub struct PageSize(U16BE);

impl PageSize {
    pub const MIN: u32 = 512;
    pub const MAX: u32 = 65536;
    pub const DEFAULT: u16 = 4096;
    pub(crate) const MIN_USABLE_SPACE: u32 = 480;

    /// Interpret a user-provided u32 as either a valid page size or None.
    pub const fn new(size: u32) -> Option<Self> {
        if size < PageSize::MIN || size > PageSize::MAX {
            return None;
        }

        // Page size must be a power of two.
        if size.count_ones() != 1 {
            return None;
        }

        if size == PageSize::MAX {
            // Internally, the value 1 represents 65536, since the on-disk value of the page size in the DB header is 2 bytes.
            return Some(Self(U16BE::new(1)));
        }

        Some(Self(U16BE::new(size as u16)))
    }

    /// Interpret a u16 on disk (DB file header) as either a valid page size or
    /// return a corrupt error.
    pub fn new_from_header_u16(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Self(U16BE::new(1))),
            n => {
                let Some(size) = Self::new(n as u32) else {
                    bail_corrupt_error!("invalid page size in database header: {n}");
                };

                Ok(size)
            }
        }
    }

    pub const fn get(self) -> u32 {
        match self.0.get() {
            1 => Self::MAX,
            v => v as u32,
        }
    }

    pub(crate) const fn has_valid_reserved_space(self, reserved_space: u8) -> bool {
        self.get().saturating_sub(reserved_space as u32) >= Self::MIN_USABLE_SPACE
    }

    /// Get the raw u16 value stored internally
    pub const fn get_raw(self) -> u16 {
        self.0.get()
    }
}

impl Default for PageSize {
    fn default() -> Self {
        Self(U16BE::new(Self::DEFAULT))
    }
}

#[derive(PartialEq, Eq, Zeroable, Pod, Clone, Copy, Debug)]
#[repr(transparent)]
/// Read/Write file format version.
pub struct CacheSize(I32BE);

impl CacheSize {
    // The negative value means that we store the amount of pages a XKiB of memory can hold.
    // We can calculate "real" cache size by diving by page size.
    pub const DEFAULT: i32 = -2000;

    // Minimum number of pages that cache can hold.
    pub const MIN: i64 = super::page_cache::MINIMUM_PAGE_CACHE_SIZE_IN_PAGES as i64;

    // SQLite uses this value as threshold for maximum cache size
    pub const MAX_SAFE: i64 = 2147450880;

    pub const fn new(size: i32) -> Self {
        match size {
            Self::DEFAULT => Self(I32BE::new(0)),
            v => Self(I32BE::new(v)),
        }
    }

    pub const fn get(self) -> i32 {
        match self.0.get() {
            0 => Self::DEFAULT,
            v => v,
        }
    }
}

impl Default for CacheSize {
    fn default() -> Self {
        Self(I32BE::new(Self::DEFAULT))
    }
}

/// Read/Write file format version.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
#[repr(u8)]
pub enum Version {
    Legacy = 1,
    Wal = 2,
    Mvcc = 255,
}

impl Version {
    #[inline]
    pub fn wal(&self) -> bool {
        matches!(self, Self::Wal)
    }

    #[inline]
    pub fn mvcc(&self) -> bool {
        matches!(self, Self::Mvcc)
    }

    #[inline]
    pub fn legacy(&self) -> bool {
        matches!(self, Self::Legacy)
    }
}

impl TryFrom<u8> for Version {
    type Error = u8;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            1 => Ok(Version::Legacy),
            2 => Ok(Version::Wal),
            255 => Ok(Version::Mvcc),
            v => Err(v),
        }
    }
}

/// Raw version byte for use in DatabaseHeader where Pod is required.
/// Use `Version::try_from(raw.0)` to convert to the validated enum.
#[derive(PartialEq, Eq, Zeroable, Pod, Clone, Copy)]
#[repr(transparent)]
pub struct RawVersion(pub u8);

impl RawVersion {
    pub fn to_version(self) -> std::result::Result<Version, u8> {
        Version::try_from(self.0)
    }
}

impl From<Version> for RawVersion {
    fn from(v: Version) -> Self {
        Self(v as u8)
    }
}

impl std::fmt::Debug for RawVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.to_version() {
            Ok(v) => write!(f, "{v:?}"),
            Err(v) => write!(f, "RawVersion::Invalid({v})"),
        }
    }
}

#[derive(PartialEq, Eq, Zeroable, Pod, Clone, Copy)]
#[repr(transparent)]
/// Text encoding.
pub struct TextEncoding(U32BE);

impl TextEncoding {
    #![allow(non_upper_case_globals)]
    // SQLite doesn't write the text encoding bytes until the first table is written, so when
    // opening an empty SQLite file, the encoding bytes will be 0. SQLite considers this to mean UTF-8.
    pub const Unset: Self = Self(U32BE::new(0));
    pub const Utf8: Self = Self(U32BE::new(1));
    pub const Utf16Le: Self = Self(U32BE::new(2));
    pub const Utf16Be: Self = Self(U32BE::new(3));

    pub fn is_utf8(&self) -> bool {
        self == &Self::Utf8 || self == &Self::Unset
    }
}

impl std::fmt::Display for TextEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Utf8 => f.write_str("UTF-8"),
            Self::Utf16Le => f.write_str("UTF-16le"),
            Self::Utf16Be => f.write_str("UTF-16be"),
            Self(v) => write!(f, "TextEncoding::Invalid({})", v.get()),
        }
    }
}

impl std::fmt::Debug for TextEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Utf8 => f.write_str("TextEncoding::Utf8"),
            Self::Utf16Le => f.write_str("TextEncoding::Utf16Le"),
            Self::Utf16Be => f.write_str("TextEncoding::Utf16Be"),
            Self(v) => write!(f, "TextEncoding::Invalid({})", v.get()),
        }
    }
}

impl Default for TextEncoding {
    fn default() -> Self {
        Self::Utf8
    }
}

#[derive(Pod, Zeroable, Clone, Copy, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
#[repr(C, packed)]
/// Database Header Format
pub struct DatabaseHeader {
    /// b"SQLite format 3\0"
    pub magic: [u8; 16],
    /// Page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
    pub page_size: PageSize,
    /// File format write version. 1 for legacy; 2 for WAL.
    pub write_version: RawVersion,
    /// File format read version. 1 for legacy; 2 for WAL.
    pub read_version: RawVersion,
    /// Bytes of unused "reserved" space at the end of each page. Usually 0.
    pub reserved_space: u8,
    /// Maximum embedded payload fraction. Must be 64.
    pub max_embed_frac: u8,
    /// Minimum embedded payload fraction. Must be 32.
    pub min_embed_frac: u8,
    /// Leaf payload fraction. Must be 32.
    pub leaf_frac: u8,
    /// File change counter.
    pub change_counter: U32BE,
    /// Size of the database file in pages. The "in-header database size".
    pub database_size: U32BE,
    /// Page number of the first freelist trunk page.
    pub freelist_trunk_page: U32BE,
    /// Total number of freelist pages.
    pub freelist_pages: U32BE,
    /// The schema cookie.
    pub schema_cookie: U32BE,
    /// The schema format number. Supported schema formats are 1, 2, 3, and 4.
    pub schema_format: U32BE,
    /// Default page cache size.
    pub default_page_cache_size: CacheSize,
    /// The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
    pub vacuum_mode_largest_root_page: U32BE,
    /// Text encoding.
    pub text_encoding: TextEncoding,
    /// The "user version" as read and set by the user_version pragma.
    pub user_version: I32BE,
    /// True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
    pub incremental_vacuum_enabled: U32BE,
    /// The "Application ID" set by PRAGMA application_id.
    pub application_id: I32BE,
    /// Reserved for expansion. Must be zero.
    _padding: [u8; 20],
    /// The version-valid-for number.
    pub version_valid_for: U32BE,
    /// SQLITE_VERSION_NUMBER
    pub version_number: U32BE,
}

impl DatabaseHeader {
    pub const PAGE_ID: usize = 1;
    pub const SIZE: usize = size_of::<Self>();

    const _CHECK: () = {
        assert!(Self::SIZE == 100);
    };

    pub fn usable_space(self) -> usize {
        (self.page_size.get() as usize) - (self.reserved_space as usize)
    }
}

impl Default for DatabaseHeader {
    fn default() -> Self {
        Self {
            magic: *b"SQLite format 3\0",
            page_size: Default::default(),
            write_version: RawVersion::from(Version::Wal),
            read_version: RawVersion::from(Version::Wal),
            reserved_space: 0,
            max_embed_frac: 64,
            min_embed_frac: 32,
            leaf_frac: 32,
            change_counter: U32BE::new(1),
            database_size: U32BE::new(0),
            freelist_trunk_page: U32BE::new(0),
            freelist_pages: U32BE::new(0),
            schema_cookie: U32BE::new(0),
            schema_format: U32BE::new(4), // latest format, new sqlite3 databases use this format
            default_page_cache_size: Default::default(),
            vacuum_mode_largest_root_page: U32BE::new(0),
            text_encoding: TextEncoding::Utf8,
            user_version: I32BE::new(0),
            incremental_vacuum_enabled: U32BE::new(0),
            application_id: I32BE::new(0),
            _padding: [0; 20],
            version_valid_for: U32BE::new(3047000),
            version_number: U32BE::new(3047000),
        }
    }
}

pub const WAL_HEADER_SIZE: usize = 32;
pub const WAL_FRAME_HEADER_SIZE: usize = 24;
// magic is a single number represented as WAL_MAGIC_LE but the big endian
// counterpart is just the same number with LSB set to 1.
pub const WAL_MAGIC_LE: u32 = 0x377f0682;
pub const WAL_MAGIC_BE: u32 = 0x377f0683;

/// The Write-Ahead Log (WAL) header.
/// The first 32 bytes of a WAL file comprise the WAL header.
/// The WAL header is divided into the following fields stored in big-endian order.
#[derive(Debug, Clone, Copy)]
#[repr(C)] // This helps with encoding because rust does not respect the order in structs, so in
           // this case we want to keep the order
pub struct WalHeader {
    /// Magic number. 0x377f0682 or 0x377f0683
    /// If the LSB is 0, checksums are native byte order, else checksums are serialized
    pub magic: u32,

    /// WAL format version. Currently 3007000
    pub file_format: u32,

    /// Database page size in bytes. Power of two between 512 and 65536 inclusive
    pub page_size: u32,

    /// Checkpoint sequence number. Increases with each checkpoint
    pub checkpoint_seq: u32,

    /// Random value used for the first salt in checksum calculations
    /// TODO: Incremented with each checkpoint
    pub salt_1: u32,

    /// Random value used for the second salt in checksum calculations.
    /// TODO: A different random value for each checkpoint
    pub salt_2: u32,

    /// First checksum value in the wal-header
    pub checksum_1: u32,

    /// Second checksum value in the wal-header
    pub checksum_2: u32,
}

impl WalHeader {
    pub const fn new() -> Self {
        let magic = if cfg!(target_endian = "big") {
            WAL_MAGIC_BE
        } else {
            WAL_MAGIC_LE
        };
        WalHeader {
            magic,
            file_format: 3007000,
            page_size: 0, // Signifies WAL header that is not persistent on disk yet.
            checkpoint_seq: 0, // TODO implement sequence number
            salt_1: 0,
            salt_2: 0,
            checksum_1: 0,
            checksum_2: 0,
        }
    }
}

impl Default for WalHeader {
    fn default() -> Self {
        Self::new()
    }
}

/// Immediately following the wal-header are zero or more frames.
/// Each frame consists of a 24-byte frame-header followed by <page-size> bytes of page data.
/// The frame-header is six big-endian 32-bit unsigned integer values, as follows:
#[allow(dead_code)]
#[derive(Debug, Default, Copy, Clone)]
pub struct WalFrameHeader {
    /// Page number
    pub(crate) page_number: u32,

    /// For commit records, the size of the database file in pages after the commit.
    /// For all other records, zero.
    pub(crate) db_size: u32,

    /// Salt-1 copied from the WAL header
    pub(crate) salt_1: u32,

    /// Salt-2 copied from the WAL header
    pub(crate) salt_2: u32,

    /// Checksum-1: Cumulative checksum up through and including this page
    pub(crate) checksum_1: u32,

    /// Checksum-2: Second half of the cumulative checksum
    pub(crate) checksum_2: u32,
}

impl WalFrameHeader {
    pub fn is_commit_frame(&self) -> bool {
        self.db_size > 0
    }
}

#[repr(u8)]
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum PageType {
    IndexInterior = 2,
    TableInterior = 5,
    IndexLeaf = 10,
    TableLeaf = 13,
}

impl PageType {
    pub fn is_table(&self) -> bool {
        match self {
            PageType::IndexInterior | PageType::IndexLeaf => false,
            PageType::TableInterior | PageType::TableLeaf => true,
        }
    }
}

impl TryFrom<u8> for PageType {
    type Error = LimboError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            2 => Ok(Self::IndexInterior),
            5 => Ok(Self::TableInterior),
            10 => Ok(Self::IndexLeaf),
            13 => Ok(Self::TableLeaf),
            _ => {
                mark_unlikely();
                Err(LimboError::Corrupt(format!("Invalid page type: {value}")))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct OverflowCell {
    pub index: usize,
    pub payload: Pin<crate::alloc::Vec<u8>>,
}

/// Send read request for DB page read to the IO
/// if allow_empty_read is set, than empty read will be raise error for the page, but will not panic
#[instrument(skip_all, level = Level::DEBUG)]
/// Reads one page from the database file. The read is added to `group`,
/// when given, before it is submitted.
pub fn begin_read_page(
    db_file: &dyn DatabaseStorage,
    buffer_pool: Arc<BufferPool>,
    page: PageRef,
    page_idx: usize,
    allow_empty_read: bool,
    io_ctx: &IOContext,
    group: Option<&mut CompletionGroup>,
) -> Result<Completion> {
    tracing::trace!("begin_read_btree_page(page_idx = {})", page_idx);
    let buf = buffer_pool.get_page();
    #[allow(clippy::arc_with_non_send_sync)]
    let buf = Arc::new(buf);
    let complete = Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
        let Ok((buf, bytes_read)) = res else {
            page.clear_locked();
            return None; // IO error already captured in completion
        };
        let buf_len = buf.len();
        // Handle truncated database files: if we read fewer bytes than expected
        // (and it's not an intentional empty read), return a ShortRead error.
        if bytes_read == 0 {
            if !allow_empty_read {
                tracing::error!("short read on page {page_idx}: expected {buf_len} bytes, got 0");
                page.clear_locked();
                return Some(CompletionError::ShortRead {
                    page_idx,
                    expected: buf_len,
                    actual: 0,
                });
            }
        } else if bytes_read != buf_len as i32 {
            tracing::error!(
                "short read on page {page_idx}: expected {buf_len} bytes, got {bytes_read}"
            );
            page.clear_locked();
            return Some(CompletionError::ShortRead {
                page_idx,
                expected: buf_len,
                actual: bytes_read as usize,
            });
        }
        let page = page.clone();
        let buffer = if bytes_read == 0 {
            Arc::new(Buffer::new_temporary(0))
        } else {
            buf
        };
        finish_read_page(page_idx, buffer, page);
        None
    });
    let c = Completion::new_read(buf, complete);
    if let Some(group) = group {
        group.add(&c);
    }
    db_file.read_page(page_idx, io_ctx, c)
}

#[instrument(skip_all, level = Level::DEBUG)]
pub fn finish_read_page(page_idx: usize, buffer: Arc<Buffer>, page: PageRef) {
    tracing::trace!("finish_read_page(page_idx = {page_idx})");
    {
        let inner = page.get();
        inner.set_buffer(buffer);
        page.clear_locked();
        page.set_loaded();
        // we set the wal tag only when reading page from log, or in allocate_page,
        // we clear it here for safety in case page is being re-loaded.
        page.clear_wal_tag();
    }
}

#[instrument(skip_all, level = Level::DEBUG)]
/// Writes one page to the database file. The write is added to `group`,
/// when given, before it is submitted.
pub fn begin_write_btree_page(
    pager: &Pager,
    page: &PageRef,
    group: Option<&mut CompletionGroup>,
) -> Result<Completion> {
    tracing::trace!("begin_write_btree_page(page={})", page.get().id);
    let page_source = &pager.db_file;
    let page_finish = page.clone();

    let page_id = page.get().id;
    tracing::trace!("begin_write_btree_page(page_id={})", page_id);

    let buffer = page.get().buffer().cloned().expect("buffer not loaded");
    let buf_len = buffer.len();

    let write_complete = {
        Box::new(move |res: Result<i32, CompletionError>| {
            let Ok(bytes_written) = res else {
                return;
            };
            tracing::trace!("finish_write_btree_page");

            page_finish.clear_dirty();
            turso_assert!(
                bytes_written == buf_len as i32,
                "wrote({bytes_written}) != expected({buf_len})"
            );
        })
    };
    let c = Completion::new_write(write_complete);
    if let Some(group) = group {
        group.add(&c);
    }
    let io_ctx = pager.io_ctx.read();
    page_source.write_page(page_id, buffer, &io_ctx, c)
}

#[instrument(skip_all, level = Level::DEBUG)]
/// Write a batch of pages to the database file.
///
/// we have a batch of pages to write, lets say the following:
/// (they are already sorted by id thanks to BTreeMap)
/// [1,2,3,6,7,9,10,11,12]
//
/// we want to collect this into runs of:
/// [1,2,3], [6,7], [9,10,11,12]
/// and submit each run as a `writev` call,
/// for 3 total syscalls instead of 9.
///
/// Each run's write is added to `group` before it is submitted. Returns
/// the number of runs submitted.
pub fn write_pages_vectored(
    pager: &Pager,
    batch: BTreeMap<usize, Arc<Buffer>>,
    done_flag: Arc<AtomicBool>,
    err: Arc<crate::sync::OnceLock<CompletionError>>,
    group: &mut CompletionGroup,
) -> Result<usize> {
    if batch.is_empty() {
        done_flag.store(true, Ordering::Release);
        return Ok(0);
    }

    let page_sz = pager.get_page_size_unchecked().get() as usize;

    let mut run_count = 0;
    let mut prev_id = None;
    for &id in batch.keys() {
        if let Some(prev) = prev_id {
            if id != prev + 1 {
                run_count += 1;
            }
        } else {
            run_count = 1;
        }
        prev_id = Some(id);
    }

    let runs_left = Arc::new(AtomicUsize::new(run_count));

    const EST_BUFF_CAPACITY: usize = 32;
    let mut run_bufs = Vec::with_capacity(EST_BUFF_CAPACITY);
    let mut run_start_id: Option<usize> = None;
    let mut completions = Vec::with_capacity(run_count);

    let mut iter = batch.iter().peekable();
    while let Some((id, buffer)) = iter.next() {
        if run_start_id.is_none() {
            run_start_id = Some(*id);
        }
        run_bufs.push(buffer.clone());

        let is_end_of_run = iter.peek().is_none_or(|(next_id, _)| **next_id != id + 1);
        if !is_end_of_run {
            continue;
        }

        let start_id = run_start_id.take().expect("start id");
        let runs_left_cl = runs_left.clone();
        let done_cl = done_flag.clone();
        let err_cl = err.clone();

        let expected_bytes = (page_sz * run_bufs.len()) as i32;

        let cmp = Completion::new_write(move |res| {
            // Record error/mismatch, but always resolve the batch progress.
            match res {
                Ok(n) => {
                    if n != expected_bytes {
                        let _ = err_cl.set(CompletionError::ShortWrite);
                        tracing::error!(
                            "write_pages_vectored: short write: wrote({n}) != expected({expected_bytes})"
                        );
                    }
                }
                Err(e) => {
                    tracing::error!("write_pages_vectored: write error: {:?}", e);
                    let _ = err_cl.set(e);
                }
            }
            // we have to decrement runs_left on both paths
            if runs_left_cl.fetch_sub(1, Ordering::AcqRel) == 1 {
                tracing::debug!("write_pages_vectored: run complete");
                done_cl.store(true, Ordering::Release);
            }
        });
        group.add(&cmp);
        let io_ctx = pager.io_ctx.read();
        let bufs = std::mem::replace(&mut run_bufs, Vec::with_capacity(EST_BUFF_CAPACITY));
        match pager
            .db_file
            .write_pages(start_id, page_sz, bufs, &io_ctx, cmp)
        {
            Ok(c) => completions.push(c),
            Err(e) => {
                // We failed to submit this run at all. Mark batch failed+done and cancel already-submitted.
                let _ = err.set(CompletionError::Aborted);
                done_flag.store(true, Ordering::Release);
                pager.io.cancel(&completions)?;
                pager.io.drain_completions(&completions)?;
                return Err(e);
            }
        }
    }
    Ok(completions.len())
}

#[instrument(skip_all, level = Level::DEBUG)]
pub fn begin_sync(
    db_file: &dyn DatabaseStorage,
    syncing: Arc<AtomicBool>,
    sync_type: FileSyncType,
) -> Result<Completion> {
    turso_assert!(!syncing.load(Ordering::SeqCst));
    syncing.store(true, Ordering::SeqCst);
    let completion = Completion::new_sync({
        let syncing = syncing.clone();
        move |_| {
            syncing.store(false, Ordering::SeqCst);
        }
    });
    #[allow(clippy::arc_with_non_send_sync)]
    db_file.sync(completion, sync_type).inspect_err(|_| {
        syncing.store(false, Ordering::SeqCst);
    })
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone)]
pub enum BTreeCell {
    TableInteriorCell(TableInteriorCell),
    TableLeafCell(TableLeafCell),
    IndexInteriorCell(IndexInteriorCell),
    IndexLeafCell(IndexLeafCell),
}

#[derive(Debug, Clone)]
pub struct TableInteriorCell {
    pub left_child_page: u32,
    pub rowid: i64,
}

#[derive(Debug, Clone)]
pub struct TableLeafCell {
    pub rowid: i64,
    /// Payload of cell, if it overflows it won't include overflowed payload.
    pub payload: &'static [u8],
    /// This is the complete payload size including overflow pages.
    pub payload_size: u64,
    pub first_overflow_page: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct IndexInteriorCell {
    pub left_child_page: u32,
    pub payload: &'static [u8],
    /// This is the complete payload size including overflow pages.
    pub payload_size: u64,
    pub first_overflow_page: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct IndexLeafCell {
    pub payload: &'static [u8],
    /// This is the complete payload size including overflow pages.
    pub payload_size: u64,
    pub first_overflow_page: Option<u32>,
}

/// read_btree_cell contructs a BTreeCell which is basically a wrapper around pointer to the payload of a cell.
/// buffer input "page" is static because we want the cell to point to the data in the page in case it has any payload.
pub fn read_btree_cell(
    page: &'static [u8],
    page_content: &PageContent,
    pos: usize,
    usable_size: usize,
) -> Result<BTreeCell> {
    let page_type = page_content.page_type()?;
    let max_local = payload_overflow_threshold_max(page_type, usable_size);
    let min_local = payload_overflow_threshold_min(page_type, usable_size);
    match page_type {
        PageType::IndexInterior => {
            let mut pos = pos;
            crate::assert_or_bail_corrupt!(
                pos + 4 <= page.len(),
                "cell offset {} out of bounds for page size {}",
                pos,
                page.len()
            );
            let left_child_page =
                u32::from_be_bytes([page[pos], page[pos + 1], page[pos + 2], page[pos + 3]]);
            pos += 4;
            let (payload_size, nr) = read_varint(crate::slice_in_bounds_or_corrupt!(page, pos..))?;
            pos += nr;

            let (overflows, to_read) =
                payload_overflows(payload_size as usize, max_local, min_local, usable_size);
            let to_read = if overflows { to_read } else { page.len() - pos };

            crate::assert_or_bail_corrupt!(
                pos + to_read <= page.len(),
                "payload range {}..{} out of bounds for page size {}",
                pos,
                pos + to_read,
                page.len()
            );
            let (payload, first_overflow_page) =
                read_payload(&page[pos..pos + to_read], payload_size as usize)?;
            Ok(BTreeCell::IndexInteriorCell(IndexInteriorCell {
                left_child_page,
                payload,
                first_overflow_page,
                payload_size,
            }))
        }
        PageType::TableInterior => {
            let mut pos = pos;
            crate::assert_or_bail_corrupt!(
                pos + 4 <= page.len(),
                "cell offset {} out of bounds for page size {}",
                pos,
                page.len()
            );
            let left_child_page =
                u32::from_be_bytes([page[pos], page[pos + 1], page[pos + 2], page[pos + 3]]);
            pos += 4;
            let (rowid, _) = read_varint(crate::slice_in_bounds_or_corrupt!(page, pos..))?;
            Ok(BTreeCell::TableInteriorCell(TableInteriorCell {
                left_child_page,
                rowid: rowid as i64,
            }))
        }
        PageType::IndexLeaf => {
            let mut pos = pos;
            let (payload_size, nr) = read_varint(crate::slice_in_bounds_or_corrupt!(page, pos..))?;
            pos += nr;

            let (overflows, to_read) =
                payload_overflows(payload_size as usize, max_local, min_local, usable_size);
            let to_read = if overflows { to_read } else { page.len() - pos };

            crate::assert_or_bail_corrupt!(
                pos + to_read <= page.len(),
                "payload range {}..{} out of bounds for page size {}",
                pos,
                pos + to_read,
                page.len()
            );
            let (payload, first_overflow_page) =
                read_payload(&page[pos..pos + to_read], payload_size as usize)?;
            Ok(BTreeCell::IndexLeafCell(IndexLeafCell {
                payload,
                first_overflow_page,
                payload_size,
            }))
        }
        PageType::TableLeaf => {
            let mut pos = pos;
            let (payload_size, nr) = read_varint(crate::slice_in_bounds_or_corrupt!(page, pos..))?;
            pos += nr;
            let (rowid, nr) = read_varint(crate::slice_in_bounds_or_corrupt!(page, pos..))?;
            pos += nr;

            let (overflows, to_read) =
                payload_overflows(payload_size as usize, max_local, min_local, usable_size);
            let to_read = if overflows { to_read } else { page.len() - pos };

            crate::assert_or_bail_corrupt!(
                pos + to_read <= page.len(),
                "payload range {}..{} out of bounds for page size {}",
                pos,
                pos + to_read,
                page.len()
            );
            let (payload, first_overflow_page) =
                read_payload(&page[pos..pos + to_read], payload_size as usize)?;
            Ok(BTreeCell::TableLeafCell(TableLeafCell {
                rowid: rowid as i64,
                payload,
                first_overflow_page,
                payload_size,
            }))
        }
    }
}

/// read_payload takes in the unread bytearray with the payload size
/// and returns the payload on the page, and optionally the first overflow page number.
#[allow(clippy::readonly_write_lock)]
fn read_payload(
    unread: &'static [u8],
    payload_size: usize,
) -> Result<(&'static [u8], Option<u32>)> {
    let cell_len = unread.len();
    // We will let overflow be constructed back if needed or requested.
    if payload_size <= cell_len {
        // fit within 1 page
        Ok((&unread[..payload_size], None))
    } else {
        // overflow
        if cell_len < 4 {
            bail_corrupt_error!(
                "overflow cell too small: {} bytes, need at least 4",
                cell_len
            );
        }
        let first_overflow_page = u32::from_be_bytes([
            unread[cell_len - 4],
            unread[cell_len - 3],
            unread[cell_len - 2],
            unread[cell_len - 1],
        ]);
        Ok((&unread[..cell_len - 4], Some(first_overflow_page)))
    }
}

#[inline(always)]
#[allow(dead_code)]
pub fn validate_serial_type(value: u64) -> Result<()> {
    if !SerialType::u64_is_valid_serial_type(value) {
        crate::bail_corrupt_error!("Invalid serial type: {}", value);
    }
    Ok(())
}

/// Reads a value that might reference the buffer it is reading from. Be sure to store RefValue with the buffer
/// always.
#[inline(always)]
pub fn read_value<'a>(buf: &'a [u8], serial_type: SerialType) -> Result<(ValueRef<'a>, usize)> {
    match serial_type.kind() {
        SerialTypeKind::Null => Ok((ValueRef::Null, 0)),
        SerialTypeKind::I8 => {
            let val = *buf.first().ok_or_else(|| {
                mark_unlikely();
                LimboError::Corrupt("Invalid UInt8 value".into())
            })?;
            Ok((ValueRef::Numeric(Numeric::Integer(val as i8 as i64)), 1))
        }
        SerialTypeKind::I16 => {
            let bytes: &[u8; 2] =
                buf.get(..2)
                    .and_then(|s| s.try_into().ok())
                    .ok_or_else(|| {
                        mark_unlikely();
                        LimboError::Corrupt("Invalid BEInt16 value".into())
                    })?;
            Ok((
                ValueRef::Numeric(Numeric::Integer(i16::from_be_bytes(*bytes) as i64)),
                2,
            ))
        }
        SerialTypeKind::I24 => {
            let bytes: &[u8; 3] =
                buf.get(..3)
                    .and_then(|s| s.try_into().ok())
                    .ok_or_else(|| {
                        mark_unlikely();
                        LimboError::Corrupt("Invalid BEInt24 value".into())
                    })?;
            let sign_extension = (bytes[0] as i8 >> 7) as u8;
            Ok((
                ValueRef::Numeric(Numeric::Integer(i32::from_be_bytes([
                    sign_extension,
                    bytes[0],
                    bytes[1],
                    bytes[2],
                ]) as i64)),
                3,
            ))
        }
        SerialTypeKind::I32 => {
            let bytes: &[u8; 4] =
                buf.get(..4)
                    .and_then(|s| s.try_into().ok())
                    .ok_or_else(|| {
                        mark_unlikely();
                        LimboError::Corrupt("Invalid BEInt32 value".into())
                    })?;
            Ok((
                ValueRef::Numeric(Numeric::Integer(i32::from_be_bytes(*bytes) as i64)),
                4,
            ))
        }
        SerialTypeKind::I48 => {
            let bytes: &[u8; 6] =
                buf.get(..6)
                    .and_then(|s| s.try_into().ok())
                    .ok_or_else(|| {
                        mark_unlikely();
                        LimboError::Corrupt("Invalid BEInt48 value".into())
                    })?;
            let sign_extension = (bytes[0] as i8 >> 7) as u8;
            Ok((
                ValueRef::Numeric(Numeric::Integer(i64::from_be_bytes([
                    sign_extension,
                    sign_extension,
                    bytes[0],
                    bytes[1],
                    bytes[2],
                    bytes[3],
                    bytes[4],
                    bytes[5],
                ]))),
                6,
            ))
        }
        SerialTypeKind::I64 => {
            let bytes: &[u8; 8] =
                buf.get(..8)
                    .and_then(|s| s.try_into().ok())
                    .ok_or_else(|| {
                        mark_unlikely();
                        LimboError::Corrupt("Invalid BEInt64 value".into())
                    })?;
            Ok((
                ValueRef::Numeric(Numeric::Integer(i64::from_be_bytes(*bytes))),
                8,
            ))
        }
        SerialTypeKind::F64 => {
            let bytes: &[u8; 8] = buf
                .get(..8)
                .and_then(|s| s.try_into().ok())
                .ok_or_else(|| LimboError::Corrupt("Invalid BEFloat64 value".into()))?;
            Ok((ValueRef::from_f64(f64::from_be_bytes(*bytes)), 8))
        }
        SerialTypeKind::ConstInt0 => Ok((ValueRef::Numeric(Numeric::Integer(0)), 0)),
        SerialTypeKind::ConstInt1 => Ok((ValueRef::Numeric(Numeric::Integer(1)), 0)),
        SerialTypeKind::Blob => {
            let content_size = serial_type.size();
            let data = buf.get(..content_size).ok_or_else(|| {
                mark_unlikely();
                LimboError::Corrupt("Invalid Blob value".into())
            })?;
            Ok((ValueRef::Blob(data), content_size))
        }
        SerialTypeKind::Text => {
            let content_size = serial_type.size();
            let data = buf.get(..content_size).ok_or_else(|| {
                mark_unlikely();
                LimboError::Corrupt(format!(
                    "Invalid String value, length {} < expected length {}",
                    buf.len(),
                    content_size
                ))
            })?;
            let val = simdutf8::basic::from_utf8(data).map_err(|_| {
                mark_unlikely();
                LimboError::Corrupt("TEXT value contains invalid UTF-8".into())
            })?;
            Ok((
                ValueRef::Text(TextRef::new(val, TextSubtype::Text)),
                content_size,
            ))
        }
    }
}

pub fn read_value_serial_type<'a>(
    buf: &'a [u8],
    serial_type: u64,
) -> Result<(ValueRef<'a>, usize)> {
    match serial_type {
        0 => Ok((ValueRef::Null, 0)),
        1 => {
            if buf.is_empty() {
                mark_unlikely();
                crate::bail_corrupt_error!("Invalid 1-byte int");
            }
            Ok((ValueRef::Numeric(Numeric::Integer(buf[0] as i8 as i64)), 1))
        }
        2 => {
            if buf.len() < 2 {
                mark_unlikely();
                crate::bail_corrupt_error!("Invalid 2-byte int");
            }
            Ok((
                ValueRef::Numeric(Numeric::Integer(i16::from_be_bytes([buf[0], buf[1]]) as i64)),
                2,
            ))
        }
        3 => {
            if buf.len() < 3 {
                mark_unlikely();
                crate::bail_corrupt_error!("Invalid 3-byte int");
            }
            let sign_extension = if buf[0] <= 0x7F { 0 } else { 0xFF };
            Ok((
                ValueRef::Numeric(Numeric::Integer(i32::from_be_bytes([
                    sign_extension,
                    buf[0],
                    buf[1],
                    buf[2],
                ]) as i64)),
                3,
            ))
        }
        4 => {
            if buf.len() < 4 {
                mark_unlikely();
                crate::bail_corrupt_error!("Invalid 4-byte int");
            }
            Ok((
                ValueRef::Numeric(Numeric::Integer(i32::from_be_bytes([
                    buf[0], buf[1], buf[2], buf[3],
                ]) as i64)),
                4,
            ))
        }
        5 => {
            if buf.len() < 6 {
                mark_unlikely();
                crate::bail_corrupt_error!("Invalid 6-byte int");
            }
            let sign_extension = if buf[0] <= 0x7F { 0 } else { 0xFF };
            Ok((
                ValueRef::Numeric(Numeric::Integer(i64::from_be_bytes([
                    sign_extension,
                    sign_extension,
                    buf[0],
                    buf[1],
                    buf[2],
                    buf[3],
                    buf[4],
                    buf[5],
                ]))),
                6,
            ))
        }
        6 => {
            if buf.len() < 8 {
                mark_unlikely();
                crate::bail_corrupt_error!("Invalid 8-byte int");
            }
            Ok((
                ValueRef::Numeric(Numeric::Integer(i64::from_be_bytes([
                    buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
                ]))),
                8,
            ))
        }
        7 => {
            if buf.len() < 8 {
                mark_unlikely();
                crate::bail_corrupt_error!("Invalid 8-byte float");
            }
            Ok((
                ValueRef::from_f64(f64::from_be_bytes([
                    buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
                ])),
                8,
            ))
        }
        8 => Ok((ValueRef::Numeric(Numeric::Integer(0)), 0)),
        9 => Ok((ValueRef::Numeric(Numeric::Integer(1)), 0)),
        n if n >= 12 => match n % 2 {
            0 => {
                // Blob
                let content_size = ((n - 12) / 2) as usize;
                let data = buf.get(..content_size).ok_or_else(|| {
                    mark_unlikely();
                    LimboError::Corrupt("Invalid Blob value".into())
                })?;
                Ok((ValueRef::Blob(data), content_size))
            }
            1 => {
                // Text
                let content_size = ((n - 13) / 2) as usize;
                let data = buf.get(..content_size).ok_or_else(|| {
                    mark_unlikely();
                    LimboError::Corrupt(format!(
                        "Invalid String value, length {} < expected length {}",
                        buf.len(),
                        content_size
                    ))
                })?;
                let val = simdutf8::basic::from_utf8(data).map_err(|_| {
                    mark_unlikely();
                    LimboError::Corrupt("TEXT value contains invalid UTF-8".into())
                })?;
                Ok((
                    ValueRef::Text(TextRef::new(val, TextSubtype::Text)),
                    content_size,
                ))
            }
            _ => unreachable!(),
        },
        _ => {
            mark_unlikely();
            crate::bail_corrupt_error!("Invalid serial type for integer")
        }
    }
}

#[inline(always)]
pub fn read_integer(buf: &[u8], serial_type: u8) -> Result<i64> {
    match serial_type {
        1 => {
            if buf.is_empty() {
                mark_unlikely();
                crate::bail_corrupt_error!("Invalid 1-byte int");
            }
            Ok(buf[0] as i8 as i64)
        }
        2 => {
            if buf.len() < 2 {
                mark_unlikely();
                crate::bail_corrupt_error!("Invalid 2-byte int");
            }
            Ok(i16::from_be_bytes([buf[0], buf[1]]) as i64)
        }
        3 => {
            if buf.len() < 3 {
                mark_unlikely();
                crate::bail_corrupt_error!("Invalid 3-byte int");
            }
            let sign_extension = if buf[0] <= 0x7F { 0 } else { 0xFF };
            Ok(i32::from_be_bytes([sign_extension, buf[0], buf[1], buf[2]]) as i64)
        }
        4 => {
            if buf.len() < 4 {
                mark_unlikely();
                crate::bail_corrupt_error!("Invalid 4-byte int");
            }
            Ok(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as i64)
        }
        5 => {
            if buf.len() < 6 {
                mark_unlikely();
                crate::bail_corrupt_error!("Invalid 6-byte int");
            }
            let sign_extension = if buf[0] <= 0x7F { 0 } else { 0xFF };
            Ok(i64::from_be_bytes([
                sign_extension,
                sign_extension,
                buf[0],
                buf[1],
                buf[2],
                buf[3],
                buf[4],
                buf[5],
            ]))
        }
        6 => {
            if buf.len() < 8 {
                crate::bail_corrupt_error!("Invalid 8-byte int");
            }
            Ok(i64::from_be_bytes([
                buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
            ]))
        }
        8 => Ok(0),
        9 => Ok(1),
        _ => {
            mark_unlikely();
            crate::bail_corrupt_error!("Invalid serial type for integer")
        }
    }
}

/// Reads varint integer from the buffer.
/// This function is similar to `sqlite3GetVarint32`
#[inline(always)]
pub fn read_varint(buf: &[u8]) -> Result<(u64, usize)> {
    let mut v: u64 = 0;
    for i in 0..8 {
        match buf.get(i) {
            Some(c) => {
                v = (v << 7) + (c & 0x7f) as u64;
                if (c & 0x80) == 0 {
                    return Ok((v, i + 1));
                }
            }
            None => {
                mark_unlikely();
                crate::bail_corrupt_error!("Invalid varint");
            }
        }
    }
    match buf.get(8) {
        Some(&c) => {
            // Values requiring 9 bytes must have non-zero in the top 8 bits (value >= 1<<56).
            // Since the final value is `(v<<8) + c`, the top 8 bits (v >> 48) must not be 0.
            // If those are zero, this should be treated as corrupt.
            // Perf? the comparison + branching happens only in parsing 9-byte varint which is rare.
            if unlikely((v >> 48) == 0) {
                bail_corrupt_error!("Invalid varint");
            }
            v = (v << 8) + c as u64;
            Ok((v, 9))
        }
        None => {
            mark_unlikely();
            bail_corrupt_error!("Invalid varint");
        }
    }
}

#[inline(always)]
/// Reads a varint from the buffer, returning None if more data is needed.
pub fn read_varint_partial(buf: &[u8]) -> Result<Option<(u64, usize)>> {
    let mut v: u64 = 0;
    for i in 0..8 {
        let Some(&c) = buf.get(i) else {
            return Ok(None);
        };
        v = (v << 7) + (c & 0x7f) as u64;
        if (c & 0x80) == 0 {
            return Ok(Some((v, i + 1)));
        }
    }
    let Some(&c) = buf.get(8) else {
        return Ok(None);
    };
    if unlikely((v >> 48) == 0) {
        bail_corrupt_error!("Invalid varint");
    }
    v = (v << 8) + c as u64;
    Ok(Some((v, 9)))
}

/// Compute the length of a varint encoding for a given u64 value.
///
/// SQLite varint: bytes 1-8 each carry 7 payload bits (56 total).
/// The optional 9th byte carries a full 8 bits (no continuation bit),
/// giving 64 bits total.  So values needing >56 bits always take 9 bytes.
#[inline(always)]
pub fn varint_len(value: u64) -> usize {
    if value <= 0x7f {
        1
    } else if value > (1u64 << 56) - 1 {
        9
    } else {
        let bits = 64 - value.leading_zeros() as usize;
        bits.div_ceil(7)
    }
}

pub fn write_varint(buf: &mut [u8], value: u64) -> usize {
    if value <= 0x7f {
        buf[0] = (value & 0x7f) as u8;
        return 1;
    }

    if value <= 0x3fff {
        buf[0] = (((value >> 7) & 0x7f) | 0x80) as u8;
        buf[1] = (value & 0x7f) as u8;
        return 2;
    }

    let mut value = value;
    if (value & ((0xff000000_u64) << 32)) > 0 {
        buf[8] = value as u8;
        value >>= 8;
        for i in (0..8).rev() {
            buf[i] = ((value & 0x7f) | 0x80) as u8;
            value >>= 7;
        }
        return 9;
    }

    let mut encoded: [u8; 9] = [0; 9];
    let mut bytes = value;
    let mut n = 0;
    while bytes != 0 {
        let v = 0x80 | (bytes & 0x7f);
        encoded[n] = v as u8;
        bytes >>= 7;
        n += 1;
    }
    encoded[0] &= 0x7f;
    for i in 0..n {
        buf[i] = encoded[n - 1 - i];
    }
    n
}

pub fn write_varint_to_vec(value: u64, payload: &mut Vec<u8>) {
    let mut varint = [0u8; 9];
    let n = write_varint(&mut varint, value);
    payload.extend_from_slice(&varint[0..n]);
}

/// Stream through frames in chunks, building frame_cache incrementally
/// Track last valid commit frame for consistency
/// Non-blocking driver for WAL recovery on open.
///
/// Created by [`BuildSharedWal::begin`] (which performs only synchronous
/// setup and may complete immediately for an empty/headerless WAL), then
/// driven via [`BuildSharedWal::poll`] until it returns `Done`. All recovery
/// state lives in the [`StreamingWalReader`] (atomics + `RwLock<StreamingState>`)
/// and is updated by the read completions' callbacks, so the only state this
/// driver tracks is which phase/completion it's waiting on.
pub struct BuildSharedWal {
    reader: Option<Arc<StreamingWalReader>>,
    wal_file_shared: Arc<RwLock<WalFileShared>>,
    file_size: u64,
    phase: BuildSharedWalPhase,
}

#[derive(Clone)]
enum BuildSharedWalPhase {
    /// Issue the WAL header read.
    NeedHeaderRead,
    /// Waiting on the header read completion.
    AwaitHeader(Completion),
    /// Decide whether to read the next chunk or finalize.
    ChunkLoop,
    /// Waiting on a chunk read that began at `offset`.
    AwaitChunk { completion: Completion, offset: u64 },
    /// Recovery complete.
    Done,
}

impl BuildSharedWal {
    /// Synchronous setup: read the file size, build the (initially unloaded)
    /// `WalFileShared`, and decide the starting phase. For a WAL smaller than
    /// the header it marks the shared state loaded and starts in `Done`.
    pub fn begin(file: &Arc<dyn File>) -> Result<Self> {
        let size = file.size()?;

        let header = Arc::new(SpinLock::new(WalHeader::default()));
        let read_locks = std::array::from_fn(|_| TursoRwLock::new());
        for (i, l) in read_locks.iter().enumerate() {
            l.write();
            l.set_value_exclusive(if i < 2 { 0 } else { READMARK_NOT_USED });
            l.unlock();
        }

        let wal_file_shared = Arc::new(RwLock::new(WalFileShared {
            metadata: WalSharedMetadata {
                enabled: AtomicBool::new(true),
                wal_header: header.clone(),
                min_frame: AtomicU64::new(0),
                max_frame: AtomicU64::new(0),
                nbackfills: AtomicU64::new(0),
                transaction_count: AtomicU64::new(0),
                last_checksum: (0, 0),
                loaded: AtomicBool::new(false),
                loaded_from_disk_scan: AtomicBool::new(true),
                initialized: AtomicBool::new(false),
            },
            runtime: WalSharedRuntime {
                frame_cache: Arc::new(SpinLock::new(FxHashMap::default())),
                frame_cache_high_water: AtomicU64::new(0),
                file: Some(file.clone()),
                read_locks,
                vacuum_lock: TursoRwLock::new(),
                write_lock: TursoRwLock::new(),
                checkpoint_lock: TursoRwLock::new(),
                epoch: AtomicU32::new(0),
                overflow_fallback_coverage: Arc::new(SpinLock::new(
                    OverflowFallbackCoverage::default(),
                )),
            },
        }));

        if size < WAL_HEADER_SIZE as u64 {
            wal_file_shared
                .write()
                .metadata
                .loaded
                .store(true, Ordering::SeqCst);
            return Ok(Self {
                reader: None,
                wal_file_shared,
                file_size: size,
                phase: BuildSharedWalPhase::Done,
            });
        }

        let reader = Arc::new(StreamingWalReader::new(
            file.clone(),
            wal_file_shared.clone(),
            header,
            size,
        ));

        Ok(Self {
            reader: Some(reader),
            wal_file_shared,
            file_size: size,
            phase: BuildSharedWalPhase::NeedHeaderRead,
        })
    }

    /// Drive the recovery state machine. Yields the in-flight read completion
    /// when it must wait; returns `Done(wal_file_shared)` once the full WAL
    /// has been scanned (or recovery short-circuited).
    pub fn poll(&mut self) -> IOResultOr<Arc<RwLock<WalFileShared>>> {
        loop {
            match self.phase.clone() {
                BuildSharedWalPhase::NeedHeaderRead => {
                    let reader = self
                        .reader
                        .clone()
                        .expect("reader must exist outside the Done phase");
                    let c = reader.read_header()?;
                    self.phase = BuildSharedWalPhase::AwaitHeader(c);
                }
                BuildSharedWalPhase::AwaitHeader(c) => {
                    if !c.succeeded() {
                        io_yield_one!(c);
                    }
                    self.phase = BuildSharedWalPhase::ChunkLoop;
                }
                BuildSharedWalPhase::ChunkLoop => {
                    let reader = self
                        .reader
                        .clone()
                        .expect("reader must exist outside the Done phase");
                    if reader.done.load(Ordering::Acquire) {
                        self.phase = BuildSharedWalPhase::Done;
                        continue;
                    }
                    let offset = reader.off_atomic.load(Ordering::Acquire);
                    if offset >= self.file_size {
                        reader.finalize_loading();
                        self.phase = BuildSharedWalPhase::Done;
                        continue;
                    }
                    let (_read_size, c) = reader.submit_one_chunk(offset)?;
                    self.phase = BuildSharedWalPhase::AwaitChunk {
                        completion: c,
                        offset,
                    };
                }
                BuildSharedWalPhase::AwaitChunk { completion, offset } => {
                    if !completion.succeeded() {
                        io_yield_one!(completion);
                    }
                    let reader = self
                        .reader
                        .clone()
                        .expect("reader must exist outside the Done phase");
                    let new_off = reader.off_atomic.load(Ordering::Acquire);
                    if new_off <= offset {
                        // No forward progress — treat as end of valid log.
                        reader.finalize_loading();
                        self.phase = BuildSharedWalPhase::Done;
                    } else {
                        self.phase = BuildSharedWalPhase::ChunkLoop;
                    }
                }
                BuildSharedWalPhase::Done => {
                    return Ok(IOResult::Done(self.wal_file_shared.clone()));
                }
            }
        }
    }
}

/// Blocking shim over [`BuildSharedWal`]. Retained for the unit test and any
/// caller not yet lifted to drive the recovery state machine directly.
pub fn build_shared_wal(
    file: &Arc<dyn File>,
    io: &Arc<dyn crate::IO>,
) -> Result<Arc<RwLock<WalFileShared>>> {
    let mut driver = BuildSharedWal::begin(file)?;
    io.block(|| driver.poll())
}

pub(super) struct StreamingWalReader {
    file: Arc<dyn File>,
    wal_shared: Arc<RwLock<WalFileShared>>,
    header: Arc<SpinLock<WalHeader>>,
    file_size: u64,
    state: RwLock<StreamingState>,
    off_atomic: AtomicU64,
    page_atomic: AtomicU64,
    pub(super) done: AtomicBool,
}

/// Mutable state for streaming reader
struct StreamingState {
    frame_idx: u64,
    cumulative_checksum: (u32, u32),
    /// checksum of the last valid commit frame
    last_valid_checksum: (u32, u32),
    last_valid_frame: u64,
    pending_frames: FxHashMap<u64, Vec<u64>>,
    page_size: usize,
    use_native_endian: bool,
    header_valid: bool,
}

impl StreamingWalReader {
    fn new(
        file: Arc<dyn File>,
        wal_shared: Arc<RwLock<WalFileShared>>,
        header: Arc<SpinLock<WalHeader>>,
        file_size: u64,
    ) -> Self {
        Self {
            file,
            wal_shared,
            header,
            file_size,
            off_atomic: AtomicU64::new(0),
            page_atomic: AtomicU64::new(0),
            done: AtomicBool::new(false),
            state: RwLock::new(StreamingState {
                frame_idx: 1,
                cumulative_checksum: (0, 0),
                last_valid_checksum: (0, 0),
                last_valid_frame: 0,
                pending_frames: FxHashMap::default(),
                page_size: 0,
                use_native_endian: false,
                header_valid: false,
            }),
        }
    }

    fn read_header(self: Arc<Self>) -> crate::Result<Completion> {
        let header_buf = Arc::new(Buffer::new_temporary(WAL_HEADER_SIZE));
        let reader = self.clone();
        let completion: Box<ReadComplete> = Box::new(move |res| {
            let _reader = reader.clone();
            _reader.handle_header_read(res);
            None
        });
        let c = Completion::new_read(header_buf, completion);
        self.file.pread(0, c)
    }

    fn submit_one_chunk(self: Arc<Self>, offset: u64) -> crate::Result<(usize, Completion)> {
        let page_size = self.page_atomic.load(Ordering::Acquire) as usize;
        if page_size == 0 {
            return Err(crate::LimboError::InternalError(
                "page size not initialized".into(),
            ));
        }
        let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
        if frame_size == 0 {
            return Err(crate::LimboError::InternalError(
                "invalid frame size".into(),
            ));
        }
        const BASE: usize = 16 * 1024 * 1024;
        let aligned = (BASE / frame_size) * frame_size;
        let read_size = aligned
            .max(frame_size)
            .min((self.file_size - offset) as usize);
        if read_size == 0 {
            // end-of-file; let caller finalize
            return Ok((0, Completion::new_yield()));
        }

        let buf = Arc::new(Buffer::new_temporary(read_size));
        let me = self.clone();
        let completion: Box<ReadComplete> = Box::new(move |res| {
            tracing::debug!("WAL chunk read complete");
            let reader = me.clone();
            reader.handle_chunk_read(res);
            None
        });
        let c = Completion::new_read(buf, completion);
        let guard = self.file.pread(offset, c)?;
        Ok((read_size, guard))
    }

    fn handle_header_read(self: Arc<Self>, res: Result<(Arc<Buffer>, i32), CompletionError>) {
        let Ok((buf, bytes_read)) = res else {
            self.finalize_loading();
            return;
        };
        if bytes_read != WAL_HEADER_SIZE as i32 {
            self.finalize_loading();
            return;
        }

        let (page_sz, c1, c2, use_native, ok) = {
            let mut h = self.header.lock();
            let s = buf.as_slice();
            h.magic = u32::from_be_bytes(s[0..4].try_into().unwrap());
            h.file_format = u32::from_be_bytes(s[4..8].try_into().unwrap());
            h.page_size = u32::from_be_bytes(s[8..12].try_into().unwrap());
            h.checkpoint_seq = u32::from_be_bytes(s[12..16].try_into().unwrap());
            h.salt_1 = u32::from_be_bytes(s[16..20].try_into().unwrap());
            h.salt_2 = u32::from_be_bytes(s[20..24].try_into().unwrap());
            h.checksum_1 = u32::from_be_bytes(s[24..28].try_into().unwrap());
            h.checksum_2 = u32::from_be_bytes(s[28..32].try_into().unwrap());
            tracing::debug!("WAL header: {:?}", *h);

            let use_native = cfg!(target_endian = "big") == ((h.magic & 1) != 0);
            let calc = checksum_wal(&s[0..24], &h, (0, 0), use_native);
            (
                h.page_size,
                h.checksum_1,
                h.checksum_2,
                use_native,
                calc == (h.checksum_1, h.checksum_2),
            )
        };
        #[cfg(debug_assertions)]
        {
            let header = self.header.lock();
            tracing::debug!(
                "WAL_SCAN header page_size={} checkpoint_seq={} salts=({}, {}) checksum=({}, {}) use_native={} valid={}",
                page_sz,
                header.checkpoint_seq,
                header.salt_1,
                header.salt_2,
                c1,
                c2,
                use_native,
                ok
            );
        }
        if PageSize::new(page_sz).is_none() || !ok {
            self.finalize_loading();
            return;
        }
        {
            let mut st = self.state.write();
            st.page_size = page_sz as usize;
            st.use_native_endian = use_native;
            st.cumulative_checksum = (c1, c2);
            st.last_valid_checksum = (c1, c2);
            st.header_valid = true;
        }
        self.off_atomic
            .store(WAL_HEADER_SIZE as u64, Ordering::Release);
        self.page_atomic.store(page_sz as u64, Ordering::Release);
    }

    fn handle_chunk_read(self: Arc<Self>, res: Result<(Arc<Buffer>, i32), CompletionError>) {
        let Ok((buf, bytes_read)) = res else {
            self.finalize_loading();
            return;
        };
        let buf_slice = &buf.as_slice()[..bytes_read as usize];
        // Snapshot salts/endianness once to avoid per-frame header locks
        let (header_copy, use_native) = {
            let st = self.state.read();
            let h = self.header.lock();
            (*h, st.use_native_endian)
        };

        let consumed = self.process_frames(buf_slice, &header_copy, use_native);
        self.off_atomic.fetch_add(consumed as u64, Ordering::AcqRel);
        // If we didn’t consume the full chunk, we hit a stop condition
        if consumed < buf_slice.len() || self.off_atomic.load(Ordering::Acquire) >= self.file_size {
            self.finalize_loading();
        }
    }

    // Processes frames from a buffer, returns bytes processed
    fn process_frames(&self, buf: &[u8], header: &WalHeader, use_native: bool) -> usize {
        let mut st = self.state.write();
        let page_size = st.page_size;
        let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
        let mut pos = 0;

        while pos + frame_size <= buf.len() {
            let fh = &buf[pos..pos + WAL_FRAME_HEADER_SIZE];
            let page = &buf[pos + WAL_FRAME_HEADER_SIZE..pos + frame_size];

            let page_no = u32::from_be_bytes(fh[0..4].try_into().unwrap());
            let db_size = u32::from_be_bytes(fh[4..8].try_into().unwrap());
            let s1 = u32::from_be_bytes(fh[8..12].try_into().unwrap());
            let s2 = u32::from_be_bytes(fh[12..16].try_into().unwrap());
            let c1 = u32::from_be_bytes(fh[16..20].try_into().unwrap());
            let c2 = u32::from_be_bytes(fh[20..24].try_into().unwrap());

            tracing::debug!("process_frames: page_no={page_no}, db_size={db_size}, s1={s1}, s2={s2}, c1={c1}, c2={c2}");

            if page_no == 0 {
                tracing::debug!(
                    "process_frames: unexpected page_no, stop reading WAL at initialization phase"
                );
                break;
            }
            if s1 != header.salt_1 || s2 != header.salt_2 {
                tracing::debug!(
                    "WAL_SCAN stop: frame={} salt mismatch frame=({}, {}) header=({}, {})",
                    st.frame_idx,
                    s1,
                    s2,
                    header.salt_1,
                    header.salt_2
                );
                tracing::debug!(
                    "process_frames: salt mismatch, stop reading WAL at initialization phase"
                );
                break;
            }

            let seed = checksum_wal(&fh[0..8], header, st.cumulative_checksum, use_native);
            let calc = checksum_wal(page, header, seed, use_native);
            if calc != (c1, c2) {
                tracing::debug!(
                    " WAL_SCAN stop: process_frames, checksum mismatch, stop reading WAL at initialization phase: frame={} checksum mismatch calc=({},{}) file=({},{})",
                    st.frame_idx,
                    calc.0,
                    calc.1,
                    c1,
                    c2
                );
                break;
            }

            st.cumulative_checksum = calc;
            let frame_idx = st.frame_idx;
            st.pending_frames
                .entry(page_no as u64)
                .or_default()
                .push(frame_idx);

            if db_size > 0 {
                st.last_valid_frame = st.frame_idx;
                st.last_valid_checksum = calc;
                tracing::debug!(
                    "WAL_SCAN commit frame={} page_no={} db_size={}",
                    st.frame_idx,
                    page_no,
                    db_size
                );
                self.flush_pending_frames(&mut st);
            }
            st.frame_idx += 1;
            pos += frame_size;
        }
        pos
    }

    fn flush_pending_frames(&self, state: &mut StreamingState) {
        if state.pending_frames.is_empty() {
            return;
        }
        let wfs = self.wal_shared.read();
        let mut frame_cache = wfs.runtime.frame_cache.lock();
        for (page, mut frames) in state.pending_frames.drain() {
            // Only include frames up to last valid commit
            frames.retain(|&f| f <= state.last_valid_frame);
            if !frames.is_empty() {
                frame_cache.entry(page).or_default().extend(frames);
            }
        }
        wfs.metadata
            .max_frame
            .store(state.last_valid_frame, Ordering::Release);
        // Recovery populates `frame_cache` directly (not via `cache_frame`), so
        // seed the high-water with the recovered frames; otherwise the first
        // post-recovery rewind/slot-reuse could go undetected.
        wfs.runtime
            .frame_cache_high_water
            .fetch_max(state.last_valid_frame, Ordering::AcqRel);
    }

    /// Finalizes the loading process
    fn finalize_loading(&self) {
        let mut wfs = self.wal_shared.write();
        let st = self.state.read();
        tracing::debug!(
            "WAL_SCAN finalize last_valid_frame={} pending_pages={} header_valid={}",
            st.last_valid_frame,
            st.pending_frames.len(),
            st.header_valid
        );

        let max_frame = st.last_valid_frame;
        if max_frame > 0 {
            let mut frame_cache = wfs.runtime.frame_cache.lock();
            for frames in frame_cache.values_mut() {
                frames.retain(|&f| f <= max_frame);
            }
            frame_cache.retain(|_, frames| !frames.is_empty());
            let header = wfs.metadata.wal_header.lock();
            wfs.runtime.overflow_fallback_coverage.lock().record(
                header.checkpoint_seq,
                header.salt_1,
                header.salt_2,
                max_frame,
            );
        } else {
            wfs.runtime.overflow_fallback_coverage.lock().clear();
        }

        wfs.metadata.max_frame.store(max_frame, Ordering::SeqCst);
        // use checksum of last valid commit frame, not necessarily the last frame
        wfs.metadata.last_checksum = st.last_valid_checksum;
        if st.header_valid {
            wfs.metadata.initialized.store(true, Ordering::SeqCst);
        }
        wfs.metadata.nbackfills.store(0, Ordering::SeqCst);
        wfs.metadata.loaded.store(true, Ordering::SeqCst);

        self.done.store(true, Ordering::Release);
        tracing::debug!(
            "WAL loading complete: {} frames processed, last commit at frame {}",
            st.frame_idx - 1,
            max_frame
        );
    }
}

pub fn begin_read_wal_frame_raw<F: File + ?Sized>(
    buffer_pool: &Arc<BufferPool>,
    io: &F,
    offset: u64,
    complete: Box<ReadComplete>,
) -> Result<Completion> {
    tracing::trace!("begin_read_wal_frame_raw(offset={})", offset);
    let buf = Arc::new(buffer_pool.get_wal_frame());
    let c = Completion::new_read(buf, complete);
    let c = io.pread(offset, c)?;
    Ok(c)
}

/// Reads one WAL frame's page body. The read is added to `group`, when
/// given, before it is submitted.
pub fn begin_read_wal_frame<F: File + ?Sized>(
    io: &F,
    offset: u64,
    buffer_pool: Arc<BufferPool>,
    complete: Box<ReadComplete>,
    page_idx: usize,
    io_ctx: &IOContext,
    group: Option<&mut CompletionGroup>,
) -> Result<Completion> {
    tracing::trace!(
        "begin_read_wal_frame(offset={}, page_idx={})",
        offset,
        page_idx
    );
    let buf = buffer_pool.get_page();
    let buf = Arc::new(buf);

    let complete: Box<ReadComplete> = match io_ctx.page_transform() {
        PageTransform::Codec(ctx) => {
            let page_codec = ctx.clone();
            let original_complete = complete;
            // TODO(v): support in-place codec decoding to avoid this extra page buffer.
            let decoded_buf = Arc::new(buffer_pool.get_page());
            let codec_context = PageCodecContext::from_page_idx(page_idx, PageLocation::Wal)?;

            Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                let Ok((encoded_buf, bytes_read)) = res else {
                    return original_complete(res);
                };
                if bytes_read == 0 {
                    return original_complete(Ok((encoded_buf, bytes_read)));
                }
                turso_assert_greater_than!(
                    bytes_read,
                    0,
                    "expected to read data for page codec page",
                    { "page_idx": page_idx }
                );
                if bytes_read as usize != encoded_buf.len() {
                    return original_complete(Ok((encoded_buf, bytes_read)));
                }
                match page_codec.decode_page(
                    codec_context,
                    encoded_buf.as_slice(),
                    decoded_buf.as_mut_slice(),
                ) {
                    Ok(()) => original_complete(Ok((decoded_buf.clone(), bytes_read))),
                    Err(e) => {
                        tracing::error!(
                            "Failed to decode WAL frame data for page_idx={page_idx}: {e}"
                        );
                        let err = page_codec_completion_error(page_codec.as_ref(), page_idx);
                        original_complete(Err(err));
                        Some(err)
                    }
                }
            })
        }
        PageTransform::Checksum(ctx) => {
            let checksum_ctx = ctx.clone();
            let original_c = complete;
            Box::new(move |res: Result<(Arc<Buffer>, i32), CompletionError>| {
                let Ok((buf, bytes_read)) = res else {
                    return original_c(res);
                };
                if bytes_read <= 0 {
                    tracing::trace!("Read page {page_idx} with {} bytes", bytes_read);
                    return original_c(Ok((buf, bytes_read)));
                }

                match checksum_ctx.verify_checksum(buf.as_mut_slice(), page_idx) {
                    Ok(_) => original_c(Ok((buf, bytes_read))),
                    Err(e) => {
                        mark_unlikely();
                        tracing::error!("Failed to verify checksum for page_id={page_idx}: {e}");
                        original_c(Err(e));
                        Some(e)
                    }
                }
            })
        }
        PageTransform::None => complete,
    };
    let c = Completion::new_read(buf, complete);
    if let Some(group) = group {
        group.add(&c);
    }
    io.pread(offset, c)
}

pub fn parse_wal_frame_header(frame: &[u8]) -> (WalFrameHeader, &[u8]) {
    let page_number = u32::from_be_bytes(frame[0..4].try_into().unwrap());
    let db_size = u32::from_be_bytes(frame[4..8].try_into().unwrap());
    let salt_1 = u32::from_be_bytes(frame[8..12].try_into().unwrap());
    let salt_2 = u32::from_be_bytes(frame[12..16].try_into().unwrap());
    let checksum_1 = u32::from_be_bytes(frame[16..20].try_into().unwrap());
    let checksum_2 = u32::from_be_bytes(frame[20..24].try_into().unwrap());
    let header = WalFrameHeader {
        page_number,
        db_size,
        salt_1,
        salt_2,
        checksum_1,
        checksum_2,
    };
    let page = &frame[WAL_FRAME_HEADER_SIZE..];
    (header, page)
}

pub fn prepare_wal_frame(
    buffer_pool: &Arc<BufferPool>,
    wal_header: &WalHeader,
    prev_checksums: (u32, u32),
    _page_size: u32,
    page_number: u32,
    db_size: u32,
    page: &[u8],
) -> ((u32, u32), Arc<Buffer>) {
    tracing::trace!(page_number);

    let buffer = prepare_wal_frame_header(buffer_pool, wal_header, page_number, db_size);
    let frame = buffer.as_mut_slice();
    frame[WAL_FRAME_HEADER_SIZE..].copy_from_slice(page);

    let final_checksum = recompute_wal_frame_checksum(frame, wal_header, prev_checksums);

    (final_checksum, buffer)
}

/// Prepares a WAL frame header while leaving the page body for a transform to fill.
///
/// The caller must write every byte after [`WAL_FRAME_HEADER_SIZE`] before computing
/// the frame checksum or submitting the frame to storage.
pub(crate) fn prepare_wal_frame_header(
    buffer_pool: &Arc<BufferPool>,
    wal_header: &WalHeader,
    page_number: u32,
    db_size: u32,
) -> Arc<Buffer> {
    let buffer = Arc::new(buffer_pool.get_wal_frame());
    let frame = buffer.as_mut_slice();
    frame[0..4].copy_from_slice(&page_number.to_be_bytes());
    frame[4..8].copy_from_slice(&db_size.to_be_bytes());
    frame[8..12].copy_from_slice(&wal_header.salt_1.to_be_bytes());
    frame[12..16].copy_from_slice(&wal_header.salt_2.to_be_bytes());
    buffer
}

/// Recompute a frame checksum after transforming its page body in place.
pub(crate) fn recompute_wal_frame_checksum(
    frame: &mut [u8],
    wal_header: &WalHeader,
    prev_checksums: (u32, u32),
) -> (u32, u32) {
    let expects_be = wal_header.magic & 1;
    let use_native_endian = cfg!(target_endian = "big") as u32 == expects_be;
    let header_checksum = checksum_wal(&frame[0..8], wal_header, prev_checksums, use_native_endian);
    let final_checksum = checksum_wal(
        &frame[WAL_FRAME_HEADER_SIZE..],
        wal_header,
        header_checksum,
        use_native_endian,
    );
    frame[16..20].copy_from_slice(&final_checksum.0.to_be_bytes());
    frame[20..24].copy_from_slice(&final_checksum.1.to_be_bytes());
    final_checksum
}
/// Writes the WAL header. The write is added to `group`, when given,
/// before it is submitted.
pub fn begin_write_wal_header<F: File + ?Sized>(
    io: &F,
    header: &WalHeader,
    group: Option<&mut CompletionGroup>,
) -> Result<Completion> {
    tracing::trace!("begin_write_wal_header");
    let buffer = {
        let buffer = Buffer::new_temporary(WAL_HEADER_SIZE);
        let buf = buffer.as_mut_slice();

        buf[0..4].copy_from_slice(&header.magic.to_be_bytes());
        buf[4..8].copy_from_slice(&header.file_format.to_be_bytes());
        buf[8..12].copy_from_slice(&header.page_size.to_be_bytes());
        buf[12..16].copy_from_slice(&header.checkpoint_seq.to_be_bytes());
        buf[16..20].copy_from_slice(&header.salt_1.to_be_bytes());
        buf[20..24].copy_from_slice(&header.salt_2.to_be_bytes());
        buf[24..28].copy_from_slice(&header.checksum_1.to_be_bytes());
        buf[28..32].copy_from_slice(&header.checksum_2.to_be_bytes());

        #[allow(clippy::arc_with_non_send_sync)]
        Arc::new(buffer)
    };

    let write_complete = move |res: Result<i32, CompletionError>| {
        let Ok(bytes_written) = res else {
            return;
        };
        turso_assert!(
            bytes_written == WAL_HEADER_SIZE as i32,
            "wal header wrote({bytes_written}) != expected({WAL_HEADER_SIZE})"
        );
    };
    #[allow(clippy::arc_with_non_send_sync)]
    let c = Completion::new_write(write_complete);
    if let Some(group) = group {
        group.add(&c);
    }
    let c = io.pwrite(0, buffer, c)?;
    Ok(c)
}

/// Checks if payload will overflow a cell based on the maximum allowed size.
/// It will return the min size that will be stored in that case,
/// including overflow pointer
/// see e.g. https://github.com/sqlite/sqlite/blob/9591d3fe93936533c8c3b0dc4d025ac999539e11/src/dbstat.c#L371
#[inline]
pub fn payload_overflows(
    payload_size: usize,
    payload_overflow_threshold_max: usize,
    payload_overflow_threshold_min: usize,
    usable_size: usize,
) -> (bool, usize) {
    if payload_size <= payload_overflow_threshold_max {
        return (false, 0);
    }

    let mut space_left = payload_overflow_threshold_min
        + (payload_size - payload_overflow_threshold_min) % (usable_size - 4);
    if space_left > payload_overflow_threshold_max {
        space_left = payload_overflow_threshold_min;
    }
    (true, space_left + 4)
}

/// The checksum is computed by interpreting the input as an even number of unsigned 32-bit integers: x(0) through x(N).
/// The 32-bit integers are big-endian if the magic number in the first 4 bytes of the WAL header is 0x377f0683
/// and the integers are little-endian if the magic number is 0x377f0682.
/// The checksum values are always stored in the frame header in a big-endian format regardless of which byte order is used to compute the checksum.
///
/// The checksum algorithm only works for content which is a multiple of 8 bytes in length.
/// In other words, if the inputs are x(0) through x(N) then N must be odd.
/// The checksum algorithm is as follows:
///
/// s0 = s1 = 0
/// for i from 0 to n-1 step 2:
///    s0 += x(i) + s1;
///    s1 += x(i+1) + s0;
/// endfor
///
/// The outputs s0 and s1 are both weighted checksums using Fibonacci weights in reverse order.
/// (The largest Fibonacci weight occurs on the first element of the sequence being summed.)
/// The s1 value spans all 32-bit integer terms of the sequence whereas s0 omits the final term.
#[inline]
pub fn checksum_wal(
    buf: &[u8],
    _wal_header: &WalHeader,
    input: (u32, u32),
    native_endian: bool, // Sqlite interprets big endian as "native"
) -> (u32, u32) {
    turso_assert_eq!(buf.len() % 8, 0, "buffer must be a multiple of 8");
    let mut s0: u32 = input.0;
    let mut s1: u32 = input.1;
    let mut i = 0;
    if native_endian {
        while i < buf.len() {
            let v0 = u32::from_ne_bytes(buf[i..i + 4].try_into().unwrap());
            let v1 = u32::from_ne_bytes(buf[i + 4..i + 8].try_into().unwrap());
            s0 = s0.wrapping_add(v0.wrapping_add(s1));
            s1 = s1.wrapping_add(v1.wrapping_add(s0));
            i += 8;
        }
    } else {
        while i < buf.len() {
            let v0 = u32::from_ne_bytes(buf[i..i + 4].try_into().unwrap()).swap_bytes();
            let v1 = u32::from_ne_bytes(buf[i + 4..i + 8].try_into().unwrap()).swap_bytes();
            s0 = s0.wrapping_add(v0.wrapping_add(s1));
            s1 = s1.wrapping_add(v1.wrapping_add(s0));
            i += 8;
        }
    }
    (s0, s1)
}

impl WalHeader {
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::mem::transmute::<&WalHeader, &[u8; size_of::<WalHeader>()]>(self) }
    }
}

#[inline]
pub fn read_u32(buf: &[u8], pos: usize) -> u32 {
    u32::from_be_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]])
}

#[cfg(test)]
mod tests {
    use crate::Value;

    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(&[], SerialType::null(), Value::Null)]
    #[case(&[255], SerialType::i8(), Value::from_i64(-1))]
    #[case(&[0x12, 0x34], SerialType::i16(), Value::from_i64(0x1234))]
    #[case(&[0xFE], SerialType::i8(), Value::from_i64(-2))]
    #[case(&[0x12, 0x34, 0x56], SerialType::i24(), Value::from_i64(0x123456))]
    #[case(&[0x12, 0x34, 0x56, 0x78], SerialType::i32(), Value::from_i64(0x12345678))]
    #[case(&[0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC], SerialType::i48(), Value::from_i64(0x123456789ABC))]
    #[case(&[0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xFF], SerialType::i64(), Value::from_i64(0x123456789ABCDEFF))]
    #[case(&[0x40, 0x09, 0x21, 0xFB, 0x54, 0x44, 0x2D, 0x18], SerialType::f64(), Value::from_f64(std::f64::consts::PI))]
    #[case(&[1, 2], SerialType::const_int0(), Value::from_i64(0))]
    #[case(&[65, 66], SerialType::const_int1(), Value::from_i64(1))]
    #[case(
        &[1, 2, 3],
        SerialType::blob(3),
        Value::from_slice(&[1, 2, 3]).expect(crate::alloc::ALLOC_ERR_MSG)
    )]
    #[case(
        &[],
        SerialType::blob(0),
        Value::from_slice(&[]).expect(crate::alloc::ALLOC_ERR_MSG)
    )] // empty blob
    #[case(&[65, 66, 67], SerialType::text(3), Value::build_text("ABC"))]
    #[case(&[0x80], SerialType::i8(), Value::from_i64(-128))]
    #[case(&[0x80, 0], SerialType::i16(), Value::from_i64(-32768))]
    #[case(&[0x80, 0, 0], SerialType::i24(), Value::from_i64(-8388608))]
    #[case(&[0x80, 0, 0, 0], SerialType::i32(), Value::from_i64(-2147483648))]
    #[case(&[0x80, 0, 0, 0, 0, 0], SerialType::i48(), Value::from_i64(-140737488355328))]
    #[case(&[0x80, 0, 0, 0, 0, 0, 0, 0], SerialType::i64(), Value::from_i64(-9223372036854775808))]
    #[case(&[0x7f], SerialType::i8(), Value::from_i64(127))]
    #[case(&[0x7f, 0xff], SerialType::i16(), Value::from_i64(32767))]
    #[case(&[0x7f, 0xff, 0xff], SerialType::i24(), Value::from_i64(8388607))]
    #[case(&[0x7f, 0xff, 0xff, 0xff], SerialType::i32(), Value::from_i64(2147483647))]
    #[case(&[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff], SerialType::i48(), Value::from_i64(140737488355327))]
    #[case(&[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], SerialType::i64(), Value::from_i64(9223372036854775807))]
    fn test_read_value(
        #[case] buf: &[u8],
        #[case] serial_type: SerialType,
        #[case] expected: Value,
    ) {
        let result = read_value(buf, serial_type).unwrap();
        assert_eq!(
            result.0.to_owned().expect(crate::alloc::ALLOC_ERR_MSG),
            expected
        );
    }

    #[test]
    fn test_text_decoders_reject_invalid_utf8() {
        for result in [
            read_value(&[0xff], SerialType::text(1)),
            read_value_serial_type(&[0xff], 15),
        ] {
            assert!(
                matches!(
                    result,
                    Err(LimboError::Corrupt(ref message))
                        if message == "TEXT value contains invalid UTF-8"
                ),
                "unexpected result: {result:?}"
            );
        }
    }

    #[test]
    fn test_serial_type_helpers() {
        assert_eq!(
            TryInto::<SerialType>::try_into(12u64).unwrap(),
            SerialType::blob(0)
        );
        assert_eq!(
            TryInto::<SerialType>::try_into(14u64).unwrap(),
            SerialType::blob(1)
        );
        assert_eq!(
            TryInto::<SerialType>::try_into(13u64).unwrap(),
            SerialType::text(0)
        );
        assert_eq!(
            TryInto::<SerialType>::try_into(15u64).unwrap(),
            SerialType::text(1)
        );
        assert_eq!(
            TryInto::<SerialType>::try_into(16u64).unwrap(),
            SerialType::blob(2)
        );
        assert_eq!(
            TryInto::<SerialType>::try_into(17u64).unwrap(),
            SerialType::text(2)
        );
    }

    #[rstest]
    #[case(0, SerialType::null())]
    #[case(1, SerialType::i8())]
    #[case(2, SerialType::i16())]
    #[case(3, SerialType::i24())]
    #[case(4, SerialType::i32())]
    #[case(5, SerialType::i48())]
    #[case(6, SerialType::i64())]
    #[case(7, SerialType::f64())]
    #[case(8, SerialType::const_int0())]
    #[case(9, SerialType::const_int1())]
    #[case(12, SerialType::blob(0))]
    #[case(13, SerialType::text(0))]
    #[case(14, SerialType::blob(1))]
    #[case(15, SerialType::text(1))]
    fn test_parse_serial_type(#[case] input: u64, #[case] expected: SerialType) {
        let result = SerialType::try_from(input).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_validate_serial_type() {
        for i in 0..=9 {
            let result = validate_serial_type(i);
            assert!(result.is_ok());
        }
        for i in 10..=11 {
            let result = validate_serial_type(i);
            assert!(result.is_err());
        }
        for i in 12..=1000 {
            let result = validate_serial_type(i);
            assert!(result.is_ok());
        }
    }

    #[rstest]
    #[case(&[])] // empty buffer
    #[case(&[0x80])] // truncated 1-byte with continuation
    #[case(&[0x80, 0x80])] // truncated 2-byte
    #[case(&[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80])] // 9-byte truncated to 8
    #[case(&[0x80; 9])] // bits set without end
    fn test_read_varint_malformed_inputs(#[case] buf: &[u8]) {
        assert!(read_varint(buf).is_err());
    }

    #[test]
    fn streaming_reader_ignores_uncommitted_checksums() {
        let io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
        let file = io
            .open_file("streaming-reader-wal", crate::OpenFlags::Create, false)
            .unwrap();

        let page_size: usize = 1024;
        let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
        buffer_pool
            .finalize_with_page_size(page_size)
            .expect("initialize buffer pool");

        let mut wal_header = WalHeader {
            magic: WAL_MAGIC_LE,
            file_format: 3007000,
            page_size: page_size as u32,
            checkpoint_seq: 0,
            salt_1: 0x1234_5678,
            salt_2: 0x9abc_def0,
            checksum_1: 0,
            checksum_2: 0,
        };
        let header_prefix = &wal_header.as_bytes()[..WAL_HEADER_SIZE - 8];
        let use_native = (wal_header.magic & 1) != 0;
        let (c1, c2) = checksum_wal(header_prefix, &wal_header, (0, 0), use_native);
        wal_header.checksum_1 = c1;
        wal_header.checksum_2 = c2;
        io.wait_for_completion(begin_write_wal_header(file.as_ref(), &wal_header, None).unwrap())
            .unwrap();

        let page = vec![0xAB; page_size];
        let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
        let mut offset = WAL_HEADER_SIZE as u64;

        let (commit_checksum, commit_frame) = prepare_wal_frame(
            &buffer_pool,
            &wal_header,
            (wal_header.checksum_1, wal_header.checksum_2),
            wal_header.page_size,
            1,
            1,
            &page,
        );
        let commit_frame_clone = commit_frame.clone();
        let c = file
            .pwrite(
                offset,
                commit_frame,
                Completion::new_write(move |res| {
                    assert_eq!(res.unwrap() as usize, frame_size);
                    let _keep = commit_frame_clone.clone();
                }),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();
        offset += frame_size as u64;

        let (after_frame2_checksum, frame2) = prepare_wal_frame(
            &buffer_pool,
            &wal_header,
            commit_checksum,
            wal_header.page_size,
            2,
            0,
            &page,
        );
        let frame2_clone = frame2.clone();
        let c = file
            .pwrite(
                offset,
                frame2,
                Completion::new_write(move |res| {
                    assert_eq!(res.unwrap() as usize, frame_size);
                    let _keep = frame2_clone.clone();
                }),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();
        offset += frame_size as u64;

        let (after_frame3_checksum, frame3) = prepare_wal_frame(
            &buffer_pool,
            &wal_header,
            after_frame2_checksum,
            wal_header.page_size,
            3,
            0,
            &page,
        );
        let frame3_clone = frame3.clone();
        let c = file
            .pwrite(
                offset,
                frame3,
                Completion::new_write(move |res| {
                    assert_eq!(res.unwrap() as usize, frame_size);
                    let _keep = frame3_clone.clone();
                }),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();

        let shared = build_shared_wal(&file, &io).unwrap();
        let guard = shared.read();
        assert_eq!(guard.metadata.max_frame.load(Ordering::Acquire), 1);
        assert_eq!(guard.metadata.last_checksum, commit_checksum);

        // checksum should only include committed frame.
        assert_ne!(guard.metadata.last_checksum, after_frame3_checksum);

        let frame_cache = guard.runtime.frame_cache.lock();
        assert_eq!(frame_cache.get(&1), Some(&vec![1u64]));
        assert!(frame_cache.get(&2).is_none());
    }

    #[quickcheck_macros::quickcheck]
    fn varint_len_matches_write_varint(value: u64) -> bool {
        let mut buf = [0u8; 9];
        let written = write_varint(&mut buf, value);
        varint_len(value) == written
    }
}
