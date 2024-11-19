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

use crate::error::LimboError;
use crate::io::{Buffer, Completion, ReadCompletion, SyncCompletion, WriteCompletion};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::database::DatabaseStorage;
use crate::storage::pager::{Page, Pager};
use crate::types::{OwnedRecord, OwnedValue};
use crate::{File, Result};
use log::trace;
use std::cell::RefCell;
use std::pin::Pin;
use std::rc::Rc;

/// The size of the database header in bytes.
pub const DATABASE_HEADER_SIZE: usize = 100;
// DEFAULT_CACHE_SIZE negative values mean that we store the amount of pages a XKiB of memory can hold.
// We can calculate "real" cache size by diving by page size.
const DEFAULT_CACHE_SIZE: i32 = -2000;
// Minimum number of pages that cache can hold.
pub const MIN_PAGE_CACHE_SIZE: usize = 10;

#[derive(Debug, Clone)]
pub struct DatabaseHeader {
    magic: [u8; 16],
    pub page_size: u16,
    write_version: u8,
    read_version: u8,
    pub unused_space: u8,
    max_embed_frac: u8,
    min_embed_frac: u8,
    min_leaf_frac: u8,
    change_counter: u32,
    pub database_size: u32,
    freelist_trunk_page: u32,
    freelist_pages: u32,
    schema_cookie: u32,
    schema_format: u32,
    pub default_cache_size: i32,
    vacuum: u32,
    text_encoding: u32,
    user_version: u32,
    incremental_vacuum: u32,
    application_id: u32,
    reserved: [u8; 20],
    version_valid_for: u32,
    pub version_number: u32,
}

pub const WAL_HEADER_SIZE: usize = 32;
pub const WAL_FRAME_HEADER_SIZE: usize = 24;

#[derive(Debug, Default)]
pub struct WalHeader {
    pub magic: [u8; 4],
    pub file_format: u32,
    pub page_size: u32,
    pub checkpoint_seq: u32,
    pub salt_1: u32,
    pub salt_2: u32,
    pub checksum_1: u32,
    pub checksum_2: u32,
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct WalFrameHeader {
    page_number: u32,
    db_size: u32,
    salt_1: u32,
    salt_2: u32,
    checksum_1: u32,
    checksum_2: u32,
}

impl Default for DatabaseHeader {
    fn default() -> Self {
        Self {
            magic: *b"SQLite format 3\0",
            page_size: 4096,
            write_version: 2,
            read_version: 2,
            unused_space: 0,
            max_embed_frac: 64,
            min_embed_frac: 32,
            min_leaf_frac: 32,
            change_counter: 1,
            database_size: 1,
            freelist_trunk_page: 0,
            freelist_pages: 0,
            schema_cookie: 0,
            schema_format: 4, // latest format, new sqlite3 databases use this format
            default_cache_size: 500, // pages
            vacuum: 0,
            text_encoding: 1, // utf-8
            user_version: 1,
            incremental_vacuum: 0,
            application_id: 0,
            reserved: [0; 20],
            version_valid_for: 3047000,
            version_number: 3047000,
        }
    }
}

pub fn begin_read_database_header(
    page_io: Rc<dyn DatabaseStorage>,
) -> Result<Rc<RefCell<DatabaseHeader>>> {
    let drop_fn = Rc::new(|_buf| {});
    let buf = Rc::new(RefCell::new(Buffer::allocate(512, drop_fn)));
    let result = Rc::new(RefCell::new(DatabaseHeader::default()));
    let header = result.clone();
    let complete = Box::new(move |buf: Rc<RefCell<Buffer>>| {
        let header = header.clone();
        finish_read_database_header(buf, header).unwrap();
    });
    let c = Rc::new(Completion::Read(ReadCompletion::new(buf, complete)));
    page_io.read_page(1, c.clone())?;
    Ok(result)
}

fn finish_read_database_header(
    buf: Rc<RefCell<Buffer>>,
    header: Rc<RefCell<DatabaseHeader>>,
) -> Result<()> {
    let buf = buf.borrow();
    let buf = buf.as_slice();
    let mut header = std::cell::RefCell::borrow_mut(&header);
    header.magic.copy_from_slice(&buf[0..16]);
    header.page_size = u16::from_be_bytes([buf[16], buf[17]]);
    header.write_version = buf[18];
    header.read_version = buf[19];
    header.unused_space = buf[20];
    header.max_embed_frac = buf[21];
    header.min_embed_frac = buf[22];
    header.min_leaf_frac = buf[23];
    header.change_counter = u32::from_be_bytes([buf[24], buf[25], buf[26], buf[27]]);
    header.database_size = u32::from_be_bytes([buf[28], buf[29], buf[30], buf[31]]);
    header.freelist_trunk_page = u32::from_be_bytes([buf[32], buf[33], buf[34], buf[35]]);
    header.freelist_pages = u32::from_be_bytes([buf[36], buf[37], buf[38], buf[39]]);
    header.schema_cookie = u32::from_be_bytes([buf[40], buf[41], buf[42], buf[43]]);
    header.schema_format = u32::from_be_bytes([buf[44], buf[45], buf[46], buf[47]]);
    header.default_cache_size = i32::from_be_bytes([buf[48], buf[49], buf[50], buf[51]]);
    if header.default_cache_size == 0 {
        header.default_cache_size = DEFAULT_CACHE_SIZE;
    }
    header.vacuum = u32::from_be_bytes([buf[52], buf[53], buf[54], buf[55]]);
    header.text_encoding = u32::from_be_bytes([buf[56], buf[57], buf[58], buf[59]]);
    header.user_version = u32::from_be_bytes([buf[60], buf[61], buf[62], buf[63]]);
    header.incremental_vacuum = u32::from_be_bytes([buf[64], buf[65], buf[66], buf[67]]);
    header.application_id = u32::from_be_bytes([buf[68], buf[69], buf[70], buf[71]]);
    header.reserved.copy_from_slice(&buf[72..92]);
    header.version_valid_for = u32::from_be_bytes([buf[92], buf[93], buf[94], buf[95]]);
    header.version_number = u32::from_be_bytes([buf[96], buf[97], buf[98], buf[99]]);
    Ok(())
}

pub fn begin_write_database_header(header: &DatabaseHeader, pager: &Pager) -> Result<()> {
    let header = Rc::new(header.clone());
    let page_source = pager.page_io.clone();

    let drop_fn = Rc::new(|_buf| {});
    let buffer_to_copy = Rc::new(RefCell::new(Buffer::allocate(512, drop_fn)));
    let buffer_to_copy_in_cb = buffer_to_copy.clone();

    let header_cb = header.clone();
    let complete = Box::new(move |buffer: Rc<RefCell<Buffer>>| {
        let header = header_cb.clone();
        let buffer: Buffer = buffer.borrow().clone();
        let buffer = Rc::new(RefCell::new(buffer));
        {
            let mut buf_mut = std::cell::RefCell::borrow_mut(&buffer);
            let buf = buf_mut.as_mut_slice();
            write_header_to_buf(buf, &header);
            let mut buffer_to_copy = std::cell::RefCell::borrow_mut(&buffer_to_copy_in_cb);
            let buffer_to_copy_slice = buffer_to_copy.as_mut_slice();

            buffer_to_copy_slice.copy_from_slice(buf);
        }
    });

    let drop_fn = Rc::new(|_buf| {});
    let buf = Rc::new(RefCell::new(Buffer::allocate(512, drop_fn)));
    let c = Rc::new(Completion::Read(ReadCompletion::new(buf.clone(), complete)));
    page_source.read_page(1, c.clone())?;
    // run get header block
    pager.io.run_once()?;

    let buffer_in_cb = buffer_to_copy.clone();
    let write_complete = Box::new(move |bytes_written: i32| {
        let buf = buffer_in_cb.clone();
        let buf_len = std::cell::RefCell::borrow(&buf).len();
        if bytes_written < buf_len as i32 {
            log::error!("wrote({bytes_written}) less than expected({buf_len})");
        }
        // finish_read_database_header(buf, header).unwrap();
    });
    let c = Rc::new(Completion::Write(WriteCompletion::new(write_complete)));
    page_source
        .write_page(0, buffer_to_copy.clone(), c)
        .unwrap();

    Ok(())
}

fn write_header_to_buf(buf: &mut [u8], header: &DatabaseHeader) {
    buf[0..16].copy_from_slice(&header.magic);
    buf[16..18].copy_from_slice(&header.page_size.to_be_bytes());
    buf[18] = header.write_version;
    buf[19] = header.read_version;
    buf[20] = header.unused_space;
    buf[21] = header.max_embed_frac;
    buf[22] = header.min_embed_frac;
    buf[23] = header.min_leaf_frac;
    buf[24..28].copy_from_slice(&header.change_counter.to_be_bytes());
    buf[28..32].copy_from_slice(&header.database_size.to_be_bytes());
    buf[32..36].copy_from_slice(&header.freelist_trunk_page.to_be_bytes());
    buf[36..40].copy_from_slice(&header.freelist_pages.to_be_bytes());
    buf[40..44].copy_from_slice(&header.schema_cookie.to_be_bytes());
    buf[44..48].copy_from_slice(&header.schema_format.to_be_bytes());
    buf[48..52].copy_from_slice(&header.default_cache_size.to_be_bytes());

    buf[52..56].copy_from_slice(&header.vacuum.to_be_bytes());
    buf[56..60].copy_from_slice(&header.text_encoding.to_be_bytes());
    buf[60..64].copy_from_slice(&header.user_version.to_be_bytes());
    buf[64..68].copy_from_slice(&header.incremental_vacuum.to_be_bytes());

    buf[68..72].copy_from_slice(&header.application_id.to_be_bytes());
    buf[72..92].copy_from_slice(&header.reserved);
    buf[92..96].copy_from_slice(&header.version_valid_for.to_be_bytes());
    buf[96..100].copy_from_slice(&header.version_number.to_be_bytes());
}

#[repr(u8)]
#[derive(Debug, PartialEq, Clone)]
pub enum PageType {
    IndexInterior = 2,
    TableInterior = 5,
    IndexLeaf = 10,
    TableLeaf = 13,
}

impl TryFrom<u8> for PageType {
    type Error = LimboError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            2 => Ok(Self::IndexInterior),
            5 => Ok(Self::TableInterior),
            10 => Ok(Self::IndexLeaf),
            13 => Ok(Self::TableLeaf),
            _ => Err(LimboError::Corrupt(format!("Invalid page type: {}", value))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OverflowCell {
    pub index: usize,
    pub payload: Pin<Vec<u8>>,
}

#[derive(Debug)]
pub struct PageContent {
    pub offset: usize,
    pub buffer: Rc<RefCell<Buffer>>,
    pub overflow_cells: Vec<OverflowCell>,
}

impl Clone for PageContent {
    fn clone(&self) -> Self {
        Self {
            offset: self.offset,
            buffer: Rc::new(RefCell::new((*self.buffer.borrow()).clone())),
            overflow_cells: self.overflow_cells.clone(),
        }
    }
}

impl PageContent {
    pub fn page_type(&self) -> PageType {
        self.read_u8(0).try_into().unwrap()
    }

    pub fn maybe_page_type(&self) -> Option<PageType> {
        match self.read_u8(0).try_into() {
            Ok(v) => Some(v),
            Err(_) => None, // this could be an overflow page
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn as_ptr(&self) -> &mut [u8] {
        unsafe {
            // unsafe trick to borrow twice
            let buf_pointer = &self.buffer.as_ptr();
            let buf = (*buf_pointer).as_mut().unwrap().as_mut_slice();
            buf
        }
    }

    fn read_u8(&self, pos: usize) -> u8 {
        let buf = self.as_ptr();
        buf[self.offset + pos]
    }

    pub fn read_u16(&self, pos: usize) -> u16 {
        let buf = self.as_ptr();
        u16::from_be_bytes([buf[self.offset + pos], buf[self.offset + pos + 1]])
    }

    fn read_u32(&self, pos: usize) -> u32 {
        let buf = self.as_ptr();
        u32::from_be_bytes([
            buf[self.offset + pos],
            buf[self.offset + pos + 1],
            buf[self.offset + pos + 2],
            buf[self.offset + pos + 3],
        ])
    }

    pub fn write_u8(&self, pos: usize, value: u8) {
        log::debug!("write_u8(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        buf[self.offset + pos] = value;
    }

    pub fn write_u16(&self, pos: usize, value: u16) {
        log::debug!("write_u16(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        buf[self.offset + pos..self.offset + pos + 2].copy_from_slice(&value.to_be_bytes());
    }

    pub fn write_u32(&self, pos: usize, value: u32) {
        log::debug!("write_u32(pos={}, value={})", pos, value);
        let buf = self.as_ptr();
        buf[self.offset + pos..self.offset + pos + 4].copy_from_slice(&value.to_be_bytes());
    }

    pub fn first_freeblock(&self) -> u16 {
        self.read_u16(1)
    }

    pub fn cell_count(&self) -> usize {
        self.read_u16(3) as usize
    }

    pub fn cell_content_area(&self) -> u16 {
        self.read_u16(5)
    }

    pub fn num_frag_free_bytes(&self) -> u8 {
        self.read_u8(7)
    }

    pub fn rightmost_pointer(&self) -> Option<u32> {
        match self.page_type() {
            PageType::IndexInterior => Some(self.read_u32(8)),
            PageType::TableInterior => Some(self.read_u32(8)),
            PageType::IndexLeaf => None,
            PageType::TableLeaf => None,
        }
    }

    pub fn cell_get(
        &self,
        idx: usize,
        pager: Rc<Pager>,
        max_local: usize,
        min_local: usize,
        usable_size: usize,
    ) -> Result<BTreeCell> {
        log::debug!("cell_get(idx={})", idx);
        let buf = self.as_ptr();

        let ncells = self.cell_count();
        let cell_start = match self.page_type() {
            PageType::IndexInterior => 12,
            PageType::TableInterior => 12,
            PageType::IndexLeaf => 8,
            PageType::TableLeaf => 8,
        };
        assert!(idx < ncells, "cell_get: idx out of bounds");
        let cell_pointer = cell_start + (idx * 2);
        let cell_pointer = self.read_u16(cell_pointer) as usize;

        read_btree_cell(
            buf,
            &self.page_type(),
            cell_pointer,
            pager,
            max_local,
            min_local,
            usable_size,
        )
    }

    /// When using this fu
    pub fn cell_get_raw_pointer_region(&self) -> (usize, usize) {
        let cell_start = match self.page_type() {
            PageType::IndexInterior => 12,
            PageType::TableInterior => 12,
            PageType::IndexLeaf => 8,
            PageType::TableLeaf => 8,
        };
        (self.offset + cell_start, self.cell_count() * 2)
    }

    /* Get region of a cell's payload */
    pub fn cell_get_raw_region(
        &self,
        idx: usize,
        max_local: usize,
        min_local: usize,
        usable_size: usize,
    ) -> (usize, usize) {
        let buf = self.as_ptr();
        let ncells = self.cell_count();
        let cell_start = match self.page_type() {
            PageType::IndexInterior => 12,
            PageType::TableInterior => 12,
            PageType::IndexLeaf => 8,
            PageType::TableLeaf => 8,
        };
        assert!(idx < ncells, "cell_get: idx out of bounds");
        let cell_pointer = cell_start + (idx * 2);
        let cell_pointer = self.read_u16(cell_pointer) as usize;
        let start = cell_pointer;
        let len = match self.page_type() {
            PageType::IndexInterior => {
                let (len_payload, n_payload) = read_varint(&buf[cell_pointer + 4..]).unwrap();
                let (overflows, to_read) =
                    payload_overflows(len_payload as usize, max_local, min_local, usable_size);
                if overflows {
                    4 + to_read + n_payload + 4
                } else {
                    4 + len_payload as usize + n_payload + 4
                }
            }
            PageType::TableInterior => {
                let (_, n_rowid) = read_varint(&buf[cell_pointer + 4..]).unwrap();
                4 + n_rowid
            }
            PageType::IndexLeaf => {
                let (len_payload, n_payload) = read_varint(&buf[cell_pointer..]).unwrap();
                let (overflows, to_read) =
                    payload_overflows(len_payload as usize, max_local, min_local, usable_size);
                if overflows {
                    to_read + n_payload + 4
                } else {
                    len_payload as usize + n_payload + 4
                }
            }
            PageType::TableLeaf => {
                let (len_payload, n_payload) = read_varint(&buf[cell_pointer..]).unwrap();
                let (_, n_rowid) = read_varint(&buf[cell_pointer + n_payload..]).unwrap();
                let (overflows, to_read) =
                    payload_overflows(len_payload as usize, max_local, min_local, usable_size);
                if overflows {
                    to_read + n_payload + n_rowid
                } else {
                    len_payload as usize + n_payload + n_rowid
                }
            }
        };
        (start, len)
    }

    pub fn is_leaf(&self) -> bool {
        match self.page_type() {
            PageType::IndexInterior => false,
            PageType::TableInterior => false,
            PageType::IndexLeaf => true,
            PageType::TableLeaf => true,
        }
    }

    pub fn write_database_header(&self, header: &DatabaseHeader) {
        let buf = self.as_ptr();
        write_header_to_buf(buf, header);
    }
}

pub fn begin_read_page(
    page_io: Rc<dyn DatabaseStorage>,
    buffer_pool: Rc<BufferPool>,
    page: Rc<RefCell<Page>>,
    page_idx: usize,
) -> Result<()> {
    trace!("begin_read_btree_page(page_idx = {})", page_idx);
    let buf = buffer_pool.get();
    let drop_fn = Rc::new(move |buf| {
        let buffer_pool = buffer_pool.clone();
        buffer_pool.put(buf);
    });
    let buf = Rc::new(RefCell::new(Buffer::new(buf, drop_fn)));
    let complete = Box::new(move |buf: Rc<RefCell<Buffer>>| {
        let page = page.clone();
        if finish_read_page(page_idx, buf, page.clone()).is_err() {
            page.borrow_mut().set_error();
        }
    });
    let c = Rc::new(Completion::Read(ReadCompletion::new(buf, complete)));
    page_io.read_page(page_idx, c.clone())?;
    Ok(())
}

fn finish_read_page(
    page_idx: usize,
    buffer_ref: Rc<RefCell<Buffer>>,
    page: Rc<RefCell<Page>>,
) -> Result<()> {
    trace!("finish_read_btree_page(page_idx = {})", page_idx);
    let pos = if page_idx == 1 {
        DATABASE_HEADER_SIZE
    } else {
        0
    };
    let inner = PageContent {
        offset: pos,
        buffer: buffer_ref.clone(),
        overflow_cells: Vec::new(),
    };
    {
        let mut page = page.borrow_mut();
        page.contents.replace(inner);
        page.set_uptodate();
        page.clear_locked();
        page.set_loaded();
    }
    Ok(())
}

pub fn begin_write_btree_page(
    pager: &Pager,
    page: &Rc<RefCell<Page>>,
    write_counter: Rc<RefCell<usize>>,
) -> Result<()> {
    let page_source = &pager.page_io;
    let page_finish = page.clone();

    let page_id = page.borrow().id;
    log::trace!("begin_write_btree_page(page_id={})", page_id);
    let buffer = {
        let page = page.borrow();
        let contents = page.contents.as_ref().unwrap();
        contents.buffer.clone()
    };

    *write_counter.borrow_mut() += 1;
    let write_complete = {
        let buf_copy = buffer.clone();
        Box::new(move |bytes_written: i32| {
            log::trace!("finish_write_btree_page");
            let buf_copy = buf_copy.clone();
            let buf_len = buf_copy.borrow().len();
            *write_counter.borrow_mut() -= 1;

            page_finish.borrow_mut().clear_dirty();
            if bytes_written < buf_len as i32 {
                log::error!("wrote({bytes_written}) less than expected({buf_len})");
            }
        })
    };
    let c = Rc::new(Completion::Write(WriteCompletion::new(write_complete)));
    page_source.write_page(page_id, buffer.clone(), c)?;
    Ok(())
}

pub fn begin_sync(page_io: Rc<dyn DatabaseStorage>, syncing: Rc<RefCell<bool>>) -> Result<()> {
    assert!(!*syncing.borrow());
    *syncing.borrow_mut() = true;
    let completion = Completion::Sync(SyncCompletion {
        complete: Box::new(move |_| {
            *syncing.borrow_mut() = false;
        }),
    });
    page_io.sync(Rc::new(completion))?;
    Ok(())
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
    pub _left_child_page: u32,
    pub _rowid: u64,
}

#[derive(Debug, Clone)]
pub struct TableLeafCell {
    pub _rowid: u64,
    pub _payload: Vec<u8>,
    pub first_overflow_page: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct IndexInteriorCell {
    pub left_child_page: u32,
    pub payload: Vec<u8>,
    pub first_overflow_page: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct IndexLeafCell {
    pub payload: Vec<u8>,
    pub first_overflow_page: Option<u32>,
}

pub fn read_btree_cell(
    page: &[u8],
    page_type: &PageType,
    pos: usize,
    pager: Rc<Pager>,
    max_local: usize,
    min_local: usize,
    usable_size: usize,
) -> Result<BTreeCell> {
    match page_type {
        PageType::IndexInterior => {
            let mut pos = pos;
            let left_child_page =
                u32::from_be_bytes([page[pos], page[pos + 1], page[pos + 2], page[pos + 3]]);
            pos += 4;
            let (payload_size, nr) = read_varint(&page[pos..])?;
            pos += nr;

            let (overflows, to_read) =
                payload_overflows(payload_size as usize, max_local, min_local, usable_size);
            let to_read = if overflows { to_read } else { page.len() - pos };

            let (payload, first_overflow_page) =
                read_payload(&page[pos..pos + to_read], payload_size as usize, pager);
            Ok(BTreeCell::IndexInteriorCell(IndexInteriorCell {
                left_child_page,
                payload,
                first_overflow_page,
            }))
        }
        PageType::TableInterior => {
            let mut pos = pos;
            let left_child_page =
                u32::from_be_bytes([page[pos], page[pos + 1], page[pos + 2], page[pos + 3]]);
            pos += 4;
            let (rowid, _) = read_varint(&page[pos..])?;
            Ok(BTreeCell::TableInteriorCell(TableInteriorCell {
                _left_child_page: left_child_page,
                _rowid: rowid,
            }))
        }
        PageType::IndexLeaf => {
            let mut pos = pos;
            let (payload_size, nr) = read_varint(&page[pos..])?;
            pos += nr;

            let (overflows, to_read) =
                payload_overflows(payload_size as usize, max_local, min_local, usable_size);
            let to_read = if overflows { to_read } else { page.len() - pos };

            let (payload, first_overflow_page) =
                read_payload(&page[pos..pos + to_read], payload_size as usize, pager);
            Ok(BTreeCell::IndexLeafCell(IndexLeafCell {
                payload,
                first_overflow_page,
            }))
        }
        PageType::TableLeaf => {
            let mut pos = pos;
            let (payload_size, nr) = read_varint(&page[pos..])?;
            pos += nr;
            let (rowid, nr) = read_varint(&page[pos..])?;
            pos += nr;

            let (overflows, to_read) =
                payload_overflows(payload_size as usize, max_local, min_local, usable_size);
            let to_read = if overflows { to_read } else { page.len() - pos };

            let (payload, first_overflow_page) =
                read_payload(&page[pos..pos + to_read], payload_size as usize, pager);
            Ok(BTreeCell::TableLeafCell(TableLeafCell {
                _rowid: rowid,
                _payload: payload,
                first_overflow_page,
            }))
        }
    }
}

/// read_payload takes in the unread bytearray with the payload size
/// and returns the payload on the page, and optionally the first overflow page number.
#[allow(clippy::readonly_write_lock)]
fn read_payload(unread: &[u8], payload_size: usize, pager: Rc<Pager>) -> (Vec<u8>, Option<u32>) {
    let cell_len = unread.len();
    if payload_size <= cell_len {
        // fit within 1 page
        (unread[..payload_size].to_vec(), None)
    } else {
        // overflow
        let first_overflow_page = u32::from_be_bytes([
            unread[cell_len - 4],
            unread[cell_len - 3],
            unread[cell_len - 2],
            unread[cell_len - 1],
        ]);
        let usable_size = pager.usable_size();
        let mut next_overflow = first_overflow_page;
        let mut payload = unread[..cell_len - 4].to_vec();
        let mut left_to_read = payload_size - (cell_len - 4); // minus four because last for bytes of a payload cell are the overflow pointer
        while next_overflow != 0 {
            assert!(left_to_read > 0);
            let page;
            loop {
                let page_ref = pager.read_page(next_overflow as usize);
                if let Ok(p) = page_ref {
                    page = p;
                    break;
                }
            }
            let mut page = page.borrow_mut();
            let contents = page.contents.as_mut().unwrap();

            let to_read = left_to_read.min(usable_size - 4);
            let buf = contents.as_ptr();
            payload.extend_from_slice(&buf[4..4 + to_read]);

            next_overflow = contents.read_u32(0);
            left_to_read -= to_read;
        }
        assert_eq!(left_to_read, 0);

        (payload, Some(first_overflow_page))
    }
}

#[derive(Debug, PartialEq)]
pub enum SerialType {
    Null,
    UInt8,
    BEInt16,
    BEInt24,
    BEInt32,
    BEInt48,
    BEInt64,
    BEFloat64,
    ConstInt0,
    ConstInt1,
    Blob(usize),
    String(usize),
}

impl TryFrom<u64> for SerialType {
    type Error = crate::error::LimboError;

    fn try_from(value: u64) -> Result<Self> {
        match value {
            0 => Ok(Self::Null),
            1 => Ok(Self::UInt8),
            2 => Ok(Self::BEInt16),
            3 => Ok(Self::BEInt24),
            4 => Ok(Self::BEInt32),
            5 => Ok(Self::BEInt48),
            6 => Ok(Self::BEInt64),
            7 => Ok(Self::BEFloat64),
            8 => Ok(Self::ConstInt0),
            9 => Ok(Self::ConstInt1),
            n if value >= 12 && value % 2 == 0 => Ok(Self::Blob(((n - 12) / 2) as usize)),
            n if value >= 13 && value % 2 == 1 => Ok(Self::String(((n - 13) / 2) as usize)),
            _ => crate::bail_corrupt_error!("Invalid serial type: {}", value),
        }
    }
}

pub fn read_record(payload: &[u8]) -> Result<OwnedRecord> {
    let mut pos = 0;
    let (header_size, nr) = read_varint(payload)?;
    assert!((header_size as usize) >= nr);
    let mut header_size = (header_size as usize) - nr;
    pos += nr;
    let mut serial_types = Vec::with_capacity(header_size);
    while header_size > 0 {
        let (serial_type, nr) = read_varint(&payload[pos..])?;
        let serial_type = SerialType::try_from(serial_type)?;
        serial_types.push(serial_type);
        pos += nr;
        assert!(header_size >= nr);
        header_size -= nr;
    }
    let mut values = Vec::with_capacity(serial_types.len());
    for serial_type in &serial_types {
        let (value, n) = read_value(&payload[pos..], serial_type)?;
        pos += n;
        values.push(value);
    }
    Ok(OwnedRecord::new(values))
}

pub fn read_value(buf: &[u8], serial_type: &SerialType) -> Result<(OwnedValue, usize)> {
    match *serial_type {
        SerialType::Null => Ok((OwnedValue::Null, 0)),
        SerialType::UInt8 => {
            if buf.is_empty() {
                crate::bail_corrupt_error!("Invalid UInt8 value");
            }
            Ok((OwnedValue::Integer(buf[0] as i64), 1))
        }
        SerialType::BEInt16 => {
            if buf.len() < 2 {
                crate::bail_corrupt_error!("Invalid BEInt16 value");
            }
            Ok((
                OwnedValue::Integer(i16::from_be_bytes([buf[0], buf[1]]) as i64),
                2,
            ))
        }
        SerialType::BEInt24 => {
            if buf.len() < 3 {
                crate::bail_corrupt_error!("Invalid BEInt24 value");
            }
            Ok((
                OwnedValue::Integer(i32::from_be_bytes([0, buf[0], buf[1], buf[2]]) as i64),
                3,
            ))
        }
        SerialType::BEInt32 => {
            if buf.len() < 4 {
                crate::bail_corrupt_error!("Invalid BEInt32 value");
            }
            Ok((
                OwnedValue::Integer(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as i64),
                4,
            ))
        }
        SerialType::BEInt48 => {
            if buf.len() < 6 {
                crate::bail_corrupt_error!("Invalid BEInt48 value");
            }
            Ok((
                OwnedValue::Integer(i64::from_be_bytes([
                    0, 0, buf[0], buf[1], buf[2], buf[3], buf[4], buf[5],
                ])),
                6,
            ))
        }
        SerialType::BEInt64 => {
            if buf.len() < 8 {
                crate::bail_corrupt_error!("Invalid BEInt64 value");
            }
            Ok((
                OwnedValue::Integer(i64::from_be_bytes([
                    buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
                ])),
                8,
            ))
        }
        SerialType::BEFloat64 => {
            if buf.len() < 8 {
                crate::bail_corrupt_error!("Invalid BEFloat64 value");
            }
            Ok((
                OwnedValue::Float(f64::from_be_bytes([
                    buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
                ])),
                8,
            ))
        }
        SerialType::ConstInt0 => Ok((OwnedValue::Integer(0), 0)),
        SerialType::ConstInt1 => Ok((OwnedValue::Integer(1), 0)),
        SerialType::Blob(n) => {
            if buf.len() < n {
                crate::bail_corrupt_error!("Invalid Blob value");
            }
            Ok((OwnedValue::Blob(buf[0..n].to_vec().into()), n))
        }
        SerialType::String(n) => {
            if buf.len() < n {
                crate::bail_corrupt_error!(
                    "Invalid String value, length {} < expected length {}",
                    buf.len(),
                    n
                );
            }
            let bytes = buf[0..n].to_vec();
            let value = unsafe { String::from_utf8_unchecked(bytes) };
            Ok((OwnedValue::Text(value.into()), n))
        }
    }
}

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
                crate::bail_corrupt_error!("Invalid varint");
            }
        }
    }
    v = (v << 8) + buf[8] as u64;
    Ok((v, 9))
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

    let mut encoded: [u8; 10] = [0; 10];
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
    let mut varint: Vec<u8> = vec![0; 9];
    let n = write_varint(&mut varint.as_mut_slice()[0..9], value);
    write_varint(&mut varint, value);
    varint.truncate(n);
    payload.extend_from_slice(&varint);
}

pub fn begin_read_wal_header(io: &Rc<dyn File>) -> Result<Rc<RefCell<WalHeader>>> {
    let drop_fn = Rc::new(|_buf| {});
    let buf = Rc::new(RefCell::new(Buffer::allocate(512, drop_fn)));
    let result = Rc::new(RefCell::new(WalHeader::default()));
    let header = result.clone();
    let complete = Box::new(move |buf: Rc<RefCell<Buffer>>| {
        let header = header.clone();
        finish_read_wal_header(buf, header).unwrap();
    });
    let c = Rc::new(Completion::Read(ReadCompletion::new(buf, complete)));
    io.pread(0, c)?;
    Ok(result)
}

fn finish_read_wal_header(buf: Rc<RefCell<Buffer>>, header: Rc<RefCell<WalHeader>>) -> Result<()> {
    let buf = buf.borrow();
    let buf = buf.as_slice();
    let mut header = header.borrow_mut();
    header.magic.copy_from_slice(&buf[0..4]);
    header.file_format = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    header.page_size = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
    header.checkpoint_seq = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
    header.salt_1 = u32::from_be_bytes([buf[16], buf[17], buf[18], buf[19]]);
    header.salt_2 = u32::from_be_bytes([buf[20], buf[21], buf[22], buf[23]]);
    header.checksum_1 = u32::from_be_bytes([buf[24], buf[25], buf[26], buf[27]]);
    header.checksum_2 = u32::from_be_bytes([buf[28], buf[29], buf[30], buf[31]]);
    Ok(())
}

pub fn begin_read_wal_frame(
    io: &Rc<dyn File>,
    offset: usize,
    buffer_pool: Rc<BufferPool>,
    page: Rc<RefCell<Page>>,
) -> Result<()> {
    let buf = buffer_pool.get();
    let drop_fn = Rc::new(move |buf| {
        let buffer_pool = buffer_pool.clone();
        buffer_pool.put(buf);
    });
    let buf = Rc::new(RefCell::new(Buffer::new(buf, drop_fn)));
    let frame = page.clone();
    let complete = Box::new(move |buf: Rc<RefCell<Buffer>>| {
        let frame = frame.clone();
        finish_read_page(2, buf, frame).unwrap();
    });
    let c = Rc::new(Completion::Read(ReadCompletion::new(buf, complete)));
    io.pread(offset, c)?;
    Ok(())
}

pub fn begin_write_wal_frame(
    io: &Rc<dyn File>,
    offset: usize,
    page: &Rc<RefCell<Page>>,
    db_size: u32,
    write_counter: Rc<RefCell<usize>>,
) -> Result<()> {
    let page_finish = page.clone();
    let page_id = page.borrow().id;
    trace!("begin_write_wal_frame(offset={}, page={})", offset, page_id);

    let header = WalFrameHeader {
        page_number: page_id as u32,
        db_size,
        salt_1: 0,
        salt_2: 0,
        checksum_1: 0,
        checksum_2: 0,
    };
    let buffer = {
        let page = page.borrow();
        let contents = page.contents.as_ref().unwrap();
        let drop_fn = Rc::new(|_buf| {});

        let mut buffer = Buffer::allocate(
            contents.buffer.borrow().len() + WAL_FRAME_HEADER_SIZE,
            drop_fn,
        );
        let buf = buffer.as_mut_slice();

        buf[0..4].copy_from_slice(&header.page_number.to_be_bytes());
        buf[4..8].copy_from_slice(&header.db_size.to_be_bytes());
        buf[8..12].copy_from_slice(&header.salt_1.to_be_bytes());
        buf[12..16].copy_from_slice(&header.salt_2.to_be_bytes());
        buf[16..20].copy_from_slice(&header.checksum_1.to_be_bytes());
        buf[20..24].copy_from_slice(&header.checksum_2.to_be_bytes());
        buf[WAL_FRAME_HEADER_SIZE..].copy_from_slice(contents.as_ptr());

        Rc::new(RefCell::new(buffer))
    };

    *write_counter.borrow_mut() += 1;
    let write_complete = {
        let buf_copy = buffer.clone();
        log::info!("finished");
        Box::new(move |bytes_written: i32| {
            let buf_copy = buf_copy.clone();
            let buf_len = buf_copy.borrow().len();
            *write_counter.borrow_mut() -= 1;

            page_finish.borrow_mut().clear_dirty();
            if bytes_written < buf_len as i32 {
                log::error!("wrote({bytes_written}) less than expected({buf_len})");
            }
        })
    };
    let c = Rc::new(Completion::Write(WriteCompletion::new(write_complete)));
    io.pwrite(offset, buffer.clone(), c)?;
    Ok(())
}

pub fn begin_write_wal_header(io: &Rc<dyn File>, header: &WalHeader) -> Result<()> {
    let buffer = {
        let drop_fn = Rc::new(|_buf| {});

        let mut buffer = Buffer::allocate(512, drop_fn);
        let buf = buffer.as_mut_slice();

        buf[0..4].copy_from_slice(&header.magic);
        buf[4..8].copy_from_slice(&header.file_format.to_be_bytes());
        buf[8..12].copy_from_slice(&header.page_size.to_be_bytes());
        buf[12..16].copy_from_slice(&header.checkpoint_seq.to_be_bytes());
        buf[16..20].copy_from_slice(&header.salt_1.to_be_bytes());
        buf[20..24].copy_from_slice(&header.salt_2.to_be_bytes());
        buf[24..28].copy_from_slice(&header.checksum_1.to_be_bytes());
        buf[28..32].copy_from_slice(&header.checksum_2.to_be_bytes());

        Rc::new(RefCell::new(buffer))
    };

    let write_complete = {
        Box::new(move |bytes_written: i32| {
            if bytes_written < WAL_HEADER_SIZE as i32 {
                log::error!(
                    "wal header wrote({bytes_written}) less than expected({WAL_HEADER_SIZE})"
                );
            }
        })
    };
    let c = Rc::new(Completion::Write(WriteCompletion::new(write_complete)));
    io.pwrite(0, buffer.clone(), c)?;
    Ok(())
}

/*
    Checks if payload will overflow a cell based on max local and
    it will return the min size that will be stored in that case,
    including overflow pointer
*/
pub fn payload_overflows(
    payload_size: usize,
    max_local: usize,
    min_local: usize,
    usable_size: usize,
) -> (bool, usize) {
    if payload_size <= max_local {
        return (false, 0);
    }

    let mut space_left = min_local + (payload_size - min_local) % (usable_size - 4);
    if space_left > max_local {
        space_left = min_local;
    }
    (true, space_left + 4)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(0, SerialType::Null)]
    #[case(1, SerialType::UInt8)]
    #[case(2, SerialType::BEInt16)]
    #[case(3, SerialType::BEInt24)]
    #[case(4, SerialType::BEInt32)]
    #[case(5, SerialType::BEInt48)]
    #[case(6, SerialType::BEInt64)]
    #[case(7, SerialType::BEFloat64)]
    #[case(8, SerialType::ConstInt0)]
    #[case(9, SerialType::ConstInt1)]
    #[case(12, SerialType::Blob(0))]
    #[case(13, SerialType::String(0))]
    #[case(14, SerialType::Blob(1))]
    #[case(15, SerialType::String(1))]
    fn test_read_serial_type(#[case] input: u64, #[case] expected: SerialType) {
        let result = SerialType::try_from(input).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_read_invalid_serial_type() {
        let result = SerialType::try_from(10);
        assert!(result.is_err());
    }

    #[rstest]
    #[case(&[], SerialType::Null, OwnedValue::Null)]
    #[case(&[255], SerialType::UInt8, OwnedValue::Integer(255))]
    #[case(&[0x12, 0x34], SerialType::BEInt16, OwnedValue::Integer(0x1234))]
    #[case(&[0x12, 0x34, 0x56], SerialType::BEInt24, OwnedValue::Integer(0x123456))]
    #[case(&[0x12, 0x34, 0x56, 0x78], SerialType::BEInt32, OwnedValue::Integer(0x12345678))]
    #[case(&[0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC], SerialType::BEInt48, OwnedValue::Integer(0x123456789ABC))]
    #[case(&[0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xFF], SerialType::BEInt64, OwnedValue::Integer(0x123456789ABCDEFF))]
    #[case(&[64, 9, 33, 251, 84, 68, 45, 24], SerialType::BEFloat64, OwnedValue::Float(std::f64::consts::PI))]
    #[case(&[], SerialType::ConstInt0, OwnedValue::Integer(0))]
    #[case(&[], SerialType::ConstInt1, OwnedValue::Integer(1))]
    #[case(&[1, 2, 3], SerialType::Blob(3), OwnedValue::Blob(vec![1, 2, 3].into()))]
    #[case(&[65, 66, 67], SerialType::String(3), OwnedValue::Text("ABC".to_string().into()))]
    fn test_read_value(
        #[case] buf: &[u8],
        #[case] serial_type: SerialType,
        #[case] expected: OwnedValue,
    ) {
        let result = read_value(buf, &serial_type).unwrap();
        assert_eq!(result, (expected, buf.len()));
    }

    #[rstest]
    #[case(&[], SerialType::UInt8)]
    #[case(&[0x12], SerialType::BEInt16)]
    #[case(&[0x12, 0x34], SerialType::BEInt24)]
    #[case(&[0x12, 0x34, 0x56], SerialType::BEInt32)]
    #[case(&[0x12, 0x34, 0x56, 0x78], SerialType::BEInt48)]
    #[case(&[0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE], SerialType::BEInt64)]
    #[case(&[64, 9, 33, 251, 84, 68, 45], SerialType::BEFloat64)]
    #[case(&[1, 2], SerialType::Blob(3))]
    #[case(&[65, 66], SerialType::String(3))]
    // TODO: UTF-8 validation is disabled #[case(&[192], SerialType::String(1))] // invalid UTF-8 sequence
    fn test_read_invalid_value(#[case] buf: &[u8], #[case] serial_type: SerialType) {
        let result = read_value(buf, &serial_type);
        assert!(result.is_err());
    }

    #[rstest]
    #[case(&[0x01], (1, 1))]
    #[case(&[0x81, 0x01], (129, 2))]
    #[case(&[0x81, 0x81, 0x01], (16513, 3))]
    #[case(&[0x81, 0x81, 0x81, 0x01], (2113665, 4))]
    #[case(&[0x81, 0x81, 0x81, 0x81, 0x01], (270549121, 5))]
    #[case(&[0x81, 0x81, 0x81, 0x81, 0x81, 0x01], (34630287489, 6))]
    #[case(&[0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x01], (4432676798593, 7))]
    #[case(&[0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x01], (567382630219905, 8))]
    #[case(&[0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x01], (145249953336295681, 9))]
    fn read_varint_test(#[case] input: &[u8], #[case] expected: (u64, usize)) {
        let result = read_varint(input).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_read_invalid_varint() {
        let buf = [0b11111110];
        let result = read_varint(&buf);
        assert!(result.is_err());
    }

    // **    0x00                      becomes  0x00000000
    // **    0x7f                      becomes  0x0000007f
    // **    0x81 0x00                 becomes  0x00000080
    // **    0x82 0x00                 becomes  0x00000100
    // **    0x80 0x7f                 becomes  0x0000007f
    // **    0x81 0x91 0xd1 0xac 0x78  becomes  0x12345678
    // **    0x81 0x81 0x81 0x81 0x01  becomes  0x10204081
    #[rstest]
    #[case((0, 1), &[0x00])]
    #[case((1, 1), &[0x01])]
    #[case((129, 2), &[0x81, 0x01] )]
    #[case((16513, 3), &[0x81, 0x81, 0x01] )]
    #[case((2113665, 4), &[0x81, 0x81, 0x81, 0x01] )]
    #[case((270549121, 5), &[0x81, 0x81, 0x81, 0x81, 0x01] )]
    #[case((34630287489, 6), &[0x81, 0x81, 0x81, 0x81, 0x81, 0x01] )]
    #[case((4432676798593, 7), &[0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x01] )]
    #[case((567382630219905, 8), &[0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x01] )]
    #[case((145249953336295681, 9), &[0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x01] )]
    fn test_write_varint(#[case] value: (u64, usize), #[case] output: &[u8]) {
        let mut buf: [u8; 10] = [0; 10];
        let n = write_varint(&mut buf, value.0);
        assert_eq!(n, value.1);
        for i in 0..output.len() {
            assert_eq!(buf[i], output[i]);
        }
    }
}
