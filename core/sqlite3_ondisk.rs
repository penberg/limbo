/// SQLite on-disk file format.
///
/// SQLite stores data in a single database file, which is divided into fixed-size
/// pages:
///
/// ```text
/// +----------+----------+----------+-----------------------------+----------+
/// |          |          |          |                             |          |
/// |  Page 1  |  Page 2  |  Page 3  |           ...               |  Page N  |
/// |          |          |          |                             |          |
/// +----------+----------+----------+-----------------------------+----------+
/// ```
///
/// The first page is special because it contains a 100 byte header at the beginning.
///
/// Each page constists of a page header and N cells, which contain the records.
///
/// +-----------------+----------------+---------------------+----------------+
/// |                 |                |                     |                |
/// |   Page header   |  Cell pointer  |     Unallocated     |  Cell content  |
/// | (8 or 12 bytes) |     array      |        space        |      area      |      
/// |                 |                |                     |                |
/// +-----------------+----------------+---------------------+----------------+
///
/// For more information, see: https://www.sqlite.org/fileformat.html
use crate::buffer_pool::BufferPool;
use crate::io::{Buffer, Completion};
use crate::pager::Page;
use crate::types::{OwnedRecord, OwnedValue};
use crate::PageSource;
use anyhow::{anyhow, Result};
use log::trace;
use std::cell::RefCell;
use std::sync::Arc;

/// The size of the database header in bytes.
pub const DATABASE_HEADER_SIZE: usize = 100;

#[derive(Debug, Default)]
pub struct DatabaseHeader {
    magic: [u8; 16],
    pub page_size: u16,
    write_version: u8,
    read_version: u8,
    unused_space: u8,
    max_embed_frac: u8,
    min_embed_frac: u8,
    min_leaf_frac: u8,
    change_counter: u32,
    database_size: u32,
    freelist_trunk_page: u32,
    freelist_pages: u32,
    schema_cookie: u32,
    schema_format: u32,
    default_cache_size: u32,
    vacuum: u32,
    text_encoding: u32,
    user_version: u32,
    incremental_vacuum: u32,
    application_id: u32,
    reserved: [u8; 20],
    version_valid_for: u32,
    version_number: u32,
}

pub fn begin_read_database_header(
    page_source: &PageSource,
) -> Result<Arc<RefCell<DatabaseHeader>>> {
    let drop_fn = Arc::new(|_buf| {});
    let buf = Buffer::allocate(512, drop_fn);
    let result = Arc::new(RefCell::new(DatabaseHeader::default()));
    let header = result.clone();
    let complete = Box::new(move |buf: &Buffer| {
        let header = header.clone();
        finish_read_database_header(buf, header).unwrap();
    });
    let c = Arc::new(Completion::new(buf, complete));
    page_source.get(1, c.clone())?;
    Ok(result)
}

fn finish_read_database_header(buf: &Buffer, header: Arc<RefCell<DatabaseHeader>>) -> Result<()> {
    let buf = buf.as_slice();
    let mut header = header.borrow_mut();
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
    header.default_cache_size = u32::from_be_bytes([buf[48], buf[49], buf[50], buf[51]]);
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

#[derive(Debug)]
pub struct BTreePageHeader {
    page_type: PageType,
    _first_freeblock_offset: u16,
    num_cells: u16,
    _cell_content_area: u16,
    _num_frag_free_bytes: u8,
    pub(crate) right_most_pointer: Option<u32>,
}

#[repr(u8)]
#[derive(Debug, PartialEq)]
pub enum PageType {
    IndexInterior = 2,
    TableInterior = 5,
    IndexLeaf = 10,
    TableLeaf = 13,
}

impl TryFrom<u8> for PageType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            2 => Ok(Self::IndexInterior),
            5 => Ok(Self::TableInterior),
            10 => Ok(Self::IndexLeaf),
            13 => Ok(Self::TableLeaf),
            _ => Err(anyhow!("Invalid page type: {}", value)),
        }
    }
}

#[derive(Debug)]
pub struct BTreePage {
    pub header: BTreePageHeader,
    pub cells: Vec<BTreeCell>,
}

pub fn begin_read_btree_page(
    page_source: &PageSource,
    buffer_pool: Arc<BufferPool>,
    page: Arc<Page>,
    page_idx: usize,
) -> Result<()> {
    trace!("begin_read_btree_page(page_idx = {})", page_idx);
    let buf = buffer_pool.get();
    let drop_fn = Arc::new(move |buf| {
        let buffer_pool = buffer_pool.clone();
        buffer_pool.put(buf);
    });
    let buf = Buffer::new(buf, drop_fn);
    let complete = Box::new(move |buf: &Buffer| {
        let page = page.clone();
        if let Err(_) = finish_read_btree_page(page_idx, buf, page.clone()) {
            page.set_error();
        }
    });
    let c = Arc::new(Completion::new(buf, complete));
    page_source.get(page_idx, c.clone())?;
    Ok(())
}

fn finish_read_btree_page(page_idx: usize, buf: &Buffer, page: Arc<Page>) -> Result<()> {
    trace!("finish_read_btree_page(page_idx = {})", page_idx);
    let mut pos = if page_idx == 1 {
        DATABASE_HEADER_SIZE
    } else {
        0
    };
    let buf = buf.as_slice();
    let mut header = BTreePageHeader {
        page_type: buf[pos].try_into()?,
        _first_freeblock_offset: u16::from_be_bytes([buf[pos + 1], buf[pos + 2]]),
        num_cells: u16::from_be_bytes([buf[pos + 3], buf[pos + 4]]),
        _cell_content_area: u16::from_be_bytes([buf[pos + 5], buf[pos + 6]]),
        _num_frag_free_bytes: buf[pos + 7],
        right_most_pointer: None,
    };
    pos += 8;
    if header.page_type == PageType::IndexInterior || header.page_type == PageType::TableInterior {
        header.right_most_pointer = Some(u32::from_be_bytes([
            buf[pos],
            buf[pos + 1],
            buf[pos + 2],
            buf[pos + 3],
        ]));
        pos += 4;
    }
    let mut cells = Vec::with_capacity(header.num_cells as usize);
    for _ in 0..header.num_cells {
        let cell_pointer = u16::from_be_bytes([buf[pos], buf[pos + 1]]);
        pos += 2;
        let cell = read_btree_cell(buf, &header.page_type, cell_pointer as usize)?;
        cells.push(cell);
    }
    let inner = BTreePage { header, cells };
    page.contents.write().unwrap().replace(inner);
    page.set_uptodate();
    page.clear_locked();
    Ok(())
}

#[derive(Debug)]
pub enum BTreeCell {
    TableInteriorCell(TableInteriorCell),
    TableLeafCell(TableLeafCell),
}

#[derive(Debug)]
pub struct TableInteriorCell {
    pub _left_child_page: u32,
    pub _rowid: u64,
}

#[derive(Debug)]
pub struct TableLeafCell {
    pub _rowid: u64,
    pub _payload: Vec<u8>,
}

pub fn read_btree_cell(page: &[u8], page_type: &PageType, pos: usize) -> Result<BTreeCell> {
    match page_type {
        PageType::IndexInterior => todo!(),
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
        PageType::IndexLeaf => todo!(),
        PageType::TableLeaf => {
            let mut pos = pos;
            let (payload_size, nr) = read_varint(&page[pos..])?;
            pos += nr;
            let (rowid, nr) = read_varint(&page[pos..])?;
            pos += nr;
            let payload = &page[pos..pos + payload_size as usize];
            // FIXME: page overflows if the payload is too large
            Ok(BTreeCell::TableLeafCell(TableLeafCell {
                _rowid: rowid,
                _payload: payload.to_vec(),
            }))
        }
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
    type Error = anyhow::Error;

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
            n if value > 12 && value % 2 == 0 => Ok(Self::Blob(((n - 12) / 2) as usize)),
            n if value > 13 && value % 2 == 1 => Ok(Self::String(((n - 13) / 2) as usize)),
            _ => Err(anyhow!("Invalid serial type: {}", value)),
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
        assert!(pos + nr < payload.len());
        pos += nr;
        assert!(header_size >= nr);
        header_size -= nr;
    }
    let mut values = Vec::with_capacity(serial_types.len());
    for serial_type in &serial_types {
        let (value, usize) = read_value(&payload[pos..], serial_type)?;
        pos += usize;
        values.push(value);
    }
    Ok(OwnedRecord::new(values))
}

pub fn read_value(buf: &[u8], serial_type: &SerialType) -> Result<(OwnedValue, usize)> {
    match *serial_type {
        SerialType::Null => Ok((OwnedValue::Null, 0)),
        SerialType::UInt8 => {
            if buf.is_empty() {
                return Err(anyhow!("Invalid UInt8 value"));
            }
            Ok((OwnedValue::Integer(buf[0] as i64), 1))
        }
        SerialType::BEInt16 => {
            if buf.len() < 2 {
                return Err(anyhow!("Invalid BEInt16 value"));
            }
            Ok((
                OwnedValue::Integer(i16::from_be_bytes([buf[0], buf[1]]) as i64),
                2,
            ))
        }
        SerialType::BEInt24 => {
            if buf.len() < 3 {
                return Err(anyhow!("Invalid BEInt24 value"));
            }
            Ok((
                OwnedValue::Integer(i32::from_be_bytes([0, buf[0], buf[1], buf[2]]) as i64),
                3,
            ))
        }
        SerialType::BEInt32 => {
            if buf.len() < 4 {
                return Err(anyhow!("Invalid BEInt32 value"));
            }
            Ok((
                OwnedValue::Integer(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as i64),
                4,
            ))
        }
        SerialType::BEInt48 => {
            if buf.len() < 6 {
                return Err(anyhow!("Invalid BEInt48 value"));
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
                return Err(anyhow!("Invalid BEInt64 value"));
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
                return Err(anyhow!("Invalid BEFloat64 value"));
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
                return Err(anyhow!("Invalid Blob value"));
            }
            Ok((OwnedValue::Blob(buf[0..n].to_vec()), n))
        }
        SerialType::String(n) => {
            if buf.len() < n {
                return Err(anyhow!("Invalid String value"));
            }
            let bytes = buf[0..n].to_vec();
            let value = unsafe { String::from_utf8_unchecked(bytes) };
            Ok((OwnedValue::Text(value), n))
        }
    }
}

fn read_varint(buf: &[u8]) -> Result<(u64, usize)> {
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
                return Err(anyhow!("Invalid varint"));
            }
        }
    }
    v = (v << 8) + buf[8] as u64;
    Ok((v, 9))
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
    #[case(&[64, 9, 33, 251, 84, 68, 45, 24], SerialType::BEFloat64, OwnedValue::Float(3.141592653589793))]
    #[case(&[], SerialType::ConstInt0, OwnedValue::Integer(0))]
    #[case(&[], SerialType::ConstInt1, OwnedValue::Integer(1))]
    #[case(&[1, 2, 3], SerialType::Blob(3), OwnedValue::Blob(vec![1, 2, 3]))]
    #[case(&[65, 66, 67], SerialType::String(3), OwnedValue::Text("ABC".to_string()))]
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
}
