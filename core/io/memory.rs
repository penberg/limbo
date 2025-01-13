use super::{Buffer, Completion, File, OpenFlags, IO};
use crate::Result;

use log::debug;
use std::{
    cell::{RefCell, RefMut},
    collections::BTreeMap,
    rc::Rc,
    sync::Arc,
};

pub struct MemoryIO {
    pages: RefCell<BTreeMap<usize, MemPage>>,
    size: RefCell<usize>,
}

// TODO: page size flag
const PAGE_SIZE: usize = 4096;
type MemPage = Box<[u8; PAGE_SIZE]>;

impl MemoryIO {
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new() -> Result<Arc<Self>> {
        debug!("Using IO backend 'memory'");
        Ok(Arc::new(Self {
            pages: RefCell::new(BTreeMap::new()),
            size: RefCell::new(0),
        }))
    }

    fn get_or_allocate_page(&self, page_no: usize) -> RefMut<MemPage> {
        let pages = self.pages.borrow_mut();
        RefMut::map(pages, |p| {
            p.entry(page_no).or_insert_with(|| Box::new([0; PAGE_SIZE]))
        })
    }

    fn get_page(&self, page_no: usize) -> Option<RefMut<MemPage>> {
        match RefMut::filter_map(self.pages.borrow_mut(), |pages| pages.get_mut(&page_no)) {
            Ok(page) => Some(page),
            Err(_) => None,
        }
    }
}

impl IO for Arc<MemoryIO> {
    fn open_file(&self, _path: &str, _flags: OpenFlags, _direct: bool) -> Result<Rc<dyn File>> {
        Ok(Rc::new(MemoryFile {
            io: Arc::clone(self),
        }))
    }

    fn run_once(&self) -> Result<()> {
        // nop
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).unwrap();
        i64::from_ne_bytes(buf)
    }

    fn get_current_time(&self) -> String {
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
    }
}

pub struct MemoryFile {
    io: Arc<MemoryIO>,
}

impl File for MemoryFile {
    // no-ops
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }
    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(&self, pos: usize, c: Rc<Completion>) -> Result<()> {
        let r = match &*c {
            Completion::Read(r) => r,
            _ => unreachable!(),
        };
        let buf_len = r.buf().len();
        if buf_len == 0 {
            c.complete(0);
            return Ok(());
        }

        let file_size = *self.io.size.borrow();
        if pos >= file_size {
            c.complete(0);
            return Ok(());
        }

        let read_len = buf_len.min(file_size - pos);
        {
            let mut read_buf = r.buf_mut();
            let mut offset = pos;
            let mut remaining = read_len;
            let mut buf_offset = 0;

            while remaining > 0 {
                let page_no = offset / PAGE_SIZE;
                let page_offset = offset % PAGE_SIZE;
                let bytes_to_read = remaining.min(PAGE_SIZE - page_offset);
                if let Some(page) = self.io.get_page(page_no) {
                    {
                        let page_data = &*page;
                        read_buf.as_mut_slice()[buf_offset..buf_offset + bytes_to_read]
                            .copy_from_slice(&page_data[page_offset..page_offset + bytes_to_read]);
                    }
                } else {
                    for b in &mut read_buf.as_mut_slice()[buf_offset..buf_offset + bytes_to_read] {
                        *b = 0;
                    }
                }

                offset += bytes_to_read;
                buf_offset += bytes_to_read;
                remaining -= bytes_to_read;
            }
        }
        c.complete(read_len as i32);
        Ok(())
    }

    fn pwrite(&self, pos: usize, buffer: Rc<RefCell<Buffer>>, c: Rc<Completion>) -> Result<()> {
        let buf = buffer.borrow();
        let buf_len = buf.len();
        if buf_len == 0 {
            c.complete(0);
            return Ok(());
        }

        let mut offset = pos;
        let mut remaining = buf_len;
        let mut buf_offset = 0;
        let data = &buf.as_slice();

        while remaining > 0 {
            let page_no = offset / PAGE_SIZE;
            let page_offset = offset % PAGE_SIZE;
            let bytes_to_write = remaining.min(PAGE_SIZE - page_offset);

            {
                let mut page = self.io.get_or_allocate_page(page_no);
                page[page_offset..page_offset + bytes_to_write]
                    .copy_from_slice(&data[buf_offset..buf_offset + bytes_to_write]);
            }

            offset += bytes_to_write;
            buf_offset += bytes_to_write;
            remaining -= bytes_to_write;
        }

        {
            let mut size = self.io.size.borrow_mut();
            *size = (*size).max(pos + buf_len);
        }

        c.complete(buf_len as i32);
        Ok(())
    }

    fn sync(&self, c: Rc<Completion>) -> Result<()> {
        // no-op
        c.complete(0);
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        Ok(*self.io.size.borrow() as u64)
    }
}

impl Drop for MemoryFile {
    fn drop(&mut self) {
        // no-op
    }
}
