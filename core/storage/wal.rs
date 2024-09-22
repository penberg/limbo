use std::collections::HashMap;
use std::{cell::RefCell, rc::Rc, sync::Arc};

use crate::io::{File, IO};
use crate::storage::sqlite3_ondisk::{
    begin_read_page, begin_read_wal_frame, begin_write_wal_frame, WAL_FRAME_HEADER_SIZE,
    WAL_HEADER_SIZE,
};
use crate::{storage::pager::Page, Result};

use super::buffer_pool::BufferPool;
use super::pager::Pager;
use super::sqlite3_ondisk;

/// Write-ahead log (WAL).
pub trait Wal {
    /// Begin a read transaction.
    fn begin_read_tx(&self) -> Result<()>;

    /// Begin a write transaction.
    fn begin_write_tx(&self) -> Result<()>;

    /// End a read transaction.
    fn end_read_tx(&self) -> Result<()>;

    /// End a write transaction.
    fn end_write_tx(&self) -> Result<()>;

    /// Find the latest frame containing a page.
    fn find_frame(&self, page_id: u64) -> Result<Option<u64>>;

    /// Read a frame from the WAL.
    fn read_frame(
        &self,
        frame_id: u64,
        page: Rc<RefCell<Page>>,
        buffer_pool: Rc<BufferPool>,
    ) -> Result<()>;

    /// Write a frame to the WAL.
    fn append_frame(
        &self,
        page: Rc<RefCell<Page>>,
        db_size: u32,
        pager: &Pager,
    ) -> Result<CheckpointStatus>;

    fn checkpoint(&self, pager: &Pager) -> Result<CheckpointStatus>;
}

#[cfg(feature = "fs")]
pub struct WalFile {
    io: Arc<dyn crate::io::IO>,
    wal_path: String,
    file: RefCell<Option<Rc<dyn File>>>,
    wal_header: RefCell<Option<Rc<RefCell<sqlite3_ondisk::WalHeader>>>>,
    min_frame: RefCell<u64>,
    max_frame: RefCell<u64>,
    nbackfills: RefCell<u64>,
    // Maps pgno to frame id and offset in wal file
    frame_cache: RefCell<HashMap<u64, Vec<u64>>>, // FIXME: for now let's use a simple hashmap instead of a shm file
    checkpoint_threshold: usize,
}

enum CheckpointStatus {
    Done,
    IO,
}

#[cfg(feature = "fs")]
impl Wal for WalFile {
    /// Begin a read transaction.
    fn begin_read_tx(&self) -> Result<()> {
        self.min_frame.replace(*self.nbackfills.borrow() + 1);
        Ok(())
    }

    /// End a read transaction.
    fn end_read_tx(&self) -> Result<()> {
        Ok(())
    }

    /// Find the latest frame containing a page.
    fn find_frame(&self, page_id: u64) -> Result<Option<u64>> {
        let frame_cache = self.frame_cache.borrow();
        dbg!(&frame_cache);
        let frames = frame_cache.get(&page_id);
        dbg!(&frames);
        if frames.is_none() {
            return Ok(None);
        }
        self.ensure_init()?;
        let frames = frames.unwrap();
        for frame in frames.iter().rev() {
            if *frame <= *self.max_frame.borrow() {
                return Ok(Some(*frame));
            }
        }
        Ok(None)
    }

    /// Read a frame from the WAL.
    fn read_frame(
        &self,
        frame_id: u64,
        page: Rc<RefCell<Page>>,
        buffer_pool: Rc<BufferPool>,
    ) -> Result<()> {
        println!("read frame {}", frame_id);
        let offset = self.frame_offset(frame_id);
        begin_read_wal_frame(
            self.file.borrow().as_ref().unwrap(),
            offset,
            buffer_pool,
            page,
        )?;
        Ok(())
    }

    /// Write a frame to the WAL.
    fn append_frame(&self, page: Rc<RefCell<Page>>, db_size: u32, pager: &Pager) -> Result<()> {
        self.ensure_init()?;
        let page_id = page.borrow().id;
        let frame_id = *self.max_frame.borrow();
        let offset = self.frame_offset(frame_id);
        println!("appending {} at {}", frame_id, offset);
        begin_write_wal_frame(self.file.borrow().as_ref().unwrap(), offset, &page, db_size)?;
        self.max_frame.replace(frame_id + 1);
        let mut frame_cache = self.frame_cache.borrow_mut();
        let frames = frame_cache.get_mut(&(page_id as u64));
        match frames {
            Some(frames) => frames.push(frame_id),
            None => {
                frame_cache.insert(page_id as u64, vec![frame_id]);
            }
        }
        dbg!(&frame_cache);
        if (frame_id + 1) as usize >= self.checkpoint_threshold {
            self.checkpoint(pager);
        }
        Ok(())
    }

    /// Begin a write transaction
    fn begin_write_tx(&self) -> Result<()> {
        Ok(())
    }

    /// End a write transaction
    fn end_write_tx(&self) -> Result<()> {
        Ok(())
    }

    fn checkpoint(&self, pager: &Pager) -> Result<CheckpointStatus> {
        for (page_id, frames) in self.frame_cache.borrow().iter() {
            // move page from WAL to database file
            // TODO(Pere): use splice syscall in linux to do zero-copy file page movements to improve perf
            let page = pager.read_page(*page_id as usize)?;
            if page.borrow().is_locked() {
                return Ok(CheckpointStatus::IO);
            }
        }
        Ok(())
    }
}

#[cfg(feature = "fs")]
impl WalFile {
    pub fn new(io: Arc<dyn IO>, wal_path: String) -> Self {
        Self {
            io,
            wal_path,
            file: RefCell::new(None),
            wal_header: RefCell::new(None),
            frame_cache: RefCell::new(HashMap::new()),
            min_frame: RefCell::new(0),
            max_frame: RefCell::new(0),
            nbackfills: RefCell::new(0),
            checkpoint_threshold: 1000,
        }
    }

    fn ensure_init(&self) -> Result<()> {
        println!("ensure");
        if self.file.borrow().is_none() {
            println!("inside ensure");
            match self.io.open_file(&self.wal_path) {
                Ok(file) => {
                    *self.file.borrow_mut() = Some(file.clone());
                    let wal_header = match sqlite3_ondisk::begin_read_wal_header(file) {
                        Ok(header) => header,
                        Err(err) => panic!("{:?}", err),
                    };
                    // TODO: Return a completion instead.
                    self.io.run_once()?;
                    self.wal_header.replace(Some(wal_header));
                    dbg!(&self.wal_header);
                }
                Err(err) => panic!("{:?}", err),
            };
        }
        Ok(())
    }

    fn frame_offset(&self, frame_id: u64) -> usize {
        let header = self.wal_header.borrow();
        let header = header.as_ref().unwrap().borrow();
        let page_size = header.page_size;
        let page_offset = frame_id * (page_size as u64 + WAL_FRAME_HEADER_SIZE as u64);
        let offset = WAL_HEADER_SIZE as u64 + WAL_FRAME_HEADER_SIZE as u64 + page_offset;
        offset as usize
    }
}
