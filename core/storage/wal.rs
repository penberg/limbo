use std::collections::{HashMap, HashSet};
use std::{cell::RefCell, rc::Rc, sync::Arc};

use log::{debug, trace};

use crate::io::{File, SyncCompletion, IO};
use crate::storage::sqlite3_ondisk::{
    begin_read_wal_frame, begin_write_wal_frame, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE,
};
use crate::Completion;
use crate::{storage::pager::Page, Result};

use super::buffer_pool::BufferPool;
use super::pager::Pager;
use super::sqlite3_ondisk::{self, begin_write_btree_page, WalHeader};

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
        &mut self,
        page: Rc<RefCell<Page>>,
        db_size: u32,
        pager: &Pager,
        write_counter: Rc<RefCell<usize>>,
    ) -> Result<()>;

    fn should_checkpoint(&self) -> bool;
    fn checkpoint(
        &mut self,
        pager: &Pager,
        write_counter: Rc<RefCell<usize>>,
    ) -> Result<CheckpointStatus>;
    fn sync(&mut self) -> Result<CheckpointStatus>;
}

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
    ongoing_checkpoint: HashSet<usize>,

    syncing: Rc<RefCell<bool>>,
    page_size: usize,
}

pub enum CheckpointStatus {
    Done,
    IO,
}

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
        let frames = frame_cache.get(&page_id);
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
        debug!("read_frame({})", frame_id);
        let offset = self.frame_offset(frame_id);
        begin_read_wal_frame(
            self.file.borrow().as_ref().unwrap(),
            offset + WAL_FRAME_HEADER_SIZE,
            buffer_pool,
            page,
        )?;
        Ok(())
    }

    /// Write a frame to the WAL.
    fn append_frame(
        &mut self,
        page: Rc<RefCell<Page>>,
        db_size: u32,
        _pager: &Pager,
        write_counter: Rc<RefCell<usize>>,
    ) -> Result<()> {
        self.ensure_init()?;
        let page_id = page.borrow().id;
        let frame_id = *self.max_frame.borrow();
        let offset = self.frame_offset(frame_id);
        trace!(
            "append_frame(frame={}, offset={}, page_id={})",
            frame_id,
            offset,
            page_id
        );
        begin_write_wal_frame(
            self.file.borrow().as_ref().unwrap(),
            offset,
            &page,
            db_size,
            write_counter,
        )?;
        self.max_frame.replace(frame_id + 1);
        {
            let mut frame_cache = self.frame_cache.borrow_mut();
            let frames = frame_cache.get_mut(&(page_id as u64));
            match frames {
                Some(frames) => frames.push(frame_id),
                None => {
                    frame_cache.insert(page_id as u64, vec![frame_id]);
                }
            }
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

    fn should_checkpoint(&self) -> bool {
        let frame_id = *self.max_frame.borrow() as usize;
        frame_id >= self.checkpoint_threshold
    }

    fn checkpoint(
        &mut self,
        pager: &Pager,
        write_counter: Rc<RefCell<usize>>,
    ) -> Result<CheckpointStatus> {
        for (page_id, _frames) in self.frame_cache.borrow().iter() {
            // move page from WAL to database file
            // TODO(Pere): use splice syscall in linux to do zero-copy file page movements to improve perf
            let page_id = *page_id as usize;
            if self.ongoing_checkpoint.contains(&page_id) {
                continue;
            }

            let page = pager.read_page(page_id)?;
            if page.borrow().is_locked() {
                return Ok(CheckpointStatus::IO);
            }

            begin_write_btree_page(pager, &page, write_counter.clone())?;
            self.ongoing_checkpoint.insert(page_id);
        }

        self.frame_cache.borrow_mut().clear();
        *self.max_frame.borrow_mut() = 0;
        self.ongoing_checkpoint.clear();
        Ok(CheckpointStatus::Done)
    }

    fn sync(&mut self) -> Result<CheckpointStatus> {
        self.ensure_init()?;
        let file = self.file.borrow();
        let file = file.as_ref().unwrap();
        {
            let syncing = self.syncing.clone();
            let completion = Completion::Sync(SyncCompletion {
                complete: Box::new(move |_| {
                    *syncing.borrow_mut() = false;
                }),
            });
            file.sync(Rc::new(completion))?;
        }

        if *self.syncing.borrow() {
            return Ok(CheckpointStatus::IO);
        } else {
            return Ok(CheckpointStatus::Done);
        }
    }
}

impl WalFile {
    pub fn new(io: Arc<dyn IO>, wal_path: String, page_size: usize) -> Self {
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
            ongoing_checkpoint: HashSet::new(),
            syncing: Rc::new(RefCell::new(false)),
            page_size,
        }
    }

    fn ensure_init(&self) -> Result<()> {
        if self.file.borrow().is_none() {
            match self
                .io
                .open_file(&self.wal_path, crate::io::OpenFlags::Create, false)
            {
                Ok(file) => {
                    if file.size()? > 0 {
                        let wal_header = match sqlite3_ondisk::begin_read_wal_header(&file) {
                            Ok(header) => header,
                            Err(err) => panic!("Couldn't read header page: {:?}", err),
                        };
                        // TODO: Return a completion instead.
                        self.io.run_once()?;
                        self.wal_header.replace(Some(wal_header));
                    } else {
                        let wal_header = WalHeader {
                            magic: (0x377f0682_u32).to_be_bytes(),
                            file_format: 3007000,
                            page_size: self.page_size as u32,
                            checkpoint_seq: 0, // TODO implement sequence number
                            salt_1: 0,         // TODO implement salt
                            salt_2: 0,
                            checksum_1: 0,
                            checksum_2: 0, // TODO implement checksum header
                        };
                        sqlite3_ondisk::begin_write_wal_header(&file, &wal_header)?;
                        self.wal_header
                            .replace(Some(Rc::new(RefCell::new(wal_header))));
                    }
                    *self.file.borrow_mut() = Some(file);
                }
                Err(err) => panic!("{:?} {}", err, &self.wal_path),
            };
        }
        Ok(())
    }

    fn frame_offset(&self, frame_id: u64) -> usize {
        let header = self.wal_header.borrow();
        let header = header.as_ref().unwrap().borrow();
        let page_size = header.page_size;
        let page_offset = frame_id * (page_size as u64 + WAL_FRAME_HEADER_SIZE as u64);
        let offset = WAL_HEADER_SIZE as u64 + page_offset;
        offset as usize
    }
}
