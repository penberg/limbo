use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::{cell::RefCell, rc::Rc, sync::Arc};

use log::{debug, trace};

use crate::io::{File, SyncCompletion, IO};
use crate::storage::sqlite3_ondisk::{
    begin_read_wal_frame, begin_write_wal_frame, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE,
};
use crate::{storage::pager::Page, Result};
use crate::{Completion, OpenFlags};

use self::sqlite3_ondisk::{checksum_wal, WAL_MAGIC_BE, WAL_MAGIC_LE};

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

    syncing: Rc<RefCell<bool>>,
    page_size: usize,

    ongoing_checkpoint: HashSet<usize>,
    shared: Arc<RwLock<WalFileShared>>,
    checkpoint_threshold: usize,
}

pub struct WalFileShared {
    wal_header: Arc<RwLock<sqlite3_ondisk::WalHeader>>,
    min_frame: u64,
    max_frame: u64,
    nbackfills: u64,
    // Maps pgno to frame id and offset in wal file
    frame_cache: HashMap<u64, Vec<u64>>, // FIXME: for now let's use a simple hashmap instead of a shm file
    last_checksum: (u32, u32), // Check of last frame in WAL, this is a cumulative checksum over all frames in the WAL
    file: Rc<dyn File>,
}

pub enum CheckpointStatus {
    Done,
    IO,
}

impl Wal for WalFile {
    /// Begin a read transaction.
    fn begin_read_tx(&self) -> Result<()> {
        let mut shared = self.shared.write().unwrap();
        shared.min_frame = shared.nbackfills + 1;
        Ok(())
    }

    /// End a read transaction.
    fn end_read_tx(&self) -> Result<()> {
        Ok(())
    }

    /// Find the latest frame containing a page.
    fn find_frame(&self, page_id: u64) -> Result<Option<u64>> {
        let shared = self.shared.read().unwrap();
        let frames = shared.frame_cache.get(&page_id);
        if frames.is_none() {
            return Ok(None);
        }
        let frames = frames.unwrap();
        for frame in frames.iter().rev() {
            if *frame <= shared.max_frame {
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
        let shared = self.shared.read().unwrap();
        begin_read_wal_frame(
            &shared.file,
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
        let page_id = page.borrow().id;
        let mut shared = self.shared.write().unwrap();
        let frame_id = shared.max_frame;
        let offset = self.frame_offset(frame_id);
        trace!(
            "append_frame(frame={}, offset={}, page_id={})",
            frame_id,
            offset,
            page_id
        );
        let header = shared.wal_header.clone();
        let header = header.read().unwrap();
        let checksums = shared.last_checksum;
        let checksums = begin_write_wal_frame(
            &shared.file,
            offset,
            &page,
            db_size,
            write_counter,
            &header,
            checksums,
        )?;
        shared.last_checksum = checksums;
        shared.max_frame = frame_id + 1;
        {
            let frames = shared.frame_cache.get_mut(&(page_id as u64));
            match frames {
                Some(frames) => frames.push(frame_id),
                None => {
                    shared.frame_cache.insert(page_id as u64, vec![frame_id]);
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
        let shared = self.shared.read().unwrap();
        let frame_id = shared.max_frame as usize;
        frame_id >= self.checkpoint_threshold
    }

    fn checkpoint(
        &mut self,
        pager: &Pager,
        write_counter: Rc<RefCell<usize>>,
    ) -> Result<CheckpointStatus> {
        let mut shared = self.shared.write().unwrap();
        for (page_id, _frames) in shared.frame_cache.iter() {
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

        // TODO: only clear checkpointed frames
        shared.frame_cache.clear();
        shared.max_frame = 0;
        self.ongoing_checkpoint.clear();
        Ok(CheckpointStatus::Done)
    }

    fn sync(&mut self) -> Result<CheckpointStatus> {
        let shared = self.shared.write().unwrap();
        {
            let syncing = self.syncing.clone();
            let completion = Completion::Sync(SyncCompletion {
                complete: Box::new(move |_| {
                    *syncing.borrow_mut() = false;
                }),
            });
            shared.file.sync(Rc::new(completion))?;
        }

        if *self.syncing.borrow() {
            Ok(CheckpointStatus::IO)
        } else {
            Ok(CheckpointStatus::Done)
        }
    }
}

impl WalFile {
    pub fn new(io: Arc<dyn IO>, page_size: usize, shared: Arc<RwLock<WalFileShared>>) -> Self {
        Self {
            io,
            shared,
            ongoing_checkpoint: HashSet::new(),
            syncing: Rc::new(RefCell::new(false)),
            checkpoint_threshold: 1000,
            page_size,
        }
    }

    fn frame_offset(&self, frame_id: u64) -> usize {
        let page_size = self.page_size;
        let page_offset = frame_id * (page_size as u64 + WAL_FRAME_HEADER_SIZE as u64);
        let offset = WAL_HEADER_SIZE as u64 + page_offset;
        offset as usize
    }
}

impl WalFileShared {
    pub fn open_shared(
        io: &Arc<dyn IO>,
        path: &str,
        page_size: u16,
    ) -> Result<Arc<RwLock<WalFileShared>>> {
        let file = io.open_file(path, crate::io::OpenFlags::Create, false)?;
        let header = if file.size()? > 0 {
            let wal_header = match sqlite3_ondisk::begin_read_wal_header(&file) {
                Ok(header) => header,
                Err(err) => panic!("Couldn't read header page: {:?}", err),
            };
            log::info!("recover not implemented yet");
            // TODO: Return a completion instead.
            io.run_once()?;
            wal_header
        } else {
            let magic = if cfg!(target_endian = "big") {
                WAL_MAGIC_BE
            } else {
                WAL_MAGIC_LE
            };
            let mut wal_header = WalHeader {
                magic,
                file_format: 3007000,
                page_size: page_size as u32,
                checkpoint_seq: 0, // TODO implement sequence number
                salt_1: 0,         // TODO implement salt
                salt_2: 0,
                checksum_1: 0,
                checksum_2: 0,
            };
            let native = cfg!(target_endian = "big"); // if target_endian is
                                                      // already big then we don't care but if isn't, header hasn't yet been
                                                      // encoded to big endian, therefore we wan't to swap bytes to compute this
                                                      // checksum.
            let checksums = (0, 0);
            let checksums = checksum_wal(
                &wal_header.as_bytes()[..WAL_HEADER_SIZE - 2 * 4], // first 24 bytes
                &wal_header,
                checksums,
                native, // this is false because we haven't encoded the wal header yet
            );
            wal_header.checksum_1 = checksums.0;
            wal_header.checksum_2 = checksums.1;
            sqlite3_ondisk::begin_write_wal_header(&file, &wal_header)?;
            Arc::new(RwLock::new(wal_header))
        };
        let checksum = {
            let checksum = header.read().unwrap();
            (checksum.checksum_1, checksum.checksum_2)
        };
        let shared = WalFileShared {
            wal_header: header,
            min_frame: 0,
            max_frame: 0,
            nbackfills: 0,
            frame_cache: HashMap::new(),
            last_checksum: checksum,
            file,
        };
        Ok(Arc::new(RwLock::new(shared)))
    }
}
