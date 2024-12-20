use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::{cell::RefCell, rc::Rc, sync::Arc};

use log::{debug, trace};

use crate::io::{File, SyncCompletion, IO};
use crate::storage::sqlite3_ondisk::{
    begin_read_wal_frame, begin_write_wal_frame, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE,
};
use crate::{Buffer, Result};
use crate::{Completion, Page};

use self::sqlite3_ondisk::{checksum_wal, PageContent, WAL_MAGIC_BE, WAL_MAGIC_LE};

use super::buffer_pool::BufferPool;
use super::page_cache::PageCacheKey;
use super::pager::{PageRef, Pager};
use super::sqlite3_ondisk::{self, begin_write_btree_page, WalHeader};

/// Write-ahead log (WAL).
pub trait Wal {
    /// Begin a read transaction.
    fn begin_read_tx(&mut self) -> Result<()>;

    /// Begin a write transaction.
    fn begin_write_tx(&mut self) -> Result<()>;

    /// End a read transaction.
    fn end_read_tx(&self) -> Result<()>;

    /// End a write transaction.
    fn end_write_tx(&self) -> Result<()>;

    /// Find the latest frame containing a page.
    fn find_frame(&self, page_id: u64) -> Result<Option<u64>>;

    /// Read a frame from the WAL.
    fn read_frame(&self, frame_id: u64, page: PageRef, buffer_pool: Rc<BufferPool>) -> Result<()>;

    /// Write a frame to the WAL.
    fn append_frame(
        &mut self,
        page: PageRef,
        db_size: u32,
        write_counter: Rc<RefCell<usize>>,
    ) -> Result<()>;

    fn should_checkpoint(&self) -> bool;
    fn checkpoint(
        &mut self,
        pager: &Pager,
        write_counter: Rc<RefCell<usize>>,
    ) -> Result<CheckpointStatus>;
    fn sync(&mut self) -> Result<CheckpointStatus>;
    fn get_max_frame(&self) -> u64;
    fn get_min_frame(&self) -> u64;
}

// Syncing requires a state machine because we need to schedule a sync and then wait until it is
// finished. If we don't wait there will be undefined behaviour that no one wants to debug.
#[derive(Copy, Clone)]
enum SyncState {
    NotSyncing,
    Syncing,
}

#[derive(Debug, Copy, Clone)]
pub enum CheckpointState {
    Start,
    ReadFrame,
    WaitReadFrame,
    WritePage,
    WaitWritePage,
    Done,
}

pub enum CheckpointStatus {
    Done,
    IO,
}

// Checkpointing is a state machine that has multiple steps. Since there are multiple steps we save
// in flight information of the checkpoint in OngoingCheckpoint. page is just a helper Page to do
// page operations like reading a frame to a page, and writing a page to disk. This page should not
// be placed back in pager page cache or anything, it's just a helper.
// min_frame and max_frame is the range of frames that can be safely transferred from WAL to db
// file.
// current_page is a helper to iterate through all the pages that might have a frame in the safe
// range. This is inneficient for now.
struct OngoingCheckpoint {
    page: PageRef,
    state: CheckpointState,
    min_frame: u64,
    max_frame: u64,
    current_page: u64,
}

pub struct WalFile {
    io: Arc<dyn crate::io::IO>,
    buffer_pool: Rc<BufferPool>,

    sync_state: RefCell<SyncState>,
    syncing: Rc<RefCell<bool>>,
    page_size: usize,

    shared: Arc<RwLock<WalFileShared>>,
    ongoing_checkpoint: OngoingCheckpoint,
    checkpoint_threshold: usize,
    // min and max frames for this connection
    max_frame: u64,
    min_frame: u64,
}

/// WalFileShared is the part of a WAL that will be shared between threads. A wal has information
/// that needs to be communicated between threads so this struct does the job.
pub struct WalFileShared {
    wal_header: Arc<RwLock<sqlite3_ondisk::WalHeader>>,
    min_frame: u64,
    max_frame: u64,
    nbackfills: u64,
    // Frame cache maps a Page to all the frames it has stored in WAL in ascending order.
    // This is do to easily find the frame it must checkpoint each connection if a checkpoint is
    // necessary.
    // One difference between SQLite and limbo is that we will never support multi process, meaning
    // we don't need WAL's index file. So we can do stuff like this without shared memory.
    // TODO: this will need refactoring because this is incredible memory inneficient.
    frame_cache: HashMap<u64, Vec<u64>>,
    // Another memory inneficient array made to just keep track of pages that are in frame_cache.
    pages_in_frames: Vec<u64>,
    last_checksum: (u32, u32), // Check of last frame in WAL, this is a cumulative checksum over all frames in the WAL
    file: Rc<dyn File>,
}

impl Wal for WalFile {
    /// Begin a read transaction.
    fn begin_read_tx(&mut self) -> Result<()> {
        let shared = self.shared.read().unwrap();
        self.min_frame = shared.nbackfills + 1;
        self.max_frame = shared.max_frame;
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
            if *frame <= self.max_frame {
                return Ok(Some(*frame));
            }
        }
        Ok(None)
    }

    /// Read a frame from the WAL.
    fn read_frame(&self, frame_id: u64, page: PageRef, buffer_pool: Rc<BufferPool>) -> Result<()> {
        debug!("read_frame({})", frame_id);
        let offset = self.frame_offset(frame_id);
        let shared = self.shared.read().unwrap();
        page.set_locked();
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
        page: PageRef,
        db_size: u32,
        write_counter: Rc<RefCell<usize>>,
    ) -> Result<()> {
        let page_id = page.get().id;
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
                    shared.pages_in_frames.push(page_id as u64);
                }
            }
        }
        Ok(())
    }

    /// Begin a write transaction
    fn begin_write_tx(&mut self) -> Result<()> {
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
        'checkpoint_loop: loop {
            let state = self.ongoing_checkpoint.state;
            log::debug!("checkpoint(state={:?})", state);
            match state {
                CheckpointState::Start => {
                    // TODO(pere): check what frames are safe to checkpoint between many readers!
                    self.ongoing_checkpoint.min_frame = self.min_frame;
                    self.ongoing_checkpoint.max_frame = self.max_frame;
                    self.ongoing_checkpoint.current_page = 0;
                    self.ongoing_checkpoint.state = CheckpointState::ReadFrame;
                }
                CheckpointState::ReadFrame => {
                    let shared = self.shared.read().unwrap();
                    assert!(
                        self.ongoing_checkpoint.current_page as usize
                            <= shared.pages_in_frames.len()
                    );
                    if self.ongoing_checkpoint.current_page as usize == shared.pages_in_frames.len()
                    {
                        self.ongoing_checkpoint.state = CheckpointState::Done;
                        continue 'checkpoint_loop;
                    }
                    let page =
                        shared.pages_in_frames[self.ongoing_checkpoint.current_page as usize];
                    let frames = shared
                        .frame_cache
                        .get(&page)
                        .expect("page must be in frame cache if it's in list");

                    for frame in frames.iter().rev() {
                        // TODO: do proper selection of frames to checkpoint
                        if *frame >= self.ongoing_checkpoint.min_frame {
                            log::debug!(
                                "checkpoint page(state={:?}, page={}, frame={})",
                                state,
                                page,
                                *frame
                            );
                            self.ongoing_checkpoint.page.get().id = page as usize;

                            self.read_frame(
                                *frame,
                                self.ongoing_checkpoint.page.clone(),
                                self.buffer_pool.clone(),
                            )?;
                            self.ongoing_checkpoint.state = CheckpointState::WaitReadFrame;
                            self.ongoing_checkpoint.current_page += 1;
                            continue 'checkpoint_loop;
                        }
                    }
                    self.ongoing_checkpoint.current_page += 1;
                }
                CheckpointState::WaitReadFrame => {
                    if self.ongoing_checkpoint.page.is_locked() {
                        return Ok(CheckpointStatus::IO);
                    } else {
                        self.ongoing_checkpoint.state = CheckpointState::WritePage;
                    }
                }
                CheckpointState::WritePage => {
                    self.ongoing_checkpoint.page.set_dirty();
                    begin_write_btree_page(
                        pager,
                        &self.ongoing_checkpoint.page,
                        write_counter.clone(),
                    )?;
                    self.ongoing_checkpoint.state = CheckpointState::WaitWritePage;
                }
                CheckpointState::WaitWritePage => {
                    if *write_counter.borrow() > 0 {
                        return Ok(CheckpointStatus::IO);
                    }
                    let shared = self.shared.read().unwrap();
                    if (self.ongoing_checkpoint.current_page as usize)
                        < shared.pages_in_frames.len()
                    {
                        self.ongoing_checkpoint.state = CheckpointState::ReadFrame;
                    } else {
                        self.ongoing_checkpoint.state = CheckpointState::Done;
                    }
                }
                CheckpointState::Done => {
                    if *write_counter.borrow() > 0 {
                        return Ok(CheckpointStatus::IO);
                    }
                    let mut shared = self.shared.write().unwrap();
                    shared.frame_cache.clear();
                    shared.pages_in_frames.clear();
                    shared.max_frame = 0;
                    shared.nbackfills = 0;
                    self.ongoing_checkpoint.state = CheckpointState::Start;
                    return Ok(CheckpointStatus::Done);
                }
            }
        }
    }

    fn sync(&mut self) -> Result<CheckpointStatus> {
        let state = *self.sync_state.borrow();
        match state {
            SyncState::NotSyncing => {
                let shared = self.shared.write().unwrap();
                log::debug!("wal_sync");
                {
                    let syncing = self.syncing.clone();
                    *syncing.borrow_mut() = true;
                    let completion = Completion::Sync(SyncCompletion {
                        complete: Box::new(move |_| {
                            log::debug!("wal_sync finish");
                            *syncing.borrow_mut() = false;
                        }),
                    });
                    shared.file.sync(Rc::new(completion))?;
                }
                self.sync_state.replace(SyncState::Syncing);
                Ok(CheckpointStatus::IO)
            }
            SyncState::Syncing => {
                if *self.syncing.borrow() {
                    Ok(CheckpointStatus::IO)
                } else {
                    self.sync_state.replace(SyncState::NotSyncing);
                    Ok(CheckpointStatus::Done)
                }
            }
        }
    }

    fn get_max_frame(&self) -> u64 {
        self.max_frame
    }

    fn get_min_frame(&self) -> u64 {
        self.min_frame
    }
}

impl WalFile {
    pub fn new(
        io: Arc<dyn IO>,
        page_size: usize,
        shared: Arc<RwLock<WalFileShared>>,
        buffer_pool: Rc<BufferPool>,
    ) -> Self {
        let checkpoint_page = Arc::new(Page::new(0));
        let buffer = buffer_pool.get();
        {
            let buffer_pool = buffer_pool.clone();
            let drop_fn = Rc::new(move |buf| {
                buffer_pool.put(buf);
            });
            checkpoint_page.get().contents = Some(PageContent {
                offset: 0,
                buffer: Rc::new(RefCell::new(Buffer::new(buffer, drop_fn))),
                overflow_cells: Vec::new(),
            });
        }
        Self {
            io,
            shared,
            ongoing_checkpoint: OngoingCheckpoint {
                page: checkpoint_page,
                state: CheckpointState::Start,
                min_frame: 0,
                max_frame: 0,
                current_page: 0,
            },
            syncing: Rc::new(RefCell::new(false)),
            checkpoint_threshold: 1000,
            page_size,
            max_frame: 0,
            min_frame: 0,
            buffer_pool,
            sync_state: RefCell::new(SyncState::NotSyncing),
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
            pages_in_frames: Vec::new(),
        };
        Ok(Arc::new(RwLock::new(shared)))
    }
}
