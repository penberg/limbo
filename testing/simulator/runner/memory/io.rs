use std::cell::RefCell;
use std::collections::BTreeSet;
use std::io::{BufWriter, Write};
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use indexmap::IndexMap;
use parking_lot::Mutex;
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_core::{
    Clock, Completion, CompletionError, IO, LimboError, MonotonicInstant, OpenFlags, Result,
    WallClockInstant,
};

use crate::runner::SimIO;
use crate::runner::clock::SimulatorClock;
use crate::runner::memory::file::MemorySimFile;

/// File descriptor
pub type Fd = String;

const FILE_BLOCK_SIZE: usize = 4096;

#[derive(Debug, Default)]
pub(super) struct FileContents {
    blocks: Vec<Arc<[u8; FILE_BLOCK_SIZE]>>,
    pub(super) len: usize,
}

#[derive(Debug)]
pub(super) struct SyncSnapshot {
    sequence: u64,
    blocks: Vec<(usize, Arc<[u8; FILE_BLOCK_SIZE]>)>,
    len: usize,
}

#[derive(Debug, Default)]
pub(super) struct FileState {
    pub(super) buffer: FileContents,
    durable: FileContents,
    dirty_blocks: BTreeSet<usize>,
    sync_sequence: u64,
    durable_sequence: u64,
}

impl FileState {
    pub(super) fn sync_started(&mut self) -> SyncSnapshot {
        self.sync_sequence = self
            .sync_sequence
            .checked_add(1)
            .expect("file sync sequence overflow");
        SyncSnapshot {
            sequence: self.sync_sequence,
            // Pending or failed syncs cannot serve as the baseline for later
            // snapshots, so keep blocks dirty until a sync succeeds.
            blocks: self
                .dirty_blocks
                .iter()
                .map(|&index| (index, self.buffer.blocks[index].clone()))
                .collect(),
            len: self.buffer.len,
        }
    }

    fn sync_completed(&mut self, snapshot: SyncSnapshot) {
        if snapshot.sequence <= self.durable_sequence {
            return;
        }
        let block_count = snapshot.len.div_ceil(FILE_BLOCK_SIZE);
        self.durable.blocks.truncate(block_count);
        for (index, block) in snapshot.blocks {
            if self
                .buffer
                .blocks
                .get(index)
                .is_some_and(|current| Arc::ptr_eq(current, &block))
            {
                self.dirty_blocks.remove(&index);
            }
            if index == self.durable.blocks.len() {
                self.durable.blocks.push(block);
            } else {
                self.durable.blocks[index] = block;
            }
        }
        assert_eq!(self.durable.blocks.len(), block_count);
        self.durable.len = snapshot.len;
        self.durable_sequence = snapshot.sequence;
    }

    fn power_loss(&mut self) {
        self.buffer.blocks.clone_from(&self.durable.blocks);
        self.buffer.len = self.durable.len;
        self.dirty_blocks.clear();
    }
}

#[derive(Debug)]
pub(super) enum OperationType {
    Read {
        completion: Completion,
        offset: usize,
    },
    Write {
        buffer: Arc<turso_core::Buffer>,
        completion: Completion,
        offset: usize,
    },
    WriteV {
        buffers: Vec<Arc<turso_core::Buffer>>,
        completion: Completion,
        offset: usize,
    },
    Sync {
        completion: Completion,
        snapshot: SyncSnapshot,
    },
    Truncate {
        completion: Completion,
        len: usize,
    },
}

impl OperationType {
    pub(super) fn get_completion(&self) -> &Completion {
        match self {
            OperationType::Read { completion, .. }
            | OperationType::Write { completion, .. }
            | OperationType::WriteV { completion, .. }
            | OperationType::Sync { completion, .. }
            | OperationType::Truncate { completion, .. } => completion,
        }
    }
}

#[derive(Debug)]
pub(super) struct Operation {
    pub(super) time: Option<turso_core::WallClockInstant>,
    pub(super) op: OperationType,
    pub(super) fault: bool,
    pub(super) file: Arc<RefCell<FileState>>,
}

impl Operation {
    fn do_operation(self) {
        match self.op {
            OperationType::Read { completion, offset } => {
                let buffer = &completion.as_read().buf;
                let bytes_read = self
                    .file
                    .borrow()
                    .buffer
                    .read(offset, buffer.as_mut_slice());
                completion.complete(bytes_read as i32);
            }
            OperationType::Write {
                buffer,
                completion,
                offset,
            } => {
                let buf_size = self.file.borrow_mut().write(offset, buffer.as_slice());
                completion.complete(buf_size as i32);
            }
            OperationType::WriteV {
                buffers,
                completion,
                offset,
            } => {
                let mut file = self.file.borrow_mut();
                let mut pos = offset;
                let written = buffers.into_iter().fold(0, |written, buffer| {
                    let buf_size = file.write(pos, buffer.as_slice());
                    pos += buf_size;
                    written + buf_size
                });
                drop(file);
                completion.complete(written as i32);
            }
            OperationType::Sync {
                completion,
                snapshot,
            } => {
                self.file.borrow_mut().sync_completed(snapshot);
                completion.complete(0);
            }
            OperationType::Truncate { completion, len } => {
                self.file.borrow_mut().resize(len);
                completion.complete(0);
            }
        }
    }
}

pub(super) type CallbackQueue = Arc<Mutex<Vec<Operation>>>;

pub struct MemorySimIO {
    callbacks: CallbackQueue,
    timeouts: CallbackQueue,
    pub files: RefCell<IndexMap<Fd, Arc<MemorySimFile>>>,
    file_states: RefCell<Vec<Weak<RefCell<FileState>>>>,
    generation: Arc<AtomicU64>,
    power_loss_in_progress: AtomicBool,
    pub rng: RefCell<ChaCha8Rng>,
    #[expect(dead_code)]
    pub page_size: usize,
    seed: u64,
    latency_probability: u8,
    clock: Arc<SimulatorClock>,
}

unsafe impl Send for MemorySimIO {}
unsafe impl Sync for MemorySimIO {}

impl MemorySimIO {
    pub fn new(
        seed: u64,
        page_size: usize,
        latency_probability: u8,
        min_tick: u64,
        max_tick: u64,
    ) -> Self {
        let files = RefCell::new(IndexMap::new());
        let rng = RefCell::new(ChaCha8Rng::seed_from_u64(seed));
        Self {
            callbacks: Arc::new(Mutex::new(Vec::new())),
            timeouts: Arc::new(Mutex::new(Vec::new())),
            files,
            file_states: RefCell::new(Vec::new()),
            generation: Arc::new(AtomicU64::new(0)),
            power_loss_in_progress: AtomicBool::new(false),
            rng,
            page_size,
            seed,
            latency_probability,
            clock: Arc::new(SimulatorClock::new(
                ChaCha8Rng::seed_from_u64(seed),
                min_tick,
                max_tick,
            )),
        }
    }

    fn ensure_io_allowed(&self) -> Result<()> {
        if self.power_loss_in_progress.load(Ordering::Acquire) {
            return Err(LimboError::CompletionError(CompletionError::IOError(
                std::io::ErrorKind::Other,
                "memory simulator I/O is unavailable during power loss",
            )));
        }
        Ok(())
    }
}

impl SimIO for MemorySimIO {
    fn inject_fault(&self, fault: bool) {
        for file in self.files.borrow().values() {
            file.inject_fault(fault);
        }
        if fault {
            tracing::debug!("fault injected");
        }
    }

    fn inject_fault_selective(&self, faults: &[(&str, bool)]) {
        for (path, file) in self.files.borrow().iter() {
            for (stem, fault) in faults {
                if path.contains(stem) {
                    file.inject_fault(*fault);
                    break;
                }
            }
        }
    }

    fn print_stats(&self) {
        for (path, file) in self.files.borrow().iter() {
            if path.contains("ephemeral") {
                // Files created for ephemeral tables just add noise to the simulator output and aren't by default very interesting to debug
                continue;
            }
            tracing::info!(
                "\n===========================\n\nPath: {}\n{}",
                path,
                file.stats_table()
            );
        }
    }

    fn syncing(&self) -> bool {
        let callbacks = self.callbacks.try_lock().unwrap();
        let timeouts = self.timeouts.try_lock().unwrap();
        callbacks.iter().chain(timeouts.iter()).any(|operation| {
            matches!(operation.op, OperationType::Sync { .. })
                && !operation.op.get_completion().finished()
        })
    }

    fn close_files(&self) {
        for file in self.files.borrow().values() {
            file.closed.set(true);
        }
    }

    fn power_loss(&self) -> Result<()> {
        assert!(
            !self.power_loss_in_progress.swap(true, Ordering::AcqRel),
            "power loss cannot be nested"
        );
        self.generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
                generation.checked_add(1)
            })
            .expect("memory simulator crash generation overflow");
        self.close_files();
        let pending = {
            let mut callbacks = self.callbacks.lock();
            let mut timeouts = self.timeouts.lock();
            callbacks
                .drain(..)
                .chain(timeouts.drain(..))
                .collect::<Vec<_>>()
        };
        for operation in pending {
            let completion = operation.op.get_completion();
            if completion.succeeded() {
                panic!("queued file operation completed without running its operation");
            }
            if !completion.finished() {
                completion.abort();
            } else {
                assert!(completion.failed());
            }
        }
        assert!(
            self.callbacks.lock().is_empty() && self.timeouts.lock().is_empty(),
            "completion callbacks must not queue new I/O during power loss"
        );
        self.file_states.borrow_mut().retain(|state| {
            let Some(state) = state.upgrade() else {
                return false;
            };
            state.borrow_mut().power_loss();
            true
        });
        self.power_loss_in_progress.store(false, Ordering::Release);
        Ok(())
    }

    fn persist_files(&self) -> anyhow::Result<()> {
        let files = self.files.borrow();
        for (file_path, file) in files.iter() {
            if file_path.ends_with(".db") || file_path.ends_with("wal") || file_path.ends_with("lg")
            {
                let state = file.state.borrow();
                let mut output = BufWriter::new(std::fs::File::create(file_path)?);
                for (index, block) in state.buffer.blocks.iter().enumerate() {
                    let len = (state.buffer.len - index * FILE_BLOCK_SIZE).min(FILE_BLOCK_SIZE);
                    output.write_all(&block[..len])?;
                }
                output.flush()?;
            }
        }
        Ok(())
    }
}

impl Clock for MemorySimIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        MonotonicInstant::now()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.clock.now().into()
    }
}

impl IO for MemorySimIO {
    fn open_file(
        &self,
        path: &str,
        _flags: OpenFlags, // TODO: ignoring open flags for now as we don't test read only mode in the simulator yet
        _direct: bool,
    ) -> Result<Arc<dyn turso_core::File>> {
        self.ensure_io_allowed()?;
        let mut files = self.files.borrow_mut();
        let fd = path.to_string();
        let file = if let Some(file) = files.get(path).cloned() {
            if !file.is_open() {
                let file = Arc::new(file.reopen());
                files.insert(fd, file.clone());
                file
            } else {
                file
            }
        } else {
            let file = Arc::new(MemorySimFile::new(
                self.callbacks.clone(),
                self.seed,
                self.latency_probability,
                self.clock.clone(),
                self.generation.clone(),
            ));
            self.file_states
                .borrow_mut()
                .push(Arc::downgrade(&file.state));
            files.insert(fd, file.clone());
            file
        };

        Ok(file)
    }

    fn step(&self) -> Result<()> {
        let mut callbacks = self.callbacks.lock();
        let mut timeouts = self.timeouts.lock();
        tracing::trace!(
            callbacks.len = callbacks.len(),
            timeouts.len = timeouts.len()
        );
        let now = self.current_time_wall_clock();

        callbacks.append(&mut timeouts);

        while let Some(callback) = callbacks.pop() {
            let completion = callback.op.get_completion();
            if completion.finished() {
                assert!(
                    completion.failed(),
                    "queued file operation completed without running its operation"
                );
                continue;
            }

            if callback.time.is_none() || callback.time.is_some_and(|time| time < now) {
                if callback.fault {
                    // Inject the fault by aborting the completion
                    tracing::error!("Fault injection: aborting completion");
                    completion.abort();
                    continue;
                }
                callback.do_operation();
            } else {
                timeouts.push(callback);
            }
        }
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        self.rng.borrow_mut().random()
    }

    fn fill_bytes(&self, dest: &mut [u8]) {
        self.rng.borrow_mut().fill_bytes(dest);
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        self.ensure_io_allowed()?;
        self.files.borrow_mut().shift_remove(path);
        Ok(())
    }

    fn file_id(&self, path: &str) -> Result<turso_core::io::FileId> {
        Ok(turso_core::io::FileId::from_path_hash(path))
    }
}

impl FileContents {
    pub(super) fn read(&self, offset: usize, buf: &mut [u8]) -> usize {
        let len = self.len.saturating_sub(offset).min(buf.len());
        if len == 0 {
            return 0;
        }
        let mut copied = 0;
        while copied < len {
            let pos = offset + copied;
            let within_block = pos % FILE_BLOCK_SIZE;
            let count = (FILE_BLOCK_SIZE - within_block).min(len - copied);
            buf[copied..copied + count].copy_from_slice(
                &self.blocks[pos / FILE_BLOCK_SIZE][within_block..within_block + count],
            );
            copied += count;
        }
        buf[len..].fill(0);
        len
    }
}

impl FileState {
    pub(super) fn write(&mut self, offset: usize, buf: &[u8]) -> usize {
        self.resize(self.buffer.len.max(offset + buf.len()));
        let mut copied = 0;
        while copied < buf.len() {
            let pos = offset + copied;
            let within_block = pos % FILE_BLOCK_SIZE;
            let count = (FILE_BLOCK_SIZE - within_block).min(buf.len() - copied);
            let index = pos / FILE_BLOCK_SIZE;
            let block = &mut self.buffer.blocks[index];
            let bytes = &buf[copied..copied + count];
            match Arc::get_mut(block) {
                Some(block) => block[within_block..within_block + count].copy_from_slice(bytes),
                None if count == FILE_BLOCK_SIZE => *block = Arc::new(bytes.try_into().unwrap()),
                None => {
                    Arc::make_mut(block)[within_block..within_block + count].copy_from_slice(bytes)
                }
            }
            self.dirty_blocks.insert(index);
            copied += count;
        }
        buf.len()
    }

    pub(super) fn resize(&mut self, len: usize) {
        let old_len = self.buffer.len;
        let block_count = len.div_ceil(FILE_BLOCK_SIZE);
        self.buffer
            .blocks
            .resize_with(block_count, || Arc::new([0; FILE_BLOCK_SIZE]));
        if len < old_len {
            self.dirty_blocks.retain(|&index| index < block_count);
            if !len.is_multiple_of(FILE_BLOCK_SIZE) {
                let last = Arc::make_mut(self.buffer.blocks.last_mut().unwrap());
                last[len % FILE_BLOCK_SIZE..].fill(0);
            }
        }
        if len != old_len {
            self.dirty_blocks
                .extend(len.min(old_len) / FILE_BLOCK_SIZE..block_count);
        }
        self.buffer.len = len;
    }
}
