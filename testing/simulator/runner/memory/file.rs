use std::{
    cell::{Cell, RefCell},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use rand::{Rng as _, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tracing::{Level, instrument};
use turso_core::{Completion, CompletionError, File, LimboError, Result};

use crate::runner::{
    clock::SimulatorClock,
    memory::io::{CallbackQueue, FileState, Operation, OperationType},
};

/// Tracks IO calls and faults for each type of I/O operation
#[derive(Clone, Debug, Default)]
struct IOTracker {
    pread_calls: usize,
    pread_faults: usize,

    pwrite_calls: usize,
    pwrite_faults: usize,

    pwritev_calls: usize,
    pwritev_faults: usize,

    sync_calls: usize,
    sync_faults: usize,

    truncate_calls: usize,
    truncate_faults: usize,
}

impl IOTracker {
    fn total_calls(&self) -> usize {
        self.pread_calls
            + self.pwrite_calls
            + self.pwritev_calls
            + self.sync_calls
            + self.truncate_calls
    }
}

pub struct MemorySimFile {
    // TODO: maybe have a pending queue which is fast to append
    // and then we just do a mem swap the pending with the callback to minimize lock contention on callback queue
    pub(super) callbacks: CallbackQueue,
    pub(super) state: Arc<RefCell<FileState>>,
    // TODO: add fault map later here
    pub(super) closed: Cell<bool>,
    generation: Arc<AtomicU64>,
    opened_generation: u64,
    io_tracker: RefCell<IOTracker>,
    pub rng: RefCell<ChaCha8Rng>,
    pub latency_probability: u8,
    clock: Arc<SimulatorClock>,
    fault: Cell<bool>,
}

unsafe impl Send for MemorySimFile {}
unsafe impl Sync for MemorySimFile {}

impl MemorySimFile {
    pub(super) fn new(
        callbacks: CallbackQueue,
        seed: u64,
        latency_probability: u8,
        clock: Arc<SimulatorClock>,
        generation: Arc<AtomicU64>,
    ) -> Self {
        let opened_generation = generation.load(Ordering::Acquire);
        Self {
            callbacks,
            state: Arc::new(RefCell::new(FileState::default())),
            closed: Cell::new(false),
            generation,
            opened_generation,
            io_tracker: RefCell::new(IOTracker::default()),
            rng: RefCell::new(ChaCha8Rng::seed_from_u64(seed)),
            latency_probability,
            clock,
            fault: Cell::new(false),
        }
    }

    pub fn inject_fault(&self, fault: bool) {
        self.fault.set(fault);
    }

    pub(super) fn reopen(&self) -> Self {
        Self {
            callbacks: self.callbacks.clone(),
            state: self.state.clone(),
            closed: Cell::new(false),
            generation: self.generation.clone(),
            opened_generation: self.generation.load(Ordering::Acquire),
            io_tracker: RefCell::new(self.io_tracker.borrow().clone()),
            rng: RefCell::new(self.rng.borrow().clone()),
            latency_probability: self.latency_probability,
            clock: self.clock.clone(),
            fault: Cell::new(self.fault.get()),
        }
    }

    fn ensure_open(&self) -> Result<()> {
        if !self.is_open() {
            return Err(LimboError::CompletionError(CompletionError::IOError(
                std::io::ErrorKind::Other,
                "memory simulator file is closed",
            )));
        }
        Ok(())
    }

    pub(super) fn is_open(&self) -> bool {
        !self.closed.get() && self.opened_generation == self.generation.load(Ordering::Acquire)
    }

    pub fn stats_table(&self) -> String {
        let io_tracker = self.io_tracker.borrow();
        let sum_calls = io_tracker.total_calls();
        let stats_table = [
            "op        calls    faults  ".to_string(),
            "--------- -------- --------".to_string(),
            format!(
                "pread     {:8} {:8}",
                io_tracker.pread_calls, io_tracker.pread_faults
            ),
            format!(
                "pwrite    {:8} {:8}",
                io_tracker.pwrite_calls, io_tracker.pwrite_faults
            ),
            format!(
                "pwritev   {:8} {:8}",
                io_tracker.pwritev_calls, io_tracker.pwritev_faults
            ),
            format!(
                "sync      {:8} {:8}",
                io_tracker.sync_calls, io_tracker.sync_faults
            ),
            format!(
                "truncate  {:8} {:8}",
                io_tracker.truncate_calls, io_tracker.truncate_faults
            ),
            "--------- -------- --------".to_string(),
            format!("total     {sum_calls:8}"),
        ];

        stats_table.join("\n")
    }

    #[instrument(skip_all, level = Level::TRACE)]
    fn generate_latency(&self) -> Option<turso_core::WallClockInstant> {
        let mut rng = self.rng.borrow_mut();
        // Chance to introduce some latency
        rng.random_bool(self.latency_probability as f64 / 100.0)
            .then(|| {
                let now = self.clock.now();
                let sum = now + std::time::Duration::from_millis(rng.random_range(5..20));
                sum.into()
            })
    }

    fn insert_op(&self, op: OperationType) {
        // FIXME: currently avoid any fsync faults until we correctly define the expected behaviour in the simulator
        let fault = self.fault.get() && !matches!(op, OperationType::Sync { .. });
        if fault {
            let mut io_tracker = self.io_tracker.borrow_mut();
            match &op {
                OperationType::Read { .. } => io_tracker.pread_faults += 1,
                OperationType::Write { .. } => io_tracker.pwrite_faults += 1,
                OperationType::WriteV { .. } => io_tracker.pwritev_faults += 1,
                OperationType::Sync { .. } => io_tracker.sync_faults += 1,
                OperationType::Truncate { .. } => io_tracker.truncate_faults += 1,
            }
        }

        self.callbacks.lock().push(Operation {
            time: self.generate_latency(),
            op,
            fault,
            file: self.state.clone(),
        });
    }
}

impl File for MemorySimFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        self.ensure_open()
    }

    fn unlock_file(&self) -> Result<()> {
        self.ensure_open()
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        self.ensure_open()?;
        self.io_tracker.borrow_mut().pread_calls += 1;

        let op = OperationType::Read {
            completion: c.clone(),
            offset: pos as usize,
        };
        self.insert_op(op);
        Ok(c)
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<turso_core::Buffer>,
        c: Completion,
    ) -> Result<Completion> {
        self.ensure_open()?;
        self.io_tracker.borrow_mut().pwrite_calls += 1;
        let op = OperationType::Write {
            buffer,
            completion: c.clone(),
            offset: pos as usize,
        };
        self.insert_op(op);
        Ok(c)
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<turso_core::Buffer>>,
        c: Completion,
    ) -> Result<Completion> {
        self.ensure_open()?;
        if buffers.is_empty() {
            c.complete(0);
            return Ok(c);
        }
        if buffers.len() == 1 {
            return self.pwrite(pos, buffers[0].clone(), c);
        }
        self.io_tracker.borrow_mut().pwritev_calls += 1;
        let op = OperationType::WriteV {
            buffers,
            completion: c.clone(),
            offset: pos as usize,
        };
        self.insert_op(op);
        Ok(c)
    }

    fn sync(&self, c: Completion, _sync_type: turso_core::io::FileSyncType) -> Result<Completion> {
        self.ensure_open()?;
        self.io_tracker.borrow_mut().sync_calls += 1;
        let op = OperationType::Sync {
            completion: c.clone(),
            snapshot: self.state.borrow_mut().sync_started(),
        };
        self.insert_op(op);
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        self.ensure_open()?;
        // TODO: size operation should also be scheduled. But this requires a change in how we
        // Use this function internally in Turso
        Ok(self.state.borrow().buffer.len as u64)
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.ensure_open()?;
        self.io_tracker.borrow_mut().truncate_calls += 1;
        let op = OperationType::Truncate {
            completion: c.clone(),
            len: len as usize,
        };
        self.insert_op(op);
        Ok(c)
    }
}
