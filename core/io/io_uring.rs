#![allow(clippy::arc_with_non_send_sync)]

use super::{
    common, Completion, CompletionInner, File, OpenFlags, SharedWalLockKind, SharedWalMappedRegion,
    IO,
};
use crate::error::io_error;
use crate::io::clock::{Clock, DefaultClock, MonotonicInstant, WallClockInstant};
use crate::io::unix::{
    unix_shared_wal_lock_byte, unix_shared_wal_map, unix_shared_wal_unlock_byte,
};
use crate::storage::wal::CKPT_BATCH_PAGES;
use crate::sync::Mutex;
use crate::turso_assert;
use crate::{CompletionError, LimboError, Result};
use rustix::fs::{self, FlockOperation, OFlags};
use std::ptr::NonNull;
use std::{
    collections::{HashMap, VecDeque},
    io::ErrorKind,
    ops::Deref,
    os::{fd::AsFd, unix::io::AsRawFd},
    sync::Arc,
};
use tracing::{debug, trace, warn};

/// Size of the io_uring submission and completion queues
const ENTRIES: u32 = 512;

/// Idle timeout for the sqpoll kernel thread before it needs
/// to be woken back up by a call IORING_ENTER_SQ_WAKEUP flag.
/// (handled by the io_uring crate in `submit_and_wait`)
const SQPOLL_IDLE: u32 = 1000;

/// Number of Vec<Box<[iovec]>> we preallocate on initialization
const IOVEC_POOL_SIZE: usize = 64;

/// Maximum number of iovec entries per writev operation.
/// IOV_MAX is typically 1024
const MAX_IOVEC_ENTRIES: usize = CKPT_BATCH_PAGES;

/// Maximum number of I/O operations to wait for in a single run,
/// waiting for > 1 can reduce the amount of `io_uring_enter` syscalls we
/// make, but can increase single operation latency.
const MAX_WAIT: usize = 4;

/// One memory arena for DB pages and another for WAL frames
const ARENA_COUNT: usize = 2;

/// user_data tag for cancellation operations
const CANCEL_TAG: u64 = 1;

/// Probed io_uring opcode support. Opcodes that are not supported by the
/// running kernel fall back to synchronous POSIX syscalls.
struct UringCapabilities {
    ftruncate: bool,
}

pub struct UringIO {
    /// The io_uring instance, shared by ref. `submit_and_wait`, `submit`, and
    /// `submitter()` only need `&self` — multiple threads can issue these
    /// concurrently and the kernel handles serialization via the ring's
    /// atomic head/tail. Pulling the ring out of the Mutex means a thread
    /// blocked on `submit_and_wait` doesn't block other threads from pushing
    /// new SQEs or draining the CQ.
    ring: Arc<io_uring::IoUring>,
    /// Mutex-guarded auxiliary state. The Mutex is held only briefly:
    /// during `submission_shared()`-backed pushes, and during
    /// `completion_shared()`-backed CQ drains. The unsafe `_shared()` APIs
    /// require that no other SQ/CQ object exists; holding this Mutex
    /// satisfies that invariant.
    state: Arc<Mutex<RingState>>,
    /// Serializes the blocking `submit_and_wait` syscall. Multiple
    /// concurrent waiters are unsafe: when N threads each ask the kernel
    /// for `min_complete=K` events and only K total arrive, the kernel
    /// satisfies *one* waiter (Linux wakes a single waiter from the
    /// `io_sq_data` wait queue) — the others stay blocked on completions
    /// that won't arrive. We avoid that by ensuring at most one thread is
    /// inside `submit_and_wait` at any time. Submission and CQ drain run
    /// under `state` and aren't blocked by this lock.
    wait_lock: Arc<Mutex<()>>,
    caps: Arc<UringCapabilities>,
}

unsafe impl Send for UringIO {}
unsafe impl Sync for UringIO {}
crate::assert::assert_send_sync!(UringIO);

struct RingState {
    pending_ops: usize,
    writev_states: HashMap<u64, WritevState>,
    overflow: VecDeque<io_uring::squeue::Entry>,
    iov_pool: IovecPool,
    free_arenas: [Option<(NonNull<u8>, usize)>; ARENA_COUNT],
}

impl RingState {
    fn empty(&self) -> bool {
        self.pending_ops == 0 && self.overflow.is_empty()
    }

    /// SAFETY: caller must guarantee no other `SubmissionQueue` exists for
    /// `ring`. Holding the `RingState` Mutex satisfies that invariant.
    unsafe fn flush_overflow(&mut self, ring: &io_uring::IoUring) {
        if self.overflow.is_empty() {
            return;
        }
        let mut sq = ring.submission_shared();
        while !self.overflow.is_empty() {
            if sq.is_full() {
                break;
            }
            let entry = self.overflow.pop_front().expect("checked not empty");
            if sq.push(&entry).is_err() {
                self.overflow.push_front(entry);
                break;
            }
            self.pending_ops += 1;
        }
    }

    /// SAFETY: caller must guarantee no other `SubmissionQueue` exists for
    /// `ring` (i.e. caller holds the `RingState` Mutex).
    unsafe fn submit_entry(&mut self, ring: &io_uring::IoUring, entry: &io_uring::squeue::Entry) {
        trace!("submit_entry({:?})", entry);
        self.flush_overflow(ring);
        let pushed = {
            let mut sq = ring.submission_shared();
            sq.push(entry).is_ok()
        };
        if pushed {
            self.pending_ops += 1;
            return;
        }
        // SQ full — buffer locally and ask the kernel to drain so the next
        // attempt has space.
        self.overflow.push_back(entry.clone());
        let _ = ring.submit();
    }

    /// SAFETY: same contract as `submit_entry`.
    unsafe fn submit_cancel_urgent(
        &mut self,
        ring: &io_uring::IoUring,
        entry: &io_uring::squeue::Entry,
    ) -> Result<()> {
        let pushed = {
            let mut sq = ring.submission_shared();
            sq.push(entry).is_ok()
        };
        if pushed {
            self.pending_ops += 1;
            return Ok(());
        }
        self.overflow.push_front(entry.clone());
        ring.submit().map_err(|e| io_error(e, "io_uring_submit"))?;
        Ok(())
    }
}

/// preallocated vec of iovec arrays to avoid allocations during writev operations
struct IovecPool {
    pool: Vec<Box<[libc::iovec; MAX_IOVEC_ENTRIES]>>,
}

impl IovecPool {
    fn new() -> Self {
        let pool = (0..IOVEC_POOL_SIZE)
            .map(|_| {
                Box::new(
                    [libc::iovec {
                        iov_base: std::ptr::null_mut(),
                        iov_len: 0,
                    }; MAX_IOVEC_ENTRIES],
                )
            })
            .collect();
        Self { pool }
    }

    #[inline(always)]
    fn acquire(&mut self) -> Option<Box<[libc::iovec; MAX_IOVEC_ENTRIES]>> {
        self.pool.pop()
    }

    #[inline(always)]
    fn release(&mut self, iovec: Box<[libc::iovec; MAX_IOVEC_ENTRIES]>) {
        if self.pool.len() < IOVEC_POOL_SIZE {
            self.pool.push(iovec);
        }
    }
}

impl UringIO {
    pub fn new() -> Result<Self> {
        let ring = match io_uring::IoUring::builder()
            .setup_sqpoll(SQPOLL_IDLE)
            .build(ENTRIES)
        {
            Ok(ring) => ring,
            Err(_) => io_uring::IoUring::new(ENTRIES).map_err(|e| io_error(e, "io_uring_setup"))?,
        };
        // RL_MEMLOCK cap is typically 8MB, the current design is to have one large arena
        // registered at startup and therefore we can simply use the zero index, falling back
        // to similar logic as the existing buffer pool for cases where it is over capacity.
        ring.submitter()
            .register_buffers_sparse(ARENA_COUNT as u32)
            .map_err(|e| io_error(e, "register_buffers"))?;
        // Probe supported opcodes so we can fall back to POSIX for unsupported ones.
        let mut probe = io_uring::register::Probe::new();
        let caps = if ring.submitter().register_probe(&mut probe).is_ok() {
            UringCapabilities {
                ftruncate: probe.is_supported(io_uring::opcode::Ftruncate::CODE),
            }
        } else {
            UringCapabilities { ftruncate: false }
        };
        if !caps.ftruncate {
            warn!("io_uring: IORING_OP_FTRUNCATE not supported by kernel, using POSIX fallback");
        }
        let state = RingState {
            overflow: VecDeque::new(),
            pending_ops: 0,
            writev_states: HashMap::default(),
            iov_pool: IovecPool::new(),
            free_arenas: [const { None }; ARENA_COUNT],
        };
        debug!("Using IO backend 'io-uring'");
        Ok(Self {
            ring: Arc::new(ring),
            state: Arc::new(Mutex::new(state)),
            wait_lock: Arc::new(Mutex::new(())),
            caps: Arc::new(caps),
        })
    }
}

/// State to track an ongoing writev operation in
/// the case of a partial write.
struct WritevState {
    /// File descriptor/id of the file we are writing to
    file_id: io_uring::types::Fd,
    /// absolute file offset for next submit
    file_pos: u64,
    /// current buffer index in `bufs`
    current_buffer_idx: usize,
    /// intra-buffer offset
    current_buffer_offset: usize,
    /// total bytes written so far
    total_written: usize,
    /// cache the sum of all buffer lengths for the total expected write
    total_len: usize,
    /// buffers to write
    bufs: Vec<Arc<crate::Buffer>>,
    /// we keep the last iovec allocation alive until final CQE
    last_iov_allocation: Option<Box<[libc::iovec; MAX_IOVEC_ENTRIES]>>,
}

impl WritevState {
    fn new(file: &UringFile, pos: u64, bufs: Vec<Arc<crate::Buffer>>) -> Self {
        let file_id = file.file.as_raw_fd();
        let total_len = bufs.iter().map(|b| b.len()).sum();
        Self {
            file_id: io_uring::types::Fd(file_id),
            file_pos: pos,
            current_buffer_idx: 0,
            current_buffer_offset: 0,
            total_written: 0,
            bufs,
            last_iov_allocation: None,
            total_len,
        }
    }

    #[inline(always)]
    fn remaining(&self) -> usize {
        self.total_len - self.total_written
    }

    /// Advance (idx, off, pos) after written bytes
    #[inline(always)]
    fn advance(&mut self, written: u64) {
        let mut remaining = written;
        while remaining > 0 {
            let current_buf_len = self.bufs[self.current_buffer_idx].len();
            let left = current_buf_len - self.current_buffer_offset;
            if remaining < left as u64 {
                self.current_buffer_offset += remaining as usize;
                self.file_pos += remaining;
                remaining = 0;
            } else {
                remaining -= left as u64;
                self.file_pos += left as u64;
                self.current_buffer_idx += 1;
                self.current_buffer_offset = 0;
            }
        }
        self.total_written += written as usize;
    }

    #[inline(always)]
    /// Free the allocation that keeps the iovec array alive while writev is ongoing
    fn free_last_iov(&mut self, pool: &mut IovecPool) {
        if let Some(allocation) = self.last_iov_allocation.take() {
            pool.release(allocation);
        }
    }
}

impl RingState {
    #[cfg(debug_assertions)]
    fn debug_check_fixed(&self, idx: u32, ptr: *const u8, len: usize) {
        let (base, blen) = self.free_arenas[idx as usize].expect("slot not registered");
        let start = base.as_ptr() as usize;
        let end = start + blen;
        let p = ptr as usize;
        turso_assert!(
            p >= start && p + len <= end,
            "Fixed operation, pointer out of registered range"
        );
    }

    /// Submit or resubmit a writev operation. SAFETY: caller must hold the
    /// `RingState` Mutex (so no other `SubmissionQueue` exists for `ring`).
    unsafe fn submit_writev(&mut self, ring: &io_uring::IoUring, key: u64, mut st: WritevState) {
        st.free_last_iov(&mut self.iov_pool);

        let mut iov_allocation = self.iov_pool.acquire().unwrap_or_else(|| {
            Box::new(
                [libc::iovec {
                    iov_base: std::ptr::null_mut(),
                    iov_len: 0,
                }; MAX_IOVEC_ENTRIES],
            )
        });

        let mut iov_count = 0;
        let mut last_end: Option<(*const u8, usize)> = None;

        for (idx, buffer) in st.bufs.iter().enumerate().skip(st.current_buffer_idx) {
            let mut ptr = buffer.as_ptr();
            let mut len = buffer.len();
            if idx == st.current_buffer_idx && st.current_buffer_offset != 0 {
                turso_assert!(
                    st.current_buffer_offset <= len,
                    "writev state offset out of bounds"
                );
                ptr = ptr.add(st.current_buffer_offset);
                len -= st.current_buffer_offset;
            }
            if let Some((last_ptr, last_len)) = last_end {
                if last_ptr.add(last_len) == ptr {
                    iov_allocation[iov_count - 1].iov_len += len;
                    last_end = Some((last_ptr, last_len + len));
                    continue;
                }
            }
            iov_allocation[iov_count] = libc::iovec {
                iov_base: ptr as *mut _,
                iov_len: len,
            };
            last_end = Some((ptr, len));
            iov_count += 1;
            if iov_count >= MAX_IOVEC_ENTRIES {
                break;
            }
        }

        let ptr = iov_allocation.as_ptr() as *mut libc::iovec;
        st.last_iov_allocation = Some(iov_allocation);
        let entry = io_uring::opcode::Writev::new(st.file_id, ptr, iov_count as u32)
            .offset(st.file_pos)
            .build()
            .user_data(key);
        self.writev_states.insert(key, st);
        self.submit_entry(ring, &entry);
    }

    /// Handle a writev CQE. SAFETY: caller must hold the `RingState` Mutex.
    unsafe fn handle_writev_completion(
        &mut self,
        ring: &io_uring::IoUring,
        mut state: WritevState,
        user_data: u64,
        result: i32,
    ) {
        if result < 0 {
            let err = std::io::Error::from_raw_os_error(-result);
            tracing::error!("writev failed (user_data: {}): {}", user_data, err);
            state.free_last_iov(&mut self.iov_pool);
            completion_from_key(user_data).error(CompletionError::IOError(err.kind(), "pwritev"));
            return;
        }

        let written = result;

        if written == 0 && state.remaining() > 0 {
            state.free_last_iov(&mut self.iov_pool);
            completion_from_key(user_data).error(CompletionError::ShortWrite);
            return;
        }
        state.advance(written as u64);

        match state.remaining() {
            0 => {
                tracing::debug!(
                    "writev operation completed: wrote {} bytes",
                    state.total_written
                );
                state.free_last_iov(&mut self.iov_pool);
                completion_from_key(user_data).complete(state.total_written as i32);
            }
            remaining => {
                tracing::trace!(
                    "resubmitting writev operation for user_data {}: wrote {} bytes, remaining {}",
                    user_data,
                    written,
                    remaining
                );
                self.submit_writev(ring, user_data, state);
                // Progress wake: the future is parked on the parent
                // completion's waker, but `complete()` only fires on the
                // final chunk. Without this, intermediate-chunk completions
                // never wake the future, the resubmitted chunks pile up,
                // and the task deadlocks.
                wake_user_data(user_data);
            }
        }
    }
}

impl IO for UringIO {
    fn supports_shared_wal_coordination(&self) -> bool {
        true
    }

    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        trace!("open_file(path = {})", path);
        let mut file = std::fs::File::options();
        file.read(true);

        if !flags.contains(OpenFlags::ReadOnly) {
            file.write(true);
            file.create(flags.contains(OpenFlags::Create));
        }

        let file = file.open(path).map_err(|e| io_error(e, "open"))?;
        // Let's attempt to enable direct I/O. Not all filesystems support it
        // so ignore any errors.
        let fd = file.as_fd();
        if direct {
            match fs::fcntl_setfl(fd, OFlags::DIRECT) {
                Ok(_) => {}
                Err(error) => debug!("Error {error:?} returned when setting O_DIRECT flag to read file. The performance of the system may be affected"),
            }
        }
        let uring_file = Arc::new(UringFile {
            ring: self.ring.clone(),
            state: self.state.clone(),
            caps: self.caps.clone(),
            file,
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err()
            && !flags.intersects(OpenFlags::ReadOnly | OpenFlags::NoLock)
        {
            uring_file.lock_file(true)?;
        }
        Ok(uring_file)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        std::fs::remove_file(path).map_err(|e| io_error(e, "remove_file"))?;
        Ok(())
    }

    fn cancel(&self, completions: &[Completion]) -> Result<()> {
        let mut state = self.state.lock();
        for c in completions {
            c.abort();
            // dont want to leak the refcount bump with `get_key`/into_raw here, so we use as_ptr
            let e = io_uring::opcode::AsyncCancel::new(Arc::as_ptr(c.get_inner()) as u64)
                .build()
                .user_data(CANCEL_TAG);
            // SAFETY: holding `state` Mutex.
            unsafe { state.submit_cancel_urgent(&self.ring, &e)? };
        }
        Ok(())
    }

    /// Drive io_uring forward.
    ///
    /// Leader/follower concurrency model:
    /// - Submitters only take `state` (briefly, to push an SQE). They're
    ///   never blocked by the kernel-side wait, so concurrent submitters
    ///   pipeline their SQEs into the ring while another thread is
    ///   waiting on the kernel.
    /// - One thread at a time is the *leader*: it holds `wait_lock`
    ///   and runs `submit_and_wait` + `drain_cq` in a tight loop until
    ///   the ring drains. The drain *must* happen while the wait guard
    ///   is still held — if released before draining, a follower could
    ///   compute `pending_ops` to include CQEs we're about to consume
    ///   and block forever on completions that no longer exist.
    /// - Followers (concurrent callers of `step`) `try_lock` the
    ///   `wait_lock`. If they lose the race they return immediately:
    ///   the current leader will fire their waker as it drains. This
    ///   replaces the previous "block on `wait_lock` and serialize"
    ///   behavior, so N concurrent readers no longer queue up
    ///   N-deep on the kernel call.
    /// - Multiple concurrent kernel waiters on the same ring would
    ///   themselves deadlock (Linux only wakes one waiter from the
    ///   ring's wait queue when `min_complete` is reached), which is the
    ///   other reason for serializing at this layer.
    fn step(&self) -> Result<()> {
        // Try to become the leader. If `wait_lock` is held, someone else
        // is already inside `submit_and_wait`/`drain_cq` and will fire
        // wakers on every completion drained: including ours. The
        // follower returns Ok immediately and lets the calling Future
        // park on its completion's waker.
        let Some(_wait_guard) = self.wait_lock.try_lock() else {
            return Ok(());
        };

        // Leader path: keep draining until the ring is empty. Looping
        // here matters: while we were in the kernel, more submitters
        // may have queued SQEs, and their futures need *us* to drain
        // their CQEs before the calling task can make progress.
        loop {
            let pending = {
                let mut state = self.state.lock();
                // SAFETY: holding `state` Mutex.
                unsafe { state.flush_overflow(&self.ring) };
                if state.empty() {
                    return Ok(());
                }
                state.pending_ops
            };

            let wants = std::cmp::min(pending, MAX_WAIT);
            tracing::trace!("submit_and_wait for {wants} pending operations to complete");
            match self.ring.submit_and_wait(wants) {
                Ok(_) => {}
                // A signal (a profiler, a debugger attaching, a timer) cuts
                // the wait short without completing anything. That is not
                // an I/O error: go round again and keep waiting.
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(io_error(e, "io_uring_submit_and_wait")),
            }

            // Drain while still holding `_wait_guard`.
            self.drain_cq()?;
        }
    }

    fn register_fixed_buffer(&self, ptr: std::ptr::NonNull<u8>, len: usize) -> Result<u32> {
        turso_assert!(
            len % 512 == 0,
            "fixed buffer length must be logical block aligned"
        );
        let mut state = self.state.lock();
        let slot =
            state.free_arenas.iter().position(|e| e.is_none()).ok_or({
                crate::error::CompletionError::UringIOError("no free fixed buffer slots")
            })?;
        unsafe {
            self.ring
                .submitter()
                .register_buffers_update(
                    slot as u32,
                    &[libc::iovec {
                        iov_base: ptr.as_ptr() as *mut libc::c_void,
                        iov_len: len,
                    }],
                    None,
                )
                .map_err(|e| io_error(e, "register_buffers_update"))?
        };
        state.free_arenas[slot] = Some((ptr, len));
        Ok(slot as u32)
    }
}

impl UringIO {
    /// Drain whatever CQEs are currently ready into completion callbacks.
    /// Returns `true` if at least one CQE was processed.
    fn drain_cq(&self) -> Result<bool> {
        let mut state = self.state.lock();
        let mut drained_any = false;
        loop {
            // SAFETY: holding `state` Mutex; no other CompletionQueue exists.
            let mut cq = unsafe { self.ring.completion_shared() };
            let Some(cqe) = cq.next() else {
                return Ok(drained_any);
            };
            drained_any = true;
            state.pending_ops -= 1;
            let user_data = cqe.user_data();
            if user_data == CANCEL_TAG {
                continue;
            }
            let result = cqe.result();
            turso_assert!(
                user_data != 0,
                "user_data must not be zero, we dont submit linked timeouts that would cause this"
            );
            if let Some(wstate) = state.writev_states.remove(&user_data) {
                drop(cq);
                // SAFETY: still holding `state` Mutex.
                unsafe { state.handle_writev_completion(&self.ring, wstate, user_data, result) };
                continue;
            }
            if result < 0 {
                let errno = -result;
                let err = std::io::Error::from_raw_os_error(errno);
                completion_from_key(user_data)
                    .error(CompletionError::IOError(err.kind(), "io_uring_cqe"));
            } else {
                completion_from_key(user_data).complete(result);
            }
        }
    }
}

impl Clock for UringIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
    }
}

#[inline(always)]
/// use the callback pointer as the user_data for the operation as is
/// common practice for io_uring to prevent more indirection
fn get_key(c: Completion) -> u64 {
    Arc::into_raw(c.get_inner().clone()) as u64
}

#[inline(always)]
/// convert the user_data back to an Completion pointer
fn completion_from_key(key: u64) -> Completion {
    let c_inner = unsafe { Arc::from_raw(key as *const CompletionInner) };
    Completion {
        inner: Some(c_inner),
    }
}

/// Wake the waker registered on the completion identified by `key` (and on
/// its parent group, if any) without consuming the kernel's `Arc::into_raw`
/// reference. Used to fire a progress wake when an operation is resubmitted
/// (e.g., a writev split across chunks) so the future re-polls and continues
/// draining the CQ.
fn wake_user_data(key: u64) {
    let ptr = key as *const CompletionInner;
    // SAFETY: kernel owns one strong ref via the Arc::into_raw'd `key`. We
    // re-materialize the Arc to clone it, then re-leak the original so the
    // kernel's ref count is unchanged when this function returns.
    let kernel_ref = unsafe { Arc::from_raw(ptr) };
    let cloned = kernel_ref.clone();
    let _ = Arc::into_raw(kernel_ref);
    Completion {
        inner: Some(cloned),
    }
    .wake_progress();
}

pub struct UringFile {
    ring: Arc<io_uring::IoUring>,
    state: Arc<Mutex<RingState>>,
    caps: Arc<UringCapabilities>,
    file: std::fs::File,
}

impl Deref for UringFile {
    type Target = std::fs::File;
    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

unsafe impl Send for UringFile {}
unsafe impl Sync for UringFile {}
crate::assert::assert_send_sync!(UringFile);

impl File for UringFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        let fd = self.file.as_fd();
        // F_SETLK is a non-blocking lock. The lock will be released when the file is closed
        // or the process exits or after an explicit unlock.
        fs::fcntl_lock(
            fd,
            if exclusive {
                FlockOperation::NonBlockingLockExclusive
            } else {
                FlockOperation::NonBlockingLockShared
            },
        )
        .map_err(|e| {
            let io_error = std::io::Error::from(e);
            let message = match io_error.kind() {
                ErrorKind::WouldBlock => {
                    "Failed locking file. File is locked by another process".to_string()
                }
                _ => format!("Failed locking file, {io_error}"),
            };
            LimboError::LockingError(message)
        })?;

        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        let fd = self.file.as_fd();
        fs::fcntl_lock(fd, FlockOperation::NonBlockingUnlock).map_err(|e| {
            LimboError::LockingError(format!(
                "Failed to release file lock: {}",
                std::io::Error::from(e)
            ))
        })?;
        Ok(())
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let r = c.as_read();
        let read_e = {
            let buf = r.buf();
            let ptr = buf.as_mut_ptr();
            let fd = io_uring::types::Fd(self.file.as_raw_fd());
            let len = buf.len();
            if let Some(idx) = buf.fixed_id() {
                trace!(
                    "pread_fixed(pos = {}, length = {}, idx = {})",
                    pos,
                    len,
                    idx
                );
                #[cfg(debug_assertions)]
                {
                    self.state.lock().debug_check_fixed(idx, ptr, len);
                }
                io_uring::opcode::ReadFixed::new(fd, ptr, len as u32, idx as u16)
                    .offset(pos)
                    .build()
                    .user_data(get_key(c.clone()))
            } else {
                trace!("pread(pos = {}, length = {})", pos, len);
                io_uring::opcode::Read::new(fd, buf.as_mut_ptr(), len as u32)
                    .offset(pos)
                    .build()
                    .user_data(get_key(c.clone()))
            }
        };
        let mut state = self.state.lock();
        // SAFETY: holding `state` Mutex.
        unsafe { state.submit_entry(&self.ring, &read_e) };
        Ok(c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<crate::Buffer>, c: Completion) -> Result<Completion> {
        let write = {
            let ptr = buffer.as_ptr();
            let len = buffer.len();
            let fd = io_uring::types::Fd(self.file.as_raw_fd());
            if let Some(idx) = buffer.fixed_id() {
                trace!(
                    "pwrite_fixed(pos = {}, length = {}, idx= {})",
                    pos,
                    len,
                    idx
                );
                #[cfg(debug_assertions)]
                {
                    self.state.lock().debug_check_fixed(idx, ptr, len);
                }
                io_uring::opcode::WriteFixed::new(fd, ptr, len as u32, idx as u16)
                    .offset(pos)
                    .build()
                    .user_data(get_key(c.clone()))
            } else {
                trace!("pwrite(pos = {}, length = {})", pos, buffer.len());
                io_uring::opcode::Write::new(fd, ptr, len as u32)
                    .offset(pos)
                    .build()
                    .user_data(get_key(c.clone()))
            }
        };

        // Keep the buffer alive until the completion is processed. For non-fixed
        // buffers the SQE holds a raw pointer; without this the Arc would drop
        // here and the kernel could read freed memory.
        c.keep_write_buffer_alive(buffer);
        let mut state = self.state.lock();
        // SAFETY: holding `state` Mutex.
        unsafe { state.submit_entry(&self.ring, &write) };
        Ok(c)
    }

    fn sync(&self, c: Completion, _sync_type: crate::io::FileSyncType) -> Result<Completion> {
        trace!("sync()");
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let sync = io_uring::opcode::Fsync::new(fd)
            .build()
            .user_data(get_key(c.clone()));
        let mut state = self.state.lock();
        // SAFETY: holding `state` Mutex.
        unsafe { state.submit_entry(&self.ring, &sync) };
        Ok(c)
    }

    fn pwritev(
        &self,
        pos: u64,
        bufs: Vec<Arc<crate::Buffer>>,
        c: Completion,
    ) -> Result<Completion> {
        tracing::trace!("pwritev(pos = {}, bufs.len() = {})", pos, bufs.len());

        let wstate = WritevState::new(self, pos, bufs);
        let mut state = self.state.lock();
        // SAFETY: holding `state` Mutex.
        unsafe { state.submit_writev(&self.ring, get_key(c.clone()), wstate) };
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        Ok(self
            .file
            .metadata()
            .map_err(|e| io_error(e, "metadata"))?
            .len())
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        if self.caps.ftruncate {
            let truncate = io_uring::opcode::Ftruncate::new(fd, len)
                .build()
                .user_data(get_key(c.clone()));
            let mut state = self.state.lock();
            // SAFETY: holding `state` Mutex.
            unsafe { state.submit_entry(&self.ring, &truncate) };
            Ok(c)
        } else {
            let result = self.file.set_len(len);
            match result {
                Ok(()) => {
                    trace!("file truncated to len=({})", len);
                    c.complete(0);
                    Ok(c)
                }
                Err(e) => Err(io_error(e, "truncate")),
            }
        }
    }

    fn shared_wal_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<()> {
        unix_shared_wal_lock_byte(self.file.as_raw_fd(), offset, exclusive, true, kind).map(|_| ())
    }

    fn shared_wal_try_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        unix_shared_wal_lock_byte(self.file.as_raw_fd(), offset, exclusive, false, kind)
    }

    fn shared_wal_unlock_byte(&self, offset: u64, kind: SharedWalLockKind) -> Result<()> {
        unix_shared_wal_unlock_byte(self.file.as_raw_fd(), offset, kind)
    }

    fn shared_wal_set_len(&self, len: u64) -> Result<()> {
        self.file
            .set_len(len)
            .map_err(|err| io_error(err, "resize shared WAL coordination file"))
    }

    fn shared_wal_map(&self, offset: u64, len: usize) -> Result<Box<dyn SharedWalMappedRegion>> {
        unix_shared_wal_map(offset, len, self.file.as_raw_fd())
    }
}

impl Drop for UringFile {
    fn drop(&mut self) {
        self.unlock_file().expect("Failed to unlock file");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::common;

    #[test]
    fn test_multiple_processes_cannot_open_file() {
        common::tests::test_multiple_processes_cannot_open_file(UringIO::new);
    }
}
