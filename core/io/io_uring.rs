use super::{common, Completion, File, OpenFlags, IO};
use crate::{LimboError, Result};
use log::{debug, trace};
use rustix::fs::{self, FlockOperation, OFlags};
use rustix::io_uring::iovec;
use std::cell::RefCell;
use std::fmt;
use std::io::ErrorKind;
use std::os::fd::AsFd;
use std::os::unix::io::AsRawFd;
use std::rc::Rc;
use thiserror::Error;

const MAX_IOVECS: u32 = 128;
const SQPOLL_IDLE: u32 = 1000;

#[derive(Debug, Error)]
enum UringIOError {
    IOUringCQError(i32),
}

impl fmt::Display for UringIOError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UringIOError::IOUringCQError(code) => write!(
                f,
                "IOUring completion queue error occurred with code {}",
                code
            ),
        }
    }
}

pub struct UringIO {
    inner: Rc<RefCell<InnerUringIO>>,
}

struct WrappedIOUring {
    ring: io_uring::IoUring,
    pending_ops: usize,
    pub pending: [Option<Completion>; MAX_IOVECS as usize + 1],
    key: u64,
}

struct InnerUringIO {
    ring: WrappedIOUring,
    iovecs: [iovec; MAX_IOVECS as usize],
    next_iovec: usize,
}

impl UringIO {
    pub fn new() -> Result<Self> {
        let ring = match io_uring::IoUring::builder()
            .setup_sqpoll(SQPOLL_IDLE)
            .build(MAX_IOVECS)
        {
            Ok(ring) => ring,
            Err(_) => io_uring::IoUring::new(MAX_IOVECS)?,
        };
        let inner = InnerUringIO {
            ring: WrappedIOUring {
                ring,
                pending_ops: 0,
                pending: [const { None }; MAX_IOVECS as usize + 1],
                key: 0,
            },
            iovecs: [iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }; MAX_IOVECS as usize],
            next_iovec: 0,
        };
        debug!("Using IO backend 'io-uring'");
        Ok(Self {
            inner: Rc::new(RefCell::new(inner)),
        })
    }
}

impl InnerUringIO {
    pub fn get_iovec(&mut self, buf: *const u8, len: usize) -> &iovec {
        let iovec = &mut self.iovecs[self.next_iovec];
        iovec.iov_base = buf as *mut std::ffi::c_void;
        iovec.iov_len = len;
        self.next_iovec = (self.next_iovec + 1) % MAX_IOVECS as usize;
        iovec
    }
}

impl WrappedIOUring {
    fn submit_entry(&mut self, entry: &io_uring::squeue::Entry, c: Completion) {
        trace!("submit_entry({:?})", entry);
        self.pending[entry.get_user_data() as usize] = Some(c);
        unsafe {
            self.ring
                .submission()
                .push(entry)
                .expect("submission queue is full");
        }
        self.pending_ops += 1;
    }

    fn wait_for_completion(&mut self) -> Result<()> {
        self.ring.submit_and_wait(1)?;
        Ok(())
    }

    fn get_completion(&mut self) -> Option<io_uring::cqueue::Entry> {
        // NOTE: This works because CompletionQueue's next function pops the head of the queue. This is not normal behaviour of iterators
        let entry = self.ring.completion().next();
        if entry.is_some() {
            trace!("get_completion({:?})", entry);
            // consumed an entry from completion queue, update pending_ops
            self.pending_ops -= 1;
        }
        entry
    }

    fn empty(&self) -> bool {
        self.pending_ops == 0
    }

    fn get_key(&mut self) -> u64 {
        self.key += 1;
        if self.key == MAX_IOVECS as u64 {
            let key = self.key;
            self.key = 0;
            return key;
        }
        self.key
    }
}

impl IO for UringIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Rc<dyn File>> {
        trace!("open_file(path = {})", path);
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .create(matches!(flags, OpenFlags::Create))
            .open(path)?;
        // Let's attempt to enable direct I/O. Not all filesystems support it
        // so ignore any errors.
        let fd = file.as_fd();
        if direct {
            match fs::fcntl_setfl(fd, OFlags::DIRECT) {
                Ok(_) => {}
                Err(error) => debug!("Error {error:?} returned when setting O_DIRECT flag to read file. The performance of the system may be affected"),
            }
        }
        let uring_file = Rc::new(UringFile {
            io: self.inner.clone(),
            file,
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err() {
            uring_file.lock_file(true)?;
        }
        Ok(uring_file)
    }

    fn run_once(&self) -> Result<()> {
        trace!("run_once()");
        let mut inner = self.inner.borrow_mut();
        let ring = &mut inner.ring;

        if ring.empty() {
            return Ok(());
        }

        ring.wait_for_completion()?;
        while let Some(cqe) = ring.get_completion() {
            let result = cqe.result();
            if result < 0 {
                return Err(LimboError::UringIOError(format!(
                    "{} cqe: {:?}",
                    UringIOError::IOUringCQError(result),
                    cqe
                )));
            }
            {
                if let Some(c) = ring.pending[cqe.user_data() as usize].as_ref() {
                    c.complete(cqe.result());
                }
            }
            ring.pending[cqe.user_data() as usize] = None;
        }
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

pub struct UringFile {
    io: Rc<RefCell<InnerUringIO>>,
    file: std::fs::File,
}

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
                _ => format!("Failed locking file, {}", io_error),
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

    fn pread(&self, pos: usize, c: Completion) -> Result<()> {
        let r = match c {
            Completion::Read(ref r) => r,
            _ => unreachable!(),
        };
        trace!("pread(pos = {}, length = {})", pos, r.buf().len());
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let mut io = self.io.borrow_mut();
        let read_e = {
            let mut buf = r.buf_mut();
            let len = buf.len();
            let buf = buf.as_mut_ptr();
            let iovec = io.get_iovec(buf, len);
            io_uring::opcode::Readv::new(fd, iovec as *const iovec as *const libc::iovec, 1)
                .offset(pos as u64)
                .build()
                .user_data(io.ring.get_key())
        };
        io.ring.submit_entry(&read_e, c);
        Ok(())
    }

    fn pwrite(&self, pos: usize, buffer: Rc<RefCell<crate::Buffer>>, c: Completion) -> Result<()> {
        let mut io = self.io.borrow_mut();
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let write = {
            let buf = buffer.borrow();
            trace!("pwrite(pos = {}, length = {})", pos, buf.len());
            let iovec = io.get_iovec(buf.as_ptr(), buf.len());
            io_uring::opcode::Writev::new(fd, iovec as *const iovec as *const libc::iovec, 1)
                .offset(pos as u64)
                .build()
                .user_data(io.ring.get_key())
        };
        io.ring.submit_entry(&write, c);
        Ok(())
    }

    fn sync(&self, c: Completion) -> Result<()> {
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let mut io = self.io.borrow_mut();
        trace!("sync()");
        let sync = io_uring::opcode::Fsync::new(fd)
            .build()
            .user_data(io.ring.get_key());
        io.ring.submit_entry(&sync, c);
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
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
