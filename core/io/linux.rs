use super::{common, Completion, File, OpenFlags, IO};
use crate::{LimboError, Result};
use libc::{c_short, fcntl, flock, iovec, F_SETLK};
use log::{debug, trace};
use nix::fcntl::{FcntlArg, OFlag};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::os::unix::io::AsRawFd;
use std::rc::Rc;
use thiserror::Error;

const MAX_IOVECS: usize = 128;

#[derive(Debug, Error)]
enum LinuxIOError {
    IOUringCQError(i32),
}

// Implement the Display trait to customize error messages
impl fmt::Display for LinuxIOError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LinuxIOError::IOUringCQError(code) => write!(
                f,
                "IOUring completion queue error occurred with code {}",
                code
            ),
        }
    }
}

pub struct LinuxIO {
    inner: Rc<RefCell<InnerLinuxIO>>,
}

struct WrappedIOUring {
    ring: io_uring::IoUring,
    pending_ops: usize,
    pub pending: HashMap<u64, Rc<Completion>>,
    key: u64,
}

struct InnerLinuxIO {
    ring: WrappedIOUring,
    iovecs: [iovec; MAX_IOVECS],
    next_iovec: usize,
}

impl LinuxIO {
    pub fn new() -> Result<Self> {
        let ring = io_uring::IoUring::new(MAX_IOVECS as u32)?;
        let inner = InnerLinuxIO {
            ring: WrappedIOUring {
                ring,
                pending_ops: 0,
                pending: HashMap::new(),
                key: 0,
            },
            iovecs: [iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }; MAX_IOVECS],
            next_iovec: 0,
        };
        Ok(Self {
            inner: Rc::new(RefCell::new(inner)),
        })
    }
}

impl InnerLinuxIO {
    pub fn get_iovec<'a>(&'a mut self, buf: *const u8, len: usize) -> &'a iovec {
        let iovec = &mut self.iovecs[self.next_iovec];
        iovec.iov_base = buf as *mut std::ffi::c_void;
        iovec.iov_len = len;
        self.next_iovec = (self.next_iovec + 1) % MAX_IOVECS;
        iovec
    }
}

impl WrappedIOUring {
    fn submit_entry(&mut self, entry: &io_uring::squeue::Entry, c: Rc<Completion>) {
        log::trace!("submit_entry({:?})", entry);
        self.pending.insert(entry.get_user_data(), c);
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
            log::trace!("get_completion({:?})", entry);
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
        self.key
    }
}

impl IO for LinuxIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Rc<dyn File>> {
        trace!("open_file(path = {})", path);
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .create(matches!(flags, OpenFlags::Create))
            .open(path)?;
        // Let's attempt to enable direct I/O. Not all filesystems support it
        // so ignore any errors.
        let fd = file.as_raw_fd();
        if direct {
            match nix::fcntl::fcntl(fd, FcntlArg::F_SETFL(OFlag::O_DIRECT)) {
                Ok(_) => {},
                Err(error) => debug!("Error {error:?} returned when setting O_DIRECT flag to read file. The performance of the system may be affected"),
            };
        }
        let linux_file = Rc::new(LinuxFile {
            io: self.inner.clone(),
            file,
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err() {
            linux_file.lock_file(true)?;
        }
        Ok(linux_file)
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
                return Err(LimboError::LinuxIOError(format!(
                    "{} cqe: {:?}",
                    LinuxIOError::IOUringCQError(result),
                    cqe
                )));
            }
            {
                let c = ring.pending.get(&cqe.user_data()).unwrap().clone();
                c.complete(cqe.result());
            }
            ring.pending.remove(&cqe.user_data());
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

pub struct LinuxFile {
    io: Rc<RefCell<InnerLinuxIO>>,
    file: std::fs::File,
}

impl File for LinuxFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        let fd = self.file.as_raw_fd();
        let flock = flock {
            l_type: if exclusive {
                libc::F_WRLCK as c_short
            } else {
                libc::F_RDLCK as c_short
            },
            l_whence: libc::SEEK_SET as c_short,
            l_start: 0,
            l_len: 0, // Lock entire file
            l_pid: 0,
        };

        // F_SETLK is a non-blocking lock. The lock will be released when the file is closed
        // or the process exits or after an explicit unlock.
        let lock_result = unsafe { fcntl(fd, F_SETLK, &flock) };
        if lock_result == -1 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::WouldBlock {
                return Err(LimboError::LockingError(
                    "File is locked by another process".into(),
                ));
            } else {
                return Err(LimboError::IOError(err));
            }
        }
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        let fd = self.file.as_raw_fd();
        let flock = flock {
            l_type: libc::F_UNLCK as c_short,
            l_whence: libc::SEEK_SET as c_short,
            l_start: 0,
            l_len: 0,
            l_pid: 0,
        };

        let unlock_result = unsafe { fcntl(fd, F_SETLK, &flock) };
        if unlock_result == -1 {
            return Err(LimboError::LockingError(format!(
                "Failed to release file lock: {}",
                std::io::Error::last_os_error()
            )));
        }
        Ok(())
    }

    fn pread(&self, pos: usize, c: Rc<Completion>) -> Result<()> {
        let r = match &(*c) {
            Completion::Read(r) => r,
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
            io_uring::opcode::Readv::new(fd, iovec, 1)
                .offset(pos as u64)
                .build()
                .user_data(io.ring.get_key())
        };
        io.ring.submit_entry(&read_e, c);
        Ok(())
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Rc<RefCell<crate::Buffer>>,
        c: Rc<Completion>,
    ) -> Result<()> {
        let mut io = self.io.borrow_mut();
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let write = {
            let buf = buffer.borrow();
            trace!("pwrite(pos = {}, length = {})", pos, buf.len());
            let iovec = io.get_iovec(buf.as_ptr(), buf.len());
            io_uring::opcode::Writev::new(fd, iovec, 1)
                .offset(pos as u64)
                .build()
                .user_data(io.ring.get_key())
        };
        io.ring.submit_entry(&write, c);
        Ok(())
    }

    fn sync(&self, c: Rc<Completion>) -> Result<()> {
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
        Ok(self.file.metadata().unwrap().len())
    }
}

impl Drop for LinuxFile {
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
        common::tests::test_multiple_processes_cannot_open_file(LinuxIO::new);
    }
}
