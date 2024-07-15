use super::{common, Completion, File, WriteCompletion, IO};
use anyhow::{ensure, Result};
use libc::{c_short, fcntl, flock, iovec, F_SETLK};
use log::{debug, trace};
use nix::fcntl::{FcntlArg, OFlag};
use std::cell::RefCell;
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

pub struct InnerLinuxIO {
    ring: io_uring::IoUring,
    iovecs: [iovec; MAX_IOVECS],
    next_iovec: usize,
}

impl LinuxIO {
    pub fn new() -> Result<Self> {
        let ring = io_uring::IoUring::new(MAX_IOVECS as u32)?;
        let inner = InnerLinuxIO {
            ring: ring,
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

impl IO for LinuxIO {
    fn open_file(&self, path: &str) -> Result<Rc<dyn File>> {
        trace!("open_file(path = {})", path);
        let file = std::fs::File::options().read(true).write(true).open(path)?;
        // Let's attempt to enable direct I/O. Not all filesystems support it
        // so ignore any errors.
        let fd = file.as_raw_fd();
        match nix::fcntl::fcntl(fd, FcntlArg::F_SETFL(OFlag::O_DIRECT)) {
            Ok(_) => {},
            Err(error) => debug!("Error {error:?} returned when setting O_DIRECT flag to read file. The performance of the system may be affected"),
        };
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
        ring.submit_and_wait(1)?;
        while let Some(cqe) = ring.completion().next() {
            let result = cqe.result();
            ensure!(result >= 0, LinuxIOError::IOUringCQError(result));
            let c = unsafe { Rc::from_raw(cqe.user_data() as *const Completion) };
            c.complete();
        }
        Ok(())
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
                return Err(anyhow::anyhow!("File is locked by another process"));
            } else {
                return Err(anyhow::anyhow!(err));
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
            return Err(anyhow::anyhow!(
                "Failed to release file lock: {}",
                std::io::Error::last_os_error()
            ));
        }
        Ok(())
    }

    fn pread(&self, pos: usize, c: Rc<Completion>) -> Result<()> {
        trace!("pread(pos = {}, length = {})", pos, c.buf().len());
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let mut io = self.io.borrow_mut();
        let read_e = {
            let mut buf = c.buf_mut();
            let len = buf.len();
            let buf = buf.as_mut_ptr();
            let ptr = Rc::into_raw(c.clone());
            let iovec = io.get_iovec(buf, len);
            io_uring::opcode::Readv::new(fd, iovec, 1)
                .offset(pos as u64)
                .build()
                .user_data(ptr as u64)
        };
        let ring = &mut io.ring;
        unsafe {
            ring.submission()
                .push(&read_e)
                .expect("submission queue is full");
        }
        Ok(())
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Rc<RefCell<crate::Buffer>>,
        c: Rc<WriteCompletion>,
    ) -> Result<()> {
        let mut io = self.io.borrow_mut();
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let write = {
            let buf = buffer.borrow();
            let ptr = Rc::into_raw(c.clone());
            let iovec = io.get_iovec(buf.as_ptr(), buf.len());
            io_uring::opcode::Writev::new(fd, iovec, 1)
                .offset(pos as u64)
                .build()
                .user_data(ptr as u64)
        };
        let ring = &mut io.ring;
        unsafe {
            ring.submission()
                .push(&write)
                .expect("submission queue is full");
        }
        Ok(())
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
