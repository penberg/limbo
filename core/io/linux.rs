use super::{Completion, File, WriteCompletion, IO};
use anyhow::Result;
use libc::iovec;
use log::trace;
use std::cell::{Ref, RefCell};
use nix::fcntl::{self, FcntlArg, OFlag};
use std::os::unix::io::AsRawFd;
use std::rc::Rc;

const MAX_IOVECS: usize = 128;

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
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .open(path)?;
        // Let's attempt to enable direct I/O. Not all filesystems support it
        // so ignore any errors.
        let fd = file.as_raw_fd();
        let _= nix::fcntl::fcntl(fd, FcntlArg::F_SETFL(OFlag::O_DIRECT));
        Ok(Rc::new(LinuxFile {
            io: self.inner.clone(),
            file,
        }))
    }

    fn run_once(&self) -> Result<()> {
        trace!("run_once()");
        let mut inner = self.inner.borrow_mut();
        let mut ring = &mut inner.ring;
        ring.submit_and_wait(1)?;
        while let Some(cqe) = ring.completion().next() {
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
        let mut ring = &mut io.ring;
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
        let mut ring = &mut io.ring;
        unsafe {
            ring.submission()
                .push(&write)
                .expect("submission queue is full");
        }
        Ok(())
    }
}
