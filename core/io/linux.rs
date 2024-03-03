use super::{Completion, File, IO};
use anyhow::Result;
use std::cell::RefCell;
use std::os::unix::io::AsRawFd;
use std::rc::Rc;
use log::trace;

pub struct LinuxIO {
    ring: Rc<RefCell<io_uring::IoUring>>,
}

impl LinuxIO {
    pub fn new() -> Result<Self> {
        let ring = io_uring::IoUring::new(128)?;
        Ok(Self {
            ring: Rc::new(RefCell::new(ring)),
        })
    }
}

impl IO for LinuxIO {
    fn open_file(&self, path: &str) -> Result<Box<dyn File>> {
        trace!("open_file(path = {})", path);
        let file = std::fs::File::open(path)?;
        Ok(Box::new(LinuxFile {
            ring: self.ring.clone(),
            file,
        }))
    }

    fn run_once(&self) -> Result<()> {
        trace!("run_once()");
        let mut ring = self.ring.borrow_mut();
        ring.submit_and_wait(0)?;
        loop {
            match ring.completion().next() {
                Some(cqe) => {
                    let c = unsafe { Rc::from_raw(cqe.user_data() as *const Completion) };
                    c.complete();
                }
                None => break,
            }
        }
        Ok(())
    }
}

pub struct LinuxFile {
    ring: Rc<RefCell<io_uring::IoUring>>,
    file: std::fs::File,
}

impl File for LinuxFile {
    fn pread(&self, pos: usize, c: Rc<Completion>) -> Result<()> {
        trace!("pread(pos = {}, length = {})", pos, c.buf().len());
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let read_e = {
            let mut buf = c.buf_mut();
            let len = buf.len();
            let buf = buf.as_mut_ptr();
            let ptr = Rc::into_raw(c.clone());
            io_uring::opcode::Read::new(fd, buf, len as u32 )
                .offset(pos as u64)
                .build()
                .user_data(ptr as u64)
        };
        let mut ring = self.ring.borrow_mut();
        unsafe {
            ring.submission()
                .push(&read_e)
                .expect("submission queue is full");
        }
        Ok(())
    }
}
