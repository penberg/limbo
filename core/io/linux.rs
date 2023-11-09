use super::Completion;
use anyhow::Result;
use std::cell::RefCell;
use std::os::unix::io::AsRawFd;
use std::rc::Rc;

pub struct IO {
    ring: Rc<RefCell<io_uring::IoUring>>,
}

impl IO {
    pub fn new() -> Result<Self> {
        let ring = io_uring::IoUring::new(8)?;
        Ok(Self {
            ring: Rc::new(RefCell::new(ring)),
        })
    }

    pub fn open_file(&self, path: &str) -> Result<File> {
        let file = std::fs::File::open(path)?;
        Ok(File {
            ring: self.ring.clone(),
            file,
        })
    }

    pub fn run_once(&self) -> Result<()> {
        let mut ring = self.ring.borrow_mut();
        ring.submit_and_wait(1)?;
        let cqe = ring.completion().next().expect("completion queue is empty");
        Ok(())
    }
}

pub struct File {
    ring: Rc<RefCell<io_uring::IoUring>>,
    file: std::fs::File,
}

impl File {
    pub fn pread(&self, pos: usize, c: &mut Completion) -> Result<()> {
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let read_e = io_uring::opcode::Read::new(fd, c.buf.as_mut_ptr(), buf.len() as u32 )
            .offset(pos as u64)
            .build();
        let mut ring = self.ring.borrow_mut();
        unsafe {
            ring.submission()
                .push(&read_e)
                .expect("submission queue is full");
        }
        ring.submit_and_wait(1)?;
        let cqe = ring.completion().next().expect("completion queue is empty");
        Ok(())
    }
}