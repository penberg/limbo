use anyhow::Result;
use std::cell::RefCell;
use std::os::unix::io::AsRawFd;
use std::rc::Rc;
use std::sync::Arc;

pub(crate) struct LinuxIO {
    ring: Rc<RefCell<io_uring::IoUring>>,
}

impl super::IO for LinuxIO {
    fn open(&self, path: &str) -> Result<super::File> {
        let file = self.open_file(path)?;
        Ok(super::File { io: Arc::new(file) })
    }
}

impl LinuxIO {
    pub(crate) fn new() -> Result<Self> {
        let ring = io_uring::IoUring::new(8)?;
        Ok(LinuxIO {
            ring: Rc::new(RefCell::new(ring)),
        })
    }

    pub(crate) fn open_file(&self, path: &str) -> Result<File> {
        let file = std::fs::File::open(path)?;
        Ok(File {
            ring: self.ring.clone(),
            file,
        })
    }

    pub(crate) fn run_once(&self) -> Result<()> {
        let mut ring = self.ring.borrow_mut();
        ring.submit_and_wait(1)?;
        let cqe = ring.completion().next().expect("completion queue is empty");
        Ok(())
    }
}

pub(crate) struct File {
    ring: Rc<RefCell<io_uring::IoUring>>,
    file: std::fs::File,
}

impl super::PageIO for File {
    fn get(&self, page_idx: usize, buf: &mut [u8]) -> Result<()> {
        let page_size = buf.len();
        assert!(page_idx > 0);
        assert!(page_size >= 512);
        assert!(page_size <= 65536);
        assert!((page_size & (page_size - 1)) == 0);
        let pos = (page_idx - 1) * page_size;
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let read_e = io_uring::opcode::Read::new(fd, buf.as_mut_ptr(), page_size as u32)
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
