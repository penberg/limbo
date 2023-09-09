use anyhow::Result;
use std::cell::RefCell;
use std::fs::File;
use std::os::unix::io::AsRawFd;

pub(crate) struct IoUring {
    ring: RefCell<io_uring::IoUring>,
    file: File,
}

impl IoUring {
    pub(crate) fn open(path: &str) -> Result<Self> {
        let mut ring = RefCell::new(io_uring::IoUring::new(8)?);
        let file = std::fs::File::open(path)?;
        Ok(IoUring { ring, file })
    }
}

impl super::PageIO for IoUring {
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
