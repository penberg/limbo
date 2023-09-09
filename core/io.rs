use anyhow::Result;
use std::cell::RefCell;
use std::io::{Read, Seek};
use std::{fs::File, sync::Arc};

/// I/O access method
enum IOMethod {
    Memory,

    #[cfg(feature = "fs")]
    Sync,
}

/// I/O access interface.
pub struct IO {
    io_method: IOMethod,
}

#[cfg(feature = "fs")]
impl Default for IO {
    fn default() -> Self {
        IO {
            io_method: IOMethod::Sync,
        }
    }
}

#[cfg(not(feature = "fs"))]
impl Default for IO {
    fn default() -> Self {
        IO {
            io_method: IOMethod::Memory,
        }
    }
}

impl IO {
    pub fn open(&self, path: &str) -> Result<PageSource> {
        match self.io_method {
            #[cfg(feature = "fs")]
            IOMethod::Sync => {
                let io = Arc::new(FileIO::open(path)?);
                Ok(PageSource { io })
            }
            IOMethod::Memory => {
                todo!();
            }
        }
    }
}

pub struct PageSource {
    io: Arc<dyn PageIO>,
}

impl PageSource {
    pub fn get(&self, page_idx: usize, buf: &mut [u8]) -> Result<()> {
        self.io.get(page_idx, buf)
    }
}

trait PageIO {
    fn get(&self, page_idx: usize, buf: &mut [u8]) -> Result<()>;
}

struct FileIO {
    file: RefCell<File>,
}

impl PageIO for FileIO {
    fn get(&self, page_idx: usize, buf: &mut [u8]) -> Result<()> {
        let page_size = buf.len();
        assert!(page_idx > 0);
        assert!(page_size >= 512);
        assert!(page_size <= 65536);
        assert!((page_size & (page_size - 1)) == 0);
        let pos = (page_idx - 1) * page_size;
        let mut file = self.file.borrow_mut();
        file.seek(std::io::SeekFrom::Start(pos as u64))?;
        file.read_exact(buf)?;
        Ok(())
    }
}

impl FileIO {
    fn open(path: &str) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        Ok(FileIO {
            file: RefCell::new(file),
        })
    }
}
