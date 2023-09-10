use anyhow::{Ok, Result};
use std::sync::Arc;

#[cfg(all(feature = "fs", target_os = "linux"))]
mod linux;

#[cfg(feature = "fs")]
mod darwin;

/// I/O access method
enum IOMethod {
    #[cfg(not(feature = "fs"))]
    Memory,

    #[cfg(feature = "fs")]
    Sync { io: darwin::Loop },

    #[cfg(target_os = "linux")]
    IoUring { io: linux::Loop },
}

/// I/O access interface.
pub struct IO {
    io_method: IOMethod,
}

impl IO {
    #[cfg(all(feature = "fs", target_os = "linux"))]
    pub fn new() -> Result<Self> {
        Ok(IO {
            io_method: IOMethod::IoUring {
                io: linux::Loop::new()?,
            },
        })
    }

    #[cfg(all(feature = "fs", target_os = "macos"))]
    pub fn new() -> Result<Self> {
        Ok(IO {
            io_method: IOMethod::Sync {
                io: darwin::Loop::new()?,
            },
        })
    }

    #[cfg(not(feature = "fs"))]
    pub fn new() -> Result<Self> {
        Ok(IO {
            io_method: IOMethod::Memory,
        })
    }
}

impl IO {
    pub fn open(&self, path: &str) -> Result<PageSource> {
        match &self.io_method {
            #[cfg(feature = "fs")]
            IOMethod::Sync { io } => {
                let io = Arc::new(io.open_file(path)?);
                Ok(PageSource { io })
            }
            #[cfg(all(feature = "fs", target_os = "linux"))]
            IOMethod::IoUring { io } => {
                let io = Arc::new(io.open_file(path)?);
                Ok(PageSource { io })
            }
            #[cfg(not(feature = "fs"))]
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
