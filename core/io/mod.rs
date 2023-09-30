use cfg_block::cfg_block;
use std::pin::Pin;

pub struct Buffer {
    data: Pin<Vec<u8>>,
}

impl Buffer {
    pub fn allocate(size: usize) -> Self {
        Self {
            data: Pin::new(vec![0; size]),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }
}

cfg_block! {
    #[cfg(target_os = "linux")] {
        mod linux;
        pub use linux::{File, IO};
    }

    #[cfg(target_os = "macos")] {
        mod darwin;
        pub use darwin::{File, IO};
    }
}
