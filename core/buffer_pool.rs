use crate::io::BufferData;
use std::{pin::Pin, sync::Mutex};

pub struct BufferPool {
    pub free_buffers: Mutex<Vec<BufferData>>,
    page_size: usize,
}

impl BufferPool {
    pub fn new(page_size: usize) -> Self {
        Self {
            free_buffers: Mutex::new(Vec::new()),
            page_size,
        }
    }

    pub fn get(&self) -> BufferData {
        let mut free_buffers = self.free_buffers.lock().unwrap();
        if let Some(buffer) = free_buffers.pop() {
            buffer
        } else {
            Pin::new(vec![0; self.page_size])
        }
    }

    pub fn put(&self, buffer: BufferData) {
        let mut free_buffers = self.free_buffers.lock().unwrap();
        free_buffers.push(buffer);
    }
}
