use crate::io::BufferData;
use std::cell::RefCell;
use std::pin::Pin;

pub struct BufferPool {
    pub free_buffers: RefCell<Vec<BufferData>>,
    page_size: usize,
}

impl BufferPool {
    pub fn new(page_size: usize) -> Self {
        Self {
            free_buffers: RefCell::new(Vec::new()),
            page_size,
        }
    }

    pub fn get(&self) -> BufferData {
        let mut free_buffers = self.free_buffers.borrow_mut();
        if let Some(buffer) = free_buffers.pop() {
            buffer
        } else {
            Pin::new(vec![0; self.page_size])
        }
    }

    pub fn put(&self, buffer: BufferData) {
        let mut free_buffers = self.free_buffers.borrow_mut();
        free_buffers.push(buffer);
    }
}
