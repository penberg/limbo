use crate::io::Buffer as IoBuffer;
use std::{mem::ManuallyDrop, sync::Mutex};

pub struct BufferPool {
    pub free_buffers: Mutex<Vec<IoBuffer>>,
    page_size: usize,
}

impl BufferPool {
    pub fn new(page_size: usize) -> Self {
        Self {
            free_buffers: Mutex::new(Vec::new()),
            page_size,
        }
    }

    pub fn get(&mut self) -> Buffer {
        let mut free_buffers = self.free_buffers.lock().unwrap();
        if let Some(buffer) = free_buffers.pop() {
            Buffer::new(self, buffer)
        } else {
            let raw_buffer = IoBuffer::allocate(self.page_size);
            Buffer::new(self, raw_buffer)
        }
    }

    pub fn put(&self, buffer: IoBuffer) {
        let mut free_buffers = self.free_buffers.lock().unwrap();
        free_buffers.push(buffer);
    }
}

pub struct Buffer<'a> {
    pool: &'a BufferPool,
    data: ManuallyDrop<IoBuffer>,
}

impl Drop for Buffer<'_> {
    fn drop(&mut self) {
        let data = unsafe { ManuallyDrop::take(&mut self.data) };
        self.pool.put(data);
    }
}

impl<'a> Buffer<'a> {
    pub fn new(pool: &'a BufferPool, data: IoBuffer) -> Self {
        Self {
            pool,
            data: ManuallyDrop::new(data),
        }
    }

    pub fn data_mut(&mut self) -> &mut IoBuffer {
        &mut self.data
    }
}
