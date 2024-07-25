use crate::Result;
use cfg_block::cfg_block;
use std::{
    cell::{Ref, RefCell, RefMut},
    mem::ManuallyDrop,
    pin::Pin,
    rc::Rc,
};

pub trait File {
    fn lock_file(&self, exclusive: bool) -> Result<()>;
    fn unlock_file(&self) -> Result<()>;
    fn pread(&self, pos: usize, c: Rc<Completion>) -> Result<()>;
    fn pwrite(&self, pos: usize, buffer: Rc<RefCell<Buffer>>, c: Rc<WriteCompletion>)
        -> Result<()>;
}

pub trait IO {
    fn open_file(&self, path: &str) -> Result<Rc<dyn File>>;

    fn run_once(&self) -> Result<()>;
}

pub type Complete = dyn Fn(&Buffer);
pub type WriteComplete = dyn Fn(usize);

pub struct Completion {
    pub buf: RefCell<Buffer>,
    pub complete: Box<Complete>,
}

pub struct WriteCompletion {
    pub complete: Box<WriteComplete>,
}

impl Completion {
    pub fn new(buf: Buffer, complete: Box<Complete>) -> Self {
        let buf = RefCell::new(buf);
        Self { buf, complete }
    }

    pub fn buf(&self) -> Ref<'_, Buffer> {
        self.buf.borrow()
    }

    pub fn buf_mut(&self) -> RefMut<'_, Buffer> {
        self.buf.borrow_mut()
    }

    pub fn complete(&self) {
        let buf = self.buf.borrow_mut();
        (self.complete)(&buf);
    }
}

impl WriteCompletion {
    pub fn new(complete: Box<WriteComplete>) -> Self {
        Self { complete }
    }
    pub fn complete(&self, bytes_written: usize) {
        (self.complete)(bytes_written);
    }
}

pub type BufferData = Pin<Vec<u8>>;

pub type BufferDropFn = Rc<dyn Fn(BufferData)>;

#[derive(Clone)]
pub struct Buffer {
    data: ManuallyDrop<BufferData>,
    drop: BufferDropFn,
}

impl Drop for Buffer {
    fn drop(&mut self) {
        let data = unsafe { ManuallyDrop::take(&mut self.data) };
        (self.drop)(data);
    }
}

impl Buffer {
    pub fn allocate(size: usize, drop: BufferDropFn) -> Self {
        let data = ManuallyDrop::new(Pin::new(vec![0; size]));
        Self { data, drop }
    }

    pub fn new(data: BufferData, drop: BufferDropFn) -> Self {
        let data = ManuallyDrop::new(data);
        Self { data, drop }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
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
        pub use linux::LinuxIO as PlatformIO;
    }

    #[cfg(target_os = "macos")] {
        mod darwin;
        pub use darwin::DarwinIO as PlatformIO;
    }

    #[cfg(target_os = "windows")] {
        mod windows;
        pub use windows::WindowsIO as PlatformIO;
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))] {
        mod generic;
        pub use generic::GenericIO as PlatformIO;
    }
}

mod common;
