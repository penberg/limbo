use anyhow::Result;
use std::sync::Arc;

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "macos")]
mod darwin;

pub trait IO {
    fn open(&self, path: &str) -> Result<File>;
}

#[cfg(target_os = "linux")]
pub fn default_io() -> Result<impl IO> {
    Ok(linux::LinuxIO::new()?)
}

#[cfg(target_os = "macos")]
pub fn default_io() -> Result<impl IO> {
    Ok(darwin::DarwinIO::new()?)
}

pub struct File {
    io: Arc<dyn PageIO>,
}

impl File {
    pub fn get(&self, page_idx: usize, buf: &mut [u8]) -> Result<()> {
        self.io.get(page_idx, buf)
    }
}

trait PageIO {
    fn get(&self, page_idx: usize, buf: &mut [u8]) -> Result<()>;
}
