use crate::error::LimboError;
use crate::io::common;
use crate::Result;

use super::{Completion, File, OpenFlags, IO};
use libc::{c_short, fcntl, flock, F_SETLK};
use log::trace;
use polling::{Event, Events, Poller};
use rustix::fd::{AsFd, AsRawFd};
use rustix::fs::OpenOptionsExt;
use rustix::io::Errno;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Read, Seek, Write};
use std::rc::Rc;

pub struct DarwinIO {
    poller: Rc<RefCell<Poller>>,
    events: Rc<RefCell<Events>>,
    callbacks: Rc<RefCell<HashMap<usize, CompletionCallback>>>,
}

impl DarwinIO {
    pub fn new() -> Result<Self> {
        Ok(Self {
            poller: Rc::new(RefCell::new(Poller::new()?)),
            events: Rc::new(RefCell::new(Events::new())),
            callbacks: Rc::new(RefCell::new(HashMap::new())),
        })
    }
}

impl IO for DarwinIO {
    fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Rc<dyn File>> {
        trace!("open_file(path = {})", path);
        let file = std::fs::File::options()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .write(true)
            .create(matches!(flags, OpenFlags::Create))
            .open(path)?;

        let darwin_file = Rc::new(DarwinFile {
            file: Rc::new(RefCell::new(file)),
            poller: self.poller.clone(),
            callbacks: self.callbacks.clone(),
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err() {
            darwin_file.lock_file(true)?;
        }
        Ok(darwin_file)
    }

    fn run_once(&self) -> Result<()> {
        if self.callbacks.borrow().is_empty() {
            return Ok(());
        }
        let mut events = self.events.borrow_mut();
        events.clear();

        trace!("run_once() waits for events");
        let poller = self.poller.borrow();
        poller.wait(&mut events, None)?;

        for event in events.iter() {
            if let Some(cf) = self.callbacks.borrow_mut().remove(&event.key) {
                let result = {
                    match cf {
                        CompletionCallback::Read(ref file, ref c, pos) => {
                            let mut file = file.borrow_mut();
                            let c: &Completion = c;
                            let r = match c {
                                Completion::Read(r) => r,
                                _ => unreachable!(),
                            };
                            let mut buf = r.buf_mut();
                            file.seek(std::io::SeekFrom::Start(pos as u64))?;
                            file.read(buf.as_mut_slice())
                        }
                        CompletionCallback::Write(ref file, _, ref buf, pos) => {
                            let mut file = file.borrow_mut();
                            let buf = buf.borrow();
                            file.seek(std::io::SeekFrom::Start(pos as u64))?;
                            file.write(buf.as_slice())
                        }
                    }
                };
                match result {
                    std::result::Result::Ok(n) => {
                        match &cf {
                            CompletionCallback::Read(_, ref c, _) => {
                                c.complete(0);
                            }
                            CompletionCallback::Write(_, ref c, _, _) => {
                                c.complete(n as i32);
                            }
                        }
                        return Ok(());
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            }
        }
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).unwrap();
        i64::from_ne_bytes(buf)
    }

    fn get_current_time(&self) -> String {
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
    }
}

enum CompletionCallback {
    Read(Rc<RefCell<std::fs::File>>, Rc<Completion>, usize),
    Write(
        Rc<RefCell<std::fs::File>>,
        Rc<Completion>,
        Rc<RefCell<crate::Buffer>>,
        usize,
    ),
}

pub struct DarwinFile {
    file: Rc<RefCell<std::fs::File>>,
    poller: Rc<RefCell<polling::Poller>>,
    callbacks: Rc<RefCell<HashMap<usize, CompletionCallback>>>,
}

impl File for DarwinFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        let fd = self.file.borrow().as_raw_fd();
        let flock = flock {
            l_type: if exclusive {
                libc::F_WRLCK as c_short
            } else {
                libc::F_RDLCK as c_short
            },
            l_whence: libc::SEEK_SET as c_short,
            l_start: 0,
            l_len: 0, // Lock entire file
            l_pid: 0,
        };

        // F_SETLK is a non-blocking lock. The lock will be released when the file is closed
        // or the process exits or after an explicit unlock.
        let lock_result = unsafe { fcntl(fd, F_SETLK, &flock) };
        if lock_result == -1 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::WouldBlock {
                return Err(LimboError::LockingError(
                    "Failed locking file. File is locked by another process".to_string(),
                ));
            } else {
                return Err(LimboError::LockingError(format!(
                    "Failed locking file, {}",
                    err
                )));
            }
        }
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        let fd = self.file.borrow().as_raw_fd();
        let flock = flock {
            l_type: libc::F_UNLCK as c_short,
            l_whence: libc::SEEK_SET as c_short,
            l_start: 0,
            l_len: 0,
            l_pid: 0,
        };

        let unlock_result = unsafe { fcntl(fd, F_SETLK, &flock) };
        if unlock_result == -1 {
            return Err(LimboError::LockingError(format!(
                "Failed to release file lock: {}",
                std::io::Error::last_os_error()
            )));
        }
        Ok(())
    }

    fn pread(&self, pos: usize, c: Rc<Completion>) -> Result<()> {
        let file = self.file.borrow();
        let result = {
            let r = match &(*c) {
                Completion::Read(r) => r,
                _ => unreachable!(),
            };
            let mut buf = r.buf_mut();
            rustix::io::pread(file.as_fd(), buf.as_mut_slice(), pos as u64)
        };
        match result {
            std::result::Result::Ok(n) => {
                trace!("pread n: {}", n);
                // Read succeeded immediately
                c.complete(0);
                Ok(())
            }
            Err(Errno::AGAIN) => {
                trace!("pread blocks");
                // Would block, set up polling
                let fd = file.as_raw_fd();
                unsafe {
                    self.poller
                        .borrow()
                        .add(&file.as_fd(), Event::readable(fd as usize))?;
                }
                self.callbacks.borrow_mut().insert(
                    fd as usize,
                    CompletionCallback::Read(self.file.clone(), c.clone(), pos),
                );
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Rc<RefCell<crate::Buffer>>,
        c: Rc<Completion>,
    ) -> Result<()> {
        let file = self.file.borrow();
        let result = {
            let buf = buffer.borrow();
            rustix::io::pwrite(file.as_fd(), buf.as_slice(), pos as u64)
        };
        match result {
            std::result::Result::Ok(n) => {
                trace!("pwrite n: {}", n);
                // Read succeeded immediately
                c.complete(n as i32);
                Ok(())
            }
            Err(Errno::AGAIN) => {
                trace!("pwrite blocks");
                // Would block, set up polling
                let fd = file.as_raw_fd();
                unsafe {
                    self.poller
                        .borrow()
                        .add(&file.as_fd(), Event::readable(fd as usize))?;
                }
                self.callbacks.borrow_mut().insert(
                    fd as usize,
                    CompletionCallback::Write(self.file.clone(), c.clone(), buffer.clone(), pos),
                );
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    fn sync(&self, c: Rc<Completion>) -> Result<()> {
        let file = self.file.borrow();
        let result = rustix::fs::fsync(file.as_fd());
        match result {
            std::result::Result::Ok(()) => {
                trace!("fsync");
                c.complete(0);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    fn size(&self) -> Result<u64> {
        let file = self.file.borrow();
        Ok(file.metadata().unwrap().len())
    }
}

impl Drop for DarwinFile {
    fn drop(&mut self) {
        self.unlock_file().expect("Failed to unlock file");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multiple_processes_cannot_open_file() {
        common::tests::test_multiple_processes_cannot_open_file(DarwinIO::new);
    }
}
