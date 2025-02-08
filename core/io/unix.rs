use crate::error::LimboError;
use crate::io::common;
use crate::Result;

use super::{Completion, File, OpenFlags, IO};
use log::{debug, trace};
use polling::{Event, Events, Poller};
use rustix::{
    fd::{AsFd, AsRawFd},
    fs::{self, FlockOperation, OFlags, OpenOptionsExt},
    io::Errno,
};
use std::cell::{RefCell, UnsafeCell};
use std::io::{ErrorKind, Read, Seek, Write};
use std::rc::Rc;

struct OwnedCallbacks(UnsafeCell<Callbacks>);
struct BorrowedCallbacks<'io>(UnsafeCell<&'io mut Callbacks>);

impl OwnedCallbacks {
    fn new() -> Self {
        Self(UnsafeCell::new(Callbacks::new()))
    }
    fn as_mut<'io>(&self) -> &'io mut Callbacks {
        unsafe { &mut *self.0.get() }
    }

    fn is_empty(&self) -> bool {
        self.as_mut().count == 0
    }

    fn remove(&self, fd: usize) -> Option<CompletionCallback> {
        let callbacks = unsafe { &mut *self.0.get() };
        callbacks.remove(fd)
    }
}

impl BorrowedCallbacks<'_> {
    fn insert(&self, fd: usize, callback: CompletionCallback) {
        let callbacks = unsafe { &mut *self.0.get() };
        callbacks.insert(fd, callback);
    }
}

struct EventsHandler(UnsafeCell<Events>);

impl EventsHandler {
    fn new() -> Self {
        Self(UnsafeCell::new(Events::new()))
    }

    fn clear(&self) {
        let events = unsafe { &mut *self.0.get() };
        events.clear();
    }

    fn iter(&self) -> impl Iterator<Item = Event> {
        let events = unsafe { &*self.0.get() };
        events.iter()
    }

    fn as_mut<'io>(&self) -> &'io mut Events {
        unsafe { &mut *self.0.get() }
    }
}
struct PollHandler(UnsafeCell<Poller>);
struct BorrowedPollHandler<'io>(UnsafeCell<&'io mut Poller>);

impl BorrowedPollHandler<'_> {
    fn add(&self, fd: &rustix::fd::BorrowedFd, event: Event) -> Result<()> {
        let poller = unsafe { &mut *self.0.get() };
        unsafe { poller.add(fd, event)? }
        Ok(())
    }
}

impl PollHandler {
    fn new() -> Self {
        Self(UnsafeCell::new(Poller::new().unwrap()))
    }
    fn wait(&self, events: &mut Events, timeout: Option<std::time::Duration>) -> Result<()> {
        let poller = unsafe { &mut *self.0.get() };
        poller.wait(events, timeout)?;
        Ok(())
    }

    fn as_mut<'io>(&self) -> &'io mut Poller {
        unsafe { &mut *self.0.get() }
    }
}

type CallbackEntry = (usize, CompletionCallback);

const FD_INLINE_SIZE: usize = 32;

struct Callbacks {
    inline_entries: [Option<(usize, CompletionCallback)>; FD_INLINE_SIZE],
    heap_entries: Vec<CallbackEntry>,
    count: usize,
}

impl Callbacks {
    fn new() -> Self {
        Self {
            inline_entries: core::array::from_fn(|_| None),
            heap_entries: Vec::new(),
            count: 0,
        }
    }

    fn insert(&mut self, fd: usize, callback: CompletionCallback) {
        if self.count < FD_INLINE_SIZE {
            self.inline_entries[self.count] = Some((fd, callback));
        } else {
            self.heap_entries.push((fd, callback));
        }
        self.count += 1;
    }

    fn remove(&mut self, fd: usize) -> Option<CompletionCallback> {
        if let Some(pos) = self
            .inline_entries
            .iter()
            .position(|cb| cb.as_ref().is_some_and(|cb| cb.0 == fd))
        {
            let callback = self.inline_entries[pos].take();
            // swap with last valid entry
            if pos < self.count - 1 {
                self.inline_entries[pos] = self.inline_entries[self.count - 1].take();
            }
            self.count -= 1;
            return callback.map(|c| c.1);
        }

        if let Some(pos) = self.heap_entries.iter().position(|&(k, _)| k == fd) {
            self.count -= 1;
            return Some(self.heap_entries.swap_remove(pos).1);
        }
        None
    }
}

/// UnixIO lives longer than any of the files it creates, so it is
/// safe to store references to it's internals in the UnixFiles
pub struct UnixIO {
    poller: PollHandler,
    events: EventsHandler,
    callbacks: OwnedCallbacks,
}

impl UnixIO {
    #[cfg(feature = "fs")]
    pub fn new() -> Result<Self> {
        debug!("Using IO backend 'syscall'");
        Ok(Self {
            poller: PollHandler::new(),
            events: EventsHandler::new(),
            callbacks: OwnedCallbacks::new(),
        })
    }
}

impl IO for UnixIO {
    fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Rc<dyn File>> {
        trace!("open_file(path = {})", path);
        let file = std::fs::File::options()
            .read(true)
            .custom_flags(OFlags::NONBLOCK.bits() as i32)
            .write(true)
            .create(matches!(flags, OpenFlags::Create))
            .open(path)?;

        let unix_file = Rc::new(UnixFile {
            file: Rc::new(RefCell::new(file)),
            poller: BorrowedPollHandler(self.poller.as_mut().into()),
            callbacks: BorrowedCallbacks(self.callbacks.as_mut().into()),
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err() {
            unix_file.lock_file(true)?;
        }
        Ok(unix_file)
    }

    fn run_once(&self) -> Result<()> {
        if self.callbacks.is_empty() {
            return Ok(());
        }
        self.events.clear();
        trace!("run_once() waits for events");
        self.poller.wait(self.events.as_mut(), None)?;

        for event in self.events.iter() {
            if let Some(cf) = self.callbacks.remove(event.key) {
                let result = match cf {
                    CompletionCallback::Read(ref file, ref c, pos) => {
                        let mut file = file.borrow_mut();
                        let r = c.as_read();
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
                };
                match result {
                    Ok(n) => match &cf {
                        CompletionCallback::Read(_, ref c, _) => c.complete(0),
                        CompletionCallback::Write(_, ref c, _, _) => c.complete(n as i32),
                    },
                    Err(e) => return Err(e.into()),
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
    Read(Rc<RefCell<std::fs::File>>, Completion, usize),
    Write(
        Rc<RefCell<std::fs::File>>,
        Completion,
        Rc<RefCell<crate::Buffer>>,
        usize,
    ),
}

pub struct UnixFile<'io> {
    file: Rc<RefCell<std::fs::File>>,
    poller: BorrowedPollHandler<'io>,
    callbacks: BorrowedCallbacks<'io>,
}

impl File for UnixFile<'_> {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        let fd = self.file.borrow();
        let fd = fd.as_fd();
        // F_SETLK is a non-blocking lock. The lock will be released when the file is closed
        // or the process exits or after an explicit unlock.
        fs::fcntl_lock(
            fd,
            if exclusive {
                FlockOperation::NonBlockingLockExclusive
            } else {
                FlockOperation::NonBlockingLockShared
            },
        )
        .map_err(|e| {
            let io_error = std::io::Error::from(e);
            let message = match io_error.kind() {
                ErrorKind::WouldBlock => {
                    "Failed locking file. File is locked by another process".to_string()
                }
                _ => format!("Failed locking file, {}", io_error),
            };
            LimboError::LockingError(message)
        })?;

        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        let fd = self.file.borrow();
        let fd = fd.as_fd();
        fs::fcntl_lock(fd, FlockOperation::NonBlockingUnlock).map_err(|e| {
            LimboError::LockingError(format!(
                "Failed to release file lock: {}",
                std::io::Error::from(e)
            ))
        })?;
        Ok(())
    }

    fn pread(&self, pos: usize, c: Completion) -> Result<()> {
        let file = self.file.borrow();
        let result = {
            let r = c.as_read();
            let mut buf = r.buf_mut();
            rustix::io::pread(file.as_fd(), buf.as_mut_slice(), pos as u64)
        };
        match result {
            Ok(n) => {
                trace!("pread n: {}", n);
                // Read succeeded immediately
                c.complete(0);
                Ok(())
            }
            Err(Errno::AGAIN) => {
                trace!("pread blocks");
                // Would block, set up polling
                let fd = file.as_raw_fd();
                self.poller
                    .add(&file.as_fd(), Event::readable(fd as usize))?;
                {
                    self.callbacks.insert(
                        fd as usize,
                        CompletionCallback::Read(self.file.clone(), c, pos),
                    );
                }
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    fn pwrite(&self, pos: usize, buffer: Rc<RefCell<crate::Buffer>>, c: Completion) -> Result<()> {
        let file = self.file.borrow();
        let result = {
            let buf = buffer.borrow();
            rustix::io::pwrite(file.as_fd(), buf.as_slice(), pos as u64)
        };
        match result {
            Ok(n) => {
                trace!("pwrite n: {}", n);
                // Read succeeded immediately
                c.complete(n as i32);
                Ok(())
            }
            Err(Errno::AGAIN) => {
                trace!("pwrite blocks");
                // Would block, set up polling
                let fd = file.as_raw_fd();
                self.poller
                    .add(&file.as_fd(), Event::readable(fd as usize))?;
                self.callbacks.insert(
                    fd as usize,
                    CompletionCallback::Write(self.file.clone(), c, buffer.clone(), pos),
                );
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    fn sync(&self, c: Completion) -> Result<()> {
        let file = self.file.borrow();
        let result = fs::fsync(file.as_fd());
        match result {
            Ok(()) => {
                trace!("fsync");
                c.complete(0);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    fn size(&self) -> Result<u64> {
        let file = self.file.borrow();
        Ok(file.metadata()?.len())
    }
}

impl Drop for UnixFile<'_> {
    fn drop(&mut self) {
        self.unlock_file().expect("Failed to unlock file");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multiple_processes_cannot_open_file() {
        common::tests::test_multiple_processes_cannot_open_file(UnixIO::new);
    }
}
