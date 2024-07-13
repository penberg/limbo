use super::{Completion, File, WriteCompletion, IO};
use anyhow::{Ok, Result};
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
    fn open_file(&self, path: &str) -> Result<Rc<dyn File>> {
        trace!("open_file(path = {})", path);
        let file = std::fs::File::options()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(path)?;
        Ok(Rc::new(DarwinFile {
            file: Rc::new(RefCell::new(file)),
            poller: self.poller.clone(),
            callbacks: self.callbacks.clone(),
        }))
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
                            let mut buf = c.buf_mut();
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
                        match cf {
                            CompletionCallback::Read(_, ref c, _) => {
                                c.complete();
                            }
                            CompletionCallback::Write(_, ref c, _, _) => {
                                c.complete(n);
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
}

enum CompletionCallback {
    Read(Rc<RefCell<std::fs::File>>, Rc<Completion>, usize),
    Write(
        Rc<RefCell<std::fs::File>>,
        Rc<WriteCompletion>,
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
    fn pread(&self, pos: usize, c: Rc<Completion>) -> Result<()> {
        let file = self.file.borrow();
        let result = {
            let mut buf = c.buf_mut();
            rustix::io::pread(file.as_fd(), buf.as_mut_slice(), pos as u64)
        };
        match result {
            std::result::Result::Ok(n) => {
                trace!("pread n: {}", n);
                // Read succeeded immediately
                c.complete();
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
        c: Rc<WriteCompletion>,
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
                c.complete(n);
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
}
