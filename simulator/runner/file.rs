use std::{cell::RefCell, rc::Rc};

use limbo_core::{File, Result};

pub(crate) struct SimulatorFile {
    pub(crate) inner: Rc<dyn File>,
    pub(crate) fault: RefCell<bool>,
    pub(crate) nr_pread_faults: RefCell<usize>,
    pub(crate) nr_pwrite_faults: RefCell<usize>,
    pub(crate) writes: RefCell<usize>,
    pub(crate) reads: RefCell<usize>,
    pub(crate) syncs: RefCell<usize>,
    pub(crate) page_size: usize,
}

impl SimulatorFile {
    pub(crate) fn inject_fault(&self, fault: bool) {
        self.fault.replace(fault);
    }

    pub(crate) fn print_stats(&self) {
        println!(
            "pread faults: {}, pwrite faults: {}, reads: {}, writes: {}, syncs: {}",
            *self.nr_pread_faults.borrow(),
            *self.nr_pwrite_faults.borrow(),
            *self.reads.borrow(),
            *self.writes.borrow(),
            *self.syncs.borrow(),
        );
    }
}

impl limbo_core::File for SimulatorFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        if *self.fault.borrow() {
            return Err(limbo_core::LimboError::InternalError(
                "Injected fault".into(),
            ));
        }
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> Result<()> {
        if *self.fault.borrow() {
            return Err(limbo_core::LimboError::InternalError(
                "Injected fault".into(),
            ));
        }
        self.inner.unlock_file()
    }

    fn pread(&self, pos: usize, c: Rc<limbo_core::Completion>) -> Result<()> {
        if *self.fault.borrow() {
            *self.nr_pread_faults.borrow_mut() += 1;
            return Err(limbo_core::LimboError::InternalError(
                "Injected fault".into(),
            ));
        }
        *self.reads.borrow_mut() += 1;
        self.inner.pread(pos, c)
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Rc<std::cell::RefCell<limbo_core::Buffer>>,
        c: Rc<limbo_core::Completion>,
    ) -> Result<()> {
        if *self.fault.borrow() {
            *self.nr_pwrite_faults.borrow_mut() += 1;
            return Err(limbo_core::LimboError::InternalError(
                "Injected fault".into(),
            ));
        }
        *self.writes.borrow_mut() += 1;
        self.inner.pwrite(pos, buffer, c)
    }

    fn sync(&self, c: Rc<limbo_core::Completion>) -> Result<()> {
        *self.syncs.borrow_mut() += 1;
        self.inner.sync(c)
    }

    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
}

impl Drop for SimulatorFile {
    fn drop(&mut self) {
        self.inner.unlock_file().expect("Failed to unlock file");
    }
}
