use std::{cell::RefCell, rc::Rc};

use limbo_core::{File, Result};
pub(crate) struct SimulatorFile {
    pub(crate) inner: Rc<dyn File>,
    pub(crate) fault: RefCell<bool>,

    /// Number of `pread` function calls (both success and failures).
    pub(crate) nr_pread_calls: RefCell<usize>,

    /// Number of `pread` function calls with injected fault.
    pub(crate) nr_pread_faults: RefCell<usize>,

    /// Number of `pwrite` function calls (both success and failures).
    pub(crate) nr_pwrite_calls: RefCell<usize>,

    /// Number of `pwrite` function calls with injected fault.
    pub(crate) nr_pwrite_faults: RefCell<usize>,

    /// Number of `sync` function calls (both success and failures).
    pub(crate) nr_sync_calls: RefCell<usize>,

    pub(crate) page_size: usize,
}

impl SimulatorFile {
    pub(crate) fn inject_fault(&self, fault: bool) {
        self.fault.replace(fault);
    }

    pub(crate) fn print_stats(&self) {
        println!("op           calls   faults");
        println!("--------- -------- --------");
        println!(
            "pread     {:8} {:8}",
            *self.nr_pread_calls.borrow(),
            *self.nr_pread_faults.borrow()
        );
        println!(
            "pwrite    {:8} {:8}",
            *self.nr_pwrite_calls.borrow(),
            *self.nr_pwrite_faults.borrow()
        );
        println!(
            "sync      {:8} {:8}",
            *self.nr_sync_calls.borrow(),
            0 // No fault counter for sync
        );
        println!("--------- -------- --------");
        let sum_calls = *self.nr_pread_calls.borrow()
            + *self.nr_pwrite_calls.borrow()
            + *self.nr_sync_calls.borrow();
        let sum_faults = *self.nr_pread_faults.borrow() + *self.nr_pwrite_faults.borrow();
        println!("total     {:8} {:8}", sum_calls, sum_faults);
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
        *self.nr_pread_calls.borrow_mut() += 1;
        if *self.fault.borrow() {
            *self.nr_pread_faults.borrow_mut() += 1;
            return Err(limbo_core::LimboError::InternalError(
                "Injected fault".into(),
            ));
        }
        self.inner.pread(pos, c)
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Rc<std::cell::RefCell<limbo_core::Buffer>>,
        c: Rc<limbo_core::Completion>,
    ) -> Result<()> {
        *self.nr_pwrite_calls.borrow_mut() += 1;
        if *self.fault.borrow() {
            *self.nr_pwrite_faults.borrow_mut() += 1;
            return Err(limbo_core::LimboError::InternalError(
                "Injected fault".into(),
            ));
        }
        self.inner.pwrite(pos, buffer, c)
    }

    fn sync(&self, c: Rc<limbo_core::Completion>) -> Result<()> {
        *self.nr_sync_calls.borrow_mut() += 1;
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
