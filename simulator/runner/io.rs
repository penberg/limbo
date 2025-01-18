use std::{cell::RefCell, rc::Rc};

use limbo_core::{OpenFlags, PlatformIO, Result, IO};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::runner::file::SimulatorFile;

pub(crate) struct SimulatorIO {
    pub(crate) inner: Box<dyn IO>,
    pub(crate) fault: RefCell<bool>,
    pub(crate) files: RefCell<Vec<Rc<SimulatorFile>>>,
    pub(crate) rng: RefCell<ChaCha8Rng>,
    pub(crate) nr_run_once_faults: RefCell<usize>,
    pub(crate) page_size: usize,
}

impl SimulatorIO {
    pub(crate) fn new(seed: u64, page_size: usize) -> Result<Self> {
        let inner = Box::new(PlatformIO::new()?);
        let fault = RefCell::new(false);
        let files = RefCell::new(Vec::new());
        let rng = RefCell::new(ChaCha8Rng::seed_from_u64(seed));
        let nr_run_once_faults = RefCell::new(0);
        Ok(Self {
            inner,
            fault,
            files,
            rng,
            nr_run_once_faults,
            page_size,
        })
    }

    pub(crate) fn inject_fault(&self, fault: bool) {
        self.fault.replace(fault);
        for file in self.files.borrow().iter() {
            file.inject_fault(fault);
        }
    }

    pub(crate) fn print_stats(&self) {
        log::info!("run_once faults: {}", self.nr_run_once_faults.borrow());
        for file in self.files.borrow().iter() {
            log::info!("");
            log::info!("===========================");
            file.print_stats();
        }
    }
}

impl IO for SimulatorIO {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        _direct: bool,
    ) -> Result<Rc<dyn limbo_core::File>> {
        let inner = self.inner.open_file(path, flags, false)?;
        let file = Rc::new(SimulatorFile {
            inner,
            fault: RefCell::new(false),
            nr_pread_faults: RefCell::new(0),
            nr_pwrite_faults: RefCell::new(0),
            nr_pread_calls: RefCell::new(0),
            nr_pwrite_calls: RefCell::new(0),
            nr_sync_calls: RefCell::new(0),
            page_size: self.page_size,
        });
        self.files.borrow_mut().push(file.clone());
        Ok(file)
    }

    fn run_once(&self) -> Result<()> {
        if *self.fault.borrow() {
            *self.nr_run_once_faults.borrow_mut() += 1;
            return Err(limbo_core::LimboError::InternalError(
                "Injected fault".into(),
            ));
        }
        self.inner.run_once()?;
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        self.rng.borrow_mut().next_u64() as i64
    }

    fn get_current_time(&self) -> String {
        "2024-01-01 00:00:00".to_string()
    }
}
