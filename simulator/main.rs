use anyhow::Result;
use limbo_core::{Database, File, PlatformIO, IO};
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::cell::RefCell;
use std::rc::Rc;

fn main() {
    let seed = match std::env::var("SEED") {
        Ok(seed) => seed.parse::<u64>().unwrap(),
        Err(_) => rand::thread_rng().next_u64(),
    };
    println!("Seed: {}", seed);
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let io = Rc::new(SimulatorIO::new().unwrap());
    for _ in 0..100000 {
        let db = match Database::open_file(io.clone(), "./testing/testing.db") {
            Ok(db) => db,
            Err(_) => continue,
        };
        io.inject_fault(rng.gen_bool(0.5));
        match io.run_once() {
            Ok(_) => {}
            Err(_) => continue,
        }
        let conn = db.connect();
        let mut stmt = conn.prepare("SELECT 1").unwrap();
        let mut rows = stmt.query().unwrap();
        match rows.next_row().unwrap() {
            limbo_core::RowResult::Row(row) => {
                assert_eq!(row.get::<i64>(0).unwrap(), 1);
            }
            limbo_core::RowResult::IO => {
                todo!();
            }
            limbo_core::RowResult::Done => {
                unreachable!();
            }
        }
        stmt.reset();
    }
}

struct SimulatorIO {
    inner: Box<dyn IO>,
    fault: RefCell<bool>,
    files: RefCell<Vec<Rc<SimulatorFile>>>,
}

impl SimulatorIO {
    fn new() -> Result<Self> {
        let inner = Box::new(PlatformIO::new()?);
        let fault = RefCell::new(false);
        let files = RefCell::new(Vec::new());
        Ok(Self {
            inner,
            fault,
            files,
        })
    }

    fn inject_fault(&self, fault: bool) {
        self.fault.replace(fault);
        for file in self.files.borrow().iter() {
            file.inject_fault(fault);
        }
    }
}

impl IO for SimulatorIO {
    fn open_file(&self, path: &str) -> Result<Rc<dyn limbo_core::File>> {
        let inner = self.inner.open_file(path)?;
        let file = Rc::new(SimulatorFile {
            inner,
            fault: RefCell::new(false),
        });
        self.files.borrow_mut().push(file.clone());
        Ok(file)
    }

    fn run_once(&self) -> Result<()> {
        if *self.fault.borrow() {
            return Err(anyhow::anyhow!("Injected fault"));
        }
        self.inner.run_once().unwrap();
        Ok(())
    }
}

struct SimulatorFile {
    inner: Rc<dyn File>,
    fault: RefCell<bool>,
}

impl SimulatorFile {
    fn inject_fault(&self, fault: bool) {
        self.fault.replace(fault);
    }
}

impl limbo_core::File for SimulatorFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        if *self.fault.borrow() {
            return Err(anyhow::anyhow!("Injected fault"));
        }
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> Result<()> {
        if *self.fault.borrow() {
            return Err(anyhow::anyhow!("Injected fault"));
        }
        self.inner.unlock_file()
    }

    fn pread(&self, pos: usize, c: Rc<limbo_core::Completion>) -> Result<()> {
        if *self.fault.borrow() {
            return Err(anyhow::anyhow!("Injected fault"));
        }
        self.inner.pread(pos, c)
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Rc<std::cell::RefCell<limbo_core::Buffer>>,
        c: Rc<limbo_core::WriteCompletion>,
    ) -> Result<()> {
        if *self.fault.borrow() {
            return Err(anyhow::anyhow!("Injected fault"));
        }
        self.inner.pwrite(pos, buffer, c)
    }
}
