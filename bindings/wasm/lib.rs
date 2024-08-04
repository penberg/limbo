use limbo_core::Result;
use std::rc::Rc;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Database {
    _inner: limbo_core::Database,
}

#[wasm_bindgen]
impl Database {
    #[wasm_bindgen(constructor)]
    pub fn new(_path: &str) -> Database {
        let io = Arc::new(IO {});
        let page_io = Rc::new(DatabaseStorage {});
        let wal = Rc::new(Wal {});
        let inner = limbo_core::Database::open(io, page_io, wal).unwrap();
        Database { _inner: inner }
    }

    #[wasm_bindgen]
    pub fn exec(&self, _sql: &str) {}
}

pub struct IO {}

impl limbo_core::IO for IO {
    fn open_file(&self, _path: &str) -> Result<Rc<dyn limbo_core::File>> {
        todo!();
    }

    fn run_once(&self) -> Result<()> {
        todo!();
    }
}

pub struct DatabaseStorage {}

impl limbo_core::DatabaseStorage for DatabaseStorage {
    fn read_page(&self, _page_idx: usize, _c: Rc<limbo_core::Completion>) -> Result<()> {
        todo!();
    }

    fn write_page(
        &self,
        _page_idx: usize,
        _buffer: Rc<std::cell::RefCell<limbo_core::Buffer>>,
        _c: Rc<limbo_core::Completion>,
    ) -> Result<()> {
        todo!()
    }
}

pub struct Wal {}

impl limbo_core::Wal for Wal {
    fn begin_read_tx(&self) -> Result<()> {
        todo!()
    }

    fn end_read_tx(&self) -> Result<()> {
        todo!()
    }

    fn find_frame(&self, _page_id: u64) -> Result<Option<u64>> {
        todo!()
    }

    fn read_frame(
        &self,
        _frame_id: u64,
        _page: Rc<std::cell::RefCell<limbo_core::Page>>,
    ) -> Result<()> {
        todo!()
    }
}

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}