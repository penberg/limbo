use anyhow::Result;
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
        let page_source = limbo_core::PageSource::from_io(Rc::new(PageIO {}));
        let inner = limbo_core::Database::open(io, page_source).unwrap();
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

pub struct PageIO {}

impl limbo_core::PageIO for PageIO {
    fn get(&self, _page_idx: usize, _c: Rc<limbo_core::Completion>) -> Result<()> {
        todo!();
    }

    fn write(
        &self,
        _page_idx: usize,
        _buffer: Rc<std::cell::RefCell<limbo_core::Buffer>>,
        _c: Rc<limbo_core::WriteCompletion>,
    ) -> Result<()> {
        todo!()
    }
}
