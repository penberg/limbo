use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Database {
    _inner: lig_core::Database,
}

#[wasm_bindgen]
impl Database {
    pub fn open(path: &str) -> Database {
        let io = IO {};
        let inner = lig_core::Database::open(&io, path).unwrap();
        Database { _inner: inner }
    }
}

struct IO {}

impl lig_core::IO for IO {
    fn open(&self, _path: &str) -> anyhow::Result<lig_core::PageSource> {
        todo!();
    }
}
