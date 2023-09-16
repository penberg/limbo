use anyhow::Result;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Database {
    _inner: lig_core::Database,
}

#[wasm_bindgen]
impl Database {
    pub fn open(_path: &str) -> Database {
        let storage = lig_core::Storage::from_io(Arc::new(IO {}));
        let inner = lig_core::Database::open(storage).unwrap();
        Database { _inner: inner }
    }
}

pub struct IO {}

impl lig_core::StorageIO for IO {
    fn get(&self, _page_idx: usize, _buf: &mut [u8]) -> Result<()> {
        todo!();
    }
}
