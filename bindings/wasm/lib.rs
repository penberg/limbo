use anyhow::Result;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Database {
    _inner: limbo_core::Database,
}

#[wasm_bindgen]
impl Database {
    pub fn open(_path: &str) -> Database {
        let storage = limbo_core::Storage::from_io(Arc::new(IO {}));
        let inner = limbo_core::Database::open(storage).unwrap();
        Database { _inner: inner }
    }
}

pub struct IO {}

impl limbo_core::StorageIO for IO {
    fn get(&self, _page_idx: usize, _c: &mut limbo_core::Completion) -> Result<()> {
        todo!();
    }
}
