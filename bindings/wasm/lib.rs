use anyhow::Result;
use lig_core::DatabaseRef;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Database {
    _inner: lig_core::Database,
}

#[wasm_bindgen]
impl Database {
    pub fn open(path: &str) -> Database {
        let inner = lig_core::Database::open(Arc::new(IO {}), path).unwrap();
        Database { _inner: inner }
    }
}

struct IO {}

impl lig_core::IO for IO {
    fn open(&self, _path: &str) -> Result<DatabaseRef> {
        todo!()
    }

    fn get(&self, _database_ref: DatabaseRef, _page_idx: usize, _buf: &mut [u8]) -> Result<()> {
        todo!()
    }
}
