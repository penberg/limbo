use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Database {
    _inner: lig_core::Database,
}

#[wasm_bindgen]
impl Database {
    pub fn open(path: &str) -> Database {
        let io = lig_core::IO::new().unwrap();
        let inner = lig_core::Database::open(io, path).unwrap();
        Database { _inner: inner }
    }
}
