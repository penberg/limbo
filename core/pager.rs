use crate::sqlite3_ondisk;
use crate::IO;
use anyhow::Result;
use std::sync::Arc;

pub struct Pager {
    _io: Arc<dyn IO>,
}

impl Pager {
    pub fn open(io: Arc<dyn IO>, path: &str) -> Result<Self> {
        let database_ref = io.open(path)?;
        let db_header = sqlite3_ondisk::read_database_header(io.clone(), database_ref)?;
        let _ = sqlite3_ondisk::read_btree_page(
            io.clone(),
            database_ref,
            db_header.page_size as usize,
            1,
        )?;
        Ok(Self { _io: io })
    }
}
