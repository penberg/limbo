use crate::pager::Pager;
use crate::sqlite3_ondisk::Record;

use anyhow::Result;

use std::sync::Arc;

pub struct Cursor {
    pager: Arc<Pager>,
    root_page: usize,
}

impl Cursor {
    pub fn new(pager: Arc<Pager>, root_page: usize) -> Self {
        Self { pager, root_page }
    }

    pub fn rewind(&mut self) -> Result<()> {
        self.pager.read_page(self.root_page)?;
        Ok(())
    }

    pub fn next(&mut self) -> Result<Option<Record>> {
        todo!()
    }
}
