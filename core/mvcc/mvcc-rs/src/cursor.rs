use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::clock::LogicalClock;
use crate::database::{Database, Result, Row, RowID};
use std::fmt::Debug;

#[derive(Debug)]
pub struct ScanCursor<'a, Clock: LogicalClock, T: Sync + Send + Clone + Serialize + DeserializeOwned + Debug> {
    pub db: &'a Database<Clock, T>,
    pub row_ids: Vec<RowID>,
    pub index: usize,
    tx_id: u64,
}

impl<'a, Clock: LogicalClock, T: Sync + Send + Clone + Serialize + DeserializeOwned + Debug + 'static> ScanCursor<'a, Clock, T> {
    pub fn new(
        db: &'a Database<Clock, T>,
        tx_id: u64,
        table_id: u64,
    ) -> Result<ScanCursor<'a, Clock, T>> {
        let row_ids = db.scan_row_ids_for_table(table_id)?;
        Ok(Self {
            db,
            tx_id,
            row_ids,
            index: 0,
        })
    }

    pub fn current_row_id(&self) -> Option<RowID> {
        if self.index >= self.row_ids.len() {
            return None;
        }
        Some(self.row_ids[self.index])
    }

    pub fn current_row(&self) -> Result<Option<Row<T>>> {
        if self.index >= self.row_ids.len() {
            return Ok(None);
        }
        let id = self.row_ids[self.index];
        self.db.read(self.tx_id, id)
    }

    pub fn close(self) -> Result<()> {
        Ok(())
    }

    pub fn forward(&mut self) -> bool {
        self.index += 1;
        self.index < self.row_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index >= self.row_ids.len()
    }
}
