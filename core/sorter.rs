use crate::types::{Cursor, CursorResult, OwnedRecord};
use anyhow::Result;
use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
};

pub struct Sorter {
    records: BTreeMap<OwnedRecord, VecDeque<OwnedRecord>>,
    current: RefCell<Option<VecDeque<OwnedRecord>>>,
    order: Vec<bool>,
}

impl Sorter {
    pub fn new(order: Vec<bool>) -> Self {
        Self {
            records: BTreeMap::new(),
            current: RefCell::new(None),
            order,
        }
    }

    pub fn insert(&mut self, key: OwnedRecord, record: OwnedRecord) {
        if let Some(vec) = self.records.get_mut(&key) {
            vec.push_back(record);
        } else {
            self.records.insert(key, VecDeque::from(vec![record]));
        }
    }
}

impl Cursor for Sorter {
    fn is_empty(&self) -> bool {
        self.current.borrow().is_none()
    }

    fn rewind(&mut self) -> Result<CursorResult<()>> {
        let empty = {
            let current = self.current.borrow();
            current.as_ref().map(|r| r.is_empty()).unwrap_or(true)
        };
        if empty {
            let mut c = self.current.borrow_mut();
            *c = self.records.pop_first().map(|(_, record)| record);
        }
        Ok(CursorResult::Ok(()))
    }

    fn next(&mut self) -> Result<CursorResult<()>> {
        let empty = {
            let current = self.current.borrow();
            current.as_ref().map(|r| r.is_empty()).unwrap_or(true)
        };
        if empty {
            let mut c = self.current.borrow_mut();
            *c = self.records.pop_first().map(|(_, record)| record);
        }
        Ok(CursorResult::Ok(()))
    }

    fn wait_for_completion(&mut self) -> Result<()> {
        Ok(())
    }

    fn rowid(&self) -> Result<Option<u64>> {
        todo!();
    }

    fn record(&self) -> Result<Option<OwnedRecord>> {
        let mut current = self.current.borrow_mut();
        Ok(current.as_mut().map(|r| r.pop_front().unwrap()))
    }

    fn insert(&mut self, record: &OwnedRecord) -> Result<()> {
        let key_fields = self.order.len();
        let key = OwnedRecord::new(record.values[0..key_fields].to_vec());
        self.insert(key, OwnedRecord::new(record.values[key_fields..].to_vec()));
        Ok(())
    }

    fn set_null_flag(&mut self, _flag: bool) {
        todo!();
    }

    fn get_null_flag(&self) -> bool {
        todo!();
    }
}
