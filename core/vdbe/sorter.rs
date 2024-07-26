use crate::{
    types::{Cursor, CursorResult, OwnedRecord, OwnedValue},
    Result,
};
use std::{
    cell::{Ref, RefCell},
    collections::{BTreeMap, VecDeque},
};

pub struct Sorter {
    records: BTreeMap<OwnedRecord, VecDeque<OwnedRecord>>,
    current: RefCell<Option<OwnedRecord>>,
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
        let mut c = self.current.borrow_mut();
        for (_, record) in self.records.iter_mut() {
            let record = record.pop_front();
            if record.is_some() {
                *c = record;
                break;
            }
        }

        Ok(CursorResult::Ok(()))
    }

    fn next(&mut self) -> Result<CursorResult<()>> {
        let mut c = self.current.borrow_mut();
        let mut matched = false;
        for (_, record) in self.records.iter_mut() {
            let record = record.pop_front();
            if record.is_some() {
                *c = record;
                matched = true;
                break;
            }
        }
        self.records.retain(|_, v| !v.is_empty());
        if !matched {
            *c = None;
        }
        Ok(CursorResult::Ok(()))
    }

    fn wait_for_completion(&mut self) -> Result<()> {
        Ok(())
    }

    fn rowid(&self) -> Result<Option<u64>> {
        todo!();
    }

    fn record(&self) -> Result<Ref<Option<OwnedRecord>>> {
        Ok(self.current.borrow())
    }

    fn insert(&mut self, key: &OwnedValue, record: &OwnedRecord) -> Result<CursorResult<()>> {
        let _ = key;
        let key_fields = self.order.len();
        let key = OwnedRecord::new(record.values[0..key_fields].to_vec());
        self.insert(key, OwnedRecord::new(record.values[key_fields..].to_vec()));
        Ok(CursorResult::Ok(()))
    }

    fn set_null_flag(&mut self, _flag: bool) {
        todo!();
    }

    fn get_null_flag(&self) -> bool {
        todo!();
    }

    fn exists(&mut self, key: &OwnedValue) -> Result<bool> {
        let _ = key;
        todo!()
    }
}
