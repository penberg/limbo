use crate::{
    types::{Cursor, CursorResult, OwnedRecord, OwnedValue, SeekKey, SeekOp},
    Result,
};
use std::cell::{Ref, RefCell};

pub struct Sorter {
    records: Vec<OwnedRecord>,
    current: RefCell<Option<OwnedRecord>>,
    order: Vec<bool>,
}

impl Sorter {
    pub fn new(order: Vec<bool>) -> Self {
        Self {
            records: Vec::new(),
            current: RefCell::new(None),
            order,
        }
    }
}

impl Cursor for Sorter {
    fn is_empty(&self) -> bool {
        self.current.borrow().is_none()
    }

    // We do the sorting here since this is what is called by the SorterSort instruction
    fn rewind(&mut self) -> Result<CursorResult<()>> {
        let key_fields = self.order.len();
        self.records
            .sort_by_cached_key(|record| OwnedRecord::new(record.values[0..key_fields].to_vec()));
        self.records.reverse();

        self.next()
    }

    fn next(&mut self) -> Result<CursorResult<()>> {
        let mut c = self.current.borrow_mut();
        *c = self.records.pop();
        Ok(CursorResult::Ok(()))
    }

    fn wait_for_completion(&mut self) -> Result<()> {
        Ok(())
    }

    fn rowid(&self) -> Result<Option<u64>> {
        todo!();
    }

    fn seek(&mut self, _: SeekKey<'_>, _: SeekOp) -> Result<CursorResult<bool>> {
        unimplemented!();
    }

    fn seek_to_last(&mut self) -> Result<CursorResult<()>> {
        unimplemented!();
    }

    fn record(&self) -> Result<Ref<Option<OwnedRecord>>> {
        let ret = self.current.borrow();
        // log::trace!("returning {:?}", ret);
        Ok(ret)
    }

    fn insert(
        &mut self,
        key: &OwnedValue,
        record: &OwnedRecord,
        moved_before: bool,
    ) -> Result<CursorResult<()>> {
        let _ = key;
        let _ = moved_before;
        self.records.push(OwnedRecord::new(record.values.to_vec()));
        Ok(CursorResult::Ok(()))
    }

    fn set_null_flag(&mut self, _flag: bool) {
        todo!();
    }

    fn get_null_flag(&self) -> bool {
        false
    }

    fn exists(&mut self, key: &OwnedValue) -> Result<CursorResult<bool>> {
        let _ = key;
        todo!()
    }

    fn btree_create(&mut self, _flags: usize) -> u32 {
        unreachable!("Why did you try to build a new tree with a sorter??? Stand up, open the door and take a walk for 30 min to come back with a better plan.");
    }
}
