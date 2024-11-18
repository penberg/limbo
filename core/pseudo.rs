use crate::{
    types::{SeekKey, SeekOp},
    Result,
};
use std::cell::{Ref, RefCell};

use crate::types::{Cursor, CursorResult, OwnedRecord, OwnedValue};

pub struct PseudoCursor {
    current: RefCell<Option<OwnedRecord>>,
}

impl PseudoCursor {
    pub fn new() -> Self {
        Self {
            current: RefCell::new(None),
        }
    }
}

impl Cursor for PseudoCursor {
    fn is_empty(&self) -> bool {
        self.current.borrow().is_none()
    }

    fn rewind(&mut self) -> Result<CursorResult<()>> {
        *self.current.borrow_mut() = None;
        Ok(CursorResult::Ok(()))
    }

    fn next(&mut self) -> Result<CursorResult<()>> {
        *self.current.borrow_mut() = None;
        Ok(CursorResult::Ok(()))
    }

    fn wait_for_completion(&mut self) -> Result<()> {
        Ok(())
    }

    fn rowid(&self) -> Result<Option<u64>> {
        let x = self
            .current
            .borrow()
            .as_ref()
            .map(|record| match record.values[0] {
                OwnedValue::Integer(rowid) => rowid as u64,
                ref ov => {
                    panic!("Expected integer value, got {:?}", ov);
                }
            });
        Ok(x)
    }

    fn seek(&mut self, _: SeekKey<'_>, _: SeekOp) -> Result<CursorResult<bool>> {
        unimplemented!();
    }

    fn seek_to_last(&mut self) -> Result<CursorResult<()>> {
        unimplemented!();
    }

    fn record(&self) -> Result<Ref<Option<OwnedRecord>>> {
        Ok(self.current.borrow())
    }

    fn insert(
        &mut self,
        key: &OwnedValue,
        record: &OwnedRecord,
        moved_before: bool,
    ) -> Result<CursorResult<()>> {
        let _ = key;
        let _ = moved_before;
        *self.current.borrow_mut() = Some(record.clone());
        Ok(CursorResult::Ok(()))
    }

    fn get_null_flag(&self) -> bool {
        false
    }

    fn set_null_flag(&mut self, _null_flag: bool) {
        // Do nothing
    }

    fn exists(&mut self, key: &OwnedValue) -> Result<CursorResult<bool>> {
        let _ = key;
        todo!()
    }

    fn btree_create(&mut self, _flags: usize) -> u32 {
        unreachable!("Please don't.")
    }
}
