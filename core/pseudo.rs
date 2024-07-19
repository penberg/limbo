use anyhow::Result;
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
                _ => panic!("Expected integer value"),
            });
        Ok(x)
    }

    fn record(&self) -> Result<Ref<Option<OwnedRecord>>> {
        Ok(self.current.borrow())
    }

    fn insert(&mut self, record: &OwnedRecord) -> Result<()> {
        *self.current.borrow_mut() = Some(record.clone());
        Ok(())
    }

    fn get_null_flag(&self) -> bool {
        false
    }

    fn set_null_flag(&mut self, _null_flag: bool) {
        // Do nothing
    }
}
