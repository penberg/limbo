use crate::types::OwnedRecord;

pub struct PseudoCursor {
    current: Option<OwnedRecord>,
}

impl PseudoCursor {
    pub fn new() -> Self {
        Self { current: None }
    }

    pub fn record(&self) -> Option<&OwnedRecord> {
        self.current.as_ref()
    }

    pub fn insert(&mut self, record: OwnedRecord) {
        self.current = Some(record);
    }
}
