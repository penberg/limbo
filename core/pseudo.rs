use crate::types::Record;

pub struct PseudoCursor {
    current: Option<Record>,
}

impl PseudoCursor {
    pub fn new() -> Self {
        Self { current: None }
    }

    pub fn record(&self) -> Option<&Record> {
        self.current.as_ref()
    }

    pub fn insert(&mut self, record: Record) {
        self.current = Some(record);
    }
}
