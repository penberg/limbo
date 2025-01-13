use crate::types::OwnedRecord;
use std::cmp::Ordering;

pub struct Sorter {
    records: Vec<OwnedRecord>,
    current: Option<OwnedRecord>,
    order: Vec<bool>,
}

impl Sorter {
    pub fn new(order: Vec<bool>) -> Self {
        Self {
            records: Vec::new(),
            current: None,
            order,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.current.is_none()
    }
    // We do the sorting here since this is what is called by the SorterSort instruction
    pub fn sort(&mut self) {
        self.records.sort_by(|a, b| {
            let cmp_by_idx = |idx: usize, ascending: bool| {
                let a = &a.values[idx];
                let b = &b.values[idx];
                if ascending {
                    a.cmp(b)
                } else {
                    b.cmp(a)
                }
            };

            let mut cmp_ret = Ordering::Equal;
            for (idx, &is_asc) in self.order.iter().enumerate() {
                cmp_ret = cmp_by_idx(idx, is_asc);
                if cmp_ret != Ordering::Equal {
                    break;
                }
            }
            cmp_ret
        });
        self.records.reverse();
        self.next()
    }
    pub fn next(&mut self) {
        self.current = self.records.pop();
    }
    pub fn record(&self) -> Option<&OwnedRecord> {
        self.current.as_ref()
    }

    pub fn insert(&mut self, record: &OwnedRecord) {
        self.records.push(OwnedRecord::new(record.values.to_vec()));
    }
}
