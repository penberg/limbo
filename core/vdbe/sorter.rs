use crate::types::Record;
use std::cmp::Ordering;

pub struct Sorter {
    records: Vec<Record>,
    current: Option<Record>,
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
        self.records.is_empty()
    }

    pub fn has_more(&self) -> bool {
        self.current.is_some()
    }

    // We do the sorting here since this is what is called by the SorterSort instruction
    pub fn sort(&mut self) {
        self.records.sort_by(|a, b| {
            let cmp_by_idx = |idx: usize, ascending: bool| {
                let a = &a.get_value(idx);
                let b = &b.get_value(idx);
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
    pub fn record(&self) -> Option<&Record> {
        self.current.as_ref()
    }

    pub fn insert(&mut self, record: &Record) {
        self.records.push(Record::new(record.get_values().to_vec()));
    }
}
