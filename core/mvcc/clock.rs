use std::sync::atomic::{AtomicU64, Ordering};

/// Logical clock.
pub trait LogicalClock {
    fn get_timestamp(&self) -> u64;
    fn reset(&self, ts: u64);
}

/// A node-local clock backed by an atomic counter.
#[derive(Debug, Default)]
pub struct LocalClock {
    ts_sequence: AtomicU64,
}

impl LocalClock {
    pub fn new() -> Self {
        Self {
            ts_sequence: AtomicU64::new(0),
        }
    }
}

impl LogicalClock for LocalClock {
    fn get_timestamp(&self) -> u64 {
        self.ts_sequence.fetch_add(1, Ordering::SeqCst)
    }

    fn reset(&self, ts: u64) {
        self.ts_sequence.store(ts, Ordering::SeqCst);
    }
}
