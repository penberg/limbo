//! Streaming SQLite tokenizer

mod scan;
pub mod sql;

pub use scan::{ScanError, Scanner, Splitter};
