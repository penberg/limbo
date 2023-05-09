//! Multiversion concurrency control (MVCC) for Rust.
//!
//! This module implements the main memory MVCC method outlined in the paper
//! "High-Performance Concurrency Control Mechanisms for Main-Memory Databases"
//! by Per-Åke Larson et al (VLDB, 2011).
//!
//! ## Data anomalies
//!
//! * A *dirty write* occurs when transaction T_m updates a value that is written by
//!   transaction T_n but not yet committed. The MVCC algorithm prevents dirty
//!   writes by validating that a row version is visible to transaction T_m before
//!   allowing update to it.
//!
//! * A *dirty read* occurs when transaction T_m reads a value that was written by
//!   transaction T_n but not yet committed. The MVCC algorithm prevents dirty
//!   reads by validating that a row version is visible to transaction T_m.
//!
//! * A *fuzzy read* (non-repeatable read) occurs when transaction T_m reads a
//!   different value in the course of the transaction because another
//!   transaction T_n has updated the value.
//!
//! * A *lost update* occurs when transactions T_m and T_n both attempt to update
//!   the same value, resulting in one of the updates being lost. The MVCC algorithm
//!   prevents lost updates by detecting the write-write conflict and letting the
//!   first-writer win by aborting the later transaction.
//!
//! TODO: phantom reads, cursor lost updates, read skew, write skew.
//!
//! ## TODO
//!
//! * Optimistic reads and writes
//! * Garbage collection

pub mod clock;
pub mod database;
pub mod errors;
pub mod persistent_storage;
pub mod sync;