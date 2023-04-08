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
//! * A *fuzzy read* (non-repetable read) occurs when transaction T_m reads a
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

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub id: u64,
    pub data: String,
}

/// A row version.
#[derive(Clone, Debug)]
struct RowVersion {
    begin: TxTimestampOrID,
    end: Option<TxTimestampOrID>,
    row: Row,
}

/// A transaction timestamp or ID.
///
/// Versions either track a timestamp or a transaction ID, depending on the
/// phase of the transaction. During the active phase, new versions track the
/// transaction ID in the `begin` and `end` fields. After a transaction commits,
/// versions switch to tracking timestamps.
#[derive(Clone, Debug, PartialEq)]
enum TxTimestampOrID {
    Timestamp(u64),
    TxID(u64),
}

/// Transaction
#[derive(Debug, Clone)]
pub struct Transaction {
    /// The state of the transaction.
    state: TransactionState,
    /// The transaction ID.
    tx_id: u64,
    /// The transaction begin timestamp.
    begin_ts: u64,
    /// The transaction write set.
    write_set: HashSet<u64>,
    /// The transaction read set.
    read_set: RefCell<HashSet<u64>>,
}

impl Transaction {
    fn new(tx_id: u64, begin_ts: u64) -> Transaction {
        Transaction {
            state: TransactionState::Active,
            tx_id,
            begin_ts,
            write_set: HashSet::new(),
            read_set: RefCell::new(HashSet::new()),
        }
    }

    fn insert_to_read_set(&self, id: u64) {
        let mut read_set = self.read_set.borrow_mut();
        read_set.insert(id);
    }

    fn insert_to_write_set(&mut self, id: u64) {
        self.write_set.insert(id);
    }
}

/// Transaction state.
#[derive(Debug, Clone)]
enum TransactionState {
    Active,
    Preparing,
    Committed,
    Aborted,
    Terminated,
}

/// A database with MVCC.
#[derive(Debug)]
pub struct Database<Clock: LogicalClock> {
    inner: Arc<Mutex<DatabaseInner<Clock>>>,
}

type TxID = u64;

/// Logical clock.
pub trait LogicalClock {
    fn get_timestamp(&self) -> u64;
}

/// A node-local clock backed by an atomic counter.
#[derive(Debug)]
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
}

impl Default for LocalClock {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct DatabaseInner<Clock: LogicalClock> {
    rows: RefCell<HashMap<u64, Vec<RowVersion>>>,
    txs: RefCell<HashMap<TxID, Transaction>>,
    tx_ids: AtomicU64,
    clock: Clock,
}

impl<Clock: LogicalClock> Database<Clock> {
    /// Creates a new database.
    pub fn new(clock: Clock) -> Self {
        let inner = DatabaseInner {
            rows: RefCell::new(HashMap::new()),
            txs: RefCell::new(HashMap::new()),
            tx_ids: AtomicU64::new(0),
            clock,
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    /// Inserts a new row into the database.
    ///
    /// This function inserts a new `row` into the database within the context
    /// of the transaction `tx_id`.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - the ID of the transaction in which to insert the new row.
    /// * `row` - the row object containing the values to be inserted.
    ///
    pub fn insert(&self, tx_id: TxID, row: Row) {
        let inner = self.inner.lock().unwrap();
        let mut txs = inner.txs.borrow_mut();
        let tx = txs.get_mut(&tx_id).unwrap();
        let id = row.id;
        let row_version = RowVersion {
            begin: TxTimestampOrID::TxID(tx.tx_id),
            end: None,
            row,
        };
        let mut rows = inner.rows.borrow_mut();
        rows.entry(id).or_insert_with(Vec::new).push(row_version);
        tx.insert_to_write_set(id);
    }

    /// Updates a row in the database with new values.
    ///
    /// This function updates an existing row in the database within the
    /// context of the transaction `tx_id`. The `row` argument identifies the
    /// row to be updated as `id` and contains the new values to be inserted.
    ///
    /// If the row identified by the `id` does not exist, this function does
    /// nothing and returns `false`. Otherwise, the function updates the row
    /// with the new values and returns `true`.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - the ID of the transaction in which to update the new row.
    /// * `row` - the row object containing the values to be updated.
    ///
    /// # Returns
    ///
    /// Returns `true` if the row was successfully updated, and `false` otherwise.
    pub fn update(&self, tx_id: TxID, row: Row) -> bool {
        if !self.delete(tx_id, row.id) {
            return false;
        }
        self.insert(tx_id, row);
        true
    }

    /// Deletes a row from the table with the given `id`.
    ///
    /// This function deletes an existing row `id` in the database within the
    /// context of the transaction `tx_id`.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - the ID of the transaction in which to delete the new row.
    /// * `id` - the ID of the row to delete.
    ///
    /// # Returns
    ///
    /// Returns `true` if the row was successfully deleted, and `false` otherwise.
    ///
    pub fn delete(&self, tx: TxID, id: u64) -> bool {
        let inner = self.inner.lock().unwrap();
        let mut rows = inner.rows.borrow_mut();
        let mut txs = inner.txs.borrow_mut();
        let row_versions = rows.get_mut(&id).unwrap();
        match row_versions.last_mut() {
            Some(v) => {
                let tx = txs.get(&tx).unwrap();
                if is_version_visible(&txs, tx, v) {
                    v.end = Some(TxTimestampOrID::TxID(tx.tx_id));
                } else {
                    return false;
                }
            }
            None => unreachable!("no versions for row {}", id),
        }
        let tx = txs.get_mut(&tx).unwrap();
        tx.insert_to_write_set(id);
        true
    }

    /// Retrieves a row from the table with the given `id`.
    ///
    /// This operation is performed within the scope of the transaction identified
    /// by `tx_id`.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The ID of the transaction to perform the read operation in.
    /// * `id` - The ID of the row to retrieve.
    ///
    /// # Returns
    ///
    /// Returns `Some(row)` with the row data if the row with the given `id` exists,
    /// and `None` otherwise.
    pub fn read(&self, tx_id: TxID, id: u64) -> Option<Row> {
        let inner = self.inner.lock().unwrap();
        let txs = inner.txs.borrow_mut();
        let tx = txs.get(&tx_id).unwrap();
        let rows = inner.rows.borrow();
        if let Some(row_versions) = rows.get(&id) {
            for rv in row_versions.iter().rev() {
                if is_version_visible(&txs, tx, rv) {
                    tx.insert_to_read_set(id);
                    return Some(rv.row.clone());
                }
            }
        }
        None
    }

    /// Begins a new transaction in the database.
    ///
    /// This function starts a new transaction in the database and returns a `TxID` value
    /// that you can use to perform operations within the transaction. All changes made within the
    /// transaction are isolated from other transactions until you commit the transaction.
    pub fn begin_tx(&self) -> TxID {
        let mut inner = self.inner.lock().unwrap();
        let tx_id = get_tx_id(&mut inner);
        let begin_ts = get_timestamp(&mut inner);
        let tx = Transaction::new(tx_id, begin_ts);
        let mut txs = inner.txs.borrow_mut();
        txs.insert(tx_id, tx);
        tx_id
    }

    /// Commits a transaction with the specified transaction ID.
    ///
    /// This function commits the changes made within the specified transaction and finalizes the
    /// transaction. Once a transaction has been committed, all changes made within the transaction
    /// are visible to other transactions that access the same data.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The ID of the transaction to commit.
    pub fn commit_tx(&self, tx_id: TxID) {
        let mut inner = self.inner.lock().unwrap();
        let end_ts = get_timestamp(&mut inner);
        let mut txs = inner.txs.borrow_mut();
        let mut tx = txs.get_mut(&tx_id).unwrap();
        let mut rows = inner.rows.borrow_mut();
        tx.state = TransactionState::Preparing;
        for id in &tx.write_set {
            if let Some(row_versions) = rows.get_mut(id) {
                for row_version in row_versions.iter_mut() {
                    if let TxTimestampOrID::TxID(id) = row_version.begin {
                        if id == tx_id {
                            row_version.begin = TxTimestampOrID::Timestamp(tx.begin_ts);
                        }
                    }
                    if let Some(TxTimestampOrID::TxID(id)) = row_version.end {
                        if id == tx_id {
                            row_version.end = Some(TxTimestampOrID::Timestamp(end_ts));
                        }
                    }
                }
            }
        }
        tx.state = TransactionState::Committed;
    }

    /// Rolls back a transaction with the specified ID.
    ///
    /// This function rolls back a transaction with the specified `tx_id` by
    /// discarding any changes made by the transaction.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The ID of the transaction to abort.
    pub fn rollback_tx(&self, tx_id: TxID) {
        let inner = self.inner.lock().unwrap();
        let mut txs = inner.txs.borrow_mut();
        let mut tx = txs.get_mut(&tx_id).unwrap();
        tx.state = TransactionState::Aborted;
        let mut rows = inner.rows.borrow_mut();
        for id in &tx.write_set {
            if let Some(row_versions) = rows.get_mut(id) {
                row_versions.retain(|rv| rv.begin != TxTimestampOrID::TxID(tx_id));
                if row_versions.is_empty() {
                    rows.remove(id);
                }
            }
        }
        tx.state = TransactionState::Terminated;
    }
}

fn is_version_visible(txs: &HashMap<TxID, Transaction>, tx: &Transaction, rv: &RowVersion) -> bool {
    is_begin_visible(txs, tx, rv) && is_end_visible(txs, tx, rv)
}

fn is_begin_visible(txs: &HashMap<TxID, Transaction>, tx: &Transaction, rv: &RowVersion) -> bool {
    match rv.begin {
        TxTimestampOrID::Timestamp(rv_begin_ts) => tx.begin_ts >= rv_begin_ts,
        TxTimestampOrID::TxID(rv_begin) => {
            let tb = txs.get(&rv_begin).unwrap();
            match tb.state {
                TransactionState::Active => tx.tx_id == tb.tx_id && rv.end.is_none(),
                TransactionState::Preparing => todo!(),
                TransactionState::Committed => todo!(),
                TransactionState::Aborted => todo!(),
                TransactionState::Terminated => todo!(),
            }
        }
    }
}

fn is_end_visible(txs: &HashMap<TxID, Transaction>, tx: &Transaction, rv: &RowVersion) -> bool {
    match rv.end {
        Some(TxTimestampOrID::Timestamp(rv_end_ts)) => tx.begin_ts < rv_end_ts,
        Some(TxTimestampOrID::TxID(rv_end)) => {
            let te = txs.get(&rv_end).unwrap();
            match te.state {
                TransactionState::Active => tx.tx_id == te.tx_id && rv.end.is_none(),
                TransactionState::Preparing => todo!(),
                TransactionState::Committed => todo!(),
                TransactionState::Aborted => todo!(),
                TransactionState::Terminated => todo!(),
            }
        }
        None => true,
    }
}

fn get_tx_id<Clock: LogicalClock>(inner: &mut DatabaseInner<Clock>) -> u64 {
    inner.tx_ids.fetch_add(1, Ordering::SeqCst)
}

fn get_timestamp<Clock: LogicalClock>(inner: &mut DatabaseInner<Clock>) -> u64 {
    inner.clock.get_timestamp()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit() {
        let clock = LocalClock::new();
        let db = Database::new(clock);
        let tx1 = db.begin_tx();
        let tx1_row = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, tx1_row.clone());
        let row = db.read(tx1, 1).unwrap();
        assert_eq!(tx1_row, row);
        let tx1_updated_row = Row {
            id: 1,
            data: "World".to_string(),
        };
        db.update(tx1, tx1_updated_row.clone());
        let row = db.read(tx1, 1).unwrap();
        assert_eq!(tx1_updated_row, row);
        db.commit_tx(tx1);

        let tx2 = db.begin_tx();
        let row = db.read(tx2, 1).unwrap();
        db.commit_tx(tx2);
        assert_eq!(tx1_updated_row, row);
    }

    #[test]
    fn test_rollback() {
        let clock = LocalClock::new();
        let db = Database::new(clock);
        let tx1 = db.begin_tx();
        let row1 = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1.clone(), row1.clone());
        let row2 = db.read(tx1.clone(), 1).unwrap();
        assert_eq!(row1, row2);
        let row3 = Row {
            id: 1,
            data: "World".to_string(),
        };
        db.update(tx1.clone(), row3.clone());
        let row4 = db.read(tx1.clone(), 1).unwrap();
        assert_eq!(row3, row4);
        db.rollback_tx(tx1);
        let tx2 = db.begin_tx();
        let row5 = db.read(tx2.clone(), 1);
        assert_eq!(row5, None);
    }

    #[test]
    fn test_dirty_write() {
        let clock = LocalClock::new();
        let db = Database::new(clock);

        // T1 inserts a row with ID 1, but does not commit.
        let tx1 = db.begin_tx();
        let tx1_row = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, tx1_row.clone());
        let row = db.read(tx1.clone(), 1).unwrap();
        assert_eq!(tx1_row, row);

        // T2 attempts to delete row with ID 1, but fails because T1 has not committed.
        let tx2 = db.begin_tx();
        let tx2_row = Row {
            id: 1,
            data: "World".to_string(),
        };
        assert_eq!(false, db.update(tx2, tx2_row.clone()));

        let row = db.read(tx1, 1).unwrap();
        assert_eq!(tx1_row, row);
    }

    #[test]
    fn test_dirty_read() {
        let clock = LocalClock::new();
        let db = Database::new(clock);

        // T1 inserts a row with ID 1, but does not commit.
        let tx1 = db.begin_tx();
        let row1 = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, row1.clone());

        // T2 attempts to read row with ID 1, but doesn't see one because T1 has not committed.
        let tx2 = db.begin_tx();
        let row2 = db.read(tx2, 1);
        assert_eq!(row2, None);
    }

    #[test]
    fn test_fuzzy_read() {
        let clock = LocalClock::new();
        let db = Database::new(clock);

        // T1 inserts a row with ID 1 and commits.
        let tx1 = db.begin_tx();
        let tx1_row = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, tx1_row.clone());
        let row = db.read(tx1.clone(), 1).unwrap();
        assert_eq!(tx1_row, row);
        db.commit_tx(tx1);

        // T2 reads the row with ID 1 within an active transaction.
        let tx2 = db.begin_tx();
        let row = db.read(tx2, 1).unwrap();
        assert_eq!(tx1_row, row);

        // T3 updates the row and commits.
        let tx3 = db.begin_tx();
        let tx3_row = Row {
            id: 1,
            data: "World".to_string(),
        };
        db.update(tx3, tx3_row.clone());
        db.commit_tx(tx3);

        // T2 still reads the same version of the row as before.
        let row = db.read(tx2, 1).unwrap();
        assert_eq!(tx1_row, row);
    }

    #[ignore]
    #[test]
    fn test_lost_update() {
        let clock = LocalClock::new();
        let db = Database::new(clock);

        // T1 inserts a row with ID 1 and commits.
        let tx1 = db.begin_tx();
        let tx1_row = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, tx1_row.clone());
        let row = db.read(tx1.clone(), 1).unwrap();
        assert_eq!(tx1_row, row);
        db.commit_tx(tx1);

        // T2 attempts to update row ID 1 within an active transaction.
        let tx2 = db.begin_tx();
        let tx2_row = Row {
            id: 1,
            data: "World".to_string(),
        };
        db.update(tx2, tx2_row.clone());

        // T3 also attempts to update row ID 1 within an active transaction.
        let tx3 = db.begin_tx();
        let tx3_row = Row {
            id: 1,
            data: "Hello, world!".to_string(),
        };
        db.update(tx3, tx3_row.clone());

        db.commit_tx(tx2);
        db.commit_tx(tx3); // TODO: this should fail
    }
}
