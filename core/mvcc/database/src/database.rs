use crate::clock::LogicalClock;
use crate::errors::DatabaseError;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

type Result<T> = std::result::Result<T, DatabaseError>;

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

type TxID = u64;

/// A transaction timestamp or ID.
///
/// Versions either track a timestamp or a transaction ID, depending on the
/// phase of the transaction. During the active phase, new versions track the
/// transaction ID in the `begin` and `end` fields. After a transaction commits,
/// versions switch to tracking timestamps.
#[derive(Clone, Debug, PartialEq)]
enum TxTimestampOrID {
    Timestamp(u64),
    TxID(TxID),
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

impl std::fmt::Display for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self.read_set.try_borrow() {
            Ok(read_set) => write!(
                f,
                "{{ id: {}, begin_ts: {}, write_set: {:?}, read_set: {:?} }}",
                self.tx_id, self.begin_ts, self.write_set, read_set
            ),
            Err(_) => write!(
                f,
                "{{ id: {}, begin_ts: {}, write_set: {:?}, read_set: <borrowed> }}",
                self.tx_id, self.begin_ts, self.write_set
            ),
        }
    }
}

/// Transaction state.
#[derive(Debug, Clone, PartialEq)]
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
    pub fn insert(&self, tx_id: TxID, row: Row) -> Result<()> {
        let inner = self.inner.lock().unwrap();
        inner.insert(tx_id, row)
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
    pub fn update(&self, tx_id: TxID, row: Row) -> Result<bool> {
        if !self.delete(tx_id, row.id)? {
            return Ok(false);
        }
        self.insert(tx_id, row)?;
        Ok(true)
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
    pub fn delete(&self, tx_id: TxID, id: u64) -> Result<bool> {
        let inner = self.inner.lock().unwrap();
        inner.delete(tx_id, id)
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
    pub fn read(&self, tx_id: TxID, id: u64) -> Result<Option<Row>> {
        let inner = self.inner.lock().unwrap();
        inner.read(tx_id, id)
    }

    /// Begins a new transaction in the database.
    ///
    /// This function starts a new transaction in the database and returns a `TxID` value
    /// that you can use to perform operations within the transaction. All changes made within the
    /// transaction are isolated from other transactions until you commit the transaction.
    pub fn begin_tx(&self) -> TxID {
        let mut inner = self.inner.lock().unwrap();
        inner.begin_tx()
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
    pub fn commit_tx(&self, tx_id: TxID) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.commit_tx(tx_id)
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
        inner.rollback_tx(tx_id);
    }
}

#[derive(Debug)]
pub struct DatabaseInner<Clock: LogicalClock> {
    rows: RefCell<HashMap<u64, Vec<RowVersion>>>,
    txs: RefCell<HashMap<TxID, Transaction>>,
    tx_ids: AtomicU64,
    clock: Clock,
}

impl<Clock: LogicalClock> DatabaseInner<Clock> {
    fn insert(&self, tx_id: TxID, row: Row) -> Result<()> {
        let mut txs = self.txs.borrow_mut();
        let tx = txs
            .get_mut(&tx_id)
            .ok_or(DatabaseError::NoSuchTransactionID(tx_id))?;
        assert!(tx.state == TransactionState::Active);
        let id = row.id;
        let row_version = RowVersion {
            begin: TxTimestampOrID::TxID(tx.tx_id),
            end: None,
            row,
        };
        let mut rows = self.rows.borrow_mut();
        rows.entry(id).or_insert_with(Vec::new).push(row_version);
        tx.insert_to_write_set(id);
        Ok(())
    }

    fn delete(&self, tx_id: TxID, id: u64) -> Result<bool> {
        let mut rows = self.rows.borrow_mut();
        let mut txs = self.txs.borrow_mut();
        if let Some(row_versions) = rows.get_mut(&id) {
            for rv in row_versions.iter_mut().rev() {
                let tx = txs
                    .get(&tx_id)
                    .ok_or(DatabaseError::NoSuchTransactionID(tx_id))?;
                assert!(tx.state == TransactionState::Active);
                if is_write_write_conflict(&txs, tx, rv) {
                    drop(txs);
                    drop(rows);
                    self.rollback_tx(tx_id);
                    return Err(DatabaseError::WriteWriteConflict);
                }
                if is_version_visible(&txs, tx, rv) {
                    rv.end = Some(TxTimestampOrID::TxID(tx.tx_id));
                    let tx = txs
                        .get_mut(&tx_id)
                        .ok_or(DatabaseError::NoSuchTransactionID(tx_id))?;
                    tx.insert_to_write_set(id);
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn read(&self, tx_id: TxID, id: u64) -> Result<Option<Row>> {
        let txs = self.txs.borrow_mut();
        let tx = txs.get(&tx_id).unwrap();
        assert!(tx.state == TransactionState::Active);
        let rows = self.rows.borrow();
        if let Some(row_versions) = rows.get(&id) {
            for rv in row_versions.iter().rev() {
                if is_version_visible(&txs, tx, rv) {
                    tx.insert_to_read_set(id);
                    return Ok(Some(rv.row.clone()));
                }
            }
        }
        Ok(None)
    }

    fn begin_tx(&mut self) -> TxID {
        let tx_id = self.get_tx_id();
        let begin_ts = self.get_timestamp();
        let tx = Transaction::new(tx_id, begin_ts);
        let mut txs = self.txs.borrow_mut();
        tracing::trace!("BEGIN    {tx}");
        txs.insert(tx_id, tx);
        tx_id
    }

    fn commit_tx(&mut self, tx_id: TxID) -> Result<()> {
        let end_ts = self.get_timestamp();
        let mut txs = self.txs.borrow_mut();
        let mut tx = txs.get_mut(&tx_id).unwrap();
        match tx.state {
            TransactionState::Terminated => return Err(DatabaseError::TxTerminated),
            _ => {
                assert!(tx.state == TransactionState::Active);
            }
        }
        let mut rows = self.rows.borrow_mut();
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
        tracing::trace!("COMMIT   {tx}");
        Ok(())
    }

    fn rollback_tx(&self, tx_id: TxID) {
        let mut txs = self.txs.borrow_mut();
        let mut tx = txs.get_mut(&tx_id).unwrap();
        assert!(tx.state == TransactionState::Active);
        tx.state = TransactionState::Aborted;
        let mut rows = self.rows.borrow_mut();
        for id in &tx.write_set {
            if let Some(row_versions) = rows.get_mut(id) {
                row_versions.retain(|rv| rv.begin != TxTimestampOrID::TxID(tx_id));
                if row_versions.is_empty() {
                    rows.remove(id);
                }
            }
        }
        tracing::trace!("ROLLBACK {tx}");
        tx.state = TransactionState::Terminated;
    }

    fn get_tx_id(&mut self) -> u64 {
        self.tx_ids.fetch_add(1, Ordering::SeqCst)
    }

    fn get_timestamp(&mut self) -> u64 {
        self.clock.get_timestamp()
    }
}

/// A write-write conflict happens when transaction T_m attempts to update a
/// row version that is currently being updated by an active transaction T_n.
fn is_write_write_conflict(
    txs: &HashMap<TxID, Transaction>,
    tx: &Transaction,
    rv: &RowVersion,
) -> bool {
    match rv.end {
        Some(TxTimestampOrID::TxID(rv_end)) => {
            let te = txs.get(&rv_end).unwrap();
            match te.state {
                TransactionState::Active => tx.tx_id != te.tx_id,
                TransactionState::Preparing => todo!(),
                TransactionState::Committed => todo!(),
                TransactionState::Aborted => todo!(),
                TransactionState::Terminated => todo!(),
            }
        }
        Some(TxTimestampOrID::Timestamp(_)) => false,
        None => false,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::LocalClock;
    use tracing_test::traced_test;

    #[traced_test]
    #[test]
    fn test_insert_read() {
        let clock = LocalClock::default();
        let db = Database::new(clock);

        let tx1 = db.begin_tx();
        let tx1_row = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, tx1_row.clone()).unwrap();
        let row = db.read(tx1, 1).unwrap().unwrap();
        assert_eq!(tx1_row, row);
        db.commit_tx(tx1).unwrap();

        let tx2 = db.begin_tx();
        let row = db.read(tx2, 1).unwrap().unwrap();
        assert_eq!(tx1_row, row);
    }

    #[traced_test]
    #[test]
    fn test_read_nonexistent() {
        let clock = LocalClock::default();
        let db = Database::new(clock);
        let tx = db.begin_tx();
        let row = db.read(tx, 1);
        assert!(row.unwrap().is_none());
    }

    #[traced_test]
    #[test]
    fn test_delete() {
        let clock = LocalClock::default();
        let db = Database::new(clock);

        let tx1 = db.begin_tx();
        let tx1_row = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, tx1_row.clone()).unwrap();
        let row = db.read(tx1, 1).unwrap().unwrap();
        assert_eq!(tx1_row, row);
        db.delete(tx1, 1).unwrap();
        let row = db.read(tx1, 1).unwrap();
        assert!(row.is_none());
        db.commit_tx(tx1).unwrap();

        let tx2 = db.begin_tx();
        let row = db.read(tx2, 1).unwrap();
        assert!(row.is_none());
    }

    #[traced_test]
    #[test]
    fn test_delete_nonexistent() {
        let clock = LocalClock::default();
        let db = Database::new(clock);
        let tx = db.begin_tx();
        assert!(!db.delete(tx, 1).unwrap());
    }

    #[traced_test]
    #[test]
    fn test_commit() {
        let clock = LocalClock::default();
        let db = Database::new(clock);
        let tx1 = db.begin_tx();
        let tx1_row = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, tx1_row.clone()).unwrap();
        let row = db.read(tx1, 1).unwrap().unwrap();
        assert_eq!(tx1_row, row);
        let tx1_updated_row = Row {
            id: 1,
            data: "World".to_string(),
        };
        db.update(tx1, tx1_updated_row.clone()).unwrap();
        let row = db.read(tx1, 1).unwrap().unwrap();
        assert_eq!(tx1_updated_row, row);
        db.commit_tx(tx1).unwrap();

        let tx2 = db.begin_tx();
        let row = db.read(tx2, 1).unwrap().unwrap();
        db.commit_tx(tx2).unwrap();
        assert_eq!(tx1_updated_row, row);
    }

    #[traced_test]
    #[test]
    fn test_rollback() {
        let clock = LocalClock::default();
        let db = Database::new(clock);
        let tx1 = db.begin_tx();
        let row1 = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, row1.clone()).unwrap();
        let row2 = db.read(tx1, 1).unwrap().unwrap();
        assert_eq!(row1, row2);
        let row3 = Row {
            id: 1,
            data: "World".to_string(),
        };
        db.update(tx1, row3.clone()).unwrap();
        let row4 = db.read(tx1, 1).unwrap().unwrap();
        assert_eq!(row3, row4);
        db.rollback_tx(tx1);
        let tx2 = db.begin_tx();
        let row5 = db.read(tx2, 1).unwrap();
        assert_eq!(row5, None);
    }

    #[traced_test]
    #[test]
    fn test_dirty_write() {
        let clock = LocalClock::default();
        let db = Database::new(clock);

        // T1 inserts a row with ID 1, but does not commit.
        let tx1 = db.begin_tx();
        let tx1_row = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, tx1_row.clone()).unwrap();
        let row = db.read(tx1, 1).unwrap().unwrap();
        assert_eq!(tx1_row, row);

        // T2 attempts to delete row with ID 1, but fails because T1 has not committed.
        let tx2 = db.begin_tx();
        let tx2_row = Row {
            id: 1,
            data: "World".to_string(),
        };
        assert!(!db.update(tx2, tx2_row).unwrap());

        let row = db.read(tx1, 1).unwrap().unwrap();
        assert_eq!(tx1_row, row);
    }

    #[traced_test]
    #[test]
    fn test_dirty_read() {
        let clock = LocalClock::default();
        let db = Database::new(clock);

        // T1 inserts a row with ID 1, but does not commit.
        let tx1 = db.begin_tx();
        let row1 = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, row1).unwrap();

        // T2 attempts to read row with ID 1, but doesn't see one because T1 has not committed.
        let tx2 = db.begin_tx();
        let row2 = db.read(tx2, 1).unwrap();
        assert_eq!(row2, None);
    }

    #[ignore]
    #[traced_test]
    #[test]
    fn test_dirty_read_deleted() {
        let clock = LocalClock::default();
        let db = Database::new(clock);

        // T1 inserts a row with ID 1 and commits.
        let tx1 = db.begin_tx();
        let tx1_row = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, tx1_row.clone()).unwrap();
        db.commit_tx(tx1).unwrap();

        // T2 deletes row with ID 1, but does not commit.
        let tx2 = db.begin_tx();
        assert!(db.delete(tx2, 1).unwrap());

        // T3 reads row with ID 1, but doesn't see the delete because T2 hasn't committed.
        let tx3 = db.begin_tx();
        let row = db.read(tx3, 1).unwrap().unwrap();
        assert_eq!(tx1_row, row);
    }

    #[traced_test]
    #[test]
    fn test_fuzzy_read() {
        let clock = LocalClock::default();
        let db = Database::new(clock);

        // T1 inserts a row with ID 1 and commits.
        let tx1 = db.begin_tx();
        let tx1_row = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, tx1_row.clone()).unwrap();
        let row = db.read(tx1, 1).unwrap().unwrap();
        assert_eq!(tx1_row, row);
        db.commit_tx(tx1).unwrap();

        // T2 reads the row with ID 1 within an active transaction.
        let tx2 = db.begin_tx();
        let row = db.read(tx2, 1).unwrap().unwrap();
        assert_eq!(tx1_row, row);

        // T3 updates the row and commits.
        let tx3 = db.begin_tx();
        let tx3_row = Row {
            id: 1,
            data: "World".to_string(),
        };
        db.update(tx3, tx3_row).unwrap();
        db.commit_tx(tx3).unwrap();

        // T2 still reads the same version of the row as before.
        let row = db.read(tx2, 1).unwrap().unwrap();
        assert_eq!(tx1_row, row);
    }

    #[traced_test]
    #[test]
    fn test_lost_update() {
        let clock = LocalClock::default();
        let db = Database::new(clock);

        // T1 inserts a row with ID 1 and commits.
        let tx1 = db.begin_tx();
        let tx1_row = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, tx1_row.clone()).unwrap();
        let row = db.read(tx1, 1).unwrap().unwrap();
        assert_eq!(tx1_row, row);
        db.commit_tx(tx1).unwrap();

        // T2 attempts to update row ID 1 within an active transaction.
        let tx2 = db.begin_tx();
        let tx2_row = Row {
            id: 1,
            data: "World".to_string(),
        };
        assert!(db.update(tx2, tx2_row.clone()).unwrap());

        // T3 also attempts to update row ID 1 within an active transaction.
        let tx3 = db.begin_tx();
        let tx3_row = Row {
            id: 1,
            data: "Hello, world!".to_string(),
        };
        assert_eq!(
            Err(DatabaseError::WriteWriteConflict),
            db.update(tx3, tx3_row)
        );

        db.commit_tx(tx2).unwrap();
        assert_eq!(Err(DatabaseError::TxTerminated), db.commit_tx(tx3));

        let tx4 = db.begin_tx();
        let row = db.read(tx4, 1).unwrap().unwrap();
        assert_eq!(tx2_row, row);
    }
}
