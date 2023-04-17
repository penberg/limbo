use crate::clock::LogicalClock;
use crate::errors::DatabaseError;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub type Result<T> = std::result::Result<T, DatabaseError>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Row {
    pub id: u64,
    pub data: String,
}

/// A row version.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RowVersion {
    begin: TxTimestampOrID,
    end: Option<TxTimestampOrID>,
    row: Row,
}

pub type TxID = u64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Mutation {
    tx_id: TxID,
    row_versions: Vec<RowVersion>,
}

impl Mutation {
    fn new(tx_id: TxID) -> Self {
        Self {
            tx_id,
            row_versions: Vec::new(),
        }
    }
}

/// A transaction timestamp or ID.
///
/// Versions either track a timestamp or a transaction ID, depending on the
/// phase of the transaction. During the active phase, new versions track the
/// transaction ID in the `begin` and `end` fields. After a transaction commits,
/// versions switch to tracking timestamps.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum TxTimestampOrID {
    Timestamp(u64),
    TxID(TxID),
}

/// Transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum TransactionState {
    Active,
    Preparing,
    Committed,
    Aborted,
    Terminated,
}

/// A database with MVCC.
#[derive(Debug)]
pub struct Database<
    Clock: LogicalClock,
    Storage: crate::persistent_storage::Storage,
    AsyncMutex: crate::sync::AsyncMutex<Inner = DatabaseInner<Clock, Storage>>,
> {
    inner: Arc<AsyncMutex>,
}

impl<
        Clock: LogicalClock,
        Storage: crate::persistent_storage::Storage,
        AsyncMutex: crate::sync::AsyncMutex<Inner = DatabaseInner<Clock, Storage>>,
    > Database<Clock, Storage, AsyncMutex>
{
    /// Creates a new database.
    pub fn new(clock: Clock, storage: Storage) -> Self {
        let inner = DatabaseInner {
            rows: RefCell::new(HashMap::new()),
            txs: RefCell::new(HashMap::new()),
            tx_ids: AtomicU64::new(0),
            clock,
            storage,
        };
        Self {
            inner: Arc::new(AsyncMutex::new(inner)),
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
    pub async fn insert(&self, tx_id: TxID, row: Row) -> Result<()> {
        let inner = self.inner.lock().await;
        inner.insert(tx_id, row).await
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
    pub async fn update(&self, tx_id: TxID, row: Row) -> Result<bool> {
        if !self.delete(tx_id, row.id).await? {
            return Ok(false);
        }
        self.insert(tx_id, row).await?;
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
    pub async fn delete(&self, tx_id: TxID, id: u64) -> Result<bool> {
        let inner = self.inner.lock().await;
        inner.delete(tx_id, id).await
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
    pub async fn read(&self, tx_id: TxID, id: u64) -> Result<Option<Row>> {
        let inner = self.inner.lock().await;
        inner.read(tx_id, id).await
    }

    /// Begins a new transaction in the database.
    ///
    /// This function starts a new transaction in the database and returns a `TxID` value
    /// that you can use to perform operations within the transaction. All changes made within the
    /// transaction are isolated from other transactions until you commit the transaction.
    pub async fn begin_tx(&self) -> TxID {
        let mut inner = self.inner.lock().await;
        inner.begin_tx().await
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
    pub async fn commit_tx(&self, tx_id: TxID) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.commit_tx(tx_id).await
    }

    /// Rolls back a transaction with the specified ID.
    ///
    /// This function rolls back a transaction with the specified `tx_id` by
    /// discarding any changes made by the transaction.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The ID of the transaction to abort.
    pub async fn rollback_tx(&self, tx_id: TxID) {
        let inner = self.inner.lock().await;
        inner.rollback_tx(tx_id).await;
    }

    #[cfg(test)]
    pub(crate) async fn scan_storage(&self) -> Result<Vec<Mutation>> {
        use futures::StreamExt;
        let inner = self.inner.lock().await;
        Ok(inner
            .storage
            .scan()
            .await?
            .collect::<Vec<Mutation>>()
            .await)
    }
}

#[derive(Debug)]
pub struct DatabaseInner<Clock: LogicalClock, Storage: crate::persistent_storage::Storage> {
    rows: RefCell<HashMap<u64, Vec<RowVersion>>>,
    txs: RefCell<HashMap<TxID, Transaction>>,
    tx_ids: AtomicU64,
    clock: Clock,
    storage: Storage,
}

impl<Clock: LogicalClock, Storage: crate::persistent_storage::Storage>
    DatabaseInner<Clock, Storage>
{
    async fn insert(&self, tx_id: TxID, row: Row) -> Result<()> {
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

    #[allow(clippy::await_holding_refcell_ref)]
    async fn delete(&self, tx_id: TxID, id: u64) -> Result<bool> {
        // NOTICE: They *are* dropped before an await point!!! But the await is conditional,
        //         so I think clippy is just confused.
        let mut txs = self.txs.borrow_mut();
        let mut rows = self.rows.borrow_mut();
        if let Some(row_versions) = rows.get_mut(&id) {
            for rv in row_versions.iter_mut().rev() {
                let tx = txs
                    .get(&tx_id)
                    .ok_or(DatabaseError::NoSuchTransactionID(tx_id))?;
                assert!(tx.state == TransactionState::Active);
                if is_write_write_conflict(&txs, tx, rv) {
                    drop(txs);
                    drop(rows);
                    self.rollback_tx(tx_id).await;
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

    async fn read(&self, tx_id: TxID, id: u64) -> Result<Option<Row>> {
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

    async fn begin_tx(&mut self) -> TxID {
        let tx_id = self.get_tx_id();
        let begin_ts = self.get_timestamp();
        let tx = Transaction::new(tx_id, begin_ts);
        tracing::trace!("BEGIN    {tx}");
        let mut txs = self.txs.borrow_mut();
        txs.insert(tx_id, tx);
        tx_id
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn commit_tx(&mut self, tx_id: TxID) -> Result<()> {
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
        tracing::trace!("PREPARE   {tx}");
        let mut mutation: Mutation = Mutation::new(tx_id);
        for id in &tx.write_set {
            if let Some(row_versions) = rows.get_mut(id) {
                for row_version in row_versions.iter_mut() {
                    if let TxTimestampOrID::TxID(id) = row_version.begin {
                        if id == tx_id {
                            row_version.begin = TxTimestampOrID::Timestamp(tx.begin_ts);
                            mutation.row_versions.push(row_version.clone()); // FIXME: optimize cloning out
                        }
                    }
                    if let Some(TxTimestampOrID::TxID(id)) = row_version.end {
                        if id == tx_id {
                            row_version.end = Some(TxTimestampOrID::Timestamp(end_ts));
                            mutation.row_versions.push(row_version.clone()); // FIXME: optimize cloning out
                        }
                    }
                }
            }
        }
        tx.state = TransactionState::Committed;
        tracing::trace!("COMMIT    {tx}");
        // We have now updated all the versions with a reference to the
        // transaction ID to a timestamp and can, therefore, remove the
        // transaction. Please note that when we move to lockless, the
        // invariant doesn't necessarily hold anymore because another thread
        // might have speculatively read a version that we want to remove.
        // But that's a problem for another day.
        txs.remove(&tx_id);
        drop(rows);
        drop(txs);
        if !mutation.row_versions.is_empty() {
            self.storage.store(mutation).await?;
        }
        Ok(())
    }

    async fn rollback_tx(&self, tx_id: TxID) {
        let mut txs = self.txs.borrow_mut();
        let mut tx = txs.get_mut(&tx_id).unwrap();
        assert!(tx.state == TransactionState::Active);
        tx.state = TransactionState::Aborted;
        tracing::trace!("ABORT     {tx}");
        let mut rows = self.rows.borrow_mut();
        for id in &tx.write_set {
            if let Some(row_versions) = rows.get_mut(id) {
                row_versions.retain(|rv| rv.begin != TxTimestampOrID::TxID(tx_id));
                if row_versions.is_empty() {
                    rows.remove(id);
                }
            }
        }
        tx.state = TransactionState::Terminated;
        tracing::trace!("TERMINATE {tx}");
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
                TransactionState::Active => tx.tx_id != te.tx_id,
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

    fn test_db() -> Database<
        LocalClock,
        crate::persistent_storage::Noop,
        tokio::sync::Mutex<DatabaseInner<LocalClock, crate::persistent_storage::Noop>>,
    > {
        let clock = LocalClock::new();
        let storage = crate::persistent_storage::Noop {};
        Database::<_, _, tokio::sync::Mutex<_>>::new(clock, storage)
    }

    #[traced_test]
    #[tokio::test]
    async fn test_insert_read() {
        let db = test_db();

        let tx1 = db.begin_tx().await;
        let tx1_row = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, tx1_row.clone()).await.unwrap();
        let row = db.read(tx1, 1).await.unwrap().unwrap();
        assert_eq!(tx1_row, row);
        db.commit_tx(tx1).await.unwrap();

        let tx2 = db.begin_tx().await;
        let row = db.read(tx2, 1).await.unwrap().unwrap();
        assert_eq!(tx1_row, row);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_read_nonexistent() {
        let db = test_db();
        let tx = db.begin_tx().await;
        let row = db.read(tx, 1).await;
        assert!(row.unwrap().is_none());
    }

    #[traced_test]
    #[tokio::test]
    async fn test_delete() {
        let db = test_db();

        let tx1 = db.begin_tx().await;
        let tx1_row = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, tx1_row.clone()).await.unwrap();
        let row = db.read(tx1, 1).await.unwrap().unwrap();
        assert_eq!(tx1_row, row);
        db.delete(tx1, 1).await.unwrap();
        let row = db.read(tx1, 1).await.unwrap();
        assert!(row.is_none());
        db.commit_tx(tx1).await.unwrap();

        let tx2 = db.begin_tx().await;
        let row = db.read(tx2, 1).await.unwrap();
        assert!(row.is_none());
    }

    #[traced_test]
    #[tokio::test]
    async fn test_delete_nonexistent() {
        let db = test_db();
        let tx = db.begin_tx().await;
        assert!(!db.delete(tx, 1).await.unwrap());
    }

    #[traced_test]
    #[tokio::test]
    async fn test_commit() {
        let db = test_db();
        let tx1 = db.begin_tx().await;
        let tx1_row = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, tx1_row.clone()).await.unwrap();
        let row = db.read(tx1, 1).await.unwrap().unwrap();
        assert_eq!(tx1_row, row);
        let tx1_updated_row = Row {
            id: 1,
            data: "World".to_string(),
        };
        db.update(tx1, tx1_updated_row.clone()).await.unwrap();
        let row = db.read(tx1, 1).await.unwrap().unwrap();
        assert_eq!(tx1_updated_row, row);
        db.commit_tx(tx1).await.unwrap();

        let tx2 = db.begin_tx().await;
        let row = db.read(tx2, 1).await.unwrap().unwrap();
        db.commit_tx(tx2).await.unwrap();
        assert_eq!(tx1_updated_row, row);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_rollback() {
        let db = test_db();
        let tx1 = db.begin_tx().await;
        let row1 = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, row1.clone()).await.unwrap();
        let row2 = db.read(tx1, 1).await.unwrap().unwrap();
        assert_eq!(row1, row2);
        let row3 = Row {
            id: 1,
            data: "World".to_string(),
        };
        db.update(tx1, row3.clone()).await.unwrap();
        let row4 = db.read(tx1, 1).await.unwrap().unwrap();
        assert_eq!(row3, row4);
        db.rollback_tx(tx1).await;
        let tx2 = db.begin_tx().await;
        let row5 = db.read(tx2, 1).await.unwrap();
        assert_eq!(row5, None);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_dirty_write() {
        let db = test_db();

        // T1 inserts a row with ID 1, but does not commit.
        let tx1 = db.begin_tx().await;
        let tx1_row = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, tx1_row.clone()).await.unwrap();
        let row = db.read(tx1, 1).await.unwrap().unwrap();
        assert_eq!(tx1_row, row);

        // T2 attempts to delete row with ID 1, but fails because T1 has not committed.
        let tx2 = db.begin_tx().await;
        let tx2_row = Row {
            id: 1,
            data: "World".to_string(),
        };
        assert!(!db.update(tx2, tx2_row).await.unwrap());

        let row = db.read(tx1, 1).await.unwrap().unwrap();
        assert_eq!(tx1_row, row);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_dirty_read() {
        let db = test_db();

        // T1 inserts a row with ID 1, but does not commit.
        let tx1 = db.begin_tx().await;
        let row1 = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, row1).await.unwrap();

        // T2 attempts to read row with ID 1, but doesn't see one because T1 has not committed.
        let tx2 = db.begin_tx().await;
        let row2 = db.read(tx2, 1).await.unwrap();
        assert_eq!(row2, None);
    }

    #[ignore]
    #[traced_test]
    #[tokio::test]
    async fn test_dirty_read_deleted() {
        let db = test_db();

        // T1 inserts a row with ID 1 and commits.
        let tx1 = db.begin_tx().await;
        let tx1_row = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, tx1_row.clone()).await.unwrap();
        db.commit_tx(tx1).await.unwrap();

        // T2 deletes row with ID 1, but does not commit.
        let tx2 = db.begin_tx().await;
        assert!(db.delete(tx2, 1).await.unwrap());

        // T3 reads row with ID 1, but doesn't see the delete because T2 hasn't committed.
        let tx3 = db.begin_tx().await;
        let row = db.read(tx3, 1).await.unwrap().unwrap();
        assert_eq!(tx1_row, row);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_fuzzy_read() {
        let db = test_db();

        // T1 inserts a row with ID 1 and commits.
        let tx1 = db.begin_tx().await;
        let tx1_row = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, tx1_row.clone()).await.unwrap();
        let row = db.read(tx1, 1).await.unwrap().unwrap();
        assert_eq!(tx1_row, row);
        db.commit_tx(tx1).await.unwrap();

        // T2 reads the row with ID 1 within an active transaction.
        let tx2 = db.begin_tx().await;
        let row = db.read(tx2, 1).await.unwrap().unwrap();
        assert_eq!(tx1_row, row);

        // T3 updates the row and commits.
        let tx3 = db.begin_tx().await;
        let tx3_row = Row {
            id: 1,
            data: "World".to_string(),
        };
        db.update(tx3, tx3_row).await.unwrap();
        db.commit_tx(tx3).await.unwrap();

        // T2 still reads the same version of the row as before.
        let row = db.read(tx2, 1).await.unwrap().unwrap();
        assert_eq!(tx1_row, row);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_lost_update() {
        let db = test_db();

        // T1 inserts a row with ID 1 and commits.
        let tx1 = db.begin_tx().await;
        let tx1_row = Row {
            id: 1,
            data: "Hello".to_string(),
        };
        db.insert(tx1, tx1_row.clone()).await.unwrap();
        let row = db.read(tx1, 1).await.unwrap().unwrap();
        assert_eq!(tx1_row, row);
        db.commit_tx(tx1).await.unwrap();

        // T2 attempts to update row ID 1 within an active transaction.
        let tx2 = db.begin_tx().await;
        let tx2_row = Row {
            id: 1,
            data: "World".to_string(),
        };
        assert!(db.update(tx2, tx2_row.clone()).await.unwrap());

        // T3 also attempts to update row ID 1 within an active transaction.
        let tx3 = db.begin_tx().await;
        let tx3_row = Row {
            id: 1,
            data: "Hello, world!".to_string(),
        };
        assert_eq!(
            Err(DatabaseError::WriteWriteConflict),
            db.update(tx3, tx3_row).await
        );

        db.commit_tx(tx2).await.unwrap();
        assert_eq!(Err(DatabaseError::TxTerminated), db.commit_tx(tx3).await);

        let tx4 = db.begin_tx().await;
        let row = db.read(tx4, 1).await.unwrap().unwrap();
        assert_eq!(tx2_row, row);
    }

    // Test for the visibility to check if a new transaction can see old committed values.
    // This test checks for the typo present in the paper, explained in https://github.com/penberg/mvcc-rs/issues/15
    #[traced_test]
    #[tokio::test]
    async fn test_committed_visibility() {
        let db = test_db();

        // let's add $10 to my account since I like money
        let tx1 = db.begin_tx().await;
        let tx1_row = Row {
            id: 1,
            data: "10".to_string(),
        };
        db.insert(tx1, tx1_row.clone()).await.unwrap();
        db.commit_tx(tx1).await.unwrap();

        // but I like more money, so let me try adding $10 more
        let tx2 = db.begin_tx().await;
        let tx2_row = Row {
            id: 1,
            data: "20".to_string(),
        };
        assert!(db.update(tx2, tx2_row.clone()).await.unwrap());
        let row = db.read(tx2, 1).await.unwrap().unwrap();
        assert_eq!(row, tx2_row);

        // can I check how much money I have?
        let tx3 = db.begin_tx().await;
        let row = db.read(tx3, 1).await.unwrap().unwrap();
        assert_eq!(tx1_row, row);
    }

    // Test to check if a older transaction can see (un)committed future rows
    #[traced_test]
    #[tokio::test]
    async fn test_future_row() {
        let db = test_db();

        let tx1 = db.begin_tx().await;

        let tx2 = db.begin_tx().await;
        let tx2_row = Row {
            id: 1,
            data: "10".to_string(),
        };
        db.insert(tx2, tx2_row.clone()).await.unwrap();

        // transaction in progress, so tx1 shouldn't be able to see the value
        let row = db.read(tx1, 1).await.unwrap();
        assert_eq!(row, None);

        // lets commit the transaction and check if tx1 can see it
        db.commit_tx(tx2).await.unwrap();
        let row = db.read(tx1, 1).await.unwrap();
        assert_eq!(row, None);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_storage1() {
        let clock = LocalClock::new();
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mvcc-rs-storage-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        let storage = crate::persistent_storage::JsonOnDisk { path };
        let db: Database<_, _, tokio::sync::Mutex<_>> = Database::new(clock, storage);

        let tx1 = db.begin_tx().await;
        let tx2 = db.begin_tx().await;
        let tx3 = db.begin_tx().await;

        db.insert(
            tx3,
            Row {
                id: 1,
                data: "testme".to_string(),
            },
        )
        .await
        .unwrap();

        db.commit_tx(tx1).await.unwrap();
        db.rollback_tx(tx2).await;
        db.commit_tx(tx3).await.unwrap();

        let tx4 = db.begin_tx().await;
        db.insert(
            tx4,
            Row {
                id: 2,
                data: "testme2".to_string(),
            },
        )
        .await
        .unwrap();
        db.insert(
            tx4,
            Row {
                id: 3,
                data: "testme3".to_string(),
            },
        )
        .await
        .unwrap();

        let mutation = db
            .scan_storage()
            .await
            .unwrap();
        println!("{:?}", mutation);

        db.commit_tx(tx4).await.unwrap();

        let mutation = db
            .scan_storage()
            .await
            .unwrap();
        println!("{:?}", mutation);

    }
}
