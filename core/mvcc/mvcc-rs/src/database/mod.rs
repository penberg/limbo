use crate::clock::LogicalClock;
use crate::errors::DatabaseError;
use crate::persistent_storage::Storage;
use crossbeam_skiplist::{SkipMap, SkipSet};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

pub type Result<T> = std::result::Result<T, DatabaseError>;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct RowID {
    pub table_id: u64,
    pub row_id: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]

pub struct Row {
    pub id: RowID,
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

/// A log record contains all the versions inserted and deleted by a transaction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogRecord {
    pub(crate) tx_timestamp: TxID,
    row_versions: Vec<RowVersion>,
}

impl LogRecord {
    fn new(tx_timestamp: TxID) -> Self {
        Self {
            tx_timestamp,
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
#[derive(Debug, Serialize, Deserialize)]
pub struct Transaction {
    /// The state of the transaction.
    state: TransactionState,
    /// The transaction ID.
    tx_id: u64,
    /// The transaction begin timestamp.
    begin_ts: u64,
    /// The transaction write set.
    #[serde(with = "skipset_rowid")]
    write_set: SkipSet<RowID>,
    /// The transaction read set.
    #[serde(with = "skipset_rowid")]
    read_set: SkipSet<RowID>,
}

mod skipset_rowid {
    use super::*;
    use serde::{de, ser, ser::SerializeSeq};

    struct SkipSetDeserializer;

    impl<'de> serde::de::Visitor<'de> for SkipSetDeserializer {
        type Value = SkipSet<RowID>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("SkipSet<RowID> key value sequence.")
        }

        fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let new_skipset = SkipSet::new();
            while let Some(elem) = seq.next_element()? {
                new_skipset.insert(elem);
            }

            Ok(new_skipset)
        }
    }

    pub fn serialize<S: ser::Serializer>(
        value: &SkipSet<RowID>,
        ser: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut set = ser.serialize_seq(Some(value.len()))?;
        for v in value {
            set.serialize_element(v.value())?;
        }
        set.end()
    }

    pub fn deserialize<'de, D: de::Deserializer<'de>>(
        de: D,
    ) -> std::result::Result<SkipSet<RowID>, D::Error> {
        de.deserialize_seq(SkipSetDeserializer)
    }
}

impl Transaction {
    fn new(tx_id: u64, begin_ts: u64) -> Transaction {
        Transaction {
            state: TransactionState::Active,
            tx_id,
            begin_ts,
            write_set: SkipSet::new(),
            read_set: SkipSet::new(),
        }
    }

    fn insert_to_read_set(&self, id: RowID) {
        self.read_set.insert(id);
    }

    fn insert_to_write_set(&mut self, id: RowID) {
        self.write_set.insert(id);
    }
}

impl std::fmt::Display for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "{{ id: {}, begin_ts: {}, write_set: {:?}, read_set: {:?}",
            self.tx_id,
            self.begin_ts,
            // FIXME: I'm sorry, we obviously shouldn't be cloning here.
            self.write_set
                .iter()
                .map(|v| *v.value())
                .collect::<Vec<RowID>>(),
            self.read_set
                .iter()
                .map(|v| *v.value())
                .collect::<Vec<RowID>>()
        )
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
pub struct Database<Clock: LogicalClock> {
    inner: Arc<Mutex<DatabaseInner<Clock>>>,
}

impl<Clock: LogicalClock> Database<Clock> {
    /// Creates a new database.
    pub fn new(clock: Clock, storage: Storage) -> Self {
        let inner = DatabaseInner {
            rows: SkipMap::new(),
            txs: SkipMap::new(),
            tx_timestamps: RefCell::new(BTreeMap::new()),
            tx_ids: AtomicU64::new(1), // let's reserve transaction 0 for special purposes
            clock,
            storage,
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
        let inner = self.inner.lock();
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
    pub fn delete(&self, tx_id: TxID, id: RowID) -> Result<bool> {
        let inner = self.inner.lock();
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
    pub fn read(&self, tx_id: TxID, id: RowID) -> Result<Option<Row>> {
        let inner = self.inner.lock();
        inner.read(tx_id, id)
    }

    pub fn scan_row_ids(&self) -> Result<Vec<RowID>> {
        let inner = self.inner.lock();
        inner.scan_row_ids()
    }

    pub fn scan_row_ids_for_table(&self, table_id: u64) -> Result<Vec<RowID>> {
        let inner = self.inner.lock();
        inner.scan_row_ids_for_table(table_id)
    }

    /// Begins a new transaction in the database.
    ///
    /// This function starts a new transaction in the database and returns a `TxID` value
    /// that you can use to perform operations within the transaction. All changes made within the
    /// transaction are isolated from other transactions until you commit the transaction.
    pub fn begin_tx(&self) -> TxID {
        let mut inner = self.inner.lock();
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
        let mut inner = self.inner.lock();
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
        let inner = self.inner.lock();
        inner.rollback_tx(tx_id);
    }

    /// Drops all unused row versions from the database.
    ///
    /// A version is considered unused if it is not visible to any active transaction
    /// and it is not the most recent version of the row.
    pub fn drop_unused_row_versions(&self) {
        let inner = self.inner.lock();
        inner.drop_unused_row_versions();
    }

    pub fn recover(&self) -> Result<()> {
        let inner = self.inner.lock();
        inner.recover()
    }
}

#[derive(Debug)]
pub struct DatabaseInner<Clock: LogicalClock> {
    rows: SkipMap<RowID, RwLock<Vec<RowVersion>>>,
    txs: SkipMap<TxID, RwLock<Transaction>>,
    tx_timestamps: RefCell<BTreeMap<u64, usize>>,
    tx_ids: AtomicU64,
    clock: Clock,
    storage: Storage,
}

impl<Clock: LogicalClock> DatabaseInner<Clock> {
    fn insert(&self, tx_id: TxID, row: Row) -> Result<()> {
        let tx = self
            .txs
            .get(&tx_id)
            .ok_or(DatabaseError::NoSuchTransactionID(tx_id))?;
        let mut tx = tx.value().write().unwrap();
        assert!(tx.state == TransactionState::Active);
        let id = row.id;
        let row_version = RowVersion {
            begin: TxTimestampOrID::TxID(tx.tx_id),
            end: None,
            row,
        };
        let versions = self.rows.get_or_insert_with(id, || RwLock::new(Vec::new()));
        let mut versions = versions.value().write().unwrap();
        versions.push(row_version);
        tx.insert_to_write_set(id);
        Ok(())
    }

    fn delete(&self, tx_id: TxID, id: RowID) -> Result<bool> {
        let row_versions_opt = self.rows.get(&id);
        if let Some(ref row_versions) = row_versions_opt {
            let mut row_versions = row_versions.value().write().unwrap();
            for rv in row_versions.iter_mut().rev() {
                let tx = self
                    .txs
                    .get(&tx_id)
                    .ok_or(DatabaseError::NoSuchTransactionID(tx_id))?;
                let tx = tx.value().read().unwrap();
                assert!(tx.state == TransactionState::Active);
                if is_write_write_conflict(&self.txs, &tx, rv) {
                    drop(row_versions);
                    drop(row_versions_opt);
                    drop(tx);
                    self.rollback_tx(tx_id);
                    return Err(DatabaseError::WriteWriteConflict);
                }
                if is_version_visible(&self.txs, &tx, rv) {
                    rv.end = Some(TxTimestampOrID::TxID(tx.tx_id));
                    drop(tx); // FIXME: maybe just grab the write lock above? Do we ever expect conflicts?
                    let tx = self
                        .txs
                        .get(&tx_id)
                        .ok_or(DatabaseError::NoSuchTransactionID(tx_id))?;
                    let mut tx = tx.value().write().unwrap();
                    tx.insert_to_write_set(id);
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn read(&self, tx_id: TxID, id: RowID) -> Result<Option<Row>> {
        let tx = self.txs.get(&tx_id).unwrap();
        let tx = tx.value().read().unwrap();
        assert!(tx.state == TransactionState::Active);
        if let Some(row_versions) = self.rows.get(&id) {
            let row_versions = row_versions.value().read().unwrap();
            for rv in row_versions.iter().rev() {
                if is_version_visible(&self.txs, &tx, rv) {
                    tx.insert_to_read_set(id);
                    return Ok(Some(rv.row.clone()));
                }
            }
        }
        Ok(None)
    }

    fn scan_row_ids(&self) -> Result<Vec<RowID>> {
        let keys = self.rows.iter().map(|entry| *entry.key());
        Ok(keys.collect())
    }

    fn scan_row_ids_for_table(&self, table_id: u64) -> Result<Vec<RowID>> {
        Ok(self
            .rows
            .range(
                RowID {
                    table_id,
                    row_id: 0,
                }..RowID {
                    table_id,
                    row_id: u64::MAX,
                },
            )
            .map(|entry| *entry.key())
            .collect())
    }

    fn begin_tx(&mut self) -> TxID {
        let tx_id = self.get_tx_id();
        let begin_ts = self.get_timestamp();
        let tx = Transaction::new(tx_id, begin_ts);
        tracing::trace!("BEGIN    {tx}");
        let mut tx_timestamps = self.tx_timestamps.borrow_mut();
        self.txs.insert(tx_id, RwLock::new(tx));
        *tx_timestamps.entry(begin_ts).or_insert(0) += 1;
        tx_id
    }

    fn commit_tx(&mut self, tx_id: TxID) -> Result<()> {
        let end_ts = self.get_timestamp();
        let tx = self.txs.get(&tx_id).unwrap();
        let mut tx = tx.value().write().unwrap();
        match tx.state {
            TransactionState::Terminated => return Err(DatabaseError::TxTerminated),
            _ => {
                assert!(tx.state == TransactionState::Active);
            }
        }
        tx.state = TransactionState::Preparing;
        tracing::trace!("PREPARE   {tx}");
        let mut log_record: LogRecord = LogRecord::new(end_ts);
        for id in &tx.write_set {
            let id = id.value();
            if let Some(row_versions) = self.rows.get(id) {
                let mut row_versions = row_versions.value().write().unwrap();
                for row_version in row_versions.iter_mut() {
                    if let TxTimestampOrID::TxID(id) = row_version.begin {
                        if id == tx_id {
                            row_version.begin = TxTimestampOrID::Timestamp(tx.begin_ts);
                            log_record.row_versions.push(row_version.clone()); // FIXME: optimize cloning out
                        }
                    }
                    if let Some(TxTimestampOrID::TxID(id)) = row_version.end {
                        if id == tx_id {
                            row_version.end = Some(TxTimestampOrID::Timestamp(end_ts));
                            log_record.row_versions.push(row_version.clone()); // FIXME: optimize cloning out
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
        let mut tx_timestamps = self.tx_timestamps.borrow_mut();
        if let Some(timestamp_entry) = tx_timestamps.get_mut(&tx.begin_ts) {
            *timestamp_entry -= 1;
            if timestamp_entry == &0 {
                tx_timestamps.remove(&tx.begin_ts);
            }
        }
        self.txs.remove(&tx_id);
        if !log_record.row_versions.is_empty() {
            self.storage.log_tx(log_record)?;
        }
        Ok(())
    }

    fn rollback_tx(&self, tx_id: TxID) {
        let tx = self.txs.get(&tx_id).unwrap();
        let mut tx = tx.value().write().unwrap();
        assert!(tx.state == TransactionState::Active);
        tx.state = TransactionState::Aborted;
        tracing::trace!("ABORT     {tx}");
        for id in &tx.write_set {
            let id = id.value();
            if let Some(row_versions) = self.rows.get(id) {
                let mut row_versions = row_versions.value().write().unwrap();
                row_versions.retain(|rv| rv.begin != TxTimestampOrID::TxID(tx_id));
                if row_versions.is_empty() {
                    self.rows.remove(id);
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

    /// Drops all rows that are not visible to any transaction.
    /// The logic is as follows. If a row version has an end marker
    /// which denotes a transaction that is not active, then we can
    /// drop the row version -- it is not visible to any transaction.
    /// If a row version has an end marker that denotes a timestamp T_END,
    /// then we can drop the row version only if all active transactions
    /// have a begin timestamp that is greater than timestamp T_END.
    /// FIXME: this function is a full scan over all rows and row versions.
    /// We can do better by keeping an index of row versions ordered
    /// by their end timestamps.
    fn drop_unused_row_versions(&self) {
        let tx_timestamps = self.tx_timestamps.borrow();
        let mut to_remove = Vec::new();
        for entry in self.rows.iter() {
            let mut row_versions = entry.value().write().unwrap();
            row_versions.retain(|rv| {
                let should_stay = match rv.end {
                    Some(TxTimestampOrID::Timestamp(version_end_ts)) => {
                        match tx_timestamps.first_key_value() {
                            // a transaction started before this row version ended,
                            // ergo row version is needed
                            Some((begin_ts, _)) => version_end_ts >= *begin_ts,
                            // no transaction => row version is not needed
                            None => false,
                        }
                    }
                    // Let's skip potentially complex logic if the transaction is still
                    // active/tracked. We will drop the row version when the transaction
                    // gets garbage-collected itself, it will always happen eventually.
                    Some(TxTimestampOrID::TxID(tx_id)) => !self.txs.contains_key(&tx_id),
                    // this row version is current, ergo visible
                    None => true,
                };
                if !should_stay {
                    tracing::debug!(
                        "Dropping row version {:?} {:?}-{:?}",
                        entry.key(),
                        rv.begin,
                        rv.end
                    );
                }
                should_stay
            });
            if row_versions.is_empty() {
                to_remove.push(*entry.key());
            }
        }
        for id in to_remove {
            self.rows.remove(&id);
        }
    }

    pub fn recover(&self) -> Result<()> {
        let tx_log = self.storage.read_tx_log()?;
        for record in tx_log {
            tracing::debug!("RECOVERING {:?}", record);
            for version in record.row_versions {
                let row_versions = self
                    .rows
                    .get_or_insert_with(version.row.id, || RwLock::new(Vec::new()));
                let mut row_versions = row_versions.value().write().unwrap();
                row_versions.push(version);
            }
            self.clock.reset(record.tx_timestamp);
        }
        Ok(())
    }
}

/// A write-write conflict happens when transaction T_m attempts to update a
/// row version that is currently being updated by an active transaction T_n.
fn is_write_write_conflict(
    txs: &SkipMap<TxID, RwLock<Transaction>>,
    tx: &Transaction,
    rv: &RowVersion,
) -> bool {
    match rv.end {
        Some(TxTimestampOrID::TxID(rv_end)) => {
            let te = txs.get(&rv_end).unwrap();
            let te = te.value().read().unwrap();
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

fn is_version_visible(
    txs: &SkipMap<TxID, RwLock<Transaction>>,
    tx: &Transaction,
    rv: &RowVersion,
) -> bool {
    is_begin_visible(txs, tx, rv) && is_end_visible(txs, tx, rv)
}

fn is_begin_visible(
    txs: &SkipMap<TxID, RwLock<Transaction>>,
    tx: &Transaction,
    rv: &RowVersion,
) -> bool {
    match rv.begin {
        TxTimestampOrID::Timestamp(rv_begin_ts) => tx.begin_ts >= rv_begin_ts,
        TxTimestampOrID::TxID(rv_begin) => {
            let tb = txs.get(&rv_begin).unwrap();
            let tb = tb.value().read().unwrap();
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

fn is_end_visible(
    txs: &SkipMap<TxID, RwLock<Transaction>>,
    tx: &Transaction,
    rv: &RowVersion,
) -> bool {
    match rv.end {
        Some(TxTimestampOrID::Timestamp(rv_end_ts)) => tx.begin_ts < rv_end_ts,
        Some(TxTimestampOrID::TxID(rv_end)) => {
            let te = txs.get(&rv_end).unwrap();
            let te = te.value().read().unwrap();
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
