use std::collections::HashMap;
use std::fmt::Display;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::panic::UnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use turso_core::SqliteDialect;

use bitmaps::Bitmap;
use garde::Validate;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sql_generation::generation::GenerationContext;
use sql_generation::generation::generated_expr::rename_column_refs_in_expr;
use sql_generation::model::query::transaction::Rollback;
use sql_generation::model::table::{SimValue, Table};
use tracing::trace;
use turso_core::Database;
use turso_parser::ast::ColumnConstraint;

use crate::generation::Shadow;
use crate::model::Query;
use crate::profiles::Profile;
use crate::runner::SimIO;
use crate::runner::cli::IoBackend;
use crate::runner::io::SimulatorIO;
use crate::runner::memory::io::MemorySimIO;
const DEFAULT_CACHE_SIZE: usize = 2000;
use super::cli::SimulatorCLI;

/// Pre-create attached DB files with MVCC journal mode so that journal modes
/// are compatible when ATTACH happens later during simulation.
fn enable_mvcc_on_attached_dbs(io: &Arc<dyn SimIO>, aux_paths: impl Iterator<Item = PathBuf>) {
    for aux_path in aux_paths {
        let aux_db = Database::open_file_with_flags(
            io.clone(),
            aux_path.to_str().unwrap(),
            turso_core::OpenFlags::default(),
            turso_core::DatabaseOpts::new()
                .with_attach(true)
                .with_generated_columns(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap_or_else(|e| panic!("Failed to open aux DB {aux_path:?}: {e}"));
        let aux_conn = aux_db
            .connect()
            .expect("Failed to connect to aux DB for MVCC setup");
        aux_conn
            .execute("PRAGMA journal_mode = 'mvcc'")
            .expect("Failed to enable MVCC on aux DB");
        aux_conn.close().expect("Failed to close aux DB connection");
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum SimulationType {
    Default,
    Doublecheck,
    Differential,
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum SimulationPhase {
    Test,
    Shrink,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TransactionMode {
    Read = 0,
    Concurrent = 1,
    Write = 2,
}

/// Represents a single operation during a transaction, applied in order.
#[derive(Debug, Clone)]
pub enum TxOperation {
    Insert {
        table_name: String,
        row: Vec<SimValue>,
    },
    Update {
        table_name: String,
        old_row: Vec<SimValue>,
        new_row: Vec<SimValue>,
    },
    Delete {
        table_name: String,
        row: Vec<SimValue>,
    },
    CreateTable {
        table: Table,
    },
    CreateIndex {
        table_name: String,
        index: sql_generation::model::table::Index,
    },
    DropIndex {
        table_name: String,
        index_name: String,
    },
    DropTable {
        table_name: String,
    },
    RenameTable {
        old_name: String,
        new_name: String,
    },
    AddColumn {
        table_name: String,
        column: sql_generation::model::table::Column,
    },
    DropColumn {
        table_name: String,
        column_index: usize,
    },
    RenameColumn {
        table_name: String,
        old_name: String,
        new_name: String,
    },
    AlterColumn {
        table_name: String,
        old_name: String,
        new_column: sql_generation::model::table::Column,
    },
}

/// Database snapshot
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// The current state after applying transaction's changes (used for reads within the transaction)
    current_tables: Vec<Table>,
    /// Operations recorded during this transaction, in order
    operations: Vec<TxOperation>,
    /// Named savepoint snapshots in this transaction.
    savepoints: Vec<ShadowSavepoint>,

    transaction_mode: TransactionMode,
}

impl Snapshot {
    #[inline]
    fn set_transaction_mode(&mut self, transaction_mode: TransactionMode) {
        self.transaction_mode = transaction_mode;
    }
}

#[derive(Debug, Clone)]
pub struct ShadowSavepoint {
    name: String,
    current_tables: Vec<Table>,
    operation_len: usize,
    starts_transaction: bool,
}

#[derive(Debug, Clone)]
pub enum TransactionTables {
    /// Deferred transaction. Snapshot of the tables has not been taken yet
    Deferred,
    Snapshot(Snapshot),
}

impl TransactionTables {
    #[inline]
    fn into_snapshot(self) -> Option<Snapshot> {
        match self {
            TransactionTables::Deferred => None,
            TransactionTables::Snapshot(snapshot) => Some(snapshot),
        }
    }

    #[inline]
    fn as_snaphot_opt(&self) -> Option<&Snapshot> {
        match self {
            TransactionTables::Deferred => None,
            TransactionTables::Snapshot(snapshot) => Some(snapshot),
        }
    }

    #[inline]
    fn as_snapshot_mut_opt(&mut self) -> Option<&mut Snapshot> {
        match self {
            TransactionTables::Deferred => None,
            TransactionTables::Snapshot(snapshot) => Some(snapshot),
        }
    }

    #[inline]
    fn expect_snaphot(&self) -> &Snapshot {
        self.as_snaphot_opt()
            .expect("snapshot must have been taken already")
    }

    #[inline]
    fn expect_snapshot_mut(&mut self) -> &mut Snapshot {
        self.as_snapshot_mut_opt()
            .expect("snapshot must have been taken already")
    }

    #[inline]
    pub fn record_insert(&mut self, table_name: String, row: Vec<SimValue>) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::Insert { table_name, row });
    }

    #[inline]
    pub fn record_update(
        &mut self,
        table_name: String,
        old_row: Vec<SimValue>,
        new_row: Vec<SimValue>,
    ) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::Update {
                table_name,
                old_row,
                new_row,
            });
    }

    #[inline]
    pub fn record_delete(&mut self, table_name: String, row: Vec<SimValue>) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::Delete { table_name, row });
    }

    pub fn record_create_table(&mut self, table: Table) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::CreateTable { table });
    }

    pub fn record_create_index(
        &mut self,
        table_name: String,
        index: sql_generation::model::table::Index,
    ) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::CreateIndex { table_name, index });
    }

    pub fn record_drop_index(&mut self, table_name: String, index_name: String) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::DropIndex {
                table_name,
                index_name,
            });
    }

    #[inline]
    pub fn record_drop_table(&mut self, table_name: String) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::DropTable { table_name });
    }

    #[inline]
    pub fn record_rename_table(&mut self, old_name: String, new_name: String) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::RenameTable { old_name, new_name });
    }

    pub fn record_add_column(
        &mut self,
        table_name: String,
        column: sql_generation::model::table::Column,
    ) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::AddColumn { table_name, column });
    }

    pub fn record_drop_column(&mut self, table_name: String, column_index: usize) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::DropColumn {
                table_name,
                column_index,
            });
    }

    pub fn record_rename_column(&mut self, table_name: String, old_name: String, new_name: String) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::RenameColumn {
                table_name,
                old_name,
                new_name,
            });
    }

    pub fn record_alter_column(
        &mut self,
        table_name: String,
        old_name: String,
        new_column: sql_generation::model::table::Column,
    ) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::AlterColumn {
                table_name,
                old_name,
                new_column,
            });
    }
}

#[derive(Debug)]
pub struct ShadowTables<'a> {
    commited_tables: &'a Vec<Table>,
    transaction_tables: Option<&'a TransactionTables>,
}

#[derive(Debug)]
pub struct ShadowTablesMut<'a> {
    commited_tables: &'a mut Vec<Table>,
    transaction_tables: &'a mut Option<TransactionTables>,
    sequences: &'a mut Vec<ShadowSequence>,
}

impl<'a> ShadowTables<'a> {
    fn tables(&self) -> &'a Vec<Table> {
        self.transaction_tables
            .and_then(|v| v.as_snaphot_opt())
            .map_or(self.commited_tables, |v| &v.current_tables)
    }
}

impl<'a> Deref for ShadowTables<'a> {
    type Target = Vec<Table>;

    fn deref(&self) -> &Self::Target {
        self.tables()
    }
}

impl<'a, 'b> ShadowTablesMut<'a>
where
    'a: 'b,
{
    fn tables(&'a self) -> &'a Vec<Table> {
        self.transaction_tables
            .as_ref()
            .map_or(self.commited_tables, |v| &v.expect_snaphot().current_tables)
    }

    fn tables_mut(&'b mut self) -> &'b mut Vec<Table> {
        self.transaction_tables
            .as_mut()
            .map_or(self.commited_tables, |v| {
                &mut v.expect_snapshot_mut().current_tables
            })
    }

    /// Record that a row was inserted during the current transaction
    pub fn record_insert(&mut self, table_name: String, row: Vec<SimValue>) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_insert(table_name, row);
        }
    }

    pub fn record_update(
        &mut self,
        table_name: String,
        old_row: Vec<SimValue>,
        new_row: Vec<SimValue>,
    ) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_update(table_name, old_row, new_row);
        }
    }

    /// Record that a row was deleted during the current transaction
    pub fn record_delete(&mut self, table_name: String, row: Vec<SimValue>) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_delete(table_name, row);
        }
    }

    /// Record that a table was created during the current transaction
    pub fn record_create_table(&mut self, table: Table) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_create_table(table);
        }
    }

    /// Record that an index was created during the current transaction
    pub fn record_create_index(
        &mut self,
        table_name: String,
        index: sql_generation::model::table::Index,
    ) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_create_index(table_name, index);
        }
    }

    /// Record that an index was dropped during the current transaction
    pub fn record_drop_index(&mut self, table_name: String, index_name: String) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_drop_index(table_name, index_name);
        }
    }

    /// Record that a table was dropped during the current transaction
    pub fn record_drop_table(&mut self, table_name: String) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_drop_table(table_name);
        }
    }

    /// Record that a table was renamed during the current transaction
    pub fn record_rename_table(&mut self, old_name: String, new_name: String) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_rename_table(old_name, new_name);
        }
    }

    /// Record that a column was added during the current transaction
    pub fn record_add_column(
        &mut self,
        table_name: String,
        column: sql_generation::model::table::Column,
    ) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_add_column(table_name, column);
        }
    }

    /// Record that a column was dropped during the current transaction
    pub fn record_drop_column(&mut self, table_name: String, column_index: usize) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_drop_column(table_name, column_index);
        }
    }

    /// Record that a column was renamed during the current transaction
    pub fn record_rename_column(&mut self, table_name: String, old_name: String, new_name: String) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_rename_column(table_name, old_name, new_name);
        }
    }

    /// Record that a column was altered during the current transaction
    pub fn record_alter_column(
        &mut self,
        table_name: String,
        old_name: String,
        new_column: sql_generation::model::table::Column,
    ) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_alter_column(table_name, old_name, new_column);
        }
    }

    /// Create a new sequence. Sequences are global and transaction-independent.
    pub fn create_sequence(
        &mut self,
        name: String,
        start: i64,
        increment: i64,
        min_value: i64,
        max_value: i64,
        cycle: bool,
    ) -> anyhow::Result<Vec<Vec<SimValue>>> {
        // The generator may pick the same `seq_<n>` twice (small name
        // space) and `CreateSequence`'s SQL emission uses
        // `CREATE SEQUENCE IF NOT EXISTS`, so the engine treats a
        // duplicate as a no-op rather than an error. Match that here:
        // a second create on the same name is a no-op (params from
        // the original creation stay authoritative).
        if self.sequences.iter().any(|s| s.name == name) {
            return Ok(vec![]);
        }
        self.sequences.push(ShadowSequence {
            name,
            current_value: start,
            increment_by: increment,
            min_value,
            max_value,
            is_called: false,
            cycle,
        });
        Ok(vec![])
    }

    /// Drop a sequence.
    pub fn drop_sequence(&mut self, name: &str) -> anyhow::Result<Vec<Vec<SimValue>>> {
        let pos = self
            .sequences
            .iter()
            .position(|s| s.name == name)
            .ok_or_else(|| anyhow::anyhow!("sequence \"{}\" does not exist", name))?;
        self.sequences.remove(pos);
        Ok(vec![])
    }

    /// Advance a sequence and return its next value. Mirrors core/schema.rs Sequence::nextval.
    pub fn nextval(&mut self, name: &str) -> anyhow::Result<i64> {
        let seq = self
            .sequences
            .iter_mut()
            .find(|s| s.name == name)
            .ok_or_else(|| anyhow::anyhow!("sequence \"{}\" does not exist", name))?;

        let value = if !seq.is_called {
            // First call: return current_value (which is start_value)
            seq.is_called = true;
            seq.current_value
        } else {
            let next = seq.current_value + seq.increment_by;
            if seq.increment_by > 0 && next > seq.max_value {
                if seq.cycle {
                    seq.current_value = seq.min_value;
                    seq.min_value
                } else {
                    return Err(anyhow::anyhow!(
                        "nextval: reached maximum value of sequence \"{}\"",
                        name
                    ));
                }
            } else if seq.increment_by < 0 && next < seq.min_value {
                if seq.cycle {
                    seq.current_value = seq.max_value;
                    seq.max_value
                } else {
                    return Err(anyhow::anyhow!(
                        "nextval: reached minimum value of sequence \"{}\"",
                        name
                    ));
                }
            } else {
                seq.current_value = next;
                next
            }
        };

        Ok(value)
    }

    /// Set a sequence's current value.
    pub fn setval(&mut self, name: &str, value: i64, is_called: bool) -> anyhow::Result<i64> {
        let seq = self
            .sequences
            .iter_mut()
            .find(|s| s.name == name)
            .ok_or_else(|| anyhow::anyhow!("sequence \"{}\" does not exist", name))?;

        if value < seq.min_value || value > seq.max_value {
            return Err(anyhow::anyhow!(
                "setval: value {} is out of bounds for sequence \"{}\" ({}..{})",
                value,
                name,
                seq.min_value,
                seq.max_value
            ));
        }

        seq.current_value = value;
        seq.is_called = is_called;
        Ok(value)
    }

    pub fn savepoint(&mut self, name: String) {
        let starts_transaction = self.transaction_tables.is_none();
        if starts_transaction
            || matches!(
                self.transaction_tables.as_ref(),
                Some(TransactionTables::Deferred)
            )
        {
            self.create_snapshot(TransactionMode::Write);
        }

        let snapshot = self
            .transaction_tables
            .as_mut()
            .expect("savepoint should create a transaction snapshot")
            .expect_snapshot_mut();
        snapshot.savepoints.push(ShadowSavepoint {
            name,
            current_tables: snapshot.current_tables.clone(),
            operation_len: snapshot.operations.len(),
            starts_transaction,
        });
    }

    pub fn rollback_to_savepoint(&mut self, name: &str) -> anyhow::Result<()> {
        let Some(txn) = self.transaction_tables.as_mut() else {
            return Err(anyhow::anyhow!(
                "cannot rollback to savepoint {name}: no active transaction"
            ));
        };
        let snapshot = txn.expect_snapshot_mut();
        let Some(savepoint_idx) = snapshot.savepoints.iter().rposition(|sp| sp.name == name) else {
            return Err(anyhow::anyhow!("no such savepoint: {name}"));
        };
        let savepoint = snapshot.savepoints[savepoint_idx].clone();
        snapshot.current_tables = savepoint.current_tables;
        snapshot.operations.truncate(savepoint.operation_len);
        // ROLLBACK TO keeps the target savepoint active and discards nested ones.
        snapshot.savepoints.truncate(savepoint_idx + 1);
        Ok(())
    }

    pub fn release_savepoint(&mut self, name: &str) -> anyhow::Result<()> {
        let Some(txn) = self.transaction_tables.as_mut() else {
            return Err(anyhow::anyhow!(
                "cannot release savepoint {name}: no active transaction"
            ));
        };
        let snapshot = txn.expect_snapshot_mut();
        let Some(savepoint_idx) = snapshot.savepoints.iter().rposition(|sp| sp.name == name) else {
            return Err(anyhow::anyhow!("no such savepoint: {name}"));
        };
        let starts_transaction = snapshot.savepoints[savepoint_idx].starts_transaction;
        snapshot.savepoints.truncate(savepoint_idx);
        if starts_transaction {
            self.apply_snapshot();
        }
        Ok(())
    }

    /// Tries to upgrade the Transaction Mode
    #[inline]
    pub fn upgrade_transaction(&mut self, query: &Query) {
        let transaction_mode = if query.is_write() {
            TransactionMode::Write
        } else if query.is_select() {
            TransactionMode::Read
        } else {
            return;
        };
        if let Some(txn) = self.transaction_tables.as_mut() {
            match txn {
                TransactionTables::Deferred => self.create_snapshot(transaction_mode),
                TransactionTables::Snapshot(snapshot) => {
                    match (snapshot.transaction_mode, transaction_mode) {
                        (_, TransactionMode::Concurrent) => {
                            unreachable!();
                        }
                        (TransactionMode::Read, TransactionMode::Write) => {
                            snapshot.set_transaction_mode(transaction_mode)
                        }
                        (TransactionMode::Concurrent, TransactionMode::Write) => {
                            if query.requires_exclusive_tx() {
                                // MVCC requires an exclusive write tx for DDL
                                // (see Query::requires_exclusive_tx). The plan
                                // generator forces a commit before these
                                // statements, so this upgrade rarely fires —
                                // kept for defensive correctness if a snapshot
                                // is built directly.
                                snapshot.set_transaction_mode(transaction_mode)
                            }
                        }
                        _ => {}
                    };
                }
            }
        }
    }

    #[inline]
    pub fn create_deferred_snapshot(&mut self) {
        *self.transaction_tables = Some(TransactionTables::Deferred);
    }

    #[inline]
    pub fn create_snapshot(&mut self, transaction_mode: TransactionMode) {
        *self.transaction_tables = Some(TransactionTables::Snapshot(Snapshot {
            current_tables: self.commited_tables.clone(),
            operations: Vec::new(),
            savepoints: Vec::new(),
            transaction_mode,
        }));
    }

    pub fn apply_snapshot(&mut self) {
        if let Some(transaction_tables) = self
            .transaction_tables
            .take()
            .and_then(|transaction_tables| transaction_tables.into_snapshot())
        {
            // Build a mapping from any table name used during the transaction to its final name.
            // This is needed because operations store the table name at the time they were recorded,
            // but transaction_tables.current_tables has tables with their final names.
            let mut name_to_final: HashMap<String, String> = HashMap::new();
            for op in &transaction_tables.operations {
                if let TxOperation::RenameTable { old_name, new_name } = op {
                    name_to_final.insert(old_name.clone(), new_name.clone());
                    // Update all existing mappings that pointed to old_name
                    for v in name_to_final.values_mut() {
                        if v == old_name {
                            v.clone_from(new_name);
                        }
                    }
                }
            }

            // Apply all operations in recorded order.
            // This ensures operations like CREATE TABLE, ADD COLUMN, DELETE are applied correctly
            // where DELETE sees rows with the same shape as when it was recorded.
            for op in &transaction_tables.operations {
                match op {
                    TxOperation::Insert { table_name, row } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == table_name)
                            .expect("Table should exist in committed tables");
                        committed.rows.push(row.clone());
                    }
                    TxOperation::Update {
                        table_name,
                        old_row,
                        new_row,
                    } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == table_name)
                            .expect("Table should exist in committed tables");

                        let Some(pos) = committed.rows.iter().position(|r| r == old_row) else {
                            panic!(
                                "failed to apply UPDATE to table '{}': row not found (old_row={:?}, committed_rows={})",
                                table_name,
                                old_row,
                                committed.rows.len()
                            );
                        };
                        committed.rows[pos].clone_from(new_row);
                    }
                    TxOperation::Delete { table_name, row } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == table_name)
                            .expect("Table should exist in committed tables");
                        if let Some(pos) = committed.rows.iter().position(|r| r == row) {
                            committed.rows.remove(pos);
                        }
                    }
                    TxOperation::CreateTable { table } => {
                        self.commited_tables.push(table.clone());
                    }
                    TxOperation::CreateIndex { table_name, index } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == table_name)
                            .expect("Table should exist in committed tables");
                        committed.indexes.push(index.clone());
                    }
                    TxOperation::DropIndex {
                        table_name,
                        index_name,
                    } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == table_name)
                            .expect("Table should exist in committed tables");
                        committed.indexes.retain(|i| &i.index_name != index_name);
                    }
                    TxOperation::DropTable { table_name } => {
                        self.commited_tables.retain(|t| &t.name != table_name);
                    }
                    TxOperation::RenameTable { old_name, new_name } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == old_name)
                            .expect("Table should exist in committed tables");
                        committed.name.clone_from(new_name);
                    }
                    TxOperation::AddColumn { table_name, column } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == table_name)
                            .expect("Table should exist in committed tables");
                        // Use the final name to look up in transaction_tables (which has current state).
                        // Note: current_tables represents the FINAL state after all transaction
                        // operations, not intermediate states. If a table was dropped later in
                        // the same transaction, it won't exist in current_tables even though
                        // we're processing an earlier AddColumn operation. In that case, we skip
                        // the sanity check but still apply the column change to committed_tables
                        // (which will be removed when we later process the DropTable operation).
                        let final_name = name_to_final.get(table_name).unwrap_or(table_name);
                        if let Some(txn_table) = transaction_tables
                            .current_tables
                            .iter()
                            .find(|t| &t.name == final_name)
                        {
                            assert!(
                                txn_table.columns.len() > committed.columns.len(),
                                "Transaction table should have more columns than committed table"
                            );
                        }
                        committed.columns.push(column.clone());
                        let new_col_count = committed.columns.len();
                        // Add NULL only for rows that need it.
                        // Rows inserted after ADD COLUMN in the same transaction
                        // already have the correct number of values.
                        for row in &mut committed.rows {
                            while row.len() < new_col_count {
                                row.push(SimValue::NULL);
                            }
                        }
                    }
                    TxOperation::DropColumn {
                        table_name,
                        column_index,
                    } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == table_name)
                            .expect("Table should exist in committed tables");
                        // Use the final name to look up in transaction_tables (which has current state).
                        // See comment in AddColumn above for why this lookup is optional.
                        let final_name = name_to_final.get(table_name).unwrap_or(table_name);
                        if let Some(txn_table) = transaction_tables
                            .current_tables
                            .iter()
                            .find(|t| &t.name == final_name)
                        {
                            assert!(
                                txn_table.columns.len() < committed.columns.len(),
                                "Transaction table should have fewer columns than committed table"
                            );
                        }
                        let old_col_count = committed.columns.len();
                        committed.columns.remove(*column_index);
                        // Only remove from rows that have the old column count.
                        // Rows inserted after DROP COLUMN in the same transaction
                        // already have the correct (new) number of values.
                        for row in &mut committed.rows {
                            if row.len() == old_col_count {
                                row.remove(*column_index);
                            }
                        }
                    }
                    TxOperation::RenameColumn {
                        table_name,
                        old_name,
                        new_name,
                    } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == table_name)
                            .expect("Table should exist in committed tables");
                        let col = committed
                            .columns
                            .iter_mut()
                            .find(|c| &c.name == old_name)
                            .expect("Column should exist");
                        col.name.clone_from(new_name);
                        // Update generated column expressions that reference the old column name
                        for col in &mut committed.columns {
                            for constraint in &mut col.constraints {
                                if let ColumnConstraint::Generated { expr, .. } = constraint {
                                    rename_column_refs_in_expr(expr, old_name, new_name);
                                }
                            }
                        }
                        // Update index column names
                        for index in &mut committed.indexes {
                            for (col_name, _) in &mut index.columns {
                                if col_name == old_name {
                                    col_name.clone_from(new_name);
                                }
                            }
                        }
                    }
                    TxOperation::AlterColumn {
                        table_name,
                        old_name,
                        new_column,
                    } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == table_name)
                            .expect("Table should exist in committed tables");
                        if let Some(col) =
                            committed.columns.iter_mut().find(|c| &c.name == old_name)
                        {
                            *col = new_column.clone();
                        }
                        // Update index column names if the column was renamed
                        for index in &mut committed.indexes {
                            for (col_name, _) in &mut index.columns {
                                if col_name == old_name {
                                    col_name.clone_from(&new_column.name);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn delete_snapshot(&mut self) {
        *self.transaction_tables = None;
    }
}

impl<'a> Deref for ShadowTablesMut<'a> {
    type Target = Vec<Table>;

    fn deref(&self) -> &Self::Target {
        self.tables()
    }
}

impl<'a> DerefMut for ShadowTablesMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.tables_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shadow_tables_mut<'a>(
        commited_tables: &'a mut Vec<Table>,
        transaction_tables: &'a mut Option<TransactionTables>,
        sequences: &'a mut Vec<ShadowSequence>,
    ) -> ShadowTablesMut<'a> {
        ShadowTablesMut {
            commited_tables,
            transaction_tables,
            sequences,
        }
    }

    #[test]
    fn savepoint_outside_transaction_commits_on_release() {
        let mut commited_tables = Vec::new();
        let mut transaction_tables = None;
        let mut sequences = Vec::new();

        {
            let mut tables = shadow_tables_mut(
                &mut commited_tables,
                &mut transaction_tables,
                &mut sequences,
            );
            tables.savepoint("sp".to_string());
            let snapshot = tables
                .transaction_tables
                .as_ref()
                .expect("savepoint should create a transaction")
                .expect_snaphot();
            assert_eq!(snapshot.savepoints.len(), 1);
            assert!(snapshot.savepoints[0].starts_transaction);

            tables.release_savepoint("sp").unwrap();
        }

        assert!(transaction_tables.is_none());
    }

    #[test]
    fn rollback_to_savepoint_keeps_target_active() {
        let mut commited_tables = Vec::new();
        let mut transaction_tables = None;
        let mut sequences = Vec::new();

        let mut tables = shadow_tables_mut(
            &mut commited_tables,
            &mut transaction_tables,
            &mut sequences,
        );
        tables.create_snapshot(TransactionMode::Write);
        tables.savepoint("sp".to_string());
        tables.record_insert("table_0".to_string(), vec![]);

        tables.rollback_to_savepoint("sp").unwrap();
        let snapshot = tables
            .transaction_tables
            .as_ref()
            .expect("rollback target transaction should exist")
            .expect_snaphot();
        assert!(snapshot.operations.is_empty());
        assert_eq!(snapshot.savepoints.len(), 1);
        assert_eq!(snapshot.savepoints[0].name, "sp");
    }

    /// Table with columns (a INTEGER, g INTEGER UNIQUE AS (a + b) VIRTUAL, b INTEGER)
    /// and one row per (a, b) pair, with g materialized in the shadow rows.
    fn table_with_unique_generated_column(rows: &[(i64, i64)]) -> Table {
        use sql_generation::model::table::{Column, ColumnType};
        use turso_parser::ast;

        let generated_expr = ast::Expr::Binary(
            Box::new(ast::Expr::Id(ast::Name::from_string("a"))),
            ast::Operator::Add,
            Box::new(ast::Expr::Id(ast::Name::from_string("b"))),
        );
        Table {
            name: "t".to_string(),
            columns: vec![
                Column {
                    name: "a".to_string(),
                    column_type: ColumnType::Integer,
                    constraints: vec![],
                },
                Column {
                    name: "g".to_string(),
                    column_type: ColumnType::Integer,
                    constraints: vec![
                        ColumnConstraint::Unique(None),
                        ColumnConstraint::Generated {
                            expr: Box::new(generated_expr),
                            typ: Some(ast::GeneratedColumnType::Virtual),
                        },
                    ],
                },
                Column {
                    name: "b".to_string(),
                    column_type: ColumnType::Integer,
                    constraints: vec![],
                },
            ],
            rows: rows
                .iter()
                .map(|(a, b)| {
                    vec![
                        SimValue(turso_core::Value::from_i64(*a)),
                        SimValue(turso_core::Value::from_i64(a + b)),
                        SimValue(turso_core::Value::from_i64(*b)),
                    ]
                })
                .collect(),
            indexes: vec![],
        }
    }

    fn update_set_b(value: i64) -> sql_generation::model::query::update::Update {
        use sql_generation::model::query::predicate::Predicate;
        use sql_generation::model::query::update::{SetValue, Update};

        Update {
            table: "t".to_string(),
            set_values: vec![(
                "b".to_string(),
                SetValue::Simple(SimValue(turso_core::Value::from_i64(value))),
            )],
            predicate: Predicate::true_(),
        }
    }

    #[test]
    fn update_unique_rejects_transient_collision_in_row_order() {
        // SQLite and Turso apply an UPDATE row by row in rowid order with an
        // immediate uniqueness check per row, so the shadow must reject an
        // update where a row's new value collides with the old value of a
        // later, not-yet-updated row — even though the final row set would be
        // unique. Here SET b = 2 rewrites g from (2, 3) to (3, 4): row 1's new
        // g = 3 collides with row 2's still-present old g = 3.
        let mut commited_tables = vec![table_with_unique_generated_column(&[(1, 1), (2, 1)])];
        let mut transaction_tables = None;
        let mut sequences = Vec::new();
        let mut tables = shadow_tables_mut(
            &mut commited_tables,
            &mut transaction_tables,
            &mut sequences,
        );

        let result = update_set_b(2).shadow(&mut tables);
        assert!(
            result.is_err(),
            "shadow accepted an update that collides with a not-yet-updated row"
        );
        // The failed update must not modify the shadow rows.
        assert_eq!(
            commited_tables[0].rows[0][2],
            SimValue(turso_core::Value::from_i64(1))
        );
    }

    #[test]
    fn update_unique_accepts_when_no_transient_collision() {
        // Same shape as above, but the rewritten values (3, 7) never collide
        // with any old value, so the update must be accepted and applied.
        let mut commited_tables = vec![table_with_unique_generated_column(&[(1, 1), (5, 1)])];
        let mut transaction_tables = None;
        let mut sequences = Vec::new();
        let mut tables = shadow_tables_mut(
            &mut commited_tables,
            &mut transaction_tables,
            &mut sequences,
        );

        let result = update_set_b(2).shadow(&mut tables);
        assert!(result.is_ok(), "shadow rejected a conflict-free update");
        let g_values: Vec<_> = commited_tables[0]
            .rows
            .iter()
            .map(|r| r[1].clone())
            .collect();
        assert_eq!(
            g_values,
            vec![
                SimValue(turso_core::Value::from_i64(3)),
                SimValue(turso_core::Value::from_i64(7)),
            ]
        );
    }

    #[test]
    fn savepoint_inside_deferred_transaction_stays_open_on_release() {
        let mut commited_tables = Vec::new();
        let mut transaction_tables = Some(TransactionTables::Deferred);
        let mut sequences = Vec::new();

        let mut tables = shadow_tables_mut(
            &mut commited_tables,
            &mut transaction_tables,
            &mut sequences,
        );
        tables.savepoint("sp".to_string());
        let snapshot = tables
            .transaction_tables
            .as_ref()
            .expect("deferred transaction should materialize")
            .expect_snaphot();
        assert_eq!(snapshot.transaction_mode, TransactionMode::Write);
        assert_eq!(snapshot.savepoints.len(), 1);
        assert!(!snapshot.savepoints[0].starts_transaction);

        tables.release_savepoint("sp").unwrap();
        let snapshot = tables
            .transaction_tables
            .as_ref()
            .expect("outer transaction should remain open")
            .expect_snaphot();
        assert!(snapshot.savepoints.is_empty());
    }
}

/// Shadow model for a sequence. Sequences are global (not per-connection) and their
/// advances are never rolled back, matching PostgreSQL semantics.
#[derive(Debug, Clone)]
pub struct ShadowSequence {
    pub name: String,
    pub current_value: i64,
    pub increment_by: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub is_called: bool,
    pub cycle: bool,
}

pub(crate) struct SimulatorEnv {
    pub(crate) opts: SimulatorOpts,
    pub profile: Profile,
    pub(crate) connections: Vec<SimConnection>,
    pub(crate) io: Arc<dyn SimIO>,
    pub(crate) db: Option<Arc<Database>>,
    pub(crate) rng: ChaCha8Rng,

    seed: u64,
    pub(crate) paths: Paths,
    pub(crate) type_: SimulationType,
    pub(crate) phase: SimulationPhase,
    pub io_backend: IoBackend,

    /// If connection state is None, means we are not in a transaction
    pub connection_tables: Vec<Option<TransactionTables>>,
    /// Bit map indicating whether a connection has executed a query that is not transaction related
    ///
    /// E.g Select, Insert, Create
    /// and not Begin, Commit, Rollback \
    /// Has max size of 64 to accomodate 64 connections
    connection_last_query: Bitmap<64>,
    // Table data that is committed into the database or wal
    pub committed_tables: Vec<Table>,
    /// Names of attached databases (e.g. ["aux0", "aux1", "aux2"])
    pub(crate) attached_dbs: Vec<String>,
    /// Sequences are global objects, not affected by transactions/savepoints
    pub sequences: Vec<ShadowSequence>,
}

impl UnwindSafe for SimulatorEnv {}

impl SimulatorEnv {
    pub(crate) fn clone_without_connections(&self) -> Self {
        SimulatorEnv {
            opts: self.opts.clone(),
            io: self.io.clone(),
            db: self.db.clone(),
            rng: self.rng.clone(),
            seed: self.seed,
            paths: self.paths.clone(),
            type_: self.type_,
            phase: self.phase,
            io_backend: self.io_backend,
            profile: self.profile.clone(),
            connections: (0..self.connections.len())
                .map(|_| SimConnection::Disconnected)
                .collect(),
            // TODO: not sure if connection_tables should be recreated instead
            connection_tables: self.connection_tables.clone(),
            connection_last_query: self.connection_last_query,
            committed_tables: self.committed_tables.clone(),
            attached_dbs: self.attached_dbs.clone(),
            sequences: self.sequences.clone(),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.clear_tables();
        self.connections.iter_mut().for_each(|c| c.disconnect());
        self.rng = ChaCha8Rng::seed_from_u64(self.opts.seed);

        let latency_prof = &self.profile.io.latency;

        let io: Arc<dyn SimIO> = match self.io_backend {
            IoBackend::Memory => Arc::new(MemorySimIO::new(
                self.opts.seed,
                self.opts.page_size,
                latency_prof.latency_probability,
                latency_prof.min_tick,
                latency_prof.max_tick,
            )),
            _ => Arc::new(
                SimulatorIO::new(
                    self.opts.seed,
                    self.opts.page_size,
                    latency_prof.latency_probability,
                    latency_prof.min_tick,
                    latency_prof.max_tick,
                    self.io_backend,
                )
                .unwrap(),
            ),
        };

        // Remove existing database file
        let db_path = self.get_db_path();
        if db_path.exists() {
            std::fs::remove_file(&db_path).unwrap();
        }

        let wal_path = db_path.with_extension("db-wal");
        if wal_path.exists() {
            std::fs::remove_file(&wal_path).unwrap();
        }

        // Remove MVCC logical log file
        let log_path = db_path.with_extension("db-log");
        if log_path.exists() {
            std::fs::remove_file(&log_path).unwrap();
        }

        // Remove attached database files
        for name in &self.attached_dbs {
            let aux_path = self.paths.aux_db(&self.type_, &self.phase, name);
            if aux_path.exists() {
                std::fs::remove_file(&aux_path).unwrap();
            }
            let aux_wal = aux_path.with_extension("db-wal");
            if aux_wal.exists() {
                std::fs::remove_file(&aux_wal).unwrap();
            }
            let aux_log = aux_path.with_extension("db-log");
            if aux_log.exists() {
                std::fs::remove_file(&aux_log).unwrap();
            }
        }

        self.db = None;

        let db = match Database::open_file_with_flags(
            io.clone(),
            db_path.to_str().unwrap(),
            turso_core::OpenFlags::default(),
            turso_core::DatabaseOpts::new()
                .with_autovacuum(true)
                .with_attach(true)
                .with_generated_columns(true),
            None,
            Arc::new(SqliteDialect),
        ) {
            Ok(db) => db,
            Err(e) => {
                tracing::error!(%e);
                panic!("error opening simulator test file {db_path:?}: {e:?}");
            }
        };

        // Re-enable MVCC mode if the profile says to use MVCC
        if self.profile.mvcc {
            let conn = db
                .connect()
                .expect("Failed to create connection for MVCC setup");
            conn.execute("PRAGMA journal_mode = 'mvcc'")
                .expect("Failed to enable MVCC mode");

            enable_mvcc_on_attached_dbs(
                &io,
                self.attached_dbs
                    .iter()
                    .map(|name| self.get_aux_db_path(name)),
            );
        }

        self.io = io;
        self.db = Some(db);
    }

    pub(crate) fn get_db_path(&self) -> PathBuf {
        self.paths.db(&self.type_, &self.phase)
    }

    pub(crate) fn get_aux_db_path(&self, name: &str) -> PathBuf {
        self.paths.aux_db(&self.type_, &self.phase, name)
    }

    pub(crate) fn get_plan_path(&self) -> PathBuf {
        self.paths.plan(&self.type_, &self.phase)
    }

    pub(crate) fn clone_as(&self, simulation_type: SimulationType) -> Self {
        let mut env = self.clone_without_connections();
        env.type_ = simulation_type;
        env.clear();
        env
    }

    pub(crate) fn clone_at_phase(&self, phase: SimulationPhase) -> Self {
        let mut env = self.clone_without_connections();
        env.phase = phase;
        env.clear();
        env
    }

    pub fn sequence_info(&self) -> Vec<(String, i64, i64)> {
        // Filter out sequences reserved by the SequenceMonotonicity
        // property — that property's assertion expects a fresh seq
        // returning `start` on the first nextval. If the regular
        // workload picks the same name and emits nextval / setval /
        // drop on it between the property's CREATE SEQUENCE and its
        // first nextval, the assertion observes engine state already
        // moved by another connection and bails.
        self.sequences
            .iter()
            .filter(|s| !s.name.starts_with("seq_mono_"))
            .map(|s| (s.name.clone(), s.min_value, s.max_value))
            .collect()
    }

    pub fn choose_conn(&self, rng: &mut impl Rng) -> usize {
        rng.random_range(0..self.connections.len())
    }

    pub(crate) fn can_simulate_power_loss(&self) -> bool {
        !self.profile.mvcc
            && self.io_backend == IoBackend::Memory
            && matches!(self.type_, SimulationType::Default)
    }

    /// Rng only used for generating interactions. By having a separate Rng we can guarantee that a particular seed
    /// will always create the same interactions plan, regardless of the changes that happen in the Database code
    pub fn gen_rng(&self) -> ChaCha8Rng {
        // Seed + 1 so that there is no relation with the original seed, and so we have no way to accidently generate
        // the first Create statement twice in a row
        ChaCha8Rng::seed_from_u64(self.seed + 1)
    }
}

impl SimulatorEnv {
    pub(crate) fn new(
        seed: u64,
        cli_opts: &SimulatorCLI,
        paths: Paths,
        simulation_type: SimulationType,
        profile: &Profile,
    ) -> Self {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        let num_attached = rng.random_range(1..=3usize);
        let attached_dbs: Vec<String> = (0..num_attached).map(|i| format!("aux{i}")).collect();

        let mut opts = SimulatorOpts {
            seed,
            ticks: usize::MAX,
            disable_select_optimizer: cli_opts.disable_select_optimizer,
            disable_insert_values_select: cli_opts.disable_insert_values_select,
            disable_double_create_failure: cli_opts.disable_double_create_failure,
            disable_select_limit: cli_opts.disable_select_limit,
            disable_delete_select: cli_opts.disable_delete_select,
            disable_drop_select: cli_opts.disable_drop_select,
            disable_where_true_false_null: cli_opts.disable_where_true_false_null,
            disable_union_all_preserves_cardinality: cli_opts
                .disable_union_all_preserves_cardinality,
            disable_savepoint_rollback: cli_opts.disable_savepoint_rollback,
            disable_fsync_no_wait: cli_opts.disable_fsync_no_wait,
            disable_faulty_query: cli_opts.disable_faulty_query,
            page_size: 4096, // TODO: randomize this too
            max_interactions: rng.random_range(cli_opts.minimum_tests..=cli_opts.maximum_tests),
            max_time_simulation: cli_opts.maximum_time,
            disable_reopen_database: cli_opts.disable_reopen_database,
            disable_integrity_check: cli_opts.disable_integrity_check,
            cache_size: profile.cache_size_pages.unwrap_or(DEFAULT_CACHE_SIZE),
        };

        // Remove existing database file if it exists
        let db_path = paths.db(&simulation_type, &SimulationPhase::Test);

        if db_path.exists() {
            std::fs::remove_file(&db_path).unwrap();
        }

        let wal_path = db_path.with_extension("db-wal");
        if wal_path.exists() {
            std::fs::remove_file(&wal_path).unwrap();
        }

        // Remove MVCC logical log file
        let log_path = db_path.with_extension("db-log");
        if log_path.exists() {
            std::fs::remove_file(&log_path).unwrap();
        }

        // Remove attached database files if they exist
        for name in &attached_dbs {
            let aux_path = paths.aux_db(&simulation_type, &SimulationPhase::Test, name);
            if aux_path.exists() {
                std::fs::remove_file(&aux_path).unwrap();
            }
            let aux_wal = aux_path.with_extension("db-wal");
            if aux_wal.exists() {
                std::fs::remove_file(&aux_wal).unwrap();
            }
            let aux_log = aux_path.with_extension("db-log");
            if aux_log.exists() {
                std::fs::remove_file(&aux_log).unwrap();
            }
        }

        let mut profile = profile.clone();
        // Conditionals here so that we can override some profile options from the CLI
        if let Some(mvcc) = cli_opts.mvcc {
            profile.mvcc = mvcc;
        }
        if let Some(latency_prob) = cli_opts.latency_probability {
            profile.io.latency.latency_probability = latency_prob;
        }
        if let Some(max_tick) = cli_opts.max_tick {
            profile.io.latency.max_tick = max_tick;
        }
        if let Some(min_tick) = cli_opts.min_tick {
            profile.io.latency.min_tick = min_tick;
        }
        if cli_opts.differential {
            // Disable faults when running against sqlite as we cannot control faults on it
            profile.io.enable = false;
            // Disable limits due to differences in return order from turso and rusqlite
            opts.disable_select_limit = true;

            // There is no `ALTER COLUMN` in SQLite
            profile.query.gen_opts.query.alter_table.alter_column = false;

            // SQLite has no CREATE SEQUENCE / nextval / setval. Disable the
            // sequence-related query generators and the SequenceMonotonicity
            // property when running differentially against rusqlite —
            // otherwise the differential run aborts on the first emitted
            // `CREATE SEQUENCE` with `syntax error near "SEQUENCE"`.
            profile.query.create_sequence_weight = 0;
            profile.query.drop_sequence_weight = 0;
            profile.query.nextval_weight = 0;
            profile.query.setval_weight = 0;
        }

        profile.validate().unwrap();

        let latency_prof = &profile.io.latency;

        let io_backend = cli_opts.io_backend;
        let io: Arc<dyn SimIO> = match io_backend {
            IoBackend::Memory => Arc::new(MemorySimIO::new(
                opts.seed,
                opts.page_size,
                latency_prof.latency_probability,
                latency_prof.min_tick,
                latency_prof.max_tick,
            )),
            _ => Arc::new(
                SimulatorIO::new(
                    opts.seed,
                    opts.page_size,
                    latency_prof.latency_probability,
                    latency_prof.min_tick,
                    latency_prof.max_tick,
                    io_backend,
                )
                .unwrap(),
            ),
        };

        let db = match Database::open_file_with_flags(
            io.clone(),
            db_path.to_str().unwrap(),
            turso_core::OpenFlags::default(),
            turso_core::DatabaseOpts::new()
                .with_autovacuum(true)
                .with_attach(true)
                .with_generated_columns(true),
            None,
            Arc::new(SqliteDialect),
        ) {
            Ok(db) => db,
            Err(e) => {
                panic!("error opening simulator test file {db_path:?}: {e:?}");
            }
        };

        // Switch to MVCC mode if the profile says to use MVCC
        if profile.mvcc {
            let conn = db
                .connect()
                .expect("Failed to create connection for MVCC setup");
            conn.execute("PRAGMA journal_mode = 'mvcc'")
                .expect("Failed to enable MVCC mode");

            enable_mvcc_on_attached_dbs(
                &io,
                attached_dbs
                    .iter()
                    .map(|name| paths.aux_db(&simulation_type, &SimulationPhase::Test, name)),
            );
        }

        let connections = (0..profile.max_connections)
            .map(|_| SimConnection::Disconnected)
            .collect::<Vec<_>>();

        SimulatorEnv {
            opts,
            connections,
            paths,
            rng,
            seed,
            io,
            db: Some(db),
            type_: simulation_type,
            phase: SimulationPhase::Test,
            io_backend,
            profile: profile.clone(),
            committed_tables: Vec::new(),
            connection_tables: vec![None; profile.max_connections],
            connection_last_query: Bitmap::new(),
            attached_dbs,
            sequences: Vec::new(),
        }
    }

    pub(crate) fn connect(&mut self, connection_index: usize) {
        if connection_index >= self.connections.len() {
            panic!("connection index out of bounds");
        }

        if self.connections[connection_index].is_connected() {
            trace!("Connection {connection_index} is already connected, skipping reconnection");
            return;
        }

        match self.type_ {
            SimulationType::Default | SimulationType::Doublecheck => {
                let conn = self
                    .db
                    .as_ref()
                    .expect("db to be Some")
                    .connect()
                    .expect("Failed to connect to Limbo database");
                if self.opts.cache_size != DEFAULT_CACHE_SIZE {
                    conn.execute(format!("PRAGMA cache_size = {}", self.opts.cache_size))
                        .expect("set pragma cache_size");
                }
                self.connections[connection_index] = SimConnection::LimboConnection(conn);
            }
            SimulationType::Differential => {
                self.connections[connection_index] = SimConnection::SQLiteConnection(
                    rusqlite::Connection::open(self.get_db_path())
                        .expect("Failed to open SQLite connection"),
                );
            }
        };

        self.attach_databases(connection_index);
    }

    fn attach_databases(&self, connection_index: usize) {
        for name in &self.attached_dbs {
            let aux_path = self.get_aux_db_path(name);
            match &self.connections[connection_index] {
                SimConnection::LimboConnection(conn) => {
                    conn.execute(format!("ATTACH '{}' AS {name}", aux_path.display()))
                        .unwrap_or_else(|e| panic!("Failed to ATTACH {name}: {e}"));
                }
                SimConnection::SQLiteConnection(conn) => {
                    conn.execute(&format!("ATTACH '{}' AS {name}", aux_path.display()), [])
                        .unwrap_or_else(|e| panic!("Failed to ATTACH {name} on SQLite: {e}"));
                }
                SimConnection::Disconnected => {}
            }
        }
    }

    /// Clears the commited tables, connection tables, and sequences
    pub fn clear_tables(&mut self) {
        self.committed_tables.clear();
        self.connection_tables.iter_mut().for_each(|t| *t = None);
        self.connection_last_query = Bitmap::new();
        self.sequences.clear();
    }

    // TODO: does not yet create the appropriate context to avoid WriteWriteConflitcs
    pub fn connection_context(&self, conn_index: usize) -> impl GenerationContext {
        struct ConnectionGenContext<'a> {
            tables: &'a Vec<sql_generation::model::table::Table>,
            opts: &'a sql_generation::generation::Opts,
        }

        impl<'a> GenerationContext for ConnectionGenContext<'a> {
            fn tables(&self) -> &Vec<sql_generation::model::table::Table> {
                self.tables
            }

            fn opts(&self) -> &sql_generation::generation::Opts {
                self.opts
            }
        }

        let tables = self.get_conn_tables(conn_index).tables();

        ConnectionGenContext {
            opts: &self.profile.query.gen_opts,
            tables,
        }
    }

    pub fn conn_in_transaction(&self, conn_index: usize) -> bool {
        self.connection_tables
            .get(conn_index)
            .is_some_and(|t| t.is_some())
    }

    /// Whether the underlying database connection is currently inside an explicit transaction
    pub fn conn_db_in_transaction(&self, conn_index: usize) -> bool {
        match self.connections.get(conn_index) {
            Some(SimConnection::LimboConnection(conn)) => !conn.get_auto_commit(),
            Some(SimConnection::SQLiteConnection(conn)) => !conn.is_autocommit(),
            _ => false,
        }
    }

    pub fn has_conn_executed_query_after_transaction(&self, conn_index: usize) -> bool {
        self.connection_last_query.get(conn_index)
    }

    pub fn update_conn_last_interaction(&mut self, conn_index: usize, query: Option<&Query>) {
        // If the conn will execute a transaction statement then we set the bitmap to false
        // to indicate we have not executed any queries yet after the transaction begun
        let value = query.is_some_and(|query| {
            matches!(
                query,
                Query::Begin(..)
                    | Query::Commit(..)
                    | Query::Rollback(..)
                    | Query::Savepoint(..)
                    | Query::RollbackToSavepoint(..)
                    | Query::ReleaseSavepoint(..)
            )
        });
        self.connection_last_query.set(conn_index, value);
    }

    pub fn rollback_conn(&mut self, conn_index: usize) {
        Rollback.shadow(&mut self.get_conn_tables_mut(conn_index));
        self.update_conn_last_interaction(conn_index, Some(&Query::Rollback(Rollback)));
    }

    pub fn get_conn_tables(&self, conn_index: usize) -> ShadowTables<'_> {
        ShadowTables {
            transaction_tables: self.connection_tables.get(conn_index).unwrap().as_ref(),
            commited_tables: &self.committed_tables,
        }
    }

    pub fn get_conn_tables_mut(&mut self, conn_index: usize) -> ShadowTablesMut<'_> {
        ShadowTablesMut {
            transaction_tables: self.connection_tables.get_mut(conn_index).unwrap(),
            commited_tables: &mut self.committed_tables,
            sequences: &mut self.sequences,
        }
    }
}

pub(crate) enum SimConnection {
    LimboConnection(Arc<turso_core::Connection>),
    SQLiteConnection(rusqlite::Connection),
    Disconnected,
}

impl SimConnection {
    pub(crate) fn is_connected(&self) -> bool {
        match self {
            SimConnection::LimboConnection(_) | SimConnection::SQLiteConnection(_) => true,
            SimConnection::Disconnected => false,
        }
    }

    pub(crate) fn disconnect(&mut self) {
        let conn = mem::replace(self, SimConnection::Disconnected);

        match conn {
            SimConnection::LimboConnection(conn) => {
                conn.close().unwrap();
            }
            SimConnection::SQLiteConnection(conn) => {
                conn.close().unwrap();
            }
            SimConnection::Disconnected => {}
        }
    }
}

impl Display for SimConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SimConnection::LimboConnection(_) => {
                write!(f, "LimboConnection")
            }
            SimConnection::SQLiteConnection(_) => {
                write!(f, "SQLiteConnection")
            }
            SimConnection::Disconnected => {
                write!(f, "Disconnected")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SimulatorOpts {
    pub(crate) seed: u64,
    pub(crate) ticks: usize,

    pub(crate) disable_select_optimizer: bool,
    pub(crate) disable_insert_values_select: bool,
    pub(crate) disable_double_create_failure: bool,
    pub(crate) disable_select_limit: bool,
    pub(crate) disable_delete_select: bool,
    pub(crate) disable_drop_select: bool,
    pub(crate) disable_where_true_false_null: bool,
    pub(crate) disable_union_all_preserves_cardinality: bool,
    pub(crate) disable_savepoint_rollback: bool,
    pub(crate) disable_fsync_no_wait: bool,
    pub(crate) disable_faulty_query: bool,
    pub(crate) disable_reopen_database: bool,
    pub(crate) disable_integrity_check: bool,

    pub(crate) max_interactions: u32,
    pub(crate) page_size: usize,
    pub(crate) cache_size: usize,
    pub(crate) max_time_simulation: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct Paths {
    pub(crate) base: PathBuf,
    pub(crate) history: PathBuf,
}

impl Paths {
    pub(crate) fn new(output_dir: &Path) -> Self {
        Paths {
            base: output_dir.to_path_buf(),
            history: PathBuf::from(output_dir).join("history.txt"),
        }
    }

    fn path_(&self, type_: &SimulationType, phase: &SimulationPhase) -> PathBuf {
        match (type_, phase) {
            (SimulationType::Default, SimulationPhase::Test) => self.base.join(Path::new("test")),
            (SimulationType::Default, SimulationPhase::Shrink) => {
                self.base.join(Path::new("shrink"))
            }
            (SimulationType::Differential, SimulationPhase::Test) => {
                self.base.join(Path::new("diff"))
            }
            (SimulationType::Differential, SimulationPhase::Shrink) => {
                self.base.join(Path::new("diff_shrink"))
            }
            (SimulationType::Doublecheck, SimulationPhase::Test) => {
                self.base.join(Path::new("doublecheck"))
            }
            (SimulationType::Doublecheck, SimulationPhase::Shrink) => {
                self.base.join(Path::new("doublecheck_shrink"))
            }
        }
    }

    pub(crate) fn db(&self, type_: &SimulationType, phase: &SimulationPhase) -> PathBuf {
        self.path_(type_, phase).with_extension("db")
    }

    pub(crate) fn aux_db(
        &self,
        type_: &SimulationType,
        phase: &SimulationPhase,
        name: &str,
    ) -> PathBuf {
        self.path_(type_, phase)
            .with_extension(format!("{name}.db"))
    }

    pub(crate) fn plan(&self, type_: &SimulationType, phase: &SimulationPhase) -> PathBuf {
        self.path_(type_, phase).with_extension("sql")
    }

    pub fn delete_all_files(&self) {
        if self.base.exists() {
            let res = std::fs::remove_dir_all(&self.base);
            if res.is_err() {
                tracing::error!(error = %res.unwrap_err(),"failed to remove directory");
            }
        }
    }
}
