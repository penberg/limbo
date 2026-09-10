use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};

use crate::{
    index_method::{btree_root_page, IndexMethodContext, BACKING_BTREE_INDEX_METHOD_NAME},
    mvcc::{cursor::MvccCursorType, database::MVTableId},
    return_if_io,
    schema::{Type, TURSO_INTERNAL_PREFIX},
    storage::btree::{BTreeCursor, CursorTrait},
    types::{IOResult, IOResultOr, IndexInfo, KeyInfo},
    util::quote_identifier,
    Connection, LimboError, MvCursor, MvStore, Result,
};

impl IndexMethodContext {
    /// Find the backing store that is `index`. Returns `None` if the index
    /// is not in the schema yet.
    pub fn backing_store(&self, index: &BackingIndex) -> Result<Option<BackingStore>> {
        BackingStore::lookup(&self.connection()?, self.database().id, index)
    }

    /// Create the tables of `schema`, then its indexes, in order. An object
    /// that already exists is skipped.
    pub fn create_backing_schema(&self, schema: &BackingSchema) -> Result<BackingStoreOp> {
        let connection = self.connection()?;
        let database_id = self.database().id;
        let db_prefix = database_prefix(&connection, database_id);
        let mut statements = Vec::new();
        for table in &schema.tables {
            if !table.exists(&connection, database_id) {
                statements.push(table.create_statement(&db_prefix));
            }
        }
        for index in &schema.indexes {
            if !index.exists(&connection, database_id) {
                statements.push(index.create_statement(&db_prefix));
            }
        }
        Ok(BackingStoreOp::new(&connection, statements))
    }

    /// Drop the indexes of `schema`, then its tables, in order. Dropping a
    /// table also drops the indexes on it.
    pub fn drop_backing_schema(&self, schema: &BackingSchema) -> Result<BackingStoreOp> {
        let connection = self.connection()?;
        let database_id = self.database().id;
        let db_prefix = database_prefix(&connection, database_id);
        let statements = schema
            .indexes
            .iter()
            .map(|index| index.drop_statement(&db_prefix))
            .chain(
                schema
                    .tables
                    .iter()
                    .map(|table| table.drop_statement(&db_prefix)),
            )
            .collect::<Vec<_>>();
        Ok(BackingStoreOp::new(&connection, statements))
    }
}

fn database_prefix(connection: &Connection, database_id: usize) -> String {
    connection
        .get_database_name_by_index(database_id)
        .filter(|name| name != "main")
        .map(|name| format!("{}.", quote_identifier(&name)))
        .unwrap_or_default()
}

/// The tables and indexes that an index method owns. Core creates and drops
/// them as one unit, and gives a `BackingStore` handle for each index.
#[derive(Debug, Clone, Default)]
pub struct BackingSchema {
    pub tables: Vec<BackingTable>,
    pub indexes: Vec<BackingIndex>,
}

impl BackingSchema {
    pub fn new(tables: Vec<BackingTable>, indexes: Vec<BackingIndex>) -> Self {
        Self { tables, indexes }
    }
}

/// A table that core creates for an index method. Its name in the schema
/// is `__turso_internal_<name>`.
#[derive(Debug, Clone)]
pub struct BackingTable {
    pub name: String,
    pub columns: Vec<BackingColumn>,
}

impl BackingTable {
    pub fn new(name: impl Into<String>, columns: Vec<BackingColumn>) -> Self {
        Self {
            name: name.into(),
            columns,
        }
    }

    /// The name of the table in the schema.
    pub fn table_name(&self) -> String {
        format!("{TURSO_INTERNAL_PREFIX}{}", self.name)
    }

    fn exists(&self, connection: &Connection, database_id: usize) -> bool {
        let table = self.table_name();
        connection.with_schema(database_id, |schema| {
            schema.get_btree_table(&table).is_some()
        })
    }

    fn create_statement(&self, db_prefix: &str) -> String {
        let column_definitions = self
            .columns
            .iter()
            .map(|column| {
                format!(
                    "{} {} NOT NULL",
                    quote_identifier(&column.name),
                    column.column_type
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "CREATE TABLE IF NOT EXISTS {db_prefix}{} ({column_definitions})",
            quote_identifier(&self.table_name())
        )
    }

    fn drop_statement(&self, db_prefix: &str) -> String {
        format!(
            "DROP TABLE IF EXISTS {db_prefix}{}",
            quote_identifier(&self.table_name())
        )
    }
}

/// A `backing_btree` index that an index method owns. The table is either a
/// `BackingTable` or any existing table. `columns` are columns of that
/// table. `keys` is the key layout of the records that the method writes
/// through the index, which can differ from the declared columns.
#[derive(Debug, Clone)]
pub struct BackingIndex {
    pub table: String,
    pub name: String,
    pub columns: Vec<String>,
    pub keys: Vec<KeyInfo>,
}

impl BackingIndex {
    /// An index on a table that core created for the method.
    pub fn on_backing_table(
        table: &BackingTable,
        name: impl Into<String>,
        columns: Vec<String>,
        keys: Vec<KeyInfo>,
    ) -> Self {
        Self {
            table: table.table_name(),
            name: name.into(),
            columns,
            keys,
        }
    }

    /// An index on any existing table.
    pub fn on_table(
        table: impl Into<String>,
        name: impl Into<String>,
        columns: Vec<String>,
        keys: Vec<KeyInfo>,
    ) -> Self {
        Self {
            table: table.into(),
            name: name.into(),
            columns,
            keys,
        }
    }

    fn exists(&self, connection: &Connection, database_id: usize) -> bool {
        connection.with_schema(database_id, |schema| {
            schema.get_index(&self.table, &self.name).is_some()
        })
    }

    fn create_statement(&self, db_prefix: &str) -> String {
        let column_names = self
            .columns
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "CREATE INDEX IF NOT EXISTS {db_prefix}{} ON {} \
             USING {BACKING_BTREE_INDEX_METHOD_NAME} ({column_names})",
            quote_identifier(&self.name),
            quote_identifier(&self.table)
        )
    }

    fn drop_statement(&self, db_prefix: &str) -> String {
        format!(
            "DROP INDEX IF EXISTS {db_prefix}{}",
            quote_identifier(&self.name)
        )
    }
}

/// A key column of a backing store.
#[derive(Debug, Clone)]
pub struct BackingColumn {
    pub name: String,
    pub column_type: Type,
}

impl BackingColumn {
    pub fn new(name: impl Into<String>, column_type: Type) -> Self {
        Self {
            name: name.into(),
            column_type,
        }
    }
}

/// A key-only B-tree (a B-tree whose rows are only keys, with no separate
/// row data) that core owns for an index method.
///
/// The method writes every row through cursors that this handle opens.
/// Under MVCC, those rows are versioned rows that belong to the transaction
/// and the snapshot that resolved the handle. The method never sees a root
/// page or an MVCC table id.
pub struct BackingStore {
    connection: Weak<Connection>,
    database_id: usize,
    table_name: String,
    root_page: i64,
    index_info: Arc<IndexInfo>,
    mvcc: Option<BackingStoreMvccBinding>,
}

struct BackingStoreMvccBinding {
    mv_store: Arc<MvStore>,
    tx_id: u64,
    table_id: MVTableId,
}

impl std::fmt::Debug for BackingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BackingStore")
            .field("database_id", &self.database_id)
            .field("table_name", &self.table_name)
            .field("root_page", &self.root_page)
            .field("mvcc", &self.mvcc.as_ref().map(|binding| binding.table_id))
            .finish()
    }
}

impl BackingStore {
    pub(crate) fn lookup(
        connection: &Arc<Connection>,
        database_id: usize,
        index: &BackingIndex,
    ) -> Result<Option<Self>> {
        let table_name = index.table.clone();
        let index_name = index.name.clone();
        let keys = &index.keys;
        let Some(index) = connection.with_schema(database_id, |schema| {
            schema.get_index(&table_name, &index_name).cloned()
        }) else {
            return Ok(None);
        };
        let index_info = Arc::new(IndexInfo::new(
            keys.iter().cloned(),
            false,
            keys.len(),
            index.unique,
        )?);
        let mvcc = match connection.mv_store_for_db(database_id) {
            None => None,
            Some(mv_store) => {
                let tx_id = connection.get_mv_tx_id_for_db(database_id).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "backing store {index_name} resolved without an active MVCC transaction"
                    ))
                })?;
                let snapshot_ts = mv_store.read_snapshot_ts(tx_id);
                let table_id = if connection.experimental_mvcc_passive_checkpoint_enabled() {
                    mv_store
                        .try_get_table_id_from_root_page_at(index.root_page, snapshot_ts)
                        .ok_or(LimboError::SchemaUpdated)?
                } else {
                    mv_store.get_table_id_from_root_page_at(index.root_page, snapshot_ts)
                };
                Some(BackingStoreMvccBinding {
                    mv_store,
                    tx_id,
                    table_id,
                })
            }
        };
        Ok(Some(Self {
            connection: Arc::downgrade(connection),
            database_id,
            table_name,
            root_page: index.root_page,
            index_info,
            mvcc,
        }))
    }

    /// Open a cursor over the rows of the store. Under MVCC, the cursor
    /// reads the snapshot of the transaction and writes versioned rows.
    pub fn open_cursor(&self) -> Result<Box<dyn CursorTrait>> {
        let connection = self.connection()?;
        let pager = connection.get_pager_from_database_index(&self.database_id)?;
        let mut cursor = BTreeCursor::new(
            pager,
            btree_root_page(&connection, self.database_id, self.root_page),
            self.index_info.num_cols,
        );
        cursor.index_info = Some(Arc::clone(&self.index_info));
        let Some(binding) = &self.mvcc else {
            return Ok(Box::new(cursor));
        };
        Ok(Box::new(MvCursor::new(
            Arc::clone(&binding.mv_store),
            &connection,
            binding.tx_id,
            self.root_page,
            MvccCursorType::Index(Arc::clone(&self.index_info)),
            Box::new(cursor),
        )?))
    }

    /// Under MVCC, take the maintenance lease of this store for the
    /// transaction of the handle. The owner can take it again. If another
    /// transaction holds it, the result is `Busy`. If the snapshot is older
    /// than the last publication, the result is `WriteWriteConflict`. In
    /// WAL mode this does nothing, because the pager write lock already
    /// serializes writers.
    pub fn acquire_maintenance_lease(&self) -> Result<()> {
        let Some(binding) = &self.mvcc else {
            return Ok(());
        };
        binding
            .mv_store
            .acquire_index_method_write_lease(binding.tx_id, binding.table_id)
    }

    pub(crate) fn register_deleter(&self) -> Result<()> {
        let Some(binding) = &self.mvcc else {
            return Ok(());
        };
        binding
            .mv_store
            .register_index_method_deleter(binding.tx_id, binding.table_id)
    }

    pub(crate) fn check_merge_admissible(&self) -> Result<()> {
        let Some(binding) = &self.mvcc else {
            return Ok(());
        };
        binding
            .mv_store
            .check_index_method_merge_admissible(binding.tx_id, binding.table_id)
    }

    /// The schema root of the B-tree of the store. The value is negative
    /// for an MVCC table that was not checkpointed yet.
    pub fn root_page(&self) -> i64 {
        self.root_page
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    fn connection(&self) -> Result<Arc<Connection>> {
        self.connection.upgrade().ok_or_else(|| {
            LimboError::InternalError("backing store handle outlived its connection".to_string())
        })
    }
}

/// A create or a drop of a backing store that is in progress. Step it until
/// it returns `Done`. It gives its I/O to the caller instead of running the
/// I/O inside the opcode.
pub struct BackingStoreOp {
    ddl: Option<NestedDdl>,
}

impl BackingStoreOp {
    fn new(connection: &Arc<Connection>, statements: Vec<String>) -> Self {
        Self {
            ddl: (!statements.is_empty()).then(|| NestedDdl::new(connection, statements)),
        }
    }

    pub fn step(&mut self) -> IOResultOr<()> {
        let Some(ddl) = self.ddl.as_mut() else {
            return Ok(IOResult::Done(()));
        };
        return_if_io!(ddl.step());
        self.ddl = None;
        Ok(IOResult::Done(()))
    }
}

impl std::fmt::Debug for BackingStoreOp {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BackingStoreOp")
            .field("pending", &self.ddl.is_some())
            .finish()
    }
}

/// Nested DDL statements that run for an index method. They are stepped one
/// at a time so that their I/O reaches the caller. The connection is nested
/// only while one statement is stepped or dropped. The nested flag tells the
/// pager that the `Halt` and the reset of the statement must not finish the
/// transaction of the parent. The flag must not leak to other statements
/// that run on this connection while the parent waits at a yield.
struct NestedDdl {
    connection: Weak<Connection>,
    /// SQL that is still to run, in order. Each statement is prepared only
    /// when its turn comes, because a later statement can depend on schema
    /// that an earlier one creates (the backing index on the backing table).
    pending: VecDeque<String>,
    current: Option<crate::Statement>,
}

impl NestedDdl {
    fn new(connection: &Arc<Connection>, statements: impl IntoIterator<Item = String>) -> Self {
        Self {
            connection: Arc::downgrade(connection),
            pending: statements.into_iter().collect(),
            current: None,
        }
    }

    fn step(&mut self) -> IOResultOr<()> {
        let connection = self.connection.upgrade().ok_or_else(|| {
            LimboError::InternalError("backing store DDL outlived its connection".into())
        })?;
        loop {
            if self.current.is_none() {
                let Some(sql) = self.pending.pop_front() else {
                    return Ok(IOResult::Done(()));
                };
                self.current = Some(Self::prepare(&connection, sql)?);
            }
            let statement = self.current.as_mut().expect("prepared above");
            connection.start_nested();
            let result = statement.run_ignore_rows_nonblock();
            if !matches!(result, Ok(IOResult::IO(_))) {
                // Drop a finished or failed statement while the connection
                // is still nested. The reset of the statement reads
                // `is_nested_stmt()`.
                self.current = None;
            }
            connection.end_nested();
            return_if_io!(result);
        }
    }

    /// Prepare `sql` as a nested statement. Top-level statements are not
    /// permitted to use `__turso_internal_` names. No statement
    /// subtransaction is started, because the transaction of the parent
    /// statement covers it. A subtransaction here fails with DatabaseBusy.
    fn prepare(connection: &Arc<Connection>, sql: String) -> Result<crate::Statement> {
        connection.start_nested();
        let statement = connection.prepare(sql);
        connection.end_nested();
        let statement = statement?;
        statement
            .program
            .prepared
            .needs_stmt_subtransactions
            .store(false, Ordering::Relaxed);
        Ok(statement)
    }
}

impl Drop for NestedDdl {
    fn drop(&mut self) {
        // A statement that was abandoned in progress (the parent statement
        // was reset) is dropped while nested, for the same reason as a
        // finished one.
        if self.current.is_none() {
            return;
        }
        let Some(connection) = self.connection.upgrade() else {
            self.current = None;
            return;
        };
        connection.start_nested();
        self.current = None;
        connection.end_nested();
    }
}
