use crate::numeric::Numeric;
use crate::sync::Arc;
use crate::sync::Mutex;
use crate::types::IOResultOr;
use crate::{
    incremental::{
        compiler::{DeltaSet, ExecuteState},
        dbsp::{Delta, HashableRow, RowKeyZSet},
        view::{IncrementalView, ViewTransactionState},
    },
    return_if_io,
    storage::btree::CursorTrait,
    types::{IOResult, SeekKey, SeekOp, SeekResult, Value},
    LimboError, Pager, Result,
};

/// State machine for seek operations
#[derive(Debug)]
enum SeekState {
    /// Initial state before seeking
    Init,

    /// Actively seeking with btree and uncommitted iterators
    Seek {
        /// The row we are trying to find
        target: i64,
    },

    /// Btree seek returned TryAdvance, now advancing with next()/prev()
    Advancing {
        /// The row we are trying to find
        target: i64,
        /// The seek operation (determines direction of advance)
        op: SeekOp,
    },

    /// Seek completed successfully
    Done,
}

/// Cursor for reading materialized views that combines:
/// 1. Persistent btree data (committed state)
/// 2. Transaction-specific DBSP deltas (uncommitted changes)
///
/// Works like a regular table cursor - reads from disk on-demand
/// and overlays transaction changes as needed.
pub struct MaterializedViewCursor {
    // Core components
    btree_cursor: Box<dyn CursorTrait>,
    view: Arc<Mutex<IncrementalView>>,
    pager: Arc<Pager>,

    // Current changes that are uncommitted
    uncommitted: RowKeyZSet,

    // Reference to shared transaction state for this specific view - shared with Connection
    tx_state: Arc<ViewTransactionState>,

    // The transaction state always grows. It never gets reduced. That is in the very nature of
    // DBSP, because deletions are just appends with weight < 0. So we will use the length of the
    // state to check if we have to recompute the transaction state
    last_tx_state_len: usize,

    // Current row cache - only cache the current row we're looking at
    current_row: Option<(i64, Vec<Value>)>,

    // Execution state for circuit processing
    execute_state: ExecuteState,

    // State machine for seek operations
    seek_state: SeekState,
}

impl MaterializedViewCursor {
    pub fn new(
        btree_cursor: Box<dyn CursorTrait>,
        view: Arc<Mutex<IncrementalView>>,
        pager: Arc<Pager>,
        tx_state: Arc<ViewTransactionState>,
    ) -> Result<Self> {
        Ok(Self {
            btree_cursor,
            view,
            pager,
            uncommitted: RowKeyZSet::new(),
            tx_state,
            last_tx_state_len: 0,
            current_row: None,
            execute_state: ExecuteState::Uninitialized,
            seek_state: SeekState::Init,
        })
    }

    /// Compute transaction changes lazily on first access
    fn ensure_tx_changes_computed(&mut self) -> IOResultOr<()> {
        // Check if we've already processed the current state
        let current_len = self.tx_state.len();
        if current_len == self.last_tx_state_len {
            return Ok(IOResult::Done(()));
        }

        // Get the view and the current transaction state
        let mut view_guard = self.view.lock();
        let table_deltas = self.tx_state.get_table_deltas();

        // Process the deltas through the circuit to get materialized changes
        let mut uncommitted = DeltaSet::new();
        for (table_name, delta) in table_deltas {
            uncommitted.insert(table_name, delta);
        }

        let processed_delta = return_if_io!(view_guard.execute_with_uncommitted(
            uncommitted,
            self.pager.clone(),
            &mut self.execute_state
        ));

        self.uncommitted = RowKeyZSet::from_delta(&processed_delta);
        self.last_tx_state_len = current_len;
        Ok(IOResult::Done(()))
    }

    // Read the current btree entry as a vector (empty if no current position)
    fn read_btree_delta_entry(&mut self) -> IOResultOr<Vec<(HashableRow, isize)>> {
        let btree_rowid = return_if_io!(self.btree_cursor.rowid());
        let rowid = match btree_rowid {
            None => return Ok(IOResult::Done(Vec::new())),
            Some(rowid) => rowid,
        };

        let btree_record = return_if_io!(self.btree_cursor.record()).ok_or_else(|| {
            crate::LimboError::InternalError(
                "Invalid data in materialized view: found a rowid, but not the row!".to_string(),
            )
        })?;
        let mut btree_values = btree_record.get_values_owned()?;

        // The last column should be the weight
        let weight_value = btree_values.pop().ok_or_else(|| {
            crate::LimboError::InternalError(
                "Invalid data in materialized view: no weight column found".to_string(),
            )
        })?;

        // Convert the Value to isize weight
        let weight = match weight_value {
            Value::Numeric(Numeric::Integer(w)) => w as isize,
            _ => {
                return Err(crate::LimboError::InternalError(format!(
                    "Invalid data in materialized view: expected integer weight, found {weight_value:?}"
                )).into())
            }
        };

        if weight <= 0 {
            return Err(crate::LimboError::InternalError(format!(
                "Invalid data in materialized view: expected a positive weight, found {weight}"
            ))
            .into());
        }

        // TODO: std boundary conversion; adjust once incremental uses the
        // allocator with fallible allocations everywhere.
        Ok(IOResult::Done(vec![(
            HashableRow::new(rowid, btree_values.into_iter().collect()),
            weight,
        )]))
    }

    /// Process btree changes: merge with uncommitted, build zset, and determine result.
    /// Returns the next state action: either Done with a result, or updates seek_state for another iteration.
    fn process_btree_changes(
        &mut self,
        target: i64,
        target_rowid: i64,
        op: SeekOp,
        changes: Vec<(HashableRow, isize)>,
    ) -> IOResultOr<()> {
        let mut btree_entries = Delta { changes };
        let changes = self.uncommitted.seek(target, op);

        let uncommitted_entries = Delta { changes };
        btree_entries.merge(&uncommitted_entries);

        // if empty pre-zset, means nothing was found. Empty post-zset can mean that
        // we just canceled weights.
        if btree_entries.is_empty() {
            self.seek_state = SeekState::Done;
            return Ok(IOResult::Done(()));
        }

        let min_seen = btree_entries
            .changes
            .first()
            .expect("cannot be empty, we just tested for it")
            .0
            .rowid;
        let max_seen = btree_entries
            .changes
            .last()
            .expect("cannot be empty, we just tested for it")
            .0
            .rowid;

        let zset = RowKeyZSet::from_delta(&btree_entries);
        let ret = zset.seek(target_rowid, op);

        if !ret.is_empty() {
            let (row, _) = &ret[0];
            self.current_row = Some((row.rowid, row.values.clone()));
            self.seek_state = SeekState::Done;
            return Ok(IOResult::Done(()));
        }

        let new_target = match op {
            SeekOp::GT => Some(max_seen),
            SeekOp::GE { eq_only: false } => Some(max_seen + 1),
            SeekOp::LT => Some(min_seen),
            SeekOp::LE { eq_only: false } => Some(min_seen - 1),
            SeekOp::LE { eq_only: true } | SeekOp::GE { eq_only: true } => None,
        };

        if let Some(target) = new_target {
            self.seek_state = SeekState::Seek { target };
        } else {
            self.seek_state = SeekState::Done;
        }
        Ok(IOResult::Done(()))
    }

    /// Internal seek implementation that doesn't check preconditions
    fn do_seek(&mut self, target_rowid: i64, op: SeekOp) -> IOResultOr<SeekResult> {
        loop {
            // Process state machine - need to handle mutable borrow carefully
            match &mut self.seek_state {
                SeekState::Init => {
                    self.current_row = None;
                    self.seek_state = SeekState::Seek {
                        target: target_rowid,
                    };
                }
                SeekState::Seek { target } => {
                    let target = *target;
                    let btree_result =
                        return_if_io!(self.btree_cursor.seek(SeekKey::TableRowId(target), op));

                    let changes = match btree_result {
                        SeekResult::Found => return_if_io!(self.read_btree_delta_entry()),
                        SeekResult::TryAdvance => {
                            // Transition to Advancing state before calling next/prev.
                            // This ensures that if next/prev returns IO, we resume in
                            // Advancing state and don't redundantly call seek again.
                            self.seek_state = SeekState::Advancing { target, op };
                            continue;
                        }
                        SeekResult::NotFound => Vec::new(),
                    };

                    return_if_io!(self.process_btree_changes(target, target_rowid, op, changes));

                    // Check if we're done or need to continue seeking
                    if matches!(self.seek_state, SeekState::Done) {
                        let result = if self.current_row.is_some() {
                            SeekResult::Found
                        } else {
                            SeekResult::NotFound
                        };
                        return Ok(IOResult::Done(result));
                    }
                    // Otherwise state is Seek with new target, loop continues
                }
                SeekState::Advancing { target, op } => {
                    let target = *target;
                    let op = *op;

                    // Cursor is positioned at the leaf but current entry doesn't match.
                    // Advance in the appropriate direction to find the next matching entry.
                    match op {
                        SeekOp::GT | SeekOp::GE { .. } => {
                            return_if_io!(self.btree_cursor.next())
                        }
                        SeekOp::LT | SeekOp::LE { .. } => {
                            return_if_io!(self.btree_cursor.prev())
                        }
                    };
                    // read_btree_delta_entry handles the case where cursor is at end
                    let changes = return_if_io!(self.read_btree_delta_entry());

                    return_if_io!(self.process_btree_changes(target, target_rowid, op, changes));

                    // Check if we're done or need to continue seeking
                    if matches!(self.seek_state, SeekState::Done) {
                        let result = if self.current_row.is_some() {
                            SeekResult::Found
                        } else {
                            SeekResult::NotFound
                        };
                        return Ok(IOResult::Done(result));
                    }
                    // Otherwise state is Seek with new target, loop continues
                }
                SeekState::Done => {
                    // We always return before setting the state to done. Meaning if we got here,
                    // this is a new seek.
                    self.seek_state = SeekState::Init;
                }
            }
        }
    }

    pub fn seek(&mut self, key: SeekKey, op: SeekOp) -> IOResultOr<SeekResult> {
        // Ensure transaction changes are computed
        return_if_io!(self.ensure_tx_changes_computed());

        let target_rowid = match &key {
            SeekKey::TableRowId(rowid) => *rowid,
            SeekKey::IndexKey(_) => {
                return Err(LimboError::ParseError(
                    "Cannot search a materialized view with an index key".to_string(),
                )
                .into());
            }
        };

        self.do_seek(target_rowid, op)
    }

    pub fn next(&mut self) -> IOResultOr<bool> {
        // If there's a pending seek operation (due to IO), complete it first.
        // SeekState::Seek or SeekState::Advancing means IO was interrupted mid-seek and we need to resume.
        // SeekState::Init means cursor was never positioned - don't resume, fall through to check current_row.
        if matches!(
            self.seek_state,
            SeekState::Seek { .. } | SeekState::Advancing { .. }
        ) {
            // target is ignored when resuming
            let result = return_if_io!(self.do_seek(0, SeekOp::GT));
            return Ok(IOResult::Done(result == SeekResult::Found));
        }

        // If cursor is not positioned (no current_row), return false
        // This matches BTreeCursor behavior when valid_state == Invalid
        let Some((current_rowid, _)) = &self.current_row else {
            return Ok(IOResult::Done(false));
        };

        // Use GT to find the next row after current position
        let result = return_if_io!(self.do_seek(*current_rowid, SeekOp::GT));
        Ok(IOResult::Done(result == SeekResult::Found))
    }

    pub fn column(&mut self, col: usize) -> IOResultOr<Value> {
        if let Some((_, ref values)) = self.current_row {
            Ok(IOResult::Done(
                values.get(col).cloned().unwrap_or(Value::Null),
            ))
        } else {
            Ok(IOResult::Done(Value::Null))
        }
    }

    pub fn rowid(&self) -> IOResultOr<Option<i64>> {
        Ok(IOResult::Done(self.current_row.as_ref().map(|(id, _)| *id)))
    }

    pub fn rewind(&mut self) -> IOResultOr<()> {
        return_if_io!(self.ensure_tx_changes_computed());
        // Seek GT from i64::MIN to find the first row using internal do_seek
        let _result = return_if_io!(self.do_seek(i64::MIN, SeekOp::GT));
        Ok(IOResult::Done(()))
    }

    pub fn is_valid(&self) -> Result<bool> {
        Ok(self.current_row.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::btree::BTreeCursor;
    use crate::sync::Arc;
    use crate::util::IOExt;
    use crate::SqliteDialect;
    use crate::{Connection, Database, OpenFlags};

    /// Helper to create a test connection with a table and materialized view
    fn create_test_connection() -> Result<Arc<Connection>> {
        // Create an in-memory database with experimental views enabled
        let io = Arc::new(crate::io::MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            ":memory:",
            OpenFlags::default(),
            crate::DatabaseOpts {
                enable_views: true,
                enable_custom_types: false,
                enable_load_extension: false,
                enable_encryption: false,
                enable_index_method: false,
                enable_autovacuum: false,
                enable_vacuum: false,
                enable_attach: false,
                enable_generated_columns: false,
                enable_multiprocess_wal: false,
                enable_without_rowid: false,
                enable_experimental_mvcc_passive_checkpoint: false,
                unsafe_testing: false,
            },
            None,
            Arc::new(SqliteDialect),
        )?;
        let conn = db.connect()?;

        // Create a test table
        conn.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, value INTEGER)")?;

        // Create materialized view
        conn.execute("CREATE MATERIALIZED VIEW test_view AS SELECT id, value FROM test_table")?;

        Ok(conn)
    }

    /// Helper to create a test cursor for the materialized view
    fn create_test_cursor(
        conn: &Arc<Connection>,
    ) -> Result<(
        MaterializedViewCursor,
        Arc<ViewTransactionState>,
        Arc<Pager>,
    )> {
        // Get the schema and view
        let view_mutex = conn
            .schema
            .read()
            .get_materialized_view("test_view")
            .ok_or_else(|| crate::LimboError::InternalError("View not found".to_string()))?;

        // Get the view's root page
        let view = view_mutex.lock();
        let root_page = view.get_root_page();
        if root_page == 0 {
            return Err(crate::LimboError::InternalError(
                "View not materialized".to_string(),
            ));
        }
        let num_columns = view.column_schema.columns.len();
        drop(view);

        // Create a btree cursor
        let pager = conn.get_pager();
        let btree_cursor = Box::new(BTreeCursor::new(pager.clone(), root_page, num_columns));

        // Get or create transaction state for this view
        let tx_state = conn.view_transaction_states.get_or_create("test_view");

        // Create the materialized view cursor
        let cursor = MaterializedViewCursor::new(
            btree_cursor,
            view_mutex.clone(),
            pager.clone(),
            tx_state.clone(),
        )?;

        Ok((cursor, tx_state, pager))
    }

    /// Helper to populate test table with data through SQL
    fn populate_test_table(conn: &Arc<Connection>, rows: Vec<(i64, i64)>) -> Result<()> {
        for (id, value) in rows {
            let sql = format!("INSERT INTO test_table (id, value) VALUES ({id}, {value})");
            conn.execute(&sql)?;
        }
        Ok(())
    }

    /// Helper to apply changes through ViewTransactionState
    fn apply_changes_to_tx_state(
        tx_state: &ViewTransactionState,
        changes: Vec<(i64, Vec<Value>, isize)>,
    ) {
        for (rowid, values, weight) in changes {
            if weight > 0 {
                tx_state.insert("test_table", rowid, values);
            } else if weight < 0 {
                tx_state.delete("test_table", rowid, values);
            }
        }
    }

    #[test]
    fn test_seek_key_exists_in_btree() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with test data: rows 1, 3, 5, 7
        populate_test_table(&conn, vec![(1, 10), (3, 30), (5, 50), (7, 70)])?;

        // Create cursor for testing
        let (mut cursor, _tx_state, pager) = create_test_cursor(&conn)?;

        // No uncommitted changes - tx_state is already empty

        // Test 1: Seek exact match (row 3)
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(3), SeekOp::GE { eq_only: true }))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));

        // Test 2: Seek GE (row 4 should find row 5)
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(4), SeekOp::GE { eq_only: false }))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(5));

        // Test 3: Seek GT (row 3 should find row 5)
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(3), SeekOp::GT))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(5));

        // Test 4: Seek LE (row 4 should find row 3)
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(4), SeekOp::LE { eq_only: false }))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));

        // Test 5: Seek LT (row 5 should find row 3)
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(5), SeekOp::LT))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));

        Ok(())
    }

    #[test]
    fn test_seek_key_exists_only_uncommitted() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 1, 5, 7
        populate_test_table(&conn, vec![(1, 10), (5, 50), (7, 70)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Add uncommitted changes: insert rows 3 and 6
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (3, vec![Value::from_i64(3), Value::from_i64(30)], 1), // Insert row 3
                (6, vec![Value::from_i64(6), Value::from_i64(60)], 1), // Insert row 6
            ],
        );

        // Test 1: Seek exact match for uncommitted row 3
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(3), SeekOp::GE { eq_only: true }))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));
        assert_eq!(pager.io.block(|| cursor.column(1))?, Value::from_i64(30));

        // Test 2: Seek GE for row 2 should find uncommitted row 3
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(2), SeekOp::GE { eq_only: false }))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));

        // Test 3: Seek GT for row 5 should find uncommitted row 6
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(5), SeekOp::GT))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(6));
        assert_eq!(pager.io.block(|| cursor.column(1))?, Value::from_i64(60));

        // Test 4: Seek LE for row 6 should find uncommitted row 6
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(6), SeekOp::LE { eq_only: false }))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(6));

        Ok(())
    }

    #[test]
    fn test_seek_key_deleted_by_uncommitted() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 1, 3, 5, 7
        populate_test_table(&conn, vec![(1, 10), (3, 30), (5, 50), (7, 70)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Delete row 3 and 5 in uncommitted changes
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (3, vec![Value::from_i64(3), Value::from_i64(30)], -1), // Delete row 3
                (5, vec![Value::from_i64(5), Value::from_i64(50)], -1), // Delete row 5
            ],
        );

        // Test 1: Seek exact match for deleted row 3 should not find it
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(3), SeekOp::GE { eq_only: true }))?;
        assert_eq!(result, SeekResult::NotFound);

        // Test 2: Seek GE for row 2 should skip deleted row 3 and find row 7
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(2), SeekOp::GE { eq_only: false }))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(7));

        // Test 3: Seek GT for row 1 should skip deleted rows and find row 7
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(1), SeekOp::GT))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(7));

        // Test 4: Seek LE for row 5 should find row 1 (skipping deleted 3 and 5)
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(5), SeekOp::LE { eq_only: false }))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(1));

        Ok(())
    }

    #[test]
    fn test_seek_with_updates() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 1, 3, 5
        populate_test_table(&conn, vec![(1, 10), (3, 30), (5, 50)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Update row 3 (delete old + insert new)
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (3, vec![Value::from_i64(3), Value::from_i64(30)], -1), // Delete old row 3
                (3, vec![Value::from_i64(3), Value::from_i64(35)], 1),  // Insert new row 3
            ],
        );

        // Test: Seek for updated row 3 should find it
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(3), SeekOp::GE { eq_only: true }))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));
        // The values should be from the uncommitted set (35 instead of 30)
        assert_eq!(pager.io.block(|| cursor.column(1))?, Value::from_i64(35));

        Ok(())
    }

    #[test]
    fn test_seek_boundary_conditions() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 5, 10
        populate_test_table(&conn, vec![(5, 50), (10, 100)])?;

        // Create cursor for testing
        let (mut cursor, _tx_state, pager) = create_test_cursor(&conn)?;

        // No uncommitted changes - tx_state is already empty

        // Test 1: Seek LT for minimum value (should find nothing)
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(1), SeekOp::LT))?;
        assert_eq!(result, SeekResult::NotFound);

        // Test 2: Seek GT for maximum value (should find nothing)
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(15), SeekOp::GT))?;
        assert_eq!(result, SeekResult::NotFound);

        // Test 3: Seek exact for non-existent key
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(7), SeekOp::GE { eq_only: true }))?;
        assert_eq!(result, SeekResult::NotFound);

        Ok(())
    }

    #[test]
    fn test_seek_complex_uncommitted_weights() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with row 5
        populate_test_table(&conn, vec![(5, 50)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Complex uncommitted changes with multiple operations on same row
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (5, vec![Value::from_i64(5), Value::from_i64(50)], -1), // Delete original
                (5, vec![Value::from_i64(5), Value::from_i64(51)], 1),  // Insert update 1
                (5, vec![Value::from_i64(5), Value::from_i64(51)], -1), // Delete update 1
                (5, vec![Value::from_i64(5), Value::from_i64(52)], 1),  // Insert update 2
                                                                        // Net effect: row 5 exists with value 52
            ],
        );

        // Seek for row 5 should find it (net weight = 1 from btree + 0 from uncommitted = 1)
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(5), SeekOp::GE { eq_only: true }))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(5));
        // The final value should be 52 from the last update
        assert_eq!(pager.io.block(|| cursor.column(1))?, Value::from_i64(52));

        Ok(())
    }

    #[test]
    fn test_seek_affected_by_transaction_state_changes() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 1 and 3
        populate_test_table(&conn, vec![(1, 10), (3, 30)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Seek for row 2 - doesn't exist
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(2), SeekOp::GE { eq_only: true }))?;
        assert_eq!(result, SeekResult::NotFound);

        // Add row 2 to uncommitted
        tx_state.insert(
            "test_table",
            2,
            vec![Value::from_i64(2), Value::from_i64(20)],
        );

        // Now seek for row 2 finds it
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(2), SeekOp::GE { eq_only: true }))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(2));
        assert_eq!(pager.io.block(|| cursor.column(1))?, Value::from_i64(20));

        Ok(())
    }

    #[test]
    fn test_rewind_btree_first_uncommitted_later() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 1, 3, 5
        populate_test_table(&conn, vec![(1, 10), (3, 30), (5, 50)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Add uncommitted rows 8, 10 (all larger than btree rows)
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (8, vec![Value::from_i64(8), Value::from_i64(80)], 1),
                (10, vec![Value::from_i64(10), Value::from_i64(100)], 1),
            ],
        );

        // Initially cursor is not positioned
        assert!(!cursor.is_valid()?);

        // Rewind should position at first btree row (1) since uncommitted are all larger
        pager.io.block(|| cursor.rewind())?;
        assert!(cursor.is_valid()?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(1));

        Ok(())
    }

    #[test]
    fn test_rewind_with_uncommitted_first() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 5, 7
        populate_test_table(&conn, vec![(5, 50), (7, 70)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Add uncommitted row 2 (smaller than any btree row)
        apply_changes_to_tx_state(
            &tx_state,
            vec![(2, vec![Value::from_i64(2), Value::from_i64(20)], 1)],
        );

        // Rewind should position at row 2 (uncommitted)
        pager.io.block(|| cursor.rewind())?;
        assert!(cursor.is_valid()?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(2));
        assert_eq!(pager.io.block(|| cursor.column(1))?, Value::from_i64(20));

        Ok(())
    }

    #[test]
    fn test_rewind_skip_deleted_first() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 1, 3, 5
        populate_test_table(&conn, vec![(1, 10), (3, 30), (5, 50)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Delete row 1 in uncommitted
        apply_changes_to_tx_state(
            &tx_state,
            vec![(1, vec![Value::from_i64(1), Value::from_i64(10)], -1)],
        );

        // Rewind should skip deleted row 1 and position at row 3
        pager.io.block(|| cursor.rewind())?;
        assert!(cursor.is_valid()?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));

        Ok(())
    }

    #[test]
    fn test_rewind_empty_btree_with_uncommitted() -> Result<()> {
        let conn = create_test_connection()?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Add uncommitted rows (no btree data)
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (3, vec![Value::from_i64(3), Value::from_i64(30)], 1),
                (7, vec![Value::from_i64(7), Value::from_i64(70)], 1),
            ],
        );

        // Rewind should find first uncommitted row
        pager.io.block(|| cursor.rewind())?;
        assert!(cursor.is_valid()?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));
        assert_eq!(pager.io.block(|| cursor.column(1))?, Value::from_i64(30));

        Ok(())
    }

    #[test]
    fn test_rewind_all_deleted() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 2, 4
        populate_test_table(&conn, vec![(2, 20), (4, 40)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Delete all rows in uncommitted
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (2, vec![Value::from_i64(2), Value::from_i64(20)], -1),
                (4, vec![Value::from_i64(4), Value::from_i64(40)], -1),
            ],
        );

        // Rewind should find no valid rows
        pager.io.block(|| cursor.rewind())?;
        assert!(!cursor.is_valid()?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, None);

        Ok(())
    }

    #[test]
    fn test_rewind_with_updates() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 1, 3
        populate_test_table(&conn, vec![(1, 10), (3, 30)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Update row 1 (delete + insert with new value)
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (1, vec![Value::from_i64(1), Value::from_i64(10)], -1),
                (1, vec![Value::from_i64(1), Value::from_i64(15)], 1),
            ],
        );

        // Rewind should position at row 1 with updated value
        pager.io.block(|| cursor.rewind())?;
        assert!(cursor.is_valid()?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(1));
        assert_eq!(pager.io.block(|| cursor.column(1))?, Value::from_i64(15));

        Ok(())
    }

    // ===== NEXT() TEST SUITE =====

    #[test]
    fn test_next_btree_only_sequential() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 1, 3, 5, 7
        populate_test_table(&conn, vec![(1, 10), (3, 30), (5, 50), (7, 70)])?;

        // Create cursor for testing
        let (mut cursor, _tx_state, pager) = create_test_cursor(&conn)?;

        // Start with rewind to position at first row
        pager.io.block(|| cursor.rewind())?;
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(1));

        // Next should move to row 3
        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));

        // Next should move to row 5
        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(5));

        // Next should move to row 7
        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(7));

        // Next should reach end
        assert!(!pager.io.block(|| cursor.next())?);
        assert!(!cursor.is_valid()?);

        Ok(())
    }

    #[test]
    fn test_next_uncommitted_only() -> Result<()> {
        let conn = create_test_connection()?;

        // Create cursor for testing (no btree data)
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Add uncommitted rows 2, 4, 6
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (2, vec![Value::from_i64(2), Value::from_i64(20)], 1),
                (4, vec![Value::from_i64(4), Value::from_i64(40)], 1),
                (6, vec![Value::from_i64(6), Value::from_i64(60)], 1),
            ],
        );

        // Start with rewind to position at first row
        pager.io.block(|| cursor.rewind())?;
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(2));

        // Next should move to row 4
        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(4));

        // Next should move to row 6
        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(6));

        // Next should reach end
        assert!(!pager.io.block(|| cursor.next())?);
        assert!(!cursor.is_valid()?);

        Ok(())
    }

    #[test]
    fn test_next_mixed_btree_uncommitted() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 1, 5, 9
        populate_test_table(&conn, vec![(1, 10), (5, 50), (9, 90)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Add uncommitted rows 3, 7
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (3, vec![Value::from_i64(3), Value::from_i64(30)], 1),
                (7, vec![Value::from_i64(7), Value::from_i64(70)], 1),
            ],
        );

        // Should iterate in order: 1, 3, 5, 7, 9
        pager.io.block(|| cursor.rewind())?;
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(1));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(5));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(7));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(9));

        assert!(!pager.io.block(|| cursor.next())?);
        assert!(!cursor.is_valid()?);

        Ok(())
    }

    #[test]
    fn test_next_skip_deleted_rows() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 1, 2, 3, 4, 5
        populate_test_table(&conn, vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Delete rows 2 and 4 in uncommitted
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (2, vec![Value::from_i64(2), Value::from_i64(20)], -1),
                (4, vec![Value::from_i64(4), Value::from_i64(40)], -1),
            ],
        );

        // Should iterate: 1, 3, 5 (skipping deleted 2 and 4)
        pager.io.block(|| cursor.rewind())?;
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(1));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(5));

        assert!(!pager.io.block(|| cursor.next())?);
        assert!(!cursor.is_valid()?);

        Ok(())
    }

    #[test]
    fn test_next_with_updates() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 1, 3, 5
        populate_test_table(&conn, vec![(1, 10), (3, 30), (5, 50)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Update row 3 (delete old + insert new)
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (3, vec![Value::from_i64(3), Value::from_i64(30)], -1),
                (3, vec![Value::from_i64(3), Value::from_i64(35)], 1),
            ],
        );

        // Should iterate all rows with updated values
        pager.io.block(|| cursor.rewind())?;
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(1));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));
        assert_eq!(pager.io.block(|| cursor.column(1))?, Value::from_i64(35)); // Updated value

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(5));

        assert!(!pager.io.block(|| cursor.next())?);

        Ok(())
    }

    #[test]
    fn test_next_from_uninitialized() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 2, 4
        populate_test_table(&conn, vec![(2, 20), (4, 40)])?;

        // Create cursor for testing
        let (mut cursor, _tx_state, pager) = create_test_cursor(&conn)?;

        // Cursor not positioned initially
        assert!(!cursor.is_valid()?);

        // Next on uninitialized cursor should return false (matching BTreeCursor behavior)
        assert!(!pager.io.block(|| cursor.next())?);
        assert!(!cursor.is_valid()?);

        // Position cursor with rewind first
        pager.io.block(|| cursor.rewind())?;
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(2));

        // Now next should work
        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(4));

        assert!(!pager.io.block(|| cursor.next())?);

        Ok(())
    }

    #[test]
    fn test_next_empty_table() -> Result<()> {
        let conn = create_test_connection()?;

        // Create cursor for testing (empty table)
        let (mut cursor, _tx_state, pager) = create_test_cursor(&conn)?;

        // Next on empty table should return false
        assert!(!pager.io.block(|| cursor.next())?);
        assert!(!cursor.is_valid()?);

        Ok(())
    }

    #[test]
    fn test_next_all_deleted() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 1, 2, 3
        populate_test_table(&conn, vec![(1, 10), (2, 20), (3, 30)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Delete all rows
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (1, vec![Value::from_i64(1), Value::from_i64(10)], -1),
                (2, vec![Value::from_i64(2), Value::from_i64(20)], -1),
                (3, vec![Value::from_i64(3), Value::from_i64(30)], -1),
            ],
        );

        // Next should find nothing
        assert!(!pager.io.block(|| cursor.next())?);
        assert!(!cursor.is_valid()?);

        Ok(())
    }

    #[test]
    fn test_next_complex_interleaving() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 2, 4, 6, 8
        populate_test_table(&conn, vec![(2, 20), (4, 40), (6, 60), (8, 80)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Complex changes:
        // - Insert row 1
        // - Delete row 2
        // - Insert row 3
        // - Update row 4
        // - Insert row 5
        // - Delete row 6
        // - Insert row 7
        // - Keep row 8 as-is
        // - Insert row 9
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (1, vec![Value::from_i64(1), Value::from_i64(10)], 1), // Insert 1
                (2, vec![Value::from_i64(2), Value::from_i64(20)], -1), // Delete 2
                (3, vec![Value::from_i64(3), Value::from_i64(30)], 1), // Insert 3
                (4, vec![Value::from_i64(4), Value::from_i64(40)], -1), // Delete old 4
                (4, vec![Value::from_i64(4), Value::from_i64(45)], 1), // Insert new 4
                (5, vec![Value::from_i64(5), Value::from_i64(50)], 1), // Insert 5
                (6, vec![Value::from_i64(6), Value::from_i64(60)], -1), // Delete 6
                (7, vec![Value::from_i64(7), Value::from_i64(70)], 1), // Insert 7
                (9, vec![Value::from_i64(9), Value::from_i64(90)], 1), // Insert 9
            ],
        );

        // Should iterate: 1, 3, 4(updated), 5, 7, 8, 9
        pager.io.block(|| cursor.rewind())?;
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(1));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(4));
        assert_eq!(pager.io.block(|| cursor.column(1))?, Value::from_i64(45)); // Updated value

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(5));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(7));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(8));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(9));

        assert!(!pager.io.block(|| cursor.next())?);
        assert!(!cursor.is_valid()?);

        Ok(())
    }

    #[test]
    fn test_next_after_seek() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 1, 3, 5, 7, 9
        populate_test_table(&conn, vec![(1, 10), (3, 30), (5, 50), (7, 70), (9, 90)])?;

        // Create cursor for testing
        let (mut cursor, _tx_state, pager) = create_test_cursor(&conn)?;

        // Seek to row 5
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(5), SeekOp::GE { eq_only: true }))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(5));

        // Next should move to row 7
        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(7));

        // Next should move to row 9
        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(9));

        // Next should reach end
        assert!(!pager.io.block(|| cursor.next())?);

        Ok(())
    }

    #[test]
    fn test_next_multiple_weights_same_row() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with row 1
        populate_test_table(&conn, vec![(1, 10)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Multiple operations on same row:
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (1, vec![Value::from_i64(1), Value::from_i64(10)], -1), // Delete original
                (1, vec![Value::from_i64(1), Value::from_i64(11)], 1),  // Insert v1
                (1, vec![Value::from_i64(1), Value::from_i64(11)], -1), // Delete v1
                (1, vec![Value::from_i64(1), Value::from_i64(12)], 1),  // Insert v2
                (1, vec![Value::from_i64(1), Value::from_i64(12)], -1), // Delete v2
                                                                        // Net weight: 1 (btree) - 1 + 1 - 1 + 1 - 1 = 0 (row deleted)
            ],
        );

        // Row should be deleted
        assert!(!pager.io.block(|| cursor.next())?);
        assert!(!cursor.is_valid()?);

        Ok(())
    }

    #[test]
    fn test_next_only_uncommitted_large_gaps() -> Result<()> {
        let conn = create_test_connection()?;

        // Create cursor for testing (no btree data)
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Add uncommitted rows with large gaps
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (100, vec![Value::from_i64(100), Value::from_i64(1000)], 1),
                (500, vec![Value::from_i64(500), Value::from_i64(5000)], 1),
                (999, vec![Value::from_i64(999), Value::from_i64(9990)], 1),
            ],
        );

        // Should iterate through all with large gaps
        pager.io.block(|| cursor.rewind())?;
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(100));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(500));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(999));

        assert!(!pager.io.block(|| cursor.next())?);

        Ok(())
    }

    #[test]
    fn test_multiple_updates_same_row_single_transaction() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 1, 2, 3
        populate_test_table(&conn, vec![(1, 10), (2, 20), (3, 30)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Multiple successive updates to row 2 in the same transaction
        // 20 -> 25 -> 28 -> 32 (final value should be 32)
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (2, vec![Value::from_i64(2), Value::from_i64(20)], -1), // Delete original
                (2, vec![Value::from_i64(2), Value::from_i64(25)], 1),  // First update
                (2, vec![Value::from_i64(2), Value::from_i64(25)], -1), // Delete first update
                (2, vec![Value::from_i64(2), Value::from_i64(28)], 1),  // Second update
                (2, vec![Value::from_i64(2), Value::from_i64(28)], -1), // Delete second update
                (2, vec![Value::from_i64(2), Value::from_i64(32)], 1),  // Final update
            ],
        );

        // Seek to row 2 should find the final value
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(2), SeekOp::GE { eq_only: true }))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(2));
        assert_eq!(pager.io.block(|| cursor.column(1))?, Value::from_i64(32));

        // Next through all rows to verify only final values are seen
        pager.io.block(|| cursor.rewind())?;
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(1));
        assert_eq!(pager.io.block(|| cursor.column(1))?, Value::from_i64(10));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(2));
        assert_eq!(pager.io.block(|| cursor.column(1))?, Value::from_i64(32)); // Final value

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));
        assert_eq!(pager.io.block(|| cursor.column(1))?, Value::from_i64(30));

        assert!(!pager.io.block(|| cursor.next())?);

        Ok(())
    }

    #[test]
    fn test_empty_materialized_view_with_uncommitted() -> Result<()> {
        let conn = create_test_connection()?;

        // Don't populate any data - view is created but empty
        // This tests a materialized view that was never populated

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Add uncommitted rows to empty materialized view
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (5, vec![Value::from_i64(5), Value::from_i64(50)], 1),
                (10, vec![Value::from_i64(10), Value::from_i64(100)], 1),
                (15, vec![Value::from_i64(15), Value::from_i64(150)], 1),
            ],
        );

        // Test seek on empty materialized view with uncommitted data
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(10), SeekOp::GE { eq_only: true }))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(10));

        // Test GT seek
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(7), SeekOp::GT))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(10));

        // Test rewind and next
        pager.io.block(|| cursor.rewind())?;
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(5));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(10));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(15));

        assert!(!pager.io.block(|| cursor.next())?);

        Ok(())
    }

    #[test]
    fn test_exact_match_btree_uncommitted_same_rowid_different_values() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 1, 3, 5
        populate_test_table(&conn, vec![(1, 10), (3, 30), (5, 50)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Add uncommitted row 3 with different value (not a delete+insert, just insert)
        // This simulates a case where uncommitted has a new version of row 3
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (3, vec![Value::from_i64(3), Value::from_i64(35)], 1), // New version with positive weight
            ],
        );

        // Exact match seek for row 3 should find the uncommitted version (35)
        // because when both exist with positive weight, uncommitted takes precedence
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(3), SeekOp::GE { eq_only: true }))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));

        // This test verifies which value we get when both btree and uncommitted
        // have the same rowid with positive weights
        // The expected behavior needs to be defined - typically uncommitted wins
        // or they get merged based on the DBSP semantics

        Ok(())
    }

    #[test]
    fn test_boundary_value_seeks() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with some normal values
        populate_test_table(&conn, vec![(100, 1000), (200, 2000)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Add uncommitted rows at extreme positions
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (
                    i64::MIN + 1,
                    vec![Value::from_i64(i64::MIN + 1), Value::from_i64(-999)],
                    1,
                ),
                (
                    i64::MAX - 1,
                    vec![Value::from_i64(i64::MAX - 1), Value::from_i64(999)],
                    1,
                ),
            ],
        );

        // Test 1: Seek GT with i64::MAX should find nothing
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(i64::MAX), SeekOp::GT))?;
        assert_eq!(result, SeekResult::NotFound);

        // Test 2: Seek LT with i64::MIN should find nothing
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(i64::MIN), SeekOp::LT))?;
        assert_eq!(result, SeekResult::NotFound);

        // Test 3: Seek GE with i64::MAX - 1 should find our extreme row
        let result = pager.io.block(|| {
            cursor.seek(
                SeekKey::TableRowId(i64::MAX - 1),
                SeekOp::GE { eq_only: false },
            )
        })?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(i64::MAX - 1));

        // Test 4: Seek LE with i64::MIN + 1 should find our extreme low row
        let result = pager.io.block(|| {
            cursor.seek(
                SeekKey::TableRowId(i64::MIN + 1),
                SeekOp::LE { eq_only: false },
            )
        })?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(i64::MIN + 1));

        // Test 5: Seek GT from i64::MIN should find the smallest row
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(i64::MIN), SeekOp::GT))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(i64::MIN + 1));

        // Test 6: Seek LT from i64::MAX should find the largest row
        let result = pager
            .io
            .block(|| cursor.seek(SeekKey::TableRowId(i64::MAX), SeekOp::LT))?;
        assert_eq!(result, SeekResult::Found);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(i64::MAX - 1));

        Ok(())
    }

    #[test]
    fn test_next_concurrent_btree_uncommitted_advance() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 1, 2, 3, 4, 5
        populate_test_table(&conn, vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Delete some btree rows and add replacements in uncommitted
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (2, vec![Value::from_i64(2), Value::from_i64(20)], -1), // Delete btree row 2
                (2, vec![Value::from_i64(2), Value::from_i64(25)], 1),  // Replace with new value
                (4, vec![Value::from_i64(4), Value::from_i64(40)], -1), // Delete btree row 4
            ],
        );

        // Should iterate: 1, 2(new), 3, 5
        pager.io.block(|| cursor.rewind())?;
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(1));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(2));
        assert_eq!(pager.io.block(|| cursor.column(1))?, Value::from_i64(25)); // New value

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(5));

        assert!(!pager.io.block(|| cursor.next())?);

        Ok(())
    }

    #[test]
    fn test_transaction_state_changes_mid_iteration() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 1, 3, 5
        populate_test_table(&conn, vec![(1, 10), (3, 30), (5, 50)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Start iteration
        pager.io.block(|| cursor.rewind())?;
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(1));

        // Move to next row
        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));

        // Now add new uncommitted changes mid-iteration
        apply_changes_to_tx_state(
            &tx_state,
            vec![
                (2, vec![Value::from_i64(2), Value::from_i64(20)], 1), // Insert before current
                (4, vec![Value::from_i64(4), Value::from_i64(40)], 1), // Insert after current
                (6, vec![Value::from_i64(6), Value::from_i64(60)], 1), // Insert at end
            ],
        );

        // Continue iteration - cursor continues from where it was, sees row 5 next
        // (new changes are only visible after rewind/seek)
        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(5));

        // No more rows in original iteration
        assert!(!pager.io.block(|| cursor.next())?);

        // Rewind and verify we see all rows including the newly added ones
        pager.io.block(|| cursor.rewind())?;
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(1));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(2));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(4));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(5));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(6));

        assert!(!pager.io.block(|| cursor.next())?);

        Ok(())
    }

    #[test]
    fn test_rewind_after_failed_seek() -> Result<()> {
        let conn = create_test_connection()?;

        // Populate table with rows 1, 3, 5
        populate_test_table(&conn, vec![(1, 10), (3, 30), (5, 50)])?;

        // Create cursor for testing
        let (mut cursor, tx_state, pager) = create_test_cursor(&conn)?;

        // Add uncommitted row 2
        apply_changes_to_tx_state(
            &tx_state,
            vec![(2, vec![Value::from_i64(2), Value::from_i64(20)], 1)],
        );

        // Seek to non-existent row 4 with exact match
        assert_eq!(
            pager
                .io
                .block(|| cursor.seek(SeekKey::TableRowId(4), SeekOp::GE { eq_only: true }))?,
            SeekResult::NotFound
        );
        assert!(!cursor.is_valid()?);

        // Rewind should work correctly after failed seek
        pager.io.block(|| cursor.rewind())?;
        assert!(cursor.is_valid()?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(1));

        // Verify we can iterate through all rows
        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(2));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(3));

        assert!(pager.io.block(|| cursor.next())?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(5));

        assert!(!pager.io.block(|| cursor.next())?);

        // Try another failed seek (GT on maximum value)
        assert_eq!(
            pager
                .io
                .block(|| cursor.seek(SeekKey::TableRowId(5), SeekOp::GT))?,
            SeekResult::NotFound
        );
        assert!(!cursor.is_valid()?);

        // Rewind again
        pager.io.block(|| cursor.rewind())?;
        assert!(cursor.is_valid()?);
        assert_eq!(pager.io.block(|| cursor.rowid())?, Some(1));

        Ok(())
    }

    // ===== IO RESUMPTION TEST SUITE =====
    // These tests verify correct behavior when btree operations return IO (pending)

    mod io_resumption_tests {
        use super::*;
        use crate::io::Completion;
        use crate::storage::btree::{BTreeKey, CursorStep, CursorTrait};
        use crate::types::{IOCompletions, ImmutableRecord, IndexInfo};
        use crate::Register;
        use std::sync::atomic::{AtomicUsize, Ordering};

        /// Mock btree cursor that tracks calls and can simulate IO pending states.
        /// Used to verify that seek operations aren't redundantly repeated after IO resumption.
        struct MockBTreeCursor {
            /// Number of times seek() was called
            seek_count: AtomicUsize,
            /// Number of times next() was called
            next_count: AtomicUsize,
            /// Number of times prev() was called
            prev_count: AtomicUsize,
            /// Current rowid to return
            current_rowid: Option<i64>,
            /// Record to return (needs to live for the cursor lifetime)
            record: ImmutableRecord,
            /// Index info
            index_info: Arc<IndexInfo>,
            advance_completion: Completion,
            advance_error: Option<Box<crate::LimboError>>,
            null_flag: bool,
        }

        #[test]
        fn cursor_step_owns_io_after_cursor_is_dropped() {
            for forward in [true, false] {
                let mut cursor = MockBTreeCursor::new();
                cursor.advance_completion = Completion::new_sync(|_| {});
                let completion = cursor.advance_completion.clone();
                let mut cursor: Box<dyn CursorTrait> = Box::new(cursor);
                let step = if forward {
                    cursor.next_row()
                } else {
                    cursor.prev_row()
                };
                let CursorStep::IO(io) = step else {
                    panic!("expected IO, got {step:?}");
                };
                let resumed = if forward {
                    cursor.next_row()
                } else {
                    cursor.prev_row()
                };
                assert!(matches!(resumed, CursorStep::Row));
                drop(cursor);
                assert!(!io.finished());
                completion.complete(0);
                assert!(io.finished());
            }
        }

        #[test]
        fn cursor_step_preserves_advance_errors() {
            for forward in [true, false] {
                let mut cursor = MockBTreeCursor::new();
                let error = Box::new(crate::LimboError::InternalError("advance failed".into()));
                let original = std::ptr::from_ref(error.as_ref());
                cursor.advance_error = Some(error);
                let cursor: &mut dyn CursorTrait = &mut cursor;
                let step = if forward {
                    cursor.next_row()
                } else {
                    cursor.prev_row()
                };
                let CursorStep::Error(error) = step else {
                    panic!("expected error, got {step:?}");
                };
                assert_eq!(std::ptr::from_ref(error.as_ref()), original);
            }
        }

        #[test]
        fn cursor_step_handles_null_and_empty_rows() {
            for forward in [true, false] {
                let mut cursor = MockBTreeCursor::new();
                cursor.set_null_flag(true);
                let advance = |cursor: &mut dyn CursorTrait| {
                    if forward {
                        cursor.next_row()
                    } else {
                        cursor.prev_row()
                    }
                };
                assert!(matches!(advance(&mut cursor), CursorStep::Empty));
                assert!(!cursor.get_null_flag());
                assert_eq!(cursor.next_count.load(Ordering::SeqCst), 0);
                assert_eq!(cursor.get_prev_count(), 0);
                assert!(matches!(advance(&mut cursor), CursorStep::IO(_)));
                assert!(matches!(advance(&mut cursor), CursorStep::Row));
                cursor.current_rowid = None;
                assert!(matches!(advance(&mut cursor), CursorStep::Empty));
            }
        }

        impl MockBTreeCursor {
            fn new() -> Self {
                // Create a minimal record with rowid=1, value=10, weight=1
                let record = Self::create_test_record(1, 10, 1);
                Self {
                    seek_count: AtomicUsize::new(0),
                    next_count: AtomicUsize::new(0),
                    prev_count: AtomicUsize::new(0),
                    current_rowid: Some(1),
                    record,
                    index_info: Arc::new(IndexInfo::default()),
                    advance_completion: Completion::new_yield(),
                    advance_error: None,
                    null_flag: false,
                }
            }

            fn create_test_record(rowid: i64, value: i64, weight: i64) -> ImmutableRecord {
                // Build a binary record with format: [header_size, type1, type2, type3, rowid, value, weight]
                // For integers, type code is 1 for 1-byte int, 2 for 2-byte, etc.
                // Using type 6 (8-byte integer) for all values
                // Header: 4 bytes (header size byte + 3 type bytes)
                let mut payload = crate::alloc::vec![
                    4u8, // header size
                    6u8, // type for rowid (8-byte int)
                    6u8, // type for value (8-byte int)
                    6u8, // type for weight (8-byte int)
                ];

                // Data: 3 x 8-byte integers
                payload.extend_from_slice(&rowid.to_be_bytes());
                payload.extend_from_slice(&value.to_be_bytes());
                payload.extend_from_slice(&weight.to_be_bytes());

                ImmutableRecord::from_bin_record(payload)
            }

            fn get_seek_count(&self) -> usize {
                self.seek_count.load(Ordering::SeqCst)
            }

            fn get_prev_count(&self) -> usize {
                self.prev_count.load(Ordering::SeqCst)
            }
        }

        impl CursorTrait for MockBTreeCursor {
            fn seek(&mut self, _key: SeekKey<'_>, _op: SeekOp) -> IOResultOr<SeekResult> {
                let count = self.seek_count.fetch_add(1, Ordering::SeqCst);
                if count == 0 {
                    // First seek returns TryAdvance
                    Ok(IOResult::Done(SeekResult::TryAdvance))
                } else {
                    // Subsequent seeks return Found to avoid infinite loop
                    // (The bug is that this second seek happens at all)
                    Ok(IOResult::Done(SeekResult::Found))
                }
            }

            fn seek_unpacked(
                &mut self,
                _registers: &[Register],
                _op: SeekOp,
            ) -> IOResultOr<SeekResult> {
                // Not used in these tests
                Ok(IOResult::Done(SeekResult::NotFound))
            }

            fn next(&mut self) -> IOResultOr<()> {
                if let Some(err) = self.advance_error.take() {
                    return Err(err);
                }
                let count = self.next_count.fetch_add(1, Ordering::SeqCst);
                if count == 0 {
                    // First call returns IO (pending)
                    Ok(IOResult::IO(IOCompletions(self.advance_completion.clone())))
                } else {
                    // Subsequent calls return Done
                    Ok(IOResult::Done(()))
                }
            }

            fn prev(&mut self) -> IOResultOr<()> {
                if let Some(err) = self.advance_error.take() {
                    return Err(err);
                }
                let count = self.prev_count.fetch_add(1, Ordering::SeqCst);
                if count == 0 {
                    // First call returns IO (pending)
                    Ok(IOResult::IO(IOCompletions(self.advance_completion.clone())))
                } else {
                    // Subsequent calls return Done
                    Ok(IOResult::Done(()))
                }
            }

            fn rowid(&mut self) -> IOResultOr<Option<i64>> {
                Ok(IOResult::Done(self.current_rowid))
            }

            fn record(&mut self) -> IOResultOr<Option<&ImmutableRecord>> {
                Ok(IOResult::Done(Some(&self.record)))
            }

            fn last(&mut self) -> IOResultOr<()> {
                Ok(IOResult::Done(()))
            }

            fn insert(&mut self, _key: &BTreeKey) -> IOResultOr<()> {
                Ok(IOResult::Done(()))
            }

            fn delete(&mut self) -> IOResultOr<()> {
                Ok(IOResult::Done(()))
            }

            fn set_null_flag(&mut self, flag: bool) {
                self.null_flag = flag;
            }

            fn get_null_flag(&self) -> bool {
                self.null_flag
            }

            fn exists(&mut self, _key: &Value) -> IOResultOr<bool> {
                Ok(IOResult::Done(false))
            }

            fn clear_btree(&mut self) -> IOResultOr<Option<usize>> {
                Ok(IOResult::Done(None))
            }

            fn btree_destroy(&mut self) -> IOResultOr<Option<usize>> {
                Ok(IOResult::Done(None))
            }

            fn count(&mut self) -> IOResultOr<usize> {
                Ok(IOResult::Done(0))
            }

            fn is_empty(&self) -> bool {
                self.current_rowid.is_none()
            }

            fn root_page(&self) -> i64 {
                1
            }

            fn rewind(&mut self) -> IOResultOr<()> {
                Ok(IOResult::Done(()))
            }

            fn has_record(&self) -> bool {
                true
            }

            fn set_has_record(&mut self, _has_record: bool) {}

            fn get_index_info(&self) -> &Arc<IndexInfo> {
                &self.index_info
            }

            fn seek_end(&mut self) -> IOResultOr<()> {
                Ok(IOResult::Done(()))
            }

            fn seek_to_last(&mut self) -> IOResultOr<()> {
                Ok(IOResult::Done(()))
            }

            fn invalidate_record(&mut self) {}

            fn has_rowid(&self) -> bool {
                true
            }

            fn get_pager(&self) -> Arc<Pager> {
                panic!("MockBTreeCursor::get_pager should not be called")
            }

            fn get_skip_advance(&self) -> bool {
                false
            }
        }

        /// Test that verifies the bug: when btree.next() returns IO after TryAdvance,
        /// resuming should NOT call btree.seek() again.
        ///
        /// Current behavior (BUG): seek is called twice
        /// Expected behavior: seek should only be called once
        #[test]
        fn test_seek_not_repeated_after_io_during_try_advance() -> Result<()> {
            let conn = create_test_connection()?;

            // Get the view for creating a cursor
            let view_mutex = conn
                .schema
                .read()
                .get_materialized_view("test_view")
                .ok_or_else(|| crate::LimboError::InternalError("View not found".to_string()))?;

            let pager = conn.get_pager();
            let tx_state = conn.view_transaction_states.get_or_create("test_view");

            // Create mock cursor that returns TryAdvance from seek and IO from next
            let mock_cursor = MockBTreeCursor::new();
            let mock_cursor_box: Box<dyn CursorTrait> = Box::new(mock_cursor);

            // Get a reference to the mock to check counts later
            // We need to use Any::downcast to access the mock's methods
            let mock_ptr = mock_cursor_box.as_ref() as *const dyn CursorTrait;

            let mut cursor =
                MaterializedViewCursor::new(mock_cursor_box, view_mutex, pager, tx_state)?;

            // Use LE so that rowid=1 satisfies the condition (1 <= 5)
            let seek_op = SeekOp::LE { eq_only: false };

            // First call to do_seek - should call btree.seek() which returns TryAdvance,
            // then btree.prev() which returns IO
            let result = cursor.do_seek(5, seek_op);

            // Should return IO (pending)
            assert!(
                matches!(result, Ok(IOResult::IO(_))),
                "Expected IO result, got {result:?}"
            );

            // Check seek was called once
            let mock_ref: &MockBTreeCursor = unsafe { &*(mock_ptr as *const MockBTreeCursor) };
            assert_eq!(
                mock_ref.get_seek_count(),
                1,
                "seek should be called exactly once before IO"
            );
            // For LE, we call prev() not next()
            assert_eq!(
                mock_ref.get_prev_count(),
                1,
                "prev should be called once (returned IO)"
            );

            // Second call to do_seek (simulating resumption after IO completes)
            // BUG: This will call btree.seek() again, which is wasteful
            let result = cursor.do_seek(5, seek_op);

            // The result might be Found or some other result
            assert!(
                matches!(result, Ok(IOResult::Done(_))),
                "Expected Done result on resume, got {result:?}"
            );

            // Check seek count - seek should only be called once
            // If this fails with seek_count=2, it means the bug exists:
            // seek is being redundantly called again after IO resumption
            let final_seek_count = mock_ref.get_seek_count();

            assert_eq!(
                final_seek_count, 1,
                "seek should only be called once, but was called {final_seek_count} times (redundant seek after IO during TryAdvance)"
            );

            Ok(())
        }
    }
}
