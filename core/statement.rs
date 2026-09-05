use std::{
    borrow::Cow,
    num::NonZero,
    ops::Deref,
    sync::{atomic::Ordering, Arc},
    task::Waker,
    time::Duration,
};

use tracing::{instrument, Level};
use turso_parser::ast::{fmt::ToTokens, Cmd};

use crate::{alloc::TursoIteratorExt, connection::PrepareOptions};
use crate::{
    busy::BusyHandlerState,
    parameters,
    schema::Trigger,
    stats::refresh_analyze_stats,
    translate::{self, display::PlanContext, emitter::TransactionMode, plan::BitSet},
    turso_assert,
    vdbe::{
        self,
        explain::{
            EXPLAIN_COLUMNS_TYPE, EXPLAIN_QUERY_PLAN_COLUMNS_TYPE,
            EXPLAIN_QUERY_PLAN_JSON_COLUMNS_TYPE,
        },
    },
    Connection, EqpFormat, LimboError, MvStore, Pager, QueryMode, Result, TransactionState, Value,
    EXPLAIN_COLUMNS, EXPLAIN_QUERY_PLAN_COLUMNS, EXPLAIN_QUERY_PLAN_JSON_COLUMNS,
};

type ProgramExecutionState = vdbe::ProgramExecutionState;
type Row = vdbe::Row;
type StepResult = vdbe::StepResult;

/// Classifies how a [`Statement`] participates in connection-level lifecycle
/// and active-statement accounting.
///
/// Use [`StatementOrigin::Root`] for ordinary top-level statements prepared on
/// behalf of the user. Root statements are the only statements that count
/// toward `Connection::n_active_root_statements` once execution begins, which
/// is the SQLite-compatible notion of "another SQL statement in progress" used
/// by operations like `VACUUM`.
///
/// Use [`StatementOrigin::InternalHelper`] when the engine prepares and runs a
/// separate helper statement on the same connection, for example helper SQL in
/// schema parsing or CDC setup. This is separately prepared SQL with its own
/// `prepare`/`step`/`reset`/`drop` lifecycle, but it is owned by a parent root
/// statement, so it stays nested and does not count as another root statement.
///
/// Use [`StatementOrigin::Subprogram`] only for bytecode subprograms that are
/// already compiled into a parent statement and entered through `OP_Program`,
/// such as trigger or foreign-key actions. This is not separately prepared SQL;
/// it is embedded child bytecode execution inside the parent statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatementOrigin {
    Root,
    InternalHelper,
    Subprogram,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementStatusCounter {
    FullscanStep,
    Sort,
    VmStep,
    Reprepare,
    RowsRead,
    RowsWritten,
}

impl StatementOrigin {
    pub(crate) const fn needs_nested_guard(self) -> bool {
        matches!(self, Self::InternalHelper)
    }
}

/// Structured type information for a result column.
///
/// Returned by [`Statement::get_column_type_info`]. Surfaces the array depth
/// and custom-type resolution that the SQLite-compat `get_column_decltype`
/// API does not expose, and also carries the inferred-affinity result for
/// computed expressions (`SELECT 1+1`, function calls in subqueries, etc.)
/// — the consumer asks one question, the API decides which path applies.
///
/// For a direct table-column reference, `declared_name` is the literal
/// string the user wrote in CREATE TABLE (`"INTEGER"`, `"cents"`,
/// `"VARCHAR"`), `array_dimensions` is the bracket depth, and `base_type` /
/// `kind` carry any CREATE TYPE / CREATE DOMAIN resolution.
///
/// For a literal (`SELECT 42`, `SELECT 'x'`, `SELECT 3.14`), `declared_name`
/// is the primitive that matches the literal's parsed value type
/// (`"INTEGER"`, `"TEXT"`, `"REAL"`). For a typed expression — CAST, rowid,
/// or anything else SQLite's affinity rules can pin down — it's the
/// inferred primitive. In both cases `array_dimensions` is `0`, `base_type`
/// is `None`, and `kind` is [`ColumnTypeKind::Builtin`]. When neither path
/// produces a usable primitive (binary arithmetic that SQLite refuses to
/// propagate through, BLOB literals, NULL literals, function calls without
/// declared return affinity), `get_column_type_info` returns `Ok(None)`
/// rather than fabricating a name — callers can fall through to their own
/// default.
///
/// New fields may be added over time; the struct is marked
/// `#[non_exhaustive]` so consumers must use struct-update or accessor
/// patterns rather than exhaustive matches.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ColumnTypeInfo {
    /// The declared type name as written in CREATE TABLE — e.g. `"INTEGER"`,
    /// `"VARCHAR"`, or the name of a `CREATE TYPE` / `CREATE DOMAIN` such as
    /// `"cents"`. This is the same string `get_column_decltype` returns.
    pub declared_name: String,
    /// Array dimensionality: `0` for scalar columns, `1` for `INTEGER[]`,
    /// `2` for `TEXT[][]`, etc.
    pub array_dimensions: u32,
    /// For columns whose declared type resolves to a `CREATE TYPE` or
    /// `CREATE DOMAIN` definition, this is the underlying primitive type name
    /// (`"INTEGER"`, `"TEXT"`, `"REAL"`, `"BLOB"`, or `"NUMERIC"`). `None`
    /// when the declared name is a built-in primitive directly.
    ///
    /// Use this to distinguish "the user wrote `INTEGER`" (base_type: `None`)
    /// from "the user wrote `cents`, which happens to be INTEGER underneath"
    /// (base_type: `Some("INTEGER")`).
    pub base_type: Option<String>,
    /// Classification of the declared type. Distinguishes `BUILTIN` (the
    /// declared name is a primitive) from the four `CREATE TYPE`/`CREATE
    /// DOMAIN` flavours.
    ///
    /// This matters for callers like wire-protocol layers that need to map
    /// a column to its native type code: a column declared as a `STRUCT`
    /// type stores a BLOB on disk (`base_type` is `Some("BLOB")`), but the
    /// caller usually wants to expose it as a composite/JSON type rather
    /// than raw bytes. The `kind` field carries that distinction directly
    /// without forcing the caller to re-query the schema.
    pub kind: ColumnTypeKind,
}

/// Classification of a result column's declared type.
///
/// Returned as part of [`ColumnTypeInfo`]. `#[non_exhaustive]` so that new
/// kinds (e.g. for future enum or table-row types) can be added without a
/// breaking change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ColumnTypeKind {
    /// A SQLite-style primitive type: `INTEGER`, `TEXT`, `REAL`, `BLOB`,
    /// `NUMERIC`, `ANY`. The declared name is itself the primitive.
    Builtin,
    /// A user- or built-in custom type defined with
    /// `CREATE TYPE name BASE primitive ENCODE ... DECODE ...`. Has an
    /// underlying primitive (see `base_type`) and an encode/decode pipeline.
    /// Built-in types like `uuid`, `boolean`, `numeric` register through
    /// this path too.
    Custom,
    /// A domain defined with `CREATE DOMAIN name AS base [CHECK ...]`.
    /// Shares an underlying primitive with its base type but adds CHECK
    /// constraints; values are otherwise identical to the base.
    Domain,
    /// A composite type defined with `CREATE TYPE name AS STRUCT(...)`.
    /// Values are stored as BLOBs containing the packed record; the
    /// declared name carries the field schema.
    Struct,
    /// A tagged union defined with `CREATE TYPE name AS UNION(...)`.
    /// Values are stored as BLOBs containing a tag and a payload; the
    /// declared name carries the variant schema.
    Union,
}

/// Recursively infer the result primitive of a non-table-column expression
/// and return its uppercase name (`"INTEGER"`, `"REAL"`, `"TEXT"`,
/// `"NUMERIC"`, `"BLOB"`) or `None` when no determination can be made.
///
/// Used by [`Statement::get_column_type_info`] to give wire-protocol layers
/// a usable type for `SELECT 1+1`-style result columns. Goes beyond SQLite's
/// `get_expr_affinity` (which deliberately stops at binary operators because
/// SQLite's affinity model is about *column* coercion, not expression
/// inference) by walking through arithmetic, bitwise, comparison, logical,
/// and concat operators — letting `SELECT 42 + 1` report INT4 to a
/// PostgreSQL client the way PG itself does.
fn infer_expression_primitive(
    expr: &turso_parser::ast::Expr,
    referenced_tables: Option<&translate::plan::TableReferences>,
) -> Option<&'static str> {
    use turso_parser::ast::{Expr, Operator, UnaryOperator};

    match expr {
        // Bare literal: read the parsed concrete value type.
        Expr::Literal(lit) => match translate::alter::literal_default_value(lit)
            .ok()?
            .value_type()
        {
            crate::types::ValueType::Integer => Some("INTEGER"),
            crate::types::ValueType::Float => Some("REAL"),
            crate::types::ValueType::Text => Some("TEXT"),
            _ => None,
        },
        Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            infer_expression_primitive(exprs.first().unwrap(), referenced_tables)
        }
        Expr::Collate(inner, _) => infer_expression_primitive(inner, referenced_tables),
        Expr::Unary(op, inner) => match op {
            UnaryOperator::Not | UnaryOperator::BitwiseNot => Some("INTEGER"),
            UnaryOperator::Negative => Some(combine_arithmetic_primitive(
                Some("INTEGER"),
                infer_expression_primitive(inner, referenced_tables),
            )),
            UnaryOperator::Positive => infer_expression_primitive(inner, referenced_tables),
        },
        Expr::Binary(left, op, right) => match op {
            // Arithmetic: widen INTEGER × INTEGER to INTEGER, anything mixed
            // with REAL becomes REAL, fall through to NUMERIC otherwise.
            Operator::Add
            | Operator::Subtract
            | Operator::Multiply
            | Operator::Divide
            | Operator::Modulus => {
                let l = infer_expression_primitive(left, referenced_tables);
                let r = infer_expression_primitive(right, referenced_tables);
                Some(combine_arithmetic_primitive(l, r))
            }
            // Bitwise: result is always INTEGER in both SQLite and PG.
            Operator::BitwiseAnd
            | Operator::BitwiseOr
            | Operator::BitwiseNot
            | Operator::LeftShift
            | Operator::RightShift => Some("INTEGER"),
            // Comparison and logical: SQLite returns 0/1 INTEGER; tursopg
            // maps INTEGER to BOOL at the wire layer for boolean columns,
            // but the type the wire layer reports is still INTEGER here.
            Operator::Equals
            | Operator::NotEquals
            | Operator::Less
            | Operator::LessEquals
            | Operator::Greater
            | Operator::GreaterEquals
            | Operator::Is
            | Operator::IsNot
            | Operator::And
            | Operator::Or
            | Operator::ArrayContains
            | Operator::ArrayOverlap => Some("INTEGER"),
            // Concat is always TEXT.
            Operator::Concat => Some("TEXT"),
            // JSON ops fall through to the affinity machinery — `->` returns
            // JSON / blob, `->>` returns TEXT; the existing affinity rules
            // give the correct answer.
            Operator::ArrowRight | Operator::ArrowRightShift => affinity_to_primitive(
                translate::expr::get_expr_affinity(expr, referenced_tables, None),
            ),
        },
        Expr::RowId { .. } => Some("INTEGER"),
        // CAST, column references, and anything else: defer to the affinity
        // machinery, which handles these shapes correctly.
        _ => affinity_to_primitive(translate::expr::get_expr_affinity(
            expr,
            referenced_tables,
            None,
        )),
    }
}

/// Map [`crate::vdbe::affinity::Affinity`] to the uppercase primitive name
/// `infer_expression_primitive` returns. `Blob` collapses to `None` because
/// SQLite's "no determined affinity" sentinel isn't a usable wire type.
fn affinity_to_primitive(affinity: crate::vdbe::affinity::Affinity) -> Option<&'static str> {
    match affinity {
        crate::vdbe::affinity::Affinity::Integer => Some("INTEGER"),
        crate::vdbe::affinity::Affinity::Real => Some("REAL"),
        crate::vdbe::affinity::Affinity::Text => Some("TEXT"),
        crate::vdbe::affinity::Affinity::Numeric => Some("NUMERIC"),
        crate::vdbe::affinity::Affinity::Blob | crate::vdbe::affinity::Affinity::None => None,
    }
}

/// Pick the widening primitive for an arithmetic binary op given each
/// operand's inferred primitive. `INTEGER + INTEGER -> INTEGER`,
/// `INTEGER + REAL -> REAL`, everything else collapses to `NUMERIC` (the
/// safe wire default for a mixed-affinity numeric result).
fn combine_arithmetic_primitive(
    left: Option<&'static str>,
    right: Option<&'static str>,
) -> &'static str {
    match (left, right) {
        (Some("INTEGER"), Some("INTEGER")) => "INTEGER",
        (Some("INTEGER"), Some("REAL"))
        | (Some("REAL"), Some("INTEGER"))
        | (Some("REAL"), Some("REAL")) => "REAL",
        _ => "NUMERIC",
    }
}

pub struct Statement {
    pub(crate) program: vdbe::Program,
    state: vdbe::ProgramState,
    pager: Arc<Pager>,
    /// indicates if the statement is a NORMAL/EXPLAIN/EXPLAIN QUERY PLAN
    query_mode: QueryMode,
    /// Flag to show if the statement was busy
    busy: bool,
    /// Busy handler state for tracking invocations and timeouts
    busy_handler_state: Option<BusyHandlerState>,
    /// Per-execution timeout override for this statement.
    /// - `None`: use connection default
    /// - `Some(Some(duration))`: override with a query-specific timeout
    /// - `Some(None)`: disable timeout for this execution
    query_timeout_override: Option<Option<Duration>>,
    /// True once step() has returned Row for a write statement (INSERT/UPDATE/DELETE
    /// with RETURNING). With ephemeral-buffered RETURNING, the first Row proves all
    /// DML completed — only the scan-back remains. Used by reset_internal to decide
    /// commit vs rollback when a statement is abandoned.
    has_returned_row: bool,
    /// Byte offset in the original SQL string where this statement ends.
    /// Used by sqlite3_prepare_v2 to set the *pzTail output parameter.
    tail_offset: usize,
    origin: StatementOrigin,
    /// True once this root statement has started executing and incremented
    /// `Connection::n_active_root_statements`.
    counted_as_active_root: bool,
    /// True for the parked statement backing an incremental blob handle.
    /// Counted separately in `Connection::n_active_blob_statements` so
    /// explicit checkpoints can subtract it — an open blob handle must not
    /// block checkpointing for its whole lifetime.
    is_blob_handle: bool,
    /// True if this statement called `Connection::start_nested()` during
    /// construction and therefore must call `end_nested()` on drop.
    nested_guard_active: bool,
}

crate::assert::assert_send_sync!(Statement);

impl std::fmt::Debug for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Statement").finish()
    }
}

impl Statement {
    pub(crate) fn prepare_index_methods(&mut self) -> crate::types::IOResultOr<()> {
        crate::vdbe::execute::index_method_stage_statement_all(&mut self.state)
    }

    pub(crate) fn abort_index_methods(&mut self) {
        crate::vdbe::execute::index_method_abort_statement_all(&mut self.state);
    }

    pub(crate) fn commit_index_methods(&mut self) {
        crate::vdbe::execute::index_method_on_transaction_committed_all(
            &mut self.state,
            &self.program.connection,
        );
    }

    pub(crate) fn register_index_methods(&mut self, connection: &Connection) -> Result<()> {
        crate::vdbe::execute::index_method_register_transaction_all(&mut self.state, connection)
    }

    pub fn new(
        program: vdbe::Program,
        pager: Arc<Pager>,
        query_mode: QueryMode,
        tail_offset: usize,
    ) -> Self {
        Self::new_with_origin(
            program,
            pager,
            query_mode,
            tail_offset,
            StatementOrigin::Root,
            false,
        )
    }

    #[turso_macros::trace_stack]
    pub(crate) fn new_with_origin(
        program: vdbe::Program,
        pager: Arc<Pager>,
        query_mode: QueryMode,
        tail_offset: usize,
        origin: StatementOrigin,
        nested_guard_active: bool,
    ) -> Self {
        let (max_registers, cursor_count) = match query_mode {
            QueryMode::Normal => (program.max_registers, program.cursor_ref.len()),
            QueryMode::Explain => (EXPLAIN_COLUMNS.len(), 0),
            QueryMode::ExplainQueryPlan {
                format: EqpFormat::Text,
            } => (EXPLAIN_QUERY_PLAN_COLUMNS.len(), 0),
            QueryMode::ExplainQueryPlan {
                format: EqpFormat::Json,
            } => (EXPLAIN_QUERY_PLAN_JSON_COLUMNS.len(), 0),
        };
        let state = vdbe::ProgramState::new(max_registers, cursor_count);
        Self {
            program,
            state,
            pager,
            query_mode,
            busy: false,
            busy_handler_state: None,
            query_timeout_override: None,
            has_returned_row: false,
            tail_offset,
            origin,
            counted_as_active_root: false,
            is_blob_handle: false,
            nested_guard_active,
        }
    }

    /// Mark this statement as the parked backing statement of an incremental
    /// blob handle. Must be called before the first `step()` so the blob
    /// accounting stays in lockstep with the root-statement count.
    pub(crate) fn mark_as_blob_handle(&mut self) {
        turso_assert!(
            !self.counted_as_active_root,
            "blob handle marked after its statement started executing"
        );
        self.is_blob_handle = true;
    }

    pub fn tail_offset(&self) -> usize {
        self.tail_offset
    }

    pub fn get_trigger(&self) -> Option<Arc<Trigger>> {
        self.program.trigger.clone()
    }

    pub fn get_query_mode(&self) -> QueryMode {
        self.query_mode
    }

    pub fn get_program(&self) -> &vdbe::Program {
        &self.program
    }

    pub fn get_pager(&self) -> &Arc<Pager> {
        &self.pager
    }

    pub fn n_change(&self) -> i64 {
        self.state
            .n_change
            .load(crate::sync::atomic::Ordering::SeqCst)
    }

    pub fn set_n_change(&self, n: i64) {
        self.state
            .n_change
            .store(n, crate::sync::atomic::Ordering::SeqCst);
    }

    pub fn n_total_change(&self) -> i64 {
        self.state
            .n_total_change
            .load(crate::sync::atomic::Ordering::SeqCst)
    }

    pub fn set_mv_tx(&mut self, mv_tx: Option<(u64, TransactionMode)>) {
        self.program.connection.set_mv_tx(mv_tx);
    }

    pub fn interrupt(&mut self) {
        self.state.interrupt();
    }

    /// Sets a per-execution timeout override for this statement.
    ///
    /// - `None`: use connection default
    /// - `Some(Some(duration))`: use query-specific timeout
    /// - `Some(None)`: disable timeout for this execution
    pub fn set_query_timeout_override(&mut self, timeout: Option<Option<Duration>>) {
        self.query_timeout_override = timeout;
    }

    pub fn execution_state(&self) -> ProgramExecutionState {
        self.state.execution_state
    }

    /// Statement metrics accumulated across executions of this prepared
    /// statement. Includes subprogram work.
    pub fn metrics(&self) -> vdbe::metrics::StatementMetrics {
        self.state.metrics()
    }

    pub fn reset_metrics(&mut self) {
        self.state.reset_metrics();
    }

    pub fn stmt_status(&self, counter: StatementStatusCounter) -> u64 {
        let metrics = self.metrics();
        match counter {
            StatementStatusCounter::FullscanStep => metrics.fullscan_steps,
            StatementStatusCounter::Sort => metrics.sort_operations,
            StatementStatusCounter::VmStep => metrics.insn_executed,
            StatementStatusCounter::Reprepare => metrics.reprepares,
            StatementStatusCounter::RowsRead => metrics.rows_read,
            StatementStatusCounter::RowsWritten => metrics.rows_written,
        }
    }

    pub fn reset_stmt_status(&mut self, counter: StatementStatusCounter) {
        self.state.reset_stmt_status(counter);
    }

    pub fn mv_store(&self) -> impl Deref<Target = Option<Arc<MvStore>>> {
        self.program.connection.mv_store()
    }

    /// Take the pending IO completions from this statement.
    /// Returns None if no IO is pending.
    /// This is used by async state machines that need to yield the completions.
    pub fn take_io_completions(&mut self) -> Option<crate::types::IOCompletions> {
        self.state.io_completions.take()
    }

    fn arm_query_timeout_if_needed(&mut self) {
        if !matches!(self.state.execution_state, ProgramExecutionState::Init)
            || self.state.query_deadline.is_some()
        {
            return;
        }
        let timeout = match self.query_timeout_override {
            Some(timeout_override) => timeout_override,
            None => {
                let connection_timeout = self.program.connection.get_query_timeout();
                if connection_timeout.is_zero() {
                    None
                } else {
                    Some(connection_timeout)
                }
            }
        };
        let Some(timeout) = timeout else {
            return;
        };
        self.state.query_deadline = Some(self.pager.io.current_time_monotonic() + timeout);
    }

    fn release_active_root_if_counted(&mut self) {
        if self.counted_as_active_root {
            // Blob count drops before the root count so a concurrent
            // checkpoint-guard read never sees fewer non-blob statements
            // than are really active (a stale-high read only causes a
            // spurious StatementsInProgress, never a missed one).
            if self.is_blob_handle {
                self.program
                    .connection
                    .n_active_blob_statements
                    .fetch_sub(1, Ordering::SeqCst);
            }
            let previous = self
                .program
                .connection
                .n_active_root_statements
                .fetch_sub(1, Ordering::SeqCst);
            if previous == 1 {
                self.program.connection.clear_interrupt_if_idle();
            }
            self.counted_as_active_root = false;
        }
    }

    /// Every step of a statement passes through here, so the work that only
    /// matters on the first call, the last call, a busy wait or an error is
    /// gated behind cheap flag tests and kept out of line. A row in the middle
    /// of a scan runs only the interpreter call and the result-row bookkeeping.
    fn _step(&mut self, waker: Option<&Waker>) -> Result<StepResult> {
        if matches!(self.state.execution_state, ProgramExecutionState::Init)
            || !self.counted_as_active_root
            || self.busy_handler_state.is_some()
        {
            if let Some(result) = self.prepare_step(waker)? {
                return Ok(result);
            }
        }
        let res = self
            .program
            .step(&mut self.state, &self.pager, self.query_mode, waker);
        if let Ok(StepResult::Row) = res {
            self.busy = true;
            // Track when a write statement yields its first Row. With ephemeral-buffered
            // RETURNING, this proves all DML completed — only the scan-back remains.
            if self.query_mode == QueryMode::Normal
                && self.program.change_cnt_on
                && !self.program.result_columns.is_empty()
            {
                self.has_returned_row = true;
            }
            return Ok(StepResult::Row);
        }
        self.finish_step(res, waker)
    }

    /// First-call and busy-wait work of [`Self::_step`]. Returns the result to
    /// hand back to the caller when the statement must not run yet.
    #[inline(never)]
    fn prepare_step(&mut self, waker: Option<&Waker>) -> Result<Option<StepResult>> {
        if !self.counted_as_active_root && matches!(self.origin, StatementOrigin::Root) {
            self.program.connection.start_root_statement()?;
            self.counted_as_active_root = true;
            // After the root count, so the checkpoint guard's subtraction
            // can only read stale-high (see release_active_root_if_counted).
            if self.is_blob_handle {
                self.program
                    .connection
                    .n_active_blob_statements
                    .fetch_add(1, Ordering::SeqCst);
            }
        }
        if matches!(self.state.execution_state, ProgramExecutionState::Init)
            && self.origin != StatementOrigin::InternalHelper
        {
            if self.program.connection.mvcc_enabled() {
                // MVCC checkpoints can publish internal schema roots without changing
                // SQLite's schema cookie, so refresh before deciding whether to reprepare.
                self.program.connection.maybe_update_schema();
            }
            if !self
                .program
                .prepare_context
                .matches_connection(&self.program.connection)
            {
                if let Err(err) = self.reprepare() {
                    self.release_active_root_if_counted();
                    return Err(err);
                }
            }
        }

        self.arm_query_timeout_if_needed();

        // If we're waiting for a busy handler timeout, check if we can proceed
        if let Some(busy_state) = self.busy_handler_state.as_ref() {
            let now = self.pager.io.current_time_monotonic();
            if now < busy_state.timeout() {
                // The timeout has not been reached yet: ask the caller to wait
                // out the remaining delay before stepping again.
                if let Some(waker) = waker {
                    waker.wake_by_ref();
                }
                return Ok(Some(StepResult::Sleep {
                    duration: busy_state.get_delay(now),
                }));
            }
        }
        Ok(None)
    }

    /// Everything [`Self::_step`] does after the interpreter returned something
    /// other than a row: schema retries, completion, busy handling and errors.
    #[inline(never)]
    fn finish_step(
        &mut self,
        mut res: std::result::Result<StepResult, Box<LimboError>>,
        waker: Option<&Waker>,
    ) -> Result<StepResult> {
        const MAX_SCHEMA_RETRY: usize = 50;
        for attempt in 0..MAX_SCHEMA_RETRY {
            // Only reprepare if we still need to update schema
            if !matches!(&res, Err(err) if matches!(**err, LimboError::SchemaUpdated)) {
                break;
            }
            // In a write transaction, reprepare may not help (e.g. cross-process
            // schema change where the in-memory schema hasn't been refreshed from
            // disk). Allow a few retries for the in-process case where reprepare
            // *can* resolve the issue, but bail early to avoid burning 50 attempts.
            if attempt >= 2
                && !self.program.connection.get_auto_commit()
                && matches!(
                    self.program.connection.get_tx_state(),
                    TransactionState::Write { .. } | TransactionState::PendingUpgrade { .. }
                )
            {
                break;
            }
            tracing::debug!("reprepare: attempt={}", attempt);
            if let Err(err) = self.reprepare() {
                self.release_active_root_if_counted();
                return Err(err);
            }
            res = self
                .program
                .step(&mut self.state, &self.pager, self.query_mode, waker);
        }

        // Aggregate metrics when statement completes
        if matches!(res, Ok(StepResult::Done)) {
            self.program
                .connection
                .metrics
                .write()
                .record_statement(&self.metrics());
            self.busy = false;
            self.busy_handler_state = None; // Reset busy state on completion
            self.state.query_deadline = None;

            // After ANALYZE completes, refresh in-memory stats so planners can use them.
            let sql = self.program.sql.trim_start().as_bytes();
            if sql.len() >= 7 && sql[..7].eq_ignore_ascii_case(b"ANALYZE") {
                // The stats refresh runs a SELECT on this same connection. At
                // this point ANALYZE is already Done, so it must not count as a
                // sibling root statement for that internal SELECT.
                self.release_active_root_if_counted();
                refresh_analyze_stats(&self.program.connection);
            }
        } else {
            self.busy = true;
        }

        // Handle busy result by invoking the busy handler
        if matches!(res, Ok(StepResult::Busy)) {
            let now = self.pager.io.current_time_monotonic();
            let handler = self.program.connection.get_busy_handler();

            // Initialize or get existing busy handler state
            let busy_state = self
                .busy_handler_state
                .get_or_insert_with(|| BusyHandlerState::new(now));

            // Invoke the busy handler to determine if we should retry
            if busy_state.invoke(&handler, now) {
                // Handler says retry: ask the caller to wait out the backoff
                // delay before stepping again.
                if let Some(waker) = waker {
                    waker.wake_by_ref();
                }
                res = Ok(StepResult::Sleep {
                    duration: busy_state.get_delay(now),
                });
                #[cfg(shuttle)]
                crate::thread::spin_loop();
            }
            // else: Handler says stop, res stays as Busy
        }

        // Track when a write statement yields its first Row. With ephemeral-buffered
        // RETURNING, this proves all DML completed — only the scan-back remains.
        if matches!(res, Ok(StepResult::Row))
            && self.query_mode == QueryMode::Normal
            && self.program.change_cnt_on
            && !self.program.result_columns.is_empty()
        {
            self.has_returned_row = true;
        }

        if self.counted_as_active_root
            && (matches!(res, Ok(StepResult::Done | StepResult::Interrupt)) || res.is_err())
        {
            self.release_active_root_if_counted();
        }

        // If the bytecode aborted between SequenceBeginInnerTx and
        // SequenceCommitInnerTx, the connection's mv_tx is still pointing
        // at the orphan inner; subsequent statements (e.g. reparse_schema
        // SELECTs from _step's own reprepare path) would inherit it and
        // deadlock in commit_txn's WaitForDependencies. Roll back and
        // restore the outer eagerly here — reset_internal alone is not
        // enough because callers do not always reset on error before
        // running another statement.
        if res.is_err() {
            self.cleanup_orphaned_seq_inner_tx();
        }

        // The interpreter chain carries a boxed error to keep per-row returns
        // register-sized; unbox once at the public boundary.
        res.map_err(|err| *err)
    }

    #[inline]
    pub fn step(&mut self) -> Result<StepResult> {
        self._step(None)
    }

    #[inline]
    pub fn step_with_waker(&mut self, waker: &Waker) -> Result<StepResult> {
        self._step(Some(waker))
    }

    /// Fast step for trigger/FK subprograms: skips reprepare checks, timeout
    /// arming, busy handler, metrics recording, and schema retry.
    /// The parent statement handles all of those concerns.
    #[inline]
    pub fn step_subprogram(&mut self) -> Result<StepResult> {
        self.program
            .step(&mut self.state, &self.pager, self.query_mode, None)
            .map_err(|err| *err)
    }

    pub fn run_ignore_rows(&mut self) -> Result<()> {
        loop {
            match self.step()? {
                vdbe::StepResult::Done => return Ok(()),
                vdbe::StepResult::IO | vdbe::StepResult::Yield | vdbe::StepResult::Sleep { .. } => {
                    self.pager.io.step()?
                }
                vdbe::StepResult::Row => continue,
                vdbe::StepResult::Interrupt | vdbe::StepResult::Busy => {
                    return Err(LimboError::Busy)
                }
            }
        }
    }

    pub fn run_collect_rows(&mut self) -> Result<Vec<Vec<Value>>> {
        let mut values = Vec::new();
        loop {
            match self.step()? {
                vdbe::StepResult::Done => return Ok(values),
                vdbe::StepResult::IO | vdbe::StepResult::Yield | vdbe::StepResult::Sleep { .. } => {
                    self.pager.io.step()?
                }
                vdbe::StepResult::Row => {
                    values.push(self.row().unwrap().get_values().cloned().collect());
                    continue;
                }
                vdbe::StepResult::Interrupt | vdbe::StepResult::Busy => {
                    return Err(LimboError::Busy)
                }
            }
        }
    }

    /// Blocks execution, advances IO, and runs to completion of the statement
    pub fn run_with_row_callback(
        &mut self,
        mut func: impl FnMut(&Row) -> Result<()>,
    ) -> Result<()> {
        loop {
            match self.step()? {
                vdbe::StepResult::Done => break,
                vdbe::StepResult::IO | vdbe::StepResult::Yield | vdbe::StepResult::Sleep { .. } => {
                    self.pager.io.step()?
                }
                vdbe::StepResult::Row => {
                    func(self.row().expect("row should be present"))?;
                }
                vdbe::StepResult::Interrupt => return Err(LimboError::Interrupt),
                vdbe::StepResult::Busy => return Err(LimboError::Busy),
            }
        }
        Ok(())
    }

    /// Non-blocking counterpart of [`Self::run_ignore_rows`]: drives the
    /// statement to completion, ignoring rows, but instead of pumping IO
    /// synchronously it yields the pending completion to the caller. Re-invoke
    /// after the yielded completion finishes; the program resumes at the same
    /// pc. Rows are discarded.
    ///
    /// Used by engine-internal callers that must stay non-blocking (MVCC
    /// bootstrap/recovery) so they don't call `io.step()` on backends that have
    /// no synchronous IO pump (e.g. WASM).
    pub fn run_ignore_rows_nonblock(&mut self) -> crate::types::IOResultOr<()> {
        loop {
            match self.step()? {
                vdbe::StepResult::Done => return Ok(crate::IOResult::Done(())),
                vdbe::StepResult::Row => continue,
                vdbe::StepResult::IO | vdbe::StepResult::Yield | vdbe::StepResult::Sleep { .. } => {
                    let io = self.take_io_completions().unwrap_or_else(|| {
                        crate::types::IOCompletions(crate::io::Completion::new_yield())
                    });
                    return Ok(crate::IOResult::IO(io));
                }
                vdbe::StepResult::Interrupt => return Err(LimboError::Interrupt.into()),
                vdbe::StepResult::Busy => return Err(LimboError::Busy.into()),
            }
        }
    }

    /// Non-blocking counterpart of [`Self::run_with_row_callback`]: drives the
    /// statement to completion, invoking `func` once per emitted row, but
    /// yields the pending completion to the caller instead of pumping IO
    /// synchronously.
    ///
    /// Re-entrancy: on an IO yield the program is paused mid-opcode (never
    /// between emitting a row and this loop observing it), so on re-invocation
    /// stepping resumes without replaying the last row — every row's `func`
    /// runs exactly once. Because the runner restarts from the top on each
    /// re-entry, `func` must append to caller-owned state that persists across
    /// yields (e.g. a field in the driving state machine), not to a local.
    pub fn run_with_row_callback_nonblock(
        &mut self,
        mut func: impl FnMut(&Row) -> Result<()>,
    ) -> crate::types::IOResultOr<()> {
        loop {
            match self.step()? {
                vdbe::StepResult::Done => return Ok(crate::IOResult::Done(())),
                vdbe::StepResult::Row => {
                    func(self.row().expect("row should be present"))?;
                }
                vdbe::StepResult::IO | vdbe::StepResult::Yield | vdbe::StepResult::Sleep { .. } => {
                    let io = self.take_io_completions().unwrap_or_else(|| {
                        crate::types::IOCompletions(crate::io::Completion::new_yield())
                    });
                    return Ok(crate::IOResult::IO(io));
                }
                vdbe::StepResult::Interrupt => return Err(LimboError::Interrupt.into()),
                vdbe::StepResult::Busy => return Err(LimboError::Busy.into()),
            }
        }
    }

    /// Blocks execution, advances IO, and stops at any StepResult except IO
    /// You can optionally pass a handler to run after IO is advanced
    pub fn run_one_step_blocking(
        &mut self,
        mut pre_io_func: impl FnMut() -> Result<()>,
        mut post_io_func: impl FnMut() -> Result<()>,
    ) -> Result<Option<&Row>> {
        let result = loop {
            match self.step()? {
                vdbe::StepResult::Done => break None,
                vdbe::StepResult::IO | vdbe::StepResult::Yield | vdbe::StepResult::Sleep { .. } => {
                    pre_io_func()?;
                    self.pager.io.step()?;
                    post_io_func()?;
                }
                vdbe::StepResult::Row => break Some(self.row().expect("row should be present")),
                vdbe::StepResult::Interrupt => return Err(LimboError::Interrupt),
                vdbe::StepResult::Busy => return Err(LimboError::Busy),
            }
        };
        Ok(result)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn reprepare(&mut self) -> Result<()> {
        tracing::trace!("repreparing statement");
        let conn = self.program.connection.clone();
        let main_pager = conn.pager.load().clone();

        // SchemaUpdated bypasses the normal abort rollback path, so in
        // autocommit mode we must unwind any implicit transaction state here
        // before reparsing. This must clear both pager locks and MVCC tx ids;
        // otherwise the retried statement can stack a fresh snapshot on top of
        // leaked transaction state from the failed attempt.
        let attached_leaked = conn.with_all_attached_pagers_with_index(|pagers| {
            pagers
                .iter()
                .any(|(_, pager)| pager.holds_write_lock() || pager.holds_read_lock())
        });
        let has_implicit_txn_state = conn.get_tx_state() != TransactionState::None
            || conn.get_mv_tx().is_some()
            || conn.next_attached_mv_tx().is_some()
            || attached_leaked
            || self.state.auto_txn_cleanup != vdbe::TxnCleanup::None;
        if conn.get_auto_commit() && has_implicit_txn_state {
            conn.rollback_current_txn_state(&main_pager, true);
            self.state.auto_txn_cleanup = vdbe::TxnCleanup::None;
        }
        if conn.get_auto_commit() && !conn.schema_reparse_in_progress() {
            conn.maybe_reparse_schema()?;
        }

        // End transactions on attached database pagers so they get a fresh view
        // of the database. Without this, the pager would still see the old page 1
        // with the stale schema cookie, causing an infinite SchemaUpdated loop.
        // SchemaUpdated can occur at different points in the Transaction opcode,
        // so the attached pager may or may not hold locks at this point.
        let attached_db_ids: BitSet = self
            .program
            .prepared
            .write_databases
            .iter()
            .chain(self.program.prepared.read_databases.iter())
            .filter(|&id| id != crate::MAIN_DB_ID)
            .try_collect()?;
        for db_id in &attached_db_ids {
            // Reprepare must not roll back an explicit transaction. SQLite allows
            // reprepare inside a transaction, and uncommitted writes in temp or
            // attached databases remain visible after the statement is retried.
            if db_id == crate::TEMP_DB_ID || !conn.get_auto_commit() {
                continue;
            }
            // Discard any connection-local schema changes for this attached DB
            // so the re-translate reads the committed schema.
            conn.database_schemas().write().remove(&db_id);
            let pager = conn.get_pager_from_database_index(&db_id)?;
            if pager.holds_read_lock() {
                pager.rollback_attached();
            }
        }

        // Refresh from shared schema only when shared is newer; this preserves a
        // connection-local schema that is ahead of shared. An MVCC checkpoint can
        // publish new btree roots without bumping the schema cookie, so
        // same-version reprepare still refreshes it.
        conn.refresh_schema_from_shared_for_reprepare();
        let new_program = {
            let (cmd, _) = conn.parse_sql(&self.program.sql)?;
            let cmd = cmd.expect("Same SQL string should be able to be parsed");

            let syms = conn.syms.read();
            let mode = self.query_mode;
            #[cfg(debug_assertions)]
            crate::turso_assert_eq!(QueryMode::new(&cmd), mode);
            let (Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan { stmt, .. }) = cmd;
            let schema = conn.schema.read().clone();
            let prepare_options = PrepareOptions::default();
            translate::translate(
                &schema,
                stmt,
                self.pager.clone(),
                conn.clone(),
                &syms,
                mode,
                &self.program.sql,
                self.origin,
                &prepare_options,
            )?
        };

        // Save parameters before they are reset
        let parameters = std::mem::take(&mut self.state.parameters);
        let (max_registers, cursor_count) = match self.query_mode {
            QueryMode::Normal => (new_program.max_registers, new_program.cursor_ref.len()),
            QueryMode::Explain => (EXPLAIN_COLUMNS.len(), 0),
            QueryMode::ExplainQueryPlan {
                format: EqpFormat::Text,
            } => (EXPLAIN_QUERY_PLAN_COLUMNS.len(), 0),
            QueryMode::ExplainQueryPlan {
                format: EqpFormat::Json,
            } => (EXPLAIN_QUERY_PLAN_JSON_COLUMNS.len(), 0),
        };
        // Repreparing a root statement must not make it disappear from
        // `n_active_root_statements` while it is still logically in progress.
        self.reset_internal(
            Some(max_registers),
            Some(cursor_count),
            self.counted_as_active_root,
        )?;
        self.state.metrics.reprepares = self.state.metrics.reprepares.saturating_add(1);
        self.program = new_program;
        // Load the parameters back into the state
        self.state.parameters = parameters;
        Ok(())
    }

    pub fn num_columns(&self) -> usize {
        match self.query_mode {
            QueryMode::Normal => self.program.result_columns.len(),
            QueryMode::Explain => EXPLAIN_COLUMNS.len(),
            QueryMode::ExplainQueryPlan {
                format: EqpFormat::Text,
            } => EXPLAIN_QUERY_PLAN_COLUMNS.len(),
            QueryMode::ExplainQueryPlan {
                format: EqpFormat::Json,
            } => EXPLAIN_QUERY_PLAN_JSON_COLUMNS.len(),
        }
    }

    pub fn get_column_name(&self, idx: usize) -> Cow<'_, str> {
        if self.query_mode == QueryMode::Explain {
            return Cow::Owned(EXPLAIN_COLUMNS.get(idx).expect("No column").to_string());
        }
        if let QueryMode::ExplainQueryPlan { format } = self.query_mode {
            let columns: &[&str] = match format {
                EqpFormat::Text => &EXPLAIN_QUERY_PLAN_COLUMNS,
                EqpFormat::Json => &EXPLAIN_QUERY_PLAN_JSON_COLUMNS,
            };
            return Cow::Owned(columns.get(idx).expect("No column").to_string());
        }
        match self.query_mode {
            QueryMode::Normal => {
                let column = &self.program.result_columns.get(idx).expect("No column");

                // 1. Explicit alias (AS clause) or SELECT * expansion always wins.
                if let Some(alias) = &column.alias {
                    return Cow::Borrowed(alias);
                }

                let full = self.program.connection.get_full_column_names();
                let short = self.program.connection.get_short_column_names();

                // 2. For column references, apply full/short column name logic.
                match &column.expr {
                    turso_parser::ast::Expr::Column {
                        table,
                        column: col_idx,
                        ..
                    } => {
                        if full {
                            // full_column_names=ON: use REAL_TABLE_NAME.COLUMN
                            if let Some((_, table_ref)) = self
                                .program
                                .table_references
                                .find_table_by_internal_id(*table)
                            {
                                let col_name = table_ref
                                    .get_column_at(*col_idx)
                                    .and_then(|c| c.name.as_deref())
                                    .unwrap_or("?");
                                return Cow::Owned(format!(
                                    "{}.{}",
                                    table_ref.get_name(),
                                    col_name
                                ));
                            }
                        }
                        if short || full {
                            // short_column_names=ON: use just COLUMN
                            if let Some(name) = column.name(&self.program.table_references) {
                                return Cow::Borrowed(name);
                            }
                        }
                        // Both OFF: use original expression text
                        if let Some(name) = &column.implicit_column_name {
                            Cow::Borrowed(name.as_str())
                        } else {
                            let tables = [&self.program.table_references];
                            let ctx = PlanContext(&tables);
                            Cow::Owned(column.expr.displayer(&ctx).to_string())
                        }
                    }
                    _ => {
                        // Non-column-ref: use implicit_column_name or displayer
                        match column.name(&self.program.table_references) {
                            Some(name) => Cow::Borrowed(name),
                            None => {
                                let tables = [&self.program.table_references];
                                let ctx = PlanContext(&tables);
                                Cow::Owned(column.expr.displayer(&ctx).to_string())
                            }
                        }
                    }
                }
            }
            QueryMode::Explain => Cow::Borrowed(EXPLAIN_COLUMNS[idx]),
            QueryMode::ExplainQueryPlan {
                format: EqpFormat::Text,
            } => Cow::Borrowed(EXPLAIN_QUERY_PLAN_COLUMNS[idx]),
            QueryMode::ExplainQueryPlan {
                format: EqpFormat::Json,
            } => Cow::Borrowed(EXPLAIN_QUERY_PLAN_JSON_COLUMNS[idx]),
        }
    }

    pub fn get_column_table_name(&self, idx: usize) -> Option<Cow<'_, str>> {
        if matches!(
            self.query_mode,
            QueryMode::Explain | QueryMode::ExplainQueryPlan { .. }
        ) {
            return None;
        }
        let column = &self.program.result_columns.get(idx).expect("No column");
        match &column.expr {
            turso_parser::ast::Expr::Column { table, .. } => self
                .program
                .table_references
                .find_table_by_internal_id(*table)
                .map(|(_, table_ref)| Cow::Borrowed(table_ref.get_name())),
            _ => None,
        }
    }

    /// Returns the declared type of a result column.
    ///
    /// This behaves similarly to SQLite's `sqlite3_column_decltype()`:
    /// If the Nth column of the returned result set of a SELECT is a table column
    /// (not an expression or subquery) then the declared type of the table column
    /// is returned. If the Nth column of the result set is an expression or subquery,
    /// then None is returned. The returned string is always UTF-8 encoded.
    ///
    /// See: <https://sqlite.org/c3ref/column_decltype.html>
    pub fn get_column_decltype(&self, idx: usize) -> Option<String> {
        if self.query_mode == QueryMode::Explain {
            return Some(
                EXPLAIN_COLUMNS_TYPE
                    .get(idx)
                    .expect("No column")
                    .to_string(),
            );
        }
        if let QueryMode::ExplainQueryPlan { format } = self.query_mode {
            let column_types: &[&str] = match format {
                EqpFormat::Text => &EXPLAIN_QUERY_PLAN_COLUMNS_TYPE,
                EqpFormat::Json => &EXPLAIN_QUERY_PLAN_JSON_COLUMNS_TYPE,
            };
            return Some(column_types.get(idx).expect("No column").to_string());
        }
        let column = &self.program.result_columns.get(idx).expect("No column");
        match &column.expr {
            turso_parser::ast::Expr::Column {
                table,
                column: column_idx,
                ..
            } => {
                let (_, table_ref) = self
                    .program
                    .table_references
                    .find_table_by_internal_id(*table)?;
                let table_column = table_ref.get_column_at(*column_idx)?;
                let ty_str = &table_column.ty_str;
                if ty_str.is_empty() {
                    None
                } else {
                    Some(ty_str.clone())
                }
            }
            _ => None,
        }
    }

    /// Returns rich type information for a result column.
    ///
    /// This is Turso's single entry point for "what is the type of this
    /// column?" — covering both **direct table-column references** (where the
    /// schema carries declared name, array depth, custom-type kind, and the
    /// resolved primitive) and **computed expressions** (where the SQLite-
    /// style affinity machinery infers a primitive type from the expression
    /// shape). One call, one shape, regardless of which path applies.
    ///
    /// ### Return value
    ///
    /// - `Err(_)` when this connection does not have the experimental
    ///   custom-types feature enabled. This API is the public surface of the
    ///   custom-types system; callers must opt in by enabling
    ///   `--experimental-custom-types` (or `DatabaseOpts::with_custom_types`)
    ///   before they can rely on it.
    /// - `Ok(None)` when the statement is in EXPLAIN mode, when `idx` is out
    ///   of bounds, when the result column has no schema column behind it
    ///   AND the affinity machinery returns `BLOB` (i.e. "no determined
    ///   affinity"), or when a join/CTE reference can't be resolved.
    /// - `Ok(Some(info))` otherwise. For a table-column reference, `info`
    ///   carries the declared name verbatim; for an expression, `declared_name`
    ///   is the inferred-affinity primitive (`"INTEGER"`, `"TEXT"`, `"REAL"`,
    ///   or `"NUMERIC"`) and `kind` is `Builtin`.
    ///
    /// This is a Turso-specific API; it has no `sqlite3_*` counterpart. The
    /// returned struct is `#[non_exhaustive]` so additional metadata can be
    /// added over time without breaking callers.
    pub fn get_column_type_info(&self, idx: usize) -> Result<Option<ColumnTypeInfo>> {
        if !self.program.connection.experimental_custom_types_enabled() {
            return Err(LimboError::ParseError(
                "get_column_type_info requires --experimental-custom-types".to_string(),
            ));
        }
        if self.query_mode != QueryMode::Normal {
            return Ok(None);
        }
        let Some(column) = self.program.result_columns.get(idx) else {
            return Ok(None);
        };
        // Direct table-column reference: pull declared name, array depth, and
        // any registered CREATE TYPE / CREATE DOMAIN resolution out of the
        // schema. Anything else falls through to the expression-affinity
        // inference path below.
        if let turso_parser::ast::Expr::Column {
            table,
            column: column_idx,
            ..
        } = &column.expr
        {
            let Some((_, table_ref)) = self
                .program
                .table_references
                .find_table_by_internal_id(*table)
            else {
                return Ok(None);
            };
            let Some(table_column) = table_ref.get_column_at(*column_idx) else {
                return Ok(None);
            };
            let declared_name = table_column.ty_str.clone();
            let array_dimensions = table_column.array_dimensions();
            let schema = self.program.connection.schema.read();
            let resolved = schema
                .resolve_type(&declared_name, table_ref.is_strict())
                .ok()
                .flatten();
            // `kind` is computed from the leaf TypeDef in the resolution chain:
            // STRUCT and UNION are tagged on `TypeDefKind`, DOMAIN is tagged
            // separately on `TypeDef.is_domain`, and anything else registered
            // through CREATE TYPE is a Custom. A column whose declared name
            // does not appear in the type registry is a Builtin.
            let (base_type, kind) = match resolved {
                Some(resolved) => {
                    let leaf = resolved.leaf();
                    let kind = if leaf.is_struct() {
                        ColumnTypeKind::Struct
                    } else if leaf.is_union() {
                        ColumnTypeKind::Union
                    } else if leaf.is_domain {
                        ColumnTypeKind::Domain
                    } else {
                        ColumnTypeKind::Custom
                    };
                    (Some(resolved.primitive.to_uppercase()), kind)
                }
                None => (None, ColumnTypeKind::Builtin),
            };
            drop(schema);
            return Ok(Some(ColumnTypeInfo {
                declared_name,
                array_dimensions,
                base_type,
                kind,
            }));
        }
        // Not a table column: infer the result primitive from the
        // expression's shape (literal value type, operand types of a binary
        // op, the CAST target, etc.).
        let Some(name) =
            infer_expression_primitive(&column.expr, Some(&self.program.table_references))
        else {
            return Ok(None);
        };
        Ok(Some(ColumnTypeInfo {
            declared_name: name.to_string(),
            array_dimensions: 0,
            base_type: None,
            kind: ColumnTypeKind::Builtin,
        }))
    }

    /// Returns the type affinity name of a result column (e.g., "INTEGER", "TEXT", "REAL", "BLOB", "NUMERIC").
    ///
    /// Unlike `get_column_decltype` which returns the original declared type string,
    /// this method returns the normalized SQLite type affinity name.
    pub fn get_column_type_name(&self, idx: usize) -> Option<String> {
        if self.query_mode == QueryMode::Explain {
            return Some(
                EXPLAIN_COLUMNS_TYPE
                    .get(idx)
                    .expect("No column")
                    .to_string(),
            );
        }
        if let QueryMode::ExplainQueryPlan { format } = self.query_mode {
            let column_types: &[&str] = match format {
                EqpFormat::Text => &EXPLAIN_QUERY_PLAN_COLUMNS_TYPE,
                EqpFormat::Json => &EXPLAIN_QUERY_PLAN_JSON_COLUMNS_TYPE,
            };
            return Some(column_types.get(idx).expect("No column").to_string());
        }
        let column = &self.program.result_columns.get(idx).expect("No column");
        match &column.expr {
            turso_parser::ast::Expr::Column {
                table,
                column: column_idx,
                ..
            } => {
                let (_, table_ref) = self
                    .program
                    .table_references
                    .find_table_by_internal_id(*table)?;
                let table_column = table_ref.get_column_at(*column_idx)?;
                match &table_column.ty() {
                    crate::schema::Type::Integer => Some("INTEGER".to_string()),
                    crate::schema::Type::Real => Some("REAL".to_string()),
                    crate::schema::Type::Text => Some("TEXT".to_string()),
                    crate::schema::Type::Blob => Some("BLOB".to_string()),
                    crate::schema::Type::Numeric => Some("NUMERIC".to_string()),
                    crate::schema::Type::Null => None,
                }
            }
            _ => None,
        }
    }

    /// Returns the inferred type affinity name for a result column by examining
    /// the column expression. Unlike `get_column_decltype` which only works for
    /// table columns, this works for arbitrary expressions (CAST, function calls,
    /// literals, etc.) by inferring the type from the expression structure.
    pub fn get_column_inferred_type(&self, idx: usize) -> Option<String> {
        if self.query_mode != QueryMode::Normal {
            return None;
        }
        let column = &self.program.result_columns.get(idx)?;
        let affinity = translate::expr::get_expr_affinity(
            &column.expr,
            Some(&self.program.table_references),
            None,
        );
        affinity_to_primitive(affinity).map(str::to_string)
    }

    pub fn parameters(&self) -> &parameters::Parameters {
        &self.program.parameters
    }

    pub fn parameters_count(&self) -> usize {
        self.program.parameters.count()
    }

    pub fn parameter_index(&self, name: &str) -> Option<NonZero<usize>> {
        self.program.parameters.index(name)
    }

    pub fn bind_at(&mut self, index: NonZero<usize>, value: Value) -> Result<()> {
        self.state.bind_at(index, value)?;
        Ok(())
    }

    /// Returns the SQL text with every parameter marker replaced by the
    /// literal of its currently bound value (NULL when unbound), following
    /// SQLite's sqlite3_expanded_sql rendering. The text is re-tokenized
    /// with the lexer — whose token stream partitions the input, emitting
    /// whitespace and comments as TK_NONE tokens carrying their bytes — so
    /// string literals, quoted identifiers and comments are never mistaken
    /// for markers. Each TK_VARIABLE token resolves to its bind index from
    /// its own text: `?N` carries the number, named markers are looked up
    /// in the parameter table (the same marker can occur several times),
    /// and a bare `?` takes one more than the largest number assigned so
    /// far, which is SQLite's numbering rule.
    pub fn expanded_sql(&self) -> String {
        let sql = self.get_sql();
        let params = &self.program.parameters;
        let mut out = String::with_capacity(sql.len());
        let mut max_index = 0usize;
        for token in turso_parser::lexer::Lexer::new(sql.as_bytes()) {
            let Ok(token) = token else {
                // The statement already parsed once, so re-lexing its text
                // cannot fail; return the unexpanded SQL if it somehow does.
                return sql.to_string();
            };
            if token.token_type == turso_parser::token::TokenType::TK_VARIABLE {
                let text = String::from_utf8_lossy(token.value);
                let index = if let Some(digits) = text.strip_prefix('?') {
                    if digits.is_empty() {
                        max_index + 1
                    } else {
                        digits.parse().unwrap_or(0)
                    }
                } else {
                    params.index(text.as_ref()).map_or(0, |i| i.get())
                };
                max_index = max_index.max(index);
                let value = NonZero::new(index)
                    .map(|i| self.state.get_parameter(i))
                    .unwrap_or(Value::Null);
                append_expanded_literal(&mut out, &value);
            } else {
                out.push_str(&String::from_utf8_lossy(token.value));
            }
        }
        out
    }

    pub fn clear_bindings(&mut self) {
        self.state.clear_bindings();
    }

    pub fn reset(&mut self) -> Result<()> {
        self.reset_internal(None, None, false)
    }

    /// If `Insn::SequenceBeginInnerTx` swapped the connection's mv_tx to
    /// an inner tx and the statement aborted before `SequenceCommitInnerTx`
    /// could clean it up, roll back the inner and restore the outer
    /// mv_tx. Otherwise the inner is leaked: it stays in `mv_store.txs`
    /// (so subsequent `commit_dep_counter` walks may wait on it forever)
    /// and the connection's mv_tx points to a dead tx, breaking the
    /// next statement that runs on the connection.
    fn cleanup_orphaned_seq_inner_tx(&mut self) {
        let Some(pending) = self.state.sequence_inner_tx_pending.take() else {
            return;
        };
        let conn = self.program.connection.clone();
        let Some(mv_store) = conn.mv_store_for_db(pending.db) else {
            return;
        };
        if mv_store.is_tx_rollbackable(pending.inner_tx_id) {
            mv_store.rollback_tx(pending.inner_tx_id, self.pager.clone(), &conn, pending.db);
        }
        conn.set_mv_tx_for_db(pending.db, pending.saved_outer);
        // When the inner tx aborted via the vdbe's catch-all error path
        // (e.g. DatabaseFull on sequence exhaustion), rollback_current_txn_state
        // rolled back what mv_tx pointed at — the inner — and set
        // auto_commit=true under the assumption it was the only live tx.
        // Restoring mv_tx to the outer without also restoring auto_commit=false
        // leaves the connection in an inconsistent state where auto_commit=true
        // but mv_tx points to a live outer tx, which causes subsequent BEGINs
        // to silently no-op and pins the caller to the outer's stale snapshot.
        if pending.saved_outer.is_some() {
            conn.auto_commit.store(false, Ordering::SeqCst);
        }
        // The commit-state-machine, if any was in flight, is now dead:
        // the inner tx it was committing is gone.
        self.state.sequence_inner_commit = None;
    }

    pub fn reset_best_effort(&mut self) {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.reset())) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                tracing::error!("Statement reset failed during best-effort cleanup: {err}");
            }
            Err(_) => {
                tracing::error!("Statement reset panicked during best-effort cleanup");
            }
        }
    }

    /// Lightweight reset for reusing a cached subprogram statement.
    /// Skips transaction handling and abort(): the caller (op_program) has
    /// already handled trigger execution tracking. Only resets ProgramState
    /// fields so the subprogram can run again from the beginning.
    pub fn reset_for_subprogram_reuse(&mut self) {
        self.cleanup_orphaned_seq_inner_tx();
        self.state.reset(None, None);
        self.state
            .n_change
            .store(0, std::sync::atomic::Ordering::Release);
        self.busy = false;
        self.has_returned_row = false;
    }

    fn reset_internal(
        &mut self,
        max_registers: Option<usize>,
        max_cursors: Option<usize>,
        preserve_active_root_count: bool,
    ) -> Result<()> {
        fn capture_reset_error(
            reset_error: &mut Option<LimboError>,
            err: LimboError,
            context: &str,
        ) {
            tracing::error!("{context}: {err}");
            if reset_error.is_none() {
                *reset_error = Some(err);
            }
        }

        let mut reset_error: Option<LimboError> = None;

        if let Some(io) = self.state.io_completions.take() {
            if let Err(err) = io.wait(self.pager.io.as_ref()) {
                capture_reset_error(
                    &mut reset_error,
                    err,
                    "Error while draining pending IO during statement reset",
                );
            }
        }

        if self.state.execution_state.is_running() {
            if self.query_mode == QueryMode::Normal
                && self.program.change_cnt_on
                && self.has_returned_row
            {
                // Write statement with RETURNING, user got at least one Row.
                // With ephemeral-buffered RETURNING, ALL DML completed before any
                // rows were yielded. The remaining work is just the scan-back
                // (in-memory) + Halt. Commit the transaction via halt().
                let mut halt_completed = false;
                loop {
                    match vdbe::execute::halt(
                        &self.program,
                        &mut self.state,
                        &self.pager,
                        0,
                        "",
                        None,
                    ) {
                        Ok(vdbe::execute::InsnFunctionStepResult::Done) => {
                            halt_completed = true;
                            break;
                        }
                        Ok(vdbe::execute::InsnFunctionStepResult::IO(_)) => {
                            if let Err(e) = self.pager.io.step() {
                                capture_reset_error(
                                    &mut reset_error,
                                    e,
                                    "Error committing during statement reset",
                                );
                                break;
                            }
                        }
                        Err(e) => {
                            capture_reset_error(
                                &mut reset_error,
                                *e,
                                "Error halting statement during reset",
                            );
                            break;
                        }
                        Ok(vdbe::execute::InsnFunctionStepResult::Row)
                        | Ok(vdbe::execute::InsnFunctionStepResult::Step) => {
                            capture_reset_error(
                                &mut reset_error,
                                LimboError::InternalError(
                                    "Unexpected halt result during reset".to_string(),
                                ),
                                "Statement reset encountered unexpected halt result",
                            );
                            break;
                        }
                    }
                }

                if !halt_completed {
                    if let Err(abort_err) = self.program.abort(
                        &self.pager,
                        reset_error.as_ref(),
                        &mut self.state,
                        self.counted_as_active_root,
                    ) {
                        capture_reset_error(
                            &mut reset_error,
                            abort_err,
                            "Abort failed during statement reset",
                        );
                    }
                }
            } else {
                // Either a read-only statement, a write statement that never
                // yielded a Row (DML still in progress or hit Busy/error), or a
                // write statement without RETURNING. Rollback to avoid committing
                // partial DML or silently retrying after transient errors (Busy).
                if let Err(abort_err) = self.program.abort(
                    &self.pager,
                    None,
                    &mut self.state,
                    self.counted_as_active_root,
                ) {
                    capture_reset_error(
                        &mut reset_error,
                        abort_err,
                        "Abort failed during statement reset",
                    );
                }
            }
        } else {
            // Statement not running (Done/Failed/Init) — cleanup only.
            if let Err(abort_err) = self.program.abort(
                &self.pager,
                None,
                &mut self.state,
                self.counted_as_active_root,
            ) {
                capture_reset_error(
                    &mut reset_error,
                    abort_err,
                    "Abort failed during statement reset",
                );
            }
        }
        // Safety net: if end_statement wasn't reached (e.g. statement dropped
        // mid-execution), ensure n_active_writes is decremented before reset
        // clears the flag.
        if self.state.is_active_write {
            let previous = self
                .program
                .connection
                .n_active_writes
                .fetch_sub(1, Ordering::SeqCst);
            turso_assert!(
                previous == 1,
                "resetting a writer with {previous} active writer(s)"
            );
            self.state.is_active_write = false;
        }
        if self.counted_as_active_root && !preserve_active_root_count {
            self.release_active_root_if_counted();
        }
        self.cleanup_orphaned_seq_inner_tx();
        self.state.reset(max_registers, max_cursors);
        self.busy = false;
        self.busy_handler_state = None;
        self.query_timeout_override = None;
        self.has_returned_row = false;

        if let Some(err) = reset_error {
            return Err(err);
        }
        Ok(())
    }

    pub fn row(&self) -> Option<&Row> {
        self.state.result_row.as_ref()
    }

    pub fn get_sql(&self) -> &str {
        &self.program.sql
    }

    pub fn is_busy(&self) -> bool {
        self.busy
    }

    /// Internal method to get IO from a statement.
    /// Used by select internal crate
    ///
    /// Avoid using this method for advancing IO while iteration over `step`.
    /// Prefer to use helper methods instead such as [Self::run_with_row_callback]
    pub fn _io(&self) -> &dyn crate::IO {
        self.pager.io.as_ref()
    }
}

/// Appends `value` to `out` as a SQL literal, the way SQLite renders bound
/// parameters into expanded SQL: NULL, decimal integers, floats, single-quoted
/// text with embedded quotes doubled, and x'..' hex blobs.
///
/// This deliberately does not reuse Value::exec_quote: SQLite's quote() and
/// its expanded-SQL rendering differ (quote() emits X'..' uppercase blobs and
/// truncates text at an embedded NUL; expanded SQL emits x'..' lowercase).
fn append_expanded_literal(out: &mut String, value: &Value) {
    use std::fmt::Write;
    match value {
        Value::Null => out.push_str("NULL"),
        Value::Text(t) => {
            out.push('\'');
            for ch in t.as_str().chars() {
                if ch == '\'' {
                    out.push('\'');
                }
                out.push(ch);
            }
            out.push('\'');
        }
        Value::Blob(b) => {
            out.push_str("x'");
            for byte in b.iter() {
                let _ = write!(out, "{byte:02x}");
            }
            out.push('\'');
        }
        other => {
            let _ = write!(out, "{other}");
        }
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        // Keep helper statements nested while drop-time reset/abort cleanup runs.
        // That cleanup consults `is_nested_stmt()` to decide whether top-level
        // transaction/savepoint finalization belongs to this statement or to its
        // parent, so we release the nested guard only after reset completes.
        self.reset_best_effort();
        if self.nested_guard_active {
            self.program.connection.end_nested();
            self.nested_guard_active = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SqliteDialect;
    use crate::{Database, DatabaseOpts, MemoryIO, OpenFlags, IO};

    fn open_test_connection() -> crate::Result<Arc<crate::Connection>> {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            ":memory:",
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )?;
        db.connect()
    }

    #[test]
    fn test_expanded_sql() {
        let conn = open_test_connection().unwrap();
        let mut stmt = conn
            .prepare("SELECT ?1, :nm, ':nm' /* ? */, -- ?\n?")
            .unwrap();

        // Unbound parameters render as NULL; markers inside string
        // literals and comments are untouched.
        assert_eq!(
            stmt.expanded_sql(),
            "SELECT NULL, NULL, ':nm' /* ? */, -- ?\nNULL"
        );

        stmt.bind_at(1.try_into().unwrap(), Value::from_i64(42))
            .unwrap();
        let nm = stmt.parameter_index(":nm").unwrap();
        stmt.bind_at(nm, Value::build_text("it's")).unwrap();
        stmt.bind_at(3.try_into().unwrap(), Value::from_slice(&[1, 2]).unwrap())
            .unwrap();
        assert_eq!(
            stmt.expanded_sql(),
            "SELECT 42, 'it''s', ':nm' /* ? */, -- ?\nx'0102'"
        );

        // Bindings survive reset; clear_bindings reverts them to NULL.
        stmt.reset().unwrap();
        assert_eq!(
            stmt.expanded_sql(),
            "SELECT 42, 'it''s', ':nm' /* ? */, -- ?\nx'0102'"
        );
        stmt.clear_bindings();
        assert_eq!(
            stmt.expanded_sql(),
            "SELECT NULL, NULL, ':nm' /* ? */, -- ?\nNULL"
        );

        // A marker can occur several times and renders its value at every
        // occurrence; a bare ? after ?2 takes index 3.
        let mut stmt = conn.prepare("SELECT ?2, :a, ?2, ?").unwrap();
        stmt.bind_at(2.try_into().unwrap(), Value::from_i64(7))
            .unwrap();
        stmt.bind_at(3.try_into().unwrap(), Value::build_text("x"))
            .unwrap();
        stmt.bind_at(4.try_into().unwrap(), Value::from_i64(9))
            .unwrap();
        assert_eq!(stmt.expanded_sql(), "SELECT 7, 'x', 7, 9");
    }

    #[test]
    fn test_tcl_style_parameter_names_bind_and_expand() {
        // The TCL binding passes namespace-qualified variables ($::x,
        // $ns::y) and array elements ($arr(k)) as parameter names. Each
        // spelling is one parameter, found by its full text, and expanded
        // SQL — which re-lexes the statement text — sees the same markers
        // the parse did, so the bound values land in the right places.
        let conn = open_test_connection().unwrap();
        let mut stmt = conn
            .prepare("SELECT $::x, $ns::y, $arr(k), $::x, '$::x'")
            .unwrap();
        let x = stmt.parameter_index("$::x").unwrap();
        let y = stmt.parameter_index("$ns::y").unwrap();
        let k = stmt.parameter_index("$arr(k)").unwrap();
        assert_eq!(stmt.parameters_count(), 3);
        stmt.bind_at(x, Value::from_i64(1)).unwrap();
        stmt.bind_at(y, Value::build_text("two")).unwrap();
        stmt.bind_at(k, Value::from_i64(3)).unwrap();

        assert_eq!(stmt.expanded_sql(), "SELECT 1, 'two', 3, 1, '$::x'");
        let rows = stmt.run_collect_rows().unwrap();
        assert_eq!(
            rows,
            vec![vec![
                Value::from_i64(1),
                Value::build_text("two"),
                Value::from_i64(3),
                Value::from_i64(1),
                Value::build_text("$::x"),
            ]]
        );
    }

    #[test]
    fn test_metrics_persist_across_reset() {
        let conn = open_test_connection().unwrap();
        conn.execute("CREATE TABLE t(x)").unwrap();
        conn.metrics.write().reset();

        let mut stmt = conn.prepare("INSERT INTO t VALUES (1)").unwrap();
        stmt.run_ignore_rows().unwrap();
        assert_eq!(stmt.metrics().rows_written, 1);

        stmt.reset().unwrap();
        assert_eq!(stmt.metrics().rows_written, 1);

        stmt.run_ignore_rows().unwrap();
        assert_eq!(stmt.metrics().rows_written, 2);

        stmt.reset_metrics();
        assert_eq!(stmt.metrics().rows_written, 0);
    }

    #[test]
    fn test_run_with_row_callback_nonblock_collects_all_rows() {
        let conn = open_test_connection().unwrap();
        conn.execute("CREATE TABLE t(x)").unwrap();
        conn.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
            .unwrap();

        let io = conn.db.io.clone();
        let mut stmt = conn.prepare("SELECT x FROM t ORDER BY x").unwrap();

        // Drive the non-blocking runner via the IOResult loop, exactly as a
        // state-machine caller would: collect into an accumulator that persists
        // across yields and wait on each yielded completion.
        let mut collected: Vec<i64> = Vec::new();
        loop {
            let res = stmt
                .run_with_row_callback_nonblock(|row| {
                    collected.push(row.get::<i64>(0)?);
                    Ok(())
                })
                .unwrap();
            match res {
                crate::IOResult::Done(()) => break,
                crate::IOResult::IO(c) => c.wait(io.as_ref()).unwrap(),
            }
        }
        assert_eq!(collected, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_run_ignore_rows_nonblock_completes() {
        let conn = open_test_connection().unwrap();
        conn.execute("CREATE TABLE t(x)").unwrap();

        let io = conn.db.io.clone();
        let mut stmt = conn.prepare("INSERT INTO t VALUES (1), (2)").unwrap();
        loop {
            match stmt.run_ignore_rows_nonblock().unwrap() {
                crate::IOResult::Done(()) => break,
                crate::IOResult::IO(c) => c.wait(io.as_ref()).unwrap(),
            }
        }
        assert_eq!(stmt.metrics().rows_written, 2);
    }

    #[test]
    fn test_metrics_include_subprogram_writes() {
        let conn = open_test_connection().unwrap();
        conn.execute("CREATE TABLE src(x)").unwrap();
        conn.execute("CREATE TABLE log(x)").unwrap();
        conn.execute(
            "CREATE TRIGGER src_log AFTER INSERT ON src BEGIN INSERT INTO log VALUES (new.x); END",
        )
        .unwrap();

        let mut stmt = conn.prepare("INSERT INTO src VALUES (1), (2)").unwrap();
        stmt.run_ignore_rows().unwrap();

        assert_eq!(
            stmt.metrics().rows_written,
            6,
            "cumulative metrics should include root and trigger writes"
        );
    }
}
