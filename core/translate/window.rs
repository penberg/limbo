use crate::alloc::TursoSliceExt;
use crate::function::{AccumulatorFunc, AggFunc, WindowFunc};
use crate::schema::{BTreeCharacteristics, BTreeTable, Index, IndexColumn, Table};
use crate::sync::Arc;
use crate::translate::aggregation::{translate_aggregation_step, AggArgumentSource};
use crate::translate::collate::{get_collseq_from_expr, CollationSeq};
use crate::translate::emitter::{Resolver, TranslateCtx};
use crate::translate::expr::{
    expr_contains_nondeterministic_scalar_function, translate_expr, translate_expr_no_constant_opt,
    walk_expr, walk_expr_mut, NoConstantOptReason, WalkControl,
};
use crate::translate::order_by::EmitOrderBy;
use crate::translate::plan::{
    Aggregate, Distinctness, JoinOrderMember, JoinedTable, QueryDestination, ResultSetColumn,
    RewrittenWindowCall, SelectPlan, TableReferences, Window, WindowFunction,
};
use crate::translate::planner::resolve_window_and_aggregate_functions;
use crate::translate::result_row::emit_select_result;
use crate::translate::subquery::plan_subqueries_from_select_plan;
use crate::types::KeyInfo;
use crate::util::exprs_are_equivalent;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{
    to_u32, AggStepData, {IdxInsertFlags, InsertFlags, Insn},
};
use crate::vdbe::{BranchOffset, CursorID};
use crate::Connection;
use crate::Result;
use crate::{turso_assert, turso_assert_eq};
use std::mem;
use turso_parser::ast::Name;
use turso_parser::ast::{Expr, Literal, Over, SortOrder, TableInternalId};

const SUBQUERY_DATABASE_ID: usize = 0;

struct WindowSubqueryContext<'a> {
    resolver: &'a Resolver<'a>,
    subquery_order_by: &'a mut Vec<(Box<Expr>, SortOrder, Option<turso_parser::ast::NullsOrder>)>,
    subquery_result_columns: &'a mut Vec<ResultSetColumn>,
    subquery_id: &'a TableInternalId,
}

/// Rewrite a `SELECT` plan for window function processing.
///
/// A `SELECT` may reference multiple window definitions, but internally, each `SELECT` plan
/// operates on **exactly one** window. Multiple window functions may reference the same window.
///
/// The original plan is rewritten into a series of nested subqueries, each  bound to a single
/// window definition. Each subquery produces rows in the order determined by its parent window
/// definition. The innermost subquery does not have any window assigned to it; instead,
/// the FROM, WHERE, GROUP BY, and HAVING clauses from the original query are pushed down to it.
/// The outermost query retains ORDER BY, LIMIT, and OFFSET.
///
/// # Examples
/// ```sql
/// -- Example 1: Query with one window
/// SELECT
///     a+1,
///     max(b) OVER (PARTITION BY c ORDER BY d),
///     min(c) OVER (PARTITION BY c ORDER BY d)
/// FROM t1
/// ORDER BY e;
///
/// -- Rewritten form
/// SELECT
///     a+1,
///     max(b) OVER (PARTITION BY c ORDER BY d),
///     min(c) OVER (PARTITION BY c ORDER BY d)
/// FROM (SELECT a, b, c, d, e FROM t1 ORDER BY c, d)
/// ORDER BY e;
///
/// -- Example 2: Query with multiple windows
/// SELECT
///     a,
///     max(b) OVER (PARTITION BY c ORDER BY d),
///     min(c) OVER (PARTITION BY e ORDER BY f)
/// FROM t1;
///
/// -- Rewritten form
/// SELECT
///     a,
///     max(b) OVER (PARTITION BY c ORDER BY d) AS w1,
///     w2
/// FROM (
///     SELECT
///         a,
///         b,
///         c,
///         d,
///         min(c) OVER (PARTITION BY e ORDER BY f) AS w2
///     FROM (SELECT a, b, c, d, e, f FROM t1 ORDER BY e, f)
///     ORDER BY c, d
/// );
/// ```
#[turso_macros::trace_stack]
pub fn plan_windows(
    program: &mut ProgramBuilder,
    plan: &mut SelectPlan,
    resolver: &Resolver,
    connection: &Arc<Connection>,
    windows: &mut Vec<Window>,
) -> crate::Result<()> {
    // Remove named windows that are not referenced by any function, as they can be ignored.
    windows.retain(|w| !w.functions.is_empty());

    if !windows.is_empty() {
        // Sanity check: this should never happen because the syntax disallows combining VALUES with windows
        turso_assert!(
            plan.values.is_empty(),
            "VALUES clause with windows is not supported"
        );
    }

    prepare_window_subquery(program, plan, resolver, connection, windows, 0)
}

fn prepare_window_subquery(
    program: &mut ProgramBuilder,
    outer_plan: &mut SelectPlan,
    resolver: &Resolver,
    connection: &Arc<Connection>,
    windows: &mut Vec<Window>,
    processed_window_count: usize,
) -> crate::Result<()> {
    if windows.is_empty() {
        // The innermost plan holds the original FROM/WHERE/GROUP BY plus any
        // raw subquery expressions pushed down from outer window layers.
        // Plan them now so they become SubqueryResult nodes with entries in
        // non_from_clause_subqueries.
        plan_subqueries_from_select_plan(program, outer_plan, resolver, connection)?;
        return Ok(());
    }

    // Layer windows in their declaration order: the first-declared window
    // becomes the outermost subquery (its PARTITION/ORDER drives the
    // user-visible row order), and later-declared windows nest deeper. Each
    // window function evaluates against rows in its own layer's order, so
    // the relative position of unordered windows like `OVER ()` matters.
    let mut current_window = windows.remove(0);
    let mut subquery_result_columns = Vec::new();
    let mut subquery_order_by = Vec::new();
    let subquery_id = program.table_reference_counter.next();

    if current_window.name.is_none() {
        // This is part of normalizing the window definition. The remaining logic lives in
        // `rewrite_expr_referencing_current_window`, which replaces inline window definitions
        // with references by name.
        //
        // The goal is to always work with named windows instead of a mix of named and
        // inline ones. This way, we don’t need to rewrite expressions embedded in inline
        // definitions (there might be many equivalent definitions per subquery). Instead,
        // we rewrite the named definition once, and all associated window functions
        // require no additional processing.
        //
        // At this stage, window definitions and window functions are already bound,
        // so this normalization is purely to keep the plan valid.
        //
        // If the generated name is not unique across the entire query, that’s acceptable —
        // the final plan always associates exactly one window with one subquery.
        current_window.name = Some(format!("window_{processed_window_count}"));
    }

    let mut ctx = WindowSubqueryContext {
        resolver,
        subquery_order_by: &mut subquery_order_by,
        subquery_result_columns: &mut subquery_result_columns,
        subquery_id: &subquery_id,
    };

    // Build the ORDER BY clause for the subquery by concatenating the window’s PARTITION BY
    // columns with its ORDER BY columns.This ensures that rows in the subquery are returned
    // in the correct order for partitioning and window function evaluation.
    for expr in current_window.partition_by.iter_mut() {
        append_order_by(outer_plan, expr, &SortOrder::Asc, None, &mut ctx)?;
        current_window.deduplicated_partition_by_len = Some(ctx.subquery_result_columns.len())
    }
    for (expr, order, nulls) in current_window.order_by.iter_mut() {
        append_order_by(outer_plan, expr, order, *nulls, &mut ctx)?;
    }

    // Rewrite expressions from the outer query’s result columns and ORDER BY clause so that
    // they reference the subquery instead. The original expressions are included in the
    // subquery’s result columns.
    for col in outer_plan.result_columns.iter_mut() {
        rewrite_terminal_expr(
            &mut outer_plan.aggregates,
            &mut col.expr,
            &mut current_window,
            &mut ctx,
        )?;
    }
    for (expr, _, _) in outer_plan.order_by.iter_mut() {
        rewrite_terminal_expr(
            &mut outer_plan.aggregates,
            expr,
            &mut current_window,
            &mut ctx,
        )?;
    }

    // When there is no ORDER BY or PARTITION BY clause, the window function takes zero arguments,
    // and no other columns are selected (e.g., "SELECT count() OVER () FROM products"),
    // `subquery_result_columns` may be empty. Add a constant expression to keep the query valid.
    if subquery_result_columns.is_empty() {
        subquery_result_columns.push(ResultSetColumn {
            expr: Expr::Literal(Literal::Numeric("0".to_string())),
            alias: None,
            implicit_column_name: None,
            contains_aggregates: false,
        });
    }

    let new_join_order = vec![JoinOrderMember {
        table_id: subquery_id,
        original_idx: 0,
        is_outer: false,
    }];
    let new_table_references = TableReferences::new(
        vec![],
        outer_plan.table_references.outer_query_refs().to_vec(),
    );

    let mut inner_plan = SelectPlan {
        join_order: mem::replace(&mut outer_plan.join_order, new_join_order),
        table_references: mem::replace(&mut outer_plan.table_references, new_table_references),
        result_columns: subquery_result_columns,
        where_clause: mem::take(&mut outer_plan.where_clause),
        group_by: mem::take(&mut outer_plan.group_by),
        order_by: subquery_order_by,
        aggregates: mem::take(&mut outer_plan.aggregates),
        limit: None,
        offset: None,
        contains_constant_false_condition: false,
        query_destination: QueryDestination::placeholder_for_subquery(),
        distinctness: Distinctness::NonDistinct,
        values: vec![],
        window: None,
        non_from_clause_subqueries: vec![],
        input_cardinality_hint: None,
        estimated_output_rows: None,
        estimated_cost: None,
        simple_aggregate: None,
        phantom_params: vec![],
    };

    prepare_window_subquery(
        program,
        &mut inner_plan,
        resolver,
        connection,
        windows,
        processed_window_count + 1,
    )?;

    let subquery = JoinedTable::new_subquery(
        format!("window_subquery_{processed_window_count}"),
        inner_plan,
        None,
        subquery_id,
    )?;

    // Verify that the subquery has the expected database ID.
    // This is required to ensure that assumptions in `rewrite_terminal_expr` are valid.
    turso_assert_eq!(
        subquery.database_id,
        SUBQUERY_DATABASE_ID,
        "subquery database id must be SUBQUERY_DATABASE_ID",
        {"SUBQUERY_DATABASE_ID": SUBQUERY_DATABASE_ID}
    );

    outer_plan.window = Some(current_window);
    outer_plan.table_references.add_joined_table(subquery);

    Ok(())
}

fn append_order_by(
    plan: &mut SelectPlan,
    expr: &mut Expr,
    sort_order: &SortOrder,
    nulls_order: Option<turso_parser::ast::NullsOrder>,
    ctx: &mut WindowSubqueryContext,
) -> crate::Result<()> {
    // Deduplicate: if an equivalent expression already exists in the subquery ORDER BY,
    // skip adding it again. This can happen when the same column appears in both
    // PARTITION BY and ORDER BY (e.g. OVER (PARTITION BY a ORDER BY a)), and prevents
    // the optimizer assertion group_by.exprs.len() >= order_by.len() from being violated.
    let already_exists = ctx
        .subquery_order_by
        .iter()
        .any(|(existing, _, _)| exprs_are_equivalent(existing, expr));
    if !already_exists {
        ctx.subquery_order_by
            .push((Box::new(expr.clone()), *sort_order, nulls_order));
    }

    let contains_aggregates = resolve_window_and_aggregate_functions(
        expr,
        ctx.resolver,
        &mut plan.aggregates,
        None,
        &mut [],
    )?;
    rewrite_expr_as_subquery_column(expr, ctx, contains_aggregates);
    Ok(())
}

fn rewrite_terminal_expr(
    aggregates: &mut Vec<Aggregate>,
    top_level_expr: &mut Expr,
    current_window: &mut Window,
    ctx: &mut WindowSubqueryContext,
) -> crate::Result<WalkControl> {
    walk_expr_mut(
        top_level_expr,
        &mut |expr: &mut Expr| -> crate::Result<WalkControl> {
            match expr {
                Expr::FunctionCall { filter_over, .. }
                | Expr::FunctionCallStar { filter_over, .. } => {
                    if filter_over.over_clause.is_none() {
                        // If the expression is a standard aggregate (non-window), push it down
                        // to the subquery.
                        if aggregates
                            .iter()
                            .any(|a| exprs_are_equivalent(&a.original_expr, expr))
                        {
                            rewrite_expr_as_subquery_column(expr, ctx, true);
                        }
                    } else if let Some(window_function) =
                        find_window_function_entry(&mut current_window.functions, expr)
                    {
                        // Window function tied to the current window: rewrite its
                        // children to reference the subquery, not the call itself.
                        if let Some(rewritten) = &window_function.rewritten {
                            *expr = rewritten.expr.clone();
                        } else {
                            let window_name = current_window
                                .name
                                .clone()
                                .expect("current_window must always have a name here");
                            let func = window_function.func.clone();
                            window_function.rewritten =
                                Some(rewrite_expr_referencing_current_window(
                                    aggregates,
                                    window_name,
                                    ctx,
                                    expr,
                                    &func,
                                )?);
                        }
                        return Ok(WalkControl::SkipChildren);
                    } else {
                        // Window function referencing a different window. Push the
                        // whole expression to the subquery; it will be rewritten later.
                        rewrite_expr_as_subquery_column(expr, ctx, false);
                    }
                }
                Expr::RowId { .. } | Expr::Column { .. } => {
                    rewrite_expr_as_subquery_column(expr, ctx, false);
                }
                Expr::SubqueryResult { .. }
                | Expr::Exists(..)
                | Expr::InSelect { .. }
                | Expr::Subquery(..) => {
                    rewrite_expr_as_subquery_column(expr, ctx, false);
                    return Ok(WalkControl::SkipChildren);
                }
                _ => {}
            }

            Ok(WalkControl::Continue)
        },
    )
}

/// Find the `WindowFunction` entry that this expression corresponds to.
/// Returns an entry that has not been rewritten yet when one exists.
fn find_window_function_entry<'a>(
    functions: &'a mut [WindowFunction],
    expr: &Expr,
) -> Option<&'a mut WindowFunction> {
    let mut fallback = None;
    let mut chosen = None;
    for (i, f) in functions.iter().enumerate() {
        if !exprs_are_equivalent(&f.original_expr, expr) {
            continue;
        }
        if f.rewritten.is_none() {
            chosen = Some(i);
            break;
        }
        fallback.get_or_insert(i);
    }
    functions.get_mut(chosen.or(fallback)?)
}

/// Add `expr` as an output column of the source subquery (the one being built
/// in `ctx`) and replace `*expr` with a reference to that column. Reuses an
/// existing equivalent column when `expr` is deterministic; nondeterministic
/// calls (e.g. `random()`) get a fresh column on every occurrence.
fn push_into_source_subquery(
    expr: &mut Expr,
    aggregates: &mut Vec<Aggregate>,
    ctx: &mut WindowSubqueryContext,
) -> crate::Result<()> {
    let contains_aggregates =
        resolve_window_and_aggregate_functions(expr, ctx.resolver, aggregates, None, &mut [])?;
    if expr_contains_nondeterministic_scalar_function(expr, ctx.resolver)? {
        push_new_subquery_column(expr, ctx, contains_aggregates);
    } else {
        rewrite_expr_as_subquery_column(expr, ctx, contains_aggregates);
    }
    Ok(())
}

/// Rewrite a window function call `expr` so its arguments and FILTER predicate
/// reference output columns of the source subquery (the one being built in
/// `ctx`). Returns the rewritten form, ready to be stored on the matching
/// `WindowFunction`.
fn rewrite_expr_referencing_current_window(
    aggregates: &mut Vec<Aggregate>,
    window_name: String,
    ctx: &mut WindowSubqueryContext,
    expr: &mut Expr,
    func: &AccumulatorFunc,
) -> crate::Result<RewrittenWindowCall> {
    let filter_over = match expr {
        Expr::FunctionCall {
            args,
            order_by,
            within_group: _,
            filter_over,
            ..
        } => {
            let evaluate_args_after_buffer = window_function_uses_subtypes(func);
            for arg in args.iter_mut() {
                if evaluate_args_after_buffer {
                    rewrite_late_window_arg(arg, aggregates, ctx)?;
                } else {
                    push_into_source_subquery(arg, aggregates, ctx)?;
                }
            }
            turso_assert!(
                order_by.is_empty(),
                "ORDER BY in window functions is not supported"
            );
            filter_over
        }
        Expr::FunctionCallStar { filter_over, .. } => filter_over,
        _ => unreachable!("only functions can reference windows"),
    };

    if let Some(filter_expr) = filter_over.filter_clause.as_deref_mut() {
        push_into_source_subquery(filter_expr, aggregates, ctx)?;
    }
    let filter_expr = filter_over.filter_clause.as_deref().cloned();
    filter_over.over_clause = Some(Over::Name(Name::exact(window_name)));
    Ok(RewrittenWindowCall {
        expr: expr.clone(),
        filter_expr,
    })
}

/// JSON aggregates inspect their arguments' runtime subtypes. SQLite records
/// do not carry subtypes, so every argument expression must run after its row
/// is read back from the window buffer.
fn window_function_uses_subtypes(window_func: &AccumulatorFunc) -> bool {
    #[cfg(feature = "json")]
    {
        matches!(
            window_func,
            AccumulatorFunc::Agg(
                AggFunc::JsonGroupArray
                    | AggFunc::JsonbGroupArray
                    | AggFunc::JsonGroupObject
                    | AggFunc::JsonbGroupObject
            )
        )
    }
    #[cfg(not(feature = "json"))]
    {
        let _ = window_func;
        false
    }
}

/// Keep a window argument in the window layer while moving the values it reads
/// into the source subquery. This mirrors SQLite's subtype-aware aggregate
/// path, including evaluating scalar calls separately for step and inverse.
fn rewrite_late_window_arg(
    arg: &mut Expr,
    aggregates: &mut [Aggregate],
    ctx: &mut WindowSubqueryContext,
) -> Result<()> {
    walk_expr_mut(arg, &mut |node| {
        if matches!(
            node,
            Expr::FunctionCall { .. } | Expr::FunctionCallStar { .. }
        ) && aggregates
            .iter()
            .any(|aggregate| exprs_are_equivalent(&aggregate.original_expr, node))
        {
            rewrite_expr_as_subquery_column(node, ctx, true);
            return Ok(WalkControl::SkipChildren);
        }
        match node {
            Expr::RowId { .. } | Expr::Column { .. } => {
                rewrite_expr_as_subquery_column(node, ctx, false);
                return Ok(WalkControl::SkipChildren);
            }
            Expr::SubqueryResult { .. }
            | Expr::Exists(..)
            | Expr::InSelect { .. }
            | Expr::Subquery(..) => {
                rewrite_expr_as_subquery_column(node, ctx, false);
                return Ok(WalkControl::SkipChildren);
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// Rewrites an expression into a reference to a subquery column. If an
/// equivalent expression was already pushed down, reuses its column index.
fn rewrite_expr_as_subquery_column(
    expr: &mut Expr,
    ctx: &mut WindowSubqueryContext,
    contains_aggregates: bool,
) {
    if let Some(pos) = ctx
        .subquery_result_columns
        .iter()
        .position(|col| exprs_are_equivalent(&col.expr, expr))
    {
        *expr = Expr::Column {
            database: Some(SUBQUERY_DATABASE_ID),
            table: *ctx.subquery_id,
            column: pos,
            is_rowid_alias: false,
        };
    } else {
        push_new_subquery_column(expr, ctx, contains_aggregates);
    }
}

/// Pushes `expr` as a fresh subquery column even if an equivalent column
/// already exists. Use this for expressions containing nondeterministic calls
/// like `random()`, which SQLite evaluates separately at each SQL occurrence.
fn push_new_subquery_column(
    expr: &mut Expr,
    ctx: &mut WindowSubqueryContext,
    contains_aggregates: bool,
) {
    let column_idx = ctx.subquery_result_columns.len();
    let subquery_ref = Expr::Column {
        database: Some(SUBQUERY_DATABASE_ID),
        table: *ctx.subquery_id,
        column: column_idx,
        is_rowid_alias: false,
    };
    let subquery_expr = mem::replace(expr, subquery_ref);
    ctx.subquery_result_columns.push(ResultSetColumn {
        expr: subquery_expr,
        alias: None,
        implicit_column_name: None,
        contains_aggregates,
    });
}

#[derive(Debug)]
pub struct WindowMetadata<'a> {
    pub labels: WindowLabels,
    pub registers: WindowRegisters,
    pub cursors: WindowCursors,
    /// Number of input columns in the source subquery.
    pub src_column_count: usize,
    /// Maps expressions in the current query that reference subquery columns
    /// to their corresponding column indexes in the subquery’s result.
    pub expressions_referencing_subquery: Vec<(&'a Expr, usize)>,
    pub buffer_table_name: String,
    /// For each window function, a sorted index used to compute `min()` or
    /// `max()` when the frame's start can move (so rows leave the frame as
    /// it slides). Most aggregates can cheaply undo one row's contribution
    /// when it leaves, but min/max can't: if you drop the current maximum,
    /// you have no way to know the next-largest value. So instead of a
    /// single running value we keep every in-frame value in a sorted index.
    /// A row joining the frame inserts its value, a row leaving deletes it,
    /// and the current answer is always the largest (or smallest) value
    /// still in the index. Each entry is a `(value, sequence)` pair; the
    /// ever-increasing sequence number keeps equal values apart, so a
    /// leaving row deletes exactly its own entry. `None` for any function
    /// that isn't a min/max needing this index.
    pub minmax: Vec<Option<WindowMinMax>>,
}

#[derive(Debug, Clone, Copy)]
pub struct WindowMinMax {
    pub cursor: CursorID,
    /// The first of three registers in a row, used to build one index
    /// entry: `[0]` the function argument's value for this row, `[1]` the
    /// sequence counter (bumped once per row so equal values stay
    /// distinct), `[2]` those two packed into the record that is inserted
    /// into the index.
    pub registers: usize,
}

/// Create the sorted index that a `min()` / `max()` window function uses
/// when the frame's start can move (see `WindowMetadata::minmax`). The
/// index sorts on two columns: the argument value, then an
/// always-increasing sequence number so equal values still get separate
/// entries. For `max()` the value sorts ascending and for `min()` it sorts
/// descending, so in both cases the current answer is just the last entry
/// in the index — one code path serves both. The index is ephemeral: it
/// exists only for the duration of this query.
fn allocate_window_minmax(
    program: &mut ProgramBuilder,
    window: &Window,
    table_references: &TableReferences,
) -> Result<Vec<Option<WindowMinMax>>> {
    let moving_start = !matches!(
        window.frame.start,
        crate::translate::plan::FrameBoundary::UnboundedPreceding
    );
    let mut states = Vec::with_capacity(window.functions.len());

    for (i, func) in window.functions.iter().enumerate() {
        let agg = match &func.func {
            AccumulatorFunc::Agg(agg @ (AggFunc::Min | AggFunc::Max))
                if moving_start && window.frame.exclude.is_none() =>
            {
                agg
            }
            _ => {
                states.push(None);
                continue;
            }
        };
        let Expr::FunctionCall { args, .. } = func.current_expr() else {
            unreachable!("min/max window calls must have one argument");
        };
        let arg = args
            .first()
            .expect("min/max window calls must have one argument");
        let collation = get_collseq_from_expr(arg, table_references)?;
        let index = Arc::new(Index {
            name: format!("window_minmax_{}_{}", program.offset().as_offset_int(), i),
            table_name: String::new(),
            root_page: 0,
            columns: crate::alloc::vec![
                IndexColumn {
                    name: "0".to_string(),
                    order: if matches!(agg, AggFunc::Min) {
                        SortOrder::Desc
                    } else {
                        SortOrder::Asc
                    },
                    nulls_order: None,
                    pos_in_table: 0,
                    collation,
                    default: None,
                    expr: None,
                },
                IndexColumn::new("1", 1),
            ],
            unique: false,
            ephemeral: true,
            has_rowid: false,
            where_clause: None,
            index_method: None,
            on_conflict: None,
        });
        let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index));
        let registers = program.alloc_registers(3);
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: cursor,
            is_table: false,
        });
        program.emit_insn(Insn::Integer {
            value: 0,
            dest: registers + 1,
        });
        states.push(Some(WindowMinMax { cursor, registers }));
    }
    Ok(states)
}

#[derive(Debug, Clone, Copy)]
pub struct WindowLabels {
    /// Address of the flush subroutine — the code that, at the end of each
    /// partition, finishes off the rows still sitting in the buffer.
    pub flush_buffer: BranchOffset,
    /// Address of a small shared subroutine that hands one finished row to
    /// the rest of the query (into the SELECT output, or into the ORDER BY
    /// sorter). Every place that emits a row (RETURN_ROW) calls this same
    /// subroutine, so the per-row output logic — for example the duplicate
    /// check for SELECT DISTINCT — is written once here instead of being
    /// repeated at every emit site. Mirrors SQLite's `addrGosub`
    /// (window.c:1988).
    pub row_output: BranchOffset,
    /// Address of the instruction just past all window processing; control
    /// jumps here once the input rows have run out.
    pub window_processing_end: BranchOffset,
}

/// One of the three cursors that walk the buffered rows of a partition.
/// Each main-loop operation drives exactly one of them: AGGSTEP → `End`,
/// RETURN_ROW → `Current`, AGGINVERSE → `Start`. Used to index per-cursor
/// state by role so a cursor and its bookkeeping can't be mixed up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameCursor {
    Start = 0,
    Current = 1,
    End = 2,
}

/// The REPLAY side of peer tracking: for each frame cursor, the start
/// register of a block holding the ORDER BY values of the peer group (a
/// run of rows with equal ORDER BY values) that cursor is currently
/// sitting on. As a cursor walks forward through the
/// buffered rows, `emit_window_op` compares the next row against its block
/// to tell when the cursor has crossed into a new group. Three separate
/// blocks because under RANGE/GROUPS the cursors can each be on a
/// different group at the same time (needed for percent_rank /
/// cume_dist).
///
/// An entry is `None` when that cursor isn't peer-tracked: always under
/// ROWS (rows are never compared for peer-equality), and `Start` also
/// when the frame start can't move (there is no `csr_start`). Indexed by
/// [`FrameCursor`]. Mirrors SQLite's `s.{start,current,end}.reg`
/// (window.c:2897-2899). See `source_peer_values` for the INPUT-side
/// counterpart.
#[derive(Debug, Clone, Copy, Default)]
pub struct CursorPeerValues([Option<usize>; 3]);

impl core::ops::Index<FrameCursor> for CursorPeerValues {
    type Output = Option<usize>;
    fn index(&self, cursor: FrameCursor) -> &Self::Output {
        &self.0[cursor as usize]
    }
}

impl CursorPeerValues {
    /// The register of each cursor that is peer-tracked, skipping the ones
    /// that aren't (`None`).
    fn allocated(&self) -> impl Iterator<Item = usize> {
        self.0.into_iter().flatten()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WindowRegisters {
    /// The rowid of the last row we inserted into the buffer table. It is
    /// set to NULL at the start of each partition, so a NULL value here
    /// means we haven't yet inserted this partition's first row — which is
    /// how the main loop tells "first row of a new partition" apart from
    /// every later row.
    pub rowid: usize,
    /// Start of a block of registers holding the PARTITION BY column values
    /// of the current partition. Comparing the next row's PARTITION BY
    /// values against these is how we notice that a new partition has
    /// begun.
    pub partition_start: Option<usize>,
    /// Start of a block of registers, one per window function, holding that
    /// function's running total. AGGSTEP folds a row into these as it joins
    /// the frame; AGGINVERSE takes one back out as it leaves.
    pub acc_start: usize,
    /// Start of a block of registers, one per window function, holding the
    /// value to output for the current row. For aggregates this is worked
    /// out from the running total (AggValue). Ranking functions like
    /// row_number() have no running total and simply keep their value here.
    pub acc_result_start: usize,
    /// The return address the flush subroutine jumps back to once it has
    /// finished emptying the buffer.
    pub flush_buffer_return_offset: usize,
    /// Start of consecutive registers holding the current row's column
    /// values, as read from the source subquery.
    pub src_columns_start: usize,
    /// Start of a block of registers holding the column values that have to
    /// be carried through from the subquery out to the parent query.
    pub result_columns_start: usize,
    /// Start of the register array holding ORDER BY column values for the current row.
    /// These registers are used to detect whether the current row is a "peer"
    /// (i.e., has identical ORDER BY values to the previous row).
    pub new_order_by_columns_start: Option<usize>,
    /// The INPUT side of peer tracking: the ORDER BY values of the peer
    /// group that the source rows coming in are currently in. (A peer group
    /// is a run of rows with equal ORDER BY values.) The main loop
    /// (`emit_window_step`) compares each new source row's values
    /// (`new_order_by_columns_start`) against this, once per row:
    /// - Equal: the row is in the same group. Under RANGE/GROUPS a whole
    ///   group is handled at once, so the row is only buffered for now —
    ///   nothing is added to the totals and no result is emitted yet.
    /// - Different: a new group has started, so this is overwritten with
    ///   the new row's values.
    ///
    /// Just one register block, because the input is read in one pass.
    /// Contrast `cursor_peer_values` below, the REPLAY side. Mirrors
    /// SQLite's `regPeer` (window.c:2896, updated at window.c:2984-2986).
    pub source_peer_values: Option<usize>,
    /// The REPLAY side of peer tracking, one entry per frame cursor. See
    /// [`CursorPeerValues`].
    pub cursor_peer_values: CursorPeerValues,
    /// The offset N from a frame whose start is `N PRECEDING` or
    /// `N FOLLOWING`, worked out once per partition (N can be an
    /// expression, not just a literal number).
    ///
    /// Like `end_offset_reg`, this is a countdown: a start boundary "N rows
    /// away" means one piece of work has to wait until we have moved N rows
    /// along. `OP_IfPos` skips that work and ticks the counter down until it
    /// reaches zero.
    /// - `ROWS BETWEEN N PRECEDING AND CURRENT ROW` uses it to delay
    ///   dropping rows off the start of the frame (AGGINVERSE), so the frame
    ///   first grows to N+1 rows before it starts sliding forward.
    /// - cume_dist's implicit `1 FOLLOWING` start uses it to delay emitting
    ///   results (RETURN_ROW).
    ///
    /// Mirrors SQLite's `regStart` (window.c:2883).
    pub start_offset_reg: Option<usize>,
    /// The offset N from a frame whose end is `N PRECEDING` or
    /// `N FOLLOWING`, worked out once at run time (N can be an expression,
    /// not just a literal number).
    ///
    /// We process one row at a time, but an end boundary "N rows away"
    /// means some work for the current row can't be done yet — we have to
    /// wait until we have moved N rows further along. This register counts
    /// those N rows down. Which piece of work waits depends on where the
    /// frame ends:
    /// - End is `N FOLLOWING` (the frame reaches N rows past the current
    ///   row): emitting the current row's result, and dropping rows that
    ///   have fallen off the start of the frame, both wait N rows.
    /// - End is `N PRECEDING` (the frame stops N rows before the current
    ///   row): adding a row to the totals waits N rows.
    ///
    /// `None` for any other end (CURRENT ROW, UNBOUNDED FOLLOWING), which
    /// need no waiting. Mirrors SQLite's `regEnd` (window.c:2885-2887).
    pub end_offset_reg: Option<usize>,
    /// The first and last buffer rowid of the frame as it stands right now.
    /// Only used when the window has an EXCLUDE clause. EXCLUDE means we
    /// cannot keep a running total as rows come and go, because which rows
    /// count keeps changing; instead we re-add up the whole frame for every
    /// output row, and to do that we need to know exactly which rows the
    /// frame currently spans. AGGSTEP moves the last-rowid forward as rows
    /// join the frame, AGGINVERSE moves the first-rowid forward as rows
    /// drop off it. They start at first=1, last=0: with the first rowid
    /// past the last, the range covers no rows, so the frame starts empty.
    pub frame_start_rowid: Option<usize>,
    pub frame_end_rowid: Option<usize>,
    /// The register holding the return address for calls to the
    /// `row_output` subroutine (see `WindowLabels::row_output`): the
    /// subroutine jumps back to whatever address is stored here when it
    /// finishes. Mirrors SQLite's `regGosub` (window.c:2793).
    pub row_output_return: usize,
    /// Two counters tracking where the frame's edges are, so `first_value`
    /// / `nth_value` can jump straight to a particular row of the frame by
    /// its position:
    ///
    /// * `+0` — rows that have left the frame (one per AGGINVERSE), i.e.
    ///   the frame start's buffer index minus one.
    /// * `+1` — rows that have entered the frame (one per AGGSTEP), i.e.
    ///   the frame end's buffer index.
    ///
    /// Buffer rowids run 1, 2, 3, ... within each partition, so these
    /// indexes are also `SeekRowid` targets. One pair per window (not per
    /// function like SQLite's `pWin->regApp`) since every function in a
    /// window shares the same frame. `None` without first_value /
    /// nth_value. Mirrors SQLite's `regApp` pair (window.c:1457-1459,
    /// 1726, 2010-2013).
    pub frame_counters: Option<usize>,
}

/// Cursors over the "buffer": a temporary B-tree table, created just for
/// this query, that holds the rows of the current partition so we can read
/// them more than once. The four cursor roles mirror SQLite's
/// `sqlite3WindowCodeStep` allocation (`window.c:2834-2837`).
#[derive(Debug, Clone, Copy)]
pub struct WindowCursors {
    /// The cursor used to append each incoming source row to the buffer.
    /// We never seek it; it simply advances as we `NewRowid` + `Insert` the
    /// next row.
    pub csr_write: CursorID,
    /// The row currently being sent to the outer query. Each function's
    /// result for this row is computed just before the cursor moves on,
    /// and the row's output columns are read from here.
    pub csr_current: CursorID,
    /// Points at the next row about to join the frame. `AggStep` reads
    /// that row, adds it to the running totals, then moves the cursor on.
    /// Under GROUPS/RANGE frames one advance steps over every row with
    /// equal ORDER BY values (a "peer group") at once.
    pub csr_end: CursorID,
    /// Points at the first row still inside the frame — the one
    /// `AggInverse` drops next as the frame's start moves forward. Only
    /// allocated when the frame start can move off the first row of the
    /// partition (ntile, percent_rank, cume_dist); an UNBOUNDED PRECEDING
    /// start never moves, so there is nothing to track (`None`). Under
    /// GROUPS/RANGE frames one advance steps over a whole peer group (rows
    /// with equal ORDER BY values). Mirrors SQLite's `s.start.csr`
    /// (`window.c:2836`).
    pub csr_start: Option<CursorID>,
    /// A separate cursor for functions that read one specific row rather
    /// than a running total — first_value, nth_value, lag, lead. At output
    /// time it jumps straight to the wanted row by its rowid (`SeekRowid`).
    /// It is a duplicate of `csr_current` (`OpenDup`) and only ever moves
    /// on those jumps, so it never disturbs the three frame cursors.
    /// Allocated only when some function needs it (`None` otherwise).
    /// Mirrors SQLite's `pWin->csrApp` (`window.c`).
    pub csr_app: Option<CursorID>,
}

/// Builds `KeyInfo` entries for the window's ORDER BY columns, populating
/// each entry's collation from the expression. Used everywhere two rows
/// are compared to see whether they are peers (have equal ORDER BY
/// values): once on each incoming source row, and once per cursor while
/// stepping over a group in `emit_window_op`. `sort_order` and
/// `nulls_order` are left at their `Insn::Compare` defaults — this check
/// only asks whether the values are equal, not which one sorts first.
fn build_order_by_key_info(
    window: &Window,
    table_references: &crate::translate::plan::TableReferences,
) -> crate::Result<Vec<KeyInfo>> {
    window
        .order_by
        .iter()
        .map(|(expr, _, _)| {
            let collation = get_collseq_from_expr(expr, table_references)?.unwrap_or_default();
            Ok(KeyInfo {
                sort_order: SortOrder::Asc,
                collation,
                nulls_order: None,
            })
        })
        .collect()
}

/// Which side of the frame an offset belongs to. Selects the error
/// message wording ("starting" vs "ending").
#[derive(Clone, Copy)]
enum FrameBoundPosition {
    Start,
    End,
}

/// Evaluate a frame-offset expression into `reg`, then emit the runtime
/// non-negative check. A non-constant offset (a per-row column
/// reference, say) is loaded as NULL rather than evaluated, mirroring
/// SQLite's substitution at `window.c:1166-1171`: the check then rejects
/// it, but only once a partition is actually processed — a statement
/// over an empty table never evaluates the offset and never errors.
fn emit_frame_offset(
    program: &mut ProgramBuilder,
    plan: &SelectPlan,
    resolver: &Resolver,
    expr: &Expr,
    reg: usize,
    mode: turso_parser::ast::FrameMode,
    pos: FrameBoundPosition,
) -> Result<()> {
    if is_constant_frame_offset(expr, resolver)? {
        translate_expr_no_constant_opt(
            program,
            Some(&plan.table_references),
            expr,
            reg,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
    } else {
        program.emit_insn(Insn::Null {
            dest: reg,
            dest_end: None,
        });
    }
    emit_window_check_offset(program, reg, mode, pos);
    Ok(())
}

/// SQLite uses a stricter constant-expression rule for frame offsets than it
/// uses for normal constant folding. In particular, function calls and the
/// current-time keywords are rejected even when they are deterministic or sit
/// in a CASE branch that cannot run.
fn is_constant_frame_offset(expr: &Expr, resolver: &Resolver<'_>) -> Result<bool> {
    use crate::translate::optimizer::Optimizable;

    if !expr.is_constant(resolver) {
        return Ok(false);
    }

    let mut allowed = true;
    walk_expr(expr, &mut |node| {
        if matches!(
            node,
            Expr::FunctionCall { .. }
                | Expr::FunctionCallStar { .. }
                | Expr::Literal(
                    Literal::CurrentDate | Literal::CurrentTime | Literal::CurrentTimestamp
                )
        ) {
            allowed = false;
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(allowed)
}

/// Emit a runtime check that the frame-offset register holds a
/// non-negative integer, halting the program with the standard SQLite
/// error message on failure. Mirrors `windowCheckValue` at
/// `window.c:1482-1523`.
fn emit_window_check_offset(
    program: &mut ProgramBuilder,
    offset_reg: usize,
    mode: turso_parser::ast::FrameMode,
    pos: FrameBoundPosition,
) {
    use turso_parser::ast::FrameMode;
    let label_halt = program.allocate_label();
    let label_ok = program.allocate_label();
    let reg_zero = program.alloc_register();
    if mode == FrameMode::Range {
        // A RANGE offset has to be a number. This checks that indirectly:
        // comparing with NUMERIC affinity turns a number (or numeric-looking
        // text) into a number, and in SQLite's type ordering numbers sort
        // below every string, including the empty string. So a valid
        // numeric offset is `< ""` and falls through, while real text or a
        // blob is `>= ""` and jumps to the error; jump_if_null sends NULL
        // there too.
        let reg_empty = program.alloc_register();
        program.emit_insn(Insn::String8 {
            value: String::new(),
            dest: reg_empty,
        });
        program.emit_insn(Insn::Ge {
            lhs: offset_reg,
            rhs: reg_empty,
            target_pc: label_halt,
            flags: crate::vdbe::insn::CmpInsFlags::default()
                .jump_if_null()
                .with_affinity(crate::vdbe::affinity::Affinity::Numeric),
            collation: None,
        });
    } else {
        // A ROWS / GROUPS offset has to be an integer. MustBeInt converts
        // it, or jumps to our own Halt if it can't — so the user gets
        // SQLite's specific frame-offset error rather than a generic
        // "datatype mismatch".
        program.emit_insn(Insn::MustBeInt {
            reg: offset_reg,
            target_pc: Some(label_halt),
        });
    }
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: reg_zero,
    });
    program.emit_insn(Insn::Ge {
        lhs: offset_reg,
        rhs: reg_zero,
        target_pc: label_ok,
        flags: crate::vdbe::insn::CmpInsFlags::default()
            .with_affinity(crate::vdbe::affinity::Affinity::Numeric),
        collation: None,
    });
    program.preassign_label_to_next_insn(label_halt);
    let msg = match (mode, pos) {
        (FrameMode::Range, FrameBoundPosition::Start) => {
            "frame starting offset must be a non-negative number"
        }
        (FrameMode::Range, FrameBoundPosition::End) => {
            "frame ending offset must be a non-negative number"
        }
        (_, FrameBoundPosition::Start) => "frame starting offset must be a non-negative integer",
        (_, FrameBoundPosition::End) => "frame ending offset must be a non-negative integer",
    };
    program.emit_insn(Insn::Halt {
        err_code: crate::error::SQLITE_ERROR,
        description: msg.to_string(),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(label_ok);
}

pub struct EmitWindow;
impl EmitWindow {
    pub fn init<'a>(
        program: &mut ProgramBuilder,
        t_ctx: &mut TranslateCtx<'a>,
        window: &'a Window,
        plan: &SelectPlan,
        result_columns: &'a [ResultSetColumn],
        order_by: &'a [(Box<Expr>, SortOrder, Option<turso_parser::ast::NullsOrder>)],
    ) -> crate::Result<()> {
        let joined_tables = &plan.joined_tables();
        turso_assert_eq!(joined_tables.len(), 1, "expected only one joined table");

        let src_table = &joined_tables[0];
        let reg_src_columns_start =
            if let Table::FromClauseSubquery(from_clause_subquery) = &src_table.table {
                from_clause_subquery
                    .result_columns_start_reg
                    .expect("Subquery result_columns_start_reg must be set")
            } else {
                panic!(
                    "expected source table to be a FromClauseSubquery, but got: {:?}",
                    src_table.table
                );
            };
        let src_columns = src_table.columns().try_to_vec()?;
        let src_column_count = src_columns.len();
        let window_name = window.name.clone().expect("window name is missing");
        let partition_by_len = window
            .deduplicated_partition_by_len
            .unwrap_or(window.partition_by.len());
        let order_by_len = window.order_by.len();
        let window_function_count = window.functions.len();

        // An ephemeral table used to buffer rows for the current frame
        let buffer_table = Arc::new(BTreeTable::new(
            0,
            // TODO: Generating the name this way may cause collisions with real tables in the
            //  attached database. Other ephemeral tables are created similarly, so it's left
            //  as-is for now. Ideally, there should be a way to mark tables as ephemeral so
            //  they can be handled differently from regular tables.
            format!("buffer_table_{window_name}"),
            crate::alloc::vec![],
            src_columns,
            BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        ));
        // `csr_current` is the primary cursor on the ephemeral buffer;
        // the others are OpenDup'd duplicates that share the same B-tree
        // with independent positions.
        let cursor_csr_current =
            program.alloc_cursor_id(CursorType::BTreeTable(buffer_table.clone()));
        let cursor_csr_write =
            program.alloc_cursor_id(CursorType::BTreeTable(buffer_table.clone()));
        let cursor_csr_end = program.alloc_cursor_id(CursorType::BTreeTable(buffer_table.clone()));
        // `csr_start` follows the start of the frame. Only needed when the
        // frame start can move off the first row of the partition (ntile,
        // percent_rank, cume_dist); AggInverse reads from this cursor as
        // rows drop off the start of the frame.
        let has_moving_start = !matches!(
            window.frame.start,
            crate::translate::plan::FrameBoundary::UnboundedPreceding
        );
        let cursor_csr_start = if has_moving_start {
            Some(program.alloc_cursor_id(CursorType::BTreeTable(buffer_table.clone())))
        } else {
            None
        };
        // `csr_app` either seeks positional values or scans the inclusive
        // frame bounds for an explicit EXCLUDE clause.
        let needs_csr_app = window.frame.exclude.is_some()
            || window.functions.iter().any(|f| {
                matches!(
                    &f.func,
                    AccumulatorFunc::Window(
                        WindowFunc::FirstValue
                            | WindowFunc::NthValue
                            | WindowFunc::Lag
                            | WindowFunc::Lead
                    ),
                )
            });
        let cursor_csr_app = if needs_csr_app {
            Some(program.alloc_cursor_id(CursorType::BTreeTable(buffer_table.clone())))
        } else {
            None
        };
        // Frame-index counters for the positional lookups; see
        // `WindowRegisters::frame_counters`. Only first_value / nth_value
        // consult them — lag / lead seek relative to the emitted row's own
        // rowid, not the frame bounds. Zeroed per partition alongside the
        // accumulator reset, so no init is needed here.
        let frame_counters = (window.frame.exclude.is_none()
            && window.functions.iter().any(|f| {
                matches!(
                    &f.func,
                    AccumulatorFunc::Window(WindowFunc::FirstValue | WindowFunc::NthValue),
                )
            }))
        .then(|| program.alloc_registers(2));
        let (frame_start_rowid, frame_end_rowid) = if window.frame.exclude.is_some() {
            let start = program.alloc_register();
            let end = program.alloc_register();
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: start,
            });
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: end,
            });
            (Some(start), Some(end))
        } else {
            (None, None)
        };
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: cursor_csr_current,
            is_table: true,
        });
        program.emit_insn(Insn::OpenDup {
            original_cursor_id: cursor_csr_current,
            new_cursor_id: cursor_csr_write,
        });
        program.emit_insn(Insn::OpenDup {
            original_cursor_id: cursor_csr_current,
            new_cursor_id: cursor_csr_end,
        });
        if let Some(csr_start) = cursor_csr_start {
            program.emit_insn(Insn::OpenDup {
                original_cursor_id: cursor_csr_current,
                new_cursor_id: csr_start,
            });
        }
        if let Some(csr_app) = cursor_csr_app {
            program.emit_insn(Insn::OpenDup {
                original_cursor_id: cursor_csr_current,
                new_cursor_id: csr_app,
            });
        }
        let minmax = allocate_window_minmax(program, window, &plan.table_references)?;

        // Window function processing is similar to aggregation processing in how results are mapped
        // to registers. Each function expression is stored in `expr_to_reg_cache` along with its
        // result register. Later, when bytecode generation encounters the expression, the value is
        // copied from the result register instead of generating code to evaluate the expression.
        let reg_acc_start = program.alloc_registers(window_function_count);
        let reg_acc_result_start = program.alloc_registers(window_function_count);
        for (i, func) in window.functions.iter().enumerate() {
            // Cache by the rewritten form (when available) so lookups against the
            // result-column / ORDER-BY expressions — which were rewritten to
            // reference this window's subquery — find the cached register.
            t_ctx.resolver.cache_expr_reg(
                std::borrow::Cow::Borrowed(func.current_expr()),
                reg_acc_result_start + i,
                false,
                None,
            );
        }

        // The same approach applies to expressions referencing the subquery (columns).
        // Instead of reading directly from the subquery, we redirect them to the corresponding
        // result registers. This is necessary because rows are buffered in an ephemeral table and
        // returned according to the rules of the window definition.
        let expressions_referencing_subquery = collect_expressions_referencing_subquery(
            result_columns,
            order_by,
            &src_table.internal_id,
        )?;
        let reg_col_start = program.alloc_registers(expressions_referencing_subquery.len());
        for (i, (expr, _)) in expressions_referencing_subquery.iter().enumerate() {
            t_ctx.resolver.cache_scalar_expr_reg(
                std::borrow::Cow::Borrowed(expr),
                reg_col_start + i,
                false,
                &plan.table_references,
            )?;
        }

        t_ctx.meta_window = Some(WindowMetadata {
            labels: WindowLabels {
                flush_buffer: program.allocate_label(),
                row_output: program.allocate_label(),
                window_processing_end: program.allocate_label(),
            },
            registers: WindowRegisters {
                rowid: program.alloc_registers_and_init_w_null(1),
                partition_start: if partition_by_len > 0 {
                    Some(program.alloc_registers_and_init_w_null(partition_by_len))
                } else {
                    None
                },
                acc_start: reg_acc_start,
                acc_result_start: reg_acc_result_start,
                flush_buffer_return_offset: program.alloc_register(),
                src_columns_start: reg_src_columns_start,
                result_columns_start: reg_col_start,
                new_order_by_columns_start: alloc_optional_registers(program, order_by_len),
                // Peer tracking only runs under RANGE / GROUPS — under
                // ROWS a row is never compared against its neighbours to
                // see if they are peers, so all of these stay `None`.
                // Mirrors SQLite's `regPeer` allocation gate at
                // window.c:2892-2896.
                source_peer_values: if window.frame.mode != turso_parser::ast::FrameMode::Rows {
                    alloc_optional_registers(program, order_by_len)
                } else {
                    None
                },
                cursor_peer_values: {
                    let is_peer_tracked = window.frame.mode != turso_parser::ast::FrameMode::Rows;
                    // Order matters: this allocates registers, so keep it
                    // Start / Current / End to match `FrameCursor`.
                    CursorPeerValues([
                        // Start: only when the frame start can move — that
                        // is the only case with a `csr_start` to track.
                        (is_peer_tracked && has_moving_start)
                            .then(|| alloc_optional_registers(program, order_by_len))
                            .flatten(),
                        is_peer_tracked
                            .then(|| alloc_optional_registers(program, order_by_len))
                            .flatten(),
                        is_peer_tracked
                            .then(|| alloc_optional_registers(program, order_by_len))
                            .flatten(),
                    ])
                },
                start_offset_reg: match window.frame.start {
                    crate::translate::plan::FrameBoundary::Preceding(_)
                    | crate::translate::plan::FrameBoundary::Following(_) => {
                        Some(program.alloc_register())
                    }
                    _ => None,
                },
                end_offset_reg: match window.frame.end {
                    crate::translate::plan::FrameBoundary::Preceding(_)
                    | crate::translate::plan::FrameBoundary::Following(_) => {
                        Some(program.alloc_register())
                    }
                    _ => None,
                },
                frame_start_rowid,
                frame_end_rowid,
                row_output_return: program.alloc_register(),
                frame_counters,
            },
            cursors: WindowCursors {
                csr_write: cursor_csr_write,
                csr_current: cursor_csr_current,
                csr_end: cursor_csr_end,
                csr_start: cursor_csr_start,
                csr_app: cursor_csr_app,
            },
            src_column_count,
            expressions_referencing_subquery,
            buffer_table_name: buffer_table.name.clone(),
            minmax,
        });

        Ok(())
    }
    /// Emits the per-source-row body for window processing.
    ///
    /// This is the Rust port of the main loop in SQLite's
    /// `sqlite3WindowCodeStep` (`window.c:2786-3037`). The three
    /// operations that move the frame forward — adding a row to the
    /// running totals, emitting a row's result, and removing a row from
    /// the totals — are emitted in one of three orders, chosen by the
    /// frame's bounds: a `FOLLOWING` start, a `PRECEDING` end, or anything
    /// else. This matches SQLite's three cases at window.c:2987-3037.
    ///
    /// Pseudocode for the most common shape (`UNBOUNDED PRECEDING TO
    /// CURRENT ROW`):
    ///
    /// ```text
    ///   load ORDER BY columns into newPeer regs
    ///   if partition_by_cols changed:
    ///       Gosub flush_partition
    ///       Null rowid_reg                 ; marks "first row of new partition"
    ///       Copy partition_by_cols → prev_partition_regs
    ///
    ///   if rowid_reg is null:                       ; FIRST ROW OF PARTITION
    ///       Copy newPeer → prevPeer
    ///       Null accumulators
    ///       Insert row into ephemeral via csr_write
    ///       Rewind csr_current, csr_end, csr_start  ; position at row 1
    ///       Goto loop_end
    ///   else:                                       ; SUBSEQUENT ROW
    ///       Insert row into ephemeral via csr_write
    ///       if RANGE/GROUPS and newPeer == prevPeer:
    ///           Goto loop_end                       ; same group: just buffer, process later
    ///       emit_window_op AGGSTEP        ; advance csr_end through peer rows; AggStep per row
    ///       emit_window_op RETURN_ROW     ; advance csr_current; emit each row
    ///       (emit_window_op AGGINVERSE is a no-op for UNBOUNDED start)
    ///       Copy newPeer → prevPeer                 ; RANGE/GROUPS only
    ///   loop_end:
    /// ```
    ///
    /// Example — `row_number() OVER (ORDER BY salary DESC)` over 3 rows:
    /// the source coroutine yields each row, this body inserts it, advances
    /// `csr_end` (AggStep increments row_number's counter) and `csr_current`
    /// (RETURN_ROW emits the previous row with its accumulated counter).
    /// The very first row's emission is deferred to the flush subroutine
    /// because `csr_current` was just rewound and there's no preceding row
    /// to emit yet.
    pub fn emit_window_step(
        program: &mut ProgramBuilder,
        t_ctx: &mut TranslateCtx,
        plan: &SelectPlan,
    ) -> crate::Result<()> {
        let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
        let window = plan.window.as_ref().expect("missing window");

        let labels = meta.labels;
        let registers = meta.registers;
        let cursors = meta.cursors;
        let src_column_count = meta.src_column_count;
        let buffer_table_name = meta.buffer_table_name.clone();
        let minmax = meta.minmax.clone();

        emit_load_order_by_columns(program, window, &registers);
        emit_flush_buffer_if_new_partition(program, &labels, &registers, window, plan)?;

        // `rowid_reg` was NULL'd at partition entry; it stays NULL until the
        // first Insert of this partition. That tells the two branches apart:
        // the first row of a partition needs one-time setup, while every
        // later row just adds itself to the totals (AGGSTEP) and emits a
        // result (RETURN_ROW).
        let label_subsequent = program.allocate_label();
        let label_step_end = program.allocate_label();
        program.emit_insn(Insn::NotNull {
            reg: registers.rowid,
            target_pc: label_subsequent,
        });

        // --- FIRST ROW OF PARTITION ---
        if let (Some(new_ob), Some(source_peer_values)) = (
            registers.new_order_by_columns_start,
            registers.source_peer_values,
        ) {
            // Seed the source-row peer reference and every per-cursor
            // peer reference with the first row's ORDER BY values.
            // Mirrors SQLite's init at `window.c:2972-2976` (regNewPeer
            // → regPeer → s.start.reg / current.reg / end.reg).
            program.add_comment(
                program.offset(),
                "initialize peer-reference registers for new partition",
            );
            let n = window.order_by.len() - 1;
            program.emit_insn(Insn::Copy {
                src_reg: new_ob,
                dst_reg: source_peer_values,
                extra_amount: n,
            });
            for peer_reg in registers.cursor_peer_values.allocated() {
                program.emit_insn(Insn::Copy {
                    src_reg: source_peer_values,
                    dst_reg: peer_reg,
                    extra_amount: n,
                });
            }
        }
        program.add_comment(program.offset(), "reset accumulator registers");
        program.emit_insn(Insn::Null {
            dest: registers.acc_start,
            dest_end: Some(registers.acc_start + window.functions.len() - 1),
        });
        // Reset every min/max index and its insertion sequence at a partition
        // boundary. Mirrors windowInitAccum (window.c:2000-2009).
        for state in minmax.iter().flatten() {
            program.emit_insn(Insn::ResetSorter {
                cursor_id: state.cursor,
            });
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: state.registers + 1,
            });
        }
        // Zero the frame-index counters for the new partition. Mirrors
        // `windowInitAccum`'s regApp reset (window.c:2010-2013).
        if let Some(frame_counters) = registers.frame_counters {
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: frame_counters,
            });
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: frame_counters + 1,
            });
        }
        // The same register holds the offset for every partition, and the
        // IfPos delays decrement it as they run. Re-evaluate it here at the
        // start of each partition: setting it just once up front would
        // leave the decremented (wrong) value in place from the second
        // partition on. Mirrors SQLite's `regStart` init at `window.c:2942`.
        if let Some(start_offset_reg) = registers.start_offset_reg {
            let offset_expr = match &window.frame.start {
                crate::translate::plan::FrameBoundary::Preceding(expr)
                | crate::translate::plan::FrameBoundary::Following(expr) => expr,
                _ => unreachable!(
                    "start_offset_reg is only allocated when frame.start is Preceding/Following"
                ),
            };
            emit_frame_offset(
                program,
                plan,
                &t_ctx.resolver,
                offset_expr,
                start_offset_reg,
                window.frame.mode,
                FrameBoundPosition::Start,
            )?;
        }
        if let Some(end_offset_reg) = registers.end_offset_reg {
            let offset_expr = match &window.frame.end {
                crate::translate::plan::FrameBoundary::Preceding(expr)
                | crate::translate::plan::FrameBoundary::Following(expr) => expr,
                _ => unreachable!(
                    "end_offset_reg is only allocated when frame.end is Preceding/Following"
                ),
            };
            emit_frame_offset(
                program,
                plan,
                &t_ctx.resolver,
                offset_expr,
                end_offset_reg,
                window.frame.mode,
                FrameBoundPosition::End,
            )?;
        }
        emit_insert_row_into_buffer(
            program,
            &registers,
            &cursors,
            &src_column_count,
            &buffer_table_name,
        );
        // Empty-frame check (window.c:2950-2961): for non-RANGE
        // frames bounded on both sides by the same kind — `N PRECEDING
        // AND M PRECEDING` or `N FOLLOWING AND M FOLLOWING` — the frame
        // is empty for every row whenever the bounds cross (M > N for
        // PRECEDING pairs, M < N for FOLLOWING pairs). Each row then
        // emits the result an empty frame gives (an aggregate that never
        // saw a row — sum → NULL, count → 0; first_value / nth_value yield
        // NULL because the frame counters stay 0) and the buffer is
        // cleared. Re-Nulling
        // `rowid_reg` sends every subsequent row back through this
        // branch — SQLite gets that re-entry for free because its
        // first-row test is `rowid == 1` and ResetSorter restarts the
        // rowids.
        let same_kind_bounded = window.frame.mode != turso_parser::ast::FrameMode::Range
            && matches!(
                (&window.frame.start, &window.frame.end),
                (
                    crate::translate::plan::FrameBoundary::Preceding(_),
                    crate::translate::plan::FrameBoundary::Preceding(_)
                ) | (
                    crate::translate::plan::FrameBoundary::Following(_),
                    crate::translate::plan::FrameBoundary::Following(_)
                )
            );
        if same_kind_bounded {
            let start_offset_reg = registers
                .start_offset_reg
                .expect("same-kind bounded frames carry a start offset");
            let end_offset_reg = registers
                .end_offset_reg
                .expect("same-kind bounded frames carry an end offset");
            let label_frame_valid = program.allocate_label();
            program.add_comment(program.offset(), "empty-frame check");
            // FOLLOWING pair: valid iff end >= start. PRECEDING pair:
            // valid iff end <= start. Mirrors the Ge/Le pick at
            // window.c:2951.
            if matches!(
                window.frame.start,
                crate::translate::plan::FrameBoundary::Following(_)
            ) {
                program.emit_insn(Insn::Ge {
                    lhs: end_offset_reg,
                    rhs: start_offset_reg,
                    target_pc: label_frame_valid,
                    flags: crate::vdbe::insn::CmpInsFlags::default(),
                    collation: None,
                });
            } else {
                program.emit_insn(Insn::Le {
                    lhs: end_offset_reg,
                    rhs: start_offset_reg,
                    target_pc: label_frame_valid,
                    flags: crate::vdbe::insn::CmpInsFlags::default(),
                    collation: None,
                });
            }
            if window.frame.exclude.is_none() {
                emit_window_agg_final(program, window, &registers, &minmax, false);
            }
            // The row was just inserted, so the empty branch of this
            // Rewind is unreachable — the label lands on the next
            // instruction either way.
            let label_unreachable_empty = program.allocate_label();
            program.emit_insn(Insn::Rewind {
                cursor_id: cursors.csr_current,
                pc_if_empty: label_unreachable_empty,
            });
            program.preassign_label_to_next_insn(label_unreachable_empty);
            emit_return_one_row(program, t_ctx, plan)?;
            program.emit_insn(Insn::ResetSorter {
                cursor_id: cursors.csr_current,
            });
            program.emit_insn(Insn::Null {
                dest: registers.rowid,
                dest_end: None,
            });
            program.emit_insn(Insn::Goto {
                target_pc: label_step_end,
            });
            program.preassign_label_to_next_insn(label_frame_valid);
        }
        // `N FOLLOWING AND M FOLLOWING`: dropping rows off the start
        // (AGGINVERSE) must lag adding them (AGGSTEP) by M - N rows,
        // rather than lag emitting the result (RETURN_ROW) by M. So the
        // start count is set to the difference of the two offsets
        // (window.c:2962-2965).
        if matches!(
            window.frame.start,
            crate::translate::plan::FrameBoundary::Following(_)
        ) && window.frame.mode != turso_parser::ast::FrameMode::Range
        {
            if let (Some(start_offset_reg), Some(end_offset_reg)) =
                (registers.start_offset_reg, registers.end_offset_reg)
            {
                program.emit_insn(Insn::Subtract {
                    lhs: end_offset_reg,
                    rhs: start_offset_reg,
                    dest: start_offset_reg,
                });
            }
        }
        // Position each frame cursor at the just-inserted first row.
        // Mirrors `window.c:2967-2971` — `csr_start` is rewound only when
        // the frame start isn't UNBOUNDED PRECEDING (otherwise the
        // cursor wasn't allocated and AggInverse is a no-op).
        let label_unreachable_empty = program.allocate_label();
        if let Some(csr_start) = cursors.csr_start {
            program.emit_insn(Insn::Rewind {
                cursor_id: csr_start,
                pc_if_empty: label_unreachable_empty,
            });
        }
        program.emit_insn(Insn::Rewind {
            cursor_id: cursors.csr_current,
            pc_if_empty: label_unreachable_empty,
        });
        program.emit_insn(Insn::Rewind {
            cursor_id: cursors.csr_end,
            pc_if_empty: label_unreachable_empty,
        });
        program.preassign_label_to_next_insn(label_unreachable_empty);
        // The first row is not added to the totals here. The flush
        // subroutine adds it once at the end of the partition (AGGSTEP)
        // and emits it there (RETURN_ROW).
        program.emit_insn(Insn::Goto {
            target_pc: label_step_end,
        });

        // --- SUBSEQUENT ROW ---
        program.preassign_label_to_next_insn(label_subsequent);
        emit_insert_row_into_buffer(
            program,
            &registers,
            &cursors,
            &src_column_count,
            &buffer_table_name,
        );

        // Under RANGE / GROUPS, a row with the same ORDER BY values as the
        // previous one (its peer) is only buffered for now: adding rows to
        // the totals and emitting results both wait until the next group
        // begins, and then handle the whole buffered group in one go. With
        // no ORDER BY the whole partition is a single group, so every row
        // waits and the end-of-partition flush does the work. Mirrors
        // SQLite's `windowIfNewPeer` call at window.c:2984-2986 (an
        // unconditional jump when there's no ORDER BY, window.c:2076).
        if window.frame.mode != turso_parser::ast::FrameMode::Rows {
            program.add_comment(
                program.offset(),
                "peer of previous row: buffer only, handle at group end",
            );
            emit_if_new_peer(
                program,
                window,
                &plan.table_references,
                registers.new_order_by_columns_start,
                registers.source_peer_values,
                label_step_end,
            )?;
        }

        // Pick the order to run the three operations in, based on the
        // frame's bounds — one of SQLite's three `sqlite3WindowCodeStep`
        // cases (window.c:2987-3037).
        use crate::translate::plan::FrameBoundary;
        let is_range = window.frame.mode == turso_parser::ast::FrameMode::Range;
        let end_is_unbounded = matches!(window.frame.end, FrameBoundary::UnboundedFollowing);
        if matches!(window.frame.start, FrameBoundary::Following(_)) {
            // Pattern A — the frame starts after the current row (`<expr>
            // FOLLOWING` start), e.g. `ROWS BETWEEN 1 FOLLOWING AND 3
            // FOLLOWING` (window.c:2987-3002). Every row is added to the
            // totals as soon as we reach it (AGGSTEP, no delay). Producing
            // a row's result waits until we have read M more rows past it
            // (M = the end offset); dropping rows that have fallen off the
            // start waits a further N rows (N = the start offset).
            emit_window_op(program, t_ctx, plan, WindowOp::AggStep, None, None, false)?;
            if !end_is_unbounded {
                if is_range {
                    let label_done = program.allocate_label();
                    let label_loop = program.allocate_label();
                    program.preassign_label_to_next_insn(label_loop);
                    emit_window_range_test(
                        program,
                        plan,
                        RangeCmp::Ge,
                        cursors.csr_current,
                        registers
                            .end_offset_reg
                            .expect("bounded RANGE end has an offset register"),
                        cursors.csr_end,
                        label_done,
                    )?;
                    emit_window_op(
                        program,
                        t_ctx,
                        plan,
                        WindowOp::AggInverse,
                        registers.start_offset_reg,
                        None,
                        false,
                    )?;
                    emit_window_op(program, t_ctx, plan, WindowOp::ReturnRow, None, None, false)?;
                    program.emit_insn(Insn::Goto {
                        target_pc: label_loop,
                    });
                    program.preassign_label_to_next_insn(label_done);
                } else {
                    emit_window_op(
                        program,
                        t_ctx,
                        plan,
                        WindowOp::ReturnRow,
                        registers.end_offset_reg,
                        None,
                        false,
                    )?;
                    emit_window_op(
                        program,
                        t_ctx,
                        plan,
                        WindowOp::AggInverse,
                        registers.start_offset_reg,
                        None,
                        false,
                    )?;
                }
            }
        } else if matches!(window.frame.end, FrameBoundary::Preceding(_)) {
            // Pattern B — the frame ends before the current row (`<expr>
            // PRECEDING` end), e.g. `ROWS BETWEEN UNBOUNDED PRECEDING AND
            // 2 PRECEDING` (window.c:3004-3009). A row's result can be
            // produced right away (RETURN_ROW, no delay). Adding rows to
            // the totals waits M rows (M = the end offset), because the
            // last row of the frame sits M rows behind the current one.
            emit_window_op(
                program,
                t_ctx,
                plan,
                WindowOp::AggStep,
                registers.end_offset_reg,
                None,
                false,
            )?;
            // When a RANGE frame has both bounds PRECEDING, drop rows off
            // the start (AGGINVERSE) before producing the result
            // (RETURN_ROW). Both cursors already sit behind the current
            // row; this order stops the start cursor from moving past the
            // end cursor when the two offsets differ. SQLite's `bRPS` at
            // window.c:3005.
            let inverse_before_return =
                is_range && matches!(window.frame.start, FrameBoundary::Preceding(_));
            if inverse_before_return {
                emit_window_op(
                    program,
                    t_ctx,
                    plan,
                    WindowOp::AggInverse,
                    registers.start_offset_reg,
                    None,
                    false,
                )?;
            }
            emit_window_op(program, t_ctx, plan, WindowOp::ReturnRow, None, None, false)?;
            if !inverse_before_return {
                emit_window_op(
                    program,
                    t_ctx,
                    plan,
                    WindowOp::AggInverse,
                    registers.start_offset_reg,
                    None,
                    false,
                )?;
            }
        } else {
            // Pattern C — everything else (window.c:3010-3037).
            emit_window_op(program, t_ctx, plan, WindowOp::AggStep, None, None, false)?;
            if !end_is_unbounded {
                let range_loop = (is_range && registers.end_offset_reg.is_some()).then(|| {
                    let label = program.allocate_label();
                    program.preassign_label_to_next_insn(label);
                    label
                });
                let range_done = range_loop.map(|_| program.allocate_label());
                if let Some(label_done) = range_done {
                    emit_window_range_test(
                        program,
                        plan,
                        RangeCmp::Ge,
                        cursors.csr_current,
                        registers.end_offset_reg.expect("checked above"),
                        cursors.csr_end,
                        label_done,
                    )?;
                }
                // A `<expr> FOLLOWING` end delays both producing the
                // result (RETURN_ROW) and dropping rows off the start
                // (AGGINVERSE) by M rows. The per-op delays inside
                // `emit_window_op` hold back one op at a time; this one
                // skips both together. SQLite emits it inline at
                // window.c:3028-3034.
                let label_skip_pair = (!is_range)
                    .then_some(registers.end_offset_reg)
                    .flatten()
                    .map(|end_offset_reg| {
                        let label = program.allocate_label();
                        program.emit_insn(Insn::IfPos {
                            reg: end_offset_reg,
                            target_pc: label,
                            decrement_by: 1,
                        });
                        label
                    });
                emit_window_op(program, t_ctx, plan, WindowOp::ReturnRow, None, None, false)?;
                // AGGINVERSE does nothing for an UNBOUNDED PRECEDING start
                // — no row ever leaves the frame, so `emit_window_op`
                // early-returns (window.c:2252-2257). For an `N PRECEDING`
                // start it holds off until the frame has grown to N+1 rows,
                // then drops one row for each new row that joins
                // (window.c:3032-3033).
                emit_window_op(
                    program,
                    t_ctx,
                    plan,
                    WindowOp::AggInverse,
                    registers.start_offset_reg,
                    None,
                    false,
                )?;
                if let Some(range_loop) = range_loop {
                    program.emit_insn(Insn::Goto {
                        target_pc: range_loop,
                    });
                    program.preassign_label_to_next_insn(
                        range_done.expect("range loop and done labels are paired"),
                    );
                }
                if let Some(label) = label_skip_pair {
                    program.preassign_label_to_next_insn(label);
                }
            }
        }

        program.preassign_label_to_next_insn(label_step_end);

        Ok(())
    }
}

/// Compare the ORDER BY values at `reg_new` against the remembered ones at
/// `reg_old`: jump to `target_if_peer` when they're equal (the two rows
/// are peers), otherwise copy the new values into `reg_old` and fall
/// through. With no ORDER BY every row counts as a peer, so this becomes
/// an unconditional jump. Mirrors SQLite's `windowIfNewPeer`
/// (window.c:2057-2078).
fn emit_if_new_peer(
    program: &mut ProgramBuilder,
    window: &Window,
    table_references: &TableReferences,
    reg_new: Option<usize>,
    reg_old: Option<usize>,
    target_if_peer: BranchOffset,
) -> Result<()> {
    let order_by_len = window.order_by.len();
    if order_by_len == 0 {
        program.emit_insn(Insn::Goto {
            target_pc: target_if_peer,
        });
        return Ok(());
    }
    let reg_new = reg_new.expect("new_order_by_columns_start must exist when ORDER BY is present");
    let reg_old = reg_old.expect("peer reference register must exist when ORDER BY is present");
    let label_new_peer = program.allocate_label();
    // `Insn::Compare` requires `start_reg_a < start_reg_b`; the Jump
    // targets are symmetric on lt/gt so operand order doesn't change
    // the semantics.
    let (reg_a, reg_b) = (reg_new.min(reg_old), reg_new.max(reg_old));
    program.emit_insn(Insn::Compare {
        start_reg_a: reg_a,
        start_reg_b: reg_b,
        count: order_by_len,
        key_info: build_order_by_key_info(window, table_references)?,
    });
    program.emit_insn(Insn::Jump {
        target_pc_lt: label_new_peer,
        target_pc_eq: target_if_peer,
        target_pc_gt: label_new_peer,
    });
    program.preassign_label_to_next_insn(label_new_peer);
    program.emit_insn(Insn::Copy {
        src_reg: reg_new,
        dst_reg: reg_old,
        extra_amount: order_by_len - 1,
    });
    Ok(())
}

fn alloc_optional_registers(program: &mut ProgramBuilder, count: usize) -> Option<usize> {
    if count > 0 {
        Some(program.alloc_registers(count))
    } else {
        None
    }
}

fn collect_expressions_referencing_subquery<'a>(
    result_columns: &'a [ResultSetColumn],
    order_by: &'a [(Box<Expr>, SortOrder, Option<turso_parser::ast::NullsOrder>)],
    subquery_id: &TableInternalId,
) -> crate::Result<Vec<(&'a Expr, usize)>> {
    let mut expressions_referencing_subquery: Vec<(&'a Expr, usize)> = Vec::new();

    for root_expr in result_columns
        .iter()
        .map(|col| &col.expr)
        .chain(order_by.iter().map(|(e, _, _)| e.as_ref()))
    {
        walk_expr(
            root_expr,
            &mut |expr: &Expr| -> crate::Result<WalkControl> {
                match expr {
                    Expr::FunctionCall { filter_over, .. }
                    | Expr::FunctionCallStar { filter_over, .. } => {
                        if filter_over.over_clause.is_some() {
                            return Ok(WalkControl::SkipChildren);
                        }
                    }
                    Expr::Column { column, table, .. } => {
                        turso_assert_eq!(
                            table,
                            subquery_id,
                            "only subquery columns can be referenced"
                        );
                        if expressions_referencing_subquery
                            .iter()
                            .all(|(_, existing_column)| column != existing_column)
                        {
                            expressions_referencing_subquery.push((expr, *column));
                        }
                    }
                    _ => {}
                };
                Ok(WalkControl::Continue)
            },
        )?;
    }

    Ok(expressions_referencing_subquery)
}

fn emit_flush_buffer_if_new_partition(
    program: &mut ProgramBuilder,
    labels: &WindowLabels,
    registers: &WindowRegisters,
    window: &Window,
    plan: &SelectPlan,
) -> Result<()> {
    if let Some(reg_partition_start) = registers.partition_start {
        let same_partition_label = program.allocate_label();
        let new_partition_label = program.allocate_label();

        // Compare the first `deduplicated_partition_by_len` source columns with the saved
        // partition keys. If they differ, this row starts a new partition and we flush the buffer.
        let partition_by_len = window
            .deduplicated_partition_by_len
            .expect("deduplicated_partition_by_len must exist");

        program.add_comment(
            program.offset(),
            "compare partition keys to detect new partition",
        );
        let mut compare_key_info = (0..partition_by_len)
            .map(|_| KeyInfo {
                sort_order: SortOrder::Asc,
                collation: CollationSeq::default(),
                nulls_order: None,
            })
            .collect::<Vec<_>>();
        for (i, c) in compare_key_info
            .iter_mut()
            .enumerate()
            .take(partition_by_len)
        {
            // After rewriting, partition_by entries are Expr::Column references to the
            // subquery. Duplicates reference the same column index, so we find the entry
            // that references column i (the i-th unique partition column) to get the
            // correct collation.
            let expr = window
                .partition_by
                .iter()
                .find(|e| matches!(e, Expr::Column { column, .. } if *column == i))
                .unwrap_or(&window.partition_by[i]);
            let maybe_collation = get_collseq_from_expr(expr, &plan.table_references)?;
            c.collation = maybe_collation.unwrap_or_default();
        }
        program.emit_insn(Insn::Compare {
            start_reg_a: registers.src_columns_start,
            start_reg_b: reg_partition_start,
            count: partition_by_len,
            key_info: compare_key_info,
        });
        program.emit_insn(Insn::Jump {
            target_pc_lt: new_partition_label,
            target_pc_eq: same_partition_label,
            target_pc_gt: new_partition_label,
        });

        program.preassign_label_to_next_insn(new_partition_label);
        program.add_comment(program.offset(), "detected new partition");
        program.emit_insn(Insn::Gosub {
            target_pc: labels.flush_buffer,
            return_reg: registers.flush_buffer_return_offset,
        });
        // Reset rowid to signal the start of processing a new partition.
        program.emit_insn(Insn::Null {
            dest: registers.rowid,
            dest_end: None,
        });
        program.emit_insn(Insn::Copy {
            src_reg: registers.src_columns_start,
            dst_reg: reg_partition_start,
            extra_amount: partition_by_len - 1,
        });

        program.preassign_label_to_next_insn(same_partition_label);
    }

    Ok(())
}

fn emit_load_order_by_columns(
    program: &mut ProgramBuilder,
    window: &Window,
    registers: &WindowRegisters,
) {
    if let Some(reg_new_order_by_columns_start) = registers.new_order_by_columns_start {
        // Source columns are deduplicated and may appear in a different order than
        // the ORDER BY terms. Therefore, we must restore the original ORDER BY layout
        // here by copying the values into an array of registers.
        for (i, (expr, _, _)) in window.order_by.iter().enumerate() {
            match expr {
                Expr::Column { column, .. } => {
                    program.emit_insn(Insn::Copy {
                        src_reg: registers.src_columns_start + column,
                        dst_reg: reg_new_order_by_columns_start + i,
                        extra_amount: 0,
                    });
                }
                _ => unreachable!("expected Column, got {:?}", expr),
            }
        }
    }
}

fn emit_insert_row_into_buffer(
    program: &mut ProgramBuilder,
    registers: &WindowRegisters,
    cursors: &WindowCursors,
    input_column_count: &usize,
    table_name: &str,
) {
    let reg_record = program.alloc_register();

    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(registers.src_columns_start),
        count: to_u32(*input_column_count),
        dest_reg: to_u32(reg_record),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: cursors.csr_write,
        rowid_reg: registers.rowid,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: cursors.csr_write,
        key_reg: registers.rowid,
        record_reg: reg_record,
        flag: InsertFlags::new().require_seek(),
        table_name: table_name.to_string(),
    });
}

/// The three operations that move the frame forward, one step at a time.
/// Each reads the row a cursor points at, updates every window function
/// with it, then moves that cursor on. Mirror SQLite's `WINDOW_AGGSTEP`,
/// `WINDOW_RETURN_ROW`, and `WINDOW_AGGINVERSE` (`window.c:1765-1773`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowOp {
    /// A new row has joined the frame. Read it from `csr_end`, add it to
    /// every function's running total (xStep), then move `csr_end` on.
    AggStep,
    /// The frame for the current row is complete. Compute every function's
    /// result for it (xValue) and send the row at `csr_current` to the
    /// outer query, then move `csr_current` on.
    ReturnRow,
    /// A row has dropped out of the frame. Read it from `csr_start`,
    /// subtract it from every function's running total (xInverse), then
    /// move `csr_start` on. Never happens when the frame always starts at
    /// the first row of the partition (UNBOUNDED PRECEDING): no row ever
    /// leaves, so there is nothing to subtract.
    AggInverse,
}

/// Once a row has been used by a given op it can be deleted from the
/// buffer to free memory sooner. This says after which op that is safe;
/// `None` means keep every row until flush. Deleting early never changes
/// results. Mirrors SQLite's `eDelete` (window.c:2845-2869), applied at
/// the `OP_Delete` in `emit_window_op`:
///
/// - FOLLOWING start, offset provably > 0, non-RANGE → after RETURN_ROW.
///   The frame starts after the current row, so once a row has been
///   output it can never fall inside any later row's frame.
/// - UNBOUNDED PRECEDING start:
///   - a `csr_app` function needs every row (`window_cache_frame`) → never.
///   - N PRECEDING end, offset provably > 0, non-RANGE → after AGGSTEP.
///   - otherwise → after RETURN_ROW.
/// - CURRENT ROW / N PRECEDING start → after AGGINVERSE. Safe with
///   first_value / nth_value: only rows that have already left the frame
///   are deleted, and those functions only ever look up rows still in it.
fn window_delete_op(window: &Window) -> Option<WindowOp> {
    use crate::translate::plan::FrameBoundary;
    let is_range = window.frame.mode == turso_parser::ast::FrameMode::Range;
    match &window.frame.start {
        FrameBoundary::Following(expr) => {
            (!is_range && window_expr_gt_zero(expr)).then_some(WindowOp::ReturnRow)
        }
        FrameBoundary::UnboundedPreceding => {
            if window_cache_frame(window) {
                None
            } else if let FrameBoundary::Preceding(expr) = &window.frame.end {
                (!is_range && window_expr_gt_zero(expr)).then_some(WindowOp::AggStep)
            } else {
                Some(WindowOp::ReturnRow)
            }
        }
        FrameBoundary::CurrentRow | FrameBoundary::Preceding(_) => Some(WindowOp::AggInverse),
        FrameBoundary::UnboundedFollowing => {
            unreachable!("UNBOUNDED FOLLOWING can never be a frame start")
        }
    }
}

/// Whether we must keep every row of the partition in the buffer until the
/// end, rather than deleting rows as the frame moves past them. This is
/// needed when a function might reach out to any row at output time —
/// first_value, nth_value, lag and lead all look up a row by position — or
/// when an EXCLUDE clause forces us to re-scan the frame. Mirrors SQLite's
/// `windowCacheFrame` (window.c:2031-2045).
fn window_cache_frame(window: &Window) -> bool {
    window.frame.exclude.is_some()
        || window.functions.iter().any(|f| {
            matches!(
                &f.func,
                AccumulatorFunc::Window(
                    WindowFunc::FirstValue
                        | WindowFunc::NthValue
                        | WindowFunc::Lag
                        | WindowFunc::Lead
                ),
            )
        })
}

/// Whether a frame offset is a literal provably greater than zero. Used
/// only to decide whether `window_delete_op` may delete rows early: a
/// `N FOLLOWING` (N > 0) start can delete rows once they are output, a
/// `0 FOLLOWING` start can't. Returning `false` is always correct — it
/// just keeps rows until flush — so this only frees memory sooner, never
/// changes results. Mirrors SQLite's `windowExprGtZero`
/// (window.c:2439-2449).
fn window_expr_gt_zero(expr: &Expr) -> bool {
    if let Expr::Literal(turso_parser::ast::Literal::Numeric(s)) = expr {
        if let Ok(n) = s.parse::<i64>() {
            return n > 0;
        }
        if let Ok(f) = s.parse::<f64>() {
            // Matches sqlite3_value_int's cast-to-integer semantics:
            // 0 < f < 1 truncates to 0, which is not > 0.
            return f as i64 > 0;
        }
    }
    false
}

/// Turn each function's running total into the value to output for the
/// current row, writing it into that function's result register, just
/// before the row is emitted (RETURN_ROW). When `finalize` is set the
/// running total is consumed and cleared (done once at the very end);
/// otherwise it is read without disturbing it (done per row as the frame
/// slides). first_value / nth_value / lag / lead are the exception: they
/// have no running total to read, so their result is filled in separately
/// — by looking up a specific row — inside `emit_return_one_row`. Mirrors
/// SQLite's `windowAggFinal(p, 0)` (window.c:1777-1808).
fn emit_window_agg_final(
    program: &mut ProgramBuilder,
    window: &Window,
    registers: &WindowRegisters,
    minmax: &[Option<WindowMinMax>],
    finalize: bool,
) {
    for (i, func) in window.functions.iter().enumerate() {
        let positional = matches!(
            &func.func,
            AccumulatorFunc::Window(WindowFunc::FirstValue | WindowFunc::NthValue)
        );
        let always_lookup = matches!(
            &func.func,
            AccumulatorFunc::Window(WindowFunc::Lag | WindowFunc::Lead)
        );
        if always_lookup || (positional && !finalize) {
            continue;
        }
        let acc_reg = registers.acc_start + i;
        let result_reg = registers.acc_result_start + i;
        if let Some(state) = minmax[i] {
            debug_assert!(!finalize, "EXCLUDE frames do not use min/max indexes");
            let label_empty = program.allocate_label();
            program.emit_insn(Insn::Null {
                dest: result_reg,
                dest_end: None,
            });
            program.emit_insn(Insn::Last {
                cursor_id: state.cursor,
                pc_if_empty: label_empty,
            });
            program.emit_insn(Insn::Column {
                cursor_id: state.cursor,
                column: 0,
                dest: result_reg,
                default: None,
            });
            program.preassign_label_to_next_insn(label_empty);
            continue;
        }
        if finalize {
            program.emit_insn(Insn::AggFinal {
                register: acc_reg,
                func: func.func.clone(),
            });
            program.emit_insn(Insn::Copy {
                src_reg: acc_reg,
                dst_reg: result_reg,
                extra_amount: 0,
            });
            program.emit_insn(Insn::Null {
                dest: acc_reg,
                dest_end: None,
            });
        } else {
            program.emit_insn(Insn::AggValue {
                acc_reg,
                dest_reg: result_reg,
                func: func.func.clone(),
            });
        }
    }
}

/// Recompute every function from scratch over the current frame. Used when
/// the window has an EXCLUDE clause: because some rows inside the frame are
/// excluded, we can't keep a simple running total, so for each output row
/// we re-add the rows the frame currently spans (the
/// `frame_start_rowid`..`frame_end_rowid` range), skipping the excluded
/// ones. Re-adding from scratch also sidesteps needing a subtract-a-row
/// step for aggregates that don't have one, such as min / max and
/// group_concat.
fn emit_window_full_scan(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let window = plan.window.as_ref().expect("missing window");
    let registers = meta.registers;
    let cursors = meta.cursors;
    let minmax = meta.minmax.clone();
    let exclude = window
        .frame
        .exclude
        .as_ref()
        .expect("full frame scan requires an explicit EXCLUDE clause");
    let frame_start_rowid = registers
        .frame_start_rowid
        .expect("EXCLUDE frame requires a start-rowid tracker");
    let frame_end_rowid = registers
        .frame_end_rowid
        .expect("EXCLUDE frame requires an end-rowid tracker");
    let scan_cursor = cursors
        .csr_app
        .expect("EXCLUDE frame requires a full-scan cursor");

    let current_rowid = program.alloc_register();
    let scan_rowid = program.alloc_register();
    program.emit_insn(Insn::RowId {
        cursor_id: cursors.csr_current,
        dest: current_rowid,
    });

    let compare_peers = matches!(
        exclude,
        turso_parser::ast::FrameExclude::Group | turso_parser::ast::FrameExclude::Ties
    );
    let order_by_len = window.order_by.len();
    let current_peer =
        (compare_peers && order_by_len > 0).then(|| program.alloc_registers(order_by_len));
    let scan_peer =
        (compare_peers && order_by_len > 0).then(|| program.alloc_registers(order_by_len));
    if let Some(current_peer) = current_peer {
        for (i, (expr, _, _)) in window.order_by.iter().enumerate() {
            let Expr::Column { column, .. } = expr else {
                unreachable!("window ORDER BY expressions are buffer columns after rewrite");
            };
            program.emit_insn(Insn::Column {
                cursor_id: cursors.csr_current,
                column: *column,
                dest: current_peer + i,
                default: None,
            });
        }
    }

    program.emit_insn(Insn::Null {
        dest: registers.acc_start,
        dest_end: Some(registers.acc_start + window.functions.len() - 1),
    });

    let label_break = program.allocate_label();
    let label_loop = program.allocate_label();
    let label_next = program.allocate_label();
    let label_step = program.allocate_label();
    program.emit_insn(Insn::SeekGE {
        is_index: false,
        cursor_id: scan_cursor,
        start_reg: frame_start_rowid,
        num_regs: 1,
        target_pc: label_break,
        eq_only: false,
        null_matching_mask: Default::default(),
    });
    program.preassign_label_to_next_insn(label_loop);
    program.emit_insn(Insn::RowId {
        cursor_id: scan_cursor,
        dest: scan_rowid,
    });
    program.emit_insn(Insn::Gt {
        lhs: scan_rowid,
        rhs: frame_end_rowid,
        target_pc: label_break,
        flags: crate::vdbe::insn::CmpInsFlags::default(),
        collation: None,
    });

    match exclude {
        turso_parser::ast::FrameExclude::NoOthers => {}
        turso_parser::ast::FrameExclude::CurrentRow => {
            program.emit_insn(Insn::Eq {
                lhs: current_rowid,
                rhs: scan_rowid,
                target_pc: label_next,
                flags: crate::vdbe::insn::CmpInsFlags::default(),
                collation: None,
            });
        }
        turso_parser::ast::FrameExclude::Group | turso_parser::ast::FrameExclude::Ties => {
            if matches!(exclude, turso_parser::ast::FrameExclude::Ties) {
                program.emit_insn(Insn::Eq {
                    lhs: current_rowid,
                    rhs: scan_rowid,
                    target_pc: label_step,
                    flags: crate::vdbe::insn::CmpInsFlags::default(),
                    collation: None,
                });
            }
            if order_by_len == 0 {
                program.emit_insn(Insn::Goto {
                    target_pc: label_next,
                });
            } else {
                let current_peer = current_peer.expect("allocated above");
                let scan_peer = scan_peer.expect("allocated above");
                for (i, (expr, _, _)) in window.order_by.iter().enumerate() {
                    let Expr::Column { column, .. } = expr else {
                        unreachable!(
                            "window ORDER BY expressions are buffer columns after rewrite"
                        );
                    };
                    program.emit_insn(Insn::Column {
                        cursor_id: scan_cursor,
                        column: *column,
                        dest: scan_peer + i,
                        default: None,
                    });
                }
                let (reg_a, reg_b) = (current_peer.min(scan_peer), current_peer.max(scan_peer));
                program.emit_insn(Insn::Compare {
                    start_reg_a: reg_a,
                    start_reg_b: reg_b,
                    count: order_by_len,
                    key_info: build_order_by_key_info(window, &plan.table_references)?,
                });
                program.emit_insn(Insn::Jump {
                    target_pc_lt: label_step,
                    target_pc_eq: label_next,
                    target_pc_gt: label_step,
                });
            }
        }
    }

    program.preassign_label_to_next_insn(label_step);
    emit_function_step(program, t_ctx, plan, scan_cursor)?;
    program.preassign_label_to_next_insn(label_next);
    program.emit_insn(Insn::Next {
        cursor_id: scan_cursor,
        pc_if_next: label_loop,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(label_break);
    emit_window_agg_final(program, window, &registers, &minmax, true);
    Ok(())
}

/// A comparison operator held as plain data. The RANGE boundary test
/// needs to reason about the operator before emitting it — flipping it
/// for DESC order (`>=` becomes `<=`, and so on) and branching on its
/// direction for the NULL cases — none of which is possible once it's a
/// built `Insn::Ge { .. }`. `emit_range_cmp` turns it into the real
/// instruction at the end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RangeCmp {
    Lt,
    Le,
    Gt,
    Ge,
}

fn emit_range_cmp(
    program: &mut ProgramBuilder,
    op: RangeCmp,
    lhs: usize,
    rhs: usize,
    target_pc: BranchOffset,
    flags: crate::vdbe::insn::CmpInsFlags,
    collation: Option<CollationSeq>,
) {
    let insn = match op {
        RangeCmp::Lt => Insn::Lt {
            lhs,
            rhs,
            target_pc,
            flags,
            collation,
        },
        RangeCmp::Le => Insn::Le {
            lhs,
            rhs,
            target_pc,
            flags,
            collation,
        },
        RangeCmp::Gt => Insn::Gt {
            lhs,
            rhs,
            target_pc,
            flags,
            collation,
        },
        RangeCmp::Ge => Insn::Ge {
            lhs,
            rhs,
            target_pc,
            flags,
            collation,
        },
    };
    program.emit_insn(insn);
}

/// Emit the value comparison a numeric RANGE offset turns on: jump to
/// `target_pc` when the two cursors' ORDER BY values, with `offset`
/// applied, satisfy `op`. Ascending order compares `csr1.value + offset`
/// against `csr2.value` — for `RANGE BETWEEN 10 PRECEDING`, this is what
/// decides whether a trailing row is still within 10 of the current one.
/// Descending order subtracts the offset and flips `op` instead.
/// Non-numeric values skip the arithmetic, and an explicit NULLS
/// FIRST/LAST is handled first. Mirrors SQLite's `windowCodeRangeTest`.
fn emit_window_range_test(
    program: &mut ProgramBuilder,
    plan: &SelectPlan,
    op: RangeCmp,
    csr1: CursorID,
    offset_reg: usize,
    csr2: CursorID,
    target_pc: BranchOffset,
) -> Result<()> {
    let window = plan.window.as_ref().expect("missing window");
    let [(order_expr, sort_order, nulls_order)] = window.order_by.as_slice() else {
        unreachable!("RANGE offsets require exactly one ORDER BY expression");
    };
    let Expr::Column { column, .. } = order_expr else {
        unreachable!("window ORDER BY expressions are buffer columns after rewrite");
    };
    let reg1 = program.alloc_register();
    let reg2 = program.alloc_register();
    program.emit_insn(Insn::Column {
        cursor_id: csr1,
        column: *column,
        dest: reg1,
        default: None,
    });
    program.emit_insn(Insn::Column {
        cursor_id: csr2,
        column: *column,
        dest: reg2,
        default: None,
    });

    let (op, subtract) = if *sort_order == SortOrder::Desc {
        (
            match op {
                RangeCmp::Ge => RangeCmp::Le,
                RangeCmp::Gt => RangeCmp::Lt,
                RangeCmp::Le => RangeCmp::Ge,
                RangeCmp::Lt => RangeCmp::Gt,
            },
            true,
        )
    } else {
        (op, false)
    };
    let collation = get_collseq_from_expr(order_expr, &plan.table_references)?.unwrap_or_default();
    let big_null = matches!(
        (sort_order, nulls_order),
        (SortOrder::Asc, Some(turso_parser::ast::NullsOrder::Last))
            | (SortOrder::Desc, Some(turso_parser::ast::NullsOrder::First))
    );
    let label_done = program.allocate_label();

    // NULLS sort at the high end here (NULLS LAST ascending, NULLS FIRST
    // descending), so a NULL value compares as greater than any real one.
    // Handle the NULL operands directly, since the arithmetic below can't.
    if big_null {
        let label_reg1_not_null = program.allocate_label();
        program.emit_insn(Insn::NotNull {
            reg: reg1,
            target_pc: label_reg1_not_null,
        });
        match op {
            RangeCmp::Ge => program.emit_insn(Insn::Goto { target_pc }),
            RangeCmp::Gt => program.emit_insn(Insn::NotNull {
                reg: reg2,
                target_pc,
            }),
            RangeCmp::Le => program.emit_insn(Insn::IsNull {
                reg: reg2,
                target_pc,
            }),
            RangeCmp::Lt => {}
        }
        program.emit_insn(Insn::Goto {
            target_pc: label_done,
        });
        program.preassign_label_to_next_insn(label_reg1_not_null);
        program.emit_insn(Insn::IsNull {
            reg: reg2,
            target_pc: if matches!(op, RangeCmp::Gt | RangeCmp::Ge) {
                label_done
            } else {
                target_pc
            },
        });
    }

    // Text and blobs sort at or above the empty string, so only numeric
    // values fall through to Add/Subtract. NULL arithmetic remains NULL.
    let reg_empty = program.alloc_register();
    let label_after_arithmetic = program.allocate_label();
    program.emit_insn(Insn::String8 {
        value: String::new(),
        dest: reg_empty,
    });
    program.emit_insn(Insn::Ge {
        lhs: reg1,
        rhs: reg_empty,
        target_pc: label_after_arithmetic,
        flags: crate::vdbe::insn::CmpInsFlags::default(),
        collation: None,
    });

    // SQLite performs this comparison before arithmetic in the two cases
    // where overflow to +/-infinity cannot change a successful result.
    if (op == RangeCmp::Ge && !subtract) || (op == RangeCmp::Le && subtract) {
        emit_range_cmp(
            program,
            op,
            reg1,
            reg2,
            target_pc,
            crate::vdbe::insn::CmpInsFlags::default(),
            Some(collation),
        );
    }
    if subtract {
        program.emit_insn(Insn::Subtract {
            lhs: reg1,
            rhs: offset_reg,
            dest: reg1,
        });
    } else {
        program.emit_insn(Insn::Add {
            lhs: reg1,
            rhs: offset_reg,
            dest: reg1,
        });
    }
    program.preassign_label_to_next_insn(label_after_arithmetic);
    emit_range_cmp(
        program,
        op,
        reg1,
        reg2,
        target_pc,
        crate::vdbe::insn::CmpInsFlags::default().null_eq(),
        Some(collation),
    );
    program.preassign_label_to_next_insn(label_done);
    Ok(())
}

/// Emit one of the three operations that move the frame forward — add a
/// row to the totals (AGGSTEP), emit a row's result (RETURN_ROW), or
/// remove a row from the totals (AGGINVERSE). Mirrors SQLite's
/// `windowCodeOp` (window.c:2229-2376).
///
/// `countdown_reg`: a register holding a count. While it is positive the
/// op is skipped and the count is decremented (`OP_IfPos`); the op only
/// starts firing once the count reaches zero. This delays an op for the
/// first N rows — cume_dist delays RETURN_ROW, and `ROWS BETWEEN N
/// PRECEDING` delays AGGINVERSE until the frame has grown to N+1 rows.
///
/// `break_on_eof`: only set at flush time, to exit a RETURN_ROW loop on
/// EOF instead of falling through.
///
/// Under RANGE / GROUPS one call handles a whole peer group at once
/// (AGGSTEP adds every row in it, RETURN_ROW emits every row in it, all
/// sharing the same totals); under ROWS one call handles exactly one row.
fn emit_window_op(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
    op: WindowOp,
    countdown_reg: Option<usize>,
    break_on_eof: Option<BranchOffset>,
    in_flush: bool,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let window = plan.window.as_ref().expect("missing window");
    let registers = meta.registers;
    let cursors = meta.cursors;
    let buffer_table_name = meta.buffer_table_name.clone();
    let minmax = meta.minmax.clone();
    let order_by_len = window.order_by.len();
    let frame_mode = window.frame.mode;
    // Under RANGE / GROUPS frames one advance steps over every row with
    // equal ORDER BY values (a peer group); with no ORDER BY every row
    // counts as equal, so it runs to the end of the partition. Mirrors
    // SQLite's `WindowCodeArg.eFrmType != TK_ROWS` test (window.c:2247).
    let b_peer = frame_mode != turso_parser::ast::FrameMode::Rows;

    // AGGINVERSE does nothing when the frame starts at UNBOUNDED
    // PRECEDING: no row ever leaves the frame. Mirrors SQLite's early
    // return at window.c:2252-2257, which lets every caller emit all three
    // operations without first checking for this case.
    if matches!(op, WindowOp::AggInverse)
        && matches!(
            window.frame.start,
            crate::translate::plan::FrameBoundary::UnboundedPreceding
        )
    {
        assert!(
            countdown_reg.is_none() && break_on_eof.is_none(),
            "an AGGINVERSE no-op cannot carry a countdown or an EOF break"
        );
        return Ok(());
    }

    let label_done = program.allocate_label();
    // Cursor + per-cursor peer reference register for each op. Mirrors
    // SQLite's switch at `window.c:2315-2344` plus the reg pickup at
    // `window.c:2317-2336`.
    let (cursor_for_op, peer_ref_reg) = match op {
        WindowOp::AggStep => (
            cursors.csr_end,
            registers.cursor_peer_values[FrameCursor::End],
        ),
        WindowOp::ReturnRow => (
            cursors.csr_current,
            registers.cursor_peer_values[FrameCursor::Current],
        ),
        WindowOp::AggInverse => (
            cursors
                .csr_start
                .expect("AggInverse can only be emitted when the window has a moving frame start"),
            registers.cursor_peer_values[FrameCursor::Start],
        ),
    };

    // ROWS/GROUPS offsets use a countdown. RANGE offsets instead compare
    // ORDER BY values and loop back after each peer group until the cursor
    // reaches the value boundary.
    let range_loop_start = if let Some(reg) = countdown_reg {
        if frame_mode == turso_parser::ast::FrameMode::Range {
            let label = program.allocate_label();
            program.preassign_label_to_next_insn(label);
            match op {
                WindowOp::AggInverse => {
                    if matches!(
                        window.frame.start,
                        crate::translate::plan::FrameBoundary::Following(_)
                    ) {
                        emit_window_range_test(
                            program,
                            plan,
                            RangeCmp::Le,
                            cursors.csr_current,
                            reg,
                            cursor_for_op,
                            label_done,
                        )?;
                    } else {
                        emit_window_range_test(
                            program,
                            plan,
                            RangeCmp::Ge,
                            cursor_for_op,
                            reg,
                            cursors.csr_current,
                            label_done,
                        )?;
                    }
                }
                WindowOp::AggStep => emit_window_range_test(
                    program,
                    plan,
                    RangeCmp::Gt,
                    cursor_for_op,
                    reg,
                    cursors.csr_current,
                    label_done,
                )?,
                WindowOp::ReturnRow => {
                    unreachable!("RANGE offsets never gate RETURN_ROW directly")
                }
            }
            Some(label)
        } else {
            program.emit_insn(Insn::IfPos {
                reg,
                target_pc: label_done,
                decrement_by: 1,
            });
            None
        }
    } else {
        None
    };

    // RETURN_ROW finalizes accumulators before emitting (SQLite's
    // windowAggFinal at window.c:2284).
    if matches!(op, WindowOp::ReturnRow) && window.frame.exclude.is_none() {
        emit_window_agg_final(program, window, &registers, &minmax, false);
    }

    let label_continue = program.allocate_label();
    program.preassign_label_to_next_insn(label_continue);

    // For same-kind RANGE offsets, keep the frame-start cursor at or before
    // the frame-end cursor. While the source is still producing rows, also
    // keep csr_end at or before the newest buffered row. During flush SQLite
    // clears its source-rowid register, disabling the latter guard.
    let same_kind_range_offsets = frame_mode == turso_parser::ast::FrameMode::Range
        && countdown_reg.is_some()
        && matches!(
            (&window.frame.start, &window.frame.end),
            (
                crate::translate::plan::FrameBoundary::Preceding(_),
                crate::translate::plan::FrameBoundary::Preceding(_)
            ) | (
                crate::translate::plan::FrameBoundary::Following(_),
                crate::translate::plan::FrameBoundary::Following(_)
            )
        );
    if same_kind_range_offsets {
        match op {
            WindowOp::AggInverse => {
                let start_rowid = program.alloc_register();
                let end_rowid = program.alloc_register();
                program.emit_insn(Insn::RowId {
                    cursor_id: cursor_for_op,
                    dest: start_rowid,
                });
                program.emit_insn(Insn::RowId {
                    cursor_id: cursors.csr_end,
                    dest: end_rowid,
                });
                program.emit_insn(Insn::Ge {
                    lhs: start_rowid,
                    rhs: end_rowid,
                    target_pc: label_done,
                    flags: crate::vdbe::insn::CmpInsFlags::default(),
                    collation: None,
                });
            }
            WindowOp::AggStep if !in_flush => {
                let end_rowid = program.alloc_register();
                program.emit_insn(Insn::RowId {
                    cursor_id: cursor_for_op,
                    dest: end_rowid,
                });
                program.emit_insn(Insn::Ge {
                    lhs: end_rowid,
                    rhs: registers.rowid,
                    target_pc: label_done,
                    flags: crate::vdbe::insn::CmpInsFlags::default(),
                    collation: None,
                });
            }
            WindowOp::AggStep | WindowOp::ReturnRow => {}
        }
    }

    match op {
        WindowOp::AggStep => {
            if let Some(frame_end_rowid) = registers.frame_end_rowid {
                assert!(
                    registers.frame_start_rowid.is_some(),
                    "EXCLUDE frame rowid trackers must be allocated as a pair"
                );
                program.emit_insn(Insn::AddImm {
                    register: frame_end_rowid,
                    value: 1,
                });
            } else {
                emit_function_step(program, t_ctx, plan, cursors.csr_end)?;
                // Count the row just added to the frame. This runs once
                // for every row stepped, so after AGGSTEP has stepped over
                // a whole peer group the counter holds the frame end's
                // position in the buffer — the row first_value / nth_value
                // seek to. Mirrors SQLite's `OP_AddImm regApp+1` inside
                // `windowAggStep` (window.c:1726).
                if let Some(frame_counters) = registers.frame_counters {
                    program.emit_insn(Insn::AddImm {
                        register: frame_counters + 1,
                        value: 1,
                    });
                }
            }
        }
        WindowOp::ReturnRow => {
            emit_return_one_row(program, t_ctx, plan)?;
        }
        WindowOp::AggInverse => {
            if let Some(frame_start_rowid) = registers.frame_start_rowid {
                assert!(
                    registers.frame_end_rowid.is_some(),
                    "EXCLUDE frame rowid trackers must be allocated as a pair"
                );
                program.emit_insn(Insn::AddImm {
                    register: frame_start_rowid,
                    value: 1,
                });
            } else {
                emit_function_inverse(program, t_ctx, plan)?;
                // Count the row that just left the frame; the counter is the
                // frame start's buffer index minus one. Mirrors SQLite's
                // `OP_AddImm regApp+0` inside `windowAggStep`
                // (window.c:1726, `regApp+1-bInverse`).
                if let Some(frame_counters) = registers.frame_counters {
                    program.emit_insn(Insn::AddImm {
                        register: frame_counters,
                        value: 1,
                    });
                }
            }
        }
    }

    // Delete the row we are done with (SQLite window.c:2346,
    // `if( op==p->eDelete ) OP_Delete`), before advancing the cursor past it.
    if window_delete_op(window) == Some(op) {
        program.emit_insn(Insn::Delete {
            cursor_id: cursor_for_op,
            table_name: buffer_table_name,
            is_part_of_update: false,
        });
    }

    // Advance the cursor. Three control-flow shapes mirroring SQLite's
    // `windowCodeOp` tail at window.c:2351-2361:
    //   - break_on_eof=Some: Next; on EOF fall through to a Goto that
    //     jumps to the caller's break label.
    //   - bPeer=true: Next; on EOF fall through to Goto label_done; else
    //     fall through to the peer-group check below (is the next row
    //     still in the same group?).
    //   - bPeer=false, no break_on_eof: Next; on EOF fall through;
    //     no peer check; label_done is the next instruction.
    let label_after_next = program.allocate_label();
    program.emit_insn(Insn::Next {
        cursor_id: cursor_for_op,
        pc_if_next: label_after_next,
        fullscan: false,
        is_index: false,
    });
    if let Some(break_target) = break_on_eof {
        program.emit_insn(Insn::Goto {
            target_pc: break_target,
        });
    } else if b_peer {
        program.emit_insn(Insn::Goto {
            target_pc: label_done,
        });
    }
    program.preassign_label_to_next_insn(label_after_next);

    if b_peer {
        // Keep repeating this operation as long as the row we just moved
        // to is a peer of the cursor's current group (same ORDER BY
        // values), so a whole group is handled in one call. When the group
        // changes, record the new group's values in this cursor's entry of
        // `cursor_peer_values`. There is one entry per cursor, so the
        // start / current / end cursors can each be on a different group
        // during flush — needed for percent_rank / cume_dist. Mirrors
        // SQLite's `windowReadPeerValues` +
        // `windowIfNewPeer` tail (window.c:2363-2369); with no ORDER BY the
        // whole partition is one group, so this just loops to EOF.
        let temp_start = (order_by_len > 0).then(|| {
            let temp_start = program.alloc_registers(order_by_len);
            for (i, (expr, _, _)) in window.order_by.iter().enumerate() {
                if let Expr::Column { column, .. } = expr {
                    program.emit_insn(Insn::Column {
                        cursor_id: cursor_for_op,
                        column: *column,
                        dest: temp_start + i,
                        default: None,
                    });
                } else {
                    unreachable!("expected Column in window.order_by, got {:?}", expr);
                }
            }
            temp_start
        });
        emit_if_new_peer(
            program,
            window,
            &plan.table_references,
            temp_start,
            peer_ref_reg,
            label_continue,
        )?;
    }

    if let Some(range_loop_start) = range_loop_start {
        program.emit_insn(Insn::Goto {
            target_pc: range_loop_start,
        });
    }
    program.preassign_label_to_next_insn(label_done);

    Ok(())
}

/// Emit the per-row FILTER predicate read used by both `AggStep` (on
/// `csr_end`, the row entering the frame) and `AggInverse` (on
/// `csr_start`, the row leaving). Caller is responsible for pinning
/// the returned label to the instruction the filtered emit should
/// skip past.
fn emit_filter_skip(
    program: &mut ProgramBuilder,
    table_references: &TableReferences,
    resolver: &Resolver,
    cursor: CursorID,
    filter_expr: &Expr,
) -> Result<BranchOffset> {
    let label = program.allocate_label();
    let filter_reg = program.alloc_register();
    if let Expr::Column { column, .. } = filter_expr {
        program.emit_insn(Insn::Column {
            cursor_id: cursor,
            column: *column,
            dest: filter_reg,
            default: None,
        });
    } else {
        translate_expr(
            program,
            Some(table_references),
            filter_expr,
            filter_reg,
            resolver,
        )?;
    }
    program.emit_insn(Insn::IfNot {
        reg: filter_reg,
        target_pc: label,
        jump_if_null: true,
    });
    Ok(label)
}

/// Read one window-function argument from a buffered row. Most arguments are
/// a single source column. A few JSON arguments intentionally remain scalar
/// expressions so their JSON subtype is created after the record buffer; for
/// those, load and cache every source column the expression reads, then
/// evaluate the expression into `dest`.
fn emit_window_arg_from_cursor(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
    cursor: CursorID,
    arg: &Expr,
    dest: usize,
) -> Result<()> {
    if let Expr::Column { column, .. } = arg {
        program.emit_insn(Insn::Column {
            cursor_id: cursor,
            column: *column,
            dest,
            default: None,
        });
    } else {
        walk_expr(arg, &mut |node| {
            if let Expr::Column { column, .. } = node {
                let value_reg = program.alloc_register();
                program.emit_insn(Insn::Column {
                    cursor_id: cursor,
                    column: *column,
                    dest: value_reg,
                    default: None,
                });
                t_ctx.resolver.cache_expr_reg(
                    std::borrow::Cow::Owned(node.clone()),
                    value_reg,
                    false,
                    None,
                );
                return Ok(WalkControl::SkipChildren);
            }
            Ok(WalkControl::Continue)
        })?;
        t_ctx.resolver.expr_to_reg_cache_enabled = true;
        translate_expr(
            program,
            Some(&plan.table_references),
            arg,
            dest,
            &t_ctx.resolver,
        )?;
    }
    t_ctx
        .resolver
        .cache_expr_reg(std::borrow::Cow::Owned(arg.clone()), dest, false, None);
    Ok(())
}

/// Emit the code that adds one row into each function's running total (an
/// AGGSTEP step), reading that row's argument values from `read_csr` — the
/// cursor sitting on the row that just joined the frame.
///
/// Mirrors SQLite's `windowAggStep` (`window.c:1658-1762`). For each
/// function, its argument values are loaded from the cursor's current row
/// into their own small block of registers, and the aggregate step is
/// emitted against that block. Those load registers are kept deliberately
/// apart from `src_columns_start` (where the source loop puts the next
/// row's columns), so stepping the previous partition here can't overwrite
/// the first row of the next one.
///
/// While emitting, each argument expression is pushed into the resolver's
/// expression cache, so that when the aggregate-step translation looks up
/// a column it finds the register we just loaded instead of
/// `src_columns_start`. The cache is put back to how it was on the way out.
fn emit_function_step(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
    read_csr: CursorID,
) -> Result<()> {
    let (acc_start, minmax, csr_current) = {
        let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
        (
            meta.registers.acc_start,
            meta.minmax.clone(),
            meta.cursors.csr_current,
        )
    };
    let window = plan.window.as_ref().expect("missing window");

    // Save cache state so the per-arg overrides we push below don't leak
    // out to other parts of the emit pipeline (e.g. emit_return_one_row).
    let initial_cache_len = t_ctx.resolver.expr_to_reg_cache.len();
    let cache_was_enabled = t_ctx.resolver.expr_to_reg_cache_enabled;

    for (i, func) in window.functions.iter().enumerate() {
        // Normally first_value / nth_value / lag / lead take no part in the
        // running totals — at output time they just jump straight to the
        // row they need. But an EXCLUDE clause can punch holes in the
        // frame, so first_value / nth_value switch to the slower approach
        // of being stepped row by row, which lets them skip the excluded
        // rows. (lag / lead ignore the frame entirely, so they are always
        // looked up directly and never stepped here.)
        let positional = matches!(
            &func.func,
            AccumulatorFunc::Window(WindowFunc::FirstValue | WindowFunc::NthValue)
        );
        let always_lookup = matches!(
            &func.func,
            AccumulatorFunc::Window(WindowFunc::Lag | WindowFunc::Lead)
        );
        if always_lookup || (positional && window.frame.exclude.is_none()) {
            continue;
        }
        let acc_reg = acc_start + i;
        let args: Vec<Expr> = match func.current_expr() {
            Expr::FunctionCall { args, .. } => args.iter().map(|a| (**a).clone()).collect(),
            Expr::FunctionCallStar { .. } => vec![],
            _ => unreachable!("window functions are FunctionCall or FunctionCallStar expressions"),
        };

        // Load each argument from the frame cursor into a fresh register.
        // Most arguments are one buffered column; JSON subtype-producing
        // arguments are evaluated here from their buffered input columns.
        // Cache entries make the aggregate-step translation reuse those
        // freshly loaded values.
        let arg_load_start = (!args.is_empty()).then(|| program.alloc_registers(args.len()));
        if let Some(base) = arg_load_start {
            for (j, arg) in args.iter().enumerate() {
                // SQLite's slow nth_value() path reads the value from each
                // included scan row, but keeps N fixed to the current output
                // row (window.c:1679-1683).
                let arg_cursor = if window.frame.exclude.is_some()
                    && j == 1
                    && matches!(&func.func, AccumulatorFunc::Window(WindowFunc::NthValue))
                {
                    csr_current
                } else {
                    read_csr
                };
                emit_window_arg_from_cursor(program, t_ctx, plan, arg_cursor, arg, base + j)?;
            }
        }
        t_ctx.resolver.expr_to_reg_cache_enabled = true;

        match &func.func {
            AccumulatorFunc::Agg(agg_func) => {
                let filter_skip_label = func
                    .rewritten
                    .as_ref()
                    .and_then(|r| r.filter_expr.as_ref())
                    .map(|f| {
                        emit_filter_skip(
                            program,
                            &plan.table_references,
                            &t_ctx.resolver,
                            read_csr,
                            f,
                        )
                    })
                    .transpose()?;

                if let Some(state) = minmax[i] {
                    let label_skip = filter_skip_label.unwrap_or_else(|| program.allocate_label());
                    translate_expr(
                        program,
                        Some(&plan.table_references),
                        &args[0],
                        state.registers,
                        &t_ctx.resolver,
                    )?;
                    program.emit_insn(Insn::IsNull {
                        reg: state.registers,
                        target_pc: label_skip,
                    });
                    program.emit_insn(Insn::AddImm {
                        register: state.registers + 1,
                        value: 1,
                    });
                    program.emit_insn(Insn::MakeRecord {
                        start_reg: to_u32(state.registers),
                        count: 2,
                        dest_reg: to_u32(state.registers + 2),
                        index_name: None,
                        affinity_str: None,
                    });
                    program.emit_insn(Insn::IdxInsert {
                        cursor_id: state.cursor,
                        record_reg: state.registers + 2,
                        unpacked_start: Some(state.registers),
                        unpacked_count: Some(2),
                        flags: IdxInsertFlags::new(),
                    });
                    program.preassign_label_to_next_insn(label_skip);
                } else {
                    translate_aggregation_step(
                        program,
                        &plan.table_references,
                        AggArgumentSource::new_from_expression(
                            agg_func,
                            &args,
                            &Distinctness::NonDistinct,
                        ),
                        acc_reg,
                        &t_ctx.resolver,
                        None,
                    )?;

                    if let Some(label) = filter_skip_label {
                        program.preassign_label_to_next_insn(label);
                    }
                }
            }
            AccumulatorFunc::Window(win_func) => {
                // 0-ary window funcs (row_number) ignore `col`; the runtime
                // only reads `state.registers[col + i]` for i in 0..arity.
                program.emit_insn(Insn::AggStep {
                    data: Box::new(AggStepData {
                        acc_reg,
                        col: arg_load_start.unwrap_or(0),
                        delimiter: 0,
                        func: AccumulatorFunc::Window(win_func.clone()),
                        comparator: None,
                        collation: None,
                    }),
                });
            }
        }
    }

    // Restore expr-cache state so the per-function arg overrides don't
    // leak into later emit calls (e.g. emit_return_one_row's outer-query
    // result emission, which expects its own result_columns_start mapping).
    t_ctx.resolver.expr_to_reg_cache.truncate(initial_cache_len);
    t_ctx.resolver.expr_to_reg_cache_enabled = cache_was_enabled;

    Ok(())
}

/// Emit the code that takes one row back out of each function's running
/// total (an AGGINVERSE step), for the row leaving the frame at its start.
/// This is the mirror image of `emit_function_step`, which adds a row at
/// the frame's end. Mirrors the inverse path inside SQLite's
/// `windowAggStep(... bInverse=1, ...)` (window.c:2329).
///
/// ntile / percent_rank / cume_dist only track counts, so removing a row
/// just adjusts an internal counter — there is no argument value to load.
/// For aggregates over a moving start (`sum`, `count(arg)`, `avg`) we first
/// load the leaving row's argument value from `csr_start`, so the runtime
/// knows what to subtract back out — the same way `emit_function_step`
/// loads from `csr_end` when adding a row.
fn emit_function_inverse(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let window = plan.window.as_ref().expect("missing window");
    let acc_start = meta.registers.acc_start;
    let minmax = meta.minmax.clone();
    let csr_start = meta.cursors.csr_start.expect(
        "emit_function_inverse: csr_start must be allocated when any AGGINVERSE is emitted",
    );
    let initial_cache_len = t_ctx.resolver.expr_to_reg_cache.len();
    let cache_was_enabled = t_ctx.resolver.expr_to_reg_cache_enabled;

    for (i, func) in window.functions.iter().enumerate() {
        let acc_reg = acc_start + i;
        let args: Vec<Expr> = match func.current_expr() {
            Expr::FunctionCall { args, .. } => args.iter().map(|a| (**a).clone()).collect(),
            Expr::FunctionCallStar { .. } => vec![],
            _ => unreachable!("window functions are FunctionCall or FunctionCallStar expressions"),
        };

        let arg_load_start = (!args.is_empty()).then(|| program.alloc_registers(args.len()));
        if let Some(base) = arg_load_start {
            for (j, arg) in args.iter().enumerate() {
                emit_window_arg_from_cursor(program, t_ctx, plan, csr_start, arg, base + j)?;
            }
        }

        // A row skipped by FILTER when it entered was never added to the
        // totals (AggStep), so it must not be subtracted (AggInverse)
        // either — otherwise the running totals go wrong and `count(*)`
        // would go negative.
        let filter_skip_label = func
            .rewritten
            .as_ref()
            .and_then(|r| r.filter_expr.as_ref())
            .map(|f| {
                emit_filter_skip(
                    program,
                    &plan.table_references,
                    &t_ctx.resolver,
                    csr_start,
                    f,
                )
            })
            .transpose()?;

        if let Some(state) = minmax[i] {
            let label_skip = filter_skip_label.unwrap_or_else(|| program.allocate_label());
            let arg_reg = arg_load_start.expect("min/max has one argument");
            program.emit_insn(Insn::Copy {
                src_reg: arg_reg,
                dst_reg: state.registers,
                extra_amount: 0,
            });
            program.emit_insn(Insn::IsNull {
                reg: state.registers,
                target_pc: label_skip,
            });
            program.emit_insn(Insn::SeekGE {
                is_index: true,
                cursor_id: state.cursor,
                start_reg: state.registers,
                num_regs: 1,
                target_pc: label_skip,
                eq_only: false,
                null_matching_mask: Default::default(),
            });
            program.emit_insn(Insn::Delete {
                cursor_id: state.cursor,
                table_name: String::new(),
                is_part_of_update: false,
            });
            program.preassign_label_to_next_insn(label_skip);
        } else {
            program.emit_insn(Insn::AggInverse {
                acc_reg,
                col: arg_load_start.unwrap_or(0),
                delimiter: 0,
                func: func.func.clone(),
                comparator: None,
            });

            if let Some(label) = filter_skip_label {
                program.preassign_label_to_next_insn(label);
            }
        }
    }
    t_ctx.resolver.expr_to_reg_cache.truncate(initial_cache_len);
    t_ctx.resolver.expr_to_reg_cache_enabled = cache_was_enabled;
    Ok(())
}

/// Emit the per-output-row lookup for every `lag` / `lead` function in the
/// current window. Mirrors SQLite's lag/lead arm in `windowReturnOneRow`
/// (`window.c:1958`):
///
/// ```text
///   reg_result := arg[2] from csr_current   (default; NULL if nArg < 3)
///   tmp := rowid(csr_current)
///   tmp := tmp ± offset                     (offset = 1 if nArg < 2,
///                                             else arg[1] from csr_current)
///   if SeekRowid(csr_app, tmp) succeeds:
///       reg_result := Column(csr_app, arg_col[0])
///   lbl_miss:
/// ```
///
/// `csr_app` is a separate cursor (a duplicate of `csr_current`) used only
/// for these lookups, so moving it doesn't disturb the three frame
/// cursors. The result goes into the function's result register
/// (`acc_result_start + i`); later, when the SELECT output is built, the
/// expression cache points the function's value at that register.
fn emit_lag_lead_lookup(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let window = plan.window.as_ref().expect("missing window");
    let registers = meta.registers;
    let cursors = meta.cursors;
    let acc_result_start = registers.acc_result_start;
    let csr_current = cursors.csr_current;

    for (i, func) in window.functions.iter().enumerate() {
        let is_lead = match &func.func {
            AccumulatorFunc::Window(WindowFunc::Lag) => false,
            AccumulatorFunc::Window(WindowFunc::Lead) => true,
            _ => continue,
        };
        let csr_app = cursors
            .csr_app
            .expect("csr_app must exist when a window contains lag / lead");
        let result_reg = acc_result_start + i;

        let args: Vec<&Expr> = match func.current_expr() {
            Expr::FunctionCall { args, .. } => args.iter().map(|a| a.as_ref()).collect(),
            _ => unreachable!("lag / lead are always Expr::FunctionCall"),
        };

        let arg0_col = match args.first() {
            Some(Expr::Column { column, .. }) => *column,
            other => unreachable!(
                "lag/lead arg[0] must be a buffer column reference after planner rewrite, got {other:?}"
            ),
        };

        // Default value first: NULL when nArg < 3, else read arg[2] from csr_current.
        if args.len() < 3 {
            program.emit_insn(Insn::Null {
                dest: result_reg,
                dest_end: None,
            });
        } else if let Expr::Column { column, .. } = args[2] {
            program.emit_insn(Insn::Column {
                cursor_id: csr_current,
                column: *column,
                dest: result_reg,
                default: None,
            });
        } else {
            unreachable!(
                "lag/lead arg[2] must be a buffer column reference after planner rewrite, got {:?}",
                args[2]
            );
        }

        // tmp = rowid(csr_current) ± offset.
        let tmp_reg = program.alloc_register();
        program.emit_insn(Insn::RowId {
            cursor_id: csr_current,
            dest: tmp_reg,
        });
        if args.len() < 2 {
            // Default offset = 1.
            program.emit_insn(Insn::AddImm {
                register: tmp_reg,
                value: if is_lead { 1 } else { -1 },
            });
        } else {
            let offset_col = match args[1] {
                Expr::Column { column, .. } => *column,
                other => unreachable!(
                    "lag/lead arg[1] must be a buffer column reference after planner rewrite, got {other:?}"
                ),
            };
            let offset_reg = program.alloc_register();
            program.emit_insn(Insn::Column {
                cursor_id: csr_current,
                column: offset_col,
                dest: offset_reg,
                default: None,
            });
            // Insn::Subtract is `dest = lhs - rhs`; Insn::Add is
            // `dest = lhs + rhs`. We want `tmp = tmp ± offset`, so tmp
            // is the lhs in both cases.
            if is_lead {
                program.emit_insn(Insn::Add {
                    lhs: tmp_reg,
                    rhs: offset_reg,
                    dest: tmp_reg,
                });
            } else {
                program.emit_insn(Insn::Subtract {
                    lhs: tmp_reg,
                    rhs: offset_reg,
                    dest: tmp_reg,
                });
            }
        }

        // SeekRowid on csr_app — on hit, overwrite result with the
        // column from the target row; on miss, fall through to lbl_miss
        // and the default register value (set above) stays put.
        let lbl_miss = program.allocate_label();
        program.emit_insn(Insn::SeekRowid {
            cursor_id: csr_app,
            src_reg: tmp_reg,
            target_pc: lbl_miss,
        });
        program.emit_insn(Insn::Column {
            cursor_id: csr_app,
            column: arg0_col,
            dest: result_reg,
            default: None,
        });
        program.preassign_label_to_next_insn(lbl_miss);
    }

    Ok(())
}

/// Emit the per-output-row lookup for every `first_value` / `nth_value` in
/// the current window: jump straight to the wanted row of the frame and
/// read its value. Mirrors SQLite's first_value / nth_value arms in
/// `windowReturnOneRow` (`window.c:1940-1965`):
///
/// ```text
///   reg_result := NULL
///   tmp := 1                                 (first_value, N=1)
///     -- or --
///   tmp := arg[1] from csr_current           (nth_value, per-row N)
///   MustBeInt tmp; if tmp <= 0: error
///   tmp := tmp + frame_counters[0]           (rows that left the frame)
///   if tmp > frame_counters[1]: goto lbl     (target past frame end → NULL)
///   SeekRowid(csr_app, tmp)
///   reg_result := Column(csr_app, arg[0])
///   lbl:
/// ```
fn emit_first_value_nth_value_lookup(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let window = plan.window.as_ref().expect("missing window");
    let registers = meta.registers;
    let cursors = meta.cursors;
    let acc_result_start = registers.acc_result_start;
    let csr_current = cursors.csr_current;

    for (i, func) in window.functions.iter().enumerate() {
        let is_nth = match &func.func {
            AccumulatorFunc::Window(WindowFunc::FirstValue) => false,
            AccumulatorFunc::Window(WindowFunc::NthValue) => true,
            _ => continue,
        };
        let csr_app = cursors
            .csr_app
            .expect("csr_app must exist when a window contains first_value or nth_value");
        let result_reg = acc_result_start + i;

        let args: Vec<&Expr> = match func.current_expr() {
            Expr::FunctionCall { args, .. } => args.iter().map(|a| a.as_ref()).collect(),
            _ => unreachable!("first_value / nth_value are always Expr::FunctionCall"),
        };
        let arg_value_col = match args.first() {
            Some(Expr::Column { column, .. }) => *column,
            other => unreachable!(
                "first_value/nth_value arg[0] must be a subquery column ref \
                 after planner rewrite, got {other:?}"
            ),
        };

        // Default result: NULL (stays NULL on seek miss or N past frame end).
        program.emit_insn(Insn::Null {
            dest: result_reg,
            dest_end: None,
        });

        // Compute target rowid. target = N + frame_start_rowid - 1.
        // For UB-CR frames frame_start_rowid = 1 → target = N. For
        // ROWS BETWEEN N PRECEDING AND CURRENT ROW it tracks csr_start.
        let target_reg = program.alloc_register();
        if is_nth {
            let n_col = match args.get(1) {
                Some(Expr::Column { column, .. }) => *column,
                other => unreachable!(
                    "nth_value arg[1] must be a subquery column ref \
                     after planner rewrite, got {other:?}"
                ),
            };
            program.emit_insn(Insn::Column {
                cursor_id: csr_current,
                column: n_col,
                dest: target_reg,
                default: None,
            });
            // Validate N: must be a positive integer. Matches SQLite's
            // `windowCheckValue(eCond=2)` at `window.c:1490-1506` — the
            // error message is the verbatim SQLite string.
            let lbl_n_ok = program.allocate_label();
            let lbl_n_err = program.allocate_label();
            program.emit_insn(Insn::MustBeInt {
                reg: target_reg,
                target_pc: Some(lbl_n_err),
            });
            let reg_zero = program.alloc_register();
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: reg_zero,
            });
            program.emit_insn(Insn::Gt {
                lhs: target_reg,
                rhs: reg_zero,
                target_pc: lbl_n_ok,
                flags: crate::vdbe::insn::CmpInsFlags::default(),
                collation: None,
            });
            program.preassign_label_to_next_insn(lbl_n_err);
            program.emit_insn(Insn::Halt {
                err_code: crate::error::SQLITE_ERROR,
                description: "second argument to nth_value must be a positive integer".to_string(),
                on_error: None,
                description_reg: None,
            });
            program.preassign_label_to_next_insn(lbl_n_ok);
        } else {
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: target_reg,
            });
        }

        // Apply the frame-start offset: nth_value counts N from the frame
        // start, whose buffer index is `frame_counters + 1` rows in
        // (`frame_counters` holds the count of rows that have left the
        // frame — 0 for an UNBOUNDED PRECEDING start). Buffer rowids are
        // dense from 1 per partition, so `N + frame_counters` is the
        // target's rowid directly. Mirrors SQLite's
        // `OP_Add tmpReg, regApp` at window.c:1949.
        let frame_counters = registers
            .frame_counters
            .expect("frame_counters must exist when a window contains first_value/nth_value");
        program.emit_insn(Insn::Add {
            lhs: target_reg,
            rhs: frame_counters,
            dest: target_reg,
        });

        // Two distinct "no value" outcomes, kept separate:
        //
        // * target past the frame end — a legitimate NULL (the Nth row lies
        //   outside this row's frame). `frame_counters + 1` is the index of
        //   the last row stepped into the frame, NOT the emitted row's own
        //   rowid: under RANGE the frame runs to the current row's last
        //   peer, so an early row of a peer group must still see its whole
        //   group. Mirrors SQLite's `OP_Gt regApp+1, lbl, tmpReg` at
        //   window.c:1950.
        //
        // * target within the frame but `SeekRowid` misses — impossible:
        //   buffered rows are only ever deleted after leaving the frame
        //   (behind `frame_counters` rows in), and the target lies at or
        //   past the frame start. Treat a miss as an invariant violation
        //   and Halt rather than silently returning NULL. Mirrors SQLite
        //   finding its row via csrApp unconditionally.
        let lbl_past_frame_end = program.allocate_label();
        let lbl_missing_row = program.allocate_label();
        let lbl_done = program.allocate_label();
        program.emit_insn(Insn::Gt {
            lhs: target_reg,
            rhs: frame_counters + 1,
            target_pc: lbl_past_frame_end,
            flags: crate::vdbe::insn::CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::SeekRowid {
            cursor_id: csr_app,
            src_reg: target_reg,
            target_pc: lbl_missing_row,
        });
        program.emit_insn(Insn::Column {
            cursor_id: csr_app,
            column: arg_value_col,
            dest: result_reg,
            default: None,
        });
        program.emit_insn(Insn::Goto {
            target_pc: lbl_done,
        });
        program.preassign_label_to_next_insn(lbl_missing_row);
        program.emit_insn(Insn::Halt {
            err_code: crate::error::SQLITE_ERROR,
            description: "first_value/nth_value could not find a saved row within its frame"
                .to_string(),
            on_error: None,
            description_reg: None,
        });
        // Past frame end falls through here with `result_reg` still NULL.
        program.preassign_label_to_next_insn(lbl_past_frame_end);
        program.preassign_label_to_next_insn(lbl_done);
    }

    Ok(())
}

/// Emit one output row's worth of work: populate `result_columns_start` from
/// `csr_current`, run the positional lookups for functions computed at
/// output time, then Gosub to the shared row-output subroutine. Mirrors
/// SQLite's `windowReturnOneRow` (`window.c:1816-1990`), restricted to the
/// streaming code path.
fn emit_return_one_row(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let labels = meta.labels;
    let registers = meta.registers;
    let cursors = meta.cursors;
    let expressions_referencing_subquery = meta.expressions_referencing_subquery.clone();

    for (i, (_, col_idx)) in expressions_referencing_subquery.iter().enumerate() {
        let reg_result = registers.result_columns_start + i;
        program.emit_column_or_rowid(cursors.csr_current, *col_idx, reg_result);
    }

    let window = plan.window.as_ref().expect("missing window");
    if window.frame.exclude.is_some() {
        emit_window_full_scan(program, t_ctx, plan)?;
    } else {
        // Per-row lookups for functions whose value is computed at output
        // time (first_value / nth_value / lag / lead). Each writes the
        // function's value register before the outer query reads it via
        // `emit_select_result`'s expression-cache lookup.
        emit_first_value_nth_value_lookup(program, t_ctx, plan)?;
        emit_lag_lead_lookup(program, t_ctx, plan)?;
    }

    // The select-result / sorter-insert code is a shared subroutine —
    // RETURN_ROW is emitted at several sites (streaming step + flush
    // loops), and jump targets inside the row-output code (SELECT
    // DISTINCT's on-conflict label in particular) must resolve to exactly
    // one address. Mirrors SQLite's `OP_Gosub regGosub, addrGosub` at the
    // end of `windowReturnOneRow` (window.c:1988).
    program.emit_insn(Insn::Gosub {
        target_pc: labels.row_output,
        return_reg: registers.row_output_return,
    });

    Ok(())
}

/// Emit the single row-output subroutine that every RETURN_ROW site calls:
/// send the filled-in result registers on to the outer query (or into the
/// ORDER BY sorter), applying OFFSET, LIMIT and SELECT DISTINCT
/// de-duplication. This is the window-loop half of what SQLite emits once
/// as the `addrGosub` target in `select.c`.
fn emit_row_output_subroutine(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let labels = meta.labels;
    let registers = meta.registers;

    program.preassign_label_to_next_insn(labels.row_output);

    let label_skip_returning_row = program.allocate_label();
    t_ctx.resolver.enable_expr_to_reg_cache();

    if plan.order_by.is_empty() {
        emit_select_result(
            program,
            &t_ctx.resolver,
            plan,
            Some(labels.window_processing_end),
            Some(label_skip_returning_row),
            t_ctx.reg_nonagg_emit_once_flag,
            t_ctx.reg_offset,
            t_ctx.reg_result_cols_start.unwrap(),
            t_ctx.limit_ctx,
        )?;
    } else {
        EmitOrderBy::sorter_insert(program, t_ctx, plan)?;
    }

    program.preassign_label_to_next_insn(label_skip_returning_row);

    if let Distinctness::Distinct { ctx } = &plan.distinctness {
        let distinct_ctx = ctx.as_ref().expect("distinct context must exist");
        program.preassign_label_to_next_insn(distinct_ctx.label_on_conflict);
    }

    program.emit_insn(Insn::Return {
        return_reg: registers.row_output_return,
        can_fallthrough: false,
    });

    Ok(())
}

/// Emit the flush subroutine for window processing — the code that runs
/// at the end of a partition to finish off any rows the frame end hasn't
/// reached yet and emit every row not yet sent to the outer query.
///
/// This is the Rust port of SQLite's flush block at `window.c:3043-3105`.
/// The loop shape is one of three patterns, chosen by the frame's bounds
/// the same way the main loop chooses. The most common shape
/// (`UNBOUNDED PRECEDING TO CURRENT ROW`, window.c:3085-3094):
///
/// ```text
///   Rewind csr_write → if empty, jump to label_empty
///   emit_window_op AGGSTEP, break_on_eof=None
///       ↑ adds the last buffered row, whose AggStep was left until now
///   addr_loop_start:
///   emit_window_op RETURN_ROW, break_on_eof=label_break
///   emit_window_op AGGINVERSE (no-op for UNBOUNDED start)
///   Goto addr_loop_start
///   label_break:
///   label_empty:
///   ResetSorter csr_current
///   Return flush_buffer_return_offset
/// ```
///
/// Entered two ways, just like SQLite's flush block:
/// - **Fallthrough** (end of source): the inline code immediately preceding
///   sets `flush_buffer_return_offset` to NULL so the `Return` falls through.
/// - **Subroutine** (partition boundary): the main loop body Gosubs to
///   `labels.flush_buffer`; `Return` jumps back to the address stored in
///   `flush_buffer_return_offset`.
pub fn emit_window_flush(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> crate::Result<()> {
    let meta = t_ctx.meta_window.as_ref().expect("missing window metadata");
    let labels = meta.labels;
    let registers = meta.registers;
    let cursors = meta.cursors;

    let label_empty = program.allocate_label();
    let label_break = program.allocate_label();

    // Fallthrough entry: the source loop just ended. Null the return
    // register so the trailing Return falls through past this subroutine.
    program.add_comment(program.offset(), "return remaining buffered rows");
    program.emit_insn(Insn::Null {
        dest: registers.flush_buffer_return_offset,
        dest_end: None,
    });

    // Subroutine entry: partition boundary Gosub lands here.
    program.preassign_label_to_next_insn(labels.flush_buffer);

    // Detect empty partition. Rewind csr_write sets its position to the
    // first row and jumps to label_empty when the table is empty; csr_write
    // isn't used past flush so resetting its position is harmless.
    program.emit_insn(Insn::Rewind {
        cursor_id: cursors.csr_write,
        pc_if_empty: label_empty,
    });

    // The flush uses the same three patterns, chosen by the frame's
    // bounds, as the main loop. Mirrors SQLite's three flush paths at
    // window.c:3052-3094. Each
    // branch opens with a final AGGSTEP: the last row inserted in the main
    // loop was never added to the totals (under ROWS), or its peer group
    // might still have been incomplete (under RANGE / GROUPS).
    let window = plan.window.as_ref().expect("missing window");
    use crate::translate::plan::FrameBoundary;
    if matches!(window.frame.end, FrameBoundary::Preceding(_)) {
        // Pattern B flush (window.c:3052-3056): the main loop emitted a
        // row on every iteration, so exactly one row is still waiting to
        // be output — a single RETURN_ROW with no delay and no loop. The
        // final AGGSTEP keeps its delay count: any rows the frame end
        // never reached must stay out of the totals.
        emit_window_op(
            program,
            t_ctx,
            plan,
            WindowOp::AggStep,
            registers.end_offset_reg,
            None,
            true,
        )?;
        let inverse_before_return = window.frame.mode == turso_parser::ast::FrameMode::Range
            && matches!(window.frame.start, FrameBoundary::Preceding(_));
        if inverse_before_return {
            emit_window_op(
                program,
                t_ctx,
                plan,
                WindowOp::AggInverse,
                registers.start_offset_reg,
                None,
                true,
            )?;
        }
        emit_window_op(program, t_ctx, plan, WindowOp::ReturnRow, None, None, true)?;
    } else if matches!(window.frame.start, FrameBoundary::Following(_)) {
        // Pattern A flush (window.c:3057-3084): two-stage loop.
        //
        //   loop_1:
        //     RETURN_ROW (delayed by IfPos → skips the first few calls),
        //                 break_on_eof = label_break
        //     AGGINVERSE,  break_on_eof = label_after_inverse_eof
        //     Goto loop_1
        //   label_after_inverse_eof:
        //   loop_2:
        //     RETURN_ROW,  break_on_eof = label_break
        //     Goto loop_2
        //   label_break:
        //
        // The stage-one delays depend on the frame end
        // (window.c:3068-3077). With an UNBOUNDED FOLLOWING end the
        // RETURN_ROW delay is the start offset — it holds back the first
        // few emits so rows drop off the start (AGGINVERSE) before their
        // result is read (cume_dist's `1 FOLLOWING` start), while
        // AGGINVERSE itself runs with no delay. With a bounded `M
        // FOLLOWING` end the RETURN_ROW delay continues the end offset and
        // AGGINVERSE continues the (rewritten, M - N) start offset — both
        // pick up whatever the main loop left unfinished. Once AGGINVERSE
        // runs out of rows (csr_start hits EOF), the second loop emits the
        // rows that remain, with the totals now final.
        emit_window_op(program, t_ctx, plan, WindowOp::AggStep, None, None, true)?;
        let (return_row_countdown, agg_inverse_countdown) =
            if window.frame.mode == turso_parser::ast::FrameMode::Range {
                (None, registers.start_offset_reg)
            } else if matches!(window.frame.end, FrameBoundary::UnboundedFollowing) {
                (registers.start_offset_reg, None)
            } else {
                (registers.end_offset_reg, registers.start_offset_reg)
            };
        let label_after_inverse_eof = program.allocate_label();
        let label_loop_1 = program.allocate_label();
        program.preassign_label_to_next_insn(label_loop_1);
        if window.frame.mode == turso_parser::ast::FrameMode::Range {
            emit_window_op(
                program,
                t_ctx,
                plan,
                WindowOp::AggInverse,
                agg_inverse_countdown,
                Some(label_after_inverse_eof),
                true,
            )?;
            emit_window_op(
                program,
                t_ctx,
                plan,
                WindowOp::ReturnRow,
                return_row_countdown,
                Some(label_break),
                true,
            )?;
        } else {
            emit_window_op(
                program,
                t_ctx,
                plan,
                WindowOp::ReturnRow,
                return_row_countdown,
                Some(label_break),
                true,
            )?;
            emit_window_op(
                program,
                t_ctx,
                plan,
                WindowOp::AggInverse,
                agg_inverse_countdown,
                Some(label_after_inverse_eof),
                true,
            )?;
        }
        program.emit_insn(Insn::Goto {
            target_pc: label_loop_1,
        });

        program.preassign_label_to_next_insn(label_after_inverse_eof);
        let label_loop_2 = program.allocate_label();
        program.preassign_label_to_next_insn(label_loop_2);
        emit_window_op(
            program,
            t_ctx,
            plan,
            WindowOp::ReturnRow,
            None,
            Some(label_break),
            true,
        )?;
        program.emit_insn(Insn::Goto {
            target_pc: label_loop_2,
        });
    } else {
        // Pattern C flush (window.c:3085-3094): paired RETURN_ROW +
        // AGGINVERSE in a single loop.
        //
        //   loop:
        //     RETURN_ROW (break_on_eof = label_break)
        //     AGGINVERSE (countdown = start_offset_reg)
        //     Goto loop
        //
        // One branch serves every non-FOLLOWING start, exactly as
        // SQLite's else-arm does: for UNBOUNDED PRECEDING the AGGINVERSE
        // does nothing (window.c:2252-2257); for CURRENT ROW there is no
        // delay count and csr_start moves forward one row for each row
        // emitted; for `N PRECEDING` the delay count lets the frame grow
        // to N+1 rows before csr_start starts moving.
        emit_window_op(program, t_ctx, plan, WindowOp::AggStep, None, None, true)?;
        let label_loop_start = program.allocate_label();
        program.preassign_label_to_next_insn(label_loop_start);
        emit_window_op(
            program,
            t_ctx,
            plan,
            WindowOp::ReturnRow,
            None,
            Some(label_break),
            true,
        )?;
        emit_window_op(
            program,
            t_ctx,
            plan,
            WindowOp::AggInverse,
            registers.start_offset_reg,
            None,
            true,
        )?;
        program.emit_insn(Insn::Goto {
            target_pc: label_loop_start,
        });
    }

    program.preassign_label_to_next_insn(label_break);
    program.preassign_label_to_next_insn(label_empty);

    program.emit_insn(Insn::ResetSorter {
        cursor_id: cursors.csr_current,
    });
    if let Some(frame_start_rowid) = registers.frame_start_rowid {
        let frame_end_rowid = registers
            .frame_end_rowid
            .expect("EXCLUDE frame rowid trackers must be allocated as a pair");
        program.emit_insn(Insn::Integer {
            value: 1,
            dest: frame_start_rowid,
        });
        program.emit_insn(Insn::Integer {
            value: 0,
            dest: frame_end_rowid,
        });
    }
    program.emit_insn(Insn::Return {
        return_reg: registers.flush_buffer_return_offset,
        can_fallthrough: true,
    });

    // The fallthrough entry (end of source) continues past the Return;
    // jump over the row-output subroutine body that follows.
    program.emit_insn(Insn::Goto {
        target_pc: labels.window_processing_end,
    });

    emit_row_output_subroutine(program, t_ctx, plan)?;

    program.preassign_label_to_next_insn(labels.window_processing_end);

    Ok(())
}
