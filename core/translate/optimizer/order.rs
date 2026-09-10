use crate::schema::{impl_effective_nulls_order, Table};
use crate::turso_assert_greater_than_or_equal;
use crate::{
    schema::{FromClauseSubquery, Index, Schema},
    translate::{
        collate::{get_collseq_from_expr, CollationSeq},
        expression_index::normalize_expr_for_index_matching,
        optimizer::access_method::AccessMethodParams,
        optimizer::constraints::{
            usable_constraints_for_lhs_mask, RangeConstraintRef, TableConstraints,
        },
        plan::{
            GroupBy, HashJoinType, IterationDirection, JoinedTable, Operation, Plan, Scan,
            SimpleAggregate, TableReferences,
        },
        planner::{table_mask_from_expr, TableMask},
    },
    util::exprs_are_equivalent,
};
use turso_parser::ast::{self, SortOrder, TableInternalId};

use super::{
    access_method::AccessMethod,
    cost::{is_unique_point_lookup, IndexInfo},
    join::JoinN,
};

/// Target component in an ORDER BY/GROUP BY that may be a plain column or an expression.
#[derive(Debug, PartialEq, Clone)]
pub enum ColumnTarget {
    Column(usize),
    RowId,
    /// We know that the ast lives at least as long as the Statement/Program,
    /// so we store a raw pointer here to avoid cloning yet another ast::Expr
    Expr(*const ast::Expr),
}

/// A convenience struct for representing a (table_no, column_target, [SortOrder]) tuple.
#[derive(Debug, PartialEq, Clone)]
pub struct ColumnOrder {
    pub table_id: TableInternalId,
    pub target: ColumnTarget,
    pub order: SortOrder,
    pub collation: CollationSeq,
    pub nulls_order: Option<ast::NullsOrder>,
}

impl_effective_nulls_order!(ColumnOrder);

#[derive(Debug, PartialEq, Clone)]
/// If an [OrderTarget] is satisfied, then [EliminatesSort] describes which part
/// of the query no longer requires sorting.
pub enum EliminatesSortBy {
    Group,
    Order,
    GroupByAndOrder,
}

#[derive(Debug, PartialEq, Clone)]
pub enum OrderTargetPurpose {
    /// Matching this target lets the planner eliminate a later ORDER BY and/or
    /// GROUP BY sort step.
    EliminatesSort(EliminatesSortBy),
    /// Matching this target enables an extremum fast path, analogous to
    /// SQLite's WHERE_ORDERBY_MIN/MAX planning mode.
    Extremum,
}

#[derive(Debug, PartialEq, Clone)]
/// An [OrderTarget] is considered in join optimization and index selection,
/// so that if a given join ordering and its access methods satisfy the [OrderTarget],
/// then the join ordering and its access methods are preferred, all other things being equal.
pub struct OrderTarget {
    pub columns: Vec<ColumnOrder>,
    pub purpose: OrderTargetPurpose,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EqualityPrefixScope {
    /// Candidate scoring may skip any equality-constrained seek prefix because
    /// it only reasons about the order within that specific seek.
    AnyEquality,
    /// Final ORDER BY / GROUP BY elimination may only skip globally constant
    /// equality prefixes. Join-dependent equalities vary per outer row and do
    /// not guarantee a globally ordered concatenation of inner scans.
    ConstantEquality,
}

impl OrderTarget {
    /// Build an `OrderTarget` from a list of expressions if they can all be
    /// satisfied by a single-table ordering (needed for index satisfaction).
    fn maybe_from_iterator<'a>(
        list: impl Iterator<Item = (&'a ast::Expr, SortOrder, Option<ast::NullsOrder>)> + Clone,
        tables: &crate::translate::plan::TableReferences,
        purpose: OrderTargetPurpose,
    ) -> Option<Self> {
        if list.clone().count() == 0 {
            return None;
        }
        let mut cols = Vec::new();
        for (expr, order, nulls) in list {
            let col = expr_to_column_order(expr, order, nulls, tables)?;
            cols.push(col);
        }
        Some(OrderTarget {
            columns: cols,
            purpose,
        })
    }

    pub fn is_extremum(&self) -> bool {
        matches!(self.purpose, OrderTargetPurpose::Extremum)
    }
}

/// Build the synthetic ordering requirement used by simple MIN/MAX aggregation.
pub fn simple_aggregate_order_target(
    simple_aggregate: &SimpleAggregate,
    tables: &TableReferences,
) -> Option<OrderTarget> {
    let SimpleAggregate::MinMax(min_max) = simple_aggregate else {
        return None;
    };

    let mut target = OrderTarget::maybe_from_iterator(
        std::iter::once((&min_max.argument, min_max.order, None)),
        tables,
        OrderTargetPurpose::Extremum,
    )?;
    if let Some(coll) = min_max.collation {
        target.columns[0].collation = coll;
    }
    Some(target)
}

/// Compute an [OrderTarget] for the join optimizer to use.
/// Ideally, a join order is both efficient in joining the tables
/// but also returns the results in an order that minimizes the amount of
/// sorting that needs to be done later (either in GROUP BY, ORDER BY, or both).
///
/// TODO: this does not currently handle the case where we definitely cannot eliminate
/// the ORDER BY sorter, but we could still eliminate the GROUP BY sorter.
pub fn compute_order_target(
    order_by: &mut Vec<(Box<ast::Expr>, SortOrder, Option<ast::NullsOrder>)>,
    group_by_opt: Option<&mut GroupBy>,
    tables: &TableReferences,
) -> Option<OrderTarget> {
    match (order_by.is_empty(), group_by_opt) {
        // No ordering demands - we don't care what order the joined result rows are in
        (true, None) => None,
        // Only ORDER BY - we would like the joined result rows to be in the order specified by the ORDER BY
        (false, None) => OrderTarget::maybe_from_iterator(
            order_by
                .iter()
                .map(|(expr, order, nulls)| (expr.as_ref(), *order, *nulls)),
            tables,
            OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Order),
        ),
        // Only GROUP BY - we would like the joined result rows to be in the order specified by the GROUP BY
        (true, Some(group_by)) => OrderTarget::maybe_from_iterator(
            group_by
                .exprs
                .iter()
                .map(|expr| (expr, SortOrder::Asc, None)),
            tables,
            OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Group),
        ),
        // Both ORDER BY and GROUP BY:
        // If the GROUP BY does not contain all the expressions in the ORDER BY,
        // then we must separately sort the result rows for ORDER BY anyway.
        // However, in that case we can use the GROUP BY expressions as the target order for the join,
        // so that we don't have to sort twice.
        //
        // If the GROUP BY contains all the expressions in the ORDER BY,
        // then we again can use the GROUP BY expressions as the target order for the join;
        // however in this case we must take the ASC/DESC from ORDER BY into account.
        (false, Some(group_by)) => {
            // Does the group by contain all expressions in the order by?
            let group_by_contains_all = order_by.iter().all(|(expr, _, _)| {
                group_by
                    .exprs
                    .iter()
                    .any(|group_by_expr| exprs_are_equivalent(expr, group_by_expr))
            });
            // If not, let's try to target an ordering that matches the group by -- we don't care about ASC/DESC
            if !group_by_contains_all {
                return OrderTarget::maybe_from_iterator(
                    group_by
                        .exprs
                        .iter()
                        .map(|expr| (expr, SortOrder::Asc, None)),
                    tables,
                    OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Group),
                );
            }
            // If yes, let's try to target an ordering that matches the GROUP BY columns,
            // but the ORDER BY orderings. First, we need to reorder the GROUP BY columns to match the ORDER BY columns.
            group_by.exprs.sort_by_key(|expr| {
                order_by
                    .iter()
                    .position(|(order_by_expr, _, _)| exprs_are_equivalent(expr, order_by_expr))
                    .map_or(usize::MAX, |i| i)
            });

            // Now, regardless of whether we can eventually eliminate the sorting entirely in the optimizer,
            // we know that we don't need ORDER BY sorting anyway, because the GROUP BY will sort the result since
            // it contains all the necessary columns required for the ORDER BY, and the GROUP BY columns are now in the correct order.
            // First, however, we need to make sure the GROUP BY sorter's column sort directions and NULLS
            // ordering match the ORDER BY requirements.
            turso_assert_greater_than_or_equal!(group_by.exprs.len(), order_by.len());
            for (i, (_, order_by_dir, order_by_nulls)) in order_by.iter().enumerate() {
                group_by.sort_order[i] = *order_by_dir;
                group_by.nulls_order[i] = *order_by_nulls;
            }
            // The sort_by_key above reordered group_by.exprs but not sort_order,
            // so remaining positions may have stale values. GROUP BY columns not
            // in ORDER BY should default to ASC (matching SQLite's tie-breaking).
            for i in order_by.len()..group_by.sort_order.len() {
                group_by.sort_order[i] = SortOrder::Asc;
                group_by.nulls_order[i] = None;
            }
            // Now we can remove the ORDER BY from the query.
            order_by.clear();

            OrderTarget::maybe_from_iterator(
                group_by
                    .exprs
                    .iter()
                    .zip(group_by.sort_order.iter())
                    .zip(group_by.nulls_order.iter())
                    .map(|((expr, dir), nulls)| (expr, *dir, *nulls)),
                tables,
                OrderTargetPurpose::EliminatesSort(EliminatesSortBy::GroupByAndOrder),
            )
        }
    }
}

/// Check if the plan's row iteration order matches the [OrderTarget]'s column order.
/// If yes, and this plan is selected, then a sort operation can be eliminated.
pub fn plan_satisfies_order_target(
    plan: &JoinN,
    access_methods_arena: &[AccessMethod],
    joined_tables: &[JoinedTable],
    table_constraints: &[TableConstraints],
    order_target: &OrderTarget,
    schema: &Schema,
) -> bool {
    let mut hash_join_build_tables = TableMask::default();
    for (_, access_method_index) in plan.data.iter() {
        let access_method = &access_methods_arena[*access_method_index];
        if let AccessMethodParams::HashJoin {
            build_table_idx,
            join_type,
            materialize_build_input,
            ..
        } = &access_method.params
        {
            hash_join_build_tables
                .set(*build_table_idx)
                .expect("a plan cannot contain more than the table limit");
            // We bail out early because as soon as there's a hash join that materializes its
            // build side, we can't rely on any of the relations to the left of its probe table
            // in the join order. This is because translation retransforms the join order later on
            // (see `prune_join_order_for_materialized_inputs`)
            // Eventually, we could narrow down this check to consider the ordering of the relations
            // at, or to the right of, the rightmost probe table, but for now this'll do.
            if *materialize_build_input {
                return false;
            }
            // Outer hash joins emit unmatched rows in hash-bucket order, not scan order.
            if matches!(join_type, HashJoinType::LeftOuter | HashJoinType::FullOuter) {
                return false;
            }
        }
    }

    let mut target_col_idx = 0;
    let mut prior_tables = TableMask::default();
    let num_cols_in_order_target = order_target.columns.len();
    for (loop_pos, (table_index, access_method_index)) in plan.data.iter().enumerate() {
        let access_method = &access_methods_arena[*access_method_index];
        let table_ref = &joined_tables[*table_index];

        // A hash build is consumed into an unordered hash table and omitted
        // from the execution loop order. Its scan order cannot order output.
        if hash_join_build_tables.get(*table_index) {
            return false;
        }

        // Outer joins can emit an extra row with NULLs on the right-hand side
        // when no match is found. Because that row is produced after the scan or
        // seek, we cannot rely on the right-hand table's access order to satisfy
        // ORDER BY / GROUP BY terms that reference that table.
        if table_ref
            .join_info
            .as_ref()
            .is_some_and(|join_info| join_info.is_outer())
            && order_target.columns[target_col_idx..]
                .iter()
                .any(|target_col| target_col.table_id == table_ref.internal_id)
        {
            return false;
        }

        // Check if this table has an access method that provides the right ordering.
        let consumed = match &access_method.params {
            AccessMethodParams::BTreeTable {
                iter_dir,
                index: index_opt,
                build_index,
                constraint_refs,
            } => {
                if *build_index {
                    let constraint_refs = usable_constraints_for_lhs_mask(
                        &table_constraints[*table_index].constraints,
                        &table_constraints[*table_index].temporary_index_terms,
                        &prior_tables,
                        *table_index,
                    );
                    temporary_index_order_consumed(
                        table_ref,
                        *iter_dir,
                        &constraint_refs,
                        order_target,
                        target_col_idx,
                        schema,
                    )
                } else {
                    btree_access_order_consumed(
                        table_ref,
                        *iter_dir,
                        index_opt.as_deref(),
                        constraint_refs,
                        order_target,
                        target_col_idx,
                        schema,
                        EqualityPrefixScope::ConstantEquality,
                    )
                }
            }
            AccessMethodParams::MaterializedSubquery {
                index,
                constraint_refs,
                iter_dir,
            } => btree_access_order_consumed(
                table_ref,
                *iter_dir,
                Some(index.as_ref()),
                constraint_refs,
                order_target,
                target_col_idx,
                schema,
                EqualityPrefixScope::ConstantEquality,
            ),
            AccessMethodParams::Subquery { iter_dir } => {
                let Table::FromClauseSubquery(from_clause_subquery) = &table_ref.table else {
                    unreachable!(
                        "access_method.params::Subquery must be for a FromClauseSubquery table"
                    );
                };
                OrderConsumption {
                    consumed: subquery_intrinsic_order_consumed(
                        table_ref.internal_id,
                        from_clause_subquery,
                        *iter_dir,
                        &order_target.columns[target_col_idx..],
                        schema,
                    ),
                    includes_rowid: false,
                }
            }
            _ => return false,
        };

        if consumed.consumed == 0 {
            return false;
        }
        target_col_idx += consumed.consumed;
        if target_col_idx == num_cols_in_order_target {
            return true;
        }

        // The next ORDER BY column can only come from a deeper loop if the rows
        // output by this loop are unique for the columns so far. If they're not unique,
        // the inner loop would repeat the same values for each duplicate, resulting in
        // an output like `A B C ... A B C ...` instead of the correct fully sorted order `A A B B ...`.
        let next_term_comes_from_later_loop =
            order_target
                .columns
                .get(target_col_idx)
                .is_some_and(|target_col| {
                    plan.data[loop_pos + 1..]
                        .iter()
                        .any(|(later_table_index, _)| {
                            joined_tables[*later_table_index].internal_id == target_col.table_id
                        })
                });
        if next_term_comes_from_later_loop
            && !access_method_emits_unique_order_prefix(access_method, &consumed)
        {
            return false;
        }
        prior_tables
            .set(*table_index)
            .expect("a plan cannot contain more than the table limit");
    }
    target_col_idx == num_cols_in_order_target
}

fn access_method_emits_unique_order_prefix(
    access_method: &AccessMethod,
    order_consumption: &OrderConsumption,
) -> bool {
    // Rows always differ in rowid, so a consumed prefix that includes the
    // rowid never repeats.
    if order_consumption.includes_rowid {
        return true;
    }
    // Otherwise the only safe claim is a point lookup: a UNIQUE index with
    // every column pinned by plain `=` returns at most one row. Anything
    // weaker can emit duplicate prefixes: an `IS` equality matches NULL keys
    // (a UNIQUE index stores any number of NULL keys), and counting
    // equality-pinned columns together with consumed ORDER BY terms counts
    // the same column twice when the ORDER BY mentions the equality column.
    match &access_method.params {
        AccessMethodParams::BTreeTable {
            index,
            build_index,
            constraint_refs,
            ..
        } => {
            !*build_index
                && is_unique_point_lookup(index_info_for_access(index.as_deref()), constraint_refs)
        }
        AccessMethodParams::MaterializedSubquery {
            index,
            constraint_refs,
            ..
        } => is_unique_point_lookup(index_info_for_access(Some(index.as_ref())), constraint_refs),
        AccessMethodParams::Subquery { .. }
        | AccessMethodParams::RecursiveCteInput
        | AccessMethodParams::HashJoin { .. }
        | AccessMethodParams::VirtualTable { .. }
        | AccessMethodParams::IndexMethod { .. }
        | AccessMethodParams::MultiIndexScan { .. }
        | AccessMethodParams::InSeek { .. } => false,
    }
}

fn index_info_for_access(index: Option<&Index>) -> IndexInfo {
    match index {
        Some(index) => IndexInfo {
            unique: index.unique,
            column_count: index.columns.len(),
            covering: false,
            rows_per_leaf_page: 0.0, // unused here — only unique/column_count matter
        },
        None => IndexInfo {
            unique: true,
            column_count: 1,
            covering: false,
            rows_per_leaf_page: 0.0, // unused here — only unique/column_count matter
        },
    }
}

/// Return how many leading target columns a FROM-subquery can provide from its
/// own output order, without fabricating an extra probe index.
///
/// We recognize three sources of intrinsic order:
/// 1. An explicit final `ORDER BY` on the subquery.
/// 2. GROUP BY keys (we always use a sorter, never hashing - FOR NOW).
/// 3. A simple single-source finalized scan whose output order is already known.
pub fn subquery_intrinsic_order_consumed(
    table_id: TableInternalId,
    subquery: &FromClauseSubquery,
    iter_dir: IterationDirection,
    target: &[ColumnOrder],
    schema: &Schema,
) -> usize {
    let Plan::Select(select_plan) = subquery.plan.as_ref() else {
        // Don't consider sort elision for compound selects
        return 0;
    };
    // Explicit ORDER BY takes priority.
    if !select_plan.order_by.is_empty() {
        let intrinsic = build_intrinsic_order(
            table_id,
            select_plan,
            select_plan
                .order_by
                .iter()
                .map(|(expr, order, nulls)| (expr.as_ref(), *order, *nulls)),
        );
        return match_intrinsic_order(&intrinsic, iter_dir, target);
    }
    // When ORDER BY was merged into GROUP BY and cleared, the GROUP BY
    // sort_order still describes the output row order.
    if let Some(group_by) = &select_plan.group_by {
        let intrinsic = build_intrinsic_order(
            table_id,
            select_plan,
            group_by
                .exprs
                .iter()
                .zip(group_by.sort_order.iter().copied())
                .zip(group_by.nulls_order.iter().copied())
                .map(|((expr, order), nulls)| (expr, order, nulls)),
        );
        let consumed = match_intrinsic_order(&intrinsic, iter_dir, target);
        if consumed > 0 {
            return consumed;
        }
    }
    finalized_scan_subquery_order_consumed(table_id, select_plan, iter_dir, target, schema)
}

/// Build a `ColumnOrder` list from expressions and sort directions by mapping
/// each expression to a result column position.
fn build_intrinsic_order(
    table_id: TableInternalId,
    select_plan: &crate::translate::plan::SelectPlan,
    exprs: impl Iterator<
        Item = (
            impl std::borrow::Borrow<ast::Expr>,
            SortOrder,
            Option<ast::NullsOrder>,
        ),
    >,
) -> Vec<ColumnOrder> {
    let mut intrinsic = Vec::new();
    for (expr, order, nulls) in exprs {
        let expr = expr.borrow();
        let Some((col_idx, result_col)) = select_plan
            .result_columns
            .iter()
            .enumerate()
            .find(|(_, result_col)| exprs_are_equivalent(expr, &result_col.expr))
        else {
            break;
        };
        let Ok(collation) = get_collseq_from_expr(expr, &select_plan.table_references) else {
            break;
        };
        intrinsic.push(ColumnOrder {
            table_id,
            target: ColumnTarget::Column(col_idx),
            order,
            collation: collation.unwrap_or_else(|| {
                get_collseq_from_expr(&result_col.expr, &select_plan.table_references)
                    .ok()
                    .flatten()
                    .unwrap_or_default()
            }),
            nulls_order: nulls,
        });
    }
    intrinsic
}

/// Compare a subquery's intrinsic column order against an outer order target,
/// accounting for iteration direction. Returns how many leading target columns
/// are satisfied.
fn match_intrinsic_order(
    intrinsic: &[ColumnOrder],
    iter_dir: IterationDirection,
    target: &[ColumnOrder],
) -> usize {
    let target_len = target.len().min(intrinsic.len());
    for (intrinsic_col, target_col) in intrinsic.iter().zip(target.iter()).take(target_len) {
        if intrinsic_col.table_id != target_col.table_id
            || intrinsic_col.target != target_col.target
            || intrinsic_col.collation != target_col.collation
        {
            return 0;
        }
        let expected_order = match iter_dir {
            IterationDirection::Forwards => intrinsic_col.order,
            IterationDirection::Backwards => match intrinsic_col.order {
                SortOrder::Asc => SortOrder::Desc,
                SortOrder::Desc => SortOrder::Asc,
            },
        };
        if expected_order != target_col.order {
            return 0;
        }
        // If the target requests an explicit NULLS ordering, the intrinsic
        // order must provide the same.
        let requested_nulls = target_col.effective_nulls_order();
        let intrinsic_nulls = intrinsic_col.effective_nulls_order_when_iterated(iter_dir);
        if requested_nulls != intrinsic_nulls {
            return 0;
        }
    }
    target_len
}

/// Derive subquery output order from the finalized inner scan when there is no
/// explicit `ORDER BY`.
///
/// This intentionally starts narrow: single-source, non-aggregate,
/// non-window, non-distinct SELECTs only. Those are the cases where insertion
/// order into the materialized table is just the underlying scan order.
fn finalized_scan_subquery_order_consumed(
    table_id: TableInternalId,
    select_plan: &crate::translate::plan::SelectPlan,
    iter_dir: IterationDirection,
    target: &[ColumnOrder],
    schema: &Schema,
) -> usize {
    if select_plan.group_by.is_some()
        || !select_plan.aggregates.is_empty()
        || select_plan.limit.is_some()
        || select_plan.offset.is_some()
        || select_plan.window.is_some()
        || select_plan.distinctness.is_distinct()
        || !select_plan.values.is_empty()
        || select_plan.join_order.len() != 1
        || select_plan.joined_tables().len() != 1
    {
        return 0;
    }

    let joined_table = &select_plan.joined_tables()[select_plan.join_order[0].original_idx];

    // Extract inner iteration direction from the scan operation.
    let inner_iter_dir = match &joined_table.op {
        Operation::Scan(Scan::BTreeTable { iter_dir, .. })
        | Operation::Scan(Scan::Subquery { iter_dir }) => *iter_dir,
        _ => return 0,
    };

    // The outer scan direction composes with the direction used to populate the
    // materialized table. Reversing a backwards-populated table restores the
    // original key order.
    let effective_iter_dir = match (inner_iter_dir, iter_dir) {
        (IterationDirection::Forwards, IterationDirection::Forwards)
        | (IterationDirection::Backwards, IterationDirection::Backwards) => {
            IterationDirection::Forwards
        }
        (IterationDirection::Forwards, IterationDirection::Backwards)
        | (IterationDirection::Backwards, IterationDirection::Forwards) => {
            IterationDirection::Backwards
        }
    };

    // Map outer target columns to inner scan columns through result column expressions.
    let mut mapped_target = Vec::with_capacity(target.len());
    for target_col in target {
        if target_col.table_id != table_id {
            return 0;
        }
        let ColumnTarget::Column(result_col_idx) = target_col.target else {
            return 0;
        };
        let Some(result_col) = select_plan.result_columns.get(result_col_idx) else {
            return 0;
        };
        // The outer query sees result columns of the materialized subquery, but
        // the ordering proof has to be checked against the inner scan columns.
        let Some(mut inner_target_col) = expr_to_column_order(
            &result_col.expr,
            target_col.order,
            target_col.nulls_order,
            &select_plan.table_references,
        ) else {
            return 0;
        };
        if inner_target_col.table_id != joined_table.internal_id
            || inner_target_col.collation != target_col.collation
        {
            return 0;
        }
        inner_target_col.order = target_col.order;
        mapped_target.push(inner_target_col);
    }

    match &joined_table.op {
        Operation::Scan(Scan::BTreeTable { index, .. }) => {
            btree_access_order_consumed(
                joined_table,
                effective_iter_dir,
                index.as_deref(),
                &[],
                &OrderTarget {
                    columns: mapped_target,
                    purpose: OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Order),
                },
                0,
                schema,
                EqualityPrefixScope::ConstantEquality,
            )
            .consumed
        }
        Operation::Scan(Scan::Subquery { .. }) => {
            let Table::FromClauseSubquery(from_clause_subquery) = &joined_table.table else {
                return 0;
            };
            subquery_intrinsic_order_consumed(
                joined_table.internal_id,
                from_clause_subquery,
                effective_iter_dir,
                &mapped_target,
                schema,
            )
        }
        _ => 0,
    }
}

fn expr_to_column_order(
    expr: &ast::Expr,
    order: SortOrder,
    nulls_order: Option<ast::NullsOrder>,
    tables: &TableReferences,
) -> Option<ColumnOrder> {
    match expr {
        ast::Expr::Column {
            table: table_id,
            column,
            ..
        } => {
            let table = tables.find_joined_table_by_internal_id(*table_id)?;
            let col = table.columns().get(*column)?;
            return Some(ColumnOrder {
                table_id: *table_id,
                target: ColumnTarget::Column(*column),
                order,
                collation: col.collation(),
                nulls_order,
            });
        }
        ast::Expr::Collate(expr, collation) => {
            if let ast::Expr::Column {
                table: table_id,
                column,
                ..
            } = expr.as_ref()
            {
                let collation = CollationSeq::new(collation.as_str()).unwrap_or_default();
                return Some(ColumnOrder {
                    table_id: *table_id,
                    target: ColumnTarget::Column(*column),
                    order,
                    collation,
                    nulls_order,
                });
            };
        }
        ast::Expr::RowId { table, .. } => {
            return Some(ColumnOrder {
                table_id: *table,
                target: ColumnTarget::RowId,
                order,
                collation: CollationSeq::default(),
                nulls_order,
            });
        }
        _ => {}
    }
    let mask = table_mask_from_expr(expr, tables, &[]).ok()?;
    if mask.count() != 1 {
        return None;
    }
    let collation = get_collseq_from_expr(expr, tables)
        .ok()?
        .unwrap_or_default();
    let table_no = tables
        .joined_tables()
        .iter()
        .enumerate()
        .find_map(|(i, _)| mask.get(i).then_some(i))?;
    let table_id = tables.joined_tables()[table_no].internal_id;
    Some(ColumnOrder {
        table_id,
        target: ColumnTarget::Expr(expr as *const ast::Expr),
        order,
        collation,
        nulls_order,
    })
}

/// The index column values needed to check its row order.
#[derive(Clone, Copy)]
struct IndexOrderColumn<'a> {
    pos_in_table: usize,
    order: SortOrder,
    nulls_order: Option<ast::NullsOrder>,
    collation: Option<CollationSeq>,
    expr: Option<&'a ast::Expr>,
}

impl IndexOrderColumn<'_> {
    /// Return the NULL order for this scan direction.
    fn nulls_order(self, iter_dir: IterationDirection) -> ast::NullsOrder {
        let nulls_order = self
            .nulls_order
            .unwrap_or_else(|| ast::NullsOrder::default_for(self.order));
        match iter_dir {
            IterationDirection::Forwards => nulls_order,
            IterationDirection::Backwards => nulls_order.reverse(),
        }
    }
}

/// Return true when an order term names this index column.
fn target_matches_order_column(
    target_col: &ColumnOrder,
    idx_col: IndexOrderColumn<'_>,
    table_ref: &JoinedTable,
) -> bool {
    if target_col.table_id != table_ref.internal_id {
        return false;
    }
    match (&target_col.target, idx_col.expr) {
        (ColumnTarget::Column(col_no), _) => idx_col.pos_in_table == *col_no,
        (ColumnTarget::Expr(expr), Some(idx_expr)) => {
            let target_expr = unsafe { &**expr };
            if exprs_are_equivalent(target_expr, idx_expr) {
                return true;
            }
            // Expression indexes are compared against the normalized form that
            // was stored in the schema. A query may write the same expression in
            // a slightly different but equivalent way, so normalize before the
            // final comparison.
            let refs = TableReferences::new(vec![table_ref.clone()], Vec::new());
            let normalized = normalize_expr_for_index_matching(target_expr, table_ref, &refs);
            exprs_are_equivalent(&normalized, idx_expr)
        }
        _ => false,
    }
}

/// What a single-table btree access path contributes to an ORDER BY / GROUP BY
/// target. See [`btree_access_order_consumed`].
pub(super) struct OrderConsumption {
    /// How many leading order-target columns the access path satisfies.
    pub consumed: usize,
    /// Whether one of those consumed columns is the table's rowid (directly,
    /// through a rowid alias, or through the implicit rowid suffix of a
    /// secondary index). Every row has a different rowid, so a consumed prefix
    /// that includes the rowid never repeats across the rows this loop emits.
    pub includes_rowid: bool,
}

impl OrderConsumption {
    pub const NONE: OrderConsumption = OrderConsumption {
        consumed: 0,
        includes_rowid: false,
    };
}

/// Return how many leading `order_target` columns this single-table btree
/// access path can satisfy.
///
/// This is shared by both candidate scoring and final ORDER BY / GROUP BY
/// elimination so they use the same column-matching, collation, custom-type,
/// and hidden-rowid-suffix rules. The caller supplies
/// [`EqualityPrefixScope`] because candidate scoring may skip any equality
/// prefix in the chosen seek key, while final global ordering proof may only
/// skip prefixes that are constant across all output rows.
#[allow(clippy::too_many_arguments)]
pub(super) fn btree_access_order_consumed(
    table_ref: &JoinedTable,
    iter_dir: IterationDirection,
    index: Option<&Index>,
    constraint_refs: &[RangeConstraintRef],
    order_target: &OrderTarget,
    start_col: usize,
    schema: &Schema,
    equality_prefix_scope: EqualityPrefixScope,
) -> OrderConsumption {
    let target_columns = &order_target.columns[start_col..];
    let Some(first_target_col) = target_columns.first() else {
        return OrderConsumption::NONE;
    };

    let rowid_alias_col = table_ref
        .table
        .columns()
        .iter()
        .position(|c| c.is_rowid_alias());

    match index {
        None => {
            // Without an index, only rowid order is available.
            if first_target_col.table_id != table_ref.internal_id {
                return OrderConsumption::NONE;
            }
            match first_target_col.target {
                ColumnTarget::RowId => {}
                ColumnTarget::Column(col_no) => {
                    let Some(rowid_alias_col) = rowid_alias_col else {
                        return OrderConsumption::NONE;
                    };
                    if col_no != rowid_alias_col {
                        return OrderConsumption::NONE;
                    }
                }
                ColumnTarget::Expr(_) => return OrderConsumption::NONE,
            }
            let correct_order = if iter_dir == IterationDirection::Forwards {
                first_target_col.order == SortOrder::Asc
            } else {
                first_target_col.order == SortOrder::Desc
            };
            OrderConsumption {
                consumed: usize::from(correct_order),
                includes_rowid: correct_order,
            }
        }
        Some(index) => index_columns_order_consumed(
            table_ref,
            iter_dir,
            constraint_refs,
            order_target,
            target_columns,
            schema,
            equality_prefix_scope,
            index.columns.iter().map(|column| IndexOrderColumn {
                pos_in_table: column.pos_in_table,
                order: column.order,
                nulls_order: column.nulls_order,
                collation: column.collation,
                expr: column.expr.as_deref(),
            }),
            index.columns.len(),
            index.has_rowid,
            rowid_alias_col,
        ),
    }
}

/// Check the order supplied by a temporary index without building its columns.
fn temporary_index_order_consumed(
    table_ref: &JoinedTable,
    iter_dir: IterationDirection,
    constraint_refs: &[RangeConstraintRef],
    order_target: &OrderTarget,
    start_col: usize,
    schema: &Schema,
) -> OrderConsumption {
    let target_columns = &order_target.columns[start_col..];
    if target_columns.is_empty() {
        return OrderConsumption::NONE;
    }
    let columns = table_ref.columns();
    let column_count = columns
        .iter()
        .enumerate()
        .filter(|(column_idx, _)| table_ref.column_is_used(*column_idx))
        .count();
    let rowid_alias_col = columns.iter().position(|column| column.is_rowid_alias());
    let key_columns = constraint_refs.iter().map(|constraint| {
        constraint
            .table_col_pos
            .expect("a temporary index key must name a table column")
    });
    let other_columns = columns
        .iter()
        .enumerate()
        .filter(|(column_pos, _)| table_ref.column_is_used(*column_pos))
        .filter(|(column_pos, _)| {
            !constraint_refs
                .iter()
                .any(|constraint| constraint.table_col_pos == Some(*column_pos))
        })
        .map(|(column_pos, _)| column_pos);
    let index_columns = key_columns.chain(other_columns).map(|column_pos| {
        let column = &columns[column_pos];
        IndexOrderColumn {
            pos_in_table: column_pos,
            order: SortOrder::Asc,
            nulls_order: None,
            collation: column.collation_opt(),
            expr: column.generated_expr(),
        }
    });

    index_columns_order_consumed(
        table_ref,
        iter_dir,
        constraint_refs,
        order_target,
        target_columns,
        schema,
        EqualityPrefixScope::ConstantEquality,
        index_columns,
        column_count,
        table_ref.table.btree().is_some_and(|table| table.has_rowid),
        rowid_alias_col,
    )
}

/// Return how many order terms these index columns supply.
#[allow(clippy::too_many_arguments)]
fn index_columns_order_consumed<'a>(
    table_ref: &JoinedTable,
    iter_dir: IterationDirection,
    constraint_refs: &[RangeConstraintRef],
    order_target: &OrderTarget,
    target_columns: &[ColumnOrder],
    schema: &Schema,
    equality_prefix_scope: EqualityPrefixScope,
    index_columns: impl Iterator<Item = IndexOrderColumn<'a>>,
    column_count: usize,
    has_rowid: bool,
    rowid_alias_col: Option<usize>,
) -> OrderConsumption {
    let mut col_idx = 0;
    let mut matched_columns = 0;
    let mut index_columns = index_columns.enumerate();
    let mut includes_rowid = false;
    let target_is_rowid = |target_col: &ColumnOrder| match target_col.target {
        ColumnTarget::RowId => true,
        ColumnTarget::Column(col_no) => rowid_alias_col == Some(col_no),
        ColumnTarget::Expr(_) => false,
    };
    while col_idx < target_columns.len() {
        let Some((idx_pos, idx_col)) = index_columns.next() else {
            break;
        };
        let target_col = &target_columns[col_idx];
        if target_col.table_id != table_ref.internal_id {
            break;
        }
        let eq_prefix_usable = constraint_refs.iter().any(|constraint| {
            constraint.index_col_pos == idx_pos
                && constraint.eq.as_ref().is_some_and(|eq| {
                    equality_prefix_scope == EqualityPrefixScope::AnyEquality || eq.is_const
                })
        });
        if eq_prefix_usable {
            if target_matches_order_column(target_col, idx_col, table_ref) {
                if target_col.collation != idx_col.collation.unwrap_or_default() {
                    break;
                }
                col_idx += 1;
            }
            matched_columns += 1;
            continue;
        }

        if !target_matches_order_column(target_col, idx_col, table_ref) {
            break;
        }

        // Custom type columns store encoded blobs. The B-tree's byte order
        // does not match the custom type's order.
        if let ColumnTarget::Column(col_no) = &target_col.target {
            if let Some(col) = table_ref.table.columns().get(*col_no) {
                if schema
                    .get_type_def(&col.ty_str, table_ref.table.is_strict())
                    .is_some()
                {
                    break;
                }
            }
        }

        if target_col.collation != idx_col.collation.unwrap_or_default() {
            break;
        }
        let correct_order = if iter_dir == IterationDirection::Forwards {
            target_col.order == idx_col.order
        } else {
            target_col.order != idx_col.order
        };
        if !correct_order {
            break;
        }
        if !order_target.is_extremum()
            && target_col.effective_nulls_order() != idx_col.nulls_order(iter_dir)
        {
            break;
        }
        if target_is_rowid(target_col) {
            includes_rowid = true;
        }
        col_idx += 1;
        matched_columns += 1;
    }

    // Rowid tables keep equal secondary-index keys ordered by rowid.
    if col_idx < target_columns.len() && matched_columns == column_count && has_rowid {
        let target_col = &target_columns[col_idx];
        let correct_order = if iter_dir == IterationDirection::Forwards {
            target_col.order == SortOrder::Asc
        } else {
            target_col.order == SortOrder::Desc
        };
        if target_col.table_id == table_ref.internal_id
            && target_is_rowid(target_col)
            && correct_order
        {
            col_idx += 1;
            includes_rowid = true;
        }
    }

    OrderConsumption {
        consumed: col_idx,
        includes_rowid,
    }
}
