use super::{
    collate::get_collseq_from_expr,
    emitter::Resolver,
    plan::{
        DeletePlan, GroupBy, InSeekSource, IterationDirection, JoinInfo, JoinOrderMember, JoinType,
        JoinedTable, MinMaxDef, MultiIndexBranch, MultiIndexScanOp, Operation, Plan, Search,
        SeekDef, SeekKey, SelectPlan, SetOperation, SimpleAggregate, TablePlanEstimate,
        TableReferences, UpdatePlan, WhereTerm,
    },
};
use crate::alloc::TursoIteratorExt;
use crate::schema::GeneratedType;
use crate::translate::expression_index::expression_index_column_usage;
use crate::translate::plan::{BitSet, ColumnMask, MultiIndexBranchAccess};
use crate::translate::planner::TableMask;
use crate::{
    function::{AggFunc, Deterministic},
    index_method::{IndexMethodCostContext, IndexMethodCostEstimate},
    numeric::Numeric,
    schema::{
        BTreeCharacteristics, BTreeTable, ColDef, Column, Index, IndexColumn, Schema, Table, Type,
        ROWID_SENTINEL,
    },
    translate::{
        expr::{
            expr_references_any_subquery, expr_references_outer_query, expression_can_fail_on_input,
        },
        insert::ROWID_COLUMN,
        optimizer::{
            access_method::{AccessMethod, AccessMethodParams},
            constraints::{
                ConstraintUseCandidate, RangeConstraintRef, SeekRangeConstraint, TableConstraints,
            },
            cost::RowCountEstimate,
            multi_index::MultiIndexBranchAccessParams,
            order::{ColumnTarget, OrderTarget},
        },
        plan::{
            DmlSafetyReason, EphemeralRowidMode, HashJoinOp, IndexMethodQuery,
            NonFromClauseSubquery, QueryDestination, ResultSetColumn, Scan, SeekKeyComponent,
            SubqueryEvalPhase, SubqueryOrigin, SubqueryState, UpdateSetClause, WriteSetPlan,
        },
        trigger_exec::has_triggers_including_temp,
    },
    types::SeekOp,
    util::{
        count_fts_column_args, exprs_are_equivalent, simple_bind_expr, try_capture_parameters,
        try_capture_parameters_column_agnostic, try_substitute_parameters,
    },
    vdbe::{
        affinity::Affinity,
        builder::{CursorKey, CursorType, ProgramBuilder},
    },
    LimboError, Result,
};
use crate::{turso_assert, turso_assert_eq, turso_debug_assert, turso_soft_unreachable};
use constraints::{
    add_implied_column_equalities, can_use_partial_index, constraints_from_where_clause,
    partial_index, partial_index_predicate_terms, Constraint,
};
use cost::Cost;
use join::{
    compute_best_join_order_with_context, count_subquery_calls_for_plan, BestJoinOrderResult,
    JoinN, JoinPlanningContext,
};
use lift_common_subexpressions::lift_common_subexpressions_from_binary_or_terms;
use order::{
    compute_order_target, plan_satisfies_order_target, simple_aggregate_order_target,
    EliminatesSortBy, OrderTargetPurpose,
};
use rustc_hash::FxHashMap as HashMap;
use smallvec::SmallVec;
use std::{
    cmp::Ordering,
    collections::{BTreeSet, VecDeque},
    sync::Arc,
};
use turso_ext::{ConstraintInfo, ConstraintUsage};
use turso_parser::ast::RefAct;
use turso_parser::ast::{self, Expr, SortOrder, SubqueryType, TableInternalId, TriggerEvent};

pub(crate) mod access_method;
pub(crate) mod constraints;
pub(crate) mod cost;
mod cost_params;
pub(crate) mod join;
pub(crate) mod lift_common_subexpressions;
pub(crate) mod multi_index;
pub(crate) mod order;
pub(crate) mod unnest;

#[derive(Debug, Default)]
pub(crate) struct AvailableIndexes {
    indexes_by_table_id: HashMap<TableInternalId, VecDeque<Arc<Index>>>,
}

impl AvailableIndexes {
    fn for_table_references(resolver: &Resolver, table_references: &TableReferences) -> Self {
        let mut available_indexes = Self::default();
        for table_ref in table_references.joined_tables() {
            if !matches!(table_ref.table, Table::BTree(_) | Table::Virtual(_)) {
                continue;
            }
            let indexes = resolver.with_schema(table_ref.database_id, |schema| {
                schema.indexes.get(table_ref.table.get_name()).cloned()
            });
            if let Some(indexes) = indexes {
                available_indexes
                    .indexes_by_table_id
                    .insert(table_ref.internal_id, indexes);
            }
        }
        available_indexes
    }

    pub(crate) fn indexes_for_table(
        &self,
        table_id: TableInternalId,
    ) -> Option<&VecDeque<Arc<Index>>> {
        self.indexes_by_table_id.get(&table_id)
    }

    pub(crate) fn btree_indexes_for_column(
        &self,
        table_id: TableInternalId,
        column_pos: usize,
    ) -> impl Iterator<Item = &Index> {
        self.indexes_for_table(table_id)
            .into_iter()
            .flat_map(|indexes| indexes.iter())
            .filter(move |index| {
                index.index_method.is_none()
                    && index.column_table_pos_to_index_pos(column_pos) == Some(0)
            })
            .map(Arc::as_ref)
    }

    fn btree_index_by_name(
        &self,
        table_id: TableInternalId,
        index_name: &str,
    ) -> Option<Arc<Index>> {
        self.indexes_for_table(table_id)?
            .iter()
            .find(|index| {
                index.name.eq_ignore_ascii_case(index_name) && index.index_method.is_none()
            })
            .cloned()
    }

    #[cfg(test)]
    pub(crate) fn insert_for_table_name(
        &mut self,
        joined_tables: &[JoinedTable],
        table_name: &str,
        indexes: VecDeque<Arc<Index>>,
    ) {
        let table_ref = joined_tables
            .iter()
            .find(|table_ref| table_ref.table.get_name() == table_name)
            .expect("test table should exist");
        self.indexes_by_table_id
            .insert(table_ref.internal_id, indexes);
    }

    #[cfg(test)]
    pub(crate) fn push_front_for_table_name(
        &mut self,
        joined_tables: &[JoinedTable],
        table_name: &str,
        index: Arc<Index>,
    ) {
        let table_ref = joined_tables
            .iter()
            .find(|table_ref| table_ref.table.get_name() == table_name)
            .expect("test table should exist");
        self.indexes_by_table_id
            .entry(table_ref.internal_id)
            .or_default()
            .push_front(index);
    }
}

/// A candidate index method that could be used for table access in a join query.
/// This struct captures all information needed to construct an IndexMethodQuery
/// operation, allowing the DP join ordering algorithm to consider custom index
/// methods alongside BTree indexes.
#[derive(Debug, Clone)]
pub struct IndexMethodCandidate {
    /// Index of the table in the joined_tables list
    pub table_idx: usize,
    /// The index that defines this index method
    pub index: Arc<Index>,
    /// Pattern index from the index method definition that matched
    pub pattern_idx: usize,
    /// Arguments captured from pattern matching
    pub arguments: Vec<ast::Expr>,
    /// Mapping from synthetic column IDs to pattern column IDs for covered columns
    pub covered_columns: HashMap<usize, usize>,
    /// Index in WHERE clause that was covered by this pattern (if any)
    pub where_covered: Option<usize>,
    /// Cost estimate from the index method
    pub cost_estimate: Option<IndexMethodCostEstimate>,
}

impl IndexMethodCandidate {
    /// Build the IndexMethodQuery operation from this candidate
    pub fn to_query(&self) -> IndexMethodQuery {
        IndexMethodQuery {
            index: self.index.clone(),
            pattern_idx: self.pattern_idx,
            arguments: self.arguments.clone(),
            covered_columns: self.covered_columns.clone(),
        }
    }
}

/// Result of successfully matching an index method pattern against a query.
/// This intermediate struct allows both `collect_index_method_candidates` and
/// `optimize_table_access_with_custom_modules` to share pattern matching logic.
#[derive(Debug, Clone)]
struct IndexMethodPatternMatch {
    /// Pattern index from the index method definition that matched
    pattern_idx: usize,
    /// Parameters captured from pattern matching (positional placeholders)
    parameters: HashMap<i32, ast::Expr>,
    /// Index in WHERE clause that was covered by this pattern (if any)
    where_covered: Option<usize>,
    /// Whether the pattern explicitly handles ORDER BY
    pattern_has_order_by: bool,
    /// Whether the pattern explicitly handles LIMIT
    pattern_has_limit: bool,
    /// Pattern result columns (needed for covered columns calculation)
    pattern_columns: Vec<ast::ResultColumn>,
}

/// Try to match an index method pattern against a query's clauses.
#[allow(clippy::too_many_arguments)]
fn try_match_index_method_pattern(
    pattern: &ast::Select,
    table: &JoinedTable,
    query_where_terms: &[WhereTerm],
    order_by: &[(
        Box<ast::Expr>,
        SortOrder,
        Option<turso_parser::ast::NullsOrder>,
    )],
    limit: &Option<Box<Expr>>,
    offset: &Option<Box<Expr>>,
    pattern_idx: usize,
    soft_bind_errors: bool,
) -> Option<IndexMethodPatternMatch> {
    let mut pattern = pattern.clone();
    if pattern.with.is_some() || !pattern.body.compounds.is_empty() {
        return None;
    }

    let ast::OneSelect::Select {
        columns,
        from: Some(ast::FromClause { select, joins }),
        distinctness: None,
        where_clause: ref mut pattern_where_clause,
        group_by: None,
        window_clause,
    } = &mut pattern.body.select
    else {
        if soft_bind_errors {
            return None;
        }
        panic!("unexpected select pattern body");
    };

    if !window_clause.is_empty() || !joins.is_empty() {
        return None;
    }

    let ast::SelectTable::Table(name, _, _) = select.as_ref() else {
        if soft_bind_errors {
            return None;
        }
        panic!("unexpected from clause");
    };

    // Bind expressions to this table
    for column in columns.iter_mut() {
        if let ast::ResultColumn::Expr(e, _) = column {
            if soft_bind_errors {
                if simple_bind_expr(table, &[], e).is_err() {
                    return None;
                }
            } else {
                simple_bind_expr(table, &[], e).ok()?;
            }
        }
    }
    for column in pattern.order_by.iter_mut() {
        if soft_bind_errors {
            if simple_bind_expr(table, columns, &mut column.expr).is_err() {
                return None;
            }
        } else {
            simple_bind_expr(table, columns, &mut column.expr).ok()?;
        }
    }
    if let Some(pattern_where) = pattern_where_clause {
        if soft_bind_errors {
            if simple_bind_expr(table, columns, pattern_where).is_err() {
                return None;
            }
        } else {
            simple_bind_expr(table, columns, pattern_where).ok()?;
        }
    }

    if name.name.as_str() != table.table.get_name() {
        return None;
    }

    let pattern_has_order_by = !pattern.order_by.is_empty();
    let pattern_has_limit = pattern.limit.is_some();

    // If pattern has ORDER BY, it must match exactly
    if pattern_has_order_by && order_by.len() != pattern.order_by.len() {
        return None;
    }

    let mut where_query_covered: Option<usize> = None;
    let mut parameters = HashMap::default();

    // Match ORDER BY if pattern has it
    if pattern_has_order_by {
        for (pattern_column, (query_column, query_order, query_nulls)) in
            pattern.order_by.iter().zip(order_by.iter())
        {
            if *query_order != pattern_column.order.unwrap_or(SortOrder::Asc) {
                return None;
            }
            // If the query has explicit NULLS ordering, the index pattern cannot
            // satisfy it (index methods have no NULLS awareness).
            if query_nulls.is_some() {
                return None;
            }
            let num_col_args = count_fts_column_args(&pattern_column.expr);
            let captured = if num_col_args > 0 {
                try_capture_parameters_column_agnostic(
                    &pattern_column.expr,
                    query_column,
                    num_col_args,
                )
            } else {
                try_capture_parameters(&pattern_column.expr, query_column)
            };
            parameters.extend(captured?);
        }
    }

    // Match LIMIT if pattern has it
    match (pattern.limit.as_ref().map(|x| &x.expr), limit) {
        (Some(_), None) => return None,
        (Some(pattern_limit), Some(query_limit)) => {
            let captured = try_capture_parameters(pattern_limit, query_limit)?;
            parameters.extend(captured);
        }
        (None, Some(_)) | (None, None) => {}
    }

    // Match OFFSET if pattern has it
    match (
        pattern.limit.as_ref().and_then(|x| x.offset.as_ref()),
        offset,
    ) {
        (Some(_), None) => return None,
        (Some(pattern_off), Some(query_off)) => {
            let captured = try_capture_parameters(pattern_off, query_off)?;
            parameters.extend(captured);
        }
        (None, Some(_)) | (None, None) => {}
    }

    // Match WHERE clause
    if let Some(pattern_where) = pattern_where_clause {
        for (i, query_where) in query_where_terms.iter().enumerate() {
            let num_col_args = count_fts_column_args(pattern_where);
            let captured = if num_col_args > 0 {
                try_capture_parameters_column_agnostic(
                    pattern_where,
                    &query_where.expr,
                    num_col_args,
                )
            } else {
                try_capture_parameters(pattern_where, &query_where.expr)
            };
            let Some(captured) = captured else {
                continue;
            };
            parameters.extend(captured);
            where_query_covered = Some(i);
            break;
        }
    }

    // Pattern requires WHERE but we didn't match any
    if pattern_where_clause.is_some() && where_query_covered.is_none() {
        return None;
    }

    let where_covered_completely = query_where_terms.is_empty()
        || (where_query_covered.is_some() && query_where_terms.len() == 1);

    // When WHERE is not completely covered, skip patterns with ORDER BY/LIMIT
    // because post-filtering would disrupt the order or apply limits incorrectly
    if !where_covered_completely && (pattern_has_order_by || pattern_has_limit) {
        return None;
    }

    Some(IndexMethodPatternMatch {
        pattern_idx,
        parameters,
        where_covered: where_query_covered,
        pattern_has_order_by,
        pattern_has_limit,
        pattern_columns: columns.clone(),
    })
}

/// Build covered columns mapping from pattern columns.
/// Returns a HashMap mapping synthetic column IDs to pattern column IDs.
fn build_covered_columns_mapping(
    pattern_columns: &[ast::ResultColumn],
    parameters: &HashMap<i32, ast::Expr>,
) -> HashMap<usize, usize> {
    let mut covered_column_id = 1_000_000;
    let mut covered_columns = HashMap::default();
    for (pattern_column_id, pattern_column) in pattern_columns.iter().enumerate() {
        let ast::ResultColumn::Expr(pattern_expr, _) = pattern_column else {
            continue;
        };
        let Some(_substituted) = try_substitute_parameters(pattern_expr, parameters) else {
            continue;
        };
        covered_columns.insert(covered_column_id, pattern_column_id);
        covered_column_id += 1;
    }
    covered_columns
}

/// Sort parameters by key and extract just the expressions as a Vec.
fn sorted_arguments_from_parameters(parameters: &HashMap<i32, ast::Expr>) -> Vec<ast::Expr> {
    let mut arguments: Vec<_> = parameters.iter().collect();
    arguments.sort_by_key(|(&i, _)| i);
    arguments.iter().map(|(_, e)| (*e).clone()).collect()
}

/// Collect index method candidates for all tables that have custom index methods.
/// This function performs pattern matching but does NOT apply the operations,
/// allowing the DP join ordering algorithm to consider index methods as candidates.
#[allow(clippy::too_many_arguments)]
fn collect_index_method_candidates(
    table_references: &TableReferences,
    available_indexes: &AvailableIndexes,
    where_clause: &[WhereTerm],
    order_by: &[(
        Box<ast::Expr>,
        SortOrder,
        Option<turso_parser::ast::NullsOrder>,
    )],
    group_by: &Option<GroupBy>,
    limit: &Option<Box<Expr>>,
    offset: &Option<Box<Expr>>,
    base_table_rows: &[RowCountEstimate],
    params: &cost_params::CostModelParams,
) -> Result<Vec<IndexMethodCandidate>> {
    let mut candidates = Vec::new();

    // Group by is not supported for index methods
    if group_by.is_some() {
        return Ok(candidates);
    }

    let tables = table_references.joined_tables();
    for (table_idx, table) in tables.iter().enumerate() {
        let Some(indexes) = available_indexes.indexes_for_table(table.internal_id) else {
            continue;
        };

        for index in indexes {
            let Some(module) = &index.index_method else {
                continue;
            };
            if index.is_backing_btree_index() {
                continue;
            }

            let definition = module.definition();
            for (pattern_idx, pattern) in definition.patterns.iter().enumerate() {
                // Use shared helper for pattern matching
                let Some(pattern_match) = try_match_index_method_pattern(
                    pattern,
                    table,
                    where_clause,
                    order_by,
                    limit,
                    offset,
                    pattern_idx,
                    true, // continue on binding failures
                ) else {
                    continue;
                };

                // Build covered columns mapping from pattern match
                let covered_columns = build_covered_columns_mapping(
                    &pattern_match.pattern_columns,
                    &pattern_match.parameters,
                );

                // Sort and collect arguments before costing so the index
                // method can inspect captured literals such as LIMIT.
                let arguments = sorted_arguments_from_parameters(&pattern_match.parameters);

                // Get cost estimate from the index method
                let cost_estimate = module.init().ok().and_then(|cursor| {
                    let base_rows = base_table_rows
                        .get(table_idx)
                        .map(|r| **r)
                        .unwrap_or(params.rows_per_table_fallback);
                    cursor.estimate_cost(&IndexMethodCostContext {
                        pattern_idx: pattern_match.pattern_idx,
                        base_table_rows: base_rows,
                        arguments: &arguments,
                    })
                });

                candidates.push(IndexMethodCandidate {
                    table_idx,
                    index: index.clone(),
                    pattern_idx: pattern_match.pattern_idx,
                    arguments,
                    covered_columns,
                    where_covered: pattern_match.where_covered,
                    cost_estimate,
                });

                // Found a match for this table+index, try next index
                break;
            }
        }
    }

    Ok(candidates)
}

#[tracing::instrument(skip_all, level = tracing::Level::DEBUG)]
#[turso_macros::trace_stack]
pub fn optimize_plan(
    program: &mut ProgramBuilder,
    plan: &mut Plan,
    resolver: &Resolver,
) -> Result<()> {
    let resources_before = subquery_resources(plan);
    match plan {
        Plan::Select(plan) => optimize_select_plan(plan, resolver)?,
        Plan::Delete(plan) => optimize_delete_plan(plan, resolver)?,
        Plan::Update(plan) => optimize_update_plan(program, plan, resolver)?,
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            optimize_select_plan(right_most, resolver)?;
            for (plan, _) in left {
                optimize_select_plan(plan, resolver)?;
            }
        }
        Plan::RecursiveCte(recursive_cte) => {
            optimize_recursive_cte_query(&mut recursive_cte.initial_query, resolver)?;
            optimize_recursive_cte_query(&mut recursive_cte.recursive_query, resolver)?;
        }
    }
    let resources_after = subquery_resources(plan);
    for cursor_id in resources_before
        .cursor_ids
        .difference(&resources_after.cursor_ids)
    {
        program.release_cursor_id(*cursor_id);
    }
    for (start, count) in resources_before
        .register_ranges
        .difference(&resources_after.register_ranges)
    {
        program.release_registers(*start, *count);
    }
    // When debug tracing is enabled, print the optimized plan as a SQL string for debugging
    tracing::debug!(plan_sql = plan.to_string());
    Ok(())
}

#[derive(Default)]
struct SubqueryResources {
    cursor_ids: BTreeSet<usize>,
    register_ranges: BTreeSet<(usize, usize)>,
}

/// Find result storage reserved by subqueries in a plan.
fn subquery_resources(plan: &Plan) -> SubqueryResources {
    fn add_subqueries(subqueries: &[NonFromClauseSubquery], resources: &mut SubqueryResources) {
        for subquery in subqueries {
            match &subquery.query_type {
                SubqueryType::Exists { .. } => {}
                SubqueryType::RowValue {
                    result_reg_start,
                    num_regs,
                } => {
                    resources
                        .register_ranges
                        .insert((*result_reg_start, *num_regs));
                }
                SubqueryType::In { cursor_id, .. } => {
                    resources.cursor_ids.insert(*cursor_id);
                }
            }
            if let SubqueryState::Unevaluated {
                plan: Some(child_plan),
            } = &subquery.state
            {
                add_plan(child_plan, resources);
            }
        }
    }

    fn add_select(plan: &SelectPlan, resources: &mut SubqueryResources) {
        add_subqueries(&plan.non_from_clause_subqueries, resources);
        for table in plan.table_references.joined_tables() {
            if let Table::FromClauseSubquery(subquery) = &table.table {
                add_plan(&subquery.plan, resources);
            }
        }
    }

    fn add_plan(plan: &Plan, resources: &mut SubqueryResources) {
        match plan {
            Plan::Select(plan) => add_select(plan, resources),
            Plan::CompoundSelect {
                left, right_most, ..
            } => {
                for (plan, _) in left {
                    add_select(plan, resources);
                }
                add_select(right_most, resources);
            }
            Plan::RecursiveCte(plan) => {
                add_plan(&plan.initial_query, resources);
                add_plan(&plan.recursive_query, resources);
            }
            Plan::Delete(plan) => {
                add_subqueries(&plan.non_from_clause_subqueries, resources);
                if let Some(rowset_plan) = &plan.rowset_plan {
                    add_select(rowset_plan, resources);
                }
            }
            Plan::Update(plan) => {
                add_subqueries(&plan.non_from_clause_subqueries, resources);
                if let Some(write_set_plan) = &plan.write_set_plan {
                    add_select(&write_set_plan.select, resources);
                }
            }
        }
    }

    let mut resources = SubqueryResources::default();
    add_plan(plan, &mut resources);
    resources
}

fn optimize_recursive_cte_query(query: &mut Plan, resolver: &Resolver) -> Result<()> {
    let mut cache = SubqueryPlanCache::default();
    optimize_recursive_cte_query_with_cache(query, resolver, &mut cache)
}

fn optimize_recursive_cte_query_with_cache(
    query: &mut Plan,
    resolver: &Resolver,
    cache: &mut SubqueryPlanCache,
) -> Result<()> {
    match query {
        Plan::Select(select) => optimize_select_plan_with_cache(select, resolver, cache),
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            for (select, _) in left {
                optimize_select_plan_with_cache(select, resolver, cache)?;
            }
            optimize_select_plan_with_cache(right_most, resolver, cache)
        }
        Plan::RecursiveCte(_) | Plan::Delete(_) | Plan::Update(_) => Err(
            LimboError::InternalError("recursive CTE query is not a SELECT".to_string()),
        ),
    }
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
/// Transform MATCH expressions to fts_match() function calls.
fn transform_match_to_fts_match(
    where_clause: &mut [WhereTerm],
    resolver: &Resolver,
    table_references: &TableReferences,
) -> Result<()> {
    use super::ast::{FunctionTail, LikeOperator, Name, TableInternalId};
    use super::expr::{walk_expr_mut, WalkControl};

    // Helper to extract table ID from a column expression
    fn get_table_id_from_expr(expr: &Expr) -> Option<TableInternalId> {
        match expr {
            Expr::Column { table, .. } => Some(*table),
            Expr::Parenthesized(exprs) if !exprs.is_empty() => get_table_id_from_expr(&exprs[0]),
            _ => None,
        }
    }

    // Helper to check if a table has an FTS index by its internal ID.
    // Resolve against the schema of the table's own database, so MATCH also
    // plans against FTS indexes in ATTACHed databases.
    let table_has_fts_index = |table_id: TableInternalId| -> bool {
        table_references
            .joined_tables()
            .iter()
            .find(|t| t.internal_id == table_id)
            .and_then(|t| {
                if let Table::BTree(btree) = &t.table {
                    Some(
                        resolver
                            .with_schema(t.database_id, |schema| schema.has_fts_index(&btree.name)),
                    )
                } else {
                    None
                }
            })
            .unwrap_or(false)
    };

    let mut match_without_fts = false;
    for term in where_clause.iter_mut() {
        let _ = walk_expr_mut(&mut term.expr, &mut |e: &mut Expr| -> Result<WalkControl> {
            match e {
                Expr::Like {
                    lhs,
                    not,
                    op: LikeOperator::Match,
                    rhs,
                    escape: _,
                } => {
                    // Check if the specific table referenced by this MATCH has an FTS index
                    let has_fts = get_table_id_from_expr(lhs).is_some_and(table_has_fts_index);

                    if !has_fts {
                        match_without_fts = true;
                        // Don't transform, we'll error after the walk
                        return Ok(WalkControl::SkipChildren);
                    }

                    // Transform MATCH to fts_match():
                    // - `col MATCH 'query'` -> `fts_match(col, 'query')`
                    // - `(col1, col2) MATCH 'query'` -> `fts_match(col1, col2, 'query')`
                    let mut args: Vec<Box<Expr>> = match lhs.as_ref() {
                        Expr::Parenthesized(cols) => cols.clone(),
                        _ => vec![lhs.clone()],
                    };
                    args.push(rhs.clone());

                    let func_call = Expr::FunctionCall {
                        name: Name::exact("fts_match".to_string()),
                        distinctness: None,
                        args,
                        order_by: vec![],
                        within_group: vec![],
                        filter_over: FunctionTail {
                            filter_clause: None,
                            over_clause: None,
                        },
                    };
                    if *not {
                        // For NOT MATCH, just wrap the whole thing in a unary NOT
                        *e = Expr::Unary(ast::UnaryOperator::Not, Box::new(func_call));
                    } else {
                        *e = func_call;
                    }
                    Ok(WalkControl::Continue)
                }
                _ => Ok(WalkControl::Continue),
            }
        });
    }

    if match_without_fts {
        return Err(LimboError::ParseError(
            "unable to use function MATCH in the requested context".to_string(),
        ));
    }

    Ok(())
}

/// Detect whether this plan qualifies for the simple-aggregate fast path.
///
/// Analogous to SQLite's `isSimpleCount()` + `minMaxQuery()`.
/// Must be called before `optimize_table_access` so the order target is available.
fn detect_simple_aggregate(plan: &SelectPlan) -> Option<SimpleAggregate> {
    // Common preconditions shared by count(*) and min/max.
    if plan.aggregates.len() != 1
        || plan.table_references.joined_tables().len() != 1
        || plan.result_columns.len() != 1
        || plan.group_by.is_some()
        || plan.contains_constant_false_condition
        || plan.aggregates.first().unwrap().filter_expr.is_some()
    {
        return None;
    }

    let table_ref = plan.table_references.joined_tables().first().unwrap();
    let agg = plan.aggregates.first().unwrap();
    let result_expr = &plan.result_columns.first().unwrap().expr;

    // The result column must be exactly the aggregate expression (not wrapped in
    // something like `length(count(*))`).
    if !exprs_are_equivalent(result_expr, &agg.original_expr) {
        return None;
    }

    match agg.func {
        AggFunc::Count0
            if matches!(table_ref.table, Table::BTree(..))
                && plan.table_references.outer_query_refs().is_empty()
                && plan.where_clause.is_empty()
                && plan.limit.is_none()
                && plan.offset.is_none() =>
        {
            Some(SimpleAggregate::Count)
        }
        AggFunc::Min | AggFunc::Max
            if agg.args.len() == 1
                && matches!(
                    table_ref.table,
                    Table::BTree(..) | Table::FromClauseSubquery(..)
                ) =>
        {
            // Unlike COUNT(*), MIN/MAX may still use the fast path with a
            // WHERE clause as long as the chosen access path can walk directly
            // to the first qualifying extremum row.
            let argument = agg.args[0].clone();
            let order = if matches!(agg.func, AggFunc::Min) {
                SortOrder::Asc
            } else {
                SortOrder::Desc
            };
            let collation = get_collseq_from_expr(&argument, &plan.table_references)
                .ok()
                .flatten();
            Some(SimpleAggregate::MinMax(Box::new(MinMaxDef {
                func: agg.func.clone(),
                argument,
                order,
                collation,
            })))
        }
        _ => None,
    }
}

/// The table reads chosen by the join search.
struct TableAccessPlan {
    access_methods: Vec<AccessMethod>,
    constraints: Vec<TableConstraints>,
    join: JoinN,
    subquery_calls: SmallVec<[(TableInternalId, f64); 2]>,
    order_target: Option<OrderTarget>,
    sort_eliminated: bool,
    initial_input_rows: f64,
}

#[derive(Default)]
struct SubqueryPlanCache {
    // The original and changed forms copy the same child subqueries. Keep a
    // finished child plan so the next copy does not plan that child again.
    from_clause: HashMap<TableInternalId, Plan>,
    correlated: HashMap<(TableInternalId, u64), Plan>,
}

/**
 * Make a few passes over the plan to optimize it.
 * TODO: these could probably be done in less passes,
 * but having them separate makes them easier to understand
 */
#[turso_macros::trace_stack]
pub fn optimize_select_plan(plan: &mut SelectPlan, resolver: &Resolver) -> Result<()> {
    let mut cache = SubqueryPlanCache::default();
    optimize_select_plan_with_cache(plan, resolver, &mut cache)
}

/// Whether the unnested form can be emitted.
///
/// Unnesting moves a subquery's WHERE clause into the outer query, so a term
/// that is always false can travel with it, as in
/// `WHERE a IN (SELECT z FROM t WHERE 'abc' AND t.z = o.b)`. The emitter skips
/// loop setup for a query that returns no rows, and that setup is where a table
/// the rewrite added to the FROM clause gets its result registers. Reading
/// those registers afterwards is a bug, so run the correlated form instead. It
/// returns the same rows.
fn rewritten_form_is_emittable(rewritten: &SelectPlan) -> bool {
    !rewritten.contains_constant_false_condition
}

#[turso_macros::trace_stack]
fn optimize_select_plan_with_cache(
    plan: &mut SelectPlan,
    resolver: &Resolver,
    cache: &mut SubqueryPlanCache,
) -> Result<()> {
    if !plan
        .non_from_clause_subqueries
        .iter()
        .any(|subquery| subquery.correlated)
    {
        return optimize_select_plan_form(plan, resolver, cache);
    }

    #[cfg(feature = "simulator")]
    if resolver.subquery_unnesting_mode() == crate::SubqueryUnnestingMode::Disabled {
        return optimize_select_plan_form(plan, resolver, cache);
    }

    // TODO: Let join search run a correlated subquery as soon as all columns
    // that it needs are ready. It can then compare that step with the added
    // join tables in one search. Until then, both forms need their own search.
    let mut rewritten = plan.clone();
    if !unnest::rewrite_correlated_subqueries(&mut rewritten, resolver)? {
        return optimize_select_plan_form(plan, resolver, cache);
    }

    let has_full_join = plan.table_references.joined_tables().iter().any(|table| {
        table
            .join_info
            .as_ref()
            .is_some_and(JoinInfo::is_full_outer)
    });
    // The correlated form cannot run on every matched and unmatched FULL JOIN
    // row yet. A complete semi-join or anti-join rewrite can, so use it.
    let full_join_rewrite_is_complete = has_full_join
        && !rewritten
            .non_from_clause_subqueries
            .iter()
            .any(|subquery| subquery.correlated);
    if full_join_rewrite_is_complete {
        let rewritten_table_plan =
            find_select_plan_form(&mut rewritten, resolver, cache, false, None)?;
        if !rewritten_form_is_emittable(&rewritten) {
            return optimize_select_plan_form(plan, resolver, cache);
        }
        *plan = rewritten;
        apply_select_table_plan(plan, rewritten_table_plan, resolver)?;
        return Ok(());
    }

    #[cfg(feature = "simulator")]
    if resolver.subquery_unnesting_mode() == crate::SubqueryUnnestingMode::Forced {
        let rewritten_table_plan =
            find_select_plan_form(&mut rewritten, resolver, cache, false, None)?;
        if !rewritten_form_is_emittable(&rewritten) {
            return optimize_select_plan_form(plan, resolver, cache);
        }
        *plan = rewritten;
        apply_select_table_plan(plan, rewritten_table_plan, resolver)?;
        return Ok(());
    }

    let original_table_plan = find_select_plan_form(plan, resolver, cache, true, None)?;
    // The query already returns no rows, so a cheaper form cannot be found.
    if plan.contains_constant_false_condition {
        apply_select_table_plan(plan, original_table_plan, resolver)?;
        return Ok(());
    }
    let cost_limit = plan.estimated_cost.map(Cost);
    let rewritten_table_plan =
        find_select_plan_form(&mut rewritten, resolver, cache, false, cost_limit)?;
    // A form that returns no rows costs nothing, so it would always win the
    // comparison below. Check that it can be emitted before comparing costs.
    let use_rewritten = rewritten_form_is_emittable(&rewritten)
        && matches!(
            (plan.estimated_cost, rewritten.estimated_cost),
            (Some(original_cost), Some(rewritten_cost)) if rewritten_cost <= original_cost
        );
    if use_rewritten {
        // Equal work is better without one subquery call per outer row.
        *plan = rewritten;
        apply_select_table_plan(plan, rewritten_table_plan, resolver)?;
    } else {
        apply_select_table_plan(plan, original_table_plan, resolver)?;
    }

    Ok(())
}

/// Choose table reads for one version of a query.
fn optimize_select_plan_form(
    plan: &mut SelectPlan,
    resolver: &Resolver,
    cache: &mut SubqueryPlanCache,
) -> Result<()> {
    let table_plan = find_select_plan_form(plan, resolver, cache, false, None)?;
    apply_select_table_plan(plan, table_plan, resolver)
}

/// Find the table reads for one version of a query.
fn find_select_plan_form(
    plan: &mut SelectPlan,
    resolver: &Resolver,
    cache: &mut SubqueryPlanCache,
    save_subquery_plans: bool,
    cost_limit: Option<Cost>,
) -> Result<Option<TableAccessPlan>> {
    let schema = resolver.schema();
    #[cfg(feature = "optimizer_params")]
    let params: &cost_params::CostModelParams = &cost_params::LOADED_PARAMS;
    #[cfg(not(feature = "optimizer_params"))]
    let params: &cost_params::CostModelParams = &cost_params::DEFAULT_PARAMS;
    plan.estimated_output_rows = None;
    plan.estimated_cost = None;

    // A rewrite can move MATCH terms out of a subquery, so do this after the
    // query form has been chosen.
    #[cfg(all(feature = "fts", not(target_family = "wasm")))]
    transform_match_to_fts_match(&mut plan.where_clause, resolver, &plan.table_references)?;

    // EXISTS only needs one row. Add LIMIT 1 to subqueries left after the
    // rewrite. The rewrite must see the limit written by the user, if any.
    for subquery in &mut plan.non_from_clause_subqueries {
        if matches!(subquery.query_type, ast::SubqueryType::Exists { .. }) {
            if let SubqueryState::Unevaluated {
                plan: Some(inner_plan),
                ..
            } = &mut subquery.state
            {
                if let Plan::Select(ref mut inner_plan) = inner_plan.as_mut() {
                    if inner_plan.limit.is_none() {
                        inner_plan.limit = Some(Box::new(Expr::Literal(ast::Literal::Numeric(
                            "1".to_string(),
                        ))));
                    }
                }
            }
        }
    }
    optimize_subqueries(plan, resolver, cache, save_subquery_plans)?;
    let available_indexes =
        AvailableIndexes::for_table_references(resolver, &plan.table_references);
    lift_common_subexpressions_from_binary_or_terms(&mut plan.where_clause)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constant_conditions(&mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        plan.estimated_output_rows = Some(0.0);
        plan.estimated_cost = Some(0.0);
        plan_correlated_subqueries(plan, resolver, &[], cache, save_subquery_plans)?;
        return Ok(None);
    }

    plan.simple_aggregate = detect_simple_aggregate(plan);
    let table_plan = find_table_access_plan(
        schema,
        &mut plan.result_columns,
        &mut plan.table_references,
        &available_indexes,
        &mut plan.where_clause,
        &mut plan.order_by,
        &mut plan.group_by,
        plan.simple_aggregate.as_ref(),
        &plan.non_from_clause_subqueries,
        &mut plan.limit,
        &mut plan.offset,
        plan.input_cardinality_hint.unwrap_or(1.0),
        cost_limit,
    )?;

    if matches!(plan.simple_aggregate, Some(SimpleAggregate::MinMax(_)))
        && !table_plan
            .as_ref()
            .is_some_and(|table_plan| table_plan.sort_eliminated)
    {
        plan.simple_aggregate = None;
    }

    let table_cost = table_plan.as_ref().map(|table_plan| table_plan.join.cost);
    let mut subquery_calls = table_plan
        .as_ref()
        .map(|table_plan| table_plan.subquery_calls.clone())
        .unwrap_or_default();

    if let Some(table_plan) = table_plan.as_ref() {
        let rows_before_limit =
            estimate_select_output_rows(plan, table_plan.join.output_cardinality, schema);
        let mut rows = rows_before_limit;
        // Clamp to LIMIT when it's a literal non-negative number.
        // Negative LIMIT means "no limit" in SQLite, so we skip those.
        if let Some(limit) = &plan.limit {
            if let Ok(value) = crate::util::parse_signed_number(limit) {
                let limit_rows = match value {
                    crate::types::Value::Numeric(Numeric::Integer(value)) if value >= 0 => {
                        Some(value as f64)
                    }
                    crate::types::Value::Numeric(Numeric::Float(value)) => {
                        let value: f64 = value.into();
                        if value >= 0.0 {
                            Some(value)
                        } else {
                            None
                        }
                    }
                    _ => None,
                };
                if let Some(limit_rows) = limit_rows {
                    rows = rows.min(limit_rows);
                    if rows_before_limit > 0.0 {
                        // These call counts cover the full result. LIMIT only
                        // needs the same share of those calls.
                        let call_scale = (rows / rows_before_limit).min(1.0);
                        for (_, calls) in &mut subquery_calls {
                            *calls *= call_scale;
                        }
                    }
                }
            }
        }
        plan.estimated_output_rows = Some(rows);
    }

    plan_correlated_subqueries(plan, resolver, &subquery_calls, cache, save_subquery_plans)?;

    let table_cost = table_cost.or_else(|| {
        plan.table_references
            .joined_tables()
            .is_empty()
            .then_some(Cost(0.0))
    });
    if let Some(table_cost) = table_cost {
        let subquery_cost =
            plan.non_from_clause_subqueries
                .iter()
                .try_fold(0.0, |total, subquery| {
                    let SubqueryState::Unevaluated {
                        plan: Some(inner_plan),
                    } = &subquery.state
                    else {
                        return None;
                    };
                    let calls = if subquery.correlated {
                        subquery_calls
                            .iter()
                            .find_map(|(id, calls)| (*id == subquery.internal_id).then_some(*calls))
                            .unwrap_or_else(|| plan.input_cardinality_hint.unwrap_or(1.0))
                    } else {
                        1.0
                    };
                    // Starting the subquery program takes work on every call.
                    let call_cost = calls.max(1.0) * params.cpu_cost_per_seek;
                    inner_plan
                        .estimated_cost()
                        .map(|cost| total + cost + call_cost)
                });
        if let Some(subquery_cost) = subquery_cost {
            plan.estimated_cost = Some(table_cost.0 + subquery_cost);
        }
    }

    Ok(table_plan)
}

/// Write the winning table plan into one version of a query.
fn apply_select_table_plan(
    plan: &mut SelectPlan,
    table_plan: Option<TableAccessPlan>,
    resolver: &Resolver,
) -> Result<()> {
    let Some(table_plan) = table_plan else {
        return Ok(());
    };
    plan.join_order = apply_table_access_plan(
        resolver,
        &mut plan.table_references,
        &mut plan.where_clause,
        &mut plan.order_by,
        &mut plan.group_by,
        table_plan,
    )?;
    Ok(())
}

fn optimize_delete_plan(plan: &mut DeletePlan, resolver: &Resolver) -> Result<()> {
    let schema = resolver.schema();
    let available_indexes =
        AvailableIndexes::for_table_references(resolver, &plan.table_references);
    #[cfg(all(feature = "fts", not(target_family = "wasm")))]
    transform_match_to_fts_match(&mut plan.where_clause, resolver, &plan.table_references)?;

    lift_common_subexpressions_from_binary_or_terms(&mut plan.where_clause)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constant_conditions(&mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        return Ok(());
    }

    if let Some(rowset_plan) = plan.rowset_plan.as_mut() {
        optimize_select_plan(rowset_plan, resolver)?;
    }

    let mut order_by = vec![];
    let _ = optimize_table_access(
        schema,
        resolver,
        &mut plan.result_columns,
        &mut plan.table_references,
        &available_indexes,
        &mut plan.where_clause,
        &mut order_by,
        &mut None,
        None,
        &plan.non_from_clause_subqueries,
        &mut None,
        &mut None,
        1.0,
    )?;

    Ok(())
}

fn optimize_update_plan(
    program: &mut ProgramBuilder,
    plan: &mut UpdatePlan,
    resolver: &Resolver,
) -> Result<()> {
    let schema = resolver.schema();
    let is_update_from = !plan.from_tables.joined_tables().is_empty();
    if is_update_from {
        plan.safety.require(DmlSafetyReason::UpdateFrom);
    }
    let mut target_tables = TableReferences::new(
        vec![plan.target_table.clone()],
        plan.from_tables.outer_query_refs().to_vec(),
    );
    #[cfg(all(feature = "fts", not(target_family = "wasm")))]
    transform_match_to_fts_match(&mut plan.where_clause, resolver, &target_tables)?;
    lift_common_subexpressions_from_binary_or_terms(&mut plan.where_clause)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constant_conditions(&mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        if is_update_from {
            let update_from_set_result_columns = update_from_set_result_columns(&plan.set_clauses);
            build_update_write_set_plan(program, plan, update_from_set_result_columns)?;
        }
        return Ok(());
    }
    if is_update_from {
        let update_from_set_result_columns = update_from_set_result_columns(&plan.set_clauses);
        build_update_write_set_plan(program, plan, update_from_set_result_columns)?;
        optimize_select_plan(
            &mut plan
                .write_set_plan
                .as_mut()
                .expect("UPDATE ... FROM must build its write-set SELECT before optimization")
                .select,
            resolver,
        )?;
        return Ok(());
    }

    let mut order_by = vec![];
    let available_indexes = AvailableIndexes::for_table_references(resolver, &target_tables);
    let optimize_result = optimize_table_access(
        schema,
        resolver,
        &mut [],
        &mut target_tables,
        &available_indexes,
        &mut plan.where_clause,
        &mut order_by,
        &mut None,
        None,
        &plan.non_from_clause_subqueries,
        &mut None,
        &mut None,
        1.0,
    )?;
    plan.target_table = target_tables
        .joined_tables()
        .first()
        .expect("UPDATE must optimize exactly one target table")
        .clone();

    if let Some(reason) = update_write_set_reason(plan, resolver)? {
        plan.safety.require(reason);
    }

    if !plan.safety.requires_stable_write_set() {
        return Ok(());
    }

    let join_order = optimize_result.unwrap_or_else(|| default_join_order(&target_tables));

    build_update_write_set_plan(program, plan, Vec::new())?;
    plan.write_set_plan
        .as_mut()
        .expect("stable-write-set UPDATE must build a write-set SELECT")
        .select
        .join_order = join_order;
    Ok(())
}

fn update_write_set_reason(
    plan: &UpdatePlan,
    resolver: &Resolver,
) -> Result<Option<DmlSafetyReason>> {
    let table_ref = &plan.target_table;
    let reason = 'requires: {
        let Some(btree_table_arc) = table_ref.table.btree() else {
            break 'requires None;
        };
        let btree_table = btree_table_arc.as_ref();

        // Multi-index scans gather rowids from multiple index branches.
        // For UPDATE, we always use the prebuilt ephemeral-table path so writes run against
        // that fixed rowid list (no surprises from branch/index overlap).
        if matches!(table_ref.op, Operation::MultiIndexScan(_)) {
            break 'requires Some(DmlSafetyReason::MultiIndexScan);
        }

        // Index method cursors that stream lazily need rowids collected first.
        if let Operation::IndexMethodQuery(query) = &table_ref.op {
            let attachment = query
                .index
                .index_method
                .as_ref()
                .expect("IndexMethodQuery always has an index_method attachment");
            if !attachment.definition().results_materialized {
                break 'requires Some(DmlSafetyReason::IndexMethodNotMaterialized);
            }
        }

        // Check if there are UPDATE triggers
        let updated_cols: ColumnMask = plan
            .set_clauses
            .iter()
            .map(|set_clause| set_clause.column_index)
            .try_collect()?;
        let database_id = table_ref.database_id;
        if has_triggers_including_temp(
            resolver,
            database_id,
            TriggerEvent::Update,
            Some(&updated_cols),
            btree_table,
        ) {
            break 'requires Some(DmlSafetyReason::Trigger);
        }

        // FK cascading actions on the target's parent key may fire writes on
        // other tables (CASCADE / SET NULL / SET DEFAULT). Those writes can in
        // turn fire triggers that mutate the target table while the UPDATE
        // scan is still iterating it, causing rows to be skipped or visited
        // twice. Self-referential cascades likewise rewrite rows in the
        // target during the scan. Materialize target rowids first to keep
        // the write set stable. (See issue #6460.)
        let referencing_fks = resolver.with_schema(database_id, |s| {
            s.resolved_fks_referencing(&btree_table.name)
        })?;
        for fk in &referencing_fks {
            if matches!(fk.fk.on_update, RefAct::NoAction | RefAct::Restrict) {
                continue;
            }
            if fk.parent_key_may_change(&updated_cols, btree_table)? {
                break 'requires Some(DmlSafetyReason::FkCascade);
            }
        }

        // Any subquery in the WHERE clause is evaluated row-by-row during the
        // UPDATE scan. If the subquery reads a table that the UPDATE could
        // mutate (directly via the target table, or transitively via triggers
        // / FKs), it may observe rows already modified by earlier iterations
        // and produce incorrect results. Detecting all such mutation paths
        // precisely is expensive, so we conservatively materialize target
        // rowids whenever the UPDATE has any WHERE-clause subquery.
        // (See issue #5806.)
        if plan
            .non_from_clause_subqueries
            .iter()
            .any(|sq| sq.origin == SubqueryOrigin::DmlWhere)
        {
            break 'requires Some(DmlSafetyReason::SubqueryInWhere);
        }

        // REPLACE mode requires ephemeral table because REPLACE deletes conflicting rows,
        // which can corrupt the iteration order when iterating via an index.
        if matches!(
            plan.or_conflict,
            Some(turso_parser::ast::ResolveType::Replace)
        ) {
            break 'requires Some(DmlSafetyReason::ReplaceMode);
        }

        let rowid_alias_used = plan.set_clauses.iter().any(|set_clause| {
            set_clause.column_index != ROWID_SENTINEL
                && btree_table.columns()[set_clause.column_index].is_rowid_alias()
        });
        let direct_rowid_update = plan
            .set_clauses
            .iter()
            .any(|set_clause| set_clause.column_index == ROWID_SENTINEL);
        if rowid_alias_used || direct_rowid_update {
            break 'requires Some(DmlSafetyReason::KeyMutation);
        }

        let Some(index) = table_ref.op.index() else {
            break 'requires None;
        };

        let affected_cols = btree_table.columns_affected_by_update(&updated_cols)?;
        for c in index.columns.iter() {
            if let Some(ref expr) = c.expr {
                let expr_idx_cols_mask =
                    expression_index_column_usage(expr.as_ref(), table_ref, resolver)?;
                if expr_idx_cols_mask
                    .iter()
                    .any(|cidx| affected_cols.get(cidx))
                {
                    break 'requires Some(DmlSafetyReason::KeyMutation);
                }
            } else if affected_cols.get(c.pos_in_table) {
                break 'requires Some(DmlSafetyReason::KeyMutation);
            }
        }
        break 'requires None;
    };

    Ok(reason)
}

fn collect_subquery_ids_from_exprs<'a>(
    exprs: impl IntoIterator<Item = &'a ast::Expr>,
) -> Result<BitSet<turso_parser::ast::TableInternalId>> {
    use crate::translate::expr::walk_expr;
    use crate::translate::expr::WalkControl;

    let mut ids = BitSet::<turso_parser::ast::TableInternalId>::default();
    let mut collector = |e: &ast::Expr| -> Result<WalkControl> {
        if let ast::Expr::SubqueryResult { subquery_id, .. } = e {
            ids.set(*subquery_id)?;
        }
        Ok(WalkControl::Continue)
    };
    for expr in exprs {
        walk_expr(expr, &mut collector)?;
    }
    Ok(ids)
}

/// Collect SubqueryResult IDs referenced in SET clause and RETURNING expressions.
/// These subqueries must stay in the main update plan (evaluated during the update phase),
/// not be moved to the ephemeral plan (which only collects rowids).
fn collect_update_phase_subquery_ids(
    plan: &UpdatePlan,
) -> Result<BitSet<turso_parser::ast::TableInternalId>> {
    let mut ids = collect_subquery_ids_from_exprs(
        plan.set_clauses
            .iter()
            .map(|set_clause| set_clause.expr.as_ref()),
    )?;
    ids.union_with(&collect_subquery_ids_from_exprs(
        plan.returning
            .iter()
            .flat_map(|returning| returning.iter().map(|column| &column.expr)),
    )?)?;
    Ok(ids)
}

fn update_from_scratch_col_name(idx: usize) -> String {
    format!("__update_from_{idx}")
}

fn update_from_scratch_columns(set_clause_count: usize) -> Result<crate::alloc::Vec<Column>> {
    Ok((0..set_clause_count)
        .map(|idx| {
            // Keep scratch-table columns at BLOB affinity so materializing SET payloads
            // does not coerce values before the real target-column affinity is applied.
            Column::new(
                Some(update_from_scratch_col_name(idx)),
                "BLOB".to_string(),
                None,
                None,
                Type::Blob,
                None,
                ColDef::default(),
            )
        })
        .try_collect()?)
}

/// Build the SELECT that gathers the stable write set for an UPDATE before the
/// mutating write loop runs.
///
/// Plain UPDATE materializes only target rowids. `UPDATE ... FROM` materializes
/// both the chosen SET payloads and the target rowid, using the FROM-side graph
/// plus the target table as the read-side SELECT source.
fn build_update_write_set_plan(
    program: &mut ProgramBuilder,
    plan: &mut UpdatePlan,
    update_from_set_result_columns: Vec<ResultSetColumn>,
) -> Result<()> {
    let scratch_table_id = program.table_reference_counter.next();
    let is_update_from = !plan.from_tables.joined_tables().is_empty();
    let columns = if is_update_from {
        update_from_scratch_columns(plan.set_clauses.len())?
    } else {
        std::iter::once((*ROWID_COLUMN).clone()).try_collect()?
    };
    let ephemeral_table = Arc::new(BTreeTable::new(
        0, // root_page, not relevant for ephemeral table definition
        "ephemeral_scratch".to_string(),
        crate::alloc::vec![],
        columns,
        BTreeCharacteristics::HAS_ROWID,
        crate::alloc::vec![],
        crate::alloc::vec![],
        crate::alloc::vec![],
        None,
    ));

    let temp_cursor_id = program.alloc_cursor_id_keyed(
        CursorKey::table(scratch_table_id),
        CursorType::BTreeTable(ephemeral_table.clone()),
    );

    let write_set_tables = if is_update_from {
        let mut from_tables = plan.from_tables.clone();
        let mut target_table = plan.target_table.clone();
        target_table.join_info = Some(JoinInfo {
            join_type: JoinType::Inner,
            using: vec![],
            no_reorder: false,
        });
        from_tables.add_joined_table(target_table);
        from_tables
    } else {
        TableReferences::new(
            vec![plan.target_table.clone()],
            plan.from_tables.outer_query_refs().to_vec(),
        )
    };
    let rowid_internal_id = plan.target_table.internal_id;

    let mut result_columns = update_from_set_result_columns;
    result_columns.push(ResultSetColumn {
        expr: Expr::RowId {
            database: None,
            table: rowid_internal_id,
        },
        alias: None,
        implicit_column_name: None,
        contains_aggregates: false,
    });

    let join_order = default_join_order(&write_set_tables);
    let write_set_select = SelectPlan {
        table_references: write_set_tables,
        result_columns,
        where_clause: std::mem::take(&mut plan.where_clause),
        group_by: None,     // N/A
        order_by: vec![],   // N/A
        aggregates: vec![], // N/A
        limit: None,        // N/A
        query_destination: QueryDestination::EphemeralTable {
            cursor_id: temp_cursor_id,
            table: ephemeral_table,
            rowid_mode: EphemeralRowidMode::FromResultColumns,
        },
        join_order,
        offset: None,
        contains_constant_false_condition: false,
        distinctness: super::plan::Distinctness::NonDistinct,
        values: vec![],
        window: None,
        input_cardinality_hint: None,
        estimated_output_rows: None,
        estimated_cost: None,
        // For regular UPDATEs, only WHERE-clause subqueries move into the ephemeral plan.
        // For UPDATE ... FROM, SET expressions become part of the ephemeral SELECT payload,
        // so their subqueries move too.
        // RETURNING subqueries always remain in the main update plan.
        non_from_clause_subqueries: {
            let ids_to_keep_in_main_plan = if is_update_from {
                collect_subquery_ids_from_exprs(
                    plan.returning
                        .iter()
                        .flat_map(|returning| returning.iter().map(|column| &column.expr)),
                )?
            } else {
                collect_update_phase_subquery_ids(plan)?
            };
            let mut ephemeral_subs = Vec::new();
            let mut remaining = Vec::new();
            for mut sq in plan.non_from_clause_subqueries.drain(..) {
                if ids_to_keep_in_main_plan.get(sq.internal_id) {
                    remaining.push(sq);
                } else {
                    if is_update_from && sq.origin == SubqueryOrigin::DmlSet {
                        sq.eval_phase = SubqueryEvalPhase::BeforeLoop;
                    }
                    ephemeral_subs.push(sq);
                }
            }
            plan.non_from_clause_subqueries = remaining;
            ephemeral_subs
        },
        simple_aggregate: None,
        phantom_params: vec![],
    };

    plan.write_set_plan = Some(WriteSetPlan {
        select: write_set_select,
        scratch_table_id,
    });

    if is_update_from {
        // For UPDATE ... FROM, the SET expression payloads are materialized, so they are direct
        // column references to the scratch table.
        for (idx, set_clause) in plan.set_clauses.iter_mut().enumerate() {
            set_clause.update_from_result = Some(Box::new(Expr::Column {
                database: None,
                table: scratch_table_id,
                column: idx,
                is_rowid_alias: false,
            }));
        }
    }

    Ok(())
}

fn default_join_order(table_references: &TableReferences) -> Vec<JoinOrderMember> {
    table_references
        .joined_tables()
        .iter()
        .enumerate()
        .map(|(i, t)| JoinOrderMember {
            table_id: t.internal_id,
            original_idx: i,
            is_outer: t
                .join_info
                .as_ref()
                .is_some_and(|join_info| join_info.is_outer()),
        })
        .collect()
}

fn update_from_set_result_columns(set_clauses: &[UpdateSetClause]) -> Vec<ResultSetColumn> {
    set_clauses
        .iter()
        .enumerate()
        .map(|(idx, set_clause)| ResultSetColumn {
            expr: set_clause.expr.as_ref().clone(),
            alias: Some(update_from_scratch_col_name(idx)),
            implicit_column_name: None,
            contains_aggregates: false,
        })
        .collect()
}

fn optimize_subqueries(
    plan: &mut SelectPlan,
    resolver: &Resolver,
    cache: &mut SubqueryPlanCache,
    save_plans: bool,
) -> Result<()> {
    for table in plan.table_references.joined_tables_mut() {
        if let Table::FromClauseSubquery(from_clause_subquery) = &mut table.table {
            let from_clause_subquery = Arc::make_mut(from_clause_subquery);
            if let Some(cached) = cache.from_clause.remove(&table.internal_id) {
                from_clause_subquery.plan = Box::new(cached);
                continue;
            }
            // Use match to handle both SelectPlan and CompoundSelect variants
            match from_clause_subquery.plan.as_mut() {
                Plan::Select(select_plan) => {
                    optimize_select_plan_with_cache(select_plan, resolver, cache)?
                }
                Plan::CompoundSelect {
                    left, right_most, ..
                } => {
                    optimize_select_plan_with_cache(right_most, resolver, cache)?;
                    for (select_plan, _) in left {
                        optimize_select_plan_with_cache(select_plan, resolver, cache)?;
                    }
                }
                Plan::RecursiveCte(recursive_cte) => {
                    optimize_recursive_cte_query_with_cache(
                        &mut recursive_cte.initial_query,
                        resolver,
                        cache,
                    )?;
                    optimize_recursive_cte_query_with_cache(
                        &mut recursive_cte.recursive_query,
                        resolver,
                        cache,
                    )?;
                }
                Plan::Delete(_) | Plan::Update(_) => {
                    turso_soft_unreachable!(
                        "DELETE/UPDATE plans should not appear in FROM clause subqueries"
                    );
                    return Err(LimboError::InternalError(
                        "DELETE/UPDATE plans should not appear in FROM clause subqueries"
                            .to_string(),
                    ));
                }
            }
            if save_plans {
                cache.from_clause.insert(
                    table.internal_id,
                    from_clause_subquery.plan.as_ref().clone(),
                );
            }
        }
    }

    Ok(())
}

/// Plan each correlated subquery with its expected number of calls.
fn plan_correlated_subqueries(
    plan: &mut SelectPlan,
    resolver: &Resolver,
    subquery_calls: &[(TableInternalId, f64)],
    cache: &mut SubqueryPlanCache,
    save_plans: bool,
) -> Result<()> {
    for subquery in &mut plan.non_from_clause_subqueries {
        // Write statements plan their subqueries while the statement is built.
        // Planning them again can reuse changed fields such as an ORDER BY that
        // the first plan removed.
        if !subquery.correlated || subquery.origin.is_write_statement() {
            continue;
        }
        let call_count = subquery_calls
            .iter()
            .find_map(|(id, calls)| (*id == subquery.internal_id).then_some(*calls))
            .unwrap_or_else(|| plan.input_cardinality_hint.unwrap_or(1.0))
            .max(1.0);
        let SubqueryState::Unevaluated {
            plan: Some(inner_plan),
        } = &mut subquery.state
        else {
            continue;
        };
        let key = (subquery.internal_id, call_count.to_bits());
        if let Some(cached) = cache.correlated.remove(&key) {
            **inner_plan = cached;
            continue;
        }
        optimize_plan_for_calls(inner_plan, resolver, call_count, cache)?;
        if save_plans {
            cache.correlated.insert(key, inner_plan.as_ref().clone());
        }
    }

    Ok(())
}

/// Set how many times a plan will run, then plan its table reads again.
fn optimize_plan_for_calls(
    plan: &mut Plan,
    resolver: &Resolver,
    call_count: f64,
    cache: &mut SubqueryPlanCache,
) -> Result<()> {
    let mut optimize = |plan: &mut SelectPlan| -> Result<()> {
        if plan
            .input_cardinality_hint
            .is_some_and(|hint| hint >= call_count)
        {
            return Ok(());
        }
        plan.input_cardinality_hint = Some(call_count);
        optimize_select_plan_with_cache(plan, resolver, cache)
    };

    match plan {
        Plan::Select(plan) => optimize(plan),
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            for (plan, _) in left {
                optimize(plan)?;
            }
            optimize(right_most)
        }
        Plan::RecursiveCte(_) | Plan::Delete(_) | Plan::Update(_) => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
fn optimize_table_access_with_custom_modules(
    result_columns: &mut [ResultSetColumn],
    table_references: &mut TableReferences,
    available_indexes: &AvailableIndexes,
    where_query: &mut [WhereTerm],
    order_by: &mut Vec<(
        Box<ast::Expr>,
        SortOrder,
        Option<turso_parser::ast::NullsOrder>,
    )>,
    group_by: &mut Option<GroupBy>,
    limit: &mut Option<Box<Expr>>,
    offset: &mut Option<Box<Expr>>,
) -> Result<bool> {
    let tables = table_references.joined_tables_mut();
    if tables.is_empty() {
        return Ok(false);
    }

    // group by is not supported for now
    if group_by.is_some() {
        return Ok(false);
    }

    // Only optimize the first table with custom index methods.
    // This allows FTS to be used as the driving table in joins.
    let table = &mut tables[0];
    let Some(indexes) = available_indexes.indexes_for_table(table.internal_id) else {
        return Ok(false);
    };
    for index in indexes {
        let Some(module) = &index.index_method else {
            continue;
        };
        if index.is_backing_btree_index() {
            continue;
        }
        let definition = module.definition();
        for (pattern_idx, pattern) in definition.patterns.iter().enumerate() {
            let Some(pattern_match) = try_match_index_method_pattern(
                pattern,
                table,
                where_query,
                order_by,
                limit,
                offset,
                pattern_idx,
                false, // panic on binding failures
            ) else {
                continue;
            };

            // Mark WHERE clause as consumed
            if let Some(where_covered) = pattern_match.where_covered {
                where_query[where_covered].consumed = true;
            }

            // Build covered columns mapping and update result_columns.
            // This differs from collect_index_method_candidates: we modify result_columns
            // and increment covered_column_id per matching query column, not per pattern column.
            let mut covered_column_id = 1_000_000;
            let mut covered_columns = HashMap::default();
            for (pattern_column_id, pattern_column) in
                pattern_match.pattern_columns.iter().enumerate()
            {
                let ast::ResultColumn::Expr(pattern_expr, _) = pattern_column else {
                    continue;
                };
                let Some(substituted) =
                    try_substitute_parameters(pattern_expr, &pattern_match.parameters)
                else {
                    continue;
                };
                for query_column in result_columns.iter_mut() {
                    if !exprs_are_equivalent(&query_column.expr, &substituted) {
                        continue;
                    }
                    query_column.expr = ast::Expr::Column {
                        database: None,
                        table: table.internal_id,
                        column: covered_column_id,
                        is_rowid_alias: false,
                    };
                    covered_columns.insert(covered_column_id, pattern_column_id);
                    covered_column_id += 1;
                }
            }

            // Calculate whether WHERE is completely covered for ORDER BY/LIMIT clearing
            let where_covered_completely = where_query.is_empty()
                || (pattern_match.where_covered.is_some() && where_query.len() == 1);

            // Only clear ORDER BY/LIMIT/OFFSET if:
            // 1. The pattern explicitly handles them (has ORDER BY/LIMIT), AND
            // 2. WHERE is completely covered (no post-filtering needed)
            // Otherwise, keep them so they're applied after post-filtering
            if pattern_match.pattern_has_order_by && where_covered_completely {
                let _ = order_by.drain(..);
            }
            if pattern_match.pattern_has_limit && where_covered_completely {
                let _ = limit.take();
                let _ = offset.take();
            }

            // Sort and collect arguments
            let arguments = sorted_arguments_from_parameters(&pattern_match.parameters);

            table.op = Operation::IndexMethodQuery(IndexMethodQuery {
                index: index.clone(),
                pattern_idx: pattern_match.pattern_idx,
                covered_columns,
                arguments,
            });
            return Ok(true);
        }
    }
    Ok(false)
}

/// We do a single pass over projected, grouping, filtering, and ordering expressions to
/// capture every expression that could be served directly from an expression index.
/// Example:
///   CREATE INDEX idx ON t(lower(a));
///   SELECT lower(a) FROM t WHERE lower(a) ORDER BY lower(a);
/// Both the SELECT list, WHERE, and ORDER BY can be covered by idx, avoiding a
/// table cursor entirely. Recording them upfront lets both the cost model
/// and covering checks reuse the same facts.
fn register_index_expression_usages_for_plan(
    table_references: &mut TableReferences,
    result_columns: &[ResultSetColumn],
    order_by: &[(
        Box<ast::Expr>,
        SortOrder,
        Option<turso_parser::ast::NullsOrder>,
    )],
    group_by: Option<&GroupBy>,
    where_clause: &mut [WhereTerm],
) {
    table_references.reset_expression_index_usages();

    for rc in result_columns {
        table_references.register_expression_index_usage(&rc.expr);
    }
    for (expr, _, _) in order_by {
        table_references.register_expression_index_usage(expr);
    }
    for where_term in where_clause {
        table_references.register_expression_index_usage(&where_term.expr);
    }

    if let Some(group_by) = group_by {
        for expr in &group_by.exprs {
            table_references.register_expression_index_usage(expr);
        }
        if let Some(having) = &group_by.having {
            for expr in having {
                table_references.register_expression_index_usage(expr);
            }
        }
    }
}

/// Derive a base row-count estimate for a table, preferring ANALYZE stats.
fn base_row_estimate(
    schema: &Schema,
    table: &JoinedTable,
    params: &cost_params::CostModelParams,
) -> RowCountEstimate {
    match &table.table {
        Table::BTree(btree) => {
            if let Some(stats) = schema.analyze_stats.table_stats(&btree.name) {
                if let Some(rows) = stats.row_count.or_else(|| {
                    stats
                        .index_stats
                        .values()
                        .find_map(|idx_stat| idx_stat.total_rows)
                }) {
                    return RowCountEstimate::AnalyzeStats(rows as f64);
                }
            }
            RowCountEstimate::hardcoded_fallback(params)
        }
        Table::FromClauseSubquery(subquery) => match subquery.plan.as_ref() {
            Plan::Select(plan) => {
                if let Some(rows) = plan.estimated_output_rows {
                    return RowCountEstimate::AnalyzeStats(rows.max(1.0));
                }
                RowCountEstimate::hardcoded_fallback(params)
            }
            Plan::CompoundSelect {
                left, right_most, ..
            } => {
                // Combine estimates from all branches according to set operation semantics.
                // left = [(A, op1), (B, op2)], right_most = C
                // represents: (A op1 B) op2 C
                // We fold left-to-right: seed with A, then apply op_i with plan_{i+1}.
                let fallback = *RowCountEstimate::hardcoded_fallback(params);
                let est = |p: &SelectPlan| p.estimated_output_rows.unwrap_or(fallback);
                let mut combined = left.first().map_or(
                    right_most.estimated_output_rows.unwrap_or(fallback),
                    |(p, _)| est(p),
                );
                // The estimates to the right of each operator: left[1..].est, then right_most.est
                let rhs_estimates = left
                    .iter()
                    .skip(1)
                    .map(|(p, _)| est(p))
                    .chain(std::iter::once(est(right_most)));
                for ((_, op), rhs) in left.iter().zip(rhs_estimates) {
                    combined = match op {
                        ast::CompoundOperator::UnionAll | ast::CompoundOperator::Union => {
                            combined + rhs
                        }
                        ast::CompoundOperator::Intersect => combined.min(rhs),
                        ast::CompoundOperator::Except => combined,
                    };
                }
                RowCountEstimate::AnalyzeStats(combined.max(1.0))
            }
            _ => RowCountEstimate::hardcoded_fallback(params),
        },
        _ => RowCountEstimate::hardcoded_fallback(params),
    }
}

/// Read a group count from ANALYZE for a simple list of table columns.
///
/// For an index that starts with the GROUP BY columns, sqlite_stat1 stores the
/// total row count and the average rows for one key. Dividing them gives the
/// number of groups.
fn group_count_from_analyze(plan: &SelectPlan, group_by: &GroupBy, schema: &Schema) -> Option<f64> {
    let mut table_id = None;
    let mut columns: SmallVec<[usize; 4]> = SmallVec::new();
    for expr in &group_by.exprs {
        let Expr::Column { table, column, .. } = expr else {
            return None;
        };
        if table_id.is_some_and(|id| id != *table) {
            return None;
        }
        table_id = Some(*table);
        columns.push(*column);
    }

    let table_id = table_id?;
    let table = plan
        .table_references
        .joined_tables()
        .iter()
        .find(|table| table.internal_id == table_id)?;
    let btree = table.btree()?;
    let table_stats = schema.analyze_stats.table_stats(&btree.name)?;

    table_stats
        .index_stats
        .iter()
        .filter_map(|(index_name, index_stats)| {
            let index = schema.get_index(&btree.name, index_name)?;
            if index.where_clause.is_some() || index.columns.len() < columns.len() {
                return None;
            }
            let columns_match =
                index
                    .columns
                    .iter()
                    .zip(&columns)
                    .all(|(index_column, group_column)| {
                        index_column.expr.is_none() && index_column.pos_in_table == *group_column
                    });
            if !columns_match {
                return None;
            }
            let total_rows = index_stats.total_rows? as f64;
            let rows_per_group = *index_stats
                .avg_rows_per_distinct_prefix
                .get(columns.len().checked_sub(1)?)? as f64;
            (rows_per_group > 0.0).then_some(total_rows / rows_per_group)
        })
        .fold(None, |best: Option<f64>, groups| {
            Some(best.map_or(groups, |best| best.max(groups)))
        })
}

/// Estimate the rows returned by a SELECT after grouping or an aggregate.
fn estimate_select_output_rows(plan: &SelectPlan, input_rows: f64, schema: &Schema) -> f64 {
    let calls = plan.input_cardinality_hint.unwrap_or(1.0);
    let Some(group_by) = &plan.group_by else {
        return if plan.aggregates.is_empty() {
            input_rows
        } else {
            calls
        };
    };
    if group_by.exprs.is_empty() {
        return calls;
    }
    if let Some(groups) = group_count_from_analyze(plan, group_by, schema) {
        return input_rows.min(calls * groups.max(1.0));
    }

    let mut table_ids: SmallVec<[TableInternalId; 2]> = SmallVec::new();
    for expr in &group_by.exprs {
        crate::translate::expr::walk_expr(expr, &mut |expr| -> Result<
            crate::translate::expr::WalkControl,
        > {
            let table_id = match expr {
                Expr::Column { table, .. } | Expr::RowId { table, .. } => Some(*table),
                _ => None,
            };
            if let Some(table_id) = table_id {
                if !table_ids.contains(&table_id) {
                    table_ids.push(table_id);
                }
            }
            Ok(crate::translate::expr::WalkControl::Continue)
        })
        .expect("walking a GROUP BY expression cannot fail");
    }
    if table_ids.is_empty() {
        return calls;
    }

    #[cfg(feature = "optimizer_params")]
    let params: &cost_params::CostModelParams = &cost_params::LOADED_PARAMS;
    #[cfg(not(feature = "optimizer_params"))]
    let params: &cost_params::CostModelParams = &cost_params::DEFAULT_PARAMS;

    let rows_per_call = table_ids.iter().try_fold(1.0, |rows, table_id| {
        let table = plan
            .table_references
            .joined_tables()
            .iter()
            .find(|table| table.internal_id == *table_id)?;
        Some(rows * *base_row_estimate(schema, table, params))
    });
    rows_per_call.map_or(input_rows, |rows| input_rows.min(calls * rows))
}

/// Returns true if a WHERE-term predicate is null-rejecting for a table: the
/// term can never be TRUE when every column of the table is NULL. On a row an
/// outer join null-extended, every column of the table IS NULL, so such a
/// term filters that row out — the join then behaves like an inner join and
/// can be rewritten to one.
///
/// Port of SQLite's `impliesNotNullRow` (expr.c). Conservative: returns false
/// when unsure. `IS`, `IS NOT` and `IS NULL` tests, functions, CASE and row
/// values can all turn NULL inputs into TRUE (or into non-NULL values a
/// comparison then accepts), so nothing below them counts. A comparison or an
/// arithmetic expression yields NULL when an input is NULL, so for those it
/// is enough that one input mentions the table.
fn where_term_is_null_rejecting_for_table(
    expr: &ast::Expr,
    table_id: ast::TableInternalId,
) -> bool {
    use ast::Operator::*;
    let rejects = |e: &ast::Expr| where_term_is_null_rejecting_for_table(e, table_id);
    match expr {
        ast::Expr::Column { table, .. } | ast::Expr::RowId { table, .. } => *table == table_id,

        // These can be TRUE (or produce a non-NULL value) even when every
        // input is NULL, so nothing below them proves anything.
        ast::Expr::Binary(_, Is | IsNot, _)
        | ast::Expr::IsNull(_)
        | ast::Expr::NotNull(_)
        | ast::Expr::Case { .. }
        | ast::Expr::FunctionCall { .. }
        | ast::Expr::FunctionCallStar { .. }
        | ast::Expr::Like { .. } => false,

        // Both arms must reject on their own: `x OR y` can be TRUE through
        // the other arm, and under a NOT so can `x AND y`. (Top-level WHERE
        // terms are already split on AND, so an AND here sits under a NOT or
        // inside parentheses, where the polarity is unknown.)
        ast::Expr::Binary(lhs, And | Or, rhs) => rejects(lhs) && rejects(rhs),

        // A comparison with a NULL input is never TRUE, and an arithmetic or
        // concatenation result with a NULL input is NULL. Either side counts.
        ast::Expr::Binary(lhs, _, rhs) => rejects(lhs) || rejects(rhs),

        // NULL is never in a list. (An empty list has no such guarantee:
        // `x NOT IN ()` is TRUE for NULL x.)
        ast::Expr::InList {
            lhs,
            rhs: in_list_values,
            ..
        } => !in_list_values.is_empty() && rejects(lhs),

        // `x NOT BETWEEN y AND z` can be TRUE for NULL x only when y or z is
        // NULL too, so it is enough that x rejects, or both bounds do.
        ast::Expr::Between {
            lhs, start, end, ..
        } => rejects(lhs) || (rejects(start) && rejects(end)),

        // NULL-propagating wrappers.
        ast::Expr::Unary(_, inner) | ast::Expr::Cast { expr: inner, .. } => rejects(inner),
        ast::Expr::Collate(inner, _) => rejects(inner),
        // A single-element parenthesized expression is just grouping; a
        // row value (more elements) is compared element-wise and can be
        // TRUE with a NULL element.
        ast::Expr::Parenthesized(exprs) => exprs.len() == 1 && rejects(&exprs[0]),

        // Literals, parameters, subqueries, and anything else: no proof.
        _ => false,
    }
}

/// Enforce INDEXED BY / NOT INDEXED hints by validating index existence and
/// filtering constraint candidates accordingly.
fn enforce_indexed_by_hints(
    table_references: &TableReferences,
    available_indexes: &AvailableIndexes,
    where_clause: &[WhereTerm],
    simple_aggregate: Option<&SimpleAggregate>,
    constraints_per_table: &mut [TableConstraints],
) -> Result<()> {
    for (i, table_ref) in table_references.joined_tables().iter().enumerate() {
        let Some(ref indexed) = table_ref.indexed else {
            continue;
        };
        if table_ref.btree().is_none() {
            continue;
        }
        let Some(cs) = constraints_per_table.get_mut(i) else {
            continue;
        };
        match indexed {
            ast::Indexed::IndexedBy(name) => {
                let idx_name = name.as_str();
                // Verify the index exists and belongs to this table.
                let forced_index =
                    available_indexes.btree_index_by_name(table_ref.internal_id, idx_name);
                let Some(forced_index) = forced_index else {
                    crate::bail_parse_error!("no such index: {}", idx_name);
                };
                let forced_partial_index_unusable = forced_index.where_clause.is_some()
                    && !can_use_partial_index(forced_index.as_ref(), table_ref, where_clause);
                if forced_partial_index_unusable
                    && matches!(simple_aggregate, Some(SimpleAggregate::Count))
                {
                    // SQLite's isSimpleCount() path emits OP_Count without
                    // invoking sqlite3WhereBegin(), so a forced unusable
                    // partial index is ignored here instead of causing
                    // "no query solution".
                    cs.candidates.retain(|c| c.index.is_none());
                    continue;
                }
                if forced_partial_index_unusable {
                    crate::bail_parse_error!("no query solution");
                }
                // Keep only the candidate for the forced index.
                let forced_index = forced_index.clone();
                cs.candidates.retain(|c| {
                    c.index
                        .as_ref()
                        .is_some_and(|idx| Arc::ptr_eq(idx, &forced_index))
                });
                // If no candidate survived (no WHERE constraints matched), add an empty one
                // so the optimizer can still scan the index.
                if cs.candidates.is_empty() {
                    cs.candidates.push(ConstraintUseCandidate {
                        index: Some(forced_index),
                        refs: Vec::new(),
                    });
                }
            }
            ast::Indexed::NotIndexed => {
                // Remove all secondary index candidates, keep only rowid.
                cs.candidates.retain(|c| c.index.is_none());
            }
        }
    }
    Ok(())
}

/// Choose table reads and write them into the query plan.
#[allow(clippy::too_many_arguments)]
fn optimize_table_access(
    schema: &Schema,
    resolver: &Resolver,
    result_columns: &mut [ResultSetColumn],
    table_references: &mut TableReferences,
    available_indexes: &AvailableIndexes,
    where_clause: &mut Vec<WhereTerm>,
    order_by: &mut Vec<(
        Box<ast::Expr>,
        SortOrder,
        Option<turso_parser::ast::NullsOrder>,
    )>,
    group_by: &mut Option<GroupBy>,
    simple_aggregate: Option<&SimpleAggregate>,
    subqueries: &[NonFromClauseSubquery],
    limit: &mut Option<Box<Expr>>,
    offset: &mut Option<Box<Expr>>,
    initial_input_cardinality: f64,
) -> Result<Option<Vec<JoinOrderMember>>> {
    let Some(plan) = find_table_access_plan(
        schema,
        result_columns,
        table_references,
        available_indexes,
        where_clause,
        order_by,
        group_by,
        simple_aggregate,
        subqueries,
        limit,
        offset,
        initial_input_cardinality,
        None,
    )?
    else {
        return Ok(None);
    };

    Ok(Some(apply_table_access_plan(
        resolver,
        table_references,
        where_clause,
        order_by,
        group_by,
        plan,
    )?))
}

/// Find the best table reads without writing them into table operations.
#[allow(clippy::too_many_arguments)]
fn find_table_access_plan(
    schema: &Schema,
    result_columns: &mut [ResultSetColumn],
    table_references: &mut TableReferences,
    available_indexes: &AvailableIndexes,
    where_clause: &mut Vec<WhereTerm>,
    order_by: &mut Vec<(
        Box<ast::Expr>,
        SortOrder,
        Option<turso_parser::ast::NullsOrder>,
    )>,
    group_by: &mut Option<GroupBy>,
    simple_aggregate: Option<&SimpleAggregate>,
    subqueries: &[NonFromClauseSubquery],
    limit: &mut Option<Box<Expr>>,
    offset: &mut Option<Box<Expr>>,
    initial_input_cardinality: f64,
    cost_limit: Option<Cost>,
) -> Result<Option<TableAccessPlan>> {
    // When optimizer_params feature is enabled, use lazily-loaded params (cached process-wide).
    // Otherwise, use the compile-time static for zero overhead.
    #[cfg(feature = "optimizer_params")]
    let params: &cost_params::CostModelParams = &cost_params::LOADED_PARAMS;
    #[cfg(not(feature = "optimizer_params"))]
    let params: &cost_params::CostModelParams = &cost_params::DEFAULT_PARAMS;

    if table_references.joined_tables().is_empty() {
        return Ok(None);
    }
    if table_references.joined_tables().len() > TableReferences::MAX_JOINED_TABLES {
        crate::bail_parse_error!(
            "Only up to {} tables can be joined",
            TableReferences::MAX_JOINED_TABLES
        );
    }

    let has_expression_idx_or_partial_idx = table_references.joined_tables().iter().any(|t| {
        matches!(&t.table, Table::BTree(_) if available_indexes
            .indexes_for_table(t.internal_id)
            .is_some_and(|indexes| indexes.iter().any(|index| index.is_expression_index() || index.where_clause.is_some())))
    });

    if has_expression_idx_or_partial_idx {
        register_index_expression_usages_for_plan(
            table_references,
            result_columns,
            order_by.as_slice(),
            group_by.as_ref(),
            where_clause,
        );
    }

    // For single-table queries, try to optimize with custom index methods directly.
    // This is the fast path that preserves the original behavior.
    // Skip when INDEXED BY / NOT INDEXED is specified — those force a specific btree index or table scan.
    let is_single_table = table_references.joined_tables().len() == 1;
    let has_indexed_by_hint = table_references
        .joined_tables()
        .iter()
        .any(|t| t.indexed.is_some());
    if is_single_table && !has_indexed_by_hint {
        let optimized = optimize_table_access_with_custom_modules(
            result_columns,
            table_references,
            available_indexes,
            where_clause,
            order_by,
            group_by,
            limit,
            offset,
        )?;
        if optimized {
            return Ok(None);
        }
    }

    let mut access_methods_arena = Vec::new();

    // For multi-table queries, collect index method candidates to pass to the DP algorithm.
    // This allows the optimizer to consider index methods at any position in the join order.
    let base_table_rows = table_references
        .joined_tables()
        .iter()
        .map(|t| base_row_estimate(schema, t, params))
        .collect::<Vec<_>>();

    let index_method_candidates = if !is_single_table {
        collect_index_method_candidates(
            table_references,
            available_indexes,
            where_clause,
            order_by,
            group_by,
            limit,
            offset,
            &base_table_rows,
            params,
        )?
    } else {
        Vec::new()
    };
    let maybe_order_target = simple_aggregate
        .and_then(|sa| simple_aggregate_order_target(sa, table_references))
        .or_else(|| compute_order_target(order_by, group_by.as_mut(), table_references));
    // Currently the expressions we evaluate as constraints are binary comparisons that (except for IS/IS NOT)
    // will never be true for a NULL operand.
    // If there are any constraints on the right hand side table of an outer join that are not part of the outer join condition,
    // the outer join can be converted into an inner join.
    // for example:
    // - SELECT * FROM t1 LEFT JOIN t2 ON false WHERE t2.id = 5
    // there can never be a situation where null columns are emitted for t2 because t2.id = 5 will never be true in that case.
    // hence: we can convert the outer join into an inner join.
    //
    // Converting a LEFT JOIN into an INNER JOIN can enable join reordering.
    loop {
        let mut outer_join_rewritten = false;
        for t in table_references.joined_tables_mut().iter_mut().filter(|t| {
            t.join_info
                .as_ref()
                // Skip FULL OUTER JOIN tables: removing `outer` would suppress
                // unmatched-probe-row emission and prevent LeftJoinMetadata
                // allocation needed by the hash join.
                .is_some_and(|join_info| join_info.is_outer() && !join_info.is_full_outer())
        }) {
            // Check if a WHERE term filters out the join's null-extended rows,
            // allowing us to convert the LEFT JOIN into an INNER JOIN for join
            // reordering purposes. This looks at the raw WHERE terms, not the
            // extracted constraints, so terms that never become constraints
            // (like `t.v = 5 OR t.w = 7`) also count.
            if where_clause.iter().any(|term| {
                term.from_outer_join.is_none()
                    && where_term_is_null_rejecting_for_table(&term.expr, t.internal_id)
            }) {
                t.join_info.as_mut().unwrap().join_type = JoinType::Inner;
                for term in where_clause.iter_mut() {
                    if let Some(from_outer_join) = term.from_outer_join {
                        if from_outer_join == t.internal_id {
                            term.from_outer_join = None;
                        }
                    }
                }
                outer_join_rewritten = true;
            }
        }
        if !outer_join_rewritten {
            break;
        }
    }

    add_implied_column_equalities(where_clause, table_references)?;
    let mut constraints_per_table = constraints_from_where_clause(
        where_clause,
        table_references,
        available_indexes,
        subqueries,
        schema,
        params,
    )?;

    // Enforce INDEXED BY / NOT INDEXED after outer-join rewrites settle, because
    // a null-rejecting WHERE term can turn a LEFT JOIN into an INNER JOIN and
    // then safely prove a forced partial index.
    enforce_indexed_by_hints(
        table_references,
        available_indexes,
        where_clause,
        simple_aggregate,
        &mut constraints_per_table,
    )?;

    let planning_context = JoinPlanningContext {
        maybe_order_target: maybe_order_target.as_ref(),
        cost_limit,
    };

    let Some(best_join_order_result) = compute_best_join_order_with_context(
        table_references.joined_tables(),
        initial_input_cardinality,
        planning_context,
        &constraints_per_table,
        &base_table_rows,
        &mut access_methods_arena,
        where_clause,
        subqueries,
        &index_method_candidates,
        params,
        &schema.analyze_stats,
        available_indexes,
        table_references,
        schema,
    )?
    else {
        return Ok(None);
    };

    let BestJoinOrderResult {
        best_plan,
        best_ordered_plan,
    } = best_join_order_result;

    // See if best_ordered_plan is better than the overall best_plan if we add a sorting penalty
    // to the unordered plan's cost.
    let best_plan = if let Some(best_ordered_plan) = best_ordered_plan {
        let best_unordered_plan_cost = best_plan.cost;
        let best_ordered_plan_cost = best_ordered_plan.cost;
        const SORT_COST_PER_ROW_MULTIPLIER: f64 = 0.001;
        let sorting_penalty = Cost(best_plan.output_cardinality * SORT_COST_PER_ROW_MULTIPLIER);
        if best_unordered_plan_cost + sorting_penalty > best_ordered_plan_cost {
            best_ordered_plan
        } else {
            best_plan
        }
    } else {
        best_plan
    };

    let sort_eliminated = maybe_order_target.as_ref().is_some_and(|order_target| {
        plan_satisfies_order_target(
            &best_plan,
            &access_methods_arena,
            table_references.joined_tables(),
            &constraints_per_table,
            order_target,
            schema,
        )
    });
    let subquery_calls = count_subquery_calls_for_plan(
        &best_plan,
        &access_methods_arena,
        &constraints_per_table,
        table_references.joined_tables(),
        where_clause,
        subqueries,
        initial_input_cardinality,
        params,
    )?;

    Ok(Some(TableAccessPlan {
        access_methods: access_methods_arena,
        constraints: constraints_per_table,
        join: best_plan,
        subquery_calls,
        order_target: maybe_order_target,
        sort_eliminated,
        initial_input_rows: initial_input_cardinality,
    }))
}

/// Write chosen table reads into the query plan.
fn apply_table_access_plan(
    resolver: &Resolver,
    table_references: &mut TableReferences,
    where_clause: &mut [WhereTerm],
    order_by: &mut Vec<(
        Box<ast::Expr>,
        SortOrder,
        Option<turso_parser::ast::NullsOrder>,
    )>,
    group_by: &mut Option<GroupBy>,
    plan: TableAccessPlan,
) -> Result<Vec<JoinOrderMember>> {
    let TableAccessPlan {
        access_methods: mut access_methods_arena,
        constraints: constraints_per_table,
        join: best_plan,
        subquery_calls: _,
        order_target: maybe_order_target,
        sort_eliminated,
        initial_input_rows,
    } = plan;

    if sort_eliminated {
        let order_target = maybe_order_target
            .as_ref()
            .expect("a sort can only be removed when the query has an order target");
        match &order_target.purpose {
            OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Group) => {
                if let Some(group) = group_by.as_mut() {
                    group.sort_elided = true;
                }
            }
            OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Order) => {
                order_by.clear();
            }
            OrderTargetPurpose::EliminatesSort(EliminatesSortBy::GroupByAndOrder) => {
                if let Some(group) = group_by.as_mut() {
                    group.sort_elided = true;
                }
                order_by.clear();
            }
            OrderTargetPurpose::Extremum => {}
        }
    }

    let (best_access_methods, best_table_numbers) = (
        best_plan.best_access_methods().collect::<Vec<_>>(),
        best_plan.table_numbers().collect::<Vec<_>>(),
    );

    for table in table_references.joined_tables_mut() {
        table.plan_estimate = None;
    }
    let mut input_rows = initial_input_rows;
    let mut total_cost = 0.0;
    for (position, (&table_idx, &access_method_idx)) in best_table_numbers
        .iter()
        .zip(&best_access_methods)
        .enumerate()
    {
        let access_method = &access_methods_arena[access_method_idx];
        total_cost += access_method.cost.0;
        let output_rows = best_plan.prefix_cardinalities[position];
        table_references.joined_tables_mut()[table_idx].plan_estimate = Some(TablePlanEstimate {
            input_rows,
            rows_per_input: if input_rows == 0.0 {
                0.0
            } else {
                output_rows / input_rows
            },
            output_rows,
            access_cost: access_method.cost.0,
            total_cost,
        });
        input_rows = output_rows;
    }

    // Collect hash join build/probe table indices. Build tables are excluded from the main
    // join order because they are consumed during hash build. A table may appear as both
    // probe and build (probe->build chaining) only when the build input is materialized.
    let (hash_join_build_tables, hash_join_probe_tables): (TableMask, TableMask) =
        best_access_methods
            .iter()
            .filter_map(|&am_idx| {
                let arena = &access_methods_arena;
                arena.get(am_idx).and_then(|am| {
                    if let AccessMethodParams::HashJoin {
                        build_table_idx,
                        probe_table_idx,
                        ..
                    } = &am.params
                    {
                        Some((*build_table_idx, *probe_table_idx))
                    } else {
                        None
                    }
                })
            })
            .try_unzip()?;
    #[cfg(debug_assertions)]
    {
        let mut probe_tables: TableMask = TableMask::default();
        let mut build_tables: HashMap<usize, bool> = HashMap::default();
        let mut pos_by_table: Vec<Option<usize>> =
            vec![None; table_references.joined_tables().len()];
        for (pos, table_idx) in best_table_numbers.iter().enumerate() {
            pos_by_table[*table_idx] = Some(pos);
        }

        for &am_idx in best_access_methods.iter() {
            let arena = &access_methods_arena;
            let Some(am) = arena.get(am_idx) else {
                continue;
            };
            if let AccessMethodParams::HashJoin {
                build_table_idx,
                probe_table_idx,
                materialize_build_input,
                ..
            } = &am.params
            {
                if let (Some(build_pos), Some(probe_pos)) = (
                    pos_by_table[*build_table_idx],
                    pos_by_table[*probe_table_idx],
                ) {
                    turso_assert!(
                        probe_pos == build_pos + 1,
                        "hash join build/probe tables are not adjacent in join order"
                    );
                }
                probe_tables.set(*probe_table_idx)?;
                build_tables.insert(*build_table_idx, *materialize_build_input);
            }
        }

        for (build_table_idx, materialize_build_input) in build_tables {
            if probe_tables.get(build_table_idx) {
                turso_assert!(
                    materialize_build_input,
                    "probe->build chaining requires materialized build input"
                );
            }
        }
    }
    let hash_join_build_only_tables: TableMask = hash_join_build_tables
        .iter()
        .filter(|table_idx| !hash_join_probe_tables.get(*table_idx))
        .try_collect()?;

    let best_join_order: Vec<JoinOrderMember> = best_table_numbers
        .iter()
        .filter(|table_number| {
            !hash_join_build_tables.get(**table_number)
                || hash_join_probe_tables.get(**table_number)
        })
        .map(|&table_number| JoinOrderMember {
            table_id: table_references.joined_tables_mut()[table_number].internal_id,
            original_idx: table_number,
            is_outer: table_references.joined_tables_mut()[table_number]
                .join_info
                .as_ref()
                .is_some_and(|join_info| join_info.is_outer()),
        })
        .collect();

    // Mutate the Operations in `joined_tables` to use the selected access methods.
    // We iterate over ALL tables (including hash join build tables) to set their operations,
    // even though build tables are not in best_join_order.
    for (i, &table_idx) in best_table_numbers.iter().enumerate() {
        // Skip tables that already have an IndexMethodQuery operation set.
        // This happens when the first table was optimized with a custom index (e.g., FTS)
        // and we're continuing to optimize remaining tables in a multi-table query.
        if matches!(
            table_references.joined_tables()[table_idx].op,
            Operation::IndexMethodQuery(_)
        ) {
            continue;
        }
        let access_method = &mut access_methods_arena[best_access_methods[i]];
        match &mut access_method.params {
            AccessMethodParams::BTreeTable {
                iter_dir,
                index,
                build_index,
                constraint_refs,
            } => {
                if *build_index {
                    turso_assert!(index.is_none(), "a new temporary index must not exist yet");
                    let prior_tables: TableMask =
                        best_table_numbers.iter().take(i).copied().try_collect()?;
                    *constraint_refs = constraints::usable_constraints_for_lhs_mask(
                        &constraints_per_table[table_idx].constraints,
                        &constraints_per_table[table_idx].temporary_index_terms,
                        &prior_tables,
                        table_idx,
                    )
                    .into_vec();
                    turso_assert!(
                        !constraint_refs.is_empty(),
                        "a temporary index must have a search key"
                    );
                    *index = Some(Arc::new(ephemeral_index_build(
                        &table_references.joined_tables()[table_idx],
                        table_references,
                        &constraints_per_table[table_idx].constraints,
                        constraint_refs,
                        where_clause,
                        resolver,
                    )?));
                    *build_index = false;
                }
                maybe_remove_index_candidate(
                    index,
                    &table_references.joined_tables()[table_idx],
                    maybe_order_target.as_ref(),
                    sort_eliminated,
                );
                if constraint_refs.is_empty() {
                    if let Some(index) = partial_index(index.as_ref()) {
                        let is_outer_join = table_references.joined_tables()[table_idx]
                            .join_info
                            .as_ref()
                            .is_some_and(|join_info| join_info.is_outer());
                        mark_partial_index_predicate_terms_consumed(
                            index,
                            &table_references.joined_tables()[table_idx],
                            where_clause,
                            is_outer_join,
                        );
                    }
                    table_references.joined_tables_mut()[table_idx].op =
                        Operation::Scan(Scan::BTreeTable {
                            iter_dir: *iter_dir,
                            index: index.clone(),
                        });
                    continue;
                } else {
                    let is_outer_join = table_references.joined_tables()[table_idx]
                        .join_info
                        .as_ref()
                        .is_some_and(|join_info| join_info.is_outer());
                    let defer_cross_table_constraints = hash_join_build_only_tables.get(table_idx);
                    if let Some(index) = partial_index(index.as_ref()) {
                        mark_partial_index_predicate_terms_consumed(
                            index,
                            &table_references.joined_tables()[table_idx],
                            where_clause,
                            is_outer_join,
                        );
                    }
                    mark_seek_constraints_consumed(
                        &constraints_per_table[table_idx].constraints,
                        constraint_refs,
                        where_clause,
                        is_outer_join,
                        defer_cross_table_constraints,
                    );
                    if let Some(index) = &index {
                        table_references.joined_tables_mut()[table_idx].op =
                            Operation::Search(Search::Seek {
                                index: Some(index.clone()),
                                seek_def: build_seek_def_from_constraints(
                                    &constraints_per_table[table_idx].constraints,
                                    constraint_refs,
                                    *iter_dir,
                                    where_clause,
                                    Some(table_references),
                                    Some(resolver),
                                )?,
                            });
                        continue;
                    }
                    turso_assert_eq!(
                        constraint_refs.len(),
                        1,
                        "expected exactly one constraint for rowid seek",
                        {"constraint_refs": format!("{constraint_refs:?}")}
                    );
                    table_references.joined_tables_mut()[table_idx].op =
                        if let Some(ref eq) = constraint_refs[0].eq {
                            Operation::Search(Search::RowidEq {
                                cmp_expr: constraints_per_table[table_idx].constraints
                                    [eq.constraint_pos]
                                    .get_constraining_expr(
                                        where_clause,
                                        Some(table_references),
                                        Some(resolver),
                                    )
                                    .1,
                            })
                        } else {
                            Operation::Search(Search::Seek {
                                index: None,
                                seek_def: build_seek_def_from_constraints(
                                    &constraints_per_table[table_idx].constraints,
                                    constraint_refs,
                                    *iter_dir,
                                    where_clause,
                                    Some(table_references),
                                    Some(resolver),
                                )?,
                            })
                        };
                }
            }
            AccessMethodParams::VirtualTable {
                idx_num,
                idx_str,
                constraints,
                constraint_usages,
            } => {
                table_references.joined_tables_mut()[table_idx].op = build_vtab_scan_op(
                    where_clause,
                    &constraints_per_table[table_idx],
                    idx_num,
                    idx_str,
                    constraints,
                    constraint_usages,
                    Some(table_references),
                )?;
            }
            AccessMethodParams::Subquery { iter_dir } => {
                table_references.joined_tables_mut()[table_idx].op =
                    Operation::Scan(Scan::Subquery {
                        iter_dir: *iter_dir,
                    });
            }
            AccessMethodParams::RecursiveCteInput => {
                table_references.joined_tables_mut()[table_idx].op =
                    Operation::Scan(Scan::RecursiveCteInput);
            }
            AccessMethodParams::MaterializedSubquery {
                index,
                constraint_refs,
                iter_dir,
            } => {
                let table_constraints = constraints_per_table
                    .iter()
                    .find(|c| c.table_id == table_references.joined_tables()[table_idx].internal_id)
                    .expect("should have constraints for this table");

                mark_seek_constraints_consumed(
                    &table_constraints.constraints,
                    constraint_refs,
                    where_clause,
                    false,
                    false,
                );

                // Build seek definition from the constraints
                let seek_def = build_seek_def_from_constraints(
                    &table_constraints.constraints,
                    constraint_refs,
                    *iter_dir,
                    where_clause,
                    Some(table_references),
                    Some(resolver),
                )?;

                table_references.joined_tables_mut()[table_idx].op =
                    Operation::Search(Search::Seek {
                        index: Some(index.clone()),
                        seek_def,
                    });
            }
            AccessMethodParams::HashJoin {
                build_table_idx,
                probe_table_idx,
                join_keys,
                mem_budget,
                materialize_build_input,
                use_bloom_filter,
                join_type,
            } => {
                // Mark WHERE clause terms as consumed since we're using hash join
                for join_key in join_keys.iter() {
                    where_clause[join_key.where_clause_idx].consumed = true;
                }
                // Set up hash join operation on the probe table
                table_references.joined_tables_mut()[table_idx].op =
                    Operation::HashJoin(HashJoinOp {
                        build_table_idx: *build_table_idx,
                        probe_table_idx: *probe_table_idx,
                        join_keys: join_keys.clone(),
                        mem_budget: *mem_budget,
                        materialize_build_input: *materialize_build_input,
                        use_bloom_filter: *use_bloom_filter,
                        join_type: *join_type,
                    });
            }
            AccessMethodParams::IndexMethod {
                query,
                where_covered,
            } => {
                // Mark WHERE clause term as consumed if the index method covered it
                if let Some(idx) = where_covered {
                    where_clause[*idx].consumed = true;
                }
                // Set up the index method query operation
                table_references.joined_tables_mut()[table_idx].op =
                    Operation::IndexMethodQuery(query.clone());
            }
            AccessMethodParams::MultiIndexScan {
                branches,
                where_term_idx,
                set_op,
            } => {
                // Mark the primary WHERE clause term as consumed
                where_clause[*where_term_idx].consumed = true;
                // For intersection, also mark additional consumed terms
                if let SetOperation::Intersection {
                    additional_consumed_terms,
                } = set_op
                {
                    for term_idx in additional_consumed_terms.iter() {
                        where_clause[term_idx].consumed = true;
                    }
                }

                let w_idx = *where_term_idx;
                let s_op = set_op.clone();
                // Build the MultiIndexScanOp from the branch parameters
                let mut multi_idx_branches = Vec::with_capacity(branches.len());
                for branch in std::mem::take(branches) {
                    let access = match branch.access {
                        MultiIndexBranchAccessParams::Seek {
                            constraints,
                            constraint_refs,
                        } => MultiIndexBranchAccess::Seek {
                            seek_def: build_seek_def_from_constraints(
                                &constraints,
                                &constraint_refs,
                                IterationDirection::Forwards, // Multi-index always scans forward
                                where_clause,
                                Some(table_references),
                                Some(resolver),
                            )?,
                        },
                        MultiIndexBranchAccessParams::InSeek { source } => {
                            MultiIndexBranchAccess::InSeek { source }
                        }
                    };
                    multi_idx_branches.push(MultiIndexBranch {
                        index: branch.index,
                        access,
                        estimated_rows: branch.estimated_rows,
                        union_residuals: branch.residuals,
                    });
                }

                table_references.joined_tables_mut()[table_idx].op =
                    Operation::MultiIndexScan(MultiIndexScanOp {
                        branches: multi_idx_branches,
                        where_term_idx: w_idx,
                        set_op: s_op,
                    });
            }
            AccessMethodParams::InSeek {
                index,
                affinity,
                where_term_idx,
            } => {
                let source = match &where_clause[*where_term_idx].expr {
                    Expr::InList { rhs, .. } => {
                        let in_values: Vec<ast::Expr> = rhs.iter().map(|e| *e.clone()).collect();
                        InSeekSource::LiteralList {
                            values: in_values,
                            affinity: *affinity,
                        }
                    }
                    Expr::SubqueryResult {
                        query_type: SubqueryType::In { cursor_id, .. },
                        ..
                    } => InSeekSource::Subquery {
                        cursor_id: *cursor_id,
                    },
                    _ => {
                        return Err(crate::LimboError::InternalError(
                            "InSeek where term is not an InList or SubqueryResult expression"
                                .into(),
                        ));
                    }
                };
                let is_outer_join = table_references.joined_tables()[table_idx]
                    .join_info
                    .as_ref()
                    .is_some_and(|join_info| join_info.is_outer());
                if let Some(index) = partial_index(index.as_ref()) {
                    mark_partial_index_predicate_terms_consumed(
                        index,
                        &table_references.joined_tables()[table_idx],
                        where_clause,
                        is_outer_join,
                    );
                }
                where_clause[*where_term_idx].consumed = true;
                table_references.joined_tables_mut()[table_idx].op =
                    Operation::Search(Search::InSeek {
                        index: index.clone(),
                        source,
                    });
            }
        }
    }

    let mut probe_pos_by_table: Vec<Option<usize>> =
        vec![None; table_references.joined_tables().len()];
    let mut hash_build_by_probe: Vec<Option<usize>> =
        vec![None; table_references.joined_tables().len()];
    for (pos, member) in best_join_order.iter().enumerate() {
        let table = &table_references.joined_tables()[member.original_idx];
        if let Operation::HashJoin(hash_join_op) = &table.op {
            probe_pos_by_table[member.original_idx] = Some(pos);
            hash_build_by_probe[member.original_idx] = Some(hash_join_op.build_table_idx);
        }
    }

    // If hash-join build constraints are still evaluated later (not consumed),
    // avoid materializing the build input to reduce redundant scans.
    for table in table_references.joined_tables_mut().iter_mut() {
        let Operation::HashJoin(hash_join_op) = &mut table.op else {
            continue;
        };
        if !hash_join_op.materialize_build_input {
            continue;
        }
        let Some(probe_pos) = best_join_order
            .iter()
            .position(|member| member.original_idx == hash_join_op.probe_table_idx)
        else {
            continue;
        };
        let build_table_was_prior_probe = probe_pos_by_table
            .get(hash_join_op.build_table_idx)
            .copied()
            .flatten()
            .is_some_and(|pos| pos < probe_pos);
        if build_table_was_prior_probe {
            continue;
        }
        let mut prior_mask: TableMask = best_join_order[..probe_pos]
            .iter()
            .map(|member| member.original_idx)
            .try_collect()?;
        // A hash build table is read through its probe table. Include it when
        // checking which earlier tables can filter this build input.
        for member in &best_join_order[..probe_pos] {
            if let Some(build_table_idx) = hash_build_by_probe[member.original_idx] {
                prior_mask.set(build_table_idx)?;
            }
        }
        let join_key_indices: BitSet = hash_join_op
            .join_keys
            .iter()
            .map(|key| key.where_clause_idx)
            .try_collect()?;
        let build_constraints = &constraints_per_table[hash_join_op.build_table_idx];
        let mut has_prior_constraints = false;
        for constraint in build_constraints.constraints.iter() {
            if !constraint.lhs_mask.intersects(&prior_mask) {
                continue;
            }
            if join_key_indices.get(constraint.where_clause_pos.0) {
                continue;
            }
            has_prior_constraints = true;
            break;
        }
        if !has_prior_constraints {
            hash_join_op.materialize_build_input = false;
        }
    }

    Ok(best_join_order)
}

fn build_vtab_scan_op(
    where_clause: &mut [WhereTerm],
    table_constraints: &TableConstraints,
    idx_num: &i32,
    idx_str: &Option<String>,
    vtab_constraints: &[ConstraintInfo],
    constraint_usages: &[ConstraintUsage],
    referenced_tables: Option<&TableReferences>,
) -> Result<Operation> {
    if constraint_usages.len() != vtab_constraints.len() {
        return Err(LimboError::ExtensionError(format!(
            "Constraint usage count mismatch (expected {}, got {})",
            vtab_constraints.len(),
            constraint_usages.len()
        )));
    }

    let mut constraints = vec![None; constraint_usages.len()];
    let mut arg_count = 0;

    for (i, vtab_constraint) in vtab_constraints.iter().enumerate() {
        let usage = constraint_usages[i];
        let argv_index = match usage.argv_index {
            Some(idx) if idx >= 1 && (idx as usize) <= constraint_usages.len() => idx,
            Some(idx) => {
                return Err(LimboError::ExtensionError(format!(
                    "argv_index {} is out of valid range [1..{}]",
                    idx,
                    constraint_usages.len()
                )));
            }
            None => continue,
        };

        let zero_based_argv_index = (argv_index - 1) as usize;
        if constraints[zero_based_argv_index].is_some() {
            return Err(LimboError::ExtensionError(format!(
                "duplicate argv_index {argv_index}"
            )));
        }

        let constraint = &table_constraints.constraints[vtab_constraint.index];
        if usage.omit {
            where_clause[constraint.where_clause_pos.0].consumed = true;
        }
        let (_, expr, _) = constraint.get_constraining_expr(where_clause, referenced_tables, None);
        constraints[zero_based_argv_index] = Some(expr);
        arg_count += 1;
    }

    // Verify that used indices form a contiguous sequence starting from 1
    let constraints = constraints
        .into_iter()
        .take(arg_count)
        .enumerate()
        .map(|(i, c)| {
            c.ok_or_else(|| {
                LimboError::ExtensionError(format!(
                    "argv_index values must form contiguous sequence starting from 1, missing index {}",
                    i + 1
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Operation::Scan(Scan::VirtualTable {
        idx_num: *idx_num,
        idx_str: idx_str.clone(),
        constraints,
    }))
}

/// Mark WHERE clause terms as consumed when they are covered by a seek
/// (index seek, ephemeral auto-index seek, or rowid seek).
///
/// `is_outer_join`: skip consuming non-ON WHERE terms for outer joins, because
/// the cursor may land on a NULL-extended row that the WHERE filter must still
/// reject (e.g. `SELECT * FROM t1 LEFT JOIN t2 ON false WHERE t2.id = 5`).
///
/// `defer_cross_table`: skip cross-table constraints for hash-join build-only
/// tables that lack a main-loop cursor — the probe side will evaluate them.
fn mark_seek_constraints_consumed(
    constraints: &[Constraint],
    constraint_refs: &[RangeConstraintRef],
    where_clause: &mut [WhereTerm],
    is_outer_join: bool,
    defer_cross_table: bool,
) {
    for cref in constraint_refs.iter() {
        for pos in [
            cref.eq.as_ref().map(|e| e.constraint_pos),
            cref.lower_bound,
            cref.upper_bound,
        ] {
            let Some(pos) = pos else { continue };
            let constraint = &constraints[pos];
            let where_term = &mut where_clause[constraint.where_clause_pos.0];
            if where_term.consumed {
                continue;
            }
            if is_outer_join && where_term.from_outer_join.is_none() {
                continue;
            }
            if defer_cross_table && !constraint.lhs_mask.is_empty() {
                continue;
            }
            where_term.consumed = true;
        }
    }
}

fn mark_partial_index_predicate_terms_consumed(
    index: &Index,
    table_reference: &JoinedTable,
    where_clause: &mut [WhereTerm],
    is_outer_join: bool,
) {
    let predicate_terms = partial_index_predicate_terms(index, table_reference, where_clause)
        .expect("selected partial index predicate must be implied by query");
    for term_idx in predicate_terms {
        let where_term = &mut where_clause[term_idx];
        if where_term.consumed {
            continue;
        }
        if is_outer_join && where_term.from_outer_join != Some(table_reference.internal_id) {
            continue;
        }
        where_term.consumed = true;
    }
}

#[derive(Debug, PartialEq, Clone)]
enum ConstantConditionEliminationResult {
    Continue,
    ImpossibleCondition,
}

/// Removes predicates that are always true.
/// Returns a ConstantEliminationResult indicating whether any predicates are always false.
/// This is used to determine whether the query can be aborted early.
fn eliminate_constant_conditions(
    where_clause: &mut [WhereTerm],
) -> Result<ConstantConditionEliminationResult> {
    let mut i = 0;
    while i < where_clause.len() {
        let predicate = &where_clause[i];
        if predicate.expr.is_always_true()? {
            // true predicates can be removed since they don't affect the result
            where_clause[i].consumed = true;
            i += 1;
        } else if predicate.expr.is_always_false()? {
            // any false predicate in a list of conjuncts (AND-ed predicates) will make the whole list false,
            // except an outer join condition, because that just results in NULLs, not skipping the whole loop
            if predicate.from_outer_join.is_some() {
                i += 1;
                continue;
            }
            where_clause
                .iter_mut()
                .for_each(|term| term.consumed = true);
            return Ok(ConstantConditionEliminationResult::ImpossibleCondition);
        } else {
            i += 1;
        }
    }

    Ok(ConstantConditionEliminationResult::Continue)
}

/// Check if the order target collation matches index column collations.
/// Only remove the index when sort elimination selected this plan.
fn maybe_remove_index_candidate(
    index: &mut Option<Arc<Index>>,
    table_reference: &JoinedTable,
    order_target: Option<&OrderTarget>,
    sort_eliminated: bool,
) {
    if !sort_eliminated {
        return;
    }
    if let Some((idx, order_target)) = index.as_mut().zip(order_target) {
        for col_order in &order_target.columns {
            // Only check columns from this table
            if col_order.table_id != table_reference.internal_id {
                continue;
            }

            // Find matching index column
            let matching_idx_col = match &col_order.target {
                ColumnTarget::Column(col_no) => {
                    idx.columns.iter().find(|ic| ic.pos_in_table == *col_no)
                }
                ColumnTarget::RowId => {
                    continue;
                }
                ColumnTarget::Expr(_expr) => {
                    continue;
                }
            };

            if let Some(idx_col) = matching_idx_col {
                // Index columns without explicit COLLATE use BINARY.
                // Treat them as BINARY for ordering compatibility checks.
                if col_order.collation != idx_col.collation.unwrap_or_default() {
                    *index = None;
                    return;
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlwaysTrueOrFalse {
    AlwaysTrue,
    AlwaysFalse,
}

/**
  Helper trait for expressions that can be optimized
  Implemented for ast::Expr
*/
pub trait Optimizable {
    // if the expression is a constant expression that, when evaluated as a condition, is always true or false
    // return a [ConstantPredicate].
    fn check_always_true_or_false(&self) -> Result<Option<AlwaysTrueOrFalse>>;
    fn is_always_true(&self) -> Result<bool> {
        Ok(self.check_always_true_or_false()? == Some(AlwaysTrueOrFalse::AlwaysTrue))
    }
    fn is_always_false(&self) -> Result<bool> {
        Ok(self.check_always_true_or_false()? == Some(AlwaysTrueOrFalse::AlwaysFalse))
    }
    fn is_constant(&self, resolver: &Resolver<'_>) -> bool;
    fn is_nonnull(&self, tables: &TableReferences) -> bool;
}

impl Optimizable for ast::Expr {
    /// Returns true if the expressions is (verifiably) non-NULL.
    /// It might still be non-NULL even if we return false; we just
    /// weren't able to prove it.
    /// This function is currently very conservative, and will return false
    /// for any expression where we aren't sure and didn't bother to find out
    /// by writing more complex code.
    fn is_nonnull(&self, tables: &TableReferences) -> bool {
        match self {
            Expr::SubqueryResult { .. } => false,
            Expr::Between {
                lhs, start, end, ..
            } => lhs.is_nonnull(tables) && start.is_nonnull(tables) && end.is_nonnull(tables),
            Expr::Binary(_, ast::Operator::Modulus | ast::Operator::Divide, _) => false, // 1 % 0, 1 / 0
            Expr::Binary(expr, _, expr1) => expr.is_nonnull(tables) && expr1.is_nonnull(tables),
            Expr::Case {
                when_then_pairs,
                else_expr,
                ..
            } => {
                // With no ELSE, the CASE yields NULL when no WHEN matches.
                // Otherwise its result is one of the THEN values or the ELSE
                // value, so it is non-null only when all of those are. The base
                // is only compared in the WHEN tests and never becomes the
                // result, so its nullability does not matter.
                when_then_pairs
                    .iter()
                    .all(|(_, then)| then.is_nonnull(tables))
                    && else_expr
                        .as_ref()
                        .is_some_and(|else_expr| else_expr.is_nonnull(tables))
            }
            Expr::Cast { expr, .. } => expr.is_nonnull(tables),
            Expr::Collate(expr, _) => expr.is_nonnull(tables),
            Expr::DoublyQualified(..) => {
                panic!("Do not call is_nonnull before DoublyQualified has been rewritten as Column")
            }
            Expr::Exists(..) => false,
            Expr::FunctionCall { .. } => false,
            Expr::FunctionCallStar { .. } => false,
            Expr::Id(..) => panic!("Do not call is_nonnull before Id has been rewritten as Column"),
            Expr::Column {
                table,
                column,
                is_rowid_alias,
                ..
            } => {
                if *is_rowid_alias {
                    return true;
                }

                let (_, table_ref) = tables
                    .find_table_by_internal_id(*table)
                    .expect("table not found");
                let columns = table_ref.columns();
                let column = &columns[*column];
                // Only INTEGER PRIMARY KEY (rowid alias) is implicitly NOT NULL.
                // Other PRIMARY KEY types (e.g., TEXT PRIMARY KEY) can contain NULL.
                column.is_rowid_alias() || column.notnull()
            }
            Expr::RowId { .. } => true,
            Expr::InList { lhs, rhs, .. } => {
                lhs.is_nonnull(tables)
                    && (rhs.is_empty() || rhs.iter().all(|v| v.is_nonnull(tables)))
            }
            Expr::InSelect { .. } => false,
            Expr::InTable { .. } => false,
            Expr::IsNull(..) => true,
            Expr::Like { lhs, rhs, .. } => lhs.is_nonnull(tables) && rhs.is_nonnull(tables),
            Expr::Literal(literal) => match literal {
                ast::Literal::Numeric(_) => true,
                ast::Literal::String(_) => true,
                ast::Literal::Blob(_) => true,
                ast::Literal::Keyword(_) => true,
                ast::Literal::Null => false,
                ast::Literal::True => true,
                ast::Literal::False => true,
                ast::Literal::CurrentDate => true,
                ast::Literal::CurrentTime => true,
                ast::Literal::CurrentTimestamp => true,
            },
            Expr::Name(..) => false,
            Expr::NotNull(..) => true,
            Expr::Parenthesized(exprs) => exprs.iter().all(|expr| expr.is_nonnull(tables)),
            Expr::Qualified(..) => {
                panic!("Do not call is_nonnull before Qualified has been rewritten as Column")
            }
            Expr::FieldAccess { .. } => false, // struct/union field extraction can return NULL
            Expr::Raise(..) => false,
            Expr::Subquery(..) => false,
            Expr::Unary(_, expr) => expr.is_nonnull(tables),
            Expr::Variable(..) => false,
            Expr::Register(..) => false, // Register values can be null
            Expr::Default => false,
            Expr::Array { .. } | Expr::Subscript { .. } => {
                unreachable!("Array and Subscript are desugared into function calls by the parser")
            }
        }
    }
    /// Returns true if the expression is a constant i.e. does not depend on columns and can be evaluated only once during the execution
    fn is_constant(&self, resolver: &Resolver<'_>) -> bool {
        match self {
            Expr::SubqueryResult { .. } => false,
            Expr::Between {
                lhs, start, end, ..
            } => {
                lhs.is_constant(resolver)
                    && start.is_constant(resolver)
                    && end.is_constant(resolver)
            }
            Expr::Binary(expr, _, expr1) => {
                expr.is_constant(resolver) && expr1.is_constant(resolver)
            }
            Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                base.as_ref().is_none_or(|base| base.is_constant(resolver))
                    && when_then_pairs.iter().all(|(when, then)| {
                        when.is_constant(resolver) && then.is_constant(resolver)
                    })
                    && else_expr
                        .as_ref()
                        .is_none_or(|else_expr| else_expr.is_constant(resolver))
            }
            Expr::Cast { expr, .. } => expr.is_constant(resolver),
            Expr::Collate(expr, _) => expr.is_constant(resolver),
            // Not constant. Normally rewritten to Expr::Column by the optimizer,
            // but CHECK constraints bypass the rewrite pass and legitimately
            // contain DoublyQualified nodes.
            Expr::DoublyQualified(_, _, _) => false,
            Expr::Exists(_) => false,
            Expr::FunctionCall {
                args,
                name,
                filter_over,
                ..
            } => {
                if filter_over.over_clause.is_some() {
                    return false;
                }
                let Some(func) = resolver
                    .resolve_function(name.as_str(), args.len())
                    .ok()
                    .flatten()
                else {
                    return false;
                };
                func.is_deterministic() && args.iter().all(|arg| arg.is_constant(resolver))
            }
            Expr::FunctionCallStar { .. } => false,
            Expr::Id(_) => true,
            Expr::Column { .. } => false,
            Expr::RowId { .. } => false,
            Expr::InList { lhs, rhs, .. } => {
                lhs.is_constant(resolver)
                    && (rhs.is_empty() || rhs.iter().all(|v| v.is_constant(resolver)))
            }
            Expr::InSelect { .. } => {
                false // might be constant, too annoying to check subqueries etc. implement later
            }
            Expr::InTable { .. } => false,
            Expr::IsNull(expr) => expr.is_constant(resolver),
            Expr::Like {
                lhs, rhs, escape, ..
            } => {
                lhs.is_constant(resolver)
                    && rhs.is_constant(resolver)
                    && escape
                        .as_ref()
                        .is_none_or(|escape| escape.is_constant(resolver))
            }
            Expr::Literal(_) => true,
            Expr::Name(_) => false,
            Expr::NotNull(expr) => expr.is_constant(resolver),
            Expr::Parenthesized(exprs) => exprs.iter().all(|expr| expr.is_constant(resolver)),
            // Not constant. Normally rewritten to Expr::Column by the optimizer,
            // but CHECK constraints bypass the rewrite pass and legitimately
            // contain Qualified nodes.
            Expr::Qualified(_, _) | Expr::FieldAccess { .. } => false,
            Expr::Raise(_, expr) => expr.as_ref().is_none_or(|expr| expr.is_constant(resolver)),
            Expr::Subquery(_) => false,
            Expr::Unary(_, expr) => expr.is_constant(resolver),
            Expr::Variable(_) => true,
            Expr::Register(_) => false,
            Expr::Default => true,
            Expr::Array { .. } | Expr::Subscript { .. } => {
                unreachable!("Array and Subscript are desugared into function calls by the parser")
            }
        }
    }
    /// Returns true if the expression is a constant expression that, when evaluated as a condition, is always true or false
    fn check_always_true_or_false(&self) -> Result<Option<AlwaysTrueOrFalse>> {
        match self {
            Self::Literal(lit) => match lit {
                ast::Literal::Numeric(b) => {
                    if let Ok(int_value) = b.parse::<i64>() {
                        return Ok(Some(if int_value == 0 {
                            AlwaysTrueOrFalse::AlwaysFalse
                        } else {
                            AlwaysTrueOrFalse::AlwaysTrue
                        }));
                    }
                    if let Ok(float_value) = b.parse::<f64>() {
                        return Ok(Some(if float_value == 0.0 {
                            AlwaysTrueOrFalse::AlwaysFalse
                        } else {
                            AlwaysTrueOrFalse::AlwaysTrue
                        }));
                    }

                    Ok(None)
                }
                ast::Literal::String(s) => {
                    // Use Numeric::from to match SQLite's string-to-numeric conversion,
                    // which extracts leading numeric prefixes (e.g., '9S' -> 9, 'abc' -> 0)
                    let without_quotes = s.trim_matches('\'');
                    let numeric = Numeric::from(without_quotes);
                    match numeric.to_bool() {
                        true => Ok(Some(AlwaysTrueOrFalse::AlwaysTrue)),
                        false => Ok(Some(AlwaysTrueOrFalse::AlwaysFalse)),
                    }
                }
                _ => Ok(None),
            },
            Self::Unary(op, expr) => {
                if *op == ast::UnaryOperator::Not {
                    let trivial = expr.check_always_true_or_false()?;
                    return Ok(trivial.map(|t| match t {
                        AlwaysTrueOrFalse::AlwaysTrue => AlwaysTrueOrFalse::AlwaysFalse,
                        AlwaysTrueOrFalse::AlwaysFalse => AlwaysTrueOrFalse::AlwaysTrue,
                    }));
                }

                if *op == ast::UnaryOperator::Negative {
                    let trivial = expr.check_always_true_or_false()?;
                    return Ok(trivial);
                }

                Ok(None)
            }
            Self::InList { lhs: _, not, rhs } => {
                if rhs.is_empty() {
                    return Ok(Some(if *not {
                        AlwaysTrueOrFalse::AlwaysTrue
                    } else {
                        AlwaysTrueOrFalse::AlwaysFalse
                    }));
                }

                Ok(None)
            }
            Self::Binary(lhs, op, rhs) => {
                let lhs_trivial = lhs.check_always_true_or_false()?;
                let rhs_trivial = rhs.check_always_true_or_false()?;
                match op {
                    ast::Operator::And => {
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                            || rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysFalse));
                        }
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                            && rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysTrue));
                        }

                        Ok(None)
                    }
                    ast::Operator::Or => {
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                            || rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysTrue));
                        }
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                            && rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysFalse));
                        }

                        Ok(None)
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}

fn ephemeral_index_build(
    table_reference: &JoinedTable,
    table_references: &TableReferences,
    constraints: &[Constraint],
    constraint_refs: &[RangeConstraintRef],
    where_clause: &[WhereTerm],
    resolver: &Resolver<'_>,
) -> Result<Index> {
    let mut ephemeral_columns: crate::alloc::Vec<IndexColumn> = table_reference
        .columns()
        .iter()
        .enumerate()
        // Only copy columns that the query reads.
        .filter(|(index, _)| table_reference.column_is_used(*index))
        .map(|(i, c)| {
            let expr = match c.generated_type() {
                GeneratedType::Virtual { .. } => c.generated_expr().cloned(),
                GeneratedType::NotGenerated => None,
            };
            IndexColumn {
                name: c.name.clone().unwrap(),
                order: SortOrder::Asc,
                nulls_order: None,
                pos_in_table: i,
                collation: c.collation_opt(),
                default: c.default.clone(),
                expr: expr.map(Box::new),
            }
        })
        .try_collect()?;
    // sort so that constraints first, then rest in whatever order they were in in the table
    ephemeral_columns.sort_by(|a, b| {
        let a_constraint = constraint_refs
            .iter()
            .enumerate()
            .find(|(_, c)| c.table_col_pos == Some(a.pos_in_table));
        let b_constraint = constraint_refs
            .iter()
            .enumerate()
            .find(|(_, c)| c.table_col_pos == Some(b.pos_in_table));
        match (a_constraint, b_constraint) {
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some((a_idx, _)), Some((b_idx, _))) => a_idx.cmp(&b_idx),
            (None, None) => Ordering::Equal,
        }
    });
    let ephemeral_index = Index {
        name: format!(
            "ephemeral_{}_{}",
            table_reference.table.get_name(),
            table_reference.internal_id
        ),
        columns: ephemeral_columns,
        unique: false,
        ephemeral: true,
        table_name: table_reference.table.get_name().to_string(),
        root_page: 0,
        where_clause: autoindex_prefilter(
            table_reference,
            table_references,
            constraints,
            constraint_refs,
            where_clause,
            resolver,
        )
        .map(Box::new),
        has_rowid: table_reference
            .table
            .btree()
            .is_some_and(|btree| btree.has_rowid),
        index_method: None,
        on_conflict: None,
    };

    Ok(ephemeral_index)
}

/// Find conditions that can filter rows while an automatic index is built.
///
/// For example, given:
///
/// ```sql
/// SELECT *
/// FROM accounts AS a
/// JOIN events AS e ON e.account_id = a.id
/// WHERE e.created_at >= '2024-01-01';
/// ```
///
/// `e.account_id = a.id` needs the current account, so it is applied when the
/// index is searched. The date condition needs only an event row and a fixed
/// value, so rows before that date can be left out of the index entirely.
fn autoindex_prefilter(
    table_reference: &JoinedTable,
    table_references: &TableReferences,
    constraints: &[Constraint],
    constraint_refs: &[RangeConstraintRef],
    where_clause: &[WhereTerm],
    resolver: &Resolver<'_>,
) -> Option<Expr> {
    let is_outer_join = table_reference
        .join_info
        .as_ref()
        .is_some_and(JoinInfo::is_outer);
    if table_reference
        .join_info
        .as_ref()
        .is_some_and(JoinInfo::is_full_outer)
    {
        return None;
    }

    let mut term_positions = SmallVec::<[usize; 4]>::new();
    for constraint_ref in constraint_refs {
        for constraint_pos in [
            constraint_ref.eq.as_ref().map(|eq| eq.constraint_pos),
            constraint_ref.lower_bound,
            constraint_ref.upper_bound,
        ] {
            let Some(constraint_pos) = constraint_pos else {
                continue;
            };
            let constraint = &constraints[constraint_pos];
            let Some(column_pos) = constraint.table_col_pos else {
                continue;
            };
            let column = &table_reference.columns()[column_pos];
            let depends_on_another_table = !constraint.lhs_mask.is_empty();
            let is_virtual_column = column.is_virtual_generated();
            let is_array = column.is_array();
            let has_custom_type = resolver
                .schema()
                .get_type_def(&column.ty_str, table_reference.table.is_strict())
                .is_some();
            if depends_on_another_table || is_virtual_column || is_array || has_custom_type {
                continue;
            }

            let term_pos = constraint.where_clause_pos.0;
            let term = &where_clause[term_pos];
            let runs_before_outer_join_condition =
                is_outer_join && term.from_outer_join != Some(table_reference.internal_id);
            let comes_from_another_outer_join = term
                .from_outer_join
                .is_some_and(|table_id| table_id != table_reference.internal_id);
            let depends_on_outer_query = expr_references_outer_query(&term.expr, table_references);
            let contains_subquery = expr_references_any_subquery(&term.expr);
            // The build scans inner rows whose keys might never be searched.
            // Do not make a possibly failing expression run for those rows.
            let can_fail_for_unused_row = expression_can_fail_on_input(&term.expr);
            let was_already_added = term_positions.contains(&term_pos);
            if runs_before_outer_join_condition
                || comes_from_another_outer_join
                || depends_on_outer_query
                || contains_subquery
                || can_fail_for_unused_row
                || was_already_added
            {
                continue;
            }
            term_positions.push(term_pos);
        }
    }

    term_positions
        .into_iter()
        .map(|term_pos| where_clause[term_pos].expr.clone())
        .reduce(|left, right| Expr::Binary(Box::new(left), ast::Operator::And, Box::new(right)))
}

/// Build a [SeekDef] for a given list of [Constraint]s
pub fn build_seek_def_from_constraints(
    constraints: &[Constraint],
    constraint_refs: &[RangeConstraintRef],
    iter_dir: IterationDirection,
    where_clause: &[WhereTerm],
    referenced_tables: Option<&TableReferences>,
    resolver: Option<&Resolver>,
) -> Result<SeekDef> {
    if constraint_refs.is_empty() {
        // Zero-prefix seeks are used for extremum scans over an already ordered
        // source: start at one end of the cursor and stop after the first
        // qualifying row.
        let (start_op, end_op) = match iter_dir {
            IterationDirection::Forwards => (SeekOp::GE { eq_only: true }, SeekOp::GT),
            IterationDirection::Backwards => (SeekOp::LE { eq_only: true }, SeekOp::LT),
        };
        return Ok(SeekDef {
            prefix: Vec::new(),
            iter_dir,
            start: SeekKey {
                last_component: SeekKeyComponent::None,
                op: start_op,
                affinity: Affinity::Blob,
            },
            end: SeekKey {
                last_component: SeekKeyComponent::None,
                op: end_op,
                affinity: Affinity::Blob,
            },
        });
    }
    // Extract the key values and operators
    let key = constraint_refs
        .iter()
        .map(|cref| {
            cref.as_seek_range_constraint(constraints, where_clause, referenced_tables, resolver)
        })
        .collect();

    let seek_def = build_seek_def(iter_dir, key)?;
    Ok(seek_def)
}

/// Build a [SeekDef] for a given [SeekRangeConstraint] and [IterationDirection].
/// To be usable as a seek key, all but potentially the last term must be equalities.
/// The last term can be a nonequality (range with potentially one unbounded range).
///
/// There are two parts to the seek definition:
/// 1. start [SeekKey], which specifies the key that we will use to seek to the first row that matches the index key.
/// 2. end [SeekKey], which specifies the key that we will use to terminate the index scan that follows the seek.
///
/// There are some nuances to how, and which parts of, the index key can be used in the start and end [SeekKey]s,
/// depending on the operator and iteration order. This function explains those nuances inline when dealing with
/// each case.
///
/// But to illustrate the general idea, consider the following examples:
///
/// 1. For example, having two conditions like (x>10 AND y>20) cannot be used as a valid [SeekKey] GT(x:10, y:20)
///    because the first row greater than (x:10, y:20) might be (x:11, y:19), which does not satisfy the where clause.
///    In this case, only GT(x:10) must be used as the [SeekKey], and rows with y <= 20 must be filtered as a regular condition expression for each value of x.
///
/// 2. In contrast, having (x=10 AND y>20) forms a valid index key GT(x:10, y:20) because after the seek, we can simply terminate as soon as x > 10,
///    i.e. use GT(x:10, y:20) as the start [SeekKey] and GT(x:10) as the end.
///
/// The preceding examples are for an ascending index. The logic is similar for descending indexes, but an important distinction is that
/// since a descending index is laid out in reverse order, the comparison operators are reversed, e.g. LT becomes GT, LE becomes GE, etc.
/// So when you see e.g. a SeekOp::GT below for a descending index, it actually means that we are seeking the first row where the index key is LESS than the seek key.
///
fn build_seek_def(
    iter_dir: IterationDirection,
    mut key: Vec<SeekRangeConstraint>,
) -> Result<SeekDef> {
    turso_assert!(!key.is_empty());
    let last = key.last().unwrap();

    // if we searching for exact key - emit definition immediately with prefix as a full key
    if last.eq.is_some() {
        let (start_op, end_op) = match iter_dir {
            IterationDirection::Forwards => (SeekOp::GE { eq_only: true }, SeekOp::GT),
            IterationDirection::Backwards => (SeekOp::LE { eq_only: true }, SeekOp::LT),
        };
        return Ok(SeekDef {
            prefix: key,
            iter_dir,
            start: SeekKey {
                last_component: SeekKeyComponent::None,
                op: start_op,
                affinity: Affinity::Blob,
            },
            end: SeekKey {
                last_component: SeekKeyComponent::None,
                op: end_op,
                affinity: Affinity::Blob,
            },
        });
    }
    turso_assert!(last.lower_bound.is_some() || last.upper_bound.is_some());

    // pop last key as we will do some form of range search
    let last = key.pop().unwrap();
    let stored_nulls = last.nulls_order;
    // after that all key components must be equality constraints
    turso_debug_assert!(key.iter().all(|k| k.eq.is_some()));

    let has_prefix = !key.is_empty();
    let apply_null_boundaries = |start: &mut SeekKey, end: &mut SeekKey| {
        // Sometimes we must add an extra NULL to the key on purpose.
        // We do this so scans over composite indexes match SQLite exactly.
        //
        // A range with only one bound leaves the other side of the scan
        // without a key: the scan just runs until the prefix stops matching.
        // That is a problem when the NULLs of the range column live on that
        // side, because the scan would walk into them, and a comparison like
        // c2<=999 must not return NULL rows. So we add the NULL key on that
        // side: as a start key it makes the scan begin right after the NULLs,
        // as an end key it makes the scan stop right before them. Which side
        // the NULLs live on depends on the column: before all values with
        // NULLS FIRST, after all values with NULLS LAST.
        if !has_prefix {
            return;
        }
        // 1) Choose a better starting point.
        //
        // Example:
        //   INDEX(c1, c2 ASC)
        //   WHERE c1='a' AND c2<=999
        //
        // If we start from key [c1='a'], we hit rows where c2 is NULL first.
        // For this case we want to start right after that NULL boundary.
        // So we:
        // - use start key [c1='a', NULL]
        // - change start op from GE to GT
        // - for backward scans in the symmetric shape, change LE to LT
        if matches!(start.last_component, SeekKeyComponent::None) {
            match (iter_dir, stored_nulls) {
                (IterationDirection::Forwards, ast::NullsOrder::First) => {
                    start.last_component = SeekKeyComponent::Null;
                    start.op = SeekOp::GT;
                }
                (IterationDirection::Backwards, ast::NullsOrder::Last) => {
                    start.last_component = SeekKeyComponent::Null;
                    start.op = SeekOp::LT;
                }
                _ => {}
            }
        }
        // 2) Choose a better stopping point.
        //
        // Example:
        //   INDEX(c1, c2 DESC)
        //   WHERE c1='a' AND c2<=999
        //
        // The stop check must also respect the NULL boundary for c2.
        // So we:
        // - use stop key [c1='a', NULL]
        // - change end op from GT to GE
        // - for backward scans, change LT to LE
        if matches!(end.last_component, SeekKeyComponent::None) {
            match (iter_dir, stored_nulls) {
                (IterationDirection::Forwards, ast::NullsOrder::Last) => {
                    end.last_component = SeekKeyComponent::Null;
                    end.op = SeekOp::GE { eq_only: false };
                }
                (IterationDirection::Backwards, ast::NullsOrder::First) => {
                    end.last_component = SeekKeyComponent::Null;
                    end.op = SeekOp::LE { eq_only: false };
                }
                _ => {}
            }
        }
    };

    // For the commented examples below, keep in mind that since a descending index is laid out in reverse order, the comparison operators are reversed, e.g. LT becomes GT, LE becomes GE, etc.
    // Also keep in mind that index keys are compared based on the number of columns given, so for example:
    // - if key is GT(x:10), then (x=10, y=usize::MAX) is not GT because only X is compared. (x=11, y=<any>) is GT.
    // - if key is GT(x:10, y:20), then (x=10, y=21) is GT because both X and Y are compared.
    // - if key is GT(x:10, y:NULL), then (x=10, y=0) is GT because NULL is always LT in index key comparisons.
    Ok(match iter_dir {
        IterationDirection::Forwards => {
            let (mut start, mut end) = match last.sort_order {
                SortOrder::Asc => {
                    let start = match last.lower_bound {
                        // Forwards, Asc, GT: (x=10 AND y>20)
                        // Start key: start from the first GT(x:10, y:20)
                        Some((ast::Operator::Greater, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GT,
                            affinity,
                        },
                        // Forwards, Asc, GE: (x=10 AND y>=20)
                        // Start key: start from the first GE(x:10, y:20)
                        Some((ast::Operator::GreaterEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GE { eq_only: false },
                            affinity,
                        },
                        // Forwards, Asc, None, (x=10 AND y<30)
                        // Start key: start from the first GE(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::GE { eq_only: false },
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    let end = match last.upper_bound {
                        // Forwards, Asc, LT, (x=10 AND y<30)
                        // End key: end at first GE(x:10, y:30)
                        Some((ast::Operator::Less, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GE { eq_only: false },
                            affinity,
                        },
                        // Forwards, Asc, LE, (x=10 AND y<=30)
                        // End key: end at first GT(x:10, y:30)
                        Some((ast::Operator::LessEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GT,
                            affinity,
                        },
                        // Forwards, Asc, None, (x=10 AND y>20)
                        // End key: end at first GT(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::GT,
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    (start, end)
                }
                SortOrder::Desc => {
                    let start = match last.upper_bound {
                        // Forwards, Desc, LT: (x=10 AND y<30)
                        // Start key: start from the first GT(x:10, y:30)
                        Some((ast::Operator::Less, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GT,
                            affinity,
                        },
                        // Forwards, Desc, LE: (x=10 AND y<=30)
                        // Start key: start from the first GE(x:10, y:30)
                        Some((ast::Operator::LessEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GE { eq_only: false },
                            affinity,
                        },
                        // Forwards, Desc, None: (x=10 AND y>20)
                        // Start key: start from the first GE(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::GE { eq_only: false },
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    let end = match last.lower_bound {
                        // Forwards, Asc, GT, (x=10 AND y>20)
                        // End key: end at first GE(x:10, y:20)
                        Some((ast::Operator::Greater, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GE { eq_only: false },
                            affinity,
                        },
                        // Forwards, Asc, GE, (x=10 AND y>=20)
                        // End key: end at first GT(x:10, y:20)
                        Some((ast::Operator::GreaterEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GT,
                            affinity,
                        },
                        // Forwards, Asc, None, (x=10 AND y<30)
                        // End key: end at first GT(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::GT,
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    (start, end)
                }
            };
            apply_null_boundaries(&mut start, &mut end);
            SeekDef {
                prefix: key,
                iter_dir,
                start,
                end,
            }
        }
        IterationDirection::Backwards => {
            let (mut start, mut end) = match last.sort_order {
                SortOrder::Asc => {
                    let start = match last.upper_bound {
                        // Backwards, Asc, LT: (x=10 AND y<30)
                        // Start key: start from the first LT(x:10, y:30)
                        Some((ast::Operator::Less, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LT,
                            affinity,
                        },
                        // Backwards, Asc, LT: (x=10 AND y<=30)
                        // Start key: start from the first LE(x:10, y:30)
                        Some((ast::Operator::LessEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LE { eq_only: false },
                            affinity,
                        },
                        // Backwards, Asc, None: (x=10 AND y>20)
                        // Start key: start from the first LE(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::LE { eq_only: false },
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op)
                        }
                    };
                    let end = match last.lower_bound {
                        // Backwards, Asc, GT, (x=10 AND y>20)
                        // End key: end at first LE(x:10, y:20)
                        Some((ast::Operator::Greater, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LE { eq_only: false },
                            affinity,
                        },
                        // Backwards, Asc, GT, (x=10 AND y>=20)
                        // End key: end at first LT(x:10, y:20)
                        Some((ast::Operator::GreaterEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LT,
                            affinity,
                        },
                        // Backwards, Asc, None, (x=10 AND y<30)
                        // End key: end at first LT(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::LT,
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    (start, end)
                }
                SortOrder::Desc => {
                    let start = match last.lower_bound {
                        // Backwards, Desc, LT: (x=10 AND y>20)
                        // Start key: start from the first LT(x:10, y:20)
                        Some((ast::Operator::Greater, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LT,
                            affinity,
                        },
                        // Backwards, Desc, LE: (x=10 AND y>=20)
                        // Start key: start from the first LE(x:10, y:20)
                        Some((ast::Operator::GreaterEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LE { eq_only: false },
                            affinity,
                        },
                        // Backwards, Desc, LE: (x=10 AND y<30)
                        // Start key: start from the first LE(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::LE { eq_only: false },
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    let end = match last.upper_bound {
                        // Backwards, Desc, LT, (x=10 AND y<30)
                        // End key: end at first LE(x:10, y:30)
                        Some((ast::Operator::Less, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LE { eq_only: false },
                            affinity,
                        },
                        // Backwards, Desc, LT, (x=10 AND y<=30)
                        // End key: end at first LT(x:10, y:30)
                        Some((ast::Operator::LessEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LT,
                            affinity,
                        },
                        // Backwards, Desc, LT, (x=10 AND y>20)
                        // End key: end at first LT(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::LT,
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    (start, end)
                }
            };
            apply_null_boundaries(&mut start, &mut end);
            SeekDef {
                prefix: key,
                iter_dir,
                start,
                end,
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{where_term_is_null_rejecting_for_table, Optimizable};
    use crate::translate::emitter::{DoubleQuotedDml, Resolver};
    use crate::{schema::Schema, DatabaseCatalog, RwLock, SymbolTable};
    use rustc_hash::FxHashMap as HashMap;
    use turso_parser::ast::{self, Expr, FunctionTail, Name, TableInternalId};

    fn empty_resolver<'a>(
        schema: &'a Schema,
        database_schemas: &'a RwLock<HashMap<usize, crate::sync::Arc<Schema>>>,
        temp_database: &'a RwLock<Option<crate::connection::TempDatabase>>,
        attached_databases: &'a RwLock<DatabaseCatalog>,
        syms: &'a SymbolTable,
    ) -> Resolver<'a> {
        Resolver::new(
            schema,
            database_schemas,
            temp_database,
            attached_databases,
            syms,
            true,
            DoubleQuotedDml::Enabled,
            crate::sync::Arc::new(crate::dialect::SqliteDialect),
            &None,
        )
    }

    fn no_tail() -> FunctionTail {
        FunctionTail {
            filter_clause: None,
            over_clause: None,
        }
    }

    fn fn_call(name: &str, args: Vec<Expr>) -> Expr {
        Expr::FunctionCall {
            name: Name::exact(name.to_string()),
            distinctness: None,
            args: args.into_iter().map(Box::new).collect(),
            order_by: vec![],
            within_group: vec![],
            filter_over: no_tail(),
        }
    }

    #[test]
    fn constant_classifier_for_coalesce_with_in_list() {
        let schema = Schema::new();
        let syms = SymbolTable::new();
        let database_schemas = RwLock::new(HashMap::default());
        let attached_databases = RwLock::new(DatabaseCatalog::new());
        let temp_database = RwLock::new(None);
        let resolver = empty_resolver(
            &schema,
            &database_schemas,
            &temp_database,
            &attached_databases,
            &syms,
        );

        let expr = fn_call(
            "coalesce",
            vec![
                fn_call(
                    "length",
                    vec![Expr::Literal(ast::Literal::String("a".into()))],
                ),
                Expr::InList {
                    lhs: Box::new(fn_call(
                        "hex",
                        vec![Expr::Literal(ast::Literal::Blob("X'01'".into()))],
                    )),
                    not: false,
                    rhs: vec![Box::new(Expr::Literal(ast::Literal::Blob("X'02'".into())))],
                },
            ],
        );

        assert!(expr.is_constant(&resolver));
    }

    #[test]
    fn constant_classifier_for_quote_of_column() {
        let schema = Schema::new();
        let syms = SymbolTable::new();
        let database_schemas = RwLock::new(HashMap::default());
        let attached_databases = RwLock::new(DatabaseCatalog::new());
        let temp_database = RwLock::new(None);
        let resolver = empty_resolver(
            &schema,
            &database_schemas,
            &temp_database,
            &attached_databases,
            &syms,
        );

        let expr = fn_call(
            "quote",
            vec![Expr::Column {
                database: None,
                table: TableInternalId::default(),
                column: 0,
                is_rowid_alias: false,
            }],
        );

        assert!(!expr.is_constant(&resolver));
    }

    #[test]
    fn null_rejection_detection_uses_function_resolution() {
        let table = TableInternalId::from(42);
        let expr = Expr::Binary(
            Box::new(fn_call(
                "IFNULL",
                vec![
                    Expr::Column {
                        database: None,
                        table,
                        column: 0,
                        is_rowid_alias: false,
                    },
                    Expr::Literal(ast::Literal::Numeric("2147483647".into())),
                ],
            )),
            ast::Operator::GreaterEquals,
            Box::new(Expr::Literal(ast::Literal::Numeric("127".into()))),
        );

        assert!(!where_term_is_null_rejecting_for_table(&expr, table));
    }

    #[test]
    fn null_rejection_detection_requires_target_table_reference() {
        let target_table = TableInternalId::from(7);
        let other_table = TableInternalId::from(8);
        // A term that never mentions the target table can be TRUE on the
        // join's null-extended rows, so it proves nothing about them.
        let expr = Expr::Binary(
            Box::new(fn_call(
                "coalesce",
                vec![
                    Expr::Column {
                        database: None,
                        table: other_table,
                        column: 0,
                        is_rowid_alias: false,
                    },
                    Expr::Literal(ast::Literal::Numeric("0".into())),
                ],
            )),
            ast::Operator::Greater,
            Box::new(Expr::Literal(ast::Literal::Numeric("1".into()))),
        );

        assert!(!where_term_is_null_rejecting_for_table(&expr, target_table));
    }

    #[test]
    fn null_rejection_detection_handles_nested_null_masking_functions() {
        let table = TableInternalId::from(9);
        let expr = Expr::Binary(
            Box::new(fn_call(
                "coalesce",
                vec![
                    fn_call(
                        "ifnull",
                        vec![
                            Expr::Column {
                                database: None,
                                table,
                                column: 1,
                                is_rowid_alias: false,
                            },
                            Expr::Literal(ast::Literal::Numeric("0".into())),
                        ],
                    ),
                    Expr::Literal(ast::Literal::Numeric("2".into())),
                ],
            )),
            ast::Operator::Equals,
            Box::new(Expr::Literal(ast::Literal::Numeric("2".into()))),
        );

        assert!(!where_term_is_null_rejecting_for_table(&expr, table));
    }

    #[test]
    fn null_rejection_detection_treats_is_operator_as_non_rejecting() {
        let table = TableInternalId::from(11);
        let expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table,
                column: 0,
                is_rowid_alias: false,
            }),
            ast::Operator::Is,
            Box::new(Expr::Literal(ast::Literal::Null)),
        );

        assert!(!where_term_is_null_rejecting_for_table(&expr, table));
    }

    #[test]
    fn null_rejection_detection_treats_empty_not_in_as_non_rejecting() {
        let table = TableInternalId::from(15);
        let column = Expr::Column {
            database: None,
            table,
            column: 0,
            is_rowid_alias: false,
        };
        let not_in_empty = Expr::InList {
            lhs: Box::new(column.clone()),
            not: true,
            rhs: vec![],
        };
        let in_value = Expr::InList {
            lhs: Box::new(column),
            not: false,
            rhs: vec![Box::new(Expr::Literal(ast::Literal::Numeric("1".into())))],
        };

        assert!(!where_term_is_null_rejecting_for_table(
            &not_in_empty,
            table
        ));
        assert!(where_term_is_null_rejecting_for_table(&in_value, table));
    }

    #[test]
    fn null_rejection_detection_treats_is_between_columns_as_non_rejecting() {
        let table = TableInternalId::from(12);
        let expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table,
                column: 0,
                is_rowid_alias: false,
            }),
            ast::Operator::Is,
            Box::new(Expr::Column {
                database: None,
                table,
                column: 1,
                is_rowid_alias: false,
            }),
        );

        assert!(!where_term_is_null_rejecting_for_table(&expr, table));
    }

    #[test]
    fn null_rejection_detection_treats_is_with_non_null_literal_as_non_rejecting() {
        let table = TableInternalId::from(13);
        let expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table,
                column: 0,
                is_rowid_alias: false,
            }),
            ast::Operator::Is,
            Box::new(Expr::Literal(ast::Literal::Numeric("5".into()))),
        );

        assert!(!where_term_is_null_rejecting_for_table(&expr, table));
    }

    #[test]
    fn null_rejection_detection_treats_is_not_with_non_null_literal_as_non_rejecting() {
        let table = TableInternalId::from(14);
        let expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table,
                column: 0,
                is_rowid_alias: false,
            }),
            ast::Operator::IsNot,
            Box::new(Expr::Literal(ast::Literal::Numeric("5".into()))),
        );

        assert!(!where_term_is_null_rejecting_for_table(&expr, table));
    }

    #[test]
    fn null_rejection_detection_case_with_is_null_check_not_rejecting() {
        let table = TableInternalId::from(15);
        // CASE WHEN t.col IS NULL THEN 1 ELSE t.col END > 0
        let expr = Expr::Binary(
            Box::new(Expr::Case {
                base: None,
                when_then_pairs: vec![(
                    Box::new(Expr::IsNull(Box::new(Expr::Column {
                        database: None,
                        table,
                        column: 0,
                        is_rowid_alias: false,
                    }))),
                    Box::new(Expr::Literal(ast::Literal::Numeric("1".into()))),
                )],
                else_expr: Some(Box::new(Expr::Column {
                    database: None,
                    table,
                    column: 0,
                    is_rowid_alias: false,
                })),
            }),
            ast::Operator::Greater,
            Box::new(Expr::Literal(ast::Literal::Numeric("0".into()))),
        );

        assert!(!where_term_is_null_rejecting_for_table(&expr, table));
    }

    #[test]
    fn null_rejection_detection_case_without_null_check_is_rejecting() {
        let table = TableInternalId::from(16);
        // CASE WHEN t.col > 5 THEN t.col ELSE 0 END > 0
        let expr = Expr::Binary(
            Box::new(Expr::Case {
                base: None,
                when_then_pairs: vec![(
                    Box::new(Expr::Binary(
                        Box::new(Expr::Column {
                            database: None,
                            table,
                            column: 0,
                            is_rowid_alias: false,
                        }),
                        ast::Operator::Greater,
                        Box::new(Expr::Literal(ast::Literal::Numeric("5".into()))),
                    )),
                    Box::new(Expr::Column {
                        database: None,
                        table,
                        column: 0,
                        is_rowid_alias: false,
                    }),
                )],
                else_expr: Some(Box::new(Expr::Literal(ast::Literal::Numeric("0".into())))),
            }),
            ast::Operator::Greater,
            Box::new(Expr::Literal(ast::Literal::Numeric("0".into()))),
        );

        // Any CASE can turn NULL inputs into a non-NULL result (here the ELSE
        // arm yields 0 for a NULL t.col), so no CASE term proves anything
        // about null-extended rows. Same rule as SQLite's impliesNotNullRow.
        assert!(!where_term_is_null_rejecting_for_table(&expr, table));
    }

    #[test]
    fn null_rejection_detection_null_test_nested_in_comparison_not_rejecting() {
        let table = TableInternalId::from(18);
        // (t.col IS NULL) = 1 — TRUE on a null-extended row.
        let expr = Expr::Binary(
            Box::new(Expr::Parenthesized(vec![Box::new(Expr::IsNull(Box::new(
                Expr::Column {
                    database: None,
                    table,
                    column: 0,
                    is_rowid_alias: false,
                },
            )))])),
            ast::Operator::Equals,
            Box::new(Expr::Literal(ast::Literal::Numeric("1".into()))),
        );

        assert!(!where_term_is_null_rejecting_for_table(&expr, table));
    }

    #[test]
    fn null_rejection_detection_or_needs_both_arms() {
        let table = TableInternalId::from(19);
        let other_table = TableInternalId::from(20);
        let col = |t: TableInternalId, c: usize| Expr::Column {
            database: None,
            table: t,
            column: c,
            is_rowid_alias: false,
        };
        let eq_five = |t: TableInternalId, c: usize| {
            Expr::Binary(
                Box::new(col(t, c)),
                ast::Operator::Equals,
                Box::new(Expr::Literal(ast::Literal::Numeric("5".into()))),
            )
        };

        // t.a = 5 OR t.b = 5: both arms are false when t's columns are NULL.
        let both_arms_on_table = Expr::Binary(
            Box::new(eq_five(table, 0)),
            ast::Operator::Or,
            Box::new(eq_five(table, 1)),
        );
        assert!(where_term_is_null_rejecting_for_table(
            &both_arms_on_table,
            table
        ));

        // t.a = 5 OR u.x = 5: the u arm can make the OR true on t's
        // null-extended rows.
        let one_arm_on_other_table = Expr::Binary(
            Box::new(eq_five(table, 0)),
            ast::Operator::Or,
            Box::new(eq_five(other_table, 0)),
        );
        assert!(!where_term_is_null_rejecting_for_table(
            &one_arm_on_other_table,
            table
        ));
    }

    #[test]
    fn null_rejection_detection_iif_with_is_null_check_not_rejecting() {
        let table = TableInternalId::from(17);
        // IIF(t.col IS NULL, 1, t.col) > 0
        let expr = Expr::Binary(
            Box::new(fn_call(
                "iif",
                vec![
                    Expr::IsNull(Box::new(Expr::Column {
                        database: None,
                        table,
                        column: 0,
                        is_rowid_alias: false,
                    })),
                    Expr::Literal(ast::Literal::Numeric("1".into())),
                    Expr::Column {
                        database: None,
                        table,
                        column: 0,
                        is_rowid_alias: false,
                    },
                ],
            )),
            ast::Operator::Greater,
            Box::new(Expr::Literal(ast::Literal::Numeric("0".into()))),
        );

        assert!(!where_term_is_null_rejecting_for_table(&expr, table));
    }
}
