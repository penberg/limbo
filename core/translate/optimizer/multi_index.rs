//! Multi-index-specific planning for OR-by-union and AND-by-intersection.
//!
//! This module owns the parts of planning that are unique to combining several
//! index probes for the same table. It reuses the generic btree candidate
//! chooser from `access_method.rs` for each individual branch, then layers the
//! union/intersection-specific decomposition, costing, and residual handling on
//! top.

use crate::alloc::{TryClone, TursoIteratorExt};
use crate::schema::{Index, Schema};
use crate::stats::AnalyzeStats;
use crate::translate::expr::expr_references_any_subquery;
use crate::translate::optimizer::access_method::{
    choose_best_btree_candidate, choose_best_in_seek_candidate, AccessMethod, AccessMethodParams,
    BranchReadMode, ChosenInSeekCandidate,
};
use crate::translate::optimizer::constraints::{
    analyze_binary_term_for_index, can_use_partial_index, constraints_from_where_clause,
    partial_index_predicate_terms, summarize_binary_term_for_index, Constraint, RangeConstraintRef,
    TableConstraints,
};
use crate::translate::optimizer::cost::{
    estimate_cost_for_scan_or_seek, estimate_rows_per_seek, rows_per_leaf_page_for_index,
    where_expr_steps, AnalyzeCtx, Cost, IndexInfo, RowCountEstimate,
};
use crate::translate::optimizer::cost_params::CostModelParams;
use crate::translate::optimizer::AvailableIndexes;
use crate::translate::plan::{
    BitSet, InSeekSource, JoinedTable, NonFromClauseSubquery, SetOperation, TableReferences,
    UnionBranchPrePostFilters, WhereTerm,
};
use crate::translate::planner::{table_mask_from_expr, TableMask};
use crate::Result;
use std::sync::Arc;
use turso_macros::turso_assert_eq;
use turso_parser::ast::{self, TableInternalId};

#[derive(Debug, Clone)]
/// Parameters for a single branch of a multi-index scan.
pub struct MultiIndexBranchParams {
    /// The index to use for this branch, or None for rowid access.
    pub index: Option<Arc<Index>>,
    /// How this branch probes the table/index.
    pub access: MultiIndexBranchAccessParams,
    /// Estimated number of rows from this branch.
    pub estimated_rows: f64,
    /// Residual filters for union (OR) branches. `None` for intersection branches.
    pub residuals: Option<UnionBranchPrePostFilters>,
}

#[derive(Debug, Clone)]
pub enum MultiIndexBranchAccessParams {
    Seek {
        constraints: Vec<Constraint>,
        constraint_refs: Vec<RangeConstraintRef>,
    },
    InSeek {
        source: InSeekSource,
    },
}

/// Internal decomposition of an AND clause into intersection branches.
#[derive(Debug)]
struct AndClauseDecomposition {
    term_indices: Vec<usize>,
    branches: Vec<AndBranch>,
}

/// One term that can participate in an AND-by-intersection plan.
#[derive(Debug)]
struct AndBranch {
    where_term_idx: usize,
    constraint: Constraint,
    index: Option<Arc<Index>>,
    constraint_refs: Vec<RangeConstraintRef>,
}

struct AndBranchSummary {
    where_term_idx: usize,
    table_col_pos: Option<usize>,
    index: Option<Arc<Index>>,
}

/// Internal branch representation while evaluating a candidate multi-index plan.
struct MultiIdxBranch {
    index: Option<Arc<Index>>,
    access: MultiIdxBranchAccess,
    cost: Cost,
    estimated_rows: f64,
    union_prepost_filters: Option<UnionBranchPrePostFilters>,
}

enum MultiIdxBranchAccess {
    Seek {
        constraints: Vec<Constraint>,
        constraint_refs: Vec<RangeConstraintRef>,
    },
    InSeek {
        source: InSeekSource,
        constraint_idx: usize,
    },
}

/// Flattens nested OR expressions into a list of disjuncts.
///
/// For example, `(a OR b) OR c` becomes `[a, b, c]`.
fn flatten_or_expr(expr: &ast::Expr) -> Vec<&ast::Expr> {
    match expr {
        ast::Expr::Binary(lhs, ast::Operator::Or, rhs) => {
            let mut result = flatten_or_expr(lhs);
            result.extend(flatten_or_expr(rhs));
            result
        }
        _ => vec![expr],
    }
}

/// Flattens nested AND expressions into a list of conjuncts.
///
/// For example, `(a AND b) AND c` becomes `[a, b, c]`.
fn flatten_and_expr(expr: &ast::Expr) -> Vec<&ast::Expr> {
    match expr {
        ast::Expr::Binary(lhs, ast::Operator::And, rhs) => {
            let mut result = flatten_and_expr(lhs);
            result.extend(flatten_and_expr(rhs));
            result
        }
        _ => vec![expr],
    }
}

/// Build temporary `WhereTerm`s from branch-local expressions and extract the
/// constraints for exactly one target table.
///
/// This is narrower than `constraints_from_where_clause()`:
/// - `exprs` are synthetic planner inputs, not the query's real top-level
///   `WHERE` terms.
/// - The returned `WhereTerm`s are only suitable for branch-local planning
///   and constraint bookkeeping for `table_reference`; they must not be reused
///   for global predicate consumption or join rewrites.
///
/// FIXME: stop synthesizing `WhereTerm`s here just to reuse
/// `constraints_from_where_clause()`. Branch-local planning should have a
/// direct constraint-extraction path that does not fabricate top-level planner
/// terms.
#[expect(clippy::too_many_arguments)]
fn get_table_local_constraints_for_branch(
    exprs: &[ast::Expr],
    from_outer_join: Option<TableInternalId>,
    table_reference: &JoinedTable,
    table_references: &TableReferences,
    available_indexes: &AvailableIndexes,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    params: &CostModelParams,
) -> crate::Result<(Vec<WhereTerm>, TableConstraints)> {
    let synthetic_where_terms = exprs
        .iter()
        .cloned()
        .map(|expr| WhereTerm {
            expr,
            from_outer_join,
            consumed: false,
        })
        .collect::<Vec<_>>();
    let table_constraints = constraints_from_where_clause(
        &synthetic_where_terms,
        table_references,
        available_indexes,
        subqueries,
        schema,
        params,
    )?
    .into_iter()
    .find(|constraints| constraints.table_id == table_reference.internal_id)
    .expect("constraints_from_where_clause must return constraints for every joined table");
    let mut table_constraints = table_constraints;
    // Branch-local constraints originate from synthetic `WhereTerm`s, so copy
    // out their constraining expressions while those temporary terms still
    // exist.
    for constraint in table_constraints.constraints.iter_mut() {
        if constraint.constraining_expr.is_some() || constraint.operator.as_ast_operator().is_none()
        {
            continue;
        }
        constraint.constraining_expr = Some(constraint.get_constraining_expr(
            &synthetic_where_terms,
            Some(table_references),
            None,
        ));
    }
    Ok((synthetic_where_terms, table_constraints))
}

/// Estimate the cost of a multi-index union scan (OR-by-union optimization).
///
/// The cost model accounts for:
/// 1. Cost of each branch scan
/// 2. RowSet insert/test work needed to deduplicate rowids
/// 3. Table fetches after deduplication
/// 4. Overlap between branches, approximated from independent selectivities
fn estimate_multi_index_scan_cost(
    branch_costs: &[Cost],
    branch_rows: &[f64],
    base_row_count: RowCountEstimate,
    input_cardinality: f64,
    params: &CostModelParams,
) -> (Cost, f64) {
    let base_row_count = *base_row_count;
    // Total cost of all branch scans.
    let branch_scan_cost: f64 = branch_costs.iter().map(|c| c.0).sum();
    // Sum of branch row counts before RowSet deduplication.
    let total_rows_before_dedup: f64 = branch_rows.iter().sum();

    // Estimate overlap between branches. For independent predicates:
    //   P(A OR B) = 1 - (1 - P(A)) * (1 - P(B))
    let mut unique_row_ratio = 1.0f64;
    for rows in branch_rows.iter() {
        let branch_selectivity = (*rows / base_row_count).min(1.0);
        unique_row_ratio *= 1.0 - branch_selectivity;
    }
    let estimated_unique_rows = base_row_count * (1.0 - unique_row_ratio);

    // RowSet operations do an insert and membership test per candidate rowid.
    let rowset_ops_cost = total_rows_before_dedup * params.cpu_cost_per_row * 2.0;

    // Table fetch cost mirrors single-index lookup costing, assuming some
    // locality benefit from rowid-ordered access after RowSet deduplication.
    let table_pages = (base_row_count / params.rows_per_table_page).max(1.0);
    let selectivity = estimated_unique_rows / base_row_count.max(1.0);
    let table_fetch_cost = selectivity * table_pages;
    let total_cost = (branch_scan_cost + rowset_ops_cost + table_fetch_cost) * input_cardinality;

    (Cost(total_cost), estimated_unique_rows)
}

/// Estimate the cost of a multi-index intersection (AND-by-intersection).
///
/// The cost model accounts for:
/// 1. Cost of each branch scan
/// 2. RowSet test work while intersecting rowids
/// 3. Table fetches for the surviving rowids
/// 4. Final result size as the product of branch selectivities
fn estimate_multi_index_intersection_cost(
    branch_costs: &[Cost],
    branch_rows: &[f64],
    base_row_count: RowCountEstimate,
    input_cardinality: f64,
    params: &CostModelParams,
) -> (Cost, f64) {
    let base_row_count = *base_row_count;
    // Total cost of all branch scans.
    let branch_scan_cost: f64 = branch_costs.iter().map(|c| c.0).sum();

    // Estimate intersection result as the product of selectivities:
    //   P(A AND B) = P(A) * P(B)
    let mut intersection_selectivity = 1.0f64;
    for rows in branch_rows.iter() {
        let branch_selectivity = (*rows / base_row_count).min(1.0);
        intersection_selectivity *= branch_selectivity;
    }
    let estimated_intersection_rows = (base_row_count * intersection_selectivity).max(1.0);

    // First branch inserts rowids; later branches test against the RowSet.
    let first_branch_rows = branch_rows.first().copied().unwrap_or(0.0);
    let subsequent_branch_rows: f64 = branch_rows.iter().skip(1).sum();
    let rowset_ops_cost =
        (first_branch_rows + subsequent_branch_rows) * params.cpu_cost_per_row * 1.5;

    // Table fetch cost mirrors single-index lookup costing, assuming some
    // locality benefit from rowid-ordered access after intersection.
    let table_pages = (base_row_count / params.rows_per_table_page).max(1.0);
    let selectivity = estimated_intersection_rows / base_row_count.max(1.0);
    let table_fetch_cost = selectivity * table_pages;
    let total_cost = (branch_scan_cost + rowset_ops_cost + table_fetch_cost) * input_cardinality;

    (Cost(total_cost), estimated_intersection_rows)
}

/// Compute [`IndexInfo`] for a multi-index branch.
///
/// RowSet-building branches only need rowids from the scan, so an index can be
/// treated as covering even if it does not contain all later table columns.
fn index_info_for_branch(
    index: Option<&Index>,
    rhs_table: &JoinedTable,
    read_mode: BranchReadMode,
    rows_per_table_page: f64,
) -> Option<IndexInfo> {
    let rowid_only = matches!(read_mode, BranchReadMode::RowIdOnly);
    match index {
        Some(index) => Some(IndexInfo {
            unique: index.unique,
            covering: rowid_only || rhs_table.index_is_covering(index),
            column_count: index.columns.len(),
            rows_per_leaf_page: rows_per_leaf_page_for_index(
                index.columns.len(),
                rhs_table,
                rows_per_table_page,
            ),
        }),
        None => Some(IndexInfo {
            unique: true,
            covering: true,
            column_count: 1,
            rows_per_leaf_page: rows_per_table_page,
        }),
    }
}

fn in_seek_source_from_expr(
    expr: &ast::Expr,
    chosen: &ChosenInSeekCandidate,
) -> Option<InSeekSource> {
    match expr {
        ast::Expr::InList { rhs, .. } => Some(InSeekSource::LiteralList {
            values: rhs.iter().map(|e| *e.clone()).collect(),
            affinity: chosen.affinity,
        }),
        ast::Expr::SubqueryResult {
            query_type: ast::SubqueryType::In { cursor_id, .. },
            ..
        } => Some(InSeekSource::Subquery {
            cursor_id: *cursor_id,
        }),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn choose_multi_index_branch_access(
    rhs_table: &JoinedTable,
    table_constraints: &TableConstraints,
    branch_terms: &[WhereTerm],
    lhs_mask: &TableMask,
    rhs_idx: usize,
    schema: &Schema,
    available_indexes: &AvailableIndexes,
    base_row_count: RowCountEstimate,
    analyze_stats: &AnalyzeStats,
    params: &CostModelParams,
) -> Result<Option<MultiIdxBranch>> {
    let chosen_seek = choose_best_btree_candidate(
        rhs_table,
        table_constraints,
        lhs_mask,
        rhs_idx,
        None,
        schema,
        available_indexes,
        analyze_stats,
        1.0,
        base_row_count,
        params,
    )?;

    let mut best_branch = chosen_seek
        .as_ref()
        .filter(|chosen| !chosen.constraint_refs.is_empty())
        .map(|chosen| {
            let index_info = index_info_for_branch(
                chosen.index.as_deref(),
                rhs_table,
                BranchReadMode::RowIdOnly,
                params.rows_per_table_page,
            )
            .expect("multi-index branches always have costable access");
            let analyze_ctx = AnalyzeCtx {
                rhs_table,
                index: chosen.index.as_ref(),
                stats: analyze_stats,
            };
            let branch_cost = estimate_cost_for_scan_or_seek(
                Some(index_info),
                &table_constraints.constraints,
                &chosen.constraint_refs,
                1.0,
                base_row_count,
                false,
                params,
                Some(&analyze_ctx),
            );
            MultiIdxBranch {
                index: chosen.index.clone(),
                access: MultiIdxBranchAccess::Seek {
                    constraints: table_constraints.constraints.clone(),
                    constraint_refs: chosen.constraint_refs.to_vec(),
                },
                cost: branch_cost,
                estimated_rows: estimate_rows_per_seek(
                    index_info,
                    &table_constraints.constraints,
                    &chosen.constraint_refs,
                    base_row_count,
                    Some(&analyze_ctx),
                ),
                union_prepost_filters: None,
            }
        });

    let in_seek_threshold = best_branch
        .as_ref()
        .map(|branch| branch.cost)
        .unwrap_or(Cost(f64::INFINITY));
    if let Some(chosen_in_seek) = choose_best_in_seek_candidate(
        rhs_table,
        table_constraints,
        lhs_mask,
        1.0,
        base_row_count,
        params,
        in_seek_threshold,
        BranchReadMode::RowIdOnly,
    )? {
        let Some(source) = in_seek_source_from_expr(
            &branch_terms[chosen_in_seek.constraint_idx].expr,
            &chosen_in_seek,
        ) else {
            return Ok(None);
        };
        best_branch = Some(MultiIdxBranch {
            index: chosen_in_seek.index,
            access: MultiIdxBranchAccess::InSeek {
                source,
                constraint_idx: chosen_in_seek.constraint_idx,
            },
            cost: chosen_in_seek.cost,
            estimated_rows: chosen_in_seek.estimated_rows_per_outer_row,
            union_prepost_filters: None,
        });
    }

    Ok(best_branch)
}

/// Residual output from [`partition_residual_multi_or_exprs`].
struct MultiOrResidualPrePostFilters {
    pre_filter_exprs: Vec<ast::Expr>,
    post_filter_exprs: Vec<ast::Expr>,
    /// Combined table mask for `post_filter_exprs`.
    post_mask: TableMask,
}

/// Classify unconsumed branch conjuncts into pre-filters (outer-table-only,
/// evaluated before the index seek) and post-filters (evaluated after the seek).
///
/// Returns `None` if any residual contains a subquery or has an unresolvable
/// table mask—matching the old `residual_tables_mask` rejection.
fn partition_residual_multi_or_exprs(
    branch_terms: &[WhereTerm],
    access: &MultiIdxBranchAccess,
    index: Option<&Index>,
    rhs_table: &JoinedTable,
    lhs_mask: &TableMask,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
) -> Result<Option<MultiOrResidualPrePostFilters>> {
    let mut consumed = vec![false; branch_terms.len()];
    match access {
        MultiIdxBranchAccess::Seek {
            constraints,
            constraint_refs,
        } => {
            for cref in constraint_refs.iter() {
                for idx in [
                    cref.eq.as_ref().map(|e| e.constraint_pos),
                    cref.lower_bound,
                    cref.upper_bound,
                ]
                .into_iter()
                .flatten()
                {
                    consumed[constraints[idx].where_clause_pos.0] = true;
                }
            }
        }
        MultiIdxBranchAccess::InSeek { constraint_idx, .. } => consumed[*constraint_idx] = true,
    }
    if let Some(index) = index {
        if index.where_clause.is_some() {
            let Some(predicate_terms) =
                partial_index_predicate_terms(index, rhs_table, branch_terms)
            else {
                return Ok(None);
            };
            for idx in predicate_terms {
                consumed[idx] = true;
            }
        }
    }

    let mut pre_filter_exprs = Vec::new();
    let mut post_filter_exprs = Vec::new();
    let mut post_mask = TableMask::default();

    for (idx, term) in branch_terms.iter().enumerate() {
        if consumed[idx] {
            continue;
        }
        let expr = &term.expr;
        if expr_references_any_subquery(expr) {
            return Ok(None);
        }
        let mask = table_mask_from_expr(expr, table_references, subqueries)?;
        if lhs_mask.contains_all_set_bits_of(&mask) {
            pre_filter_exprs.push(expr.clone());
        } else {
            post_mask.union_with(&mask)?;
            post_filter_exprs.push(expr.clone());
        }
    }

    Ok(Some(MultiOrResidualPrePostFilters {
        pre_filter_exprs,
        post_filter_exprs,
        post_mask,
    }))
}

/// Estimate selectivity for a residual predicate that remains after a branch
/// seek is chosen.
///
/// We keep this intentionally heuristic: recurse through boolean structure and,
/// for leaf predicates, reuse normal constraint selectivity analysis when the
/// expression can be recognized as a single-table constraint.
#[allow(clippy::too_many_arguments)]
fn estimate_residual_expr_selectivity(
    expr: &ast::Expr,
    rhs_table: &JoinedTable,
    table_references: &TableReferences,
    available_indexes: &AvailableIndexes,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    params: &CostModelParams,
) -> f64 {
    let Ok(expr) = crate::translate::expr::unwrap_parens(expr) else {
        return params.sel_other;
    };

    match expr {
        ast::Expr::Binary(lhs, ast::Operator::And, rhs) => {
            estimate_residual_expr_selectivity(
                lhs,
                rhs_table,
                table_references,
                available_indexes,
                subqueries,
                schema,
                params,
            ) * estimate_residual_expr_selectivity(
                rhs,
                rhs_table,
                table_references,
                available_indexes,
                subqueries,
                schema,
                params,
            )
        }
        ast::Expr::Binary(lhs, ast::Operator::Or, rhs) => {
            let lhs_selectivity = estimate_residual_expr_selectivity(
                lhs,
                rhs_table,
                table_references,
                available_indexes,
                subqueries,
                schema,
                params,
            );
            let rhs_selectivity = estimate_residual_expr_selectivity(
                rhs,
                rhs_table,
                table_references,
                available_indexes,
                subqueries,
                schema,
                params,
            );
            1.0 - (1.0 - lhs_selectivity) * (1.0 - rhs_selectivity)
        }
        ast::Expr::Unary(ast::UnaryOperator::Not, inner) => {
            1.0 - estimate_residual_expr_selectivity(
                inner,
                rhs_table,
                table_references,
                available_indexes,
                subqueries,
                schema,
                params,
            )
        }
        _ => {
            let Ok((_, table_constraints)) = get_table_local_constraints_for_branch(
                &[expr.clone()],
                None,
                rhs_table,
                table_references,
                available_indexes,
                subqueries,
                schema,
                params,
            ) else {
                return params.sel_other;
            };

            table_constraints
                .constraints
                .iter()
                .filter(|constraint| constraint.where_clause_pos.0 == 0)
                .map(|constraint| constraint.selectivity)
                // A single residual expression can sometimes yield multiple
                // derived constraints (for example, self-comparisons). Use the
                // strongest single estimate instead of multiplying duplicates.
                .reduce(f64::min)
                .unwrap_or(params.sel_other)
        }
    }
    .clamp(0.0, 1.0)
}

#[allow(clippy::too_many_arguments)]
fn estimate_multi_or_residual_selectivity(
    residual_exprs: &[ast::Expr],
    rhs_table: &JoinedTable,
    table_references: &TableReferences,
    available_indexes: &AvailableIndexes,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    params: &CostModelParams,
) -> f64 {
    residual_exprs
        .iter()
        .map(|expr| {
            estimate_residual_expr_selectivity(
                expr,
                rhs_table,
                table_references,
                available_indexes,
                subqueries,
                schema,
                params,
            )
        })
        .product::<f64>()
        .clamp(0.0, 1.0)
}

#[allow(clippy::too_many_arguments)]
/// Evaluate a fully decomposed multi-index plan and return it if it beats the
/// current best non-multi-index access cost.
fn evaluate_multi_index_branches(
    branches: Vec<MultiIdxBranch>,
    set_op: SetOperation,
    where_term_idx: usize,
    rhs_table: &JoinedTable,
    table_references: &TableReferences,
    available_indexes: &AvailableIndexes,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    base_row_count: RowCountEstimate,
    input_cardinality: f64,
    params: &CostModelParams,
    best_cost: Cost,
) -> Result<Option<AccessMethod>> {
    let mut branch_costs = Vec::with_capacity(branches.len());
    let mut branch_rows = Vec::with_capacity(branches.len());
    let mut branch_params = Vec::with_capacity(branches.len());

    for branch in branches {
        let where_cost = branch
            .union_prepost_filters
            .as_ref()
            .map(|filters| {
                let pre_steps: usize = filters.pre_filter_exprs.iter().map(where_expr_steps).sum();
                let post_steps: usize =
                    filters.post_filter_exprs.iter().map(where_expr_steps).sum();
                Cost(
                    (pre_steps as f64 + branch.estimated_rows * post_steps as f64)
                        * params.cpu_cost_per_where_step,
                )
            })
            .unwrap_or(Cost(0.0));
        let post_filter_exprs = branch
            .union_prepost_filters
            .as_ref()
            .map(|r| &r.post_filter_exprs);
        let selectivity = if let Some(post_filter_exprs) = post_filter_exprs {
            estimate_multi_or_residual_selectivity(
                post_filter_exprs,
                rhs_table,
                table_references,
                available_indexes,
                subqueries,
                schema,
                params,
            )
        } else {
            1.0
        };
        let estimated_rows = branch.estimated_rows * selectivity;

        let params_for_branch = MultiIndexBranchParams {
            index: branch.index.clone(),
            access: match branch.access {
                MultiIdxBranchAccess::Seek {
                    constraints,
                    constraint_refs,
                } => MultiIndexBranchAccessParams::Seek {
                    constraints,
                    constraint_refs,
                },
                MultiIdxBranchAccess::InSeek { source, .. } => {
                    MultiIndexBranchAccessParams::InSeek { source }
                }
            },
            estimated_rows,
            residuals: branch.union_prepost_filters,
        };

        branch_costs.push(branch.cost + where_cost);
        branch_rows.push(params_for_branch.estimated_rows);
        branch_params.push(params_for_branch);
    }

    let (multi_index_cost, estimated_rows) = match &set_op {
        SetOperation::Union => estimate_multi_index_scan_cost(
            &branch_costs,
            &branch_rows,
            base_row_count,
            input_cardinality,
            params,
        ),
        SetOperation::Intersection { .. } => estimate_multi_index_intersection_cost(
            &branch_costs,
            &branch_rows,
            base_row_count,
            input_cardinality,
            params,
        ),
    };

    if multi_index_cost < best_cost {
        let mut consumed_where_terms: BitSet<usize> = BitSet::default();
        consumed_where_terms.set(where_term_idx)?;
        if let SetOperation::Intersection {
            additional_consumed_terms,
        } = &set_op
        {
            for term_idx in additional_consumed_terms.iter() {
                consumed_where_terms.set(term_idx)?;
            }
        }
        for branch in &branch_params {
            if let MultiIndexBranchAccessParams::Seek { constraints, .. } = &branch.access {
                for constraint in constraints {
                    let where_term_idx = constraint.where_clause_pos.0;
                    consumed_where_terms.set(where_term_idx)?;
                }
            }
        }
        Ok(Some(AccessMethod {
            cost: multi_index_cost,
            estimated_rows_per_outer_row: estimated_rows,
            consumed_where_terms,
            params: AccessMethodParams::MultiIndexScan {
                branches: branch_params,
                where_term_idx,
                set_op,
            },
        }))
    } else {
        Ok(None)
    }
}

/// Whether a multi-index scan on `table` may be driven by `term`.
///
/// The scan *is* the term's evaluation: rows failing it are never visited, and
/// the term is marked consumed so nothing checks it again. For a table that an
/// outer join can null-extend (the right-hand table of a LEFT/FULL JOIN, or
/// any table on the left side of a FULL JOIN) that only holds for the join's
/// own ON clause, which defines what counts as a match. Any other term must
/// also reject the null-extended row the join emits when nothing matched, and
/// that row is produced by jumping straight past the scan — so consuming such
/// a term silently drops it.
fn multi_index_can_consume_term(
    table: &JoinedTable,
    term: &WhereTerm,
    table_references: &TableReferences,
) -> bool {
    !table_references.outer_join_may_null_extend(table.internal_id)
        || term.from_outer_join == Some(table.internal_id)
}

#[allow(clippy::too_many_arguments)]
/// Analyze top-level AND terms to determine whether they can be executed as an
/// AND-by-intersection plan.
///
/// Returns `Some(...)` only when:
/// 1. Multiple terms constrain the same table
/// 2. Each term is individually indexable
/// 3. No single composite index already covers multiple terms more directly
/// 4. At least two distinct indexes participate in the final branch set
fn analyze_and_terms_for_multi_index(
    table_reference: &JoinedTable,
    where_clause: &[WhereTerm],
    available_indexes: &AvailableIndexes,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    params: &CostModelParams,
) -> Option<AndClauseDecomposition> {
    let table_id = table_reference.internal_id;
    let indexes = available_indexes.indexes_for_table(table_reference.internal_id);
    let rowid_alias_column = table_reference
        .columns()
        .iter()
        .position(|c| c.is_rowid_alias());

    // Collect AND terms that:
    // 1. Reference this table
    // 2. Are simple binary comparisons
    // 3. Can use an index
    // 4. Are not already consumed
    // 5. Are local constraints rather than cross-table join conditions
    let mut candidate_branches: Vec<AndBranchSummary> = Vec::new();

    for (where_term_idx, term) in where_clause.iter().enumerate() {
        if term.consumed || matches!(&term.expr, ast::Expr::Binary(_, ast::Operator::Or, _)) {
            continue;
        }
        if !multi_index_can_consume_term(table_reference, term, table_references) {
            continue;
        }

        let Some(summary) = summarize_binary_term_for_index(
            &term.expr,
            table_id,
            table_reference,
            where_clause,
            indexes,
            rowid_alias_column,
            table_references,
            subqueries,
        ) else {
            continue;
        };

        if !summary.lhs_mask.is_empty() {
            continue;
        }

        candidate_branches.push(AndBranchSummary {
            where_term_idx,
            table_col_pos: summary.table_col_pos,
            index: summary.best_index,
        });
    }

    if candidate_branches.len() < 2 {
        return None;
    }

    // If a composite index already covers multiple constrained columns, prefer
    // that single lookup path over intersection.
    if let Some(indexes) = indexes {
        for index in indexes.iter().filter(|idx| idx.index_method.is_none()) {
            // An unproven partial index cannot be the single-index alternative
            // that suppresses intersection planning.
            if index.where_clause.is_some()
                && !can_use_partial_index(index, table_reference, where_clause)
            {
                continue;
            }
            let mut columns_covered = 0;
            for (i, branch) in candidate_branches.iter().enumerate() {
                let col_pos = branch.table_col_pos;
                if let Some(col_pos) = col_pos {
                    if let Some(idx_pos) = index.column_table_pos_to_index_pos(col_pos) {
                        if idx_pos < index.columns.len() {
                            let earlier_covered = candidate_branches[..i]
                                .iter()
                                .filter_map(|candidate| candidate.table_col_pos)
                                .any(|c| {
                                    index
                                        .column_table_pos_to_index_pos(c)
                                        .is_some_and(|p| p < idx_pos)
                                });
                            if idx_pos == 0 || earlier_covered {
                                columns_covered += 1;
                            }
                        }
                    }
                }
            }
            if columns_covered >= 2 {
                return None;
            }
        }
    }

    // Keep only branches that use distinct named indexes. Rowid (`None`) may
    // still appear more than once because it is not tied to a named index.
    let mut selected_branches: Vec<AndBranchSummary> = Vec::new();
    let mut seen_indexes: Vec<*const Index> = Vec::new();
    for branch in candidate_branches {
        if let Some(index) = branch.index.as_ref() {
            let index_ptr = Arc::as_ptr(index);
            if seen_indexes.contains(&index_ptr) {
                continue;
            }
            seen_indexes.push(index_ptr);
        }
        selected_branches.push(branch);
    }

    if selected_branches.len() < 2 {
        return None;
    }

    let unique_branches = selected_branches
        .into_iter()
        .map(|branch| {
            let analyzed = analyze_binary_term_for_index(
                &where_clause[branch.where_term_idx].expr,
                branch.where_term_idx,
                table_id,
                table_reference,
                where_clause,
                indexes,
                rowid_alias_column,
                table_references,
                subqueries,
                schema,
                params,
            )
            .expect("multi-index prepass accepted a term that full analysis rejected");

            turso_assert_eq!(analyzed.constraint.table_col_pos, branch.table_col_pos);

            AndBranch {
                where_term_idx: branch.where_term_idx,
                constraint: analyzed.constraint,
                index: analyzed.best_index,
                constraint_refs: analyzed.constraint_refs,
            }
        })
        .collect::<Vec<_>>();

    Some(AndClauseDecomposition {
        term_indices: unique_branches.iter().map(|b| b.where_term_idx).collect(),
        branches: unique_branches,
    })
}

#[allow(clippy::too_many_arguments)]
/// Analyze OR clauses for OR-by-union optimization.
///
/// Returns a `MultiIndexScan` access method when every disjunct can be planned
/// as an individual lookup branch and the combined cost beats the current best
/// non-multi-index alternative.
pub fn consider_multi_index_union(
    rhs_table: &JoinedTable,
    where_clause: &[WhereTerm],
    available_indexes: &AvailableIndexes,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
    best_cost: Cost,
    lhs_mask: &TableMask,
    analyze_stats: &AnalyzeStats,
) -> Result<Option<AccessMethod>> {
    for (where_term_idx, term) in where_clause.iter().enumerate() {
        if term.consumed {
            continue;
        }
        if !multi_index_can_consume_term(rhs_table, term, table_references) {
            continue;
        }

        let ast::Expr::Binary(_, ast::Operator::Or, _) = &term.expr else {
            continue;
        };

        let disjuncts = flatten_or_expr(&term.expr);
        if disjuncts.len() < 2 {
            continue;
        }

        let mut allowed_mask = lhs_mask.try_clone()?;
        let Some(rhs_idx) = table_references
            .joined_tables()
            .iter()
            .position(|t| t.internal_id == rhs_table.internal_id)
        else {
            continue;
        };
        allowed_mask.set(rhs_idx)?;

        // Each disjunct is replanned with branch-local `TableConstraints`, so
        // compound conjuncts can reuse the same compound-seek analysis as
        // ordinary btree access.
        let branches = disjuncts
            .into_iter()
            .map(|disjunct_expr| {
                let Ok(disjunct_expr) = crate::translate::expr::unwrap_parens(disjunct_expr) else {
                    return Ok(None);
                };
                let conjuncts = flatten_and_expr(disjunct_expr)
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                let Some((synthetic_where_terms, table_constraints)) =
                    get_table_local_constraints_for_branch(
                        &conjuncts,
                        term.from_outer_join,
                        rhs_table,
                        table_references,
                        available_indexes,
                        subqueries,
                        schema,
                        params,
                    )
                    .ok()
                else {
                    return Ok(None);
                };
                let Some(mut chosen) = choose_multi_index_branch_access(
                    rhs_table,
                    &table_constraints,
                    &synthetic_where_terms,
                    lhs_mask,
                    rhs_idx,
                    schema,
                    available_indexes,
                    base_row_count,
                    analyze_stats,
                    params,
                )?
                else {
                    return Ok(None);
                };
                // Partition residuals in a single pass: pre-filters reference
                // only outer (lhs) tables and can short-circuit the branch
                // before the index seek; post-filters reference the target
                // table and are evaluated after the seek.
                let Some(partitioned_pre_post) = partition_residual_multi_or_exprs(
                    &synthetic_where_terms,
                    &chosen.access,
                    chosen.index.as_deref(),
                    rhs_table,
                    lhs_mask,
                    table_references,
                    subqueries,
                )?
                else {
                    return Ok(None);
                };
                if !allowed_mask.contains_all_set_bits_of(&partitioned_pre_post.post_mask) {
                    return Ok(None);
                }
                chosen.union_prepost_filters = Some(UnionBranchPrePostFilters {
                    requires_table_cursor: partitioned_pre_post.post_mask.get(rhs_idx),
                    pre_filter_exprs: partitioned_pre_post.pre_filter_exprs,
                    post_filter_exprs: partitioned_pre_post.post_filter_exprs,
                });
                Ok(Some(chosen))
            })
            .collect::<Result<_>>()?;

        let Some(branches) = branches else {
            continue;
        };

        if let Some(access_method) = evaluate_multi_index_branches(
            branches,
            SetOperation::Union,
            where_term_idx,
            rhs_table,
            table_references,
            available_indexes,
            subqueries,
            schema,
            base_row_count,
            input_cardinality,
            params,
            best_cost,
        )? {
            return Ok(Some(access_method));
        }
    }

    Ok(None)
}

/// Analyze top-level AND terms for AND-by-intersection optimization.
///
/// This is more restrictive than OR-by-union because every branch must be a
/// local term on the current table, and the final plan only survives if it
/// beats the best ordinary access path.
#[expect(clippy::too_many_arguments)]
pub fn consider_multi_index_intersection(
    rhs_table: &JoinedTable,
    where_clause: &[WhereTerm],
    available_indexes: &AvailableIndexes,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
    best_cost: Cost,
    lhs_mask: &TableMask,
    analyze_stats: &AnalyzeStats,
) -> Result<Option<AccessMethod>> {
    let Some(decomposition) = analyze_and_terms_for_multi_index(
        rhs_table,
        where_clause,
        available_indexes,
        table_references,
        subqueries,
        schema,
        params,
    ) else {
        return Ok(None);
    };

    if decomposition.branches.len() < 2 {
        return Ok(None);
    }

    let all_usable = decomposition
        .branches
        .iter()
        .all(|b| lhs_mask.contains_all_set_bits_of(&b.constraint.lhs_mask));
    if !all_usable {
        return Ok(None);
    }

    let branches: Vec<_> = decomposition
        .branches
        .iter()
        .map(|b| {
            let constraints = vec![b.constraint.clone()];
            let index_info = index_info_for_branch(
                b.index.as_deref(),
                rhs_table,
                BranchReadMode::RowIdOnly,
                params.rows_per_table_page,
            )
            .expect("intersection branches always have costable access");
            let analyze_ctx = AnalyzeCtx {
                rhs_table,
                index: b.index.as_ref(),
                stats: analyze_stats,
            };
            MultiIdxBranch {
                index: b.index.clone(),
                access: MultiIdxBranchAccess::Seek {
                    constraints: constraints.clone(),
                    constraint_refs: b.constraint_refs.clone(),
                },
                cost: estimate_cost_for_scan_or_seek(
                    Some(index_info),
                    &constraints,
                    &b.constraint_refs,
                    1.0,
                    base_row_count,
                    false,
                    params,
                    Some(&analyze_ctx),
                ),
                estimated_rows: estimate_rows_per_seek(
                    index_info,
                    &constraints,
                    &b.constraint_refs,
                    base_row_count,
                    Some(&analyze_ctx),
                ),
                union_prepost_filters: None,
            }
        })
        .collect();

    let where_term_idx = decomposition.term_indices[0];
    let additional_consumed_terms: BitSet = decomposition
        .term_indices
        .iter()
        .skip(1)
        .copied()
        .try_collect()?;

    evaluate_multi_index_branches(
        branches,
        SetOperation::Intersection {
            additional_consumed_terms,
        },
        where_term_idx,
        rhs_table,
        table_references,
        available_indexes,
        subqueries,
        schema,
        base_row_count,
        input_cardinality,
        params,
        best_cost,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        consider_multi_index_intersection, consider_multi_index_union, AnalyzeStats,
        MultiIndexBranchParams,
    };
    use crate::alloc::TursoIteratorExt;
    use crate::alloc::TursoSliceExt;
    use crate::{
        schema::{
            BTreeCharacteristics, BTreeTable, ColDef, Column, Index, IndexColumn, Schema, Table,
            Type,
        },
        translate::{
            optimizer::{
                access_method::AccessMethodParams,
                cost::{Cost, RowCountEstimate},
                cost_params::DEFAULT_PARAMS,
                AvailableIndexes,
            },
            plan::{
                ColumnUsedMask, JoinInfo, JoinType, JoinedTable, Operation, TableReferences,
                WhereTerm,
            },
            planner::TableMask,
        },
        vdbe::builder::TableRefIdCounter,
        MAIN_DB_ID,
    };
    use std::{collections::VecDeque, sync::Arc};
    use turso_parser::ast::{self, Expr, Operator, TableInternalId};

    struct TestColumn {
        name: String,
        ty: Type,
        is_rowid_alias: bool,
    }

    fn empty_schema() -> Schema {
        Schema::default()
    }

    fn create_column(c: &TestColumn) -> Column {
        Column::new(
            Some(c.name.clone()),
            c.ty.to_string(),
            None,
            None,
            c.ty,
            None,
            ColDef {
                primary_key: false,
                rowid_alias: c.is_rowid_alias,
                ..Default::default()
            },
        )
    }

    fn create_column_of_type(name: &str, ty: Type) -> Column {
        create_column(&TestColumn {
            name: name.to_string(),
            ty,
            is_rowid_alias: false,
        })
    }

    fn create_btree_table(name: &str, columns: Vec<Column>) -> Arc<BTreeTable> {
        Arc::new(BTreeTable::new(
            1,
            name.to_string(),
            crate::alloc::vec![],
            columns.try_to_vec().expect(crate::alloc::ALLOC_ERR_MSG),
            BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        ))
    }

    fn create_table_reference(
        table: Arc<BTreeTable>,
        join_info: Option<JoinInfo>,
        internal_id: TableInternalId,
    ) -> JoinedTable {
        let name = table.name.clone();
        let table = Table::BTree(table);
        JoinedTable {
            op: Operation::default_scan_for(&table),
            table,
            identifier: name,
            internal_id,
            join_info,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: MAIN_DB_ID,
            plan_estimate: None,
            indexed: None,
        }
    }

    fn create_column_expr(table: TableInternalId, column: usize, is_rowid_alias: bool) -> Expr {
        Expr::Column {
            database: None,
            table,
            column,
            is_rowid_alias,
        }
    }

    fn create_numeric_literal(value: &str) -> Expr {
        Expr::Literal(ast::Literal::Numeric(value.to_string()))
    }

    fn create_string_literal(value: &str) -> Expr {
        Expr::Literal(ast::Literal::String(value.to_string()))
    }

    fn assert_is_multi_index(
        access_method: &crate::translate::optimizer::access_method::AccessMethod,
    ) -> &Vec<MultiIndexBranchParams> {
        let AccessMethodParams::MultiIndexScan { branches, .. } = &access_method.params else {
            panic!("expected multi-index scan access method");
        };
        branches
    }

    #[test]
    fn test_multi_index_union_rejects_residuals_on_future_tables() {
        let link = create_btree_table(
            "link",
            vec![
                create_column_of_type("src", Type::Integer),
                create_column_of_type("dst", Type::Integer),
            ],
        );
        let item = create_btree_table(
            "item",
            vec![
                create_column_of_type("id", Type::Integer),
                create_column_of_type("kind", Type::Text),
            ],
        );
        let meta = create_btree_table(
            "meta",
            vec![
                create_column_of_type("id", Type::Integer),
                create_column_of_type("kind", Type::Text),
            ],
        );

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![
            create_table_reference(link, None, table_id_counter.next()),
            create_table_reference(
                item,
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            ),
            create_table_reference(
                meta,
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            ),
        ];

        const LINK: usize = 0;
        const ITEM: usize = 1;
        const META: usize = 2;

        let mut available_indexes = AvailableIndexes::default();
        available_indexes.insert_for_table_name(
            &joined_tables,
            "item",
            VecDeque::from([Arc::new(Index {
                name: "idx_item_id".to_string(),
                table_name: "item".to_string(),
                where_clause: None,
                columns: IndexColumn::new_many(vec!["id"]),
                unique: false,
                ephemeral: false,
                root_page: 2,
                has_rowid: true,
                index_method: None,
                on_conflict: None,
            })]),
        );

        let lhs_link_src = Expr::Binary(
            Box::new(create_column_expr(
                joined_tables[LINK].internal_id,
                0,
                false,
            )),
            Operator::Equals,
            Box::new(create_numeric_literal("1")),
        );
        let lhs_link_dst_item_id = Expr::Binary(
            Box::new(create_column_expr(
                joined_tables[LINK].internal_id,
                1,
                false,
            )),
            Operator::Equals,
            Box::new(create_column_expr(
                joined_tables[ITEM].internal_id,
                0,
                false,
            )),
        );
        let rhs_link_dst = Expr::Binary(
            Box::new(create_column_expr(
                joined_tables[LINK].internal_id,
                1,
                false,
            )),
            Operator::Equals,
            Box::new(create_numeric_literal("1")),
        );
        let rhs_link_src_item_id = Expr::Binary(
            Box::new(create_column_expr(
                joined_tables[LINK].internal_id,
                0,
                false,
            )),
            Operator::Equals,
            Box::new(create_column_expr(
                joined_tables[ITEM].internal_id,
                0,
                false,
            )),
        );
        let future_meta_kind = Expr::Binary(
            Box::new(create_column_expr(
                joined_tables[META].internal_id,
                1,
                false,
            )),
            Operator::Equals,
            Box::new(create_string_literal("entity")),
        );

        let left_disjunct = Expr::Binary(
            Box::new(Expr::Binary(
                Box::new(lhs_link_src),
                Operator::And,
                Box::new(lhs_link_dst_item_id),
            )),
            Operator::And,
            Box::new(future_meta_kind.clone()),
        );
        let right_disjunct = Expr::Binary(
            Box::new(Expr::Binary(
                Box::new(rhs_link_dst),
                Operator::And,
                Box::new(rhs_link_src_item_id),
            )),
            Operator::And,
            Box::new(future_meta_kind),
        );
        let where_clause = vec![WhereTerm {
            expr: Expr::Binary(
                Box::new(left_disjunct),
                Operator::Or,
                Box::new(right_disjunct),
            ),
            from_outer_join: None,
            consumed: false,
        }];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let base_row_count = RowCountEstimate::hardcoded_fallback(&DEFAULT_PARAMS);
        let lhs_mask: TableMask = [LINK].into_iter().try_collect().unwrap();

        let access_method = consider_multi_index_union(
            &table_references.joined_tables()[ITEM],
            &where_clause,
            &available_indexes,
            &table_references,
            &[],
            &empty_schema(),
            1.0,
            base_row_count,
            &DEFAULT_PARAMS,
            Cost(f64::INFINITY),
            &lhs_mask,
            &AnalyzeStats::default(),
        )
        .unwrap();

        assert!(
            access_method.is_none(),
            "future-table residuals must not produce a multi-index OR access method"
        );
    }

    #[test]
    fn test_multi_index_intersection_supports_rowid_and_secondary_index_branches() {
        let item = create_btree_table(
            "item",
            vec![
                create_column(&TestColumn {
                    name: "id".to_string(),
                    ty: Type::Integer,
                    is_rowid_alias: true,
                }),
                create_column_of_type("a", Type::Integer),
            ],
        );

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![create_table_reference(item, None, table_id_counter.next())];
        let item_id = joined_tables[0].internal_id;

        let mut available_indexes = AvailableIndexes::default();
        available_indexes.insert_for_table_name(
            &joined_tables,
            "item",
            VecDeque::from([Arc::new(Index {
                name: "idx_item_a".to_string(),
                table_name: "item".to_string(),
                where_clause: None,
                columns: crate::alloc::vec![IndexColumn::new("a", 1)],
                unique: false,
                ephemeral: false,
                root_page: 2,
                has_rowid: true,
                index_method: None,
                on_conflict: None,
            })]),
        );

        let where_clause = vec![
            WhereTerm {
                expr: Expr::Binary(
                    Box::new(create_column_expr(item_id, 0, true)),
                    Operator::Greater,
                    Box::new(create_numeric_literal("10")),
                ),
                from_outer_join: None,
                consumed: false,
            },
            WhereTerm {
                expr: Expr::Binary(
                    Box::new(create_column_expr(item_id, 1, false)),
                    Operator::Equals,
                    Box::new(create_numeric_literal("7")),
                ),
                from_outer_join: None,
                consumed: false,
            },
        ];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let base_row_count = RowCountEstimate::hardcoded_fallback(&DEFAULT_PARAMS);

        let access_method = consider_multi_index_intersection(
            &table_references.joined_tables()[0],
            &where_clause,
            &available_indexes,
            &table_references,
            &[],
            &empty_schema(),
            1.0,
            base_row_count,
            &DEFAULT_PARAMS,
            Cost(f64::INFINITY),
            &TableMask::default(),
            &AnalyzeStats::default(),
        )
        .unwrap()
        .expect("rowid and secondary-index terms should be eligible for intersection");

        let branches = assert_is_multi_index(&access_method);
        assert_eq!(branches.len(), 2);
        assert!(
            branches.iter().any(|branch| branch.index.is_none()),
            "expected one rowid branch"
        );
        assert!(
            branches
                .iter()
                .any(|branch| branch.index.as_ref().map(|idx| idx.name.as_str())
                    == Some("idx_item_a")),
            "expected one secondary-index branch"
        );
    }

    #[test]
    fn test_multi_index_union_branch_reuses_compound_seek_analysis() {
        let link = create_btree_table(
            "link",
            vec![
                create_column_of_type("src", Type::Integer),
                create_column_of_type("dst", Type::Integer),
            ],
        );
        let item = create_btree_table(
            "item",
            vec![
                create_column_of_type("id", Type::Integer),
                create_column_of_type("kind", Type::Integer),
            ],
        );

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![
            create_table_reference(link, None, table_id_counter.next()),
            create_table_reference(
                item,
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            ),
        ];

        const LINK: usize = 0;
        const ITEM: usize = 1;

        let mut available_indexes = AvailableIndexes::default();
        available_indexes.insert_for_table_name(
            &joined_tables,
            "item",
            VecDeque::from([Arc::new(Index {
                name: "idx_item_id_kind".to_string(),
                table_name: "item".to_string(),
                where_clause: None,
                columns: IndexColumn::new_many(vec!["id", "kind"]),
                unique: false,
                ephemeral: false,
                root_page: 2,
                has_rowid: true,
                index_method: None,
                on_conflict: None,
            })]),
        );

        let left_disjunct = Expr::Binary(
            Box::new(Expr::Binary(
                Box::new(Expr::Binary(
                    Box::new(create_column_expr(
                        joined_tables[LINK].internal_id,
                        0,
                        false,
                    )),
                    Operator::Equals,
                    Box::new(create_numeric_literal("1")),
                )),
                Operator::And,
                Box::new(Expr::Binary(
                    Box::new(create_column_expr(
                        joined_tables[ITEM].internal_id,
                        0,
                        false,
                    )),
                    Operator::Equals,
                    Box::new(create_column_expr(
                        joined_tables[LINK].internal_id,
                        1,
                        false,
                    )),
                )),
            )),
            Operator::And,
            Box::new(Expr::Binary(
                Box::new(create_column_expr(
                    joined_tables[ITEM].internal_id,
                    1,
                    false,
                )),
                Operator::Equals,
                Box::new(create_numeric_literal("7")),
            )),
        );
        let right_disjunct = Expr::Binary(
            Box::new(Expr::Binary(
                Box::new(Expr::Binary(
                    Box::new(create_column_expr(
                        joined_tables[LINK].internal_id,
                        1,
                        false,
                    )),
                    Operator::Equals,
                    Box::new(create_numeric_literal("1")),
                )),
                Operator::And,
                Box::new(Expr::Binary(
                    Box::new(create_column_expr(
                        joined_tables[ITEM].internal_id,
                        0,
                        false,
                    )),
                    Operator::Equals,
                    Box::new(create_column_expr(
                        joined_tables[LINK].internal_id,
                        0,
                        false,
                    )),
                )),
            )),
            Operator::And,
            Box::new(Expr::Binary(
                Box::new(create_column_expr(
                    joined_tables[ITEM].internal_id,
                    1,
                    false,
                )),
                Operator::Equals,
                Box::new(create_numeric_literal("7")),
            )),
        );

        let where_clause = vec![WhereTerm {
            expr: Expr::Binary(
                Box::new(left_disjunct),
                Operator::Or,
                Box::new(right_disjunct),
            ),
            from_outer_join: None,
            consumed: false,
        }];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let lhs_mask = [LINK].into_iter().try_collect().unwrap();
        let base_row_count = RowCountEstimate::hardcoded_fallback(&DEFAULT_PARAMS);

        let access_method = consider_multi_index_union(
            &table_references.joined_tables()[ITEM],
            &where_clause,
            &available_indexes,
            &table_references,
            &[],
            &empty_schema(),
            1.0,
            base_row_count,
            &DEFAULT_PARAMS,
            Cost(f64::INFINITY),
            &lhs_mask,
            &AnalyzeStats::default(),
        )
        .unwrap()
        .expect("compound OR branches should produce a multi-index union");

        let branches = assert_is_multi_index(&access_method);
        assert_eq!(branches.len(), 2);
        for branch in branches {
            assert_eq!(
                branch.index.as_ref().map(|idx| idx.name.as_str()),
                Some("idx_item_id_kind")
            );
            let super::MultiIndexBranchAccessParams::Seek {
                constraint_refs, ..
            } = &branch.access
            else {
                panic!("compound OR test should choose ordinary seek branches");
            };
            assert_eq!(
                constraint_refs.len(),
                2,
                "branch should use both id and kind in the compound seek"
            );
        }
    }

    #[test]
    fn test_multi_index_union_residual_selectivity_reduces_row_estimate() {
        let link = create_btree_table(
            "link",
            vec![
                create_column_of_type("src", Type::Integer),
                create_column_of_type("dst", Type::Integer),
            ],
        );
        let item = create_btree_table(
            "item",
            vec![
                create_column_of_type("id", Type::Integer),
                create_column_of_type("kind", Type::Integer),
            ],
        );

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![
            create_table_reference(link, None, table_id_counter.next()),
            create_table_reference(
                item,
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            ),
        ];

        const LINK: usize = 0;
        const ITEM: usize = 1;
        let link_id = joined_tables[LINK].internal_id;
        let item_id = joined_tables[ITEM].internal_id;

        let mut available_indexes = AvailableIndexes::default();
        available_indexes.insert_for_table_name(
            &joined_tables,
            "item",
            VecDeque::from([Arc::new(Index {
                name: "idx_item_id".to_string(),
                table_name: "item".to_string(),
                where_clause: None,
                columns: IndexColumn::new_many(vec!["id"]),
                unique: false,
                ephemeral: false,
                root_page: 2,
                has_rowid: true,
                index_method: None,
                on_conflict: None,
            })]),
        );

        let make_branch = |literal_col, join_col, item_kind: Option<&str>| {
            let branch = Expr::Binary(
                Box::new(Expr::Binary(
                    Box::new(create_column_expr(link_id, literal_col, false)),
                    Operator::Equals,
                    Box::new(create_numeric_literal("1")),
                )),
                Operator::And,
                Box::new(Expr::Binary(
                    Box::new(create_column_expr(item_id, 0, false)),
                    Operator::Equals,
                    Box::new(create_column_expr(link_id, join_col, false)),
                )),
            );

            if let Some(kind) = item_kind {
                Expr::Binary(
                    Box::new(branch),
                    Operator::And,
                    Box::new(Expr::Binary(
                        Box::new(create_column_expr(item_id, 1, false)),
                        Operator::Equals,
                        Box::new(create_numeric_literal(kind)),
                    )),
                )
            } else {
                branch
            }
        };
        let make_join_expr = |item_kind: Option<&str>| {
            vec![WhereTerm {
                expr: Expr::Binary(
                    Box::new(make_branch(0, 1, item_kind)),
                    Operator::Or,
                    Box::new(make_branch(1, 0, item_kind)),
                ),
                from_outer_join: None,
                consumed: false,
            }]
        };

        let table_references = TableReferences::new(joined_tables, vec![]);
        let lhs_mask = [LINK].into_iter().try_collect().unwrap();
        let base_row_count = RowCountEstimate::hardcoded_fallback(&DEFAULT_PARAMS);

        let without_residual = consider_multi_index_union(
            &table_references.joined_tables()[ITEM],
            &make_join_expr(None),
            &available_indexes,
            &table_references,
            &[],
            &empty_schema(),
            1.0,
            base_row_count,
            &DEFAULT_PARAMS,
            Cost(f64::INFINITY),
            &lhs_mask,
            &AnalyzeStats::default(),
        )
        .unwrap()
        .expect("plain OR branches should produce a multi-index union");

        let with_residual = consider_multi_index_union(
            &table_references.joined_tables()[ITEM],
            &make_join_expr(Some("7")),
            &available_indexes,
            &table_references,
            &[],
            &empty_schema(),
            1.0,
            base_row_count,
            &DEFAULT_PARAMS,
            Cost(f64::INFINITY),
            &lhs_mask,
            &AnalyzeStats::default(),
        )
        .unwrap()
        .expect("residual-filtered OR branches should still produce a multi-index union");

        assert!(
            with_residual.estimated_rows_per_outer_row
                < without_residual.estimated_rows_per_outer_row,
            "branch-local residual filters must reduce the multi-index row estimate"
        );
    }
}
