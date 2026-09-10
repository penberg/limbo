use crate::sync::Arc;
use rustc_hash::FxHashMap as HashMap;
use smallvec::SmallVec;
use std::iter;

use turso_ext::{ConstraintInfo, ConstraintUsage, ResultCode};
use turso_parser::ast::{self, SortOrder, TableInternalId};

use crate::alloc::{TursoIteratorExt, TursoTryWithCapacityExt, TursoVecExt};
use crate::schema::Schema;
use crate::stats::AnalyzeStats;
use crate::translate::expr::{as_binary_components, comparison_affinity, walk_expr, WalkControl};
use crate::translate::optimizer::constraints::{
    convert_to_vtab_constraint, expr_uses_custom_collation, ordered_ephemeral_key_columns,
    partial_index, partial_index_predicate_terms, BinaryExprSide, Constraint, ConstraintOperator,
    RangeConstraintRef,
};
use crate::translate::optimizer::cost::{rows_per_leaf_page_for_index, RowCountEstimate};
use crate::translate::optimizer::cost_params::CostModelParams;
use crate::translate::plan::{
    plan_has_outer_scope_dependency, BitSet, HashJoinKey, HashJoinType, NonFromClauseSubquery,
    Plan, SetOperation, SubqueryState, TableReferences, WhereTerm,
};
use crate::util::exprs_are_equivalent;
use crate::vdbe::affinity::Affinity;
use crate::vdbe::hash_table::DEFAULT_MEM_BUDGET;
use crate::{
    schema::{FromClauseSubquery, Index, IndexColumn, Table},
    translate::plan::{IndexMethodQuery, IterationDirection, JoinOrderMember, JoinedTable},
    vtab::VirtualTable,
    LimboError, Result,
};

use super::{
    constraints::{
        usable_constraints_for_join_order, usable_constraints_for_lhs_mask, TableConstraints,
    },
    cost::{
        estimate_btree_depth, estimate_cost_for_scan_or_seek, estimate_ephemeral_index_build_cost,
        estimate_index_cost, estimate_rows_per_seek, AnalyzeCtx, Cost, IndexInfo,
    },
    join::JoinPlanningContext,
    multi_index::{
        consider_multi_index_intersection, consider_multi_index_union, MultiIndexBranchParams,
    },
    order::{
        btree_access_order_consumed, subquery_intrinsic_order_consumed, ColumnTarget,
        EqualityPrefixScope, OrderTarget,
    },
    AvailableIndexes,
};
use crate::translate::planner::TableMask;

#[derive(Debug, Clone)]
/// Represents a way to access a table.
pub struct AccessMethod {
    /// The estimated page and CPU work for this path.
    pub cost: Cost,
    /// Estimated rows produced per outer row before applying remaining filters.
    pub estimated_rows_per_outer_row: f64,
    /// WHERE-term indices already accounted for by this access path's row estimate.
    pub consumed_where_terms: BitSet<usize>,
    /// Table-type specific access method details.
    pub params: AccessMethodParams,
}

/// Table‑specific details of how an [`AccessMethod`] operates.
#[derive(Debug, Clone)]
pub enum AccessMethodParams {
    BTreeTable {
        /// The direction of iteration for the access method.
        /// Typically this is backwards only if it helps satisfy an [OrderTarget].
        iter_dir: IterationDirection,
        /// The index that is being used, if any. For rowid based searches (and full table scans), this is None.
        index: Option<Arc<Index>>,
        /// True when this path needs a temporary index.
        /// The index is built after the planner chooses one plan.
        build_index: bool,
        /// The constraint references that are being used, if any.
        /// An empty list of constraint refs means a scan (full table or index);
        /// a non-empty list means a search.
        constraint_refs: Vec<RangeConstraintRef>,
    },
    VirtualTable {
        /// Index identifier returned by the table's `best_index` method.
        idx_num: i32,
        /// Optional index string returned by the table's `best_index` method.
        idx_str: Option<String>,
        /// Constraint descriptors passed to the virtual table’s `filter` method.
        /// Each corresponds to a column/operator pair from the WHERE clause.
        constraints: Vec<ConstraintInfo>,
        /// Information returned by the virtual table's `best_index` method
        /// describing how each constraint will be used.
        constraint_usages: Vec<ConstraintUsage>,
    },
    /// FROM-subquery scan. Coroutine-backed scans run forwards; materialized
    /// subqueries may also be scanned backwards when their intrinsic order
    /// matches the requested extremum order.
    Subquery { iter_dir: IterationDirection },
    /// The single row currently being expanded by a recursive CTE.
    RecursiveCteInput,
    /// Materialized subquery with an ephemeral index for seeking.
    /// The subquery results are materialized once into an ephemeral index,
    /// which can then be seeked using join conditions.
    MaterializedSubquery {
        /// The ephemeral index to build and seek into.
        index: Arc<Index>,
        /// The constraint references used for seeking.
        constraint_refs: Vec<RangeConstraintRef>,
        /// The direction to iterate the ephemeral index once positioned.
        iter_dir: IterationDirection,
    },
    HashJoin {
        /// The table to build the hash table from.
        build_table_idx: usize,
        /// The table to probe the hash table with.
        probe_table_idx: usize,
        /// Join key references - each entry contains the where_clause index and which side
        /// of the equality belongs to the build table. Supports expression-based join keys.
        join_keys: Vec<HashJoinKey>,
        /// Memory budget for the hash table in bytes.
        mem_budget: usize,
        /// Whether the build input should be materialized as a rowid list before hash build.
        materialize_build_input: bool,
        /// Whether to use a bloom filter on the probe side.
        use_bloom_filter: bool,
        /// Join semantics: Inner, LeftOuter, or FullOuter.
        join_type: HashJoinType,
    },
    /// Custom index method access (e.g., FTS).
    /// This variant is used when the optimizer determines that a custom index method
    /// should be used for table access in a join query.
    IndexMethod {
        /// The fully constructed IndexMethodQuery operation to apply to this table.
        query: IndexMethodQuery,
        /// Index in WHERE clause that was covered by this index method (if any).
        where_covered: Option<usize>,
    },
    /// Multi-index scan for OR-by-union or AND-by-intersection optimization.
    /// Used when a WHERE clause has OR/AND terms that can each use a different index.
    /// Example: WHERE a = 1 AND|OR b = 2 with separate indexes on a and b.
    MultiIndexScan {
        /// Each branch represents one term with its own index access.
        branches: Vec<MultiIndexBranchParams>,
        /// Index of the primary WHERE term.
        where_term_idx: usize,
        /// The set operation (Union for OR, Intersection for AND).
        set_op: SetOperation,
    },
    /// IN-list driven index seek.
    InSeek {
        index: Option<Arc<Index>>,
        affinity: Affinity,
        where_term_idx: usize,
    },
}

/// Result of generic btree candidate selection before it is wrapped into a full
/// [`AccessMethod`].
pub(super) struct ChosenBtreeCandidate {
    pub(super) iter_dir: IterationDirection,
    pub(super) index: Option<Arc<Index>>,
    pub(super) constraint_refs: SmallVec<[RangeConstraintRef; 2]>,
    pub(super) base_row_count: RowCountEstimate,
    pub(super) cost: Cost,
}

#[derive(Debug, Clone)]
pub(super) struct ChosenInSeekCandidate {
    pub(super) index: Option<Arc<Index>>,
    pub(super) affinity: Affinity,
    pub(super) constraint_idx: usize,
    pub(super) cost: Cost,
    pub(super) estimated_rows_per_outer_row: f64,
}

/// Describes what a caller needs to read from a branch-local scan.
///
/// Ordinary table access needs the scanned rows themselves, but multi-index
/// branches only harvest rowids into a RowSet and fetch full rows later.
/// Making this explicit avoids threading "mystery bool" flags through the cost
/// model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BranchReadMode {
    /// Cost the branch as if it only needs rowids from the scan.
    RowIdOnly,
    /// Cost the branch as a normal table/index access that may need full row data.
    FullRow,
}

#[allow(clippy::too_many_arguments)]
/// Choose the best ordinary btree lookup candidate for one table under the
/// current join-order prefix.
pub(super) fn choose_best_btree_candidate(
    rhs_table: &JoinedTable,
    rhs_constraints: &TableConstraints,
    lhs_mask: &TableMask,
    rhs_table_idx: usize,
    maybe_order_target: Option<&OrderTarget>,
    schema: &Schema,
    available_indexes: &AvailableIndexes,
    analyze_stats: &AnalyzeStats,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
) -> Result<Option<ChosenBtreeCandidate>> {
    // Seed the baseline with a table scan only if a rowid candidate exists
    // (i.e. no INDEXED BY has removed it). Otherwise start at infinite cost
    // so the forced index candidate always wins.
    let has_rowid_candidate = rhs_constraints.candidates.iter().any(|c| c.index.is_none());
    let mut best_cost = if has_rowid_candidate {
        estimate_cost_for_scan_or_seek(
            None,
            &[],
            &[],
            input_cardinality,
            base_row_count,
            false,
            params,
            None,
        )
    } else {
        Cost(f64::MAX)
    };
    let mut best_choice = ChosenBtreeCandidate {
        iter_dir: IterationDirection::Forwards,
        index: None,
        constraint_refs: SmallVec::new(),
        base_row_count,
        cost: best_cost,
    };
    let mut best_adjusted_output = f64::MAX;
    let mut best_is_ordered = false;

    // Build a mask for the rhs table itself.
    let mut rhs_table_mask = TableMask::default();
    rhs_table_mask.set(rhs_table_idx)?;

    // Estimate cost for each candidate index (including the rowid index) and
    // keep the best candidate.
    for candidate in rhs_constraints.candidates.iter() {
        let usable_constraint_refs = usable_constraints_for_lhs_mask(
            &rhs_constraints.constraints,
            &candidate.refs,
            lhs_mask,
            rhs_table_idx,
        );

        let index_info = match candidate.index.as_ref() {
            Some(index) => IndexInfo {
                unique: index.unique,
                covering: rhs_table.index_is_covering(index),
                column_count: index.columns.len(),
                rows_per_leaf_page: rows_per_leaf_page_for_index(
                    index.columns.len(),
                    rhs_table,
                    params.rows_per_table_page,
                ),
            },
            None => IndexInfo {
                unique: true,
                covering: !usable_constraint_refs.is_empty(),
                column_count: 1,
                rows_per_leaf_page: params.rows_per_table_page,
            },
        };

        let (iter_dir, is_index_ordered, order_satisfiability_bonus) =
            if let Some(order_target) = maybe_order_target {
                // Reuse the same index-vs-order matching logic as final plan
                // validation, but allow any equality-constrained seek prefix to
                // be skipped here. Candidate scoring only needs to know whether
                // this specific access path can emit rows ordered after its seek
                // key; final global ORDER BY validation is stricter and only
                // skips globally constant prefixes.
                let all_same_direction = btree_access_order_consumed(
                    rhs_table,
                    IterationDirection::Forwards,
                    candidate.index.as_deref(),
                    &usable_constraint_refs,
                    order_target,
                    0,
                    schema,
                    EqualityPrefixScope::AnyEquality,
                )
                .consumed
                    == order_target.columns.len();
                let all_opposite_direction = btree_access_order_consumed(
                    rhs_table,
                    IterationDirection::Backwards,
                    candidate.index.as_deref(),
                    &usable_constraint_refs,
                    order_target,
                    0,
                    schema,
                    EqualityPrefixScope::AnyEquality,
                )
                .consumed
                    == order_target.columns.len();

                let satisfies_order = all_same_direction || all_opposite_direction;
                if satisfies_order {
                    // Bonus = estimated sort cost saved. Sorting is O(n log n).
                    let n = *base_row_count;
                    let sort_cost_saved = Cost(n * (n.max(1.0).log2()) * params.sort_cpu_per_row);
                    (
                        if all_same_direction {
                            IterationDirection::Forwards
                        } else {
                            IterationDirection::Backwards
                        },
                        true,
                        sort_cost_saved,
                    )
                } else {
                    (IterationDirection::Forwards, false, Cost(0.0))
                }
            } else {
                (IterationDirection::Forwards, false, Cost(0.0))
            };

        let analyze_ctx = AnalyzeCtx {
            rhs_table,
            index: candidate.index.as_ref(),
            stats: analyze_stats,
        };
        // For partial indexes, the index physically contains only the rows whose
        // values pass the index's WHERE clause. Discount the row count estimate
        // accordingly so the cost model recognizes the partial index as cheaper
        // than a full table scan.
        let candidate_base_row_count = match candidate
            .index
            .as_ref()
            .and_then(|idx| idx.where_clause.as_ref())
        {
            Some(where_expr) => {
                let selectivity = super::constraints::estimate_partial_index_where_selectivity(
                    where_expr.as_ref(),
                    rhs_table,
                    schema,
                    available_indexes,
                    params,
                )
                .clamp(1e-6, 1.0);
                RowCountEstimate::AnalyzeStats((*base_row_count * selectivity).max(1.0))
            }
            None => base_row_count,
        };
        let cost = estimate_cost_for_scan_or_seek(
            Some(index_info),
            &rhs_constraints.constraints,
            &usable_constraint_refs,
            input_cardinality,
            candidate_base_row_count,
            is_index_ordered,
            params,
            Some(&analyze_ctx),
        );

        // Residual filter output adjustment (mirrors SQLite's whereLoopOutputAdjust).
        //
        // When two indexes have the same seek cost, the one whose seek
        // prerequisites already cover more residual WHERE constraints will
        // produce fewer output rows (because those residual filters can be
        // accounted for). This breaks ties correctly: a join-driven seek
        // like fromId=e1.toId (prereqs={e1}) can claim credit for the
        // constant residual label='requires', but a constant seek like
        // label='requires' (prereqs={}) cannot claim credit for the
        // join-dependent residual fromId=e1.toId.
        let loop_prereq_mask = {
            let mut mask = TableMask::default();
            for ucref in usable_constraint_refs.iter() {
                for idx in [
                    ucref.eq.as_ref().map(|e| e.constraint_pos),
                    ucref.lower_bound,
                    ucref.upper_bound,
                ]
                .into_iter()
                .flatten()
                {
                    let c = &rhs_constraints.constraints[idx];
                    mask = mask.iter().chain(c.lhs_mask.iter()).try_collect()?;
                }
            }
            mask
        };
        // Tables whose constraints this loop can account for: the loop's own
        // prerequisite tables plus the current table itself.
        let allowed_mask: TableMask = loop_prereq_mask
            .iter()
            .chain(rhs_table_mask.iter())
            .try_collect()?;

        // Collect which constraint positions are consumed by the index seek.
        let consumed: SmallVec<[usize; 8]> = usable_constraint_refs
            .iter()
            .flat_map(|ucref| {
                [
                    ucref.eq.as_ref().map(|e| e.constraint_pos),
                    ucref.lower_bound,
                    ucref.upper_bound,
                ]
                .into_iter()
                .flatten()
            })
            .collect();

        // Multiply selectivities of residual constraints whose prerequisites
        // are within the allowed mask (i.e. already satisfied by this loop).
        let residual_selectivity: f64 = rhs_constraints
            .constraints
            .iter()
            .enumerate()
            .filter(|(i, c)| {
                !consumed.contains(i)
                    && c.usable
                    && allowed_mask.contains_all_set_bits_of(&c.lhs_mask)
                    && matches!(
                        c.operator,
                        ConstraintOperator::AstNativeOperator(ast::Operator::Equals)
                            | ConstraintOperator::AstNativeOperator(ast::Operator::Greater)
                            | ConstraintOperator::AstNativeOperator(ast::Operator::GreaterEquals)
                            | ConstraintOperator::AstNativeOperator(ast::Operator::Less)
                            | ConstraintOperator::AstNativeOperator(ast::Operator::LessEquals)
                    )
            })
            .map(|(_, c)| c.selectivity)
            .product();

        // Adjusted output: lower means the loop delivers fewer rows downstream.
        let adjusted_output = residual_selectivity;

        // Only apply the order bonus when this candidate satisfies order but
        // the current best does not. When both satisfy order, switching saves
        // no additional sort cost.
        let effective_bonus = if is_index_ordered && !best_is_ordered {
            order_satisfiability_bonus
        } else {
            Cost(0.0)
        };
        let adjusted_best = best_cost + effective_bonus;
        let costs_equal = (cost.0 - adjusted_best.0).abs() < 1e-9;
        if cost < adjusted_best || (costs_equal && adjusted_output < best_adjusted_output - 1e-12) {
            best_cost = cost;
            best_adjusted_output = adjusted_output;
            best_is_ordered = is_index_ordered;
            best_choice = ChosenBtreeCandidate {
                iter_dir,
                index: candidate.index.clone(),
                constraint_refs: usable_constraint_refs,
                base_row_count: candidate_base_row_count,
                cost,
            };
        }
    }

    Ok(Some(best_choice))
}

fn consumed_where_terms_from_constraint_refs(
    constraints: &[Constraint],
    constraint_refs: &[RangeConstraintRef],
) -> Result<BitSet<usize>> {
    let mut consumed = BitSet::default();
    for cref in constraint_refs {
        for constraint_idx in [
            cref.eq.as_ref().map(|eq| eq.constraint_pos),
            cref.lower_bound,
            cref.upper_bound,
        ]
        .into_iter()
        .flatten()
        {
            let where_term_idx = constraints[constraint_idx].where_clause_pos.0;
            consumed.set(where_term_idx)?;
        }
    }
    Ok(consumed)
}

fn consume_partial_index_predicate_terms(
    consumed: &mut BitSet<usize>,
    index: &Index,
    rhs_table: &JoinedTable,
    where_clause: &[WhereTerm],
) -> Result<()> {
    let predicate_terms = partial_index_predicate_terms(index, rhs_table, where_clause)
        .expect("selected partial index predicate must be implied by query");
    for term_idx in predicate_terms {
        consumed.set(term_idx)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
/// Evaluate whether an `IN (...)` predicate should replace the ordinary btree
/// access path with repeated equality seeks.
///
/// This is intentionally separate from `choose_best_btree_candidate()`: the
/// generic btree chooser reasons about a single continuous scan/seek over one
/// candidate, while `InSeek` emits a two-level loop that materializes the RHS
/// into an ephemeral cursor and performs one equality seek per RHS value.
/// Because of that execution shape, only rowid or the first column of an index
/// can drive `InSeek`, and the comparison collation must match the chosen
/// index's first-key collation.
pub(super) fn choose_best_in_seek_candidate(
    rhs_table: &JoinedTable,
    rhs_constraints: &TableConstraints,
    lhs_mask: &TableMask,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
    best_cost: Cost,
    read_mode: BranchReadMode,
) -> Result<Option<ChosenInSeekCandidate>> {
    let Table::BTree(btree) = &rhs_table.table else {
        return Err(LimboError::InternalError(
            "consider_in_seek_access_method called on non-BTree table".into(),
        ));
    };

    let base = *base_row_count;
    let tree_depth = estimate_btree_depth(base, params.rows_per_table_page);
    let mut best_in_seek = None;
    let mut best_in_seek_cost = best_cost;

    for candidate in rhs_constraints.candidates.iter() {
        let first_col_pos = candidate
            .index
            .as_ref()
            .and_then(|idx| idx.columns.first().map(|c| c.pos_in_table));

        let rowid_only = matches!(read_mode, BranchReadMode::RowIdOnly);
        let index_info = match candidate.index.as_ref() {
            Some(index) => IndexInfo {
                unique: index.unique,
                covering: rowid_only || rhs_table.index_is_covering(index),
                column_count: index.columns.len(),
                rows_per_leaf_page: rows_per_leaf_page_for_index(
                    index.columns.len(),
                    rhs_table,
                    params.rows_per_table_page,
                ),
            },
            None => IndexInfo {
                unique: true,
                covering: false,
                column_count: 1,
                rows_per_leaf_page: params.rows_per_table_page,
            },
        };

        for constraint in &rhs_constraints.constraints {
            let ConstraintOperator::In {
                not,
                estimated_values,
            } = constraint.operator
            else {
                continue;
            };
            if not || !lhs_mask.contains_all_set_bits_of(&constraint.lhs_mask) {
                continue;
            }

            let matches = if candidate.index.is_none() {
                constraint.is_rowid
            } else {
                !constraint.is_rowid
                    && constraint.table_col_pos.is_some()
                    && constraint.table_col_pos == first_col_pos
            };
            if !matches {
                continue;
            }

            // `open_loop` copies the chosen index collation onto the ephemeral
            // IN cursor. Reject mismatches here so a BINARY `IN` comparison
            // cannot silently become `NOCASE`/`RTRIM` just because the index is.
            if let (Some(index), Some(col_pos)) = (&candidate.index, constraint.table_col_pos) {
                let constrained_column = &rhs_table.table.columns()[col_pos];
                let table_collation = constrained_column.collation();
                let index_collation = index.columns[0].collation.unwrap_or_default();
                if table_collation != index_collation {
                    continue;
                }
                let idx_aff = constrained_column.affinity_with_strict(rhs_table.table.is_strict());
                if !constraint.satisfies_index_affinity(idx_aff) {
                    continue;
                }
            }

            let rows_per_seek = if (index_info.unique && index_info.column_count == 1)
                || candidate.index.is_none()
            {
                1.0
            } else {
                (base * params.sel_eq_indexed).sqrt().max(1.0)
            };
            let in_cost = estimate_index_cost(
                base,
                tree_depth,
                index_info,
                estimated_values * input_cardinality,
                rows_per_seek,
                params,
            );
            if in_cost >= best_in_seek_cost {
                continue;
            }

            let lhs_affinity = if let Some(col_pos) = constraint.table_col_pos {
                btree
                    .columns()
                    .get(col_pos)
                    .map(|col| col.affinity())
                    .unwrap_or(Affinity::Blob)
            } else {
                Affinity::Integer
            };
            let affinity = comparison_affinity(lhs_affinity, Affinity::None, None, None);
            best_in_seek_cost = in_cost;
            best_in_seek = Some(ChosenInSeekCandidate {
                index: candidate.index.clone(),
                affinity,
                constraint_idx: constraint.where_clause_pos.0,
                cost: in_cost,
                estimated_rows_per_outer_row: (constraint.selectivity * base).max(1.0),
            });
        }
    }

    Ok(best_in_seek)
}

fn consider_in_seek_access_method(
    rhs_table: &JoinedTable,
    rhs_constraints: &TableConstraints,
    lhs_mask: &TableMask,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
    best_cost: Cost,
) -> Result<Option<AccessMethod>> {
    choose_best_in_seek_candidate(
        rhs_table,
        rhs_constraints,
        lhs_mask,
        input_cardinality,
        base_row_count,
        params,
        best_cost,
        BranchReadMode::FullRow,
    )?
    .map(|chosen| -> Result<AccessMethod> {
        Ok(AccessMethod {
            cost: chosen.cost,
            estimated_rows_per_outer_row: chosen.estimated_rows_per_outer_row,
            consumed_where_terms: iter::once(chosen.constraint_idx).try_collect()?,
            params: AccessMethodParams::InSeek {
                index: chosen.index,
                affinity: chosen.affinity,
                where_term_idx: chosen.constraint_idx,
            },
        })
    })
    .transpose()
}

/// Add the cost of ready `WHERE` conditions.
fn cost_with_where_work(
    method: &AccessMethod,
    ready_where: &[(usize, usize)],
    input_cardinality: f64,
    params: &CostModelParams,
) -> Cost {
    let used_steps: usize = ready_where
        .iter()
        .filter(|(term_idx, _)| method.consumed_where_terms.get(*term_idx))
        .map(|(_, step_count)| step_count)
        .sum();
    let remaining_steps: usize = ready_where
        .iter()
        .filter(|(term_idx, _)| !method.consumed_where_terms.get(*term_idx))
        .map(|(_, step_count)| step_count)
        .sum();
    let output_rows = input_cardinality * method.estimated_rows_per_outer_row;
    let used_rows = input_cardinality.max(output_rows);
    let work = used_rows * used_steps as f64 + output_rows * remaining_steps as f64;
    method.cost + Cost(work * params.cpu_cost_per_where_step)
}

pub(super) fn add_where_cost(
    method: &mut AccessMethod,
    ready_where: &[(usize, usize)],
    input_cardinality: f64,
    params: &CostModelParams,
) {
    method.cost = cost_with_where_work(method, ready_where, input_cardinality, params);
}

fn replace_if_cheaper(
    best_method: &mut AccessMethod,
    best_cost: &mut Cost,
    method: AccessMethod,
    ready_where: &[(usize, usize)],
    input_cardinality: f64,
    params: &CostModelParams,
) {
    let cost = cost_with_where_work(&method, ready_where, input_cardinality, params);
    if cost < *best_cost {
        *best_method = method;
        *best_cost = cost;
    }
}

/// Return the best [AccessMethod] for a given join order.
#[allow(clippy::too_many_arguments)]
pub fn find_best_access_method_for_join_order(
    rhs_table: &JoinedTable,
    rhs_constraints: &TableConstraints,
    lhs_mask: &TableMask,
    join_order: &[JoinOrderMember],
    planning_context: JoinPlanningContext<'_>,
    where_clause: &[WhereTerm],
    ready_where: &[(usize, usize)],
    available_indexes: &AvailableIndexes,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    analyze_stats: &AnalyzeStats,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
) -> Result<Option<AccessMethod>> {
    match &rhs_table.table {
        Table::BTree(_) => find_best_access_method_for_btree(
            rhs_table,
            rhs_constraints,
            lhs_mask,
            join_order,
            planning_context.maybe_order_target,
            where_clause,
            ready_where,
            available_indexes,
            table_references,
            subqueries,
            schema,
            analyze_stats,
            input_cardinality,
            base_row_count,
            params,
        ),
        Table::Virtual(vtab) => find_best_access_method_for_vtab(
            vtab,
            &rhs_constraints.constraints,
            join_order,
            input_cardinality,
            base_row_count,
            params,
        ),
        Table::FromClauseSubquery(subquery) => find_best_access_method_for_subquery(
            rhs_table,
            subquery,
            rhs_constraints,
            join_order,
            planning_context,
            ready_where,
            schema,
            input_cardinality,
            base_row_count,
            params,
        ),
        Table::RecursiveCteInput(_) => Ok(Some(AccessMethod {
            cost: estimate_cost_for_scan_or_seek(
                None,
                &[],
                &[],
                input_cardinality,
                RowCountEstimate::HardcodedFallback(1.0),
                false,
                params,
                None,
            ),
            estimated_rows_per_outer_row: 1.0,
            consumed_where_terms: Default::default(),
            params: AccessMethodParams::RecursiveCteInput,
        })),
    }
}

#[allow(clippy::too_many_arguments)]
fn find_best_access_method_for_btree(
    rhs_table: &JoinedTable,
    rhs_constraints: &TableConstraints,
    lhs_mask: &TableMask,
    join_order: &[JoinOrderMember],
    maybe_order_target: Option<&OrderTarget>,
    where_clause: &[WhereTerm],
    ready_where: &[(usize, usize)],
    available_indexes: &AvailableIndexes,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    analyze_stats: &AnalyzeStats,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
) -> Result<Option<AccessMethod>> {
    let rhs_table_idx = join_order.last().unwrap().original_idx;
    let best = choose_best_btree_candidate(
        rhs_table,
        rhs_constraints,
        lhs_mask,
        rhs_table_idx,
        maybe_order_target,
        schema,
        available_indexes,
        analyze_stats,
        input_cardinality,
        base_row_count,
        params,
    )?
    .expect("btree candidate selection must always consider the rowid candidate");

    let access_base_row_count = best.base_row_count;
    let estimated_rows_per_outer_row = if best.constraint_refs.is_empty() {
        *access_base_row_count
    } else {
        let index_info = match best.index.as_ref() {
            Some(index) => IndexInfo {
                unique: index.unique,
                covering: rhs_table.index_is_covering(index),
                column_count: index.columns.len(),
                rows_per_leaf_page: rows_per_leaf_page_for_index(
                    index.columns.len(),
                    rhs_table,
                    params.rows_per_table_page,
                ),
            },
            None => IndexInfo {
                unique: true,
                covering: true,
                column_count: 1,
                rows_per_leaf_page: params.rows_per_table_page,
            },
        };
        let analyze_ctx = AnalyzeCtx {
            rhs_table,
            index: best.index.as_ref(),
            stats: analyze_stats,
        };
        estimate_rows_per_seek(
            index_info,
            &rhs_constraints.constraints,
            &best.constraint_refs,
            access_base_row_count,
            Some(&analyze_ctx),
        )
    };
    let mut consumed_where_terms = consumed_where_terms_from_constraint_refs(
        &rhs_constraints.constraints,
        &best.constraint_refs,
    )?;
    if let Some(index) = partial_index(best.index.as_ref()) {
        consume_partial_index_predicate_terms(
            &mut consumed_where_terms,
            index,
            rhs_table,
            where_clause,
        )?;
    }
    let mut best_access_method = AccessMethod {
        cost: best.cost,
        estimated_rows_per_outer_row,
        consumed_where_terms,
        params: AccessMethodParams::BTreeTable {
            iter_dir: best.iter_dir,
            index: best.index,
            build_index: false,
            constraint_refs: best.constraint_refs.into_vec(),
        },
    };
    let mut best_cost_with_filters =
        cost_with_where_work(&best_access_method, ready_where, input_cardinality, params);

    let is_full_outer = rhs_table
        .join_info
        .as_ref()
        .is_some_and(|join_info| join_info.is_full_outer());
    let uses_full_table_scan = matches!(
        &best_access_method.params,
        AccessMethodParams::BTreeTable {
            index: None,
            build_index: false,
            constraint_refs,
            ..
        } if constraint_refs.is_empty()
    );
    if rhs_table.indexed.is_none() && uses_full_table_scan && !lhs_mask.is_empty() && !is_full_outer
    {
        let constraint_refs = usable_constraints_for_lhs_mask(
            &rhs_constraints.constraints,
            &rhs_constraints.temporary_index_terms,
            lhs_mask,
            rhs_table_idx,
        );
        if !constraint_refs.is_empty() {
            let column_count = rhs_table
                .columns()
                .iter()
                .enumerate()
                .filter(|(column_idx, _)| rhs_table.column_is_used(*column_idx))
                .count();
            let index_info = IndexInfo {
                unique: false,
                column_count,
                covering: true,
                rows_per_leaf_page: rows_per_leaf_page_for_index(
                    column_count,
                    rhs_table,
                    params.rows_per_table_page,
                ),
            };
            let rows_per_seek = estimate_rows_per_seek(
                index_info,
                &rhs_constraints.constraints,
                &constraint_refs,
                base_row_count,
                None,
            );
            let scan_cost = estimate_cost_for_scan_or_seek(
                None,
                &[],
                &[],
                1.0,
                base_row_count,
                false,
                params,
                None,
            );
            let build_cost = estimate_ephemeral_index_build_cost(*base_row_count, params);
            let seek_cost = Cost(
                input_cardinality * params.cpu_cost_per_seek
                    + input_cardinality * rows_per_seek * params.cpu_cost_per_row,
            );
            let cost = scan_cost + build_cost + seek_cost;
            let temporary_index = AccessMethod {
                cost,
                estimated_rows_per_outer_row: rows_per_seek,
                consumed_where_terms: consumed_where_terms_from_constraint_refs(
                    &rhs_constraints.constraints,
                    &constraint_refs,
                )?,
                params: AccessMethodParams::BTreeTable {
                    iter_dir: best.iter_dir,
                    index: None,
                    build_index: true,
                    constraint_refs: Vec::new(),
                },
            };
            replace_if_cheaper(
                &mut best_access_method,
                &mut best_cost_with_filters,
                temporary_index,
                ready_where,
                input_cardinality,
                params,
            );
        }
    }

    // Skip alternative access methods (in-seek, multi-index) when INDEXED BY or NOT INDEXED
    // is specified — the user explicitly requested a specific index or no index.
    if rhs_table.indexed.is_none() && rhs_table.btree().is_some_and(|b| b.has_rowid) {
        if let Some(in_seek_method) = consider_in_seek_access_method(
            rhs_table,
            rhs_constraints,
            lhs_mask,
            input_cardinality,
            base_row_count,
            params,
            best_cost_with_filters,
        )? {
            let mut in_seek_method = in_seek_method;
            if let AccessMethodParams::InSeek { index, .. } = &in_seek_method.params {
                if let Some(index) = partial_index(index.as_ref()) {
                    consume_partial_index_predicate_terms(
                        &mut in_seek_method.consumed_where_terms,
                        index,
                        rhs_table,
                        where_clause,
                    )?;
                }
            }
            replace_if_cheaper(
                &mut best_access_method,
                &mut best_cost_with_filters,
                in_seek_method,
                ready_where,
                input_cardinality,
                params,
            );
        }

        if let Some(multi_idx_method) = consider_multi_index_union(
            rhs_table,
            where_clause,
            available_indexes,
            table_references,
            subqueries,
            schema,
            input_cardinality,
            base_row_count,
            params,
            best_cost_with_filters,
            lhs_mask,
            analyze_stats,
        )? {
            replace_if_cheaper(
                &mut best_access_method,
                &mut best_cost_with_filters,
                multi_idx_method,
                ready_where,
                input_cardinality,
                params,
            );
        }

        if let Some(multi_idx_and_method) = consider_multi_index_intersection(
            rhs_table,
            where_clause,
            available_indexes,
            table_references,
            subqueries,
            schema,
            input_cardinality,
            base_row_count,
            params,
            best_cost_with_filters,
            lhs_mask,
            analyze_stats,
        )? {
            replace_if_cheaper(
                &mut best_access_method,
                &mut best_cost_with_filters,
                multi_idx_and_method,
                ready_where,
                input_cardinality,
                params,
            );
        }
    }

    Ok(Some(best_access_method))
}

fn find_best_access_method_for_vtab(
    vtab: &VirtualTable,
    constraints: &[Constraint],
    join_order: &[JoinOrderMember],
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
) -> Result<Option<AccessMethod>> {
    let vtab_constraints = convert_to_vtab_constraint(constraints, join_order)?;

    // TODO: get proper order_by information to pass to the vtab.
    // maybe encode more info on t_ctx? we need: [col_idx , is_descending]
    let best_index_result = vtab.best_index(&vtab_constraints, &[]);

    match best_index_result {
        Ok(index_info) => {
            if index_info.constraint_usages.len() != vtab_constraints.len() {
                return Err(LimboError::ExtensionError(format!(
                    "Constraint usage count mismatch (expected {}, got {})",
                    vtab_constraints.len(),
                    index_info.constraint_usages.len()
                )));
            }
            let has_row_estimate = index_info.estimated_rows != u32::MAX;
            let estimated_rows_per_outer_row = if has_row_estimate {
                f64::from(index_info.estimated_rows)
            } else {
                *base_row_count
            };
            // A row estimate includes each condition passed to the virtual table.
            // Do not apply the same row cut again in the join planner.
            let consumed_where_terms = if has_row_estimate {
                vtab_constraints
                    .iter()
                    .zip(&index_info.constraint_usages)
                    .filter(|(_, usage)| usage.argv_index.is_some())
                    .map(|(vtab_constraint, _)| {
                        constraints[vtab_constraint.index].where_clause_pos.0
                    })
                    .try_collect()?
            } else {
                BitSet::default()
            };
            Ok(Some(AccessMethod {
                // TODO: Base cost on `IndexInfo::estimated_cost`.
                cost: estimate_cost_for_scan_or_seek(
                    None,
                    &[],
                    &[],
                    input_cardinality,
                    base_row_count,
                    false,
                    params,
                    None,
                ),
                estimated_rows_per_outer_row,
                consumed_where_terms,
                params: AccessMethodParams::VirtualTable {
                    idx_num: index_info.idx_num,
                    idx_str: index_info.idx_str,
                    constraints: vtab_constraints,
                    constraint_usages: index_info.constraint_usages,
                },
            }))
        }
        Err(ResultCode::ConstraintViolation) => Ok(None),
        Err(e) => Err(LimboError::from(e)),
    }
}

/// Return the one table read by an expression.
fn one_table_in_expr(expr: &ast::Expr) -> Option<TableInternalId> {
    let mut table = None;
    let mut has_more_than_one = false;
    let result = walk_expr(expr, &mut |e| {
        match e {
            ast::Expr::Column {
                table: found_table, ..
            }
            | ast::Expr::RowId {
                table: found_table, ..
            } => {
                if table.is_some_and(|table| table != *found_table) {
                    has_more_than_one = true;
                } else {
                    table = Some(*found_table);
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    });
    result.ok()?;
    (!has_more_than_one).then_some(table).flatten()
}

/// Return the two tables used by one equal test.
pub(super) fn tables_in_equal_test(expr: &ast::Expr) -> Option<(TableInternalId, TableInternalId)> {
    let Ok(Some((left, operator, right))) = as_binary_components(expr) else {
        return None;
    };
    if !matches!(operator.as_ast_operator(), Some(ast::Operator::Equals)) {
        return None;
    }
    Some((one_table_in_expr(left)?, one_table_in_expr(right)?))
}

/// Find equal tests that a hash join can use.
///
/// Each side must read one table. This function does not mark a test as used.
fn find_hash_join_keys(
    build_table_id: TableInternalId,
    probe_table_id: TableInternalId,
    terms: impl Iterator<Item = (usize, TableInternalId, TableInternalId)>,
) -> SmallVec<[HashJoinKey; 2]> {
    let mut join_keys = SmallVec::new();

    for (where_idx, lhs_table, rhs_table) in terms {
        // Accept either orientation: build=probe or probe=build.
        let build_side = if lhs_table == build_table_id && rhs_table == probe_table_id {
            Some(BinaryExprSide::Lhs)
        } else if rhs_table == build_table_id && lhs_table == probe_table_id {
            Some(BinaryExprSide::Rhs)
        } else {
            None
        };

        if let Some(build_side) = build_side {
            join_keys.push(HashJoinKey {
                where_clause_idx: where_idx,
                build_side,
            });
        }
    }

    join_keys
}

/// Estimate the cost of a hash join between two tables.
///
/// The cost model accounts for:
/// - Build phase: Creating the hash table from the build side (one-time cost)
/// - Probe phase: Looking up each probe row in the hash table (one scan of probe table)
/// - Memory pressure: Additional IO cost if the hash table spills to disk
pub fn estimate_hash_join_cost(
    build_cardinality: f64,
    probe_cardinality: f64,
    mem_budget: usize,
    probe_multiplier: f64,
    params: &CostModelParams,
) -> Cost {
    // Estimate if the hash table will fit in memory based on actual row counts
    let estimated_hash_table_size =
        (build_cardinality as usize).saturating_mul(params.hash_bytes_per_row as usize);
    let will_spill = estimated_hash_table_size > mem_budget;

    // Build phase: hash and insert all rows from build table (one-time cost)
    // With real ANALYZE stats, this accurately reflects the actual build table size
    let build_cost = build_cardinality * (params.hash_cpu_cost + params.hash_insert_cost);

    // Probe phase: scan probe table, hash each row and lookup in hash table.
    // If the hash-join probe loop is nested under prior tables, the probe
    // scan repeats per outer row, so scale by probe_multiplier.
    let probe_cost =
        probe_cardinality * (params.hash_cpu_cost + params.hash_lookup_cost) * probe_multiplier;

    // Spill cost: if hash table exceeds memory budget, we need to write/read partitions to disk.
    // Grace hash join writes partitions and reads them back, so it's 2x the page IO.
    // Use page-based IO cost (rows / rows_per_page) rather than per-row IO.
    let spill_cost = if will_spill {
        let build_pages = (build_cardinality / params.rows_per_table_page).ceil();
        let probe_pages = (probe_cardinality / params.rows_per_table_page).ceil();
        // Write both sides to partitions, then read back: 2 * (build_pages + probe_pages)
        (build_pages + probe_pages) * 2.0 * probe_multiplier
    } else {
        0.0
    };

    Cost(build_cost + probe_cost + spill_cost)
}

/// Try to create a hash join access method for joining two tables.
#[allow(clippy::too_many_arguments)]
pub fn try_hash_join_access_method(
    build_table: &JoinedTable,
    probe_table: &JoinedTable,
    build_table_idx: usize,
    probe_table_idx: usize,
    build_constraints: &TableConstraints,
    probe_constraints: &TableConstraints,
    where_clause: &mut [WhereTerm],
    equal_terms: impl Iterator<Item = (usize, TableInternalId, TableInternalId)>,
    build_cardinality: f64,
    probe_cardinality: f64,
    probe_multiplier: f64,
    subqueries: &[NonFromClauseSubquery],
    params: &CostModelParams,
) -> Result<Option<AccessMethod>> {
    // Only works for B-tree tables
    if !matches!(build_table.table, Table::BTree(_))
        || !matches!(probe_table.table, Table::BTree(_))
    {
        return Ok(None);
    }
    // Avoid hash join on self-joins over the same underlying table for INNER /
    // LEFT joins: a nested-loop with index seek is usually preferred and avoids
    // double-buffering the table in the hash table. FULL OUTER has no
    // nested-loop form yet, so it must use hash join even for self-joins.
    let probe_root_page = probe_table.table.btree().expect("table is BTree").root_page;
    let build_root_page = build_table.table.btree().expect("table is BTree").root_page;
    let is_full_outer = probe_table
        .join_info
        .as_ref()
        .is_some_and(|ji| ji.is_full_outer());
    if build_root_page == probe_root_page && !is_full_outer {
        return Ok(None);
    }
    // Explicit INDEXED BY / NOT INDEXED directives must be honored. A hash join
    // bypasses the normal access-path selection for the build/probe pair, so it
    // would ignore the user's requested scan shape.
    if build_table.indexed.is_some() || probe_table.indexed.is_some() {
        return Ok(None);
    }
    // No hash join for semi/anti-joins (nested loop with index seek is preferred).
    if probe_table
        .join_info
        .as_ref()
        .is_some_and(|ji| ji.is_semi_or_anti())
        || build_table
            .join_info
            .as_ref()
            .is_some_and(|ji| ji.is_semi_or_anti())
    {
        return Ok(None);
    }
    // Determine join type from the probe table's join_info.
    let hash_join_type = if probe_table
        .join_info
        .as_ref()
        .is_some_and(|ji| ji.is_full_outer())
    {
        HashJoinType::FullOuter
    } else if probe_table
        .join_info
        .as_ref()
        .is_some_and(|ji| ji.is_outer())
    {
        HashJoinType::LeftOuter
    } else {
        HashJoinType::Inner
    };

    // Can't build from a NullRow'd table — the hash table would hold real data
    // even when the cursor is in NullRow mode.
    if build_table
        .join_info
        .as_ref()
        .is_some_and(|ji| ji.is_outer())
    {
        return Ok(None);
    }

    // Skip hash join on USING/NATURAL joins.
    if build_table
        .join_info
        .as_ref()
        .is_some_and(|ji| !ji.using.is_empty())
        || probe_table
            .join_info
            .as_ref()
            .is_some_and(|ji| !ji.using.is_empty())
    {
        return Ok(None);
    }

    // Avoid hash joins when there are correlated subqueries that reference the joined tables.
    for subquery in subqueries {
        if !subquery.correlated {
            continue;
        }
        // Check if the subquery references the build or probe table
        if let SubqueryState::Unevaluated { plan } = &subquery.state {
            if let Some(plan) = plan.as_ref() {
                let outer_ref_ids = plan.used_outer_query_ref_ids();
                for outer_ref_id in &outer_ref_ids {
                    if *outer_ref_id == build_table.internal_id
                        || *outer_ref_id == probe_table.internal_id
                    {
                        return Ok(None);
                    }
                }
            }
        }
    }

    let join_keys = find_hash_join_keys(
        build_table.internal_id,
        probe_table.internal_id,
        equal_terms,
    );
    tracing::debug!(
        build_table = build_table.table.get_name(),
        probe_table = probe_table.table.get_name(),
        join_key_count = join_keys.len(),
        "hash-join equi-join keys"
    );

    // A hash join normally needs at least one equi-join condition. A FULL OUTER
    // JOIN is the exception: it has no nested-loop form, so when the ON clause has
    // no equality (e.g. `a.x < b.x`) we still build a single-bucket hash join and
    // let the predicate apply as a residual, rather than rejecting the query.
    if join_keys.is_empty() && hash_join_type != HashJoinType::FullOuter {
        return Ok(None);
    }
    // Custom-collated equality depends on a connection-owned callback, so the
    // hash join planner cannot derive a stable hash/equality pair here.
    if join_keys.iter().any(|join_key| {
        expr_uses_custom_collation(join_key.get_build_expr(where_clause))
            || expr_uses_custom_collation(join_key.get_probe_expr(where_clause))
    }) {
        return Ok(None);
    }

    // Prefer nested-loop with index lookup when an index exists on join columns.
    // FULL OUTER must use hash join (needed for the unmatched-build scan).
    // Check both tables because we could potentially use a different
    // join order where the indexed table becomes the probe/inner table.
    if hash_join_type != HashJoinType::FullOuter {
        for join_key in &join_keys {
            let probe_expr = join_key.get_probe_expr(where_clause);
            let probe_is_simple_column =
                expr_is_simple_column_from_table(probe_expr, probe_table.internal_id);
            let build_expr = join_key.get_build_expr(where_clause);
            let build_is_simple_column =
                expr_is_simple_column_from_table(build_expr, build_table.internal_id);
            // Check probe table constraints for index on join column, only when the probe side
            // references the probe table alone and is a simple column/rowid reference.
            if probe_is_simple_column {
                if let Some(constraint) = probe_constraints
                    .constraints
                    .iter()
                    .find(|c| c.where_clause_pos.0 == join_key.where_clause_idx)
                {
                    if let Some(col_pos) = constraint.table_col_pos {
                        // Check if the join column is a rowid alias directly from the table schema
                        if let Some(column) = probe_table.columns().get(col_pos) {
                            if column.is_rowid_alias() {
                                return Ok(None);
                            }
                        }
                        // Also check regular indexes
                        for candidate in &probe_constraints.candidates {
                            if let Some(index) = &candidate.index {
                                if index.column_table_pos_to_index_pos(col_pos).is_some() {
                                    return Ok(None);
                                }
                            }
                        }
                    }
                }
            }

            // Check build table constraints for index on join column, only when the build side
            // is a simple column/rowid reference.
            if build_is_simple_column {
                if let Some(constraint) = build_constraints
                    .constraints
                    .iter()
                    .find(|c| c.where_clause_pos.0 == join_key.where_clause_idx)
                {
                    if let Some(col_pos) = constraint.table_col_pos {
                        // Check if the join column is a rowid alias directly from the table schema
                        if let Some(column) = build_table.columns().get(col_pos) {
                            if column.is_rowid_alias() {
                                return Ok(None);
                            }
                        }
                        // Also check regular indexes
                        for candidate in &build_constraints.candidates {
                            if let Some(index) = &candidate.index {
                                if index.column_table_pos_to_index_pos(col_pos).is_some() {
                                    return Ok(None);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let join_selectivity = join_keys
        .iter()
        .map(|key| {
            probe_constraints
                .constraints
                .iter()
                .find(|constraint| constraint.where_clause_pos.0 == key.where_clause_idx)
                .map_or(params.sel_eq_unindexed, |constraint| constraint.selectivity)
        })
        .product::<f64>();
    let rows_per_build_row = probe_cardinality * join_selectivity;
    let estimated_rows_per_outer_row = match hash_join_type {
        HashJoinType::Inner => rows_per_build_row,
        HashJoinType::LeftOuter => rows_per_build_row.max(1.0),
        HashJoinType::FullOuter => rows_per_build_row
            .max(1.0)
            .max(probe_cardinality / build_cardinality.max(1.0)),
    };

    let cost = estimate_hash_join_cost(
        build_cardinality,
        probe_cardinality,
        DEFAULT_MEM_BUDGET,
        probe_multiplier,
        params,
    );
    Ok(Some(AccessMethod {
        cost,
        estimated_rows_per_outer_row,
        consumed_where_terms: join_keys
            .iter()
            .map(|key| key.where_clause_idx)
            .try_collect()?,
        params: AccessMethodParams::HashJoin {
            build_table_idx,
            probe_table_idx,
            join_keys: join_keys.into_vec(),
            mem_budget: DEFAULT_MEM_BUDGET,
            materialize_build_input: false,
            use_bloom_filter: false,
            join_type: hash_join_type,
        },
    }))
}

/// Returns true when the expression is a simple column/rowid reference to the table.
/// Used to decide if an index seek could replace a hash join.
fn expr_is_simple_column_from_table(expr: &ast::Expr, table_id: TableInternalId) -> bool {
    matches!(
        expr,
        ast::Expr::Column { table, .. } | ast::Expr::RowId { table, .. } if *table == table_id
    )
}

/// Check whether a subquery's intrinsic row order (from its ORDER BY or
/// finalized inner scan) already satisfies the outer order target, and if so
/// in which direction.
///
/// Backwards iteration (`Last`/`Prev`) is only possible when
/// `table_materialization_required` is true — coroutine scans cannot be
/// reversed at runtime.
fn intrinsic_subquery_scan_direction(
    rhs_table: &JoinedTable,
    subquery: &FromClauseSubquery,
    maybe_order_target: Option<&OrderTarget>,
    table_materialization_required: bool,
    schema: &Schema,
) -> Option<IterationDirection> {
    let order_target = maybe_order_target?;
    let cols = &order_target.columns;

    let matches_forwards = subquery_intrinsic_order_consumed(
        rhs_table.internal_id,
        subquery,
        IterationDirection::Forwards,
        cols,
        schema,
    ) == cols.len();
    if matches_forwards {
        return Some(IterationDirection::Forwards);
    }

    let matches_backwards = table_materialization_required
        && subquery_intrinsic_order_consumed(
            rhs_table.internal_id,
            subquery,
            IterationDirection::Backwards,
            cols,
            schema,
        ) == cols.len();
    matches_backwards.then_some(IterationDirection::Backwards)
}

/// Find the best access method for a FROM clause subquery.
///
/// Uncorrelated FROM-subqueries can either stay as coroutine scans or be treated
/// like a table-backed row source with a synthesized ephemeral probe index. When
/// the latter is worthwhile, we materialize the subquery into an EphemeralTable
/// and later build the probe index lazily in the main-loop open phase.
#[expect(clippy::too_many_arguments)]
fn find_best_access_method_for_subquery(
    rhs_table: &JoinedTable,
    subquery: &FromClauseSubquery,
    rhs_constraints: &TableConstraints,
    join_order: &[JoinOrderMember],
    planning_context: JoinPlanningContext<'_>,
    ready_where: &[(usize, usize)],
    schema: &Schema,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
) -> Result<Option<AccessMethod>> {
    use super::constraints::ConstraintRef;
    let maybe_order_target = planning_context.maybe_order_target;

    let coroutine_scan_cost = estimate_cost_for_scan_or_seek(
        None,
        &[],
        &[],
        input_cardinality,
        base_row_count,
        false,
        params,
        None,
    );
    let subquery_cost = subquery.plan.estimated_cost().unwrap_or(0.0);
    let coroutine_reexecution_overhead =
        Cost((input_cardinality - 1.0).max(0.0) * *base_row_count * params.cpu_cost_per_seek);
    let coroutine_cost = coroutine_scan_cost
        + coroutine_reexecution_overhead
        + Cost(input_cardinality * subquery_cost);
    let table_materialization_required = subquery.requires_table_materialization();
    let can_direct_materialize_index = subquery.supports_direct_index_materialization();
    let scan_cost = if table_materialization_required {
        // Explicit MATERIALIZED hints and shared CTEs already produce a table-backed
        // row source. Scanning them behaves like rescanning cached rows, not rerunning
        // a coroutine body for each outer probe.
        coroutine_scan_cost + Cost(subquery_cost)
    } else {
        // The generic scan model treats repeated probes like cached rescans of a
        // row source. A coroutine-backed subquery is slightly more expensive: each
        // extra outer row reruns the subquery program instead of probing a
        // materialized result. Charge that extra work explicitly here.
        coroutine_cost
    };

    // Plans with outer-scope dependencies cannot be materialized once -
    // they must re-execute for each outer row. Use coroutine for these.
    // This check must come first because correlated CTEs should NOT share materialized data.
    if plan_has_outer_scope_dependency(&subquery.plan) {
        return Ok(Some(AccessMethod {
            // Correlated subqueries always rerun for each outer row, even if the
            // enclosing CTE/subquery might otherwise be shareable.
            cost: coroutine_cost,
            estimated_rows_per_outer_row: *base_row_count,
            consumed_where_terms: Default::default(),
            params: AccessMethodParams::Subquery {
                iter_dir: IterationDirection::Forwards,
            },
        }));
    }

    // Build synthetic index columns from the constraints this materialized
    // subquery could probe. Because the index is ephemeral, we can order its
    // key columns to fit the chosen seek shape: equality columns first, then
    // range-only columns, then the remaining payload columns.
    let usable: Vec<(usize, &Constraint)> = rhs_constraints
        .constraints
        .iter()
        .enumerate()
        .filter(|(_, c)| {
            matches!(
                c.operator.as_ast_operator(),
                Some(
                    ast::Operator::Equals
                        | ast::Operator::Greater
                        | ast::Operator::GreaterEquals
                        | ast::Operator::Less
                        | ast::Operator::LessEquals
                )
            ) && c.can_drive_index_seek(&subquery.columns, false)
        })
        .collect();

    // For extremum (MIN/MAX) targets we can reuse the subquery's intrinsic
    // order as a plain scan when every usable constraint is on the extremum
    // column itself. Once other key columns participate, the direct table
    // scan no longer has a simple "walk from one end" shape.
    let extremum_constraints_compatible = maybe_order_target.is_some_and(|ot| ot.is_extremum())
        && match maybe_order_target
            .and_then(|ot| ot.columns.first())
            .map(|c| &c.target)
        {
            Some(ColumnTarget::Column(pos)) => {
                usable.iter().all(|(_, c)| c.table_col_pos == Some(*pos))
            }
            _ => false,
        };

    // Try to reuse the subquery's intrinsic row order (from its ORDER BY or
    // finalized inner scan) to satisfy the outer order target directly.
    // For non-extremum targets this only applies when there are no seek
    // constraints — with constraints, we fall through to the materialized
    // index path which has its own order-satisfaction logic.
    if extremum_constraints_compatible || usable.is_empty() {
        if let Some(iter_dir) = intrinsic_subquery_scan_direction(
            rhs_table,
            subquery,
            maybe_order_target,
            table_materialization_required,
            schema,
        ) {
            return Ok(Some(AccessMethod {
                cost: scan_cost,
                estimated_rows_per_outer_row: *base_row_count,
                consumed_where_terms: Default::default(),
                params: AccessMethodParams::Subquery { iter_dir },
            }));
        }
    }

    if usable.is_empty() {
        return Ok(Some(AccessMethod {
            cost: scan_cost,
            estimated_rows_per_outer_row: *base_row_count,
            consumed_where_terms: Default::default(),
            params: AccessMethodParams::Subquery {
                iter_dir: IterationDirection::Forwards,
            },
        }));
    }

    let usable_constraints: Vec<&Constraint> = usable.iter().map(|(_, c)| *c).collect();
    let key_col_positions = ordered_ephemeral_key_columns(&usable_constraints);
    if key_col_positions.is_empty() {
        return Ok(Some(AccessMethod {
            cost: scan_cost,
            estimated_rows_per_outer_row: *base_row_count,
            consumed_where_terms: Default::default(),
            params: AccessMethodParams::Subquery {
                iter_dir: IterationDirection::Forwards,
            },
        }));
    }

    let key_col_pos_to_index_pos: HashMap<usize, usize> = key_col_positions
        .iter()
        .enumerate()
        .map(|(index_col_pos, table_col_pos)| (*table_col_pos, index_col_pos))
        .collect();

    // Map each usable constraint to a ConstraintRef
    let mut temp_constraint_refs: Vec<ConstraintRef> = usable
        .iter()
        .map(|(orig_idx, c)| {
            let table_col_pos = c.table_col_pos.expect("table_col_pos was Some above");
            let index_col_pos = *key_col_pos_to_index_pos
                .get(&table_col_pos)
                .expect("table_col_pos must exist in key_col_positions");
            ConstraintRef {
                constraint_vec_pos: *orig_idx,
                index_col_pos,
                sort_order: SortOrder::Asc,
                nulls_order: ast::NullsOrder::First,
            }
        })
        .collect();

    temp_constraint_refs.sort_by_key(|x| x.index_col_pos);

    // Filter to only constraints that can be used given the current join order
    let usable_constraint_refs = usable_constraints_for_join_order(
        &rhs_constraints.constraints,
        &temp_constraint_refs,
        join_order,
    )?;

    let has_search_constraints = !usable_constraint_refs.is_empty();
    if !has_search_constraints {
        tracing::trace!(
            table = rhs_table.table.get_name(),
            cost = ?scan_cost,
            "using coroutine subquery access because no usable seek constraints remain"
        );
        return Ok(Some(AccessMethod {
            cost: scan_cost,
            estimated_rows_per_outer_row: *base_row_count,
            consumed_where_terms: Default::default(),
            params: AccessMethodParams::Subquery {
                iter_dir: IterationDirection::Forwards,
            },
        }));
    }

    let ephemeral_index =
        materialized_subquery_ephemeral_index(rhs_table, subquery, &key_col_positions)?;
    let (iter_dir, _is_index_ordered, order_satisfiability_bonus) =
        materialized_subquery_order_properties(
            rhs_table,
            &ephemeral_index,
            &usable_constraint_refs,
            maybe_order_target,
            schema,
            base_row_count,
            params,
        );

    let estimated_rows_per_outer_row =
        if grouped_subquery_lookup_is_unique(subquery, &usable_constraint_refs) {
            1.0
        } else {
            estimate_rows_per_seek(
                IndexInfo {
                    unique: false,
                    column_count: key_col_positions.len(),
                    covering: true,
                    rows_per_leaf_page: params.rows_per_table_page,
                },
                &rhs_constraints.constraints,
                &usable_constraint_refs,
                base_row_count,
                None,
            )
        };
    let one_pass_scan_cost =
        estimate_cost_for_scan_or_seek(None, &[], &[], 1.0, base_row_count, false, params, None);
    let append_build_cost = estimate_ephemeral_index_build_cost(*base_row_count, params);
    let seek_setup_cost = if table_materialization_required || can_direct_materialize_index {
        // Both table-backed materialization and direct-index materialization avoid
        // the extra "scan table into probe index" pass. They differ in storage,
        // not in setup work.
        one_pass_scan_cost + append_build_cost
    } else {
        // Compound SELECTs and other table-backed materializations need two passes:
        // first produce the ephemeral table, then scan it once to build the probe
        // index used by SEARCH.
        one_pass_scan_cost + one_pass_scan_cost + append_build_cost
    };
    let seek_cost = Cost(
        input_cardinality * params.cpu_cost_per_seek
            + input_cardinality * estimated_rows_per_outer_row * params.cpu_cost_per_row,
    );
    let total_cost = Cost(subquery_cost) + seek_setup_cost + seek_cost;
    let scan_method = AccessMethod {
        cost: scan_cost,
        estimated_rows_per_outer_row: *base_row_count,
        consumed_where_terms: Default::default(),
        params: AccessMethodParams::Subquery {
            iter_dir: IterationDirection::Forwards,
        },
    };
    let index_method = AccessMethod {
        cost: total_cost,
        estimated_rows_per_outer_row,
        consumed_where_terms: consumed_where_terms_from_constraint_refs(
            &rhs_constraints.constraints,
            &usable_constraint_refs,
        )?,
        params: AccessMethodParams::MaterializedSubquery {
            index: ephemeral_index,
            constraint_refs: usable_constraint_refs,
            iter_dir,
        },
    };
    let scan_cost = cost_with_where_work(&scan_method, ready_where, input_cardinality, params);
    let index_cost = cost_with_where_work(&index_method, ready_where, input_cardinality, params);

    if index_cost >= scan_cost + order_satisfiability_bonus {
        Ok(Some(scan_method))
    } else {
        Ok(Some(index_method))
    }
}

/// A grouped query has at most one row for one full group key.
fn grouped_subquery_lookup_is_unique(
    subquery: &FromClauseSubquery,
    constraint_refs: &[RangeConstraintRef],
) -> bool {
    let Plan::Select(plan) = subquery.plan.as_ref() else {
        return false;
    };
    let Some(group_by) = &plan.group_by else {
        return false;
    };
    if group_by.exprs.is_empty() {
        return false;
    }

    group_by.exprs.iter().all(|group_expr| {
        plan.result_columns
            .iter()
            .position(|column| exprs_are_equivalent(&column.expr, group_expr))
            .is_some_and(|column_pos| {
                constraint_refs.iter().any(|constraint| {
                    constraint.table_col_pos == Some(column_pos) && constraint.eq.is_some()
                })
            })
    })
}

/// Describe the temporary index layout we would build on top of a materialized
/// subquery if the planner chooses a seekable access path.
///
/// This is planner metadata, not the runtime build step itself. The optimizer
/// needs this shape up front so it can reason about seek prefixes, order
/// coverage, and result-column remapping before any bytecode is emitted.
fn materialized_subquery_ephemeral_index(
    rhs_table: &JoinedTable,
    subquery: &FromClauseSubquery,
    key_col_positions: &[usize],
) -> Result<Arc<Index>> {
    let mut index_columns: crate::alloc::Vec<IndexColumn> =
        crate::alloc::Vec::try_with_capacity_ext(subquery.columns.len())?;
    let mut seen_col_positions = std::collections::HashSet::new();

    for &col_pos in key_col_positions {
        let column = subquery
            .columns
            .get(col_pos)
            .expect("key column position out of bounds for materialized subquery");
        if !seen_col_positions.insert(col_pos) {
            continue;
        }
        index_columns
            .push_within_capacity(IndexColumn {
                name: column.name.clone().unwrap_or_default(),
                order: SortOrder::Asc,
                nulls_order: None,
                pos_in_table: col_pos,
                collation: column.collation_opt(),
                default: column.default.clone(),
                expr: None,
            })
            .expect("subquery index columns vector was preallocated to subquery.columns.len()");
    }

    for (col_pos, column) in subquery.columns.iter().enumerate() {
        if seen_col_positions.contains(&col_pos) {
            continue;
        }
        index_columns
            .push_within_capacity(IndexColumn {
                name: column.name.clone().unwrap_or_default(),
                order: SortOrder::Asc,
                nulls_order: None,
                pos_in_table: col_pos,
                collation: column.collation_opt(),
                default: column.default.clone(),
                expr: None,
            })
            .expect("subquery index columns vector was preallocated to subquery.columns.len()");
    }

    Ok(Arc::new(Index {
        // Match the runtime autoindex naming so EQP and bytecode make it clear
        // that this is a synthetic probe/index-on-temp-table path.
        name: format!("ephemeral_subquery_{}", rhs_table.internal_id),
        columns: index_columns,
        unique: false,
        ephemeral: true,
        table_name: subquery.name.clone(),
        root_page: 0,
        where_clause: None,
        has_rowid: true,
        index_method: None,
        on_conflict: None,
    }))
}

/// Decide whether the synthetic materialized-subquery index would also satisfy
/// the requested order target, and if so in which direction.
///
/// The returned bonus is the estimated sorter work avoided by getting rows in
/// the right order directly from the temporary index.
fn materialized_subquery_order_properties(
    rhs_table: &JoinedTable,
    index: &Arc<Index>,
    constraint_refs: &[RangeConstraintRef],
    maybe_order_target: Option<&OrderTarget>,
    schema: &Schema,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
) -> (IterationDirection, bool, Cost) {
    let Some(order_target) = maybe_order_target else {
        return (IterationDirection::Forwards, false, Cost(0.0));
    };

    // Candidate scoring may ignore any equality-constrained prefix because a
    // seek fixes those columns to one value before iteration begins.
    let all_same_direction = btree_access_order_consumed(
        rhs_table,
        IterationDirection::Forwards,
        Some(index.as_ref()),
        constraint_refs,
        order_target,
        0,
        schema,
        EqualityPrefixScope::AnyEquality,
    )
    .consumed
        == order_target.columns.len();
    let all_opposite_direction = btree_access_order_consumed(
        rhs_table,
        IterationDirection::Backwards,
        Some(index.as_ref()),
        constraint_refs,
        order_target,
        0,
        schema,
        EqualityPrefixScope::AnyEquality,
    )
    .consumed
        == order_target.columns.len();

    if !(all_same_direction || all_opposite_direction) {
        return (IterationDirection::Forwards, false, Cost(0.0));
    }

    // Reuse the same rough sorter cost model as ordinary ORDER BY planning:
    // if this index yields the needed order, we avoid an O(n log n) sort.
    let n = *base_row_count;
    let order_bonus = Cost(n * n.max(1.0).log2() * params.sort_cpu_per_row);
    (
        if all_same_direction {
            IterationDirection::Forwards
        } else {
            IterationDirection::Backwards
        },
        true,
        order_bonus,
    )
}
