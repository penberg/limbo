use crate::{alloc::TryReserveError, turso_assert_eq, turso_assert_greater_than};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use smallvec::SmallVec;

use turso_parser::ast::{Operator, TableInternalId};

use super::{
    access_method::{add_where_cost, find_best_access_method_for_join_order, AccessMethod},
    constraints::{usable_constraints_for_lhs_mask, TableConstraints},
    cost_params::CostModelParams,
    order::OrderTarget,
    AvailableIndexes, IndexMethodCandidate,
};
use crate::alloc::{TryClone, TursoIteratorExt};
use crate::translate::plan::BitSet;
use crate::{
    schema::Schema,
    stats::AnalyzeStats,
    translate::{
        expr::expr_references_subquery_id,
        optimizer::{
            access_method::{
                estimate_hash_join_cost, tables_in_equal_test, try_hash_join_access_method,
                AccessMethodParams,
            },
            cost::{
                estimate_rows_per_seek, rows_per_leaf_page_for_index, where_expr_steps, AnalyzeCtx,
                Cost, IndexInfo, RowCountEstimate,
            },
            order::plan_satisfies_order_target,
        },
        plan::{
            HashJoinKey, HashJoinType, JoinOrderMember, JoinedTable, NonFromClauseSubquery,
            SubqueryState, TableReferences, WhereTerm,
        },
        planner::{table_mask_from_expr, TableMask},
    },
    LimboError, Result,
};

#[derive(Debug, Clone, Copy)]
/// Small bag of planner context that needs to flow through join enumeration.
///
/// Keeping this as a struct avoids threading more ad-hoc parameters through the
/// join planner as we add order-aware access path choices.
pub(crate) struct JoinPlanningContext<'a> {
    pub maybe_order_target: Option<&'a OrderTarget>,
    /// Stop growing a join plan after it costs more than another query form.
    pub cost_limit: Option<Cost>,
}

impl<'a> JoinPlanningContext<'a> {
    /// Convenience constructor used by the default planner entrypoints and tests.
    #[cfg_attr(not(test), allow(dead_code))]
    fn default_with_order_target(maybe_order_target: Option<&'a OrderTarget>) -> Self {
        Self {
            maybe_order_target,
            cost_limit: None,
        }
    }
}

// Upper bound on rowids to materialize for a hash build input.
// This is a safety limit, not a cost tuning parameter.
const MAX_MATERIALIZED_BUILD_ROWS: f64 = 200_000.0;

/// Estimate how much the remaining `WHERE` terms cut the row count.
///
/// Some callers skip a term because they need the count before that term runs.
fn constraint_output_multipliers(
    rhs_constraints: &TableConstraints,
    lhs_mask: &TableMask,
    rhs_self_mask: TableMask,
    consumed_where_terms: &BitSet<usize>,
    skipped_where_terms: &BitSet<usize>,
    where_clause: &[WhereTerm],
    params: &CostModelParams,
) -> f64 {
    constraint_output_multipliers_for(
        rhs_constraints,
        lhs_mask,
        rhs_self_mask,
        consumed_where_terms,
        skipped_where_terms,
        where_clause,
        params,
        |_| true,
    )
}

#[allow(clippy::too_many_arguments)]
fn constraint_output_multipliers_for(
    rhs_constraints: &TableConstraints,
    lhs_mask: &TableMask,
    rhs_self_mask: TableMask,
    consumed_where_terms: &BitSet<usize>,
    skipped_where_terms: &BitSet<usize>,
    where_clause: &[WhereTerm],
    params: &CostModelParams,
    include: impl Fn(&super::constraints::Constraint) -> bool,
) -> f64 {
    let mut multiplier = 1.0;
    let mut bounds: SmallVec<[(Option<usize>, bool, bool); 4]> = SmallVec::new();

    let record_bound = |bounds: &mut SmallVec<[(Option<usize>, bool, bool); 4]>,
                        dominated_col: Option<usize>,
                        is_lower: bool,
                        is_upper: bool| {
        if !(is_lower || is_upper) {
            return;
        }
        if let Some(entry) = bounds.iter_mut().find(|(col, _, _)| *col == dominated_col) {
            entry.1 |= is_lower;
            entry.2 |= is_upper;
        } else {
            bounds.push((dominated_col, is_lower, is_upper));
        }
    };

    for constraint in rhs_constraints.constraints.iter().filter(|constraint| {
        (lhs_mask.contains_all_set_bits_of(&constraint.lhs_mask)
            || constraint.lhs_mask == rhs_self_mask
            || constraint.lhs_mask.is_empty())
            && !consumed_where_terms.get(constraint.where_clause_pos.0)
            && !skipped_where_terms.get(constraint.where_clause_pos.0)
            && !where_clause[constraint.where_clause_pos.0].consumed
            && include(constraint)
    }) {
        multiplier *= constraint.selectivity;

        let dominated_col = constraint.table_col_pos;
        let is_lower = matches!(
            constraint.operator.as_ast_operator(),
            Some(Operator::Greater | Operator::GreaterEquals)
        );
        let is_upper = matches!(
            constraint.operator.as_ast_operator(),
            Some(Operator::Less | Operator::LessEquals)
        );
        record_bound(&mut bounds, dominated_col, is_lower, is_upper);
    }

    for (_, has_lower, has_upper) in &bounds {
        if *has_lower && *has_upper {
            multiplier *= params.closed_range_selectivity_factor;
        }
    }

    multiplier
}

/// Return the row count after one table and its ready filters.
#[allow(clippy::too_many_arguments)]
fn rows_after_join(
    input_cardinality: f64,
    method: &AccessMethod,
    rhs_constraints: &TableConstraints,
    lhs_mask: &TableMask,
    rhs_mask: TableMask,
    rhs_table: &JoinedTable,
    where_clause: &[WhereTerm],
    params: &CostModelParams,
) -> f64 {
    if rhs_table
        .join_info
        .as_ref()
        .is_some_and(|join_info| join_info.is_semi_or_anti())
    {
        return input_cardinality;
    }
    let is_outer_join = rhs_table
        .join_info
        .as_ref()
        .is_some_and(|join_info| join_info.is_outer());
    if !is_outer_join {
        let remaining_filter_selectivity = constraint_output_multipliers(
            rhs_constraints,
            lhs_mask,
            rhs_mask,
            &method.consumed_where_terms,
            &Default::default(),
            where_clause,
            params,
        );
        return input_cardinality
            * method.estimated_rows_per_outer_row
            * remaining_filter_selectivity;
    }

    let is_on_term = |constraint: &super::constraints::Constraint| {
        where_clause[constraint.where_clause_pos.0].from_outer_join == Some(rhs_table.internal_id)
    };
    let on_selectivity = constraint_output_multipliers_for(
        rhs_constraints,
        lhs_mask,
        rhs_mask.clone(),
        &method.consumed_where_terms,
        &Default::default(),
        where_clause,
        params,
        is_on_term,
    );
    let matching_rows_per_input = method.estimated_rows_per_outer_row * on_selectivity;
    // Statistics give an average match count, but not its distribution.
    // A Poisson model estimates the chance that an input row has no match.
    let unmatched_probability = (-matching_rows_per_input).exp();
    let outer_join_rows_per_input = matching_rows_per_input + unmatched_probability;

    let selects_unmatched_rows = |constraint: &super::constraints::Constraint| {
        if is_on_term(constraint)
            || !matches!(constraint.operator.as_ast_operator(), Some(Operator::Is))
        {
            return false;
        }
        matches!(
            constraint.get_constraining_expr_ref(where_clause),
            turso_parser::ast::Expr::Literal(turso_parser::ast::Literal::Null)
        ) && constraint.table_col_pos.is_some_and(|column_pos| {
            rhs_table
                .table
                .get_column_at(column_pos)
                .is_some_and(|column| column.is_rowid_alias() || column.notnull())
        })
    };
    let selects_only_unmatched_rows = rhs_constraints.constraints.iter().any(|constraint| {
        !method
            .consumed_where_terms
            .get(constraint.where_clause_pos.0)
            && selects_unmatched_rows(constraint)
    });
    let where_selectivity = constraint_output_multipliers_for(
        rhs_constraints,
        lhs_mask,
        rhs_mask,
        &method.consumed_where_terms,
        &Default::default(),
        where_clause,
        params,
        |constraint| !is_on_term(constraint) && !selects_unmatched_rows(constraint),
    );
    let rows_per_input = if selects_only_unmatched_rows {
        unmatched_probability
    } else {
        outer_join_rows_per_input
    };
    input_cardinality * rows_per_input * where_selectivity
}

/// Count calls to each subquery when all of its outer tables have been read.
#[allow(clippy::too_many_arguments)]
fn count_subquery_calls_after_join(
    subqueries: &[NonFromClauseSubquery],
    joined_tables: &[JoinedTable],
    prior_tables: &TableMask,
    new_table_number: usize,
    outer_rows: f64,
    method: &AccessMethod,
    new_table_constraints: &TableConstraints,
    new_table_mask: TableMask,
    where_clause: &[WhereTerm],
    params: &CostModelParams,
) -> Result<SmallVec<[(TableInternalId, f64); 2]>> {
    let mut current_tables = prior_tables.clone();
    current_tables.set(new_table_number)?;

    let rows_before_filters = outer_rows * method.estimated_rows_per_outer_row;
    let mut subquery_calls = SmallVec::new();

    for subquery in subqueries.iter().filter(|subquery| subquery.correlated) {
        let SubqueryState::Unevaluated {
            plan: Some(inner_plan),
        } = &subquery.state
        else {
            continue;
        };
        let mut required_tables = TableMask::default();
        for table_id in inner_plan.used_outer_query_ref_ids() {
            let Some(table_number) = joined_tables
                .iter()
                .position(|table| table.internal_id == table_id)
            else {
                continue;
            };
            required_tables.set(table_number)?;
        }
        if required_tables.is_empty()
            || !current_tables.contains_all_set_bits_of(&required_tables)
            || prior_tables.contains_all_set_bits_of(&required_tables)
        {
            continue;
        }

        let first_subquery_term = where_clause.iter().enumerate().find_map(|(index, term)| {
            expr_references_subquery_id(&term.expr, subquery.internal_id).then_some(index)
        });
        // A WHERE term cannot cut the call count for a subquery in that term
        // or in an earlier term. Terms before it may cut the count.
        let skipped_where_terms: BitSet<usize> = if let Some(first) = first_subquery_term {
            (first..where_clause.len()).try_collect()?
        } else {
            Default::default()
        };
        let multiplier = constraint_output_multipliers(
            new_table_constraints,
            prior_tables,
            new_table_mask.clone(),
            &method.consumed_where_terms,
            &skipped_where_terms,
            where_clause,
            params,
        );
        let rows = rows_before_filters * multiplier;
        subquery_calls.push((subquery.internal_id, rows.max(1.0)));
    }

    Ok(subquery_calls)
}

/// Count subquery calls for the chosen join plan.
#[allow(clippy::too_many_arguments)]
pub(super) fn count_subquery_calls_for_plan(
    plan: &JoinN,
    access_methods: &[AccessMethod],
    constraints: &[TableConstraints],
    joined_tables: &[JoinedTable],
    where_clause: &[WhereTerm],
    subqueries: &[NonFromClauseSubquery],
    initial_input_cardinality: f64,
    params: &CostModelParams,
) -> Result<SmallVec<[(TableInternalId, f64); 2]>> {
    if !subqueries.iter().any(|subquery| subquery.correlated) {
        return Ok(SmallVec::new());
    }

    let mut calls = SmallVec::new();
    let mut prior_tables = TableMask::default();
    let mut input_cardinality = initial_input_cardinality;

    for (table_number, access_method_index) in &plan.data {
        let method = &access_methods[*access_method_index];
        let mut table_mask = TableMask::default();
        table_mask.set(*table_number)?;
        calls.extend(count_subquery_calls_after_join(
            subqueries,
            joined_tables,
            &prior_tables,
            *table_number,
            input_cardinality,
            method,
            &constraints[*table_number],
            table_mask.clone(),
            where_clause,
            params,
        )?);
        input_cardinality = rows_after_join(
            input_cardinality,
            method,
            &constraints[*table_number],
            &prior_tables,
            table_mask,
            &joined_tables[*table_number],
            where_clause,
            params,
        );
        prior_tables.set(*table_number)?;
    }
    Ok(calls)
}

/// Represents an n-ary join, anywhere from 1 table to N tables.
#[derive(Debug, Clone)]
pub struct JoinN {
    /// Tuple: (table_number, access_method_index)
    pub data: Vec<(usize, usize)>,
    /// The estimated number of rows returned by joining these n tables together.
    pub output_cardinality: f64,
    /// Estimated execution cost of this N-ary join.
    pub cost: Cost,
    /// Estimated output rows after each table access in `data`.
    pub prefix_cardinalities: Vec<f64>,
}

struct WhereTermInfo {
    table_mask: TableMask,
    extra_steps: usize,
    equal_tables: Option<(TableInternalId, TableInternalId, Option<TableInternalId>)>,
}

impl JoinN {
    pub fn table_numbers(&self) -> impl Iterator<Item = usize> + use<'_> {
        self.data.iter().map(|(table_number, _)| *table_number)
    }

    pub fn best_access_methods(&self) -> impl Iterator<Item = usize> + use<'_> {
        self.data
            .iter()
            .map(|(_, access_method_index)| *access_method_index)
    }
}

/// Join n-1 tables with the n'th table.
/// Returns None if the plan is worse than the provided cost upper bound or if no valid access method is found.
///
/// Hash-joins:
/// - We only consider hash join once there is a non-empty LHS.
/// - The build side is the most recently joined table (left-deep hash join); the RHS is the probe.
/// - We avoid hash-join shapes that would drop build-side filters unless we can preserve them
///   via materialized build rowids.
/// - Probe->build chaining is only allowed when the build input is materialized from the
///   join prefix; rebuilding from the full table would ignore prior join filters.
#[allow(clippy::too_many_arguments)]
fn join_lhs_and_rhs<'a>(
    lhs: Option<&JoinN>,
    initial_input_cardinality: f64,
    rhs_table_reference: &JoinedTable,
    rhs_constraints: &'a TableConstraints,
    all_constraints: &'a [TableConstraints],
    base_table_rows: &[RowCountEstimate],
    join_order: &[JoinOrderMember],
    planning_context: JoinPlanningContext<'_>,
    access_methods_arena: &'a mut Vec<AccessMethod>,
    cost_upper_bound: Cost,
    joined_tables: &[JoinedTable],
    where_clause: &mut [WhereTerm],
    where_terms: &[WhereTermInfo],
    subqueries: &[NonFromClauseSubquery],
    index_method_candidates: &[IndexMethodCandidate],
    params: &CostModelParams,
    analyze_stats: &AnalyzeStats,
    available_indexes: &AvailableIndexes,
    table_references: &TableReferences,
    schema: &Schema,
) -> Result<Option<JoinN>> {
    // The input cardinality for this join is the output cardinality of the previous join.
    // For example, in a 2-way join, if the left table has 1000 rows, and the right table will return 2 rows for each of the left table's rows,
    // then the output cardinality of the join will be 2000.
    let input_cardinality = lhs.map_or(initial_input_cardinality, |l| l.output_cardinality);

    let rhs_table_number = join_order.last().unwrap().original_idx;
    let rhs_base_rows = base_table_rows
        .get(rhs_table_number)
        .copied()
        .unwrap_or_else(|| RowCountEstimate::hardcoded_fallback(params));
    let lhs_mask = match lhs {
        Some(lhs) => lhs.table_numbers().try_collect()?,
        None => TableMask::default(),
    };
    let mut joined_mask = lhs_mask.try_clone()?;
    joined_mask.set(rhs_table_number)?;
    let ready_where = ready_where_work(
        where_clause,
        where_terms,
        &joined_mask,
        rhs_table_number,
        rhs_table_reference.internal_id,
    );

    let Some(method) = find_best_access_method_for_join_order(
        rhs_table_reference,
        rhs_constraints,
        &lhs_mask,
        join_order,
        planning_context,
        where_clause,
        &ready_where,
        available_indexes,
        table_references,
        subqueries,
        schema,
        analyze_stats,
        input_cardinality,
        rhs_base_rows,
        params,
    )?
    else {
        return Ok(None);
    };

    let lhs_cost = lhs.map_or(Cost(0.0), |l| l.cost);
    // If we have a previous table, consider hash join as an alternative
    let mut best_access_method = method;
    add_where_cost(
        &mut best_access_method,
        &ready_where,
        input_cardinality,
        params,
    );

    // Reuse for hash cost and output cardinality computation
    // Self-constraints are conditions comparing columns within the same table
    // (e.g., t.col1 < t.col2). Include them in selectivity since they filter rows.
    let rhs_self_mask = {
        let mut m = TableMask::default();
        m.set(rhs_table_number)?;
        m
    };

    let has_join_constraint = lhs.is_some()
        && where_terms.iter().any(|term| {
            term.table_mask.get(rhs_table_number) && term.table_mask.intersects(&lhs_mask)
        });
    if lhs.is_some() && !has_join_constraint {
        let rhs_self_constraint_selectivity =
            build_self_constraint_selectivity(rhs_constraints, rhs_table_number);
        // Penalize cross products so we don't introduce a table before it can join.
        let effective_rhs_rows = (*rhs_base_rows) * rhs_self_constraint_selectivity;
        let cross_cost = (input_cardinality) * effective_rhs_rows;
        best_access_method.cost = best_access_method.cost + Cost(cross_cost);
    }

    // If we already have a non-empty LHS (at least one table has been joined),
    // consider a hash-join alternative for the current RHS. This is a left-deep
    // join: the last table in the LHS becomes the build side, and the new RHS
    // table is the probe side. We only allow hash joins when:
    //
    // - The would-be build table is accessed via a scan, or we can preserve its
    //   filters by materializing rowids.
    // - The probe table is not using a selective index seek we’d prefer to keep.
    // - The build table has no remaining constraints from prior tables that are
    //   not already consumed as hash-join keys in earlier hash joins.
    if let Some(lhs) = lhs {
        let rhs_table_idx = join_order.last().unwrap().original_idx;
        let last_lhs_table_idx = join_order[join_order.len() - 2].original_idx;
        let lhs_table_numbers: TableMask = lhs.table_numbers().try_collect()?;

        let rhs_has_selective_seek = matches!(
            best_access_method.params,
            AccessMethodParams::BTreeTable {
                ref index,
                build_index,
                ref constraint_refs,
                ..
            } if !constraint_refs.is_empty()
                && !build_index
                && index.as_ref().is_none_or(|index| !index.ephemeral)
        );

        // The probe table must NOT be the build table of any earlier hash join,
        // otherwise we would need to re-probe a table that is already being
        // produced by a hash build.
        let arena = &access_methods_arena;
        let probe_table_is_prior_build = lhs.data.iter().any(|(_, am_idx)| {
            arena.get(*am_idx).is_some_and(|am| {
                if let AccessMethodParams::HashJoin {
                    build_table_idx, ..
                } = &am.params
                {
                    *build_table_idx == rhs_table_idx
                } else {
                    false
                }
            })
        });

        for build_table_idx in lhs_table_numbers {
            if build_table_idx != last_lhs_table_idx {
                continue;
            }
            let build_table = &joined_tables[build_table_idx];
            let build_has_rowid = build_table.btree().is_some_and(|btree| btree.has_rowid);

            // If the chosen access method for the build table already uses constraints,
            // skip hash join to avoid dropping those filters (unless we later decide
            // to materialize the filtered rowids).
            let build_access_method_uses_constraints = lhs
                .data
                .iter()
                .find(|(table_no, _)| *table_no == build_table_idx)
                .map(|(_, am_idx)| *am_idx)
                .map(|am_idx| {
                    let arena = &access_methods_arena;
                    arena.get(am_idx).is_some_and(|am| {
                        if let AccessMethodParams::BTreeTable {
                            build_index,
                            constraint_refs,
                            ..
                        } = &am.params
                        {
                            *build_index || !constraint_refs.is_empty()
                        } else {
                            false
                        }
                    })
                })
                .unwrap_or(false);

            let build_constraints = &all_constraints[build_table_idx];
            let build_base_rows = base_table_rows
                .get(build_table_idx)
                .copied()
                .unwrap_or_else(|| RowCountEstimate::hardcoded_fallback(params));
            let build_self_selectivity =
                build_self_constraint_selectivity(build_constraints, build_table_idx);
            let build_cardinality = (*build_base_rows) * build_self_selectivity;
            let probe_cardinality = *rhs_base_rows;

            let prior_mask = {
                let mut mask = lhs_mask.try_clone()?;
                mask.clear(build_table_idx);
                mask
            };
            let prior_constraint_selectivity =
                build_prior_constraint_selectivity(build_constraints, &prior_mask);
            let probe_multiplier = if lhs.data.len() == 1 && lhs.data[0].0 == build_table_idx {
                1.0
            } else {
                let join_selectivity = prior_constraint_selectivity.clamp(0.0, 1.0);
                let denom = (build_cardinality * join_selectivity).max(1.0);
                (input_cardinality / denom).max(1.0)
            };

            // The build table must NOT have any constraints from prior tables that won't be
            // consumed as hash-join keys. When a table becomes a hash build table, its
            // cursor is exhausted after building. If there are constraints like
            // `prior.x = build.x` that aren't part of the build & probe hash join, they
            // can't be evaluated because the build cursor is no longer positioned.
            //
            // HOWEVER: If the constraint references only tables that are the BUILD side
            // of earlier hash joins where this proposed build table was the PROBE, then
            // that equality is already used as a hash-join key. In that case, when we
            // later SeekRowid into the build table for that earlier join, the cursor is
            // correctly positioned and the constraint is effectively "consumed".
            //
            // Example:
            // `SELECT... items JOIN products ON items.name = products.name JOIN order_items ON products.price = order_items.price`:
            // - First hash join: items(build) - products(probe)
            // - When considering: products(build) - order_items(probe)
            // - products has constraint from items, BUT items is build of an earlier hash join where products was probe
            // - So the constraint IS consumed, and products cursor IS positioned via SeekRowid
            // Get the set of tables that are build tables for hash joins where build_table_idx was probe
            let prior_hash_build_mask: TableMask = {
                let arena = &access_methods_arena;
                lhs.data
                    .iter()
                    .filter_map(|(_, am_idx)| {
                        arena.get(*am_idx).and_then(|am| {
                            if let AccessMethodParams::HashJoin {
                                build_table_idx: prior_build_table_idx,
                                probe_table_idx,
                                ..
                            } = &am.params
                            {
                                if *probe_table_idx == build_table_idx {
                                    Some(*prior_build_table_idx)
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                    })
                    .try_collect()?
            };

            let build_has_prior_constraints = {
                build_constraints.constraints.iter().any(|c| {
                    // Check if this constraint references prior tables that are NOT already
                    // handled by a hash join where we were the probe table
                    if !c.lhs_mask.intersects(&prior_mask) {
                        return false; // Constraint doesn't reference prior tables
                    }
                    // Check if ALL referenced prior tables are already hash-joined with us as probe
                    // If so, the constraint is consumed and we're OK
                    for table_idx in 0..64 {
                        if c.lhs_mask.get(table_idx)
                            && prior_mask.get(table_idx)
                            && !prior_hash_build_mask.get(table_idx)
                        {
                            // This prior table is NOT handled by a hash join, constraint not consumed
                            return true;
                        }
                    }
                    false // All referenced prior tables are hash-joined
                })
            };

            // If this build table was already a probe in a prior hash join, scanning it again
            // for a hash build would ignore the prior join filters. The planner disallows
            // probe-to-build chaining, even with materialization.
            let build_table_is_prior_probe = lhs.data.iter().any(|(_, am_idx)| {
                let arena = &access_methods_arena;
                arena.get(*am_idx).is_some_and(|am| {
                    if let AccessMethodParams::HashJoin {
                        probe_table_idx, ..
                    } = &am.params
                    {
                        *probe_table_idx == build_table_idx
                    } else {
                        false
                    }
                })
            });
            // Avoid probe->build chaining across outer-join boundaries.
            let prefix_has_outer = join_order
                .iter()
                .take(join_order.len().saturating_sub(1))
                .any(|member| member.is_outer);
            let chaining_across_outer = build_table_is_prior_probe && prefix_has_outer;

            // Hash joins are safe only if we won't drop build-side filters:
            // - If the build scan is unconstrained, we can build directly.
            // - If there are prior/self constraints, we need materialization to preserve them.
            // Full index scans are treated as unconstrained for hash-join eligibility.
            //
            // We intentionally do NOT (yet) allow a table that is already the probe side of
            // a hash join to become the build side of another hash join; the second hash join
            // would rebuild from ALL rows of the middle table, not just the matching rows from the first.
            let build_am_is_plain_table_scan = lhs
                .data
                .iter()
                .find(|(table_no, _)| *table_no == build_table_idx)
                .map(|(_, am_idx)| {
                    let arena = &access_methods_arena;
                    arena.get(*am_idx).is_some_and(|am| {
                        matches!(
                            &am.params,
                            AccessMethodParams::BTreeTable {
                                build_index,
                                constraint_refs,
                                ..
                            } if !build_index && constraint_refs.is_empty()
                        )
                    })
                })
                .unwrap_or(false);

            let build_table_is_last = build_table_idx == last_lhs_table_idx;

            // Eligibility gate: prefer nested-loop when uses a selective probe seek.
            // Probe->build chaining is only allowed when the
            // build input is materialized from the join prefix.
            let allow_hash_join = !rhs_has_selective_seek
                && !probe_table_is_prior_build
                && (!build_has_prior_constraints || build_has_rowid)
                && !chaining_across_outer;

            tracing::debug!(
                lhs_table = build_table.table.get_name(),
                rhs_table = rhs_table_reference.table.get_name(),
                allow_hash_join,
                rhs_has_selective_seek,
                probe_table_is_prior_build,
                build_table_is_prior_probe,
                chaining_across_outer,
                build_am_is_plain_table_scan,
                build_has_rowid,
                "hash-join eligibility check"
            );
            if allow_hash_join {
                let lhs_constraints = build_constraints;
                if let Some(hash_join_method) = try_hash_join_access_method(
                    build_table,
                    rhs_table_reference,
                    build_table_idx,
                    rhs_table_idx,
                    lhs_constraints,
                    rhs_constraints,
                    where_clause,
                    where_terms.iter().enumerate().filter_map(|(index, term)| {
                        let (left, right, owner) = term.equal_tables?;
                        // An outer join condition belongs to that join only.
                        owner
                            .is_none_or(|owner| owner == rhs_table_reference.internal_id)
                            .then_some((index, left, right))
                    }),
                    build_cardinality,
                    probe_cardinality,
                    probe_multiplier,
                    subqueries,
                    params,
                )? {
                    let mut hash_join_method = hash_join_method;
                    let mut hash_join_allowed = true;
                    let mem_budget = match &hash_join_method.params {
                        AccessMethodParams::HashJoin { mem_budget, .. } => *mem_budget,
                        _ => unreachable!("hash join params expected"),
                    };
                    if let AccessMethodParams::HashJoin {
                        materialize_build_input,
                        use_bloom_filter,
                        join_keys,
                        ..
                    } = &mut hash_join_method.params
                    {
                        let needs_materialization = build_has_uncovered_prior_constraints(
                            lhs_constraints,
                            join_keys,
                            &prior_mask,
                            &prior_hash_build_mask,
                        ) || build_table_is_prior_probe
                            || !build_table_is_last;
                        let estimated_filtered_rows = (*build_base_rows)
                            * build_self_selectivity
                            * prior_constraint_selectivity;

                        // Hard cap: avoid materializing huge lists when materialization is required.
                        let materialization_too_large = needs_materialization
                            && estimated_filtered_rows > MAX_MATERIALIZED_BUILD_ROWS;
                        let can_materialize =
                            build_has_indexable_prior_constraints(lhs_constraints, &prior_mask);
                        let selectivity_threshold = if probe_multiplier > 1.0 {
                            params.hash_nested_probe_selectivity_threshold
                        } else {
                            params.hash_materialize_selectivity_threshold
                        };
                        // When probe is nested under prior loops, require stricter selectivity
                        // to justify materialization.
                        let wants_materialization = needs_materialization
                            || (build_access_method_uses_constraints
                                && prior_constraint_selectivity < selectivity_threshold);

                        let optional_materialization_too_large = !needs_materialization
                            && wants_materialization
                            && estimated_filtered_rows > MAX_MATERIALIZED_BUILD_ROWS;

                        // Build eligibility: a plain scan is always safe; otherwise we need
                        // materialization or existing constraints that make the scan selective.
                        let build_is_eligible = build_am_is_plain_table_scan
                            || needs_materialization
                            || build_access_method_uses_constraints;

                        hash_join_allowed = build_is_eligible
                            && (!needs_materialization || build_has_rowid)
                            && !materialization_too_large;

                        if hash_join_allowed {
                            let should_materialize = if needs_materialization {
                                build_has_rowid
                            } else {
                                wants_materialization
                                    && build_has_rowid
                                    && can_materialize
                                    && !optional_materialization_too_large
                            };
                            let hash_probe_multiplier = if should_materialize {
                                1.0
                            } else {
                                probe_multiplier
                            };
                            let effective_build_cardinality = if should_materialize {
                                estimated_filtered_rows
                            } else {
                                build_cardinality
                            };
                            // Estimate probe filters that apply only to the probe table itself
                            // (not join predicates) to inform the bloom filter heuristic.
                            let probe_self_selectivity = rhs_constraints
                                .constraints
                                .iter()
                                .filter(|c| c.lhs_mask.is_empty())
                                .map(|c| c.selectivity)
                                .product::<f64>();
                            let probe_filtered_rows =
                                (*rhs_base_rows) * probe_self_selectivity.clamp(0.0, 1.0);
                            if probe_filtered_rows > 0.0 {
                                let build_filtered_rows = if build_is_eligible {
                                    effective_build_cardinality
                                } else {
                                    build_cardinality
                                };
                                // Bloom filters help when the probe side is much larger than the build.
                                *use_bloom_filter = build_filtered_rows > 0.0
                                    && probe_filtered_rows / build_filtered_rows >= 2.0;
                            } else {
                                *use_bloom_filter = false;
                            }
                            if should_materialize {
                                hash_join_method.cost = estimate_hash_join_cost(
                                    effective_build_cardinality,
                                    probe_cardinality,
                                    mem_budget,
                                    hash_probe_multiplier,
                                    params,
                                );
                            }
                            if should_materialize {
                                // Materialize build-side rowids so the hash build only includes
                                // rows that already match prior join constraints.
                                *materialize_build_input = true;
                                let materialize_cost = effective_build_cardinality * 0.003;
                                hash_join_method.cost =
                                    hash_join_method.cost + Cost(materialize_cost);
                                // When two materialized hash-join plans have equal cost,
                                // prefer the one that filters by earlier prior tables.
                                //
                                // This is a deterministic tie-breaker that nudges the planner
                                // toward chaining a more selective prefix without changing
                                // the primary cost model.
                                let tie_breaker =
                                    prior_mask.iter().min().unwrap_or(0) as f64 * 1.0e-6;
                                hash_join_method.cost = hash_join_method.cost + Cost(tie_breaker);
                            } else {
                                *materialize_build_input = false;
                            }
                            tracing::debug!(
                                lhs_table = build_table.table.get_name(),
                                rhs_table = rhs_table_reference.table.get_name(),
                                materialize_build_input = *materialize_build_input,
                                needs_materialization,
                                estimated_filtered_rows,
                                prior_constraint_selectivity,
                                materialization_too_large,
                                can_materialize,
                                build_cardinality,
                                effective_build_cardinality,
                                probe_cardinality,
                                probe_multiplier,
                                hash_probe_multiplier,
                                prior_mask = ?prior_mask,
                                lhs_mask = ?lhs_mask,
                                hash_join_cost = ?hash_join_method.cost,
                                "hash-join candidate"
                            );
                        }
                    }
                    add_where_cost(
                        &mut hash_join_method,
                        &ready_where,
                        input_cardinality,
                        params,
                    );
                    // FULL OUTER requires hash join for the unmatched-build scan.
                    let is_full_outer = matches!(
                        &hash_join_method.params,
                        AccessMethodParams::HashJoin {
                            join_type: HashJoinType::FullOuter,
                            ..
                        }
                    );
                    if hash_join_allowed
                        && (is_full_outer || hash_join_method.cost < best_access_method.cost)
                    {
                        best_access_method = hash_join_method;
                    }
                }
            }
        }
    }

    // Check if there's an index method candidate for this table (e.g., FTS)
    // and compare its cost against the current best access method.
    if let Some(candidate) = index_method_candidates
        .iter()
        .find(|c| c.table_idx == rhs_table_number)
    {
        if let Some(cost_estimate) = &candidate.cost_estimate {
            // FTS cost depends on whether it's the outer table (no LHS) or inner table
            let fts_cost = if lhs.is_none() {
                // Outer table: FTS cost is fixed
                Cost(cost_estimate.estimated_cost)
            } else {
                // Inner table: FTS cost is multiplied by input cardinality
                Cost(cost_estimate.estimated_cost * input_cardinality)
            };

            let mut index_method = AccessMethod {
                cost: fts_cost,
                estimated_rows_per_outer_row: cost_estimate.estimated_rows as f64,
                consumed_where_terms: candidate.where_covered.into_iter().try_collect()?,
                params: AccessMethodParams::IndexMethod {
                    query: candidate.to_query(),
                    where_covered: candidate.where_covered,
                },
            };
            add_where_cost(&mut index_method, &ready_where, input_cardinality, params);
            if index_method.cost < best_access_method.cost {
                best_access_method = index_method;
            }
        }
    }

    // FULL OUTER needs a hash join. If the optimizer couldn't pick one, bail.
    if lhs.is_some() {
        let is_full_outer = rhs_table_reference
            .join_info
            .as_ref()
            .is_some_and(|ji| ji.is_full_outer());
        if is_full_outer
            && !matches!(
                best_access_method.params,
                AccessMethodParams::HashJoin {
                    join_type: HashJoinType::FullOuter,
                    ..
                }
            )
        {
            // This ordering can't satisfy FULL OUTER. Let the planner try others.
            return Ok(None);
        }
    }

    let cost = lhs_cost + best_access_method.cost;

    if cost > cost_upper_bound {
        return Ok(None);
    }
    // ============================================================================
    // OUTPUT CARDINALITY CALCULATION
    // ============================================================================
    //
    // Formula: output_rows = input_rows × rows_from_access_path × remaining_filter_selectivity.
    //
    // Each access method provides its own per-outer-row row estimate for:
    // - full BTree scans and full index scans
    // - rowid seeks
    // - ordinary secondary-index seeks
    // - multi-index OR/AND scans
    // - index-method access such as FTS
    //
    // Join planning only applies the selectivity of WHERE terms that the chosen
    // access path did not already consume.
    //
    let output_cardinality = rows_after_join(
        input_cardinality,
        &best_access_method,
        rhs_constraints,
        &lhs_mask,
        rhs_self_mask,
        &joined_tables[rhs_table_number],
        where_clause,
        params,
    );

    access_methods_arena.push(best_access_method);

    let mut best_access_methods = Vec::with_capacity(join_order.len());
    best_access_methods.extend(lhs.map_or(vec![], |l| l.data.clone()));
    best_access_methods.push((rhs_table_number, access_methods_arena.len() - 1));
    let mut prefix_cardinalities = Vec::with_capacity(join_order.len());
    if let Some(lhs) = lhs {
        prefix_cardinalities.extend_from_slice(&lhs.prefix_cardinalities);
    }
    prefix_cardinalities.push(output_cardinality);

    Ok(Some(JoinN {
        data: best_access_methods,
        output_cardinality,
        cost,
        prefix_cardinalities,
    }))
}

/// Returns true when build-side constraints reference prior tables in ways that
/// are not already consumed by hash-join keys.
fn build_has_uncovered_prior_constraints(
    build_constraints: &TableConstraints,
    join_keys: &[HashJoinKey],
    prior_mask: &TableMask,
    prior_hash_build_mask: &TableMask,
) -> bool {
    let mut join_key_indices = HashSet::default();
    for join_key in join_keys {
        join_key_indices.insert(join_key.where_clause_idx);
    }

    build_constraints.constraints.iter().any(|constraint| {
        if !constraint.lhs_mask.intersects(prior_mask) {
            return false;
        }
        if join_key_indices.contains(&constraint.where_clause_pos.0) {
            return false;
        }
        if constraint.operator != Operator::Equals.into() {
            return true;
        }
        if !constraint.lhs_mask.intersects(prior_hash_build_mask) {
            return true;
        }
        for table_idx in prior_mask.iter() {
            if constraint.lhs_mask.get(table_idx) && !prior_hash_build_mask.get(table_idx) {
                return true;
            }
        }
        false
    })
}

/// Estimates selectivity from prior equality constraints on the build side.
fn build_prior_constraint_selectivity(
    build_constraints: &TableConstraints,
    prior_mask: &TableMask,
) -> f64 {
    let mut selectivity = 1.0;
    let mut saw_constraint = false;
    for constraint in build_constraints.constraints.iter() {
        if constraint.operator == Operator::Equals.into()
            && constraint.lhs_mask.intersects(prior_mask)
        {
            tracing::debug!(
                where_clause_pos = ?constraint.where_clause_pos,
                lhs_mask = ?constraint.lhs_mask,
                prior_mask = ?prior_mask,
                selectivity = constraint.selectivity,
                "prior constraint selectivity contributor"
            );
            selectivity *= constraint.selectivity;
            saw_constraint = true;
        }
    }
    if !saw_constraint {
        return 1.0;
    }
    selectivity.clamp(0.0, 1.0)
}

/// Estimates selectivity from build-side constraints that reference only the build table.
fn build_self_constraint_selectivity(
    build_constraints: &TableConstraints,
    build_table_idx: usize,
) -> f64 {
    let build_only_mask: TableMask = [build_table_idx]
        .into_iter()
        .try_collect()
        .expect("does not heap allocate with just a single value");
    let mut selectivity = 1.0;
    let mut saw_constraint = false;
    for constraint in build_constraints.constraints.iter() {
        if !build_only_mask.contains_all_set_bits_of(&constraint.lhs_mask) {
            continue;
        }
        selectivity *= constraint.selectivity;
        saw_constraint = true;
    }
    if !saw_constraint {
        return 1.0;
    }
    selectivity.clamp(0.0, 1.0)
}

/// Returns true if any prior constraints can be turned into an index lookup.
fn build_has_indexable_prior_constraints(
    build_constraints: &TableConstraints,
    prior_mask: &TableMask,
) -> bool {
    build_constraints.candidates.iter().any(|candidate| {
        candidate.refs.iter().any(|constraint_ref| {
            let constraint = &build_constraints.constraints[constraint_ref.constraint_vec_pos];
            constraint.usable && constraint.lhs_mask.intersects(prior_mask)
        })
    })
}

/// The result of [compute_best_join_order].
#[derive(Debug)]
pub struct BestJoinOrderResult {
    /// The best plan overall.
    pub best_plan: JoinN,
    /// The best plan for the given order target, if it isn't the overall best.
    pub best_ordered_plan: Option<JoinN>,
}

/// Compute the best way to join a given set of tables.
/// Returns the best [JoinN] if one exists, otherwise returns None.
#[allow(clippy::too_many_arguments)]
#[cfg_attr(not(test), allow(dead_code))]
pub fn compute_best_join_order<'a>(
    joined_tables: &[JoinedTable],
    initial_input_cardinality: f64,
    maybe_order_target: Option<&OrderTarget>,
    constraints: &'a [TableConstraints],
    base_table_rows: &[RowCountEstimate],
    access_methods_arena: &'a mut Vec<AccessMethod>,
    where_clause: &mut [WhereTerm],
    subqueries: &[NonFromClauseSubquery],
    index_method_candidates: &[IndexMethodCandidate],
    params: &CostModelParams,
    analyze_stats: &AnalyzeStats,
    available_indexes: &AvailableIndexes,
    table_references: &TableReferences,
    schema: &Schema,
) -> Result<Option<BestJoinOrderResult>> {
    compute_best_join_order_with_context(
        joined_tables,
        initial_input_cardinality,
        JoinPlanningContext::default_with_order_target(maybe_order_target),
        constraints,
        base_table_rows,
        access_methods_arena,
        where_clause,
        subqueries,
        index_method_candidates,
        params,
        analyze_stats,
        available_indexes,
        table_references,
        schema,
    )
}

/// Enumerate join orders while carrying a small amount of planner context that
/// influences access-path scoring, such as an order target for sort elimination
/// or simple MIN/MAX planning.
#[expect(clippy::too_many_arguments)]
pub(crate) fn compute_best_join_order_with_context<'a>(
    joined_tables: &[JoinedTable],
    initial_input_cardinality: f64,
    planning_context: JoinPlanningContext<'_>,
    constraints: &'a [TableConstraints],
    base_table_rows: &[RowCountEstimate],
    access_methods_arena: &'a mut Vec<AccessMethod>,
    where_clause: &mut [WhereTerm],
    subqueries: &[NonFromClauseSubquery],
    index_method_candidates: &[IndexMethodCandidate],
    params: &CostModelParams,
    analyze_stats: &AnalyzeStats,
    available_indexes: &AvailableIndexes,
    table_references: &TableReferences,
    schema: &Schema,
) -> Result<Option<BestJoinOrderResult>> {
    // Skip work if we have no tables to consider.
    if joined_tables.is_empty() {
        return Ok(None);
    }

    let num_tables = joined_tables.len();

    // For large queries, use greedy join ordering instead of exhaustive DP.
    // The DP algorithm has O(2^n) complexity which becomes prohibitively slow
    // beyond ~12 tables. The greedy algorithm is O(n²) and produces good
    // (though not always optimal) plans.
    let where_terms = build_where_term_info(where_clause, table_references, subqueries)?;
    if num_tables > GREEDY_JOIN_THRESHOLD {
        return compute_greedy_join_order(
            joined_tables,
            initial_input_cardinality,
            planning_context,
            constraints,
            base_table_rows,
            access_methods_arena,
            where_clause,
            &where_terms,
            subqueries,
            index_method_candidates,
            params,
            analyze_stats,
            available_indexes,
            table_references,
            schema,
        );
    }

    // Compute naive left-to-right plan to use as pruning threshold
    let naive_plan = compute_naive_left_deep_plan(
        joined_tables,
        initial_input_cardinality,
        planning_context,
        base_table_rows,
        access_methods_arena,
        constraints,
        where_clause,
        &where_terms,
        subqueries,
        index_method_candidates,
        params,
        analyze_stats,
        available_indexes,
        table_references,
        schema,
    )?;

    // Keep track of both 1. the best plan overall (not considering sorting), and 2. the best ordered plan (which might not be the same).
    // We assign Some Cost (tm) to any required sort operation, so the best ordered plan may end up being
    // the one we choose, if the cost reduction from avoiding sorting brings it below the cost of the overall best one.
    let mut best_ordered_plan: Option<JoinN> = None;
    let mut best_plan_is_also_ordered =
        match (naive_plan.as_ref(), planning_context.maybe_order_target) {
            (Some(plan), Some(order_target)) => plan_satisfies_order_target(
                plan,
                access_methods_arena,
                joined_tables,
                constraints,
                order_target,
                schema,
            ),
            _ => false,
        };

    // If we have one table, then the "naive left-to-right plan" is always the best.
    if joined_tables.len() == 1 {
        return match naive_plan {
            Some(plan) => Ok(Some(BestJoinOrderResult {
                best_plan: plan,
                best_ordered_plan: None,
            })),
            None => Err(LimboError::PlanningError(
                "No valid query plan found".to_string(),
            )),
        };
    }
    let mut best_plan = naive_plan;

    // Reuse a single mutable join order to avoid allocating join orders per permutation.
    let mut join_order = Vec::with_capacity(num_tables);
    join_order.push(JoinOrderMember {
        table_id: TableInternalId::default(),
        original_idx: 0,
        is_outer: false,
    });

    // Keep track of the current best cost so we can short-circuit planning for subplans
    // that already exceed the cost of the current best plan.
    let mut cost_upper_bound = best_plan.as_ref().map_or(Cost(f64::MAX), |plan| plan.cost);
    if let Some(cost_limit) = planning_context.cost_limit {
        if cost_limit < cost_upper_bound {
            cost_upper_bound = cost_limit;
        }
    }

    // Keep track of the best plan for a given subset of tables.
    // Consider this example: we have tables a,b,c,d to join.
    // if we find that 'b JOIN a' is better than 'a JOIN b', then we don't need to even try
    // to do 'a JOIN b JOIN c', because we know 'b JOIN a JOIN c' is going to be better.
    // This is due to the commutativity and associativity of inner joins.
    // Memo table keyed by a subset mask, then by the last table in the join order.
    //
    // We keep multiple plans per subset instead of only the cheapest one. The cheapest
    // subset plan is not always the best foundation for the next join. Keeping variants
    // lets the planner choose a better join order later (e.g. for hash-join chaining).
    let mut best_plan_memo: HashMap<TableMask, HashMap<usize, JoinN>> =
        HashMap::with_capacity_and_hasher(2usize.pow(num_tables as u32 - 1), Default::default());

    // Dynamic programming base case: calculate the best way to access each single table, as if
    // there were no other tables.
    for i in 0..num_tables {
        let mut mask = TableMask::default();
        mask.set(i)?;
        let table_ref = &joined_tables[i];
        join_order[0] = JoinOrderMember {
            table_id: table_ref.internal_id,
            original_idx: i,
            is_outer: false,
        };
        turso_assert_eq!(join_order.len(), 1);
        let rel = join_lhs_and_rhs(
            None,
            initial_input_cardinality,
            table_ref,
            &constraints[i],
            constraints,
            base_table_rows,
            &join_order,
            planning_context,
            access_methods_arena,
            cost_upper_bound,
            joined_tables,
            where_clause,
            &where_terms,
            subqueries,
            index_method_candidates,
            params,
            analyze_stats,
            available_indexes,
            table_references,
            schema,
        )?;
        if let Some(rel) = rel {
            best_plan_memo.entry(mask).or_default().insert(i, rel);
        }
    }
    join_order.clear();

    // As mentioned, inner joins are commutative. Outer joins are NOT.
    // Example:
    // "a LEFT JOIN b" can NOT be reordered as "b LEFT JOIN a".
    // If there are outer joins in the plan, ensure correct ordering.
    let (left_join_illegal_map, required_lhs_by_table) = {
        let ordering_constrained_count = joined_tables
            .iter()
            .filter(|t| {
                t.join_info
                    .as_ref()
                    .is_some_and(|j| j.is_ordering_constrained())
            })
            .count();
        let has_full_outer = joined_tables
            .iter()
            .any(|t| t.join_info.as_ref().is_some_and(|j| j.is_full_outer()));
        if ordering_constrained_count == 0 && !has_full_outer {
            (None, None)
        } else {
            // map from rhs table index to lhs table index
            let mut left_join_illegal_map: HashMap<usize, TableMask> =
                HashMap::with_capacity_and_hasher(ordering_constrained_count, Default::default());
            let mut required_lhs_by_table = vec![TableMask::default(); num_tables];
            for (i, _) in joined_tables.iter().enumerate() {
                for (j, joined_table) in joined_tables.iter().enumerate().skip(i + 1) {
                    // LEFT/FULL OUTER, SEMI, and ANTI joins all require the RHS table
                    // to appear after the LHS table in the join order.
                    if joined_table
                        .join_info
                        .as_ref()
                        .is_some_and(|j| j.is_ordering_constrained())
                    {
                        required_lhs_by_table[j].set(i)?;
                        // bitwise OR the masks
                        if let Some(illegal_lhs) = left_join_illegal_map.get_mut(&i) {
                            illegal_lhs.set(j)?;
                        } else {
                            let mut mask = TableMask::default();
                            mask.set(j)?;
                            left_join_illegal_map.insert(i, mask);
                        }
                    }
                }
            }
            // FULL OUTER acts as a reordering barrier in both directions: tables
            // originally after a FULL OUTER table cannot be moved before it, or
            // the planner produces e.g. `(t1 INNER t3) FULL OUTER t2` instead of
            // the requested `(t1 FULL OUTER t2) INNER t3`, which can leak
            // NULL-filled probe rows past the inner join.
            for (k, t) in joined_tables.iter().enumerate() {
                if !t.join_info.as_ref().is_some_and(|j| j.is_full_outer()) {
                    continue;
                }
                for (j, required_lhs) in required_lhs_by_table.iter_mut().enumerate().skip(k + 1) {
                    required_lhs.set(k)?;
                    if let Some(illegal_lhs) = left_join_illegal_map.get_mut(&k) {
                        illegal_lhs.set(j)?;
                    } else {
                        let mut mask = TableMask::default();
                        mask.set(j)?;
                        left_join_illegal_map.insert(k, mask);
                    }
                }
            }
            (Some(left_join_illegal_map), Some(required_lhs_by_table))
        }
    };

    // Now that we have our single-table base cases, we can start considering join subsets of 2 tables and more.
    // Try to join each single table to each other table.
    for subset_size in 2..=num_tables {
        for mask in generate_join_bitmasks(num_tables, subset_size) {
            let mask = mask?;
            if required_lhs_by_table.as_ref().is_some_and(|required| {
                required.iter().enumerate().any(|(table, required)| {
                    mask.get(table) && !mask.contains_all_set_bits_of(required)
                })
            }) {
                continue;
            }
            // Keep track of the best way to join this subset of tables per possible last table.
            // This preserves alternative join orders that may be more expensive for the subset
            // but enable cheaper joins when adding more tables.
            let mut best_for_mask_by_last: HashMap<usize, JoinN> = HashMap::default();
            // Also keep track of the best plan for this subset that orders the rows in an
            // Interesting Way (tm), i.e. allows us to eliminate sort operations downstream.
            let mut best_ordered_for_mask: Option<JoinN> = None;

            // Try to join all subsets (masks) with all other tables.
            // In this block, LHS is always (n-1) tables, and RHS is a single table.
            for rhs_idx in 0..num_tables {
                // If the RHS table isn't a member of this join subset, skip.
                if !mask.get(rhs_idx) {
                    continue;
                }

                // If there are no other tables except RHS, skip.
                let lhs_mask = {
                    let mut copy = mask.try_clone()?;
                    copy.clear(rhs_idx);
                    copy
                };
                if lhs_mask.is_empty() {
                    continue;
                }

                if has_connected_legal_candidate(
                    &lhs_mask,
                    num_tables,
                    &where_terms,
                    required_lhs_by_table.as_deref(),
                    left_join_illegal_map.as_ref(),
                ) && !tables_are_connected(&lhs_mask, rhs_idx, &where_terms)
                {
                    continue;
                }

                // If this join ordering would violate LEFT JOIN ordering restrictions, skip.
                if let Some(illegal_lhs) = left_join_illegal_map
                    .as_ref()
                    .and_then(|deps| deps.get(&rhs_idx))
                {
                    let legal = !lhs_mask.intersects(illegal_lhs);
                    if !legal {
                        continue; // Don't allow RHS before its LEFT in LEFT JOIN
                    }
                }

                let Some(lhs_variants) = best_plan_memo.get(&lhs_mask) else {
                    continue;
                };

                // Stable iteration keeps tie-breaks consistent across runs.
                let lhs_keys: TableMask = lhs_variants.keys().copied().try_collect()?;
                for lhs_key in &lhs_keys {
                    let lhs = &lhs_variants[&lhs_key];
                    // Build a JoinOrder out of the table bitmask under consideration.
                    for table_no in lhs.table_numbers() {
                        join_order.push(JoinOrderMember {
                            table_id: joined_tables[table_no].internal_id,
                            original_idx: table_no,
                            is_outer: joined_tables[table_no]
                                .join_info
                                .as_ref()
                                .is_some_and(|j| j.is_outer()),
                        });
                    }
                    join_order.push(JoinOrderMember {
                        table_id: joined_tables[rhs_idx].internal_id,
                        original_idx: rhs_idx,
                        is_outer: joined_tables[rhs_idx]
                            .join_info
                            .as_ref()
                            .is_some_and(|j| j.is_outer()),
                    });
                    turso_assert_eq!(join_order.len(), subset_size);

                    // Calculate the best way to join LHS with RHS.
                    let arena_len = access_methods_arena.len();
                    let rel = join_lhs_and_rhs(
                        Some(lhs),
                        initial_input_cardinality,
                        &joined_tables[rhs_idx],
                        &constraints[rhs_idx],
                        constraints,
                        base_table_rows,
                        &join_order,
                        planning_context,
                        access_methods_arena,
                        cost_upper_bound,
                        joined_tables,
                        where_clause,
                        &where_terms,
                        subqueries,
                        index_method_candidates,
                        params,
                        analyze_stats,
                        available_indexes,
                        table_references,
                        schema,
                    )?;
                    join_order.clear();

                    let Some(rel) = rel else {
                        access_methods_arena.truncate(arena_len);
                        continue;
                    };

                    let satisfies_order_target =
                        if let Some(order_target) = planning_context.maybe_order_target {
                            plan_satisfies_order_target(
                                &rel,
                                access_methods_arena,
                                joined_tables,
                                constraints,
                                order_target,
                                schema,
                            )
                        } else {
                            false
                        };

                    // If this plan is worse than our overall best, it might still be the best ordered plan.
                    if rel.cost >= cost_upper_bound {
                        // But if it isn't, skip.
                        if !satisfies_order_target {
                            access_methods_arena.truncate(arena_len);
                            continue;
                        }
                        let existing_ordered_cost: Cost = best_ordered_for_mask
                            .as_ref()
                            .map_or(Cost(f64::MAX), |p: &JoinN| p.cost);
                        if rel.cost < existing_ordered_cost {
                            best_ordered_for_mask = Some(rel);
                        } else {
                            access_methods_arena.truncate(arena_len);
                        }
                        continue;
                    }

                    let should_replace = match best_for_mask_by_last.get(&rhs_idx) {
                        Some(existing) => rel.cost < existing.cost,
                        None => true,
                    };
                    if should_replace {
                        best_for_mask_by_last.insert(rhs_idx, rel);
                    } else {
                        access_methods_arena.truncate(arena_len);
                    }
                }
            }

            let has_all_tables = mask.count() == num_tables;
            if has_all_tables {
                for rel in best_for_mask_by_last.into_values() {
                    if cost_upper_bound <= rel.cost {
                        continue;
                    }
                    let satisfies_order_target =
                        if let Some(order_target) = planning_context.maybe_order_target {
                            plan_satisfies_order_target(
                                &rel,
                                access_methods_arena,
                                joined_tables,
                                constraints,
                                order_target,
                                schema,
                            )
                        } else {
                            false
                        };
                    if best_plan.as_ref().is_none_or(|plan| rel.cost < plan.cost) {
                        best_plan = Some(rel);
                        best_plan_is_also_ordered = satisfies_order_target;
                    }
                }
                if let Some(rel) = best_ordered_for_mask.take() {
                    let cost = rel.cost;
                    if cost_upper_bound > cost {
                        best_ordered_plan = Some(rel);
                    }
                }
            } else if !best_for_mask_by_last.is_empty() {
                best_plan_memo.insert(mask, best_for_mask_by_last);
            }
        }
    }

    match best_plan {
        Some(best_plan) => Ok(Some(BestJoinOrderResult {
            best_plan,
            best_ordered_plan: if best_plan_is_also_ordered {
                None
            } else {
                best_ordered_plan
            },
        })),
        None => {
            // Give a targeted error for FULL OUTER when no plan was found.
            let has_full_outer = joined_tables
                .iter()
                .any(|t| t.join_info.as_ref().is_some_and(|ji| ji.is_full_outer()));
            if has_full_outer {
                // Distinguish chaining from a missing equi-join condition.
                let build_is_outer = joined_tables.iter().any(|t| {
                    let is_full = t.join_info.as_ref().is_some_and(|ji| ji.is_full_outer());
                    if !is_full {
                        return false;
                    }
                    // Check if any earlier table (potential build) is also outer.
                    joined_tables.iter().any(|other| {
                        !std::ptr::eq(t, other)
                            && other.join_info.as_ref().is_some_and(|ji| ji.is_outer())
                    })
                });
                // A recursive CTE input cannot be the build side of the hash
                // join that FULL OUTER requires, so no plan exists for
                // `recursive_table FULL JOIN other`.
                let has_recursive_input = joined_tables
                    .iter()
                    .any(|t| matches!(t.table, crate::schema::Table::RecursiveCteInput(_)));
                let has_correlated_subquery = subqueries.iter().any(|sq| sq.correlated);
                let msg = if build_is_outer {
                    "FULL OUTER JOIN chaining is not yet supported"
                } else if has_recursive_input {
                    "FULL OUTER JOIN with a recursive reference is not yet supported"
                } else if has_correlated_subquery {
                    "FULL OUTER JOIN is not supported with correlated subqueries that reference the joined tables"
                } else {
                    "FULL OUTER JOIN requires an equality condition in the ON clause"
                };
                Err(LimboError::ParseError(msg.to_string()))
            } else {
                Err(LimboError::PlanningError(
                    "No valid query plan found".to_string(),
                ))
            }
        }
    }
}

fn has_connected_legal_candidate(
    prefix: &TableMask,
    num_tables: usize,
    where_terms: &[WhereTermInfo],
    required_lhs_by_table: Option<&[TableMask]>,
    left_join_illegal_map: Option<&HashMap<usize, TableMask>>,
) -> bool {
    (0..num_tables).any(|candidate| {
        !prefix.get(candidate)
            && can_add_table_to_prefix(
                prefix,
                candidate,
                required_lhs_by_table,
                left_join_illegal_map,
            )
            && tables_are_connected(prefix, candidate, where_terms)
    })
}

fn can_add_table_to_prefix(
    prefix: &TableMask,
    candidate: usize,
    required_lhs_by_table: Option<&[TableMask]>,
    left_join_illegal_map: Option<&HashMap<usize, TableMask>>,
) -> bool {
    let has_required_tables = required_lhs_by_table
        .and_then(|required| required.get(candidate))
        .is_none_or(|required| prefix.contains_all_set_bits_of(required));
    let keeps_outer_join_order = left_join_illegal_map
        .and_then(|illegal| illegal.get(&candidate))
        .is_none_or(|illegal| !prefix.intersects(illegal));
    has_required_tables && keeps_outer_join_order
}

fn tables_are_connected(
    prefix: &TableMask,
    candidate: usize,
    where_terms: &[WhereTermInfo],
) -> bool {
    where_terms
        .iter()
        .any(|term| term.table_mask.get(candidate) && term.table_mask.intersects(prefix))
}

/// Above this threshold, use greedy O(n²) ordering instead of exhaustive O(2^n) DP.
pub const GREEDY_JOIN_THRESHOLD: usize = 12;

/// Greedy Operator Ordering (GOO) for join optimization. O(n²) time, O(n) space.
///
/// Builds a left-deep join tree by:
/// 1. Starting with the table that best balances local filtering and indexed-seek benefits
/// 2. Greedily adding the remaining table with lowest marginal cost
///
/// Respects outer join ordering constraints.
#[allow(clippy::too_many_arguments)]
fn compute_greedy_join_order<'a>(
    joined_tables: &[JoinedTable],
    initial_input_cardinality: f64,
    planning_context: JoinPlanningContext<'_>,
    constraints: &'a [TableConstraints],
    base_table_rows: &[RowCountEstimate],
    access_methods_arena: &'a mut Vec<AccessMethod>,
    where_clause: &mut [WhereTerm],
    where_terms: &[WhereTermInfo],
    subqueries: &[NonFromClauseSubquery],
    index_method_candidates: &[IndexMethodCandidate],
    params: &CostModelParams,
    analyze_stats: &AnalyzeStats,
    available_indexes: &AvailableIndexes,
    table_references: &TableReferences,
    schema: &Schema,
) -> Result<Option<BestJoinOrderResult>> {
    let num_tables = joined_tables.len();
    if num_tables == 0 {
        return Ok(None);
    }

    // Outer join RHS tables require all preceding tables to be joined first.
    let left_join_deps: HashMap<usize, TableMask> = joined_tables
        .iter()
        .enumerate()
        .filter(|(_, t)| {
            t.join_info
                .as_ref()
                .is_some_and(|ji| ji.is_ordering_constrained())
        })
        .map(|(j, _)| {
            let mut required = TableMask::default();
            for k in 0..j {
                required.set(k)?;
            }
            Ok((j, required))
        })
        .collect::<Result<_>>()?;

    let mut remaining: TableMask = (0..num_tables).try_collect()?;
    let mut join_order: Vec<JoinOrderMember> = Vec::with_capacity(num_tables);

    // Pick the starting table using local filters and directed indexed-seek benefits.
    let first_idx = find_best_starting_table(
        num_tables,
        joined_tables,
        constraints,
        base_table_rows,
        &left_join_deps,
        analyze_stats,
        params,
    )?;
    let first_table = &joined_tables[first_idx];
    join_order.push(JoinOrderMember {
        table_id: first_table.internal_id,
        original_idx: first_idx,
        is_outer: false, // First table cannot be outer join RHS
    });
    remaining.clear(first_idx);

    let mut current_plan: Option<JoinN> = join_lhs_and_rhs(
        None,
        initial_input_cardinality,
        first_table,
        &constraints[first_idx],
        constraints,
        base_table_rows,
        &join_order,
        planning_context,
        access_methods_arena,
        Cost(f64::MAX),
        joined_tables,
        where_clause,
        where_terms,
        subqueries,
        index_method_candidates,
        params,
        analyze_stats,
        available_indexes,
        table_references,
        schema,
    )?;

    if current_plan.is_none() {
        return Err(LimboError::PlanningError(
            "No valid query plan found for first table".to_string(),
        ));
    }

    // Greedily add remaining tables, always picking lowest marginal cost.
    while !remaining.is_empty() {
        let current_mask: TableMask = join_order.iter().map(|m| m.original_idx).try_collect()?;

        // Placeholder for candidate evaluation (avoids cloning)
        join_order.push(JoinOrderMember::default());

        let mut best: Option<(usize, JoinN)> = None;

        let candidate_is_legal = |candidate| {
            left_join_deps
                .get(&candidate)
                .is_none_or(|required| current_mask.contains_all_set_bits_of(required))
        };
        let must_stay_connected = remaining.iter().any(|candidate| {
            candidate_is_legal(candidate)
                && tables_are_connected(&current_mask, candidate, where_terms)
        });

        for idx in &remaining {
            if !candidate_is_legal(idx)
                || (must_stay_connected && !tables_are_connected(&current_mask, idx, where_terms))
            {
                continue;
            }

            let table = &joined_tables[idx];
            let last = join_order.last_mut().unwrap();
            last.table_id = table.internal_id;
            last.original_idx = idx;
            last.is_outer = table.join_info.as_ref().is_some_and(|ji| ji.is_outer());

            if let Some(plan) = join_lhs_and_rhs(
                current_plan.as_ref(),
                initial_input_cardinality,
                table,
                &constraints[idx],
                constraints,
                base_table_rows,
                &join_order,
                planning_context,
                access_methods_arena,
                Cost(f64::MAX),
                joined_tables,
                where_clause,
                where_terms,
                subqueries,
                index_method_candidates,
                params,
                analyze_stats,
                available_indexes,
                table_references,
                schema,
            )? {
                if best.as_ref().is_none_or(|(_, b)| plan.cost < b.cost) {
                    best = Some((idx, plan));
                }
            }
        }

        join_order.pop();

        let (next_idx, next_plan) = best.ok_or_else(|| {
            LimboError::PlanningError("Greedy join ordering: no valid next table".to_string())
        })?;

        let next_table = &joined_tables[next_idx];
        join_order.push(JoinOrderMember {
            table_id: next_table.internal_id,
            original_idx: next_idx,
            is_outer: next_table
                .join_info
                .as_ref()
                .is_some_and(|ji| ji.is_outer()),
        });
        remaining.clear(next_idx);
        current_plan = Some(next_plan);
    }

    Ok(Some(BestJoinOrderResult {
        best_plan: current_plan.expect("loop invariant: current_plan always Some"),
        best_ordered_plan: None, // Greedy doesn't track ordered variants
    }))
}

/// Select the best starting table for greedy join ordering by evaluating indexed-seek benefits.
///
/// Score = base_rows * filter_selectivity * table's indexed-seek multiplier
///
/// The multiplier is lower (better) for tables that enable indexed seeks on others
/// (e.g., a fact table in a star schema) and higher for tables that are themselves
/// better reached via an index only after a predecessor has been joined.
///
/// Lower score wins. Outer join RHS tables are excluded.
fn find_best_starting_table(
    num_tables: usize,
    joined_tables: &[JoinedTable],
    constraints: &[TableConstraints],
    base_table_rows: &[RowCountEstimate],
    left_join_deps: &HashMap<usize, TableMask>,
    analyze_stats: &AnalyzeStats,
    params: &CostModelParams,
) -> Result<usize> {
    let multipliers = compute_indexed_seek_benefits(
        num_tables,
        joined_tables,
        constraints,
        base_table_rows,
        left_join_deps,
        analyze_stats,
        params,
    )?;

    let mut best: Option<(usize, f64)> = None;
    for t in 0..num_tables {
        if left_join_deps.contains_key(&t) {
            continue; // Outer join RHS - cannot be first
        }

        let base_rows = *base_table_rows[t];

        // Self-constraints compare columns within the same table (e.g., t.col1 < t.col2).
        let self_mask = {
            let mut m = TableMask::default();
            m.set(t)?;
            m
        };

        // Include literal constraints (lhs_mask empty) and self-constraints in selectivity
        let selectivity: f64 = constraints[t]
            .constraints
            .iter()
            .filter(|c| c.lhs_mask.is_empty() || c.lhs_mask == self_mask)
            .map(|c| c.selectivity)
            .product();

        let score = base_rows * selectivity * multipliers[t];

        if best.is_none_or(|(_, s)| score < s) {
            best = Some((t, score));
        }
    }

    // Table 0 can never be outer join RHS, so best is always Some.
    Ok(best.expect("no valid starting table").0)
}

#[derive(Debug, Default, Clone, Copy)]
struct IndexedSeekBenefit {
    /// measures how much choosing this table first enables indexed seeks on other tables.
    /// Higher is better for a starting table.
    reward: f64,
    /// measures how much this table prefers some other table to be joined first
    /// so it can be reached through an indexed seek.
    /// Higher indicates that it is a poor starting choice.
    penalty: f64,
}

impl IndexedSeekBenefit {
    fn multiplier(&self) -> f64 {
        (1.0 + self.penalty) / (1.0 + self.reward)
    }
}

/// Compute directed indexed-seek benefits for single-table starting choices.
///
/// A table enables a seek on another when its presence in the join prefix allows
/// the second table to use an indexed seek instead of a scan. The returned
/// multiplier is lower for better starting choices.
fn compute_indexed_seek_benefits(
    num_tables: usize,
    joined_tables: &[JoinedTable],
    constraints: &[TableConstraints],
    base_table_rows: &[RowCountEstimate],
    left_join_deps: &HashMap<usize, TableMask>,
    analyze_stats: &AnalyzeStats,
    params: &CostModelParams,
) -> Result<Vec<f64>> {
    let mut benefits = vec![IndexedSeekBenefit::default(); num_tables];

    let mut total_constant_score = 0.0;
    let mut constant_scores = vec![0.0; num_tables];
    let empty_lhs_mask = TableMask::default();

    for rhs in 0..num_tables {
        let rhs_constraints = &constraints[rhs];
        let rhs_table = &joined_tables[rhs];
        let rhs_base_rows = base_table_rows[rhs];

        if let Some(deps) = left_join_deps.get(&rhs) {
            if deps.count() > 1 {
                continue;
            }
            if let Some(dep_t) = deps.iter().next() {
                let mut lhs_mask = TableMask::default();
                lhs_mask.set(dep_t)?;
                let score = get_best_seek_score(
                    rhs_constraints,
                    &lhs_mask,
                    rhs,
                    rhs_table,
                    rhs_base_rows,
                    analyze_stats,
                    params,
                );
                if score > 0.0 {
                    benefits[dep_t].reward += score;
                    benefits[rhs].penalty += score;
                }
                continue;
            }
        }

        let mut potential_predecessors = TableMask::default();
        for candidate in &rhs_constraints.candidates {
            for cref in &candidate.refs {
                // Only the first index column can be enabled for an index
                // seek by joining a single table first.
                if cref.index_col_pos > 0 {
                    break;
                }
                let c = &rhs_constraints.constraints[cref.constraint_vec_pos];
                if c.lhs_mask.count() != 1 {
                    continue;
                }
                let Some(t) = c.lhs_mask.iter().next() else {
                    continue;
                };
                potential_predecessors.set(t)?;
            }
        }

        let constant_score = get_best_seek_score(
            rhs_constraints,
            &empty_lhs_mask,
            rhs,
            rhs_table,
            rhs_base_rows,
            analyze_stats,
            params,
        );
        if constant_score > 0.0 {
            total_constant_score += constant_score;
            constant_scores[rhs] = constant_score;
            benefits[rhs].penalty += constant_score * (num_tables.saturating_sub(1)) as f64;
        }

        for t in potential_predecessors.iter() {
            if t == rhs {
                continue;
            }
            let mut lhs_mask = TableMask::default();
            lhs_mask.set(t)?;
            let specific_score = get_best_seek_score(
                rhs_constraints,
                &lhs_mask,
                rhs,
                rhs_table,
                rhs_base_rows,
                analyze_stats,
                params,
            );
            let delta = specific_score - constant_score;
            if delta != 0.0 {
                benefits[t].reward += delta;
                benefits[rhs].penalty += delta;
            }
        }
    }

    for start in 0..num_tables {
        benefits[start].reward += total_constant_score - constant_scores[start];
    }

    Ok(benefits.into_iter().map(|b| b.multiplier()).collect())
}

/// Estimate how much `lhs_mask` improves indexed access to `rhs`.
///
/// Higher is better, based on estimated row reduction:
///
/// `ln(base_rows / estimated_rows_per_seek)`
fn get_best_seek_score(
    rhs_constraints: &TableConstraints,
    lhs_mask: &TableMask,
    rhs: usize,
    rhs_table: &JoinedTable,
    base_row_count: RowCountEstimate,
    analyze_stats: &AnalyzeStats,
    params: &CostModelParams,
) -> f64 {
    let mut best_score = 0.0;
    for candidate in &rhs_constraints.candidates {
        let usable_constraint_refs = usable_constraints_for_lhs_mask(
            &rhs_constraints.constraints,
            &candidate.refs,
            lhs_mask,
            rhs,
        );
        if usable_constraint_refs.is_empty() {
            continue;
        }

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
                covering: true,
                column_count: 1,
                rows_per_leaf_page: params.rows_per_table_page,
            },
        };
        let analyze_ctx = AnalyzeCtx {
            rhs_table,
            index: candidate.index.as_ref(),
            stats: analyze_stats,
        };
        let estimated_rows = estimate_rows_per_seek(
            index_info,
            &rhs_constraints.constraints,
            &usable_constraint_refs,
            base_row_count,
            Some(&analyze_ctx),
        )
        .max(1.0);
        let base_rows = (*base_row_count).max(1.0);
        let score = (base_rows / estimated_rows).ln().max(0.0);
        if score > best_score {
            best_score = score;
        }
    }
    best_score
}

/// Specialized version of [compute_best_join_order] that just joins tables in the order they are given
/// in the SQL query. This is used as an upper bound for any other plans -- we can give up enumerating
/// permutations if they exceed this cost during enumeration.
#[allow(clippy::too_many_arguments)]
fn compute_naive_left_deep_plan<'a>(
    joined_tables: &[JoinedTable],
    initial_input_cardinality: f64,
    planning_context: JoinPlanningContext<'_>,
    base_table_rows: &[RowCountEstimate],
    access_methods_arena: &'a mut Vec<AccessMethod>,
    constraints: &'a [TableConstraints],
    where_clause: &mut [WhereTerm],
    where_terms: &[WhereTermInfo],
    subqueries: &[NonFromClauseSubquery],
    index_method_candidates: &[IndexMethodCandidate],
    params: &CostModelParams,
    analyze_stats: &AnalyzeStats,
    available_indexes: &AvailableIndexes,
    table_references: &TableReferences,
    schema: &Schema,
) -> Result<Option<JoinN>> {
    let n = joined_tables.len();
    turso_assert_greater_than!(n, 0);

    let join_order = joined_tables
        .iter()
        .enumerate()
        .map(|(i, t)| JoinOrderMember {
            table_id: t.internal_id,
            original_idx: i,
            is_outer: t.join_info.as_ref().is_some_and(|j| j.is_outer()),
        })
        .collect::<Vec<_>>();

    // Start with first table
    let mut best_plan = join_lhs_and_rhs(
        None,
        initial_input_cardinality,
        &joined_tables[0],
        &constraints[0],
        constraints,
        base_table_rows,
        &join_order[..1],
        planning_context,
        access_methods_arena,
        Cost(f64::MAX),
        joined_tables,
        where_clause,
        where_terms,
        subqueries,
        index_method_candidates,
        params,
        analyze_stats,
        available_indexes,
        table_references,
        schema,
    )?;
    if best_plan.is_none() {
        return Ok(None);
    }

    // Add remaining tables one at a time from left to right
    for i in 1..n {
        best_plan = join_lhs_and_rhs(
            best_plan.as_ref(),
            initial_input_cardinality,
            &joined_tables[i],
            &constraints[i],
            constraints,
            base_table_rows,
            &join_order[..=i],
            planning_context,
            access_methods_arena,
            Cost(f64::MAX),
            joined_tables,
            where_clause,
            where_terms,
            subqueries,
            index_method_candidates,
            params,
            analyze_stats,
            available_indexes,
            table_references,
            schema,
        )?;
        if best_plan.is_none() {
            return Ok(None);
        }
    }

    Ok(best_plan)
}

/// Read the table IDs and extra work for each `WHERE` term once.
fn build_where_term_info(
    where_clause: &[WhereTerm],
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
) -> Result<Vec<WhereTermInfo>> {
    where_clause
        .iter()
        .map(|term| {
            Ok(WhereTermInfo {
                table_mask: table_mask_from_expr(&term.expr, table_references, subqueries)?,
                // FIXME: The row cost also includes one simple condition. Give row work
                // and condition work separate costs so this does not need to subtract one.
                extra_steps: where_expr_steps(&term.expr).saturating_sub(1),
                equal_tables: (!term.consumed)
                    .then(|| tables_in_equal_test(&term.expr))
                    .flatten()
                    .map(|(left, right)| (left, right, term.from_outer_join)),
            })
        })
        .collect()
}

/// Return the extra `WHERE` work that can run after this table.
fn ready_where_work(
    where_clause: &[WhereTerm],
    where_terms: &[WhereTermInfo],
    joined_mask: &TableMask,
    rhs_table_number: usize,
    rhs_table_id: TableInternalId,
) -> SmallVec<[(usize, usize); 4]> {
    where_clause
        .iter()
        .zip(where_terms)
        .enumerate()
        .filter_map(|(term_idx, (term, info))| {
            if term.consumed || info.extra_steps == 0 {
                return None;
            }
            let ready = match term.from_outer_join {
                Some(table_id) => table_id == rhs_table_id,
                None => {
                    info.table_mask.get(rhs_table_number)
                        && joined_mask.contains_all_set_bits_of(&info.table_mask)
                }
            };
            ready.then_some((term_idx, info.extra_steps))
        })
        .collect()
}

/// Iterator that generates all possible size k bitmasks for a given number of tables.
/// For example, given: 3 tables and k=2, the bitmasks are:
/// - 0b011 (tables 0, 1)
/// - 0b101 (tables 0, 2)
/// - 0b110 (tables 1, 2)
///
/// This is used in the dynamic programming approach to finding the best way to join a subset of N tables.
struct JoinBitmaskIter {
    current: u128,
    max_exclusive: u128,
}

impl JoinBitmaskIter {
    fn new(table_number_max_exclusive: usize, how_many: usize) -> Self {
        Self {
            current: (1 << how_many) - 1, // Start with smallest k-bit number (e.g., 000111 for k=3)
            max_exclusive: 1 << table_number_max_exclusive,
        }
    }
}

impl Iterator for JoinBitmaskIter {
    type Item = Result<TableMask, TryReserveError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.max_exclusive {
            return None;
        }

        let result = match TableMask::try_from(self.current) {
            Ok(res) => res,
            Err(e) => return Some(Err(e)),
        };

        // Gosper's hack: compute next k-bit combination in lexicographic order
        let c = self.current & (!self.current + 1); // rightmost set bit
        let r = self.current + c; // add it to get a carry
        let ones = self.current ^ r; // changed bits
        let ones = (ones >> 2) / c; // right-adjust shifted bits
        self.current = r | ones; // form the next combination

        Some(Ok(result))
    }
}

/// Generate all possible bitmasks of size `how_many` for a given number of tables.
fn generate_join_bitmasks(table_number_max_exclusive: usize, how_many: usize) -> JoinBitmaskIter {
    JoinBitmaskIter::new(table_number_max_exclusive, how_many)
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Arc};

    use turso_parser::ast::{self, Expr, Operator, TableInternalId};

    use super::*;
    use crate::alloc::TursoSliceExt;
    use crate::{
        schema::{
            BTreeCharacteristics, BTreeTable, ColDef, Column, Index, IndexColumn, Schema, Table,
            Type,
        },
        stats::AnalyzeStats,
        translate::{
            optimizer::{
                access_method::AccessMethodParams,
                constraints::{constraints_from_where_clause, BinaryExprSide, RangeConstraintRef},
                cost_params::DEFAULT_PARAMS,
            },
            plan::{
                ColumnUsedMask, IterationDirection, JoinInfo, JoinType, Operation, TableReferences,
                WhereTerm,
            },
        },
        vdbe::builder::TableRefIdCounter,
        MAIN_DB_ID,
    };

    fn default_base_rows(n: usize) -> Vec<RowCountEstimate> {
        vec![RowCountEstimate::hardcoded_fallback(&DEFAULT_PARAMS); n]
    }

    fn empty_schema() -> Schema {
        Schema::default()
    }

    fn single_table_plan_cost(where_expr: Expr) -> Cost {
        let table =
            _create_btree_table("test_table", _create_column_list(&["value"], Type::Integer));
        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![_create_table_reference(
            table,
            None,
            table_id_counter.next(),
        )];
        let table_references = TableReferences::new(joined_tables, vec![]);
        let available_indexes = AvailableIndexes::default();
        let mut where_clause = vec![WhereTerm::from(where_expr)];
        let constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();
        let mut access_methods = Vec::new();
        let base_rows = default_base_rows(1);
        let schema = empty_schema();
        compute_best_join_order(
            table_references.joined_tables(),
            1.0,
            None,
            &constraints,
            &base_rows,
            &mut access_methods,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap()
        .unwrap()
        .best_plan
        .cost
    }

    #[test]
    fn automatic_index_puts_equalities_before_ranges() {
        let mut table_id_counter = TableRefIdCounter::new();
        let outer = _create_table_reference(
            _create_btree_table("outer_rows", _create_column_list(&["k"], Type::Integer)),
            None,
            table_id_counter.next(),
        );
        let inner = _create_table_reference(
            _create_btree_table(
                "inner_rows",
                _create_column_list(&["x", "k"], Type::Integer),
            ),
            Some(JoinInfo {
                join_type: JoinType::Inner,
                using: vec![],
                no_reorder: false,
            }),
            table_id_counter.next(),
        );
        let inner_id = inner.internal_id;
        let outer_id = outer.internal_id;
        let table_references = TableReferences::new(vec![outer, inner], vec![]);
        let where_clause = vec![
            _create_binary_expr(
                _create_column_expr(inner_id, 0, false),
                Operator::Greater,
                _create_numeric_literal("10"),
            ),
            _create_binary_expr(
                _create_column_expr(inner_id, 1, false),
                Operator::Equals,
                _create_column_expr(outer_id, 0, false),
            ),
        ];
        let constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &AvailableIndexes::default(),
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let operators: Vec<_> = constraints[1]
            .temporary_index_terms
            .iter()
            .map(|term| {
                constraints[1].constraints[term.constraint_vec_pos]
                    .operator
                    .as_ast_operator()
                    .unwrap()
            })
            .collect();

        assert_eq!(operators, vec![Operator::Equals, Operator::Greater]);
    }

    /// `WHERE` work waits for every table it needs.
    #[test]
    fn where_work_runs_after_the_needed_tables() -> Result<()> {
        let mut table_id_counter = TableRefIdCounter::new();
        let first_id = table_id_counter.next();
        let second_id = table_id_counter.next();
        let joined_tables = vec![
            _create_table_reference(
                _create_btree_table("first", _create_column_list(&["value"], Type::Integer)),
                None,
                first_id,
            ),
            _create_table_reference(
                _create_btree_table("second", _create_column_list(&["value"], Type::Integer)),
                None,
                second_id,
            ),
        ];
        let table_references = TableReferences::new(joined_tables, vec![]);
        let check = |table_id| {
            _create_binary_expr(
                _create_column_expr(table_id, 0, false),
                Operator::Equals,
                _create_numeric_literal("1"),
            )
            .expr
        };
        let two_table_where = vec![WhereTerm::from(Expr::Binary(
            Box::new(check(first_id)),
            Operator::Or,
            Box::new(check(second_id)),
        ))];
        let where_terms = build_where_term_info(&two_table_where, &table_references, &[])?;

        let mut joined_mask = TableMask::default();
        joined_mask.set(0)?;
        assert!(
            ready_where_work(&two_table_where, &where_terms, &joined_mask, 0, first_id).is_empty()
        );

        joined_mask.set(1)?;
        let ready = ready_where_work(&two_table_where, &where_terms, &joined_mask, 1, second_id);
        assert_eq!(ready.as_slice(), &[(0, where_terms[0].extra_steps)]);
        let mut term = WhereTerm::from(Expr::Binary(
            Box::new(check(first_id)),
            Operator::Or,
            Box::new(check(first_id)),
        ));
        term.from_outer_join = Some(second_id);
        let outer_join_where = vec![term];
        let where_terms = build_where_term_info(&outer_join_where, &table_references, &[])?;

        let mut joined_mask = TableMask::default();
        joined_mask.set(0)?;
        assert!(
            ready_where_work(&outer_join_where, &where_terms, &joined_mask, 0, first_id).is_empty()
        );

        joined_mask.set(1)?;
        let ready = ready_where_work(&outer_join_where, &where_terms, &joined_mask, 1, second_id);
        assert_eq!(ready.as_slice(), &[(0, where_terms[0].extra_steps)]);
        Ok(())
    }

    #[test]
    fn connected_component_finishes_before_a_cross_join() -> Result<()> {
        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = (0..4)
            .map(|index| {
                _create_table_reference(
                    _create_btree_table(
                        &format!("table_{index}"),
                        _create_column_list(&["key"], Type::Integer),
                    ),
                    None,
                    table_id_counter.next(),
                )
            })
            .collect::<Vec<_>>();
        let mut where_clause = vec![
            _create_binary_expr(
                _create_column_expr(joined_tables[0].internal_id, 0, false),
                Operator::Equals,
                _create_column_expr(joined_tables[1].internal_id, 0, false),
            ),
            _create_binary_expr(
                _create_column_expr(joined_tables[2].internal_id, 0, false),
                Operator::Equals,
                _create_column_expr(joined_tables[3].internal_id, 0, false),
            ),
        ];
        let table_references = TableReferences::new(joined_tables, vec![]);
        let available_indexes = AvailableIndexes::default();
        let constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )?;
        let base_table_rows =
            [1.0, 1_000_000.0, 10.0, 10.0].map(RowCountEstimate::HardcodedFallback);
        let mut access_methods = Vec::new();
        let schema = empty_schema();
        let plan = compute_best_join_order(
            table_references.joined_tables(),
            1.0,
            None,
            &constraints,
            &base_table_rows,
            &mut access_methods,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )?
        .unwrap()
        .best_plan;
        let order = plan.table_numbers().collect::<Vec<_>>();
        let component = |table| table / 2;

        assert_eq!(component(order[0]), component(order[1]), "order: {order:?}");
        assert_eq!(component(order[2]), component(order[3]), "order: {order:?}");
        assert_ne!(component(order[1]), component(order[2]), "order: {order:?}");
        Ok(())
    }

    #[test]
    fn equality_class_connects_columns_through_a_third_table() -> Result<()> {
        let (table_references, table_ids) =
            equality_test_tables([Type::Integer, Type::Integer, Type::Integer]);
        let mut where_clause = vec![
            _create_binary_expr(
                _create_column_expr(table_ids[0], 0, false),
                Operator::Equals,
                _create_column_expr(table_ids[1], 0, false),
            ),
            _create_binary_expr(
                _create_column_expr(table_ids[1], 0, false),
                Operator::Equals,
                _create_column_expr(table_ids[2], 0, false),
            ),
        ];

        super::super::constraints::add_implied_column_equalities(
            &mut where_clause,
            &table_references,
        )?;

        assert_eq!(where_clause.len(), 3);
        assert!(where_clause[2].consumed);
        assert_eq!(
            table_mask_from_expr(&where_clause[2].expr, &table_references, &[])?,
            TableMask::try_from(0b101_u128)?
        );
        Ok(())
    }

    #[test]
    fn equality_class_does_not_cross_column_affinities() -> Result<()> {
        let (table_references, table_ids) =
            equality_test_tables([Type::Integer, Type::Integer, Type::Text]);
        let mut where_clause = vec![
            _create_binary_expr(
                _create_column_expr(table_ids[0], 0, false),
                Operator::Equals,
                _create_column_expr(table_ids[1], 0, false),
            ),
            _create_binary_expr(
                _create_column_expr(table_ids[1], 0, false),
                Operator::Equals,
                _create_column_expr(table_ids[2], 0, false),
            ),
        ];

        super::super::constraints::add_implied_column_equalities(
            &mut where_clause,
            &table_references,
        )?;

        assert_eq!(where_clause.len(), 2);
        Ok(())
    }

    #[test]
    fn equality_class_does_not_link_rowid_aliases() -> Result<()> {
        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = (0..3)
            .map(|index| {
                _create_table_reference(
                    _create_btree_table(
                        &format!("table_{index}"),
                        vec![_create_column_rowid_alias("id")],
                    ),
                    None,
                    table_id_counter.next(),
                )
            })
            .collect::<Vec<_>>();
        let table_references = TableReferences::new(joined_tables, vec![]);
        let table_ids: [TableInternalId; 3] =
            std::array::from_fn(|index| table_references.joined_tables()[index].internal_id);
        let mut where_clause = vec![
            _create_binary_expr(
                _create_column_expr(table_ids[0], 0, true),
                Operator::Equals,
                _create_column_expr(table_ids[1], 0, true),
            ),
            _create_binary_expr(
                _create_column_expr(table_ids[1], 0, true),
                Operator::Equals,
                _create_column_expr(table_ids[2], 0, true),
            ),
        ];

        super::super::constraints::add_implied_column_equalities(
            &mut where_clause,
            &table_references,
        )?;

        assert_eq!(where_clause.len(), 2);
        Ok(())
    }

    fn equality_test_tables(column_types: [Type; 3]) -> (TableReferences, [TableInternalId; 3]) {
        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = column_types
            .into_iter()
            .enumerate()
            .map(|(index, column_type)| {
                _create_table_reference(
                    _create_btree_table(
                        &format!("table_{index}"),
                        _create_column_list(&["key"], column_type),
                    ),
                    None,
                    table_id_counter.next(),
                )
            })
            .collect::<Vec<_>>();
        let table_references = TableReferences::new(joined_tables, vec![]);
        let table_ids =
            std::array::from_fn(|index| table_references.joined_tables()[index].internal_id);
        (table_references, table_ids)
    }

    #[test]
    fn test_generate_bitmasks() -> std::result::Result<(), TryReserveError> {
        let bitmasks = generate_join_bitmasks(4, 2).collect::<std::result::Result<Vec<_>, _>>()?;
        assert!(bitmasks.contains(&TableMask::try_from(0b0011u128)?)); // {0,1}
        assert!(bitmasks.contains(&TableMask::try_from(0b0101u128)?)); // {0,2}
        assert!(bitmasks.contains(&TableMask::try_from(0b0110u128)?)); // {1,2}
        assert!(bitmasks.contains(&TableMask::try_from(0b1001u128)?)); // {0,3}
        assert!(bitmasks.contains(&TableMask::try_from(0b1010u128)?)); // {1,3}
        assert!(bitmasks.contains(&TableMask::try_from(0b1100u128)?)); // {2,3}
        Ok(())
    }

    #[test]
    fn test_seek_score_accounts_for_composite_index_prefix() {
        let mut table_id_counter = TableRefIdCounter::new();
        let t1 = _create_btree_table("table1", _create_column_list(&["x"], Type::Integer));
        let t2 = _create_btree_table(
            "table2",
            _create_column_list(&["x", "y", "z"], Type::Integer),
        );
        let joined_tables = vec![
            _create_table_reference(t1, None, table_id_counter.next()),
            _create_table_reference(
                t2,
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            ),
        ];

        const TABLE1: usize = 0;
        const TABLE2: usize = 1;

        let where_clause = vec![
            _create_binary_expr(
                _create_column_expr(joined_tables[TABLE2].internal_id, 0, false),
                ast::Operator::Equals,
                _create_column_expr(joined_tables[TABLE1].internal_id, 0, false),
            ),
            _create_binary_expr(
                _create_column_expr(joined_tables[TABLE2].internal_id, 1, false),
                ast::Operator::Equals,
                _create_numeric_literal("1"),
            ),
            _create_binary_expr(
                _create_column_expr(joined_tables[TABLE2].internal_id, 2, false),
                ast::Operator::Equals,
                _create_numeric_literal("2"),
            ),
        ];

        let single_col_index = _create_index("idx_table2_x", "table2", &[("x", 0)], false);
        let composite_index = _create_index(
            "idx_table2_xyz",
            "table2",
            &[("x", 0), ("y", 1), ("z", 2)],
            false,
        );

        let single_col_score = seek_score_for_indexes(
            &joined_tables,
            &where_clause,
            VecDeque::from([single_col_index]),
        );
        let composite_score = seek_score_for_indexes(
            &joined_tables,
            &where_clause,
            VecDeque::from([composite_index]),
        );

        assert!(composite_score > single_col_score);
    }

    #[test]
    fn plan_cost_counts_long_where_condition() {
        let table_id = TableInternalId::default();
        let check = |value| {
            Expr::Binary(
                Box::new(_create_column_expr(table_id, 0, false)),
                Operator::Equals,
                Box::new(_create_numeric_literal(value)),
            )
        };
        let simple_cost = single_table_plan_cost(check("1"));
        let long_cost = single_table_plan_cost(Expr::Binary(
            Box::new(check("1")),
            Operator::Or,
            Box::new(check("2")),
        ));

        assert!(
            long_cost > simple_cost,
            "simple cost: {simple_cost:?}, long cost: {long_cost:?}"
        );
    }

    #[test]
    /// Test that [compute_best_join_order] returns None when there are no table references.
    fn test_compute_best_join_order_empty() {
        let table_references = TableReferences::new(vec![], vec![]);
        let available_indexes = AvailableIndexes::default();
        let mut where_clause = vec![];

        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let result = compute_best_join_order(
            table_references.joined_tables(),
            1.0,
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    /// Test that [compute_best_join_order] returns a table scan access method when the where clause is empty.
    fn test_compute_best_join_order_single_table_no_indexes() {
        let t1 = _create_btree_table("test_table", _create_column_list(&["id"], Type::Integer));
        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![_create_table_reference(t1, None, table_id_counter.next())];
        let table_references = TableReferences::new(joined_tables, vec![]);
        let available_indexes = AvailableIndexes::default();
        let mut where_clause = vec![];

        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        // SELECT * from test_table
        // expecting best_best_plan() not to do any work due to empty where clause.
        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            1.0,
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap()
        .unwrap();
        // Should just be a table scan access method
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (iter_dir, _, constraint_refs) = _as_btree(access_method);
        assert!(constraint_refs.is_empty());
        assert!(iter_dir == IterationDirection::Forwards);
    }

    #[test]
    /// Test that [compute_best_join_order] returns a RowidEq access method when the where clause has an EQ constraint on the rowid alias.
    fn test_compute_best_join_order_single_table_rowid_eq() {
        let t1 = _create_btree_table("test_table", vec![_create_column_rowid_alias("id")]);
        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![_create_table_reference(t1, None, table_id_counter.next())];

        let mut where_clause = vec![_create_binary_expr(
            _create_column_expr(joined_tables[0].internal_id, 0, true), // table 0, column 0 (rowid)
            ast::Operator::Equals,
            _create_numeric_literal("42"),
        )];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let available_indexes = AvailableIndexes::default();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        // SELECT * FROM test_table WHERE id = 42
        // expecting a RowidEq access method because id is a rowid alias.
        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let result = compute_best_join_order(
            table_references.joined_tables(),
            1.0,
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();
        assert_eq!(best_plan.table_numbers().collect::<Vec<_>>(), vec![0]);
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (iter_dir, _, constraint_refs) = _as_btree(access_method);
        assert!(!constraint_refs.is_empty());
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(constraint_refs.len() == 1);
        assert!(
            table_constraints[0].constraints
                [constraint_refs[0].eq.as_ref().unwrap().constraint_pos]
                .where_clause_pos
                == (0, BinaryExprSide::Rhs)
        );
    }

    #[test]
    /// Test that [compute_best_join_order] returns an IndexScan access method when the where clause has an EQ constraint on a primary key.
    fn test_compute_best_join_order_single_table_pk_eq() {
        let t1 = _create_btree_table(
            "test_table",
            vec![_create_column_of_type("id", Type::Integer)],
        );
        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![_create_table_reference(t1, None, table_id_counter.next())];

        let mut where_clause = vec![_create_binary_expr(
            _create_column_expr(joined_tables[0].internal_id, 0, false), // table 0, column 0 (id)
            ast::Operator::Equals,
            _create_numeric_literal("42"),
        )];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let mut available_indexes = AvailableIndexes::default();
        let index = Arc::new(Index {
            name: "sqlite_autoindex_test_table_1".to_string(),
            table_name: "test_table".to_string(),
            where_clause: None,
            columns: crate::alloc::vec![IndexColumn::new("id", 0)],
            unique: true,
            ephemeral: false,
            root_page: 1,
            has_rowid: true,
            index_method: None,
            on_conflict: None,
        });
        available_indexes.insert_for_table_name(
            table_references.joined_tables(),
            "test_table",
            VecDeque::from([index]),
        );

        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();
        // SELECT * FROM test_table WHERE id = 42
        // expecting an IndexScan access method because id is a primary key with an index
        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let result = compute_best_join_order(
            table_references.joined_tables(),
            1.0,
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();
        assert_eq!(best_plan.table_numbers().collect::<Vec<_>>(), vec![0]);
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(!constraint_refs.is_empty());
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.as_ref().unwrap().name == "sqlite_autoindex_test_table_1");
        assert!(constraint_refs.len() == 1);
        assert!(
            table_constraints[0].constraints
                [constraint_refs[0].eq.as_ref().unwrap().constraint_pos]
                .where_clause_pos
                == (0, BinaryExprSide::Rhs)
        );
    }

    #[test]
    /// Test that [compute_best_join_order] moves the outer table to the inner position when an index can be used on it, but not the original inner table.
    fn test_compute_best_join_order_two_tables() {
        let t1 = _create_btree_table("table1", _create_column_list(&["id"], Type::Integer));
        let t2 = _create_btree_table("table2", _create_column_list(&["id"], Type::Integer));

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![
            _create_table_reference(t1, None, table_id_counter.next()),
            _create_table_reference(
                t2,
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            ),
        ];

        const TABLE1: usize = 0;
        const TABLE2: usize = 1;

        let mut available_indexes = AvailableIndexes::default();
        // Index on the outer table (table1)
        let index1 = Arc::new(Index {
            name: "index1".to_string(),
            table_name: "table1".to_string(),
            where_clause: None,
            columns: crate::alloc::vec![IndexColumn::new("id", 0)],
            unique: true,
            ephemeral: false,
            root_page: 1,
            has_rowid: true,
            index_method: None,
            on_conflict: None,
        });
        available_indexes.insert_for_table_name(&joined_tables, "table1", VecDeque::from([index1]));

        // SELECT * FROM table1 JOIN table2 WHERE table1.id = table2.id
        // expecting table2 to be chosen first due to the index on table1.id
        let mut where_clause = vec![_create_binary_expr(
            _create_column_expr(joined_tables[TABLE1].internal_id, 0, false), // table1.id
            ast::Operator::Equals,
            _create_column_expr(joined_tables[TABLE2].internal_id, 0, false), // table2.id
        )];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let result = compute_best_join_order(
            table_references.joined_tables(),
            1.0,
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();
        assert_eq!(best_plan.table_numbers().collect::<Vec<_>>(), vec![1, 0]);
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (iter_dir, _, constraint_refs) = _as_btree(access_method);
        assert!(constraint_refs.is_empty());
        assert!(iter_dir == IterationDirection::Forwards);
        let access_method = &access_methods_arena[best_plan.data[1].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(!constraint_refs.is_empty());
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.as_ref().unwrap().name == "index1");
        assert!(constraint_refs.len() == 1);
        assert!(
            table_constraints[TABLE1].constraints
                [constraint_refs[0].eq.as_ref().unwrap().constraint_pos]
                .where_clause_pos
                == (0, BinaryExprSide::Rhs)
        );
    }

    #[test]
    /// Test that [compute_best_join_order] returns a sensible order and plan for three tables, each with indexes.
    fn test_compute_best_join_order_three_tables_indexed() {
        let table_orders = _create_btree_table(
            "orders",
            vec![
                _create_column_of_type("id", Type::Integer),
                _create_column_of_type("customer_id", Type::Integer),
                _create_column_of_type("total", Type::Integer),
            ],
        );
        let table_customers = _create_btree_table(
            "customers",
            vec![
                _create_column_of_type("id", Type::Integer),
                _create_column_of_type("name", Type::Integer),
            ],
        );
        let table_order_items = _create_btree_table(
            "order_items",
            vec![
                _create_column_of_type("id", Type::Integer),
                _create_column_of_type("order_id", Type::Integer),
                _create_column_of_type("product_id", Type::Integer),
                _create_column_of_type("quantity", Type::Integer),
            ],
        );

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![
            _create_table_reference(table_orders, None, table_id_counter.next()),
            _create_table_reference(
                table_customers,
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            ),
            _create_table_reference(
                table_order_items,
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            ),
        ];

        const TABLE_NO_ORDERS: usize = 0;
        const TABLE_NO_CUSTOMERS: usize = 1;
        const TABLE_NO_ORDER_ITEMS: usize = 2;

        let mut available_indexes = AvailableIndexes::default();
        ["orders", "customers", "order_items"]
            .iter()
            .for_each(|table_name| {
                // add primary key index called sqlite_autoindex_<tablename>_1
                let index_name = format!("sqlite_autoindex_{table_name}_1");
                let index = Arc::new(Index {
                    name: index_name,
                    where_clause: None,
                    table_name: table_name.to_string(),
                    columns: crate::alloc::vec![IndexColumn::new("id", 0)],
                    unique: true,
                    ephemeral: false,
                    root_page: 1,
                    has_rowid: true,
                    index_method: None,
                    on_conflict: None,
                });
                available_indexes.insert_for_table_name(
                    &joined_tables,
                    table_name,
                    VecDeque::from([index]),
                );
            });
        let customer_id_idx = Arc::new(Index {
            name: "orders_customer_id_idx".to_string(),
            table_name: "orders".to_string(),
            where_clause: None,
            columns: crate::alloc::vec![IndexColumn::new("customer_id", 1)],
            unique: false,
            ephemeral: false,
            root_page: 1,
            has_rowid: true,
            index_method: None,
            on_conflict: None,
        });
        let order_id_idx = Arc::new(Index {
            name: "order_items_order_id_idx".to_string(),
            table_name: "order_items".to_string(),
            where_clause: None,
            columns: crate::alloc::vec![IndexColumn::new("order_id", 1)],
            unique: false,
            ephemeral: false,
            root_page: 1,
            has_rowid: true,
            index_method: None,
            on_conflict: None,
        });

        available_indexes.push_front_for_table_name(&joined_tables, "orders", customer_id_idx);
        available_indexes.push_front_for_table_name(&joined_tables, "order_items", order_id_idx);

        // SELECT * FROM orders JOIN customers JOIN order_items
        // WHERE orders.customer_id = customers.id AND orders.id = order_items.order_id AND customers.id = 42
        // expecting customers to be chosen first due to the index on customers.id and it having a selective filter (=42)
        // then orders to be chosen next due to the index on orders.customer_id
        // then order_items to be chosen last due to the index on order_items.order_id
        let mut where_clause = vec![
            // orders.customer_id = customers.id
            _create_binary_expr(
                _create_column_expr(joined_tables[TABLE_NO_ORDERS].internal_id, 1, false), // orders.customer_id
                ast::Operator::Equals,
                _create_column_expr(joined_tables[TABLE_NO_CUSTOMERS].internal_id, 0, false), // customers.id
            ),
            // orders.id = order_items.order_id
            _create_binary_expr(
                _create_column_expr(joined_tables[TABLE_NO_ORDERS].internal_id, 0, false), // orders.id
                ast::Operator::Equals,
                _create_column_expr(joined_tables[TABLE_NO_ORDER_ITEMS].internal_id, 1, false), // order_items.order_id
            ),
            // customers.id = 42
            _create_binary_expr(
                _create_column_expr(joined_tables[TABLE_NO_CUSTOMERS].internal_id, 0, false), // customers.id
                ast::Operator::Equals,
                _create_numeric_literal("42"),
            ),
        ];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let result = compute_best_join_order(
            table_references.joined_tables(),
            1.0,
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();

        // Customers (due to =42 filter) -> Orders (due to index on customer_id) -> Order_items (due to index on order_id)
        assert_eq!(
            best_plan.table_numbers().collect::<Vec<_>>(),
            vec![TABLE_NO_CUSTOMERS, TABLE_NO_ORDERS, TABLE_NO_ORDER_ITEMS]
        );

        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.as_ref().unwrap().name == "sqlite_autoindex_customers_1");
        assert!(constraint_refs.len() == 1);
        let constraint = &table_constraints[TABLE_NO_CUSTOMERS].constraints
            [constraint_refs[0].eq.as_ref().unwrap().constraint_pos];
        assert!(constraint.lhs_mask.is_empty());

        let access_method = &access_methods_arena[best_plan.data[1].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.as_ref().unwrap().name == "orders_customer_id_idx");
        assert!(constraint_refs.len() == 1);
        let constraint = &table_constraints[TABLE_NO_ORDERS].constraints
            [constraint_refs[0].eq.as_ref().unwrap().constraint_pos];
        assert!(constraint.lhs_mask.get(TABLE_NO_CUSTOMERS));

        let access_method = &access_methods_arena[best_plan.data[2].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.as_ref().unwrap().name == "order_items_order_id_idx");
        assert!(constraint_refs.len() == 1);
        let constraint = &table_constraints[TABLE_NO_ORDER_ITEMS].constraints
            [constraint_refs[0].eq.as_ref().unwrap().constraint_pos];
        assert!(constraint.lhs_mask.get(TABLE_NO_ORDERS));
    }

    struct TestColumn {
        name: String,
        ty: Type,
        is_rowid_alias: bool,
    }

    impl Default for TestColumn {
        fn default() -> Self {
            Self {
                name: "a".to_string(),
                ty: Type::Integer,
                is_rowid_alias: false,
            }
        }
    }

    #[test]
    fn test_join_order_three_tables_no_indexes() {
        let t1 = _create_btree_table("t1", _create_column_list(&["id", "foo"], Type::Integer));
        let t2 = _create_btree_table("t2", _create_column_list(&["id", "foo"], Type::Integer));
        let t3 = _create_btree_table("t3", _create_column_list(&["id", "foo"], Type::Integer));

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![
            _create_table_reference(t1, None, table_id_counter.next()),
            _create_table_reference(
                t2,
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            ),
            _create_table_reference(
                t3,
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            ),
        ];

        let mut where_clause = vec![
            // t2.foo = 42 (equality filter, more selective)
            _create_binary_expr(
                _create_column_expr(joined_tables[1].internal_id, 1, false), // table 1, column 1 (foo)
                ast::Operator::Equals,
                _create_numeric_literal("42"),
            ),
            // t1.foo > 10 (inequality filter, less selective)
            _create_binary_expr(
                _create_column_expr(joined_tables[0].internal_id, 1, false), // table 0, column 1 (foo)
                ast::Operator::Greater,
                _create_numeric_literal("10"),
            ),
        ];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let available_indexes = AvailableIndexes::default();
        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            1.0,
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap()
        .unwrap();

        // Put the table with no filter first. The two inner tables can then
        // build an automatic index once instead of scanning once per outer row.
        assert_eq!(best_plan.table_numbers().collect::<Vec<_>>(), vec![2, 1, 0]);

        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(constraint_refs.is_empty());
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.is_none());

        let access_method = &access_methods_arena[best_plan.data[1].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(constraint_refs.is_empty());
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.is_none());
        assert!(matches!(
            access_method.params,
            AccessMethodParams::BTreeTable {
                build_index: true,
                ..
            }
        ));

        let access_method = &access_methods_arena[best_plan.data[2].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(constraint_refs.is_empty());
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.is_none());
        assert!(matches!(
            access_method.params,
            AccessMethodParams::BTreeTable {
                build_index: true,
                ..
            }
        ));
    }

    #[test]
    /// Test that [compute_best_join_order] chooses a "fact table" as the outer table,
    /// when it has a foreign key to all dimension tables.
    fn test_compute_best_join_order_star_schema() {
        const NUM_DIM_TABLES: usize = 9;
        const FACT_TABLE_IDX: usize = 9;

        // Create fact table with foreign keys to all dimension tables
        let mut fact_columns = vec![_create_column_rowid_alias("id")];
        for i in 0..NUM_DIM_TABLES {
            fact_columns.push(_create_column_of_type(&format!("dim{i}_id"), Type::Integer));
        }
        let fact_table = _create_btree_table("fact", fact_columns);

        // Create dimension tables, each with an id and value column
        let dim_tables: Vec<_> = (0..NUM_DIM_TABLES)
            .map(|i| {
                _create_btree_table(
                    &format!("dim{i}"),
                    vec![
                        _create_column_rowid_alias("id"),
                        _create_column_of_type("value", Type::Integer),
                    ],
                )
            })
            .collect();

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = {
            let mut refs = vec![_create_table_reference(
                dim_tables[0].clone(),
                None,
                table_id_counter.next(),
            )];
            refs.extend(dim_tables.iter().skip(1).map(|t| {
                _create_table_reference(
                    t.clone(),
                    Some(JoinInfo {
                        join_type: JoinType::Inner,
                        using: vec![],
                        no_reorder: false,
                    }),
                    table_id_counter.next(),
                )
            }));
            refs.push(_create_table_reference(
                fact_table,
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            ));
            refs
        };

        let mut where_clause = vec![];

        // Add join conditions between fact and each dimension table
        for i in 0..NUM_DIM_TABLES {
            let internal_id_fact = joined_tables[FACT_TABLE_IDX].internal_id;
            let internal_id_other = joined_tables[i].internal_id;
            where_clause.push(_create_binary_expr(
                _create_column_expr(internal_id_fact, i + 1, false), // fact.dimX_id
                ast::Operator::Equals,
                _create_column_expr(internal_id_other, 0, true), // dimX.id
            ));
        }

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let available_indexes = AvailableIndexes::default();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let result = compute_best_join_order(
            table_references.joined_tables(),
            1.0,
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();

        // Expected optimal order: fact table as outer, with rowid seeks in any order on each dimension table
        // Verify fact table is selected as the outer table as all the other tables can use SeekRowid
        assert_eq!(
            best_plan.table_numbers().next().unwrap(),
            FACT_TABLE_IDX,
            "First table should be fact (table {}) due to available index, got table {} instead",
            FACT_TABLE_IDX,
            best_plan.table_numbers().next().unwrap()
        );

        // Verify access methods
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.is_none());
        assert!(constraint_refs.is_empty());

        for (table_number, access_method_index) in best_plan.data.iter().skip(1) {
            let access_method = &access_methods_arena[*access_method_index];
            let (iter_dir, index, constraint_refs) = _as_btree(access_method);
            assert!(iter_dir == IterationDirection::Forwards);
            assert!(index.is_none());
            assert!(constraint_refs.len() == 1);
            let constraint = &table_constraints[*table_number].constraints
                [constraint_refs[0].eq.as_ref().unwrap().constraint_pos];
            assert!(constraint.lhs_mask.get(FACT_TABLE_IDX));
            assert!(constraint.operator.as_ast_operator() == Some(ast::Operator::Equals));
        }
    }

    #[test]
    /// Test that [compute_best_join_order] figures out that the tables form a "linked list" pattern
    /// where a column in each table points to an indexed column in the next table,
    /// and chooses the best order based on that.
    fn test_compute_best_join_order_linked_list() {
        const NUM_TABLES: usize = 5;

        // Create tables t1 -> t2 -> t3 -> t4 -> t5 where there is a foreign key from each table to the next
        let mut tables = Vec::with_capacity(NUM_TABLES);
        for i in 0..NUM_TABLES {
            let mut columns = vec![_create_column_rowid_alias("id")];
            if i < NUM_TABLES - 1 {
                columns.push(_create_column_of_type("next_id", Type::Integer));
            }
            tables.push(_create_btree_table(&format!("t{}", i + 1), columns));
        }

        let available_indexes = AvailableIndexes::default();

        let mut table_id_counter = TableRefIdCounter::new();
        // Create table references
        let joined_tables: Vec<_> = tables
            .iter()
            .map(|t| _create_table_reference(t.clone(), None, table_id_counter.next()))
            .collect();

        // Create where clause linking each table to the next
        let mut where_clause = Vec::new();
        for i in 0..NUM_TABLES - 1 {
            let internal_id_left = joined_tables[i].internal_id;
            let internal_id_right = joined_tables[i + 1].internal_id;
            where_clause.push(_create_binary_expr(
                _create_column_expr(internal_id_left, 1, false), // ti.next_id
                ast::Operator::Equals,
                _create_column_expr(internal_id_right, 0, true), // t(i+1).id
            ));
        }

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        // Run the optimizer
        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            1.0,
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap()
        .unwrap();

        // Verify the join order is exactly t1 -> t2 -> t3 -> t4 -> t5
        for i in 0..NUM_TABLES {
            assert_eq!(
                best_plan.table_numbers().nth(i).unwrap(),
                i,
                "Expected table {} at position {}, got table {} instead",
                i,
                i,
                best_plan.table_numbers().nth(i).unwrap()
            );
        }

        // Verify access methods:
        // - First table should use Table scan
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (iter_dir, index, constraint_refs) = _as_btree(access_method);
        assert!(iter_dir == IterationDirection::Forwards);
        assert!(index.is_none());
        assert!(constraint_refs.is_empty());

        // all of the rest should use rowid equality
        for (i, table_constraints) in table_constraints
            .iter()
            .enumerate()
            .take(NUM_TABLES)
            .skip(1)
        {
            let access_method = &access_methods_arena[best_plan.data[i].1];
            let (iter_dir, index, constraint_refs) = _as_btree(access_method);
            assert!(iter_dir == IterationDirection::Forwards);
            assert!(index.is_none());
            assert!(constraint_refs.len() == 1);
            let constraint = &table_constraints.constraints
                [constraint_refs[0].eq.as_ref().unwrap().constraint_pos];
            assert!(constraint.lhs_mask.get(i - 1));
            assert!(constraint.operator.as_ast_operator() == Some(ast::Operator::Equals));
        }
    }

    #[test]
    /// Test that [compute_best_join_order] figures out that the index can't be used when only the second column is referenced
    fn test_index_second_column_only() {
        let mut joined_tables = Vec::new();

        let mut table_id_counter = TableRefIdCounter::new();

        // Create a table with two columns
        let table = _create_btree_table("t1", _create_column_list(&["x", "y"], Type::Integer));

        // Create a two-column index on (x,y)
        let index = Arc::new(Index {
            name: "idx_xy".to_string(),
            table_name: "t1".to_string(),
            where_clause: None,
            columns: crate::alloc::vec![IndexColumn::new("x", 0), IndexColumn::new("y", 1),],
            unique: false,
            root_page: 2,
            ephemeral: false,
            has_rowid: true,
            index_method: None,
            on_conflict: None,
        });

        let mut available_indexes = AvailableIndexes::default();

        let table = Table::BTree(table);
        joined_tables.push(JoinedTable {
            op: Operation::default_scan_for(&table),
            table,
            internal_id: table_id_counter.next(),
            identifier: "t1".to_string(),
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: MAIN_DB_ID,
            plan_estimate: None,
            indexed: None,
        });
        available_indexes.insert_for_table_name(&joined_tables, "t1", VecDeque::from([index]));

        // Create where clause that only references second column
        let mut where_clause = vec![WhereTerm {
            expr: Expr::Binary(
                Box::new(Expr::Column {
                    database: None,
                    table: joined_tables[0].internal_id,
                    column: 1,
                    is_rowid_alias: false,
                }),
                ast::Operator::Equals,
                Box::new(Expr::Literal(ast::Literal::Numeric(5.to_string()))),
            ),
            from_outer_join: None,
            consumed: false,
        }];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            1.0,
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap()
        .unwrap();

        // Verify access method is a scan, not a seek, because the index can't be used when only the second column is referenced
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (_, _, constraint_refs) = _as_btree(access_method);
        assert!(constraint_refs.is_empty());
    }

    #[test]
    /// Test that an index with a gap in referenced columns (e.g. index on (a,b,c), where clause on a and c)
    /// only uses the prefix before the gap.
    fn test_index_skips_middle_column() {
        let mut table_id_counter = TableRefIdCounter::new();
        let mut joined_tables = Vec::new();
        let mut available_indexes = AvailableIndexes::default();

        let columns = _create_column_list(&["c1", "c2", "c3"], Type::Integer);
        let table = _create_btree_table("t1", columns);
        let index = Arc::new(Index {
            name: "idx1".to_string(),
            table_name: "t1".to_string(),
            where_clause: None,
            columns: crate::alloc::vec![
                IndexColumn::new("c1", 0),
                IndexColumn::new("c2", 1),
                IndexColumn::new("c3", 2),
            ],
            unique: false,
            root_page: 2,
            ephemeral: false,
            has_rowid: true,
            index_method: None,
            on_conflict: None,
        });
        let table = Table::BTree(table);
        joined_tables.push(JoinedTable {
            op: Operation::default_scan_for(&table),
            table,
            internal_id: table_id_counter.next(),
            identifier: "t1".to_string(),
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: MAIN_DB_ID,
            plan_estimate: None,
            indexed: None,
        });
        available_indexes.insert_for_table_name(&joined_tables, "t1", VecDeque::from([index]));

        // Create where clause that references first and third columns
        let mut where_clause = vec![
            WhereTerm {
                expr: Expr::Binary(
                    Box::new(Expr::Column {
                        database: None,
                        table: joined_tables[0].internal_id,
                        column: 0, // c1
                        is_rowid_alias: false,
                    }),
                    ast::Operator::Equals,
                    Box::new(Expr::Literal(ast::Literal::Numeric(5.to_string()))),
                ),
                from_outer_join: None,
                consumed: false,
            },
            WhereTerm {
                expr: Expr::Binary(
                    Box::new(Expr::Column {
                        database: None,
                        table: joined_tables[0].internal_id,
                        column: 2, // c3
                        is_rowid_alias: false,
                    }),
                    ast::Operator::Equals,
                    Box::new(Expr::Literal(ast::Literal::Numeric(7.to_string()))),
                ),
                from_outer_join: None,
                consumed: false,
            },
        ];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            1.0,
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap()
        .unwrap();

        // Verify access method is a seek, and only uses the first column of the index
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (_, index, constraint_refs) = _as_btree(access_method);
        assert!(index.as_ref().is_some_and(|i| i.name == "idx1"));
        assert!(constraint_refs.len() == 1);
        let constraint = &table_constraints[0].constraints
            [constraint_refs[0].eq.as_ref().unwrap().constraint_pos];
        assert!(constraint.operator.as_ast_operator() == Some(ast::Operator::Equals));
        assert!(constraint.table_col_pos == Some(0)); // c1
    }

    #[test]
    /// Test that an index seek stops after a range operator.
    /// e.g. index on (a,b,c), where clause a=1, b>2, c=3. Only a and b should be used for seek.
    fn test_index_stops_at_range_operator() {
        let mut table_id_counter = TableRefIdCounter::new();
        let mut joined_tables = Vec::new();
        let mut available_indexes = AvailableIndexes::default();

        let columns = _create_column_list(&["c1", "c2", "c3"], Type::Integer);
        let table = _create_btree_table("t1", columns);
        let index = Arc::new(Index {
            name: "idx1".to_string(),
            table_name: "t1".to_string(),
            where_clause: None,
            columns: IndexColumn::new_many(vec!["c1", "c2", "c3"]),
            root_page: 2,
            ephemeral: false,
            has_rowid: true,
            unique: false,
            index_method: None,
            on_conflict: None,
        });
        let table = Table::BTree(table);
        joined_tables.push(JoinedTable {
            op: Operation::default_scan_for(&table),
            table,
            internal_id: table_id_counter.next(),
            identifier: "t1".to_string(),
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: MAIN_DB_ID,
            plan_estimate: None,
            indexed: None,
        });
        available_indexes.insert_for_table_name(&joined_tables, "t1", VecDeque::from([index]));

        // Create where clause: c1 = 5 AND c2 > 10 AND c3 = 7
        let mut where_clause = vec![
            WhereTerm {
                expr: Expr::Binary(
                    Box::new(Expr::Column {
                        database: None,
                        table: joined_tables[0].internal_id,
                        column: 0, // c1
                        is_rowid_alias: false,
                    }),
                    ast::Operator::Equals,
                    Box::new(Expr::Literal(ast::Literal::Numeric(5.to_string()))),
                ),
                from_outer_join: None,
                consumed: false,
            },
            WhereTerm {
                expr: Expr::Binary(
                    Box::new(Expr::Column {
                        database: None,
                        table: joined_tables[0].internal_id,
                        column: 1, // c2
                        is_rowid_alias: false,
                    }),
                    ast::Operator::Greater,
                    Box::new(Expr::Literal(ast::Literal::Numeric(10.to_string()))),
                ),
                from_outer_join: None,
                consumed: false,
            },
            WhereTerm {
                expr: Expr::Binary(
                    Box::new(Expr::Column {
                        database: None,
                        table: joined_tables[0].internal_id,
                        column: 2, // c3
                        is_rowid_alias: false,
                    }),
                    ast::Operator::Equals,
                    Box::new(Expr::Literal(ast::Literal::Numeric(7.to_string()))),
                ),
                from_outer_join: None,
                consumed: false,
            },
        ];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let BestJoinOrderResult { best_plan, .. } = compute_best_join_order(
            table_references.joined_tables(),
            1.0,
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap()
        .unwrap();

        // Verify access method is a seek, and uses the first two columns of the index.
        // The third column can't be used because the second is a range query.
        let access_method = &access_methods_arena[best_plan.data[0].1];
        let (_, index, constraint_refs) = _as_btree(access_method);
        assert!(index.as_ref().is_some_and(|i| i.name == "idx1"));
        assert!(constraint_refs.len() == 2);
        let constraint = &table_constraints[0].constraints
            [constraint_refs[0].eq.as_ref().unwrap().constraint_pos];
        assert!(constraint.operator.as_ast_operator() == Some(ast::Operator::Equals));
        assert!(constraint.table_col_pos == Some(0)); // c1
        let constraint = &table_constraints[0].constraints[constraint_refs[1].lower_bound.unwrap()];
        assert!(constraint.operator.as_ast_operator() == Some(ast::Operator::Greater));
        assert!(constraint.table_col_pos == Some(1)); // c2
    }

    fn _create_column(c: &TestColumn) -> Column {
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
    fn _create_column_of_type(name: &str, ty: Type) -> Column {
        _create_column(&TestColumn {
            name: name.to_string(),
            ty,
            is_rowid_alias: false,
        })
    }

    fn _create_column_list(names: &[&str], ty: Type) -> Vec<Column> {
        names
            .iter()
            .map(|name| _create_column_of_type(name, ty))
            .collect()
    }

    fn _create_column_rowid_alias(name: &str) -> Column {
        _create_column(&TestColumn {
            name: name.to_string(),
            ty: Type::Integer,
            is_rowid_alias: true,
        })
    }

    /// Creates a BTreeTable with the given name and columns
    fn _create_btree_table(name: &str, columns: Vec<Column>) -> Arc<BTreeTable> {
        Arc::new(BTreeTable::new(
            1, // root_page, doesn't matter for tests
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

    fn _create_index(
        name: &str,
        table_name: &str,
        columns: &[(&str, usize)],
        unique: bool,
    ) -> Arc<Index> {
        Arc::new(Index {
            name: name.to_string(),
            table_name: table_name.to_string(),
            where_clause: None,
            columns: columns
                .iter()
                .map(|(name, pos_in_table)| IndexColumn::new((*name).to_string(), *pos_in_table))
                .try_collect()
                .unwrap(),
            unique,
            ephemeral: false,
            root_page: 1,
            has_rowid: true,
            index_method: None,
            on_conflict: None,
        })
    }

    /// Creates a TableReference for a BTreeTable
    fn _create_table_reference(
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

    /// Creates a column expression
    fn _create_column_expr(table: TableInternalId, column: usize, is_rowid_alias: bool) -> Expr {
        Expr::Column {
            database: None,
            table,
            column,
            is_rowid_alias,
        }
    }

    /// Creates a binary expression for a WHERE clause
    fn _create_binary_expr(lhs: Expr, op: Operator, rhs: Expr) -> WhereTerm {
        WhereTerm {
            expr: Expr::Binary(Box::new(lhs), op, Box::new(rhs)),
            from_outer_join: None,
            consumed: false,
        }
    }

    /// Creates a numeric literal expression
    fn _create_numeric_literal(value: &str) -> Expr {
        Expr::Literal(ast::Literal::Numeric(value.to_string()))
    }

    fn seek_score_for_indexes(
        joined_tables: &[JoinedTable],
        where_clause: &[WhereTerm],
        indexes: VecDeque<Arc<Index>>,
    ) -> f64 {
        let mut available_indexes = AvailableIndexes::default();
        available_indexes.insert_for_table_name(joined_tables, "table2", indexes);
        let table_references = TableReferences::new(joined_tables.to_vec(), vec![]);
        let constraints = constraints_from_where_clause(
            where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let mut lhs_mask = TableMask::default();
        lhs_mask.set(0).unwrap();
        get_best_seek_score(
            &constraints[1],
            &lhs_mask,
            1,
            &joined_tables[1],
            RowCountEstimate::hardcoded_fallback(&DEFAULT_PARAMS),
            &AnalyzeStats::default(),
            &DEFAULT_PARAMS,
        )
    }

    fn _as_btree(
        access_method: &AccessMethod,
    ) -> (
        IterationDirection,
        Option<Arc<Index>>,
        &'_ [RangeConstraintRef],
    ) {
        match &access_method.params {
            AccessMethodParams::BTreeTable {
                iter_dir,
                index,
                constraint_refs,
                ..
            } => (*iter_dir, index.clone(), constraint_refs),
            _ => panic!("expected BTreeTable access method"),
        }
    }

    #[test]
    /// Test that when an index is available on the join column, the optimizer prefers
    /// index lookup over hash join.
    fn test_prefer_index_lookup_over_hash_join() {
        // CREATE TABLE t1(a,b,c);
        // CREATE TABLE t2(a,b,c);
        // CREATE INDEX idx_t2_a ON t2(a);
        // SELECT * FROM t1 JOIN t2 ON t1.a = t2.a;
        // Expected: SCAN t1, SEARCH t2 USING INDEX idx_t2_a (a=?)
        // Not: HASH JOIN

        let t1 = _create_btree_table("t1", _create_column_list(&["a", "b", "c"], Type::Integer));
        let t2 = _create_btree_table("t2", _create_column_list(&["a", "b", "c"], Type::Integer));

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![
            _create_table_reference(t1, None, table_id_counter.next()),
            _create_table_reference(
                t2,
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            ),
        ];

        const TABLE1: usize = 0;
        const TABLE2: usize = 1;

        // Index on t2.a
        let mut available_indexes = AvailableIndexes::default();
        let index_t2_a = Arc::new(Index {
            name: "idx_t2_a".to_string(),
            table_name: "t2".to_string(),
            where_clause: None,
            columns: crate::alloc::vec![IndexColumn::new("a", 0)],
            unique: false, // Non-unique index
            ephemeral: false,
            root_page: 2,
            has_rowid: true,
            index_method: None,
            on_conflict: None,
        });
        available_indexes.insert_for_table_name(&joined_tables, "t2", VecDeque::from([index_t2_a]));

        // WHERE t1.a = t2.a
        let mut where_clause = vec![_create_binary_expr(
            _create_column_expr(joined_tables[TABLE1].internal_id, 0, false), // t1.a
            ast::Operator::Equals,
            _create_column_expr(joined_tables[TABLE2].internal_id, 0, false), // t2.a
        )];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let mut access_methods_arena = Vec::new();
        let table_constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();

        let base_table_rows = default_base_rows(table_references.joined_tables().len());
        let schema = empty_schema();
        let result = compute_best_join_order(
            table_references.joined_tables(),
            1.0,
            None,
            &table_constraints,
            &base_table_rows,
            &mut access_methods_arena,
            &mut where_clause,
            &[],
            &[],
            &DEFAULT_PARAMS,
            &AnalyzeStats::default(),
            &available_indexes,
            &table_references,
            &schema,
        )
        .unwrap();
        assert!(result.is_some());
        let BestJoinOrderResult { best_plan, .. } = result.unwrap();

        // Expected: t1 first (scan), t2 second (index seek)
        assert_eq!(
            best_plan.table_numbers().collect::<Vec<_>>(),
            vec![TABLE1, TABLE2],
            "Expected join order [t1, t2] to use index on t2.a"
        );

        // t1 should use table scan (no constraints)
        let access_method_t1 = &access_methods_arena[best_plan.data[0].1];
        let (_, _, constraint_refs_t1) = _as_btree(access_method_t1);
        assert!(
            constraint_refs_t1.is_empty(),
            "t1 should use table scan with no constraints"
        );

        // t2 should use index seek, NOT hash join
        let access_method_t2 = &access_methods_arena[best_plan.data[1].1];
        match &access_method_t2.params {
            AccessMethodParams::BTreeTable {
                index,
                constraint_refs,
                ..
            } => {
                assert!(
                    index.is_some(),
                    "t2 should use index idx_t2_a, not a hash join"
                );
                assert_eq!(
                    index.as_ref().unwrap().name,
                    "idx_t2_a",
                    "t2 should use index idx_t2_a"
                );
                assert!(
                    !constraint_refs.is_empty(),
                    "t2 should have constraints for index seek"
                );
            }
            AccessMethodParams::HashJoin { .. } => {
                panic!("Expected index lookup on t2, but got hash join instead");
            }
            _ => panic!("Unexpected access method for t2"),
        }
    }

    #[test]
    fn hash_join_uses_estimated_matches_for_row_count() {
        let t1 = _create_btree_table("t1", _create_column_list(&["value"], Type::Integer));
        let mut t2 = _create_btree_table("t2", _create_column_list(&["value"], Type::Integer));
        Arc::get_mut(&mut t2).unwrap().root_page = 2;
        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![
            _create_table_reference(t1, None, table_id_counter.next()),
            _create_table_reference(
                t2,
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            ),
        ];
        let mut where_clause = vec![_create_binary_expr(
            _create_column_expr(joined_tables[0].internal_id, 0, false),
            Operator::Equals,
            _create_column_expr(joined_tables[1].internal_id, 0, false),
        )];
        let table_references = TableReferences::new(joined_tables, vec![]);
        let available_indexes = AvailableIndexes::default();
        let constraints = constraints_from_where_clause(
            &where_clause,
            &table_references,
            &available_indexes,
            &[],
            &empty_schema(),
            &DEFAULT_PARAMS,
        )
        .unwrap();
        let method = try_hash_join_access_method(
            &table_references.joined_tables()[0],
            &table_references.joined_tables()[1],
            0,
            1,
            &constraints[0],
            &constraints[1],
            &mut where_clause,
            std::iter::once((
                0,
                table_references.joined_tables()[0].internal_id,
                table_references.joined_tables()[1].internal_id,
            )),
            1_000.0,
            1_000.0,
            1.0,
            &[],
            &DEFAULT_PARAMS,
        )
        .unwrap()
        .unwrap();

        assert!(method.estimated_rows_per_outer_row < 1_000.0);
    }
}
