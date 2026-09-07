use std::sync::Arc;

use crate::alloc::{TryClone, TursoSliceExt};

use rustc_hash::FxHashMap as HashMap;
use turso_parser::ast::{self, SortOrder, SubqueryType, TableInternalId};

use super::{
    emitter::{Resolver, TranslateCtx},
    main_loop::LoopLabels,
    plan::{Aggregate, Operation, QueryDestination, Search, SelectPlan},
    planner::{resolve_window_and_aggregate_functions, TableMask},
};
use crate::translate::expr::comparison_affinity;
use crate::{
    alloc::TursoIteratorExt,
    emit_explain,
    schema::{BTreeCharacteristics, BTreeTable, Column, Index, IndexColumn, Table},
    translate::{
        collate::get_collseq_from_expr,
        compound_select::emit_program_for_compound_select,
        emitter::select::{
            emit_materialized_build_inputs, emit_program_for_select,
            emit_program_for_select_with_resolver, emit_query,
        },
        eqp::{eqp_detail_for_table_op, EqpDetail, EqpJoin, EqpSubquery, EqpSubqueryExec},
        expr::{get_expr_affinity, unwrap_parens, walk_expr, walk_expr_mut, WalkControl},
        optimizer::optimize_select_plan,
        plan::{
            plan_has_outer_scope_dependency, plan_is_correlated,
            select_plan_has_outer_scope_dependency, ColumnUsedMask, EvalAt, JoinOrderMember,
            JoinedTable, NonFromClauseSubquery, OuterQueryReference, Plan, SubqueryEvalPhase,
            SubqueryOrigin, SubqueryPosition, SubqueryState, TableReferences, WhereTerm,
        },
        select::prepare_select_plan,
    },
    types::Value,
    util::parse_signed_number,
    vdbe::{
        builder::{CursorKey, CursorType, MaterializedCteInfo, ProgramBuilder},
        insn::Insn,
        CursorID,
    },
    Connection, Numeric, Result,
};

struct DirectMaterializedSubquery {
    index: Arc<Index>,
    affinity_str: Option<Arc<String>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MaterializedFromClauseSubqueryStorage {
    TableBacked,
    DirectIndex,
}

enum FromClauseSubqueryExecutionMode {
    Coroutine,
    MaterializedTable,
    DirectMaterializedIndex(DirectMaterializedSubquery),
}

pub(crate) fn materialized_from_clause_subquery_storage(
    subquery: &crate::schema::FromClauseSubquery,
) -> Option<MaterializedFromClauseSubqueryStorage> {
    match subquery.plan.select_query_destination() {
        Some(QueryDestination::EphemeralTable { .. }) => {
            Some(MaterializedFromClauseSubqueryStorage::TableBacked)
        }
        Some(QueryDestination::EphemeralIndex { .. }) => {
            Some(MaterializedFromClauseSubqueryStorage::DirectIndex)
        }
        _ => None,
    }
}

// Count the CTE reads in this query tree that can share one materialized
// result.
//
// Reads from correlated post-write RETURNING subqueries are skipped because
// they run once per updated row instead of once for the statement.
fn count_shared_cte_references(
    counts: &mut HashMap<usize, usize>,
    table_references: &TableReferences,
    non_from_clause_subqueries: &[NonFromClauseSubquery],
) {
    for table in table_references.joined_tables() {
        if let Table::FromClauseSubquery(from_clause_subquery) = &table.table {
            if let Some(cte_id) = from_clause_subquery.cte_id() {
                *counts.entry(cte_id).or_default() += 1;
                continue;
            }
            count_shared_cte_references_in_plan(counts, from_clause_subquery.plan.as_ref());
        }
    }

    for subquery in non_from_clause_subqueries {
        let SubqueryState::Unevaluated {
            plan: Some(subquery_plan),
        } = &subquery.state
        else {
            continue;
        };
        // A correlated RETURNING subquery runs after each updated row is
        // written, so its CTE reads must not be counted as part of the shared
        // pre-write snapshot used by earlier readers in the same statement.
        if subquery.origin.is_post_write_returning()
            && plan_has_outer_scope_dependency(subquery_plan)
        {
            continue;
        }
        count_shared_cte_references_in_plan(counts, subquery_plan);
    }
}

fn count_shared_cte_references_in_plan(counts: &mut HashMap<usize, usize>, plan: &Plan) {
    match plan {
        Plan::Select(select_plan) => count_shared_cte_references(
            counts,
            &select_plan.table_references,
            &select_plan.non_from_clause_subqueries,
        ),
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            for (select_plan, _) in left {
                count_shared_cte_references(
                    counts,
                    &select_plan.table_references,
                    &select_plan.non_from_clause_subqueries,
                );
            }
            count_shared_cte_references(
                counts,
                &right_most.table_references,
                &right_most.non_from_clause_subqueries,
            );
        }
        Plan::RecursiveCte(recursive_cte) => {
            count_shared_cte_references_in_plan(counts, &recursive_cte.initial_query);
            count_shared_cte_references_in_plan(counts, &recursive_cte.recursive_query);
        }
        Plan::Delete(_) | Plan::Update(_) => {}
    }
}

/// Mark CTE references that must be materialized once and shared across
/// multiple reads of the same query tree.
pub(crate) fn mark_shared_cte_materialization_requirements(
    table_references: &mut TableReferences,
    non_from_clause_subqueries: &mut [NonFromClauseSubquery],
) {
    fn annotate_plan(plan: &mut Plan) {
        match plan {
            Plan::Select(select_plan) => mark_shared_cte_materialization_requirements(
                &mut select_plan.table_references,
                &mut select_plan.non_from_clause_subqueries,
            ),
            Plan::CompoundSelect {
                left, right_most, ..
            } => {
                for (select_plan, _) in left.iter_mut() {
                    mark_shared_cte_materialization_requirements(
                        &mut select_plan.table_references,
                        &mut select_plan.non_from_clause_subqueries,
                    );
                }
                mark_shared_cte_materialization_requirements(
                    &mut right_most.table_references,
                    &mut right_most.non_from_clause_subqueries,
                );
            }
            Plan::RecursiveCte(recursive_cte) => {
                annotate_plan(&mut recursive_cte.initial_query);
                annotate_plan(&mut recursive_cte.recursive_query);
            }
            Plan::Delete(_) | Plan::Update(_) => {}
        }
    }

    let mut shared_ref_counts = HashMap::default();
    count_shared_cte_references(
        &mut shared_ref_counts,
        table_references,
        non_from_clause_subqueries,
    );

    for table in table_references.joined_tables_mut().iter_mut() {
        if let Table::FromClauseSubquery(from_clause_subquery) = &mut table.table {
            let from_clause_subquery = Arc::make_mut(from_clause_subquery);
            let shared_materialization = from_clause_subquery.cte_id().is_some_and(|cte_id| {
                shared_ref_counts.get(&cte_id).copied().unwrap_or_default() > 1
                    && !plan_has_outer_scope_dependency(&from_clause_subquery.plan)
            });
            from_clause_subquery.set_shared_materialization(shared_materialization);
            if let Some(cte_id) = from_clause_subquery.cte_id() {
                tracing::trace!(
                    cte_id,
                    shared_ref_count = shared_ref_counts.get(&cte_id).copied().unwrap_or_default(),
                    shared_materialization,
                    outer_scope_dependency = plan_has_outer_scope_dependency(
                        &from_clause_subquery.plan,
                    ),
                    contains_nested_correlation = plan_is_correlated(&from_clause_subquery.plan),
                    identifier = %table.identifier,
                    "annotated CTE materialization requirements"
                );
            }
            annotate_plan(from_clause_subquery.plan.as_mut());
        }
    }

    for subquery in non_from_clause_subqueries.iter_mut() {
        let SubqueryState::Unevaluated {
            plan: Some(subquery_plan),
        } = &mut subquery.state
        else {
            continue;
        };
        annotate_plan(subquery_plan);
    }
}

// Compute query plans for subqueries occurring in any position other than the FROM clause.
// This includes the WHERE clause, HAVING clause, GROUP BY clause, ORDER BY clause, LIMIT clause, and OFFSET clause.
/// The AST expression containing the subquery ([ast::Expr::Exists], [ast::Expr::Subquery], [ast::Expr::InSelect]) is replaced with a [ast::Expr::SubqueryResult] expression.
/// The [ast::Expr::SubqueryResult] expression contains the subquery ID, the left-hand side expression (only applicable to IN subqueries), the NOT IN flag (only applicable to IN subqueries), and the subquery type.
/// The computed plans are stored in the [NonFromClauseSubquery] structs on the [SelectPlan], and evaluated at the appropriate time during the translation of the main query.
/// The appropriate time is determined by whether the subquery is correlated or uncorrelated;
/// if it is uncorrelated, it can be evaluated as early as possible, but if it is correlated, it must be evaluated after all of its dependencies from the
/// outer query are 'in scope', i.e. their cursors are open and rewound.
#[turso_macros::trace_stack]
pub fn plan_subqueries_from_select_plan(
    program: &mut ProgramBuilder,
    plan: &mut SelectPlan,
    resolver: &Resolver,
    connection: &Arc<Connection>,
) -> Result<()> {
    // Common-subexpression elimination for scalar subqueries shared by GROUP BY and the SELECT
    // list (e.g. `SELECT (subq) AS rs ... GROUP BY rs`, where the GROUP BY term is a copy of the
    // result column): plan and evaluate the subquery once instead of once per use.
    //
    // The set of shared subqueries is computed up front, independent of the order the clause passes
    // below run, so correctness does not hinge on GROUP BY being processed before the SELECT list.
    // Whichever pass reaches a shared subquery first registers it in `cse_map`; the other reuses
    // that registration. Shared subqueries are registered at the GROUP BY eval phase (the earliest
    // phase among their uses), so the single evaluation is ready for whichever clause reads it.
    let shared_subqueries: Vec<ast::Expr> = match &plan.group_by {
        Some(group_by) => {
            let in_group_by = collect_scalar_subqueries(group_by.exprs.iter())?;
            if in_group_by.is_empty() {
                Vec::new()
            } else {
                let in_select =
                    collect_scalar_subqueries(plan.result_columns.iter().map(|c| &c.expr))?;
                in_group_by
                    .into_iter()
                    .filter(|g| in_select.contains(g))
                    .collect()
            }
        }
        None => Vec::new(),
    };
    let mut cse_map: Vec<(ast::Expr, ast::Expr)> = Vec::new();
    let mut same_query_map: Vec<(ast::Expr, TableInternalId, SubqueryOrigin)> = Vec::new();
    // WHERE
    {
        crate::stack::trace_stack!("select_where");
        plan_subqueries_with_outer_query_access(
            program,
            &mut plan.non_from_clause_subqueries,
            &mut plan.table_references,
            resolver,
            plan.where_clause.iter_mut().map(|t| &mut t.expr),
            connection,
            SubqueryPosition::Where,
            SubqueryOrigin::SelectWhere,
            SubqueryPosition::Where.allow_correlated(),
            &mut cse_map,
            &mut same_query_map,
            &[],
        )?;
    }

    // GROUP BY
    if let Some(group_by) = &mut plan.group_by {
        {
            crate::stack::trace_stack!("select_group_by");
            plan_subqueries_with_outer_query_access(
                program,
                &mut plan.non_from_clause_subqueries,
                &mut plan.table_references,
                resolver,
                group_by.exprs.iter_mut(),
                connection,
                SubqueryPosition::GroupBy,
                SubqueryOrigin::SelectGroupBy,
                SubqueryPosition::GroupBy.allow_correlated(),
                &mut cse_map,
                &mut same_query_map,
                &shared_subqueries,
            )?;
        }
        if let Some(having) = group_by.having.as_mut() {
            crate::stack::trace_stack!("select_having");
            plan_subqueries_with_outer_query_access(
                program,
                &mut plan.non_from_clause_subqueries,
                &mut plan.table_references,
                resolver,
                having.iter_mut(),
                connection,
                SubqueryPosition::Having,
                SubqueryOrigin::SelectHaving,
                !group_by.exprs.is_empty(),
                &mut cse_map,
                &mut same_query_map,
                &[],
            )?;
        }
    }

    // Result columns
    {
        crate::stack::trace_stack!("select_result_columns");
        plan_subqueries_with_outer_query_access(
            program,
            &mut plan.non_from_clause_subqueries,
            &mut plan.table_references,
            resolver,
            plan.result_columns.iter_mut().map(|c| &mut c.expr),
            connection,
            SubqueryPosition::ResultColumn,
            SubqueryOrigin::SelectList,
            SubqueryPosition::ResultColumn.allow_correlated(),
            &mut cse_map,
            &mut same_query_map,
            &shared_subqueries,
        )?;
    }

    // ORDER BY
    {
        crate::stack::trace_stack!("select_order_by");
        plan_subqueries_with_outer_query_access(
            program,
            &mut plan.non_from_clause_subqueries,
            &mut plan.table_references,
            resolver,
            plan.order_by.iter_mut().map(|(expr, _, _)| &mut **expr),
            connection,
            SubqueryPosition::OrderBy,
            SubqueryOrigin::SelectOrderBy,
            SubqueryPosition::OrderBy.allow_correlated(),
            &mut cse_map,
            &mut same_query_map,
            &[],
        )?;
    }

    // LIMIT and OFFSET cannot reference columns from the outer query
    let get_outer_query_refs = |_: &TableReferences| Ok(crate::alloc::try_vec![]?);
    {
        let mut subquery_parser = get_subquery_parser(
            program,
            &mut plan.non_from_clause_subqueries,
            &mut plan.table_references,
            resolver,
            connection,
            get_outer_query_refs,
            SubqueryPosition::LimitOffset,
            SubqueryOrigin::SelectLimitOffset,
            false,
            &mut cse_map,
            &mut same_query_map,
            &[],
        );
        // Limit
        if let Some(limit) = &mut plan.limit {
            crate::stack::trace_stack!("select_limit");
            walk_expr_mut(limit, &mut subquery_parser)?;
        }
        // Offset
        if let Some(offset) = &mut plan.offset {
            crate::stack::trace_stack!("select_offset");
            walk_expr_mut(offset, &mut subquery_parser)?;
        }
    }

    // Recollect aggregates after all subquery planning.
    // This is necessary because:
    // 1. Aggregates are collected with cloned expressions before subquery planning modifies them
    //    (e.g., EXISTS -> SubqueryResult), causing stale args in aggregates.
    // 2. ORDER BY may be cleared for single-row aggregates AFTER aggregates were collected from it,
    //    leaving orphaned aggregates with unprocessed subqueries in their args.
    // Recollecting from the current state of result_columns, HAVING, and ORDER BY ensures
    // aggregates have updated expressions and excludes aggregates from cleared ORDER BY.
    if !plan.aggregates.is_empty() {
        recollect_aggregates(plan, resolver)?;
    }

    assign_select_subquery_eval_phases(plan);
    mark_shared_cte_materialization_requirements(
        &mut plan.table_references,
        &mut plan.non_from_clause_subqueries,
    );

    update_column_used_masks(
        &mut plan.table_references,
        &mut plan.non_from_clause_subqueries,
    )?;
    Ok(())
}

/// Compute query plans for subqueries in a DML statement's WHERE clause.
/// This is used by DELETE and UPDATE statements which only have subqueries in the WHERE clause.
/// Similar to [plan_subqueries_from_select_plan] but only handles the WHERE clause
/// since these statements don't have GROUP BY, ORDER BY, or result column subqueries.
#[turso_macros::trace_stack]
pub fn plan_subqueries_from_where_clause(
    program: &mut ProgramBuilder,
    non_from_clause_subqueries: &mut Vec<NonFromClauseSubquery>,
    table_references: &mut TableReferences,
    where_clause: &mut [WhereTerm],
    resolver: &Resolver,
    connection: &Arc<Connection>,
) -> Result<()> {
    plan_subqueries_with_outer_query_access(
        program,
        non_from_clause_subqueries,
        table_references,
        resolver,
        where_clause.iter_mut().map(|term| &mut term.expr),
        connection,
        SubqueryPosition::Where,
        SubqueryOrigin::DmlWhere,
        SubqueryPosition::Where.allow_correlated(),
        &mut Vec::new(),
        &mut Vec::new(),
        &[],
    )?;

    update_column_used_masks(table_references, non_from_clause_subqueries)?;
    Ok(())
}

/// Compute query plans for subqueries in VALUES expressions.
/// This is used by INSERT statements with VALUES clauses and SELECT with VALUES.
/// The VALUES expressions may contain scalar subqueries that need to be planned.
#[allow(clippy::vec_box)]
pub fn plan_subqueries_from_values(
    program: &mut ProgramBuilder,
    non_from_clause_subqueries: &mut Vec<NonFromClauseSubquery>,
    table_references: &mut TableReferences,
    values: &mut [Vec<Box<ast::Expr>>],
    resolver: &Resolver,
    connection: &Arc<Connection>,
) -> Result<()> {
    plan_subqueries_with_outer_query_access(
        program,
        non_from_clause_subqueries,
        table_references,
        resolver,
        values.iter_mut().flatten().map(|e| e.as_mut()),
        connection,
        SubqueryPosition::ResultColumn, // VALUES are similar to result columns in terms of subquery handling
        SubqueryOrigin::SelectList,
        SubqueryPosition::ResultColumn.allow_correlated(),
        &mut Vec::new(),
        &mut Vec::new(),
        &[],
    )?;

    update_column_used_masks(table_references, non_from_clause_subqueries)?;
    Ok(())
}

/// Compute query plans for subqueries in UPDATE SET clause expressions.
/// This is used by UPDATE statements where SET clause values contain scalar subqueries.
/// e.g. `UPDATE t SET col = (SELECT max(id) FROM t2)`
pub fn plan_subqueries_from_update_sets(
    program: &mut ProgramBuilder,
    non_from_clause_subqueries: &mut Vec<NonFromClauseSubquery>,
    table_references: &mut TableReferences,
    sets: &mut [ast::Set],
    resolver: &Resolver,
    connection: &Arc<Connection>,
) -> Result<()> {
    plan_subqueries_with_outer_query_access(
        program,
        non_from_clause_subqueries,
        table_references,
        resolver,
        sets.iter_mut().map(|set| set.expr.as_mut()),
        connection,
        SubqueryPosition::ResultColumn,
        SubqueryOrigin::DmlSet,
        SubqueryPosition::ResultColumn.allow_correlated(),
        &mut Vec::new(),
        &mut Vec::new(),
        &[],
    )?;

    update_column_used_masks(table_references, non_from_clause_subqueries)?;
    Ok(())
}

/// Compute query plans for subqueries in RETURNING expressions.
/// This is used by INSERT, UPDATE, and DELETE statements with RETURNING clauses.
/// RETURNING expressions may contain scalar subqueries that need to be planned.
#[turso_macros::trace_stack]
pub fn plan_subqueries_from_returning(
    program: &mut ProgramBuilder,
    non_from_clause_subqueries: &mut Vec<NonFromClauseSubquery>,
    table_references: &mut TableReferences,
    returning: &mut [ast::ResultColumn],
    resolver: &Resolver,
    connection: &Arc<Connection>,
) -> Result<()> {
    // Extract mutable references to expressions from ResultColumn::Expr variants
    let exprs = returning.iter_mut().filter_map(|rc| match rc {
        ast::ResultColumn::Expr(expr, _) => Some(expr.as_mut()),
        ast::ResultColumn::Star | ast::ResultColumn::TableStar(_) => None,
    });

    plan_subqueries_with_outer_query_access(
        program,
        non_from_clause_subqueries,
        table_references,
        resolver,
        exprs,
        connection,
        SubqueryPosition::ResultColumn,
        SubqueryOrigin::DmlReturning,
        SubqueryPosition::ResultColumn.allow_correlated(),
        &mut Vec::new(),
        &mut Vec::new(),
        &[],
    )?;

    update_column_used_masks(table_references, non_from_clause_subqueries)?;
    Ok(())
}

/// Plan subqueries in a trigger WHEN clause expression.
/// The WHEN clause has no FROM clause, so there are no outer query references.
/// NEW/OLD references should already be rewritten to Expr::Register before calling this.
#[turso_macros::trace_stack]
pub fn plan_subqueries_from_trigger_when_clause(
    program: &mut ProgramBuilder,
    non_from_clause_subqueries: &mut Vec<NonFromClauseSubquery>,
    expr: &mut ast::Expr,
    resolver: &Resolver,
    connection: &Arc<Connection>,
) -> Result<()> {
    let mut table_references = TableReferences::new(vec![], vec![]);
    plan_subqueries_with_outer_query_access(
        program,
        non_from_clause_subqueries,
        &mut table_references,
        resolver,
        std::iter::once(expr),
        connection,
        SubqueryPosition::Where,
        SubqueryOrigin::TriggerWhen,
        false,
        &mut Vec::new(),
        &mut Vec::new(),
        &[],
    )
}

/// Compute query plans for subqueries in the WHERE clause and HAVING clause (both of which have access to the outer query scope)
#[allow(clippy::too_many_arguments)]
#[turso_macros::trace_stack]
fn plan_subqueries_with_outer_query_access<'a>(
    program: &mut ProgramBuilder,
    out_subqueries: &mut Vec<NonFromClauseSubquery>,
    referenced_tables: &mut TableReferences,
    resolver: &Resolver,
    exprs: impl Iterator<Item = &'a mut ast::Expr>,
    connection: &Arc<Connection>,
    position: SubqueryPosition,
    origin: SubqueryOrigin,
    allow_correlated: bool,
    cse_map: &mut Vec<(ast::Expr, ast::Expr)>,
    same_query_map: &mut Vec<(ast::Expr, TableInternalId, SubqueryOrigin)>,
    shared: &[ast::Expr],
) -> Result<()> {
    // Most subqueries can reference columns from the outer query,
    // including nested cases where a subquery inside a subquery references columns from its parent's parent
    // and so on.
    let get_outer_query_refs = |referenced_tables: &TableReferences| {
        let outer_refs = referenced_tables
            .joined_tables()
            .iter()
            .map(|t| {
                // Extract cte_id from FromClauseSubquery if this is a CTE reference
                let cte_id = match &t.table {
                    Table::FromClauseSubquery(subq) => subq.cte_id(),
                    _ => None,
                };
                let outer_ref = OuterQueryReference {
                    table: t.table.clone(),
                    identifier: t.identifier.clone(),
                    internal_id: t.internal_id,
                    using_dedup_hidden_cols: t.using_dedup_hidden_cols()?,
                    col_used_mask: ColumnUsedMask::default(),
                    cte_select: None,
                    cte_explicit_columns: Vec::new(),
                    cte_id,
                    cte_definition_only: false,
                    rowid_referenced: false,
                    scope_depth: 0,
                };
                Ok::<_, crate::LimboError>(outer_ref)
            })
            .chain(referenced_tables.outer_query_refs().iter().map(|t| {
                Ok(OuterQueryReference {
                    table: t.table.clone(),
                    identifier: t.identifier.clone(),
                    internal_id: t.internal_id,
                    using_dedup_hidden_cols: t.using_dedup_hidden_cols.try_clone()?,
                    col_used_mask: ColumnUsedMask::default(),
                    cte_select: t.cte_select.clone(),
                    cte_explicit_columns: t.cte_explicit_columns.clone(),
                    cte_id: t.cte_id, // Preserve CTE ID from outer query refs
                    cte_definition_only: t.cte_definition_only,
                    rowid_referenced: false,
                    scope_depth: t.scope_depth + 1,
                })
            }))
            .try_collect::<Result<crate::alloc::Vec<_>>>()??;
        Ok(outer_refs)
    };

    let mut subquery_parser = get_subquery_parser(
        program,
        out_subqueries,
        referenced_tables,
        resolver,
        connection,
        get_outer_query_refs,
        position,
        origin,
        allow_correlated,
        cse_map,
        same_query_map,
        shared,
    );
    for expr in exprs {
        walk_expr_mut(expr, &mut subquery_parser)?;
    }

    Ok(())
}

/// Collect every scalar-subquery (`ast::Expr::Subquery`) node that appears anywhere in `exprs`.
/// Used to find, up front, the subqueries shared between the GROUP BY clause and the SELECT list
/// so they can be common-subexpression-eliminated independently of the order the clause passes run.
fn collect_scalar_subqueries<'a>(
    exprs: impl Iterator<Item = &'a ast::Expr>,
) -> Result<Vec<ast::Expr>> {
    let mut found = Vec::new();
    for expr in exprs {
        walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
            if matches!(e, ast::Expr::Subquery(_)) {
                found.push(e.clone());
            }
            Ok(WalkControl::Continue)
        })?;
    }
    Ok(found)
}

/// Create a closure that will walk the AST and replace subqueries with [ast::Expr::SubqueryResult] expressions.]
#[allow(clippy::too_many_arguments)]
fn get_subquery_parser<'a>(
    program: &'a mut ProgramBuilder,
    out_subqueries: &'a mut Vec<NonFromClauseSubquery>,
    referenced_tables: &'a mut TableReferences,
    resolver: &'a Resolver,
    connection: &'a Arc<Connection>,
    get_outer_query_refs: impl Fn(&TableReferences) -> Result<crate::alloc::Vec<OuterQueryReference>>
        + 'a,
    position: SubqueryPosition,
    origin: SubqueryOrigin,
    allow_correlated: bool,
    cse_map: &'a mut Vec<(ast::Expr, ast::Expr)>,
    same_query_map: &'a mut Vec<(ast::Expr, TableInternalId, SubqueryOrigin)>,
    shared: &'a [ast::Expr],
) -> impl FnMut(&mut ast::Expr) -> Result<WalkControl> + 'a {
    let handle_unsupported_correlation =
        |correlated: bool, position: SubqueryPosition, allow_correlated: bool| -> Result<()> {
            if correlated && !allow_correlated {
                crate::bail_parse_error!(
                    "correlated subqueries in {} clause are not supported yet",
                    position.name()
                );
            }
            Ok(())
        };

    move |expr: &mut ast::Expr| -> Result<WalkControl> {
        match expr {
            ast::Expr::Exists(_) => {
                let subquery_id = program.table_reference_counter.next();
                let outer_query_refs = {
                    crate::stack::trace_stack!("get_outer_refs");
                    get_outer_query_refs(referenced_tables)
                }?;

                let result_reg = program.alloc_register();
                let subquery_type = SubqueryType::Exists { result_reg };
                let result_expr = ast::Expr::SubqueryResult {
                    subquery_id,
                    lhs: None,
                    not_in: false,
                    query_type: subquery_type.clone(),
                };
                let ast::Expr::Exists(subselect) = ({
                    crate::stack::trace_stack!("replace_exists_expr");
                    std::mem::replace(expr, result_expr)
                }) else {
                    unreachable!();
                };

                let plan = prepare_select_plan(
                    subselect,
                    resolver,
                    program,
                    &outer_query_refs,
                    QueryDestination::ExistsSubqueryResult { result_reg },
                    connection,
                )?;
                let Plan::Select(mut plan) = plan else {
                    crate::bail_parse_error!(
                        "compound SELECT queries not supported yet in WHERE clause subqueries"
                    );
                };
                // EXISTS only checks whether a row comes out, so ORDER BY and
                // DISTINCT cannot change the result and SQLite drops them
                // (select.c, "dropping superfluous ORDER BY"). Dropping them
                // here — after the plan is prepared, so name errors in the
                // ORDER BY still surface — also means their expressions are
                // never evaluated: an ORDER BY term that would error at
                // runtime (e.g. an integer overflow) must not fail the query.
                // LIMIT and OFFSET stay: with DISTINCT gone they count plain
                // rows, which matches SQLite.
                plan.order_by.clear();
                plan.distinctness = crate::translate::plan::Distinctness::NonDistinct;
                let correlated = select_plan_has_outer_scope_dependency(&plan);
                handle_unsupported_correlation(correlated, position, allow_correlated)?;
                if !correlated || origin.is_write_statement() {
                    optimize_select_plan(&mut plan, resolver)?;
                }
                out_subqueries.push(NonFromClauseSubquery {
                    internal_id: subquery_id,
                    same_query: None,
                    query_type: subquery_type,
                    state: SubqueryState::Unevaluated {
                        plan: Some(Box::new(Plan::Select(plan))),
                    },
                    correlated,
                    origin,
                    eval_phase: origin.phase_floor(),
                });
                Ok(WalkControl::Continue)
            }
            ast::Expr::Subquery(_) => {
                // CSE: a scalar subquery in `shared` appears in both GROUP BY and the SELECT list,
                // so plan and evaluate it once. Whichever clause reaches it first registers it in
                // cse_map; the other reuses that registration. Order-independent: membership in
                // `shared` is computed up front, so this does not depend on GROUP BY being walked
                // before the SELECT list.
                let is_shared = shared.contains(&*expr);
                if is_shared {
                    if let Some((_, result)) = cse_map.iter().find(|(k, _)| *k == *expr) {
                        *expr = result.clone();
                        return Ok(WalkControl::SkipChildren);
                    }
                }
                let cse_key = is_shared.then(|| expr.clone());
                // A shared subquery is registered at the GROUP BY eval phase (the earliest among its
                // uses), so its single evaluation is ready for whichever clause reads it first,
                // regardless of the order these passes run.
                let effective_origin = if is_shared {
                    SubqueryOrigin::SelectGroupBy
                } else {
                    origin
                };
                let same_query = same_query_map.iter().find_map(|(query, id, query_origin)| {
                    (*query_origin == effective_origin && *query == *expr).then_some(*id)
                });
                let same_query_key = same_query.is_none().then(|| expr.clone());
                let subquery_id = program.table_reference_counter.next();
                let outer_query_refs = {
                    crate::stack::trace_stack!("get_outer_refs");
                    get_outer_query_refs(referenced_tables)
                }?;

                let result_expr = ast::Expr::SubqueryResult {
                    subquery_id,
                    lhs: None,
                    not_in: false,
                    // Placeholder values because the number of columns returned is not known until the plan is prepared.
                    // These are replaced below after planning.
                    query_type: SubqueryType::RowValue {
                        result_reg_start: 0,
                        num_regs: 0,
                    },
                };
                let ast::Expr::Subquery(subselect) = ({
                    crate::stack::trace_stack!("replace_scalar_expr");
                    std::mem::replace(expr, result_expr)
                }) else {
                    unreachable!();
                };
                let plan = prepare_select_plan(
                    subselect,
                    resolver,
                    program,
                    &outer_query_refs,
                    QueryDestination::Unset,
                    connection,
                )?;
                let Plan::Select(mut plan) = plan else {
                    crate::bail_parse_error!(
                        "compound SELECT queries not supported yet in WHERE clause subqueries"
                    );
                };
                let correlated = select_plan_has_outer_scope_dependency(&plan);
                handle_unsupported_correlation(correlated, position, allow_correlated)?;
                if !correlated || origin.is_write_statement() {
                    optimize_select_plan(&mut plan, resolver)?;
                }
                let reg_count = plan.result_columns.len();
                let reg_start = program.alloc_registers(reg_count);

                if reg_count == 1 {
                    if let Some(result_col) = plan.result_columns.first() {
                        let affinity =
                            get_expr_affinity(&result_col.expr, Some(&plan.table_references), None);
                        resolver
                            .subquery_affinities
                            .borrow_mut()
                            .insert(subquery_id, affinity);
                    }
                }

                plan.query_destination = QueryDestination::RowValueSubqueryResult {
                    result_reg_start: reg_start,
                    num_regs: reg_count,
                };

                // Only inject LIMIT 1 if there's no existing limit, or the existing limit is > 1,
                // If LIMIT 0, subquery should return no rows (NULL).
                let limit = match &plan.limit {
                    Some(expr) => match parse_signed_number(expr) {
                        Ok(Value::Numeric(Numeric::Integer(v))) => !(0..=1).contains(&v),
                        _ => true,
                    },
                    None => true,
                };
                if limit {
                    // RowValue subqueries are satisfied after at most 1 row has been returned,
                    // as they are used in comparisons with a scalar or a tuple of scalars like (x,y) = (SELECT ...) or x = (SELECT ...).
                    plan.limit = Some(Box::new(ast::Expr::Literal(ast::Literal::Numeric(
                        "1".to_string(),
                    ))));
                }

                let ast::Expr::SubqueryResult {
                    subquery_id,
                    lhs: None,
                    not_in: false,
                    query_type:
                        SubqueryType::RowValue {
                            result_reg_start,
                            num_regs,
                        },
                } = &mut *expr
                else {
                    unreachable!();
                };
                *result_reg_start = reg_start;
                *num_regs = reg_count;
                let subquery_id = *subquery_id;

                out_subqueries.push(NonFromClauseSubquery {
                    internal_id: subquery_id,
                    same_query,
                    query_type: SubqueryType::RowValue {
                        result_reg_start: reg_start,
                        num_regs: reg_count,
                    },
                    state: SubqueryState::Unevaluated {
                        plan: Some(Box::new(Plan::Select(plan))),
                    },
                    correlated,
                    origin: effective_origin,
                    eval_phase: effective_origin.phase_floor(),
                });
                if let Some(key) = cse_key {
                    cse_map.push((key, expr.clone()));
                }
                if let Some(query) = same_query_key {
                    same_query_map.push((query, subquery_id, effective_origin));
                }
                Ok(WalkControl::Continue)
            }
            ast::Expr::InSelect { .. } => {
                let subquery_id = program.table_reference_counter.next();
                let outer_query_refs = {
                    crate::stack::trace_stack!("get_outer_refs");
                    get_outer_query_refs(referenced_tables)
                }?;

                let ast::Expr::InSelect { lhs, not, rhs } = ({
                    crate::stack::trace_stack!("take_in_select_expr");
                    std::mem::take(expr)
                }) else {
                    unreachable!();
                };
                let plan = prepare_select_plan(
                    rhs,
                    resolver,
                    program,
                    &outer_query_refs,
                    QueryDestination::Unset,
                    connection,
                )?;
                let mut plan = plan;
                let correlated = plan_has_outer_scope_dependency(&plan);
                handle_unsupported_correlation(correlated, position, allow_correlated)?;
                if !correlated || origin.is_write_statement() {
                    match &mut plan {
                        Plan::Select(select_plan) => {
                            optimize_select_plan(select_plan, resolver)?;
                        }
                        Plan::CompoundSelect {
                            left, right_most, ..
                        } => {
                            optimize_select_plan(right_most, resolver)?;
                            for (select_plan, _) in left.iter_mut() {
                                optimize_select_plan(select_plan, resolver)?;
                            }
                        }
                        _ => unreachable!("prepare_select_plan cannot return Delete/Update"),
                    }
                }
                let result_columns = plan.select_result_columns();
                let table_references = plan.select_table_references();
                // e.g. (x,y) IN (SELECT ...)
                // or x IN (SELECT ...)
                let lhs_columns = match unwrap_parens(lhs.as_ref())? {
                    ast::Expr::Parenthesized(exprs) => {
                        either::Left(exprs.iter().map(|e| e.as_ref()))
                    }
                    expr => either::Right(core::iter::once(expr)),
                };
                let lhs_column_count = lhs_columns.len();
                if lhs_column_count != result_columns.len() {
                    crate::bail_parse_error!(
                        "sub-select returns {} columns - expected {lhs_column_count}",
                        result_columns.len()
                    );
                }
                // Collect affinity and LHS collation in a single pass over lhs_columns.
                // "x IN (SELECT y ...)" uses the collation of x
                // (https://www.sqlite.org/datatype3.html#collation §7.1),
                // so the ephemeral index must use the LHS collation for correct
                // NotFound/Found probe comparisons.
                let mut affinity_chars = String::with_capacity(lhs_column_count);
                let mut lhs_collations = Vec::with_capacity(lhs_column_count);
                for (i, lhs_expr) in lhs_columns.enumerate() {
                    let lhs_affinity = get_expr_affinity(lhs_expr, Some(referenced_tables), None);
                    affinity_chars.push(
                        comparison_affinity(
                            &result_columns[i].expr,
                            lhs_affinity,
                            Some(table_references),
                            None,
                        )
                        .aff_mask(),
                    );
                    lhs_collations.push(get_collseq_from_expr(lhs_expr, referenced_tables)?);
                }
                let in_affinity_str: Arc<String> = Arc::new(affinity_chars);

                let columns = result_columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        let rhs_collation = get_collseq_from_expr(&c.expr, table_references)?;
                        Ok::<_, crate::LimboError>(IndexColumn {
                            name: c.name(table_references).unwrap_or("").to_string(),
                            order: SortOrder::Asc,
                            nulls_order: None,
                            pos_in_table: i,
                            collation: lhs_collations[i].or(rhs_collation),
                            default: None,
                            expr: None,
                        })
                    })
                    .try_collect::<Result<crate::alloc::Vec<_>>>()??;

                let ephemeral_index = Arc::new(Index {
                    columns,
                    name: format!("ephemeral_index_where_sub_{subquery_id}"),
                    table_name: String::new(),
                    ephemeral: true,
                    has_rowid: false,
                    root_page: 0,
                    unique: false,
                    where_clause: None,
                    index_method: None,
                    on_conflict: None,
                });

                let cursor_id =
                    program.alloc_cursor_id(CursorType::BTreeIndex(ephemeral_index.clone()));

                *plan.select_query_destination_mut().unwrap() = QueryDestination::EphemeralIndex {
                    cursor_id,
                    index: ephemeral_index,
                    affinity_str: Some(in_affinity_str.clone()),
                    is_delete: false,
                };
                *expr = ast::Expr::SubqueryResult {
                    subquery_id,
                    lhs: Some(lhs),
                    not_in: not,
                    query_type: SubqueryType::In {
                        cursor_id,
                        affinity_str: in_affinity_str.clone(),
                    },
                };

                out_subqueries.push(NonFromClauseSubquery {
                    internal_id: subquery_id,
                    same_query: None,
                    query_type: SubqueryType::In {
                        cursor_id,
                        affinity_str: in_affinity_str,
                    },
                    state: SubqueryState::Unevaluated {
                        plan: Some(Box::new(plan)),
                    },
                    correlated,
                    origin,
                    eval_phase: origin.phase_floor(),
                });
                Ok(WalkControl::Continue)
            }
            _ => Ok(WalkControl::Continue),
        }
    }
}

/// Recollect all aggregates after subquery planning.
///
/// Aggregates are collected during parsing with cloned expressions. When subquery planning
/// modifies expressions in place (e.g. replacing EXISTS with SubqueryResult), the aggregate's
/// cloned original_expr and args become stale. This causes cache misses during translation.
///
/// Instead of trying to sync stale clones, this function recollects all aggregates fresh
/// from the updated expressions in result_columns, HAVING, and ORDER BY.
fn recollect_aggregates(plan: &mut SelectPlan, resolver: &Resolver) -> Result<()> {
    let mut new_aggregates: Vec<Aggregate> = Vec::new();

    // Collect from result columns (same order as original collection)
    for rc in &plan.result_columns {
        resolve_window_and_aggregate_functions(
            &rc.expr,
            resolver,
            &mut new_aggregates,
            None,
            &mut [],
        )?;
    }

    // Collect from HAVING
    if let Some(group_by) = &plan.group_by {
        if let Some(having) = &group_by.having {
            for expr in having {
                resolve_window_and_aggregate_functions(
                    expr,
                    resolver,
                    &mut new_aggregates,
                    None,
                    &mut [],
                )?;
            }
        }
    }

    // Collect from ORDER BY
    for (expr, _, _) in &plan.order_by {
        resolve_window_and_aggregate_functions(expr, resolver, &mut new_aggregates, None, &mut [])?;
    }

    plan.aggregates = new_aggregates;
    Ok(())
}

/// We make decisions about when to evaluate expressions or whether to use covering indexes based on
/// which columns of a table have been referenced.
/// Since subquery nesting is arbitrarily deep, a reference to a column must propagate recursively
/// up to the parent. Example:
///
/// SELECT * FROM t WHERE EXISTS (SELECT * FROM u WHERE EXISTS (SELECT * FROM v WHERE v.foo = t.foo))
///
/// In this case, t.foo is referenced in the innermost subquery, so the top level query must be notified
/// that t.foo has been used.
fn update_column_used_masks(
    table_refs: &mut TableReferences,
    subqueries: &mut [NonFromClauseSubquery],
) -> Result<()> {
    fn propagate_outer_refs_from_select_plan(
        table_refs: &mut TableReferences,
        plan: &SelectPlan,
    ) -> Result<()> {
        for child_outer_query_ref in plan
            .table_references
            .outer_query_refs()
            .iter()
            .filter(|t| t.is_used())
        {
            if let Some(joined_table) =
                table_refs.find_joined_table_by_internal_id_mut(child_outer_query_ref.internal_id)
            {
                // Propagate column_use_counts so that expression index coverage
                // checks see the additional references from correlated subqueries.
                // Without this, apply_expression_index_coverage() may conclude that
                // all uses of a column are satisfied by an expression index when in
                // fact the correlated subquery needs the column directly.
                for col_idx in child_outer_query_ref.col_used_mask.iter() {
                    if col_idx >= joined_table.column_use_counts.len() {
                        joined_table.column_use_counts.resize(col_idx + 1, 0);
                    }
                    joined_table.column_use_counts[col_idx] += 1;
                }
                joined_table
                    .col_used_mask
                    .union_with(&child_outer_query_ref.col_used_mask)?;
            }
            if let Some(outer_query_ref) = table_refs
                .find_outer_query_ref_by_internal_id_mut(child_outer_query_ref.internal_id)
            {
                outer_query_ref
                    .col_used_mask
                    .union_with(&child_outer_query_ref.col_used_mask)?;
            }
        }

        for joined_table in plan.table_references.joined_tables().iter() {
            if let Table::FromClauseSubquery(from_clause_subquery) = &joined_table.table {
                propagate_outer_refs_from_plan(table_refs, from_clause_subquery.plan.as_ref())?;
            }
        }
        Ok(())
    }

    fn propagate_outer_refs_from_plan(table_refs: &mut TableReferences, plan: &Plan) -> Result<()> {
        match plan {
            Plan::Select(select_plan) => {
                propagate_outer_refs_from_select_plan(table_refs, select_plan)?;
            }
            Plan::CompoundSelect {
                left, right_most, ..
            } => {
                for (select_plan, _) in left.iter() {
                    propagate_outer_refs_from_select_plan(table_refs, select_plan)?;
                }
                propagate_outer_refs_from_select_plan(table_refs, right_most)?;
            }
            Plan::RecursiveCte(recursive_cte) => {
                propagate_outer_refs_from_plan(table_refs, &recursive_cte.initial_query)?;
                propagate_outer_refs_from_plan(table_refs, &recursive_cte.recursive_query)?;
            }
            Plan::Delete(_) | Plan::Update(_) => {
                return Err(crate::LimboError::InternalError(
                    "DELETE/UPDATE plans should not appear in FROM clause subqueries".into(),
                ));
            }
        }
        Ok(())
    }

    for subquery in subqueries.iter_mut() {
        let SubqueryState::Unevaluated { plan } = &mut subquery.state else {
            return Err(crate::LimboError::InternalError(
                "subquery has already been evaluated".into(),
            ));
        };
        let Some(child_plan) = plan.as_mut() else {
            return Err(crate::LimboError::InternalError(
                "subquery has no plan".into(),
            ));
        };

        propagate_outer_refs_from_plan(table_refs, child_plan)?;
    }

    // Collect raw plan pointers to avoid cloning while sidestepping borrow rules.
    let from_clause_plans = table_refs
        .joined_tables()
        .iter()
        .filter_map(|t| match &t.table {
            Table::FromClauseSubquery(from_clause_subquery) => {
                Some(from_clause_subquery.plan.as_ref() as *const Plan)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    for plan in from_clause_plans {
        // SAFETY: plans live within table_refs for the duration of this function.
        let plan = unsafe { &*plan };
        propagate_outer_refs_from_plan(table_refs, plan)?;
    }
    Ok(())
}

/// Recursively pre-materialize all multi-ref CTEs in a plan tree.
/// This must be called BEFORE emitting any coroutines to ensure CTEs referenced
/// inside coroutines have their cursors opened at the top level.
fn pre_materialize_multi_ref_ctes(
    program: &mut ProgramBuilder,
    plan: &mut Plan,
    t_ctx: &mut TranslateCtx,
) -> Result<()> {
    match plan {
        Plan::Select(select_plan) => {
            pre_materialize_multi_ref_ctes_in_select_plan(program, select_plan, t_ctx)?;
        }
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            for (select_plan, _) in left.iter_mut() {
                pre_materialize_multi_ref_ctes_in_select_plan(program, select_plan, t_ctx)?;
            }
            pre_materialize_multi_ref_ctes_in_select_plan(program, right_most, t_ctx)?;
        }
        Plan::RecursiveCte(recursive_cte) => {
            pre_materialize_multi_ref_ctes(program, &mut recursive_cte.initial_query, t_ctx)?;
            pre_materialize_multi_ref_ctes(program, &mut recursive_cte.recursive_query, t_ctx)?;
        }
        Plan::Delete(_) | Plan::Update(_) => {}
    }
    Ok(())
}

fn pre_materialize_multi_ref_ctes_in_select_plan(
    program: &mut ProgramBuilder,
    plan: &mut SelectPlan,
    t_ctx: &mut TranslateCtx,
) -> Result<()> {
    pre_materialize_multi_ref_ctes_in_tables(program, &mut plan.table_references, t_ctx)?;
    pre_materialize_multi_ref_ctes_in_non_from_subqueries(
        program,
        &mut plan.non_from_clause_subqueries,
        t_ctx,
    )
}

fn pre_materialize_multi_ref_ctes_in_non_from_subqueries(
    program: &mut ProgramBuilder,
    subqueries: &mut [NonFromClauseSubquery],
    t_ctx: &mut TranslateCtx,
) -> Result<()> {
    for subquery in subqueries.iter_mut() {
        let SubqueryState::Unevaluated {
            plan: Some(subquery_plan),
        } = &mut subquery.state
        else {
            continue;
        };
        pre_materialize_multi_ref_ctes(program, subquery_plan.as_mut(), t_ctx)?;
    }
    Ok(())
}

fn pre_materialize_multi_ref_ctes_in_tables(
    program: &mut ProgramBuilder,
    tables: &mut TableReferences,
    t_ctx: &mut TranslateCtx,
) -> Result<()> {
    for table_reference in tables.joined_tables_mut().iter_mut() {
        if let Table::FromClauseSubquery(from_clause_subquery) = &mut table_reference.table {
            let from_clause_subquery = Arc::make_mut(from_clause_subquery);
            // First, recursively process nested plans
            pre_materialize_multi_ref_ctes(program, from_clause_subquery.plan.as_mut(), t_ctx)?;

            // Then check if THIS CTE should be materialized
            if let Some(cte_id) = from_clause_subquery.cte_id() {
                if program.get_materialized_cte(cte_id).is_some() {
                    continue;
                }
                if from_clause_subquery.requires_table_materialization() {
                    tracing::trace!(
                        cte_id,
                        identifier = %table_reference.identifier,
                        "pre-materializing shared CTE"
                    );
                    let (result_columns_start, cte_cursor_id, cte_table) = program
                        .with_cte_materialization_eqp(
                            cte_id,
                            &from_clause_subquery.name,
                            |program| {
                                emit_materialized_subquery_table(
                                    program,
                                    from_clause_subquery.plan.as_mut(),
                                    t_ctx,
                                    &from_clause_subquery.columns,
                                )
                            },
                        )?;
                    program.register_materialized_cte(
                        cte_id,
                        MaterializedCteInfo {
                            cursor_id: cte_cursor_id,
                            table: cte_table,
                            num_columns: from_clause_subquery.columns.len(),
                        },
                    );
                    from_clause_subquery.materialized_cursor_id = Some(cte_cursor_id);
                    from_clause_subquery.result_columns_start_reg = Some(result_columns_start);
                    program
                        .set_subquery_result_reg(table_reference.internal_id, result_columns_start);
                }
            }
        }
    }
    Ok(())
}

/// Describe how a FROM-clause subquery reference will be executed, for
/// EXPLAIN QUERY PLAN consumers. Returns None for plain tables.
fn eqp_subquery_info(
    program: &ProgramBuilder,
    table_reference: &JoinedTable,
    execution_mode: Option<&FromClauseSubqueryExecutionMode>,
) -> Option<EqpSubquery> {
    let Table::FromClauseSubquery(from_clause_subquery) = &table_reference.table else {
        return None;
    };
    let cte_id = from_clause_subquery.cte_id();
    let already_materialized = cte_id.is_some_and(|id| program.get_materialized_cte(id).is_some());
    let exec = if already_materialized {
        EqpSubqueryExec::MaterializedReuse
    } else {
        match execution_mode {
            Some(FromClauseSubqueryExecutionMode::Coroutine) | None => EqpSubqueryExec::Coroutine,
            Some(FromClauseSubqueryExecutionMode::MaterializedTable) => {
                EqpSubqueryExec::Materialized
            }
            Some(FromClauseSubqueryExecutionMode::DirectMaterializedIndex(_)) => {
                EqpSubqueryExec::IndexedMaterialized
            }
        }
    };
    Some(EqpSubquery {
        exec,
        cte_id,
        recursive: matches!(from_clause_subquery.plan.as_ref(), Plan::RecursiveCte(_)),
    })
}

fn choose_from_clause_subquery_execution_mode(
    operation: &Operation,
    from_clause_subquery: &crate::schema::FromClauseSubquery,
) -> FromClauseSubqueryExecutionMode {
    let needs_materialized_seek = matches!(
        operation,
        Operation::Search(Search::Seek {
            index: Some(index), ..
        }) if index.ephemeral
    );

    // Compound SELECTs still need their own internal ephemeral indexes for
    // UNION/INTERSECT/EXCEPT bookkeeping. Reusing the subquery's synthesized
    // seek index as the storage target would collapse those roles together and
    // break set-operation semantics, so keep the direct-index fast path limited
    // to simple SELECT plans.
    let can_direct_materialize_index = from_clause_subquery.supports_direct_index_materialization();

    match operation {
        Operation::Search(Search::Seek {
            index: Some(index),
            seek_def,
        }) if index.ephemeral && can_direct_materialize_index => {
            FromClauseSubqueryExecutionMode::DirectMaterializedIndex(DirectMaterializedSubquery {
                index: index.clone(),
                affinity_str: super::plan::synthesized_seek_affinity_str(index, seek_def),
            })
        }
        _ if needs_materialized_seek => FromClauseSubqueryExecutionMode::MaterializedTable,
        _ if from_clause_subquery.requires_table_materialization() => {
            FromClauseSubqueryExecutionMode::MaterializedTable
        }
        _ => FromClauseSubqueryExecutionMode::Coroutine,
    }
}

/// Emit the subqueries contained in the FROM clause.
/// This is done first so the results can be read in the main query loop.
pub fn emit_from_clause_subqueries(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &mut TableReferences,
    join_order: &[JoinOrderMember],
) -> Result<()> {
    if tables.joined_tables().is_empty() {
        emit_explain!(program, false, EqpDetail::ConstantRow);
    }

    // FIRST PASS: Pre-materialize all recursively reachable multi-ref / hinted CTEs
    // before any coroutine bodies are emitted. Otherwise a coroutine could try to
    // OpenDup a CTE whose backing table has not been created yet.
    pre_materialize_multi_ref_ctes_in_tables(program, tables, t_ctx)?;

    // Build the iteration order: join_order first (execution order), then any
    // hash-join build tables that aren't already in the join order.
    let mut visit_order: Vec<usize> = join_order
        .iter()
        .map(|member| member.original_idx)
        .collect();
    let visit_set: TableMask = visit_order.iter().copied().try_collect()?;
    for table in tables.joined_tables().iter() {
        if let Operation::HashJoin(hash_join_op) = &table.op {
            let build_idx = hash_join_op.build_table_idx;
            if !visit_set.get(build_idx) {
                visit_order.push(build_idx);
            }
        }
    }

    // Build lookup from table index to is_outer for LEFT-JOIN annotations
    let outer_table_set: TableMask = join_order
        .iter()
        .filter(|m| m.is_outer)
        .map(|m| m.original_idx)
        .try_collect()?;

    for table_index in visit_order {
        let table_reference = &mut tables.joined_tables_mut()[table_index];
        let execution_mode = match &table_reference.table {
            Table::FromClauseSubquery(from_clause_subquery) => {
                Some(choose_from_clause_subquery_execution_mode(
                    &table_reference.op,
                    from_clause_subquery.as_ref(),
                ))
            }
            _ => None,
        };
        let eqp_subquery = eqp_subquery_info(program, table_reference, execution_mode.as_ref());
        emit_explain!(
            program,
            true,
            eqp_detail_for_table_op(
                table_reference,
                EqpJoin::from_join_info(
                    table_reference.join_info.as_ref(),
                    outer_table_set.get(table_index),
                ),
                eqp_subquery,
            )
        );

        if let Table::FromClauseSubquery(from_clause_subquery) = &mut table_reference.table {
            let execution_mode =
                execution_mode.expect("execution mode was computed above for subquery tables");
            let from_clause_subquery = Arc::make_mut(from_clause_subquery);
            // Check if this is a CTE that's already materialized
            if let Some(cte_id) = from_clause_subquery.cte_id() {
                if let Some(cte_info) = program.get_materialized_cte(cte_id).cloned() {
                    if from_clause_subquery.materialized_cursor_id.is_some() {
                        tracing::trace!(
                            cte_id,
                            identifier = %table_reference.identifier,
                            "reusing pre-materialized CTE on original reference"
                        );
                        program.pop_current_parent_explain();
                        continue;
                    }
                    // === SUBSEQUENT CTE REFERENCE: Use OpenDup ===
                    // Create a dup cursor pointing to the same ephemeral table
                    let dup_cursor_id =
                        program.alloc_cursor_id(CursorType::BTreeTable(cte_info.table.clone()));
                    program.emit_insn(Insn::OpenDup {
                        new_cursor_id: dup_cursor_id,
                        original_cursor_id: cte_info.cursor_id,
                    });
                    tracing::trace!(
                        cte_id,
                        identifier = %table_reference.identifier,
                        original_cursor_id = cte_info.cursor_id,
                        dup_cursor_id,
                        "opening duplicate cursor for materialized CTE"
                    );

                    // Update the plan's query destination to EphemeralTable so that
                    // main_loop knows to use Rewind/Next instead of coroutine Yield
                    if let Some(dest) = from_clause_subquery.plan.select_query_destination_mut() {
                        *dest = QueryDestination::EphemeralTable {
                            cursor_id: dup_cursor_id,
                            table: cte_info.table.clone(),
                            rowid_mode: super::plan::EphemeralRowidMode::Auto,
                        };
                    }

                    // Each CTE reference needs its OWN registers to read column values into.
                    // We cannot share the original's result_columns_start_reg because multiple
                    // iterators of the same CTE (e.g., outer query and subquery) would
                    // overwrite each other's values when reading columns from their cursors.
                    let result_columns_start = program.alloc_registers(cte_info.num_columns);
                    from_clause_subquery.materialized_cursor_id = Some(dup_cursor_id);
                    from_clause_subquery.result_columns_start_reg = Some(result_columns_start);
                    program
                        .set_subquery_result_reg(table_reference.internal_id, result_columns_start);
                    program.pop_current_parent_explain();
                    continue; // Skip normal emission
                }
            }

            let result_columns_start = match execution_mode {
                FromClauseSubqueryExecutionMode::Coroutine => Some(emit_from_clause_subquery(
                    program,
                    from_clause_subquery.plan.as_mut(),
                    t_ctx,
                )?),
                FromClauseSubqueryExecutionMode::MaterializedTable => {
                    let (result_columns_start, cte_cursor_id, cte_table) =
                        emit_materialized_subquery_table(
                            program,
                            from_clause_subquery.plan.as_mut(),
                            t_ctx,
                            &from_clause_subquery.columns,
                        )?;
                    from_clause_subquery.materialized_cursor_id = Some(cte_cursor_id);
                    if let Some(cte_id) = from_clause_subquery.cte_id() {
                        program.register_materialized_cte(
                            cte_id,
                            MaterializedCteInfo {
                                cursor_id: cte_cursor_id,
                                table: cte_table,
                                num_columns: from_clause_subquery.columns.len(),
                            },
                        );
                    }
                    Some(result_columns_start)
                }
                FromClauseSubqueryExecutionMode::DirectMaterializedIndex(direct_index) => {
                    emit_indexed_materialized_subquery(
                        program,
                        from_clause_subquery.plan.as_mut(),
                        t_ctx,
                        table_reference.internal_id,
                        direct_index.index,
                        direct_index.affinity_str,
                    )?;
                    None
                }
            };

            from_clause_subquery.result_columns_start_reg = result_columns_start;
            if let Some(result_columns_start) = result_columns_start {
                program.set_subquery_result_reg(table_reference.internal_id, result_columns_start);
            }
        }

        program.pop_current_parent_explain();
    }
    Ok(())
}

/// Emit a FROM clause subquery and return the start register of the result columns.
/// This is done by emitting a coroutine that stores the result columns in sequential registers.
/// Each FROM clause subquery has its own Plan (either SelectPlan or CompoundSelect) which is wrapped in a coroutine.
///
/// The resulting bytecode from a subquery is mostly exactly the same as a regular query, except:
/// - it ends in an EndCoroutine instead of a Halt.
/// - instead of emitting ResultRows, the coroutine yields to the main query loop.
/// - the first register of the result columns is returned to the parent query,
///   so that translate_expr() can read the result columns of the subquery,
///   as if it were reading from a regular table.
///
/// Since a subquery has its own Plan, it can contain nested subqueries,
/// which can contain even more nested subqueries, etc.
pub fn emit_from_clause_subquery(
    program: &mut ProgramBuilder,
    plan: &mut Plan,
    t_ctx: &mut TranslateCtx,
) -> Result<usize> {
    let yield_reg = program.alloc_register();
    let coroutine_implementation_start_offset = program.allocate_label();

    // Set up the coroutine yield destination for the plan
    match plan.select_query_destination_mut() {
        Some(QueryDestination::CoroutineYield {
            yield_reg: y,
            coroutine_implementation_start,
        }) => {
            // The parent query will use this register to jump to/from the subquery.
            *y = yield_reg;
            // The parent query will use this register to reinitialize the coroutine when it needs to run multiple times.
            *coroutine_implementation_start = coroutine_implementation_start_offset;
        }
        _ => unreachable!("emit_from_clause_subquery called on non-subquery"),
    }

    let subquery_body_end_label = program.allocate_label();

    program.emit_insn(Insn::InitCoroutine {
        yield_reg,
        jump_on_definition: subquery_body_end_label,
        start_offset: coroutine_implementation_start_offset,
    });
    program.preassign_label_to_next_insn(coroutine_implementation_start_offset);

    // Coroutine bodies may be re-invoked from an outer loop (e.g. as the inner
    // side of a LEFT JOIN). Emit under `nested()` so that HashClose for any
    // hash join inside the body is deferred to statement teardown; otherwise
    // the second invocation would find the hash table already removed and
    // produce no matches. The hash build itself is guarded by Once and
    // therefore correctly persists across re-invocations.
    let result_column_start_reg = program.nested(|program| -> Result<usize> {
        Ok(match plan {
            Plan::Select(select_plan) => {
                let mut metadata = Box::new(TranslateCtx {
                    labels_main_loop: (0..select_plan.joined_tables().len())
                        .map(|_| LoopLabels::new(program))
                        .collect(),
                    label_main_loop_end: None,
                    meta_group_by: None,
                    meta_left_joins: (0..select_plan.joined_tables().len())
                        .map(|_| None)
                        .collect(),
                    meta_semi_anti_joins: (0..select_plan.joined_tables().len())
                        .map(|_| None)
                        .collect(),
                    meta_sort: None,
                    reg_agg_start: None,
                    reg_nonagg_emit_once_flag: None,
                    reg_result_cols_start: None,
                    limit_ctx: None,
                    reg_offset: None,
                    reg_limit_offset_sum: None,
                    resolver: t_ctx.resolver.fork(),
                    non_aggregate_expressions: Vec::new(),
                    agg_leaf_columns: Vec::new(),
                    cdc_cursor_id: None,
                    meta_window: None,
                    meta_in_seeks: (0..select_plan.joined_tables().len())
                        .map(|_| None)
                        .collect(),
                    materialized_build_inputs: HashMap::default(),
                    hash_table_contexts: HashMap::default(),
                    unsafe_testing: t_ctx.unsafe_testing,
                });
                metadata.materialized_build_inputs =
                    emit_materialized_build_inputs(program, &metadata.resolver, select_plan)?;
                emit_query(program, select_plan, &mut metadata)?
            }
            Plan::CompoundSelect { .. } => {
                let resolver = t_ctx.resolver.fork();
                // emit_program_for_compound_select returns the result column start register
                // for coroutine mode, which is needed by the outer query.
                emit_program_for_compound_select(program, &resolver, plan)?
                    .expect("compound CTE in coroutine mode must have result register")
            }
            Plan::RecursiveCte(recursive_cte) => {
                super::recursive_cte::emit_recursive_cte(program, &t_ctx.resolver, recursive_cte)?
            }
            Plan::Delete(_) | Plan::Update(_) => {
                unreachable!("DELETE/UPDATE plans cannot be FROM clause subqueries")
            }
        })
    })?;

    program.emit_insn(Insn::EndCoroutine { yield_reg });
    program.preassign_label_to_next_insn(subquery_body_end_label);
    Ok(result_column_start_reg)
}
/// Materialize a single-reference seekable FROM-subquery directly into an
/// ephemeral index.
///
/// This skips the intermediate EphemeralTable when we only need seek access and do
/// not need table-backed sharing via OpenDup. Result columns for this path are read
/// back from the index using `pos_in_table` mapping rather than raw index position.
fn emit_indexed_materialized_subquery(
    program: &mut ProgramBuilder,
    plan: &mut Plan,
    t_ctx: &mut TranslateCtx,
    internal_id: ast::TableInternalId,
    index: Arc<Index>,
    affinity_str: Option<Arc<String>>,
) -> Result<()> {
    let cursor_id = program
        .alloc_cursor_index_if_not_exists(CursorKey::index(internal_id, index.clone()), &index)?;

    if let Some(dest) = plan.select_query_destination_mut() {
        *dest = QueryDestination::EphemeralIndex {
            cursor_id,
            index,
            affinity_str,
            is_delete: false,
        };
    }

    let build_end = if plan_is_correlated(plan) {
        None
    } else {
        let label = program.allocate_label();
        program.emit_insn(Insn::Once {
            target_pc_when_reentered: label,
        });
        Some(label)
    };
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id,
        is_table: false,
    });

    match plan {
        Plan::Select(select_plan) => {
            let mut metadata = Box::new(TranslateCtx {
                labels_main_loop: (0..select_plan.joined_tables().len())
                    .map(|_| LoopLabels::new(program))
                    .collect(),
                label_main_loop_end: None,
                meta_group_by: None,
                meta_left_joins: (0..select_plan.joined_tables().len())
                    .map(|_| None)
                    .collect(),
                meta_semi_anti_joins: (0..select_plan.joined_tables().len())
                    .map(|_| None)
                    .collect(),
                meta_sort: None,
                reg_agg_start: None,
                reg_nonagg_emit_once_flag: None,
                reg_result_cols_start: None,
                limit_ctx: None,
                reg_offset: None,
                reg_limit_offset_sum: None,
                resolver: t_ctx.resolver.fork(),
                non_aggregate_expressions: Vec::new(),
                agg_leaf_columns: Vec::new(),
                cdc_cursor_id: None,
                meta_window: None,
                meta_in_seeks: (0..select_plan.joined_tables().len())
                    .map(|_| None)
                    .collect(),
                materialized_build_inputs: HashMap::default(),
                hash_table_contexts: HashMap::default(),
                unsafe_testing: t_ctx.unsafe_testing,
            });
            metadata.materialized_build_inputs =
                emit_materialized_build_inputs(program, &metadata.resolver, select_plan)?;
            emit_query(program, select_plan, &mut metadata)?;
        }
        Plan::CompoundSelect { .. } => {
            let resolver = t_ctx.resolver.fork();
            emit_program_for_compound_select(program, &resolver, plan)?;
        }
        Plan::RecursiveCte(_) => {
            unreachable!("recursive CTEs require table-backed materialization for indexed access")
        }
        Plan::Delete(_) | Plan::Update(_) => {
            unreachable!("DELETE/UPDATE plans cannot be FROM clause subqueries")
        }
    }

    if let Some(build_end) = build_end {
        program.preassign_label_to_next_insn(build_end);
    }

    Ok(())
}

fn emit_materialized_subquery_table(
    program: &mut ProgramBuilder,
    plan: &mut Plan,
    t_ctx: &mut TranslateCtx,
    columns: &[Column],
) -> Result<(usize, CursorID, Arc<BTreeTable>)> {
    use super::plan::EphemeralRowidMode;

    // EphemeralTable (not EphemeralIndex) is required because it preserves
    // insertion order, which SQL semantics require for UNION ALL. It also
    // needs the subquery's column layout so later Column opcodes can read
    // materialized rows through the normal table-cursor path.
    let ephemeral_table = Arc::new(BTreeTable::new(
        0,
        String::new(),
        crate::alloc::vec![],
        columns.try_to_vec()?,
        BTreeCharacteristics::HAS_ROWID,
        crate::alloc::vec![],
        crate::alloc::vec![],
        crate::alloc::vec![],
        None,
    ));

    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(ephemeral_table.clone()));

    // Allocate registers for reading result columns
    let result_columns_start_reg = program.alloc_registers(columns.len());

    let build_end = if plan_is_correlated(plan) {
        None
    } else {
        let label = program.allocate_label();
        // A correlated parent query can reach this code more than once.
        // SQLite keeps an uncorrelated FROM source for the full statement.
        program.emit_insn(Insn::Once {
            target_pc_when_reentered: label,
        });
        Some(label)
    };

    // Open the ephemeral table
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id,
        is_table: true,
    });

    // Set the query destination to write to the ephemeral table
    if let Some(dest) = plan.select_query_destination_mut() {
        *dest = QueryDestination::EphemeralTable {
            cursor_id,
            table: ephemeral_table.clone(),
            rowid_mode: EphemeralRowidMode::Auto,
        };
    }

    // Emit the subquery - it will insert rows into the ephemeral table
    match plan {
        Plan::Select(select_plan) => {
            let mut metadata = Box::new(TranslateCtx {
                labels_main_loop: (0..select_plan.joined_tables().len())
                    .map(|_| LoopLabels::new(program))
                    .collect(),
                label_main_loop_end: None,
                meta_group_by: None,
                meta_left_joins: (0..select_plan.joined_tables().len())
                    .map(|_| None)
                    .collect(),
                meta_semi_anti_joins: (0..select_plan.joined_tables().len())
                    .map(|_| None)
                    .collect(),
                meta_sort: None,
                reg_agg_start: None,
                reg_nonagg_emit_once_flag: None,
                reg_result_cols_start: None,
                limit_ctx: None,
                reg_offset: None,
                reg_limit_offset_sum: None,
                resolver: t_ctx.resolver.fork(),
                non_aggregate_expressions: Vec::new(),
                agg_leaf_columns: Vec::new(),
                cdc_cursor_id: None,
                meta_window: None,
                meta_in_seeks: (0..select_plan.joined_tables().len())
                    .map(|_| None)
                    .collect(),
                materialized_build_inputs: HashMap::default(),
                hash_table_contexts: HashMap::default(),
                unsafe_testing: t_ctx.unsafe_testing,
            });
            metadata.materialized_build_inputs =
                emit_materialized_build_inputs(program, &metadata.resolver, select_plan)?;
            emit_query(program, select_plan, &mut metadata)?;
        }
        Plan::CompoundSelect { .. } => {
            let resolver = t_ctx.resolver.fork();
            emit_program_for_compound_select(program, &resolver, plan)?;
        }
        Plan::RecursiveCte(recursive_cte) => {
            super::recursive_cte::emit_recursive_cte(program, &t_ctx.resolver, recursive_cte)?;
        }
        Plan::Delete(_) | Plan::Update(_) => {
            unreachable!("DELETE/UPDATE plans cannot be FROM clause subqueries")
        }
    }

    if let Some(build_end) = build_end {
        program.preassign_label_to_next_insn(build_end);
    }

    Ok((result_columns_start_reg, cursor_id, ephemeral_table))
}

/// Translate a subquery that is not part of the FROM clause.
/// If a subquery is uncorrelated (i.e. does not reference columns from the outer query),
/// it will be executed only once.
///
/// If it is correlated (i.e. references columns from the outer query),
/// it will be executed for each row of the outer query.
///
/// The result of the subquery is stored in:
///
/// - a single register for EXISTS subqueries,
/// - a range of registers for RowValue subqueries,
/// - an ephemeral index for IN subqueries.
pub fn emit_non_from_clause_subquery(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    plan: Plan,
    query_type: &SubqueryType,
    is_correlated: bool,
    preserve_outer_expr_cache: bool,
) -> Result<()> {
    program.nested(|program| {
        let subquery_id = program.next_subquery_eqp_id();
        match query_type {
            SubqueryType::Exists { .. } => {
                // EXISTS subqueries don't get a separate EQP annotation in SQLite;
                // instead the SEARCH/SCAN line gets an "EXISTS" suffix handled elsewhere.
            }
            SubqueryType::In { .. } => {
                emit_explain!(
                    program,
                    true,
                    EqpDetail::ListSubquery {
                        id: subquery_id,
                        correlated: is_correlated,
                    }
                );
            }
            SubqueryType::RowValue { .. } => {
                emit_explain!(
                    program,
                    true,
                    EqpDetail::ScalarSubquery {
                        id: subquery_id,
                        correlated: is_correlated,
                    }
                );
            }
        }

        let label_skip_after_first_run = if !is_correlated {
            let label = program.allocate_label();
            program.emit_insn(Insn::Once {
                target_pc_when_reentered: label,
            });
            Some(label)
        } else {
            None
        };

        // Helper closure to emit a select plan (simple or compound). The
        // closure captures `resolver`, `plan`, and `preserve_outer_expr_cache`
        // from the enclosing scope; only `program` is passed explicitly so
        // that the outer scope can keep emitting instructions in between.
        // Called at most once, hence `FnOnce`.
        let emit_plan = move |program: &mut ProgramBuilder| -> Result<()> {
            match plan {
                Plan::Select(select_plan) => {
                    if preserve_outer_expr_cache {
                        emit_program_for_select_with_resolver(
                            program,
                            resolver.fork_with_expr_cache(),
                            *select_plan,
                        )
                    } else {
                        emit_program_for_select(program, resolver, *select_plan)
                    }
                }
                mut compound @ Plan::CompoundSelect { .. } => {
                    emit_program_for_compound_select(program, resolver, &mut compound)?;
                    Ok(())
                }
                _ => unreachable!("DML plans cannot be subqueries"),
            }
        };

        match query_type {
            SubqueryType::Exists { result_reg, .. } => {
                let subroutine_reg = program.alloc_register();
                program.emit_insn(Insn::BeginSubrtn {
                    dest: subroutine_reg,
                    dest_end: None,
                });
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: *result_reg,
                });
                emit_plan(program)?;
                program.emit_insn(Insn::Return {
                    return_reg: subroutine_reg,
                    can_fallthrough: true,
                });
            }
            SubqueryType::In { cursor_id, .. } => {
                program.emit_insn(Insn::OpenEphemeral {
                    cursor_id: *cursor_id,
                    is_table: false,
                });
                emit_plan(program)?;
            }
            SubqueryType::RowValue {
                result_reg_start,
                num_regs,
            } => {
                let subroutine_reg = program.alloc_register();
                program.emit_insn(Insn::BeginSubrtn {
                    dest: subroutine_reg,
                    dest_end: None,
                });
                for result_reg in *result_reg_start..*result_reg_start + *num_regs {
                    program.emit_insn(Insn::Null {
                        dest: result_reg,
                        dest_end: None,
                    });
                }
                emit_plan(program)?;
                program.emit_insn(Insn::Return {
                    return_reg: subroutine_reg,
                    can_fallthrough: true,
                });
            }
        }
        // Pop the parent explain for LIST/SCALAR SUBQUERY annotations.
        if !matches!(query_type, SubqueryType::Exists { .. }) {
            program.pop_current_parent_explain();
        }
        if let Some(label) = label_skip_after_first_run {
            program.preassign_label_to_next_insn(label);
        }
        Ok(())
    })
}

pub fn emit_non_from_clause_subqueries_for_phase(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    subqueries: &mut [NonFromClauseSubquery],
    join_order: &[JoinOrderMember],
    table_references: Option<&TableReferences>,
    phase: SubqueryEvalPhase,
    mut should_emit: impl FnMut(&NonFromClauseSubquery) -> bool,
) -> Result<()> {
    for subquery in subqueries.iter_mut() {
        if subquery.has_been_evaluated() || !should_emit(subquery) {
            continue;
        }

        let evaluated_at = match phase {
            SubqueryEvalPhase::BeforeLoop | SubqueryEvalPhase::Loop(_) => {
                if !matches!(subquery.eval_phase, SubqueryEvalPhase::BeforeLoop) {
                    continue;
                }
                let expected_eval_at = match phase {
                    SubqueryEvalPhase::BeforeLoop => EvalAt::BeforeLoop,
                    SubqueryEvalPhase::Loop(loop_idx) => EvalAt::Loop(loop_idx),
                    _ => unreachable!(),
                };
                let evaluated_at = subquery.get_eval_at(join_order, table_references)?;
                if evaluated_at != expected_eval_at {
                    continue;
                }
                evaluated_at
            }
            _ => {
                if subquery.eval_phase != phase {
                    continue;
                }
                subquery.get_eval_at(join_order, table_references)?
            }
        };

        let subquery_plan = subquery.consume_plan(evaluated_at);
        emit_non_from_clause_subquery(
            program,
            resolver,
            *subquery_plan,
            &subquery.query_type,
            subquery.correlated,
            !matches!(
                phase,
                SubqueryEvalPhase::BeforeLoop | SubqueryEvalPhase::Loop(_)
            ),
        )?;
    }

    Ok(())
}

pub fn emit_non_from_clause_subqueries_for_eval_at(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    subqueries: &mut [NonFromClauseSubquery],
    join_order: &[JoinOrderMember],
    table_references: Option<&TableReferences>,
    eval_at: EvalAt,
    should_emit: impl FnMut(&NonFromClauseSubquery) -> bool,
) -> Result<()> {
    emit_non_from_clause_subqueries_for_phase(
        program,
        resolver,
        subqueries,
        join_order,
        table_references,
        match eval_at {
            EvalAt::BeforeLoop => SubqueryEvalPhase::BeforeLoop,
            EvalAt::Loop(loop_idx) => SubqueryEvalPhase::Loop(loop_idx),
        },
        should_emit,
    )
}

fn assign_select_subquery_eval_phases(plan: &mut SelectPlan) {
    let has_grouped_output = plan
        .group_by
        .as_ref()
        .is_some_and(|group_by| !group_by.exprs.is_empty());

    // Subqueries inside an aggregate's arguments or FILTER clause are evaluated
    // per input row by the aggregate step code in the main loop, even when the
    // aggregate itself belongs to HAVING or ORDER BY. Deferring them to the
    // grouped output subroutine would emit their materialization after their
    // first use, so they must keep their phase floor (issue #6807).
    let mut aggregate_subquery_ids: Vec<ast::TableInternalId> = Vec::new();
    for agg in &plan.aggregates {
        for expr in agg.args.iter().chain(agg.filter_expr.iter()) {
            walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
                if let ast::Expr::SubqueryResult { subquery_id, .. } = e {
                    aggregate_subquery_ids.push(*subquery_id);
                }
                Ok(WalkControl::Continue)
            })
            .expect("walking an expression with an infallible visitor cannot fail");
        }
    }

    let mut outer_aggregate_subquery_ids: Vec<ast::TableInternalId> = Vec::new();
    for subquery in plan.non_from_clause_subqueries.iter_mut() {
        // A subquery that reads an aggregate the outer query computes must run
        // in the outer query's aggregate-output phase, after that aggregate has
        // been finalized into a register; run earlier, the aggregate expression
        // inside the subquery has no register to read.
        if subquery_reads_outer_aggregate(subquery) {
            subquery.eval_phase = if has_grouped_output {
                SubqueryEvalPhase::GroupedOutput
            } else {
                SubqueryEvalPhase::UngroupedAggregateOutput
            };
            outer_aggregate_subquery_ids.push(subquery.internal_id);
            continue;
        }
        subquery.eval_phase = match subquery.origin {
            SubqueryOrigin::SelectHaving | SubqueryOrigin::SelectOrderBy
                if has_grouped_output
                    && !aggregate_subquery_ids.contains(&subquery.internal_id) =>
            {
                SubqueryEvalPhase::GroupedOutput
            }
            _ => subquery.origin.phase_floor(),
        };
    }

    // A result column that reads one of these subqueries depends on an
    // aggregate computed in the output phase, so it must be evaluated there
    // rather than once per input row — the same treatment SQLite gives any
    // result-column expression that references an aggregate result (select.c
    // evaluates those after the aggregation loop, in the output step). Mark it
    // so the emitter defers it, instead of taking an arbitrary row's NULL
    // during the scan.
    for rc in plan.result_columns.iter_mut() {
        if !rc.contains_aggregates && expr_reads_subquery(&rc.expr, &outer_aggregate_subquery_ids) {
            rc.contains_aggregates = true;
        }
    }
}

/// True when `expr` reads one of the given subqueries.
fn expr_reads_subquery(expr: &ast::Expr, subquery_ids: &[ast::TableInternalId]) -> bool {
    let mut found = false;
    let _ = walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        if let ast::Expr::SubqueryResult { subquery_id, .. } = e {
            if subquery_ids.contains(subquery_id) {
                found = true;
                return Ok(WalkControl::SkipChildren);
            }
        }
        Ok(WalkControl::Continue)
    });
    found
}

/// True when a result column of the subquery contains an aggregate whose value
/// the outer query computes: a plain aggregate call that is not one of the
/// subquery's own aggregates, because its argument referenced an outer column
/// and it was moved to the outer query during resolution. The subquery reads
/// that aggregate back from the register the outer query finalizes it into.
///
/// Window functions (an aggregate name with an `OVER` clause) are computed by
/// the subquery itself, not the outer query, so they are not counted here even
/// though they also do not appear in `aggregates`.
fn subquery_reads_outer_aggregate(subquery: &NonFromClauseSubquery) -> bool {
    let SubqueryState::Unevaluated { plan: Some(plan) } = &subquery.state else {
        return false;
    };
    let Plan::Select(select) = plan.as_ref() else {
        return false;
    };
    let mut found = false;
    for rc in &select.result_columns {
        let _ = walk_expr(&rc.expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
            if let ast::Expr::FunctionCall {
                name,
                args,
                filter_over,
                ..
            } = e
            {
                let is_plain_aggregate = filter_over.over_clause.is_none()
                    && matches!(
                        crate::function::Func::resolve_function(name.as_str(), args.len()),
                        Ok(Some(crate::function::Func::Agg(_)))
                    );
                if is_plain_aggregate && !select.aggregates.iter().any(|a| a.original_expr == *e) {
                    found = true;
                    return Ok(WalkControl::SkipChildren);
                }
            }
            Ok(WalkControl::Continue)
        });
        if found {
            break;
        }
    }
    found
}
