use crate::schema::{BTreeTable, Table};
use crate::sync::Arc;
use crate::translate::emitter::{emit_program, Resolver};
use crate::translate::expr::{process_returning_clause, walk_expr, WalkControl};
use crate::translate::optimizer::optimize_plan;
use crate::translate::plan::{
    DeletePlan, DmlSafety, DmlSafetyReason, IterationDirection, JoinOrderMember, Operation, Plan,
    QueryDestination, ResultSetColumn, Scan, SelectPlan,
};
use crate::translate::planner::{parse_where, plan_ctes_as_outer_refs};
use crate::translate::subquery::{
    plan_subqueries_from_returning, plan_subqueries_from_select_plan,
    plan_subqueries_from_where_clause,
};
use crate::translate::trigger_exec::has_triggers_including_temp;
use crate::util::normalize_ident;
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts};
use crate::Result;
use smallvec::SmallVec;
use turso_parser::ast::{Expr, QualifiedName, RefAct, ResultColumn, TriggerEvent, With};

use super::plan::{ColumnUsedMask, JoinedTable, TableReferences, WhereTerm};

// validate the delete statment, returning the underlying table if validation passes
fn validate_delete(
    resolver: &Resolver,
    tbl_name: &str,
    qualified_name: &QualifiedName,
    database_id: usize,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<Arc<Table>> {
    // Check if this is a system table that should be protected from direct writes
    if !connection.is_nested_stmt()
        && !connection.is_mvcc_bootstrap_connection()
        && !crate::schema::allow_user_dml(tbl_name)
    {
        crate::bail_parse_error!("table {tbl_name} may not be modified");
    }
    let table = match resolver.with_schema(database_id, |s| s.get_table(tbl_name)) {
        Some(table) => table,
        None => crate::bail_parse_error!(
            "no such table: {}",
            crate::util::table_name_for_error(qualified_name)
        ),
    };
    if program.trigger.is_some() && table.virtual_table().is_some() {
        crate::bail_parse_error!("unsafe use of virtual table \"{}\"", tbl_name);
    }
    if table.btree().is_some_and(|bt| !bt.has_rowid) {
        crate::bail_parse_error!("DELETE from WITHOUT ROWID tables is not supported");
    }

    // Check if this is a materialized view
    if resolver.schema().is_materialized_view(tbl_name) {
        crate::bail_parse_error!("cannot modify materialized view {}", tbl_name);
    }

    // Check if this table has any incompatible dependent views
    resolver.schema().with_incompatible_dependent_views(tbl_name, |views| {
    if !views.is_empty() {
        use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
        crate::bail_parse_error!(
            "Cannot DELETE from table '{tbl_name}' because it has incompatible dependent materialized view(s): {}. \n\
             These views were created with a different DBSP version than the current version ({DBSP_CIRCUIT_VERSION}). \n\
             Please DROP and recreate the view(s) before modifying this table.",
            views.iter().fold(String::new(), |_, s| s.to_string() + ", "),
        );
    }
    // Pins the closure's error type: bail_parse_error! is polymorphic over
    // boxed and unboxed LimboError since the InsnResult migration.
    Ok::<(), crate::LimboError>(())
    })?;
    Ok(table)
}

#[allow(clippy::too_many_arguments)]
#[turso_macros::trace_stack]
pub fn translate_delete(
    tbl_name: &QualifiedName,
    resolver: &Resolver,
    where_clause: Option<Box<Expr>>,
    returning: Vec<ResultColumn>,
    indexed: Option<turso_parser::ast::Indexed>,
    with: Option<With>,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    let database_id = resolver.resolve_existing_table_database_id_qualified(tbl_name)?;
    let normalized_table_name = normalize_ident(tbl_name.name.as_str());
    let table = validate_delete(
        resolver,
        &normalized_table_name,
        tbl_name,
        database_id,
        program,
        connection,
    )?;

    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;

    let mut delete_plan = prepare_delete_plan(
        program,
        resolver,
        tbl_name,
        table,
        where_clause,
        returning,
        indexed,
        with,
        connection,
        database_id,
    )?;

    // Plan subqueries in the WHERE clause
    if let Plan::Delete(ref mut delete_plan_inner) = delete_plan {
        if let Some(ref mut rowset_plan) = delete_plan_inner.rowset_plan {
            // When using rowset (triggers or subqueries present), subqueries are in the rowset_plan's WHERE
            plan_subqueries_from_select_plan(program, rowset_plan, resolver, connection)?;
        } else {
            // Normal path: subqueries are in the DELETE plan's WHERE
            plan_subqueries_from_where_clause(
                program,
                &mut delete_plan_inner.non_from_clause_subqueries,
                &mut delete_plan_inner.table_references,
                &mut delete_plan_inner.where_clause,
                resolver,
                connection,
            )?;
        }
    }

    optimize_plan(program, &mut delete_plan, resolver)?;
    if let Plan::Delete(delete_plan_inner) = &mut delete_plan {
        // Re-check after optimization: chosen access paths can make "delete while scanning"
        // unsafe, so we may need to collect rowids first.
        record_delete_optimizer_safety(delete_plan_inner);
        if delete_plan_inner.safety.requires_stable_write_set() {
            ensure_delete_uses_rowset(program, delete_plan_inner);
        }

        // Rewrite the Delete plan after optimization whenever a RowSet is used (trigger/subquery
        // safety or optimizer-induced safety), so the joined table is treated as a plain table
        // scan again.
        //
        // RowSets re-seek the base table cursor for every delete, so expressions that reference
        // columns during index maintenance must bind to the table cursor again (not the index we
        // originally used to find the rowids).
        //
        // e.g. DELETE using idx_x gathers rowids, but BEFORE DELETE trigger causes re-seek on
        // table, so expression indexes must read from that table cursor.
        if delete_plan_inner.rowset_plan.is_some() {
            if let Some(joined_table) = delete_plan_inner
                .table_references
                .joined_tables_mut()
                .first_mut()
            {
                if matches!(joined_table.table, Table::BTree(_)) {
                    joined_table.op = Operation::Scan(Scan::BTreeTable {
                        iter_dir: IterationDirection::Forwards,
                        index: None,
                    });
                }
            }
        }
    }
    let Plan::Delete(ref delete) = delete_plan else {
        panic!("delete_plan is not a DeletePlan");
    };
    super::stmt_journal::set_delete_stmt_journal_flags(
        program,
        delete,
        resolver,
        connection,
        database_id,
    )?;
    let opts = ProgramBuilderOpts::new(1, estimate_num_instructions(delete), 0);
    program.extend(&opts);
    emit_program(connection, resolver, program, delete_plan, |_| {})?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[turso_macros::trace_stack]
pub fn prepare_delete_plan(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    qualified_name: &QualifiedName,
    table: Arc<Table>,
    where_clause: Option<Box<Expr>>,
    mut returning: Vec<ResultColumn>,
    indexed: Option<turso_parser::ast::Indexed>,
    with: Option<With>,
    connection: &Arc<crate::Connection>,
    database_id: usize,
) -> Result<Plan> {
    let schema = resolver.schema();

    let btree_table_for_triggers = table.btree();
    let table = if let Some(table) = table.virtual_table() {
        Table::Virtual(table)
    } else if let Some(table) = table.btree() {
        Table::BTree(table)
    } else {
        crate::bail_parse_error!("Table is neither a virtual table nor a btree table");
    };
    let indexes = schema.get_indices(table.get_name()).cloned().collect();
    let joined_tables = vec![JoinedTable {
        op: Operation::default_scan_for(&table),
        table,
        identifier: qualified_name.identifier(),
        internal_id: program.table_reference_counter.next(),
        join_info: None,
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        database_id,
        indexed,
        plan_estimate: None,
    }];
    let mut table_references = TableReferences::new(joined_tables, vec![]);

    // Plan CTEs and add them as outer query references for subquery resolution
    plan_ctes_as_outer_refs(with, resolver, program, &mut table_references, connection)?;

    let mut where_predicates = vec![];

    // Parse the WHERE clause
    parse_where(
        where_clause.as_deref(),
        &mut table_references,
        None,
        &mut where_predicates,
        resolver,
    )?;

    // Plan subqueries in RETURNING expressions before processing
    // (so SubqueryResult nodes are cloned into result_columns)
    let mut non_from_clause_subqueries = vec![];
    plan_subqueries_from_returning(
        program,
        &mut non_from_clause_subqueries,
        &mut table_references,
        &mut returning,
        resolver,
        connection,
    )?;

    let result_columns = process_returning_clause(&mut returning, &mut table_references, resolver)?;

    // Check if there are DELETE triggers. If so, we need to materialize the write set into a RowSet first.
    // This is done in SQLite for all DELETE triggers on the affected table even if the trigger would not have an impact
    // on the target table -- presumably due to lack of static analysis capabilities to determine whether it's safe
    // to skip the rowset materialization.
    let has_delete_triggers = btree_table_for_triggers
        .as_ref()
        .map(|bt| {
            has_triggers_including_temp(resolver, database_id, TriggerEvent::Delete, None, bt)
        })
        .unwrap_or(false);

    let has_fk_cascade_triggers = match btree_table_for_triggers.as_ref() {
        Some(bt) => table_has_fk_cascade_triggers(resolver, database_id, &bt.name)?,
        None => false,
    };

    let mut safety = DmlSafety::default();
    if has_delete_triggers {
        safety.require(DmlSafetyReason::Trigger);
    }
    if has_fk_cascade_triggers {
        safety.require(DmlSafetyReason::FkCascade);
    }
    if where_clause_has_subquery(&where_predicates) {
        safety.require(DmlSafetyReason::SubqueryInWhere);
    }

    let mut delete_plan = DeletePlan {
        table_references,
        result_columns,
        where_clause: where_predicates,
        contains_constant_false_condition: false,
        indexes,
        rowset_plan: None,
        rowset_reg: None,
        non_from_clause_subqueries,
        safety,
    };

    if delete_plan.safety.requires_stable_write_set() {
        ensure_delete_uses_rowset(program, &mut delete_plan);
    }

    Ok(Plan::Delete(Box::new(delete_plan)))
}

/// Returns true if any FK referencing `table_name` (transitively, following CASCADE chains)
/// has triggers on the child table side, which could write back to `table_name` and
/// invalidate a live DELETE scan iterator.
fn table_has_fk_cascade_triggers(
    resolver: &crate::translate::emitter::Resolver,
    database_id: usize,
    table_name: &str,
) -> Result<bool> {
    let check_temp = database_id != crate::TEMP_DB_ID && resolver.has_temp_database();

    let mut visited: SmallVec<[Arc<BTreeTable>; 2]> = SmallVec::new();
    let mut worklist: SmallVec<[Arc<BTreeTable>; 2]> = SmallVec::new();

    let start = resolver
        .with_schema(database_id, |s| s.get_btree_table(table_name))
        .ok_or_else(|| {
            crate::LimboError::InternalError(format!(
                "btree table {table_name} missing from schema after delete validation"
            ))
        })?;
    worklist.push(start);

    while let Some(current) = worklist.pop() {
        if visited.iter().any(|t| Arc::ptr_eq(t, &current)) {
            continue;
        }
        visited.push(current.clone());

        let referencing_fks =
            resolver.with_schema(database_id, |s| s.resolved_fks_referencing(&current.name))?;

        for fk_ref in referencing_fks {
            if matches!(fk_ref.fk.on_delete, RefAct::NoAction | RefAct::Restrict) {
                continue;
            }
            let child_name = fk_ref.child_table.name.as_str();
            let has_triggers = resolver.with_schema(database_id, |s| {
                s.get_triggers_for_table(child_name).next().is_some()
            });
            if has_triggers {
                return Ok(true);
            }
            if check_temp {
                let has_temp = resolver.with_schema(crate::TEMP_DB_ID, |s| {
                    s.get_triggers_for_table(child_name).next().is_some()
                });
                if has_temp {
                    return Ok(true);
                }
            }
            worklist.push(fk_ref.child_table);
        }
    }
    Ok(false)
}

/// Check if any WHERE predicate contains a subquery (Subquery, InSelect, or Exists).
fn where_clause_has_subquery(predicates: &[WhereTerm]) -> bool {
    for pred in predicates {
        let mut found = false;
        let _ = walk_expr(&pred.expr, &mut |e| {
            if matches!(
                e,
                Expr::Subquery(_) | Expr::InSelect { .. } | Expr::Exists(_)
            ) {
                found = true;
            }
            Ok(if found {
                WalkControl::SkipChildren
            } else {
                WalkControl::Continue
            })
        });
        if found {
            return true;
        }
    }
    false
}

fn estimate_num_instructions(plan: &DeletePlan) -> usize {
    let base = 20;

    base + plan.table_references.joined_tables().len() * 10
}

/// Add post-optimizer reasons that force "collect rowids first, then delete".
fn record_delete_optimizer_safety(plan: &mut DeletePlan) {
    if plan
        .table_references
        .joined_tables()
        .first()
        .is_some_and(|table| matches!(table.op, Operation::MultiIndexScan(_)))
    {
        plan.safety.require(DmlSafetyReason::MultiIndexScan);
    }
    if let Some(Operation::IndexMethodQuery(query)) =
        plan.table_references.joined_tables().first().map(|t| &t.op)
    {
        let attachment = query
            .index
            .index_method
            .as_ref()
            .expect("IndexMethodQuery always has an index_method attachment");
        if !attachment.definition().results_materialized {
            plan.safety
                .require(DmlSafetyReason::IndexMethodNotMaterialized);
        }
    }
}

/// Convert a DELETE plan into a RowSet-driven delete:
/// 1. execute a SELECT-like rowid producer into RowSet
/// 2. iterate RowSet to perform actual deletes
fn ensure_delete_uses_rowset(program: &mut ProgramBuilder, plan: &mut DeletePlan) {
    if plan.rowset_plan.is_some() {
        return;
    }

    let rowid_internal_id = plan
        .table_references
        .joined_tables()
        .first()
        .expect("DELETE should have one target table")
        .internal_id;
    let rowset_reg = plan.rowset_reg.unwrap_or_else(|| {
        let reg = program.alloc_register();
        plan.rowset_reg = Some(reg);
        reg
    });

    let rowset_plan = SelectPlan {
        table_references: plan.table_references.clone(),
        result_columns: vec![ResultSetColumn {
            expr: Expr::RowId {
                database: None,
                table: rowid_internal_id,
            },
            alias: None,
            implicit_column_name: None,
            contains_aggregates: false,
        }],
        where_clause: std::mem::take(&mut plan.where_clause),
        group_by: None,
        order_by: vec![],
        aggregates: vec![],
        limit: None,
        query_destination: QueryDestination::RowSet { rowset_reg },
        join_order: plan
            .table_references
            .joined_tables()
            .iter()
            .enumerate()
            .map(|(i, t)| JoinOrderMember {
                table_id: t.internal_id,
                original_idx: i,
                is_outer: false,
            })
            .collect(),
        offset: None,
        contains_constant_false_condition: false,
        distinctness: super::plan::Distinctness::NonDistinct,
        values: vec![],
        window: None,
        // WHERE subqueries should already be planned into this SelectPlan when needed.
        non_from_clause_subqueries: vec![],
        input_cardinality_hint: None,
        estimated_output_rows: None,
        estimated_cost: None,
        simple_aggregate: None,
        phantom_params: vec![],
    };
    plan.rowset_plan = Some(rowset_plan);
}
