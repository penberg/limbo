use crate::{
    alloc::{TursoFromIterator, TursoIteratorExt},
    emit_explain,
    schema::{BTreeCharacteristics, BTreeTable},
    sync::Arc,
    translate::{
        aggregation::emit_ungrouped_aggregation,
        emitter::{
            build_rowid_column, init_exists_result_regs, init_limit, Column, CursorID, CursorType,
            MaterializedBuildInput, MaterializedBuildInputMode, MaterializedColumnRef,
            OperationMode, ResultSetColumn, TableMask, TranslateCtx,
        },
        group_by::{group_by_agg_phase, group_by_emit_row_phase, EmitGroupBy, GroupByRowSource},
        main_loop::{init_distinct, CloseLoop, InitLoop, LoopBodyEmitter, OpenLoop},
        order_by::EmitOrderBy,
        plan::{
            BitSet, Distinctness, EphemeralRowidMode, EvalAt, IndexMethodQuery, JoinOrderMember,
            Operation, QueryDestination, Scan, Search, SeekKeyComponent, SelectPlan,
            SimpleAggregate,
        },
        planner::table_mask_from_expr,
        select::emit_simple_count,
        subquery::{emit_from_clause_subqueries, emit_non_from_clause_subqueries_for_eval_at},
        values::emit_values,
        window::{emit_window_flush, EmitWindow},
        ProgramBuilder, Resolver,
    },
    vdbe::insn::Insn,
    HashMap, HashSet, Result,
};
use tracing::{instrument, Level};
use turso_macros::turso_assert;
use turso_parser::ast::Expr;

#[instrument(skip_all, level = Level::DEBUG)]
pub fn emit_program_for_select(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    plan: SelectPlan,
) -> Result<()> {
    emit_program_for_select_with_resolver(program, resolver.fork(), plan)
}

pub fn emit_program_for_select_with_resolver(
    program: &mut ProgramBuilder,
    resolver: Resolver,
    mut plan: SelectPlan,
) -> Result<()> {
    let materialized_build_inputs = emit_materialized_build_inputs(program, &resolver, &mut plan)?;
    emit_program_for_select_with_inputs(program, &resolver, plan, materialized_build_inputs)
}

fn emit_program_for_select_with_inputs(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    mut plan: SelectPlan,
    materialized_build_inputs: HashMap<usize, MaterializedBuildInput>,
) -> Result<()> {
    let result_cols_start = program.with_scoped_result_cols_start(|program| {
        // Boxed to keep ~960 B off the prepare-path stack; see TranslateCtx size.
        let mut t_ctx = Box::new(TranslateCtx::new(
            program,
            resolver.fork_with_expr_cache(),
            plan.table_references.joined_tables().len(),
            false,
        ));
        t_ctx.materialized_build_inputs = materialized_build_inputs;
        emit_query(program, &mut plan, &mut t_ctx)
    })?;

    program.result_columns = plan.result_columns;
    program.table_references.extend(plan.table_references);
    program.reg_result_cols_start = Some(result_cols_start);
    Ok(())
}

#[instrument(skip_all, level = Level::DEBUG)]
pub fn emit_query<'a>(
    program: &mut ProgramBuilder,
    plan: &'a mut SelectPlan,
    t_ctx: &mut TranslateCtx<'a>,
) -> Result<usize> {
    let after_main_loop_label = program.allocate_label();
    t_ctx.label_main_loop_end = Some(after_main_loop_label);

    // Register parameters from EXISTS subquery result columns that were dropped
    // during semi/anti-join unnesting. No code is emitted for these, but the
    // parameter slots must exist for bind-time validation to succeed.
    for variable in &plan.phantom_params {
        program.register_variable(variable);
    }

    // Evaluate uncorrelated subqueries as early as possible, because even LIMIT can reference a subquery.
    // This must happen before VALUES emission since VALUES expressions may contain scalar subqueries.
    emit_non_from_clause_subqueries_for_eval_at(
        program,
        &t_ctx.resolver,
        &mut plan.non_from_clause_subqueries,
        &plan.join_order,
        Some(&plan.table_references),
        EvalAt::BeforeLoop,
        |_| true,
    )?;

    // Handle VALUES clause - emit values after subqueries are prepared
    if !plan.values.is_empty() {
        init_limit(program, t_ctx, &plan.limit, &plan.offset)?;
        let reg_result_cols_start = emit_values(program, plan, t_ctx)?;
        program.preassign_label_to_next_insn(after_main_loop_label);
        return Ok(reg_result_cols_start);
    }

    // Emit FROM clause subqueries first so the results can be read in the main query loop.
    emit_from_clause_subqueries(program, t_ctx, &mut plan.table_references, &plan.join_order)?;

    // For non-grouped aggregation queries that also have non-aggregate columns,
    // we need to ensure non-aggregate columns are only emitted once.
    // This flag helps track whether we've already emitted these columns.
    let has_ungrouped_nonagg_cols = !plan.aggregates.is_empty()
        && plan.group_by.is_none()
        && plan.result_columns.iter().any(|c| !c.contains_aggregates);

    if has_ungrouped_nonagg_cols {
        let flag = program.alloc_register();
        program.emit_int(0, flag); // Initialize flag to 0 (not yet emitted)
        t_ctx.reg_nonagg_emit_once_flag = Some(flag);
    }

    // Allocate registers for result columns
    if t_ctx.reg_result_cols_start.is_none() {
        t_ctx.reg_result_cols_start = Some(program.alloc_registers(plan.result_columns.len()));
        program.reg_result_cols_start = t_ctx.reg_result_cols_start
    }

    // For ungrouped aggregates with non-aggregate columns, initialize EXISTS subquery
    // result_regs to 0. EXISTS returns 0 (not NULL) when the subquery is never evaluated
    // (correlated EXISTS in empty loop). Non-aggregate columns themselves are evaluated
    // after the loop in emit_ungrouped_aggregation if the loop never ran.
    // We only initialize EXISTS subqueries that haven't been evaluated yet (correlated ones).
    if has_ungrouped_nonagg_cols {
        for rc in plan.result_columns.iter() {
            if !rc.contains_aggregates {
                init_exists_result_regs(program, &rc.expr, &plan.non_from_clause_subqueries);
            }
        }
    }

    let has_group_by_exprs = plan
        .group_by
        .as_ref()
        .is_some_and(|gb| !gb.exprs.is_empty());

    // Initialize cursors and other resources needed for query execution
    if !plan.order_by.is_empty() {
        EmitOrderBy::init(
            program,
            t_ctx,
            &plan.result_columns,
            &plan.order_by,
            &plan.table_references,
            has_group_by_exprs,
            plan.distinctness != Distinctness::NonDistinct,
            &plan.aggregates,
        )?;
    }

    if has_group_by_exprs {
        if let Some(ref group_by) = plan.group_by {
            EmitGroupBy::init(
                program,
                t_ctx,
                group_by,
                plan,
                &plan.result_columns,
                &plan.order_by,
            )?;
        }
    } else if !plan.aggregates.is_empty() {
        // Handle aggregation without GROUP BY (or HAVING without GROUP BY)
        // Aggregate registers need to be NULLed at the start because the same registers might be reused on another invocation of a subquery,
        // and if they are not NULLed, the 2nd invocation of the same subquery will have values left over from the first invocation.
        t_ctx.reg_agg_start = Some(program.alloc_registers_and_init_w_null(plan.aggregates.len()));
    } else if let Some(window) = &plan.window {
        EmitWindow::init(
            program,
            t_ctx,
            window,
            plan,
            &plan.result_columns,
            &plan.order_by,
        )?;
    }

    let distinct_ctx = if let Distinctness::Distinct { .. } = &plan.distinctness {
        Some(init_distinct(program, plan, &t_ctx.resolver)?)
    } else {
        None
    };
    if let Distinctness::Distinct { ctx } = &mut plan.distinctness {
        *ctx = distinct_ctx
    }
    if let Distinctness::Distinct { ctx: Some(ctx) } = &plan.distinctness {
        program.emit_insn(Insn::HashClear {
            hash_table_id: ctx.hash_table_id,
        });
        emit_explain!(program, false, crate::translate::eqp::EqpDetail::Distinct);
    }

    init_limit(program, t_ctx, &plan.limit, &plan.offset)?;

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    // however an aggregation might still happen,
    // e.g. SELECT COUNT(*) WHERE 0 returns a row with 0, not an empty result set.
    // This Goto must be placed AFTER all initialization (cursors, sorters, etc.) so that
    // resources like the GROUP BY sorter are properly opened before we skip to the aggregation phase.
    if plan.contains_constant_false_condition {
        program.emit_insn(Insn::Goto {
            target_pc: after_main_loop_label,
        });
    }
    InitLoop::emit(
        program,
        t_ctx,
        &plan.table_references,
        &mut plan.aggregates,
        &OperationMode::SELECT,
        &plan.where_clause,
        &plan.join_order,
        &mut plan.non_from_clause_subqueries,
    )?;

    if matches!(plan.simple_aggregate, Some(SimpleAggregate::Count))
        && emit_simple_count(program, t_ctx, plan)?
    {
        // Keep LIMIT's early-exit jump target valid even on the simple_count fast path.
        // init_limit may emit an IfNot to after_main_loop_label (e.g. scalar subquery injects LIMIT 1).
        // Without resolving this label before the early return, bytecode assembly fails
        // with an unresolved IfNot target.
        program.preassign_label_to_next_insn(after_main_loop_label);
        return Ok(t_ctx.reg_result_cols_start.unwrap());
    }

    // Set up main query execution loop
    OpenLoop::emit(
        program,
        t_ctx,
        &plan.table_references,
        &plan.join_order,
        &plan.where_clause,
        None,
        OperationMode::SELECT,
        &mut plan.non_from_clause_subqueries,
    )?;

    // Process result columns and expressions in the inner loop
    LoopBodyEmitter::emit(program, t_ctx, plan)?;

    // Clean up and close the main execution loop
    CloseLoop::emit(
        program,
        t_ctx,
        &plan.table_references,
        &plan.join_order,
        OperationMode::SELECT,
        Some(plan),
    )?;

    program.preassign_label_to_next_insn(after_main_loop_label);

    let has_order_by = !plan.order_by.is_empty();
    let order_by_necessary = has_order_by && !plan.contains_constant_false_condition;
    let mut grouped_output_subqueries = plan.non_from_clause_subqueries.clone();

    // Handle GROUP BY and aggregation processing
    if has_group_by_exprs {
        let row_source = &t_ctx
            .meta_group_by
            .as_ref()
            .expect("group by metadata not found")
            .row_source;
        if matches!(row_source, GroupByRowSource::Sorter { .. }) {
            group_by_agg_phase(program, t_ctx, plan)?;
        }
        group_by_emit_row_phase(program, t_ctx, plan, &mut grouped_output_subqueries)?;
    } else if !plan.aggregates.is_empty() {
        // Handle aggregation without GROUP BY (or HAVING without GROUP BY)
        emit_ungrouped_aggregation(program, t_ctx, plan, &mut grouped_output_subqueries)?;
    } else if plan.window.is_some() {
        emit_window_flush(program, t_ctx, plan)?;
    }

    // Process ORDER BY results if needed
    if has_order_by && order_by_necessary {
        EmitOrderBy::emit(program, t_ctx, plan)?;
    }

    Ok(t_ctx.reg_result_cols_start.unwrap())
}

#[derive(Debug, Clone)]
/// Captures the parameters needed to materialize one hash-build input.
struct MaterializationSpec {
    build_table_idx: usize,
    probe_table_idx: usize,
    mode: MaterializedBuildInputMode,
    prefix_tables: TableMask,
    key_exprs: Vec<Expr>,
    payload_columns: Vec<MaterializedColumnRef>,
}

/// Build materialized hash-build inputs for hash joins that depend on prior joins.
///
/// A materialized build input is an ephemeral table that captures the rows
/// a hash join is allowed to build from after earlier joins and filters have
/// been applied. This prevents the build side from being re-scanned in its
/// full, unfiltered form when prior join constraints must be respected.
///
/// The materialization uses a join-prefix: all tables that appear before the
/// probe table in the join order, plus the build table itself. This prefix
/// represents the minimal context needed to evaluate build-side constraints.
/// For probe->build chaining we store join keys and payload columns directly
/// in the ephemeral table; otherwise we only store rowids and `SeekRowid`
/// during probing when needed.
pub(crate) fn emit_materialized_build_inputs(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    plan: &mut SelectPlan,
) -> Result<HashMap<usize, MaterializedBuildInput>> {
    let mut build_inputs: HashMap<usize, MaterializedBuildInput> = HashMap::default();
    let mut materializations: Vec<MaterializationSpec> = Vec::new();
    let mut hash_tables_to_keep_open = BitSet::default();

    // Keep hash tables open while running materialization subplans so we can reuse them.
    // A build table may appear in multiple hash joins when chaining, so we do not
    // treat repeated build tables as an error.
    for table in plan.table_references.joined_tables().iter() {
        if let Operation::HashJoin(hash_join_op) = &table.op {
            let build_table = &plan.table_references.joined_tables()[hash_join_op.build_table_idx];
            hash_tables_to_keep_open.set(build_table.internal_id.into())?;
        }
    }

    let mut seen_build_tables: TableMask = TableMask::default();

    // decide per-hash-join materialization mode (rowid-only vs key+payload).
    for member in plan.join_order.iter() {
        let table = &plan.table_references.joined_tables()[member.original_idx];
        if let Operation::HashJoin(hash_join_op) = &table.op {
            if !hash_join_op.materialize_build_input
                || seen_build_tables.get(hash_join_op.build_table_idx)
            {
                continue;
            }
            seen_build_tables.set(hash_join_op.build_table_idx)?;

            let probe_table_idx = hash_join_op.probe_table_idx;
            let probe_pos = plan
                .join_order
                .iter()
                .position(|member| member.original_idx == probe_table_idx)
                .unwrap_or(plan.join_order.len());
            let build_table_was_prior_probe = plan.join_order[..probe_pos].iter().any(|member| {
                let table_ref = &plan.table_references.joined_tables()[member.original_idx];
                matches!(
                    table_ref.op,
                    Operation::HashJoin(ref hj) if hj.probe_table_idx == hash_join_op.build_table_idx
                )
            });

            // The join prefix is the set of tables we include when building this hash
            // input (all tables before the probe + the build table). If the prefix
            // has *any* table besides the build table, then rowid-only materialization
            // is unsafe. Here's why:
            //
            // Rowid-only keeps each build-table rowid at most once. That throws away
            // which prefix row it came from, so we lose the one-to-one link between
            // a prefix match and a build row.
            //
            // Example (t1 is a left-side table earlier in the join order):
            //   t1 rows:     t1_1(c=1), t1_2(c=2)
            //   t2 rows:     t2_7(c=1), t2_8(c=2)   (build table)
            //   t3 rows:     one row per t2 row
            //
            // Correct result after joining:
            //   t1_1 + t2_7 + t2_7's t3 row
            //   t1_2 + t2_8 + t2_8's t3 row   (2 rows)
            //
            // Key+payload materialization lets us PRUNE the prefix tables (like t1)
            // from the main join order, because their needed columns now live in
            // the payload. So the main plan does NOT loop t1 again.
            //
            // However, rowid-only materialization keeps just {t2_7, t2_8} with no link to t1_1/t1_2.
            // Since t1 stays in the main join loop, each t1 row joins against the
            // materialized t2 set. With no t1→t2 correlation, every t1 row matches
            // both t2 rows, incorrectly producing 4 rows (a cross product).
            //
            // Therefore: if the prefix has other tables, we must store key+payload
            // rows so each prefix match stays distinct and the main plan can drop
            // the prefix loops.
            let (_, included_tables) =
                materialization_prefix(plan, hash_join_op.build_table_idx, probe_table_idx)?;
            let prefix_has_other_tables = included_tables
                .iter()
                .any(|table_idx| table_idx != hash_join_op.build_table_idx);

            if build_table_was_prior_probe || prefix_has_other_tables {
                // Prior probe -> build chaining OR any multi-table prefix requires keys+payload
                // so we do not lose multiplicity or correlation.
                let payload_columns = collect_materialized_payload_columns(plan, &included_tables)?;
                let key_exprs: Vec<Expr> = hash_join_op
                    .join_keys
                    .iter()
                    .map(|key| key.get_build_expr(&plan.where_clause).clone())
                    .collect();
                let mode = MaterializedBuildInputMode::KeyPayload {
                    num_keys: key_exprs.len(),
                    payload_columns: payload_columns.clone(),
                };
                materializations.push(MaterializationSpec {
                    build_table_idx: hash_join_op.build_table_idx,
                    probe_table_idx,
                    mode,
                    prefix_tables: included_tables,
                    key_exprs,
                    payload_columns,
                });
            } else {
                // Single-table prefix: a rowid list preserves the build-side filters
                // without losing multiplicity (as explained in the comment above).
                materializations.push(MaterializationSpec {
                    build_table_idx: hash_join_op.build_table_idx,
                    probe_table_idx,
                    mode: MaterializedBuildInputMode::RowidOnly,
                    prefix_tables: TableMask::default(),
                    key_exprs: Vec::new(),
                    payload_columns: Vec::new(),
                });
            }
        }
    }

    // Now we emit each of the materialization subplans into an ephemeral table.
    for spec in materializations.iter() {
        let build_table = &plan.table_references.joined_tables()[spec.build_table_idx];
        let internal_id = program.table_reference_counter.next();
        let columns = match &spec.mode {
            MaterializedBuildInputMode::RowidOnly => {
                std::iter::once(build_rowid_column()).try_collect()?
            }
            MaterializedBuildInputMode::KeyPayload {
                num_keys,
                payload_columns,
            } => build_materialized_input_columns(*num_keys, payload_columns)?,
        };
        let ephemeral_table = Arc::new(BTreeTable::new(
            0,
            format!("hash_build_input_{internal_id}"),
            crate::alloc::vec![],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        ));
        let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(ephemeral_table.clone()));

        // Build a plan that emits only rowids for the build table using the join prefix
        // that makes the hash join legal (including any earlier hash joins).
        let materialize_plan = build_materialized_build_input_plan(
            plan,
            spec.build_table_idx,
            spec.probe_table_idx,
            cursor_id,
            ephemeral_table,
            &spec.mode,
            &spec.key_exprs,
            &spec.payload_columns,
            &build_inputs,
        )?;

        // Make the materialization plan show up as a subtree in EXPLAIN QUERY PLAN output.
        emit_explain!(
            program,
            true,
            crate::translate::eqp::EqpDetail::HashBuild {
                table: crate::translate::eqp::EqpTable::from_joined(build_table),
                estimate: build_table.plan_estimate,
            }
        );
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id,
            is_table: true,
        });
        program.nested(|program| -> Result<()> {
            program.set_hash_tables_to_keep_open(&hash_tables_to_keep_open);
            // emit_program_for_select_with_inputs unconditionally overwrites
            // program.result_columns and extends program.table_references with
            // the materialize subplan's columns/refs. In a nested context (e.g.
            // a compound branch or CTE) those belong to the *outer* SELECT, so
            // save and restore them around the nested emission.
            let saved_result_columns = std::mem::take(&mut program.result_columns);
            let saved_table_references = std::mem::take(&mut program.table_references);
            emit_program_for_select_with_inputs(
                program,
                resolver,
                materialize_plan,
                build_inputs.clone(),
            )?;
            program.result_columns = saved_result_columns;
            program.table_references = saved_table_references;
            program.clear_hash_tables_to_keep_open();
            Ok(())
        })?;
        program.pop_current_parent_explain();

        build_inputs.insert(
            spec.build_table_idx,
            MaterializedBuildInput {
                cursor_id,
                mode: spec.mode.clone(),
                prefix_tables: spec.prefix_tables.clone(),
            },
        );
    }

    // Drop any join-prefix tables already captured by key+payload materializations.
    prune_join_order_for_materialized_inputs(plan, &build_inputs)?;

    #[cfg(debug_assertions)]
    turso_assert!(
        {
            let join_order_tables: HashSet<_> = plan
                .join_order
                .iter()
                .map(|member| member.original_idx)
                .collect();
            let build_tables_in_plan: HashSet<_> = plan
                .join_order
                .iter()
                .filter_map(|member| {
                    let table = &plan.table_references.joined_tables()[member.original_idx];
                    if let Operation::HashJoin(hash_join_op) = &table.op {
                        Some(hash_join_op.build_table_idx)
                    } else {
                        None
                    }
                })
                .collect();
            build_inputs.iter().all(|(build_table_idx, input)| {
                if !build_tables_in_plan.contains(build_table_idx) {
                    return true;
                }
                if !matches!(input.mode, MaterializedBuildInputMode::KeyPayload { .. }) {
                    return true;
                }
                input
                    .prefix_tables
                    .iter()
                    .all(|table_idx| !join_order_tables.contains(&table_idx))
            })
        },
        "materialized build input prefix table still present in join order"
    );
    Ok(build_inputs)
}

/// Remove join-order entries already satisfied by key+payload materializations.
///
/// This prevents redundant scans (and cross products) when a hash-build input
/// already captures a join prefix. It also marks fully covered WHERE terms as
/// consumed so they are not re-applied later in the main plan.
fn prune_join_order_for_materialized_inputs(
    plan: &mut SelectPlan,
    build_inputs: &HashMap<usize, MaterializedBuildInput>,
) -> Result<()> {
    if build_inputs.is_empty() {
        return Ok(());
    }

    let mut build_tables_in_plan = TableMask::default();
    for member in plan.join_order.iter() {
        let table = &plan.table_references.joined_tables()[member.original_idx];
        if let Operation::HashJoin(hash_join_op) = &table.op {
            build_tables_in_plan.set(hash_join_op.build_table_idx)?;
        }
    }

    let mut tables_to_remove: TableMask = TableMask::default();
    for (build_table_idx, input) in build_inputs.iter() {
        if !build_tables_in_plan.get(*build_table_idx) {
            continue;
        }
        if matches!(input.mode, MaterializedBuildInputMode::KeyPayload { .. }) {
            tables_to_remove.try_extend(input.prefix_tables.iter())?;
        }
    }

    if tables_to_remove.is_empty() {
        return Ok(());
    }

    for term in plan.where_clause.iter_mut() {
        if term.consumed {
            continue;
        }
        if term.from_outer_join.is_some() {
            // OUTER JOIN terms still belong to the right-table loop recorded in
            // `from_outer_join`. Materializing and pruning the build-side prefix
            // does not make those terms safe to consume here, because the
            // materialization subplan does not include the probe table that
            // determines the null-extension boundary.
            continue;
        }
        let mask = table_mask_from_expr(
            &term.expr,
            &plan.table_references,
            &plan.non_from_clause_subqueries,
        )?;
        if tables_to_remove.contains_all_set_bits_of(&mask) {
            term.consumed = true;
        }
    }
    plan.join_order
        .retain(|member| !tables_to_remove.get(member.original_idx));
    Ok(())
}

/// Compute the join-prefix used to materialize a hash-build input.
///
/// The prefix consists of all tables before the probe table plus the build
/// table itself (if not already present). The returned `included_tables`
/// list also includes build tables of earlier hash joins so payload collection
/// can capture all referenced columns.
fn materialization_prefix(
    plan: &SelectPlan,
    build_table_idx: usize,
    probe_table_idx: usize,
) -> Result<(Vec<JoinOrderMember>, TableMask)> {
    let mut join_order = plan.join_order.clone();
    if join_order
        .iter()
        .all(|member| member.original_idx != probe_table_idx)
    {
        let probe_table = &plan.table_references.joined_tables()[probe_table_idx];
        join_order.push(JoinOrderMember {
            table_id: probe_table.internal_id,
            original_idx: probe_table_idx,
            is_outer: probe_table
                .join_info
                .as_ref()
                .is_some_and(|join_info| join_info.is_outer()),
        });
    }
    let probe_pos = join_order
        .iter()
        .position(|m| m.original_idx == probe_table_idx)
        .expect("probe table just ensured in join order");

    // Only include tables prior to the probe table. The materialization subplan
    // should filter the build table using prior join constraints, not scan the probe.
    let mut prefix_join_order = join_order[..probe_pos].to_vec();
    if prefix_join_order
        .iter()
        .all(|member| member.original_idx != build_table_idx)
    {
        let build_table = &plan.table_references.joined_tables()[build_table_idx];
        prefix_join_order.push(JoinOrderMember {
            table_id: build_table.internal_id,
            original_idx: build_table_idx,
            is_outer: build_table
                .join_info
                .as_ref()
                .is_some_and(|join_info| join_info.is_outer()),
        });
    }

    let mut included_tables: TableMask = prefix_join_order
        .iter()
        .map(|m| m.original_idx)
        .try_collect()?;
    for member in prefix_join_order.iter() {
        let table_ref = &plan.table_references.joined_tables()[member.original_idx];
        if let Operation::HashJoin(hash_join_op) = &table_ref.op {
            included_tables.set(hash_join_op.build_table_idx)?;
        }
    }
    Ok((prefix_join_order, included_tables))
}

/// Collect the payload columns needed for a materialized build input.
///
/// This gathers referenced columns from the included tables and always adds
/// rowids for tables that have them so probe-side expressions can be satisfied
/// without seeking back into base tables.
fn collect_materialized_payload_columns(
    plan: &SelectPlan,
    included_tables: &TableMask,
) -> Result<Vec<MaterializedColumnRef>> {
    let mut payload_columns: Vec<MaterializedColumnRef> = Vec::new();
    let mut seen: HashSet<MaterializedColumnRef> = HashSet::default();
    for table_idx in included_tables.iter() {
        let table = &plan.table_references.joined_tables()[table_idx];
        for col_idx in table.col_used_mask.iter() {
            let is_rowid_alias = table
                .columns()
                .get(col_idx)
                .is_some_and(|col| col.is_rowid_alias());
            let col_ref = MaterializedColumnRef::Column {
                table_id: table.internal_id,
                column_idx: col_idx,
                is_rowid_alias,
            };
            if seen.insert(col_ref.clone()) {
                payload_columns.push(col_ref);
            }
        }
        if table.btree().is_some_and(|btree| btree.has_rowid) {
            let rowid_ref = MaterializedColumnRef::RowId {
                table_id: table.internal_id,
            };
            if seen.insert(rowid_ref.clone()) {
                payload_columns.push(rowid_ref);
            }
        }
    }
    Ok(payload_columns)
}

/// Build the ephemeral-table schema for key+payload materializations.
///
/// Keys are stored first (typed as BLOB for join-key affinity handling),
/// followed by payload columns with integer or blob affinity.
fn build_materialized_input_columns(
    num_keys: usize,
    payload_columns: &[MaterializedColumnRef],
) -> Result<crate::alloc::Vec<Column>> {
    Ok((0..num_keys)
        .map(|i| Column::new_default_text(Some(format!("key_{i}")), "BLOB".to_string(), None))
        .chain(payload_columns.iter().enumerate().map(|(i, payload)| {
            let name = Some(format!("payload_{i}"));
            match payload {
                MaterializedColumnRef::RowId { .. } => {
                    Column::new_default_integer(name, "INTEGER".to_string(), None)
                }
                MaterializedColumnRef::Column { .. } => {
                    Column::new_default_text(name, "BLOB".to_string(), None)
                }
            }
        }))
        .try_collect()?)
}

/// Construct a SELECT plan that materializes build-side inputs into an ephemeral table.
/// This plan is separate from the main query plan and is exclusively used for the materialization.
/// process.
///
/// The join order is the original prefix up to (but excluding) the probe table, plus
/// the build table itself. This filters build rows using only prior join constraints
/// and then prunes any tables already captured by earlier key+payload materializations.
#[allow(clippy::too_many_arguments)]
fn build_materialized_build_input_plan(
    plan: &SelectPlan,
    build_table_idx: usize,
    probe_table_idx: usize,
    cursor_id: CursorID,
    table: Arc<BTreeTable>,
    mode: &MaterializedBuildInputMode,
    key_exprs: &[Expr],
    payload_columns: &[MaterializedColumnRef],
    materialized_build_inputs: &HashMap<usize, MaterializedBuildInput>,
) -> Result<SelectPlan> {
    // Build a materialization subplan that only includes the join prefix
    // (all tables prior to the probe + the build table). The resulting plan
    // is smaller than the original select plan, so any access methods or
    // predicates that depend on tables outside this prefix must be dropped.
    let (join_order, included_tables) =
        materialization_prefix(plan, build_table_idx, probe_table_idx)?;
    // Bitmask of tables that are actually in the prefix join order for
    // this materialization subplan. Anything that depends on other tables
    // cannot be evaluated during those table scans.
    let join_prefix_mask: TableMask = join_order.iter().map(|m| m.original_idx).try_collect()?;

    // Clone WHERE terms for the materialization subplan. We cannot reuse the
    // parent plan's consumed flags because the optimizer may have consumed
    // terms for access methods (e.g. ephemeral autoindex seeks) that get
    // overwritten to scans inside the subplan. Reset each term's consumed
    // flag: only terms referencing tables outside the prefix are consumed.
    let mut where_clause = plan.where_clause.clone();
    for term in where_clause.iter_mut() {
        let mask = table_mask_from_expr(
            &term.expr,
            &plan.table_references,
            &plan.non_from_clause_subqueries,
        )?;
        // Expressions can also reference build tables of earlier hash joins in this subplan,
        // because those tables are available during probe loops. Use the broader "included"
        // set when deciding which WHERE terms can be evaluated inside the materialization.
        term.consumed = !included_tables.contains_all_set_bits_of(&mask);
    }

    // Clone table references and then "sanitize" each access method so that
    // the materialization subplan does not try to use an access path that
    // requires tables outside the prefix. If it does, we fall back to a scan.
    let mut table_references = plan.table_references.clone();
    for joined_table in table_references.joined_tables_mut().iter_mut() {
        if let Operation::HashJoin(hash_join_op) = &mut joined_table.op {
            if hash_join_op.build_table_idx == build_table_idx {
                // Avoid recursive materialization and disable the hash join for the build table
                // so it can be accessed using the join constraints.
                hash_join_op.materialize_build_input = false;
                joined_table.op = Operation::default_scan_for(&joined_table.table);
            } else if hash_join_op.probe_table_idx == probe_table_idx {
                // The probe table is not part of the materialization prefix, so
                // disable hash joins anchored on it.
                joined_table.op = Operation::default_scan_for(&joined_table.table);
            }
        }
    }

    // Helper to decide whether an expression depends on tables outside
    // the prefix. If it does, any access method that relies on that
    // expression must be invalidated for the materialization subplan.
    let expr_depends_outside_prefix = |expr: &Expr| -> Result<bool> {
        let mask = table_mask_from_expr(
            expr,
            &plan.table_references,
            &plan.non_from_clause_subqueries,
        )?;
        Ok(!join_prefix_mask.contains_all_set_bits_of(&mask))
    };

    // Walk each table in the cloned plan and ensure its access method is
    // valid within the prefix. If the access method depends on tables
    // outside the prefix, downgrade to a plain scan.
    for (table_idx, joined_table) in table_references.joined_tables_mut().iter_mut().enumerate() {
        if !join_prefix_mask.get(table_idx) {
            continue;
        }

        let mut reset_op = false;
        match &joined_table.op {
            Operation::Search(Search::RowidEq { cmp_expr }) => {
                // Rowid equality searches may depend on other tables (e.g. column = other.col).
                reset_op = expr_depends_outside_prefix(cmp_expr)?;
            }
            Operation::Search(Search::Seek { seek_def, .. }) => {
                // Seek keys can include expressions bound by other tables. If so,
                // the seek is not valid in the prefix-only subplan.
                for component in seek_def.iter(&seek_def.start) {
                    if let SeekKeyComponent::Expr(expr) = component {
                        if expr_depends_outside_prefix(expr)? {
                            reset_op = true;
                            break;
                        }
                    }
                }
                if !reset_op {
                    for component in seek_def.iter(&seek_def.end) {
                        if let SeekKeyComponent::Expr(expr) = component {
                            if expr_depends_outside_prefix(expr)? {
                                reset_op = true;
                                break;
                            }
                        }
                    }
                }
            }
            Operation::IndexMethodQuery(IndexMethodQuery { arguments, .. }) => {
                // Index method queries are driven by argument expressions.
                // If any argument depends on non-prefix tables, we cannot use it.
                for expr in arguments {
                    if expr_depends_outside_prefix(expr)? {
                        reset_op = true;
                        break;
                    }
                }
            }
            Operation::Scan(Scan::VirtualTable { constraints, .. }) => {
                // Virtual table constraints are evaluated against expressions.
                // If any constraint depends on non-prefix tables, drop the scan
                // specialization and fall back to a full scan.
                for expr in constraints {
                    if expr_depends_outside_prefix(expr)? {
                        reset_op = true;
                        break;
                    }
                }
            }
            Operation::HashJoin(hash_join_op) => {
                // Hash joins are driven by the probe table's loop. That probe table
                // must be in the prefix; otherwise the hash join cannot be evaluated
                // inside this subplan. The build table may live outside the prefix
                // because the hash build phase scans it independently.
                if !join_prefix_mask.get(hash_join_op.probe_table_idx) {
                    reset_op = true;
                }
            }
            _ => {}
        }

        if reset_op {
            // Downgrade to a default scan. This ensures the subplan only uses
            // access paths that are valid within the prefix join order.
            joined_table.op = Operation::default_scan_for(&joined_table.table);
        }
    }

    let build_internal_id = plan.table_references.joined_tables()[build_table_idx].internal_id;
    let result_columns = match mode {
        MaterializedBuildInputMode::RowidOnly => vec![ResultSetColumn {
            expr: Expr::RowId {
                database: None,
                table: build_internal_id,
            },
            alias: None,
            implicit_column_name: None,
            contains_aggregates: false,
        }],
        MaterializedBuildInputMode::KeyPayload { num_keys, .. } => {
            turso_assert!(
                *num_keys == key_exprs.len(),
                "materialized hash build input key count mismatch"
            );
            let mut result_columns: Vec<ResultSetColumn> = Vec::new();
            for expr in key_exprs.iter() {
                result_columns.push(ResultSetColumn {
                    expr: expr.clone(),
                    alias: None,
                    implicit_column_name: None,
                    contains_aggregates: false,
                });
            }
            for payload in payload_columns.iter() {
                let expr = match payload {
                    MaterializedColumnRef::Column {
                        table_id,
                        column_idx,
                        is_rowid_alias,
                    } => Expr::Column {
                        database: None,
                        table: *table_id,
                        column: *column_idx,
                        is_rowid_alias: *is_rowid_alias,
                    },
                    MaterializedColumnRef::RowId { table_id } => Expr::RowId {
                        database: None,
                        table: *table_id,
                    },
                };
                result_columns.push(ResultSetColumn {
                    expr,
                    alias: None,
                    implicit_column_name: None,
                    contains_aggregates: false,
                });
            }
            result_columns
        }
    };

    let mut materialize_plan = SelectPlan {
        table_references,
        join_order,
        result_columns,
        where_clause,
        group_by: None,
        order_by: vec![],
        aggregates: vec![],
        limit: None,
        offset: None,
        contains_constant_false_condition: false,
        query_destination: QueryDestination::EphemeralTable {
            cursor_id,
            table,
            rowid_mode: match mode {
                MaterializedBuildInputMode::RowidOnly => EphemeralRowidMode::FromResultColumns,
                MaterializedBuildInputMode::KeyPayload { .. } => EphemeralRowidMode::Auto,
            },
        },
        distinctness: Distinctness::NonDistinct,
        values: vec![],
        window: None,
        non_from_clause_subqueries: plan.non_from_clause_subqueries.clone(),
        input_cardinality_hint: None,
        estimated_output_rows: None,
        estimated_cost: None,
        simple_aggregate: None,
        phantom_params: vec![],
    };

    prune_join_order_for_materialized_inputs(&mut materialize_plan, materialized_build_inputs)?;

    Ok(materialize_plan)
}
