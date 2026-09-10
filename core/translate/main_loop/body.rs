use crate::translate::plan::Operation::HashJoin;
use crate::translate::plan::SimpleAggregate;
use crate::translate::{
    aggregation::agg_arg_collation,
    order_by::{custom_type_comparator, EmitOrderBy},
    window::EmitWindow,
};
use crate::vdbe::insn::AggStepData;

use super::*;

/// SQLite (and so Turso) processes joins as a nested loop.
/// The loop may emit rows to various destinations depending on the query:
/// - a GROUP BY sorter (grouping is done by sorting based on the GROUP BY keys and aggregating while the GROUP BY keys match)
/// - a GROUP BY phase with no sorting (when the rows are already in the order required by the GROUP BY keys)
/// - an AggStep (the columns are collected for aggregation, which is finished later)
/// - a Window (rows are buffered and returned according to the rules of the window definition)
/// - an ORDER BY sorter (when there is none of the above, but there is an ORDER BY)
/// - a QueryResult (there is none of the above, so the loop either emits a ResultRow, or if it's a subquery, yields to the parent query)
enum LoopEmitTarget {
    GroupBy,
    OrderBySorter,
    AggStep,
    Window,
    QueryResult,
}

/// Emits the bytecode for the inner loop of a query.
/// At this point the cursors for all tables have been opened and rewound.
pub fn emit_loop<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    plan: &'a SelectPlan,
) -> Result<()> {
    LoopBodyEmitter::emit(program, t_ctx, plan)
}

/// Emits the select-loop body.
pub struct LoopBodyEmitter;

/// Internal state for loop-body emission.
///
/// The body has one non-obvious ordering rule: anti-join body entry must be
/// resolved before any body instructions are emitted, otherwise relocated
/// constants can make the backward jump land incorrectly.
struct LoopBody<'prog, 'ctx, 'plan> {
    program: &'prog mut ProgramBuilder,
    t_ctx: &'ctx mut TranslateCtx<'plan>,
    plan: &'plan SelectPlan,
}

impl LoopBodyEmitter {
    pub fn emit<'a>(
        program: &mut ProgramBuilder,
        t_ctx: &mut TranslateCtx<'a>,
        plan: &'a SelectPlan,
    ) -> Result<()> {
        LoopBody::new(program, t_ctx, plan).emit()
    }
}

impl<'prog, 'ctx, 'plan> LoopBody<'prog, 'ctx, 'plan> {
    const fn new(
        program: &'prog mut ProgramBuilder,
        t_ctx: &'ctx mut TranslateCtx<'plan>,
        plan: &'plan SelectPlan,
    ) -> Self {
        Self {
            program,
            t_ctx,
            plan,
        }
    }

    /// Resolve the final anti-join body target before any row-emission logic runs.
    fn resolve_anti_join_entry(&mut self) {
        // The innermost anti-join body entry must be resolved before any row
        // emission target is chosen, otherwise the late jump back into the body
        // can land on the wrong relocated instruction.
        if let Some(last_join) = self.plan.join_order.last() {
            let last_idx = last_join.original_idx;
            let is_anti = self.plan.table_references.joined_tables()[last_idx]
                .join_info
                .as_ref()
                .is_some_and(|ji| ji.is_anti());
            if is_anti {
                if let Some(sa_meta) = self.t_ctx.meta_semi_anti_joins[last_idx].as_ref() {
                    self.program
                        .preassign_label_to_next_insn(sa_meta.label_body);
                }
            }
        }
    }

    /// Choose the row-consumption target for the already-open main loop.
    fn select_emit_target(&self) -> LoopEmitTarget {
        if self
            .plan
            .group_by
            .as_ref()
            .is_some_and(|gb| !gb.exprs.is_empty())
        {
            return LoopEmitTarget::GroupBy;
        }
        if !self.plan.aggregates.is_empty() {
            return LoopEmitTarget::AggStep;
        }
        if self.plan.window.is_some() {
            return LoopEmitTarget::Window;
        }
        if !self.plan.order_by.is_empty() {
            return LoopEmitTarget::OrderBySorter;
        }
        LoopEmitTarget::QueryResult
    }

    /// Emit the loop body once all required entry labels are fixed.
    fn emit(mut self) -> Result<()> {
        self.resolve_anti_join_entry();
        emit_loop_source(
            self.program,
            self.t_ctx,
            self.plan,
            self.select_emit_target(),
        )
    }
}

/// This is a helper function for inner_loop_emit,
/// which does a different thing depending on the emit target.
/// See the InnerLoopEmitTarget enum for more details.
fn emit_loop_source<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    plan: &'a SelectPlan,
    emit_target: LoopEmitTarget,
) -> Result<()> {
    match emit_target {
        LoopEmitTarget::GroupBy => {
            let GroupByMetadata {
                row_source,
                registers,
                ..
            } = t_ctx.meta_group_by.as_ref().unwrap();

            let start_reg = registers.reg_group_by_source_cols_start;
            let mut cur_reg = start_reg;

            // Collect all non-aggregate expressions in the following order:
            // 1. GROUP BY expressions. These serve as sort keys.
            // 2. Remaining non-aggregate expressions that are not in GROUP BY.
            //
            // Example:
            //   SELECT col1, col2, SUM(col3) FROM table GROUP BY col1
            //   - col1 is added first (from GROUP BY)
            //   - col2 is added second (non-aggregate, in SELECT, not in GROUP BY)
            for (expr, _) in t_ctx.non_aggregate_expressions.iter() {
                let key_reg = cur_reg;
                cur_reg += 1;
                translate_expr(
                    program,
                    Some(&plan.table_references),
                    expr,
                    key_reg,
                    &t_ctx.resolver,
                )?;
            }

            match row_source {
                GroupByRowSource::Sorter {
                    sort_cursor,
                    sorter_column_count,
                    reg_sorter_key,
                    ..
                } => {
                    // Sorter path: store only unique leaf columns from aggregate args.
                    // Full expressions are re-evaluated from the pseudo cursor during aggregation.
                    for leaf_expr in t_ctx.agg_leaf_columns.iter() {
                        let reg = cur_reg;
                        cur_reg += 1;
                        translate_expr(
                            program,
                            Some(&plan.table_references),
                            leaf_expr,
                            reg,
                            &t_ctx.resolver,
                        )?;
                    }
                    sorter_insert(
                        program,
                        start_reg,
                        *sorter_column_count,
                        *sort_cursor,
                        *reg_sorter_key,
                    );
                }
                GroupByRowSource::MainLoop { .. } => {
                    for agg in plan.aggregates.iter() {
                        for expr in agg.args.iter() {
                            let agg_reg = cur_reg;
                            cur_reg += 1;
                            translate_expr(
                                program,
                                Some(&plan.table_references),
                                expr,
                                agg_reg,
                                &t_ctx.resolver,
                            )?;
                        }
                    }
                    group_by_agg_phase(program, t_ctx, plan)?;
                }
            }

            Ok(())
        }
        LoopEmitTarget::OrderBySorter => {
            EmitOrderBy::sorter_insert(program, t_ctx, plan)?;

            if let Distinctness::Distinct { ctx } = &plan.distinctness {
                let distinct_ctx = ctx.as_ref().expect("distinct context must exist");
                program.preassign_label_to_next_insn(distinct_ctx.label_on_conflict);
            }

            Ok(())
        }
        LoopEmitTarget::AggStep => {
            let start_reg = t_ctx
                .reg_agg_start
                .expect("aggregate registers must be initialized");
            if let Some(SimpleAggregate::MinMax(min_max)) = &plan.simple_aggregate {
                let expr_reg = program.alloc_register();
                translate_expr(
                    program,
                    Some(&plan.table_references),
                    &min_max.argument,
                    expr_reg,
                    &t_ctx.resolver,
                )?;
                let loop_end = t_ctx
                    .label_main_loop_end
                    .expect("simple min/max requires the main-loop end label");
                let label_on_null = if matches!(min_max.func, crate::function::AggFunc::Min) {
                    // Ascending index order places NULLs first. Keep scanning until
                    // the first non-NULL value, then jump straight to AggFinal.
                    let label_on_null = program.allocate_label();
                    program.emit_insn(Insn::IsNull {
                        reg: expr_reg,
                        target_pc: label_on_null,
                    });
                    Some(label_on_null)
                } else {
                    None
                };

                let arg_collation =
                    agg_arg_collation(&plan.table_references, &min_max.argument, &t_ctx.resolver);
                let comparator = custom_type_comparator(
                    &min_max.argument,
                    &plan.table_references,
                    t_ctx.resolver.schema(),
                );
                program.emit_insn(Insn::AggStep {
                    data: Box::new(AggStepData {
                        acc_reg: start_reg,
                        col: expr_reg,
                        delimiter: 0,
                        func: crate::function::AccumulatorFunc::Agg(min_max.func.clone()),
                        comparator,
                        collation: Some(arg_collation),
                    }),
                });
                program.emit_insn(Insn::Goto {
                    target_pc: loop_end,
                });

                if let Some(label_on_null) = label_on_null {
                    program.preassign_label_to_next_insn(label_on_null);
                }
                return Ok(());
            }

            // In planner.rs, we have collected all aggregates from the SELECT clause, including ones where the aggregate is embedded inside
            // a more complex expression. Some examples: length(sum(x)), sum(x) + avg(y), sum(x) + 1, etc.
            // The result of those more complex expressions depends on the final result of the aggregate, so we don't translate the complete expressions here.
            // Instead, we accumulate the intermediate results of all aggreagates, and evaluate any expressions that do not contain aggregates.
            for (i, agg) in plan.aggregates.iter().enumerate() {
                let reg = start_reg + i;

                // FILTER: skip AggStep if filter condition is false
                let filter_skip_label = if let Some(filter_expr) = &agg.filter_expr {
                    let label = program.allocate_label();
                    let filter_reg = program.alloc_register();
                    translate_expr(
                        program,
                        Some(&plan.table_references),
                        filter_expr,
                        filter_reg,
                        &t_ctx.resolver,
                    )?;
                    program.emit_insn(Insn::IfNot {
                        reg: filter_reg,
                        target_pc: label,
                        jump_if_null: true,
                    });
                    Some(label)
                } else {
                    None
                };

                translate_aggregation_step(
                    program,
                    &plan.table_references,
                    AggArgumentSource::new_from_expression(&agg.func, &agg.args, &agg.distinctness),
                    reg,
                    &t_ctx.resolver,
                    agg.fraction_reg,
                )?;
                if let Distinctness::Distinct { ctx } = &agg.distinctness {
                    let ctx = ctx
                        .as_ref()
                        .expect("distinct aggregate context not populated");
                    program.preassign_label_to_next_insn(ctx.label_on_conflict);
                }

                if let Some(label) = filter_skip_label {
                    program.preassign_label_to_next_insn(label);
                }
            }

            let label_emit_nonagg_only_once = if let Some(flag) = t_ctx.reg_nonagg_emit_once_flag {
                let if_label = program.allocate_label();
                program.emit_insn(Insn::If {
                    reg: flag,
                    target_pc: if_label,
                    jump_if_null: false,
                });
                Some(if_label)
            } else {
                None
            };

            let col_start = t_ctx.reg_result_cols_start.unwrap();

            // Process only non-aggregate columns
            let non_agg_columns = plan
                .result_columns
                .iter()
                .enumerate()
                .filter(|(_, rc)| !rc.contains_aggregates);

            for (i, rc) in non_agg_columns {
                let reg = col_start + i;

                // Must use no_constant_opt to prevent constant hoisting: in compound
                // selects (UNION ALL), all branches share the same result registers,
                // so hoisted constants from the last branch overwrite earlier branches.
                translate_expr_no_constant_opt(
                    program,
                    Some(&plan.table_references),
                    &rc.expr,
                    reg,
                    &t_ctx.resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
            }

            // For result columns that contain aggregates but also reference
            // non-aggregate columns (e.g. CASE WHEN SUM(1) THEN a ELSE b END),
            // pre-read those column references while the cursor is still valid.
            // They are cached in expr_to_reg_cache so that when the full
            // expression is evaluated after AggFinal, translate_expr finds
            // the cached values instead of reading from the exhausted cursor.
            for rc in plan
                .result_columns
                .iter()
                .filter(|rc| rc.contains_aggregates)
            {
                walk_expr(&rc.expr, &mut |expr: &Expr| -> Result<WalkControl> {
                    match expr {
                        Expr::Column { .. } | Expr::RowId { .. } => {
                            let reg = program.alloc_register();
                            translate_expr(
                                program,
                                Some(&plan.table_references),
                                expr,
                                reg,
                                &t_ctx.resolver,
                            )?;
                            t_ctx.resolver.cache_scalar_expr_reg(
                                Cow::Owned(expr.clone()),
                                reg,
                                false,
                                &plan.table_references,
                            )?;
                            Ok(WalkControl::SkipChildren)
                        }
                        _ => {
                            if plan.aggregates.iter().any(|a| a.original_expr == *expr) {
                                return Ok(WalkControl::SkipChildren);
                            }
                            Ok(WalkControl::Continue)
                        }
                    }
                })?;
            }

            if let Some(label) = label_emit_nonagg_only_once {
                program.preassign_label_to_next_insn(label);
                let flag = t_ctx.reg_nonagg_emit_once_flag.unwrap();
                program.emit_int(1, flag);
            }

            Ok(())
        }
        LoopEmitTarget::QueryResult => {
            turso_assert!(
                plan.aggregates.is_empty(),
                "QueryResult target should not have aggregates"
            );
            let offset_jump_to = plan
                .join_order
                .first()
                .and_then(|j| t_ctx.labels_main_loop.get(j.original_idx))
                .map(|l| l.next)
                .or(t_ctx.label_main_loop_end);

            emit_select_result(
                program,
                &t_ctx.resolver,
                plan,
                t_ctx.label_main_loop_end,
                offset_jump_to,
                t_ctx.reg_nonagg_emit_once_flag,
                t_ctx.reg_offset,
                t_ctx.reg_result_cols_start.unwrap(),
                t_ctx.limit_ctx,
            )?;

            if let Distinctness::Distinct { ctx } = &plan.distinctness {
                let distinct_ctx = ctx.as_ref().expect("distinct context must exist");
                program.preassign_label_to_next_insn(distinct_ctx.label_on_conflict);
            }

            Ok(())
        }
        LoopEmitTarget::Window => {
            EmitWindow::emit_window_step(program, t_ctx, plan)?;

            Ok(())
        }
    }
}

/// Whether every column a condition reads holds the right value at the
/// unmatched-row scan point.
///
/// A column is readable if its table's cursor is one of `allowed` and so still
/// positioned, or if it lives in `payload_regs` — this hash join's payload,
/// which the unmatched scan reloads for each row it produces. Registers cached
/// by anything else are stale here: nothing refills them once the probe loop has
/// exited. Skipping a condition whose columns are all readable silently drops it
/// from the null-extended rows, letting through rows the query filtered out.
fn condition_operands_are_available(
    expr: &Expr,
    table_references: &TableReferences,
    allowed: &TableMask,
    resolver: &Resolver,
    payload_regs: Range<usize>,
) -> bool {
    let mut ok = true;
    let _ = walk_expr(expr, &mut |e: &Expr| -> Result<WalkControl> {
        let (Expr::Column { table, .. } | Expr::RowId { table, .. }) = e else {
            return Ok(WalkControl::Continue);
        };
        if resolver
            .resolve_cached_expr_reg(e)
            .is_some_and(|(reg, ..)| payload_regs.contains(&reg))
        {
            return Ok(WalkControl::SkipChildren);
        }
        if let Some(idx) = table_references
            .joined_tables()
            .iter()
            .position(|t| t.internal_id == *table)
        {
            if !allowed.get(idx) {
                ok = false;
                return Ok(WalkControl::SkipChildren);
            }
        }
        // Outer query references are already in scope — allow them.
        Ok(WalkControl::Continue)
    });
    ok
}

/// Emit WHERE conditions and inner-loop entry for an unmatched outer hash join row.
///
/// Filters applicable WHERE terms (non-ON, non-consumed), optionally restricted to
/// the ones whose columns are readable here when a Gosub wraps inner tables. Then
/// either enters the inner-loop subroutine via Gosub or calls `emit_loop` directly.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_unmatched_row_conditions_and_loop<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    plan: &'a SelectPlan,
    build_table_idx: usize,
    probe_table_idx: usize,
    skip_label: BranchOffset,
    gosub: Option<(usize, BranchOffset)>,
    payload_regs: Range<usize>,
) -> Result<()> {
    let has_gosub = gosub.is_some();
    let allowed_tables = {
        let mut m = TableMask::default();
        m.set(build_table_idx)?;
        m.set(probe_table_idx)?;
        // When there's a gosub wrapping inner tables, we must also allow
        // conditions that reference outer tables (those appearing before the
        // hash join probe in the join order), since their cursors are valid
        // at the unmatched-scan point.
        if has_gosub {
            let probe_pos = plan
                .join_order
                .iter()
                .position(|j| j.original_idx == probe_table_idx)
                .expect("probe table must be in join order");
            for join in &plan.join_order[..probe_pos] {
                m.set(join.original_idx)?;
                // An earlier hash join's build table is also accessible.
                if let HashJoin(hj) = &plan.table_references.joined_tables()[join.original_idx].op {
                    m.set(hj.build_table_idx)?;
                }
            }
        }
        m
    };
    let conditions = plan
        .where_clause
        .iter()
        .filter(|c| !c.consumed && c.from_outer_join.is_none())
        .filter(|c| {
            !has_gosub
                || condition_operands_are_available(
                    &c.expr,
                    &plan.table_references,
                    &allowed_tables,
                    &t_ctx.resolver,
                    payload_regs.clone(),
                )
        })
        .collect::<Vec<_>>();
    for cond in conditions {
        let jump_target_when_true = program.allocate_label();
        let condition_metadata = ConditionMetadata {
            jump_if_condition_is_true: false,
            jump_target_when_true,
            jump_target_when_false: skip_label,
            jump_target_when_null: skip_label,
        };
        translate_condition_expr(
            program,
            &plan.table_references,
            &cond.expr,
            condition_metadata,
            &t_ctx.resolver,
        )?;
        program.preassign_label_to_next_insn(jump_target_when_true);
    }

    if let Some((reg, label)) = gosub {
        program.emit_insn(Insn::Gosub {
            target_pc: label,
            return_reg: reg,
        });
    } else {
        emit_loop(program, t_ctx, plan)?;
    }
    Ok(())
}
