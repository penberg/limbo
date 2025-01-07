use crate::{
    vdbe::{builder::ProgramBuilder, insn::Insn, BranchOffset},
    Result,
};

use super::{
    emitter::TranslateCtx,
    expr::translate_expr,
    plan::{SelectPlan, SelectQueryType},
};

/// Emits the bytecode for:
/// - all result columns
/// - result row (or if a subquery, yields to the parent query)
/// - limit
pub fn emit_select_result(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
    label_on_limit_reached: Option<BranchOffset>,
) -> Result<()> {
    let start_reg = t_ctx.reg_result_cols_start.unwrap();
    for (i, rc) in plan.result_columns.iter().enumerate() {
        let reg = start_reg + i;
        translate_expr(
            program,
            Some(&plan.referenced_tables),
            &rc.expr,
            reg,
            &t_ctx.resolver,
        )?;
    }
    emit_result_row_and_limit(program, t_ctx, plan, start_reg, label_on_limit_reached)?;
    Ok(())
}

/// Emits the bytecode for:
/// - result row (or if a subquery, yields to the parent query)
/// - limit
pub fn emit_result_row_and_limit(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
    result_columns_start_reg: usize,
    label_on_limit_reached: Option<BranchOffset>,
) -> Result<()> {
    match &plan.query_type {
        SelectQueryType::TopLevel => {
            program.emit_insn(Insn::ResultRow {
                start_reg: result_columns_start_reg,
                count: plan.result_columns.len(),
            });
        }
        SelectQueryType::Subquery { yield_reg, .. } => {
            program.emit_insn(Insn::Yield {
                yield_reg: *yield_reg,
                end_offset: BranchOffset::Offset(0),
            });
        }
    }

    if let Some(limit) = plan.limit {
        if label_on_limit_reached.is_none() {
            // There are cases where LIMIT is ignored, e.g. aggregation without a GROUP BY clause.
            // We already early return on LIMIT 0, so we can just return here since the n of rows
            // is always 1 here.
            return Ok(());
        }
        program.emit_insn(Insn::Integer {
            value: limit as i64,
            dest: t_ctx.reg_limit.unwrap(),
        });
        program.mark_last_insn_constant();
        program.emit_insn(Insn::DecrJumpZero {
            reg: t_ctx.reg_limit.unwrap(),
            target_pc: label_on_limit_reached.unwrap(),
        });
    }
    Ok(())
}
