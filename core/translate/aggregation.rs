use sqlite3_parser::ast;

use crate::{
    function::AggFunc,
    vdbe::{builder::ProgramBuilder, insn::Insn},
    Result,
};

use super::{
    emitter::{Resolver, TranslateCtx},
    expr::translate_expr,
    plan::{Aggregate, SelectPlan, TableReference},
    result_row::emit_select_result,
};

/// Emits the bytecode for processing an aggregate without a GROUP BY clause.
/// This is called when the main query execution loop has finished processing,
/// and we can now materialize the aggregate results.
pub fn emit_ungrouped_aggregation<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    plan: &'a SelectPlan,
) -> Result<()> {
    let agg_start_reg = t_ctx.reg_agg_start.unwrap();
    for (i, agg) in plan.aggregates.iter().enumerate() {
        let agg_result_reg = agg_start_reg + i;
        program.emit_insn(Insn::AggFinal {
            register: agg_result_reg,
            func: agg.func.clone(),
        });
    }
    // we now have the agg results in (agg_start_reg..agg_start_reg + aggregates.len() - 1)
    // we need to call translate_expr on each result column, but replace the expr with a register copy in case any part of the
    // result column expression matches a) a group by column or b) an aggregation result.
    for (i, agg) in plan.aggregates.iter().enumerate() {
        t_ctx
            .resolver
            .expr_to_reg_cache
            .push((&agg.original_expr, agg_start_reg + i));
    }

    // This always emits a ResultRow because currently it can only be used for a single row result
    // Limit is None because we early exit on limit 0 and the max rows here is 1
    emit_select_result(program, t_ctx, plan, None)?;

    Ok(())
}

/// Emits the bytecode for processing an aggregate step.
/// E.g. in `SELECT SUM(price) FROM t`, 'price' is evaluated for every row, and the result is added to the accumulator.
///
/// This is distinct from the final step, which is called after the main loop has finished processing
/// and the actual result value of the aggregation is materialized.
pub fn translate_aggregation_step(
    program: &mut ProgramBuilder,
    referenced_tables: &[TableReference],
    agg: &Aggregate,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let dest = match agg.func {
        AggFunc::Avg => {
            if agg.args.len() != 1 {
                crate::bail_parse_error!("avg bad number of arguments");
            }
            let expr = &agg.args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Avg,
            });
            target_register
        }
        AggFunc::Count => {
            let expr_reg = if agg.args.is_empty() {
                program.alloc_register()
            } else {
                let expr = &agg.args[0];
                let expr_reg = program.alloc_register();
                let _ = translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
                expr_reg
            };
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Count,
            });
            target_register
        }
        AggFunc::GroupConcat => {
            if agg.args.len() != 1 && agg.args.len() != 2 {
                crate::bail_parse_error!("group_concat bad number of arguments");
            }

            let expr_reg = program.alloc_register();
            let delimiter_reg = program.alloc_register();

            let expr = &agg.args[0];
            let delimiter_expr: ast::Expr;

            if agg.args.len() == 2 {
                match &agg.args[1] {
                    ast::Expr::Column { .. } => {
                        delimiter_expr = agg.args[1].clone();
                    }
                    ast::Expr::Literal(ast::Literal::String(s)) => {
                        delimiter_expr = ast::Expr::Literal(ast::Literal::String(s.to_string()));
                    }
                    _ => crate::bail_parse_error!("Incorrect delimiter parameter"),
                };
            } else {
                delimiter_expr = ast::Expr::Literal(ast::Literal::String(String::from("\",\"")));
            }

            translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
            translate_expr(
                program,
                Some(referenced_tables),
                &delimiter_expr,
                delimiter_reg,
                resolver,
            )?;

            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: delimiter_reg,
                func: AggFunc::GroupConcat,
            });

            target_register
        }
        AggFunc::Max => {
            if agg.args.len() != 1 {
                crate::bail_parse_error!("max bad number of arguments");
            }
            let expr = &agg.args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Max,
            });
            target_register
        }
        AggFunc::Min => {
            if agg.args.len() != 1 {
                crate::bail_parse_error!("min bad number of arguments");
            }
            let expr = &agg.args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Min,
            });
            target_register
        }
        AggFunc::StringAgg => {
            if agg.args.len() != 2 {
                crate::bail_parse_error!("string_agg bad number of arguments");
            }

            let expr_reg = program.alloc_register();
            let delimiter_reg = program.alloc_register();

            let expr = &agg.args[0];
            let delimiter_expr = match &agg.args[1] {
                ast::Expr::Column { .. } => agg.args[1].clone(),
                ast::Expr::Literal(ast::Literal::String(s)) => {
                    ast::Expr::Literal(ast::Literal::String(s.to_string()))
                }
                _ => crate::bail_parse_error!("Incorrect delimiter parameter"),
            };

            translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
            translate_expr(
                program,
                Some(referenced_tables),
                &delimiter_expr,
                delimiter_reg,
                resolver,
            )?;

            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: delimiter_reg,
                func: AggFunc::StringAgg,
            });

            target_register
        }
        AggFunc::Sum => {
            if agg.args.len() != 1 {
                crate::bail_parse_error!("sum bad number of arguments");
            }
            let expr = &agg.args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Sum,
            });
            target_register
        }
        AggFunc::Total => {
            if agg.args.len() != 1 {
                crate::bail_parse_error!("total bad number of arguments");
            }
            let expr = &agg.args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                delimiter: 0,
                func: AggFunc::Total,
            });
            target_register
        }
    };
    Ok(dest)
}
