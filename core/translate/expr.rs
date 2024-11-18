use sqlite3_parser::ast::{self, UnaryOperator};

use super::optimizer::CachedResult;
#[cfg(feature = "json")]
use crate::function::JsonFunc;
use crate::function::{AggFunc, Func, FuncCtx, ScalarFunc};
use crate::schema::{PseudoTable, Table, Type};
use crate::util::normalize_ident;
use crate::vdbe::{builder::ProgramBuilder, BranchOffset, Insn};
use crate::Result;

use super::plan::{Aggregate, BTreeTableReference};

#[derive(Default, Debug, Clone, Copy)]
pub struct ConditionMetadata {
    pub jump_if_condition_is_true: bool,
    pub jump_target_when_true: BranchOffset,
    pub jump_target_when_false: BranchOffset,
}

pub fn translate_condition_expr(
    program: &mut ProgramBuilder,
    referenced_tables: &[BTreeTableReference],
    expr: &ast::Expr,
    cursor_hint: Option<usize>,
    condition_metadata: ConditionMetadata,
) -> Result<()> {
    match expr {
        ast::Expr::Between { .. } => todo!(),
        ast::Expr::Binary(lhs, ast::Operator::And, rhs) => {
            // In a binary AND, never jump to the 'jump_target_when_true' label on the first condition, because
            // the second condition must also be true.
            let _ = translate_condition_expr(
                program,
                referenced_tables,
                lhs,
                cursor_hint,
                ConditionMetadata {
                    jump_if_condition_is_true: false,
                    ..condition_metadata
                },
            );
            let _ = translate_condition_expr(
                program,
                referenced_tables,
                rhs,
                cursor_hint,
                condition_metadata,
            );
        }
        ast::Expr::Binary(lhs, ast::Operator::Or, rhs) => {
            let jump_target_when_false = program.allocate_label();
            let _ = translate_condition_expr(
                program,
                referenced_tables,
                lhs,
                cursor_hint,
                ConditionMetadata {
                    // If the first condition is true, we don't need to evaluate the second condition.
                    jump_if_condition_is_true: true,
                    jump_target_when_false,
                    ..condition_metadata
                },
            );
            program.resolve_label(jump_target_when_false, program.offset());
            let _ = translate_condition_expr(
                program,
                referenced_tables,
                rhs,
                cursor_hint,
                condition_metadata,
            );
        }
        ast::Expr::Binary(lhs, op, rhs) => {
            let lhs_reg = program.alloc_register();
            let _ = translate_expr(
                program,
                Some(referenced_tables),
                lhs,
                lhs_reg,
                cursor_hint,
                None,
            );
            if let ast::Expr::Literal(_) = lhs.as_ref() {
                program.mark_last_insn_constant()
            }
            let rhs_reg = program.alloc_register();
            let _ = translate_expr(
                program,
                Some(referenced_tables),
                rhs,
                rhs_reg,
                cursor_hint,
                None,
            );
            if let ast::Expr::Literal(_) = rhs.as_ref() {
                program.mark_last_insn_constant()
            }
            match op {
                ast::Operator::Greater => {
                    if condition_metadata.jump_if_condition_is_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Gt {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: condition_metadata.jump_target_when_true,
                            },
                            condition_metadata.jump_target_when_true,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::Le {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: condition_metadata.jump_target_when_false,
                            },
                            condition_metadata.jump_target_when_false,
                        )
                    }
                }
                ast::Operator::GreaterEquals => {
                    if condition_metadata.jump_if_condition_is_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Ge {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: condition_metadata.jump_target_when_true,
                            },
                            condition_metadata.jump_target_when_true,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::Lt {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: condition_metadata.jump_target_when_false,
                            },
                            condition_metadata.jump_target_when_false,
                        )
                    }
                }
                ast::Operator::Less => {
                    if condition_metadata.jump_if_condition_is_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Lt {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: condition_metadata.jump_target_when_true,
                            },
                            condition_metadata.jump_target_when_true,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::Ge {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: condition_metadata.jump_target_when_false,
                            },
                            condition_metadata.jump_target_when_false,
                        )
                    }
                }
                ast::Operator::LessEquals => {
                    if condition_metadata.jump_if_condition_is_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Le {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: condition_metadata.jump_target_when_true,
                            },
                            condition_metadata.jump_target_when_true,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::Gt {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: condition_metadata.jump_target_when_false,
                            },
                            condition_metadata.jump_target_when_false,
                        )
                    }
                }
                ast::Operator::Equals => {
                    if condition_metadata.jump_if_condition_is_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Eq {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: condition_metadata.jump_target_when_true,
                            },
                            condition_metadata.jump_target_when_true,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::Ne {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: condition_metadata.jump_target_when_false,
                            },
                            condition_metadata.jump_target_when_false,
                        )
                    }
                }
                ast::Operator::NotEquals => {
                    if condition_metadata.jump_if_condition_is_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Ne {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: condition_metadata.jump_target_when_true,
                            },
                            condition_metadata.jump_target_when_true,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::Eq {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: condition_metadata.jump_target_when_false,
                            },
                            condition_metadata.jump_target_when_false,
                        )
                    }
                }
                ast::Operator::Is => todo!(),
                ast::Operator::IsNot => todo!(),
                _ => {
                    todo!("op {:?} not implemented", op);
                }
            }
        }
        ast::Expr::Literal(lit) => match lit {
            ast::Literal::Numeric(val) => {
                let maybe_int = val.parse::<i64>();
                if let Ok(int_value) = maybe_int {
                    let reg = program.alloc_register();
                    program.emit_insn(Insn::Integer {
                        value: int_value,
                        dest: reg,
                    });
                    if condition_metadata.jump_if_condition_is_true {
                        program.emit_insn_with_label_dependency(
                            Insn::If {
                                reg,
                                target_pc: condition_metadata.jump_target_when_true,
                                null_reg: reg,
                            },
                            condition_metadata.jump_target_when_true,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::IfNot {
                                reg,
                                target_pc: condition_metadata.jump_target_when_false,
                                null_reg: reg,
                            },
                            condition_metadata.jump_target_when_false,
                        )
                    }
                } else {
                    crate::bail_parse_error!("unsupported literal type in condition");
                }
            }
            ast::Literal::String(string) => {
                let reg = program.alloc_register();
                program.emit_insn(Insn::String8 {
                    value: string.clone(),
                    dest: reg,
                });
                if condition_metadata.jump_if_condition_is_true {
                    program.emit_insn_with_label_dependency(
                        Insn::If {
                            reg,
                            target_pc: condition_metadata.jump_target_when_true,
                            null_reg: reg,
                        },
                        condition_metadata.jump_target_when_true,
                    )
                } else {
                    program.emit_insn_with_label_dependency(
                        Insn::IfNot {
                            reg,
                            target_pc: condition_metadata.jump_target_when_false,
                            null_reg: reg,
                        },
                        condition_metadata.jump_target_when_false,
                    )
                }
            }
            unimpl => todo!("literal {:?} not implemented", unimpl),
        },
        ast::Expr::InList { lhs, not, rhs } => {
            // lhs is e.g. a column reference
            // rhs is an Option<Vec<Expr>>
            // If rhs is None, it means the IN expression is always false, i.e. tbl.id IN ().
            // If rhs is Some, it means the IN expression has a list of values to compare against, e.g. tbl.id IN (1, 2, 3).
            //
            // The IN expression is equivalent to a series of OR expressions.
            // For example, `a IN (1, 2, 3)` is equivalent to `a = 1 OR a = 2 OR a = 3`.
            // The NOT IN expression is equivalent to a series of AND expressions.
            // For example, `a NOT IN (1, 2, 3)` is equivalent to `a != 1 AND a != 2 AND a != 3`.
            //
            // SQLite typically optimizes IN expressions to use a binary search on an ephemeral index if there are many values.
            // For now we don't have the plumbing to do that, so we'll just emit a series of comparisons,
            // which is what SQLite also does for small lists of values.
            // TODO: Let's refactor this later to use a more efficient implementation conditionally based on the number of values.

            if rhs.is_none() {
                // If rhs is None, IN expressions are always false and NOT IN expressions are always true.
                if *not {
                    // On a trivially true NOT IN () expression we can only jump to the 'jump_target_when_true' label if 'jump_if_condition_is_true'; otherwise me must fall through.
                    // This is because in a more complex condition we might need to evaluate the rest of the condition.
                    // Note that we are already breaking up our WHERE clauses into a series of terms at "AND" boundaries, so right now we won't be running into cases where jumping on true would be incorrect,
                    // but once we have e.g. parenthesization and more complex conditions, not having this 'if' here would introduce a bug.
                    if condition_metadata.jump_if_condition_is_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Goto {
                                target_pc: condition_metadata.jump_target_when_true,
                            },
                            condition_metadata.jump_target_when_true,
                        );
                    }
                } else {
                    program.emit_insn_with_label_dependency(
                        Insn::Goto {
                            target_pc: condition_metadata.jump_target_when_false,
                        },
                        condition_metadata.jump_target_when_false,
                    );
                }
                return Ok(());
            }

            // The left hand side only needs to be evaluated once we have a list of values to compare against.
            let lhs_reg = program.alloc_register();
            let _ = translate_expr(
                program,
                Some(referenced_tables),
                lhs,
                lhs_reg,
                cursor_hint,
                None,
            )?;

            let rhs = rhs.as_ref().unwrap();

            // The difference between a local jump and an "upper level" jump is that for example in this case:
            // WHERE foo IN (1,2,3) OR bar = 5,
            // we can immediately jump to the 'jump_target_when_true' label of the ENTIRE CONDITION if foo = 1, foo = 2, or foo = 3 without evaluating the bar = 5 condition.
            // This is why in Binary-OR expressions we set jump_if_condition_is_true to true for the first condition.
            // However, in this example:
            // WHERE foo IN (1,2,3) AND bar = 5,
            // we can't jump to the 'jump_target_when_true' label of the entire condition foo = 1, foo = 2, or foo = 3, because we still need to evaluate the bar = 5 condition later.
            // This is why in that case we just jump over the rest of the IN conditions in this "local" branch which evaluates the IN condition.
            let jump_target_when_true = if condition_metadata.jump_if_condition_is_true {
                condition_metadata.jump_target_when_true
            } else {
                program.allocate_label()
            };

            if !*not {
                // If it's an IN expression, we need to jump to the 'jump_target_when_true' label if any of the conditions are true.
                for (i, expr) in rhs.iter().enumerate() {
                    let rhs_reg = program.alloc_register();
                    let last_condition = i == rhs.len() - 1;
                    let _ = translate_expr(
                        program,
                        Some(referenced_tables),
                        expr,
                        rhs_reg,
                        cursor_hint,
                        None,
                    )?;
                    // If this is not the last condition, we need to jump to the 'jump_target_when_true' label if the condition is true.
                    if !last_condition {
                        program.emit_insn_with_label_dependency(
                            Insn::Eq {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: jump_target_when_true,
                            },
                            jump_target_when_true,
                        );
                    } else {
                        // If this is the last condition, we need to jump to the 'jump_target_when_false' label if there is no match.
                        program.emit_insn_with_label_dependency(
                            Insn::Ne {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: condition_metadata.jump_target_when_false,
                            },
                            condition_metadata.jump_target_when_false,
                        );
                    }
                }
                // If we got here, then the last condition was a match, so we jump to the 'jump_target_when_true' label if 'jump_if_condition_is_true'.
                // If not, we can just fall through without emitting an unnecessary instruction.
                if condition_metadata.jump_if_condition_is_true {
                    program.emit_insn_with_label_dependency(
                        Insn::Goto {
                            target_pc: condition_metadata.jump_target_when_true,
                        },
                        condition_metadata.jump_target_when_true,
                    );
                }
            } else {
                // If it's a NOT IN expression, we need to jump to the 'jump_target_when_false' label if any of the conditions are true.
                for expr in rhs.iter() {
                    let rhs_reg = program.alloc_register();
                    let _ = translate_expr(
                        program,
                        Some(referenced_tables),
                        expr,
                        rhs_reg,
                        cursor_hint,
                        None,
                    )?;
                    program.emit_insn_with_label_dependency(
                        Insn::Eq {
                            lhs: lhs_reg,
                            rhs: rhs_reg,
                            target_pc: condition_metadata.jump_target_when_false,
                        },
                        condition_metadata.jump_target_when_false,
                    );
                }
                // If we got here, then none of the conditions were a match, so we jump to the 'jump_target_when_true' label if 'jump_if_condition_is_true'.
                // If not, we can just fall through without emitting an unnecessary instruction.
                if condition_metadata.jump_if_condition_is_true {
                    program.emit_insn_with_label_dependency(
                        Insn::Goto {
                            target_pc: condition_metadata.jump_target_when_true,
                        },
                        condition_metadata.jump_target_when_true,
                    );
                }
            }

            if !condition_metadata.jump_if_condition_is_true {
                program.resolve_label(jump_target_when_true, program.offset());
            }
        }
        ast::Expr::Like {
            lhs,
            not,
            op,
            rhs,
            escape: _,
        } => {
            let cur_reg = program.alloc_register();
            match op {
                ast::LikeOperator::Like | ast::LikeOperator::Glob => {
                    let pattern_reg = program.alloc_register();
                    let column_reg = program.alloc_register();
                    let mut constant_mask = 0;
                    let _ = translate_expr(
                        program,
                        Some(referenced_tables),
                        lhs,
                        column_reg,
                        cursor_hint,
                        None,
                    )?;
                    if let ast::Expr::Literal(_) = lhs.as_ref() {
                        program.mark_last_insn_constant();
                    }
                    let _ = translate_expr(
                        program,
                        Some(referenced_tables),
                        rhs,
                        pattern_reg,
                        cursor_hint,
                        None,
                    )?;
                    if let ast::Expr::Literal(_) = rhs.as_ref() {
                        program.mark_last_insn_constant();
                        constant_mask = 1;
                    }
                    let func = match op {
                        ast::LikeOperator::Like => ScalarFunc::Like,
                        ast::LikeOperator::Glob => ScalarFunc::Glob,
                        _ => unreachable!(),
                    };
                    program.emit_insn(Insn::Function {
                        constant_mask,
                        start_reg: pattern_reg,
                        dest: cur_reg,
                        func: FuncCtx {
                            func: Func::Scalar(func),
                            arg_count: 2,
                        },
                    });
                }
                ast::LikeOperator::Match => todo!(),
                ast::LikeOperator::Regexp => todo!(),
            }
            if !*not {
                if condition_metadata.jump_if_condition_is_true {
                    program.emit_insn_with_label_dependency(
                        Insn::If {
                            reg: cur_reg,
                            target_pc: condition_metadata.jump_target_when_true,
                            null_reg: cur_reg,
                        },
                        condition_metadata.jump_target_when_true,
                    );
                } else {
                    program.emit_insn_with_label_dependency(
                        Insn::IfNot {
                            reg: cur_reg,
                            target_pc: condition_metadata.jump_target_when_false,
                            null_reg: cur_reg,
                        },
                        condition_metadata.jump_target_when_false,
                    );
                }
            } else if condition_metadata.jump_if_condition_is_true {
                program.emit_insn_with_label_dependency(
                    Insn::IfNot {
                        reg: cur_reg,
                        target_pc: condition_metadata.jump_target_when_true,
                        null_reg: cur_reg,
                    },
                    condition_metadata.jump_target_when_true,
                );
            } else {
                program.emit_insn_with_label_dependency(
                    Insn::If {
                        reg: cur_reg,
                        target_pc: condition_metadata.jump_target_when_false,
                        null_reg: cur_reg,
                    },
                    condition_metadata.jump_target_when_false,
                );
            }
        }
        ast::Expr::Parenthesized(exprs) => {
            // TODO: this is probably not correct; multiple expressions in a parenthesized expression
            // are reserved for special cases like `(a, b) IN ((1, 2), (3, 4))`.
            for expr in exprs {
                let _ = translate_condition_expr(
                    program,
                    referenced_tables,
                    expr,
                    cursor_hint,
                    condition_metadata,
                );
            }
        }
        _ => todo!("op {:?} not implemented", expr),
    }
    Ok(())
}

pub fn get_cached_or_translate(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&[BTreeTableReference]>,
    expr: &ast::Expr,
    cursor_hint: Option<usize>,
    cached_results: Option<&Vec<&CachedResult>>,
) -> Result<usize> {
    if let Some(cached_results) = cached_results {
        if let Some(cached_result) = cached_results
            .iter()
            .find(|cached_result| cached_result.source_expr == *expr)
        {
            return Ok(cached_result.register_idx);
        }
    }
    let reg = program.alloc_register();
    translate_expr(
        program,
        referenced_tables,
        expr,
        reg,
        cursor_hint,
        cached_results,
    )?;
    Ok(reg)
}

pub fn translate_expr(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&[BTreeTableReference]>,
    expr: &ast::Expr,
    target_register: usize,
    cursor_hint: Option<usize>,
    cached_results: Option<&Vec<&CachedResult>>,
) -> Result<usize> {
    if let Some(cached_results) = &cached_results {
        if let Some(cached_result) = cached_results
            .iter()
            .find(|cached_result| cached_result.source_expr == *expr)
        {
            program.emit_insn(Insn::Copy {
                src_reg: cached_result.register_idx,
                dst_reg: target_register,
                amount: 0,
            });
            return Ok(target_register);
        }
    }

    match expr {
        ast::Expr::Between { .. } => todo!(),
        ast::Expr::Binary(e1, op, e2) => {
            let e1_reg = get_cached_or_translate(
                program,
                referenced_tables,
                e1,
                cursor_hint,
                cached_results,
            )?;
            let e2_reg = get_cached_or_translate(
                program,
                referenced_tables,
                e2,
                cursor_hint,
                cached_results,
            )?;

            match op {
                ast::Operator::NotEquals => {
                    let if_true_label = program.allocate_label();
                    wrap_eval_jump_expr(
                        program,
                        Insn::Ne {
                            lhs: e1_reg,
                            rhs: e2_reg,
                            target_pc: if_true_label,
                        },
                        target_register,
                        if_true_label,
                    );
                }
                ast::Operator::Equals => {
                    let if_true_label = program.allocate_label();
                    wrap_eval_jump_expr(
                        program,
                        Insn::Eq {
                            lhs: e1_reg,
                            rhs: e2_reg,
                            target_pc: if_true_label,
                        },
                        target_register,
                        if_true_label,
                    );
                }
                ast::Operator::Less => {
                    let if_true_label = program.allocate_label();
                    wrap_eval_jump_expr(
                        program,
                        Insn::Lt {
                            lhs: e1_reg,
                            rhs: e2_reg,
                            target_pc: if_true_label,
                        },
                        target_register,
                        if_true_label,
                    );
                }
                ast::Operator::LessEquals => {
                    let if_true_label = program.allocate_label();
                    wrap_eval_jump_expr(
                        program,
                        Insn::Le {
                            lhs: e1_reg,
                            rhs: e2_reg,
                            target_pc: if_true_label,
                        },
                        target_register,
                        if_true_label,
                    );
                }
                ast::Operator::Greater => {
                    let if_true_label = program.allocate_label();
                    wrap_eval_jump_expr(
                        program,
                        Insn::Gt {
                            lhs: e1_reg,
                            rhs: e2_reg,
                            target_pc: if_true_label,
                        },
                        target_register,
                        if_true_label,
                    );
                }
                ast::Operator::GreaterEquals => {
                    let if_true_label = program.allocate_label();
                    wrap_eval_jump_expr(
                        program,
                        Insn::Ge {
                            lhs: e1_reg,
                            rhs: e2_reg,
                            target_pc: if_true_label,
                        },
                        target_register,
                        if_true_label,
                    );
                }
                ast::Operator::Add => {
                    program.emit_insn(Insn::Add {
                        lhs: e1_reg,
                        rhs: e2_reg,
                        dest: target_register,
                    });
                }
                ast::Operator::Subtract => {
                    program.emit_insn(Insn::Subtract {
                        lhs: e1_reg,
                        rhs: e2_reg,
                        dest: target_register,
                    });
                }
                ast::Operator::Multiply => {
                    program.emit_insn(Insn::Multiply {
                        lhs: e1_reg,
                        rhs: e2_reg,
                        dest: target_register,
                    });
                }
                other_unimplemented => todo!("{:?}", other_unimplemented),
            }
            Ok(target_register)
        }
        ast::Expr::Case { .. } => todo!(),
        ast::Expr::Cast { expr, type_name } => {
            let type_name = type_name.as_ref().unwrap(); // TODO: why is this optional?
            let reg_expr = program.alloc_register();
            translate_expr(
                program,
                referenced_tables,
                expr,
                reg_expr,
                cursor_hint,
                cached_results,
            )?;
            let reg_type = program.alloc_register();
            program.emit_insn(Insn::String8 {
                // we make a comparison against uppercase static strs in the affinity() function,
                // so we need to make sure we're comparing against the uppercase version,
                // and it's better to do this once instead of every time we check affinity
                value: type_name.name.to_uppercase(),
                dest: reg_type,
            });
            program.mark_last_insn_constant();
            program.emit_insn(Insn::Function {
                constant_mask: 0,
                start_reg: reg_expr,
                dest: target_register,
                func: FuncCtx {
                    func: Func::Scalar(ScalarFunc::Cast),
                    arg_count: 2,
                },
            });
            Ok(target_register)
        }
        ast::Expr::Collate(_, _) => todo!(),
        ast::Expr::DoublyQualified(_, _, _) => todo!(),
        ast::Expr::Exists(_) => todo!(),
        ast::Expr::FunctionCall {
            name,
            distinctness: _,
            args,
            filter_over: _,
            order_by: _,
        } => {
            let args_count = if let Some(args) = args { args.len() } else { 0 };
            let func_type: Option<Func> =
                Func::resolve_function(normalize_ident(name.0.as_str()).as_str(), args_count).ok();

            if func_type.is_none() {
                crate::bail_parse_error!("unknown function {}", name.0);
            }

            let func_ctx = FuncCtx {
                func: func_type.unwrap(),
                arg_count: args_count,
            };

            match &func_ctx.func {
                Func::Agg(_) => {
                    crate::bail_parse_error!("aggregation function in non-aggregation context")
                }
                #[cfg(feature = "json")]
                Func::Json(j) => match j {
                    JsonFunc::Json => {
                        let args = if let Some(args) = args {
                            if args.len() != 1 {
                                crate::bail_parse_error!(
                                    "{} function with not exactly 1 argument",
                                    j.to_string()
                                );
                            }
                            args
                        } else {
                            crate::bail_parse_error!(
                                "{} function with no arguments",
                                j.to_string()
                            );
                        };
                        let regs = program.alloc_register();
                        translate_expr(
                            program,
                            referenced_tables,
                            &args[0],
                            regs,
                            cursor_hint,
                            cached_results,
                        )?;
                        program.emit_insn(Insn::Function {
                            constant_mask: 0,
                            start_reg: regs,
                            dest: target_register,
                            func: func_ctx,
                        });
                        Ok(target_register)
                    }
                },
                Func::Scalar(srf) => {
                    match srf {
                        ScalarFunc::Cast => {
                            unreachable!("this is always ast::Expr::Cast")
                        }
                        ScalarFunc::Char => {
                            let args = args.clone().unwrap_or_else(Vec::new);

                            for arg in args.iter() {
                                let reg = program.alloc_register();
                                translate_expr(
                                    program,
                                    referenced_tables,
                                    arg,
                                    reg,
                                    cursor_hint,
                                    cached_results,
                                )?;
                            }

                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: target_register + 1,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Coalesce => {
                            let args = if let Some(args) = args {
                                if args.len() < 2 {
                                    crate::bail_parse_error!(
                                        "{} function with less than 2 arguments",
                                        srf.to_string()
                                    );
                                }
                                args
                            } else {
                                crate::bail_parse_error!(
                                    "{} function with no arguments",
                                    srf.to_string()
                                );
                            };

                            // coalesce function is implemented as a series of not null checks
                            // whenever a not null check succeeds, we jump to the end of the series
                            let label_coalesce_end = program.allocate_label();
                            for (index, arg) in args.iter().enumerate() {
                                let reg = translate_expr(
                                    program,
                                    referenced_tables,
                                    arg,
                                    target_register,
                                    cursor_hint,
                                    cached_results,
                                )?;
                                if index < args.len() - 1 {
                                    program.emit_insn_with_label_dependency(
                                        Insn::NotNull {
                                            reg,
                                            target_pc: label_coalesce_end,
                                        },
                                        label_coalesce_end,
                                    );
                                }
                            }
                            program.preassign_label_to_next_insn(label_coalesce_end);

                            Ok(target_register)
                        }
                        ScalarFunc::Concat => {
                            let args = if let Some(args) = args {
                                args
                            } else {
                                crate::bail_parse_error!(
                                    "{} function with no arguments",
                                    srf.to_string()
                                );
                            };
                            let mut start_reg = None;
                            for arg in args.iter() {
                                let reg = program.alloc_register();
                                start_reg = Some(start_reg.unwrap_or(reg));

                                translate_expr(
                                    program,
                                    referenced_tables,
                                    arg,
                                    reg,
                                    cursor_hint,
                                    cached_results,
                                )?;
                            }
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: start_reg.unwrap(),
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::ConcatWs => {
                            let args = match args {
                                Some(args) if args.len() >= 2 => args,
                                Some(_) => crate::bail_parse_error!(
                                    "{} function requires at least 2 arguments",
                                    srf.to_string()
                                ),
                                None => crate::bail_parse_error!(
                                    "{} function requires arguments",
                                    srf.to_string()
                                ),
                            };

                            let temp_register = program.alloc_register();
                            for arg in args.iter() {
                                let reg = program.alloc_register();
                                translate_expr(
                                    program,
                                    referenced_tables,
                                    arg,
                                    reg,
                                    cursor_hint,
                                    cached_results,
                                )?;
                            }
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: temp_register + 1,
                                dest: temp_register,
                                func: func_ctx,
                            });

                            program.emit_insn(Insn::Copy {
                                src_reg: temp_register,
                                dst_reg: target_register,
                                amount: 1,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::IfNull => {
                            let args = match args {
                                Some(args) if args.len() == 2 => args,
                                Some(_) => crate::bail_parse_error!(
                                    "{} function requires exactly 2 arguments",
                                    srf.to_string()
                                ),
                                None => crate::bail_parse_error!(
                                    "{} function requires arguments",
                                    srf.to_string()
                                ),
                            };

                            let temp_reg = program.alloc_register();
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                temp_reg,
                                cursor_hint,
                                cached_results,
                            )?;
                            program.emit_insn(Insn::NotNull {
                                reg: temp_reg,
                                target_pc: program.offset() + 2,
                            });

                            translate_expr(
                                program,
                                referenced_tables,
                                &args[1],
                                temp_reg,
                                cursor_hint,
                                cached_results,
                            )?;
                            program.emit_insn(Insn::Copy {
                                src_reg: temp_reg,
                                dst_reg: target_register,
                                amount: 0,
                            });

                            Ok(target_register)
                        }
                        ScalarFunc::Glob | ScalarFunc::Like => {
                            let args = if let Some(args) = args {
                                if args.len() < 2 {
                                    crate::bail_parse_error!(
                                        "{} function with less than 2 arguments",
                                        srf.to_string()
                                    );
                                }
                                args
                            } else {
                                crate::bail_parse_error!(
                                    "{} function with no arguments",
                                    srf.to_string()
                                );
                            };
                            for arg in args {
                                let reg = program.alloc_register();
                                let _ = translate_expr(
                                    program,
                                    referenced_tables,
                                    arg,
                                    reg,
                                    cursor_hint,
                                    cached_results,
                                )?;
                                if let ast::Expr::Literal(_) = arg {
                                    program.mark_last_insn_constant()
                                }
                            }
                            program.emit_insn(Insn::Function {
                                // Only constant patterns for LIKE are supported currently, so this
                                // is always 1
                                constant_mask: 1,
                                start_reg: target_register + 1,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Abs
                        | ScalarFunc::Lower
                        | ScalarFunc::Upper
                        | ScalarFunc::Length
                        | ScalarFunc::Typeof
                        | ScalarFunc::Unicode
                        | ScalarFunc::Quote
                        | ScalarFunc::RandomBlob
                        | ScalarFunc::Sign
                        | ScalarFunc::ZeroBlob => {
                            let args = if let Some(args) = args {
                                if args.len() != 1 {
                                    crate::bail_parse_error!(
                                        "{} function with not exactly 1 argument",
                                        srf.to_string()
                                    );
                                }
                                args
                            } else {
                                crate::bail_parse_error!(
                                    "{} function with no arguments",
                                    srf.to_string()
                                );
                            };

                            let regs = program.alloc_register();
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                regs,
                                cursor_hint,
                                cached_results,
                            )?;
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: regs,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Random => {
                            if args.is_some() {
                                crate::bail_parse_error!(
                                    "{} function with arguments",
                                    srf.to_string()
                                );
                            }
                            let regs = program.alloc_register();
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: regs,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Date => {
                            if let Some(args) = args {
                                for arg in args.iter() {
                                    // register containing result of each argument expression
                                    let target_reg = program.alloc_register();
                                    _ = translate_expr(
                                        program,
                                        referenced_tables,
                                        arg,
                                        target_reg,
                                        cursor_hint,
                                        cached_results,
                                    )?;
                                }
                            }
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: target_register + 1,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Substr | ScalarFunc::Substring => {
                            let args = if let Some(args) = args {
                                if !(args.len() == 2 || args.len() == 3) {
                                    crate::bail_parse_error!(
                                        "{} function with wrong number of arguments",
                                        srf.to_string()
                                    )
                                }
                                args
                            } else {
                                crate::bail_parse_error!(
                                    "{} function with no arguments",
                                    srf.to_string()
                                );
                            };

                            let str_reg = program.alloc_register();
                            let start_reg = program.alloc_register();
                            let length_reg = program.alloc_register();

                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                str_reg,
                                cursor_hint,
                                cached_results,
                            )?;
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[1],
                                start_reg,
                                cursor_hint,
                                cached_results,
                            )?;
                            if args.len() == 3 {
                                translate_expr(
                                    program,
                                    referenced_tables,
                                    &args[2],
                                    length_reg,
                                    cursor_hint,
                                    cached_results,
                                )?;
                            }

                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: str_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Hex => {
                            let args = if let Some(args) = args {
                                if args.len() != 1 {
                                    crate::bail_parse_error!(
                                        "hex function must have exactly 1 argument",
                                    );
                                }
                                args
                            } else {
                                crate::bail_parse_error!("hex function with no arguments",);
                            };
                            let regs = program.alloc_register();
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                regs,
                                cursor_hint,
                                cached_results,
                            )?;
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: regs,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::UnixEpoch => {
                            let mut start_reg = 0;
                            match args {
                                Some(args) if args.len() > 1 => {
                                    crate::bail_parse_error!("epoch function with > 1 arguments. Modifiers are not yet supported.");
                                }
                                Some(args) if args.len() == 1 => {
                                    let arg_reg = program.alloc_register();
                                    let _ = translate_expr(
                                        program,
                                        referenced_tables,
                                        &args[0],
                                        arg_reg,
                                        cursor_hint,
                                        cached_results,
                                    )?;
                                    start_reg = arg_reg;
                                }
                                _ => {}
                            }
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Time => {
                            if let Some(args) = args {
                                for arg in args.iter() {
                                    // register containing result of each argument expression
                                    let target_reg = program.alloc_register();
                                    _ = translate_expr(
                                        program,
                                        referenced_tables,
                                        arg,
                                        target_reg,
                                        cursor_hint,
                                        cached_results,
                                    )?;
                                }
                            }
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: target_register + 1,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Trim
                        | ScalarFunc::LTrim
                        | ScalarFunc::RTrim
                        | ScalarFunc::Round
                        | ScalarFunc::Unhex => {
                            let args = if let Some(args) = args {
                                if args.len() > 2 {
                                    crate::bail_parse_error!(
                                        "{} function with more than 2 arguments",
                                        srf.to_string()
                                    );
                                }
                                args
                            } else {
                                crate::bail_parse_error!(
                                    "{} function with no arguments",
                                    srf.to_string()
                                );
                            };

                            for arg in args.iter() {
                                let reg = program.alloc_register();
                                translate_expr(
                                    program,
                                    referenced_tables,
                                    arg,
                                    reg,
                                    cursor_hint,
                                    cached_results,
                                )?;
                                if let ast::Expr::Literal(_) = arg {
                                    program.mark_last_insn_constant();
                                }
                            }
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: target_register + 1,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Min => {
                            let args = if let Some(args) = args {
                                if args.is_empty() {
                                    crate::bail_parse_error!(
                                        "min function with less than one argument"
                                    );
                                }
                                args
                            } else {
                                crate::bail_parse_error!("min function with no arguments");
                            };
                            for arg in args {
                                let reg = program.alloc_register();
                                let _ = translate_expr(
                                    program,
                                    referenced_tables,
                                    arg,
                                    reg,
                                    cursor_hint,
                                    cached_results,
                                )?;
                                if let ast::Expr::Literal(_) = arg {
                                    program.mark_last_insn_constant()
                                }
                            }

                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: target_register + 1,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Max => {
                            let args = if let Some(args) = args {
                                if args.is_empty() {
                                    crate::bail_parse_error!(
                                        "max function with less than one argument"
                                    );
                                }
                                args
                            } else {
                                crate::bail_parse_error!("max function with no arguments");
                            };
                            for arg in args {
                                let reg = program.alloc_register();
                                let _ = translate_expr(
                                    program,
                                    referenced_tables,
                                    arg,
                                    reg,
                                    cursor_hint,
                                    cached_results,
                                )?;
                                if let ast::Expr::Literal(_) = arg {
                                    program.mark_last_insn_constant()
                                }
                            }

                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: target_register + 1,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Nullif | ScalarFunc::Instr => {
                            let args = if let Some(args) = args {
                                if args.len() != 2 {
                                    crate::bail_parse_error!(
                                        "{} function must have two argument",
                                        srf.to_string()
                                    );
                                }
                                args
                            } else {
                                crate::bail_parse_error!(
                                    "{} function with no arguments",
                                    srf.to_string()
                                );
                            };

                            let first_reg = program.alloc_register();
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                first_reg,
                                cursor_hint,
                                cached_results,
                            )?;
                            let second_reg = program.alloc_register();
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[1],
                                second_reg,
                                cursor_hint,
                                cached_results,
                            )?;
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: first_reg,
                                dest: target_register,
                                func: func_ctx,
                            });

                            Ok(target_register)
                        }
                        ScalarFunc::SqliteVersion => {
                            if args.is_some() {
                                crate::bail_parse_error!("sqlite_version function with arguments");
                            }

                            let output_register = program.alloc_register();
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: output_register,
                                dest: output_register,
                                func: func_ctx,
                            });

                            program.emit_insn(Insn::Copy {
                                src_reg: output_register,
                                dst_reg: target_register,
                                amount: 1,
                            });
                            Ok(target_register)
                        }
                    }
                }
            }
        }
        ast::Expr::FunctionCallStar { .. } => todo!(),
        ast::Expr::Id(ident) => {
            // let (idx, col) = table.unwrap().get_column(&ident.0).unwrap();
            let (idx, col_type, cursor_id, is_rowid_alias) =
                resolve_ident_table(program, &ident.0, referenced_tables, cursor_hint)?;
            if is_rowid_alias {
                program.emit_insn(Insn::RowId {
                    cursor_id,
                    dest: target_register,
                });
            } else {
                program.emit_insn(Insn::Column {
                    column: idx,
                    dest: target_register,
                    cursor_id,
                });
            }
            maybe_apply_affinity(col_type, target_register, program);
            Ok(target_register)
        }
        ast::Expr::InList { .. } => todo!(),
        ast::Expr::InSelect { .. } => todo!(),
        ast::Expr::InTable { .. } => todo!(),
        ast::Expr::IsNull(_) => todo!(),
        ast::Expr::Like { .. } => todo!(),
        ast::Expr::Literal(lit) => match lit {
            ast::Literal::Numeric(val) => {
                let maybe_int = val.parse::<i64>();
                if let Ok(int_value) = maybe_int {
                    program.emit_insn(Insn::Integer {
                        value: int_value,
                        dest: target_register,
                    });
                } else {
                    // must be a float
                    program.emit_insn(Insn::Real {
                        value: val.parse().unwrap(),
                        dest: target_register,
                    });
                }
                Ok(target_register)
            }
            ast::Literal::String(s) => {
                program.emit_insn(Insn::String8 {
                    value: s[1..s.len() - 1].to_string(),
                    dest: target_register,
                });
                Ok(target_register)
            }
            ast::Literal::Blob(s) => {
                let bytes = s
                    .as_bytes()
                    .chunks_exact(2)
                    .map(|pair| {
                        // We assume that sqlite3-parser has already validated that
                        // the input is valid hex string, thus unwrap is safe.
                        let hex_byte = std::str::from_utf8(pair).unwrap();
                        u8::from_str_radix(hex_byte, 16).unwrap()
                    })
                    .collect();
                program.emit_insn(Insn::Blob {
                    value: bytes,
                    dest: target_register,
                });
                Ok(target_register)
            }
            ast::Literal::Keyword(_) => todo!(),
            ast::Literal::Null => {
                program.emit_insn(Insn::Null {
                    dest: target_register,
                    dest_end: None,
                });
                Ok(target_register)
            }
            ast::Literal::CurrentDate => todo!(),
            ast::Literal::CurrentTime => todo!(),
            ast::Literal::CurrentTimestamp => todo!(),
        },
        ast::Expr::Name(_) => todo!(),
        ast::Expr::NotNull(_) => todo!(),
        ast::Expr::Parenthesized(exprs) => {
            if exprs.is_empty() {
                crate::bail_parse_error!("parenthesized expression with no arguments");
            }
            if exprs.len() == 1 {
                translate_expr(
                    program,
                    referenced_tables,
                    &exprs[0],
                    target_register,
                    cursor_hint,
                    cached_results,
                )?;
            } else {
                // Parenthesized expressions with multiple arguments are reserved for special cases
                // like `(a, b) IN ((1, 2), (3, 4))`.
                todo!("TODO: parenthesized expression with multiple arguments not yet supported");
            }
            Ok(target_register)
        }
        ast::Expr::Qualified(tbl, ident) => {
            let (idx, col_type, cursor_id, is_primary_key) = resolve_ident_qualified(
                program,
                &tbl.0,
                &ident.0,
                referenced_tables.unwrap(),
                cursor_hint,
            )?;
            if is_primary_key {
                program.emit_insn(Insn::RowId {
                    cursor_id,
                    dest: target_register,
                });
            } else {
                program.emit_insn(Insn::Column {
                    column: idx,
                    dest: target_register,
                    cursor_id,
                });
            }
            maybe_apply_affinity(col_type, target_register, program);
            Ok(target_register)
        }
        ast::Expr::Raise(_, _) => todo!(),
        ast::Expr::Subquery(_) => todo!(),
        ast::Expr::Unary(op, expr) => match (op, expr.as_ref()) {
            (UnaryOperator::Negative, ast::Expr::Literal(ast::Literal::Numeric(numeric_value))) => {
                let maybe_int = numeric_value.parse::<i64>();
                if let Ok(value) = maybe_int {
                    program.emit_insn(Insn::Integer {
                        value: -value,
                        dest: target_register,
                    });
                } else {
                    program.emit_insn(Insn::Real {
                        value: -numeric_value.parse::<f64>()?,
                        dest: target_register,
                    });
                }
                Ok(target_register)
            }
            _ => todo!(),
        },
        ast::Expr::Variable(_) => todo!(),
    }
}

fn wrap_eval_jump_expr(
    program: &mut ProgramBuilder,
    insn: Insn,
    target_register: usize,
    if_true_label: BranchOffset,
) {
    program.emit_insn(Insn::Integer {
        value: 1, // emit True by default
        dest: target_register,
    });
    program.emit_insn_with_label_dependency(insn, if_true_label);
    program.emit_insn(Insn::Integer {
        value: 0, // emit False if we reach this point (no jump)
        dest: target_register,
    });
    program.preassign_label_to_next_insn(if_true_label);
}

pub fn resolve_ident_qualified(
    program: &ProgramBuilder,
    table_name: &str,
    ident: &str,
    referenced_tables: &[BTreeTableReference],
    cursor_hint: Option<usize>,
) -> Result<(usize, Type, usize, bool)> {
    let ident = normalize_ident(ident);
    let table_name = normalize_ident(table_name);
    for table_reference in referenced_tables.iter() {
        if table_reference.table_identifier == table_name {
            let res = table_reference
                .table
                .columns
                .iter()
                .enumerate()
                .find(|(_, col)| col.name == *ident)
                .map(|(idx, col)| (idx, col.ty, col.primary_key));
            let mut idx;
            let mut col_type;
            let mut is_primary_key;
            if res.is_some() {
                (idx, col_type, is_primary_key) = res.unwrap();
                // overwrite if cursor hint is provided
                if let Some(cursor_hint) = cursor_hint {
                    let cols = &program.cursor_ref[cursor_hint].1;
                    if let Some(res) = cols.as_ref().and_then(|res| {
                        res.columns()
                            .iter()
                            .enumerate()
                            .find(|x| x.1.name == format!("{}.{}", table_name, ident))
                    }) {
                        idx = res.0;
                        col_type = res.1.ty;
                        is_primary_key = res.1.primary_key;
                    }
                }
                let cursor_id =
                    program.resolve_cursor_id(&table_reference.table_identifier, cursor_hint);
                return Ok((idx, col_type, cursor_id, is_primary_key));
            }
        }
    }
    crate::bail_parse_error!(
        "column with qualified name {}.{} not found",
        table_name,
        ident
    );
}

pub fn resolve_ident_table(
    program: &ProgramBuilder,
    ident: &str,
    referenced_tables: Option<&[BTreeTableReference]>,
    cursor_hint: Option<usize>,
) -> Result<(usize, Type, usize, bool)> {
    let ident = normalize_ident(ident);
    let mut found = Vec::new();
    for table_reference in referenced_tables.unwrap() {
        let res = table_reference
            .table
            .columns
            .iter()
            .enumerate()
            .find(|(_, col)| col.name == *ident)
            .map(|(idx, col)| {
                (
                    idx,
                    col.ty,
                    table_reference.table.column_is_rowid_alias(col),
                )
            });
        let mut idx;
        let mut col_type;
        let mut is_rowid_alias;
        if res.is_some() {
            (idx, col_type, is_rowid_alias) = res.unwrap();
            // overwrite if cursor hint is provided
            if let Some(cursor_hint) = cursor_hint {
                let cols = &program.cursor_ref[cursor_hint].1;
                if let Some(res) = cols.as_ref().and_then(|res| {
                    res.columns()
                        .iter()
                        .enumerate()
                        .find(|x| x.1.name == *ident)
                }) {
                    idx = res.0;
                    col_type = res.1.ty;
                    is_rowid_alias = table_reference.table.column_is_rowid_alias(res.1);
                }
            }
            let cursor_id =
                program.resolve_cursor_id(&table_reference.table_identifier, cursor_hint);
            found.push((idx, col_type, cursor_id, is_rowid_alias));
        }
    }
    if found.len() == 1 {
        return Ok(found[0]);
    }
    if found.is_empty() {
        crate::bail_parse_error!("column with name {} not found", ident.as_str());
    }

    crate::bail_parse_error!("ambiguous column name {}", ident.as_str());
}

pub fn resolve_ident_pseudo_table(ident: &String, pseudo_table: &PseudoTable) -> Result<usize> {
    let res = pseudo_table
        .columns
        .iter()
        .enumerate()
        .find(|(_, col)| col.name == *ident);
    if res.is_some() {
        let (idx, _) = res.unwrap();
        return Ok(idx);
    }
    crate::bail_parse_error!("column with name {} not found", ident.as_str());
}

pub fn maybe_apply_affinity(col_type: Type, target_register: usize, program: &mut ProgramBuilder) {
    if col_type == crate::schema::Type::Real {
        program.emit_insn(Insn::RealAffinity {
            register: target_register,
        })
    }
}

pub fn translate_table_columns(
    program: &mut ProgramBuilder,
    cursor_id: usize,
    table: &Table,
    start_column_offset: usize,
    start_reg: usize,
) -> usize {
    let mut cur_reg = start_reg;
    for i in start_column_offset..table.columns().len() {
        let is_rowid = table.column_is_rowid_alias(table.get_column_at(i));
        let col_type = &table.get_column_at(i).ty;
        if is_rowid {
            program.emit_insn(Insn::RowId {
                cursor_id,
                dest: cur_reg,
            });
        } else {
            program.emit_insn(Insn::Column {
                cursor_id,
                column: i,
                dest: cur_reg,
            });
        }
        maybe_apply_affinity(*col_type, cur_reg, program);
        cur_reg += 1;
    }
    cur_reg
}

pub fn translate_aggregation(
    program: &mut ProgramBuilder,
    referenced_tables: &[BTreeTableReference],
    agg: &Aggregate,
    target_register: usize,
    cursor_hint: Option<usize>,
) -> Result<usize> {
    let dest = match agg.func {
        AggFunc::Avg => {
            if agg.args.len() != 1 {
                crate::bail_parse_error!("avg bad number of arguments");
            }
            let expr = &agg.args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(
                program,
                Some(referenced_tables),
                expr,
                expr_reg,
                cursor_hint,
                None,
            )?;
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
                let _ = translate_expr(
                    program,
                    Some(referenced_tables),
                    expr,
                    expr_reg,
                    cursor_hint,
                    None,
                );
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
                    ast::Expr::Id(ident) => {
                        if ident.0.starts_with('"') {
                            delimiter_expr =
                                ast::Expr::Literal(ast::Literal::String(ident.0.to_string()));
                        } else {
                            delimiter_expr = agg.args[1].clone();
                        }
                    }
                    ast::Expr::Literal(ast::Literal::String(s)) => {
                        delimiter_expr = ast::Expr::Literal(ast::Literal::String(s.to_string()));
                    }
                    _ => crate::bail_parse_error!("Incorrect delimiter parameter"),
                };
            } else {
                delimiter_expr = ast::Expr::Literal(ast::Literal::String(String::from("\",\"")));
            }

            translate_expr(
                program,
                Some(referenced_tables),
                expr,
                expr_reg,
                cursor_hint,
                None,
            )?;
            translate_expr(
                program,
                Some(referenced_tables),
                &delimiter_expr,
                delimiter_reg,
                cursor_hint,
                None,
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
            let _ = translate_expr(
                program,
                Some(referenced_tables),
                expr,
                expr_reg,
                cursor_hint,
                None,
            )?;
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
            let _ = translate_expr(
                program,
                Some(referenced_tables),
                expr,
                expr_reg,
                cursor_hint,
                None,
            )?;
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
            let delimiter_expr: ast::Expr;

            match &agg.args[1] {
                ast::Expr::Id(ident) => {
                    if ident.0.starts_with('"') {
                        crate::bail_parse_error!("no such column: \",\" - should this be a string literal in single-quotes?");
                    } else {
                        delimiter_expr = agg.args[1].clone();
                    }
                }
                ast::Expr::Literal(ast::Literal::String(s)) => {
                    delimiter_expr = ast::Expr::Literal(ast::Literal::String(s.to_string()));
                }
                _ => crate::bail_parse_error!("Incorrect delimiter parameter"),
            };

            translate_expr(
                program,
                Some(referenced_tables),
                expr,
                expr_reg,
                cursor_hint,
                None,
            )?;
            translate_expr(
                program,
                Some(referenced_tables),
                &delimiter_expr,
                delimiter_reg,
                cursor_hint,
                None,
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
            let _ = translate_expr(
                program,
                Some(referenced_tables),
                expr,
                expr_reg,
                cursor_hint,
                None,
            )?;
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
            let _ = translate_expr(
                program,
                Some(referenced_tables),
                expr,
                expr_reg,
                cursor_hint,
                None,
            )?;
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
