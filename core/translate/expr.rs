use sqlite3_parser::ast::{self, UnaryOperator};

#[cfg(feature = "uuid")]
use crate::ext::{ExtFunc, UuidFunc};
#[cfg(feature = "json")]
use crate::function::JsonFunc;
use crate::function::{Func, FuncCtx, MathFuncArity, ScalarFunc};
use crate::schema::Type;
use crate::util::normalize_ident;
use crate::vdbe::{builder::ProgramBuilder, insn::Insn, BranchOffset};
use crate::Result;

use super::emitter::Resolver;
use super::plan::{TableReference, TableReferenceType};

#[derive(Debug, Clone, Copy)]
pub struct ConditionMetadata {
    pub jump_if_condition_is_true: bool,
    pub jump_target_when_true: BranchOffset,
    pub jump_target_when_false: BranchOffset,
    pub parent_op: Option<ast::Operator>,
}

fn emit_cond_jump(program: &mut ProgramBuilder, cond_meta: ConditionMetadata, reg: usize) {
    if cond_meta.jump_if_condition_is_true {
        program.emit_insn(Insn::If {
            reg,
            target_pc: cond_meta.jump_target_when_true,
            null_reg: reg,
        });
    } else {
        program.emit_insn(Insn::IfNot {
            reg,
            target_pc: cond_meta.jump_target_when_false,
            null_reg: reg,
        });
    }
}
macro_rules! emit_cmp_insn {
    (
        $program:expr,
        $cond:expr,
        $op_true:ident,
        $op_false:ident,
        $lhs:expr,
        $rhs:expr
    ) => {{
        if $cond.jump_if_condition_is_true {
            $program.emit_insn(Insn::$op_true {
                lhs: $lhs,
                rhs: $rhs,
                target_pc: $cond.jump_target_when_true,
            });
        } else {
            $program.emit_insn(Insn::$op_false {
                lhs: $lhs,
                rhs: $rhs,
                target_pc: $cond.jump_target_when_false,
            });
        }
    }};
}

macro_rules! expect_arguments_exact {
    (
        $args:expr,
        $expected_arguments:expr,
        $func:ident
    ) => {{
        let args = if let Some(args) = $args {
            if args.len() != $expected_arguments {
                crate::bail_parse_error!(
                    "{} function called with not exactly {} arguments",
                    $func.to_string(),
                    $expected_arguments,
                );
            }
            args
        } else {
            crate::bail_parse_error!("{} function with no arguments", $func.to_string());
        };

        args
    }};
}

macro_rules! expect_arguments_max {
    (
        $args:expr,
        $expected_arguments:expr,
        $func:ident
    ) => {{
        let args = if let Some(args) = $args {
            if args.len() > $expected_arguments {
                crate::bail_parse_error!(
                    "{} function called with more than {} arguments",
                    $func.to_string(),
                    $expected_arguments,
                );
            }
            args
        } else {
            crate::bail_parse_error!("{} function with no arguments", $func.to_string());
        };

        args
    }};
}

macro_rules! expect_arguments_min {
    (
        $args:expr,
        $expected_arguments:expr,
        $func:ident
    ) => {{
        let args = if let Some(args) = $args {
            if args.len() < $expected_arguments {
                crate::bail_parse_error!(
                    "{} function with less than {} arguments",
                    $func.to_string(),
                    $expected_arguments
                );
            }
            args
        } else {
            crate::bail_parse_error!("{} function with no arguments", $func.to_string());
        };
        args
    }};
}

pub fn translate_condition_expr(
    program: &mut ProgramBuilder,
    referenced_tables: &[TableReference],
    expr: &ast::Expr,
    condition_metadata: ConditionMetadata,
    resolver: &Resolver,
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
                ConditionMetadata {
                    jump_if_condition_is_true: false,
                    // Mark that the parent op for sub-expressions is AND
                    parent_op: Some(ast::Operator::And),
                    ..condition_metadata
                },
                resolver,
            );
            let _ = translate_condition_expr(
                program,
                referenced_tables,
                rhs,
                ConditionMetadata {
                    parent_op: Some(ast::Operator::And),
                    ..condition_metadata
                },
                resolver,
            );
        }
        ast::Expr::Binary(lhs, ast::Operator::Or, rhs) => {
            if matches!(condition_metadata.parent_op, Some(ast::Operator::And)) {
                // we are inside a bigger AND expression, so we do NOT jump to parent's 'true' if LHS or RHS is true.
                // we only short-circuit the parent's false label if LHS and RHS are both false.
                let local_true_label = program.allocate_label();
                let local_false_label = program.allocate_label();

                // evaluate LHS in normal OR fashion, short-circuit local if true
                let lhs_metadata = ConditionMetadata {
                    jump_if_condition_is_true: true,
                    jump_target_when_true: local_true_label,
                    jump_target_when_false: local_false_label,
                    parent_op: Some(ast::Operator::Or),
                };
                translate_condition_expr(program, referenced_tables, lhs, lhs_metadata, resolver)?;

                // if lhs was false, we land here:
                program.resolve_label(local_false_label, program.offset());

                // evaluate rhs with normal OR: short-circuit if true, go to local_true
                let rhs_metadata = ConditionMetadata {
                    jump_if_condition_is_true: true,
                    jump_target_when_true: local_true_label,
                    jump_target_when_false: condition_metadata.jump_target_when_false,
                    // if rhs is also false => parent's false
                    parent_op: Some(ast::Operator::Or),
                };
                translate_condition_expr(program, referenced_tables, rhs, rhs_metadata, resolver)?;

                // if we get here, both lhs+rhs are false: explicit jump to parent's false
                program.emit_insn(Insn::Goto {
                    target_pc: condition_metadata.jump_target_when_false,
                });
                // local_true: we do not jump to parent's "true" label because the parent is AND,
                // so we want to keep evaluating the rest
                program.resolve_label(local_true_label, program.offset());
            } else {
                let jump_target_when_false = program.allocate_label();

                let lhs_metadata = ConditionMetadata {
                    jump_if_condition_is_true: true,
                    jump_target_when_false,
                    parent_op: Some(ast::Operator::Or),
                    ..condition_metadata
                };

                translate_condition_expr(program, referenced_tables, lhs, lhs_metadata, resolver)?;

                // if LHS was false, we land here:
                program.resolve_label(jump_target_when_false, program.offset());
                let rhs_metadata = ConditionMetadata {
                    parent_op: Some(ast::Operator::Or),
                    ..condition_metadata
                };
                translate_condition_expr(program, referenced_tables, rhs, rhs_metadata, resolver)?;
            }
        }
        ast::Expr::Binary(lhs, op, rhs) => {
            let lhs_reg = translate_and_mark(program, Some(referenced_tables), lhs, resolver)?;
            let rhs_reg = translate_and_mark(program, Some(referenced_tables), rhs, resolver)?;
            match op {
                ast::Operator::Greater => {
                    emit_cmp_insn!(program, condition_metadata, Gt, Le, lhs_reg, rhs_reg)
                }
                ast::Operator::GreaterEquals => {
                    emit_cmp_insn!(program, condition_metadata, Ge, Lt, lhs_reg, rhs_reg)
                }
                ast::Operator::Less => {
                    emit_cmp_insn!(program, condition_metadata, Lt, Ge, lhs_reg, rhs_reg)
                }
                ast::Operator::LessEquals => {
                    emit_cmp_insn!(program, condition_metadata, Le, Gt, lhs_reg, rhs_reg)
                }
                ast::Operator::Equals => {
                    emit_cmp_insn!(program, condition_metadata, Eq, Ne, lhs_reg, rhs_reg)
                }
                ast::Operator::NotEquals => {
                    emit_cmp_insn!(program, condition_metadata, Ne, Eq, lhs_reg, rhs_reg)
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
                    emit_cond_jump(program, condition_metadata, reg);
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
                emit_cond_jump(program, condition_metadata, reg);
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
                        program.emit_insn(Insn::Goto {
                            target_pc: condition_metadata.jump_target_when_true,
                        });
                    }
                } else {
                    program.emit_insn(Insn::Goto {
                        target_pc: condition_metadata.jump_target_when_false,
                    });
                }
                return Ok(());
            }

            // The left hand side only needs to be evaluated once we have a list of values to compare against.
            let lhs_reg = program.alloc_register();
            let _ = translate_expr(program, Some(referenced_tables), lhs, lhs_reg, resolver)?;

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
                    let _ =
                        translate_expr(program, Some(referenced_tables), expr, rhs_reg, resolver)?;
                    // If this is not the last condition, we need to jump to the 'jump_target_when_true' label if the condition is true.
                    if !last_condition {
                        program.emit_insn(Insn::Eq {
                            lhs: lhs_reg,
                            rhs: rhs_reg,
                            target_pc: jump_target_when_true,
                        });
                    } else {
                        // If this is the last condition, we need to jump to the 'jump_target_when_false' label if there is no match.
                        program.emit_insn(Insn::Ne {
                            lhs: lhs_reg,
                            rhs: rhs_reg,
                            target_pc: condition_metadata.jump_target_when_false,
                        });
                    }
                }
                // If we got here, then the last condition was a match, so we jump to the 'jump_target_when_true' label if 'jump_if_condition_is_true'.
                // If not, we can just fall through without emitting an unnecessary instruction.
                if condition_metadata.jump_if_condition_is_true {
                    program.emit_insn(Insn::Goto {
                        target_pc: condition_metadata.jump_target_when_true,
                    });
                }
            } else {
                // If it's a NOT IN expression, we need to jump to the 'jump_target_when_false' label if any of the conditions are true.
                for expr in rhs.iter() {
                    let rhs_reg = program.alloc_register();
                    let _ =
                        translate_expr(program, Some(referenced_tables), expr, rhs_reg, resolver)?;
                    program.emit_insn(Insn::Eq {
                        lhs: lhs_reg,
                        rhs: rhs_reg,
                        target_pc: condition_metadata.jump_target_when_false,
                    });
                }
                // If we got here, then none of the conditions were a match, so we jump to the 'jump_target_when_true' label if 'jump_if_condition_is_true'.
                // If not, we can just fall through without emitting an unnecessary instruction.
                if condition_metadata.jump_if_condition_is_true {
                    program.emit_insn(Insn::Goto {
                        target_pc: condition_metadata.jump_target_when_true,
                    });
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
                    let mut constant_mask = 0;
                    let _ = translate_and_mark(program, Some(referenced_tables), lhs, resolver);
                    let _ = translate_expr(
                        program,
                        Some(referenced_tables),
                        rhs,
                        pattern_reg,
                        resolver,
                    )?;
                    if matches!(rhs.as_ref(), ast::Expr::Literal(_)) {
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
                emit_cond_jump(program, condition_metadata, cur_reg);
            } else if condition_metadata.jump_if_condition_is_true {
                program.emit_insn(Insn::IfNot {
                    reg: cur_reg,
                    target_pc: condition_metadata.jump_target_when_true,
                    null_reg: cur_reg,
                });
            } else {
                program.emit_insn(Insn::If {
                    reg: cur_reg,
                    target_pc: condition_metadata.jump_target_when_false,
                    null_reg: cur_reg,
                });
            }
        }
        ast::Expr::Parenthesized(exprs) => {
            if exprs.len() == 1 {
                let _ = translate_condition_expr(
                    program,
                    referenced_tables,
                    &exprs[0],
                    condition_metadata,
                    resolver,
                );
            } else {
                crate::bail_parse_error!(
                    "parenthesized condtional should have exactly one expression"
                );
            }
        }
        _ => todo!("op {:?} not implemented", expr),
    }
    Ok(())
}

pub fn translate_expr(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&[TableReference]>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    if let Some(reg) = resolver.resolve_cached_expr_reg(expr) {
        program.emit_insn(Insn::Copy {
            src_reg: reg,
            dst_reg: target_register,
            amount: 0,
        });
        return Ok(target_register);
    }
    match expr {
        ast::Expr::Between { .. } => todo!(),
        ast::Expr::Binary(e1, op, e2) => {
            let e1_reg = program.alloc_registers(2);
            let e2_reg = e1_reg + 1;

            translate_expr(program, referenced_tables, e1, e1_reg, resolver)?;
            translate_expr(program, referenced_tables, e2, e2_reg, resolver)?;

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
                ast::Operator::Divide => {
                    program.emit_insn(Insn::Divide {
                        lhs: e1_reg,
                        rhs: e2_reg,
                        dest: target_register,
                    });
                }
                ast::Operator::Modulus => {
                    program.emit_insn(Insn::Remainder {
                        lhs: e1_reg,
                        rhs: e2_reg,
                        dest: target_register,
                    });
                }
                ast::Operator::BitwiseAnd => {
                    program.emit_insn(Insn::BitAnd {
                        lhs: e1_reg,
                        rhs: e2_reg,
                        dest: target_register,
                    });
                }
                ast::Operator::BitwiseOr => {
                    program.emit_insn(Insn::BitOr {
                        lhs: e1_reg,
                        rhs: e2_reg,
                        dest: target_register,
                    });
                }
                #[cfg(feature = "json")]
                op @ (ast::Operator::ArrowRight | ast::Operator::ArrowRightShift) => {
                    let json_func = match op {
                        ast::Operator::ArrowRight => JsonFunc::JsonArrowExtract,
                        ast::Operator::ArrowRightShift => JsonFunc::JsonArrowShiftExtract,
                        _ => unreachable!(),
                    };

                    program.emit_insn(Insn::Function {
                        constant_mask: 0,
                        start_reg: e1_reg,
                        dest: target_register,
                        func: FuncCtx {
                            func: Func::Json(json_func),
                            arg_count: 2,
                        },
                    })
                }
                other_unimplemented => todo!("{:?}", other_unimplemented),
            }
            Ok(target_register)
        }
        ast::Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            // There's two forms of CASE, one which checks a base expression for equality
            // against the WHEN values, and returns the corresponding THEN value if it matches:
            //   CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END
            // And one which evaluates a series of boolean predicates:
            //   CASE WHEN is_good THEN 'good' WHEN is_bad THEN 'bad' ELSE 'okay' END
            // This just changes which sort of branching instruction to issue, after we
            // generate the expression if needed.
            let return_label = program.allocate_label();
            let mut next_case_label = program.allocate_label();
            // Only allocate a reg to hold the base expression if one was provided.
            // And base_reg then becomes the flag we check to see which sort of
            // case statement we're processing.
            let base_reg = base.as_ref().map(|_| program.alloc_register());
            let expr_reg = program.alloc_register();
            if let Some(base_expr) = base {
                translate_expr(
                    program,
                    referenced_tables,
                    base_expr,
                    base_reg.unwrap(),
                    resolver,
                )?;
            };
            for (when_expr, then_expr) in when_then_pairs {
                translate_expr(program, referenced_tables, when_expr, expr_reg, resolver)?;
                match base_reg {
                    // CASE 1 WHEN 0 THEN 0 ELSE 1 becomes 1==0, Ne branch to next clause
                    Some(base_reg) => program.emit_insn(Insn::Ne {
                        lhs: base_reg,
                        rhs: expr_reg,
                        target_pc: next_case_label,
                    }),
                    // CASE WHEN 0 THEN 0 ELSE 1 becomes ifnot 0 branch to next clause
                    None => program.emit_insn(Insn::IfNot {
                        reg: expr_reg,
                        target_pc: next_case_label,
                        null_reg: 1,
                    }),
                };
                // THEN...
                translate_expr(
                    program,
                    referenced_tables,
                    then_expr,
                    target_register,
                    resolver,
                )?;
                program.emit_insn(Insn::Goto {
                    target_pc: return_label,
                });
                // This becomes either the next WHEN, or in the last WHEN/THEN, we're
                // assured to have at least one instruction corresponding to the ELSE immediately follow.
                program.preassign_label_to_next_insn(next_case_label);
                next_case_label = program.allocate_label();
            }
            match else_expr {
                Some(expr) => {
                    translate_expr(program, referenced_tables, expr, target_register, resolver)?;
                }
                // If ELSE isn't specified, it means ELSE null.
                None => {
                    program.emit_insn(Insn::Null {
                        dest: target_register,
                        dest_end: None,
                    });
                }
            };
            program.resolve_label(return_label, program.offset());
            Ok(target_register)
        }
        ast::Expr::Cast { expr, type_name } => {
            let type_name = type_name.as_ref().unwrap(); // TODO: why is this optional?
            let reg_expr = program.alloc_register();
            translate_expr(program, referenced_tables, expr, reg_expr, resolver)?;
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
            let func_name = normalize_ident(name.0.as_str());
            let func_type = resolver.resolve_function(&func_name, args_count);

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
                Func::External(_) => {
                    let regs = program.alloc_register();
                    program.emit_insn(Insn::Function {
                        constant_mask: 0,
                        start_reg: regs,
                        dest: target_register,
                        func: func_ctx,
                    });
                    Ok(target_register)
                }
                #[cfg(feature = "json")]
                Func::Json(j) => match j {
                    JsonFunc::Json => {
                        let args = expect_arguments_exact!(args, 1, j);

                        translate_function(
                            program,
                            args,
                            referenced_tables,
                            resolver,
                            target_register,
                            func_ctx,
                        )
                    }
                    JsonFunc::JsonArray | JsonFunc::JsonExtract => translate_function(
                        program,
                        args.as_deref().unwrap_or_default(),
                        referenced_tables,
                        resolver,
                        target_register,
                        func_ctx,
                    ),
                    JsonFunc::JsonArrowExtract | JsonFunc::JsonArrowShiftExtract => {
                        unreachable!(
                            "These two functions are only reachable via the -> and ->> operators"
                        )
                    }
                    JsonFunc::JsonArrayLength | JsonFunc::JsonType => {
                        let args = expect_arguments_max!(args, 2, j);

                        translate_function(
                            program,
                            args,
                            referenced_tables,
                            resolver,
                            target_register,
                            func_ctx,
                        )
                    }
                    JsonFunc::JsonErrorPosition => {
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
                        let json_reg = program.alloc_register();
                        translate_expr(program, referenced_tables, &args[0], json_reg, resolver)?;
                        program.emit_insn(Insn::Function {
                            constant_mask: 0,
                            start_reg: json_reg,
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
                        ScalarFunc::Changes => {
                            if args.is_some() {
                                crate::bail_parse_error!(
                                    "{} function with more than 0 arguments",
                                    srf
                                );
                            }
                            let start_reg = program.alloc_register();
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Char => translate_function(
                            program,
                            args.as_deref().unwrap_or_default(),
                            referenced_tables,
                            resolver,
                            target_register,
                            func_ctx,
                        ),
                        ScalarFunc::Coalesce => {
                            let args = expect_arguments_min!(args, 2, srf);

                            // coalesce function is implemented as a series of not null checks
                            // whenever a not null check succeeds, we jump to the end of the series
                            let label_coalesce_end = program.allocate_label();
                            for (index, arg) in args.iter().enumerate() {
                                let reg = translate_expr(
                                    program,
                                    referenced_tables,
                                    arg,
                                    target_register,
                                    resolver,
                                )?;
                                if index < args.len() - 1 {
                                    program.emit_insn(Insn::NotNull {
                                        reg,
                                        target_pc: label_coalesce_end,
                                    });
                                }
                            }
                            program.preassign_label_to_next_insn(label_coalesce_end);

                            Ok(target_register)
                        }
                        ScalarFunc::LastInsertRowid => {
                            let regs = program.alloc_register();
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: regs,
                                dest: target_register,
                                func: func_ctx,
                            });
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
                                translate_expr(program, referenced_tables, arg, reg, resolver)?;
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
                            let args = expect_arguments_min!(args, 2, srf);

                            let temp_register = program.alloc_register();
                            for arg in args.iter() {
                                let reg = program.alloc_register();
                                translate_expr(program, referenced_tables, arg, reg, resolver)?;
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
                                resolver,
                            )?;
                            program.emit_insn(Insn::NotNull {
                                reg: temp_reg,
                                target_pc: program.offset().add(2u32),
                            });

                            translate_expr(
                                program,
                                referenced_tables,
                                &args[1],
                                temp_reg,
                                resolver,
                            )?;
                            program.emit_insn(Insn::Copy {
                                src_reg: temp_reg,
                                dst_reg: target_register,
                                amount: 0,
                            });

                            Ok(target_register)
                        }
                        ScalarFunc::Iif => {
                            let args = match args {
                                Some(args) if args.len() == 3 => args,
                                _ => crate::bail_parse_error!(
                                    "{} requires exactly 3 arguments",
                                    srf.to_string()
                                ),
                            };
                            let temp_reg = program.alloc_register();
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                temp_reg,
                                resolver,
                            )?;
                            let jump_target_when_false = program.allocate_label();
                            program.emit_insn(Insn::IfNot {
                                reg: temp_reg,
                                target_pc: jump_target_when_false,
                                null_reg: 1,
                            });
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[1],
                                target_register,
                                resolver,
                            )?;
                            let jump_target_result = program.allocate_label();
                            program.emit_insn(Insn::Goto {
                                target_pc: jump_target_result,
                            });
                            program.resolve_label(jump_target_when_false, program.offset());
                            translate_expr(
                                program,
                                referenced_tables,
                                &args[2],
                                target_register,
                                resolver,
                            )?;
                            program.resolve_label(jump_target_result, program.offset());
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
                                let _ =
                                    translate_and_mark(program, referenced_tables, arg, resolver);
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
                        | ScalarFunc::OctetLength
                        | ScalarFunc::Typeof
                        | ScalarFunc::Unicode
                        | ScalarFunc::Quote
                        | ScalarFunc::RandomBlob
                        | ScalarFunc::Sign
                        | ScalarFunc::Soundex
                        | ScalarFunc::ZeroBlob => {
                            let args = expect_arguments_exact!(args, 1, srf);
                            let reg =
                                translate_and_mark(program, referenced_tables, &args[0], resolver)?;
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: reg,
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
                        ScalarFunc::Date | ScalarFunc::DateTime => {
                            if let Some(args) = args {
                                for arg in args.iter() {
                                    // register containing result of each argument expression
                                    let _ = translate_and_mark(
                                        program,
                                        referenced_tables,
                                        arg,
                                        resolver,
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
                            let str_reg = translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                str_reg,
                                resolver,
                            )?;
                            let _ = translate_expr(
                                program,
                                referenced_tables,
                                &args[1],
                                start_reg,
                                resolver,
                            )?;
                            if args.len() == 3 {
                                translate_expr(
                                    program,
                                    referenced_tables,
                                    &args[2],
                                    length_reg,
                                    resolver,
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
                            let regs =
                                translate_and_mark(program, referenced_tables, &args[0], resolver)?;
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: regs,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::UnixEpoch | ScalarFunc::JulianDay => {
                            let mut start_reg = 0;
                            match args {
                                Some(args) if args.len() > 1 => {
                                    crate::bail_parse_error!("epoch or julianday function with > 1 arguments. Modifiers are not yet supported.");
                                }
                                Some(args) if args.len() == 1 => {
                                    let arg_reg = program.alloc_register();
                                    let _ = translate_expr(
                                        program,
                                        referenced_tables,
                                        &args[0],
                                        arg_reg,
                                        resolver,
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
                                    let _ = translate_and_mark(
                                        program,
                                        referenced_tables,
                                        arg,
                                        resolver,
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
                        ScalarFunc::TotalChanges => {
                            if args.is_some() {
                                crate::bail_parse_error!(
                                    "{} fucntion with more than 0 arguments",
                                    srf.to_string()
                                );
                            }
                            let start_reg = program.alloc_register();
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg,
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
                            let args = expect_arguments_max!(args, 2, srf);

                            for arg in args.iter() {
                                translate_and_mark(program, referenced_tables, arg, resolver)?;
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
                                translate_and_mark(program, referenced_tables, arg, resolver)?;
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
                                translate_and_mark(program, referenced_tables, arg, resolver)?;
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
                                resolver,
                            )?;
                            let second_reg = program.alloc_register();
                            let _ = translate_expr(
                                program,
                                referenced_tables,
                                &args[1],
                                second_reg,
                                resolver,
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
                                amount: 0,
                            });
                            Ok(target_register)
                        }
                        ScalarFunc::Replace => {
                            let args = if let Some(args) = args {
                                if !args.len() == 3 {
                                    crate::bail_parse_error!(
                                        "function {}() requires exactly 3 arguments",
                                        srf.to_string()
                                    )
                                }
                                args
                            } else {
                                crate::bail_parse_error!(
                                    "function {}() requires exactly 3 arguments",
                                    srf.to_string()
                                );
                            };
                            let str_reg = program.alloc_register();
                            let pattern_reg = program.alloc_register();
                            let replacement_reg = program.alloc_register();
                            let _ = translate_expr(
                                program,
                                referenced_tables,
                                &args[0],
                                str_reg,
                                resolver,
                            )?;
                            let _ = translate_expr(
                                program,
                                referenced_tables,
                                &args[1],
                                pattern_reg,
                                resolver,
                            )?;
                            let _ = translate_expr(
                                program,
                                referenced_tables,
                                &args[2],
                                replacement_reg,
                                resolver,
                            )?;
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: str_reg,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                    }
                }
                Func::Extension(ext_func) => match ext_func {
                    #[cfg(feature = "uuid")]
                    ExtFunc::Uuid(ref uuid_fn) => match uuid_fn {
                        UuidFunc::UuidStr | UuidFunc::UuidBlob | UuidFunc::Uuid7TS => {
                            let args = expect_arguments_exact!(args, 1, ext_func);
                            let regs = program.alloc_register();
                            translate_expr(program, referenced_tables, &args[0], regs, resolver)?;
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: regs,
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                        UuidFunc::Uuid4Str => {
                            if args.is_some() {
                                crate::bail_parse_error!(
                                    "{} function with arguments",
                                    ext_func.to_string()
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
                        UuidFunc::Uuid7 => {
                            let args = expect_arguments_max!(args, 1, ext_func);
                            let mut start_reg = None;
                            if let Some(arg) = args.first() {
                                start_reg = Some(translate_and_mark(
                                    program,
                                    referenced_tables,
                                    arg,
                                    resolver,
                                )?);
                            }
                            program.emit_insn(Insn::Function {
                                constant_mask: 0,
                                start_reg: start_reg.unwrap_or(target_register),
                                dest: target_register,
                                func: func_ctx,
                            });
                            Ok(target_register)
                        }
                    },
                    #[allow(unreachable_patterns)]
                    _ => unreachable!("{ext_func} not implemented yet"),
                },
                Func::Math(math_func) => match math_func.arity() {
                    MathFuncArity::Nullary => {
                        if args.is_some() {
                            crate::bail_parse_error!("{} function with arguments", math_func);
                        }

                        program.emit_insn(Insn::Function {
                            constant_mask: 0,
                            start_reg: 0,
                            dest: target_register,
                            func: func_ctx,
                        });
                        Ok(target_register)
                    }

                    MathFuncArity::Unary => {
                        let args = expect_arguments_exact!(args, 1, math_func);
                        let reg =
                            translate_and_mark(program, referenced_tables, &args[0], resolver)?;
                        program.emit_insn(Insn::Function {
                            constant_mask: 0,
                            start_reg: reg,
                            dest: target_register,
                            func: func_ctx,
                        });
                        Ok(target_register)
                    }

                    MathFuncArity::Binary => {
                        let args = expect_arguments_exact!(args, 2, math_func);
                        let reg1 = program.alloc_register();
                        let _ =
                            translate_expr(program, referenced_tables, &args[0], reg1, resolver)?;
                        let reg2 = program.alloc_register();
                        let _ =
                            translate_expr(program, referenced_tables, &args[1], reg2, resolver)?;
                        program.emit_insn(Insn::Function {
                            constant_mask: 0,
                            start_reg: target_register + 1,
                            dest: target_register,
                            func: func_ctx,
                        });
                        Ok(target_register)
                    }

                    MathFuncArity::UnaryOrBinary => {
                        let args = expect_arguments_max!(args, 2, math_func);

                        let regs = program.alloc_registers(args.len());
                        for (i, arg) in args.iter().enumerate() {
                            translate_expr(program, referenced_tables, arg, regs + i, resolver)?;
                        }

                        program.emit_insn(Insn::Function {
                            constant_mask: 0,
                            start_reg: regs,
                            dest: target_register,
                            func: func_ctx,
                        });
                        Ok(target_register)
                    }
                },
            }
        }
        ast::Expr::FunctionCallStar { .. } => todo!(),
        ast::Expr::Id(_) => unreachable!("Id should be resolved to a Column before translation"),
        ast::Expr::Column {
            database: _,
            table,
            column,
            is_rowid_alias,
        } => {
            let tbl_ref = referenced_tables.as_ref().unwrap().get(*table).unwrap();
            match tbl_ref.reference_type {
                // If we are reading a column from a table, we find the cursor that corresponds to
                // the table and read the column from the cursor.
                TableReferenceType::BTreeTable => {
                    let cursor_id = program.resolve_cursor_id(&tbl_ref.table_identifier);
                    if *is_rowid_alias {
                        program.emit_insn(Insn::RowId {
                            cursor_id,
                            dest: target_register,
                        });
                    } else {
                        program.emit_insn(Insn::Column {
                            cursor_id,
                            column: *column,
                            dest: target_register,
                        });
                    }
                    let column = tbl_ref.table.get_column_at(*column);
                    maybe_apply_affinity(column.ty, target_register, program);
                    Ok(target_register)
                }
                // If we are reading a column from a subquery, we instead copy the column from the
                // subquery's result registers.
                TableReferenceType::Subquery {
                    result_columns_start_reg,
                    ..
                } => {
                    program.emit_insn(Insn::Copy {
                        src_reg: result_columns_start_reg + *column,
                        dst_reg: target_register,
                        amount: 0,
                    });
                    Ok(target_register)
                }
            }
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
                    value: sanitize_string(s),
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
                    resolver,
                )?;
            } else {
                // Parenthesized expressions with multiple arguments are reserved for special cases
                // like `(a, b) IN ((1, 2), (3, 4))`.
                todo!("TODO: parenthesized expression with multiple arguments not yet supported");
            }
            Ok(target_register)
        }
        ast::Expr::Qualified(_, _) => {
            unreachable!("Qualified should be resolved to a Column before translation")
        }
        ast::Expr::Raise(_, _) => todo!(),
        ast::Expr::Subquery(_) => todo!(),
        ast::Expr::Unary(op, expr) => match (op, expr.as_ref()) {
            (
                UnaryOperator::Negative | UnaryOperator::Positive,
                ast::Expr::Literal(ast::Literal::Numeric(numeric_value)),
            ) => {
                let maybe_int = numeric_value.parse::<i64>();
                let multiplier = if let UnaryOperator::Negative = op {
                    -1
                } else {
                    1
                };
                if let Ok(value) = maybe_int {
                    program.emit_insn(Insn::Integer {
                        value: value * multiplier,
                        dest: target_register,
                    });
                } else {
                    program.emit_insn(Insn::Real {
                        value: multiplier as f64 * numeric_value.parse::<f64>()?,
                        dest: target_register,
                    });
                }
                program.mark_last_insn_constant();
                Ok(target_register)
            }
            (UnaryOperator::Negative | UnaryOperator::Positive, _) => {
                let value = if let UnaryOperator::Negative = op {
                    -1
                } else {
                    1
                };

                let reg = program.alloc_register();
                translate_expr(program, referenced_tables, expr, reg, resolver)?;
                let zero_reg = program.alloc_register();
                program.emit_insn(Insn::Integer {
                    value,
                    dest: zero_reg,
                });
                program.mark_last_insn_constant();
                program.emit_insn(Insn::Multiply {
                    lhs: zero_reg,
                    rhs: reg,
                    dest: target_register,
                });
                Ok(target_register)
            }
            (UnaryOperator::BitwiseNot, ast::Expr::Literal(ast::Literal::Numeric(num_val))) => {
                let maybe_int = num_val.parse::<i64>();
                if let Ok(val) = maybe_int {
                    program.emit_insn(Insn::Integer {
                        value: !val,
                        dest: target_register,
                    });
                } else {
                    let num_val = num_val.parse::<f64>()? as i64;
                    program.emit_insn(Insn::Integer {
                        value: !num_val,
                        dest: target_register,
                    });
                }
                program.mark_last_insn_constant();
                Ok(target_register)
            }
            (UnaryOperator::BitwiseNot, ast::Expr::Literal(ast::Literal::Null)) => {
                program.emit_insn(Insn::Null {
                    dest: target_register,
                    dest_end: None,
                });
                program.mark_last_insn_constant();
                Ok(target_register)
            }
            (UnaryOperator::BitwiseNot, _) => {
                let reg = program.alloc_register();
                translate_expr(program, referenced_tables, expr, reg, resolver)?;
                program.emit_insn(Insn::BitNot {
                    reg,
                    dest: target_register,
                });
                Ok(target_register)
            }
            _ => todo!(),
        },
        ast::Expr::Variable(_) => todo!(),
    }
}

/// Emits a whole insn for a function call.
/// Assumes the number of parameters is valid for the given function.
/// Returns the target register for the function.
fn translate_function(
    program: &mut ProgramBuilder,
    args: &[ast::Expr],
    referenced_tables: Option<&[TableReference]>,
    resolver: &Resolver,
    target_register: usize,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let start_reg = program.alloc_registers(args.len());
    let mut current_reg = start_reg;

    for arg in args.iter() {
        translate_expr(program, referenced_tables, arg, current_reg, resolver)?;
        current_reg += 1;
    }

    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg,
        dest: target_register,
        func: func_ctx,
    });

    Ok(target_register)
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
    program.emit_insn(insn);
    program.emit_insn(Insn::Integer {
        value: 0, // emit False if we reach this point (no jump)
        dest: target_register,
    });
    program.preassign_label_to_next_insn(if_true_label);
}

pub fn maybe_apply_affinity(col_type: Type, target_register: usize, program: &mut ProgramBuilder) {
    if col_type == crate::schema::Type::Real {
        program.emit_insn(Insn::RealAffinity {
            register: target_register,
        })
    }
}

pub fn translate_and_mark(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&[TableReference]>,
    expr: &ast::Expr,
    resolver: &Resolver,
) -> Result<usize> {
    let target_register = program.alloc_register();
    translate_expr(program, referenced_tables, expr, target_register, resolver)?;
    if matches!(expr, ast::Expr::Literal(_)) {
        program.mark_last_insn_constant();
    }
    Ok(target_register)
}

/// Get an appropriate name for an expression.
/// If the query provides an alias (e.g. `SELECT a AS b FROM t`), use that (e.g. `b`).
/// If the expression is a column from a table, use the column name (e.g. `a`).
/// Otherwise we just use a generic fallback name (e.g. `expr_<index>`).
pub fn get_name(
    maybe_alias: Option<&ast::As>,
    expr: &ast::Expr,
    referenced_tables: &[TableReference],
    fallback: impl Fn() -> String,
) -> String {
    let alias = maybe_alias.map(|a| match a {
        ast::As::As(id) => id.0.clone(),
        ast::As::Elided(id) => id.0.clone(),
    });
    if let Some(alias) = alias {
        return alias;
    }
    match expr {
        ast::Expr::Column { table, column, .. } => {
            let table_ref = referenced_tables.get(*table).unwrap();
            table_ref.table.get_column_at(*column).name.clone()
        }
        _ => fallback(),
    }
}

/// Sanitaizes a string literal by removing single quote at front and back
/// and escaping double single quotes
pub fn sanitize_string(input: &str) -> String {
    input[1..input.len() - 1].replace("''", "'").to_string()
}
