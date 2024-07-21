use anyhow::Result;
use sqlite3_parser::ast::{self, JoinOperator};

use crate::{
    translate::expr::{resolve_ident_qualified, resolve_ident_table, translate_expr},
    function::SingleRowFunc,
    select::Select,
    vdbe::{BranchOffset, Insn, ProgramBuilder},
};

const HARDCODED_CURSOR_LEFT_TABLE: usize = 0;
const HARDCODED_CURSOR_RIGHT_TABLE: usize = 1;

#[derive(Debug)]
pub struct Where {
    pub constraint_expr: ast::Expr,
    pub no_match_jump_label: BranchOffset,
    pub no_match_target_cursor: usize,
}

#[derive(Debug)]
pub struct Join {
    pub constraint_expr: ast::Expr,
    pub no_match_jump_label: BranchOffset,
    pub no_match_target_cursor: usize,
}

#[derive(Debug)]
pub struct Left {
    pub where_clause: Option<Where>,
    pub join_clause: Option<Join>,
    pub match_flag: usize,
    pub match_flag_hit_marker: BranchOffset,
    pub found_match_next_row_label: BranchOffset,
    pub left_cursor: usize,
    pub right_cursor: usize,
}

#[derive(Debug)]
pub struct Inner {
    pub where_clause: Option<Where>,
    pub join_clause: Option<Join>,
}

pub enum QueryConstraint {
    Left(Left),
    Inner(Inner),
}

pub fn translate_where(
    select: &Select,
    program: &mut ProgramBuilder,
) -> Result<Option<BranchOffset>> {
    if let Some(w) = &select.where_clause {
        let label = program.allocate_label();
        translate_condition_expr(program, select, w, label, false, None)?;
        Ok(Some(label))
    } else {
        Ok(None)
    }
}

pub fn evaluate_conditions(
    program: &mut ProgramBuilder,
    select: &Select,
    cursor_hint: Option<usize>,
) -> Result<Option<QueryConstraint>> {
    let join_constraints = select
        .src_tables
        .iter()
        .map(|v| v.join_info)
        .filter_map(|v| v.map(|v| (v.constraint.clone(), v.operator)))
        .collect::<Vec<_>>();
    // TODO: only supports one JOIN; -> add support for multiple JOINs, e.g. SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id
    if join_constraints.len() > 1 {
        anyhow::bail!("Parse error: multiple JOINs not supported");
    }

    let join_maybe = join_constraints.first();

    let parsed_where_maybe = select.where_clause.as_ref().map(|where_clause| Where {
        constraint_expr: where_clause.clone(),
        no_match_jump_label: program.allocate_label(),
        no_match_target_cursor: get_no_match_target_cursor(
            program,
            select,
            where_clause,
            cursor_hint,
        ),
    });

    let parsed_join_maybe = join_maybe.and_then(|(constraint, _)| {
        if let Some(ast::JoinConstraint::On(expr)) = constraint {
            Some(Join {
                constraint_expr: expr.clone(),
                no_match_jump_label: program.allocate_label(),
                no_match_target_cursor: get_no_match_target_cursor(
                    program,
                    select,
                    expr,
                    cursor_hint,
                ),
            })
        } else {
            None
        }
    });

    let constraint_maybe = match (parsed_where_maybe, parsed_join_maybe) {
        (None, None) => None,
        (Some(where_clause), None) => Some(QueryConstraint::Inner(Inner {
            where_clause: Some(where_clause),
            join_clause: None,
        })),
        (where_clause, Some(join_clause)) => {
            let (_, op) = join_maybe.unwrap();
            match op {
                JoinOperator::TypedJoin { natural, join_type } => {
                    if *natural {
                        todo!("Natural join not supported");
                    }
                    // default to inner join when no join type is specified
                    let join_type = join_type.unwrap_or(ast::JoinType::Inner);
                    match join_type {
                        ast::JoinType::Inner | ast::JoinType::Cross => {
                            // cross join with a condition is an inner join
                            Some(QueryConstraint::Inner(Inner {
                                where_clause,
                                join_clause: Some(join_clause),
                            }))
                        }
                        ast::JoinType::LeftOuter | ast::JoinType::Left => {
                            let left_join_match_flag = program.alloc_register();
                            let left_join_match_flag_hit_marker = program.allocate_label();
                            let left_join_found_match_next_row_label = program.allocate_label();

                            Some(QueryConstraint::Left(Left {
                                where_clause,
                                join_clause: Some(join_clause),
                                found_match_next_row_label: left_join_found_match_next_row_label,
                                match_flag: left_join_match_flag,
                                match_flag_hit_marker: left_join_match_flag_hit_marker,
                                left_cursor: HARDCODED_CURSOR_LEFT_TABLE, // FIXME: hardcoded
                                right_cursor: HARDCODED_CURSOR_RIGHT_TABLE, // FIXME: hardcoded
                            }))
                        }
                        ast::JoinType::RightOuter | ast::JoinType::Right => {
                            todo!();
                        }
                        ast::JoinType::FullOuter | ast::JoinType::Full => {
                            todo!();
                        }
                    }
                }
                JoinOperator::Comma => {
                    todo!();
                }
            }
        }
    };

    Ok(constraint_maybe)
}

pub fn translate_conditions(
    program: &mut ProgramBuilder,
    select: &Select,
    conditions: Option<QueryConstraint>,
    cursor_hint: Option<usize>,
) -> Result<Option<QueryConstraint>> {
    match conditions.as_ref() {
        Some(QueryConstraint::Left(Left {
            where_clause,
            join_clause,
            match_flag,
            match_flag_hit_marker,
            ..
        })) => {
            if let Some(where_clause) = where_clause {
                translate_condition_expr(
                    program,
                    select,
                    &where_clause.constraint_expr,
                    where_clause.no_match_jump_label,
                    false,
                    cursor_hint,
                )?;
            }
            if let Some(join_clause) = join_clause {
                translate_condition_expr(
                    program,
                    select,
                    &join_clause.constraint_expr,
                    join_clause.no_match_jump_label,
                    false,
                    cursor_hint,
                )?;
            }
            // Set match flag to 1 if we hit the marker (i.e. jump didn't happen to no_match_label as a result of the condition)
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: *match_flag,
            });
            program.defer_label_resolution(*match_flag_hit_marker, (program.offset() - 1) as usize);
        }
        Some(QueryConstraint::Inner(inner_join)) => {
            if let Some(where_clause) = &inner_join.where_clause {
                translate_condition_expr(
                    program,
                    select,
                    &where_clause.constraint_expr,
                    where_clause.no_match_jump_label,
                    false,
                    cursor_hint,
                )?;
            }
            if let Some(join_clause) = &inner_join.join_clause {
                translate_condition_expr(
                    program,
                    select,
                    &join_clause.constraint_expr,
                    join_clause.no_match_jump_label,
                    false,
                    cursor_hint,
                )?;
            }
        }
        None => {}
    }

    Ok(conditions)
}

fn translate_condition_expr(
    program: &mut ProgramBuilder,
    select: &Select,
    expr: &ast::Expr,
    target_jump: BranchOffset,
    jump_if_true: bool, // if true jump to target on op == true, if false invert op
    cursor_hint: Option<usize>,
) -> Result<()> {
    match expr {
        ast::Expr::Between { .. } => todo!(),
        ast::Expr::Binary(lhs, ast::Operator::And, rhs) => {
            if jump_if_true {
                let label = program.allocate_label();
                let _ = translate_condition_expr(program, select, lhs, label, false, cursor_hint);
                let _ =
                    translate_condition_expr(program, select, rhs, target_jump, true, cursor_hint);
                program.resolve_label(label, program.offset());
            } else {
                let _ =
                    translate_condition_expr(program, select, lhs, target_jump, false, cursor_hint);
                let _ =
                    translate_condition_expr(program, select, rhs, target_jump, false, cursor_hint);
            }
        }
        ast::Expr::Binary(lhs, ast::Operator::Or, rhs) => {
            if jump_if_true {
                let _ =
                    translate_condition_expr(program, select, lhs, target_jump, true, cursor_hint);
                let _ =
                    translate_condition_expr(program, select, rhs, target_jump, true, cursor_hint);
            } else {
                let label = program.allocate_label();
                let _ = translate_condition_expr(program, select, lhs, label, true, cursor_hint);
                let _ =
                    translate_condition_expr(program, select, rhs, target_jump, false, cursor_hint);
                program.resolve_label(label, program.offset());
            }
        }
        ast::Expr::Binary(lhs, op, rhs) => {
            let lhs_reg = program.alloc_register();
            let rhs_reg = program.alloc_register();
            let _ = translate_expr(program, select, lhs, lhs_reg, cursor_hint);
            match lhs.as_ref() {
                ast::Expr::Literal(_) => program.mark_last_insn_constant(),
                _ => {}
            }
            let _ = translate_expr(program, select, rhs, rhs_reg, cursor_hint);
            match rhs.as_ref() {
                ast::Expr::Literal(_) => program.mark_last_insn_constant(),
                _ => {}
            }
            match op {
                ast::Operator::Greater => {
                    if jump_if_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Gt {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::Le {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    }
                }
                ast::Operator::GreaterEquals => {
                    if jump_if_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Ge {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::Lt {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    }
                }
                ast::Operator::Less => {
                    if jump_if_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Lt {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::Ge {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    }
                }
                ast::Operator::LessEquals => {
                    if jump_if_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Le {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::Gt {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    }
                }
                ast::Operator::Equals => {
                    if jump_if_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Eq {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::Ne {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    }
                }
                ast::Operator::NotEquals => {
                    if jump_if_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Ne {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::Eq {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
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
                    if target_jump < 0 {
                        program.add_label_dependency(target_jump, program.offset());
                    }
                    program.emit_insn(Insn::IfNot {
                        reg,
                        target_pc: target_jump,
                        null_reg: reg,
                    });
                } else {
                    anyhow::bail!("Parse error: unsupported literal type in condition");
                }
            }
            _ => todo!(),
        },
        ast::Expr::InList {
            lhs: _,
            not: _,
            rhs: _,
        } => {}
        ast::Expr::Like {
            lhs,
            not,
            op,
            rhs,
            escape: _,
        } => {
            let cur_reg = program.alloc_register();
            assert!(match rhs.as_ref() {
                ast::Expr::Literal(_) => true,
                _ => false,
            });
            match op {
                ast::LikeOperator::Like => {
                    let pattern_reg = program.alloc_register();
                    let column_reg = program.alloc_register();
                    // LIKE(pattern, column). We should translate the pattern first before the column
                    let _ = translate_expr(program, select, rhs, pattern_reg, cursor_hint)?;
                    program.mark_last_insn_constant();
                    let _ = translate_expr(program, select, lhs, column_reg, cursor_hint)?;
                    program.emit_insn(Insn::Function {
                        func: SingleRowFunc::Like,
                        start_reg: pattern_reg,
                        dest: cur_reg,
                    });
                }
                ast::LikeOperator::Glob => todo!(),
                ast::LikeOperator::Match => todo!(),
                ast::LikeOperator::Regexp => todo!(),
            }
            if jump_if_true ^ *not {
                program.emit_insn_with_label_dependency(
                    Insn::If {
                        reg: cur_reg,
                        target_pc: target_jump,
                        null_reg: cur_reg,
                    },
                    target_jump,
                )
            } else {
                program.emit_insn_with_label_dependency(
                    Insn::IfNot {
                        reg: cur_reg,
                        target_pc: target_jump,
                        null_reg: cur_reg,
                    },
                    target_jump,
                )
            }
        }
        _ => todo!("op {:?} not implemented", expr),
    }
    Ok(())
}

fn introspect_expression_for_cursors(
    program: &ProgramBuilder,
    select: &Select,
    where_expr: &ast::Expr,
    cursor_hint: Option<usize>,
) -> Result<Vec<usize>> {
    let mut cursors = vec![];
    match where_expr {
        ast::Expr::Binary(e1, _, e2) => {
            cursors.extend(introspect_expression_for_cursors(
                program,
                select,
                e1,
                cursor_hint,
            )?);
            cursors.extend(introspect_expression_for_cursors(
                program,
                select,
                e2,
                cursor_hint,
            )?);
        }
        ast::Expr::Id(ident) => {
            let (_, _, cursor_id, _) = resolve_ident_table(program, &ident.0, select, cursor_hint)?;
            cursors.push(cursor_id);
        }
        ast::Expr::Qualified(tbl, ident) => {
            let (_, _, cursor_id, _) =
                resolve_ident_qualified(program, &tbl.0, &ident.0, select, cursor_hint)?;
            cursors.push(cursor_id);
        }
        ast::Expr::Literal(_) => {}
        ast::Expr::Like {
            lhs,
            not: _,
            op: _,
            rhs,
            escape: _,
        } => {
            cursors.extend(introspect_expression_for_cursors(
                program,
                select,
                lhs,
                cursor_hint,
            )?);
            cursors.extend(introspect_expression_for_cursors(
                program,
                select,
                rhs,
                cursor_hint,
            )?);
        }
        other => {
            anyhow::bail!("Parse error: unsupported expression: {:?}", other);
        }
    }

    Ok(cursors)
}

fn get_no_match_target_cursor(
    program: &ProgramBuilder,
    select: &Select,
    expr: &ast::Expr,
    cursor_hint: Option<usize>,
) -> usize {
    // This is the hackiest part of the code. We are finding the cursor that should be advanced to the next row
    // when the condition is not met. This is done by introspecting the expression and finding the innermost cursor that is
    // used in the expression. This is a very naive approach and will not work in all cases.
    // Thankfully though it might be possible to just refine the logic contained here to make it work in all cases. Maybe.
    let cursors =
        introspect_expression_for_cursors(program, select, expr, cursor_hint).unwrap_or_default();
    if cursors.is_empty() {
        HARDCODED_CURSOR_LEFT_TABLE
    } else {
        *cursors.iter().max().unwrap()
    }
}
