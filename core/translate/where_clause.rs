use anyhow::Result;
use sqlite3_parser::ast::{self};

use crate::{
    function::SingleRowFunc,
    translate::expr::{resolve_ident_qualified, resolve_ident_table, translate_expr},
    translate::select::Select,
    vdbe::{BranchOffset, Insn, builder::ProgramBuilder},
};

use super::select::LoopInfo;

#[derive(Debug)]
pub struct WhereTerm {
    pub expr: ast::Expr,
    pub evaluate_at_cursor: usize,
}

#[derive(Debug)]
pub struct ProcessedWhereClause {
    pub terms: Vec<WhereTerm>,
}

/**
* Split a constraint into a flat list of WhereTerms.
* The splitting is done at logical 'AND' operator boundaries.
* WhereTerms are currently just a wrapper around an ast::Expr,
* combined with the ID of the cursor where the term should be evaluated.
*/
pub fn split_constraint_to_terms<'a>(
    program: &'a mut ProgramBuilder,
    select: &'a Select,
    where_clause_or_join_constraint: &ast::Expr,
    outer_join_table_name: Option<&'a String>,
) -> Result<Vec<WhereTerm>> {
    let mut terms = Vec::new();
    let mut queue = vec![where_clause_or_join_constraint];

    while let Some(expr) = queue.pop() {
        match expr {
            ast::Expr::Binary(left, ast::Operator::And, right) => {
                queue.push(left);
                queue.push(right);
            }
            expr => {
                let term = WhereTerm {
                    expr: expr.clone(),
                    evaluate_at_cursor: match outer_join_table_name {
                        Some(table) => {
                            // If we had e.g. SELECT * FROM t1 LEFT JOIN t2 WHERE t1.a > 10,
                            // we could evaluate the t1.a > 10 condition at the cursor for t1, i.e. the outer table,
                            // skipping t1 rows that don't match the condition.
                            //
                            // However, if we have SELECT * FROM t1 LEFT JOIN t2 ON t1.a > 10,
                            // we need to evaluate the t1.a > 10 condition at the cursor for t2, i.e. the inner table,
                            // because we need to skip rows from t2 that don't match the condition.
                            //
                            // In inner joins, both of the above are equivalent, but in left joins they are not.
                            select
                                .loops
                                .iter()
                                .find(|t| t.identifier == *table)
                                .ok_or(anyhow::anyhow!(
                                    "Could not find cursor for table {}",
                                    table
                                ))?
                                .open_cursor
                        }
                        None => {
                            // For any non-outer-join condition expression, find the cursor that it should be evaluated at.
                            // This is the cursor that is the rightmost/innermost cursor that the expression depends on.
                            // In SELECT * FROM t1, t2 WHERE t1.a > 10, the condition should be evaluated at the cursor for t1.
                            // In SELECT * FROM t1, t2 WHERE t1.a > 10 OR t2.b > 20, the condition should be evaluated at the cursor for t2.
                            //
                            // We are splitting any AND expressions in this function, so for example in this query:
                            // 'SELECT * FROM t1, t2 WHERE t1.a > 10 AND t2.b > 20'
                            // we can evaluate the t1.a > 10 condition at the cursor for t1, and the t2.b > 20 condition at the cursor for t2.
                            //
                            // For expressions that don't depend on any cursor, we can evaluate them at the leftmost/outermost cursor.
                            // E.g. 'SELECT * FROM t1 JOIN t2 ON false' can be evaluated at the cursor for t1.
                            let cursors =
                                introspect_expression_for_cursors(program, select, expr, None)?;

                            let outermost_cursor = select
                                .loops
                                .iter()
                                .map(|t| t.open_cursor)
                                .min()
                                .ok_or_else(|| {
                                    anyhow::anyhow!("No open cursors found in any of the loops")
                                })?;

                            *cursors.iter().max().unwrap_or(&outermost_cursor)
                        }
                    },
                };
                terms.push(term);
            }
        }
    }

    Ok(terms)
}

/**
* Split the WHERE clause and any JOIN ON clauses into a flat list of WhereTerms
* that can be evaluated at the appropriate cursor.
*/
pub fn process_where<'a>(
    program: &'a mut ProgramBuilder,
    select: &'a Select,
) -> Result<ProcessedWhereClause> {
    let mut wc = ProcessedWhereClause { terms: Vec::new() };
    if let Some(w) = &select.where_clause {
        wc.terms
            .extend(split_constraint_to_terms(program, select, w, None)?);
    }

    for table in select.src_tables.iter() {
        if table.join_info.is_none() {
            continue;
        }
        let join_info = table.join_info.unwrap();
        if let Some(ast::JoinConstraint::On(expr)) = &join_info.constraint {
            let terms = split_constraint_to_terms(
                program,
                select,
                expr,
                if table.is_outer_join() {
                    Some(&table.identifier)
                } else {
                    None
                },
            )?;
            wc.terms.extend(terms);
        }
    }

    Ok(wc)
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

/**
* Translate the WHERE clause and JOIN ON clauses into a series of conditional jump instructions.
* At this point the WHERE clause and JOIN ON clauses have been split into a series of terms that can be evaluated at the appropriate cursor.
* We evaluate each term at the appropriate cursor.
*/
pub fn translate_processed_where<'a>(
    program: &mut ProgramBuilder,
    select: &'a Select,
    current_loop: &'a LoopInfo,
    where_c: &'a ProcessedWhereClause,
    cursor_hint: Option<usize>,
) -> Result<()> {
    for term in where_c.terms.iter() {
        if term.evaluate_at_cursor != current_loop.open_cursor {
            continue;
        }
        let target_jump = current_loop.next_row_label;
        translate_condition_expr(program, select, &term.expr, target_jump, false, cursor_hint)?;
    }

    Ok(())
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
        ast::Expr::Like { lhs, rhs, .. } => {
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
        ast::Expr::FunctionCall { args, .. } => {
            if let Some(args) = args {
                for arg in args {
                    cursors.extend(introspect_expression_for_cursors(
                        program,
                        select,
                        arg,
                        cursor_hint,
                    )?);
                }
            }
        }
        other => {
            anyhow::bail!("Parse error: unsupported expression: {:?}", other);
        }
    }

    Ok(cursors)
}
