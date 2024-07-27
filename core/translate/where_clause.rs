use crate::{
    error::LimboError,
    function::ScalarFunc,
    translate::{
        expr::{resolve_ident_qualified, resolve_ident_table, translate_expr},
        select::Select,
    },
    vdbe::{builder::ProgramBuilder, BranchOffset, Insn},
    Result,
};

use super::select::LoopInfo;

use sqlite3_parser::ast::{self};

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
    processed_where_clause: &mut ProcessedWhereClause,
    where_clause_or_join_constraint: &ast::Expr,
    outer_join_table_name: Option<&'a String>,
) -> Result<()> {
    let mut queue = vec![where_clause_or_join_constraint];

    while let Some(expr) = queue.pop() {
        match expr {
            ast::Expr::Binary(left, ast::Operator::And, right) => {
                queue.push(left);
                queue.push(right);
            }
            expr => {
                if expr.is_always_true()? {
                    continue;
                }
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
                                .ok_or(LimboError::ParseError(format!(
                                    "Could not find cursor for table {}",
                                    table
                                )))?
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
                                    LimboError::ParseError(format!(
                                        "No open cursors found in any of the loops"
                                    ))
                                })?;

                            *cursors.iter().max().unwrap_or(&outermost_cursor)
                        }
                    },
                };
                processed_where_clause.terms.push(term);
            }
        }
    }

    Ok(())
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
        split_constraint_to_terms(program, select, &mut wc, w, None)?;
    }

    for table in select.src_tables.iter() {
        if table.join_info.is_none() {
            continue;
        }
        let join_info = table.join_info.unwrap();
        if let Some(ast::JoinConstraint::On(expr)) = &join_info.constraint {
            split_constraint_to_terms(
                program,
                select,
                &mut wc,
                expr,
                if table.is_outer_join() {
                    Some(&table.identifier)
                } else {
                    None
                },
            )?;
        }
    }

    Ok(wc)
}

/**
 * Translate the WHERE clause of a SELECT statement that doesn't have any tables.
 * TODO: refactor this to use the same code path as the other WHERE clause translation functions.
 */
pub fn translate_tableless_where(
    select: &Select,
    program: &mut ProgramBuilder,
    early_terminate_label: BranchOffset,
) -> Result<Option<BranchOffset>> {
    if let Some(w) = &select.where_clause {
        if w.is_always_false()? {
            program.emit_insn_with_label_dependency(
                Insn::Goto {
                    target_pc: early_terminate_label,
                },
                early_terminate_label,
            );
            return Ok(None);
        }
        if w.is_always_true()? {
            return Ok(None);
        }

        let jump_target_when_false = program.allocate_label();
        let jump_target_when_true = program.allocate_label();
        translate_condition_expr(
            program,
            select,
            w,
            None,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_false,
                jump_target_when_true,
            },
        )?;

        program.resolve_label(jump_target_when_true, program.offset());

        Ok(Some(jump_target_when_false))
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
    skip_entire_table_label: BranchOffset,
    cursor_hint: Option<usize>,
) -> Result<()> {
    if where_c
        .terms
        .iter()
        .filter(|t| t.evaluate_at_cursor == current_loop.open_cursor)
        .any(|t| t.expr.is_always_false().unwrap_or(false))
    {
        program.emit_insn_with_label_dependency(
            Insn::Goto {
                target_pc: skip_entire_table_label,
            },
            skip_entire_table_label,
        );
        return Ok(());
    }
    for term in where_c
        .terms
        .iter()
        .filter(|t| t.evaluate_at_cursor == current_loop.open_cursor)
    {
        let jump_target_when_false = current_loop.next_row_label;
        let jump_target_when_true = program.allocate_label();
        translate_condition_expr(
            program,
            select,
            &term.expr,
            cursor_hint,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_false,
                jump_target_when_true,
            },
        )?;

        program.resolve_label(jump_target_when_true, program.offset());
    }

    Ok(())
}

#[derive(Default, Debug, Clone, Copy)]
struct ConditionMetadata {
    jump_if_condition_is_true: bool,
    jump_target_when_true: BranchOffset,
    jump_target_when_false: BranchOffset,
}

fn translate_condition_expr(
    program: &mut ProgramBuilder,
    select: &Select,
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
                select,
                lhs,
                cursor_hint,
                ConditionMetadata {
                    jump_if_condition_is_true: false,
                    ..condition_metadata
                },
            );
            let _ = translate_condition_expr(program, select, rhs, cursor_hint, condition_metadata);
        }
        ast::Expr::Binary(lhs, ast::Operator::Or, rhs) => {
            let jump_target_when_false = program.allocate_label();
            let _ = translate_condition_expr(
                program,
                select,
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
            let _ = translate_condition_expr(program, select, rhs, cursor_hint, condition_metadata);
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
            let _ = translate_expr(program, select, lhs, lhs_reg, cursor_hint)?;

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
                    let _ = translate_expr(program, select, expr, rhs_reg, cursor_hint)?;
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
                    let _ = translate_expr(program, select, expr, rhs_reg, cursor_hint)?;
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
                        func: ScalarFunc::Like,
                        start_reg: pattern_reg,
                        dest: cur_reg,
                    });
                }
                ast::LikeOperator::Glob => todo!(),
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
            } else {
                if condition_metadata.jump_if_condition_is_true {
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
        ast::Expr::InList { lhs, rhs, .. } => {
            cursors.extend(introspect_expression_for_cursors(
                program,
                select,
                lhs,
                cursor_hint,
            )?);
            if let Some(rhs_list) = rhs {
                for rhs_expr in rhs_list {
                    cursors.extend(introspect_expression_for_cursors(
                        program,
                        select,
                        rhs_expr,
                        cursor_hint,
                    )?);
                }
            }
        }
        _ => {}
    }

    Ok(cursors)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstantCondition {
    AlwaysTrue,
    AlwaysFalse,
}

pub trait Evaluatable {
    fn check_constant(&self) -> Result<Option<ConstantCondition>>;
    fn is_always_true(&self) -> Result<bool> {
        Ok(self
            .check_constant()?
            .map_or(false, |c| c == ConstantCondition::AlwaysTrue))
    }
    fn is_always_false(&self) -> Result<bool> {
        Ok(self
            .check_constant()?
            .map_or(false, |c| c == ConstantCondition::AlwaysFalse))
    }
}

impl Evaluatable for ast::Expr {
    fn check_constant(&self) -> Result<Option<ConstantCondition>> {
        match self {
            ast::Expr::Literal(lit) => match lit {
                ast::Literal::Null => Ok(Some(ConstantCondition::AlwaysFalse)),
                ast::Literal::Numeric(b) => {
                    if let Ok(int_value) = b.parse::<i64>() {
                        return Ok(Some(if int_value == 0 {
                            ConstantCondition::AlwaysFalse
                        } else {
                            ConstantCondition::AlwaysTrue
                        }));
                    }
                    if let Ok(float_value) = b.parse::<f64>() {
                        return Ok(Some(if float_value == 0.0 {
                            ConstantCondition::AlwaysFalse
                        } else {
                            ConstantCondition::AlwaysTrue
                        }));
                    }

                    Ok(None)
                }
                ast::Literal::String(s) => {
                    let without_quotes = s.trim_matches('\'');
                    if let Ok(int_value) = without_quotes.parse::<i64>() {
                        return Ok(Some(if int_value == 0 {
                            ConstantCondition::AlwaysFalse
                        } else {
                            ConstantCondition::AlwaysTrue
                        }));
                    }

                    if let Ok(float_value) = without_quotes.parse::<f64>() {
                        return Ok(Some(if float_value == 0.0 {
                            ConstantCondition::AlwaysFalse
                        } else {
                            ConstantCondition::AlwaysTrue
                        }));
                    }

                    Ok(Some(ConstantCondition::AlwaysFalse))
                }
                _ => Ok(None),
            },
            ast::Expr::Unary(op, expr) => {
                if *op == ast::UnaryOperator::Not {
                    let trivial = expr.check_constant()?;
                    return Ok(trivial.map(|t| match t {
                        ConstantCondition::AlwaysTrue => ConstantCondition::AlwaysFalse,
                        ConstantCondition::AlwaysFalse => ConstantCondition::AlwaysTrue,
                    }));
                }

                if *op == ast::UnaryOperator::Negative {
                    let trivial = expr.check_constant()?;
                    return Ok(trivial);
                }

                Ok(None)
            }
            ast::Expr::InList { lhs: _, not, rhs } => {
                if rhs.is_none() {
                    return Ok(Some(if *not {
                        ConstantCondition::AlwaysTrue
                    } else {
                        ConstantCondition::AlwaysFalse
                    }));
                }
                let rhs = rhs.as_ref().unwrap();
                if rhs.is_empty() {
                    return Ok(Some(if *not {
                        ConstantCondition::AlwaysTrue
                    } else {
                        ConstantCondition::AlwaysFalse
                    }));
                }

                Ok(None)
            }
            ast::Expr::Binary(lhs, op, rhs) => {
                let lhs_trivial = lhs.check_constant()?;
                let rhs_trivial = rhs.check_constant()?;
                match op {
                    ast::Operator::And => {
                        if lhs_trivial == Some(ConstantCondition::AlwaysFalse)
                            || rhs_trivial == Some(ConstantCondition::AlwaysFalse)
                        {
                            return Ok(Some(ConstantCondition::AlwaysFalse));
                        }
                        if lhs_trivial == Some(ConstantCondition::AlwaysTrue)
                            && rhs_trivial == Some(ConstantCondition::AlwaysTrue)
                        {
                            return Ok(Some(ConstantCondition::AlwaysTrue));
                        }

                        Ok(None)
                    }
                    ast::Operator::Or => {
                        if lhs_trivial == Some(ConstantCondition::AlwaysTrue)
                            || rhs_trivial == Some(ConstantCondition::AlwaysTrue)
                        {
                            return Ok(Some(ConstantCondition::AlwaysTrue));
                        }
                        if lhs_trivial == Some(ConstantCondition::AlwaysFalse)
                            && rhs_trivial == Some(ConstantCondition::AlwaysFalse)
                        {
                            return Ok(Some(ConstantCondition::AlwaysFalse));
                        }

                        Ok(None)
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}
