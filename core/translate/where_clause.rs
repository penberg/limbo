use crate::{
    function::ScalarFunc,
    translate::{expr::translate_expr, select::Select},
    util::normalize_ident,
    vdbe::{builder::ProgramBuilder, BranchOffset, Insn},
    Result,
};

use super::select::LoopInfo;

use sqlite3_parser::ast::{self};

#[derive(Debug)]
pub struct SeekRowid<'a> {
    pub table: &'a str,
    pub rowid_expr: &'a ast::Expr,
}

#[derive(Debug)]
pub enum WhereExpr<'a> {
    Expr(&'a ast::Expr),
    SeekRowid(SeekRowid<'a>),
}

#[derive(Debug)]
pub struct WhereTerm<'a> {
    pub expr: WhereExpr<'a>,
    pub outer_join_table: Option<usize>,
    pub table_references_bitmask: usize,
}

impl<'a> WhereTerm<'a> {
    pub fn evaluate_at_loop(&self, select: &'a Select) -> usize {
        if let Some(outer_join_table) = self.outer_join_table {
            outer_join_table
        } else {
            self.innermost_table(select)
        }
    }

    pub fn innermost_table(&self, select: &'a Select) -> usize {
        let mut table = 0;
        for i in 0..select.src_tables.len() {
            if self.table_references_bitmask & (1 << i) != 0 {
                table = i;
            }
        }
        table
    }
}

#[derive(Debug)]
pub struct ProcessedWhereClause<'a> {
    pub loop_order: Vec<usize>,
    pub terms: Vec<WhereTerm<'a>>,
}

/**
* Split a constraint into a flat list of WhereTerms.
* The splitting is done at logical 'AND' operator boundaries.
* WhereTerms are currently just a wrapper around an ast::Expr,
* combined with the ID of the cursor where the term should be evaluated.
*/
pub fn split_constraint_to_terms<'a>(
    select: &'a Select,
    mut processed_where_clause: ProcessedWhereClause<'a>,
    where_clause_or_join_constraint: &'a ast::Expr,
    outer_join_table: Option<usize>,
) -> Result<ProcessedWhereClause<'a>> {
    let mut queue = vec![where_clause_or_join_constraint];

    while let Some(expr) = queue.pop() {
        match expr {
            ast::Expr::Binary(left, ast::Operator::And, right) => {
                queue.push(left);
                queue.push(right);
            }
            expr => {
                if expr.is_always_true()? {
                    // Terms that are always true can be skipped, as they don't constrain the result set in any way.
                    continue;
                }
                let term = WhereTerm {
                    expr: {
                        let seekrowid_candidate = select
                            .src_tables
                            .iter()
                            .enumerate()
                            .find_map(|(i, _)| {
                                expr.check_seekrowid_candidate(i, select).unwrap_or(None)
                            })
                            .map(WhereExpr::SeekRowid);

                        seekrowid_candidate.unwrap_or(WhereExpr::Expr(expr))
                    },
                    outer_join_table,
                    table_references_bitmask: introspect_expression_for_table_refs(select, expr)?,
                };
                processed_where_clause.terms.push(term);
            }
        }
    }

    Ok(processed_where_clause)
}

/**
* Split the WHERE clause and any JOIN ON clauses into a flat list of WhereTerms
* that can be evaluated at the appropriate cursor.
*/
pub fn process_where<'a>(select: &'a Select) -> Result<ProcessedWhereClause<'a>> {
    let mut wc = ProcessedWhereClause {
        terms: Vec::new(),
        // In the future, analysis of the WHERE clause and JOIN ON clauses will be used to determine the optimal loop order.
        // For now, we just use the order of the tables in the FROM clause.
        loop_order: select
            .src_tables
            .iter()
            .enumerate()
            .map(|(i, _)| i)
            .collect(),
    };
    if let Some(w) = &select.where_clause {
        wc = split_constraint_to_terms(select, wc, w, None)?;
    }

    for (i, table) in select.src_tables.iter().enumerate() {
        if table.join_info.is_none() {
            continue;
        }
        let join_info = table.join_info.unwrap();
        if let Some(ast::JoinConstraint::On(expr)) = &join_info.constraint {
            wc = split_constraint_to_terms(
                select,
                wc,
                expr,
                if table.is_outer_join() { Some(i) } else { None },
            )?;
        }
    }

    // sort seekrowids first (if e.g. u.id = 1 and u.age > 50, we want to seek on u.id = 1 first)
    wc.terms.sort_by(|a, b| {
        if let WhereExpr::SeekRowid(_) = a.expr {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        }
    });

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
    loops: &[LoopInfo],
    current_loop: &'a LoopInfo,
    where_c: &'a ProcessedWhereClause,
    skip_entire_table_label: BranchOffset,
    cursor_hint: Option<usize>,
) -> Result<()> {
    for t in where_c.terms.iter().filter(|t| {
        select.src_tables[t.evaluate_at_loop(select)].identifier == current_loop.identifier
    }) {
        if let WhereExpr::Expr(e) = &t.expr {
            if e.is_always_false().unwrap_or(false) {
                program.emit_insn_with_label_dependency(
                    Insn::Goto {
                        target_pc: skip_entire_table_label,
                    },
                    skip_entire_table_label,
                );
                return Ok(());
            }
        }
    }

    for term in where_c.terms.iter().filter(|t| {
        select.src_tables[t.evaluate_at_loop(select)].identifier == current_loop.identifier
    }) {
        let jump_target_when_false = loops[term.evaluate_at_loop(select)].next_row_label;
        let jump_target_when_true = program.allocate_label();
        match &term.expr {
            WhereExpr::Expr(e) => {
                translate_condition_expr(
                    program,
                    select,
                    e,
                    cursor_hint,
                    ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_false,
                        jump_target_when_true,
                    },
                )?;
            }
            WhereExpr::SeekRowid(s) => {
                let cursor_id = program.resolve_cursor_id(s.table, cursor_hint);

                let computed_rowid_reg = program.alloc_register();
                let _ = translate_expr(
                    program,
                    Some(select),
                    s.rowid_expr,
                    computed_rowid_reg,
                    cursor_hint,
                )?;

                if !program.has_cursor_emitted_seekrowid(cursor_id) {
                    program.emit_insn_with_label_dependency(
                        Insn::SeekRowid {
                            cursor_id,
                            src_reg: computed_rowid_reg,
                            target_pc: jump_target_when_false,
                        },
                        jump_target_when_false,
                    );
                } else {
                    // If we have already emitted a SeekRowid instruction for this cursor, then other equality checks
                    // against that table should be done using the row that was already fetched.
                    // e.g. select u.age, p.name from users u join products p on u.id = p.id and p.id = 5;
                    // emitting two SeekRowid instructions for the same 'p' cursor would yield an incorrect result.
                    // Assume we are looping over users u, and right now u.id = 3.
                    // We first SeekRowid on p.id = 3, and find a row.
                    // If we then SeekRowid for p.id = 5, we would find a row with p.id = 5,
                    // and end up with a result where u.id = 3 and p.id = 5, which is incorrect.
                    // Instead we replace the second SeekRowid with a comparison against the row that was already fetched,
                    // i.e. we compare p.id == 5, which would not match (and is the correct result).
                    //
                    // It would probably be better to modify the AST in the WhereTerms directly, but that would require
                    // refactoring to not use &'a Ast::Expr references in the WhereTerms, i.e. the WhereClause would own its data
                    // and could mutate it to change the query as needed. We probably need to do this anyway if we want to have some
                    // kind of Query Plan construct that is not just a container for AST nodes.
                    let rowid_reg = program.alloc_register();
                    program.emit_insn(Insn::RowId {
                        cursor_id,
                        dest: rowid_reg,
                    });
                    program.emit_insn_with_label_dependency(
                        Insn::Ne {
                            lhs: rowid_reg,
                            rhs: computed_rowid_reg,
                            target_pc: jump_target_when_false,
                        },
                        jump_target_when_false,
                    );
                }
            }
        }

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
            let _ = translate_expr(program, Some(select), lhs, lhs_reg, cursor_hint);
            match lhs.as_ref() {
                ast::Expr::Literal(_) => program.mark_last_insn_constant(),
                _ => {}
            }
            let _ = translate_expr(program, Some(select), rhs, rhs_reg, cursor_hint);
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
            let _ = translate_expr(program, Some(select), lhs, lhs_reg, cursor_hint)?;

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
                    let _ = translate_expr(program, Some(select), expr, rhs_reg, cursor_hint)?;
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
                    let _ = translate_expr(program, Some(select), expr, rhs_reg, cursor_hint)?;
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
                    let _ = translate_expr(program, Some(select), rhs, pattern_reg, cursor_hint)?;
                    program.mark_last_insn_constant();
                    let _ = translate_expr(program, Some(select), lhs, column_reg, cursor_hint)?;
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

fn introspect_expression_for_table_refs<'a>(
    select: &'a Select,
    where_expr: &'a ast::Expr,
) -> Result<usize> {
    let mut table_refs_mask = 0;
    match where_expr {
        ast::Expr::Binary(e1, _, e2) => {
            table_refs_mask |= introspect_expression_for_table_refs(select, e1)?;
            table_refs_mask |= introspect_expression_for_table_refs(select, e2)?;
        }
        ast::Expr::Id(ident) => {
            let ident = normalize_ident(&ident.0);
            let matching_tables = select
                .src_tables
                .iter()
                .enumerate()
                .filter(|(_, t)| t.table.get_column(&ident).is_some());

            let mut matches = 0;
            let mut matching_tbl = None;
            for table in matching_tables {
                matching_tbl = Some(table);
                matches += 1;
                if matches > 1 {
                    crate::bail_parse_error!("ambiguous column name {}", &ident)
                }
            }

            if let Some((tbl_index, _)) = matching_tbl {
                table_refs_mask |= 1 << tbl_index;
            } else {
                crate::bail_parse_error!("column not found: {}", &ident)
            }
        }
        ast::Expr::Qualified(tbl, ident) => {
            let tbl = normalize_ident(&tbl.0);
            let ident = normalize_ident(&ident.0);
            let matching_table = select
                .src_tables
                .iter()
                .enumerate()
                .find(|(_, t)| t.identifier == tbl);

            if matching_table.is_none() {
                crate::bail_parse_error!("table not found: {}", &tbl)
            }
            let matching_table = matching_table.unwrap();
            if matching_table.1.table.get_column(&ident).is_none() {
                crate::bail_parse_error!("column with qualified name {}.{} not found", &tbl, &ident)
            }

            table_refs_mask |= 1 << matching_table.0;
        }
        ast::Expr::Literal(_) => {}
        ast::Expr::Like { lhs, rhs, .. } => {
            table_refs_mask |= introspect_expression_for_table_refs(select, lhs)?;
            table_refs_mask |= introspect_expression_for_table_refs(select, rhs)?;
        }
        ast::Expr::FunctionCall {
            args: Some(args), ..
        } => {
            for arg in args {
                table_refs_mask |= introspect_expression_for_table_refs(select, arg)?;
            }
        }
        ast::Expr::InList { lhs, rhs, .. } => {
            table_refs_mask |= introspect_expression_for_table_refs(select, lhs)?;
            if let Some(rhs_list) = rhs {
                for rhs_expr in rhs_list {
                    table_refs_mask |= introspect_expression_for_table_refs(select, rhs_expr)?;
                }
            }
        }
        _ => {}
    }

    Ok(table_refs_mask)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstantCondition {
    AlwaysTrue,
    AlwaysFalse,
}

pub trait Evaluatable<'a> {
    // if the expression is a constant expression e.g. '1', returns the constant condition
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
    // if the expression is the primary key of a table, returns the index of the table
    fn check_primary_key(&self, select: &'a Select) -> Result<Option<usize>>;
    // Returns a bitmask of which table indexes the expression references
    fn get_table_references_bitmask(&self, select: &'a Select) -> Result<usize>;
    // Checks if the expression is a candidate for seekrowid optimization
    fn check_seekrowid_candidate(
        &'a self,
        table_index: usize,
        select: &'a Select,
    ) -> Result<Option<SeekRowid<'a>>>;
}

impl<'a> Evaluatable<'a> for ast::Expr {
    fn get_table_references_bitmask(&self, select: &'a Select) -> Result<usize> {
        match self {
            ast::Expr::Id(ident) => {
                let ident = normalize_ident(&ident.0);
                let tables = select.src_tables.iter().enumerate().filter_map(|(i, t)| {
                    if t.table.get_column(&ident).is_some() {
                        Some(i)
                    } else {
                        None
                    }
                });

                let mut matches = 0;
                let mut matching_tbl = None;

                for tbl in tables {
                    matching_tbl = Some(tbl);
                    matches += 1;
                    if matches > 1 {
                        crate::bail_parse_error!("ambiguous column name {}", ident)
                    }
                }

                Ok(matching_tbl.unwrap_or(0))
            }
            ast::Expr::Qualified(tbl, ident) => {
                let tbl = normalize_ident(&tbl.0);
                let ident = normalize_ident(&ident.0);
                let table = select
                    .src_tables
                    .iter()
                    .enumerate()
                    .find(|(_, t)| t.identifier == tbl && t.table.get_column(&ident).is_some());

                if table.is_none() {
                    crate::bail_parse_error!("table not found: {}", tbl)
                }

                let table = table.unwrap();

                Ok(table.0)
            }
            ast::Expr::Binary(lhs, _, rhs) => {
                let lhs = lhs.as_ref().get_table_references_bitmask(select)?;
                let rhs = rhs.as_ref().get_table_references_bitmask(select)?;

                Ok(lhs | rhs)
            }
            _ => Ok(0),
        }
    }
    fn check_primary_key(&self, select: &'a Select) -> Result<Option<usize>> {
        match self {
            ast::Expr::Id(ident) => {
                let ident = normalize_ident(&ident.0);
                let tables = select.src_tables.iter().enumerate().filter_map(|(i, t)| {
                    if t.table
                        .get_column(&ident)
                        .map_or(false, |(_, c)| c.primary_key)
                    {
                        Some(i)
                    } else {
                        None
                    }
                });

                let mut matches = 0;
                let mut matching_tbl = None;

                for tbl in tables {
                    matching_tbl = Some(tbl);
                    matches += 1;
                    if matches > 1 {
                        crate::bail_parse_error!("ambiguous column name {}", ident)
                    }
                }

                Ok(matching_tbl)
            }
            ast::Expr::Qualified(tbl, ident) => {
                let tbl = normalize_ident(&tbl.0);
                let ident = normalize_ident(&ident.0);
                let table = select.src_tables.iter().enumerate().find(|(_, t)| {
                    t.identifier == tbl
                        && t.table
                            .get_column(&ident)
                            .map_or(false, |(_, c)| c.primary_key)
                });

                if table.is_none() {
                    crate::bail_parse_error!("table not found: {}", tbl)
                }

                let table = table.unwrap();

                Ok(Some(table.0))
            }
            _ => Ok(None),
        }
    }
    fn check_seekrowid_candidate(
        &'a self,
        table_index: usize,
        select: &'a Select,
    ) -> Result<Option<SeekRowid<'a>>> {
        match self {
            ast::Expr::Binary(lhs, ast::Operator::Equals, rhs) => {
                let lhs = lhs.as_ref();
                let rhs = rhs.as_ref();

                if let Some(lhs_table_index) = lhs.check_primary_key(select)? {
                    let rhs_table_refs_bitmask = rhs.get_table_references_bitmask(select)?;
                    // For now, we only support seekrowid optimization if the primary key is in an inner loop compared to the other expression.
                    // Example: explain select u.age, p.name from users u join products p on u.id = p.id;
                    // In this case, we loop over the users table and seek the products table.
                    // We also support the case where the other expression is a constant,
                    // e.g. SELECT * FROM USERS u WHERE u.id = 5.
                    // In this case the bitmask of the other expression is 0.
                    if lhs_table_index == table_index && lhs_table_index >= rhs_table_refs_bitmask {
                        return Ok(Some(SeekRowid {
                            table: &select.src_tables[table_index].identifier,
                            rowid_expr: rhs,
                        }));
                    }
                }

                if let Some(rhs_table_index) = rhs.check_primary_key(select)? {
                    let lhs_table_refs_bitmask = lhs.get_table_references_bitmask(select)?;
                    if rhs_table_index == table_index && rhs_table_index >= lhs_table_refs_bitmask {
                        return Ok(Some(SeekRowid {
                            table: &select.src_tables[table_index].identifier,
                            rowid_expr: lhs,
                        }));
                    }
                }

                Ok(None)
            }
            _ => Ok(None),
        }
    }
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
