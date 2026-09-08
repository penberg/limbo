use super::*;

/// Core implementation of IN expression logic that can be used in both conditional and expression contexts.
/// This follows SQLite's approach where a single core function handles all InList cases.
///
/// This is extracted from the original conditional implementation to be reusable.
/// The logic exactly matches the original conditional InList implementation.
///
/// An IN expression has one of the following formats:
///  ```sql
///      x IN (y1, y2,...,yN)
///      x IN (subquery) (Not yet implemented)
///  ```
/// The result of an IN operator is one of TRUE, FALSE, or NULL.  A NULL result
/// means that it cannot be determined if the LHS is contained in the RHS due
/// to the presence of NULL values.
///
/// Currently, we do a simple full-scan, yet it's not ideal when there are many rows
/// on RHS. (Check sqlite's in-operator.md)
///
/// Algorithm:
/// 1. Set the null-flag to false
/// 2. For each row in the RHS:
///     - Compare LHS and RHS
///     - If LHS matches RHS, returns TRUE
///     - If the comparison results in NULL, set the null-flag to true
/// 3. If the null-flag is true, return NULL
/// 4. Return FALSE
///
/// A "NOT IN" operator is computed by first computing the equivalent IN
/// operator, then interchanging the TRUE and FALSE results.
/// Compute the affinity for an IN expression.
/// For `x IN (y1, y2, ..., yN)`, the affinity is determined by the LHS expression `x`.
/// This follows SQLite's `exprINAffinity()` function.
pub(super) fn in_expr_affinity(
    lhs: &ast::Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: Option<&Resolver>,
) -> Affinity {
    // For parenthesized expressions (vectors), we take the first element's affinity
    // since scalar IN comparisons only use the first element
    match lhs {
        Expr::Parenthesized(exprs) if !exprs.is_empty() => {
            get_expr_affinity(&exprs[0], referenced_tables, resolver)
        }
        _ => get_expr_affinity(lhs, referenced_tables, resolver),
    }
}

#[instrument(skip(program, referenced_tables, resolver), level = Level::DEBUG)]
pub(super) fn translate_in_list(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    lhs: &ast::Expr,
    rhs: &[Box<ast::Expr>],
    condition_metadata: ConditionMetadata,
    // dest if null should be in ConditionMetadata
    resolver: &Resolver,
) -> Result<()> {
    let lhs_arity = expr_vector_size(lhs)?;
    let lhs_reg = program.alloc_registers(lhs_arity);
    let _ = translate_expr(program, referenced_tables, lhs, lhs_reg, resolver)?;
    let mut check_null_reg = 0;
    let label_ok = program.allocate_label();

    // Compute the affinity for the IN comparison based on the LHS expression
    // This follows SQLite's exprINAffinity() approach
    let affinity = in_expr_affinity(lhs, referenced_tables, Some(resolver));
    let cmp_flags = CmpInsFlags::default().with_affinity(affinity);

    if condition_metadata.jump_target_when_false != condition_metadata.jump_target_when_null {
        check_null_reg = program.alloc_register();
        program.emit_insn(Insn::BitAnd {
            lhs: lhs_reg,
            rhs: lhs_reg,
            dest: check_null_reg,
        });
    }

    for (i, expr) in rhs.iter().enumerate() {
        let last_condition = i == rhs.len() - 1;
        let rhs_reg = program.alloc_registers(lhs_arity);
        let _ = translate_expr(program, referenced_tables, expr, rhs_reg, resolver)?;

        if check_null_reg != 0 && expr.can_be_null() {
            program.emit_insn(Insn::BitAnd {
                lhs: check_null_reg,
                rhs: rhs_reg,
                dest: check_null_reg,
            });
        }

        if lhs_arity == 1 {
            // Scalar comparison path
            if !last_condition
                || condition_metadata.jump_target_when_false
                    != condition_metadata.jump_target_when_null
            {
                if lhs_reg != rhs_reg {
                    program.emit_insn(Insn::Eq {
                        lhs: lhs_reg,
                        rhs: rhs_reg,
                        target_pc: label_ok,
                        flags: cmp_flags,
                        collation: program.curr_collation(),
                    });
                } else {
                    program.emit_insn(Insn::NotNull {
                        reg: lhs_reg,
                        target_pc: label_ok,
                    });
                }
            } else if lhs_reg != rhs_reg {
                program.emit_insn(Insn::Ne {
                    lhs: lhs_reg,
                    rhs: rhs_reg,
                    target_pc: condition_metadata.jump_target_when_false,
                    flags: cmp_flags.jump_if_null(),
                    collation: program.curr_collation(),
                });
            } else {
                program.emit_insn(Insn::IsNull {
                    reg: lhs_reg,
                    target_pc: condition_metadata.jump_target_when_false,
                });
            }
        } else {
            // Row-valued comparison path: compare each component
            if !last_condition
                || condition_metadata.jump_target_when_false
                    != condition_metadata.jump_target_when_null
            {
                // If all components match, jump to label_ok; otherwise skip to next RHS item
                let skip_label = program.allocate_label();
                for j in 0..lhs_arity {
                    let (aff, collation) = row_component_affinity_collation(
                        lhs,
                        expr,
                        j,
                        referenced_tables,
                        Some(resolver),
                    )?;
                    let flags = CmpInsFlags::default().with_affinity(aff);
                    if j < lhs_arity - 1 {
                        program.emit_insn(Insn::Ne {
                            lhs: lhs_reg + j,
                            rhs: rhs_reg + j,
                            target_pc: skip_label,
                            flags,
                            collation,
                        });
                    } else {
                        program.emit_insn(Insn::Eq {
                            lhs: lhs_reg + j,
                            rhs: rhs_reg + j,
                            target_pc: label_ok,
                            flags,
                            collation,
                        });
                    }
                }
                program.preassign_label_to_next_insn(skip_label);
            } else {
                // Last condition, simple case: jump to false if any component doesn't match
                for j in 0..lhs_arity {
                    let (aff, collation) = row_component_affinity_collation(
                        lhs,
                        expr,
                        j,
                        referenced_tables,
                        Some(resolver),
                    )?;
                    let flags = CmpInsFlags::default().with_affinity(aff).jump_if_null();
                    program.emit_insn(Insn::Ne {
                        lhs: lhs_reg + j,
                        rhs: rhs_reg + j,
                        target_pc: condition_metadata.jump_target_when_false,
                        flags,
                        collation,
                    });
                }
            }
        }
    }
    program.reset_collation();
    if check_null_reg != 0 {
        program.emit_insn(Insn::IsNull {
            reg: check_null_reg,
            target_pc: condition_metadata.jump_target_when_null,
        });
        program.emit_insn(Insn::Goto {
            target_pc: condition_metadata.jump_target_when_false,
        });
    }

    // we don't know exactly what instruction will came next and it's important to chain label to the execution flow rather then exact next instruction
    // for example, next instruction can be register assignment, which can be moved by optimized to the constant section
    // in this case, label_ok must be changed accordingly and be re-binded to another instruction followed the current translation unit after constants reording
    program.preassign_label_to_next_insn(label_ok);

    // by default if IN expression is true we just continue to the next instruction
    if condition_metadata.jump_if_condition_is_true {
        program.emit_insn(Insn::Goto {
            target_pc: condition_metadata.jump_target_when_true,
        });
    }
    // todo: deallocate check_null_reg

    Ok(())
}

#[instrument(skip(program, referenced_tables, expr, resolver), level = Level::DEBUG)]
pub fn translate_condition_expr(
    program: &mut ProgramBuilder,
    referenced_tables: &TableReferences,
    expr: &ast::Expr,
    condition_metadata: ConditionMetadata,
    resolver: &Resolver,
) -> Result<()> {
    match expr {
        ast::Expr::SubqueryResult { query_type, .. } => match query_type {
            SubqueryType::Exists { result_reg } => {
                emit_cond_jump(program, condition_metadata, *result_reg);
            }
            SubqueryType::In { .. } => {
                let result_reg = program.alloc_register();
                translate_expr(program, Some(referenced_tables), expr, result_reg, resolver)?;
                emit_cond_jump(program, condition_metadata, result_reg);
            }
            SubqueryType::RowValue { num_regs, .. } => {
                if *num_regs != 1 {
                    // A query like SELECT * FROM t WHERE (SELECT ...) must return a single column.
                    crate::bail_parse_error!("sub-select returns {num_regs} columns - expected 1");
                }
                let result_reg = program.alloc_register();
                translate_expr(program, Some(referenced_tables), expr, result_reg, resolver)?;
                emit_cond_jump(program, condition_metadata, result_reg);
            }
        },
        ast::Expr::Register(_) => {
            crate::bail_parse_error!(
                "Register in WHERE clause is currently unused. Consider removing Resolver::expr_to_reg_cache and using Expr::Register instead"
            );
        }
        ast::Expr::Collate(_, _) => {
            crate::bail_parse_error!("Collate in WHERE clause is not supported");
        }
        ast::Expr::DoublyQualified(_, _, _) | ast::Expr::Id(_) | ast::Expr::Qualified(_, _) => {
            crate::bail_parse_error!(
                "DoublyQualified/Id/Qualified should have been rewritten to Column during binding"
            );
        }
        ast::Expr::FieldAccess { .. } => {
            crate::bail_parse_error!(
                "struct/union field access cannot be used as a bare boolean condition in WHERE"
            );
        }
        ast::Expr::Exists(_) => {
            crate::bail_parse_error!("EXISTS in WHERE clause is not supported");
        }
        ast::Expr::Subquery(_) => {
            crate::bail_parse_error!("Subquery in WHERE clause is not supported");
        }
        ast::Expr::InSelect { .. } => {
            crate::bail_parse_error!("IN (...subquery) in WHERE clause is not supported");
        }
        ast::Expr::InTable { .. } => {
            crate::bail_parse_error!("Table expression in WHERE clause is not supported");
        }
        ast::Expr::FunctionCallStar { .. } => {
            crate::bail_parse_error!("FunctionCallStar in WHERE clause is not supported");
        }
        ast::Expr::Raise(_, _) => {
            crate::bail_parse_error!("RAISE in WHERE clause is not supported");
        }
        ast::Expr::Between { .. } => {
            let between_result_reg = program.alloc_register();
            translate_between_expr(
                program,
                Some(referenced_tables),
                expr.clone(),
                between_result_reg,
                resolver,
            )?;
            emit_cond_jump(program, condition_metadata, between_result_reg);
        }
        ast::Expr::Variable(_) => {
            let reg = program.alloc_register();
            translate_expr(program, Some(referenced_tables), expr, reg, resolver)?;
            emit_cond_jump(program, condition_metadata, reg);
        }
        ast::Expr::Name(_) => {
            crate::bail_parse_error!("Name as a direct predicate in WHERE clause is not supported");
        }
        ast::Expr::Binary(lhs, ast::Operator::And, rhs) => {
            // In a binary AND, never jump to the parent 'jump_target_when_true' label on the first condition, because
            // the second condition MUST also be true. Instead we instruct the child expression to jump to a local
            // true label.
            let jump_target_when_true = program.allocate_label();
            translate_condition_expr(
                program,
                referenced_tables,
                lhs,
                ConditionMetadata {
                    jump_if_condition_is_true: false,
                    jump_target_when_true,
                    ..condition_metadata
                },
                resolver,
            )?;
            program.preassign_label_to_next_insn(jump_target_when_true);
            translate_condition_expr(
                program,
                referenced_tables,
                rhs,
                condition_metadata,
                resolver,
            )?;
        }
        ast::Expr::Binary(lhs, ast::Operator::Or, rhs) => {
            // In a binary OR, never jump to the parent 'jump_target_when_false' or
            // 'jump_target_when_null' label on the first condition, because the second
            // condition CAN also be true. Instead we instruct the child expression to
            // jump to a local false label so the right side of OR gets evaluated.
            // This is critical for cases like `x IN (NULL, 3) OR b` where the left side
            // evaluates to NULL — we must still evaluate the right side.
            let jump_target_when_false = program.allocate_label();
            translate_condition_expr(
                program,
                referenced_tables,
                lhs,
                ConditionMetadata {
                    jump_if_condition_is_true: true,
                    jump_target_when_false,
                    jump_target_when_null: jump_target_when_false,
                    ..condition_metadata
                },
                resolver,
            )?;
            program.preassign_label_to_next_insn(jump_target_when_false);
            translate_condition_expr(
                program,
                referenced_tables,
                rhs,
                condition_metadata,
                resolver,
            )?;
        }
        // Handle IS TRUE/IS FALSE/IS NOT TRUE/IS NOT FALSE in conditions
        // Delegate to translate_expr which handles these correctly with IsTrue instruction
        ast::Expr::Binary(_, ast::Operator::Is | ast::Operator::IsNot, e2)
            if truth_test_rhs(e2).is_some() =>
        {
            let reg = program.alloc_register();
            translate_expr(program, Some(referenced_tables), expr, reg, resolver)?;
            emit_cond_jump(program, condition_metadata, reg);
        }
        // Handle IS NULL/IS NOT NULL in conditions using IsNull/NotNull opcodes.
        // "a IS NULL" is parsed as Binary(a, Is, Null), but we need to use the IsNull opcode
        // (not Eq/Ne with null_eq flag) for correct NULL handling in WHERE clauses.
        ast::Expr::Binary(e1, ast::Operator::Is, e2)
            if matches!(e2.as_ref(), ast::Expr::Literal(ast::Literal::Null)) =>
        {
            let cur_reg = program.alloc_register();
            translate_expr(program, Some(referenced_tables), e1, cur_reg, resolver)?;
            if condition_metadata.jump_if_condition_is_true {
                program.emit_insn(Insn::IsNull {
                    reg: cur_reg,
                    target_pc: condition_metadata.jump_target_when_true,
                });
            } else {
                program.emit_insn(Insn::NotNull {
                    reg: cur_reg,
                    target_pc: condition_metadata.jump_target_when_false,
                });
            }
        }
        ast::Expr::Binary(e1, ast::Operator::IsNot, e2)
            if matches!(e2.as_ref(), ast::Expr::Literal(ast::Literal::Null)) =>
        {
            let cur_reg = program.alloc_register();
            translate_expr(program, Some(referenced_tables), e1, cur_reg, resolver)?;
            if condition_metadata.jump_if_condition_is_true {
                program.emit_insn(Insn::NotNull {
                    reg: cur_reg,
                    target_pc: condition_metadata.jump_target_when_true,
                });
            } else {
                program.emit_insn(Insn::IsNull {
                    reg: cur_reg,
                    target_pc: condition_metadata.jump_target_when_false,
                });
            }
        }
        ast::Expr::Binary(e1, op, e2) => {
            // Check if either operand has a custom type with a matching operator
            if let Some(resolved) =
                find_custom_type_operator(e1, e2, op, Some(referenced_tables), resolver)
            {
                let result_reg = emit_custom_type_operator(
                    program,
                    Some(referenced_tables),
                    e1,
                    e2,
                    &resolved,
                    resolver,
                )?;
                emit_cond_jump(program, condition_metadata, result_reg);
            } else {
                let result_reg = program.alloc_register();
                binary_expr_shared(
                    program,
                    Some(referenced_tables),
                    e1,
                    e2,
                    op,
                    result_reg,
                    resolver,
                    BinaryEmitMode::Condition(condition_metadata),
                )?;
            }
        }
        ast::Expr::Literal(_)
        | ast::Expr::Cast { .. }
        | ast::Expr::FunctionCall { .. }
        | ast::Expr::Column { .. }
        | ast::Expr::RowId { .. }
        | ast::Expr::Case { .. } => {
            let reg = program.alloc_register();
            translate_expr(program, Some(referenced_tables), expr, reg, resolver)?;
            emit_cond_jump(program, condition_metadata, reg);
        }

        ast::Expr::InList { lhs, not, rhs } => {
            let ConditionMetadata {
                jump_if_condition_is_true,
                jump_target_when_true,
                jump_target_when_false,
                jump_target_when_null,
            } = condition_metadata;

            // Adjust targets if `NOT IN`
            let (adjusted_metadata, not_true_label, not_false_label) = if *not {
                let not_true_label = program.allocate_label();
                let not_false_label = program.allocate_label();
                (
                    ConditionMetadata {
                        jump_if_condition_is_true,
                        jump_target_when_true: not_true_label,
                        jump_target_when_false: not_false_label,
                        jump_target_when_null,
                    },
                    Some(not_true_label),
                    Some(not_false_label),
                )
            } else {
                (condition_metadata, None, None)
            };

            translate_in_list(
                program,
                Some(referenced_tables),
                lhs,
                rhs,
                adjusted_metadata,
                resolver,
            )?;

            if *not {
                // When IN is TRUE (match found), NOT IN should be FALSE
                program.preassign_label_to_next_insn(not_true_label.unwrap());
                program.emit_insn(Insn::Goto {
                    target_pc: jump_target_when_false,
                });

                // When IN is FALSE (no match), NOT IN should be TRUE
                program.preassign_label_to_next_insn(not_false_label.unwrap());
                program.emit_insn(Insn::Goto {
                    target_pc: jump_target_when_true,
                });
            }
        }
        ast::Expr::Like { not, .. } => {
            let cur_reg = program.alloc_register();
            translate_like_base(program, Some(referenced_tables), expr, cur_reg, resolver)?;
            if !*not {
                emit_cond_jump(program, condition_metadata, cur_reg);
            } else if condition_metadata.jump_if_condition_is_true {
                program.emit_insn(Insn::IfNot {
                    reg: cur_reg,
                    target_pc: condition_metadata.jump_target_when_true,
                    jump_if_null: false,
                });
            } else {
                program.emit_insn(Insn::If {
                    reg: cur_reg,
                    target_pc: condition_metadata.jump_target_when_false,
                    jump_if_null: true,
                });
            }
        }
        ast::Expr::Parenthesized(exprs) => {
            if exprs.len() == 1 {
                translate_condition_expr(
                    program,
                    referenced_tables,
                    &exprs[0],
                    condition_metadata,
                    resolver,
                )?;
            } else {
                crate::bail_parse_error!(
                    "parenthesized conditional should have exactly one expression"
                );
            }
        }
        ast::Expr::NotNull(expr) => {
            let cur_reg = program.alloc_register();
            translate_expr(program, Some(referenced_tables), expr, cur_reg, resolver)?;
            if condition_metadata.jump_if_condition_is_true {
                program.emit_insn(Insn::NotNull {
                    reg: cur_reg,
                    target_pc: condition_metadata.jump_target_when_true,
                });
            } else {
                program.emit_insn(Insn::IsNull {
                    reg: cur_reg,
                    target_pc: condition_metadata.jump_target_when_false,
                });
            }
        }
        ast::Expr::IsNull(expr) => {
            let cur_reg = program.alloc_register();
            translate_expr(program, Some(referenced_tables), expr, cur_reg, resolver)?;
            if condition_metadata.jump_if_condition_is_true {
                program.emit_insn(Insn::IsNull {
                    reg: cur_reg,
                    target_pc: condition_metadata.jump_target_when_true,
                });
            } else {
                program.emit_insn(Insn::NotNull {
                    reg: cur_reg,
                    target_pc: condition_metadata.jump_target_when_false,
                });
            }
        }
        ast::Expr::Unary(_, _) => {
            // This is an inefficient implementation for op::NOT, because translate_expr() will emit an Insn::Not,
            // and then we immediately emit an Insn::If/Insn::IfNot for the conditional jump. In reality we would not
            // like to emit the negation instruction Insn::Not at all, since we could just emit the "opposite" jump instruction
            // directly. However, using translate_expr() directly simplifies our conditional jump code for unary expressions,
            // and we'd rather be correct than maximally efficient, for now.
            let expr_reg = program.alloc_register();
            translate_expr(program, Some(referenced_tables), expr, expr_reg, resolver)?;
            emit_cond_jump(program, condition_metadata, expr_reg);
        }
        ast::Expr::Default => {
            crate::bail_parse_error!("DEFAULT is only valid in INSERT VALUES");
        }
        ast::Expr::Array { .. } | ast::Expr::Subscript { .. } => {
            unreachable!("Array and Subscript are desugared into function calls by the parser")
        }
    }
    Ok(())
}
