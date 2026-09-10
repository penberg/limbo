use super::*;
use crate::translate::sequence::emit_sequence_descriptor_literals;
use crate::vdbe::builder::CursorType;
use crate::vdbe::insn::{to_u32, InsertFlags, RegisterOrLiteral};

/// The base logic for translating LIKE and GLOB expressions.
/// The logic for handling "NOT LIKE" is different depending on whether the expression
/// is a conditional jump or not. This is why the caller handles the "NOT LIKE" behavior;
/// see [translate_condition_expr] and [translate_expr] for implementations.
pub(super) fn translate_like_base(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
) -> Result<usize> {
    let ast::Expr::Like {
        lhs,
        op,
        rhs,
        escape,
        ..
    } = expr
    else {
        crate::bail_parse_error!("expected Like expression");
    };
    match op {
        ast::LikeOperator::Like | ast::LikeOperator::Glob => {
            let arg_count = if escape.is_some() { 3 } else { 2 };
            let start_reg = program.alloc_registers(arg_count);
            let mut constant_mask = 0;
            translate_expr(program, referenced_tables, lhs, start_reg + 1, resolver)?;
            let _ = translate_expr(program, referenced_tables, rhs, start_reg, resolver)?;
            if arg_count == 3 {
                if let Some(escape) = escape {
                    translate_expr(program, referenced_tables, escape, start_reg + 2, resolver)?;
                }
            }
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
                start_reg,
                dest: target_register,
                func: FuncCtx {
                    func: Func::Scalar(func),
                    arg_count,
                },
            });
        }
        #[cfg(all(feature = "fts", not(target_family = "wasm")))]
        ast::LikeOperator::Match => {
            // Transform MATCH to fts_match():
            // - `col MATCH 'query'` -> `fts_match(col, 'query')`
            // - `(col1, col2) MATCH 'query'` -> `fts_match(col1, col2, 'query')`
            let columns: Vec<&ast::Expr> = match lhs.as_ref() {
                ast::Expr::Parenthesized(cols) => cols.iter().map(|c| c.as_ref()).collect(),
                other => vec![other],
            };
            let arg_count = columns.len() + 1; // columns + query
            let start_reg = program.alloc_registers(arg_count);

            for (i, col) in columns.iter().enumerate() {
                translate_expr(program, referenced_tables, col, start_reg + i, resolver)?;
            }
            translate_expr(
                program,
                referenced_tables,
                rhs,
                start_reg + columns.len(),
                resolver,
            )?;

            program.emit_insn(Insn::Function {
                constant_mask: 0,
                start_reg,
                dest: target_register,
                func: FuncCtx {
                    func: Func::Fts(FtsFunc::Match),
                    arg_count,
                },
            });
        }
        #[cfg(any(not(feature = "fts"), target_family = "wasm"))]
        ast::LikeOperator::Match => {
            crate::bail_parse_error!("MATCH requires the 'fts' feature to be enabled")
        }
        ast::LikeOperator::Regexp => {
            if escape.is_some() {
                crate::bail_parse_error!("wrong number of arguments to function regexp()");
            }
            let func = resolver.resolve_function("regexp", 2)?;
            let Some(func) = func else {
                crate::bail_parse_error!("no such function: regexp");
            };
            let arg_count = 2;
            let start_reg = program.alloc_registers(arg_count);
            // regexp(pattern, haystack) — pattern is rhs, haystack is lhs
            translate_expr(program, referenced_tables, rhs, start_reg, resolver)?;
            translate_expr(program, referenced_tables, lhs, start_reg + 1, resolver)?;
            program.emit_insn(Insn::Function {
                constant_mask: 0,
                start_reg,
                dest: target_register,
                func: FuncCtx { func, arg_count },
            });
        }
    }

    Ok(target_register)
}

/// Emits a whole insn for a function call.
/// Assumes the number of parameters is valid for the given function.
/// Returns the target register for the function.
pub(super) fn translate_function(
    program: &mut ProgramBuilder,
    args: &[Box<ast::Expr>],
    referenced_tables: Option<&TableReferences>,
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

pub(super) fn wrap_eval_jump_expr(
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

pub(super) fn wrap_eval_jump_expr_zero_or_null(
    program: &mut ProgramBuilder,
    insn: Insn,
    target_register: usize,
    if_true_label: BranchOffset,
    e1_reg: usize,
    e2_reg: usize,
) {
    program.emit_insn(Insn::Integer {
        value: 1, // emit True by default
        dest: target_register,
    });
    program.emit_insn(insn);
    program.emit_insn(Insn::ZeroOrNull {
        rg1: e1_reg,
        rg2: e2_reg,
        dest: target_register,
    });
    program.preassign_label_to_next_insn(if_true_label);
}

/// Translate nextval/setval with disk-only backing-table state.
///
/// ## Persistence design
///
/// The backing table (`__turso_internal_seq_<name>`) is the sole source of
/// truth for the sequence's runtime state. There is no in-memory atomic.
/// Every nextval/setval reads the current watermark from the table inside
/// the executing transaction, computes the new value, and writes it back.
///
/// **NextVal**: seek to the watermark row (Last for ascending sequences,
/// Rewind for descending), read its (value, is_called); the
/// `SequenceComputeNext` opcode applies start/increment/cycle/exhaustion
/// logic to derive the next value; the standard MakeRecord+Insert pattern
/// writes a new row keyed by that value. The backing table may transiently
/// hold multiple rows (one per nextval in a long transaction); checkpoint
/// compacts to a single watermark row.
///
/// **SetVal**: validate the arguments (Function still runs for type-check
/// and per-connection currval update), DELETE every existing row, INSERT a
/// new single watermark row at the user-supplied value.
///
/// Cross-process correctness: under WAL the write lock serializes the
/// disk read+write; the next holder reads the latest committed watermark.
/// Under MVCC the autonomous-tx wrapper (handled at the opcode dispatch
/// layer) serializes via WriteWriteConflict + bounded retry.
pub(super) fn translate_sequence_function(
    program: &mut ProgramBuilder,
    args: &[Box<ast::Expr>],
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
    target_register: usize,
    func_ctx: FuncCtx,
) -> Result<usize> {
    let is_nextval = matches!(&func_ctx.func, Func::Scalar(ScalarFunc::NextVal));

    let seq_name_raw = extract_string_literal(&args[0])?;
    let (database_id, normalized_name) = if let Some((schema, name)) = seq_name_raw.split_once('.')
    {
        let schema_norm = normalize_ident(schema);
        let db_id = match schema_norm.as_str() {
            "main" => crate::MAIN_DB_ID,
            "temp" => crate::TEMP_DB_ID,
            _ => resolver
                .get_attached_database(&schema_norm)
                .map(|(idx, _)| idx)
                .ok_or_else(|| {
                    LimboError::InvalidArgument(format!("no such database: {schema_norm}"))
                })?,
        };
        (db_id, normalize_ident(name))
    } else {
        (crate::MAIN_DB_ID, normalize_ident(&seq_name_raw))
    };

    let backing_table_name =
        crate::translate::sequence::sequence_backing_table_name(&normalized_name);
    let backing_table =
        resolver.with_schema(database_id, |s| s.get_btree_table(&backing_table_name));
    if backing_table.is_none() {
        crate::bail_parse_error!("sequence \"{}\" does not exist", seq_name_raw);
    }

    // Fetch the immutable sequence descriptor — start/inc/min/max/cycle are
    // baked into the bytecode as literal Integers since they never change
    // for a given sequence object.
    let seq_arc = resolver
        .with_schema(database_id, |s| s.get_sequence(&normalized_name).cloned())
        .ok_or_else(|| {
            LimboError::ParseError(format!("sequence \"{seq_name_raw}\" does not exist"))
        })?;

    let start_reg = program.alloc_registers(args.len());
    for (i, arg) in args.iter().enumerate() {
        translate_expr(program, referenced_tables, arg, start_reg + i, resolver)?;
    }

    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;

    if is_nextval {
        // Reuse `start_reg` (the user's raw arg, possibly schema-qualified
        // such as `'aux.my_seq'`) as the seq-name register so per-session
        // `currval` stays keyed by the user's original spelling. The
        // runtime handler strips the schema prefix when looking up the
        // sequence object; the opcode's `db` field already encodes the
        // correct database id resolved here.
        crate::translate::sequence::emit_disk_read_nextval(
            program,
            resolver,
            database_id,
            &normalized_name,
            &seq_arc,
            target_register,
            Some(start_reg),
        )?;
    } else {
        // setval keeps its own cursor lifecycle: open the backing table,
        // validate the user-supplied value via the Function handler,
        // DELETE every existing row, then INSERT one at the requested
        // value with the descriptor suffix.
        let backing_table = backing_table.unwrap();
        let root_page = backing_table.root_page;
        let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(backing_table));
        program.emit_insn(Insn::OpenWrite {
            cursor_id,
            root_page: RegisterOrLiteral::Literal(root_page),
            db: database_id,
        });

        // Function instruction performs argument validation and writes the
        // user-visible result into target_register. The handler no longer
        // mutates any in-memory atomic.
        program.emit_insn(Insn::Function {
            constant_mask: 0,
            start_reg,
            dest: target_register,
            func: func_ctx,
        });

        let empty_label = program.allocate_label();
        let loop_label = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id,
            pc_if_empty: empty_label,
        });
        program.preassign_label_to_next_insn(loop_label);
        program.emit_insn(Insn::Delete {
            cursor_id,
            table_name: normalized_name.clone(),
            // Sequence storage is internal bookkeeping, not a SQL row change.
            is_part_of_update: true,
        });
        program.emit_insn(Insn::Next {
            cursor_id,
            pc_if_next: loop_label,
            fullscan: false,
            is_index: false,
        });
        program.preassign_label_to_next_insn(empty_label);

        let col_base = program.alloc_registers(7);
        program.emit_insn(Insn::Copy {
            src_reg: start_reg + 1,
            dst_reg: col_base,
            extra_amount: 0,
        });
        if args.len() > 2 {
            program.emit_insn(Insn::Copy {
                src_reg: start_reg + 2,
                dst_reg: col_base + 1,
                extra_amount: 0,
            });
        } else {
            program.emit_insn(Insn::Integer {
                dest: col_base + 1,
                value: 1,
            });
        }
        emit_sequence_descriptor_literals(program, &seq_arc, col_base + 2);

        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(col_base),
            count: 7,
            dest_reg: to_u32(record_reg),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: cursor_id,
            key_reg: start_reg + 1,
            record_reg,
            flag: InsertFlags::new().require_seek().skip_all_change_counts(),
            table_name: normalized_name.clone(),
        });
        program.emit_insn(Insn::SetSequenceCurrval {
            seq_name_reg: start_reg,
            value_reg: start_reg + 1,
        });
        program.emit_insn(Insn::Close { cursor_id });
        // For AUTOINCREMENT-backing sequences, also mirror the new
        // watermark into `sqlite_sequence` so the SQLite-compat row
        // tracks `setval()` without waiting for a checkpoint. Matches
        // the inline sync the nextval / advance_past paths emit; see
        // `emit_autoincrement_sqlite_sequence_sync` for the rationale.
        crate::translate::sequence::emit_autoincrement_sqlite_sequence_sync(
            program,
            resolver,
            database_id,
            &normalized_name,
            start_reg + 1,
        )?;
    }

    Ok(target_register)
}
