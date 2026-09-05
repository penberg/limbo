use super::*;
use crate::alloc::TursoIteratorExt;

/// Emit literal values - shared between regular and RETURNING expression evaluation
pub fn emit_literal(
    program: &mut ProgramBuilder,
    literal: &ast::Literal,
    target_register: usize,
) -> Result<usize> {
    match literal {
        ast::Literal::Numeric(val) => {
            match parse_numeric_literal(val)? {
                Value::Numeric(Numeric::Integer(int_value)) => {
                    program.emit_insn(Insn::Integer {
                        value: int_value,
                        dest: target_register,
                    });
                }
                Value::Numeric(Numeric::Float(real_value)) => {
                    program.emit_insn(Insn::Real {
                        value: real_value.into(),
                        dest: target_register,
                    });
                }
                _ => unreachable!(),
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
            let bytes = ast::blob_literal_hex(s)
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| {
                    // We assume that sqlite3-parser has already validated that
                    // the input is valid hex string, thus unwrap is safe.
                    let hex_byte = std::str::from_utf8(pair).unwrap();
                    u8::from_str_radix(hex_byte, 16).unwrap()
                })
                .try_collect()?;
            program.emit_insn(Insn::Blob {
                value: bytes,
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::Keyword(_) => {
            crate::bail_parse_error!("Keyword in WHERE clause is not supported")
        }
        ast::Literal::Null => {
            program.emit_insn(Insn::Null {
                dest: target_register,
                dest_end: None,
            });
            Ok(target_register)
        }
        ast::Literal::True => {
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::False => {
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::CurrentDate => {
            program.emit_insn(Insn::String8 {
                value: datetime::exec_date::<&[_; 0], std::slice::Iter<'_, Value>, &Value>(&[])
                    .to_string(),
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::CurrentTime => {
            program.emit_insn(Insn::String8 {
                value: datetime::exec_time::<&[_; 0], std::slice::Iter<'_, Value>, &Value>(&[])
                    .to_string(),
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::CurrentTimestamp => {
            program.emit_insn(
                Insn::String8 {
                    value: datetime::exec_datetime_full::<
                        &[_; 0],
                        std::slice::Iter<'_, Value>,
                        &Value,
                    >(&[])
                    .to_string(),
                    dest: target_register,
                },
            );
            Ok(target_register)
        }
    }
}

/// Emit a function call instruction with pre-allocated argument registers
/// This is shared between different function call contexts
pub fn emit_function_call(
    program: &mut ProgramBuilder,
    func_ctx: FuncCtx,
    arg_registers: &[usize],
    target_register: usize,
) -> Result<()> {
    let start_reg = if arg_registers.is_empty() {
        target_register // If no arguments, use target register as start
    } else {
        arg_registers[0] // Use first argument register as start
    };

    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg,
        dest: target_register,
        func: func_ctx,
    });

    Ok(())
}

/// Process a RETURNING clause, converting ResultColumn expressions into ResultSetColumn structures
/// with proper column binding and alias handling.
pub fn process_returning_clause(
    returning: &mut [ast::ResultColumn],
    table_references: &mut TableReferences,
    resolver: &Resolver<'_>,
) -> Result<Vec<ResultSetColumn>> {
    let mut result_columns = Vec::with_capacity(returning.len());

    for rc in returning.iter_mut() {
        match rc {
            ast::ResultColumn::Expr(expr, alias) => {
                bind_and_rewrite_expr(
                    expr,
                    Some(table_references),
                    None,
                    resolver,
                    BindingBehavior::TryResultColumnsFirst,
                )?;

                let vec_size = expr_vector_size(expr)?;
                if vec_size != 1 {
                    crate::bail_parse_error!(
                        "sub-select returns {} columns - expected 1",
                        vec_size
                    );
                }

                // An implicit column name is the verbatim SQL text of the
                // expression, not a user-written alias. Keeping the two apart
                // lets a plain column reference report its own name, so
                // RETURNING "t"."id" is named `id` just like SELECT is.
                let (alias, implicit_column_name) = match alias.as_ref() {
                    Some(ast::As::As(name)) | Some(ast::As::Elided(name)) => {
                        (Some(name.as_str().to_string()), None)
                    }
                    Some(ast::As::ImplicitColumnName(name)) => {
                        (None, Some(name.as_str().to_string()))
                    }
                    None => (None, None),
                };

                result_columns.push(ResultSetColumn {
                    expr: expr.as_ref().clone(),
                    alias,
                    implicit_column_name,
                    contains_aggregates: false,
                });
            }
            ast::ResultColumn::Star => {
                let table = table_references
                    .joined_tables()
                    .first()
                    .expect("RETURNING clause must reference at least one table");
                let internal_id = table.internal_id;

                // Handle RETURNING * by expanding to all table columns
                // Use the shared internal_id for all columns
                for (column_index, column) in table.columns().iter().enumerate() {
                    let column_expr = Expr::Column {
                        database: None,
                        table: internal_id,
                        column: column_index,
                        is_rowid_alias: column.is_rowid_alias(),
                    };

                    result_columns.push(ResultSetColumn {
                        expr: column_expr,
                        alias: column.name.clone(),
                        implicit_column_name: None,
                        contains_aggregates: false,
                    });
                }
            }
            ast::ResultColumn::TableStar(_) => {
                crate::bail_parse_error!("RETURNING may not use \"TABLE.*\" wildcards");
            }
        }
    }

    Ok(result_columns)
}

/// Context for buffering RETURNING results into an ephemeral table
/// instead of yielding them immediately via ResultRow.
/// When used, the DML loop buffers each result row into the ephemeral table,
/// and a scan-back loop after the DML loop yields them to the caller.
pub struct ReturningBufferCtx {
    /// Cursor ID of the ephemeral table to buffer results into
    pub cursor_id: usize,
    /// Number of RETURNING columns (used for scan-back)
    pub num_columns: usize,
}

/// Emit the scan-back loop that reads all buffered RETURNING rows from the
/// ephemeral table and yields them via ResultRow. Called after all DML is complete.
pub(crate) fn emit_returning_scan_back(program: &mut ProgramBuilder, buf: &ReturningBufferCtx) {
    let end_label = program.allocate_label();
    let scan_start = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: buf.cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(scan_start);

    let result_start_reg = program.alloc_registers(buf.num_columns);
    for i in 0..buf.num_columns {
        program.emit_insn(Insn::Column {
            cursor_id: buf.cursor_id,
            column: i,
            dest: result_start_reg + i,
            default: None,
        });
    }
    program.emit_insn(Insn::ResultRow {
        start_reg: result_start_reg,
        count: buf.num_columns,
    });
    program.emit_insn(Insn::Next {
        cursor_id: buf.cursor_id,
        pc_if_next: scan_start,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(end_label);
}

/// Emit bytecode to evaluate RETURNING expressions and produce result rows.
/// RETURNING result expressions are otherwise evaluated as normal, but the columns of the target table
/// are added to [Resolver::expr_to_reg_cache], meaning a reference to e.g tbl.col will effectively
/// refer to a register where the OLD/NEW value of tbl.col is stored after an INSERT/UPDATE/DELETE.
///
/// When `returning_buffer` is `Some`, the results are buffered into an ephemeral table
/// instead of being yielded immediately. A subsequent call to `emit_returning_scan_back`
/// will drain the buffer and yield the rows to the caller.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_returning_results<'a>(
    program: &mut ProgramBuilder,
    table_references: &TableReferences,
    result_columns: &[ResultSetColumn],
    reg_columns_start: usize,
    rowid_reg: usize,
    resolver: &mut Resolver<'a>,
    returning_buffer: Option<&ReturningBufferCtx>,
    layout: &ColumnLayout,
) -> Result<()> {
    if result_columns.is_empty() {
        return Ok(());
    }

    let cache_state = seed_returning_row_image_in_cache(
        program,
        table_references,
        reg_columns_start,
        rowid_reg,
        resolver,
        layout,
    )?;

    let result = (|| {
        let result_start_reg = program.alloc_registers(result_columns.len());

        for (i, result_column) in result_columns.iter().enumerate() {
            let reg = result_start_reg + i;
            translate_expr_no_constant_opt(
                program,
                Some(table_references),
                &result_column.expr,
                reg,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
        }

        // Decode array columns in RETURNING results (record blob -> JSON text).
        crate::translate::result_row::emit_array_decode_for_results(
            program,
            result_columns,
            table_references,
            result_start_reg,
            resolver,
        )?;

        if let Some(buf) = returning_buffer {
            // Buffer into ephemeral table instead of yielding directly.
            // All DML completes before any RETURNING rows are yielded to the caller.
            let record_reg = program.alloc_register();
            let eph_rowid_reg = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: crate::vdbe::insn::to_u32(result_start_reg),
                count: crate::vdbe::insn::to_u32(result_columns.len()),
                dest_reg: crate::vdbe::insn::to_u32(record_reg),
                index_name: None,
                affinity_str: None,
            });
            program.emit_insn(Insn::NewRowid {
                cursor: buf.cursor_id,
                rowid_reg: eph_rowid_reg,
                prev_largest_reg: 0,
            });
            program.emit_insn(Insn::Insert {
                cursor: buf.cursor_id,
                key_reg: eph_rowid_reg,
                record_reg,
                flag: InsertFlags::new().is_ephemeral_table_insert(),
                table_name: String::new(),
            });
        } else {
            program.emit_insn(Insn::ResultRow {
                start_reg: result_start_reg,
                count: result_columns.len(),
            });
        }

        Ok(())
    })();

    restore_returning_row_image_in_cache(resolver, cache_state);
    result
}

pub(crate) struct ReturningRowImageCacheState {
    cache_len: usize,
    cache_enabled: bool,
}

pub(crate) fn seed_returning_row_image_in_cache<'a>(
    program: &mut ProgramBuilder,
    table_references: &TableReferences,
    reg_columns_start: usize,
    rowid_reg: usize,
    resolver: &mut Resolver<'a>,
    layout: &ColumnLayout,
) -> Result<ReturningRowImageCacheState> {
    turso_assert!(
        table_references.joined_tables().len() == 1,
        "RETURNING is only used with INSERT, UPDATE, or DELETE statements, which target a single table"
    );
    let table = table_references.joined_tables().first().unwrap();

    let cache_len = resolver.expr_to_reg_cache.len();
    let cache_enabled = resolver.expr_to_reg_cache_enabled;
    resolver.enable_expr_to_reg_cache();
    resolver.cache_expr_reg(
        std::borrow::Cow::Owned(Expr::RowId {
            database: None,
            table: table.internal_id,
        }),
        rowid_reg,
        false,
        None,
    );
    for (i, column) in table.columns().iter().enumerate() {
        let raw_reg = if column.is_rowid_alias() {
            rowid_reg
        } else {
            reg_columns_start + layout.to_reg_offset(i)
        };
        // The write registers hold stored (encoded) values. Produce the
        // user-facing value in a fresh register so RETURNING shows decoded
        // results — this is a no-op for regular columns.
        let decoded_reg = program.alloc_register();
        emit_user_facing_column_value(
            program,
            raw_reg,
            decoded_reg,
            column,
            table.table.is_strict(),
            resolver,
        )?;
        let expr = Expr::Column {
            database: None,
            table: table.internal_id,
            column: i,
            is_rowid_alias: column.is_rowid_alias(),
        };
        resolver.cache_scalar_expr_reg(
            std::borrow::Cow::Owned(expr),
            decoded_reg,
            false,
            table_references,
        )?;
    }

    Ok(ReturningRowImageCacheState {
        cache_len,
        cache_enabled,
    })
}

pub(crate) fn restore_returning_row_image_in_cache(
    resolver: &mut Resolver<'_>,
    state: ReturningRowImageCacheState,
) {
    resolver.expr_to_reg_cache.truncate(state.cache_len);
    resolver.expr_to_reg_cache_enabled = state.cache_enabled;
}
