use crate::alloc::TursoIteratorExt;
use crate::sync::Arc;
use crate::{bail_parse_error, turso_assert_eq, turso_assert_ne};
use turso_parser::{
    ast::{self, TableInternalId},
    parser::Parser,
};

use super::{
    index::emit_refill_index,
    schema::{validate_check_expr, SQLITE_TABLEID},
    update::translate_update_for_schema_change,
};
use crate::{
    error::SQLITE_CONSTRAINT_CHECK,
    function::{AlterTableFunc, Func},
    schema::{
        collect_column_dependencies_of_expr, BTreeTable, CheckConstraint, Column, ColumnLayout,
        ForeignKey, Index, Table, EXPR_INDEX_SENTINEL, RESERVED_TABLE_PREFIXES,
    },
    translate::{
        emitter::{emit_check_constraints, gencol::compute_virtual_columns, Resolver},
        expr::{translate_expr, walk_expr, walk_expr_mut, WalkControl},
        plan::{ColumnMask, ColumnUsedMask, OuterQueryReference, TableReferences},
        trigger::create_trigger_to_sql,
    },
    util::{
        check_expr_references_column, escape_sql_string_literal, normalize_ident,
        parse_numeric_literal, rename_identifiers, rewrite_check_expr_table_refs,
        rewrite_trigger_cmd_table_refs, rewrite_view_sql_for_column_rename,
    },
    vdbe::{
        affinity::Affinity,
        builder::{CursorType, DmlColumnContext, ProgramBuilder},
        insn::{to_u32, AddColumnData, CmpInsFlags, Cookie, Insn, RegisterOrLiteral},
    },
    vtab::VirtualTable,
    LimboError, Numeric, Result, Value, ValueRef,
};
use either::Either;
use rustc_hash::FxHashSet as HashSet;

fn validate(alter_table: &ast::AlterTableBody, table_name: &str) -> Result<()> {
    // Check if someone is trying to ALTER a system table
    if crate::schema::is_system_table(table_name) {
        crate::bail_parse_error!("table {} may not be modified", table_name);
    }
    if let ast::AlterTableBody::RenameTo(new_table_name) = alter_table {
        let normalized_new_name = normalize_ident(new_table_name.as_str());
        if RESERVED_TABLE_PREFIXES
            .iter()
            .any(|prefix| normalized_new_name.starts_with(prefix))
        {
            crate::bail_parse_error!("Object name reserved for internal use: {}", new_table_name);
        }
    }

    Ok(())
}

#[derive(Clone)]
struct SchemaTriggerEntry {
    database_id: usize,
    trigger: Arc<crate::schema::Trigger>,
}

fn schema_table_name_for_db(resolver: &Resolver, database_id: usize) -> String {
    if database_id == crate::MAIN_DB_ID {
        SQLITE_TABLEID.to_string()
    } else if database_id == crate::TEMP_DB_ID {
        format!("temp.{SQLITE_TABLEID}")
    } else {
        let db_name = resolver
            .get_database_name_by_index(database_id)
            .unwrap_or_else(|| "main".to_string());
        format!("{db_name}.{SQLITE_TABLEID}")
    }
}

fn database_uses_mvcc(connection: &Arc<crate::Connection>, database_id: usize) -> bool {
    connection.mv_store_for_db(database_id).is_some()
}

fn collect_triggers_for_alter_target(
    resolver: &Resolver,
    target_database_id: usize,
) -> Vec<SchemaTriggerEntry> {
    let mut database_ids = vec![target_database_id];
    if target_database_id != crate::TEMP_DB_ID {
        database_ids.push(crate::TEMP_DB_ID);
    }

    let mut triggers = Vec::new();
    for database_id in database_ids {
        let schema_triggers = resolver.with_schema(database_id, |schema| {
            schema
                .triggers
                .values()
                .flatten()
                .cloned()
                .collect::<Vec<_>>()
        });
        triggers.extend(
            schema_triggers
                .into_iter()
                .map(|trigger| SchemaTriggerEntry {
                    database_id,
                    trigger,
                }),
        );
    }
    triggers
}

/// Check if an expression is a valid "constant" default for ALTER TABLE ADD COLUMN.
/// SQLite is very strict here - it only allows:
/// - Literals (numbers, strings, blobs, NULL, CURRENT_TIME/DATE/TIMESTAMP)
/// - Bare identifiers (treated as string literals, e.g., `DEFAULT hello` → "hello")
/// - Signed literals (+5, -5, +NULL, -NULL)
/// - Parenthesized versions of the above
///
/// It does NOT allow:
/// - Binary operations like (5 + 3)
/// - Function calls like COALESCE(NULL, 5)
/// - Comparisons, CASE expressions, CAST, etc.
///
/// Note: CURRENT_TIME/DATE/TIMESTAMP are allowed here but will be rejected at
/// runtime if the table has existing rows (see `default_requires_empty_table`).
fn is_strict_constant_default(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Literal(_) => true,
        // Bare identifiers are treated as string literals in DEFAULT clause
        ast::Expr::Id(_) => true,
        ast::Expr::Unary(ast::UnaryOperator::Positive | ast::UnaryOperator::Negative, inner) => {
            // Only allow unary +/- on literals
            matches!(inner.as_ref(), ast::Expr::Literal(_))
        }
        ast::Expr::Parenthesized(exprs) => {
            // Parenthesized expression with a single inner expression
            exprs.len() == 1 && is_strict_constant_default(&exprs[0])
        }
        _ => false,
    }
}

/// Check if a default expression requires the table to be empty (non-deterministic defaults).
/// CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP cannot be used to backfill existing rows.
fn default_requires_empty_table(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Literal(lit) => matches!(
            lit,
            ast::Literal::CurrentDate | ast::Literal::CurrentTime | ast::Literal::CurrentTimestamp
        ),
        ast::Expr::Parenthesized(exprs) => {
            exprs.len() == 1 && default_requires_empty_table(&exprs[0])
        }
        _ => false,
    }
}

/// Rewrite the sqlite_schema row for the AUTOINCREMENT backing table when
/// the parent table is renamed. The backing table is keyed by name
/// `__turso_internal_seq___turso_internal_autoincrement_<table>`, so its
/// row is not touched by the general `Func::AlterTable::RenameTable` sweep
/// (which only matches rows whose `tbl_name` column equals the renamed
/// user table). Without this rewrite, the persistent sqlite_schema row
/// keeps the old backing-table name and SQL, so the next process to open
/// the DB rebuilds the in-memory schema with the stale backing table —
/// and an INSERT into the renamed table fails with "missing implicit
/// sequence for AUTOINCREMENT table".
///
/// Mirrors the existing `emit_rename_sqlite_sequence_entry` helper for
/// the `sqlite_sequence` row; the in-memory rename of `schema.sequences`
/// and `schema.tables` happens in `op_rename_table`.
fn emit_rename_autoincrement_backing_table_entry(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    connection: &Arc<crate::Connection>,
    database_id: usize,
    old_table_name_norm: &str,
    new_table_name_norm: &str,
) {
    use crate::schema::autoincrement_sequence_name;
    use crate::translate::sequence::{sequence_backing_table_name, sequence_backing_table_sql};

    let old_backing_name =
        sequence_backing_table_name(&autoincrement_sequence_name(old_table_name_norm));
    let new_seq_name = autoincrement_sequence_name(new_table_name_norm);
    let new_backing_name = sequence_backing_table_name(&new_seq_name);
    let new_backing_sql = sequence_backing_table_sql(&new_seq_name);

    let Some(sqlite_schema) =
        resolver.with_schema(database_id, |s| s.get_btree_table(SQLITE_TABLEID))
    else {
        return;
    };

    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_schema.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: RegisterOrLiteral::Literal(sqlite_schema.root_page),
        db: database_id,
    });

    let old_backing_reg = program.emit_string8_new_reg(old_backing_name);
    program.mark_last_insn_constant();
    let new_backing_reg = program.emit_string8_new_reg(new_backing_name);
    program.mark_last_insn_constant();
    let new_sql_reg = program.emit_string8_new_reg(new_backing_sql);
    program.mark_last_insn_constant();

    program.cursor_loop(cursor_id, |program, rowid| {
        let name_reg = program.alloc_register();
        program.emit_column_or_rowid(cursor_id, 1, name_reg);

        let continue_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: name_reg,
            rhs: old_backing_reg,
            target_pc: continue_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });

        // Preserve type (col 0) and rootpage (col 3); overwrite name/tbl_name
        // (cols 1, 2) and sql (col 4). The backing table is INTERNAL — its
        // sql is regenerated from `sequence_backing_table_sql(new_seq_name)`
        // and never altered by user DDL, so we don't need to AST-rewrite it.
        let rec_start = program.alloc_registers(5);
        program.emit_column_or_rowid(cursor_id, 0, rec_start); // type
        program.emit_insn(Insn::Copy {
            src_reg: new_backing_reg,
            dst_reg: rec_start + 1, // name
            extra_amount: 0,
        });
        program.emit_insn(Insn::Copy {
            src_reg: new_backing_reg,
            dst_reg: rec_start + 2, // tbl_name
            extra_amount: 0,
        });
        program.emit_column_or_rowid(cursor_id, 3, rec_start + 3); // rootpage
        program.emit_insn(Insn::Copy {
            src_reg: new_sql_reg,
            dst_reg: rec_start + 4, // sql
            extra_amount: 0,
        });

        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(rec_start),
            count: to_u32(5),
            dest_reg: to_u32(record_reg),
            index_name: None,
            affinity_str: None,
        });

        // MVCC: end the old version before writing the new one
        // (Hekaton-style UPDATE = DELETE + INSERT), mirroring the
        // pattern used by `emit_rename_sqlite_sequence_entry` and the
        // main-table rewrite cursor_loop.
        if database_uses_mvcc(connection, database_id) {
            program.emit_insn(Insn::Delete {
                cursor_id,
                table_name: SQLITE_TABLEID.to_string(),
                is_part_of_update: true,
            });
        }

        program.emit_insn(Insn::Insert {
            cursor: cursor_id,
            key_reg: rowid,
            record_reg,
            flag: crate::vdbe::insn::InsertFlags(0),
            table_name: SQLITE_TABLEID.to_string(),
        });

        program.preassign_label_to_next_insn(continue_label);
    });
}

fn emit_rename_sqlite_sequence_entry(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    connection: &Arc<crate::Connection>,
    database_id: usize,
    old_table_name_norm: &str,
    new_table_name_norm: &str,
) {
    let Some(sqlite_sequence) = resolver.with_schema(database_id, |s| {
        s.get_btree_table(crate::schema::SQLITE_SEQUENCE_TABLE_NAME)
    }) else {
        return;
    };

    let seq_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_sequence.clone()));
    let sequence_name_reg = program.alloc_register();
    let sequence_value_reg = program.alloc_register();
    let row_name_to_replace_reg = program.emit_string8_new_reg(old_table_name_norm.to_string());
    program.mark_last_insn_constant();
    let replacement_row_name_reg = program.emit_string8_new_reg(new_table_name_norm.to_string());
    program.mark_last_insn_constant();

    let affinity_str = sqlite_sequence
        .columns()
        .iter()
        .map(|col| col.affinity().aff_mask())
        .collect::<String>();

    program.emit_insn(Insn::OpenWrite {
        cursor_id: seq_cursor_id,
        root_page: RegisterOrLiteral::Literal(sqlite_sequence.root_page),
        db: database_id,
    });

    program.cursor_loop(seq_cursor_id, |program, rowid| {
        program.emit_column_or_rowid(seq_cursor_id, 0, sequence_name_reg);

        let continue_loop_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: sequence_name_reg,
            rhs: row_name_to_replace_reg,
            target_pc: continue_loop_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });

        program.emit_column_or_rowid(seq_cursor_id, 1, sequence_value_reg);

        let record_start_reg = program.alloc_registers(2);
        let record_reg = program.alloc_register();

        program.emit_insn(Insn::Copy {
            src_reg: replacement_row_name_reg,
            dst_reg: record_start_reg,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Copy {
            src_reg: sequence_value_reg,
            dst_reg: record_start_reg + 1,
            extra_amount: 0,
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(record_start_reg),
            count: to_u32(2),
            dest_reg: to_u32(record_reg),
            index_name: None,
            affinity_str: Some(affinity_str.clone()),
        });

        // In MVCC mode, we need to delete before insert to properly
        // end the old version (Hekaton-style UPDATE = DELETE + INSERT)
        if database_uses_mvcc(connection, database_id) {
            program.emit_insn(Insn::Delete {
                cursor_id: seq_cursor_id,
                table_name: crate::schema::SQLITE_SEQUENCE_TABLE_NAME.to_string(),
                is_part_of_update: true,
            });
        }

        program.emit_insn(Insn::Insert {
            cursor: seq_cursor_id,
            key_reg: rowid,
            record_reg,
            flag: crate::vdbe::insn::InsertFlags(0),
            table_name: crate::schema::SQLITE_SEQUENCE_TABLE_NAME.to_string(),
        });

        program.preassign_label_to_next_insn(continue_loop_label);
    });
}

fn emit_delete_sqlite_sequence_entry(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    table_name_norm: &str,
) {
    let Some(sqlite_sequence) = resolver.with_schema(database_id, |s| {
        s.get_btree_table(crate::schema::SQLITE_SEQUENCE_TABLE_NAME)
    }) else {
        return;
    };

    let seq_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_sequence.clone()));
    let sequence_name_reg = program.alloc_register();
    let row_name_to_delete_reg = program.emit_string8_new_reg(table_name_norm.to_string());
    program.mark_last_insn_constant();

    program.emit_insn(Insn::OpenWrite {
        cursor_id: seq_cursor_id,
        root_page: RegisterOrLiteral::Literal(sqlite_sequence.root_page),
        db: database_id,
    });

    program.cursor_loop(seq_cursor_id, |program, _rowid| {
        program.emit_column_or_rowid(seq_cursor_id, 0, sequence_name_reg);

        let continue_loop_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: sequence_name_reg,
            rhs: row_name_to_delete_reg,
            target_pc: continue_loop_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });

        program.emit_insn(Insn::Delete {
            cursor_id: seq_cursor_id,
            table_name: crate::schema::SQLITE_SEQUENCE_TABLE_NAME.to_string(),
            is_part_of_update: false,
        });

        program.preassign_label_to_next_insn(continue_loop_label);
    });
}

pub(crate) fn literal_default_value(literal: &ast::Literal) -> Result<Value> {
    match literal {
        ast::Literal::Numeric(val) => parse_numeric_literal(val),
        ast::Literal::String(s) => Ok(Value::from_text(crate::translate::expr::sanitize_string(s))),
        ast::Literal::Blob(s) => Ok(Value::Blob(
            ast::blob_literal_hex(s)
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| {
                    let hex_byte = std::str::from_utf8(pair).expect("parser validated hex string");
                    u8::from_str_radix(hex_byte, 16).expect("parser validated hex digit")
                })
                .try_collect()?,
        )),
        ast::Literal::Null => Ok(Value::Null),
        ast::Literal::True => Ok(Value::from_i64(1)),
        ast::Literal::False => Ok(Value::from_i64(0)),
        ast::Literal::CurrentDate => Ok(Value::from_text("CURRENT_DATE")),
        ast::Literal::CurrentTime => Ok(Value::from_text("CURRENT_TIME")),
        ast::Literal::CurrentTimestamp => Ok(Value::from_text("CURRENT_TIMESTAMP")),
        ast::Literal::Keyword(_) => Err(LimboError::ParseError(
            "Cannot add a column with non-constant default".to_string(),
        )),
    }
}

pub(crate) fn eval_constant_default_value(expr: &ast::Expr) -> Result<Value> {
    match expr {
        ast::Expr::Literal(literal) => literal_default_value(literal),
        ast::Expr::Id(name) => Ok(Value::from_text(name.as_str().to_string())),
        ast::Expr::Unary(op, inner) => {
            let value = eval_constant_default_value(inner)?;
            match (op, value) {
                (ast::UnaryOperator::Positive, value) => Ok(value),
                (ast::UnaryOperator::Negative, Value::Numeric(Numeric::Integer(i))) => {
                    // Hex literals like 0x8000000000000000 parse to i64::MIN, whose
                    // negation does not fit in an i64. Match SQLite's valueFromExpr,
                    // which promotes -(i64::MIN) to the REAL value 2^63.
                    match i.checked_neg() {
                        Some(negated) => Ok(Value::from_i64(negated)),
                        None => Ok(Value::from_f64(-(i64::MIN as f64))),
                    }
                }
                (ast::UnaryOperator::Negative, Value::Numeric(Numeric::Float(f))) => {
                    Ok(Value::from_f64(-f64::from(f)))
                }
                (ast::UnaryOperator::Negative, Value::Null) => Ok(Value::Null),
                (_, value) => Ok(value),
            }
        }
        ast::Expr::Parenthesized(exprs) => {
            if exprs.len() == 1 {
                eval_constant_default_value(&exprs[0])
            } else {
                Err(LimboError::ParseError(
                    "Cannot add a column with non-constant default".to_string(),
                ))
            }
        }
        _ => Err(LimboError::ParseError(
            "Cannot add a column with non-constant default".to_string(),
        )),
    }
}

fn apply_affinity_to_value(value: &mut Value, affinity: Affinity) {
    if let Some(converted) = affinity.convert(value) {
        *value = match converted {
            Either::Left(ValueRef::Numeric(numeric)) => Value::from(numeric),
            Either::Left(_) => {
                unreachable!("affinity conversion returned an unexpected borrowed value")
            }
            Either::Right(val) => val,
        };
    }
}

fn strict_default_type_mismatch(column: &Column) -> Result<bool> {
    let Some(default_expr) = column.default.as_ref() else {
        return Ok(false);
    };

    // Non-constant defaults cannot be evaluated here; they are only legal on
    // an empty table (enforced at runtime), where no backfill value is needed.
    if !is_strict_constant_default(default_expr) {
        return Ok(false);
    }

    let mut value = eval_constant_default_value(default_expr)?;
    if matches!(value, Value::Null) {
        return Ok(false);
    }

    apply_affinity_to_value(&mut value, column.affinity());

    let ty = column.ty_str.as_str();
    if ty.eq_ignore_ascii_case("ANY") {
        return Ok(false);
    };

    let ok = if ty.eq_ignore_ascii_case("INT") || ty.eq_ignore_ascii_case("INTEGER") {
        match value {
            Value::Numeric(Numeric::Integer(_)) => true,
            Value::Numeric(Numeric::Float(f)) => {
                let f = f64::from(f);
                let i = f as i64;
                (i as f64) == f
            }
            _ => false,
        }
    } else if ty.eq_ignore_ascii_case("REAL") {
        matches!(value, Value::Numeric(Numeric::Float(_)))
    } else if ty.eq_ignore_ascii_case("TEXT") {
        matches!(value, Value::Text(_))
    } else if ty.eq_ignore_ascii_case("BLOB") {
        matches!(value, Value::Blob(_))
    } else {
        true
    };

    Ok(!ok)
}

fn emit_add_column_default_type_validation(
    program: &mut ProgramBuilder,
    original_btree: &Arc<BTreeTable>,
    database_id: usize,
) -> Result<()> {
    let check_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(original_btree.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: check_cursor_id,
        root_page: original_btree.root_page,
        db: database_id,
    });

    let skip_check_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: check_cursor_id,
        pc_if_empty: skip_check_label,
    });

    program.emit_insn(Insn::Halt {
        err_code: 1,
        description: "type mismatch on DEFAULT".to_string(),
        on_error: None,
        description_reg: None,
    });

    program.preassign_label_to_next_insn(skip_check_label);
    Ok(())
}

/// Validate NOT NULL and CHECK constraints for a new virtual generated column
/// by scanning each row and computing the expression.
#[allow(clippy::too_many_arguments)]
fn emit_add_virtual_column_validation(
    program: &mut ProgramBuilder,
    table: &BTreeTable,
    column: &Column,
    constraints: &[ast::NamedColumnConstraint],
    resolver: &Resolver,
    connection: &Arc<crate::Connection>,
    database_id: usize,
) -> Result<()> {
    let has_notnull = column.notnull();
    let check_constraints: Vec<CheckConstraint> = constraints
        .iter()
        .filter_map(|c| {
            if let ast::ColumnConstraint::Check { expr, source } = &c.constraint {
                Some(CheckConstraint::new(
                    c.name.as_ref(),
                    expr,
                    source.as_deref(),
                    column.name.as_deref(),
                ))
            } else {
                None
            }
        })
        .collect();

    if !has_notnull && check_constraints.is_empty() {
        return Ok(());
    }

    let mut resolved_table = table.clone();
    resolved_table.prepare_generated_columns()?;
    let new_column_name = column
        .name
        .as_deref()
        .ok_or_else(|| LimboError::ParseError("generated column name is missing".to_string()))?;
    let (new_column_idx, _) = resolved_table.get_column(new_column_name).ok_or_else(|| {
        LimboError::ParseError(format!(
            "new generated column unexpectedly missing from table {}",
            resolved_table.name
        ))
    })?;

    let mut original_table = table.clone();
    original_table
        .columns_mut()
        .retain(|c| !c.is_virtual_generated());
    let original_table = Arc::new(original_table);
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(original_table.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id,
        root_page: original_table.root_page,
        db: database_id,
    });

    let skip_label = program.allocate_label();
    let loop_start = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id,
        pc_if_empty: skip_label,
    });
    program.preassign_label_to_next_insn(loop_start);

    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::RowId {
        cursor_id,
        dest: rowid_reg,
    });

    let layout = resolved_table.column_layout()?;
    let base_dest_reg = program.alloc_registers(layout.column_count());
    for (idx, table_column) in resolved_table.columns().iter().enumerate() {
        if table_column.is_virtual_generated() || table_column.is_rowid_alias() {
            continue;
        }

        program.emit_column_or_rowid(
            cursor_id,
            layout.to_reg_offset(idx),
            layout.to_register(base_dest_reg, idx),
        );
    }

    let dml_ctx =
        DmlColumnContext::layout(resolved_table.columns(), base_dest_reg, rowid_reg, layout);
    let resolved_table_arc = Arc::new(resolved_table.clone());
    compute_virtual_columns(
        program,
        &resolved_table.columns_topo_sort()?,
        &dml_ctx,
        resolver,
        &resolved_table_arc,
    )?;
    let result_reg = dml_ctx.to_column_reg(new_column_idx);

    if !check_constraints.is_empty() {
        let mut check_resolver = resolver.fork();
        let skip_row_label = program.allocate_label();
        emit_check_constraints(
            program,
            &check_constraints,
            &mut check_resolver,
            resolved_table.name.as_str(),
            rowid_reg,
            resolved_table
                .columns()
                .iter()
                .enumerate()
                .filter_map(|(idx, col)| {
                    col.name
                        .as_deref()
                        .map(|name| (name, dml_ctx.to_column_reg(idx)))
                }),
            connection,
            ast::ResolveType::Abort,
            skip_row_label,
            None,
        )?;
    }

    if has_notnull {
        let notnull_passed = program.allocate_label();
        program.emit_insn(Insn::NotNull {
            reg: result_reg,
            target_pc: notnull_passed,
        });
        program.emit_insn(Insn::Halt {
            err_code: 1,
            description: "NOT NULL constraint failed".to_string(),
            on_error: None,
            description_reg: None,
        });
        program.preassign_label_to_next_insn(notnull_passed);
    }

    program.emit_insn(Insn::Next {
        cursor_id,
        pc_if_next: loop_start,
        fullscan: false,
        is_index: false,
    });

    program.preassign_label_to_next_insn(skip_label);
    Ok(())
}

/// Validate CHECK constraints on a newly added column against the column's DEFAULT value.
///
/// When a table has existing rows, the new column gets the DEFAULT value (or NULL).
/// If that value would violate a CHECK constraint, the ALTER TABLE must be rejected.
/// This emits bytecode that:
/// 1. Checks if the table has any rows (Rewind)
/// 2. Evaluates the CHECK expression with the column reference substituted by the DEFAULT
/// 3. Halts with a CHECK constraint error if the result is false
#[allow(clippy::too_many_arguments)]
fn emit_add_column_check_validation(
    program: &mut ProgramBuilder,
    btree: &BTreeTable,
    original_btree: &Arc<BTreeTable>,
    new_column_name: &str,
    column: &Column,
    constraints: &[ast::NamedColumnConstraint],
    resolver: &Resolver,
    database_id: usize,
) -> Result<()> {
    // Determine the effective default value. If no DEFAULT, existing rows get NULL,
    // which always passes CHECK per SQL standard (NULL is not false).
    let default_expr = match &column.default {
        Some(expr) if !crate::util::expr_contains_null(expr) => *expr.clone(),
        _ => return Ok(()),
    };

    // Collect CHECK constraints from column-level constraints + domain CHECKs.
    // Domain CHECKs use `value` as placeholder which must be rewritten to the column name.
    let mut all_checks: Vec<(Option<String>, Option<String>, Box<ast::Expr>)> = constraints
        .iter()
        .filter_map(|c| {
            if let ast::ColumnConstraint::Check { expr, source } = &c.constraint {
                Some((
                    c.name.as_ref().map(|n| n.as_str().to_string()),
                    source.clone(),
                    expr.clone(),
                ))
            } else {
                None
            }
        })
        .collect();

    if let Ok(Some(resolved)) = resolver
        .schema()
        .resolve_type(&column.ty_str, btree.is_strict)
    {
        if resolved.is_domain() {
            for td in &resolved.chain {
                for dc in &td.domain_checks {
                    let rewritten =
                        crate::schema::rewrite_value_to_column(&dc.check, new_column_name);
                    all_checks.push((dc.name.clone(), None, rewritten));
                }
            }
        }
    }

    if all_checks.is_empty() {
        return Ok(());
    }

    let table_name = &btree.name;
    let col_name_lower = normalize_ident(new_column_name);

    // Open the table to check if it has rows.
    let check_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(original_btree.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: check_cursor_id,
        root_page: original_btree.root_page,
        db: database_id,
    });

    let skip_check_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: check_cursor_id,
        pc_if_empty: skip_check_label,
    });

    // Table has rows -- evaluate each CHECK constraint with the default value substituted.
    for (constraint_name, check_source, check_expr) in &all_checks {
        let mut substituted = *check_expr.clone();

        // Replace references to the new column with the default value expression.
        let _ = walk_expr_mut(
            &mut substituted,
            &mut |e: &mut ast::Expr| -> Result<WalkControl> {
                match e {
                    ast::Expr::Id(name) if normalize_ident(name.as_str()) == col_name_lower => {
                        *e = default_expr.clone();
                        Ok(WalkControl::SkipChildren)
                    }
                    ast::Expr::Qualified(tbl, col)
                        if normalize_ident(tbl.as_str()) == normalize_ident(table_name)
                            && normalize_ident(col.as_str()) == col_name_lower =>
                    {
                        *e = default_expr.clone();
                        Ok(WalkControl::SkipChildren)
                    }
                    _ => Ok(WalkControl::Continue),
                }
            },
        );

        let result_reg = program.alloc_register();
        translate_expr(program, None, &substituted, result_reg, resolver)?;

        // CHECK passes if the result is NULL or non-zero (truthy).
        let check_passed_label = program.allocate_label();

        program.emit_insn(Insn::IsNull {
            reg: result_reg,
            target_pc: check_passed_label,
        });

        program.emit_insn(Insn::If {
            reg: result_reg,
            target_pc: check_passed_label,
            jump_if_null: false,
        });

        // CHECK failed -- halt with constraint error. SQLite reports the
        // constraint name, or the expression's source text as written.
        let name = match (constraint_name, check_source) {
            (Some(name), _) => name.clone(),
            (None, Some(source)) => crate::util::check_source_for_error(source),
            (None, None) => format!("{check_expr}"),
        };
        program.emit_insn(Insn::Halt {
            err_code: SQLITE_CONSTRAINT_CHECK,
            description: name,
            on_error: None,
            description_reg: None,
        });

        program.preassign_label_to_next_insn(check_passed_label);
    }

    program.preassign_label_to_next_insn(skip_check_label);
    Ok(())
}

pub fn translate_alter_table(
    alter: ast::AlterTable,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    input: &str,
) -> Result<()> {
    let ast::AlterTable {
        name: qualified_name,
        body: alter_table,
    } = alter;
    let database_id = resolver.resolve_existing_table_database_id_qualified(&qualified_name)?;
    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;
    program.begin_write_operation()?;
    let table_name = qualified_name.name.as_str();
    // For attached databases, qualify sqlite_schema with the database name
    // so that the UPDATE targets the correct database's schema table.
    let qualified_schema_table = schema_table_name_for_db(resolver, database_id);
    let schema_version = resolver.with_schema(database_id, |s| s.schema_version);
    validate(&alter_table, table_name)?;

    let table_indexes = resolver.with_schema(database_id, |s| {
        s.get_indices(table_name).cloned().collect::<Vec<_>>()
    });

    let Some(table) = resolver.with_schema(database_id, |s| s.get_table(table_name)) else {
        return Err(LimboError::ParseError(format!(
            "no such table: {}",
            crate::util::table_name_for_error(&qualified_name)
        )));
    };
    if let Some(tbl) = table.virtual_table() {
        if let ast::AlterTableBody::RenameTo(new_name) = &alter_table {
            let new_name_norm = normalize_ident(new_name.as_str());
            return translate_rename_virtual_table(
                program,
                tbl,
                table_name,
                new_name_norm,
                resolver,
                connection,
                database_id,
            );
        }
    }
    let Some(original_btree) = table.btree() else {
        crate::bail_parse_error!("ALTER TABLE is only supported for BTree tables");
    };

    // Check if this table has dependent materialized views
    let dependent_views = resolver.with_schema(database_id, |s| {
        s.get_dependent_materialized_views(table_name)
    });
    if !dependent_views.is_empty() {
        return Err(LimboError::ParseError(format!(
            "cannot alter table \"{table_name}\": it has dependent materialized view(s): {}",
            dependent_views.join(", ")
        )));
    }

    let mut btree = (*original_btree).clone();

    match alter_table {
        ast::AlterTableBody::DropColumn(column_name) => {
            let column_name = column_name.as_str();

            // Tables always have at least one column.
            turso_assert_ne!(btree.columns().len(), 0);

            if btree.columns().len() == 1 {
                return Err(LimboError::ParseError(format!(
                    "cannot drop column \"{column_name}\": no other columns exist"
                )));
            }

            let (dropped_index, column) = btree.get_column(column_name).ok_or_else(|| {
                LimboError::ParseError(format!("no such column: \"{column_name}\""))
            })?;

            // Column cannot be dropped if:
            // The column is a PRIMARY KEY or part of one.
            // The column has a UNIQUE constraint.
            // The column is indexed.
            // The column is referenced in an expression index.
            // The column is named in the WHERE clause of a partial index.
            // The column is named in a table or column CHECK constraint not associated with the column being dropped.
            // The column is used in a foreign key constraint.
            // The column is used in the expression of a generated column.
            // The column appears in a trigger or view.

            if column.primary_key() {
                return Err(LimboError::ParseError(format!(
                    "cannot drop column \"{column_name}\": PRIMARY KEY"
                )));
            }

            if column.unique()
                || btree.unique_sets.iter().any(|set| {
                    set.columns
                        .iter()
                        .any(|c| c.name == normalize_ident(column_name))
                })
            {
                return Err(LimboError::ParseError(format!(
                    "cannot drop column \"{column_name}\": UNIQUE"
                )));
            }

            let col_normalized = normalize_ident(column_name);
            for index in table_indexes.iter() {
                // Referenced in regular index
                let maybe_indexed_col = index
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, col)| col.pos_in_table == dropped_index);
                if let Some((pos_in_index, indexed_col)) = maybe_indexed_col {
                    return Err(LimboError::ParseError(format!(
                        "cannot drop column \"{column_name}\": it is referenced in the index {}; position in index is {pos_in_index}, position in table is {}",
                        index.name, indexed_col.pos_in_table
                    )));
                }
                // Referenced in expression index
                for idx_col in &index.columns {
                    if let Some(expr) = &idx_col.expr {
                        if check_expr_references_column(expr, &col_normalized) {
                            return Err(LimboError::ParseError(format!(
                                "error in index {} after drop column: no such column: {column_name}",
                                index.name
                            )));
                        }
                    }
                }
                // Referenced in partial index
                if index.where_clause.is_some() {
                    let mut table_references = TableReferences::new(
                        vec![],
                        vec![OuterQueryReference {
                            identifier: table_name.to_string(),
                            internal_id: TableInternalId::from(0),
                            table: Table::BTree(Arc::new(btree.clone())),
                            using_dedup_hidden_cols: ColumnMask::default(),
                            col_used_mask: ColumnUsedMask::default(),
                            cte_select: None,
                            cte_explicit_columns: vec![],
                            cte_id: None,
                            cte_definition_only: false,
                            rowid_referenced: false,
                            scope_depth: 0,
                        }],
                    );
                    let where_copy = index
                        .bind_where_expr(Some(&mut table_references), resolver)?
                        .ok_or_else(|| {
                            LimboError::ParseError(
                                "index where clause unexpectedly missing".to_string(),
                            )
                        })?;
                    let mut column_referenced = false;
                    walk_expr(
                        &where_copy,
                        &mut |e: &ast::Expr| -> crate::Result<WalkControl> {
                            if let ast::Expr::Column {
                                table,
                                column: column_index,
                                ..
                            } = e
                            {
                                if *table == TableInternalId::from(0)
                                    && *column_index == dropped_index
                                {
                                    column_referenced = true;
                                    return Ok(WalkControl::SkipChildren);
                                }
                            }
                            Ok(WalkControl::Continue)
                        },
                    )?;
                    if column_referenced {
                        return Err(LimboError::ParseError(format!(
                            "cannot drop column \"{column_name}\": indexed"
                        )));
                    }
                }
            }

            // Handle CHECK constraints:
            // - Column-level CHECK constraints for the dropped column are silently removed
            // - Table-level CHECK constraints referencing the dropped column cause an error
            for check in &btree.check_constraints {
                if check.column.is_some() {
                    // Column-level constraint: will be removed below
                    continue;
                }
                // Table-level constraint: check if it references the dropped column
                if check_expr_references_column(&check.expr, &col_normalized) {
                    return Err(LimboError::ParseError(format!(
                        "error in table {table_name} after drop column: no such column: {column_name}"
                    )));
                }
            }
            // Remove column-level CHECK constraints for the dropped column
            btree.check_constraints.retain(|c| {
                c.column
                    .as_ref()
                    .is_none_or(|col| normalize_ident(col) != normalize_ident(column_name))
            });

            // Check if column is used in a foreign key constraint (child side)
            // SQLite does not allow dropping a column that is part of a FK constraint
            let column_name_norm = normalize_ident(column_name);
            for fk in &btree.foreign_keys {
                if fk.child_columns.contains(&column_name_norm) {
                    return Err(LimboError::ParseError(format!(
                        "error in table {table_name} after drop column: unknown column \"{column_name}\" in foreign key definition"
                    )));
                }
            }

            // Must have at least one non-generated column after the drop
            {
                let remaining_non_generated = btree
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(|(idx, col)| *idx != dropped_index && !col.is_generated())
                    .count();
                if remaining_non_generated == 0 {
                    return Err(LimboError::ParseError(format!(
                        "error in table {table_name} after drop column: must have at least one non-generated column"
                    )));
                }
            }

            // Check if any virtual column depends on the dropped column
            {
                let affected = btree.columns_affected_by_update([dropped_index])?;
                for idx in &affected {
                    if idx != dropped_index && btree.columns()[idx].is_virtual_generated() {
                        return Err(LimboError::ParseError(format!(
                            "error in table {table_name} after drop column: no such column: {column_name}"
                        )));
                    }
                }
            }

            // References in VIEWs are checked in the VDBE layer op_drop_column instruction.

            // Like SQLite, re-validate ALL triggers after the drop. Any trigger whose
            // body references a nonexistent column (in the altered table or any other
            // table) causes the DROP to fail. This catches cross-table references to
            // the dropped column as well as pre-existing errors that would surface at
            // trigger execution time.
            let post_drop_btree = {
                let mut t = btree.clone();
                t.columns_mut().remove(dropped_index);
                t
            };
            let table_name_norm = normalize_ident(table_name);
            for trigger_entry in collect_triggers_for_alter_target(resolver, database_id) {
                if let Some(missing_table) = validate_trigger_table_refs_after_rename(
                    &trigger_entry.trigger,
                    &table_name_norm,
                    resolver,
                    trigger_entry.database_id,
                    database_id,
                )? {
                    return Err(LimboError::ParseError(format!(
                        "error in trigger {}: no such table: {}",
                        trigger_entry.trigger.name, missing_table
                    )));
                }
                if let Some(bad_col) = validate_trigger_columns_after_drop(
                    &trigger_entry.trigger,
                    &table_name_norm,
                    &btree,
                    resolver,
                    trigger_entry.database_id,
                    database_id,
                )? {
                    return Err(LimboError::ParseError(format!(
                        "error in trigger {}: no such column: {}",
                        trigger_entry.trigger.name, bad_col
                    )));
                }
                if let Some(bad_col) = validate_trigger_columns_after_drop(
                    &trigger_entry.trigger,
                    &table_name_norm,
                    &post_drop_btree,
                    resolver,
                    trigger_entry.database_id,
                    database_id,
                )? {
                    return Err(LimboError::ParseError(format!(
                        "error in trigger {} after drop column: no such column: {}",
                        trigger_entry.trigger.name, bad_col
                    )));
                }
            }

            btree.columns_mut().remove(dropped_index);

            let sql = escape_sql_string_literal(&btree.to_sql());

            let escaped_table_name = escape_sql_string_literal(table_name);
            let stmt = format!(
                r#"
                    UPDATE {qualified_schema_table}
                    SET sql = '{sql}'
                    WHERE name = '{escaped_table_name}' COLLATE NOCASE AND type = 'table'
                "#,
            );

            let mut parser = Parser::new(stmt.as_bytes());
            let cmd = parser.next_cmd().map_err(|e| {
                LimboError::ParseError(format!("failed to parse generated UPDATE statement: {e}"))
            })?;
            let Some(ast::Cmd::Stmt(ast::Stmt::Update(update))) = cmd else {
                return Err(LimboError::ParseError(
                    "generated UPDATE statement did not parse as expected".to_string(),
                ));
            };

            let rewrite_rows = if !original_btree.columns()[dropped_index].is_virtual_generated() {
                let source_column_by_schema_idx: Vec<Option<usize>> = btree
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(new_idx, column)| {
                        if column.is_virtual_generated() {
                            None
                        } else if new_idx < dropped_index {
                            Some(new_idx)
                        } else {
                            Some(new_idx + 1)
                        }
                    })
                    .collect();
                Some((source_column_by_schema_idx, btree.column_layout()?))
            } else {
                None
            };

            translate_update_for_schema_change(
                update,
                resolver,
                program,
                connection,
                input,
                |program| {
                    if let Some((source_column_by_schema_idx, layout)) = &rewrite_rows {
                        emit_rewrite_table_rows(
                            program,
                            original_btree.clone(),
                            &btree,
                            source_column_by_schema_idx,
                            layout,
                            connection,
                            database_id,
                        );
                    }

                    program.emit_insn(Insn::SetCookie {
                        db: database_id,
                        cookie: Cookie::SchemaVersion,
                        value: schema_version as i32 + 1,
                        p5: 0,
                    });

                    program.emit_insn(Insn::DropColumn {
                        db: database_id,
                        table: btree.name.clone(),
                        column_index: dropped_index,
                    })
                },
            )?
        }
        ast::AlterTableBody::AddColumn(col_def) => {
            let is_generated = col_def
                .constraints
                .iter()
                .any(|c| matches!(c.constraint, ast::ColumnConstraint::Generated { .. }));
            let is_stored = col_def.constraints.iter().any(|c| {
                matches!(
                    c.constraint,
                    ast::ColumnConstraint::Generated {
                        typ: Some(ast::GeneratedColumnType::Stored),
                        ..
                    }
                )
            });
            if is_stored {
                return Err(LimboError::ParseError(
                    "cannot add a STORED column".to_string(),
                ));
            }
            if is_generated && !connection.experimental_generated_columns_enabled() {
                bail_parse_error!(
                    "Generated columns require --experimental-generated-columns flag"
                );
            }
            if is_generated {
                for c in &col_def.constraints {
                    if let ast::ColumnConstraint::Generated { expr, .. } = &c.constraint {
                        crate::schema::validate_generated_expr(expr)?;
                    }
                }
            }
            let constraints = col_def.constraints.clone();
            let mut column = Column::try_from(&col_def)?;

            if btree.columns().len() >= crate::types::MAX_COLUMN {
                return Err(LimboError::ParseError(format!(
                    "too many columns on {}",
                    btree.name
                )));
            }

            let new_column_name = column.name.clone().ok_or_else(|| {
                LimboError::ParseError(
                    "column name is missing in ALTER TABLE ADD COLUMN".to_string(),
                )
            })?;
            if btree.get_column(&new_column_name).is_some() {
                return Err(LimboError::ParseError(
                    "duplicate column name: ".to_string() + &new_column_name,
                ));
            }

            let default_type_mismatch;
            {
                let ty = column.ty_str.as_str();
                if btree.is_strict && ty.is_empty() {
                    return Err(LimboError::ParseError(format!(
                        "missing datatype for {table_name}.{new_column_name}"
                    )));
                }
                let is_builtin = ty.is_empty()
                    || ty.eq_ignore_ascii_case("INT")
                    || ty.eq_ignore_ascii_case("INTEGER")
                    || ty.eq_ignore_ascii_case("REAL")
                    || ty.eq_ignore_ascii_case("TEXT")
                    || ty.eq_ignore_ascii_case("BLOB")
                    || ty.eq_ignore_ascii_case("ANY");
                // Domain types require STRICT tables because domain constraints
                // (CHECK, NOT NULL, DEFAULT) are only enforced on STRICT tables.
                if !is_builtin && !btree.is_strict {
                    let type_def = resolver
                        .schema()
                        .get_type_def_unchecked(&normalize_ident(ty));
                    if let Some(td) = type_def {
                        if td.is_domain {
                            return Err(LimboError::ParseError(format!(
                                "domain type columns require STRICT tables: {table_name}.{new_column_name}"
                            )));
                        }
                    }
                }

                if !is_builtin && btree.is_strict {
                    let type_def = resolver
                        .schema()
                        .get_type_def_unchecked(&normalize_ident(ty));
                    if type_def.is_none() {
                        return Err(LimboError::ParseError(format!(
                            "unknown datatype for {table_name}.{new_column_name}: \"{ty}\""
                        )));
                    }
                }

                default_type_mismatch = strict_default_type_mismatch(&column)?;
            }

            // If a column has no explicit DEFAULT but its custom type defines
            // one, propagate the type-level DEFAULT to the column so that
            // existing rows get the type default instead of NULL.
            if column.default.is_none() {
                if let Ok(Some(resolved)) = resolver
                    .schema()
                    .resolve_type(&column.ty_str, btree.is_strict)
                {
                    if let Some(type_default) = resolved.default_expr() {
                        column.default = Some(Box::new(type_default.clone()));
                    }
                }
            }

            // TODO: All quoted ids will be quoted with `[]`, we should store some info from the parsed AST
            btree.columns_mut().push(column.clone());

            // Add foreign key constraints and CHECK constraints to the btree table
            for constraint in &constraints {
                match &constraint.constraint {
                    ast::ColumnConstraint::ForeignKey {
                        clause,
                        defer_clause,
                    } => {
                        if clause.columns.len() > 1 {
                            return Err(LimboError::ParseError(format!(
                                "foreign key on {new_column_name} should reference only one column of table {}",
                                clause.tbl_name.as_str()
                            )));
                        }
                        let decl_order = btree
                            .foreign_keys
                            .iter()
                            .map(|fk| fk.decl_order)
                            .max()
                            .map_or(0, |order| order + 1);
                        let fk = ForeignKey {
                            parent_table: normalize_ident(clause.tbl_name.as_str()),
                            parent_columns: clause
                                .columns
                                .iter()
                                .map(|c| normalize_ident(c.col_name.as_str()))
                                .collect::<Vec<_>>()
                                .into_boxed_slice(),
                            on_delete: clause
                                .args
                                .iter()
                                .find_map(|arg| {
                                    if let ast::RefArg::OnDelete(act) = arg {
                                        Some(*act)
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or(ast::RefAct::NoAction),
                            on_update: clause
                                .args
                                .iter()
                                .find_map(|arg| {
                                    if let ast::RefArg::OnUpdate(act) = arg {
                                        Some(*act)
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or(ast::RefAct::NoAction),
                            child_columns: Box::from([new_column_name.to_string()]),
                            deferred: match defer_clause {
                                Some(d) => {
                                    d.deferrable
                                        && matches!(
                                            d.init_deferred,
                                            Some(ast::InitDeferredPred::InitiallyDeferred)
                                        )
                                }
                                None => false,
                            },
                            decl_order,
                        };
                        btree.foreign_keys.push(Arc::new(fk));
                    }
                    ast::ColumnConstraint::Check { expr, source } => {
                        let column_names: Vec<&str> = btree
                            .columns()
                            .iter()
                            .filter_map(|c| c.name.as_deref())
                            .collect();
                        validate_check_expr(expr, &btree.name, &column_names, resolver)?;
                        btree.check_constraints.push(CheckConstraint::new(
                            constraint.name.as_ref(),
                            expr,
                            source.as_deref(),
                            Some(&new_column_name),
                        ));
                    }
                    _ => {
                        // Other constraints (PRIMARY KEY, NOT NULL, etc.) are handled elsewhere
                    }
                }
            }

            // Propagate domain CHECK and NOT NULL constraints to the newly
            // added column. Without this, columns typed with a domain would
            // silently skip domain-level enforcement after ALTER TABLE.
            resolver.with_schema(database_id, |schema| {
                btree.propagate_domain_constraints(schema)
            })?;
            // Refresh local `column` from btree so that domain NOT NULL is
            // visible to the empty-table check below.
            column = btree.columns().last().unwrap().clone();

            let escaped = escape_sql_string_literal(&btree.to_sql());
            let escaped_table_name = escape_sql_string_literal(table_name);
            let stmt = format!(
                r#"
                    UPDATE {qualified_schema_table}
                    SET sql = '{escaped}'
                    WHERE name = '{escaped_table_name}' COLLATE NOCASE AND type = 'table'
                "#,
            );

            let mut parser = Parser::new(stmt.as_bytes());
            let cmd = parser.next_cmd().map_err(|e| {
                LimboError::ParseError(format!("failed to parse generated UPDATE statement: {e}"))
            })?;
            let Some(ast::Cmd::Stmt(ast::Stmt::Update(update))) = cmd else {
                return Err(LimboError::ParseError(
                    "generated UPDATE statement did not parse as expected".to_string(),
                ));
            };

            if is_generated {
                emit_add_virtual_column_validation(
                    program,
                    &btree,
                    &column,
                    &constraints,
                    resolver,
                    connection,
                    database_id,
                )?;
            } else {
                // Check if we need to verify the table is empty at runtime.
                let type_default = resolver
                    .schema()
                    .resolve_type(&column.ty_str, btree.is_strict)
                    .ok()
                    .flatten()
                    .and_then(|r| r.default_expr().cloned());
                let effective_default = column.default.as_deref().or(type_default.as_ref());
                let needs_notnull_check = column.notnull()
                    && effective_default.is_none_or(crate::util::expr_contains_null);

                // SQLite is very strict about what constitutes a "constant" default
                // for ALTER TABLE ADD COLUMN: only literals, signed literals, and
                // bare identifiers qualify. Anything else — (5 + 3), random(),
                // CURRENT_TIMESTAMP — is permitted only if the table is empty,
                // checked at runtime (mirroring SQLite's sqlite3ErrorIfNotEmpty).
                let needs_nondeterministic_check = column.default.as_ref().is_some_and(|default| {
                    default_requires_empty_table(default) || !is_strict_constant_default(default)
                });

                let (needs_empty_table_check, error_message) = if needs_notnull_check {
                    (true, "Cannot add a NOT NULL column with default value NULL")
                } else if needs_nondeterministic_check {
                    (true, "Cannot add a column with non-constant default")
                } else {
                    (false, "")
                };

                if needs_empty_table_check {
                    let check_cursor_id =
                        program.alloc_cursor_id(CursorType::BTreeTable(original_btree.clone()));
                    program.emit_insn(Insn::OpenRead {
                        cursor_id: check_cursor_id,
                        root_page: original_btree.root_page,
                        db: database_id,
                    });

                    let skip_error_label = program.allocate_label();
                    program.emit_insn(Insn::Rewind {
                        cursor_id: check_cursor_id,
                        pc_if_empty: skip_error_label,
                    });

                    program.emit_insn(Insn::Halt {
                        err_code: 1,
                        description: error_message.to_string(),
                        on_error: None,
                        description_reg: None,
                    });

                    program.preassign_label_to_next_insn(skip_error_label);
                }

                if default_type_mismatch {
                    emit_add_column_default_type_validation(program, &original_btree, database_id)?;
                }

                emit_add_column_check_validation(
                    program,
                    &btree,
                    &original_btree,
                    &new_column_name,
                    &column,
                    &constraints,
                    resolver,
                    database_id,
                )?;
            }

            translate_update_for_schema_change(
                update,
                resolver,
                program,
                connection,
                input,
                |program| {
                    program.emit_insn(Insn::SetCookie {
                        db: database_id,
                        cookie: Cookie::SchemaVersion,
                        value: schema_version as i32 + 1,
                        p5: 0,
                    });
                    program.emit_insn(Insn::AddColumn {
                        data: Box::new(AddColumnData {
                            db: database_id,
                            table: table_name.to_owned(),
                            column,
                            check_constraints: btree.check_constraints.to_vec(),
                            foreign_keys: btree.foreign_keys.to_vec(),
                        }),
                    });
                },
            )?
        }
        ast::AlterTableBody::RenameTo(new_name) => {
            let new_name = new_name.as_str();
            let normalized_old_name = normalize_ident(table_name);
            let normalized_new_name = normalize_ident(new_name);
            let mut temp_triggers_to_rewrite: Vec<(String, String, bool)> = Vec::new();

            // Views whose SQL failed to load are absent from the schema maps
            // but their rows are still in sqlite_schema, so the name is taken.
            let new_name_taken = resolver.with_schema(database_id, |s| {
                s.get_object_type(&normalized_new_name).is_some()
                    || s.broken_views.contains(&normalized_new_name)
                    || s.incompatible_views.contains(&normalized_new_name)
            });
            if new_name_taken {
                return Err(LimboError::ParseError(format!(
                    "there is already another table or index with this name: {new_name}"
                )));
            }

            for trigger_entry in collect_triggers_for_alter_target(resolver, database_id) {
                if let Some(missing_table) = validate_trigger_table_refs_after_rename(
                    &trigger_entry.trigger,
                    &normalized_old_name,
                    resolver,
                    trigger_entry.database_id,
                    database_id,
                )? {
                    return Err(LimboError::ParseError(format!(
                        "error in trigger {}: no such table: {}",
                        trigger_entry.trigger.name, missing_table
                    )));
                }
                if trigger_entry.database_id == crate::TEMP_DB_ID {
                    let temp_trigger_targets_renamed_table =
                        normalize_ident(&trigger_entry.trigger.table_name) == normalized_old_name
                            && match trigger_entry.trigger.target_database_id {
                                Some(target_db) => target_db == database_id,
                                None => resolver.with_schema(crate::TEMP_DB_ID, |schema| {
                                    schema.get_table(&normalized_old_name).is_none()
                                }),
                            };
                    // Pass the renamed database's NAME so the rewrite
                    // only touches triggers whose `tbl_name.db_name`
                    // actually points at the db we are renaming in
                    // (or is unqualified). Otherwise a temp trigger
                    // referencing `aux.t` would be wrongly rewritten
                    // during `ALTER TABLE main.t RENAME TO ...`.
                    let renamed_db_name = resolver
                        .get_database_name_by_index(database_id)
                        .unwrap_or_else(|| "main".to_string());
                    let new_sql = rewrite_trigger_sql_for_table_rename(
                        &trigger_entry.trigger.sql,
                        table_name,
                        new_name,
                        &renamed_db_name,
                    )?;
                    if new_sql != trigger_entry.trigger.sql {
                        temp_triggers_to_rewrite.push((
                            trigger_entry.trigger.name.clone(),
                            new_sql,
                            temp_trigger_targets_renamed_table,
                        ));
                    }
                }
            }

            let temp_schema_version = if !temp_triggers_to_rewrite.is_empty() {
                let schema_cookie = resolver.with_schema(crate::TEMP_DB_ID, |s| s.schema_version);
                program.begin_write_on_database(crate::TEMP_DB_ID, schema_cookie)?;
                Some(schema_cookie)
            } else {
                None
            };

            let sqlite_schema = resolver
                .with_schema(database_id, |s| s.get_btree_table(SQLITE_TABLEID))
                .ok_or_else(|| {
                    LimboError::ParseError("sqlite_schema table not found in schema".to_string())
                })?;

            let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_schema.clone()));

            program.emit_insn(Insn::OpenWrite {
                cursor_id,
                root_page: RegisterOrLiteral::Literal(sqlite_schema.root_page),
                db: database_id,
            });

            program.cursor_loop(cursor_id, |program, rowid| {
                let sqlite_schema_column_len = sqlite_schema.columns().len();
                turso_assert_eq!(sqlite_schema_column_len, 5);

                let first_column = program.alloc_registers(sqlite_schema_column_len);

                for i in 0..sqlite_schema_column_len {
                    program.emit_column_or_rowid(cursor_id, i, first_column + i);
                }

                program.emit_string8_new_reg(table_name.to_string());
                program.mark_last_insn_constant();

                program.emit_string8_new_reg(new_name.to_string());
                program.mark_last_insn_constant();

                let out = program.alloc_registers(5);

                program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg: first_column,
                    dest: out,
                    func: crate::function::FuncCtx {
                        func: Func::AlterTable(AlterTableFunc::RenameTable),
                        arg_count: 7,
                    },
                });

                let record = program.alloc_register();

                program.emit_insn(Insn::MakeRecord {
                    start_reg: to_u32(out),
                    count: to_u32(sqlite_schema_column_len),
                    dest_reg: to_u32(record),
                    index_name: None,
                    affinity_str: None,
                });

                // In MVCC mode, we need to delete before insert to properly
                // end the old version (Hekaton-style UPDATE = DELETE + INSERT)
                if database_uses_mvcc(connection, database_id) {
                    program.emit_insn(Insn::Delete {
                        cursor_id,
                        table_name: SQLITE_TABLEID.to_string(),
                        is_part_of_update: true,
                    });
                }

                program.emit_insn(Insn::Insert {
                    cursor: cursor_id,
                    key_reg: rowid,
                    record_reg: record,
                    flag: crate::vdbe::insn::InsertFlags(0),
                    table_name: table_name.to_string(),
                });
            });

            emit_rename_sqlite_sequence_entry(
                program,
                resolver,
                connection,
                database_id,
                &normalized_old_name,
                &normalized_new_name,
            );

            // For AUTOINCREMENT tables, also rewrite the persistent
            // sqlite_schema row for the hidden backing table. The in-memory
            // rename happens in `op_rename_table`; without this bytecode the
            // schema state on disk and in memory would diverge across a
            // reopen. Cheap no-op for non-AUTOINCREMENT tables: we look up
            // the implicit sequence via `autoincrement_sequence_name` and
            // only emit the sweep when the descriptor is present.
            let has_implicit_seq = resolver.with_schema(database_id, |s| {
                s.get_sequence(&crate::schema::autoincrement_sequence_name(
                    &normalized_old_name,
                ))
                .is_some()
            });
            if has_implicit_seq {
                emit_rename_autoincrement_backing_table_entry(
                    program,
                    resolver,
                    connection,
                    database_id,
                    &normalized_old_name,
                    &normalized_new_name,
                );
            }

            for (trigger_name, new_sql, renames_target) in temp_triggers_to_rewrite {
                let escaped_sql = escape_sql_string_literal(&new_sql);
                let escaped_trigger_name = escape_sql_string_literal(&trigger_name);
                let qualified_schema_table = schema_table_name_for_db(resolver, crate::TEMP_DB_ID);
                let tbl_name_update = if renames_target {
                    let escaped_new_name = escape_sql_string_literal(new_name);
                    format!(", tbl_name = '{escaped_new_name}'")
                } else {
                    String::new()
                };
                let update_stmt = format!(
                    r#"
                        UPDATE {qualified_schema_table}
                        SET sql = '{escaped_sql}'{tbl_name_update}
                        WHERE name = '{escaped_trigger_name}' COLLATE NOCASE AND type = 'trigger'
                    "#,
                );

                let mut parser = Parser::new(update_stmt.as_bytes());
                let cmd = parser.next_cmd().map_err(|e| {
                    LimboError::ParseError(format!(
                        "failed to parse trigger update SQL for {trigger_name}: {e}"
                    ))
                })?;
                let Some(ast::Cmd::Stmt(ast::Stmt::Update(update))) = cmd else {
                    return Err(LimboError::ParseError(format!(
                        "failed to parse trigger update SQL for {trigger_name}",
                    )));
                };

                translate_update_for_schema_change(
                    update,
                    resolver,
                    program,
                    connection,
                    input,
                    |_program| {},
                )?;
            }

            if let Some(temp_schema_version) = temp_schema_version {
                program.emit_insn(Insn::SetCookie {
                    db: crate::TEMP_DB_ID,
                    cookie: Cookie::SchemaVersion,
                    value: temp_schema_version as i32 + 1,
                    p5: 0,
                });
            }

            program.emit_insn(Insn::SetCookie {
                db: database_id,
                cookie: Cookie::SchemaVersion,
                value: schema_version as i32 + 1,
                p5: 0,
            });

            program.emit_insn(Insn::RenameTable {
                db: database_id,
                from: table_name.to_owned(),
                to: new_name.to_owned(),
            });
        }
        body @ (ast::AlterTableBody::AlterColumn { .. }
        | ast::AlterTableBody::RenameColumn { .. }) => {
            let from;
            let definition;
            let col_name;
            let rename;

            match body {
                ast::AlterTableBody::AlterColumn { old, new } => {
                    from = old;
                    definition = new;
                    col_name = definition.col_name.clone();
                    rename = false;
                }
                ast::AlterTableBody::RenameColumn { old, new } => {
                    from = old;
                    definition = ast::ColumnDefinition {
                        col_name: new.clone(),
                        col_type: None,
                        constraints: vec![],
                    };
                    col_name = new;
                    rename = true;
                }
                _ => unreachable!(),
            }

            let from = from.as_str();
            let col_name = col_name.as_str();

            let Some((column_index, _)) = btree.get_column(from) else {
                return Err(LimboError::ParseError(format!(
                    "no such column: \"{from}\""
                )));
            };

            if btree.get_column(col_name).is_some() {
                return Err(LimboError::ParseError(format!(
                    "duplicate column name: \"{col_name}\""
                )));
            };

            if definition
                .constraints
                .iter()
                .any(|c| matches!(c.constraint, ast::ColumnConstraint::PrimaryKey { .. }))
            {
                return Err(LimboError::ParseError(
                    "PRIMARY KEY constraint cannot be altered".to_string(),
                ));
            }

            if definition
                .constraints
                .iter()
                .any(|c| matches!(c.constraint, ast::ColumnConstraint::Unique { .. }))
            {
                return Err(LimboError::ParseError(
                    "UNIQUE constraint cannot be altered".to_string(),
                ));
            }

            let (rewrites_physical_layout, virtual_generated_values_may_change, replacement_column) =
                match rename {
                    true => (false, false, None),
                    false => {
                        let replacement_column = Column::try_from(&definition)?;
                        let old_column = &btree.columns()[column_index];
                        let becomes_generated =
                            !old_column.is_generated() && replacement_column.is_generated();
                        // Toggling the virtual-generated bit changes whether the column
                        // occupies a stored slot, so the on-disk row layout shifts even
                        // if neither side is "becomes_generated" (e.g. virtual -> regular).
                        // Without rewriting, post-ALTER reads use the new schema's column
                        // indexes against rows that still hold the pre-ALTER layout, and
                        // values land in the wrong logical columns. See issue #6624.
                        let virtuality_changed = old_column.is_virtual_generated()
                            != replacement_column.is_virtual_generated();
                        // A change of declared type can change the column's affinity, in
                        // which case existing on-disk values must be coerced to match the
                        // new affinity. Without this, the row payload retains the old
                        // serial type and SQLite's `PRAGMA integrity_check` reports the
                        // file as corrupt (e.g. "NUMERIC value in <table>.<col>" when
                        // changing NUMERIC -> TEXT). See issue #3706.
                        let affinity_changed = old_column.affinity_with_strict(btree.is_strict)
                            != replacement_column.affinity_with_strict(btree.is_strict);
                        let rewrites_physical_layout =
                            becomes_generated || virtuality_changed || affinity_changed;
                        (
                            rewrites_physical_layout,
                            old_column.is_virtual_generated()
                                || replacement_column.is_virtual_generated(),
                            Some(replacement_column),
                        )
                    }
                };
            let clears_autoincrement_sequence = !rename
                && btree.has_autoincrement
                && btree.columns()[column_index].is_rowid_alias()
                && replacement_column
                    .as_ref()
                    .is_some_and(|column| !column.is_rowid_alias());

            let is_making_column_generated = definition
                .constraints
                .iter()
                .any(|c| matches!(c.constraint, ast::ColumnConstraint::Generated { .. }));

            if is_making_column_generated {
                let is_stored = definition.constraints.iter().any(|c| {
                    matches!(
                        c.constraint,
                        ast::ColumnConstraint::Generated {
                            typ: Some(ast::GeneratedColumnType::Stored),
                            ..
                        }
                    )
                });
                if is_stored {
                    return Err(LimboError::ParseError(
                        "cannot add a STORED column".to_string(),
                    ));
                }

                for constraint in &definition.constraints {
                    if let ast::ColumnConstraint::Generated { expr, .. } = &constraint.constraint {
                        crate::schema::validate_generated_expr(expr)?;
                    }
                }

                let non_generated_count = btree
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(|(idx, col)| *idx != column_index && !col.is_generated())
                    .count();

                if non_generated_count == 0 {
                    return Err(LimboError::ParseError(
                        "must have at least one non-generated column".to_string(),
                    ));
                }
            }

            let altered_table = if let Some(replacement_column) = &replacement_column {
                let mut table = btree.clone();
                table.columns_mut()[column_index] = replacement_column.clone();
                table.prepare_generated_columns()?;
                Some(table)
            } else {
                None
            };

            let indexes_to_rewrite = if let Some(altered_table) = &altered_table {
                if !rewrites_physical_layout && !virtual_generated_values_may_change {
                    Vec::new()
                } else {
                    let indexes = indexes_affected_by_column_rewrite(
                        &original_btree,
                        altered_table,
                        &table_indexes,
                        column_index,
                    )?;
                    validate_indexes_can_be_rewritten(&indexes, from, connection, database_id)?;
                    indexes
                }
            } else {
                Vec::new()
            };

            // If renaming, rewrite trigger SQL for all triggers that reference this column
            // We'll collect the triggers to rewrite and update them in sqlite_schema
            let mut triggers_to_rewrite: Vec<(usize, String, String)> = Vec::new();
            let mut views_to_rewrite: Vec<(usize, String, String)> = Vec::new();
            if rename {
                // Try to rewrite every trigger's SQL for the column rename.
                // If the rewritten SQL differs from the original, include it
                // in the update list. This matches SQLite's approach and avoids
                // incomplete detection heuristics that miss expression-level refs
                // (e.g., `SELECT b FROM src` in a trigger on a different table).
                let target_table_name = normalize_ident(table_name);
                for trigger_entry in collect_triggers_for_alter_target(resolver, database_id) {
                    if let Some(missing_table) = validate_trigger_table_refs_after_rename(
                        &trigger_entry.trigger,
                        &target_table_name,
                        resolver,
                        trigger_entry.database_id,
                        database_id,
                    )? {
                        return Err(LimboError::ParseError(format!(
                            "error in trigger {}: no such table: {}",
                            trigger_entry.trigger.name, missing_table
                        )));
                    }
                    match rewrite_trigger_sql_for_column_rename(
                        &trigger_entry.trigger.sql,
                        table_name,
                        from,
                        col_name,
                        trigger_entry.database_id,
                        database_id,
                        resolver,
                    ) {
                        Ok(new_sql) => {
                            if new_sql != trigger_entry.trigger.sql {
                                triggers_to_rewrite.push((
                                    trigger_entry.database_id,
                                    trigger_entry.trigger.name.clone(),
                                    new_sql,
                                ));
                            }
                        }
                        Err(LimboError::ParseError(message)) => {
                            let message = message.strip_prefix("Parse error: ").unwrap_or(&message);
                            let error =
                                if let Some(column) = message.strip_prefix("no such column: ") {
                                    format!(
                                        "error in trigger {} after rename: no such column: {}",
                                        trigger_entry.trigger.name, column
                                    )
                                } else {
                                    format!(
                                        "error in trigger {} after rename column: {}",
                                        trigger_entry.trigger.name, message
                                    )
                                };
                            return Err(LimboError::ParseError(error));
                        }
                        Err(e) => {
                            // If we can't rewrite the trigger, fail the ALTER TABLE operation
                            return Err(LimboError::ParseError(format!(
                                "error in trigger {} after rename column: {}",
                                trigger_entry.trigger.name, e
                            )));
                        }
                    }
                }
                let target_db_name = resolver
                    .get_database_name_by_index(database_id)
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "unknown database id {database_id} during ALTER TABLE"
                        ))
                    })?;
                views_to_rewrite = resolver.with_schema(database_id, |s| -> Result<_> {
                    let mut rewrites = Vec::new();
                    for (view_name, view) in s.views.iter() {
                        if let Some(rewritten) = rewrite_view_sql_for_column_rename(
                            &view.sql,
                            s,
                            table_name,
                            &target_db_name,
                            from,
                            col_name,
                        )? {
                            rewrites.push((database_id, view_name.clone(), rewritten.sql));
                        }
                    }
                    Ok(rewrites)
                })?;
                // Also search temp schema for views referencing the renamed column
                // (temp views can reference main/attached tables).
                if database_id != crate::TEMP_DB_ID {
                    let temp_rewrites =
                        resolver.with_schema(crate::TEMP_DB_ID, |s| -> Result<_> {
                            let mut rewrites = Vec::new();
                            for (view_name, view) in s.views.iter() {
                                if let Some(rewritten) = rewrite_view_sql_for_column_rename(
                                    &view.sql,
                                    s,
                                    table_name,
                                    &target_db_name,
                                    from,
                                    col_name,
                                )? {
                                    rewrites.push((
                                        crate::TEMP_DB_ID,
                                        view_name.clone(),
                                        rewritten.sql,
                                    ));
                                }
                            }
                            Ok(rewrites)
                        })?;
                    views_to_rewrite.extend(temp_rewrites);
                }
            }
            let has_temp_rewrites = triggers_to_rewrite
                .iter()
                .any(|(db, _, _)| *db == crate::TEMP_DB_ID)
                || views_to_rewrite
                    .iter()
                    .any(|(db, _, _)| *db == crate::TEMP_DB_ID);
            let temp_schema_version = if has_temp_rewrites {
                let schema_cookie = resolver.with_schema(crate::TEMP_DB_ID, |s| s.schema_version);
                program.begin_write_on_database(crate::TEMP_DB_ID, schema_cookie)?;
                Some(schema_cookie)
            } else {
                None
            };

            let sqlite_schema = resolver
                .with_schema(database_id, |s| s.get_btree_table(SQLITE_TABLEID))
                .ok_or_else(|| {
                    LimboError::ParseError("sqlite_schema table not found in schema".to_string())
                })?;

            let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_schema.clone()));

            program.emit_insn(Insn::OpenWrite {
                cursor_id,
                root_page: RegisterOrLiteral::Literal(sqlite_schema.root_page),
                db: database_id,
            });

            program.cursor_loop(cursor_id, |program, rowid| {
                let sqlite_schema_column_len = sqlite_schema.columns().len();
                turso_assert_eq!(sqlite_schema_column_len, 5);

                let first_column = program.alloc_registers(sqlite_schema_column_len);

                for i in 0..sqlite_schema_column_len {
                    program.emit_column_or_rowid(cursor_id, i, first_column + i);
                }

                program.emit_string8_new_reg(table_name.to_string());
                program.mark_last_insn_constant();

                program.emit_string8_new_reg(from.to_string());
                program.mark_last_insn_constant();

                program.emit_string8_new_reg(definition.to_string());
                program.mark_last_insn_constant();

                let out = program.alloc_registers(sqlite_schema_column_len);

                program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg: first_column,
                    dest: out,
                    func: crate::function::FuncCtx {
                        func: Func::AlterTable(if rename {
                            AlterTableFunc::RenameColumn
                        } else {
                            AlterTableFunc::AlterColumn
                        }),
                        arg_count: 8,
                    },
                });

                let record = program.alloc_register();

                program.emit_insn(Insn::MakeRecord {
                    start_reg: to_u32(out),
                    count: to_u32(sqlite_schema_column_len),
                    dest_reg: to_u32(record),
                    index_name: None,
                    affinity_str: None,
                });

                // In MVCC mode, we need to delete before insert to properly
                // end the old version (Hekaton-style UPDATE = DELETE + INSERT)
                if database_uses_mvcc(connection, database_id) {
                    program.emit_insn(Insn::Delete {
                        cursor_id,
                        table_name: SQLITE_TABLEID.to_string(),
                        is_part_of_update: true,
                    });
                }

                program.emit_insn(Insn::Insert {
                    cursor: cursor_id,
                    key_reg: rowid,
                    record_reg: record,
                    flag: crate::vdbe::insn::InsertFlags(0),
                    table_name: table_name.to_string(),
                });
            });

            // Update trigger SQL for renamed columns
            for (trigger_database_id, trigger_name, new_sql) in triggers_to_rewrite {
                let escaped_sql = escape_sql_string_literal(&new_sql);
                let escaped_trigger_name = escape_sql_string_literal(&trigger_name);
                let qualified_schema_table =
                    schema_table_name_for_db(resolver, trigger_database_id);
                let update_stmt = format!(
                    r#"
                        UPDATE {qualified_schema_table}
                        SET sql = '{escaped_sql}'
                        WHERE name = '{escaped_trigger_name}' COLLATE NOCASE AND type = 'trigger'
                    "#,
                );

                let mut parser = Parser::new(update_stmt.as_bytes());
                let cmd = parser.next_cmd().map_err(|e| {
                    LimboError::ParseError(format!(
                        "failed to parse trigger update SQL for {trigger_name}: {e}"
                    ))
                })?;
                let Some(ast::Cmd::Stmt(ast::Stmt::Update(update))) = cmd else {
                    return Err(LimboError::ParseError(format!(
                        "failed to parse trigger update SQL for {trigger_name}",
                    )));
                };

                translate_update_for_schema_change(
                    update,
                    resolver,
                    program,
                    connection,
                    input,
                    |_program| {},
                )?;
            }

            // Update view SQL for renamed columns
            for (view_database_id, view_name, new_sql) in views_to_rewrite {
                let escaped_sql = escape_sql_string_literal(&new_sql);
                let view_schema_table = schema_table_name_for_db(resolver, view_database_id);
                let update_stmt = format!(
                    r#"
                        UPDATE {view_schema_table}
                        SET sql = '{escaped_sql}'
                        WHERE name = '{view_name}' COLLATE NOCASE AND type = 'view'
                    "#,
                );

                let mut parser = Parser::new(update_stmt.as_bytes());
                let cmd = parser.next_cmd().map_err(|e| {
                    LimboError::ParseError(format!(
                        "failed to parse view update SQL for {view_name}: {e}"
                    ))
                })?;
                let Some(ast::Cmd::Stmt(ast::Stmt::Update(update))) = cmd else {
                    return Err(LimboError::ParseError(format!(
                        "failed to parse view update SQL for {view_name}",
                    )));
                };

                translate_update_for_schema_change(
                    update,
                    resolver,
                    program,
                    connection,
                    input,
                    |_program| {},
                )?;
            }

            if let Some(temp_schema_version) = temp_schema_version {
                program.emit_insn(Insn::SetCookie {
                    db: crate::TEMP_DB_ID,
                    cookie: Cookie::SchemaVersion,
                    value: temp_schema_version as i32 + 1,
                    p5: 0,
                });
            }

            if let Some(altered_table) = altered_table {
                let altered_table = Arc::new(altered_table);
                if rewrites_physical_layout {
                    emit_add_virtual_column_validation(
                        program,
                        &altered_table,
                        &altered_table.columns()[column_index],
                        &definition.constraints,
                        resolver,
                        connection,
                        database_id,
                    )?;

                    let original_columns = original_btree.columns();
                    let source_column_by_schema_idx: Vec<Option<usize>> = altered_table
                        .columns()
                        .iter()
                        .enumerate()
                        .map(|(idx, column)| {
                            if column.is_virtual_generated() {
                                // Virtual columns don't occupy a slot in the rewritten record.
                                None
                            } else if original_columns
                                .get(idx)
                                .is_some_and(|c| c.is_virtual_generated())
                            {
                                // Newly-stored slot (the original column was virtual): no
                                // source value exists on the old row image — leave NULL.
                                None
                            } else {
                                // The cursor is opened on the original btree, so the logical
                                // index passed here is mapped to the original physical slot
                                // by `emit_column_or_rowid` via the original's logical-to-
                                // physical map. Schema order is preserved by ALTER COLUMN,
                                // so the rewritten schema_idx also identifies the same
                                // logical column in the original.
                                Some(idx)
                            }
                        })
                        .collect();
                    let layout = altered_table.column_layout()?;
                    emit_rewrite_table_rows(
                        program,
                        original_btree.clone(),
                        &altered_table,
                        &source_column_by_schema_idx,
                        &layout,
                        connection,
                        database_id,
                    );
                }
                emit_rewrite_table_indexes(
                    program,
                    resolver,
                    database_id,
                    &altered_table,
                    &indexes_to_rewrite,
                )?;
            }

            if clears_autoincrement_sequence {
                let table_name_norm = normalize_ident(table_name);
                emit_delete_sqlite_sequence_entry(program, resolver, database_id, &table_name_norm);
            }

            program.emit_insn(Insn::SetCookie {
                db: database_id,
                cookie: Cookie::SchemaVersion,
                value: schema_version as i32 + 1,
                p5: 0,
            });
            program.emit_insn(Insn::AlterColumn {
                db: database_id,
                table: table_name.to_owned(),
                column_index,
                definition: Box::new(definition),
                rename,
            });
        }
    };

    Ok(())
}

// Return the indexes whose persisted entries may become stale after ALTER COLUMN,
// rewritten to match the post-ALTER table metadata. We consider both the old and
// new generated-column dependency graphs so direct indexes, expression indexes,
// partial-index WHERE clauses, and indexes on dependent generated columns are
// rebuilt when their computed keys can change.
// Example: `x NUMERIC -> y TEXT` requires rebuilding `INDEX ON t(x)` with TEXT
// keys; `g AS ('old:' || a) -> g AS ('new:' || a)` requires rebuilding
// `INDEX ON t(g)` even though `g` is virtual and table rows are not rewritten.
fn indexes_affected_by_column_rewrite(
    original_table: &BTreeTable,
    rewritten_table: &BTreeTable,
    indexes: &[Arc<Index>],
    column_index: usize,
) -> Result<Vec<Arc<Index>>> {
    let mut affected_columns = original_table.columns_affected_by_update([column_index])?;
    affected_columns.union_with(&rewritten_table.columns_affected_by_update([column_index])?)?;
    let affected_names = affected_column_names(original_table, rewritten_table, &affected_columns);
    let old_column_name = original_table.columns()[column_index]
        .name
        .as_deref()
        .expect("ALTER COLUMN target must be named");
    let new_column_name = rewritten_table.columns()[column_index]
        .name
        .as_deref()
        .expect("ALTER COLUMN replacement must be named");

    Ok(indexes
        .iter()
        .filter(|index| {
            index_references_rewritten_column(
                index.as_ref(),
                original_table,
                &affected_columns,
                &affected_names,
            )
        })
        .map(|index| {
            rewrite_index_for_column_rewrite(
                index.as_ref(),
                rewritten_table,
                old_column_name,
                new_column_name,
            )
        })
        .collect())
}

fn affected_column_names(
    original_table: &BTreeTable,
    rewritten_table: &BTreeTable,
    affected_columns: &ColumnMask,
) -> HashSet<String> {
    let mut names = HashSet::default();
    for table in [original_table, rewritten_table] {
        for (idx, column) in table.columns().iter().enumerate() {
            if affected_columns.get(idx) {
                if let Some(name) = &column.name {
                    names.insert(normalize_ident(name));
                }
            }
        }
    }
    names
}

// Return true if this index observes any column whose value may change under the
// rewritten table schema. Direct index columns are checked by column position,
// while expression-index terms and partial-index WHERE clauses are checked by
// resolving their column dependencies.
// Example: `INDEX ON t(z) WHERE typeof(x) = 'text'` must be rebuilt when
// `x NUMERIC -> y TEXT`, even though `x` is not part of the stored index key.
fn index_references_rewritten_column(
    index: &Index,
    table: &BTreeTable,
    affected_columns: &ColumnMask,
    affected_names: &HashSet<String>,
) -> bool {
    for column in &index.columns {
        if column.pos_in_table != EXPR_INDEX_SENTINEL && affected_columns.get(column.pos_in_table) {
            return true;
        }
        if let Some(expr) = &column.expr {
            if expr_references_any_affected_column(expr, table, affected_names) {
                return true;
            }
        }
    }

    index
        .where_clause
        .as_ref()
        .is_some_and(|expr| expr_references_any_affected_column(expr, table, affected_names))
}

fn expr_references_any_affected_column(
    expr: &ast::Expr,
    table: &BTreeTable,
    affected_names: &HashSet<String>,
) -> bool {
    collect_column_dependencies_of_expr(expr, table.columns())
        .iter()
        .any(|name| affected_names.contains(name))
}

// Build the post-ALTER index metadata used when refilling an affected index.
// Expression-index terms and partial-index WHERE clauses need identifier
// rewrites, while direct index columns need to be refreshed from the rewritten
// table so renamed columns and generated-column expressions stay in sync.
// Example: after `x NUMERIC -> y TEXT`, `INDEX ON t(typeof(x)) WHERE x IS NOT NULL`
// must be refilled as `INDEX ON t(typeof(y)) WHERE y IS NOT NULL`.
fn rewrite_index_for_column_rewrite(
    index: &Index,
    rewritten_table: &BTreeTable,
    old_column_name: &str,
    new_column_name: &str,
) -> Arc<Index> {
    let mut rewritten_index = index.clone();
    for column in &mut rewritten_index.columns {
        if let Some(expr) = &mut column.expr {
            rename_identifiers(expr.as_mut(), old_column_name, new_column_name);
            if column.pos_in_table == EXPR_INDEX_SENTINEL {
                column.name = expr.to_string();
            }
        }

        if column.pos_in_table != EXPR_INDEX_SENTINEL {
            let table_column = &rewritten_table.columns()[column.pos_in_table];
            if let Some(name) = &table_column.name {
                column.name = normalize_ident(name);
            }
            column.default.clone_from(&table_column.default);
            column.expr = table_column
                .generated_expr()
                .map(|expr| Box::new(expr.clone()));
        }
    }

    if let Some(where_clause) = &mut rewritten_index.where_clause {
        rename_identifiers(where_clause, old_column_name, new_column_name);
    }

    Arc::new(rewritten_index)
}

fn validate_indexes_can_be_rewritten(
    indexes: &[Arc<Index>],
    column_name: &str,
    connection: &Arc<crate::Connection>,
    database_id: usize,
) -> Result<()> {
    if !indexes.is_empty() && database_uses_mvcc(connection, database_id) {
        return Err(LimboError::ParseError(format!(
            "cannot ALTER COLUMN \"{column_name}\": rebuilding affected indexes is not supported in MVCC mode"
        )));
    }

    for index in indexes {
        if !index.has_rowid {
            return Err(LimboError::ParseError(format!(
                "cannot ALTER COLUMN \"{column_name}\": rebuilding affected indexes is not supported for WITHOUT ROWID tables"
            )));
        }
        if index
            .index_method
            .as_ref()
            .is_some_and(|method| !method.definition().backing_btree)
        {
            return Err(LimboError::ParseError(format!(
                "cannot ALTER COLUMN \"{column_name}\": rebuilding affected custom index {} is not supported",
                index.name
            )));
        }
    }
    Ok(())
}

fn emit_rewrite_table_indexes(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    rewritten_table: &Arc<BTreeTable>,
    indexes: &[Arc<Index>],
) -> Result<()> {
    for index in indexes {
        let index_cursor_id = program.alloc_cursor_index(None, index)?;
        emit_refill_index(
            program,
            resolver,
            database_id,
            rewritten_table,
            index,
            index_cursor_id,
            RegisterOrLiteral::Literal(index.root_page),
            Some(index.root_page),
        )?;
    }
    Ok(())
}

/// Rewrite every row in `original_btree` into the physical layout of `rewritten_table`.
///
/// `source_column_by_schema_idx` is indexed by the rewritten table's schema order. Each
/// entry is either the physical column index to read from the old row image or `None`
/// for virtual generated columns, which are omitted from the stored record entirely.
fn emit_rewrite_table_rows(
    program: &mut ProgramBuilder,
    original_btree: Arc<BTreeTable>,
    rewritten_table: &BTreeTable,
    source_column_by_schema_idx: &[Option<usize>],
    layout: &ColumnLayout,
    connection: &Arc<crate::Connection>,
    database_id: usize,
) {
    turso_assert_eq!(
        source_column_by_schema_idx.len(),
        rewritten_table.columns().len()
    );

    let non_virtual_column_count = layout.num_non_virtual_cols();
    let root_page = rewritten_table.root_page;
    let table_name = rewritten_table.name.clone();
    let affinity_str = non_virtual_affinity_str(rewritten_table);
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(original_btree));

    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: RegisterOrLiteral::Literal(root_page),
        db: database_id,
    });

    program.cursor_loop(cursor_id, |program, rowid| {
        // Initialize all destination slots to NULL so that columns without a
        // source value (e.g. when a virtual generated column becomes stored,
        // there is no pre-existing value on the old row image) default to NULL.
        let base_dest_reg = program.alloc_registers_and_init_w_null(non_virtual_column_count);
        for (schema_idx, source_column_idx) in source_column_by_schema_idx.iter().enumerate() {
            let Some(source_column_idx) = source_column_idx else {
                continue;
            };

            program.emit_column_or_rowid(
                cursor_id,
                *source_column_idx,
                layout.to_register(base_dest_reg, schema_idx),
            );
        }

        let record = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(base_dest_reg),
            count: to_u32(non_virtual_column_count),
            dest_reg: to_u32(record),
            index_name: None,
            affinity_str: Some(affinity_str.clone()),
        });

        if database_uses_mvcc(connection, database_id) {
            program.emit_insn(Insn::Delete {
                cursor_id,
                table_name: table_name.clone(),
                is_part_of_update: true,
            });
        }

        program.emit_insn(Insn::Insert {
            cursor: cursor_id,
            key_reg: rowid,
            record_reg: record,
            flag: crate::vdbe::insn::InsertFlags(0),
            table_name: table_name.clone(),
        });
    });
}

fn non_virtual_affinity_str(table: &BTreeTable) -> String {
    table
        .columns()
        .iter()
        .filter(|col| !col.is_virtual_generated())
        .map(|col| col.affinity_with_strict(table.is_strict).aff_mask())
        .collect()
}

fn translate_rename_virtual_table(
    program: &mut ProgramBuilder,
    vtab: Arc<VirtualTable>,
    old_name: &str,
    new_name_norm: String,
    resolver: &Resolver,
    connection: &Arc<crate::Connection>,
    database_id: usize,
) -> Result<()> {
    let schema_version = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_operation()?;
    let vtab_cur = program.alloc_cursor_id(CursorType::VirtualTable(vtab));
    program.emit_insn(Insn::VOpen {
        cursor_id: vtab_cur,
    });

    let new_name_reg = program.emit_string8_new_reg(new_name_norm.clone());
    program.emit_insn(Insn::VRename {
        cursor_id: vtab_cur,
        new_name_reg,
    });
    // Rewrite sqlite_schema entry
    let sqlite_schema = resolver
        .schema()
        .get_btree_table(SQLITE_TABLEID)
        .ok_or_else(|| {
            LimboError::ParseError("sqlite_schema table not found in schema".to_string())
        })?;

    let schema_cur = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_schema.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: schema_cur,
        root_page: RegisterOrLiteral::Literal(sqlite_schema.root_page),
        db: database_id,
    });

    program.cursor_loop(schema_cur, |program, rowid| {
        let ncols = sqlite_schema.columns().len();
        turso_assert_eq!(ncols, 5);

        let first_col = program.alloc_registers(ncols);
        for i in 0..ncols {
            program.emit_column_or_rowid(schema_cur, i, first_col + i);
        }

        program.emit_string8_new_reg(old_name.to_string());
        program.mark_last_insn_constant();

        program.emit_string8_new_reg(new_name_norm.clone());
        program.mark_last_insn_constant();

        let out = program.alloc_registers(ncols);

        program.emit_insn(Insn::Function {
            constant_mask: 0,
            start_reg: first_col,
            dest: out,
            func: crate::function::FuncCtx {
                func: Func::AlterTable(AlterTableFunc::RenameTable),
                arg_count: 7,
            },
        });

        let rec = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(out),
            count: to_u32(ncols),
            dest_reg: to_u32(rec),
            index_name: None,
            affinity_str: None,
        });

        // In MVCC mode, we need to delete before insert to properly
        // end the old version (Hekaton-style UPDATE = DELETE + INSERT)
        if database_uses_mvcc(connection, database_id) {
            program.emit_insn(Insn::Delete {
                cursor_id: schema_cur,
                table_name: SQLITE_TABLEID.to_string(),
                is_part_of_update: true,
            });
        }

        program.emit_insn(Insn::Insert {
            cursor: schema_cur,
            key_reg: rowid,
            record_reg: rec,
            flag: crate::vdbe::insn::InsertFlags(0),
            table_name: old_name.to_string(),
        });
    });

    // Bump schema cookie
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: schema_version as i32 + 1,
        p5: 0,
    });

    program.emit_insn(Insn::RenameTable {
        db: database_id,
        from: old_name.to_owned(),
        to: new_name_norm,
    });

    program.emit_insn(Insn::Close {
        cursor_id: schema_cur,
    });
    program.emit_insn(Insn::Close {
        cursor_id: vtab_cur,
    });

    Ok(())
}

/* Triggers must be rewritten when a column is renamed, and DROP COLUMN on table T must be forbidden if any trigger on T references the column.
Here are some helpers related to that: */

/// Rewrite a trigger's SQL after the table it targets has been
/// renamed.
///
/// `renamed_db_name` is the name of the database whose table is being
/// renamed (e.g. `"main"` or an attached alias). It's used to make
/// sure we only rewrite triggers that actually target *this*
/// database's version of the table — previously we keyed only on the
/// unqualified table name, which incorrectly rewrote e.g.
/// `CREATE TEMP TRIGGER ... ON aux.t` during
/// `ALTER TABLE main.t RENAME TO ...`.
fn rewrite_trigger_sql_for_table_rename(
    trigger_sql: &str,
    old_table_name: &str,
    new_table_name: &str,
    renamed_db_name: &str,
) -> Result<String> {
    let mut parser = Parser::new(trigger_sql.as_bytes());
    let cmd = parser
        .next_cmd()
        .map_err(|e| LimboError::ParseError(format!("failed to parse trigger SQL: {e}")))?;
    let Some(ast::Cmd::Stmt(ast::Stmt::CreateTrigger {
        temporary,
        if_not_exists,
        trigger_name,
        time,
        event,
        tbl_name,
        for_each_row,
        mut when_clause,
        mut commands,
    })) = cmd
    else {
        return Err(LimboError::ParseError(format!(
            "failed to parse trigger SQL: {trigger_sql}"
        )));
    };

    // Only rewrite when this trigger's `tbl_name` actually targets the
    // renamed database. An explicit qualifier that points elsewhere
    // (e.g. `ON aux.t`) must be left alone. An absent qualifier is
    // ambiguous — it may resolve to temp (if a shadow exists) or to
    // the parent schema — but treating "no qualifier + name match" as
    // targeting the renamed db matches what the previous
    // implementation did, and the caller already narrowed the
    // candidate set by iterating temp schema triggers for renames in
    // main only.
    let qualifier_matches = match tbl_name.db_name.as_ref() {
        Some(db) => db.as_str().eq_ignore_ascii_case(renamed_db_name),
        None => true,
    };
    let new_tbl_name =
        if qualifier_matches && tbl_name.name.as_str().eq_ignore_ascii_case(old_table_name) {
            ast::QualifiedName {
                db_name: tbl_name.db_name,
                name: ast::Name::exact(new_table_name.to_string()),
                alias: None,
            }
        } else {
            tbl_name
        };

    if let Some(ref mut when) = when_clause {
        rewrite_check_expr_table_refs(when, old_table_name, new_table_name);
    }

    for cmd in &mut commands {
        rewrite_trigger_cmd_table_refs(cmd, old_table_name, new_table_name);
    }

    Ok(ast::Stmt::CreateTrigger {
        temporary,
        if_not_exists,
        trigger_name,
        time,
        event,
        tbl_name: new_tbl_name,
        for_each_row,
        when_clause,
        commands,
    }
    .to_string())
}

/// Check if a trigger contains qualified references to a specific column in its owning table.
/// Rewrite trigger SQL to replace old column name with new column name.
/// This handles old.x, new.x, and unqualified x references.
fn rewrite_trigger_sql_for_column_rename(
    trigger_sql: &str,
    table_name: &str,
    old_column_name: &str,
    new_column_name: &str,
    trigger_database_id: usize,
    target_database_id: usize,
    resolver: &Resolver,
) -> Result<String> {
    // Parse the trigger SQL
    let mut parser = Parser::new(trigger_sql.as_bytes());
    let cmd = parser
        .next_cmd()
        .map_err(|e| LimboError::ParseError(format!("failed to parse trigger SQL: {e}")))?;
    let Some(ast::Cmd::Stmt(ast::Stmt::CreateTrigger {
        temporary,
        if_not_exists,
        trigger_name,
        time,
        event,
        tbl_name,
        for_each_row,
        when_clause,
        commands,
    })) = cmd
    else {
        return Err(LimboError::ParseError(format!(
            "failed to parse trigger SQL: {trigger_sql}"
        )));
    };

    let old_col_norm = normalize_ident(old_column_name);
    let new_col_norm = normalize_ident(new_column_name);

    // Get the trigger's owning table to check unqualified column references
    let trigger_table_name_raw = tbl_name.name.as_str();
    let trigger_table_name = normalize_ident(trigger_table_name_raw);
    let trigger_table = resolver
        .with_schema(trigger_database_id, |schema| {
            schema.get_btree_table(&trigger_table_name)
        })
        .ok_or_else(|| {
            LimboError::ParseError(format!("trigger table not found: {trigger_table_name}"))
        })?;

    // Check if this trigger references the column being renamed
    // We need to check if the column exists in the table being renamed
    let target_table_name = normalize_ident(table_name);
    if resolver
        .with_schema(target_database_id, |schema| {
            schema.get_btree_table(&target_table_name)
        })
        .is_none()
    {
        return Err(LimboError::ParseError(format!(
            "target table not found: {target_table_name}"
        )));
    }

    // Rewrite UPDATE OF column list if renaming a column in the trigger's owning table
    let is_renaming_trigger_table = trigger_table_name == target_table_name;
    let new_event = if is_renaming_trigger_table {
        match event {
            ast::TriggerEvent::UpdateOf(mut cols) => {
                // Rewrite column names in UPDATE OF list
                for col in &mut cols {
                    let col_norm = normalize_ident(col.as_str());
                    if col_norm == old_col_norm {
                        *col = ast::Name::from_string(new_col_norm.clone());
                    }
                }
                ast::TriggerEvent::UpdateOf(cols)
            }
            other => other,
        }
    } else {
        event
    };

    // Rewrite WHEN clause column references if present.
    // In WHEN clauses, only NEW.col / OLD.col qualified references are valid;
    // bare column names are not valid in trigger WHEN clauses per SQLite semantics,
    // so we only rewrite qualified NEW/OLD references here.
    let new_when_clause = when_clause
        .map(|e| {
            let mut expr = *e;
            walk_expr_mut(
                &mut expr,
                &mut |ex: &mut ast::Expr| -> Result<WalkControl> {
                    if let ast::Expr::Qualified(ns, col) | ast::Expr::DoublyQualified(_, ns, col) =
                        ex
                    {
                        let ns_norm = normalize_ident(ns.as_str());
                        let col_norm = normalize_ident(col.as_str());
                        if (ns_norm.eq_ignore_ascii_case("new")
                            || ns_norm.eq_ignore_ascii_case("old"))
                            && col_norm == *old_col_norm
                            && is_renaming_trigger_table
                            && trigger_table.get_column(&col_norm).is_some()
                        {
                            *col = ast::Name::from_string(&*new_col_norm);
                        }
                    }
                    Ok(WalkControl::Continue)
                },
            )?;
            Ok::<Box<ast::Expr>, LimboError>(Box::new(expr))
        })
        .transpose()?;

    // Validate: if the WHEN clause still contains a bare reference to the old column,
    // SQLite would error with "no such column". We must do the same.
    if let Some(ref when_expr) = new_when_clause {
        let mut has_bare_old_col = false;
        let _ = walk_expr_mut(
            &mut when_expr.clone(),
            &mut |ex: &mut ast::Expr| -> Result<WalkControl> {
                if let ast::Expr::Id(ref name) | ast::Expr::Name(ref name) = ex {
                    if normalize_ident(name.as_str()) == *old_col_norm {
                        has_bare_old_col = true;
                    }
                }
                Ok(WalkControl::Continue)
            },
        );
        if has_bare_old_col {
            return Err(LimboError::ParseError(format!(
                "error in trigger {}: no such column: {}",
                trigger_name.name.as_str(),
                old_col_norm
            )));
        }
    }

    let mut new_commands = Vec::new();
    for cmd in commands {
        let new_cmd = rewrite_trigger_cmd_for_column_rename(
            cmd,
            &trigger_table,
            trigger_table_name_raw,
            &target_table_name,
            &old_col_norm,
            &new_col_norm,
            trigger_database_id,
            resolver,
        )?;
        new_commands.push(new_cmd);
    }

    validate_trigger_after_column_rename(
        &new_event,
        new_when_clause.as_deref(),
        &new_commands,
        trigger_table.as_ref(),
        trigger_table_name_raw,
        &target_table_name,
        &old_col_norm,
        trigger_database_id,
        resolver,
    )?;

    // Reconstruct the SQL
    let new_sql = create_trigger_to_sql(
        temporary,
        if_not_exists,
        &trigger_name,
        time,
        &new_event,
        &tbl_name,
        for_each_row,
        new_when_clause.as_deref(),
        &new_commands,
    );

    // Validate the rewritten trigger against the dropped column.
    //
    // For temp-schema triggers the `UPDATE sqlite_schema` recompile
    // backstop never runs (temp rows are rewritten in-place), so we
    // need an explicit validation pass here. We only run it when the
    // trigger's owning table IS the altered table — i.e. when the
    // rename actually changes columns the trigger's NEW/OLD refs
    // bind against. Cross-table references inside the trigger body
    // (e.g. `SELECT other_col FROM other_table` where the rename is
    // on `other_table`) are already handled by the per-expression
    // rewrite above and don't need the extra pass; `validate_trigger_
    // columns_after_drop` only resolves against the trigger's owning
    // table and would false-positive on such cross-table refs.
    //
    // The old guard was `trigger_db == TEMP && target_db == TEMP`
    // which silently skipped TEMP-trigger-on-MAIN (case C:
    // `CREATE TEMP TRIGGER tr ON main.t`); adding
    // `is_renaming_trigger_table` restores that coverage without
    // breaking cross-table body refs.
    if trigger_database_id == crate::TEMP_DB_ID && is_renaming_trigger_table {
        let rewritten_trigger = crate::schema::Trigger::new(
            trigger_name.name.as_str().to_string(),
            new_sql.clone(),
            tbl_name.name.as_str().to_string(),
            time,
            new_event,
            for_each_row,
            new_when_clause.as_deref().cloned(),
            new_commands.clone(),
            temporary,
            Some(target_database_id),
        );
        if let Some(bad_column) = validate_trigger_columns_after_drop(
            &rewritten_trigger,
            &target_table_name,
            trigger_table.as_ref(),
            resolver,
            trigger_database_id,
            target_database_id,
        )? {
            return Err(LimboError::ParseError(format!(
                "error in trigger {}: no such column: {}",
                trigger_name.name.as_str(),
                bad_column
            )));
        }
    }

    Ok(new_sql)
}

#[derive(Clone, Copy)]
enum ColumnRenameMode<'a> {
    Validate,
    Rewrite { new_col_norm: &'a str },
}

impl<'a> ColumnRenameMode<'a> {
    fn rewritten_name(self) -> Option<&'a str> {
        match self {
            Self::Validate => None,
            Self::Rewrite { new_col_norm } => Some(new_col_norm),
        }
    }
}

#[derive(Clone, Copy)]
enum ColumnRenameExprTraversal {
    Normal,
    RewriteResultExpr,
}

fn no_such_column_error(column_ref: &str) -> LimboError {
    LimboError::ParseError(format!("no such column: {column_ref}"))
}

#[allow(clippy::too_many_arguments)]
fn apply_expr_column_ref_with_context(
    mode: ColumnRenameMode<'_>,
    e: &mut ast::Expr,
    trigger_table: &BTreeTable,
    trigger_table_name: &str,
    old_col_norm: &str,
    is_renaming_trigger_table: bool,
    context_table: Option<(&BTreeTable, &str, bool)>,
    from_target_qualifiers: &[String],
) -> Result<()> {
    match e {
        ast::Expr::Qualified(..) | ast::Expr::DoublyQualified(..) => {
            let (db, ns, col) = match e {
                ast::Expr::Qualified(ns, col) => (None, &*ns, col),
                ast::Expr::DoublyQualified(db, ns, col) => (Some(&*db), &*ns, col),
                _ => unreachable!("outer match arm only admits (Doubly)Qualified"),
            };
            let ns_norm = normalize_ident(ns.as_str());
            let col_norm = normalize_ident(col.as_str());

            if col_norm != *old_col_norm {
                return Ok(());
            }

            // SQLite reports unresolvable references as written in the
            // original expression (e.g. "no such column: t.b"), so keep
            // the qualifier(s) when building validation error messages.
            let error_column_ref = match db {
                Some(db) => format!("{}.{}.{}", db.as_str(), ns.as_str(), col.as_str()),
                None => format!("{}.{}", ns.as_str(), col.as_str()),
            };

            // These branches are mutually exclusive — a qualifier can
            // match at most one of: NEW/OLD (the trigger table), the
            // rewrite context table, the bare trigger table name, or
            // a target qualifier from the FROM clause. Using an
            // `else if` chain makes the mutual exclusion explicit;
            // the previous sequence of independent `if`s could
            // double-rewrite if the context table and the trigger
            // table shared a name, because two branches both issued
            // a rewrite on the same `col` node.
            if ns_norm.eq_ignore_ascii_case("new") || ns_norm.eq_ignore_ascii_case("old") {
                if is_renaming_trigger_table && trigger_table.get_column(&col_norm).is_some() {
                    if let Some(new_col_norm) = mode.rewritten_name() {
                        *col = ast::Name::from_string(new_col_norm);
                    } else {
                        return Err(no_such_column_error(&error_column_ref));
                    }
                }
                return Ok(());
            } else if let Some((_, ctx_name_norm, is_renaming_ctx)) =
                context_table
                    .as_ref()
                    .filter(|(_, ctx_name_norm, is_renaming_ctx)| {
                        *is_renaming_ctx && ns_norm == **ctx_name_norm
                    })
            {
                let _ = (ctx_name_norm, is_renaming_ctx);
                if let Some(new_col_norm) = mode.rewritten_name() {
                    *col = ast::Name::from_string(new_col_norm);
                } else {
                    return Err(no_such_column_error(&error_column_ref));
                }
            } else if is_renaming_trigger_table
                && ns_norm.eq_ignore_ascii_case(trigger_table_name)
                && trigger_table.get_column(&col_norm).is_some()
            {
                if let Some(new_col_norm) = mode.rewritten_name() {
                    let trigger_table_name_norm = normalize_ident(trigger_table_name);
                    let ctx_is_different_table = context_table
                        .as_ref()
                        .is_some_and(|(_, ctx_name, _)| *ctx_name != trigger_table_name_norm);
                    if ctx_is_different_table {
                        return Err(no_such_column_error(&error_column_ref));
                    }
                    *col = ast::Name::from_string(new_col_norm);
                } else {
                    return Err(no_such_column_error(&error_column_ref));
                }
            } else if from_target_qualifiers.contains(&ns_norm) {
                if let Some(new_col_norm) = mode.rewritten_name() {
                    *col = ast::Name::from_string(new_col_norm);
                } else {
                    return Err(no_such_column_error(&error_column_ref));
                }
            }
        }
        ast::Expr::Id(col) => {
            let col_norm = normalize_ident(col.as_str());
            if col_norm != *old_col_norm {
                return Ok(());
            }

            if let Some((ctx_table, _, is_renaming_ctx)) = context_table {
                if ctx_table.get_column(&col_norm).is_some() {
                    if is_renaming_ctx {
                        if let Some(new_col_norm) = mode.rewritten_name() {
                            *e = ast::Expr::Id(ast::Name::from_string(new_col_norm));
                        } else {
                            return Err(no_such_column_error(old_col_norm));
                        }
                    }
                    return Ok(());
                }
            }

            // TODO: ambiguous-join correctness. A bare `old_col`
            // reference inside a trigger body that contains e.g.
            // `JOIN other USING (old_col)` can legitimately bind
            // to `other.old_col` rather than the trigger table's
            // column. SQLite rewrites the post-resolution bound AST,
            // so it never has to guess. We walk the unresolved AST,
            // so "in a FROM that mentions a rename target" is the
            // best proxy we have — which mis-rewrites a USING-joined
            // reference when both sides happen to use the same
            // pre-rename column name.
            if (is_renaming_trigger_table && trigger_table.get_column(&col_norm).is_some())
                || !from_target_qualifiers.is_empty()
            {
                if let Some(new_col_norm) = mode.rewritten_name() {
                    *e = ast::Expr::Id(ast::Name::from_string(new_col_norm));
                } else {
                    return Err(no_such_column_error(old_col_norm));
                }
            }
        }
        _ => {}
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_expr_for_column_rename(
    mode: ColumnRenameMode<'_>,
    expr: &mut ast::Expr,
    traversal: ColumnRenameExprTraversal,
    trigger_table: &BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    old_col_norm: &str,
    context_table_name: Option<&str>,
    from_target_qualifiers: &[String],
    database_id: usize,
    resolver: &Resolver,
) -> Result<()> {
    let is_renaming_trigger_table = trigger_table_name.eq_ignore_ascii_case(target_table_name);

    let context_table_info: Option<(Arc<BTreeTable>, String, bool)> = if let Some(ctx_name) =
        context_table_name
    {
        let ctx_name_norm = normalize_ident(ctx_name);
        let is_renaming = ctx_name_norm == *target_table_name;
        let table = resolve_trigger_command_table_for_alter(resolver, database_id, &ctx_name_norm)
            .ok_or_else(|| {
                LimboError::ParseError(format!("context table not found: {ctx_name_norm}"))
            })?;
        Some((table, ctx_name_norm, is_renaming))
    } else {
        None
    };

    walk_expr_mut(expr, &mut |e: &mut ast::Expr| -> Result<WalkControl> {
        match e {
            ast::Expr::Exists(select) => {
                if matches!(traversal, ColumnRenameExprTraversal::RewriteResultExpr) {
                    return Ok(WalkControl::SkipChildren);
                }
                apply_select_for_column_rename(
                    mode,
                    select,
                    trigger_table,
                    trigger_table_name,
                    target_table_name,
                    old_col_norm,
                    from_target_qualifiers,
                    database_id,
                    resolver,
                )?;
            }
            ast::Expr::Subquery(select) => {
                apply_select_for_column_rename(
                    mode,
                    select,
                    trigger_table,
                    trigger_table_name,
                    target_table_name,
                    old_col_norm,
                    from_target_qualifiers,
                    database_id,
                    resolver,
                )?;
            }
            ast::Expr::InSelect { rhs, .. } => {
                apply_select_for_column_rename(
                    mode,
                    rhs,
                    trigger_table,
                    trigger_table_name,
                    target_table_name,
                    old_col_norm,
                    from_target_qualifiers,
                    database_id,
                    resolver,
                )?;
            }
            _ => {
                apply_expr_column_ref_with_context(
                    mode,
                    e,
                    trigger_table,
                    trigger_table_name,
                    old_col_norm,
                    is_renaming_trigger_table,
                    context_table_info
                        .as_ref()
                        .map(|(table, name, is_renaming)| {
                            (table.as_ref(), name.as_str(), *is_renaming)
                        }),
                    from_target_qualifiers,
                )?;
            }
        }
        Ok(WalkControl::Continue)
    })?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_result_expr_for_column_rename(
    mode: ColumnRenameMode<'_>,
    expr: &mut ast::Expr,
    trigger_table: &BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    old_col_norm: &str,
    visible_target_qualifiers: &[String],
    database_id: usize,
    resolver: &Resolver,
) -> Result<()> {
    let traversal = match mode {
        ColumnRenameMode::Validate => ColumnRenameExprTraversal::Normal,
        ColumnRenameMode::Rewrite { .. } => ColumnRenameExprTraversal::RewriteResultExpr,
    };
    apply_expr_for_column_rename(
        mode,
        expr,
        traversal,
        trigger_table,
        trigger_table_name,
        target_table_name,
        old_col_norm,
        None,
        visible_target_qualifiers,
        database_id,
        resolver,
    )
}

#[allow(clippy::too_many_arguments)]
fn apply_upsert_for_column_rename(
    mode: ColumnRenameMode<'_>,
    upsert: &mut ast::Upsert,
    trigger_table: &BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    insert_table_name: &str,
    old_col_norm: &str,
    database_id: usize,
    resolver: &Resolver,
) -> Result<()> {
    let insert_table_name_norm = normalize_ident(insert_table_name);
    let insert_targets_renamed_table = insert_table_name_norm == *target_table_name;

    if let Some(index) = &mut upsert.index {
        for target in &mut index.targets {
            apply_expr_for_column_rename(
                mode,
                &mut target.expr,
                ColumnRenameExprTraversal::Normal,
                trigger_table,
                trigger_table_name,
                target_table_name,
                old_col_norm,
                Some(&insert_table_name_norm),
                &[],
                database_id,
                resolver,
            )?;
            if insert_targets_renamed_table {
                if let Some(new_col_norm) = mode.rewritten_name() {
                    rename_excluded_column_refs(&mut target.expr, old_col_norm, new_col_norm)?;
                }
            }
        }
        if let Some(where_clause) = &mut index.where_clause {
            apply_expr_for_column_rename(
                mode,
                where_clause,
                ColumnRenameExprTraversal::Normal,
                trigger_table,
                trigger_table_name,
                target_table_name,
                old_col_norm,
                Some(&insert_table_name_norm),
                &[],
                database_id,
                resolver,
            )?;
            if insert_targets_renamed_table {
                if let Some(new_col_norm) = mode.rewritten_name() {
                    rename_excluded_column_refs(where_clause, old_col_norm, new_col_norm)?;
                }
            }
        }
    }

    if let ast::UpsertDo::Set { sets, where_clause } = &mut upsert.do_clause {
        for set in sets {
            if insert_targets_renamed_table {
                for col_name in &mut set.col_names {
                    if col_name.as_str().eq_ignore_ascii_case(old_col_norm) {
                        if let Some(new_col_norm) = mode.rewritten_name() {
                            *col_name = ast::Name::from_string(new_col_norm);
                        } else {
                            return Err(no_such_column_error(old_col_norm));
                        }
                    }
                }
            }
            apply_expr_for_column_rename(
                mode,
                &mut set.expr,
                ColumnRenameExprTraversal::Normal,
                trigger_table,
                trigger_table_name,
                target_table_name,
                old_col_norm,
                Some(&insert_table_name_norm),
                &[],
                database_id,
                resolver,
            )?;
            if insert_targets_renamed_table {
                if let Some(new_col_norm) = mode.rewritten_name() {
                    rename_excluded_column_refs(&mut set.expr, old_col_norm, new_col_norm)?;
                }
            }
        }
        if let Some(expr) = where_clause {
            apply_expr_for_column_rename(
                mode,
                expr,
                ColumnRenameExprTraversal::Normal,
                trigger_table,
                trigger_table_name,
                target_table_name,
                old_col_norm,
                Some(&insert_table_name_norm),
                &[],
                database_id,
                resolver,
            )?;
            if insert_targets_renamed_table {
                if let Some(new_col_norm) = mode.rewritten_name() {
                    rename_excluded_column_refs(expr, old_col_norm, new_col_norm)?;
                }
            }
        }
    }

    if let Some(next) = &mut upsert.next {
        apply_upsert_for_column_rename(
            mode,
            next,
            trigger_table,
            trigger_table_name,
            target_table_name,
            insert_table_name,
            old_col_norm,
            database_id,
            resolver,
        )?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_select_for_column_rename(
    mode: ColumnRenameMode<'_>,
    select: &mut ast::Select,
    trigger_table: &BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    old_col_norm: &str,
    outer_target_qualifiers: &[String],
    database_id: usize,
    resolver: &Resolver,
) -> Result<()> {
    if let Some(ref mut with_clause) = select.with {
        for cte in &mut with_clause.ctes {
            apply_select_for_column_rename(
                mode,
                &mut cte.select,
                trigger_table,
                trigger_table_name,
                target_table_name,
                old_col_norm,
                outer_target_qualifiers,
                database_id,
                resolver,
            )?;
        }
    }

    apply_one_select_for_column_rename(
        mode,
        &mut select.body.select,
        trigger_table,
        trigger_table_name,
        target_table_name,
        old_col_norm,
        outer_target_qualifiers,
        database_id,
        resolver,
    )?;

    for compound in &mut select.body.compounds {
        apply_one_select_for_column_rename(
            mode,
            &mut compound.select,
            trigger_table,
            trigger_table_name,
            target_table_name,
            old_col_norm,
            outer_target_qualifiers,
            database_id,
            resolver,
        )?;
    }

    let body_target_qualifiers = match &select.body.select {
        ast::OneSelect::Select { from, .. } => merge_target_qualifiers(
            outer_target_qualifiers,
            &from_clause_target_qualifiers(from, target_table_name),
        ),
        _ => outer_target_qualifiers.to_vec(),
    };

    // Per SQLite's ORDER BY resolution rules, a bare identifier that matches
    // an output column alias refers to that alias, not to a column in the
    // FROM clause. Such a reference must not be rewritten — its identity is
    // the alias label, which is independent of the renamed table column.
    let body = &select.body;
    for sorted_col in &mut select.order_by {
        if crate::util::is_order_by_alias_ref(body, &sorted_col.expr, old_col_norm) {
            continue;
        }
        apply_expr_for_column_rename(
            mode,
            &mut sorted_col.expr,
            ColumnRenameExprTraversal::Normal,
            trigger_table,
            trigger_table_name,
            target_table_name,
            old_col_norm,
            None,
            &body_target_qualifiers,
            database_id,
            resolver,
        )?;
    }

    if let Some(ref mut limit) = select.limit {
        apply_expr_for_column_rename(
            mode,
            &mut limit.expr,
            ColumnRenameExprTraversal::Normal,
            trigger_table,
            trigger_table_name,
            target_table_name,
            old_col_norm,
            None,
            &body_target_qualifiers,
            database_id,
            resolver,
        )?;
        if let Some(ref mut offset) = limit.offset {
            apply_expr_for_column_rename(
                mode,
                offset,
                ColumnRenameExprTraversal::Normal,
                trigger_table,
                trigger_table_name,
                target_table_name,
                old_col_norm,
                None,
                &body_target_qualifiers,
                database_id,
                resolver,
            )?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_one_select_for_column_rename(
    mode: ColumnRenameMode<'_>,
    one_select: &mut ast::OneSelect,
    trigger_table: &BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    old_col_norm: &str,
    outer_target_qualifiers: &[String],
    database_id: usize,
    resolver: &Resolver,
) -> Result<()> {
    match one_select {
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            window_clause,
            ..
        } => {
            let visible_target_qualifiers = merge_target_qualifiers(
                outer_target_qualifiers,
                &from_clause_target_qualifiers(from, target_table_name),
            );

            for col in columns {
                if let ast::ResultColumn::Expr(expr, _) = col {
                    apply_result_expr_for_column_rename(
                        mode,
                        expr,
                        trigger_table,
                        trigger_table_name,
                        target_table_name,
                        old_col_norm,
                        &visible_target_qualifiers,
                        database_id,
                        resolver,
                    )?;
                }
            }

            if let Some(ref mut from_clause) = from {
                apply_from_clause_for_column_rename(
                    mode,
                    from_clause,
                    trigger_table,
                    trigger_table_name,
                    target_table_name,
                    old_col_norm,
                    outer_target_qualifiers,
                    database_id,
                    resolver,
                )?;
            }

            if let Some(ref mut where_expr) = where_clause {
                apply_expr_for_column_rename(
                    mode,
                    where_expr,
                    ColumnRenameExprTraversal::Normal,
                    trigger_table,
                    trigger_table_name,
                    target_table_name,
                    old_col_norm,
                    None,
                    &visible_target_qualifiers,
                    database_id,
                    resolver,
                )?;
            }

            if let Some(ref mut group_by) = group_by {
                for expr in &mut group_by.exprs {
                    apply_expr_for_column_rename(
                        mode,
                        expr,
                        ColumnRenameExprTraversal::Normal,
                        trigger_table,
                        trigger_table_name,
                        target_table_name,
                        old_col_norm,
                        None,
                        &visible_target_qualifiers,
                        database_id,
                        resolver,
                    )?;
                }
                if let Some(ref mut having_expr) = group_by.having {
                    apply_expr_for_column_rename(
                        mode,
                        having_expr,
                        ColumnRenameExprTraversal::Normal,
                        trigger_table,
                        trigger_table_name,
                        target_table_name,
                        old_col_norm,
                        None,
                        &visible_target_qualifiers,
                        database_id,
                        resolver,
                    )?;
                }
            }

            for window_def in window_clause {
                apply_window_for_column_rename(
                    mode,
                    &mut window_def.window,
                    trigger_table,
                    trigger_table_name,
                    target_table_name,
                    old_col_norm,
                    &visible_target_qualifiers,
                    database_id,
                    resolver,
                )?;
            }
        }
        ast::OneSelect::Values(values) => {
            for row in values {
                for expr in row {
                    apply_expr_for_column_rename(
                        mode,
                        expr,
                        ColumnRenameExprTraversal::Normal,
                        trigger_table,
                        trigger_table_name,
                        target_table_name,
                        old_col_norm,
                        None,
                        outer_target_qualifiers,
                        database_id,
                        resolver,
                    )?;
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_from_clause_for_column_rename(
    mode: ColumnRenameMode<'_>,
    from_clause: &mut ast::FromClause,
    trigger_table: &BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    old_col_norm: &str,
    visible_target_qualifiers: &[String],
    database_id: usize,
    resolver: &Resolver,
) -> Result<()> {
    apply_select_table_for_column_rename(
        mode,
        &mut from_clause.select,
        trigger_table,
        trigger_table_name,
        target_table_name,
        old_col_norm,
        visible_target_qualifiers,
        database_id,
        resolver,
    )?;

    for join in &mut from_clause.joins {
        apply_select_table_for_column_rename(
            mode,
            &mut join.table,
            trigger_table,
            trigger_table_name,
            target_table_name,
            old_col_norm,
            visible_target_qualifiers,
            database_id,
            resolver,
        )?;
        if let Some(ast::JoinConstraint::On(expr)) = &mut join.constraint {
            apply_expr_for_column_rename(
                mode,
                expr,
                ColumnRenameExprTraversal::Normal,
                trigger_table,
                trigger_table_name,
                target_table_name,
                old_col_norm,
                None,
                visible_target_qualifiers,
                database_id,
                resolver,
            )?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_select_table_for_column_rename(
    mode: ColumnRenameMode<'_>,
    select_table: &mut ast::SelectTable,
    trigger_table: &BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    old_col_norm: &str,
    outer_target_qualifiers: &[String],
    database_id: usize,
    resolver: &Resolver,
) -> Result<()> {
    match select_table {
        ast::SelectTable::Select(select, _) => {
            apply_select_for_column_rename(
                mode,
                select,
                trigger_table,
                trigger_table_name,
                target_table_name,
                old_col_norm,
                outer_target_qualifiers,
                database_id,
                resolver,
            )?;
        }
        ast::SelectTable::Sub(from_clause, _) => {
            apply_from_clause_for_column_rename(
                mode,
                from_clause,
                trigger_table,
                trigger_table_name,
                target_table_name,
                old_col_norm,
                outer_target_qualifiers,
                database_id,
                resolver,
            )?;
        }
        ast::SelectTable::TableCall(_, args, _) => {
            for arg in args {
                apply_expr_for_column_rename(
                    mode,
                    arg,
                    ColumnRenameExprTraversal::Normal,
                    trigger_table,
                    trigger_table_name,
                    target_table_name,
                    old_col_norm,
                    None,
                    outer_target_qualifiers,
                    database_id,
                    resolver,
                )?;
            }
        }
        ast::SelectTable::Table(_, _, _) => {}
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_window_for_column_rename(
    mode: ColumnRenameMode<'_>,
    window: &mut ast::Window,
    trigger_table: &BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    old_col_norm: &str,
    visible_target_qualifiers: &[String],
    database_id: usize,
    resolver: &Resolver,
) -> Result<()> {
    for expr in &mut window.partition_by {
        apply_expr_for_column_rename(
            mode,
            expr,
            ColumnRenameExprTraversal::Normal,
            trigger_table,
            trigger_table_name,
            target_table_name,
            old_col_norm,
            None,
            visible_target_qualifiers,
            database_id,
            resolver,
        )?;
    }

    for sorted_col in &mut window.order_by {
        apply_expr_for_column_rename(
            mode,
            &mut sorted_col.expr,
            ColumnRenameExprTraversal::Normal,
            trigger_table,
            trigger_table_name,
            target_table_name,
            old_col_norm,
            None,
            visible_target_qualifiers,
            database_id,
            resolver,
        )?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_trigger_cmd_for_column_rename(
    mode: ColumnRenameMode<'_>,
    cmd: &mut ast::TriggerCmd,
    trigger_table: &BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    old_col_norm: &str,
    database_id: usize,
    resolver: &Resolver,
) -> Result<()> {
    match cmd {
        ast::TriggerCmd::Update {
            tbl_name,
            sets,
            from,
            where_clause,
            ..
        } => {
            let update_table_name_norm = normalize_ident(tbl_name.as_str());
            let is_renaming_update_table = update_table_name_norm == *target_table_name;
            let from_target_qualifiers = from_clause_target_qualifiers(from, target_table_name);

            if is_renaming_update_table {
                for set in sets.iter_mut() {
                    for col_name in &mut set.col_names {
                        if col_name.as_str().eq_ignore_ascii_case(old_col_norm) {
                            if let Some(new_col_norm) = mode.rewritten_name() {
                                *col_name = ast::Name::from_string(new_col_norm);
                            } else {
                                return Err(no_such_column_error(old_col_norm));
                            }
                        }
                    }
                }
            }

            for set in sets.iter_mut() {
                apply_expr_for_column_rename(
                    mode,
                    &mut set.expr,
                    ColumnRenameExprTraversal::Normal,
                    trigger_table,
                    trigger_table_name,
                    target_table_name,
                    old_col_norm,
                    Some(&update_table_name_norm),
                    &from_target_qualifiers,
                    database_id,
                    resolver,
                )?;
            }

            if let Some(where_expr) = where_clause {
                apply_expr_for_column_rename(
                    mode,
                    where_expr,
                    ColumnRenameExprTraversal::Normal,
                    trigger_table,
                    trigger_table_name,
                    target_table_name,
                    old_col_norm,
                    Some(&update_table_name_norm),
                    &from_target_qualifiers,
                    database_id,
                    resolver,
                )?;
            }

            if let Some(from_clause) = from {
                apply_from_clause_for_column_rename(
                    mode,
                    from_clause,
                    trigger_table,
                    trigger_table_name,
                    target_table_name,
                    old_col_norm,
                    &[],
                    database_id,
                    resolver,
                )?;
            }
        }
        ast::TriggerCmd::Insert {
            tbl_name,
            col_names,
            select,
            upsert,
            ..
        } => {
            if tbl_name.as_str().eq_ignore_ascii_case(target_table_name) {
                for col_name in col_names.iter_mut() {
                    if col_name.as_str().eq_ignore_ascii_case(old_col_norm) {
                        if let Some(new_col_norm) = mode.rewritten_name() {
                            *col_name = ast::Name::from_string(new_col_norm);
                        } else {
                            return Err(no_such_column_error(old_col_norm));
                        }
                    }
                }
            }

            apply_select_for_column_rename(
                mode,
                select,
                trigger_table,
                trigger_table_name,
                target_table_name,
                old_col_norm,
                &[],
                database_id,
                resolver,
            )?;

            if let Some(upsert) = upsert {
                apply_upsert_for_column_rename(
                    mode,
                    upsert,
                    trigger_table,
                    trigger_table_name,
                    target_table_name,
                    tbl_name.as_str(),
                    old_col_norm,
                    database_id,
                    resolver,
                )?;
            }
        }
        ast::TriggerCmd::Delete {
            tbl_name,
            where_clause,
        } => {
            let delete_table_name_norm = normalize_ident(tbl_name.as_str());
            if let Some(where_expr) = where_clause {
                apply_expr_for_column_rename(
                    mode,
                    where_expr,
                    ColumnRenameExprTraversal::Normal,
                    trigger_table,
                    trigger_table_name,
                    target_table_name,
                    old_col_norm,
                    Some(&delete_table_name_norm),
                    &[],
                    database_id,
                    resolver,
                )?;
            }
        }
        ast::TriggerCmd::Select(select) => {
            apply_select_for_column_rename(
                mode,
                select,
                trigger_table,
                trigger_table_name,
                target_table_name,
                old_col_norm,
                &[],
                database_id,
                resolver,
            )?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_trigger_after_column_rename(
    event: &ast::TriggerEvent,
    when_clause: Option<&ast::Expr>,
    commands: &[ast::TriggerCmd],
    trigger_table: &BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    old_col_norm: &str,
    database_id: usize,
    resolver: &Resolver,
) -> Result<()> {
    let mut event = event.clone();
    if trigger_table_name.eq_ignore_ascii_case(target_table_name) {
        if let ast::TriggerEvent::UpdateOf(cols) = &mut event {
            for col in cols {
                if normalize_ident(col.as_str()) == *old_col_norm {
                    return Err(no_such_column_error(old_col_norm));
                }
            }
        }
    }

    if let Some(when_expr) = when_clause {
        let mut when_expr = when_expr.clone();
        apply_expr_for_column_rename(
            ColumnRenameMode::Validate,
            &mut when_expr,
            ColumnRenameExprTraversal::Normal,
            trigger_table,
            trigger_table_name,
            target_table_name,
            old_col_norm,
            None,
            &[],
            database_id,
            resolver,
        )?;
    }

    for cmd in commands {
        let mut cmd = cmd.clone();
        apply_trigger_cmd_for_column_rename(
            ColumnRenameMode::Validate,
            &mut cmd,
            trigger_table,
            trigger_table_name,
            target_table_name,
            old_col_norm,
            database_id,
            resolver,
        )?;
    }
    Ok(())
}

fn rename_excluded_column_refs(
    expr: &mut ast::Expr,
    old_col_norm: &str,
    new_col_norm: &str,
) -> Result<()> {
    walk_expr_mut(expr, &mut |e: &mut ast::Expr| -> Result<WalkControl> {
        if let ast::Expr::Qualified(ns, col) | ast::Expr::DoublyQualified(_, ns, col) = e {
            if ns.as_str().eq_ignore_ascii_case("excluded")
                && col.as_str().eq_ignore_ascii_case(old_col_norm)
            {
                *col = ast::Name::from_string(new_col_norm);
            }
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

fn from_clause_target_qualifiers(
    from: &Option<ast::FromClause>,
    target_table_name: &str,
) -> Vec<String> {
    let Some(from_clause) = from else {
        return Vec::new();
    };

    let mut qualifiers = Vec::new();
    let mut seen = HashSet::default();
    collect_select_table_target_qualifiers(
        &from_clause.select,
        target_table_name,
        &mut qualifiers,
        &mut seen,
    );
    for join in &from_clause.joins {
        collect_select_table_target_qualifiers(
            &join.table,
            target_table_name,
            &mut qualifiers,
            &mut seen,
        );
    }
    qualifiers
}

fn collect_select_table_target_qualifiers(
    select_table: &ast::SelectTable,
    target_table_name: &str,
    qualifiers: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    let ast::SelectTable::Table(name, alias, _) = select_table else {
        return;
    };
    if !name.name.as_str().eq_ignore_ascii_case(target_table_name) {
        return;
    }

    if seen.insert(target_table_name.to_string()) {
        qualifiers.push(target_table_name.to_string());
    }
    if let Some(alias) = alias {
        let alias_norm = normalize_ident(alias.name().as_str());
        if seen.insert(alias_norm.clone()) {
            qualifiers.push(alias_norm);
        }
    }
}

fn merge_target_qualifiers(outer: &[String], local: &[String]) -> Vec<String> {
    let mut seen = HashSet::default();
    let mut merged = Vec::with_capacity(outer.len() + local.len());
    for qualifier in outer {
        if seen.insert(qualifier.as_str()) {
            merged.push(qualifier.clone());
        }
    }
    for qualifier in local {
        if seen.insert(qualifier.as_str()) {
            merged.push(qualifier.clone());
        }
    }
    merged
}

/// Rewrite a trigger command to replace column references.
#[allow(clippy::too_many_arguments)]
fn rewrite_trigger_cmd_for_column_rename(
    cmd: ast::TriggerCmd,
    trigger_table: &BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    old_col_norm: &str,
    new_col_norm: &str,
    database_id: usize,
    resolver: &Resolver,
) -> Result<ast::TriggerCmd> {
    let mut cmd = cmd;
    apply_trigger_cmd_for_column_rename(
        ColumnRenameMode::Rewrite { new_col_norm },
        &mut cmd,
        trigger_table,
        trigger_table_name,
        target_table_name,
        old_col_norm,
        database_id,
        resolver,
    )?;
    Ok(cmd)
}

/// Validate all column references in a trigger after a DROP COLUMN operation.
/// Like SQLite, this re-validates the entire trigger — any unresolvable column
/// reference (whether related to the drop or pre-existing) causes the drop to fail.
///
/// Returns `Some(column_name)` if a bad column reference is found, `None` if all OK.
fn validate_trigger_columns_after_drop(
    trigger: &crate::schema::Trigger,
    altered_table_norm: &str,
    post_drop_table: &BTreeTable,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> Result<Option<String>> {
    let trigger_table_norm = normalize_ident(&trigger.table_name);
    let allow_bare_owning_columns =
        trigger_database_id == altered_database_id && trigger_table_norm == *altered_table_norm;

    // Determine the trigger's owning table columns (post-drop if it's the altered table)
    let owning_table_columns: Option<Vec<String>> = if trigger_database_id == altered_database_id
        && trigger_table_norm == *altered_table_norm
    {
        Some(
            post_drop_table
                .columns()
                .iter()
                .filter_map(|c| c.name.as_deref().map(normalize_ident))
                .collect(),
        )
    } else {
        resolver.with_schema(trigger_database_id, |s| {
            s.get_table(&trigger_table_norm).and_then(|t| {
                t.btree().map(|bt| {
                    bt.columns()
                        .iter()
                        .filter_map(|c| c.name.as_deref().map(normalize_ident))
                        .collect()
                })
            })
        })
    };

    // Validate WHEN clause — NEW/OLD refs resolve against the trigger's owning table
    if let Some(ref when_expr) = trigger.when_clause {
        if owning_table_columns.is_some() {
            if let Some(bad) = validate_expr_column_refs_after_drop(
                when_expr,
                &[],
                &owning_table_columns,
                allow_bare_owning_columns,
                altered_table_norm,
                post_drop_table,
                resolver,
                trigger_database_id,
                altered_database_id,
            )? {
                return Ok(Some(bad));
            }
        }
    }

    for cmd in &trigger.commands {
        match cmd {
            ast::TriggerCmd::Update {
                tbl_name,
                sets,
                from,
                where_clause,
                ..
            } => {
                let cmd_table_norm = normalize_ident(tbl_name.as_str());
                let cmd_table_cols = get_table_columns(
                    &cmd_table_norm,
                    altered_table_norm,
                    post_drop_table,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                    None,
                );
                let mut visible_columns = cmd_table_cols.clone().unwrap_or_default();
                if let Some(from_clause) = from {
                    if let Some(bad) = validate_from_clause_column_refs_after_drop(
                        from_clause,
                        &visible_columns,
                        &owning_table_columns,
                        allow_bare_owning_columns,
                        altered_table_norm,
                        post_drop_table,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )? {
                        return Ok(Some(bad));
                    }
                    visible_columns = merge_column_lists(
                        &visible_columns,
                        &collect_from_clause_visible_columns(
                            from_clause,
                            altered_table_norm,
                            post_drop_table,
                            resolver,
                            trigger_database_id,
                            altered_database_id,
                        ),
                    );
                }
                // Check expressions in SET values and WHERE. They can use the
                // table being updated, tables named by FROM, and NEW/OLD.
                // Note: SET target column names are NOT checked here — SQLite defers
                // that validation to trigger execution time.
                for set in sets {
                    if let Some(bad) = validate_expr_column_refs_after_drop(
                        &set.expr,
                        &visible_columns,
                        &owning_table_columns,
                        allow_bare_owning_columns,
                        altered_table_norm,
                        post_drop_table,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )? {
                        return Ok(Some(bad));
                    }
                }
                if let Some(ref where_expr) = where_clause {
                    if let Some(bad) = validate_expr_column_refs_after_drop(
                        where_expr,
                        &visible_columns,
                        &owning_table_columns,
                        allow_bare_owning_columns,
                        altered_table_norm,
                        post_drop_table,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )? {
                        return Ok(Some(bad));
                    }
                }
            }
            // Note: INSERT column lists are NOT checked — SQLite defers that
            // validation to trigger execution time. But expressions in
            // INSERT ... VALUES and INSERT ... SELECT are checked.
            ast::TriggerCmd::Insert { select, .. } => {
                if let Some(bad) = validate_select_column_refs_after_drop(
                    select,
                    &[],
                    &owning_table_columns,
                    allow_bare_owning_columns,
                    altered_table_norm,
                    post_drop_table,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                )? {
                    return Ok(Some(bad));
                }
            }
            ast::TriggerCmd::Delete {
                tbl_name,
                where_clause,
                ..
            } => {
                let cmd_table_norm = normalize_ident(tbl_name.as_str());
                let cmd_table_cols = get_table_columns(
                    &cmd_table_norm,
                    altered_table_norm,
                    post_drop_table,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                    None,
                );
                if let Some(ref where_expr) = where_clause {
                    if let Some(bad) = validate_expr_column_refs_after_drop(
                        where_expr,
                        cmd_table_cols.as_deref().unwrap_or(&[]),
                        &owning_table_columns,
                        allow_bare_owning_columns,
                        altered_table_norm,
                        post_drop_table,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )? {
                        return Ok(Some(bad));
                    }
                }
            }
            ast::TriggerCmd::Select(select) => {
                if let Some(bad) = validate_select_column_refs_after_drop(
                    select,
                    &[],
                    &owning_table_columns,
                    allow_bare_owning_columns,
                    altered_table_norm,
                    post_drop_table,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                )? {
                    return Ok(Some(bad));
                }
            }
        }
    }

    Ok(None)
}

fn validate_trigger_table_refs_after_rename(
    trigger: &crate::schema::Trigger,
    altered_table_norm: &str,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> Result<Option<String>> {
    if !table_reference_exists_after_rename(
        &trigger.table_name,
        None,
        altered_table_norm,
        resolver,
        trigger_database_id,
        altered_database_id,
    ) {
        return Ok(Some(trigger.table_name.clone()));
    }

    if let Some(when_expr) = &trigger.when_clause {
        if let Some(missing_table) = validate_expr_table_refs_after_rename(
            when_expr,
            altered_table_norm,
            resolver,
            trigger_database_id,
            altered_database_id,
        )? {
            return Ok(Some(missing_table));
        }
    }

    for cmd in &trigger.commands {
        if let Some(missing_table) = validate_trigger_cmd_table_refs_after_rename(
            cmd,
            altered_table_norm,
            resolver,
            trigger_database_id,
            altered_database_id,
        )? {
            return Ok(Some(missing_table));
        }
    }

    Ok(None)
}

fn validate_trigger_cmd_table_refs_after_rename(
    cmd: &ast::TriggerCmd,
    altered_table_norm: &str,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> Result<Option<String>> {
    match cmd {
        ast::TriggerCmd::Update {
            tbl_name,
            sets,
            from,
            where_clause,
            ..
        } => {
            if !table_reference_exists_after_rename(
                tbl_name.as_str(),
                None,
                altered_table_norm,
                resolver,
                trigger_database_id,
                altered_database_id,
            ) {
                return Ok(Some(tbl_name.as_str().to_string()));
            }

            if let Some(from_clause) = from {
                if let Some(missing_table) = validate_from_clause_table_refs_after_rename(
                    from_clause,
                    altered_table_norm,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                )? {
                    return Ok(Some(missing_table));
                }
            }

            for set in sets {
                if let Some(missing_table) = validate_expr_table_refs_after_rename(
                    &set.expr,
                    altered_table_norm,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                )? {
                    return Ok(Some(missing_table));
                }
            }

            if let Some(where_expr) = where_clause {
                if let Some(missing_table) = validate_expr_table_refs_after_rename(
                    where_expr,
                    altered_table_norm,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                )? {
                    return Ok(Some(missing_table));
                }
            }
        }
        ast::TriggerCmd::Insert {
            tbl_name,
            select,
            upsert,
            ..
        } => {
            if !table_reference_exists_after_rename(
                tbl_name.as_str(),
                None,
                altered_table_norm,
                resolver,
                trigger_database_id,
                altered_database_id,
            ) {
                return Ok(Some(tbl_name.as_str().to_string()));
            }

            if let Some(missing_table) = validate_select_table_refs_after_rename(
                select,
                altered_table_norm,
                resolver,
                trigger_database_id,
                altered_database_id,
            )? {
                return Ok(Some(missing_table));
            }

            if let Some(upsert) = upsert {
                if let Some(missing_table) = validate_upsert_table_refs_after_rename(
                    upsert,
                    altered_table_norm,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                )? {
                    return Ok(Some(missing_table));
                }
            }
        }
        ast::TriggerCmd::Delete {
            tbl_name,
            where_clause,
        } => {
            if !table_reference_exists_after_rename(
                tbl_name.as_str(),
                None,
                altered_table_norm,
                resolver,
                trigger_database_id,
                altered_database_id,
            ) {
                return Ok(Some(tbl_name.as_str().to_string()));
            }

            if let Some(where_expr) = where_clause {
                if let Some(missing_table) = validate_expr_table_refs_after_rename(
                    where_expr,
                    altered_table_norm,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                )? {
                    return Ok(Some(missing_table));
                }
            }
        }
        ast::TriggerCmd::Select(select) => {
            if let Some(missing_table) = validate_select_table_refs_after_rename(
                select,
                altered_table_norm,
                resolver,
                trigger_database_id,
                altered_database_id,
            )? {
                return Ok(Some(missing_table));
            }
        }
    }

    Ok(None)
}

fn validate_expr_table_refs_after_rename(
    expr: &ast::Expr,
    altered_table_norm: &str,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> Result<Option<String>> {
    match expr {
        ast::Expr::Exists(select) | ast::Expr::Subquery(select) => {
            return validate_select_table_refs_after_rename(
                select,
                altered_table_norm,
                resolver,
                trigger_database_id,
                altered_database_id,
            );
        }
        ast::Expr::InSelect { lhs, rhs, .. } => {
            if let Some(missing_table) = validate_expr_table_refs_after_rename(
                lhs,
                altered_table_norm,
                resolver,
                trigger_database_id,
                altered_database_id,
            )? {
                return Ok(Some(missing_table));
            }
            return validate_select_table_refs_after_rename(
                rhs,
                altered_table_norm,
                resolver,
                trigger_database_id,
                altered_database_id,
            );
        }
        _ => {}
    }

    let mut missing_table = None;
    walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        if missing_table.is_some() {
            return Ok(WalkControl::SkipChildren);
        }
        match e {
            ast::Expr::Exists(select) | ast::Expr::Subquery(select) => {
                missing_table = validate_select_table_refs_after_rename(
                    select,
                    altered_table_norm,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                )?;
                Ok(WalkControl::SkipChildren)
            }
            ast::Expr::InSelect { lhs, rhs, .. } => {
                missing_table = validate_expr_table_refs_after_rename(
                    lhs,
                    altered_table_norm,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                )?;
                if missing_table.is_none() {
                    missing_table = validate_select_table_refs_after_rename(
                        rhs,
                        altered_table_norm,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )?;
                }
                Ok(WalkControl::SkipChildren)
            }
            _ => Ok(WalkControl::Continue),
        }
    })?;

    Ok(missing_table)
}

fn validate_select_table_refs_after_rename(
    select: &ast::Select,
    altered_table_norm: &str,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> Result<Option<String>> {
    if let Some(with_clause) = &select.with {
        for cte in &with_clause.ctes {
            if let Some(missing_table) = validate_select_table_refs_after_rename(
                &cte.select,
                altered_table_norm,
                resolver,
                trigger_database_id,
                altered_database_id,
            )? {
                return Ok(Some(missing_table));
            }
        }
    }

    if let Some(missing_table) = validate_one_select_table_refs_after_rename(
        &select.body.select,
        altered_table_norm,
        resolver,
        trigger_database_id,
        altered_database_id,
    )? {
        return Ok(Some(missing_table));
    }

    for compound in &select.body.compounds {
        if let Some(missing_table) = validate_one_select_table_refs_after_rename(
            &compound.select,
            altered_table_norm,
            resolver,
            trigger_database_id,
            altered_database_id,
        )? {
            return Ok(Some(missing_table));
        }
    }

    for sorted_col in &select.order_by {
        if let Some(missing_table) = validate_expr_table_refs_after_rename(
            &sorted_col.expr,
            altered_table_norm,
            resolver,
            trigger_database_id,
            altered_database_id,
        )? {
            return Ok(Some(missing_table));
        }
    }

    if let Some(limit) = &select.limit {
        if let Some(missing_table) = validate_expr_table_refs_after_rename(
            &limit.expr,
            altered_table_norm,
            resolver,
            trigger_database_id,
            altered_database_id,
        )? {
            return Ok(Some(missing_table));
        }
        if let Some(offset) = &limit.offset {
            if let Some(missing_table) = validate_expr_table_refs_after_rename(
                offset,
                altered_table_norm,
                resolver,
                trigger_database_id,
                altered_database_id,
            )? {
                return Ok(Some(missing_table));
            }
        }
    }

    Ok(None)
}

fn validate_one_select_table_refs_after_rename(
    one_select: &ast::OneSelect,
    altered_table_norm: &str,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> Result<Option<String>> {
    match one_select {
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            window_clause,
            ..
        } => {
            if let Some(from_clause) = from {
                if let Some(missing_table) = validate_from_clause_table_refs_after_rename(
                    from_clause,
                    altered_table_norm,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                )? {
                    return Ok(Some(missing_table));
                }
            }

            for column in columns {
                if let ast::ResultColumn::Expr(expr, _) = column {
                    if let Some(missing_table) = validate_expr_table_refs_after_rename(
                        expr,
                        altered_table_norm,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )? {
                        return Ok(Some(missing_table));
                    }
                }
            }

            if let Some(where_expr) = where_clause {
                if let Some(missing_table) = validate_expr_table_refs_after_rename(
                    where_expr,
                    altered_table_norm,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                )? {
                    return Ok(Some(missing_table));
                }
            }

            if let Some(group_by) = group_by {
                for expr in &group_by.exprs {
                    if let Some(missing_table) = validate_expr_table_refs_after_rename(
                        expr,
                        altered_table_norm,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )? {
                        return Ok(Some(missing_table));
                    }
                }
                if let Some(having) = &group_by.having {
                    if let Some(missing_table) = validate_expr_table_refs_after_rename(
                        having,
                        altered_table_norm,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )? {
                        return Ok(Some(missing_table));
                    }
                }
            }

            for window in window_clause {
                for partition in &window.window.partition_by {
                    if let Some(missing_table) = validate_expr_table_refs_after_rename(
                        partition,
                        altered_table_norm,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )? {
                        return Ok(Some(missing_table));
                    }
                }
                for order in &window.window.order_by {
                    if let Some(missing_table) = validate_expr_table_refs_after_rename(
                        &order.expr,
                        altered_table_norm,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )? {
                        return Ok(Some(missing_table));
                    }
                }
            }
        }
        ast::OneSelect::Values(rows) => {
            for row in rows {
                for expr in row {
                    if let Some(missing_table) = validate_expr_table_refs_after_rename(
                        expr,
                        altered_table_norm,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )? {
                        return Ok(Some(missing_table));
                    }
                }
            }
        }
    }

    Ok(None)
}

fn validate_from_clause_table_refs_after_rename(
    from_clause: &ast::FromClause,
    altered_table_norm: &str,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> Result<Option<String>> {
    if let Some(missing_table) = validate_select_table_refs_after_rename_in_table(
        &from_clause.select,
        altered_table_norm,
        resolver,
        trigger_database_id,
        altered_database_id,
    )? {
        return Ok(Some(missing_table));
    }

    for join in &from_clause.joins {
        if let Some(missing_table) = validate_select_table_refs_after_rename_in_table(
            &join.table,
            altered_table_norm,
            resolver,
            trigger_database_id,
            altered_database_id,
        )? {
            return Ok(Some(missing_table));
        }

        if let Some(ast::JoinConstraint::On(expr)) = &join.constraint {
            if let Some(missing_table) = validate_expr_table_refs_after_rename(
                expr,
                altered_table_norm,
                resolver,
                trigger_database_id,
                altered_database_id,
            )? {
                return Ok(Some(missing_table));
            }
        }
    }

    Ok(None)
}

fn validate_select_table_refs_after_rename_in_table(
    select_table: &ast::SelectTable,
    altered_table_norm: &str,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> Result<Option<String>> {
    match select_table {
        ast::SelectTable::Table(qualified_name, _, _) => {
            if table_reference_exists_after_rename(
                qualified_name.name.as_str(),
                qualified_name.db_name.as_ref().map(|db| db.as_str()),
                altered_table_norm,
                resolver,
                trigger_database_id,
                altered_database_id,
            ) {
                Ok(None)
            } else {
                Ok(Some(qualified_name.name.as_str().to_string()))
            }
        }
        ast::SelectTable::TableCall(_, args, _) => {
            for arg in args {
                if let Some(missing_table) = validate_expr_table_refs_after_rename(
                    arg,
                    altered_table_norm,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                )? {
                    return Ok(Some(missing_table));
                }
            }
            Ok(None)
        }
        ast::SelectTable::Select(select, _) => validate_select_table_refs_after_rename(
            select,
            altered_table_norm,
            resolver,
            trigger_database_id,
            altered_database_id,
        ),
        ast::SelectTable::Sub(from_clause, _) => validate_from_clause_table_refs_after_rename(
            from_clause,
            altered_table_norm,
            resolver,
            trigger_database_id,
            altered_database_id,
        ),
    }
}

fn validate_upsert_table_refs_after_rename(
    upsert: &ast::Upsert,
    altered_table_norm: &str,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> Result<Option<String>> {
    if let Some(index) = &upsert.index {
        for target in &index.targets {
            if let Some(missing_table) = validate_expr_table_refs_after_rename(
                &target.expr,
                altered_table_norm,
                resolver,
                trigger_database_id,
                altered_database_id,
            )? {
                return Ok(Some(missing_table));
            }
        }
        if let Some(where_clause) = &index.where_clause {
            if let Some(missing_table) = validate_expr_table_refs_after_rename(
                where_clause,
                altered_table_norm,
                resolver,
                trigger_database_id,
                altered_database_id,
            )? {
                return Ok(Some(missing_table));
            }
        }
    }

    if let ast::UpsertDo::Set { sets, where_clause } = &upsert.do_clause {
        for set in sets {
            if let Some(missing_table) = validate_expr_table_refs_after_rename(
                &set.expr,
                altered_table_norm,
                resolver,
                trigger_database_id,
                altered_database_id,
            )? {
                return Ok(Some(missing_table));
            }
        }
        if let Some(expr) = where_clause {
            if let Some(missing_table) = validate_expr_table_refs_after_rename(
                expr,
                altered_table_norm,
                resolver,
                trigger_database_id,
                altered_database_id,
            )? {
                return Ok(Some(missing_table));
            }
        }
    }

    if let Some(next) = &upsert.next {
        return validate_upsert_table_refs_after_rename(
            next,
            altered_table_norm,
            resolver,
            trigger_database_id,
            altered_database_id,
        );
    }

    Ok(None)
}

fn table_reference_exists_after_rename(
    table_name: &str,
    explicit_db_name: Option<&str>,
    altered_table_norm: &str,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> bool {
    let table_name_norm = normalize_ident(table_name);
    let lookup_database_id = if let Some(db_name) = explicit_db_name {
        resolver
            .resolve_database_id(&ast::QualifiedName::fullname(
                ast::Name::exact(db_name.to_string()),
                ast::Name::exact(table_name.to_string()),
            ))
            .ok()
    } else if trigger_database_id == crate::TEMP_DB_ID {
        if resolver.with_schema(crate::TEMP_DB_ID, |s| {
            s.get_table(&table_name_norm).is_some()
                || s.get_view(&table_name_norm).is_some()
                || s.get_materialized_view(&table_name_norm).is_some()
        }) {
            Some(crate::TEMP_DB_ID)
        } else {
            Some(crate::MAIN_DB_ID)
        }
    } else {
        Some(trigger_database_id)
    };

    let Some(lookup_database_id) = lookup_database_id else {
        return false;
    };

    if lookup_database_id == altered_database_id && table_name_norm == *altered_table_norm {
        return true;
    }

    resolver.with_schema(lookup_database_id, |s| {
        s.get_table(&table_name_norm).is_some()
            || s.get_view(&table_name_norm).is_some()
            || s.get_materialized_view(&table_name_norm).is_some()
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_expr_column_refs_after_drop(
    expr: &ast::Expr,
    visible_columns: &[String],
    owning_table_columns: &Option<Vec<String>>,
    allow_bare_owning_columns: bool,
    altered_table_norm: &str,
    post_drop_table: &BTreeTable,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> Result<Option<String>> {
    match expr {
        ast::Expr::Exists(select) | ast::Expr::Subquery(select) => {
            return validate_select_column_refs_after_drop(
                select,
                visible_columns,
                owning_table_columns,
                allow_bare_owning_columns,
                altered_table_norm,
                post_drop_table,
                resolver,
                trigger_database_id,
                altered_database_id,
            );
        }
        ast::Expr::InSelect { lhs, rhs, .. } => {
            let lhs_bad = validate_expr_column_refs_after_drop(
                lhs,
                visible_columns,
                owning_table_columns,
                allow_bare_owning_columns,
                altered_table_norm,
                post_drop_table,
                resolver,
                trigger_database_id,
                altered_database_id,
            )?;
            if lhs_bad.is_some() {
                return Ok(lhs_bad);
            }
            return validate_select_column_refs_after_drop(
                rhs,
                visible_columns,
                owning_table_columns,
                allow_bare_owning_columns,
                altered_table_norm,
                post_drop_table,
                resolver,
                trigger_database_id,
                altered_database_id,
            );
        }
        _ => {
            if let Some(bad) = check_column_ref_valid(
                expr,
                visible_columns,
                owning_table_columns,
                allow_bare_owning_columns,
                altered_table_norm,
                post_drop_table,
                resolver,
                trigger_database_id,
                altered_database_id,
            ) {
                return Ok(Some(bad));
            }
        }
    }

    let mut bad = None;
    walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        if bad.is_some() {
            return Ok(WalkControl::SkipChildren);
        }
        match e {
            ast::Expr::Exists(select) | ast::Expr::Subquery(select) => {
                bad = validate_select_column_refs_after_drop(
                    select,
                    visible_columns,
                    owning_table_columns,
                    allow_bare_owning_columns,
                    altered_table_norm,
                    post_drop_table,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                )?;
                Ok(WalkControl::SkipChildren)
            }
            ast::Expr::InSelect { lhs, rhs, .. } => {
                bad = validate_expr_column_refs_after_drop(
                    lhs,
                    visible_columns,
                    owning_table_columns,
                    allow_bare_owning_columns,
                    altered_table_norm,
                    post_drop_table,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                )?;
                if bad.is_none() {
                    bad = validate_select_column_refs_after_drop(
                        rhs,
                        visible_columns,
                        owning_table_columns,
                        allow_bare_owning_columns,
                        altered_table_norm,
                        post_drop_table,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )?;
                }
                Ok(WalkControl::SkipChildren)
            }
            _ => {
                bad = check_column_ref_valid(
                    e,
                    visible_columns,
                    owning_table_columns,
                    allow_bare_owning_columns,
                    altered_table_norm,
                    post_drop_table,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                );
                if bad.is_some() {
                    Ok(WalkControl::SkipChildren)
                } else {
                    Ok(WalkControl::Continue)
                }
            }
        }
    })?;
    Ok(bad)
}

#[allow(clippy::too_many_arguments)]
fn validate_select_column_refs_after_drop(
    select: &ast::Select,
    outer_visible_columns: &[String],
    owning_table_columns: &Option<Vec<String>>,
    allow_bare_owning_columns: bool,
    altered_table_norm: &str,
    post_drop_table: &BTreeTable,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> Result<Option<String>> {
    if let Some(with_clause) = &select.with {
        for cte in &with_clause.ctes {
            if let Some(bad) = validate_select_column_refs_after_drop(
                &cte.select,
                &[],
                owning_table_columns,
                allow_bare_owning_columns,
                altered_table_norm,
                post_drop_table,
                resolver,
                trigger_database_id,
                altered_database_id,
            )? {
                return Ok(Some(bad));
            }
        }
    }

    if let Some(bad) = validate_one_select_column_refs_after_drop(
        &select.body.select,
        outer_visible_columns,
        owning_table_columns,
        allow_bare_owning_columns,
        altered_table_norm,
        post_drop_table,
        resolver,
        trigger_database_id,
        altered_database_id,
    )? {
        return Ok(Some(bad));
    }

    for compound in &select.body.compounds {
        if let Some(bad) = validate_one_select_column_refs_after_drop(
            &compound.select,
            outer_visible_columns,
            owning_table_columns,
            allow_bare_owning_columns,
            altered_table_norm,
            post_drop_table,
            resolver,
            trigger_database_id,
            altered_database_id,
        )? {
            return Ok(Some(bad));
        }
    }

    let body_visible_columns = match &select.body.select {
        ast::OneSelect::Select { from, .. } => merge_column_lists(
            outer_visible_columns,
            &from
                .as_ref()
                .map(|from| {
                    collect_from_clause_visible_columns(
                        from,
                        altered_table_norm,
                        post_drop_table,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )
                })
                .unwrap_or_default(),
        ),
        ast::OneSelect::Values(_) => outer_visible_columns.to_vec(),
    };

    for sorted_col in &select.order_by {
        if let Some(bad) = validate_expr_column_refs_after_drop(
            &sorted_col.expr,
            &body_visible_columns,
            owning_table_columns,
            allow_bare_owning_columns,
            altered_table_norm,
            post_drop_table,
            resolver,
            trigger_database_id,
            altered_database_id,
        )? {
            return Ok(Some(bad));
        }
    }

    if let Some(limit) = &select.limit {
        if let Some(bad) = validate_expr_column_refs_after_drop(
            &limit.expr,
            &body_visible_columns,
            owning_table_columns,
            allow_bare_owning_columns,
            altered_table_norm,
            post_drop_table,
            resolver,
            trigger_database_id,
            altered_database_id,
        )? {
            return Ok(Some(bad));
        }
        if let Some(offset) = &limit.offset {
            if let Some(bad) = validate_expr_column_refs_after_drop(
                offset,
                &body_visible_columns,
                owning_table_columns,
                allow_bare_owning_columns,
                altered_table_norm,
                post_drop_table,
                resolver,
                trigger_database_id,
                altered_database_id,
            )? {
                return Ok(Some(bad));
            }
        }
    }

    Ok(None)
}

#[allow(clippy::too_many_arguments)]
fn validate_one_select_column_refs_after_drop(
    one_select: &ast::OneSelect,
    outer_visible_columns: &[String],
    owning_table_columns: &Option<Vec<String>>,
    allow_bare_owning_columns: bool,
    altered_table_norm: &str,
    post_drop_table: &BTreeTable,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> Result<Option<String>> {
    match one_select {
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            window_clause,
            ..
        } => {
            let local_visible_columns = merge_column_lists(
                outer_visible_columns,
                &from
                    .as_ref()
                    .map(|from| {
                        collect_from_clause_visible_columns(
                            from,
                            altered_table_norm,
                            post_drop_table,
                            resolver,
                            trigger_database_id,
                            altered_database_id,
                        )
                    })
                    .unwrap_or_default(),
            );

            if let Some(from_clause) = from {
                if let Some(bad) = validate_from_clause_column_refs_after_drop(
                    from_clause,
                    outer_visible_columns,
                    owning_table_columns,
                    allow_bare_owning_columns,
                    altered_table_norm,
                    post_drop_table,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                )? {
                    return Ok(Some(bad));
                }
            }

            for column in columns {
                if let ast::ResultColumn::Expr(expr, _) = column {
                    if let Some(bad) = validate_expr_column_refs_after_drop(
                        expr,
                        &local_visible_columns,
                        owning_table_columns,
                        allow_bare_owning_columns,
                        altered_table_norm,
                        post_drop_table,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )? {
                        return Ok(Some(bad));
                    }
                }
            }

            if let Some(where_clause) = where_clause {
                if let Some(bad) = validate_expr_column_refs_after_drop(
                    where_clause,
                    &local_visible_columns,
                    owning_table_columns,
                    allow_bare_owning_columns,
                    altered_table_norm,
                    post_drop_table,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                )? {
                    return Ok(Some(bad));
                }
            }

            if let Some(group_by) = group_by {
                for expr in &group_by.exprs {
                    if let Some(bad) = validate_expr_column_refs_after_drop(
                        expr,
                        &local_visible_columns,
                        owning_table_columns,
                        allow_bare_owning_columns,
                        altered_table_norm,
                        post_drop_table,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )? {
                        return Ok(Some(bad));
                    }
                }
                if let Some(having) = &group_by.having {
                    if let Some(bad) = validate_expr_column_refs_after_drop(
                        having,
                        &local_visible_columns,
                        owning_table_columns,
                        allow_bare_owning_columns,
                        altered_table_norm,
                        post_drop_table,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )? {
                        return Ok(Some(bad));
                    }
                }
            }

            for window in window_clause {
                for partition in &window.window.partition_by {
                    if let Some(bad) = validate_expr_column_refs_after_drop(
                        partition,
                        &local_visible_columns,
                        owning_table_columns,
                        allow_bare_owning_columns,
                        altered_table_norm,
                        post_drop_table,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )? {
                        return Ok(Some(bad));
                    }
                }
                for order in &window.window.order_by {
                    if let Some(bad) = validate_expr_column_refs_after_drop(
                        &order.expr,
                        &local_visible_columns,
                        owning_table_columns,
                        allow_bare_owning_columns,
                        altered_table_norm,
                        post_drop_table,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )? {
                        return Ok(Some(bad));
                    }
                }
            }
        }
        ast::OneSelect::Values(rows) => {
            for row in rows {
                for expr in row {
                    if let Some(bad) = validate_expr_column_refs_after_drop(
                        expr,
                        outer_visible_columns,
                        owning_table_columns,
                        allow_bare_owning_columns,
                        altered_table_norm,
                        post_drop_table,
                        resolver,
                        trigger_database_id,
                        altered_database_id,
                    )? {
                        return Ok(Some(bad));
                    }
                }
            }
        }
    }

    Ok(None)
}

#[allow(clippy::too_many_arguments)]
fn validate_from_clause_column_refs_after_drop(
    from_clause: &ast::FromClause,
    outer_visible_columns: &[String],
    owning_table_columns: &Option<Vec<String>>,
    allow_bare_owning_columns: bool,
    altered_table_norm: &str,
    post_drop_table: &BTreeTable,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> Result<Option<String>> {
    let mut visible_columns = outer_visible_columns.to_vec();

    if let Some(bad) = validate_select_table_column_refs_after_drop(
        &from_clause.select,
        owning_table_columns,
        allow_bare_owning_columns,
        altered_table_norm,
        post_drop_table,
        resolver,
        trigger_database_id,
        altered_database_id,
    )? {
        return Ok(Some(bad));
    }
    visible_columns = merge_column_lists(
        &visible_columns,
        &collect_select_table_visible_columns(
            &from_clause.select,
            altered_table_norm,
            post_drop_table,
            resolver,
            trigger_database_id,
            altered_database_id,
        ),
    );

    for join in &from_clause.joins {
        if let Some(bad) = validate_select_table_column_refs_after_drop(
            &join.table,
            owning_table_columns,
            allow_bare_owning_columns,
            altered_table_norm,
            post_drop_table,
            resolver,
            trigger_database_id,
            altered_database_id,
        )? {
            return Ok(Some(bad));
        }
        let join_visible_columns = collect_select_table_visible_columns(
            &join.table,
            altered_table_norm,
            post_drop_table,
            resolver,
            trigger_database_id,
            altered_database_id,
        );
        let on_visible_columns = merge_column_lists(&visible_columns, &join_visible_columns);
        if let Some(ast::JoinConstraint::On(expr)) = &join.constraint {
            if let Some(bad) = validate_expr_column_refs_after_drop(
                expr,
                &on_visible_columns,
                owning_table_columns,
                allow_bare_owning_columns,
                altered_table_norm,
                post_drop_table,
                resolver,
                trigger_database_id,
                altered_database_id,
            )? {
                return Ok(Some(bad));
            }
        }
        visible_columns = on_visible_columns;
    }

    Ok(None)
}

#[allow(clippy::too_many_arguments)]
fn validate_select_table_column_refs_after_drop(
    select_table: &ast::SelectTable,
    owning_table_columns: &Option<Vec<String>>,
    allow_bare_owning_columns: bool,
    altered_table_norm: &str,
    post_drop_table: &BTreeTable,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> Result<Option<String>> {
    match select_table {
        ast::SelectTable::Select(select, _) => validate_select_column_refs_after_drop(
            select,
            &[],
            owning_table_columns,
            allow_bare_owning_columns,
            altered_table_norm,
            post_drop_table,
            resolver,
            trigger_database_id,
            altered_database_id,
        ),
        ast::SelectTable::Sub(from_clause, _) => validate_from_clause_column_refs_after_drop(
            from_clause,
            &[],
            owning_table_columns,
            allow_bare_owning_columns,
            altered_table_norm,
            post_drop_table,
            resolver,
            trigger_database_id,
            altered_database_id,
        ),
        ast::SelectTable::TableCall(_, args, _) => {
            for arg in args {
                if let Some(bad) = validate_expr_column_refs_after_drop(
                    arg,
                    &[],
                    owning_table_columns,
                    allow_bare_owning_columns,
                    altered_table_norm,
                    post_drop_table,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                )? {
                    return Ok(Some(bad));
                }
            }
            Ok(None)
        }
        ast::SelectTable::Table(..) => Ok(None),
    }
}

fn collect_from_clause_visible_columns(
    from_clause: &ast::FromClause,
    altered_table_norm: &str,
    post_drop_table: &BTreeTable,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> Vec<String> {
    let mut visible_columns = collect_select_table_visible_columns(
        &from_clause.select,
        altered_table_norm,
        post_drop_table,
        resolver,
        trigger_database_id,
        altered_database_id,
    );
    for join in &from_clause.joins {
        visible_columns = merge_column_lists(
            &visible_columns,
            &collect_select_table_visible_columns(
                &join.table,
                altered_table_norm,
                post_drop_table,
                resolver,
                trigger_database_id,
                altered_database_id,
            ),
        );
    }
    visible_columns
}

fn collect_select_table_visible_columns(
    select_table: &ast::SelectTable,
    altered_table_norm: &str,
    post_drop_table: &BTreeTable,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> Vec<String> {
    match select_table {
        ast::SelectTable::Table(qualified_name, alias, _) => {
            collect_qualified_table_visible_columns(
                qualified_name,
                alias.as_ref(),
                altered_table_norm,
                post_drop_table,
                resolver,
                trigger_database_id,
                altered_database_id,
            )
        }
        ast::SelectTable::TableCall(qualified_name, _, alias) => {
            collect_qualified_table_visible_columns(
                qualified_name,
                alias.as_ref(),
                altered_table_norm,
                post_drop_table,
                resolver,
                trigger_database_id,
                altered_database_id,
            )
        }
        ast::SelectTable::Select(select, _) => collect_select_output_columns(select),
        ast::SelectTable::Sub(from_clause, _) => collect_from_clause_output_columns(from_clause),
    }
}

#[allow(clippy::too_many_arguments)]
fn collect_qualified_table_visible_columns(
    qualified_name: &ast::QualifiedName,
    alias: Option<&ast::As>,
    altered_table_norm: &str,
    post_drop_table: &BTreeTable,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> Vec<String> {
    let columns = get_table_columns(
        &normalize_ident(qualified_name.name.as_str()),
        altered_table_norm,
        post_drop_table,
        resolver,
        trigger_database_id,
        altered_database_id,
        qualified_name.db_name.as_ref().map(|name| name.as_str()),
    )
    .unwrap_or_default();
    let qualifier = alias.map_or_else(
        || normalize_ident(qualified_name.name.as_str()),
        |alias| normalize_ident(alias.name().as_str()),
    );
    let mut visible = columns.clone();
    visible.extend(columns.iter().map(|column| format!("{qualifier}.{column}")));
    if alias.is_none() {
        if let Some(db_name) = &qualified_name.db_name {
            let database = normalize_ident(db_name.as_str());
            let table = normalize_ident(qualified_name.name.as_str());
            visible.extend(
                columns
                    .iter()
                    .map(|column| format!("{database}.{table}.{column}")),
            );
        }
    }
    visible
}

fn collect_select_output_columns(select: &ast::Select) -> Vec<String> {
    collect_one_select_output_columns(&select.body.select)
}

fn collect_from_clause_output_columns(from_clause: &ast::FromClause) -> Vec<String> {
    collect_select_table_visible_columns_from_output(&from_clause.select)
}

fn collect_select_table_visible_columns_from_output(
    select_table: &ast::SelectTable,
) -> Vec<String> {
    match select_table {
        ast::SelectTable::Table(..) | ast::SelectTable::TableCall(..) => Vec::new(),
        ast::SelectTable::Select(select, _) => collect_select_output_columns(select),
        ast::SelectTable::Sub(from_clause, _) => collect_from_clause_output_columns(from_clause),
    }
}

fn collect_one_select_output_columns(one_select: &ast::OneSelect) -> Vec<String> {
    crate::util::output_column_aliases(one_select)
}

/// Check a single expression node for invalid column references after a DROP COLUMN.
/// Returns `Some(bad_column_description)` if invalid, `None` if OK.
#[allow(clippy::too_many_arguments)]
fn check_column_ref_valid(
    e: &ast::Expr,
    valid_columns: &[String],
    owning_table_columns: &Option<Vec<String>>,
    allow_bare_owning_columns: bool,
    altered_table_norm: &str,
    post_drop_table: &BTreeTable,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
) -> Option<String> {
    match e {
        ast::Expr::Id(col) | ast::Expr::Name(col) => {
            let col_norm = normalize_ident(col.as_str());
            let is_visible = valid_columns.contains(&col_norm)
                || (allow_bare_owning_columns
                    && owning_table_columns
                        .as_ref()
                        .is_some_and(|cols| cols.contains(&col_norm)));
            if !is_visible {
                return Some(col.to_string());
            }
        }
        ast::Expr::Qualified(ns, col) => {
            let ns_norm = normalize_ident(ns.as_str());
            let col_norm = normalize_ident(col.as_str());
            if ns_norm.eq_ignore_ascii_case("new") || ns_norm.eq_ignore_ascii_case("old") {
                // NEW.col / OLD.col — validate against owning table columns
                if let Some(ref cols) = owning_table_columns {
                    if !cols.contains(&col_norm) {
                        return Some(format!("{ns}.{col}"));
                    }
                }
            } else {
                let qualified_col = format!("{ns_norm}.{col_norm}");
                if valid_columns.contains(&qualified_col) {
                    return None;
                }
                let qualifier_prefix = format!("{ns_norm}.");
                if valid_columns
                    .iter()
                    .any(|valid| valid.starts_with(&qualifier_prefix))
                {
                    return Some(format!("{ns}.{col}"));
                }
                // table.col — validate against that table's columns
                let table_cols = get_table_columns(
                    &ns_norm,
                    altered_table_norm,
                    post_drop_table,
                    resolver,
                    trigger_database_id,
                    altered_database_id,
                    None,
                );
                if let Some(cols) = table_cols {
                    if !cols.contains(&col_norm) {
                        return Some(format!("{ns}.{col}"));
                    }
                }
            }
        }
        ast::Expr::DoublyQualified(db_name, table_name, col) => {
            let database_norm = normalize_ident(db_name.as_str());
            let table_norm = normalize_ident(table_name.as_str());
            let col_norm = normalize_ident(col.as_str());
            let qualifier_prefix = format!("{database_norm}.{table_norm}.");
            let qualified_col = format!("{qualifier_prefix}{col_norm}");
            if valid_columns.contains(&qualified_col) {
                return None;
            }
            if valid_columns
                .iter()
                .any(|valid| valid.starts_with(&qualifier_prefix))
            {
                return Some(format!("{db_name}.{table_name}.{col}"));
            }
            let table_cols = get_table_columns(
                &table_norm,
                altered_table_norm,
                post_drop_table,
                resolver,
                trigger_database_id,
                altered_database_id,
                Some(db_name.as_str()),
            );
            if let Some(cols) = table_cols {
                if !cols.contains(&col_norm) {
                    return Some(format!("{db_name}.{table_name}.{col}"));
                }
            }
        }
        _ => {}
    }
    None
}

/// Get the column names for a table, using the post-drop schema if it's the altered table.
fn get_table_columns(
    table_name_norm: &str,
    altered_table_norm: &str,
    post_drop_table: &BTreeTable,
    resolver: &Resolver,
    trigger_database_id: usize,
    altered_database_id: usize,
    explicit_db_name: Option<&str>,
) -> Option<Vec<String>> {
    let lookup_database_id = if let Some(db_name) = explicit_db_name {
        resolver
            .resolve_database_id(&ast::QualifiedName::fullname(
                ast::Name::exact(db_name.to_string()),
                ast::Name::exact(table_name_norm.to_string()),
            ))
            .ok()?
    } else if trigger_database_id == crate::TEMP_DB_ID {
        if resolver.with_schema(crate::TEMP_DB_ID, |s| {
            s.get_table(table_name_norm).is_some()
        }) {
            crate::TEMP_DB_ID
        } else {
            crate::MAIN_DB_ID
        }
    } else {
        trigger_database_id
    };

    if lookup_database_id == altered_database_id && table_name_norm == altered_table_norm {
        Some(
            post_drop_table
                .columns()
                .iter()
                .filter_map(|c| c.name.as_deref().map(normalize_ident))
                .collect(),
        )
    } else {
        resolver.with_schema(lookup_database_id, |s| {
            s.get_table(table_name_norm).and_then(|t| {
                t.btree().map(|bt| {
                    bt.columns()
                        .iter()
                        .filter_map(|c| c.name.as_deref().map(normalize_ident))
                        .collect()
                })
            })
        })
    }
}

fn resolve_trigger_command_table_for_alter(
    resolver: &Resolver,
    trigger_database_id: usize,
    table_name_norm: &str,
) -> Option<Arc<BTreeTable>> {
    if trigger_database_id == crate::TEMP_DB_ID {
        resolver
            .with_schema(crate::TEMP_DB_ID, |schema| {
                schema.get_btree_table(table_name_norm)
            })
            .or_else(|| {
                resolver.with_schema(crate::MAIN_DB_ID, |schema| {
                    schema.get_btree_table(table_name_norm)
                })
            })
    } else {
        resolver.with_schema(trigger_database_id, |schema| {
            schema.get_btree_table(table_name_norm)
        })
    }
}

fn merge_column_lists(left: &[String], right: &[String]) -> Vec<String> {
    let mut result = left.to_vec();
    for col in right {
        if !result.contains(col) {
            result.push(col.clone());
        }
    }
    result
}
