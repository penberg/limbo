use rustc_hash::FxHashMap as HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use turso_parser::ast::{self, TriggerEvent, TriggerTime, Upsert};

use super::emitter::gencol::compute_virtual_columns;
use crate::alloc::TursoIteratorExt;
use crate::error::SQLITE_CONSTRAINT_PRIMARYKEY;
use crate::schema::{BTreeTable, ColumnLayout, IndexColumn, ROWID_SENTINEL};
use crate::translate::emitter::{emit_check_constraints, emit_make_record, UpdateRowSource};
use crate::translate::expr::{walk_expr, WalkControl};
use crate::translate::fkeys::{
    emit_fk_child_update_counters, emit_fk_update_parent_actions, fire_fk_update_actions,
    ParentKeyNewProbeMode,
};
use crate::translate::insert::{format_unique_violation_desc, InsertEmitCtx};
use crate::translate::plan::ColumnMask;
use crate::translate::planner::ROWID_STRS;
use crate::translate::trigger_exec::{
    fire_trigger, get_triggers_including_temp, has_triggers_including_temp, TriggerContext,
};
use crate::vdbe::insn::{to_u32, CmpInsFlags};
use crate::{
    bail_parse_error,
    error::SQLITE_CONSTRAINT_NOTNULL,
    schema::{Index, Schema, Table},
    translate::{
        emitter::{
            emit_cdc_full_record, emit_cdc_insns, emit_cdc_patch_record, OperationMode, Resolver,
        },
        expr::{
            emit_returning_results, emit_table_column, translate_expr,
            translate_expr_no_constant_opt, walk_expr_mut, NoConstantOptReason,
        },
        insert::Insertion,
        plan::{ResultSetColumn, TableReferences},
    },
    util::{exprs_are_equivalent, normalize_ident},
    vdbe::{
        affinity::Affinity,
        builder::{DmlColumnContext, ProgramBuilder},
        insn::{IdxInsertFlags, InsertFlags, Insn},
    },
};
use crate::{CaptureDataChangesExt, Connection};
// The following comment is copied directly from SQLite source and should be used as a guiding light
// whenever we encounter compatibility bugs related to conflict clause handling:

/* UNIQUE and PRIMARY KEY constraints should be handled in the following
** order:
**
**   (1)  OE_Update
**   (2)  OE_Abort, OE_Fail, OE_Rollback, OE_Ignore
**   (3)  OE_Replace
**
** OE_Fail and OE_Ignore must happen before any changes are made.
** OE_Update guarantees that only a single row will change, so it
** must happen before OE_Replace.  Technically, OE_Abort and OE_Rollback
** could happen in any order, but they are grouped up front for
** convenience.
**
** 2018-08-14: Ticket https://www.sqlite.org/src/info/908f001483982c43
** The order of constraints used to have OE_Update as (2) and OE_Abort
** and so forth as (1). But apparently PostgreSQL checks the OE_Update
** constraint before any others, so it had to be moved.
**
** Constraint checking code is generated in this order:
**   (A)  The rowid constraint
**   (B)  Unique index constraints that do not have OE_Replace as their
**        default conflict resolution strategy
**   (C)  Unique index that do use OE_Replace by default.
**
** The ordering of (2) and (3) is accomplished by making sure the linked
** list of indexes attached to a table puts all OE_Replace indexes last
** in the list.  See sqlite3CreateIndex() for where that happens.
*/

/// A ConflictTarget is extracted from each ON CONFLICT target,
// e.g. INSERT INTO x(a) ON CONFLICT  *(a COLLATE nocase)*
#[derive(Debug, Clone)]
pub struct ConflictTarget {
    /// The normalized column name in question
    col_name: String,
    /// Possible collation name, normalized to lowercase
    collate: Option<String>,
}

// Extract `(column, optional_collate)` from an ON CONFLICT target Expr.
// Accepts: Id, Qualified, DoublyQualified, Parenthesized, Collate
fn extract_conflict_target(e: &ast::Expr) -> Option<ConflictTarget> {
    match e {
        ast::Expr::Collate(inner, collation) => {
            let mut conflict_target = extract_conflict_target(inner.as_ref())?;
            let collation_str = collation.as_str();
            conflict_target.collate = Some(collation_str.to_ascii_lowercase());
            Some(conflict_target)
        }
        ast::Expr::Parenthesized(v) if v.len() == 1 => extract_conflict_target(&v[0]),

        ast::Expr::Id(name) => Some(ConflictTarget {
            col_name: normalize_ident(name.as_str()),
            collate: None,
        }),
        // t.a or db.t.a: accept ident or quoted in the column position
        ast::Expr::Qualified(_, col) | ast::Expr::DoublyQualified(_, _, col) => {
            let cname = col.as_str();
            Some(ConflictTarget {
                col_name: normalize_ident(cname),
                collate: None,
            })
        }
        _ => None,
    }
}

/// For an ON CONFLICT target that is an expression (not a simple column),
/// extract the inner expression and an optional COLLATE annotation.
/// E.g. `lower(val) COLLATE nocase` -> (lower(val), Some("nocase"))
fn extract_target_expr(e: &ast::Expr) -> (&ast::Expr, Option<String>) {
    match e {
        ast::Expr::Collate(inner, c) => {
            let (expr, _) = extract_target_expr(inner.as_ref());
            (expr, Some(c.as_str().to_ascii_lowercase()))
        }
        ast::Expr::Parenthesized(v) if v.len() == 1 => extract_target_expr(&v[0]),
        _ => (e, None),
    }
}

// Return the index key’s effective collation.
// If `idx_col.collation` is None, fall back to the column default or "BINARY".
fn effective_collation_for_index_col(idx_col: &IndexColumn, table: &Table) -> String {
    if let Some(c) = idx_col.collation.as_ref() {
        return c.to_string().to_ascii_lowercase();
    }
    // Otherwise use the table default, or default to BINARY
    table
        .get_column_by_name(&idx_col.name)
        .map(|s| s.1.collation().to_string())
        .unwrap_or_else(|| "binary".to_string())
}

/// Match ON CONFLICT target to the PRIMARY KEY/rowid alias.
pub fn upsert_matches_rowid_alias(upsert: &Upsert, table: &Table) -> bool {
    let Some(t) = upsert.index.as_ref() else {
        // omitted target matches everything, CatchAll handled elsewhere
        return false;
    };
    if t.targets.len() != 1 {
        return false;
    }
    // Only treat as PK if the PK is the rowid alias (INTEGER PRIMARY KEY)
    let pk = table.columns().iter().find(|c| c.is_rowid_alias());
    if let Some(pkcol) = pk {
        extract_conflict_target(&t.targets[0].expr).is_some_and(|tk| {
            tk.col_name
                .eq_ignore_ascii_case(pkcol.name.as_ref().unwrap_or(&String::new()))
        })
    } else {
        false
    }
}

/// Returns array of chaned column indicies and whether rowid was changed.
fn collect_changed_cols(
    table: &Table,
    set_pairs: &[(usize, Box<ast::Expr>)],
) -> (ColumnMask, bool) {
    let mut cols_changed = ColumnMask::default();
    let mut rowid_changed = false;
    for (col_idx, _) in set_pairs {
        if let Some(c) = table.columns().get(*col_idx) {
            if c.is_rowid_alias() {
                rowid_changed = true;
            } else {
                cols_changed.set(*col_idx).expect("TODO: alloc error");
            }
        }
    }
    (cols_changed, rowid_changed)
}

#[inline]
fn upsert_index_is_affected(
    table: &Table,
    idx: &Index,
    directly_changed_cols: &ColumnMask,
    rowid_changed: bool,
) -> crate::Result<bool> {
    if rowid_changed {
        return Ok(true);
    }

    for c in referenced_index_cols(idx, table)? {
        if directly_changed_cols.get(c) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Collect the set of columns referenced by the partial WHERE (empty if none), or
/// by the expression of any IndexColumn on the index. Virtual-column references
/// are expanded to their transitive stored-column dependencies.
fn referenced_index_cols(idx: &Index, table: &Table) -> crate::Result<ColumnMask> {
    let mut referenced_cols = ColumnMask::default();

    if let Some(expr) = &idx.where_clause {
        index_expression_cols(table, &mut referenced_cols, expr);
    }
    for ic in &idx.columns {
        if let Some(expr) = &ic.expr {
            index_expression_cols(table, &mut referenced_cols, expr);
        } else {
            referenced_cols.set(ic.pos_in_table)?;
        }
    }
    match table.btree() {
        Some(btree) => btree.dependencies_of_columns(referenced_cols),
        None => Ok(referenced_cols),
    }
}

/// Columns referenced by any expression index columns on the index.
fn index_expression_cols(table: &Table, out: &mut ColumnMask, expr: &ast::Expr) {
    use ast::Expr;
    let _ = walk_expr(expr, &mut |e: &ast::Expr| -> crate::Result<WalkControl> {
        match e {
            Expr::Id(n) => {
                if let Some((i, _)) = table.get_column_by_name(&normalize_ident(n.as_str())) {
                    out.set(i)?;
                } else if ROWID_STRS
                    .iter()
                    .any(|r| r.eq_ignore_ascii_case(n.as_str()))
                {
                    if let Some(rowid_pos) = table
                        .btree()
                        .and_then(|t| t.get_rowid_alias_column().map(|(p, _)| p))
                    {
                        out.set(rowid_pos)?;
                    }
                }
            }
            Expr::Qualified(ns, c) | Expr::DoublyQualified(_, ns, c) => {
                let nsn = normalize_ident(ns.as_str());
                let tname = normalize_ident(table.get_name());
                if nsn.eq_ignore_ascii_case(&tname) {
                    if let Some((i, _)) = table.get_column_by_name(&normalize_ident(c.as_str())) {
                        out.set(i)?;
                    }
                }
            }
            Expr::Column { column, .. } => out.set(*column)?,
            _ => {}
        }
        Ok(WalkControl::Continue)
    });
}

fn bind_partial_index_where_expr(expr: &mut ast::Expr, table: &Table) {
    let table_name = normalize_ident(table.get_name());

    let _ = walk_expr_mut(
        expr,
        &mut |e: &mut ast::Expr| -> crate::Result<WalkControl> {
            match e {
                ast::Expr::Id(name) => {
                    if let Some((column, col)) =
                        table.get_column_by_name(&normalize_ident(name.as_str()))
                    {
                        *e = ast::Expr::Column {
                            database: None,
                            table: ast::TableInternalId::SELF_TABLE,
                            column,
                            is_rowid_alias: col.is_rowid_alias(),
                        };
                    } else if ROWID_STRS
                        .iter()
                        .any(|rowid| rowid.eq_ignore_ascii_case(name.as_str()))
                    {
                        *e = ast::Expr::RowId {
                            database: None,
                            table: ast::TableInternalId::SELF_TABLE,
                        };
                    }
                }
                ast::Expr::Qualified(ns, col) | ast::Expr::DoublyQualified(_, ns, col)
                    if normalize_ident(ns.as_str()).eq_ignore_ascii_case(&table_name) =>
                {
                    if let Some((column, table_col)) =
                        table.get_column_by_name(&normalize_ident(col.as_str()))
                    {
                        *e = ast::Expr::Column {
                            database: None,
                            table: ast::TableInternalId::SELF_TABLE,
                            column,
                            is_rowid_alias: table_col.is_rowid_alias(),
                        };
                    } else if ROWID_STRS
                        .iter()
                        .any(|rowid| rowid.eq_ignore_ascii_case(col.as_str()))
                    {
                        *e = ast::Expr::RowId {
                            database: None,
                            table: ast::TableInternalId::SELF_TABLE,
                        };
                    }
                }
                _ => {}
            }
            Ok(WalkControl::Continue)
        },
    );
}

fn partial_index_where_clauses_match(
    target_where: &ast::Expr,
    index_where: &ast::Expr,
    table: &Table,
) -> bool {
    let mut target_where = target_where.clone();
    let mut index_where = index_where.clone();
    // TODO: ideally we would have a binding step where we wouldn't need to do these ad-hoc bindings just to compare exprs
    bind_partial_index_where_expr(&mut target_where, table);
    bind_partial_index_where_expr(&mut index_where, table);
    exprs_are_equivalent(&target_where, &index_where)
}

/// Match ON CONFLICT target to a UNIQUE index, *ignoring order* but requiring
/// exact coverage (same column multiset). If the target specifies a COLLATED
/// column, the collation must match the index column's effective collation.
/// If the target omits collation, any index collation is accepted.
/// Partial indexes require a matching conflict-target WHERE clause.
pub fn upsert_matches_index(upsert: &Upsert, index: &Index, table: &Table) -> bool {
    let Some(target) = upsert.index.as_ref() else {
        return true;
    };

    let partial_index_predicate_matches = match (&index.where_clause, &target.where_clause) {
        (Some(index_where), Some(target_where)) => {
            partial_index_where_clauses_match(target_where.as_ref(), index_where.as_ref(), table)
        }
        (Some(_), None) => false,
        (None, _) => true,
    };

    if !index.unique
        || !partial_index_predicate_matches
        || target.targets.len() != index.columns.len()
    {
        return false;
    }

    // Track which index columns have been matched (consumed).
    let mut matched = ColumnMask::default();

    for te in &target.targets {
        let mut found = None;

        if let Some(conflict_target) = extract_conflict_target(&te.expr) {
            // Simple column reference target: match by name and collation.
            let tname = &conflict_target.col_name;
            for (i, ic) in index.columns.iter().enumerate() {
                if matched.get(i) || ic.expr.is_some() {
                    continue;
                }
                let iname = normalize_ident(&ic.name);
                let icoll = effective_collation_for_index_col(ic, table);
                if tname.eq_ignore_ascii_case(&iname)
                    && match conflict_target.collate.as_ref() {
                        Some(c) => c.eq_ignore_ascii_case(&icoll),
                        None => true, // unspecified collation -> accept any
                    }
                {
                    found = Some(i);
                    break;
                }
            }
        } else {
            // Expression target (e.g. lower(val)): match against expression index
            // columns using semantic equivalence.
            let (target_expr, target_collate) = extract_target_expr(&te.expr);
            for (i, ic) in index.columns.iter().enumerate() {
                if matched.get(i) {
                    continue;
                }
                if let Some(idx_expr) = &ic.expr {
                    if exprs_are_equivalent(target_expr, idx_expr) {
                        // If target specifies a collation, it must match the index column's.
                        if let Some(ref tc) = target_collate {
                            let icoll = effective_collation_for_index_col(ic, table);
                            if !tc.eq_ignore_ascii_case(&icoll) {
                                continue;
                            }
                        }
                        found = Some(i);
                        break;
                    }
                }
            }
        }

        if let Some(i) = found {
            matched.set(i).expect("TODO: alloc error");
        } else {
            return false;
        }
    }
    // All target columns matched exactly once, and all index columns consumed
    matched.count() == index.columns.len()
}

#[derive(Clone, Debug)]
pub enum ResolvedUpsertTarget {
    // ON CONFLICT DO
    CatchAll,
    // ON CONFLICT(pk) DO
    PrimaryKey,
    // matched this non-partial UNIQUE index
    Index(Arc<Index>),
}

pub fn resolve_upsert_target(
    schema: &Schema,
    table: &Table,
    upsert: &Upsert,
) -> crate::Result<ResolvedUpsertTarget> {
    // Omitted target, catch-all
    let Some(target) = upsert.index.as_ref() else {
        return Ok(ResolvedUpsertTarget::CatchAll);
    };
    // SQLite rejects explicit NULLS FIRST/LAST in conflict targets
    // (sqlite3UpsertAnalyzeTarget -> sqlite3HasExplicitNulls).
    crate::translate::index::reject_explicit_nulls(&target.targets)?;

    // Targeted: must match PK, only if PK is a rowid alias
    if upsert_matches_rowid_alias(upsert, table) {
        return Ok(ResolvedUpsertTarget::PrimaryKey);
    }

    // Otherwise match a UNIQUE index, also covering non-rowid PRIMARY KEYs
    for idx in schema.get_indices(table.get_name()) {
        if idx.unique && upsert_matches_index(upsert, idx, table) {
            return Ok(ResolvedUpsertTarget::Index(Arc::clone(idx)));
        }
    }
    crate::bail_parse_error!(
        "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint"
    );
}

#[allow(clippy::too_many_arguments)]
/// Emit the bytecode to implement the `DO UPDATE` arm of an UPSERT.
///
/// This routine is entered after the caller has determined that an INSERT
/// would violate a UNIQUE/PRIMARY KEY constraint and that the user requested
/// `ON CONFLICT ... DO UPDATE`.
///
/// High-level flow:
/// 1. Seek to the conflicting row by rowid and load the current row snapshot
///    into a contiguous set of registers.
/// 2. Optionally duplicate CURRENT into BEFORE* (for index rebuild and CDC).
/// 3. Copy CURRENT into NEW, then evaluate SET expressions into NEW,
///    with all references to the target table columns rewritten to read from
///    the CURRENT registers (per SQLite semantics).
/// 4. Enforce NOT NULL constraints and (if STRICT) type checks on NEW.
/// 5. Rebuild indexes (delete keys using BEFORE, insert keys using NEW).
/// 6. Rewrite the table row payload at the same rowid with NEW.
/// 7. Emit CDC rows and RETURNING output if requested.
/// 8. Jump to `row_done_label`.
///
/// Semantics reference: https://sqlite.org/lang_upsert.html
/// Column references in the DO UPDATE expressions refer to the original
/// (unchanged) row. To refer to would-be inserted values, use `excluded.x`.
#[allow(clippy::too_many_arguments)]
pub fn emit_upsert(
    program: &mut ProgramBuilder,
    table: &Table,
    ctx: &InsertEmitCtx,
    insertion: &Insertion,
    set_pairs: &mut [(usize, Box<ast::Expr>)],
    where_clause: &mut Option<Box<ast::Expr>>,
    resolver: &mut Resolver,
    returning: &mut [ResultSetColumn],
    connection: &Arc<Connection>,
    table_references: &mut TableReferences,
    table_alias: Option<&str>,
) -> crate::Result<()> {
    // Seek & snapshot CURRENT
    program.emit_insn(Insn::SeekRowid {
        cursor_id: ctx.cursor_id,
        src_reg: ctx.conflict_rowid_reg,
        target_pc: ctx.loop_labels.row_done,
    });
    let num_cols = ctx.table.columns().len();
    let layout = ctx.table.column_layout()?;

    let table_ref_id = table_references
        .joined_tables()
        .first()
        .expect("upsert must have a target table")
        .internal_id;
    let current_start = program.alloc_registers(num_cols);
    for i in 0..num_cols {
        let col = &table.columns()[i];
        let reg = layout.to_register(current_start, i);
        emit_table_column(
            program,
            ctx.cursor_id,
            table_ref_id,
            table_references,
            col,
            i,
            reg,
            resolver,
        )?;
    }

    // BEFORE for index maintenance / CDC
    let before_start = if ctx.cdc_table.is_some() || !ctx.idx_cursors.is_empty() {
        let s = program.alloc_registers(num_cols);
        program.emit_insn(Insn::Copy {
            src_reg: current_start,
            dst_reg: s,
            extra_amount: num_cols - 1,
        });
        Some(s)
    } else {
        None
    };

    // NEW = CURRENT, then apply SET
    let new_start = program.alloc_registers(num_cols);
    program.emit_insn(Insn::Copy {
        src_reg: current_start,
        dst_reg: new_start,
        extra_amount: num_cols - 1,
    });

    // For STRICT tables with custom types, values loaded from disk (current_start)
    // are in encoded form. We need decoded copies so that:
    // - WHERE clause expressions see user-facing values (Bug 13)
    // - SET expressions referencing t1.column see user-facing values
    // - excluded.column references also see decoded values (Bug 7)
    // current_start itself stays encoded for trigger OLD registers and before_start.
    // After SET evaluation, we encode ALL columns in new_start before writing to disk.
    let (decoded_current_start, excluded_decoded_start) = if let Some(bt) = table.btree() {
        if bt.is_strict {
            // Create decoded copy of current_start for WHERE/SET expressions
            let decoded_current = program.alloc_registers(num_cols);
            program.emit_insn(Insn::Copy {
                src_reg: current_start,
                dst_reg: decoded_current,
                extra_amount: num_cols - 1,
            });
            crate::translate::expr::emit_custom_type_decode_columns(
                program,
                resolver,
                bt.columns(),
                decoded_current,
                None,
                &layout,
            )?;
            // Decode new_start in-place (was copied from encoded current_start;
            // after SET applies decoded values, we encode ALL columns)
            crate::translate::expr::emit_custom_type_decode_columns(
                program,
                resolver,
                bt.columns(),
                new_start,
                None,
                &layout,
            )?;
            // Create decoded copies of excluded (insertion) registers so that
            // excluded.column references see user-facing values
            let decoded_excluded = program.alloc_registers(num_cols);
            program.emit_insn(Insn::Copy {
                src_reg: insertion.first_col_register(),
                dst_reg: decoded_excluded,
                extra_amount: num_cols - 1,
            });
            crate::translate::expr::emit_custom_type_decode_columns(
                program,
                resolver,
                bt.columns(),
                decoded_excluded,
                None,
                &layout,
            )?;
            (Some(decoded_current), Some(decoded_excluded))
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    // For WHERE and SET, use decoded_current_start if available (STRICT with custom types),
    // otherwise fall back to current_start (already decoded or non-custom-type).
    let expr_current_start = decoded_current_start.unwrap_or(current_start);

    // rewrite_expr_to_registers turns column references into bare
    // Expr::Register nodes, which lose the column's affinity and implicit
    // collation. Comparisons in the WHERE/SET expressions must still apply
    // them (`int_col < '2'` compares numerically, a NOCASE column compares
    // case-insensitively), so record both per register for the conflicting
    // row image and its rowid. The excluded (insertion) registers are
    // already recorded by the enclosing INSERT translation, except for the
    // decoded copies made for STRICT custom-type tables. Cleared wholesale
    // at the end of the enclosing INSERT translation.
    for (idx, col) in table.columns().iter().enumerate() {
        if col.is_rowid_alias() {
            // The rewrite maps rowid-alias references to conflict_rowid_reg;
            // the image register for the alias column is never referenced.
            continue;
        }
        let reg = layout.to_register(expr_current_start, idx);
        resolver.register_affinities.insert(reg, col.affinity());
        resolver.register_collations.insert(reg, col.collation());
        if let Some(decoded_start) = excluded_decoded_start {
            let excluded_reg = layout.to_register(decoded_start, idx);
            resolver
                .register_affinities
                .insert(excluded_reg, col.affinity());
            resolver
                .register_collations
                .insert(excluded_reg, col.collation());
        }
    }
    resolver
        .register_affinities
        .insert(ctx.conflict_rowid_reg, Affinity::Integer);

    // WHERE on target row
    if let Some(pred) = where_clause.as_mut() {
        rewrite_expr_to_registers(
            pred,
            table,
            expr_current_start,
            ctx.conflict_rowid_reg,
            Some(table.get_name()),
            table_alias,
            Some(insertion),
            true,
            excluded_decoded_start,
            &layout,
        )?;
        let pr = program.alloc_register();
        translate_expr(program, None, pred, pr, resolver)?;
        program.emit_insn(Insn::IfNot {
            reg: pr,
            target_pc: ctx.loop_labels.row_done,
            jump_if_null: true,
        });
    }

    // Apply SET; capture rowid change if any
    let mut new_rowid_reg: Option<usize> = None;
    for (col_idx, expr) in set_pairs.iter_mut() {
        rewrite_expr_to_registers(
            expr,
            table,
            expr_current_start,
            ctx.conflict_rowid_reg,
            Some(table.get_name()),
            table_alias,
            Some(insertion),
            true,
            excluded_decoded_start,
            &layout,
        )?;
        // Save/restore target_union_type so union_value() resolves tags
        // against this column's union type. See ProgramBuilder::target_union_type.
        let col = &table.columns()[*col_idx];
        let union_td = resolver
            .schema()
            .get_type_def_unchecked(&col.ty_str)
            .filter(|td| td.is_union())
            .cloned();
        let prev_union = program.target_union_type.take();
        program.target_union_type = union_td;
        let translate_result = translate_expr_no_constant_opt(
            program,
            None,
            expr,
            layout.to_register(new_start, *col_idx),
            resolver,
            NoConstantOptReason::RegisterReuse,
        );
        program.target_union_type = prev_union;
        translate_result?;
        if col.notnull() && !col.is_rowid_alias() {
            program.emit_insn(Insn::HaltIfNull {
                target_reg: layout.to_register(new_start, *col_idx),
                err_code: SQLITE_CONSTRAINT_NOTNULL,
                description: String::from(table.get_name()) + "." + col.name.as_ref().unwrap(),
            });
        }
        if col.is_rowid_alias() {
            // Must be integer; remember the NEW rowid value
            let r = program.alloc_register();
            program.emit_insn(Insn::Copy {
                src_reg: layout.to_register(new_start, *col_idx),
                dst_reg: r,
                extra_amount: 0,
            });
            program.emit_insn(Insn::MustBeInt {
                reg: r,
                target_pc: None,
            });
            new_rowid_reg = Some(r);
        }
    }

    // Recompute virtual columns for the new row after SET clauses have modified base columns.
    // This must happen before CHECK constraints, triggers, and index updates.
    if ctx.table.has_virtual_columns() {
        let rowid_reg = new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg);
        let dml_ctx =
            DmlColumnContext::layout(ctx.table.columns(), new_start, rowid_reg, layout.clone());
        compute_virtual_columns(
            program,
            &ctx.table.columns_topo_sort()?,
            &dml_ctx,
            resolver,
            ctx.table,
        )?;
    }

    if let Some(bt) = table.btree() {
        if bt.is_strict {
            // Pre-encode TypeCheck: all columns are decoded (user-facing) at this point.
            program.emit_insn(Insn::TypeCheck {
                start_reg: new_start,
                count: layout.num_non_virtual_cols(),
                check_generated: true,
                table_reference: BTreeTable::input_type_check_table_ref(
                    &bt,
                    resolver.schema(),
                    None,
                )?,
            });

            // Encode ALL columns. Both non-SET columns (decoded from disk above)
            // and SET columns (user-facing values from expressions) need encoding
            // before being written to disk.
            crate::translate::expr::emit_custom_type_encode_columns(
                program,
                resolver,
                bt.columns(),
                new_start,
                None,
                &bt.name,
                &layout,
            )?;

            // Post-encode TypeCheck: validate encoded values match storage type.
            program.emit_insn(Insn::TypeCheck {
                start_reg: new_start,
                count: layout.num_non_virtual_cols(),
                check_generated: true,
                table_reference: BTreeTable::type_check_table_ref(&bt, resolver.schema()),
            });
        } else {
            // For non-STRICT tables, apply column affinity to the values.
            // This must happen early so that both index records and the table record
            // use the converted values.
            let affinity = bt
                .columns()
                .iter()
                .filter(|c| !c.is_virtual_generated())
                .map(|c| c.affinity());

            if affinity.clone().any(|a| a != Affinity::Blob) {
                if let Ok(count) = NonZeroUsize::try_from(layout.num_non_virtual_cols()) {
                    program.emit_insn(Insn::Affinity {
                        start_reg: new_start,
                        count,
                        affinities: affinity.map(|a| a.aff_mask()).collect(),
                    });
                }
            }
        }

        // Evaluate CHECK constraints on the new values
        emit_check_constraints(
            program,
            &bt.check_constraints,
            resolver,
            &bt.name,
            new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg),
            bt.columns().iter().enumerate().filter_map(|(idx, col)| {
                col.name
                    .as_deref()
                    .map(|n| (n, layout.to_register(new_start, idx)))
            }),
            connection,
            ast::ResolveType::Abort,
            ctx.loop_labels.row_done,
            Some(table_references),
        )?;
    }

    let (directly_changed_cols, rowid_changed) = collect_changed_cols(table, set_pairs);

    // Fire BEFORE UPDATE triggers
    let upsert_database_id = ctx.database_id;
    let preserved_old_registers: Option<Vec<usize>> = if let Some(btree_table) = table.btree() {
        let updated_column_indices: ColumnMask = set_pairs
            .iter()
            .map(|(col_idx, _)| *col_idx)
            .try_collect()?;
        let relevant_before_update_triggers = get_triggers_including_temp(
            resolver,
            upsert_database_id,
            TriggerEvent::Update,
            TriggerTime::Before,
            Some(updated_column_indices.clone()),
            &btree_table,
        );
        // OLD row values are in current_start registers
        let old_registers: Vec<usize> = (0..num_cols)
            .map(|i| layout.to_register(current_start, i))
            .chain(std::iter::once(ctx.conflict_rowid_reg))
            .collect();
        if !relevant_before_update_triggers.is_empty() {
            // NEW row values are in new_start registers. At this point they are
            // encoded (post-encode for STRICT custom types). Mark new_encoded=true
            // so fire_trigger's decode_trigger_registers will decode them.
            let new_rowid_for_trigger = new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg);
            let new_registers: Vec<usize> = (0..num_cols)
                .map(|i| layout.to_register(new_start, i))
                .chain(std::iter::once(new_rowid_for_trigger))
                .collect();

            // In UPSERT DO UPDATE context, trigger's INSERT/UPDATE OR IGNORE/REPLACE
            // clauses should not suppress errors. Override conflict resolution to Abort.
            // Use new_after variant because NEW values are encoded at this point.
            let trigger_ctx = TriggerContext::new_after_with_override_conflict(
                btree_table.clone(),
                Some(new_registers),
                Some(old_registers.clone()),
                ast::ResolveType::Abort,
            );

            for trigger in relevant_before_update_triggers {
                fire_trigger(
                    program,
                    resolver,
                    trigger,
                    &trigger_ctx,
                    connection,
                    upsert_database_id,
                    ctx.loop_labels.row_done,
                )?;
            }

            // BEFORE UPDATE triggers may have altered the btree, need to re-seek
            program.emit_insn(Insn::NotExists {
                cursor: ctx.cursor_id,
                rowid_reg: ctx.conflict_rowid_reg,
                target_pc: ctx.loop_labels.row_done,
            });

            let has_relevant_after_triggers = has_triggers_including_temp(
                resolver,
                upsert_database_id,
                TriggerEvent::Update,
                Some(&updated_column_indices),
                &btree_table,
            );
            if has_relevant_after_triggers {
                // Preserve OLD registers for AFTER triggers
                let preserved: Vec<usize> = old_registers
                    .iter()
                    .map(|old_reg| {
                        let preserved_reg = program.alloc_register();
                        program.emit_insn(Insn::Copy {
                            src_reg: *old_reg,
                            dst_reg: preserved_reg,
                            extra_amount: 0,
                        });
                        preserved_reg
                    })
                    .collect();
                Some(preserved)
            } else {
                None
            }
        } else {
            // Check if we need to preserve for AFTER triggers
            let has_relevant_after_triggers = has_triggers_including_temp(
                resolver,
                upsert_database_id,
                TriggerEvent::Update,
                Some(&updated_column_indices),
                &btree_table,
            );
            if has_relevant_after_triggers {
                Some(old_registers)
            } else {
                None
            }
        }
    } else {
        None
    };
    let rowid_alias_idx = table.columns().iter().position(|c| c.is_rowid_alias());
    let has_direct_rowid_update = set_pairs
        .iter()
        .any(|(idx, _)| *idx == rowid_alias_idx.unwrap_or(ROWID_SENTINEL));
    let has_user_provided_rowid = if let Some(i) = rowid_alias_idx {
        set_pairs.iter().any(|(idx, _)| *idx == i) || has_direct_rowid_update
    } else {
        has_direct_rowid_update
    };

    let rowid_set_clause_reg = if has_user_provided_rowid {
        Some(new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg))
    } else {
        None
    };
    let updated_positions: ColumnMask = set_pairs
        .iter()
        .map(|(col_idx, _)| *col_idx)
        .try_collect()?;
    if let Some(bt) = table.btree() {
        if connection.foreign_keys_enabled() {
            let rowid_new_reg = new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg);

            // Child-side checks
            if resolver.with_schema(upsert_database_id, |s| s.has_child_fks(bt.name.as_str())) {
                emit_fk_child_update_counters(
                    program,
                    &bt,
                    table.get_name(),
                    ctx.cursor_id,
                    new_start,
                    rowid_new_reg,
                    &directly_changed_cols,
                    upsert_database_id,
                    resolver,
                    &layout,
                )?;
            }
            let upsert_indices: Vec<_> = resolver.with_schema(upsert_database_id, |s| {
                s.get_indices(table.get_name()).cloned().collect()
            });
            let affected_upsert_indices: Vec<_> = upsert_indices
                .iter()
                .filter_map(|idx| {
                    upsert_index_is_affected(table, idx, &directly_changed_cols, rowid_changed)
                        .map(|affected| affected.then_some(idx))
                        .transpose()
                })
                .collect::<crate::Result<_>>()?;
            let _ = emit_fk_update_parent_actions(
                program,
                &bt,
                affected_upsert_indices.into_iter(),
                ctx.cursor_id,
                ctx.conflict_rowid_reg,
                new_start,
                new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg),
                rowid_set_clause_reg,
                &updated_positions,
                ParentKeyNewProbeMode::BeforeWrite,
                upsert_database_id,
                resolver,
            )?;
        }
    }

    // Index maintenance (DELETE old key, INSERT new key), honoring
    // partial-index WHEREs. Mirroring SQLite, every UNIQUE constraint (the
    // rowid first, then each unique index) is verified against the NEW row
    // image before any index entry is touched: a constraint failure must
    // abort the statement without leaving the indexes out of sync with the
    // table (issue #6858).
    let new_rowid = new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg);

    // If SET changed the rowid, ensure no other row already owns the new one.
    if let Some(rnew) = new_rowid_reg {
        let ok = program.allocate_label();

        // If equal to old rowid, skip uniqueness probe
        program.emit_insn(Insn::Eq {
            lhs: rnew,
            rhs: ctx.conflict_rowid_reg,
            target_pc: ok,
            flags: CmpInsFlags::default(),
            collation: program.curr_collation(),
        });

        // If another row already has rnew -> constraint
        program.emit_insn(Insn::NotExists {
            cursor: ctx.cursor_id,
            rowid_reg: rnew,
            target_pc: ok,
        });
        program.emit_insn(Insn::Halt {
            err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
            description: format!(
                "{}.{}",
                table.get_name(),
                table
                    .columns()
                    .iter()
                    .find(|c| c.is_rowid_alias())
                    .and_then(|c| c.name.as_deref())
                    .unwrap_or("rowid")
            ),
            on_error: None,
            description_reg: None,
        });
        program.preassign_label_to_next_insn(ok);
    }

    struct PendingIndexRebuild {
        idx_cid: usize,
        idx_meta: Arc<Index>,
        before_pred_reg: Option<usize>,
        new_pred_reg: Option<usize>,
        ins_start: usize,
        record_reg: usize,
    }

    if let Some(before) = before_start {
        let mut pending_rebuilds: Vec<PendingIndexRebuild> = Vec::new();

        // Pass 1: compute the NEW key for every affected index and probe the
        // unique ones for conflicts, without modifying any index yet.
        for (idx_name, _root, idx_cid) in &ctx.idx_cursors {
            let idx_meta = resolver
                .with_schema(ctx.database_id, |s| {
                    s.get_index(table.get_name(), idx_name).cloned()
                })
                .expect("index exists");

            if !upsert_index_is_affected(table, &idx_meta, &directly_changed_cols, rowid_changed)? {
                continue; // skip untouched index completely
            }
            let k = idx_meta.columns.len();

            let before_pred_reg = eval_partial_pred_for_row_image(
                program,
                table,
                &idx_meta,
                before,
                ctx.conflict_rowid_reg,
                resolver,
                &layout,
            );
            let new_pred_reg = eval_partial_pred_for_row_image(
                program, table, &idx_meta, new_start, new_rowid, resolver, &layout,
            );

            // Skip key computation and probe if NEW predicate false/NULL:
            // a key that fails the partial-index predicate is never inserted,
            // so it cannot conflict.
            let maybe_skip_probe = new_pred_reg.map(|r| {
                let lbl = program.allocate_label();
                program.emit_insn(Insn::IfNot {
                    reg: r,
                    target_pc: lbl,
                    jump_if_null: true,
                });
                lbl
            });

            // NEW key (use NEW rowid if present)
            let ins = program.alloc_registers(k + 1);
            for (i, ic) in idx_meta.columns.iter().enumerate() {
                if ic.expr.is_some() {
                    emit_upsert_expr_index_value(
                        program,
                        resolver,
                        table,
                        ic,
                        new_start,
                        new_rowid,
                        ins + i,
                        &layout,
                    )?;
                } else {
                    let (ci, _) = table.get_column_by_name(&ic.name).unwrap();
                    program.emit_insn(Insn::Copy {
                        src_reg: layout.to_register(new_start, ci),
                        dst_reg: ins + i,
                        extra_amount: 0,
                    });
                }
            }
            program.emit_insn(Insn::Copy {
                src_reg: new_rowid,
                dst_reg: ins + k,
                extra_amount: 0,
            });

            let rec = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(ins),
                count: to_u32(k + 1),
                dest_reg: to_u32(rec),
                index_name: Some((*idx_name).clone()),
                affinity_str: None,
            });

            if idx_meta.unique {
                // Affinity on the key columns for the NoConflict probe
                let ok = program.allocate_label();
                let aff: String = idx_meta
                    .columns
                    .iter()
                    .map(|c| {
                        c.expr.as_ref().map_or_else(
                            || {
                                table
                                    .get_column_by_name(&c.name)
                                    .map(|(_, col)| {
                                        let is_strict =
                                            table.btree().is_some_and(|btree| btree.is_strict);
                                        col.affinity_with_strict(is_strict).aff_mask()
                                    })
                                    .unwrap_or('B')
                            },
                            |_| crate::vdbe::affinity::Affinity::Blob.aff_mask(),
                        )
                    })
                    .collect();

                program.emit_insn(Insn::Affinity {
                    start_reg: ins,
                    count: NonZeroUsize::new(k).unwrap(),
                    affinities: aff,
                });
                program.emit_insn(Insn::NoConflict {
                    cursor_id: *idx_cid,
                    target_pc: ok,
                    record_reg: ins,
                    num_regs: k,
                });
                let hit = program.alloc_register();
                program.emit_insn(Insn::IdxRowId {
                    cursor_id: *idx_cid,
                    dest: hit,
                });
                // A hit on the row being updated is not a conflict: its old
                // key is deleted before the new one is inserted below.
                program.emit_insn(Insn::Eq {
                    lhs: ctx.conflict_rowid_reg,
                    rhs: hit,
                    target_pc: ok,
                    flags: CmpInsFlags::default(),
                    collation: program.curr_collation(),
                });
                let description = format_unique_violation_desc(table.get_name(), &idx_meta);
                program.emit_insn(Insn::Halt {
                    err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                    description,
                    on_error: None,
                    description_reg: None,
                });
                program.preassign_label_to_next_insn(ok);
            }

            if let Some(lbl) = maybe_skip_probe {
                program.preassign_label_to_next_insn(lbl);
            }

            pending_rebuilds.push(PendingIndexRebuild {
                idx_cid: *idx_cid,
                idx_meta,
                before_pred_reg,
                new_pred_reg,
                ins_start: ins,
                record_reg: rec,
            });
        }

        // Pass 2: every UNIQUE constraint holds, so the index mutations can
        // no longer be interrupted by a constraint failure.
        for pending in pending_rebuilds {
            let k = pending.idx_meta.columns.len();

            // Skip delete if BEFORE predicate false/NULL
            let maybe_skip_del = pending.before_pred_reg.map(|r| {
                let lbl = program.allocate_label();
                program.emit_insn(Insn::IfNot {
                    reg: r,
                    target_pc: lbl,
                    jump_if_null: true,
                });
                lbl
            });

            // DELETE old key
            let del = program.alloc_registers(k + 1);
            for (i, ic) in pending.idx_meta.columns.iter().enumerate() {
                if ic.expr.is_some() {
                    emit_upsert_expr_index_value(
                        program,
                        resolver,
                        table,
                        ic,
                        before,
                        ctx.conflict_rowid_reg,
                        del + i,
                        &layout,
                    )?;
                } else {
                    let (ci, _) = table.get_column_by_name(&ic.name).unwrap();
                    program.emit_insn(Insn::Copy {
                        src_reg: layout.to_register(before, ci),
                        dst_reg: del + i,
                        extra_amount: 0,
                    });
                }
            }
            program.emit_insn(Insn::Copy {
                src_reg: ctx.conflict_rowid_reg,
                dst_reg: del + k,
                extra_amount: 0,
            });
            program.emit_insn(Insn::IdxDelete {
                start_reg: del,
                num_regs: k + 1,
                cursor_id: pending.idx_cid,
                raise_error_if_no_matching_entry: false,
            });
            if let Some(label) = maybe_skip_del {
                program.preassign_label_to_next_insn(label);
            }

            // Skip insert if NEW predicate false/NULL
            let maybe_skip_ins = pending.new_pred_reg.map(|r| {
                let lbl = program.allocate_label();
                program.emit_insn(Insn::IfNot {
                    reg: r,
                    target_pc: lbl,
                    jump_if_null: true,
                });
                lbl
            });

            program.emit_insn(Insn::IdxInsert {
                cursor_id: pending.idx_cid,
                record_reg: pending.record_reg,
                unpacked_start: Some(pending.ins_start),
                unpacked_count: Some((k + 1) as u32),
                flags: IdxInsertFlags::new().nchange(true),
            });

            if let Some(lbl) = maybe_skip_ins {
                program.preassign_label_to_next_insn(lbl);
            }
        }
    }

    // Build NEW table payload
    let record_reg = program.alloc_register();
    emit_make_record(
        program,
        table.columns().iter(),
        new_start,
        record_reg,
        table.btree().is_some_and(|bt| bt.is_strict),
    );

    // If rowid changed, delete+insert (uniqueness of the new rowid was
    // already verified before index maintenance above)
    if let Some(rnew) = new_rowid_reg {
        // important: the cursor was repositioned in the earlier rowid uniqueness
        // probe via NotExists, so we need to re-seek to the row under update.
        program.emit_insn(Insn::SeekRowid {
            cursor_id: ctx.cursor_id,
            src_reg: ctx.conflict_rowid_reg,
            target_pc: ctx.loop_labels.row_done,
        });

        // Now replace the row
        program.emit_insn(Insn::Delete {
            cursor_id: ctx.cursor_id,
            table_name: table.get_name().to_string(),
            is_part_of_update: true,
        });
        program.emit_insn(Insn::Insert {
            cursor: ctx.cursor_id,
            key_reg: rnew,
            record_reg,
            flag: InsertFlags::new()
                .require_seek()
                .update_rowid_change()
                .skip_last_rowid(),
            table_name: table.get_name().to_string(),
        });

        // MVCC AUTOINCREMENT: an ON CONFLICT DO UPDATE that moves the rowid
        // forward must advance the implicit sequence past the new rowid, just
        // like an explicit-rowid INSERT or a plain UPDATE does. The MVCC
        // allocator trusts the backing-table watermark ONLY (it never consults
        // MAX(rowid)), so without this a later AUTOINCREMENT INSERT would emit
        // a value at or below the manually-set rowid and eventually collide.
        // WAL mode is unaffected: its NewRowid path already takes the max of
        // sqlite_sequence.seq and MAX(rowid), so it skips past the new rowid
        // automatically. Mirrors the UPDATE path in emitter/update.rs.
        if table.btree().is_some_and(|bt| bt.has_autoincrement)
            && connection.mv_store_for_db(upsert_database_id).is_some()
        {
            let seq_name = crate::schema::autoincrement_sequence_name(table.get_name());
            let seq = resolver
                .with_schema(upsert_database_id, |s| s.get_sequence(&seq_name).cloned())
                .ok_or_else(|| {
                    crate::LimboError::InternalError(format!(
                        "missing implicit sequence for AUTOINCREMENT table \"{}\"",
                        table.get_name()
                    ))
                })?;
            crate::translate::sequence::emit_disk_advance_past(
                program,
                resolver,
                upsert_database_id,
                &seq_name,
                &seq,
                rnew,
            )?;
        }
    } else {
        program.emit_insn(Insn::Insert {
            cursor: ctx.cursor_id,
            key_reg: ctx.conflict_rowid_reg,
            record_reg,
            flag: InsertFlags::new().skip_last_rowid(),
            table_name: table.get_name().to_string(),
        });
    }

    // Fire FK actions (CASCADE, SET NULL, SET DEFAULT) for parent-side updates.
    // This must be done after the update is complete but before AFTER triggers.
    if let Some(bt) = table.btree() {
        if connection.foreign_keys_enabled()
            && resolver.with_schema(upsert_database_id, |s| {
                s.any_resolved_fks_referencing(bt.name.as_str())
            })
        {
            fire_fk_update_actions(
                program,
                resolver,
                bt.name.as_str(),
                ctx.conflict_rowid_reg, // old_rowid_reg
                current_start,          // old_values_start
                new_start,              // new_values_start
                new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg), // new_rowid_reg
                connection,
                upsert_database_id,
            )?;
        }
    }

    // emit CDC instructions
    if let Some((cdc_id, _)) = ctx.cdc_table {
        let new_rowid = new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg);
        if new_rowid_reg.is_some() {
            // DELETE (before)
            let before_rec = if program.capture_data_changes_info().has_before() {
                Some(emit_cdc_full_record(
                    program,
                    table.columns(),
                    ctx.cursor_id,
                    ctx.conflict_rowid_reg,
                    table.btree().is_some_and(|btree| btree.is_strict),
                ))
            } else {
                None
            };
            emit_cdc_insns(
                program,
                resolver,
                OperationMode::DELETE,
                cdc_id,
                ctx.conflict_rowid_reg,
                before_rec,
                None,
                None,
                table.get_name(),
            )?;

            // INSERT (after)
            let after_rec = if program.capture_data_changes_info().has_after() {
                Some(emit_cdc_patch_record(
                    program, table, new_start, record_reg, new_rowid, &layout,
                ))
            } else {
                None
            };
            emit_cdc_insns(
                program,
                resolver,
                OperationMode::INSERT,
                cdc_id,
                new_rowid,
                None,
                after_rec,
                None,
                table.get_name(),
            )?;
        } else {
            let after_rec = if program.capture_data_changes_info().has_after() {
                Some(emit_cdc_patch_record(
                    program,
                    table,
                    new_start,
                    record_reg,
                    ctx.conflict_rowid_reg,
                    &layout,
                ))
            } else {
                None
            };
            let before_rec = if program.capture_data_changes_info().has_before() {
                Some(emit_cdc_full_record(
                    program,
                    table.columns(),
                    ctx.cursor_id,
                    ctx.conflict_rowid_reg,
                    table.btree().is_some_and(|btree| btree.is_strict),
                ))
            } else {
                None
            };
            emit_cdc_insns(
                program,
                resolver,
                OperationMode::UPDATE(UpdateRowSource::Normal),
                cdc_id,
                ctx.conflict_rowid_reg,
                before_rec,
                after_rec,
                None,
                table.get_name(),
            )?;
        }
    }

    // Fire AFTER UPDATE triggers
    if let (Some(btree_table), Some(old_regs)) = (table.btree(), preserved_old_registers) {
        let updated_column_indices: ColumnMask = set_pairs
            .iter()
            .map(|(col_idx, _)| *col_idx)
            .try_collect()?;
        let relevant_triggers = get_triggers_including_temp(
            resolver,
            upsert_database_id,
            TriggerEvent::Update,
            TriggerTime::After,
            Some(updated_column_indices),
            &btree_table,
        );
        if !relevant_triggers.is_empty() {
            let new_rowid_for_trigger = new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg);
            let new_registers_after: Vec<usize> = (0..num_cols)
                .map(|i| layout.to_register(new_start, i))
                .chain(std::iter::once(new_rowid_for_trigger))
                .collect();

            // In UPSERT DO UPDATE context, trigger's INSERT/UPDATE OR IGNORE/REPLACE
            // clauses should not suppress errors. Override conflict resolution to Abort.
            // NEW values are encoded at this point; fire_trigger will decode them.
            let trigger_ctx_after = TriggerContext::new_after_with_override_conflict(
                btree_table,
                Some(new_registers_after),
                Some(old_regs),
                ast::ResolveType::Abort,
            );

            // RAISE(IGNORE) in an AFTER trigger should only abort the trigger body,
            // not skip post-row work (RETURNING).
            let after_trigger_done = program.allocate_label();
            for trigger in relevant_triggers {
                fire_trigger(
                    program,
                    resolver,
                    trigger,
                    &trigger_ctx_after,
                    connection,
                    upsert_database_id,
                    after_trigger_done,
                )?;
            }
            program.preassign_label_to_next_insn(after_trigger_done);
        }
    }

    // Compute virtual columns for RETURNING (if any virtual columns exist)
    if !returning.is_empty() && ctx.table.has_virtual_columns() {
        let rowid_reg = new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg);
        let dml_ctx =
            DmlColumnContext::layout(ctx.table.columns(), new_start, rowid_reg, layout.clone());
        compute_virtual_columns(
            program,
            &ctx.table.columns_topo_sort()?,
            &dml_ctx,
            resolver,
            ctx.table,
        )?;
    }

    // RETURNING from NEW image + final rowid
    if !returning.is_empty() {
        emit_returning_results(
            program,
            table_references,
            returning,
            new_start,
            new_rowid_reg.unwrap_or(ctx.conflict_rowid_reg),
            resolver,
            ctx.returning_buffer.as_ref(),
            &layout,
        )?;
    }

    program.emit_insn(Insn::Goto {
        target_pc: ctx.loop_labels.row_done,
    });
    Ok(())
}

/// Normalize the `SET` clause into `(column_index, Expr)` pairs using table layout.
///
/// Supports multi-target row-value SETs: `SET (a, b) = (expr1, expr2)`.
/// Enforces same number of column names and RHS values.
/// If the same column is assigned multiple times, the last assignment wins.
pub fn collect_set_clauses_for_upsert(
    table: &Table,
    set_items: &mut [ast::Set],
) -> crate::Result<Vec<(usize, Box<ast::Expr>)>> {
    let lookup: HashMap<String, usize> = table
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(i, c)| c.name.as_ref().map(|n| (n.to_lowercase(), i)))
        .collect();

    let mut out: Vec<(usize, Box<ast::Expr>)> = vec![];

    for set in set_items {
        let values: Vec<Box<ast::Expr>> = match set.expr.as_ref() {
            ast::Expr::Parenthesized(v) => v.clone(),
            e => vec![e.clone().into()],
        };
        if set.col_names.len() != values.len() {
            bail_parse_error!(
                "{} columns assigned {} values",
                set.col_names.len(),
                values.len()
            );
        }
        for (cn, e) in set.col_names.iter().zip(values.into_iter()) {
            let Some(idx) = lookup.get(&normalize_ident(cn.as_str())) else {
                bail_parse_error!("no such column: {}", cn);
            };
            // cannot upsert generated column
            table.columns()[*idx].ensure_not_generated("UPDATE", cn.as_str())?;
            if let Some(existing) = out.iter_mut().find(|(i, _)| *i == *idx) {
                existing.1 = e;
            } else {
                out.push((*idx, e));
            }
        }
    }
    Ok(out)
}

fn eval_partial_pred_for_row_image(
    prg: &mut ProgramBuilder,
    table: &Table,
    idx: &Index,
    row_start: usize, // base of CURRENT or NEW image
    rowid_reg: usize, // rowid for that image
    resolver: &Resolver,
    layout: &ColumnLayout,
) -> Option<usize> {
    let Some(where_expr) = &idx.where_clause else {
        return None;
    };
    let expr = where_expr.as_ref().clone();
    let columns = table.columns();
    let bt = table.require_btree().ok()?;

    let mut column_regs: Vec<usize> = columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            if col.is_rowid_alias() {
                rowid_reg
            } else {
                layout.to_register(row_start, i)
            }
        })
        .collect();

    let r = prg.alloc_register();
    crate::translate::expr::emit_dml_expr_index_value(
        prg,
        resolver,
        expr,
        columns,
        &mut column_regs,
        &bt,
        r,
    )
    .ok()?;
    Some(r)
}

#[allow(clippy::too_many_arguments)]
fn emit_upsert_expr_index_value(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table: &Table,
    idx_col: &IndexColumn,
    row_start: usize,
    rowid_reg: usize,
    dest_reg: usize,
    layout: &ColumnLayout,
) -> crate::Result<()> {
    let expr = idx_col.expr.as_ref().expect("caller checked is_some");
    let expr = expr.as_ref().clone();
    let columns = table.columns();
    let bt = table.require_btree()?;

    let mut column_regs: Vec<usize> = columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            if col.is_rowid_alias() {
                rowid_reg
            } else {
                layout.to_register(row_start, i)
            }
        })
        .collect();
    crate::translate::expr::emit_dml_expr_index_value(
        program,
        resolver,
        expr,
        columns,
        &mut column_regs,
        &bt,
        dest_reg,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn rewrite_expr_to_registers(
    e: &mut ast::Expr,
    table: &Table,
    base_start: usize,
    rowid_reg: usize,
    table_name: Option<&str>,
    table_alias: Option<&str>,
    insertion: Option<&Insertion>,
    allow_excluded: bool,
    excluded_decoded_start: Option<usize>,
    layout: &ColumnLayout,
) -> crate::Result<WalkControl> {
    use ast::Expr;
    let table_name_norm = table_name.map(normalize_ident);

    // Map a column name to a register within the row image at `base_start`.
    let col_reg_from_row_image = |name: &str| -> Option<usize> {
        if ROWID_STRS.iter().any(|s| s.eq_ignore_ascii_case(name)) {
            return Some(rowid_reg);
        }
        let (idx, c) = table.get_column_by_name(name)?;
        if c.is_rowid_alias() {
            Some(rowid_reg)
        } else {
            Some(base_start + layout.to_reg_offset(idx))
        }
    };

    walk_expr_mut(
        e,
        &mut |expr: &mut ast::Expr| -> crate::Result<WalkControl> {
            match expr {
                Expr::Qualified(ns, c) | Expr::DoublyQualified(_, ns, c) => {
                    let ns = normalize_ident(ns.as_str());
                    let c = normalize_ident(c.as_str());
                    // An INSERT target alias replaces the base table name in
                    // the DO UPDATE scope.  It also shadows the special
                    // `excluded` pseudo-table when the alias is literally
                    // named `excluded` (SQLite's name-resolution rule).
                    let is_target_namespace = if let Some(alias) = table_alias {
                        ns.eq_ignore_ascii_case(alias)
                    } else {
                        table_name_norm
                            .as_ref()
                            .is_some_and(|tn| ns.eq_ignore_ascii_case(tn))
                    };
                    // Handle EXCLUDED.* if enabled
                    if allow_excluded && ns.eq_ignore_ascii_case("excluded") && !is_target_namespace
                    {
                        if let Some(ins) = insertion {
                            if ROWID_STRS.iter().any(|s| s.eq_ignore_ascii_case(&c)) {
                                *expr = Expr::Register(ins.key_register());
                            } else if let Some(cm) = ins.get_col_mapping_by_name(&c) {
                                // Use decoded excluded registers when available
                                // to prevent double-encoding of custom type values
                                if let Some(decoded_start) = excluded_decoded_start {
                                    let (col_idx, _) =
                                        table.get_column_by_name(&c).expect("column exists");
                                    *expr = Expr::Register(
                                        decoded_start + layout.to_reg_offset(col_idx),
                                    );
                                } else {
                                    *expr = Expr::Register(cm.register);
                                }
                            } else {
                                bail_parse_error!("no such column in EXCLUDED: {}", c);
                            }
                        }
                        // If insertion is None, leave EXCLUDED.* untouched.
                        return Ok(WalkControl::Continue);
                    }

                    // Match the target table namespace if provided
                    if is_target_namespace {
                        if let Some(r) = col_reg_from_row_image(&c) {
                            *expr = Expr::Register(r);
                        } else {
                            bail_parse_error!("no such column: {}.{}", ns, c);
                        }
                        return Ok(WalkControl::Continue);
                    }

                    // In UPSERT DO UPDATE context (allow_excluded=true), a qualified
                    // reference that doesn't match the target table or EXCLUDED is
                    // invalid. Return a graceful error instead of leaving it
                    // unresolved (which would panic later in translate_expr).
                    if allow_excluded {
                        bail_parse_error!("no such column: {}.{}", ns, c);
                    }
                }
                // Unqualified id -> row image (CURRENT/NEW depending on caller)
                Expr::Id(name) => {
                    if let Some(r) = col_reg_from_row_image(&normalize_ident(name.as_str())) {
                        *expr = Expr::Register(r);
                    }
                }
                _ => {}
            }
            Ok(WalkControl::Continue)
        },
    )
}
